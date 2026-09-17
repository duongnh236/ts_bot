"""Protocol-driven battle lifecycle and state."""

from __future__ import annotations

from dataclasses import dataclass
import logging
import struct

log = logging.getLogger("bot")


ROLE_HEADER_FORMAT = "<BB8sH8sBBIIIIHBB"
ROLE_HEADER_SIZE = struct.calcsize(ROLE_HEADER_FORMAT)
# role_kind = EHuman (RoleController.lua). CHI 2 nhom co DU LIEU THEM sau header, dung y
# FightField.RoleAppear:
#   if kind in (Player=1, Players=2, Divide=9, AutomanualPlayer=28) -> ngoai hinh nguoi choi
#   elif kind in (FollowNpc=4, AutomanualNpc=29)                    -> [L][ten]
#   con lai (ke ca MapNpc=3) -> KHONG co gi them
#
# BUG THAT (user: "vao tran ma bot khong danh", log 12:31:25):
#   PLAYER_ROLE_KINDS cu = (1,2,3,5) -> co 3 = EHuman.MapNpc = QUAI. Bot coi QUAI la nguoi choi
#   roi di doc phan ngoai hinh KHONG HE TON TAI -> goi 42 byte (dung bang ROLE_HEADER_SIZE, tuc
#   header an het, khong con byte nao) -> parse hong -> VUT SACH ca roster -> tracker khong co
#   unit nao -> available rong -> _arm_decision khong bao gio duoc goi -> BOT DUNG IM.
#   Con 5 = SceneElm va 6 = GuardNpc thi Lua KHONG doc them gi -> cung phai bo ra.
PLAYER_ROLE_KINDS = frozenset((1, 2, 9, 28))
NAMED_NPC_ROLE_KINDS = frozenset((4, 29))
ACTION_EFFECT_STATUS_KIND = {
    11014: 1,  # Bang Phong
    11039: 1,
    10010: 2,  # Ket Gioi
    10041: 2,
}


@dataclass
class BattleUnit:
    row: int
    col: int
    role_id: bytes
    template_id: int
    master_id: bytes
    war_type: int
    role_kind: int
    hp: int
    hp_max: int
    sp: int
    sp_max: int
    level: int
    upgrade_level: int
    element: int
    name: str = ""
    alive: bool = True
    state: str = "active"

    @property
    def position(self):
        return self.row, self.col


@dataclass(frozen=True)
class BattleEvent:
    kind: str
    generation: int
    turn: int
    position: tuple[int, int] | None = None
    source: tuple[int, int] | None = None
    skill_id: int = 0
    payload: tuple = ()


@dataclass(frozen=True)
class BattleSnapshot:
    generation: int
    turn: int
    revision: int
    active: bool
    units: tuple
    statuses: tuple


class BattleTracker:
    def __init__(self, local_role_id: bytes = b""):
        self.local_role_id = bytes(local_role_id)
        self._end_warned = set()   # (generation, ly do) da log - xem _log_end_bo
        self.generation = 0
        self.turn = 0
        self.revision = 0
        self.active = False
        self.terrain = 0
        self.fight_number = 0
        self.war_style = 0
        self.round_limit = 0
        self.limit_kind = 0
        self.limit_value = 0
        self.units = {}
        self.statuses = {}
        self.buffs = {}
        self.extra_buffs = {}
        self.pending_actions = {}

    def snapshot(self):
        units = tuple(
            (position, tuple(vars(unit).items()))
            for position, unit in sorted(self.units.items())
        )
        statuses = tuple(
            (position, tuple(sorted(by_kind.items())))
            for position, by_kind in sorted(self.statuses.items())
        )
        return BattleSnapshot(
            self.generation,
            self.turn,
            self.revision,
            self.active,
            units,
            statuses,
        )

    def restore_snapshot(self, snapshot: BattleSnapshot):
        if not isinstance(snapshot, BattleSnapshot) or not snapshot.active:
            return False
        self.generation = snapshot.generation
        self.turn = snapshot.turn
        self.revision = snapshot.revision
        self.active = True
        self.units = {
            tuple(position): BattleUnit(**dict(attributes))
            for position, attributes in snapshot.units
        }
        self.statuses = {
            tuple(position): dict(by_kind)
            for position, by_kind in snapshot.statuses
        }
        self.buffs = {}
        self.extra_buffs = {}
        self.pending_actions = {}
        return True

    def apply(self, opcode: int, body: bytes):
        body = self._packet_body(opcode, body)
        if len(body) < 2:
            return ()
        sub = int.from_bytes(body[:2], "little")
        data = body[2:]
        if opcode == 0x0B:
            return self._apply_0x0b(sub, data)
        if opcode == 0x34 and sub == 1:
            return self._start_turn(data)
        if opcode == 0x35:
            return self._apply_0x35(sub, data)
        if opcode == 0x32 and sub == 1:
            return self._apply_actions(data)
        if opcode == 0x33 and sub == 1:
            return self._apply_absolute(data)
        return ()

    @staticmethod
    def _packet_body(opcode: int, packet: bytes):
        if len(packet) > 7 and packet[6] == opcode:
            return packet[7:]
        return packet

    def _apply_0x0b(self, sub: int, data: bytes):
        if sub == 0xFA:
            return self._create(data)
        if sub == 0x0A:
            return self._set_war_style(data)
        if sub == 0:
            return self._end(data)
        if sub == 1:
            return self._exit(data)
        if sub == 5:
            return self._spawn(data)
        return ()

    def _create(self, data: bytes):
        if len(data) < 3:
            return ()
        # _create THAY NGUYEN tran -> du lieu MOT PHAN o day la nguy hiem (goi cat cut se xoa
        # mat tran dang chay). Giu nghiem: hong thi KHONG dung gi ca.
        units = self._parse_roles(data[3:], tag="create")
        if units is None:
            return ()
        self.generation += 1
        self.turn = 0
        self.revision += 1
        self.active = True
        self.terrain = data[0]
        self.fight_number = int.from_bytes(data[1:3], "little")
        self.units = {unit.position: unit for unit in units}
        self.statuses = {}
        self.buffs = {}
        self.extra_buffs = {}
        self.pending_actions = {}
        events = [self._event("start", payload=(self.terrain, self.fight_number))]
        events.extend(self._spawn_event(unit) for unit in units)
        return tuple(events)

    def _spawn(self, data: bytes):
        # _spawn chi THEM unit vao tran dang co -> lay duoc bao nhieu tot bay nhieu. Truoc day
        # hong 1 ban ghi la bo SACH danh sach (im lang) -> mat het roster quai.
        units = self._parse_roles(data, tag="spawn", mot_phan=True)
        if not units:
            return ()
        for unit in units:
            self.units[unit.position] = unit
        if units:
            self.revision += 1
        return tuple(self._spawn_event(unit) for unit in units)

    @classmethod
    def _parse_roles(cls, data: bytes, tag: str = "", mot_phan: bool = False):
        """Doc danh sach nhan vat tham chien (S:011-005 / S:011-250).

        TRUOC DAY: gap bat ky loi nao la `return None` -> caller VUT BO TOAN BO danh sach, va
        KHONG LOG GI. Hau qua that (party 1, map thap 2K): phe minh (role_kind thuoc
        PLAYER_ROLE_KINDS) parse duoc, nhung neu ban ghi QUAI co role_kind la khac -> lech con tro
        -> mat SACH roster quai -> tracker khong co muc tieu nao -> BOT VAO TRAN KHONG DANH, ma
        khong mot dong log nao.
        NAY: giu lai nhung ban ghi DA doc duoc truoc cho hong (chung parse voi offset dung nen van
        tin duoc), va LOG ro hong o dau. Du lieu MOT PHAN van hon khong co gi."""
        units = []
        offset = 0
        while offset < len(data):
            chunk = data[offset:offset + ROLE_HEADER_SIZE]
            if len(chunk) != ROLE_HEADER_SIZE:
                return cls._hong(tag, "thieu byte header", offset, data, units, mot_phan)
            values = struct.unpack_from(ROLE_HEADER_FORMAT, chunk)
            war_type, role_kind, role_id, template_id, master_id = values[:5]
            row, col, hp_max, sp_max, hp, sp, level, upgrade, element = values[5:]
            if row > 3 or col > 5:
                return cls._hong(tag, "row=%d col=%d ngoai bang (role_kind=%d)"
                                 % (row, col, role_kind), offset, data, units, mot_phan)
            cursor = offset + ROLE_HEADER_SIZE
            name = ""
            if role_kind in PLAYER_ROLE_KINDS:
                parsed = cls._parse_player_appearance(data, cursor)
                if parsed is None:
                    return cls._hong(tag, "doc ngoai hinh nguoi choi hong (role_kind=%d)"
                                     % role_kind, offset, data, units, mot_phan)
                name, cursor = parsed
            elif role_kind in NAMED_NPC_ROLE_KINDS:
                parsed = cls._parse_name(data, cursor)
                if parsed is None:
                    return cls._hong(tag, "doc ten NPC hong (role_kind=%d)" % role_kind,
                                     offset, data, units, mot_phan)
                name, cursor = parsed
            units.append(BattleUnit(
                row=row,
                col=col,
                role_id=role_id,
                template_id=template_id,
                master_id=master_id,
                war_type=war_type,
                role_kind=role_kind,
                hp=hp,
                hp_max=hp_max,
                sp=sp,
                sp_max=sp_max,
                level=level,
                upgrade_level=upgrade,
                element=element,
                name=name,
                alive=hp > 0,
            ))
            offset = cursor
        return units

    _parse_warned = set()

    @classmethod
    def _hong(cls, tag, ly_do, offset, data, units, mot_phan):
        """Log roi tra ket qua: nhanh CHO PHEP mot phan thi giu nhung ban ghi doc duoc (chung
        parse voi offset dung nen van tin duoc); nhanh NGHIEM (create) tra None de caller khong
        dung gi ca."""
        cls._log_parse_hong(tag, ly_do, offset, data, units)
        return units if mot_phan else None

    @classmethod
    def _log_parse_hong(cls, tag: str, ly_do: str, offset: int, data: bytes, units):
        """Log 1 lan cho moi ly do - de biet NGAY ban ghi nao lam lech, thay vi im lang mat roster."""
        if ly_do.split("(")[0] in cls._parse_warned:
            return
        if len(cls._parse_warned) > 50:
            cls._parse_warned.clear()
        cls._parse_warned.add(ly_do.split("(")[0])
        log.warning("[BATTLE] doc roster%s HONG tai offset %d/%d: %s -> giu %d ban ghi doc duoc, "
                    "BO phan con lai. raw[%d:%d]=%s",
                    (" " + tag) if tag else "", offset, len(data), ly_do, len(units),
                    offset, offset + 24, data[offset:offset + 24].hex())

    @staticmethod
    def _parse_name(data: bytes, offset: int):
        if offset >= len(data):
            return None
        size = data[offset]
        end = offset + 1 + size
        if end > len(data):
            return None
        try:
            name = data[offset + 1:end].decode("utf-16le")
        except UnicodeDecodeError:
            return None
        return name, end

    @classmethod
    def _parse_player_appearance(cls, data: bytes, offset: int):
        parsed = cls._parse_name(data, offset)
        if parsed is None:
            return None
        name, cursor = parsed
        if cursor + 28 > len(data):
            return None
        equip_count = data[cursor + 27]
        cursor += 28 + equip_count * 2
        if cursor >= len(data):
            return None
        outfit_count = data[cursor]
        cursor += 1 + outfit_count * 2
        if cursor + 5 > len(data):
            return None
        return name, cursor + 5

    def _spawn_event(self, unit: BattleUnit):
        return self._event(
            "spawn",
            position=unit.position,
            payload=(unit.role_id, unit.template_id, unit.name),
        )

    def _set_war_style(self, data: bytes):
        if len(data) != 8:
            return ()
        war_style, round_limit, limit_kind, limit_value = struct.unpack("<BHBI", data)
        self.war_style = war_style
        self.round_limit = round_limit
        self.limit_kind = limit_kind
        self.limit_value = limit_value
        self.revision += 1
        return (self._event("war_style", payload=(war_style, round_limit, limit_kind, limit_value)),)

    def _apply_0x35(self, sub: int, data: bytes):
        handlers = {
            1: self._restore_status,
            3: self._flyout,
            5: self._ack,
            7: self._move,
            14: self._transform,
            15: self._buff,
            20: self._extra_buff,
        }
        handler = handlers.get(sub)
        return handler(data) if handler else ()

    def _restore_status(self, data: bytes):
        if len(data) % 5:
            return ()
        records = []
        for offset in range(0, len(data), 5):
            row, col, status_kind, skill_id = struct.unpack_from("<BBBH", data, offset)
            if row > 3 or col > 5 or not 1 <= status_kind <= 6:
                return ()
            records.append((row, col, status_kind, skill_id))
        events = []
        for row, col, status_kind, skill_id in records:
            position = (row, col)
            by_kind = self.statuses.setdefault(position, {})
            if skill_id:
                by_kind[status_kind] = skill_id
            else:
                by_kind.pop(status_kind, None)
                if not by_kind:
                    self.statuses.pop(position, None)
            events.append(self._event(
                "status",
                position=position,
                skill_id=skill_id,
                payload=(status_kind,),
            ))
        if records:
            self.revision += 1
        return tuple(events)

    def _flyout(self, data: bytes):
        if len(data) != 2:
            return ()
        position = tuple(data)
        unit = self.units.get(position)
        if unit is not None:
            unit.alive = False
            unit.state = "flyout"
        self.revision += 1
        return (self._event("flyout", position=position),)

    def register_action(self, source, skill_id: int, target):
        if not self.active:
            return False
        self.pending_actions[tuple(source)] = (self.generation, self.turn, skill_id, tuple(target))
        return True

    def confirm_end(self):
        if not self.active:
            return ()
        self.active = False
        self.revision += 1
        self.pending_actions = {}
        return (self._event("end"),)

    def _ack(self, data: bytes):
        if len(data) != 2:
            return ()
        source = tuple(data)
        self.pending_actions.pop(source, None)
        self.revision += 1
        return (self._event("ack", source=source),)

    def _move(self, data: bytes):
        if len(data) != 4:
            return ()
        source = tuple(data[:2])
        target = tuple(data[2:])
        unit = self.units.get(source)
        if unit is None or target[0] > 3 or target[1] > 5:
            return ()
        self.units.pop(target, None)
        self.units.pop(source)
        unit.row, unit.col = target
        self.units[target] = unit
        self._move_position_state(source, target)
        self.revision += 1
        return (self._event("move", source=source, position=target),)

    def _move_position_state(self, source, target):
        for collection in (self.statuses, self.buffs, self.extra_buffs):
            collection.pop(target, None)
            value = collection.pop(source, None)
            if value is not None:
                collection[target] = value

    def _transform(self, data: bytes):
        if len(data) != 4:
            return ()
        row, col, template_id = struct.unpack("<BBH", data)
        position = (row, col)
        unit = self.units.get(position)
        if unit is None:
            return ()
        unit.template_id = template_id
        self.revision += 1
        return (self._event("transform", position=position, payload=(template_id,)),)

    def _buff(self, data: bytes):
        if len(data) != 6:
            return ()
        row, col, kind, rounds, value = struct.unpack("<BBBBh", data)
        if row > 3 or col > 5:
            return ()
        position = (row, col)
        self.buffs.setdefault(position, {})[kind] = (rounds, value)
        self.revision += 1
        return (self._event("buff", position=position, payload=(kind, rounds, value)),)

    def _extra_buff(self, data: bytes):
        if len(data) < 3:
            return ()
        row, col, count = data[:3]
        record_format = "<HBBHBBBBHi"
        record_size = struct.calcsize(record_format)
        if row > 3 or col > 5 or len(data) != 3 + count * record_size:
            return ()
        records = [
            struct.unpack_from(record_format, data, 3 + index * record_size)
            for index in range(count)
        ]
        position = (row, col)
        by_status = self.extra_buffs.setdefault(position, {})
        events = []
        for record in records:
            skill, level, weight, status_id, status_kind, rounds, layers, caster, attr, value = record
            by_status[status_id] = (
                skill, level, weight, status_kind, rounds, layers, caster, attr, value,
            )
            events.append(self._event(
                "extra_buff",
                position=position,
                skill_id=skill,
                payload=(status_id, status_kind, rounds, layers, caster, attr, value),
            ))
        if records:
            self.revision += 1
        return tuple(events)

    def _apply_actions(self, data: bytes):
        actions = self._parse_actions(data)
        if actions is None:
            return ()
        events = []
        for source, skill_id, fight_area, targets in actions:
            for position, result, be_hit, attributes in targets:
                changes = self._apply_deltas(position, result, attributes)
                self._apply_effect_status(position, skill_id, result)
                events.append(self._event(
                    "action",
                    source=source,
                    position=position,
                    skill_id=skill_id,
                    payload=(fight_area, result, be_hit, tuple(changes)),
                ))
        if actions:
            self.revision += 1
        return tuple(events)

    def _apply_effect_status(self, position, skill_id: int, result: int):
        status_kind = ACTION_EFFECT_STATUS_KIND.get(skill_id)
        if result not in (1, 2) or status_kind is None or position not in self.units:
            return
        self.statuses.setdefault(position, {})[status_kind] = skill_id

    @staticmethod
    def _parse_actions(data: bytes):
        actions = []
        offset = 0
        while offset < len(data):
            if offset + 8 > len(data):
                return None
            chunk_size = int.from_bytes(data[offset:offset + 2], "little")
            end = offset + 2 + chunk_size
            if chunk_size < 6 or end > len(data):
                return None
            row, col, skill_id, fight_area, count = struct.unpack_from(
                "<BBHBB", data, offset + 2,
            )
            cursor = offset + 8
            targets = []
            for _ in range(count):
                if cursor + 5 > end:
                    return None
                target_row, target_col, result, be_hit, attr_count = struct.unpack_from(
                    "<BBBBB", data, cursor,
                )
                cursor += 5
                attributes = []
                for _ in range(attr_count):
                    if cursor + 6 > end:
                        return None
                    kind, value, sign = struct.unpack_from("<BIB", data, cursor)
                    if sign not in (0, 1):
                        return None
                    attributes.append((kind, value, sign))
                    cursor += 6
                targets.append(((target_row, target_col), result, be_hit, tuple(attributes)))
            if skill_id == 20008 and cursor + 2 == end:
                cursor += 2
            if cursor != end:
                return None
            actions.append(((row, col), skill_id, fight_area, tuple(targets)))
            offset = end
        return actions

    def _apply_deltas(self, position, result: int, attributes):
        changes = []
        if result not in (1, 2):
            return changes
        unit = self.units.get(position)
        if unit is None:
            return changes
        for kind, value, sign in attributes:
            delta = value if sign == 0 else -value
            final = self._apply_delta(unit, kind, delta)
            if final is not None:
                changes.append((kind, delta, final))
        return changes

    @staticmethod
    def _apply_delta(unit: BattleUnit, kind: int, delta: int):
        if kind == 0x19:
            unit.hp = max(0, min(unit.hp_max, unit.hp + delta))
            unit.alive = unit.hp > 0
            unit.state = "active" if unit.alive else "dead"
            return unit.hp
        if kind == 0x1A:
            unit.sp = max(0, min(unit.sp_max, unit.sp + delta))
            return unit.sp
        if kind == 0xCD:
            unit.hp_max = max(0, unit.hp_max + delta)
            unit.hp = min(unit.hp, unit.hp_max)
            return unit.hp_max
        if kind == 0xCE:
            unit.sp_max = max(0, unit.sp_max + delta)
            unit.sp = min(unit.sp, unit.sp_max)
            return unit.sp_max
        return None

    def _apply_absolute(self, data: bytes):
        if len(data) < 1 or (len(data) - 1) % 7:
            return ()
        is_revive = bool(data[0])
        records = []
        for offset in range(1, len(data), 7):
            row, col, kind, value = struct.unpack_from("<BBBi", data, offset)
            if row > 3 or col > 5:
                return ()
            records.append(((row, col), kind, value))
        events = []
        for position, kind, value in records:
            unit = self.units.get(position)
            if unit is None:
                continue
            self._set_absolute(unit, kind, value, is_revive)
            events.append(self._event(
                "attribute",
                position=position,
                payload=(kind, value, is_revive),
            ))
        if records:
            self.revision += 1
        return tuple(events)

    @staticmethod
    def _set_absolute(unit: BattleUnit, kind: int, value: int, is_revive: bool):
        if kind == 0x19:
            unit.hp = max(0, min(unit.hp_max, value))
            unit.alive = unit.hp > 0
            unit.state = "active" if unit.alive else "dead"
        elif kind == 0x1A:
            unit.sp = max(0, min(unit.sp_max, value))
        elif kind == 0xCD:
            unit.hp_max = max(0, value)
            unit.hp = min(unit.hp, unit.hp_max)
        elif kind == 0xCE:
            unit.sp_max = max(0, value)
            unit.sp = min(unit.sp, unit.sp_max)

    def _start_turn(self, data: bytes):
        if data or not self.active:
            return ()
        self.turn += 1
        self.revision += 1
        self.pending_actions = {}
        return (self._event("turn_start"),)

    def _end(self, data: bytes):
        """S:011-000 <結束戰鬥> +玩家ID(8) +NPCIndex(2) -> FightManager.FightOver.

        Server gui goi nay CHO TUNG NGUOI tham chien ("某id玩家結束戰役"), guardIndex==0 la nguoi
        choi. Client goc chi coi la "tran cua MINH xong" khi roleId == Role.playerId -> bot loc y het.

        3 nhanh loai bo o day TRUOC DAY IM LANG HOAN TOAN (return () khong log). Khi no truot thi
        bot chi don gian NGOI IM khong danh, khong bao gi -> loi song rat lau vi khong ai thay.
        Log that bai (co gioi han) de lan sau biet no rot o dieu kien NAO.
        Dac biet: `local_role_id` dang duoc gan = self_entity (client.py). S:011-000 mang
        玩家ID (Int64) - CHUA CHUNG MINH duoc hai thu do la mot; log ra de doi chieu."""
        if len(data) != 10:
            self._log_end_bo("do dai != 10 (%d)" % len(data))
            return ()
        role_id, guard_index = struct.unpack("<8sH", data)
        if guard_index:
            self._log_end_bo("guard_index=%d (khong phai nguoi choi)" % guard_index)
            return ()
        if role_id != self.local_role_id:
            self._log_end_bo("role_id KHAC: goi=%s local_role_id=%s"
                             % (role_id.hex(), bytes(self.local_role_id or b"").hex() or "(rong)"))
            return ()
        if not self.active:
            self._log_end_bo("tracker khong active (da ket tran tu truoc?)")
            return ()
        return self.confirm_end()

    def _log_end_bo(self, ly_do: str):
        """Log 1 lan cho moi (the he tran, ly do) - khong spam nhung khong bo sot ca nao."""
        key = (self.generation, ly_do.split(":")[0])
        if key in self._end_warned:
            return
        if len(self._end_warned) > 200:
            self._end_warned.clear()
        self._end_warned.add(key)
        log.warning("[BATTLE g=%s] BO goi KET TRAN (0x0b sub0): %s", self.generation, ly_do)

    def _exit(self, data: bytes):
        if len(data) != 2:
            return ()
        position = tuple(data)
        if position not in self.units:
            return ()
        self.units.pop(position)
        self.statuses.pop(position, None)
        self.buffs.pop(position, None)
        self.extra_buffs.pop(position, None)
        self.pending_actions.pop(position, None)
        self.revision += 1
        return (self._event("exit", position=position),)

    def _event(self, kind: str, **kwargs):
        return BattleEvent(kind, self.generation, self.turn, **kwargs)
