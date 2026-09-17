"""In-process coordination for accounts fighting in the same party."""

from collections import defaultdict
import logging
import threading

from .battle_tracker import BattleEvent


log = logging.getLogger("bot")
_registry = {}
_registry_lock = threading.Lock()


def _nhan_party(party_idx):
    """So party HIEN TRONG LOG - phai khop GUI (dem tu 1).

    `party_idx` la 0-based (`config.ACCOUNT_PARTY` dung `enumerate(PARTIES)`) nhung GUI dem party
    tu 1, va cac dong log khac trong `run_party_digioi` da in `pidx + 1`. Truoc day file nay in
    thang `party_idx` -> trong CUNG mot file log co hai kieu danh so lech nhau 1, doc log la dinh
    bay ngay: tat party 28 tren GUI ma van thay `[P28 BATTLE ...]` chay (thuc ra la party 29).
    Da dinh that (user 03/09: "p28 tat roi ma sao van co log").

    Client chay SOLO thi `party_idx` la tuple `("solo", id(client))` -> khong cong duoc, giu nguyen.
    """
    if isinstance(party_idx, int) and not isinstance(party_idx, bool):
        return party_idx + 1
    return party_idx


class PartyBattleCoordinator:
    def __init__(self, party_idx):
        self.party_idx = party_idx
        self.nhan_party = _nhan_party(party_idx)   # so hien trong log (khop GUI, dem tu 1)
        self._lock = threading.RLock()
        self.generation = 0
        self.turn = 0
        self.active = False
        self.accounts = set()
        self.local_turns = {}
        self._sent = set()
        self._reservations = {}
        self._events = {}
        self._variants = defaultdict(lambda: defaultdict(set))
        self._warned_conflicts = set()
        self.common_event_count = 0
        self._snapshot = None

    @property
    def active_key(self):
        with self._lock:
            return (self.generation, self.turn) if self.active else None

    def register_accounts(self, account_ids):
        with self._lock:
            self.accounts.update(account_ids)

    def reset_session(self):
        with self._lock:
            self.generation = 0
            self.turn = 0
            self.active = False
            self.accounts = set()
            self.local_turns = {}
            self._sent = set()
            self._reservations = {}
            self._events = {}
            self._variants.clear()
            self._warned_conflicts.clear()
            self.common_event_count = 0
            self._snapshot = None

    def observe(self, account_id, event, snapshot=None):
        if not isinstance(event, BattleEvent):
            return False
        with self._lock:
            self.accounts.add(account_id)
            if event.kind == "start":
                accepted = self._observe_start(account_id, event)
            elif event.kind == "turn_start":
                accepted = self._observe_turn(account_id, event)
            elif event.kind == "end":
                accepted = self._observe_end(account_id, event)
            elif not self.active or (event.generation, event.turn) != self.active_key:
                accepted = False
            else:
                accepted = self._observe_common(account_id, event)
            if accepted and snapshot is not None:
                self._snapshot = snapshot
            return accepted

    def canonical_snapshot(self):
        with self._lock:
            return self._snapshot

    def _observe_start(self, account_id, event):
        # Generation KHAC (ke ca NHO HON) = server da mo phien battle MOI (vd ca party relogin sau
        # khi PB vo -> server danh so lai tu dau). Server la su that: theo no. Truoc day loai
        # generation nho hon -> phien cu ket lai vinh vien, moi turn sau deu bi loai.
        if not self.active or event.generation != self.generation:
            self.generation = event.generation
            self.turn = 0
            self.active = True
            self.local_turns = {}
            self._sent = set()
            self._reservations = {}
            self._events = {}
            self._variants.clear()
            self._warned_conflicts.clear()
            self._snapshot = None
        return self._observe_common(account_id, event)

    def _observe_turn(self, account_id, event):
        if not self.active or event.generation != self.generation:
            # Turn cua mot generation khac -> phien dang giu la RAC (xem _observe_start). Nhan
            # phien moi tai day luon, khong thi ca tran khong ai gui duoc lenh nao.
            self.generation = event.generation
            self.turn = 0
            self.active = True
            self.local_turns = {}
            self._reservations = {}
            self._events = {}
            self._variants.clear()
            self._warned_conflicts.clear()
            self._snapshot = None
        if event.turn < self.turn:
            return False
        if event.turn > self.turn:
            self.turn = event.turn
            self._sent = set()
            self._reservations = {}
        self.local_turns[account_id] = (event.generation, event.turn)
        return self._observe_common(account_id, event)

    def _observe_end(self, account_id, event):
        if not self.active or event.generation != self.generation:
            return False
        accepted = self._observe_common(account_id, event)
        self.active = False
        self._sent = set()
        self._reservations = {}
        return accepted

    def _observe_common(self, account_id, event):
        key = self._semantic_key(event)
        variants = self._variants[key]
        variants[event].add(account_id)
        current = self._events.get(key)
        if current is None:
            self._events[key] = event
            self.common_event_count += 1
            self._log_common(event)
            return True
        if current == event:
            return False
        if key not in self._warned_conflicts:
            self._warned_conflicts.add(key)
            log.warning("[P%s BATTLE] conflicting copies for %s", self.nhan_party, key)
        if len(variants[event]) > len(variants[current]):
            self._events[key] = event
        return False

    @staticmethod
    def _semantic_key(event):
        prefix = (event.generation, event.turn, event.kind)
        if event.kind == "action":
            return prefix + (event.source, event.skill_id, event.position)
        if event.kind == "status":
            status_kind = event.payload[0] if event.payload else None
            return prefix + (event.position, status_kind)
        if event.kind == "ack":
            return prefix + (event.source,)
        if event.kind == "spawn":
            role_id = event.payload[0] if event.payload else None
            return prefix + (event.position, role_id)
        if event.kind in ("exit", "flyout", "move", "transform"):
            return prefix + (event.position, event.source)
        return prefix

    def canonical_event(self, event):
        with self._lock:
            return self._events.get(self._semantic_key(event))

    def _log_common(self, event):
        if event.turn:
            prefix = f"[P{self.nhan_party} BATTLE g={event.generation} t={event.turn}]"
        else:
            prefix = f"[P{self.nhan_party} BATTLE g={event.generation}]"
        if event.kind == "start":
            log.info("%s START", prefix)
        elif event.kind == "turn_start":
            log.info("%s TURN START", prefix)
        elif event.kind == "end":
            log.info("%s END", prefix)
        elif event.kind == "action":
            log.info("%s %s skill=%d -> %s: %s", prefix, self._pos(event.source),
                     event.skill_id, self._pos(event.position), self._format_action(event.payload))
        elif event.kind == "status":
            # DEBUG, khong phai INFO. Do tren party.log 10/09: rieng dong nay chiem 156.112/300.000
            # dong = 52% TOAN BO log; cong `spawn`/`ack`/`exit` nua la ~63%. Deu la trace trong
            # tran, khong ai doc khi may chay binh thuong.
            #
            # Log khong mien phi: moi dong di qua format -> file -> `_log_queue` -> widget Text cua
            # GUI, tren mot tien trinh Python da bao hoa GIL (256 acc, ~880 thread, CPU 1.0s moi
            # giay thuc). Main thread cua Tk phai xep hang voi tat ca -> user thay "Not Responding"
            # (user 10/09: "sao bot chay hay bi no responding the").
            status_kind = event.payload[0] if event.payload else 0
            log.debug("%s STATUS %s kind=%d skill=%d", prefix, self._pos(event.position),
                      status_kind, event.skill_id)
        elif event.kind in ("spawn", "exit", "flyout", "move", "transform", "ack"):
            log.debug("%s %s %s", prefix, event.kind.upper(),
                      self._pos(event.position or event.source))

    @staticmethod
    def _pos(position):
        return "?" if position is None else f"({position[0]},{position[1]})"

    @staticmethod
    def _format_action(payload):
        if len(payload) < 4:
            return str(payload)
        _area, result, _be_hit, changes = payload
        names = {0x19: "HP", 0x1A: "SP", 0xCD: "MAX_HP", 0xCE: "MAX_SP"}
        values = [
            f"{names.get(kind, hex(kind))} {delta:+d} => {final}"
            for kind, delta, final in changes
        ]
        result_name = {0: "MISS", 1: "HIT", 2: "THUNDER", 3: "HEART"}.get(result, str(result))
        return result_name + (("; " + "; ".join(values)) if values else "")

    def open_local_turn(self, account_id, generation: int, turn: int):
        with self._lock:
            if self.active_key != (generation, turn):
                return False
            self.local_turns[account_id] = (generation, turn)
            return True

    def can_plan(self, generation: int, turn: int):
        with self._lock:
            return self.active_key == (generation, turn)

    def can_send(self, account_id, generation: int, turn: int, source=None):
        """CHI chan khi da GUI DUNG (account, source, generation, turn) nay roi.

        TRUOC DAY con chan khi `active_key != (g,t)` hoac chua thay turn cua chinh minh ->
        phien dong bo lech la NUOT SACH lenh danh, im lang (log.debug), tran dung hinh vinh vien
        (bug that 17:09: enemy_hp 488/488/488 suot 4 luot, server hoi lai luot moi 26s, HP minh
        van tut). Chan trung la viec CAN, chan vi "chua dong bo phien" la MAT LENH -> bo.
        Khoa chong trung nay gan lien (generation, turn) nen dung CA KHI phien lech.
        """
        with self._lock:
            if source is None:
                return True
            return (account_id, tuple(source), generation, turn) not in self._sent

    def sent_state(self, account_id, generation: int, turn: int):
        """(phien co khop khong, da thay turn cua minh chua) - de caller LOG canh bao, khong chan."""
        with self._lock:
            return (self.active_key == (generation, turn),
                    self.local_turns.get(account_id) == (generation, turn))

    def mark_sent(self, account_id, source, generation: int, turn: int):
        source = tuple(source)
        with self._lock:
            key = (account_id, source, generation, turn)
            if key in self._sent:
                return False
            self._sent.add(key)
            return True

    def reserve(self, account_id, action_class, target, generation: int, turn: int):
        with self._lock:
            if self.active_key != (generation, turn):
                return False
            if action_class == "damage":
                return True
            key = (action_class, tuple(target))
            owner = self._reservations.get(key)
            if owner is None:
                self._reservations[key] = account_id
                return True
            return owner == account_id


def get_party_battle(party_idx):
    with _registry_lock:
        coordinator = _registry.get(party_idx)
        if coordinator is None:
            coordinator = PartyBattleCoordinator(party_idx)
            _registry[party_idx] = coordinator
        return coordinator


def clear_party_battles():
    with _registry_lock:
        _registry.clear()
