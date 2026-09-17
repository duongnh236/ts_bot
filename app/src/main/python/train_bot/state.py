"""Theo doi trang thai tran dau: HP/SP cua char, pet, dong doi, quai.

Phan tich tu cac packet S2C:
  - 0x0b: full stats 1 entity (HP/SP max+cur)
  - 0x33: cap nhat HP/SP theo luot (block 03 02 = char, 03 01 = pet)
  - 0x0c: thong tin quai luc vao tran
"""
import struct
import unicodedata
import logging
from . import config
log = logging.getLogger("bot")

# Stat type trong 0x33 / 0x0b
T_HP_CUR = 0x19
T_SP_CUR = 0x1A
T_HP_MAX = 0xCD
T_SP_MAX = 0xCE   # maxSP - nam trong S2C 0x08 sub0300 (theo entity), KHONG phai 0x33

# Skill bao ve dang doc duoc tu 0x35 status-list / 0x32 echo.
# Chia nhom de AI tranh buff trung va biet dung skill pha phu hop.
PROTECT_KET_GIOI = frozenset((10010, 10041))
PROTECT_STEALTH = frozenset((13005, 13042))
PROTECT_KINH = frozenset((10015, 10026, 10038, 10039, 10040))
PROTECT_PASSIVE = frozenset((11002,))  # Bang Tuong: co status thi coi la da co bao ve, khong auto-cast
PROTECT_SKILLS = PROTECT_KET_GIOI | PROTECT_STEALTH | PROTECT_KINH | PROTECT_PASSIVE
CLEAR_PROTECT_SKILLS = frozenset((10009, 10014, 11012))
CC_HIGH_SKILLS = frozenset((11014, 11039, 20026, 11027, 11028, 14008, 13007))
CC_LOW_LOCK_SKILLS = frozenset((10004, 10033, 20025, 20049, 13002, 13052, 13015,
                                20027, 20048, 13050, 13046))
CC_CHAOS_SKILLS = frozenset((14021, 14065, 20014, 20051, 20055, 20058))
CC_SKILLS = CC_HIGH_SKILLS | CC_LOW_LOCK_SKILLS | CC_CHAOS_SKILLS


class Unit:
    def __init__(self, name=""):
        self.name = name
        self.hp = 0
        self.hp_max = 0
        self.sp = 0
        self.sp_max = 0

    @property
    def hp_pct(self):
        return self.hp / self.hp_max if self.hp_max else 1.0

    def __repr__(self):
        return f"{self.name}(HP={self.hp}/{self.hp_max} SP={self.sp}/{self.sp_max})"


class BattleState:
    def __init__(self):
        self.tracker = None
        self.battle_coordinator = None
        self.char = Unit("char")
        self.pet = Unit("pet")
        self.self_entity = None   # entity 8 byte cua nhan vat minh (set tu client)
        self.skills_char = []     # LIST skill char (tu 0x05 day du - giu thu tu: skill[0]=boss fallback)
        self.skills_pet  = set()  # skill ID pet co (tu 0x28 login)
        self.my_atype = 3         # atype = vi tri formation cua minh (leader o giua)
        self.label = ""           # nhan account (de tao key dieu phoi heal)
        self.party_idx = None     # index party (dieu phoi hoi sinh chéo account)
        self.battle_config = {}   # custom battle settings rieng acc (accounts.json settings.battle)
        self.pet_skills = []      # LIST skill cua pet dang dung (pets.json, giu thu tu - skill[0]=boss fallback)
        self.active_pet_id = None # id pet dang dung (tu S2C 0x13)
        self.carried_pets = []    # [(pid, ten)] pet mang theo (0x0f) - tab skill per-pet
        self.active_pet_confirmed = False  # True = active_pet_id den tu goi 0x13 THAT (server),
                                          # False = doan tam tu record dau 0x0f (xem client)
        self.pet_cfg_owner = None # pet DAU TIEN thay sau login = chu cua bo rule 'pet' CHUNG cu:
                                  # config cu khong ghi pet id, coi rule do la cua pet user dang
                                  # dung; doi sang pet khac -> auto (yeu cau user, xem combat)
        self.boss_mode = False    # True = dang trong dungeon danh boss -> pet dung skill manh (tu suy)
        # SP DAY (sp==sp_max) luc nao trong tran -> spam combo CA TRAN, bat chap so quai (1 quai cung dung).
        self.char_spam = False
        self.pet_spam = False
        # dong doi trong party (entity_id -> Unit), khong gom char/pet cua minh.
        # maxHP+maxSP (char & pet) nap tu 0x0b full-stat (update_0x0b); cur HP/SP cap nhat tung luot 0x33.
        self.allies = {}
        # maxSP (b1,slot)->val tu 0x0b. BEN: allies bi clear MOI tran (0x34) nhung 0x0b party chi toi
        # luc spawn -> luu rieng de ko mat maxSP giua cac tran. Nguon duy nhat co pet maxSP.
        self.ally_spmax = {}
        self.ally_hpmax = {}          # ben qua allies.clear() moi 0x34; 40NPC dung de doc HP cuoi tu 0x32
        self.mobs = []  # list HP_max cua quai (theo thu tu xuat hien)
        self.in_battle = False
        # vi tri quai con song (slot B2) - decode tu 0x33; dung lam target combat
        self.enemy_slots = []          # vd [2] = co 1 quai o slot 2
        self.enemy_hp = {}             # slot -> curHP
        self.enemy_names = set()       # TEN quai trong tran (tu 0x0b: entity[2:4]=template_id -> npc_names)
        self.enemy_pos_names = {}       # pos(row*10+col) -> set ten quai, neu 0x0b co row/col
        self.enemy_pos_tids = {}        # pos(row*10+col) -> set template_id
        self.mineral_battle = False    # True neu tran co quai khoang (bat theo ten hoac template id)
        self.self_slot = None          # B2 (vi tri tran) cua minh - tu 0x0b battle (entity-based)
        # --- PHE TRONG TRAN ---
        # Moi tran thuong (train / PB / Di Gioi / 40NPC / 2K) deu xep PHE TA o hang 2-3 va DICH o
        # hang 0-1, nen truoc day hai hang nay duoc viet CHET khap noi. LOAN DAU (PvP) thi KHONG:
        # server xep minh vao phe nao cung duoc - capture 25/08 cho thay nguoi that o (0,1) tran 1
        # va (0,0) tran 2. Danh vao hang 0-1 luc dang DUNG o do = danh dong doi -> server tra
        # `S:000-000` ly do 42 `修改戰鬥封包` (goi chien dau bi sua) va DA HAN acc (quit=true).
        #
        # Gia tri MAC DINH duoi day GIU Y HET hanh vi cu -> moi mode khac khong doi mot ly nao.
        # CHI loan dau goi `doi_phe_theo_hang_cua_minh()` de lat lai.
        self.enemy_rows = (0, 1)       # hang cua DICH
        self.ally_rows = (2, 3)        # hang cua PHE TA
        self.char_row = 3              # trong phe ta: hang cua CHAR (hang con lai la pet)
        # QUEST mode: START tran ma >6 quai -> True ca tran (latch). >6 con -> all-target; <=6 -> nhu boss.
        self.quest_mode = False
        # MODE EVENT (40NPC / 2K): LUON quest_mode, khong phu thuoc leader hay so quai.
        # Truoc day chi ep trong nhanh "elif is_leader" cua run_party_digioi -> mode event KHONG
        # CO LEADER thi khong ai ep, phai nho auto-latch (>6 quai). Tran mo ra <=6 quai la chay
        # nguyen TRAIN mode ca tran -> chi dung AoE RE NHAT (Hoa Tien/Nem Da) du quai rat dong.
        self.force_quest_mode = False
        self._battle_counted = False   # latch: da dem so quai luc start tran chua
        # DEM THE HE du lieu quai: tang moi lan CO goi 0x33 THAT cap nhat nhom quai (saw_enemy_group).
        # Goi 0x35 (offer luot) KHONG mang du lieu quai -> neu 0x35 den ma KHONG co 0x33 moi kem theo
        # (vi du: tran da ket that nhung con offer "tan du" den tre) -> enemy_gen KHONG doi -> combat.py
        # se BO QUA (khong danh lai bang du lieu quai CU) thay vi danh mu/danh lai tren trang thai stale.
        self.enemy_gen = 0
        self.last_atk_gen_char = -1
        self.last_atk_gen_pet = -1
        # DI GIOI SOLO: toi da 4 pet ra tran CUNG LUC, moi con 1 atype RIENG (0,1,3,4 - atype 2 la
        # cua CHAR). KHAC han truong hop binh thuong (1 pet, dung state.pet + skills_pet chung voi
        # char). solo_multipet=True -> client.py dung nhanh combat rieng (combat.decide_multipet).
        self.solo_multipet = False
        self.multi_pet = {}          # atype (0,1,3,4) -> Unit (HP/SP tung pet, tu update_0x33)
        self.multi_pet_skills = {}   # atype -> [skill id] (tu pets.json, xem client._on_pet_list)
        self.last_atk_gen_multipet = {}   # atype -> enemy_gen da danh (tranh danh lap khi 0x33 cu)
        # (row,col)->set(skill_id): trang thai bao ve hien co, tu 0x35 status-list.
        # row 0/1=dich, 2=pet minh, 3=char minh.
        self.status_by_kind = {}       # (row,col)->{status_kind: skill_id}, giong client HandleStatus
        self.protect_status = {}
        self.crowd_status = {}          # (row,col)->set(skill_id) CC/khong che tu 0x35 status-list

    def attach_tracker(self, tracker, coordinator=None):
        self.tracker = tracker
        self.battle_coordinator = coordinator

    def sync_from_tracker(self):
        tracker = self.tracker
        if tracker is None:
            return
        self.in_battle = tracker.active
        self.enemy_gen = tracker.revision
        # CHI dung tracker lam nguon quai khi tracker THUC SU CO ban ghi quai.
        #
        # BUG THAT (party 1, "vao tran ma bot khong danh"): roster quai den trong goi S:011-005;
        # neu ban ghi nao lam lech con tro thi _parse_roles BO SACH ca danh sach (im lang) ->
        # tracker.units khong co con quai nao. Ham nay truoc day GHI DE VO DIEU KIEN -> xoa luon
        # enemy_hp/enemy_slots ma 0x33 vua doc DUNG (0x33 co ca 2 hang: pos = b1*10 + b2) ->
        # bot con 0 muc tieu -> dung im du in_battle=True.
        # Tracker CO quai (ke ca da chet het, hp=0) thi van la nguon dung -> ghi de binh thuong.
        _quai_tracker = {
            row * 10 + col: unit.hp
            for (row, col), unit in tracker.units.items()
            if row in self.enemy_rows
        }
        if _quai_tracker:
            self.enemy_hp = _quai_tracker
            self.enemy_slots = sorted(
                position for position, hp in self.enemy_hp.items() if hp > 0
            )
            # PHAI cham latch O DAY NUA. Truoc day latch CHI nam trong update_0x33; sau khi sua
            # loi roster (bot coi QUAI MapNpc=3 la nguoi choi) thi TRACKER tro thanh nguon quai
            # chinh -> 10 quai vao bang duong nay ma KHONG AI XET -> tran 10 quai van khong vao
            # quest mode (user bao dung: "tran 1 quai danh luon roi ma tran 10 quai van khong vao").
            self.latch_quest_mode()
        if self.self_entity:
            for (row, col), tracked in tracker.units.items():
                if row == self.char_row and tracked.role_id == self.self_entity:
                    self.self_slot = col
                    self.my_atype = col
                    break
        allies = {}
        for position, tracked in tracker.units.items():
            row, col = position
            if row not in self.ally_rows:
                continue
            unit = Unit(tracked.role_id.hex())
            unit.hp = tracked.hp
            unit.hp_max = tracked.hp_max
            unit.sp = tracked.sp
            unit.sp_max = tracked.sp_max
            unit.slot = col
            allies[position] = unit
            self.ally_hpmax[position] = tracked.hp_max
            self.ally_spmax[position] = tracked.sp_max
            if col == self.self_slot:
                if row == 3:
                    self.char = unit
                else:
                    self.pet = unit
        self.allies = allies
        self.status_by_kind = {
            position: dict(by_kind)
            for position, by_kind in tracker.statuses.items()
        }
        self._refresh_tracked_status()

    def latch_quest_mode(self):
        """RULE (user, tuyet doi): tran co >6 quai thi PHAI vao quest mode.

        Xet MOI lan du lieu quai doi, chi BAT len True khong bao gio ha (giet bot quai con <=6 van
        giu quest ca tran). Goi tu CA HAI nguon quai: update_0x33 va sync_from_tracker - de mot
        nguon nao do tro thanh nguon chinh thi luat van chay."""
        if self.force_quest_mode or len(self.enemy_slots) > 6:
            self.quest_mode = True

    PHE_MAC_DINH = ((0, 1), (2, 3), 3)     # (hang dich, hang phe ta, hang char) - tran thuong

    def dat_phe_mac_dinh(self):
        """Tra ve cach chia phe CUA TRAN THUONG. Goi moi khi bat dau tran de mot tran loan dau
        khong lam lech tran ke tiep."""
        self.enemy_rows, self.ally_rows, self.char_row = self.PHE_MAC_DINH

    def doi_phe_theo_hang_cua_minh(self, hang_cua_minh):
        """LOAN DAU: suy phe tu chinh o cua minh trong goi tao tran.

        Bo cuc dau truong (capture 25/08): char (kind=2) o hang 0 va 3, pet (kind=4) o hang 1 va 2
        -> hai phe la {0,1} va {3,2}. Minh o hang nao thi phe do la TA.
        Tra ve True neu co doi so voi hien tai.
        """
        if hang_cua_minh in (0, 1):
            moi = ((2, 3), (0, 1), 0)
        elif hang_cua_minh in (2, 3):
            moi = ((0, 1), (2, 3), 3)
        else:
            return False
        cu = (self.enemy_rows, self.ally_rows, self.char_row)
        self.enemy_rows, self.ally_rows, self.char_row = moi
        return cu != moi

    def reset_battle(self):
        self.mobs = []
        self.in_battle = False
        self.status_by_kind = {}
        self.protect_status = {}
        self.crowd_status = {}

    def reset_enemies(self, reset_quest=True, reset_protect=True):
        """Xoa HP/slot quai (goi luc battle moi bat dau, tranh dinh quai tran cu).
        reset_quest=False: GIU quest_mode/_battle_counted. 0x34 ban MOI TURN -> neu reset moi turn thi
        khi quai con <=6 se MAT latch quest_mode (set luc >6 dau tran) -> roi nham ve TRAIN mode.
        reset_protect=False: GIU status bao ve qua 0x34; buff/status that se sync bang 0x35 status-list.
        Chi reset khi la ENCOUNTER MOI (gap thoi gian lon giua 2 turn -> client.py quyet dinh)."""
        self.enemy_hp = {}
        self.enemy_slots = []
        if reset_protect:
            self.status_by_kind = {}
            self.protect_status = {}
            self.crowd_status = {}
        if reset_quest:
            self.enemy_names = set()
            self.enemy_pos_names = {}
            self.enemy_pos_tids = {}
            self.mineral_battle = False
            self.quest_mode = bool(self.force_quest_mode)   # event: khong bao gio ha xuong False
            self._battle_counted = False

    @staticmethod
    def _is_mineral_enemy(tid=None, name=None):
        # CHUAN: template_id thuoc set NPC kind==16 (config.MINERAL_NPC_IDS, crack_mineral_npcs.py) -
        # khop dung client CheckMineral, KHONG phu thuoc ten (252 con: Thuy Tinh/Quang/Long Thu/Khoang
        # dao chu... ma heuristic ten cu sot gan het). Fallback ten "Khoang " khi chua co set/tid.
        if tid is not None and tid in getattr(config, "MINERAL_NPC_IDS", ()):
            return True
        if name:
            norm = unicodedata.normalize("NFKD", str(name)).encode("ascii", "ignore").decode("ascii").lower()
            return norm.startswith("khoang ")
        return False

    def _remember_enemy_entity(self, row, col, tid, name):
        """Ghi nho ten/id quai theo vi tri battle neu packet co du row/col."""
        if row not in self.enemy_rows or col > 5:
            return
        pos = row * 10 + col
        if name:
            self.enemy_pos_names[pos] = {name}
            self.enemy_names.add(name)
        else:
            self.enemy_pos_names.pop(pos, None)
        if tid is not None:
            self.enemy_pos_tids[pos] = {tid}
        else:
            self.enemy_pos_tids.pop(pos, None)

    # ---- parse 0x33 (stat update theo luot) ----
    def update_0x33(self, pkt: bytes):
        """Block 7 byte: [00][B1][B2][type][val 2B LE][00].
          B1: 3=nhan vat, 2=pet, 0=QUAI(dich). B2: slot/vi tri (1..n).
          type: 0x19=curHP, 0x1a=SP, 0xcd=maxHP.
        -> lay danh sach quai (B1=0, curHP>0) de target; cap nhat HP/SP char/pet cua minh.
        """
        body = pkt[7:] if len(pkt) > 7 and pkt[6] == 0x33 else pkt
        # bo 2 byte prefix (01 00)
        p = body[2:]
        groups = {}  # (B1,B2) -> {type: val}
        i = 0
        while i + 7 <= len(p):
            a, b1, b2, tt = p[i], p[i + 1], p[i + 2], p[i + 3]
            # b1: 0=quai hang truoc, 1=quai hang sau, 2=pet, 3=nhan vat
            if a == 0x00 and b1 in (0x00, 0x01, 0x02, 0x03) and tt in (T_HP_CUR, T_SP_CUR, T_HP_MAX):
                val = int.from_bytes(p[i + 4:i + 6], "little")
                groups.setdefault((b1, b2), {})[tt] = val
                i += 7
            else:
                i += 1
        # QUAI = b1 in (0,1): hang truoc b1=0, hang sau b1=1; cot = b2.
        # Vi tri noi bo = b1*10 + b2 -> hang=pos//10, cot=pos%10 (gui combat: b=hang, target=cot)
        saw_enemy_group = False
        start_enemy_slots = None
        for (b1, b2), d in groups.items():
            if b1 in self.enemy_rows:
                saw_enemy_group = True
                pos = b1 * 10 + b2
                self.enemy_hp[pos] = d.get(T_HP_CUR, 0)
        if saw_enemy_group:
            self.enemy_gen += 1   # co du lieu quai MOI that su -> danh gia lai duoc phep danh lai
            # enemy_slots = TAT CA slot con song theo enemy_hp TICH LUY (khong chi goi nay).
            # Tranh mat con khong bi danh trong turn (vd giet 1-2-3 con con o slot 7 van song).
            self.enemy_slots = sorted(s for s, hp in self.enemy_hp.items() if hp > 0)
            # LATCH quest_mode: TRAN CO >6 QUAI -> QUEST ca tran (yeu cau user, tuyet doi).
            #
            # BUG THAT (party 1, map 12930 thap 2K, 66 tran khong lan nao vao quest mode): truoc day
            # chi cham DUNG MOT LAN o `not self._battle_counted` = lan DAU thay quai. Nhung quai
            # xep theo HANG, moi hang toi da 5 con -> goi 0x33 dau tien thuong chi mang 1 hang
            # (<=5) -> cham False roi KHOA LUON, 5 con hang sau den cung khong xet lai.
            # Ket qua: dung nhung tran 10 quai (2 hang) - tuc dung luc rule >6 CAN chay nhat - thi
            # no khong bao gio chay. Bang chung: char co CC 11014 Bang Phong ma 0 lan dung.
            #
            # Nay xet MOI lan du lieu quai doi. Chi BAT len True, khong bao gio ha -> giet bot quai
            # con <=6 van giu quest ca tran (dung y dinh cu cua latch).
            if self.enemy_slots:
                # _battle_counted / start_enemy_slots van la "LAN DAU thay quai" - thong ke block
                # train (_record_train_block_stats) can dung nghia do, KHONG duoc gop vao latch.
                if not self._battle_counted:
                    self._battle_counted = True
                    start_enemy_slots = tuple(self.enemy_slots)
                self.latch_quest_mode()
        # DI GIOI SOLO: 4 pet CUNG LUC, moi con atype RIENG (b1=2, b2=atype - xac nhan qua capture
        # thuc te: b2 trong 0x33 CHINH LA atype dung de gui 0x32, KHONG can quy doi them). Cap nhat
        # RIENG cho tung atype, KHONG dua vao self_slot (self_slot chi ung voi pet DUY NHAT truong
        # hop binh thuong, sai hoan toan khi co 4 pet).
        if self.solo_multipet:
            for (b1, b2), d in groups.items():
                if b1 == next((r for r in self.ally_rows if r != self.char_row), 0x02):
                    u = self.multi_pet.get(b2)
                    if u is None:
                        u = Unit(f"pet_at{b2}")
                        self.multi_pet[b2] = u
                    if T_HP_MAX in d: u.hp_max = d[T_HP_MAX]
                    if T_HP_CUR in d: u.hp = d[T_HP_CUR]
                    if T_SP_CUR in d: u.sp = d[T_SP_CUR]
        # self_slot xac dinh tu 0x0b battle (entity-based, o client) hoac roster. KHONG dua HP.
        # Doc HP/SP char+pet cua minh theo slot (uu tien roster -> chinh xac, KHONG can 0x0b)
        if self.self_slot is not None:
            _hang_pet = next((r for r in self.ally_rows if r != self.char_row), 0x02)
            pd = groups.get((_hang_pet, self.self_slot))
            if pd:
                if T_HP_MAX in pd: self.pet.hp_max = pd[T_HP_MAX]
                if T_HP_CUR in pd: self.pet.hp = pd[T_HP_CUR]
                if T_SP_CUR in pd: self.pet.sp = pd[T_SP_CUR]
            cd = groups.get((self.char_row, self.self_slot))
            if cd:
                if T_HP_MAX in cd: self.char.hp_max = cd[T_HP_MAX]
                if T_HP_CUR in cd: self.char.hp = cd[T_HP_CUR]
                if T_SP_CUR in cd: self.char.sp = cd[T_SP_CUR]
        # Cap nhat HP + SP TAT CA dong doi (char B1=3, pet B1=2 cua moi slot) -> de quyet dinh hoi mau/SP.
        # SP: 0x33 co T_SP_CUR cua MOI member (server gui ca party). sp_max = MAX SP da thay (dau tran
        # full SP -> bat dung max that), vi 0x33 khong mang maxSP (chi 0x0b cua RIENG minh co).
        for (b1, b2), d in groups.items():
            if b1 in (0x02, 0x03) and (T_HP_CUR in d or T_HP_MAX in d or T_SP_CUR in d):
                u = self.allies.get((b1, b2))
                if u is None:
                    u = Unit(f"{'char' if b1==3 else 'pet'}{b2}")
                    self.allies[(b1, b2)] = u
                if T_HP_MAX in d:
                    u.hp_max = d[T_HP_MAX]
                    self.ally_hpmax[(b1, b2)] = u.hp_max
                elif not u.hp_max:
                    u.hp_max = self.ally_hpmax.get((b1, b2), 0)
                if T_HP_CUR in d: u.hp = d[T_HP_CUR]
                if T_SP_CUR in d: u.sp = d[T_SP_CUR]   # maxSP nap tu 0x0b (update_0x0b)
                u.slot = b2
        return start_enemy_slots

    def update_0x32(self, pkt: bytes):
        """Apply current-HP blocks embedded in battle action packets.

        Stable block observed in 40NPC capture:
        ``[b1][slot] 01 00 01 19 [curHP u32] 01``.
        The normal 0x33 snapshot may only arrive at battle start, so these action
        blocks are needed to know whether the party was wiped at the end.
        """
        body = pkt[7:] if len(pkt) > 7 and pkt[6] == 0x32 else pkt
        self._update_protect_from_0x32(body)
        marker = b"\x01\x00\x01\x19"
        for i in range(max(0, len(body) - 10)):
            b1, slot = body[i], body[i + 1]
            if b1 not in (0x02, 0x03) or body[i + 2:i + 6] != marker:
                continue
            if i + 11 > len(body) or body[i + 10] != 0x01:
                continue
            unit = self.allies.get((b1, slot))
            if unit is None:
                hp_max = self.ally_hpmax.get((b1, slot), 0)
                if not hp_max:
                    continue
                unit = Unit(f"{'char' if b1 == 3 else 'pet'}{slot}")
                unit.hp_max = hp_max
                self.allies[(b1, slot)] = unit
            unit.hp = int.from_bytes(body[i + 6:i + 10], "little")

    def _set_protect(self, b1, b2, skill_id):
        if b1 not in (0, 1, 2, 3) or b2 > 5 or skill_id not in PROTECT_SKILLS:
            return
        self.protect_status.setdefault((b1, b2), set()).add(skill_id)

    def _set_crowd(self, b1, b2, skill_id):
        if b1 not in self.enemy_rows or b2 > 5 or skill_id not in CC_SKILLS:
            return
        self.crowd_status.setdefault((b1, b2), set()).add(skill_id)

    def _clear_protect(self, b1, b2):
        if b1 not in (0, 1, 2, 3) or b2 > 5:
            return
        self.protect_status.pop((b1, b2), None)

    def has_protection(self, b1, b2):
        return bool(self.protect_status.get((b1, b2)))

    def protection_skills(self, b1, b2):
        return set(self.protect_status.get((b1, b2), ()))

    def crowd_skills(self, b1, b2):
        return set(self.crowd_status.get((b1, b2), ()))

    def _refresh_tracked_status(self):
        protect = {}
        crowd = {}
        for target, by_kind in self.status_by_kind.items():
            for skill_id in by_kind.values():
                if skill_id in PROTECT_SKILLS:
                    protect.setdefault(target, set()).add(skill_id)
                if skill_id in CC_SKILLS:
                    crowd.setdefault(target, set()).add(skill_id)
        self.protect_status = protect
        self.crowd_status = crowd

    def update_0x35_status(self, pkt: bytes):
        """Apply S2C 0x35/01 records exactly like client ``RevRestoreStatus``.

        Each record updates one ``status_kind`` on one battle target.  ``skill_id=0``
        clears only that kind; packets are incremental and must not replace other targets.
        """
        body = pkt[7:] if len(pkt) > 7 and pkt[6] == 0x35 else pkt
        if len(body) < 2 or body[:2] != b"\x01\x00":
            return False
        if len(body) == 2:
            return True
        if len(body) < 7 or (len(body) - 2) % 5 != 0:
            return False
        i = 2
        while i + 5 <= len(body):
            b1, b2, kind = body[i], body[i + 1], body[i + 2]
            skill_id = body[i + 3] | (body[i + 4] << 8)
            if b1 not in (0, 1, 2, 3) or b2 > 5:
                return False
            target = (b1, b2)
            by_kind = self.status_by_kind.setdefault(target, {})
            if skill_id:
                by_kind[kind] = skill_id
            else:
                by_kind.pop(kind, None)
                if not by_kind:
                    self.status_by_kind.pop(target, None)
            i += 5
        self._refresh_tracked_status()
        return True

    def _update_protect_from_0x32(self, body: bytes):
        """Mark/clear protect cache from S2C 0x32 RevAttackSkill echo."""
        if len(body) < 10:
            return
        p = 2 if body[:2] == b"\x01\x00" else 0
        while p + 8 <= len(body):
            chunk_len = body[p] | (body[p + 1] << 8)
            if chunk_len < 8:
                break
            if p + chunk_len <= len(body):
                cend = p + chunk_len
                # Ket Gioi capture co 1 byte tail sau chunk; cho phep doc tail de lay tron attr.
                if len(body) - cend <= 2:
                    cend = len(body)
            elif p + 2 + chunk_len <= len(body):
                cend = p + 2 + chunk_len
            else:
                cend = len(body)
            cstart = p + 2
            skill_id = body[cstart + 2] | (body[cstart + 3] << 8)
            target_count = body[cstart + 5]
            q = cstart + 6
            for _ in range(target_count):
                if q + 5 > cend:
                    break
                tb1, tb2, result = body[q], body[q + 1], body[q + 2]
                attr_count = body[q + 4]
                q += 5
                if result == 0:
                    q = min(cend, q + attr_count * 5)
                    continue
                if skill_id in PROTECT_SKILLS:
                    # Kinh/Ket Gioi da thay result success; An Than chua co capture, nen result
                    # thanh cong la du de mark tam cho toi khi 0x35 status-list sync lai.
                    self._set_protect(tb1, tb2, skill_id)
                elif skill_id in CC_SKILLS:
                    # CC co ti le hut/miss va co the het som; 0x35 status-list dau turn la nguon chuan.
                    # Khong persist CC tu echo 0x32. combat.py giu target-claim theo enemy_gen den
                    # snapshot quai cua turn ke tiep de nhieu acc khong cast trung cung turn.
                    pass
                elif skill_id in CLEAR_PROTECT_SKILLS:
                    self._clear_protect(tb1, tb2)
                q = min(cend, q + attr_count * 5)
            p = cend

    # ---- parse 0x0b (full stats char/pet) ----
    def _read_0b_block(self, pkt, ent, b1, slot, who):
        """Block 0x0b: [entity 8B][10 byte][marker b1,slot][maxHP u32][maxSP u32][curHP][curSP].
        Anchor = ENTITY (duy nhat) -> marker o ent_idx+18. Tranh quet marker [b1][slot] truc tiep
        (slot=0 -> '03 00' qua pho bien -> khop nham -> maxSP rac nhu 244736)."""
        if not ent:
            return
        i = pkt.find(ent)
        while i != -1:
            m = i + 18   # marker o sau entity 8B + 10 byte
            if m + 18 <= len(pkt) and pkt[m] == b1 and pkt[m + 1] == slot:
                mh = struct.unpack_from("<I", pkt, m + 2)[0]
                ms = struct.unpack_from("<I", pkt, m + 6)[0]
                ch = struct.unpack_from("<I", pkt, m + 10)[0]
                cs = struct.unpack_from("<I", pkt, m + 14)[0]
                if 0 < mh < 1_000_000 and 0 < ms < 1_000_000 and ch <= mh and cs <= ms + 1:
                    who.hp_max, who.sp_max, who.hp, who.sp = mh, ms, ch, cs
                    return
            i = pkt.find(ent, i + 1)

    def update_0x0b(self, pkt: bytes):
        """MAX SP char+pet (0x33 chi co maxHP). CHAR anchor self_entity; PET anchor pet-entity
        (= active_pet_id 2B + 6 byte 00, vd 0xa05a -> 5a a0 00..). Marker o entity+18.
        (xac nhan spmax.pcap: char 454/208, pet 1194/339.)"""
        # 0x0b full-stat ca party: quet TOAN BO block char/pet (b1=2 pet, 3 char) cua MOI thanh vien
        # -> nap maxSP+maxHP dong doi theo (b1,slot). DAY la nguon DUY NHAT co pet maxSP (0x33/0x08
        # ko co). Validate 4 field (cur<=max...) tranh khop nham. Phu CA nguoi choi tay.
        # PHAI chay TRUOC guard self_slot (quet party ko can self_slot; goi party lon toi luc spawn
        # khi self_slot co the chua co -> neu return som se mat maxSP).
        if len(pkt) > 100:
            j = 0
            while j + 18 <= len(pkt):
                bb, sl = pkt[j], pkt[j + 1]
                if bb in self.ally_rows and sl < 6:
                    mh, ms, ch, cs = struct.unpack_from("<IIII", pkt, j + 2)
                    if 50 < mh < 1_000_000 and 0 < ms < 100_000 and ch <= mh and cs <= ms + 1 and mh > ms:
                        u = self.allies.get((bb, sl))
                        if u is None:
                            u = Unit(f"{'char' if bb == 3 else 'pet'}{sl}")
                            self.allies[(bb, sl)] = u
                        u.hp, u.hp_max, u.sp, u.sp_max = ch, mh, cs, ms
                        u.slot = sl
                        self.ally_hpmax[(bb, sl)] = mh
                        self.ally_spmax[(bb, sl)] = ms   # BEN qua cac tran (allies bi clear)
                        # Di Gioi SOLO co toi da 4 pet cung luc. 0x33 khong co SP_max, nen nap
                        # SP_max/HP tu 0x0b vao multi_pet de hoi item ngoai tran cho tung con.
                        if self.solo_multipet and bb == 2 and sl in (0, 1, 3, 4):
                            mp = self.multi_pet.get(sl)
                            if mp is None:
                                mp = Unit(f"pet_at{sl}")
                                self.multi_pet[sl] = mp
                            mp.hp, mp.hp_max, mp.sp, mp.sp_max = ch, mh, cs, ms
                        j += 18
                        continue
                j += 1
        slot = self.self_slot
        if slot is None:
            return
        self._read_0b_block(pkt, self.self_entity, 3, slot, self.char)   # CHAR
        if self.active_pet_id:                                            # PET
            pe = self.active_pet_id.to_bytes(2, "little") + b"\x00" * 6
            self._read_0b_block(pkt, pe, 2, slot, self.pet)

    def note_enemy_entities(self, pkt: bytes, npc_names: dict):
        """Bat TEN QUAI trong tran tu goi 0x0b (full-stat entity). Entity quai = 8 byte co dang
        [2B ngau nhien][template_id 2B LE][3 byte base phien]. entity[2:4]=template_id -> tra
        npc_names ra ten (vd 'Khoang Bac', 'Vuong Binh'). Luat tach dich: window 8 byte co 3 byte
        cuoi TRUNG base phien cua minh (self_entity[-3:]) VA entity[2:4] resolve trong npc_names
        -> la 1 con quai. Nguoi choi/pet [2:4] random khong resolve -> tu loai.
        (Xac nhan ts_capture: entity ...9d8c8d0300 -> tid 0x9d0e='Tieu Thai Giam', 0x9d11='Doan
        Khue', 0x9d14='Ho Chan', 0x9d15='Ly Mong', 0x9d16='Phan Phuong'.)"""
        if not npc_names:
            npc_names = {}
        body = bytes(pkt[7:]) if len(pkt) > 7 else bytes(pkt)
        # DANG 2 (xac nhan capture khoang/PB110 mumu12): block enemy
        # `05 00 [n] 07 [tid 2B] ... [row][col] [hp/sp...]`; byte3=07=DICH (04=dong doi).
        # PB110: Tran Cung 0x9d3f co row=0 col=4 tai body[22:24].
        if len(body) >= 6 and body[0:2] == b"\x05\x00" and body[3] == 0x07:
            tid = body[4] | (body[5] << 8)
            name = npc_names.get(tid)
            if name:
                self.enemy_names.add(name)
            if len(body) >= 24:
                self._remember_enemy_entity(body[22], body[23], tid, name)
            if self._is_mineral_enemy(tid, name):
                self.mineral_battle = True
        # DANG 1 (xac nhan ts_capture): entity 8B `[2B ngau nhien][tid 2B][1 byte type][3B base]`.
        # base phien = self_entity[5:8]; quet cac window co base o cuoi + tid resolve.
        if self.self_entity and len(self.self_entity) >= 8:
            base = bytes(self.self_entity[5:8])
            i = 0
            n = len(body)
            while i < n:
                j = body.find(base, i)
                if j < 0:
                    break
                if j >= 5:
                    tid = body[j - 3] | (body[j - 2] << 8)
                    name = npc_names.get(tid)
                    if name:
                        self.enemy_names.add(name)
                    if self._is_mineral_enemy(tid, name):
                        self.mineral_battle = True
                i = j + 1

    def lowest_hp_ally(self):
        """Unit (char/pet bat ky thanh vien) thap mau nhat - CHI con SONG (hp>0). None neu khong co.
        Con HP=0 da CHET -> bo qua (hoi mau vo dung)."""
        alive = [u for u in self.allies.values() if u.hp_max > 0 and u.hp > 0]
        if not alive:
            return None
        return min(alive, key=lambda u: u.hp_pct)

    def dead_allies(self):
        """List (b1, b2, hp_max) dong doi DA CHET (hp_max>0, hp<=0). b1=3 char/2 pet, b2=slot.
        Dung cho HOI SINH (target con chet)."""
        return [(b1, b2, u.hp_max) for (b1, b2), u in self.allies.items()
                if u.hp_max > 0 and u.hp <= 0]

    def any_ally_low(self, threshold: float):
        """Co thanh vien nao (char/pet) HP% <= threshold + CON SONG (hp>0) khong (gom ca minh).
        Con HP=0 da CHET -> KHONG tinh (hoi mau vo dung)."""
        for u in self.allies.values():
            if u.hp_max > 0 and u.hp > 0 and u.hp_pct <= threshold:
                return True
        return False

    def ally_low_sp(self, threshold: float, exclude):
        """Slot (b2) cua dong doi CON SONG co SP% < threshold, TRU caster (exclude=(b1,slot)).
        Tra slot de target skill hoi SP team; None neu khong co. Nhieu dua -> tra dua dau gap."""
        for (b1, b2), u in self.allies.items():
            if (b1, b2) == exclude:
                continue
            if u.hp <= 0:   # CHET roi -> bo qua
                continue
            smax = self.ally_spmax.get((b1, b2), u.sp_max)   # maxSP ben (allies bi clear moi tran)
            if smax > 0 and (u.sp / smax) < threshold:
                return b2
        return None
