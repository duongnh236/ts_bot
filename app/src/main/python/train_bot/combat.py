"""Logic AI combat - quyet dinh skill + target moi luot.

atype=1 (gia tri server chap nhan cho bot; atype 2/3 bi da khoi tran).
Quy tac skill:
  CHAR (unit 3): SP>=100 -> Hoa Tien (AOE 3 hang ngang); ally HP<=60% -> Toan Tri Lieu; con lai -> danh thuong
  PET  (unit 2): SP>=15 -> Hoa Tien; con lai -> danh thuong

RULE TARGET (dung CHUNG cho danh thuong + combo -> moi unit dong target, combo moi an):
  1. Block 3 quai lien nhau cung hang (dau tien) -> con GIUA (AoE trung ca 3)
  2. Khong co -> block 2 quai (dau tien) -> con DAU
  3. Khong co -> con LE dau tien
(KHONG dung focus lowest-HP nua vi moi unit ra target khac nhau -> vo combo.)
"""
import threading, time, logging, unicodedata
from . import config

log = logging.getLogger("bot")

# --- Dieu phoi HEAL: ca party chi 1 unit heal/luot, chon con SP CAO NHAT ---
# bot_standalone chay moi nick trong cung 1 process (thread) -> chia se bien nay.
# Co che: unit muon heal -> DANG KY (key, SP) -> cho HEAL_BARRIER giay cho cac con khac
# dang ky -> chi con SP CAO NHAT moi heal, con lai danh quai. Tranh thua heal thieu dame.
_heal_lock = threading.Lock()
_heal_pool = {}          # key -> (sp, ts) ung vien heal
_heal_done = {"t": 0.0}  # thoi diem heal gan nhat (ca party)
HEAL_BARRIER = 0.4       # giay cho cac unit khac dang ky truoc khi chon
HEAL_COOLDOWN = 2.5      # giay: trong cua so nay chi 1 unit heal


def _heal_decide(key, sp):
    """Dang ky ung vien heal, cho barrier, tra ve True neu minh la SP cao nhat + gianh quyen."""
    now = time.time()
    with _heal_lock:
        if now - _heal_done["t"] < HEAL_COOLDOWN:
            return False                 # da co nguoi heal turn nay
        _heal_pool[key] = (sp, now)
    time.sleep(HEAL_BARRIER)             # cho cac unit khac trong party dang ky
    with _heal_lock:
        if time.time() - _heal_done["t"] < HEAL_COOLDOWN:
            return False                 # ai do da heal trong luc cho
        recent = {k: v for k, v in _heal_pool.items() if now - v[1] <= HEAL_BARRIER + 1.0}
        # con SP cao nhat (tie-break: key) -> winner
        winner = max(recent, key=lambda k: (recent[k][0], k))
        if winner == key:
            _heal_done["t"] = time.time()
            return True
        return False


# --- HOI SINH: dieu phoi theo target chet.
# Cu: 1 con hoi sinh/luot -> neu 2 dong doi chet va 2 caster con song thi caster thu 2 bi mat luot.
# Moi: moi target chet chi 1 caster, nhung nhieu target chet thi nhieu caster co the cast cung luot.
_revive_lock = threading.Lock()
_revive_pool = {}            # key -> (sp, ts, party_idx, dead_targets)
_revive_claims = {}          # party_idx -> {(b1,b2): (owner_key, ts)}
_revive_reg = {}             # (party_idx, b1, slot) -> True: o vi tri do co skill hoi sinh
_support_reg = {}            # (party_idx, b1, slot) -> revive/protect/hp_heal/sp_restore roles
REVIVE_BARRIER = 0.4
REVIVE_COOLDOWN = 2.5


def register_revive(party_idx, b1, slot):
    """Dang ky: party_idx, b1(3=char/2=pet), slot CO skill hoi sinh -> de chon target con chet
    co revive skill TRUOC (uu tien hoi sinh nguoi biet hoi sinh, ho lai cuu nguoi khac)."""
    _revive_reg[(party_idx, b1, slot)] = True


def _slot_has_revive(party_idx, b1, slot):
    if _revive_reg.get((party_idx, b1, slot), False):
        return True
    return bool((_support_reg.get((party_idx, b1, slot)) or {}).get("revive"))


def _slot_has_protect_skill(party_idx, b1, slot):
    return bool((_support_reg.get((party_idx, b1, slot)) or {}).get("protect"))


def _slot_has_hp_heal(party_idx, b1, slot):
    return bool((_support_reg.get((party_idx, b1, slot)) or {}).get("hp_heal"))


def _slot_has_sp_restore(party_idx, b1, slot):
    return bool((_support_reg.get((party_idx, b1, slot)) or {}).get("sp_restore"))


def _dead_target_sort_key(state, party_idx, target):
    b1, b2, hp_max = target
    if hasattr(state, "has_protection"):
        has_protect_status = state.has_protection(b1, b2)
    else:
        has_protect_status = bool(getattr(state, "protect_status", {}).get((b1, b2)))
    return (
        not _slot_has_revive(party_idx, b1, b2),
        not has_protect_status,
        not _slot_has_protect_skill(party_idx, b1, b2),
        not _slot_has_hp_heal(party_idx, b1, b2),
        not _slot_has_sp_restore(party_idx, b1, b2),
        -hp_max,
        b1,
        b2,
    )


def _revive_cleanup(now):
    for k, (_sp, ts, _pidx, _dead) in list(_revive_pool.items()):
        if now - ts > REVIVE_BARRIER + 2.0:
            _revive_pool.pop(k, None)
    for pidx, claims in list(_revive_claims.items()):
        for target, (_owner, ts) in list(claims.items()):
            if now - ts > REVIVE_COOLDOWN:
                claims.pop(target, None)
        if not claims:
            _revive_claims.pop(pidx, None)


def _revive_decide(key, sp, party_idx, dead_targets):
    """Tra target (b1,b2,hp_max) ma caster nay duoc hoi sinh, hoac None.
    Dieu phoi bang barrier ngan de nhieu acc cung turn co the chia target chet cho nhau."""
    group = party_idx if party_idx is not None else key.rsplit(":", 1)[0]
    now = time.time()
    with _revive_lock:
        _revive_cleanup(now)
        _revive_pool[key] = (sp, now, group, tuple(dead_targets))
    time.sleep(REVIVE_BARRIER)
    with _revive_lock:
        now = time.time()
        _revive_cleanup(now)
        claims = _revive_claims.setdefault(group, {})

        # Neu thread khac da chia target cho minh trong luc minh dang doi barrier, nhan lai target do.
        for target in dead_targets:
            target_key = (target[0], target[1])
            owner = claims.get(target_key)
            if owner and owner[0] == key:
                return target

        recent = {
            k: v for k, v in _revive_pool.items()
            if v[2] == group and now - v[1] <= REVIVE_BARRIER + 1.0
        }
        claimed_owners = {owner for owner, _ts in claims.values()}
        available = [t for t in dead_targets if (t[0], t[1]) not in claims]
        available_by_key = {(t[0], t[1]): t for t in available}
        assigned = {}
        for cand in sorted(recent, key=lambda k: (recent[k][0], k), reverse=True):
            if cand in claimed_owners:
                continue
            cand_dead = recent[cand][3]
            target = next(
                (available_by_key.get((t[0], t[1])) for t in cand_dead
                 if (t[0], t[1]) in available_by_key),
                None,
            )
            if target is None:
                continue
            target_key = (target[0], target[1])
            claims[target_key] = (cand, now)
            assigned[cand] = target
            available_by_key.pop(target_key, None)
            if not available_by_key:
                break
        return assigned.get(key)


# --- BUFF BAO VE / PHA BAO VE: claim ngan de nhieu acc khong cung chon 1 target trong 1 turn. ---
_protect_lock = threading.Lock()
_protect_claims = {}
_break_lock = threading.Lock()
_break_claims = {}
_cc_lock = threading.Lock()
_cc_claims = {}
PROTECT_CLAIM_COOLDOWN = 2.5


def _short_claim(claims_by_group, lock, group, target, owner,
                 ttl=PROTECT_CLAIM_COOLDOWN, turn_token=None):
    """Claim target ngan theo TTL, hoac giu tron turn neu co ``turn_token``."""
    now = time.time()
    with lock:
        claims = claims_by_group.setdefault(group, {})
        if turn_token is None:
            for t, (_o, ts) in list(claims.items()):
                if now - ts > ttl:
                    claims.pop(t, None)
        else:
            # CC phai giu claim den het turn, bat ke cac acc ra quyet dinh lech nhau bao lau.
            # enemy_gen tang khi server gui snapshot quai cua turn moi; luc do moi xoa claim cu.
            for t, (_o, claimed_turn) in list(claims.items()):
                if claimed_turn != turn_token:
                    claims.pop(t, None)
        if target in claims:
            return False
        claims[target] = (owner, now if turn_token is None else turn_token)
        return True


def _claim_target(state, action_class, target, owner, claims, lock, group, turn_token=None):
    coordinator = getattr(state, "battle_coordinator", None)
    tracker = getattr(state, "tracker", None)
    if coordinator is not None and tracker is not None and tracker.active:
        return coordinator.reserve(
            owner, action_class, target, tracker.generation, tracker.turn,
        )
    return _short_claim(
        claims, lock, group, target, owner, turn_token=turn_token,
    )


def _claim_support_action(state, action_class, target, owner, sp, legacy_decider):
    coordinator = getattr(state, "battle_coordinator", None)
    tracker = getattr(state, "tracker", None)
    if coordinator is not None and tracker is not None and tracker.active:
        return coordinator.reserve(
            owner, action_class, target, tracker.generation, tracker.turn,
        )
    return legacy_decider(owner, sp)


# --- HOI SP TOAN TEAM (Toan Hoi Ma): dieu phoi giong heal - 1 con cast/luot, con SP cao nhat ---
_spr_lock = threading.Lock()
_spr_pool = {}
_spr_done = {"t": 0.0}
SPR_BARRIER = 0.4
SPR_COOLDOWN = 2.5


def _sprestore_decide(key, sp):
    """Giong _heal_decide: trong cac unit CO skill hoi SP + thay dong doi thieu SP, con SP cao nhat
    gianh quyen cast luot nay (chi 1 con cast la du)."""
    now = time.time()
    with _spr_lock:
        if now - _spr_done["t"] < SPR_COOLDOWN:
            return False
        _spr_pool[key] = (sp, now)
    time.sleep(SPR_BARRIER)
    with _spr_lock:
        if time.time() - _spr_done["t"] < SPR_COOLDOWN:
            return False
        recent = {k: v for k, v in _spr_pool.items() if now - v[1] <= SPR_BARRIER + 1.0}
        winner = max(recent, key=lambda k: (recent[k][0], k))
        if winner == key:
            _spr_done["t"] = time.time()
            return True
        return False


# atype = VI TRI FORMATION cua member (leader o giua). Tinh tu roster, luu o state.my_atype.
# vd: 2 member + leader -> member1=vi tri 1 (atype1), leader=2, member2=vi tri 3 (atype3).
# Sai atype = bi server da (slot strict).


# Ten skill LOGIC (id 1xxxx dung trong Decision) - CHI de log cho de doc (thay vi soi id chay).
# Nguon: KNOWLEDGE.md bang skill + config.SKILL_SP_COST comments.
SKILL_NAMES = {
    10000: "Đánh thường",
    10005: "Ném Đá",
    11004: "Thanh Lưu",
    11009: "Toàn Hồi Ma Thuật",   # hoi SP TOAN TEAM (0x2b01, cat6) - KHONG phai Hoi Sinh!
    11010: "Toàn Trị Liệu",
    12003: "Hỏa Tiễn",
    12006: "Nhất Kích",
    12009: "Hỏa Kiếm",
    13013: "Loạn Kích",
    17001: "Phòng thủ",
    17997: "Bỏ chạy",
    18001: "Bỏ chạy",
}

SKILL_NAMES.update({
    10009: "Giai Ket Gioi",
    10010: "Ket Gioi",
    10014: "Giai Kinh",
    10015: "Kinh",
    10026: "Linh Kinh",
    10038: "Song Kinh",
    10039: "Due Kinh",
    10040: "Thuan Kinh",
    10041: "S.Ket Gioi",
    10004: "Cay Tinh",
    10033: "Boc Cam",
    11014: "Bang Phong",
    11027: "Thien Bang Vu",
    11028: "Suong Quyen",
    11039: "Bang Sieu",
    11002: "Bang Tuong",
    11012: "Giai Tru",
    13002: "Tuyen Phong",
    13005: "An Minh",
    13007: "Huyen Kich",
    13015: "Thanh Long",
    13042: "S.An Than",
    13046: "Vo Tan Lam Phong",
    13050: "Bua Tiec",
    13052: "The Loc Xoay",
    14008: "Hon Me",
    14021: "Hon Loan",
    14065: "C.Hon Loan",
    20014: "Tu Nhan Hon Loan",
    20025: "Thu Tinh",
    20026: "Bang Phong",
    20027: "Hoan Phong",
    20048: "Toan Phong Sieu",
    20049: "Thu Tinh Sieu",
    20051: "Cuu Vi Ho Mi Hoac",
    20055: "S.Hon Loan",
    20058: "Ban Thu Doa Dia",
})


# Nhom skill bao ve. Dung de tranh buff trung va de pha buff dich dung loai.
PROTECT_KET_GIOI = (10010, 10041)                         # Ket Gioi, S.Ket Gioi
PROTECT_STEALTH = (13005, 13042)                          # An Minh, S.An Than
PROTECT_KINH = (10015, 10026, 10038, 10039, 10040)         # Kinh + cac bien the
PROTECT_PASSIVE = (11002,)                                 # Bang Tuong: chi de nhan status, khong auto-cast
PROTECT_SKILLS = frozenset(PROTECT_KET_GIOI + PROTECT_STEALTH + PROTECT_KINH + PROTECT_PASSIVE)
BREAK_GENERIC = 11012     # Giai Tru
BREAK_KET_GIOI = 10009    # Giai Ket Gioi
BREAK_KINH = 10014        # Giai Kinh

# Khong che chi dung trong quest_mode. Boss mode khang CC -> khong dung.
CC_HIGH_SKILLS = (11014, 11039, 20026, 11027, 11028, 14008, 13007)
CC_LOW_LOCK_SKILLS = (10004, 10033, 20025, 20049, 13002, 13052, 13015,
                      20027, 20048, 13050, 13046)
CC_CHAOS_SKILLS = (14021, 14065, 20014, 20051, 20055, 20058)
# Tat ca CC khong phai Hon Loan nam chung 1 nhom: Bang/choang/troi/gio khoa/hon me...
CC_CONTROL_SKILLS = frozenset(CC_HIGH_SKILLS + CC_LOW_LOCK_SKILLS)
CC_LOCK_SKILLS = CC_CONTROL_SKILLS
CC_SKILLS = frozenset(CC_HIGH_SKILLS + CC_LOW_LOCK_SKILLS + CC_CHAOS_SKILLS)
AUTO_PHASE_ENEMY_HP_THRESHOLD = 1500

# NPC nguy hiem: neu nhieu con cung xuat hien thi CC theo thu tu nay truoc.
DANGEROUS_CC_NPC_NAMES = (
    "chu cong",
    "hang nga",
    "gia cat luong",
    "tu ma y",
    "luc ton",
    "bang thong",
    "lu bo",
    "tran cung",
)


def _dangerous_npc_names():
    raw = getattr(config, "DANGEROUS_NPC_NAMES", DANGEROUS_CC_NPC_NAMES)
    if raw is None:
        raw = DANGEROUS_CC_NPC_NAMES
    return [_norm_name(n) for n in raw if str(n or "").strip()]


class Decision:
    # b = loai dich cua skill: 0=danh quai, 2=1 dong doi, 3=toan party (tu defend_test.pcap)
    def __init__(self, unit, atype, target, skill, b=0):
        self.unit = unit
        self.atype = atype
        self.target = target
        self.skill = skill
        self.b = b

    def __repr__(self):
        _sn = SKILL_NAMES.get(self.skill)
        _sk = f"{self.skill}({_sn})" if _sn else str(self.skill)
        return f"Decision(unit={self.unit} atype={self.atype} b={self.b} target={self.target} skill={_sk})"


def _offered_targets(options, atype):
    """Cac target hop le cho atype dang dung (server liet ke trong 0x35).
    KHONG FALLBACK sang target cua atype KHAC: server gui 1 goi 0x35 RIENG cho tung unit (party 5
    nguoi = toi 10 goi/luot, moi goi CHI 1 atype) -> bot nhan duoc goi 0x35 cua THANH VIEN KHAC (chua
    phai luot minh) VAN kich _arm_decision (khong loc theo atype o tang goi). Truoc day fallback
    'options rong cho atype minh -> dung target cua nguoi khac' -> gui atk SAI LUC/SAI DU LIEU (server
    im lang bo qua -> turn khong tien -> lap lai Y HET vi enemy_hp chua doi). Rong -> [] (KHONG danh,
    _make_decisions se bo qua cycle nay, cho goi 0x35 That cua minh)."""
    return [o[1] for o in options if o[0] == atype]


# Vi tri quai noi bo: pos = hang*10 + cot. hang(b byte)=pos//10, cot(target)=pos%10.
def _row(pos):
    return pos // 10


def _col(pos):
    return pos % 10


def _same_row(a, b):
    """Cung hang battle? (2 hang: pos//10 = 0 hang truoc, 1 hang sau)."""
    return a // 10 == b // 10


def _col_reachable(col_val, off):
    """Cot 'col_val' (0-indexed noi bo) co NAM TRONG offered (server cho phep) khong - thu CA 2
    quy uoc (0-indexed thang, hoac +1 kieu pho ban to doi) THAY VI doan 1 offset chung cho ca tran
    (da xac nhan sai - xem _offer_min)."""
    return col_val in off or (col_val + 1) in off


def _resolve_target(pos, offered):
    """Target THAT gui cho server ung voi pos: uu tien cot dung y (0-indexed) neu no nam trong
    offered, khong thi thu +1 (quy uoc pho ban to doi). Tra None neu KHONG cot nao hop le (offered
    khong chua con nay - hiem, coi nhu khong the danh con do luc nay)."""
    c = _col(pos)
    off = set(offered)
    if c in off:
        return c
    if (c + 1) in off:
        return c + 1
    return None


def _train_target(enemy_slots, offered):
    """RULE TARGET KHI TRAIN (dung CHUNG cho danh thuong + combo -> moi unit dong target,
    combo moi an). 'offered' = danh sach COT hop le (0x35, theo offer-space). Tra ve pos (hang*10+cot).
      1. Block 3 quai lien nhau cung hang (DAU TIEN) -> con GIUA (AoE trung ca 3)
      2. Khong co -> block 2 quai (DAU TIEN) -> con DAU (thap nhat)
      3. Khong co -> con LE dau tien (thap nhat)
    So sanh cot: dung _col_reachable (kiem tra TUNG cot that, KHONG doan offset chung - xem
    _resolve_target/_offer_min)."""
    off = set(offered)
    es = set(enemy_slots)
    if not es:
        return None
    s = sorted(es)
    for a in s:   # nhom 3 cung hang
        if (a + 1) in es and (a + 2) in es and _same_row(a, a + 2):
            # Uu tien con GIUA (AoE trung ca 3) NEU cot no offered. Cot giua KHONG offered (atype
            # nay khong voi toi giua) -> FALLBACK NGAY trong CUNG khoi 3 nay (uu tien DAU truoc,
            # roi CUOI) - TRUOC DAY roi thang xuong vong quet "nhom 2" o duoi, vo tinh khop cap
            # (a+1,a+2) truoc cap (a,a+1) o LAN QUET SAU -> ra con CUOI (thu 3) thay vi con DAU,
            # sai voi ky vong "gan nhat voi giua" (da xac nhan qua quan sat thuc te: 3 con gan
            # nhau nhung bi target con thu 3 thay vi giua).
            if _col_reachable(_col(a + 1), off):
                return a + 1
            log.warning("TRAIN-TARGET: 3 con lien nhau (pos=%d,%d,%d) nhung COT GIUA (%d) KHONG "
                        "offered (off=%s) -> fallback DAU/CUOI. Can log nay de xac dinh day la "
                        "server THAT SU khong cho with toi giua (atype nay) hay bug o cho khac.",
                        a, a + 1, a + 2, _col(a + 1), sorted(off))
            if _col_reachable(_col(a), off):
                return a
            if _col_reachable(_col(a + 2), off):
                return a + 2
    for a in s:   # nhom 2 cung hang -> con thap nhat
        if (a + 1) in es and _same_row(a, a + 1):
            if _col_reachable(_col(a), off):
                return a
            if _col_reachable(_col(a + 1), off):
                return a + 1
    for t in s:   # le -> con thap nhat co cot offered
        if _col_reachable(_col(t), off):
            return t
    return None


def _attack(unit, atype, pos, skill, fb_col, offered=None):
    """Tao Decision tan cong: pos -> b=hang(pos//10), target=cot THAT (_resolve_target - kiem tra
    cot co nam trong offered KHONG, KHONG doan offset chung nhu _offer_min cu - da xac nhan sai:
    co truong hop offered=[1..5] ma KHONG can +1, lam target lech sang con ke ben con dinh danh).
    pos None -> fallback cot fb_col (da o offer-space, hang truoc, b=0)."""
    if pos is None:
        return Decision(unit, atype, fb_col, skill, b=0)
    t = _resolve_target(pos, offered) if offered else _col(pos)
    if t is None:   # cot that KHONG nam trong offered (hiem) -> fallback ve cot tho (con hon khong gui)
        t = _col(pos)
    return Decision(unit, atype, t, skill, b=_row(pos))


def _has_group3(enemy_slots):
    """Co 3 con quai lien nhau cung hang khong (de Hoa Tien dang dong SP)."""
    es = set(enemy_slots)
    for a in sorted(es):
        if (a + 1) in es and (a + 2) in es and _same_row(a, a + 2):
            return True
    return False


def _has_group2(enemy_slots):
    """Co >=2 con quai lien nhau cung hang khong (Hoa Tien trung >=2 con)."""
    es = set(enemy_slots)
    for a in sorted(es):
        if (a + 1) in es and _same_row(a, a + 1):
            return True
    return False


# ---- Tra cuu thuoc tinh skill tu config.SKILL_INFO (auto tu skills_data.json: cost/combo/splash).
#      Thieu data -> fallback (SKILL_SP_COST / coi nhu combo duoc) de khong vo combat. ----
def _sinfo(skill):
    return getattr(config, "SKILL_INFO", {}).get(skill)


def _skill_cost(skill):
    """SP cost cua skill (tu SKILL_INFO; fallback SKILL_SP_COST; 0 neu chua biet)."""
    info = _sinfo(skill)
    if info is not None:
        return info.get("cost", 0)
    return getattr(config, "SKILL_SP_COST", {}).get(skill, 0)


def _cat(skill):
    """LOAI skill (idx11): 1=dame combo duoc, 2=dame khong combo, 4..15=support. Thieu -> 1."""
    info = _sinfo(skill)
    return info.get("cat", 1) if info is not None else 1


def _is_attack(skill):
    """True = skill GAY DAME (cat in {1,2}). False = support (heal/buff/giai/hoi sinh...)."""
    return _cat(skill) in (1, 2)


def _can_combo(skill):
    """True = skill COMBO DUOC (cat==1: HoaTien/NemDa/LoanKich/DaLan). False = khong combo
    (dame cat=2 nhu MuaDa/all-target, hoac support)."""
    return _cat(skill) == 1


def _splash(skill):
    """Kieu nham: 1=don, 2=trai doc, 3=trai ngang, 4=don dap (multi-hit 1 con), 8=TOAN BO quai."""
    info = _sinfo(skill)
    return info.get("splash", 1) if info is not None else 1


def _is_alltarget(skill):
    """True = skill DAME danh TOAN BO quai (splash==8: LieuNguyenHoa, LongTroiLoDat)."""
    return _is_attack(skill) and _splash(skill) == 8


def _is_revive(skill):
    """True = skill HOI SINH (cat==8: 'Hoi Sinh' 11013)."""
    return _cat(skill) == 8


def _protect_class(skill):
    if skill in PROTECT_KET_GIOI:
        return "ket_gioi"
    if skill in PROTECT_STEALTH:
        return "stealth"
    if skill in PROTECT_KINH:
        return "kinh"
    return None


def _is_protect_skill(skill):
    return _protect_class(skill) is not None


def _norm_name(name):
    s = str(name or "").replace("Đ", "D").replace("đ", "d")
    return unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii").lower().strip()


def _cc_class(skill):
    if skill in CC_CHAOS_SKILLS:
        return "chaos"
    if skill in CC_CONTROL_SKILLS:
        return "control"
    return None


def _is_cc_skill(skill):
    return _cc_class(skill) is not None


def _enemy_cc_classes(state, pos):
    b1, b2 = _row(pos), _col(pos)
    if hasattr(state, "crowd_skills"):
        skills = state.crowd_skills(b1, b2)
    else:
        skills = set(getattr(state, "crowd_status", {}).get((b1, b2), ()))
    return {c for c in (_cc_class(s) for s in skills) if c}


def _dangerous_enemy_rank(state, pos):
    names = _dangerous_npc_names()
    best = len(names)
    for name in (getattr(state, "enemy_pos_names", {}) or {}).get(pos, ()) or ():
        norm = _norm_name(name)
        for rank, target in enumerate(names):
            if target in norm:
                best = min(best, rank)
    return best


def _dangerous_enemy_positions(state, positions):
    names = _dangerous_npc_names()
    ranked = []
    for pos in positions:
        rank = _dangerous_enemy_rank(state, pos)
        if rank < len(names):
            ranked.append((rank, pos))
    hp = getattr(state, "enemy_hp", {}) or {}
    return [pos for _rank, pos in sorted(ranked, key=lambda x: (x[0], -hp.get(x[1], 1), -x[1]))]


def pick_cc_skill(skills, sp, phase):
    """Chon skill khong che cho quest_mode.
    phase='high': bang/hon me/stun; phase='low': cay/gio/hon loan."""
    learned = list(skills or [])
    base_order = CC_HIGH_SKILLS if phase == "high" else (CC_LOW_LOCK_SKILLS + CC_CHAOS_SKILLS)
    # Skill vua gay dame vua CC (vd Huyen Kich) uu tien hon skill chi CC, giu thu tu trong tung nhom.
    order = [s for s in base_order if _is_attack(s)] + [s for s in base_order if not _is_attack(s)]
    for group_skill in order:
        if group_skill in learned and sp >= _skill_cost(group_skill):
            return group_skill
    return None


def _has_support_skill(skills):
    """Unit co it nhat 1 skill HOI: hoi sinh (cat8) / Toan Tri Lieu (11010) / Toan Hoi Ma (cat6).
    -> quest mode: de danh SP cho vai tro hoi, chi danh skill atk khi SP > SUPPORT_RESERVE_SP."""
    if any(_is_revive(s) for s in skills):
        return True
    if any(_is_protect_skill(s) for s in skills):
        return True
    if getattr(config, "SKILL_HEAL_ALL", None) in skills:
        return True
    return pick_sp_restore_skill(skills) is not None


def _hang_cua(state, unit):
    """Hang cua unit CUA MINH trong tran (b1). Tran thuong: char=3, pet=2.

    LOAN DAU (PvP) server co the xep minh o phe kia -> char=0, pet=1. Doc tu state thay vi viet
    chet; mac dinh cua state GIU y het gia tri cu nen moi mode khac khong doi.
    """
    char_row = getattr(state, "char_row", 3)
    if unit == config.UNIT_CHAR:
        return char_row
    khac = [r for r in getattr(state, "ally_rows", (2, 3)) if r != char_row]
    return khac[0] if khac else 2


def _hang_char_ta(state):
    """Hang cua CHAR phe TA. Tran thuong = 3, LOAN DAU bi xep phe kia = 0."""
    return _hang_cua(state, config.UNIT_CHAR)


def _hang_pet_ta(state):
    """Hang cua PET phe TA. Tran thuong = 2, LOAN DAU bi xep phe kia = 1."""
    return _hang_cua(state, config.UNIT_PET)


def _revive_decision_for_skill(state, unit, stat, rev):
    """Dung skill hoi sinh cu the, target con chet tu dong theo rule uu tien chung."""
    if rev is None or not _is_revive(rev):
        return None
    # HET QUAI SONG = tran DA KET (server gui them 0x35 "tan du" sau khi thang) -> KHONG hoi sinh:
    # dong doi chet se TU song lai sau tran, cast luc nay chi phi luot + spam.
    if not getattr(state, "enemy_slots", None):
        return None
    # GATE THEO enemy_gen (giong _combat_attack): enemy_gen CHI tang khi co 0x33 QUAI THAT, KHONG
    # tang o goi "tan du 0x35" sau khi thang. Guard 'not enemy_slots' o tren CO THE bi RACE (thread
    # _make_decisions doc enemy_slots luc CON quai -> qua guard -> thread packet reset_enemies clear
    # ngay sau -> revive lot; log in enemy_slots=[] gay hieu nham). Gate gen thi luot tan du (gen
    # chua tang so voi lan hanh dong truoc) LUON bi chan -> het cast Hoi Sinh cuoi tran (fix that).
    gen_attr = "last_atk_gen_char" if unit == config.UNIT_CHAR else "last_atk_gen_pet"
    if getattr(state, gen_attr, -1) == state.enemy_gen:
        return None
    if stat.hp_max > 0 and stat.hp <= 0:      # caster da chet -> ko cast
        return None
    if stat.sp < _skill_cost(rev):
        return None
    dead = state.dead_allies()                # [(b1,b2,hp_max)]
    if not dead:
        return None
    # dang ky chinh minh CO revive (de party biet vi tri minh la nguoi cuu duoc)
    pidx = getattr(state, "party_idx", None)
    if state.self_slot is not None:
        register_revive(pidx, _hang_cua(state, unit), state.self_slot)
    # target: co Hoi Sinh TRUOC; sau do moi den dang co bao ve / role support / maxHP.
    dead.sort(key=lambda x: _dead_target_sort_key(state, pidx, x))
    owner = state.label + (":char" if unit == config.UNIT_CHAR else ":pet")
    coordinator = getattr(state, "battle_coordinator", None)
    tracker = getattr(state, "tracker", None)
    if coordinator is not None and tracker is not None and tracker.active:
        target = next(
            (
                candidate for candidate in dead
                if coordinator.reserve(
                    owner, "revive", (candidate[0], candidate[1]),
                    tracker.generation, tracker.turn,
                )
            ),
            None,
        )
    else:
        target = _revive_decide(owner, stat.sp, pidx, dead)
    if target is None:
        return None
    b1, b2, _hp = target
    at = state.my_atype
    setattr(state, gen_attr, state.enemy_gen)   # danh dau da hanh dong tren gen nay (giong attack) -> tan du/lap khong revive lai
    return Decision(unit, at, b2, rev, b=b1)   # target=slot con chet, b=loai con chet (3char/2pet)


def _try_revive(state, unit, skills, stat, options):
    """HOI SINH (check TRUOC heal): caster CON SONG + co skill hoi sinh + du SP + co dong doi CHET
    + dieu phoi moi target chet 1 caster. Target con chet uu tien:
      1) con chet CO skill hoi sinh 2) dang co bao ve 3) support 4) maxHP goc cao nhat.
    Con chet thi KHONG cast (caster phai song). Tra Decision hoac None."""
    rev = next((s for s in skills if _is_revive(s)), None)
    return _revive_decision_for_skill(state, unit, stat, rev)


def _combo_block_ok(combo, enemy_slots):
    """Du block quai de XAI skill combo nay chua?
      - splash=4 (don dap, DAT SP nhu Loan Kich) -> chi xai khi block 3 (2 quai -> phi).
      - splash 2/3 (trai, RE) -> block 2 la du."""
    need3 = _splash(combo) == 4
    return _has_group3(enemy_slots) if need3 else _has_group2(enemy_slots)


def pick_combo_skill(skills):
    """COMBO TRAIN: skill COMBO DUOC (cat==1) + splash 2/3/4 (don dap/trai), RE nhat. None neu khong.
    (Bo splash=1 don - phi SP danh combo 1 con.) Fallback COMBO_TRAIN_SKILLS neu thieu SKILL_INFO."""
    aoe = [s for s in skills if _can_combo(s) and _splash(s) in (2, 3, 4)]
    if aoe:
        return min(aoe, key=_skill_cost)
    for s in getattr(config, "COMBO_TRAIN_SKILLS", []):   # fallback khi chua co skills_data
        if s in skills:
            return s
    return None


def pick_boss_skill(skills):
    """Skill danh BOSS/don le: DAME (cat in {1,2}), uu tien splash 4 (don dap) > 1 (don), cung hang
    -> cost cao nhat. KHONG can combo. Khong co don dap/don -> skill DAU (skill[0], luon dame).
    skills phai co THU TU (list) de fallback skill[0] dung."""
    RANK = {4: 2, 1: 1}
    cand = [s for s in skills if s not in CC_SKILLS and _is_attack(s) and _splash(s) in (4, 1)]
    if cand:
        return max(cand, key=lambda s: (RANK[_splash(s)], _skill_cost(s)))
    lst = [s for s in skills if s not in CC_SKILLS and _is_attack(s)]
    return lst[0] if lst else None


def pick_alltarget_skill(skills, sp=None):
    """Skill DAME danh TOAN BO quai (splash==8). DAT NHAT (= manh nhat) TRONG TAM SP.

    Truoc day lay RE NHAT -> dac ky all-target (thuong dat) gan nhu khong bao gio duoc dung khi
    pet co san mot skill all-target thuong re hon: do duoc 8/48 pet roi vao canh nay, nang nhat la
    0x9f94 "Quan Vu Ba" co dac ky 365SP ma luon danh skill thuong 84SP. User: doi lai lay skill
    DAT NHAT truoc.

    VI SAO PHAI XET SP o day chu khong de caller lo: caller chi kiem `sp >= cost(allt)` roi thoi -
    tra ve skill dat ma KHONG DU SP thi caller BO LUON ca nhanh all-target, roi xuong combo/danh
    thuong, MAT ca skill all-target re ma dang le dung duoc. Nen: chon dat nhat trong so DU SP;
    khong con nao du SP thi tra cai RE NHAT de caller tu loai (giu y het hanh vi cu o ca do).
    sp=None -> khong xet SP, lay dat nhat (dung cho cho nao chi can biet "skill all-target manh nhat").
    """
    allt = [s for s in skills if _is_alltarget(s)]
    if not allt:
        return None
    if sp is None:
        return max(allt, key=_skill_cost)
    du = [s for s in allt if _skill_cost(s) <= sp]
    return max(du, key=_skill_cost) if du else min(allt, key=_skill_cost)


def pick_sp_restore_skill(skills):
    """Skill HOI SP TOAN TEAM (Toan Hoi Ma = cat 6). Lay cat-6 COST CAO NHAT (ban team dat hon ban
    don, vd 0x2b01/40 > 0x2afe/35). None neu unit khong co."""
    cand = [s for s in skills if (_sinfo(s) or {}).get("cat") == 6]
    return max(cand, key=_skill_cost) if cand else None


def pick_protect_skill(skills, sp):
    """Chon buff bao ve theo uu tien: Ket Gioi -> An Than -> Kinh."""
    learned = list(skills or [])
    for group in (PROTECT_KET_GIOI, PROTECT_STEALTH, PROTECT_KINH):
        for s in learned:
            if s in group and sp >= _skill_cost(s):
                return s
    return None


def register_support_skills(party_idx, b1, slot, skills):
    if slot is None:
        return
    learned = set(skills or [])
    info = {
        "revive": any(_is_revive(s) for s in learned),
        "protect": any(_is_protect_skill(s) for s in learned),
        "hp_heal": getattr(config, "SKILL_HEAL_ALL", None) in learned,
        "sp_restore": pick_sp_restore_skill(learned) is not None,
    }
    if any(info.values()):
        _support_reg[(party_idx, b1, slot)] = info
    else:
        _support_reg.pop((party_idx, b1, slot), None)
    if info["revive"]:
        register_revive(party_idx, b1, slot)


def _register_current_unit(state, unit, skills):
    slot = getattr(state, "self_slot", None)
    if slot is None:
        return
    b1 = _hang_cua(state, unit)
    register_support_skills(getattr(state, "party_idx", None), b1, slot, skills)


def _protect_mode_enabled(state):
    return bool(getattr(state, "quest_mode", False) or getattr(state, "boss_mode", False))


def _should_skip_protect_after_cc(state):
    """Quest: chi skip bao ve khi CC that da co trong status-list dau turn."""
    if not _cc_mode_enabled(state):
        return False
    hp = getattr(state, "enemy_hp", {}) or {}
    alive = [pos for pos in (getattr(state, "enemy_slots", []) or []) if hp.get(pos, 0) > 0]
    if not alive:
        return False
    dangerous = _dangerous_enemy_positions(state, alive)
    if any(not _enemy_cc_classes(state, pos) for pos in dangerous):
        return False
    dangerous_set = set(dangerous)
    normal_uncc = [
        pos for pos in alive
        if pos not in dangerous_set and not _enemy_cc_classes(state, pos)
    ]
    return len(normal_uncc) <= 2


def _has_high_hp_enemy(state, threshold=AUTO_PHASE_ENEMY_HP_THRESHOLD):
    hp = getattr(state, "enemy_hp", {}) or {}
    return any(
        hp.get(pos, 0) > threshold
        for pos in (getattr(state, "enemy_slots", []) or [])
    )


def _alive_allies_with_self(state, unit, stat):
    cands = {}
    for key, u in getattr(state, "allies", {}).items():
        b1, b2 = key
        if b1 not in state.ally_rows or u.hp_max <= 0 or u.hp <= 0:
            continue
        cands[key] = u
    slot = getattr(state, "self_slot", None)
    if slot is not None and getattr(stat, "hp_max", 0) > 0 and getattr(stat, "hp", 0) > 0:
        cands[(_hang_cua(state, unit), slot)] = stat
    return [(b1, b2, u) for (b1, b2), u in cands.items()]


def _hp_abs(u):
    return getattr(u, "hp", 0)


def _protect_target_order(state, unit, stat):
    pidx = getattr(state, "party_idx", None)
    self_slot = getattr(state, "self_slot", None)
    self_key = (_hang_cua(state, unit), self_slot) if self_slot is not None else None
    all_alive = _alive_allies_with_self(state, unit, stat)
    available = []
    for b1, b2, u in all_alive:
        if hasattr(state, "has_protection") and state.has_protection(b1, b2):
            continue
        available.append((b1, b2, u))
    ordered = []
    rest = list(available)

    def _take(pred, include_self=False):
        nonlocal rest
        group = sorted(
            (x for x in rest if pred(x[0], x[1]) and (include_self or (x[0], x[1]) != self_key)),
            key=lambda x: (_hp_abs(x[2]), x[0], x[1]),
        )
        if not group:
            return
        ordered.extend(group)
        used = {(b1, b2) for b1, b2, _u in ordered}
        rest = [x for x in rest if (x[0], x[1]) not in used]

    self_has_revive_and_protect = (
        self_key is not None
        and _slot_has_revive(pidx, self_key[0], self_key[1])
        and _slot_has_protect_skill(pidx, self_key[0], self_key[1])
    )
    if self_has_revive_and_protect:
        _take(lambda b1, b2: (b1, b2) == self_key, include_self=True)
    _take(lambda b1, b2: _slot_has_revive(pidx, b1, b2))
    if self_key is not None and not self_has_revive_and_protect:
        _take(lambda b1, b2: (b1, b2) == self_key, include_self=True)
    _take(lambda b1, b2: _slot_has_protect_skill(pidx, b1, b2))
    _take(lambda b1, b2: _slot_has_hp_heal(pidx, b1, b2))
    _take(lambda b1, b2: _slot_has_sp_restore(pidx, b1, b2))
    ordered.extend(sorted(rest, key=lambda x: (_hp_abs(x[2]), x[0], x[1])))
    return [(b1, b2) for b1, b2, _u in ordered]


def _try_protect(state, unit, skills, stat):
    """Quest/boss: sau Hoi Sinh, buff bao ve truoc khi heal HP/SP."""
    if not _protect_mode_enabled(state) or not getattr(state, "enemy_slots", None):
        return None
    if not _has_high_hp_enemy(state):
        return None
    if _should_skip_protect_after_cc(state):
        return None
    if getattr(stat, "hp_max", 0) > 0 and getattr(stat, "hp", 0) <= 0:
        return None
    skill = pick_protect_skill(skills, getattr(stat, "sp", 0))
    if skill is None:
        return None
    group = getattr(state, "party_idx", None)
    if group is None:
        group = state.label
    owner = state.label + (":char:protect" if unit == config.UNIT_CHAR else ":pet:protect")
    for b1, b2 in _protect_target_order(state, unit, stat):
        if _claim_target(
            state, "protect", (b1, b2), owner,
            _protect_claims, _protect_lock, group,
        ):
            return Decision(unit, state.my_atype, b2, skill, b=b1)
    return None


def _status_classes(skills):
    out = set()
    for s in skills or ():
        cls = _protect_class(s)
        if cls:
            out.add(cls)
    return out


def _try_break_enemy_protect(state, unit, skills, stat, options):
    """Quest/boss: sau heal HP/SP, pha bao ve cua DICH (row 0/1), khong cham team minh."""
    if not _protect_mode_enabled(state) or not getattr(state, "enemy_slots", None):
        return None
    if getattr(stat, "hp_max", 0) > 0 and getattr(stat, "hp", 0) <= 0:
        return None
    learned = set(skills or [])
    offered = _offered_targets(options, state.my_atype)
    if not offered:
        return None
    targets = []
    alive_enemies = set(getattr(state, "enemy_slots", []) or [])
    for (b1, b2), ss in getattr(state, "protect_status", {}).items():
        if b1 not in state.enemy_rows:
            continue
        pos = b1 * 10 + b2
        if pos not in alive_enemies:
            continue
        classes = _status_classes(ss)
        if not classes:
            continue
        # Uu tien target dang An Than -> Ket Gioi -> Kinh.
        rank = 0 if "stealth" in classes else 1 if "ket_gioi" in classes else 2
        targets.append((rank, b1, b2, classes))
    if not targets:
        return None
    group = getattr(state, "party_idx", None)
    if group is None:
        group = state.label
    owner = state.label + (":char:break" if unit == config.UNIT_CHAR else ":pet:break")
    for _rank, b1, b2, classes in sorted(targets):
        pos = b1 * 10 + b2
        target_col = _resolve_target(pos, offered)
        if target_col is None:
            continue
        skill = None
        if BREAK_GENERIC in learned and stat.sp >= _skill_cost(BREAK_GENERIC):
            skill = BREAK_GENERIC
        elif "ket_gioi" in classes and BREAK_KET_GIOI in learned and stat.sp >= _skill_cost(BREAK_KET_GIOI):
            skill = BREAK_KET_GIOI
        elif "kinh" in classes and BREAK_KINH in learned and stat.sp >= _skill_cost(BREAK_KINH):
            skill = BREAK_KINH
        if skill is None:
            continue
        if _claim_target(
            state, "break_protect", (b1, b2), owner,
            _break_claims, _break_lock, group,
        ):
            return Decision(unit, state.my_atype, target_col, skill, b=b1)
    return None


def _cc_mode_enabled(state):
    return bool(getattr(state, "quest_mode", False) and not getattr(state, "boss_mode", False))


def _enemy_target_candidates(state, offered, target_key):
    target_key = target_key or "auto"
    off = set(offered or [])
    alive = [
        pos for pos in (getattr(state, "enemy_slots", []) or [])
        if getattr(state, "enemy_hp", {}).get(pos, 0) > 0 and _resolve_target(pos, off) is not None
    ]
    if not alive:
        return []
    if target_key == "enemy_low_hp":
        return sorted(alive, key=lambda pos: (getattr(state, "enemy_hp", {}).get(pos, 1), pos))
    if target_key == "enemy_high_hp":
        return sorted(alive, key=lambda pos: (-getattr(state, "enemy_hp", {}).get(pos, 1), pos))
    if target_key == "enemy_last":
        return sorted(alive, reverse=True)
    if target_key == "dangerous_npc":
        dangerous = _dangerous_enemy_positions(state, alive)
        if dangerous:
            return dangerous
    if target_key == "block":
        first = _train_target(alive, offered)
        rest = [pos for pos in sorted(alive) if pos != first]
        return ([first] if first is not None else []) + rest
    hp = getattr(state, "enemy_hp", {}) or {}
    return sorted(alive, key=lambda pos: (_dangerous_enemy_rank(state, pos), -hp.get(pos, 1), -pos))


def _cc_target_order(state, offered, cc_kind, target_key="auto"):
    base = _enemy_target_candidates(state, offered, target_key)
    if not base:
        return []
    clean = [pos for pos in base if not _enemy_cc_classes(state, pos)]
    if clean:
        candidates = clean
    elif cc_kind == "chaos":
        candidates = [pos for pos in base if "chaos" not in _enemy_cc_classes(state, pos)]
    else:
        candidates = [pos for pos in base if "control" not in _enemy_cc_classes(state, pos)]
    return candidates


def _try_cc_skill(state, unit, skill, stat, options, phase, target_key="auto", atype=None, require_mode=True):
    """Chon target cho skill khong che, dung chung auto + custom de tranh de CC trung."""
    if require_mode and not _cc_mode_enabled(state):
        return None
    if not getattr(state, "enemy_slots", None):
        return None
    if getattr(stat, "hp_max", 0) > 0 and getattr(stat, "hp", 0) <= 0:
        return None
    at = state.my_atype if atype is None else atype
    offered = _offered_targets(options, at)
    if not offered:
        return None
    cc_kind = _cc_class(skill)
    if cc_kind is None:
        return None
    if getattr(stat, "sp", 0) < _skill_cost(skill):
        return None
    gen_attr = "last_atk_gen_char" if unit == config.UNIT_CHAR else "last_atk_gen_pet"
    if getattr(state, gen_attr, -1) == state.enemy_gen:
        return None
    group = getattr(state, "party_idx", None)
    if group is None:
        group = state.label
    owner = state.label + (":char:cc:" if unit == config.UNIT_CHAR else ":pet:cc:") + str(phase)
    for pos in _cc_target_order(state, offered, cc_kind, target_key):
        target_col = _resolve_target(pos, offered)
        if target_col is None:
            continue
        b1, b2 = _row(pos), _col(pos)
        if _claim_target(
            state, "cc", (b1, b2), owner,
            _cc_claims, _cc_lock, group,
            turn_token=getattr(state, "enemy_gen", 0),
        ):
            setattr(state, gen_attr, state.enemy_gen)
            return Decision(unit, at, target_col, skill, b=b1)
    return None


def _try_cc(state, unit, skills, stat, options, phase):
    """Quest only: CC target, uu tien NPC nguy hiem neu biet ten theo vi tri."""
    if not _cc_mode_enabled(state):
        return None
    if not _has_high_hp_enemy(state):
        return None
    skill = pick_cc_skill(skills, getattr(stat, "sp", 0), phase)
    if skill is None:
        return None
    return _try_cc_skill(state, unit, skill, stat, options, phase, require_mode=False)


def _try_sp_restore(state, unit, skills, stat):
    """HOI SP TOAN TEAM - quest_mode (luon) HOAC train_mode (chi khi quai con block le), goi SAU
    heal HP / TRUOC attack:
      - unit co skill cat-6 (Toan Hoi Ma) + ban than du SP (>=cost)
      - co dong doi (TRU chinh minh) SP < 50%  (doc tu state.allies, SP ca party co trong 0x33)
      - la unit SP cao nhat trong nhom co skill CHUA co lenh (_sprestore_decide dieu phoi, 1 con cast la du)
    Target = slot dong doi <50%, b=3 (skill team). Tra Decision hoac None."""
    spr = pick_sp_restore_skill(skills)
    if spr is None:
        return None
    if state.self_slot is None:
        return None
    if stat.sp < _skill_cost(spr):
        return None
    # CHE DO cho phep hoi SP team:
    #  - QUEST mode: luon (dong quai, atk/combo ton SP -> can duy tri SP team).
    #  - TRAIN mode: CHI khi quai con toan BLOCK LE (khong con cum >=2 con lien nhau de bung combo
    #    AoE) -> it quai, uu tien hoi SP thay vi phi combo (giong y quest, them dieu kien so quai).
    #  - BOSS mode: KHONG hoi SP (don SP nuke boss).
    if getattr(state, "boss_mode", False):
        return None
    if not getattr(state, "quest_mode", False) and _has_group2(state.enemy_slots):
        return None
    b1 = _hang_cua(state, unit)
    low_slot = state.ally_low_sp(getattr(config, "SP_RESTORE_THRESHOLD", 0.5), (b1, state.self_slot))
    if low_slot is None:
        return None
    key = state.label + (":char" if unit == config.UNIT_CHAR else ":pet") + ":spr"
    # HANG DICH = hang CHAR phe TA (khong phai hang 3 co dinh): LOAN DAU co the bi xep sang phe
    # kia -> char ta o hang 0, gui b=3 la HOI SP CHO DICH (va co the bi coi la goi chien dau sai).
    _hc = _hang_char_ta(state)
    if not _claim_support_action(
        state, "heal_sp", (_hc, low_slot), key, stat.sp, _sprestore_decide,
    ):
        return None
    return Decision(unit, state.my_atype, low_slot, spr, b=_hc)


def _lowest_hp_enemy(state, offered):
    """Pos quai con SONG it mau NHAT (cot phai trong offered). None neu khong co.
    Dung khi danh boss/don le (quest <=5) - dam con sap chet truoc."""
    off = set(offered)
    alive = [(pos, hp) for pos, hp in state.enemy_hp.items() if hp > 0 and _col_reachable(_col(pos), off)]
    if not alive:
        return None
    return min(alive, key=lambda x: x[1])[0]


_CUSTOM_AUTO = object()


def _battle_rules(state, unit_key):
    cfg = getattr(state, "battle_config", {}) or {}
    raw = cfg.get(unit_key)
    if unit_key == "pet":
        pets_cfg = cfg.get("pets")
        if isinstance(pets_cfg, dict):
            # Format MOI: rule RIENG TUNG PET (GUI 4 tab), key = str(pet id). Pet chua co
            # config -> auto (list rong), KHONG roi ve bo "pet" chung.
            pid = getattr(state, "active_pet_id", None)
            raw = pets_cfg.get(str(pid)) if pid is not None else None
        elif raw is not None:
            # Format CU (chi co "pet" chung): rule do la cua PET USER DANG DUNG luc set config
            # (config cu khong ghi pet id) -> chi ap cho pet DAU TIEN thay sau login
            # (pet_cfg_owner); doi pet khac -> auto. (Yeu cau user khi len ban per-pet.)
            if getattr(state, "active_pet_id", None) != getattr(state, "pet_cfg_owner", None):
                raw = None
    if isinstance(raw, list):
        return [r for r in raw if isinstance(r, dict) and r.get("enabled", True) is not False]
    if isinstance(raw, dict):   # tuong thich ban format tam thoi cu
        mode = raw.get("mode", "auto")
        skill = {"normal": "normal", "defend": "defend", "skill": raw.get("train_skill")}.get(mode, "auto")
        return [{"enabled": True, "condition": "always", "skill": skill or "auto", "target": raw.get("target", "auto")}]
    return []


def _compare_num(actual, op, expect):
    if op == "gt":
        return actual > expect
    if op == "lt":
        return actual < expect
    if op == "eq":
        return actual == expect
    if op == "lte":
        return actual <= expect
    return actual >= expect


def _caster_key(state, unit):
    if getattr(state, "self_slot", None) is None:
        return None
    return _hang_cua(state, unit), state.self_slot


def _any_ally_hp_below(state, pct, exclude=None):
    for key, u in getattr(state, "allies", {}).items():
        if exclude is not None and key == exclude:
            continue
        if u.hp_max > 0 and u.hp > 0 and (u.hp * 100 / u.hp_max) < pct:
            return True
    return False


def _any_ally_sp_below(state, pct, exclude=None):
    for key, u in getattr(state, "allies", {}).items():
        if exclude is not None and key == exclude:
            continue
        if u.hp <= 0:
            continue
        spmax = state.ally_spmax.get(key, getattr(u, "sp_max", 0))
        if spmax > 0 and (u.sp * 100 / spmax) < pct:
            return True
    return False


def _max_enemy_block(enemy_slots):
    rows = {}
    for pos in enemy_slots or []:
        rows.setdefault(_row(pos), []).append(_col(pos))
    best = 0
    for cols in rows.values():
        cur = 0
        prev = None
        for col in sorted(set(cols)):
            cur = cur + 1 if prev is not None and col == prev + 1 else 1
            best = max(best, cur)
            prev = col
    return best


def _is_mineral_battle(state):
    if getattr(state, "mineral_battle", False):
        return True
    for name in getattr(state, "enemy_names", ()) or ():
        s = unicodedata.normalize("NFKD", str(name)).encode("ascii", "ignore").decode("ascii").lower()
        if s.startswith("khoang "):
            return True
    return False


def _rule_condition_ok(rule, state, stat, unit=None):
    cond = (rule or {}).get("condition", "always") if isinstance(rule, dict) else (rule or "always")
    cond = str(cond or "always")
    op = (rule or {}).get("op", "gte") if isinstance(rule, dict) else "gte"
    val = (rule or {}).get("value", None) if isinstance(rule, dict) else None
    n = len(getattr(state, "enemy_slots", []) or [])
    if cond == "always":
        return True
    if cond == "mineral":
        return _is_mineral_battle(state)
    # Tuong thich config cu: mob_gte_4 / hp_lte_50 / sp_gte_65...
    if cond.startswith("mob_gte_"):
        return n >= int(cond.rsplit("_", 1)[1])
    if cond.startswith("mob_lte_"):
        return n <= int(cond.rsplit("_", 1)[1])
    if cond.startswith("sp_gte_"):
        return stat.sp >= int(cond.rsplit("_", 1)[1])
    if cond.startswith("sp_lte_"):
        return stat.sp <= int(cond.rsplit("_", 1)[1])
    if cond == "sp_full":
        return stat.sp_max > 0 and stat.sp >= stat.sp_max
    if cond.startswith("hp_lte_"):
        return stat.hp_max > 0 and (stat.hp * 100 / stat.hp_max) <= int(cond.rsplit("_", 1)[1])
    if cond == "boss":
        return bool(getattr(state, "boss_mode", False))
    if cond == "quest":
        return bool(getattr(state, "quest_mode", False))
    if cond == "ally_dead":
        return bool(state.dead_allies())
    try:
        expect = int(val)
    except Exception:
        return False
    if cond == "mob":
        return _compare_num(n, op, expect)
    if cond == "block":
        return _compare_num(_max_enemy_block(getattr(state, "enemy_slots", []) or []), op, expect)
    if cond == "sp":
        return _compare_num(stat.sp, op, expect)
    if cond == "hp_pct":
        if stat.hp_max <= 0:
            return False
        return _compare_num((stat.hp * 100 / stat.hp_max), op, expect)
    if cond == "ally_hp_pct":
        return _any_ally_hp_below(state, expect, _caster_key(state, unit))
    if cond == "ally_sp_pct":
        return _any_ally_sp_below(state, expect, _caster_key(state, unit))
    return False


def _enemy_target_pos(state, offered, target_key):
    target_key = target_key or "auto"
    es = getattr(state, "enemy_slots", []) or []
    if not es:
        return None
    if target_key in ("auto", "block"):
        return _train_target(es, offered)
    alive = [(pos, hp) for pos, hp in state.enemy_hp.items()
             if hp > 0 and _col_reachable(_col(pos), set(offered))]
    if not alive:
        return None
    if target_key == "enemy_low_hp":
        return min(alive, key=lambda x: x[1])[0]
    if target_key == "enemy_high_hp":
        return max(alive, key=lambda x: x[1])[0]
    if target_key == "enemy_last":
        return max(alive, key=lambda x: x[0])[0]
    if target_key == "dangerous_npc":
        dangerous = _dangerous_enemy_positions(state, [pos for pos, _hp in alive])
        if dangerous:
            return dangerous[0]
    return min(alive, key=lambda x: x[0])[0]


def _ally_target(state, target_key, unit, atype):
    if target_key == "self":
        return _hang_cua(state, unit), atype
    cands = []
    pidx = getattr(state, "party_idx", None)
    for (b1, b2), u in getattr(state, "allies", {}).items():
        if u.hp_max <= 0 or u.hp <= 0:
            continue
        spmax = state.ally_spmax.get((b1, b2), getattr(u, "sp_max", 0))
        sppct = (u.sp / spmax) if spmax else 1.0
        cands.append((b1, b2, u.hp / u.hp_max, sppct))
    if not cands:
        return _hang_cua(state, unit), atype
    if target_key == "ally_high_hp":
        b1, b2, *_ = max(cands, key=lambda x: x[2])
    elif target_key == "ally_low_sp":
        b1, b2, *_ = min(cands, key=lambda x: x[3])
    elif target_key == "ally_high_sp":
        b1, b2, *_ = max(cands, key=lambda x: x[3])
    elif target_key == "ally_revive_skill":
        hits = [x for x in cands if _slot_has_revive(pidx, x[0], x[1])]
        b1, b2, *_ = min(hits or cands, key=lambda x: x[2])
    elif target_key == "ally_protect_skill":
        hits = [x for x in cands if _slot_has_protect_skill(pidx, x[0], x[1])]
        b1, b2, *_ = min(hits or cands, key=lambda x: x[2])
    else:  # ally_low_hp
        b1, b2, *_ = min(cands, key=lambda x: x[2])
    return b1, b2


def _parse_rule_skill(value):
    if value in ("auto", "normal", "defend", "flee"):
        return value
    try:
        return int(value)
    except Exception:
        return "auto"


def _can_attack_new_enemy_gen(state, unit, atype=None):
    if unit == config.UNIT_PET and atype is not None and getattr(state, "solo_multipet", False):
        gen_map = state.last_atk_gen_multipet
        if gen_map.get(atype, -1) == state.enemy_gen:
            return False
        gen_map[atype] = state.enemy_gen
        return True
    gen_attr = "last_atk_gen_char" if unit == config.UNIT_CHAR else "last_atk_gen_pet"
    if getattr(state, gen_attr, -1) == state.enemy_gen:
        return False
    setattr(state, gen_attr, state.enemy_gen)
    return True


def _custom_decision(state, unit, unit_key, skills, stat, options, atype=None):
    rules = _battle_rules(state, unit_key)
    if not rules:
        return None
    at = state.my_atype if atype is None else atype
    offered = _offered_targets(options, at)
    fb = offered[0] if offered else 1
    learned = set(skills or [])
    for rule in rules:
        if not _rule_condition_ok(rule, state, stat, unit):
            continue
        skill = _parse_rule_skill(rule.get("skill", "auto"))
        target_key = rule.get("target", "auto")
        if skill == "auto":
            return _CUSTOM_AUTO
        if skill == "defend":
            return Decision(unit, at, at, config.SKILL_DEFEND, b=_hang_cua(state, unit))
        if skill == "flee":
            return Decision(unit, at, at, config.SKILL_FLEE, b=_hang_cua(state, unit))
        skill_id = config.SKILL_NORMAL if skill == "normal" else skill
        if isinstance(skill_id, int) and skill_id != config.SKILL_NORMAL:
            if skill_id not in learned:
                continue
            if stat.sp < _skill_cost(skill_id):
                continue
            if _is_revive(skill_id):
                rv = _revive_decision_for_skill(state, unit, stat, skill_id)
                if rv is not None:
                    return rv
                continue
            if _is_cc_skill(skill_id):
                cc = _try_cc_skill(state, unit, skill_id, stat, options, "custom",
                                   target_key=target_key, atype=atype, require_mode=False)
                if cc is not None:
                    return cc
                continue
        if target_key in ("ally_low_hp", "ally_high_hp", "ally_low_sp", "ally_high_sp",
                          "ally_revive_skill", "ally_protect_skill", "self"):
            b1, b2 = _ally_target(state, target_key, unit, at)
            if isinstance(skill_id, int) and skill_id != config.SKILL_NORMAL:
                key = state.label + (":char" if unit == config.UNIT_CHAR else ":pet")
                if skill_id in (getattr(config, "SKILL_HEAL_ALL", None),
                                getattr(config, "SKILL_HEAL_ONE", None)):
                    if not _claim_support_action(
                        state, "heal_hp", (b1, b2), key, stat.sp, _heal_decide,
                    ):
                        continue
                elif _cat(skill_id) == 6:
                    if not _claim_support_action(
                        state, "heal_sp", (b1, b2), key + ":spr", stat.sp,
                        _sprestore_decide,
                    ):
                        continue
            return Decision(unit, at, b2, skill_id, b=b1)
        if not _can_attack_new_enemy_gen(state, unit, atype=atype):
            return None
        pos = _enemy_target_pos(state, offered, target_key)
        if pos is None:
            return None
        return _attack(unit, at, pos, skill_id, fb, offered)
    return None



def _combat_attack(state, unit, skills, stat, options, spam_attr, fire_min):
    """Quyet dinh TAN CONG (sau khi da loai heal) - DUNG CHUNG char + pet. 3 che do:
      BOSS  (boss_mode, dungeon): nuke = pick_boss_skill (don dap>don>skill dau), target it mau nhat.
      QUEST (quest_mode, start >6 quai):
            > 6 quai con  -> all-target (neu co) -> ko thi combo AoE -> danh thuong
            <=6 quai con  -> nhu boss + target IT MAU NHAT
      TRAIN (mac dinh): combo (AoE re, combo duoc) khi du SP+block (hoac spam SP day), ko thi danh thuong.
    SP thieu cho skill manh -> danh thuong, cho quan su hoi SP."""
    at = state.my_atype
    offered = _offered_targets(options, at)
    fb = offered[0] if offered else 1
    sp = stat.sp
    es = state.enemy_slots
    cost = _skill_cost
    # CHI danh khi CO du lieu quai MOI (goi 0x33 that) ke tu lan danh truoc. Goi 0x35 (offer luot)
    # KHONG mang du lieu quai -> neu 0x35 den ma KHONG co 0x33 moi kem theo (vd tran DA KET THAT
    # nhung con offer "tan du" den tre, hoac server chi re-broadcast lai cung turn) -> danh LAI tren
    # trang thai CU (stale) la SAI - co the tran da xong that roi. User quan sat truc tiep: end battle
    # -> van gui atk. Skip (None) neu enemy_gen KHONG doi so voi lan danh truoc cua UNIT nay.
    gen_attr = "last_atk_gen_char" if unit == config.UNIT_CHAR else "last_atk_gen_pet"
    if getattr(state, gen_attr, -1) == state.enemy_gen:
        return None
    setattr(state, gen_attr, state.enemy_gen)
    if not es:
        # KHONG CON QUAI SONG (da chet het) -> KHONG danh (nhanh TRAIN ben duoi khong check es rong,
        # se fallback danh MU cot 1 -> goi 0x32 thua sau khi tran DA KET THUC that (server gui them
        # 1 goi 0x35 "tan du" sau khi thang). User xac nhan qua quan sat truc tiep man hinh: end
        # battle -> bot van gui atk 1 lan nua. Return None -> _make_decisions bo qua, khong gui gi.
        return None
    if _is_mineral_battle(state):
        return Decision(unit, at, at, config.SKILL_FLEE,
                        b=_hang_cua(state, unit))

    def low_or_train():
        p = _lowest_hp_enemy(state, offered)
        return p if p is not None else _train_target(es, offered)

    # 1) BOSS mode
    if getattr(state, "boss_mode", False) and es:
        boss = pick_boss_skill(skills)
        pos = low_or_train()
        sk = boss if (boss and sp >= cost(boss)) else config.SKILL_NORMAL
        return _attack(unit, at, pos, sk, fb, offered)

    # 2) QUEST mode (start >6 quai)
    if getattr(state, "quest_mode", False) and es:
        # Unit CO skill hoi (hoi sinh/11010/11009) -> DE DANH SP: SP <= reserve thi danh thuong,
        # giu SP phong turn sau can hoi sinh/hoi mau/hoi SP. Chi SP > reserve moi xai skill atk.
        if _has_support_skill(skills) and sp <= getattr(config, "SUPPORT_RESERVE_SP", 100):
            return _attack(unit, at, low_or_train(), config.SKILL_NORMAL, fb, offered)
        if len(es) > 6:
            allt = pick_alltarget_skill(skills, sp)
            if allt and sp >= cost(allt):
                return _attack(unit, at, _train_target(es, offered), allt, fb, offered)
            combo = pick_combo_skill(skills)   # ko co all-target -> combo AoE thuong
            if combo and sp >= max(fire_min, cost(combo)) and _combo_block_ok(combo, es):
                return _attack(unit, at, _train_target(es, offered), combo, fb, offered)
            return _attack(unit, at, _train_target(es, offered), config.SKILL_NORMAL, fb, offered)
        # <=6 quai -> nhu boss + target it mau nhat
        boss = pick_boss_skill(skills)
        pos = low_or_train()
        sk = boss if (boss and sp >= cost(boss)) else config.SKILL_NORMAL
        return _attack(unit, at, pos, sk, fb, offered)

    # 3) TRAIN mode (combo)
    if stat.sp_max > 0 and sp >= stat.sp_max:
        setattr(state, spam_attr, True)
    combo = pick_combo_skill(skills)
    if (combo and sp >= max(fire_min, cost(combo))
            and (getattr(state, spam_attr) or _combo_block_ok(combo, es))):
        return _attack(unit, at, _train_target(es, offered), combo, fb, offered)
    return _attack(unit, at, _train_target(es, offered), config.SKILL_NORMAL, fb, offered)


def decide_multipet(state, atype, skills, stat, options):
    """DI GIOI SOLO: quyet dinh cho 1 PET RIENG trong so toi da 4 con cung tran (moi con 1 atype
    RIENG: 0,1,3,4 - atype 2 la CHAR). Logic dung GIONG TRAIN mode don gian (combo Hoa Tien/Nem
    Da/Loan Kich neu co du SP+block, khong thi danh thuong) - KHONG lam BOSS/QUEST rieng cho
    truong hop nay (theo yeu cau: "train mode thoi, khong co combo thi danh thuong").
    skills/stat = skill list + Unit (HP/SP) CUA RIENG con pet nay (tu state.multi_pet_skills /
    state.multi_pet[atype]), KHAC voi decide_pet (dung state.skills_pet/state.pet chung 1 con)."""
    offered = _offered_targets(options, atype)
    if not offered:
        return None
    fb = offered[0]
    sp = stat.sp
    es = state.enemy_slots
    if not es:
        return None
    custom = _custom_decision(state, config.UNIT_PET, "pet", skills, stat, options, atype=atype)
    if custom is _CUSTOM_AUTO:
        pass
    elif custom is not None:
        return custom
    if _is_mineral_battle(state):
        return Decision(config.UNIT_PET, atype, atype, config.SKILL_FLEE, b=_hang_pet_ta(state))
    # CHI danh khi CO du lieu quai MOI rieng cho ATYPE nay (tranh danh lap tren 0x33 cu - xem
    # _combat_attack ban goc; o day dung dict rieng theo atype vi 4 pet KHONG the dung chung 1
    # bien gen (se dam vao nhau, chi 1 con "thay" du lieu moi moi luot)).
    gen_map = state.last_atk_gen_multipet
    if gen_map.get(atype, -1) == state.enemy_gen:
        return None
    gen_map[atype] = state.enemy_gen
    combo = pick_combo_skill(skills)
    fire_min = getattr(config, "PET_FIRE_MIN_SP", 0)
    if combo and sp >= max(fire_min, _skill_cost(combo)) and _combo_block_ok(combo, es):
        sk = combo
    else:
        sk = config.SKILL_NORMAL
    pos = _train_target(es, offered)
    if pos is None:
        return None
    return _attack(config.UNIT_PET, atype, pos, sk, fb, offered)


def decide_char(state, options, first_turn=False):
    at = state.my_atype
    _register_current_unit(state, config.UNIT_CHAR, state.skills_char)
    custom = _custom_decision(state, config.UNIT_CHAR, "char", state.skills_char, state.char, options)
    if custom is _CUSTOM_AUTO:
        pass
    elif custom is not None:
        return custom
    # HOI SINH (truoc heal): co dong doi chet + char co skill hoi sinh + thang dieu phoi
    rv = _try_revive(state, config.UNIT_CHAR, state.skills_char, state.char, options)
    if rv is not None:
        return rv
    # CC ti le cao (quest only): sau Hoi Sinh, truoc buff bao ve.
    cc = _try_cc(state, config.UNIT_CHAR, state.skills_char, state.char, options, "high")
    if cc is not None:
        return cc
    # BUFF BAO VE (quest/boss): Ket Gioi -> An Than -> Kinh, truoc heal HP/SP.
    prot = _try_protect(state, config.UNIT_CHAR, state.skills_char, state.char)
    if prot is not None:
        return prot
    # CC ti le thap / Hon Loan (quest only): sau buff bao ve.
    cc = _try_cc(state, config.UNIT_CHAR, state.skills_char, state.char, options, "low")
    if cc is not None:
        return cc
    # HOI MAU: thanh vien HP yeu + du SP + co skill heal + la con SP cao nhat duoc heal
    if (state.any_ally_low(config.HEAL_HP_THRESHOLD)
            and state.char.sp >= config.HEAL_SP_COST
            and config.SKILL_HEAL_ALL in state.skills_char):
        _low = state.lowest_hp_ally()
        _ht = _low.slot if (_low is not None and getattr(_low, "slot", None) is not None) else at
        _hc = _hang_char_ta(state)   # LOAN DAU doi phe -> char ta khong con o hang 3
        if _claim_support_action(
            state, "heal_hp", (_hc, _ht), state.label + ":char",
            state.char.sp, _heal_decide,
        ):
            return Decision(config.UNIT_CHAR, at, _ht, config.SKILL_HEAL_ALL, b=_hc)
    # HOI SP TOAN TEAM (chi quest_mode): sau heal HP, truoc tan cong
    spr = _try_sp_restore(state, config.UNIT_CHAR, state.skills_char, state.char)
    if spr is not None:
        return spr
    br = _try_break_enemy_protect(state, config.UNIT_CHAR, state.skills_char, state.char, options)
    if br is not None:
        return br
    if _is_mineral_battle(state):
        # BO CHAY nham CHINH MINH -> hang phai la hang cua MINH (loan dau = 0, khong phai 3).
        return Decision(config.UNIT_CHAR, at, at, config.SKILL_FLEE, b=_hang_char_ta(state))
    return _combat_attack(state, config.UNIT_CHAR, state.skills_char, state.char, options,
                          "char_spam", config.CHAR_FIRE_MIN_SP)


def decide_pet(state, options, first_turn=False):
    at = state.my_atype
    _register_current_unit(state, config.UNIT_PET, state.pet_skills)
    custom = _custom_decision(state, config.UNIT_PET, "pet", state.pet_skills, state.pet, options)
    if custom is _CUSTOM_AUTO:
        pass
    elif custom is not None:
        return custom
    # HOI SINH (truoc heal): co dong doi chet + pet co skill hoi sinh + thang dieu phoi
    rv = _try_revive(state, config.UNIT_PET, state.pet_skills, state.pet, options)
    if rv is not None:
        return rv
    # CC ti le cao (quest only): sau Hoi Sinh, truoc buff bao ve.
    cc = _try_cc(state, config.UNIT_PET, state.pet_skills, state.pet, options, "high")
    if cc is not None:
        return cc
    # BUFF BAO VE (quest/boss): Ket Gioi -> An Than -> Kinh, truoc heal HP/SP.
    prot = _try_protect(state, config.UNIT_PET, state.pet_skills, state.pet)
    if prot is not None:
        return prot
    # CC ti le thap / Hon Loan (quest only): sau buff bao ve.
    cc = _try_cc(state, config.UNIT_PET, state.pet_skills, state.pet, options, "low")
    if cc is not None:
        return cc
    # HOI MAU: pet co skill heal + dong doi yeu + du SP + la con SP cao nhat
    if (state.any_ally_low(config.HEAL_HP_THRESHOLD)
            and state.pet.sp >= config.HEAL_SP_COST
            and config.SKILL_HEAL_ALL in state.pet_skills):
        _low = state.lowest_hp_ally()
        _ht = _low.slot if (_low is not None and getattr(_low, "slot", None) is not None) else at
        _hc = _hang_char_ta(state)   # LOAN DAU doi phe -> char ta khong con o hang 3
        if _claim_support_action(
            state, "heal_hp", (_hc, _ht), state.label + ":pet",
            state.pet.sp, _heal_decide,
        ):
            return Decision(config.UNIT_PET, at, _ht, config.SKILL_HEAL_ALL, b=_hc)
    # HOI SP TOAN TEAM (chi quest_mode): sau heal HP, truoc tan cong
    spr = _try_sp_restore(state, config.UNIT_PET, state.pet_skills, state.pet)
    if spr is not None:
        return spr
    br = _try_break_enemy_protect(state, config.UNIT_PET, state.pet_skills, state.pet, options)
    if br is not None:
        return br
    if _is_mineral_battle(state):
        return Decision(config.UNIT_PET, at, at, config.SKILL_FLEE, b=_hang_pet_ta(state))
    return _combat_attack(state, config.UNIT_PET, state.pet_skills, state.pet, options,
                          "pet_spam", config.PET_FIRE_MIN_SP)
