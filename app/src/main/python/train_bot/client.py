"""TCP client TS Online: ket noi, auth, heartbeat, recv loop, dispatch + combat."""
import functools
import socket
import struct
import threading
import time
import logging
import collections
import json
import os

from . import config, protocol, combat, pathfind, npc40, loandau, pet_login_stats, team_dungeon_lv110
from . import event_exchange as _evx
from .battle_tracker import BattleTracker
from .party_battle import get_party_battle


from .auth import (build_auth_packet, build_unbounded_auth_packet,
                   parse_bao_chuyen_vo_gioi, parse_ket_qua_login, parse_connect_code)
from .state import BattleState, Unit

log = logging.getLogger("bot")


# recv KHONG block vo han: server lom hay rot kieu "half-open" (khong gui RST/FIN) -> recv() block mai
# -> acc DUNG HINH, khong nhan goi, khong log, ca party cho vo han (bug that: sga019/lbo005 im tuyet
# doi @12003). Dat timeout de recv co co hoi thoat + kiem tra "bao lau khong nhan gi"; qua nguong ->
# coi nhu ROT -> supervisor relogin (day la DISCONNECT that, khong phai lech pha).
RECV_SOCK_TIMEOUT = 30.0    # giay: moi lan recv toi da block bao lau
RECV_DEAD_SECS = 120.0      # khong nhan GOI nao suot >120s -> coi server half-open (rot) -> relogin
                            # (heartbeat gui moi 15s -> server song gan nhu chac chan gui gi do <120s;
                            #  im hoan toan 120s = rot that. Nguong an toan tranh false-positive luc idle.)

def _open_game_socket(host, port):
    sock = socket.create_connection((host, port), timeout=15)
    sock.settimeout(RECV_SOCK_TIMEOUT)
    return sock


# ===== BAO CAO CONG VIEC MOI ACC (activity) =====
# De biet acc dang lam gi -> luc reform/barrier ket ("4/5, cho 1 thang khong ve") log ra duoc thang
# thieu DANG LAM GI + bao lau roi -> phan biet "dang lam viec khac chua xong" (vd danh boss QD, dungeon)
# vs "k/treo reform that". Moi acc cap nhat 1 chuoi ngan + timestamp; ai cung doc duoc.
_ACC_ACTIVITY = {}               # username -> (task_str, phase, ts_cap_nhat)
_ACC_ACTIVITY_LOCK = threading.Lock()

# Cac PHA (phase) de member so voi leader. Leader la nhac truong -> pha party = pha leader.
#   train        = dang train binh thuong
#   reform       = dang reform (ve thanh gom + lap lai party)
#   team_dungeon = dang o pha pho ban to doi (cho report luot / danh)
#   boss_qd      = dang danh boss Quan Doan   (WHITELIST: member cho la hop le)
#   login_chore  = viec vat sau login (world boss, daily dungeon solo, van tieu...)  (WHITELIST)
_PHASE_WHITELIST = ("boss_qd", "login_chore")   # leader o pha nay -> member CHO, khong bo viec

# Pha VIEC LE: acc lam mot minh, dong doi phai CHO chu khong duoc coi la "lech viec".
PHASE_LOGIN_CHORE = "login_chore"   # boss the gioi, van tieu, PB don, doi qua, thanh tuu...
PHASE_BOSS_QD = "boss_qd"
PHASE_TRAIN = "train"
PHASE_REFORM = "reform"
PHASE_TEAM_DUNGEON = "team_dungeon"
PHASE_IDLE = "idle"                 # vua xong 1 viec, chua sang viec moi
PHASE_DIGIOI = "digioi"             # dang trong Di Gioi (khac train thuong)
# DANG CHO DONG DOI - KHONG phai treo. Cho co the rat lau va van hoan toan binh thuong:
#   xong DG truoc, doi ca party xong (toi 2 TIENG); cho leader danh xong PB (10-20 phut).
# Chi la van de khi CA PARTY cung dang cho -> luc do moi la deadlock that.
PHASE_WAIT = "wait"

_ACC_TASK_SEQ = 0


def set_account_activity(username, task, phase="", _new_task=True):
    """Bao "dang lam gi". _new_task=False = chi cap nhat tien do cua viec DANG lam (giu `start`)."""
    global _ACC_TASK_SEQ
    if not username:
        return
    now = time.time()
    with _ACC_ACTIVITY_LOCK:
        old = _ACC_ACTIVITY.get(username)
        same = (old is not None and not _new_task
                and old.get("task") == task and old.get("phase") == phase)
        if same:
            old["update"] = now
            return
        _ACC_TASK_SEQ += 1
        _ACC_ACTIVITY[username] = {"task": task, "phase": phase, "start": now,
                                   "update": now, "seq": _ACC_TASK_SEQ, "done": False}


def task_heartbeat(username, task=None):
    """Viec dang chay: cap nhat `update` (KHONG doi `start`). Dung trong vong lap lau."""
    if not username:
        return
    with _ACC_ACTIVITY_LOCK:
        v = _ACC_ACTIVITY.get(username)
        if v is not None:
            v["update"] = time.time()
            if task:
                v["task"] = task


def mark_account_task_done(username, task=""):
    """Danh dau XONG viec hien tai -> dong doi biet no da xong chu khong phai treo."""
    if not username:
        return
    with _ACC_ACTIVITY_LOCK:
        v = _ACC_ACTIVITY.get(username)
        if v is None:
            return
        v["done"] = True
        v["phase"] = PHASE_IDLE
        v["update"] = time.time()
        v["done_at"] = v["update"]   # XONG luc nao. KHONG dung `update` de do: `update` bi
        # task_heartbeat (vong RECV) dap lai moi 5s khi co goi ve -> no chi chung minh SOCKET con
        # song, khong chung minh luong con TIEN. Acc xong viec roi ket cung van "tre 1-6s".
        v["task"] = "xong: %s" % (task or v.get("task") or "")


def get_account_activity(username):
    """Tra (task, phase, so_giay_ke_tu_cap_nhat) - GIU nguyen dang cu cho code dang dung."""
    with _ACC_ACTIVITY_LOCK:
        v = _ACC_ACTIVITY.get(username)
    return (v["task"], v["phase"], time.time() - v["update"]) if v else None


def get_account_task(username):
    """Ban ghi DAY DU cho watcher: {task, phase, start, update, seq, done, age, elapsed}."""
    with _ACC_ACTIVITY_LOCK:
        v = _ACC_ACTIVITY.get(username)
        if v is None:
            return None
        now = time.time()
        d = dict(v)
    d["age"] = now - d["update"]        # bao lau khong cap nhat (nghi treo)
    d["elapsed"] = now - d["start"]     # viec nay da chay bao lau (viec lau la binh thuong)
    return d


class account_task:
    """Bao viec + TU DANH DAU XONG khi thoat (ke ca khi loi).

    with account_task(user, "boss the gioi", PHASE_LOGIN_CHORE):
        ...
    """

    def __init__(self, username, task, phase="", client=None):
        self.username, self.task, self.phase = username, task, phase
        self.client = client        # co thi `__exit__` nha loi moi party da giu (xem duoi)

    def __enter__(self):
        set_account_activity(self.username, self.task, self.phase)
        return self

    def beat(self, task=None):
        task_heartbeat(self.username, task)

    def __exit__(self, et, ev, tb):
        mark_account_task_done(self.username, self.task)
        # XONG VIEC VAT -> XU LY LOI MOI PARTY DA GIU LAI.
        #
        # Acc dang lam viec vat thi GIU loi moi chu khong nhan (nhan la bo do viec vat - user
        # 14/09: "dang lam do ma vao pt roi bi keo di luon thi no hong viec vat"). Nhung phai co
        # cho NHA RA, va cho do phai la noi KHONG THE QUEN: day - `__exit__` chay ke ca khi viec
        # vat nem loi hay bi abort.
        #
        # Truoc day viec nha ra do co `party_invite_ready` - mot co GAN TAY o vai nhanh cu the.
        # Acc bi ABORT giua chung khong bao gio toi dong gan co -> co ket False VINH VIEN -> giu
        # moi loi moi -> party khong bao gio du. Da de ra BON duong lach co do (accept ngay du dang
        # viec vat / ep bat co khi co lenh moi/gom/dong bo...), va chinh cac duong lach do lai pha
        # viec vat. Sua goc: co tu nha o cho viec vat KET THUC.
        try:
            _c = self.client
            if _c is not None and getattr(_c, "_pending_party_invites", None):
                _c.set_party_invite_ready(True)
        except Exception:
            pass
        return False


# ===== LY DO SERVER NGAT KET NOI: S:000-000 <斷線> +斷線原因(1) (protocal.lua:32) =====
# Truoc day bot KHONG doc goi nay -> 1574 lan dut trong 1 phien ma chi biet "Server dong ket noi",
# khong biet vi sao. Doc ra moi thay 78% la ma 90 = DANG NHAP QUA THUONG XUYEN (server chan toc do).
DISCONNECT_CAUSE = {
    1: "goi du lieu qua nhieu", 2: "sai cau hoi 3 lan", 3: "login sai 3 lan",
    4: "bi server kick", 5: "su kien vi pham", 6: "su kien vi pham",
    7: "khong tim thay su kien", 8: "trigger ngoai du kien", 9: "sai phien ban bang su kien",
    10: "loi tra bang", 11: "di vao diem chuong ngai", 12: "bam min khong hop le",
    13: "gui goi lien tuc qua nhanh", 14: "di chuyen QUA XA",
    15: "xoa thanh cong, khoi dong lai", 16: "IP dang nhap khong hop le",
    17: "sai phien ban, can cap nhat", 18: "sua du lieu", 19: "DANG NHAP TRUNG LAP",
    20: "server bat thuong", 21: "loi thong tin file save", 22: "sai dinh dang goi",
    23: "doi ten", 24: "mat khau qua ngan", 25: "ten trung",
    26: "su kien vi pham", 27: "loi dang nhap", 28: "phong ve",
    29: "du lieu don qua nhieu", 30: "khoa tai khoan", 31: "ID chua duoc mo",
    32: "chien dau lien scene", 33: "scene khong khop moc", 34: "trung lap lien server",
    35: "gui goi dang nhap lien tuc", 36: "ID ngoai pham vi", 37: "khac scene",
    38: "scene dich khong khop", 40: "sua goi hop thanh",
    60: "SERVER TAT MAY BAO TRI", 61: "thong bao rieng cua server",
    90: "DANG NHAP QUA THUONG XUYEN (server chan toc do)",
}
DISCONNECT_RATE_LIMIT = 90     # ma 90: login lai ngay lap tuc chi lam server chan tiep
# Cho them toi da bao lau cho moc KET TRAN THAT (`0x14 sub0700`) truoc khi cho caller di chuyen.
# Xem `_wait_combat_clear`: di khi server con dang giai tran = `di chuyen QUA XA (ma 14)`.
WAIT_END_THAT_SEC = 8.0

# 4 map PHO BAN TO DOI (instance). Da kiem chung deu co trong Ground.mmg (xem _td_walk).
# Dung de biet acc DANG O TRONG pho ban: trong do khong teleport/ve thanh duoc, va "ca party
# cung cho nhau" la BINH THUONG (member cho leader danh) chu khong phai deadlock.
TEAM_DUNGEON_MAPS = frozenset({62002, 62011, 62012, 62013})   # 4 PB bot co danh (lv20/50/80/110)

# CA DAI 62xxx la khong gian PHO BAN/INSTANCE (20 map: Thao Phat Thien Su, Dai Chien Lu Bo, San Kho
# Bau, Bach Chien...). Da kiem: KHONG map 62xxx nao la map train hay thanh.
# Dung cho cau hoi "minh co DANG O TRONG instance khong" - RONG hon TEAM_DUNGEON_MAPS co y:
# lay 4 map kia de tra loi cau nay thi acc lac vao instance KHAC se bi tra "dang o ngoai" -> caller
# bo luon duong relogin du phong -> KET TRONG PHO BAN VINH VIEN.
_INSTANCE_MAP_LO, _INSTANCE_MAP_HI = 62000, 63000


def in_instance_map(map_id) -> bool:
    try:
        return _INSTANCE_MAP_LO <= int(map_id) < _INSTANCE_MAP_HI
    except (TypeError, ValueError):
        return False

def task_report(task, phase=""):
    """Decorator cho method cua GameClient: tu bao viec + danh dau xong. Khong the quen."""
    def deco(fn):
        @functools.wraps(fn)
        def wrap(self, *a, **k):
            with account_task(getattr(self, "_username", ""), task, phase, client=self):
                return fn(self, *a, **k)
        return wrap
    return deco


# ===== UU TIEN THREAD KHI DANH PHO BAN TO DOI (PB) =====
# 15 party x 5 acc = ~75 client / 1 tien trinh Python -> GIL: moi luc chi 1 thread chay bytecode.
# Khi nhieu party cung chay, thread cua acc DANG DANH PB bi tranh CPU -> heartbeat/lenh danh tre ->
# server da / lech phien -> PB vo. Giai phap: nang priority OS cho thread acc DANG trong PB (recv +
# heartbeat), va HA priority thread acc dang TRAIN khi co party khac dang danh PB (dua GIL cho PB).
# Best-effort: sai moi truong / thieu quyen -> bo qua an toan (Windows chac chan chay; Android nang
# can quyen nhung HA train thi khong -> van co tac dung tuong doi).
_TD_ACTIVE = 0                    # so client dang trong pho ban to doi (leader + member)
_TD_LOCK = threading.Lock()

def _td_active_inc(delta: int) -> int:
    global _TD_ACTIVE
    with _TD_LOCK:
        _TD_ACTIVE = max(0, _TD_ACTIVE + delta)
        return _TD_ACTIVE

_WIN_K32 = None
def _win_kernel32():
    """kernel32 da cau hinh argtypes/restype (BAT BUOC: pseudo-handle GetCurrentThread = 0xFFFF...FFFE,
    thieu c_void_p thi ctypes truncate HANDLE 64-bit -> SetThreadPriority FAIL am tham)."""
    global _WIN_K32
    if _WIN_K32 is None:
        import ctypes
        k = ctypes.windll.kernel32
        k.GetCurrentThread.restype = ctypes.c_void_p
        k.SetThreadPriority.argtypes = [ctypes.c_void_p, ctypes.c_int]
        k.SetThreadPriority.restype = ctypes.c_int
        _WIN_K32 = k
    return _WIN_K32

def _set_thread_prio(level: int):
    """level: 1=cao (dang danh PB), 0=thuong, -1=thap (train khi co party khac danh PB)."""
    try:
        if os.name == "nt":
            k = _win_kernel32()
            # THREAD_PRIORITY: ABOVE_NORMAL=1, NORMAL=0, BELOW_NORMAL=-1
            k.SetThreadPriority(k.GetCurrentThread(), {1: 1, 0: 0, -1: -1}[level])
        else:
            # *nix/Android: nang (-2) can quyen -> co the fail (bo qua); ha (+5) khong can quyen.
            os.setpriority(os.PRIO_PROCESS, threading.get_native_id(), {1: -2, 0: 0, -1: 5}[level])
    except Exception:
        pass


# NGOC PHUC THAN - DU 4 LOAI, xep TOT -> KEM theo he so kinh nghiem (items_desc.json).
# Ca 4 deu `ft=6` (EQUIP_POS_SPEC), cap yeu cau 15 -> dung mot o, deo duoc nhu nhau.
#   0x5AAC Ngoc Ba Phuc Than    x3
#   0x5AAB Ngoc Sieu Phuc Than  x2,5
#   0x5A2D Ngoc Dai Phuc Than   x2
#   0x59EF Ngoc Tieu Phuc Than  x1,5
# Truoc day bot chi biet Sieu + Dai -> co Ba (xin nhat) van deo Sieu, va chi con Tieu thi khong
# deo gi ca ma nhay thang sang item tieu hao (user phat hien 31/08).
PHUC_THAN_GEM_ORDER = (0x5AAC, 0x5AAB, 0x5A2D, 0x59EF)
PHUC_THAN_PROTECTION_PRIORITY = (
    tuple((_tid, "equip") for _tid in PHUC_THAN_GEM_ORDER) + ((0xB5F4, "use"),)
)
PHUC_THAN_GEM_TIDS = set(PHUC_THAN_GEM_ORDER)


def phuc_than_hang(tid) -> int:
    """Thu hang ngoc Phuc Than: 0 = tot nhat. Khong phai ngoc -> so RAT LON (coi nhu khong co).

    Dung de so sanh "co dang deo cai TOT HON khong" thay vi hardcode tung tid."""
    try:
        return PHUC_THAN_GEM_ORDER.index(int(tid))
    except (ValueError, TypeError):
        return 999
BROKEN_PHUC_THAN_TID = 0x59F0
# EItemFitType.Equip_Spec = 6 (ItemData.lua) - O DO cua NGOC. Ca 3 tid ngoc (Sieu/Dai/Ngoc Hu)
# deu fitType=6 (xac nhan tu gamedata_Item.dat) nen ngoc LUON nam o vi tri nay.
# Client suy vi tri tu fitType cua item (S:023-011 khong gui vi tri) - bot chi can ngoc CHAR nen
# hardcode 6 la du (ngoc Phuc Than KHONG deo cho pet duoc).
EQUIP_POS_SPEC = 6
# Nguong con lai de dung item Phuc Than (client goc: godMission < 1; user chon < 5) va so item
# toi da moi luot.
PHUC_THAN_LOW = 5
PHUC_THAN_USE_MAX = 10
# THU TU DUNG item tieu hao: manh truoc (Dai Phuc Than > Phuc Than) -> dung IT item hon cho cung
# so luot buff. Item khong co trong bang nay xep sau cung. (KHONG sap theo "qty" trong
# use_items.json: qty la so dung/luot, Phuc Than = 50 > Dai = 25 -> sap theo do la chon nham
# loai YEU truoc.)
PHUC_THAN_CONSUMABLE_ORDER = (0xB3D6, 0xB3D5)
# `S:007-002 <換分區結果> +結果(1)` - chep DUNG tu protocal.lua:1091. LUU Y: 0 moi la THANH CONG
# (client: nhanh `else` -> dong UI); 1..4 deu la THONG BAO LOI (ShowCenterMessage).
#   1 不可換到同一區   3 組隊不可換分區
#   2 無指定區可換     4 人數已滿
CHANNEL_SWITCH_ERRORS = {
    1: "khong doi sang CUNG khu duoc (tuc la dang o san khu do)",
    2: "khong co khu do de doi",
    3: "DANG TO DOI thi khong doi khu duoc",
    4: "khu da day nguoi",
}
TEAM_DUNGEONS = {
    20: {"id": 0x0001, "daily_flag": 0x302E, "daily_count": 1},
    50: {"id": 0x000E, "daily_flag": 0x30A6, "daily_count": 1},
    80: {"id": 0x000F, "daily_flag": 0x30AA, "daily_count": 1},
    110: {"id": 0x0010, "daily_flag": 0x30AE, "daily_count": 1},
}
# PHO BAN DON (solo, o 1 bingo) - boc Dungeon_C.dat 15/09 (8 ban ghi x 42B = 340B, khop tuyet doi).
# Bon ban ghi kind=2/maxPlayer=1/scene=62001, KHAC NHAU chi o dai level -> `id` chinh la cai
# `_dungeon_tier()` van tra ve. CA BON dung CHUNG mot `daily_flag` va moi ngay chi 1 luot free;
# o 1 bingo doi 2 luot nen luot thu hai BUOC phai mua (khong phai loi). Xem KNOWLEDGE.md.
SOLO_DUNGEON_IDS = ((80, 2), (150, 3), (200, 4), (450, 5))   # (level toi da, dungeonId)
SOLO_DUNGEON_DAILY_FLAG = 0x3030
SOLO_DUNGEON_DAILY_COUNT = 1
ONLINE_GIFT_KIND = 0x03
ONLINE_GIFT_ROLECOUNT = 10
WORLD_BOSS_MISSION_ID = 12207
WORLD_BOSS_MAX_ATTEMPTS = 5
WORLD_BOSS_CHALLENGE_TIDS = (0xB625,)
VANTIEU_CLAIM_RESULT_TEXT = {
    1: "thanh cong",
    2: "loai van tieu sai",
    3: "chi so pet/slot sai",
    4: "chua hoan thanh",
    5: "tui do day",
    6: "vat pham/function dong",
}
ONLINE_GIFT_DEFAULT_FLAGS = {
    10: 2,
    20: 3,
    30: 4,
    60: 5,
    90: 6,
    180: 7,
}

_GROUND_STORE = None
_GROUND_STORE_PATH = None
_GROUND_STORE_FAILED = False
_SMART_ROUTER = None
_SMART_ROUTER_KEY = None
_SMART_ROUTER_FAILED = False
_FORCE_WALK_SEA_GATES = {
    # Gate center nam tren o sea nhung day la cong script/di bo, khong len thuyen.
    (23521, 23000, 2),
}


def _ground_store():
    global _GROUND_STORE, _GROUND_STORE_PATH, _GROUND_STORE_FAILED
    path = getattr(config, "GROUND_MAP_PATH", "")
    if not getattr(config, "SMART_PATHFIND", True) or not path:
        return None
    if _GROUND_STORE is not None and _GROUND_STORE_PATH == path:
        return _GROUND_STORE
    if _GROUND_STORE_FAILED and _GROUND_STORE_PATH == path:
        return None
    try:
        _GROUND_STORE = pathfind.GroundMapStore(path)
        _GROUND_STORE_PATH = path
        _GROUND_STORE_FAILED = False
        log.info("Smart path: da nap %d map tu %s", len(_GROUND_STORE.index), path)
    except (OSError, ValueError, struct.error) as exc:
        _GROUND_STORE_PATH = path
        _GROUND_STORE_FAILED = True
        log.info("Smart path: khong nap duoc Ground.mmg (%s), dung navigate cu", exc)
    return _GROUND_STORE


def _smart_world_router():
    global _SMART_ROUTER, _SMART_ROUTER_KEY, _SMART_ROUTER_FAILED
    if not getattr(config, "SMART_WORLD_ROUTING", True):
        return None
    nav_path = getattr(config, "WORLD_NAV_PATH", "")
    cache_path = getattr(config, "SMART_ROUTE_CACHE_PATH", "")
    ground = _ground_store()
    key = (nav_path, cache_path, id(ground))
    if not nav_path or not cache_path or ground is None:
        return None
    if _SMART_ROUTER is not None and _SMART_ROUTER_KEY == key:
        return _SMART_ROUTER
    if _SMART_ROUTER_FAILED and _SMART_ROUTER_KEY == key:
        return None
    try:
        from .smart_route import SmartRouteCache, SmartWorldRouter
        from .world_nav import WorldNavStore

        nav = WorldNavStore(nav_path)
        _SMART_ROUTER = SmartWorldRouter(
            nav, ground, SmartRouteCache(cache_path)
        )
        _SMART_ROUTER_KEY = key
        _SMART_ROUTER_FAILED = False
        log.info("Smart world route: da nap %d canh tu %s",
                 len(nav.data["edges"]), nav_path)
    except (OSError, ValueError, KeyError, struct.error) as exc:
        _SMART_ROUTER = None
        _SMART_ROUTER_KEY = key
        _SMART_ROUTER_FAILED = True
        log.warning("Smart world route: khong nap duoc (%s)", exc)
    return _SMART_ROUTER


def _route_boat_state(route):
    """(needs_boat, first_sea, last_sea) cho 1 route: leg nao o NUOC (is_sea) -> can thuyen."""
    needs_boat = False; first_sea = -1; last_sea = -1
    try:
        _gs = _ground_store()
        if _gs is not None:
            for _j, lg in enumerate(route["legs"]):
                key = (
                    int(lg["scene"]),
                    int(lg["target_scene"]),
                    int(lg["gate"]),
                )
                if key in _FORCE_WALK_SEA_GATES:
                    continue
                if _gs.is_sea_world(lg["scene"], tuple(lg["gate_center"])):
                    needs_boat = True
                    if first_sea < 0:
                        first_sea = _j
                    last_sea = _j
    except Exception:
        return False, -1, -1
    return needs_boat, first_sea, last_sea


# So lan toi da PLAN LAI khi cong ra map ngoai du kien (vd cong 11 map 11000 random -> 55000 HOAC
# 58000). Moi lan random ~50% ra dung -> 16 lan gan nhu chac chan qua.
_MAX_ROUTE_REPLANS = 16

def execute_smart_route(client, route, abort=None, flee=True):
    """Execute a built route. Cong RA MAP NGOAI DU KIEN (cong random nhieu dich / bi phuc kich day
    lech map) -> KHONG bo cuoc: doc map thuc te roi PLAN LAI duong con lai toi dich va di tiep."""
    client._smart_route_failure = None
    dest_map = int(route["dest_map"])
    safe = route.get("safe")
    replans = 0
    while True:
        # Route co cong GIUA BIEN (o nuoc)? -> phai LEN THUYEN tai cong DAU (ben) truoc, khong thi
        # cac cong bien sau bi kick (di bo nhay cong bien). Xem capture thuyen_thanhchau.
        needs_boat, first_sea, last_sea = _route_boat_state(route)
        # LEN THUYEN o cong VAO scene bien DAU TIEN (ben = leg first_sea-1). SAIL cac leg bien
        # (first_sea..last_sea). Cong thuc nay tong quat: bien dau route (first_sea=1) -> board_leg=0;
        # bien giua route (vd 12061->18001, bien la leg 2,3) -> board leg 1 = ben, sail 2,3.
        board_leg = max(0, first_sea - 1) if needs_boat else -1
        replanned = False
        for _i, leg in enumerate(route["legs"]):
            if not client.running or (abort and abort()):
                client._smart_route_failure = "aborted"
                return False
            if client.current_map != leg["scene"]:
                # Loi thoat IM LANG truoc day: dung giua duong ma khong mot dong log -> caller chi
                # thay "that bai (map=X)". Neu roi vao day nghia la route va map thuc te da lech
                # nhau TRUOC KHI di chang nay (bi keo/day sang map khac).
                log.warning("[%s] scene route DUNG o chang %d: dang o map %s nhung chang nay xuat "
                            "phat tu %s", client._label, _i, client.current_map, leg["scene"])
                client._smart_route_failure = "unexpected_scene"
                return False
            # TIM DUONG khong phan biet bo/bien - y het client (xem pathfind._blocked). `sailing`
            # chi con de bao _enter_gate biet minh DANG tren thuyen (anh huong chuoi goi qua cong),
            # KHONG dung de tim duong nua.
            sailing = needs_boat and first_sea <= _i <= last_sea
            client.navigate_to(*leg["gate_center"], abort=abort, flee=flee)
            if not client.running or (abort and abort()):
                client._smart_route_failure = "aborted"
                return False
            # _in_scene_gate: trong luc qua cong, moi acc danh tran phuc kich RIENG -> in_combat()
            # KHONG duoc ha in_battle theo member-confirm (member khac xong tran cong khac -> ha oan
            # -> gui 0x14 06 transit luc server con xu battle -> KICK). Chi tin tran cua CHINH minh.
            client._in_scene_gate = True
            try:
                _gate_ok = client._enter_gate(*leg["gate_center"], leg["gate"],
                                              expected_map=leg["target_scene"],
                                              board_boat=(needs_boat and _i == board_leg),
                                              on_boat=sailing)
            finally:
                client._in_scene_gate = False
            if not _gate_ok:
                client._smart_route_failure = "gate_failed"
                return False
            if client.current_map != leg["target_scene"]:
                # CONG RA MAP NGOAI DU KIEN. Vd cong 11 @map 11000 co 2 dich: 55000 HOAC 58000
                # (deu hop le, server random). Cung xay ra khi phuc kich/day lech map.
                if client.current_map == dest_map:
                    break   # da toi dich som -> ra xu ly safe o duoi
                if replans >= _MAX_ROUTE_REPLANS:
                    client._smart_route_failure = "unexpected_scene"
                    return False
                # cho co toa do moi (0x03) truoc khi plan lai tu map thuc te
                _t0 = time.time()
                while client.pos is None and client.running and time.time() - _t0 < 8.0:
                    if abort and abort():
                        client._smart_route_failure = "aborted"
                        return False
                    time.sleep(0.2)
                new_route = None
                try:
                    new_route = client.build_smart_scene_route(
                        client.current_map, dest_map, safe if safe else None)
                except Exception:
                    new_route = None
                if not new_route or not new_route.get("legs"):
                    client._smart_route_failure = "unexpected_scene"
                    return False
                log.info("[%s] cong idx=%d ra map %s (khac du kien %s) -> plan lai duong con lai "
                         "toi %s (lan %d)", client._label, leg["gate"], client.current_map,
                         leg["target_scene"], dest_map, replans + 1)
                route = new_route
                replans += 1
                replanned = True
                break
            if client.pos is None and leg.get("target_arrival"):
                client.pos = tuple(leg["target_arrival"])
            # SAU KHI QUA CONG (nhat la cong co quai phuc kich -> battle): char o trang thai 'chua san
            # sang' tren map moi -> move bi server nuot. Gui lai 0x41 de di chuyen duoc o leg sau.
            # KHONG rearm khi vua LEN THUYEN (board_leg) hoac dang SAIL -> 0x41 lam ROT THUYEN.
            boat_leg = needs_boat and (_i == board_leg or first_sea <= _i <= last_sea)
            if not boat_leg:
                try:
                    client.rearm_ready()
                except Exception:
                    pass
        if replanned:
            continue   # plan lai -> chay lai vong voi route moi tu map hien tai
        break

    if not client.running or (abort and abort()):
        client._smart_route_failure = "aborted"
        return False
    if safe is not None:
        client.navigate_to(*safe, abort=abort, flee=flee)
        if not client.running or (abort and abort()):
            client._smart_route_failure = "aborted"
            return False
    if client.current_map != dest_map:
        client._smart_route_failure = "unexpected_scene"
        return False
    return True

# Registry entity cac bot cung party (chia se trong process). party_idx -> set(entity bytes).
# Bot dang ky self_entity luc login -> khi nhan loi moi, accept neu nguoi moi cung party.
# (map, cong) -> {"try": da do bao nhieu ma, "ok": ma DUNG neu da tim ra}.
# De o cap MODULE chu khong o client: chon sai ma thi server NGAT KET NOI, client bi tao lai ->
# dem o client se reset ve 0 moi lan, do mai mot ma dau tien va ket vinh vien (log 17:44).
_GATE_CHOICE_STATE = {}

def thing_data_info(raw: bytes) -> dict:
    """Doc ThingData (35 byte) = du lieu cua MON CU THE trong tui, khac han ban mau item.

    Bo cuc lay tu Logic_Item.lua ThingData.New (KHONG doan):
      +0  Id(2) | +2 quant(4) | +6 damage(1) | +7 element(1) | +8 elementValue(1)
      +9  proofKind(1) | +10 growLv(1) | +11 growExp(4) | +15 specialKind(1)
      +16 stoneAttr(1) | +17 stoneLv(1) | +18 enhanceLv(1) | +19 delTime(double 8)
      +27 damagedItemId(2) | +29 isLock(1) | +30 Reinforced(1) = CAP CUONG HOA
      +31 affix1 | +32 affix2 | +33 affix3 = DONG PHU | +34 styleLv
    Tong 35 -> khop. 5 moc bot da dung tu truoc (7, 8, 16, 17, 27) deu trung -> tin duoc.
    """
    if len(raw) < 35:
        return {}
    return {
        "damage": raw[6], "element": raw[7], "element_value": raw[8],
        "proof": raw[9], "grow_lv": raw[10],
        "stone_attr": raw[16], "stone_lv": raw[17], "enhance_lv": raw[18],
        "lock": bool(raw[29]), "reinforced": raw[30],
        "affix": [raw[31], raw[32], raw[33]], "style_lv": raw[34],
    }


_PARTY_ENTITIES = {}
_PARTY_CLIENTS = {}
_PARTY_LOCK = threading.Lock()

def _register_party_entity(party_idx, entity):
    if party_idx is None or not entity:
        return
    with _PARTY_LOCK:
        _PARTY_ENTITIES.setdefault(party_idx, set()).add(bytes(entity))

def _register_party_client(party_idx, entity, client):
    """Dang ky client theo entity, VA DON entity CU cua chinh acc nay.

    Entity DOI MOI LAN LOGIN. `_PARTY_ENTITIES` truoc day chi duoc THEM, khong bao gio xoa - nen sau
    moi lan acc relogin, leader lai co them mot entity CHET trong danh sach moi. No moi mai vao cai
    entity do, server khong he thay ai nhu vay quanh no, va doi khong bao gio du.

    Ca that 07/09 party 3 (16:46 -> 16:54, dieu phoi ra lenh lap lai party moi 2 phut ma vo ich):
        [nanam] (LEADER) moi 1 member theo entity (live dung map/kenh): ['4ef7d7f8']
        [nanam] (LEADER) moi 1 nguoi ma SERVER CHUA HE cho thay ho quanh minh:
                ['4ef7d7f8:server CHUA HE bao thay nguoi nay quanh minh (chua co 0x03)']
        [party 3] DIEU PHOI: ... DOI chua du (sga005=3 sga007=0 sga008=3 sga009=3 sga010=3)
    `4ef7d7f8` la entity tu 12:34 cua mot phien truoc; sga007 (baybay) da relogin luc 16:44 va mang
    entity khac. Roster cua no dung `0` suot.

    Nhan dien "cung mot acc" bang `_username` - khoa ON DINH qua cac lan login (entity thi khong)."""
    if party_idx is None or not entity or client is None:
        return
    entity = bytes(entity)
    _u = getattr(client, "_username", None)
    with _PARTY_LOCK:
        _map = _PARTY_CLIENTS.setdefault(party_idx, {})
        _cu = [e for e, cc in _map.items()
               if e != entity and (cc is client or (_u and getattr(cc, "_username", None) == _u))]
        for e in _cu:
            _map.pop(e, None)
            _PARTY_ENTITIES.get(party_idx, set()).discard(e)
            _PARTY_JOINED.get(party_idx, set()).discard(e)
            _dong_bo_joined_ro(party_idx)
        _map[entity] = client
    if _cu:
        log.info("[%s] entity moi %s -> bo %d entity CU (%s) khoi danh sach party",
                 getattr(client, "_label", _u or "?"), entity.hex()[:8], len(_cu),
                 ", ".join(e.hex()[:8] for e in _cu))

def _is_party_member(party_idx, entity):
    # DOC KHONG KHOA - xem `is_joined`. `in` tren set khong duyet o muc Python nen an toan duoi
    # GIL ke ca khi luong khac dang `add`.
    if party_idx is None:
        return False
    return bytes(entity) in _PARTY_ENTITIES.get(party_idx, set())

# Member da ACCEPT loi moi tu party-mate (tin hieu chia se de LEADER biet party da thanh).
# party_idx -> set(self_entity cua cac member da join). Tin cay hon doc roster broadcast.
_PARTY_JOINED = {}
# BAN CHUP CHI-DOC cua `_PARTY_JOINED`: party_idx -> frozenset. Duong doc (`is_joined`,
# `joined_member_count`) dung ban nay va KHONG can `_PARTY_LOCK` - xem `is_joined` de biet vi sao.
# Moi lan doi `_PARTY_JOINED` phai goi `_dong_bo_joined_ro(party_idx)` NGAY, trong cung khoi khoa.
_PARTY_JOINED_RO = {}


def _dong_bo_joined_ro(party_idx):
    """Cap nhat ban chup chi-doc. GOI TRONG `_PARTY_LOCK`, ngay sau moi lan sua `_PARTY_JOINED`."""
    cur = _PARTY_JOINED.get(party_idx)
    if cur is None:
        _PARTY_JOINED_RO.pop(party_idx, None)
    else:
        _PARTY_JOINED_RO[party_idx] = frozenset(cur)

# PHA PHO BAN TO DOI cua tung party: party_idx -> True/False. MOT CHO GHI (dieu phoi), moi acc DOC.
#
# Truoc day moi acc tu om `self._phoban_until = time.time() + 600` luc accept loi moi PB, roi
# `go_to_town()` BAIL khi co con han. PB vo giua chung thi khong ai ha co: leader ha cua rieng no,
# member nao da roi vong cho `o5_state` thi giu du 10 phut. Ca that 07/09 - 651 dong
# "dang vao pho ban -> ngung teleport": p53 leader thoat PB luc 02:40:33 ma qv813/814/815 con spam
# toi 02:45:41 roi bi relogin hang loat; p51 (mh212) ket y het.
#
# User chot 07/09: "clear voi giu cai lon gi nua, bot dieu phoi het di". Dung: ca party chay trong
# MOT tien trinh, dieu phoi BIET pha PB dang chay hay khong - khong can timer tu het han, khong can
# di ha co tung acc, va khong acc nao phai cho acc khac bao cao (L2).
_PARTY_PB_PHA = {}


def dat_pha_pho_ban(party_idx, dang_chay):
    """DIEU PHOI bat/tat pha pho ban to doi cua ca party. Chi dieu phoi duoc goi (L1)."""
    if party_idx is None:
        return
    with _PARTY_LOCK:
        if dang_chay:
            _PARTY_PB_PHA[party_idx] = True
        else:
            _PARTY_PB_PHA.pop(party_idx, None)


# PARTY DANG GOM: party_idx -> True/False. MOT CHO GHI (dieu phoi), moi acc DOC.
# Trong luc gom thi VIEC VAT (cat tien trang, ban Noi Dat...) phai nhuong - do la L0: du party roi
# lam gi thi lam. Viec vat keo acc sang map khac, ma gom la chuyen ai cung phai o mot cho.
_PARTY_GOM = {}
# DIEU PHOI giao viec "keo ca party di duong" cho DUNG MOT acc. Moi duong di ra bai train deu bat
# dau bang teleport, ma `Teleport: dang o to doi -> ROI DOI truoc (client chan tele khi con doi)` -
# nen hai acc cung tu di la party tan ngay. Party 6, 11/09: leader qua cong idx=2 luc 09:49:05, ba
# giay sau bon member tu di route -> moi dua tu roi doi -> roster 4 -> 3 -> 2 -> 0.
_PARTY_NGUOI_KEO = {}


def dat_party_dang_gom(party_idx, dang_gom):
    """DIEU PHOI bat/tat co "party dang gom". Chi dieu phoi duoc goi (L1)."""
    if party_idx is None:
        return
    with _PARTY_LOCK:
        if dang_gom:
            _PARTY_GOM[party_idx] = True
        else:
            _PARTY_GOM.pop(party_idx, None)


def party_dang_gom(party_idx):
    """Party nay co dang gom/thieu doi khong - moi acc deu doc duoc, khong ai phai hoi.

    DOC KHONG KHOA - xem `is_joined`. Duong ghi chi THAY THE ca gia tri (set/pop mot key), khong
    sua tai cho, nen `.get()` duoi GIL luon tra ve mot trong hai gia tri hoan chinh. Gianh
    `_PARTY_LOCK` de doc mot o dict la vo ich, ma no la dung cai da lam MainThread ket cung.
    """
    if party_idx is None:
        return False
    return bool(_PARTY_GOM.get(party_idx))


def dat_nguoi_keo(party_idx, username):
    """DIEU PHOI chot AI la nguoi keo ca party di duong. Chi dieu phoi duoc goi (L1).

    `username` = acc duy nhat duoc phep tu di route ra bai; `None` = khong ai keo, acc nao cung tu
    di duoc (party solo / party khong co bot-leader).
    """
    if party_idx is None:
        return
    with _PARTY_LOCK:
        if username:
            _PARTY_NGUOI_KEO[party_idx] = username
        else:
            _PARTY_NGUOI_KEO.pop(party_idx, None)


def nguoi_keo(party_idx):
    """Acc nao dang duoc DIEU PHOI giao viec keo party di duong (None = khong ai).

    DOC KHONG KHOA - xem `is_joined`. Ham nay nam tren DUONG NONG: `teleport()` va
    `_switch_channel_locked()` goi no, tuc ca 250 acc deu di qua day lien tuc. Khoa o mot cho nhu
    the la tu tao ra mot hang doi cho ca he (user 13/09: "click doi party thay do rat lau" ->
    "con bi not responding luon").
    """
    if party_idx is None:
        return None
    return _PARTY_NGUOI_KEO.get(party_idx)


def dang_pha_pho_ban(party_idx):
    """Party nay co dang trong pha pho ban to doi khong (moi acc deu doc duoc, khong ai phai hoi).

    DOC KHONG KHOA - xem `is_joined`."""
    if party_idx is None:
        return False
    return bool(_PARTY_PB_PHA.get(party_idx))


def mark_joined(party_idx, entity):
    """Ghi nhan 1 member DA vao party. Cong khai (khong con `_`) vi coordinator can sua so dem khi
    ROSTER SERVER noi member da o trong doi ma so nho noi la chua - su that thuoc ve server."""
    if party_idx is None or not entity:
        return
    with _PARTY_LOCK:
        _PARTY_JOINED.setdefault(party_idx, set()).add(bytes(entity))
        _dong_bo_joined_ro(party_idx)

def joined_member_count(party_idx):
    # Doc ban chup - khong gianh khoa voi ca tram luong dang ghi (xem `is_joined`).
    return len(_PARTY_JOINED_RO.get(party_idx, ()))

# PHO BAN TO DOI: member da gui "CHUAN BI" (0x2f 0b00) that su - KHAC voi _PARTY_JOINED (party
# THUONG). Leader truoc day chi CHO CO DINH ready_wait giay roi START bat ke - neu member dang ban
# viec khac (dailies chua xong) chua kip nhan+chuan bi trong khung do -> leader START mot minh
# (dungeon tinh nhu da lam, mat luot cho ca party). Dung registry nay de leader POLL that su thay vi
# doan thoi gian co dinh.
_DUNGEON_READY = {}

def _mark_dungeon_ready(party_idx, entity):
    if party_idx is None or not entity:
        return
    with _PARTY_LOCK:
        _DUNGEON_READY.setdefault(party_idx, set()).add(bytes(entity))

def dungeon_ready_count(party_idx):
    # DOC KHONG KHOA - xem `is_joined`. `len()` khong duyet o muc Python.
    return len(_DUNGEON_READY.get(party_idx, set()))

def reset_dungeon_ready(party_idx):
    with _PARTY_LOCK:
        _DUNGEON_READY.pop(party_idx, None)


TEAM_DUNGEON_WHITELIST_READY_GRACE = 10.0


def _team_dungeon_can_start(ready_count, needed, elapsed, whitelist_count):
    if ready_count < needed:
        return False
    return not whitelist_count or elapsed >= TEAM_DUNGEON_WHITELIST_READY_GRACE

def reset_party_joined(party_idx):
    """Xoa danh sach member da join (khi leader GIAI TAN party de relogin) -> leader tinh lai tu
    dau, vong retry 60s se MOI LAI cho du member. Member se mark_joined lai khi accept loi moi moi."""
    if party_idx is None:
        return
    with _PARTY_LOCK:
        _PARTY_JOINED.pop(party_idx, None)
        _dong_bo_joined_ro(party_idx)
        # Xoa luon quyen ghi: giai tan xong thi roster CU cua member khong duoc coi la nguon nua,
        # va leader phai duoc ghi lai tu dau.
        _PARTY_JOINED_SRC.pop(party_idx, None)

# Ai vua ghi _PARTY_JOINED: party_idx -> (entity nguoi ghi, co phai LEADER khong, luc ghi)
_PARTY_JOINED_SRC = {}
# Trong khoang nay ke tu lan LEADER ghi gan nhat, MEMBER khong duoc ghi de (xem _sync_party_joined).
PARTY_JOINED_LEADER_UU_TIEN = 30.0


def _sync_party_joined(party_idx, leader, members, nguoi_ghi=None, label=""):
    """Dat _PARTY_JOINED theo ROSTER SERVER (0x0d sub06). Leader KHONG tinh la member.

    CHI LEADER DUOC GHI (khi leader cua party dang chay trong tien trinh nay).

    `0x0d sub06` phat cho MOI client trong party, ma ca 5 acc dung CHUNG mot dict global -> truoc
    day ai nhan roster sau cung thi GHI DE SACH. Mot member nhan roster tam thoi 1 nguoi (dung
    luc leader vua giai tan / dang lap lai) la xoa sach so dem ma leader vua dung dung -> leader
    dem thieu -> giai tan -> lap lai -> quay vong vo tan.
    Log that party 15 (27/08 08:48-09:00, 13 phut): 08:48:18 member bao "da vao party" ma
    08:48:35 leader van dem THIEU roi giai tan; sau do 446 luot moi, 3/4 member khong con nhan
    duoc goi moi nao.

    Neu KHONG co leader nao ghi trong `PARTY_JOINED_LEADER_UU_TIEN` giay (vd leader dang relogin,
    hoac party do NGUOI THAT lam chu) thi member duoc ghi - khong thi so dem dung yen mai.
    """
    if party_idx is None:
        return
    lead = bytes(leader) if leader else None
    la_leader = bool(nguoi_ghi and lead and bytes(nguoi_ghi) == lead)
    now = {bytes(m) for m in (members or []) if m and (lead is None or bytes(m) != lead)}
    with _PARTY_LOCK:
        src = _PARTY_JOINED_SRC.get(party_idx)
        if not la_leader and src and src[1] and time.time() - src[2] <= PARTY_JOINED_LEADER_UU_TIEN:
            _bo = _PARTY_JOINED_SRC.get(("bo_qua_log", party_idx), 0.0)
            if time.time() - _bo >= 15:      # roster phat lien tuc -> chan spam log
                _PARTY_JOINED_SRC[("bo_qua_log", party_idx)] = time.time()
                log.info("[%s] PARTY-JOINED: BO QUA roster cua member (%d nguoi) - leader dang "
                         "giu quyen ghi, so dem hien tai=%d",
                         label or "?", len(now), len(_PARTY_JOINED.get(party_idx, ())))
            return
        cur = _PARTY_JOINED.get(party_idx)
        _PARTY_JOINED_SRC[party_idx] = (bytes(nguoi_ghi or b""), la_leader, time.time())
        if cur == now:
            return
        _PARTY_JOINED[party_idx] = now
        _dong_bo_joined_ro(party_idx)
        log.info("[%s] PARTY-JOINED: %d -> %d (nguoi ghi=%s%s) | %s",
                 label or "?", len(cur or ()), len(now),
                 (bytes(nguoi_ghi).hex()[:8] if nguoi_ghi else "?"),
                 ", LEADER" if la_leader else "", sorted(e.hex()[:8] for e in now))


def is_joined(party_idx, entity):
    """Member nay da accept vao party chua (self_entity co trong _PARTY_JOINED).

    DOC KHONG KHOA. `_PARTY_JOINED_RO` la ban chup BAT BIEN (`frozenset`), duoc thay the - khong
    sua tai cho - moi lan duong ghi doi `_PARTY_JOINED`. Doc mot tham chieu roi `in` tren frozenset
    la an toan duoi GIL, va khong dung den `_PARTY_LOCK`.

    VI SAO PHAI BO KHOA (do bang py-spy tren tien trinh that 10/09, 256 acc / ~520 thread, user:
    "sao bot chay hay bi no responding the" -> "van not responding"): 15/15 mau, MainThread deu
    dang cho o day hoac o `is_strategist`:
        is_joined (bot/client.py) | account_status (run_party_digioi.py) | _refresh (gui.py)
    GUI goi `account_status` cho 256 acc MOI GIAY, moi lan gianh `_PARTY_LOCK` voi ca tram luong
    dang ghi (`dat_party_dang_gom`, `name_for_entity`, `mark_joined`...). Hai ham nay khong tinh gi
    ca - chung CHO KHOA. Cung mot dump: thread `dieu-phoi` ket o `dat_party_dang_gom`, thread acc
    ket o `name_for_entity` - ca he xep hang tren mot khoa.
    """
    if party_idx is None or not entity:
        return False
    return bytes(entity) in _PARTY_JOINED_RO.get(party_idx, frozenset())

def unmark_joined(party_idx, entity):
    """Go 1 member khoi danh sach da-join khi acc do THOAT/MAT KET NOI. Thieu buoc nay:
    _PARTY_JOINED giu entity cu qua lan reconnect -> leader moi vua moi da thay "du 4/4 join"
    (dem stale) -> bo qua cho accept that -> leader danh 1 minh ca phien (bug thuc te DG 09:18)."""
    if party_idx is None or not entity:
        return
    with _PARTY_LOCK:
        _PARTY_JOINED.get(party_idx, set()).discard(bytes(entity))
        _dong_bo_joined_ro(party_idx)

# Pho ban to doi: goi ket tran THAT (0x14 sub0800, in_battle_TRUOC=True) chi gui rieng cho
# MEMBER, LEADER khong bao gio nhan duoc (xac nhan tu nhieu log capture). LEADER cung KHONG
# the tu suy luan qua enemy_slots rong (HP quai cu >0 con luu vi khong co 0x33 cuoi cap nhat
# ve 0 rieng cho leader). -> MEMBER xac nhan xong thi ghi timestamp CHUNG theo party_idx,
# LEADER doc timestamp nay de biet tran da ket THAT ma khong can doi 25s SAFETY.
_PARTY_BATTLE_END = {}

def _mark_battle_end(party_idx, who=None, map_id=None):
    if party_idx is None:
        return
    with _PARTY_LOCK:
        _PARTY_BATTLE_END.setdefault(party_idx, {})[who] = (time.time(), map_id)

def _recent_battle_end(party_idx, within=3.0, map_id=None, need=1, since=None):
    """CO IT NHAT `need` member CON CUNG MAP voi leader vua xac nhan ket tran that.
    Diem mau chot = LOC THEO MAP: 1 member CHET giua tran se bay ve thanh (map khac) va van ban
    goi sub0800 - neu tin goi do, leader ha in_battle OAN trong khi tran VAN dang chay -> vong
    post-battle gui 0x14 06 dung luc server giai tran -> BI DONG KET NOI (dinh that 10:43:32-35).
    Loc map => member chet khong tinh; party 2 nguoi (need=1) van chay dung."""
    if party_idx is None:
        return False
    now = time.time()
    with _PARTY_LOCK:
        rec = dict(_PARTY_BATTLE_END.get(party_idx, {}))
    n = 0
    for _who, (t, m) in rec.items():
        if now - t >= within:
            continue
        if since is not None and t <= since:
            continue
        if map_id is not None and m is not None and m != map_id:
            continue   # member o map khac (vd da chet -> ve thanh) -> khong tinh
        n += 1
    return n >= need

# party_idx -> entity QUAN SU (leader da set). Chia se de GUI hien vai tro "quan su".
_PARTY_STRATEGIST = {}

def strategist_of(party_idx):
    # DOC KHONG KHOA - xem `is_strategist`.
    return _PARTY_STRATEGIST.get(party_idx)

def is_strategist(party_idx, entity):
    # DOC KHONG KHOA: `_PARTY_STRATEGIST` la dict {party_idx: bytes}, moi lan set la THAY THE ca
    # gia tri chu khong sua tai cho, nen `.get()` duoi GIL luon tra ve mot gia tri hoan chinh -
    # hoac cai cu, hoac cai moi. Gianh `_PARTY_LOCK` o day chi de doc mot o dict la vo ich, ma no
    # dung la cho MainThread ket (xem `is_joined`).
    if party_idx is None or not entity:
        return False
    return _PARTY_STRATEGIST.get(party_idx) == bytes(entity)

# Chi so INT (tri luc) tung char trong party (chia se de leader chon quan su INT cao nhat).
# party_idx -> {entity: int_value}.  STAT_INT = id 0x1b (xac nhan tu int.pcap).
STAT_INT = 0x1b
STAT_AGI = 0x1e

# TANG DIEM TIEM NANG (xem KNOWLEDGE.md muc 7o). Ma = `EAttribute` trong
# `_lua_dec/Controller/RoleController.lua:34`. THU TU O TREN UI KHONG LIEN MACH:
# o 4/5 la Hpx/Spx (31/32), o 6 moi la Agi (30) - `Logic/Status.lua:399-412`.
ATTR_POINT = 38                    # EAttribute.AttrPoint - so diem CHUA CONG
ATTR_KINDS = (                     # (ma, khoa, ten hien thi) - dung THU TU O UI cua client
    (27, "int", "INT"),
    (28, "atk", "ATK"),
    (29, "def", "DEF"),
    (31, "hpx", "HPx"),
    (32, "spx", "SPx"),
    (30, "agi", "AGI"),
)
ATTR_KEY_TO_CODE = {k: c for c, k, _t in ATTR_KINDS}
ATTR_CODE_TO_KEY = {c: k for c, k, _t in ATTR_KINDS}
ATTR_CODE_TO_TEN = {c: t for c, _k, t in ATTR_KINDS}
_PARTY_INT = {}

def _register_party_int(party_idx, entity, value):
    if party_idx is None or not entity:
        return
    with _PARTY_LOCK:
        _PARTY_INT.setdefault(party_idx, {})[bytes(entity)] = value

# ten nhan vat -> {username: client}. Nhan log dung TEN NHAN VAT cho de doc, nhung ten nhan vat
# KHONG duy nhat: hai server khac nhau hoan toan co the co char trung ten.
# Da xay ra (01/09/2026): party 40 (dt801-805, server dong_trac, dang danh Di Gioi) va party 48
# (dt901-905, server dieu_thuyen, mode city) co char TRUNG TEN het: dtmot/dthai/dtba/dtbon/dtnam.
# GUI map `_char2user[char] = user` la 1-1 nen acc login SAU de len acc truoc -> loc log theo
# party 48 hut ca dong "[dtmot] BATTLE SEND ..." cua party 40 (user: "party 48 dung o quang
# truong ma cu bao battle"). Trung ten thi CA HAI acc deu duoc them duoi "~username".
_NHAN_CHU = {}
_NHAN_LOCK = threading.Lock()


def _nhan_log(username, char_name, trung):
    return "%s~%s" % (char_name, username) if trung else char_name

# entity(bytes) -> ten nhan vat (chia se giua cac thread acc trong process). Moi acc tu dang ky
# entity+ten cua chinh no -> leader tra cuu ten member khi log (set quan su, moi...).
_PARTY_NAMES = {}

def _register_party_name(entity, name):
    if not entity or not name:
        return
    with _PARTY_LOCK:
        _PARTY_NAMES[bytes(entity)] = name

def name_for_entity(entity):
    """Ten nhan vat theo entity (khop 8B day du HOAC 4B prefix). None neu chua biet."""
    if not entity:
        return None
    eb = bytes(entity)
    with _PARTY_LOCK:
        if eb in _PARTY_NAMES:
            return _PARTY_NAMES[eb]
        for k, v in _PARTY_NAMES.items():   # khop prefix 4B (entity party luu dang rut gon)
            if k[:4] == eb[:4]:
                return v
    return None

def best_int_member(party_idx, candidates):
    """Tra entity co INT cao nhat trong 'candidates' (list entity). None neu khong biet INT.

    DOC KHONG KHOA - xem `is_joined`. Duyet theo `candidates` (list cua nguoi goi), khong duyet
    dict chia se, nen luong khac ghi INT giua chung cung khong lam hong vong nay."""
    ints = _PARTY_INT.get(party_idx, {})
    known = [(e, ints.get(e)) for e in candidates if ints.get(e) is not None]
    if not known:
        return None
    return max(known, key=lambda x: x[1])[0]

def party_int_for(party_idx, entity):
    """INT live da nhan tu server cua mot entity, None neu chua co packet stat."""
    if not entity:
        return None
    return _PARTY_INT.get(party_idx, {}).get(bytes(entity))


def check_duplicate_accounts(parties):
    """Kiem tra 1 username dien o NHIEU noi trong config.PARTIES -> raise ValueError de bao loi
    ngay luc khoi dong (con biet duong sua config)."""
    seen = {}          # username -> (party_idx, slot_idx)
    dups = []
    for pi, party in enumerate(parties or []):
        for si, acc in enumerate(party or []):
            if not (acc and acc[0] and acc[0].strip()):
                continue
            u = acc[0].strip()
            if u in seen:
                dups.append((u, seen[u], (pi, si)))
            else:
                seen[u] = (pi, si)
    if dups:
        # CHI canh bao (khong chan) - van cho chay. Acc trung se bi login 2 lan -> co the bi
        # da/disconnect, nen tot nhat van nen sua, nhung khong block GUI khoi dong.
        lines = [f"  - '{u}' dien o party{a[0]} slot{a[1]} VA party{b[0]} slot{b[1]}"
                 for u, a, b in dups]
        log.warning("CONFIG: co user dien TRUNG o nhieu noi (van cho chay):\n" + "\n".join(lines))


# Khung gio nhan mail (gio bat dau, moi khung 2h): 12-14, 16-18, 22-24.
MAIL_WINDOWS = [12, 16, 22]


def mail_window_now():
    """Tra ve gio bat dau cua khung mail hien tai (12/16/22), hoac None neu ngoai khung."""
    import datetime
    h = datetime.datetime.now().hour
    for ws in MAIL_WINDOWS:
        if ws <= h < ws + 2:
            return ws
    return None


_GIFT_FILE = "gift_state.json"
_gift_lock = threading.Lock()
_online_gift_flags = None

# ITEM HP/SP: bot TU HOC qua self-calibrate (probe -> doc delta HP/SP tu S2C 0x08),
# luu items_learned.json. KHONG can gamedata/config. Format:
#   { "<tid>": {"hp": <heal HP do duoc>, "sp": <heal SP>, "name": "", "none": false} }
#   none=true -> da thu, item KHONG hoi HP/SP (vat pham khac) -> khoi probe lai.
# tid consumable da xac dinh tu phan tich capture = DIEM XUAT PHAT danh sach probe
# (heal van DO LIVE, khong gan cung). Bot tu mo rong qua owned_items (S2C 0x16 inventory).
_KNOWN_CONSUMABLES = [0x0116, 0x0117, 0x011b, 0x011c, 0x0139]

def _learned_file_path():
    """Duong dan TUYET DOI items_learned.json (canh root project/.exe) -> KHONG le thuoc CWD."""
    try:
        from ._appdir import app_dir
        import os
        return os.path.join(app_dir(), "items_learned.json")
    except Exception:
        return "items_learned.json"

_LEARNED_FILE = _learned_file_path()
_learned_lock = threading.Lock()
# CACHE RIENG TUNG ACC: { username: { tid: {hp,sp,hp_zero,sp_zero,none} } }. Item availability +
# luong heal KHAC NHAU moi acc (stack rieng, heal scale theo level) -> KHONG dung chung duoc.
_all_learned = None

def _load_all_learned() -> dict:
    """{ tid_str: {hp,sp,hp_zero,sp_zero,none,unusable} }. CHUNG mọi acc (key = tid template)."""
    global _all_learned
    if _all_learned is not None:
        return _all_learned
    import json as _json
    try:
        with open(_LEARNED_FILE, encoding="utf-8") as fh:
            d = _json.load(fh)
        # chi nhan format phang tid->dict (gia tri la dict co 'hp'/'sp'); khac -> bo, lam lai
        _all_learned = d if isinstance(d, dict) and all(
            isinstance(v, dict) and ("hp" in v or "sp" in v or "unusable" in v) for v in d.values()) else {}
    except Exception:
        _all_learned = {}
    # MOI PHIEN: bo 'unusable'/'strikes' -> re-verify lai (item bi tu choi = server reject = KHONG mat
    # item -> probe lai mien phi). Tranh mark oan luc loan (relogin/lag) khoa vinh vien. Giu hp/sp/none.
    for v in _all_learned.values():
        v.pop("unusable", None)
        v.pop("strikes", None)
    return _all_learned

def _save_all_learned():
    import json as _json
    with _learned_lock:
        try:
            d = _all_learned or {}
            # SAP XEP: item HOI (hp/sp>0) len dau (heal lon truoc), roi den item khac (none...).
            ordered = dict(sorted(d.items(),
                                  key=lambda kv: -(kv[1].get("hp", 0) + kv[1].get("sp", 0))))
            with open(_LEARNED_FILE, "w", encoding="utf-8") as fh:
                _json.dump(ordered, fh, ensure_ascii=False, indent=2)
        except Exception as e:
            log.warning("save items_learned.json fail: %s", e)

# ITEM DA XAC NHAN 100% (items_known.json): { tid: {name,hp,sp} }. Bot KHONG bao gio tu sua/probe/khoa
# nhung tid nay -> tin tuyet doi (vd cac item da capture). Locked > auto-learn.
def _load_json_data_file(filename):
    import json as _json, os as _os
    paths = []
    try:
        bundle_path = getattr(config, "_bundle_data_path", None)
        if bundle_path is not None:
            paths.append(bundle_path(filename))
    except Exception:
        pass
    try:
        from ._appdir import app_dir
        paths.append(_os.path.join(app_dir(), filename))
    except Exception:
        pass
    paths.append(filename)
    for path in paths:
        try:
            with open(path, encoding="utf-8") as fh:
                return _json.load(fh)
        except Exception:
            pass
    # Android keeps bundled data in assets/train_bot_data instead of app_dir().
    try:
        reader = getattr(config, "_read_asset", None)
        if reader is not None:
            return _json.loads(reader(filename))
    except Exception:
        pass
    return None

def _load_data_bytes(*filenames):
    paths = []
    for filename in filenames:
        try:
            bundle_path = getattr(config, "_bundle_data_path", None)
            if bundle_path is not None:
                paths.append(bundle_path(filename))
        except Exception:
            pass
        try:
            from ._appdir import app_dir
            paths.append(os.path.join(app_dir(), filename))
        except Exception:
            pass
        paths.append(filename)
    for path in paths:
        try:
            with open(path, "rb") as fh:
                return fh.read()
        except Exception:
            pass
    return None

def _load_online_gift_flags() -> dict:
    """Map moc phut qua online -> BitFlag id, lay tu data game neu co."""
    global _online_gift_flags
    if _online_gift_flags is not None:
        return _online_gift_flags

    flags = {}
    data = _load_json_data_file("login_awards.json")
    if isinstance(data, dict):
        raw = data.get("online_gifts", data)
        if isinstance(raw, dict):
            for k, v in raw.items():
                try:
                    flags[int(k)] = int(v)
                except (TypeError, ValueError):
                    pass

    if not flags:
        raw = _load_data_bytes(
            os.path.join("gamedata", "Data", "LoginAwardData_C.dat"),
            os.path.join("Data", "LoginAwardData_C.dat"),
            "LoginAwardData_C.dat",
        )
        try:
            if raw and len(raw) >= 4:
                count = struct.unpack_from("<i", raw, 0)[0]
                off = 4
                for _ in range(count):
                    if off + 46 > len(raw):
                        break
                    group = raw[off]
                    day = struct.unpack_from("<I", raw, off + 2)[0]
                    flag = struct.unpack_from("<H", raw, off + 44)[0]
                    if group == ONLINE_GIFT_KIND and flag:
                        flags[int(day)] = int(flag)
                    off += 46
        except (struct.error, ValueError):
            flags = {}

    if not flags:
        flags = dict(ONLINE_GIFT_DEFAULT_FLAGS)

    _online_gift_flags = dict(sorted(flags.items()))
    return _online_gift_flags

_known_items = None
def _load_known_items() -> dict:
    """{ tid_int: {name,hp,sp} } tu items_known.json (canh root). Khoa cung, auto-learn ko dung den."""
    global _known_items
    if _known_items is not None:
        return _known_items
    _known_items = {}
    data = _load_json_data_file("items_known.json")
    if isinstance(data, dict):
        for k, v in data.get("items", {}).items():
            tid = int(k, 16) if isinstance(k, str) and k.lower().startswith("0x") else int(k)
            _known_items[tid] = {"name": v.get("name", ""), "type": v.get("type", ""),
                                 "hp": int(v.get("hp", 0)), "sp": int(v.get("sp", 0))}
    return _known_items

# TU DIEN GAMEDATA (items_gamedata.json): { item_id_hex: {name,hp,sp} } - tu crack gamedata_Item.dat.
# Bot tra item_id -> biet loai+heal NGAY, KHONG can probe. items_known.json (m khai) uu tien hon.
_gamedata_items = None
def _load_gamedata_items() -> dict:
    """{ item_id_int: {name,hp,sp,battle,restrict} } tu items_gamedata.json (crack tu gamedata).

    `restrict` = bitmask han che cua item (ItemData.lua --[30]); bit 4 = KHONG dung lam nguyen
    lieu HOP - do_combine_item dua vao day de loc (xem RESTRICT_NOT_COMBINE_MATERIAL).
    """
    global _gamedata_items
    if _gamedata_items is not None:
        return _gamedata_items
    _gamedata_items = {}
    data = _load_json_data_file("items_gamedata.json")
    if isinstance(data, dict):
        for k, v in data.items():
            iid = int(k, 16) if isinstance(k, str) and k.lower().startswith("0x") else int(k)
            _gamedata_items[iid] = {"name": v.get("name", ""), "battle": bool(v.get("battle")),
                                    "hp": int(v.get("hp", 0)), "sp": int(v.get("sp", 0)),
                                    "restrict": int(v.get("restrict", 0) or 0),
                                    # ft = fitType = VI TRI MAC (1 mu, 2 ao, 3 vu khi...). Can de
                                    # suy mon DANG mac o vi tri do khi THAY DO - xem _on_equip_done.
                                    "ft": int(v.get("ft", 0) or 0),
                                    # BON TRUONG DUOI DAY BAT BUOC PHAI CO. Loader nay chep tay
                                    # tung truong, nen quen mot cai la o dung KHONG BAO GIO thay no
                                    # (get -> None -> 0) ma KHONG he bao loi.
                                    #   fc  = so manh khi phan giai (0 = khong phan giai duoc)
                                    #   mat = material, lv = level, kd = kind -> UIArmy.ArmyFilter
                                    # Da xay ra that (03/09): thieu ca 4 -> tu mo hop trang bi coi
                                    # MOI mon la "khong phan giai, khong donate duoc" va VUT SACH.
                                    "fc": int(v.get("fc", 0) or 0),
                                    "mat": int(v.get("mat", 0) or 0),
                                    "lv": int(v.get("lv", 0) or 0),
                                    "kd": int(v.get("kd", 0) or 0),
                                    # st = truong 排序 cua Item_C.dat. Client sap tui do VA tien
                                    # trang bang `Item.Sort` = (sort ASC, Id ASC). Thieu no thi
                                    # moi mon deu 999 -> thu tu hien ra khac han trong game.
                                    "st": int(v.get("st", 999) or 999),
                                    "q": int(v.get("q", 0) or 0)}
    return _gamedata_items


# Item tab CHUYEN SINH (lo) -> vo tuong nao, bac nao. tid -> {"loai", "rb0", "rb1", "rb2"}
_chuyen_sinh_map = None

# a1k/a2k trong items_gamedata.json phan biet 3 loai item chuyen sinh (da doi chieu ca 777 item
# trong furnace_pool.json - moi item CHI thuoc dung mot loai, va a1k luon == a2k):
CS_KIM_TOA, CS_ME, CS_TUONG_TINH = 65, 66, 67


def _load_chuyen_sinh_map() -> dict:
    """{item_id: {"loai": 65/66/67, "rb0"/"rb1"/"rb2": npc_id}} cho item tab chuyen sinh.

    Ten item bi VIET TAT trong game ("K.Toa Ma Ng.Nghia" trong khi vo tuong ten day du la "Ma
    Nguyen Nghia") nen KHONG the ghep item voi vo tuong bang ten. Nhung `items_gamedata.json` co
    san npc_id:

        K.Toa Ma Ng.Nghia  a1k=65 a1v=10007 (0x2717 "Ma Nguyen Nghia rb0")  a2v=41099 (rb1)
        Tieu Kieu Me       a1k=66 a1v=41080 (rb1)                           a2v=45034 (rb2)
        T.Tinh Tran Cung   a1k=67 a1v=41113 (rb1)                           a2v=45081 (rb2)

    => dai id: 10xxx (0x27xx) = rb0, 41xxx (0xA0xx) = rb1, 45xxx = rb2.
    Kim Toa chi cho biet rb0+rb1; rb2 suy tiep tu item Me/T.Tinh CUNG rb1.
    """
    global _chuyen_sinh_map
    if _chuyen_sinh_map is not None:
        return _chuyen_sinh_map
    # CHI nhan gia tri THAT SU la npc_id vo tuong. `a1v`/`a2v` khong phai luc nao cung la vo tuong:
    #   'Me Nhan Dieu Tuyet' a1k=66 a1v=100 (LEVEL yeu cau, khong phai npc) a2v=45424 (moi la npc)
    #   'K.Toa Ngoc Tho'     a1v=22095 a2v=22096 - ca hai deu KHONG co trong pets.json
    # 50/938 mon dinh truong hop nay. Lay bua vao la chan nham (hoac bo sot) mon can mua.
    _pets = _load_json_data_file("pets.json") or {}
    _pets = _pets.get("pets", _pets) if isinstance(_pets, dict) else {}
    npc_hop_le = set()
    for k in _pets:
        try:
            npc_hop_le.add(int(k, 16) if isinstance(k, str) and k.lower().startswith("0x") else int(k))
        except Exception:
            pass

    def _npc(v):
        v = int(v or 0)
        return v if v in npc_hop_le else 0

    tho = {}
    for iid, v in (_load_json_data_file("items_gamedata.json") or {}).items():
        if not isinstance(v, dict):
            continue
        a1k, a2k = int(v.get("a1k", 0) or 0), int(v.get("a2k", 0) or 0)
        if a1k != a2k or a1k not in (CS_KIM_TOA, CS_ME, CS_TUONG_TINH):
            continue
        try:
            tid = int(iid, 16) if isinstance(iid, str) and iid.lower().startswith("0x") else int(iid)
        except Exception:
            continue
        tho[tid] = (a1k, _npc(v.get("a1v")), _npc(v.get("a2v")))
    # rb1 -> rb2 (chi Me/T.Tinh moi mang cap nay)
    rb2_theo_rb1 = {a1v: a2v for (k, a1v, a2v) in tho.values()
                    if k in (CS_ME, CS_TUONG_TINH) and a1v and a2v}
    _chuyen_sinh_map = {}
    for tid, (k, a1v, a2v) in tho.items():
        if k == CS_KIM_TOA:
            m = {"loai": k, "rb0": a1v, "rb1": a2v, "rb2": rb2_theo_rb1.get(a2v, 0)}
        else:
            m = {"loai": k, "rb0": 0, "rb1": a1v, "rb2": a2v}
        if not (m["rb1"] or m["rb2"]):
            continue        # khong lan ra vo tuong nao -> giu nguyen hanh vi cu (van mua)
        _chuyen_sinh_map[tid] = m
    return _chuyen_sinh_map


_mark_bitids = None
_warp_points = None
def _load_warp_points() -> list:
    """[{scene, mark, x, y}] tu warp_points.json (tools/crack_warp.py = Warp_C.dat).

    `mark` = co nhiem vu can co de MO diem dich chuyen do (Data_WarpData.lua --身上標記).
    Dung cho city_unlocked(): biet TRUOC thanh nao da mo thay vi tele mu roi doan.
    """
    global _warp_points
    if _warp_points is not None:
        return _warp_points
    data = _load_json_data_file("warp_points.json") or {}
    _warp_points = data.get("warps") or []
    return _warp_points


def _load_mark_bitids() -> dict:
    """{missionId: bitId} tu mark_bitids.json (tools/crack_mark_bitids.py).

    Dung cho dieu kien thanh tuu kind=15 (MissionFlag): client tra
    CheckFlag(MarkManager.flags, markDatas[missionId].bitId).
    """
    global _mark_bitids
    if _mark_bitids is not None:
        return _mark_bitids
    _mark_bitids = {}
    for k, v in (_load_json_data_file("mark_bitids.json") or {}).items():
        try:
            _mark_bitids[int(k)] = int(v)
        except Exception:
            pass
    return _mark_bitids


_achievements = None
def _load_achievements() -> dict:
    """{ id_int: {"name","cf","gf","item","count"} } tu achievements.json.

    cf = complete_flag, gf = get_flag: CHI SO BIT trong mang "forever flags" (goi 0x51 = opcode
    81) ma bot da parse san. Client ve DUNG 3 trang thai chi bang 2 bit nay (UIAchievement.lua:97):
        cf BAT + gf TAT -> co the NHAN | cf BAT + gf BAT -> da nhan | con lai -> dang lam
    Sinh boi tools/crack_achievements.py.
    """
    global _achievements
    if _achievements is not None:
        return _achievements
    _achievements = {}
    data = _load_json_data_file("achievements.json")
    if isinstance(data, dict):
        for k, v in data.items():
            try:
                aid = int(k)
            except (TypeError, ValueError):
                continue
            if isinstance(v, dict) and v.get("complete_flag") and v.get("get_flag"):
                _achievements[aid] = {"name": v.get("name", ""),
                                      "cf": int(v["complete_flag"]), "gf": int(v["get_flag"]),
                                      "item": int(v.get("item", 0) or 0),
                                      "count": int(v.get("count", 0) or 0),
                                      # DIEU KIEN: de bot tu tinh hoan thanh (C:082-001)
                                      "score": int(v.get("score", 0) or 0),
                                      "kind": int(v.get("kind", 0) or 0),
                                      "kind_value": int(v.get("kind_value", 0) or 0),
                                      "opr": int(v.get("opr", 3) or 3),
                                      "value": int(v.get("value", 0) or 0)}
    return _achievements


_pet_scrolls = None
def _load_pet_scrolls() -> dict:
    """{ tid_int: {"name","npc","vkcd"} } tu pet_scrolls.json - TAT CA cuon goi vo tuong (Bi Cap).

    Dung cho "Tu phan giai cuon vo tuong rac": mac dinh cuon cua tuong CO vu khi chuyen dung
    (vkcd) = GIU LAI, con lai = PHAN GIAI. Mac dinh nay chi la GOI Y - pet co vkcd nhieu con van
    lom nen user doi duoc ca 2 chieu. Sinh boi tools/crack_pet_scrolls.py.
    """
    global _pet_scrolls
    if _pet_scrolls is not None:
        return _pet_scrolls
    _pet_scrolls = {}
    data = _load_json_data_file("pet_scrolls.json")
    if isinstance(data, dict):
        for k, v in data.items():
            try:
                tid = int(k, 16) if isinstance(k, str) and k.lower().startswith("0x") else int(k)
            except Exception:
                continue
            if isinstance(v, dict):
                _extra = []
                for e in (v.get("extra") or ()):
                    try:
                        _extra.append(int(e, 16) if isinstance(e, str) else int(e))
                    except Exception:
                        pass
                _pet_scrolls[tid] = {"name": v.get("name", ""), "npc": v.get("npc", ""),
                                     "vkcd": bool(v.get("vkcd")), "extra": _extra}
    return _pet_scrolls


_furnace_notify_ids = None
def _load_furnace_default_notify_ids() -> set:
    """Set item_id MAC DINH "Thong bao" (furnace_default_notify.json).

    Cuon goi / K.Toa / T.Tinh / Me cua vo tuong CO VU KHI CHUYEN DUNG -> mach cua no dang co gia
    tri, khong duoc am tham bo qua. Sinh boi tools/crack_furnace_notify.py (ghep theo ID trong
    gamedata_Item.dat, KHONG theo ten vi ten trong pool bi cat ngan).
    Config rieng cua acc VAN DE LEN: user chon "Bo qua" item nao thi item do bo qua.
    """
    global _furnace_notify_ids
    if _furnace_notify_ids is not None:
        return _furnace_notify_ids
    _furnace_notify_ids = set()
    data = _load_json_data_file("furnace_default_notify.json")
    if isinstance(data, dict):
        for tab in data.values():
            if not isinstance(tab, dict):
                continue
            for k in tab:
                try:
                    _furnace_notify_ids.add(
                        int(k, 16) if isinstance(k, str) and k.lower().startswith("0x") else int(k))
                except Exception:
                    pass
    return _furnace_notify_ids


_furnace_pool_ids = None
def _load_furnace_pool_ids() -> set:
    """Set TAT CA item_id (int) da biet trong furnace_pool.json (pool hien tai cua game). Dung de
    phat hien item LA (game update them item moi ngoai pool) -> mac dinh THONG BAO cho user."""
    global _furnace_pool_ids
    if _furnace_pool_ids is not None:
        return _furnace_pool_ids
    _furnace_pool_ids = set()
    data = _load_json_data_file("furnace_pool.json")
    if isinstance(data, dict):
        for tab in data.values():
            if not isinstance(tab, dict):
                continue
            for k in tab:
                try:
                    _furnace_pool_ids.add(int(k, 16) if isinstance(k, str) and k.lower().startswith("0x") else int(k))
                except Exception:
                    pass
    return _furnace_pool_ids

_bliss_boxes = None
def _load_bliss_boxes() -> dict:
    """{tid_int: {name, luot, kindCount, items:[{id, fc, ...}]}} tu bliss_bag.json.

    Sinh boi tools/crack_bliss_bag.py tu Data/BlissBag_C.dat. `fc` (furnaceCount) > 0 = PHAN GIAI
    duoc (ra manh trang bi); == 0 -> chi con duong DONATE quan doan.
    """
    global _bliss_boxes
    if _bliss_boxes is not None:
        return _bliss_boxes
    _bliss_boxes = {}
    data = _load_json_data_file("bliss_bag.json")
    for k, v in ((data or {}).get("boxes") or {}).items():
        try:
            tid = int(k, 16) if isinstance(k, str) and k.lower().startswith("0x") else int(k)
        except Exception:
            continue
        if isinstance(v, dict):
            _bliss_boxes[tid] = v
    return _bliss_boxes


_npc_names = None
def _load_npc_names() -> dict:
    """{ npc_id_int: ten } tu npc_names.json (dung log ten vo tuong thuong nhan cuoi tran)."""
    global _npc_names
    if _npc_names is not None:
        return _npc_names
    _npc_names = {}
    data = _load_json_data_file("npc_names.json")
    if isinstance(data, dict):
        for k, v in data.items():
            try:
                _npc_names[int(k, 16) if isinstance(k, str) and k.lower().startswith("0x") else int(k)] = v
            except Exception:
                pass
    return _npc_names

_pet_stat_data = None
def _load_pet_stat_data() -> dict:
    global _pet_stat_data
    if _pet_stat_data is None:
        _pet_stat_data = _load_json_data_file("pet_stats.json") or {}
    return _pet_stat_data

_collect_style = None
def _load_collect_style() -> dict:
    """{ tid_int: (collectStyleId, part) } tu collect_style.json (246 tid do thoi trang, crack tu
    CollectStyle_C.dat). Dung de biet item tui nao la thoi trang + gui dung (id,part) tha vao S.Tam."""
    global _collect_style
    if _collect_style is not None:
        return _collect_style
    _collect_style = {}
    data = _load_json_data_file("collect_style.json")
    if isinstance(data, dict):
        for k, v in data.items():
            try:
                tid = int(k, 16) if isinstance(k, str) and k.lower().startswith("0x") else int(k)
                _collect_style[tid] = (int(v[0]), int(v[1]))
            except Exception:
                pass
    return _collect_style




def _gift_day(today=None) -> str:
    import datetime
    if today is None:
        return datetime.date.today().isoformat()
    return today.isoformat() if hasattr(today, "isoformat") else str(today)


def _gift_key(label: str, today=None) -> str:
    return f"{label}:{_gift_day(today)}"


def _load_gift_state(label: str, today=None) -> dict:
    """Load state qua online HOM NAY: {'online_sec': float, 'claimed': set}."""
    import json, os
    default = {"online_sec": 0.0, "claimed": set()}
    if not os.path.exists(_GIFT_FILE):
        return default
    try:
        with open(_GIFT_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        rec = data.get(_gift_key(label, today))
        if not rec or rec.get("version") != 2:
            return default
        return {"online_sec": float(rec.get("online_sec", 0)),
                "claimed": set(rec.get("claimed", []))}
    except Exception:
        return default


def _save_gift_state(label: str, online_sec: float, claimed: set, today=None):
    """Luu online_sec + claimed cho hom nay; don key ngay cu."""
    import json, os
    day = _gift_day(today)
    with _gift_lock:
        data = {}
        if os.path.exists(_GIFT_FILE):
            try:
                with open(_GIFT_FILE, "r", encoding="utf-8") as f:
                    data = json.load(f)
            except Exception:
                data = {}
        data = {k: v for k, v in data.items() if k.endswith(day)}
        data[_gift_key(label, day)] = {
            "version": 2,
            "online_sec": round(online_sec, 1),
            "claimed": sorted(claimed),
        }
        try:
            with open(_GIFT_FILE, "w", encoding="utf-8") as f:
                json.dump(data, f)
        except Exception:
            pass


# ---- LIST CAT VAO TIEN TRANG: MOT file CHUNG cho moi acc/party ----
# User chot 04/09: "lam 1 list chung cho tat ca cac bot thoi, the cho nhe bot". Truoc do list
# luu trong TUNG party cua accounts.json -> sua mot mon phai sua N cho, va moi party mang mot
# ban sao rieng. File nay bot TU SINH (nhu checkin_state.json) nen KHONG can khai bao trong
# SHARED_ASSETS / DATA_JSON.
_CAT_DO_FILE = "cat_do_items.json"
CAT_DO_CAT = "cat"      # tick    -> CAT vao tien trang
CAT_DO_LAY = "lay"      # bo tick -> LAY RA khoi tien trang
# Mon KHONG co trong list = bot khong dung toi (user da xoa dong di).
# Mac dinh khi file chua ton tai. Ca hai deu `restrict & 32 == 0` -> game cho gui ngan hang.
CAT_DO_MAC_DINH = {
    "0xb3e2": CAT_DO_CAT,     # Don Thang Hoa
    "0xb49f": CAT_DO_CAT,     # Tien Don Nang Luong
}


def _cat_do_path():
    try:
        from ._appdir import app_dir
        return os.path.join(app_dir(), _CAT_DO_FILE)
    except Exception:
        return _CAT_DO_FILE


def _chuan_hoa_cat_do(ds) -> dict:
    """{tid_hex: "cat"|"lay"}. Nhan ca dang CU {tid: true/false} de doc lai file da luu.

    Dang cu chi co hai trang thai (co trong list = cat, khong co = ke). Gio co BA: cat / lay /
    khong co trong list -> `true` cu doc thanh "cat", `false` cu (neu co) doc thanh "lay".
    """
    out = {}
    for k, v in (ds or {}).items():
        k = str(k).lower()
        if v is True:
            out[k] = CAT_DO_CAT
        elif v is False:
            out[k] = CAT_DO_LAY
        elif str(v) in (CAT_DO_CAT, CAT_DO_LAY):
            out[k] = str(v)
    return out


def load_cat_do_items() -> dict:
    """{tid_hex: "cat"|"lay"} - DUNG CHUNG moi acc.

    Chua co file -> tra MAC DINH. File co roi thi ton trong NGUYEN VAN, ke ca khi rong:
    user xoa het la co y, nhoi lai mac dinh thi ho xoa bao nhieu lan cung khong duoc.
    """
    p = _cat_do_path()
    if not os.path.exists(p):
        return dict(CAT_DO_MAC_DINH)
    try:
        with open(p, encoding="utf-8") as fh:
            d = json.load(fh)
    except Exception:
        return dict(CAT_DO_MAC_DINH)
    return _chuan_hoa_cat_do(d.get("items") if isinstance(d, dict) else None)


def save_cat_do_items(items) -> bool:
    try:
        with open(_cat_do_path(), "w", encoding="utf-8") as fh:
            json.dump({"items": _chuan_hoa_cat_do(items)}, fh, ensure_ascii=False, indent=2)
        return True
    except Exception as e:
        log.warning("khong ghi duoc %s: %s", _CAT_DO_FILE, e)
        return False


# ---- State DIEM DANH (so lan da diem danh) ----
_CHECKIN_FILE = "checkin_state.json"

def _load_checkin(label: str, kind: str = "checkin") -> dict:
    """{'date': 'YYYY-MM-DD', 'day': N} - lan nhan gan nhat (kind: checkin / gift14 / ...)."""
    import json, os
    if not os.path.exists(_CHECKIN_FILE):
        return {"date": "", "day": 0}
    try:
        with open(_CHECKIN_FILE, encoding="utf-8") as f:
            return json.load(f).get(f"{label}:{kind}", {"date": "", "day": 0})
    except Exception:
        return {"date": "", "day": 0}

def _save_checkin(label: str, kind: str, date: str, day: int):
    import json, os
    with _gift_lock:
        data = {}
        if os.path.exists(_CHECKIN_FILE):
            try:
                with open(_CHECKIN_FILE, encoding="utf-8") as f:
                    data = json.load(f)
            except Exception:
                data = {}
        data[f"{label}:{kind}"] = {"date": date, "day": day}
        try:
            with open(_CHECKIN_FILE, "w", encoding="utf-8") as f:
                json.dump(data, f)
        except Exception:
            pass


# ---- Tracker viec lam HANG NGAY 1 lan (vd qua quan doan): {label:task -> date} ----
_DAILY_FILE = "daily_state.json"

def _daily_done(label: str, task: str) -> bool:
    import json, os, datetime
    if not os.path.exists(_DAILY_FILE):
        return False
    try:
        with open(_DAILY_FILE, encoding="utf-8") as f:
            d = json.load(f)
        return d.get(f"{label}:{task}") == datetime.date.today().isoformat()
    except Exception:
        return False

def _mark_daily(label: str, task: str):
    import json, os, datetime
    today = datetime.date.today().isoformat()
    with _gift_lock:
        d = {}
        if os.path.exists(_DAILY_FILE):
            try:
                with open(_DAILY_FILE, encoding="utf-8") as f:
                    d = json.load(f)
            except Exception:
                d = {}
        # don key ngay cu: giu str == today VA dict co date == today (vd legion_boss luu count/next)
        d = {k: v for k, v in d.items()
             if v == today or (isinstance(v, dict) and v.get("date") == today)}
        d[f"{label}:{task}"] = today
        try:
            with open(_DAILY_FILE, "w", encoding="utf-8") as f:
                json.dump(d, f)
        except Exception:
            pass




# ---- State BOSS QUAN DOAN: luu BEN thoi diem duoc thu lai (legion_boss_next) qua cac lan
# reconnect/relogin. BUG THAT (xac nhan chac chan): legion_boss_next TRUOC DAY chi la thuoc
# tinh instance (RAM) - moi lan reconnect/relogin tao GameClient MOI se mat sach, khien bot
# THU LAI tu dau ngay ca khi vua that bai truoc do (bat ke cooldown that su la bao lau) ->
# lap lai dung "vao instance boss khi chua du dieu kien" nhieu lan, moi lan co the lam ket/dơ
# trang thai enter_di_gioi_safe() ngay sau do (xem do_legion_boss). Fix nay (luu ben) dung bat
# ke gia tri LEGION_BOSS_COOLDOWN chinh xac bao nhieu.
_LEGION_BOSS_FILE = "legion_boss_state.json"
_legion_boss_lock = threading.Lock()

def _load_legion_boss_next(label: str) -> float:
    import json, os
    if not os.path.exists(_LEGION_BOSS_FILE):
        return 0.0
    try:
        with open(_LEGION_BOSS_FILE, encoding="utf-8") as f:
            return float(json.load(f).get(label, 0.0))
    except Exception:
        return 0.0

def _save_legion_boss_next(label: str, next_ts: float):
    import json, os
    with _legion_boss_lock:
        d = {}
        if os.path.exists(_LEGION_BOSS_FILE):
            try:
                with open(_LEGION_BOSS_FILE, encoding="utf-8") as f:
                    d = json.load(f)
            except Exception:
                d = {}
        d[label] = next_ts
        try:
            with open(_LEGION_BOSS_FILE, "w", encoding="utf-8") as f:
                json.dump(d, f)
        except Exception:
            pass


# ---- CACHE skill/pet theo account: de dialog Kich ban Skill dung duoc khi acc DA TAT --------
# Ghi tu CLIENT (khong phai tu vong lap cua run_party_digioi): luong that co nhieu nhanh khong
# di qua vong lap do (da dinh 1 lan - cache khong bao gio duoc ghi). _on_pet_list chay luc login
# VA moi lan doi pet (handler 0x13 goi lai) nen la cho chac chan nhat.
_skill_cache_lock = threading.Lock()
_skill_cache_sig = {}


def _ghi_json_an_toan(path, data):
    """Ghi JSON KIEU NGUYEN TU: ghi ra file tam roi `os.replace` de len.

    `open(path,"w")` CAT TRANG file truoc roi moi ghi -> giua hai buoc do, moi ai doc file deu
    thay ban CUT DO DANG. Do bang tay 06/09 luc bot dang chay 90 acc: `account_skills_cache.json`
    nhay 8KB -> 80KB -> 56KB trong vai giay; GUI doc trung luc do la `json.load` nem loi va coi
    nhu "chua co cache". Sap nguon (hoac tat may) dung luc do thi MAT SACH ca file.
    `os.replace` la thao tac nguyen tu tren cung o dia: doc gia thay ban CU hoac ban MOI, khong
    bao gio thay ban do dang.
    """
    import json, os
    tam = "%s.tam%d" % (path, os.getpid())
    try:
        with open(tam, "w", encoding="utf-8") as fh:
            json.dump(data, fh, ensure_ascii=False)
        os.replace(tam, path)
        return True
    except Exception:
        try:
            os.remove(tam)
        except Exception:
            pass
        raise


def _skill_cache_path():
    # import TUONG DOI nhu _learned_file_path/_load_json_data_file: ban APK khong co package "bot"
    # (cong chan trong tools/sync_apk_python.py bat import tuyet doi 'bot.*').
    try:
        from ._appdir import app_dir
        import os
        return os.path.join(app_dir(), "account_skills_cache.json")
    except Exception:
        return "account_skills_cache.json"


def save_skill_cache(username, data):
    """Ghi cache 1 account. Chi ghi khi DU LIEU DOI (so chu ky) - ham nay bi goi rat nhieu."""
    import json, os
    username = str(username or "").strip()
    if not username or not data:
        return False
    sig = json.dumps(data, sort_keys=True, ensure_ascii=False)
    with _skill_cache_lock:
        if _skill_cache_sig.get(username) == sig:
            return False
        path = _skill_cache_path()
        allc = {}
        if os.path.exists(path):
            try:
                with open(path, encoding="utf-8") as fh:
                    allc = json.load(fh) or {}
            except Exception:
                allc = {}
        # GIU khoa "inn" (list pet nha tro, do save_inn_cache ghi): ham nay THAY nguyen entry nen
        # khong giu thi moi lan cache skill se xoa mat list pet -> dialog van tieu trong khi acc tat.
        cu = allc.get(username) or {}
        allc[username] = dict(data, ts=int(time.time()))
        if isinstance(cu, dict) and cu.get("inn"):
            allc[username]["inn"] = cu["inn"]
        try:
            _ghi_json_an_toan(path, allc)
        except Exception as e:
            log.debug("ghi cache skill loi: %s", e)
            return False
        _skill_cache_sig[username] = sig
        return True


def ten_pet_kem_lv(ten, lv):
    """Nhan pet hien cho user: `Ten_lv` (vd `Quan Vu_111`). User chot 07/09 - de soat pet nao chua
    nuoi o nha tro / battle / tui do ma khong phai mo tung con ra xem.

    Cap khong hop le (0, None, > 200) -> tra ten tran, KHONG bia so."""
    ten = (ten or "").strip()
    try:
        lv = int(lv or 0)
    except Exception:
        return ten
    if not ten or not (1 <= lv <= 200):
        return ten
    return "%s_%d" % (ten, lv)


def save_inn_cache(username, inn):
    """Ghi cache PET NHA TRO cua 1 acc (de dialog van tieu sua duoc khi acc DA TAT).

    `inn` = [[pet_id, ten], ...] theo thu tu index nha tro. Dung CHUNG file
    account_skills_cache.json (khoa "inn") thay vi them file moi: file do da co san duong nap o ca
    PC lan APK, va da co san co che "acc tat thi doc cache".
    KHONG ghi de cac khoa khac cua acc (char/pet/pets/active)."""
    import json, os
    username = str(username or "").strip()
    if not username or not inn:
        return False
    sig = json.dumps(inn, ensure_ascii=False)
    with _skill_cache_lock:
        if _skill_cache_sig.get("inn:" + username) == sig:
            return False
        path = _skill_cache_path()
        allc = {}
        if os.path.exists(path):
            try:
                with open(path, encoding="utf-8") as fh:
                    allc = json.load(fh) or {}
            except Exception:
                allc = {}
        entry = allc.get(username)
        if not isinstance(entry, dict):
            entry = {}
        entry["inn"] = inn
        entry["inn_ts"] = int(time.time())
        allc[username] = entry
        try:
            _ghi_json_an_toan(path, allc)
        except Exception as e:
            log.debug("ghi cache pet nha tro loi: %s", e)
            return False
        _skill_cache_sig["inn:" + username] = sig
        return True


def save_point_cache(username, diem):
    """Ghi cache BANG DIEM cua 1 acc de dialog Point XEM DUOC khi acc DA TAT.

    `diem` = {"left": int|None, "stats": [{key, ten, ma, goc, tong}, ...]}.
    Dung CHUNG file `account_skills_cache.json` (khoa "point") y het cach `save_inn_cache` lam:
    file do da co san duong nap o ca PC lan APK va da co san co che "acc tat thi doc cache".
    KHONG ghi de cac khoa khac cua acc.

    Chi de XEM. CONG DIEM van phai co acc dang chay (phai gui goi `C:008-001` len server).
    """
    import json, os
    username = str(username or "").strip()
    if not username or not diem:
        return False
    sig = json.dumps(diem, sort_keys=True, ensure_ascii=False)
    with _skill_cache_lock:
        if _skill_cache_sig.get("point:" + username) == sig:
            return False
        path = _skill_cache_path()
        allc = {}
        if os.path.exists(path):
            try:
                with open(path, encoding="utf-8") as fh:
                    allc = json.load(fh) or {}
            except Exception:
                allc = {}
        entry = allc.get(username)
        if not isinstance(entry, dict):
            entry = {}
        entry["point"] = diem
        entry["point_ts"] = int(time.time())
        allc[username] = entry
        try:
            _ghi_json_an_toan(path, allc)
        except Exception as e:
            log.debug("ghi cache bang diem loi: %s", e)
            return False
        _skill_cache_sig["point:" + username] = sig
        return True


def load_point_cache(username):
    """Bang diem da cache cua 1 acc -> (diem, ts) hoac (None, 0)."""
    import json, os
    username = str(username or "").strip()
    if not username:
        return None, 0
    path = _skill_cache_path()
    if not os.path.exists(path):
        return None, 0
    try:
        with open(path, encoding="utf-8") as fh:
            allc = json.load(fh) or {}
    except Exception:
        return None, 0
    entry = allc.get(username)
    if not isinstance(entry, dict):
        return None, 0
    diem = entry.get("point")
    return (diem if isinstance(diem, dict) else None), int(entry.get("point_ts") or 0)


def save_skill_char_cache(username, du_lieu) -> bool:
    """Cache BANG SKILL NHAN VAT (khoa "skill_char") de dialog XEM duoc khi acc DA TAT.

    `du_lieu` = {"element": int|None, "left": int|None, "lv": {"0x2b02": 3, ...}}.
    Dung CHUNG file `account_skills_cache.json` y het `save_point_cache`.
    """
    import json, os
    username = str(username or "").strip()
    if not username or not du_lieu:
        return False
    sig = json.dumps(du_lieu, ensure_ascii=False, sort_keys=True)
    with _skill_cache_lock:
        if _skill_cache_sig.get("skill_char:" + username) == sig:
            return False
        path = _skill_cache_path()
        allc = {}
        if os.path.exists(path):
            try:
                with open(path, encoding="utf-8") as fh:
                    allc = json.load(fh) or {}
            except Exception:
                allc = {}
        entry = allc.get(username)
        if not isinstance(entry, dict):
            entry = {}
        entry["skill_char"] = du_lieu
        entry["skill_char_ts"] = int(time.time())
        allc[username] = entry
        try:
            _ghi_json_an_toan(path, allc)
        except Exception as e:
            log.debug("ghi cache skill nhan vat loi: %s", e)
            return False
        _skill_cache_sig["skill_char:" + username] = sig
        return True


def load_skill_char_cache(username):
    """(du_lieu, ts) hoac (None, 0)."""
    import json, os
    username = str(username or "").strip()
    if not username:
        return None, 0
    path = _skill_cache_path()
    if not os.path.exists(path):
        return None, 0
    try:
        with open(path, encoding="utf-8") as fh:
            allc = json.load(fh) or {}
    except Exception:
        return None, 0
    entry = allc.get(username)
    if not isinstance(entry, dict):
        return None, 0
    d = entry.get("skill_char")
    return (d if isinstance(d, dict) else None), int(entry.get("skill_char_ts") or 0)


def _cache_ghi(username, khoa, du_lieu, moc_ts=True) -> bool:
    """Ghi MOT khoa vao entry cua acc trong account_skills_cache.json, giu nguyen cac khoa khac.

    Gop lai tu 4 ham `save_*_cache` viet tay giong het nhau. Chi ghi khi noi dung DOI (so chu ky)
    - cac cho goi deu la duong nong (moi goi tui do la mot lan goi).
    """
    import json, os
    username = str(username or "").strip()
    if not username or du_lieu is None:
        return False
    try:
        sig = json.dumps(du_lieu, sort_keys=True, ensure_ascii=False)
    except Exception:
        return False
    with _skill_cache_lock:
        if _skill_cache_sig.get(khoa + ":" + username) == sig:
            return False
        path = _skill_cache_path()
        allc = {}
        if os.path.exists(path):
            try:
                with open(path, encoding="utf-8") as fh:
                    allc = json.load(fh) or {}
            except Exception:
                allc = {}
        entry = allc.get(username)
        if not isinstance(entry, dict):
            entry = {}
        entry[khoa] = du_lieu
        if moc_ts:
            entry[khoa + "_ts"] = int(time.time())
        allc[username] = entry
        try:
            _ghi_json_an_toan(path, allc)
        except Exception as e:
            log.debug("ghi cache %s loi: %s", khoa, e)
            return False
        _skill_cache_sig[khoa + ":" + username] = sig
        return True


def _cache_doc(username, khoa):
    """(du_lieu, ts) cua MOT khoa; (None, 0) neu chua co."""
    import json, os
    username = str(username or "").strip()
    if not username:
        return None, 0
    path = _skill_cache_path()
    if not os.path.exists(path):
        return None, 0
    try:
        with open(path, encoding="utf-8") as fh:
            allc = json.load(fh) or {}
    except Exception:
        return None, 0
    entry = allc.get(username)
    if not isinstance(entry, dict):
        return None, 0
    return entry.get(khoa), int(entry.get(khoa + "_ts") or 0)


# ---- CACHE TUI DO + TIEN TRANG -----------------------------------------------------------
# TUI DO: de XEM LAI khi acc da TAT (user 06/09). Ban cache la CHI XEM - moi nut (dung / trang bi
# / phan giai / bo) phai tat, vi bam theo so cu la mat nham do. Do la ly do truoc day bag_info()
# tu choi cache han; gio cache nhung khoa nut lai, khong bo canh bao do.
#
# TIEN TRANG: de NHO trong kho dang co gi (user 06/09, de mo rong soi lo check ca do trong kho).
# LUU Y QUAN TRONG: server KHONG BAO GIO tu gui kho. Bot chi biet dung luc no di NPC Trac Quan mo
# kho (`S:030-001` ca kho luc mo, `S:030-004` tung o khi cat/lay). Dong kho la het - login lai
# khong co goi nao mang kho ve. Nen cache kho chi moi bang LAN MO KHO GAN NHAT, va acc chua tung
# mo kho thi COI NHU KHONG CO GI (user chot: khong lam nut "doc lai kho" - ton mot chuyen di).
def save_bag_cache(username, tui) -> bool:
    """`tui` = {"slots": {slot: [tid, cnt]}, "cap": int, "used": int}."""
    return _cache_ghi(username, "bag", tui)


def load_bag_cache(username):
    """(tui, ts) - `tui["slots"]` khoa la CHU (JSON), doi lai int o cho dung."""
    d, ts = _cache_doc(username, "bag")
    return (d if isinstance(d, dict) else None), ts


def save_bank_cache(username, kho) -> bool:
    """`kho` = {"slots": {idx: [tid, cnt]}}."""
    return _cache_ghi(username, "bank", kho)


def load_bank_cache(username):
    d, ts = _cache_doc(username, "bank")
    return (d if isinstance(d, dict) else None), ts


def bank_counts_cache(username) -> dict:
    """{tid: tong so luong} trong TIEN TRANG theo cache - CUNG HINH DANG voi `c.bag_counts`.

    De soi lo (va bat ky luat "da co chua") cong them nguon thu hai ma khong phai boc lai goi.
    Acc CHUA TUNG mo kho -> {} = COI NHU KHONG CO GI trong kho (user chot 06/09). Khong co nut
    "doc lai kho": mo kho ton mot chuyen di Trac Quan, khong bo cong.
    """
    kho, _ts = load_bank_cache(username)
    dem = {}
    for _idx, val in ((kho or {}).get("slots") or {}).items():
        try:
            tid, cnt = int(val[0]), int(val[1])
        except Exception:
            continue
        if tid and cnt > 0:
            dem[tid] = dem.get(tid, 0) + cnt
    return dem


def load_dac_ky_cache(username) -> set:
    """Set npc_id vo tuong TUNG THAY da hoc dac ky (khoa "dac_ky" trong account_skills_cache.json).

    VI SAO PHAI CACHE: co `specialSkillLearned` CHI co trong goi `0x0f` (vo tuong MANG THEO,
    `Role.lua:857`). Vo tuong nam NHA TRO chi ve qua `S:031-006` -> `Inn.SaveNpc` voi vo ven
    npcId/level/exp/hp/name/status (`_lua_dec/Logic/Inn.lua:25`) - KHONG co co dac ky, tuc CHINH
    CLIENT cung khong biet. Nen phai nho lai luc no con mang theo.
    """
    import json, os
    username = str(username or "").strip()
    if not username:
        return set()
    path = _skill_cache_path()
    if not os.path.exists(path):
        return set()
    try:
        with open(path, encoding="utf-8") as fh:
            allc = json.load(fh) or {}
    except Exception:
        return set()
    entry = allc.get(username)
    if not isinstance(entry, dict):
        return set()
    return {int(x) for x in (entry.get("dac_ky") or []) if str(x).isdigit() or isinstance(x, int)}


def save_dac_ky_cache(username, npc_ids) -> bool:
    """GOP them npc_id vo cache dac ky (khong bao gio xoa: dac ky hoc roi la vinh vien)."""
    import json, os
    username = str(username or "").strip()
    moi = {int(i) for i in (npc_ids or ()) if i}
    if not username or not moi:
        return False
    with _skill_cache_lock:
        # `load_dac_ky_cache` KHONG lay lock (co y) -> goi trong day an toan. `_skill_cache_lock`
        # la Lock thuong, KHONG reentrant: dung cho ham nao lay lock vao day.
        cu = load_dac_ky_cache(username)
        if moi <= cu:
            return False
        path = _skill_cache_path()
        allc = {}
        if os.path.exists(path):
            try:
                with open(path, encoding="utf-8") as fh:
                    allc = json.load(fh) or {}
            except Exception:
                allc = {}
        entry = allc.get(username)
        if not isinstance(entry, dict):
            entry = {}
        entry["dac_ky"] = sorted(cu | moi)
        allc[username] = entry
        try:
            _ghi_json_an_toan(path, allc)
        except Exception as e:
            log.debug("ghi cache dac ky loi: %s", e)
            return False
        return True


def skills_snapshot(st):
    """{char, pet, pets:[[pid,ten,[choice]]], active} tu state - dung chung cho cache va GUI."""
    def _choice(sid):
        sid = int(sid)
        info = getattr(config, "SKILL_INFO", {}).get(sid, {}) or {}
        return [sid, info.get("name") or ("Skill %d" % sid), info.get("cost"), info.get("cat")]

    pet_skills = list(getattr(st, "pet_skills", []) or []) or list(getattr(st, "skills_pet", []) or [])
    pets = []
    for pid, nm in (getattr(st, "carried_pets", []) or [])[:4]:
        sks = set(getattr(config, "PET_SKILLS", {}).get(pid, []) or [])
        # DAC KY: chi hien cho con DA MO va bot co du lieu skill (giong pet_usable_skills)
        if (getattr(st, "pet_special_skill", None) or {}).get(pid):
            _sp = (getattr(config, "PET_SPECIAL_SKILL", {}) or {}).get(pid)
            if _sp and _sp in (getattr(config, "SKILL_INFO", {}) or {}):
                sks.add(_sp)
        sks = sorted(sks)
        pets.append([int(pid), nm or ("Pet 0x%04x" % pid), [_choice(x) for x in sks]])
    return {
        "char": [_choice(x) for x in sorted(list(getattr(st, "skills_char", []) or []))],
        "pet": [_choice(x) for x in sorted(set(pet_skills))],
        "pets": pets,
        "active": int(getattr(st, "active_pet_id", 0) or 0),
    }


def _pet_role(role):
    """Doi pet sang `role` truoc khi chay hoat dong, va LUON tra ve vai mac dinh khi xong.

    Dung try/finally o CHINH ham hoat dong thay vi dua vao vong lap ngoai: luong that co nhieu
    nhanh khong di qua vong lap keepalive cua run_account (bug that: danh PB xong khong doi lai
    pet train, log co "DOI PET" nhung khong bao gio doi nguoc).
    """
    def deco(fn):
        @functools.wraps(fn)
        def wrap(self, *a, **k):
            # Daily bam tay phai dung dung pet user da chon trong PET & Skill, khong de vai
            # boss/quest tu dong doi sang mot pet khac.
            if getattr(self, "_daily_use_selected_pet", False):
                return fn(self, *a, **k)
            try:
                self.ensure_pet_role(role)
            except Exception as e:
                log.warning("[%s] doi pet vai '%s' loi: %s", self._label, role, e)
            try:
                return fn(self, *a, **k)
            finally:
                try:
                    self.ensure_pet_role(getattr(self, "default_pet_role", "train"))
                except Exception as e:
                    log.warning("[%s] tra pet ve vai mac dinh loi: %s", self._label, e)
        return wrap
    return deco


class GameClient:
    def __init__(self, user_id: str, access_token: str, host: str = None, server_id: int = 1):
        self.user_id = user_id
        self.access_token = access_token
        self.host = host or config.GAME_HOST   # IP server (theo party); None -> mac dinh
        self.server_id = server_id             # ID server trong goi auth (1=Trieu Van, 2=Tao Thao)
        self.sock = None
        self.recv_buf = b""
        self._recent_sends = collections.deque(maxlen=40)  # (op, hex) - dump khi bi kick de debug
        self._recent_recvs = collections.deque(maxlen=40)  # (ts, op, hex) goi server gui - debug kick
        self.running = False
        self.state = BattleState()
        self.battle_tracker = BattleTracker()
        self.state.attach_tracker(self.battle_tracker)
        self._battle_party_key = None
        self._battle_party_coordinator = None

        # combat turn handling
        self.available = {}          # unit -> list (atype, target)
        self._acted_turn = False
        self._decision_timer = None
        self.auto_combat = True
        self.auto_accept_party = True
        self.party_invite_ready = False
        self._pending_party_invites = collections.OrderedDict()
        self.self_entity = None      # entity 8 byte cua nhan vat minh
        self.last_turn_time = 0.0    # thoi diem nhan luot/battle gan nhat
        self._label = ""             # nhan log: username luc dau, doi sang TEN NHAN VAT khi biet
        self._username = ""          # username login (key tra cuu config rieng tung acc)
        self._heal_giveup = {}       # target(0 char/1 pet) -> thoi diem het tam nghi hoi mau (con ket/da day)
        self._username = ""          # username login (giu lai de tham chieu)
        self.char_name = None        # ten nhan vat trong game (tu 0x27 theo self_entity)
        self.char_int = None         # chi so INT (tri luc) - tu S2C 0x08 id=0x1b
        self.char_agi = None         # AGI thuc te cua char sau khi cong do/collection/horse
        self.pet_agi = None          # AGI thuc te cua pet dang xuat chien
        self._char_int_base = None
        self._char_equip_int = 0
        self._char_turn3_int = 0
        self._char_agi_base = None
        self._char_equip_agi = 0
        self._char_turn3_agi = 0
        self.mount_level = 0         # cap thu cuoi (S:079-001 luc login, S:079-002 khi len cap)
        self.mount_points = {}       # kind(1..6) -> diem CONG DON (S:079-001, S:079-003)
        self._mount_level_ev = threading.Event()   # S:079-002 ve -> khong phai poll
        self._mount_point_ev = {}                  # kind -> Event (S:079-003)
        self._mount_base_int = 0
        self._mount_equip_int = 0
        self._mount_equip_agi = 0
        self._mount_collection_count = 0
        self.char_level = None       # cap nhan vat - tu S2C 0x05 (payload offset 21 = pkt[28])
        # EXP hien tai cua cap nhan vat: S2C 0x05/sub0300 payload +22, UInt32 LE.
        # De None cho toi khi server gui packet; UI ghi ro nguon 0x05 de user doi chieu.
        self.char_exp = None
        self.pet_exp_values = {}  # pet id/index -> EXP tong; S2C 0x08/sub02 cap nhat live
        # Nhat ky EXP tung account cho UI Android. Moi record giu ca raw kind de doi chieu
        # protocol: kind=1 char, kind=2 pet (can capture tran that de xac nhan kind pet).
        self.combat_exp_log = collections.deque(maxlen=100)
        self.activity_log = collections.deque(maxlen=200)  # EXP + nhat/do dung, rieng tung account
        # Thong ke farm theo phien login. EXP/gio dung thoi gian tu tran dau tien (gom ca
        # khoang nghi giua cac tran), nen phan anh toc do farm thuc te thay vi chi noi suy combat.
        self.exp_stats_started_at = None
        self.exp_stats_char_total = 0
        self.exp_stats_pet_total = 0
        self._metrics_battle_started_at = None
        self._metrics_battle_char_exp = 0
        self._metrics_battle_pet_exp = 0
        self._metrics_battle_char_exp_start = None
        self.last_battle_seconds = None
        self.last_battle_char_exp = 0
        self.last_battle_pet_exp = 0
        self.exp_stats_battles = 0
        self.pet_level = None        # cap pet dang dung - tu S2C 0x0f sub=08
        self.pet_levels = {}         # pid -> cap CUA TUNG con mang theo (0x0f, byte +7)
        # EVENT LIEN SERVER (vo gioi): lenh chuyen may tu S:001-020, va co dang o may do.
        self._vo_gioi = None
        self._vo_gioi_ve = False
        self._vg_acc = None       # cap acc/pwd server phat luc login, DUNG cho auth vo gioi
        self._vg_pwd = None
        self._connect_code = 0    # S:001-013 <登入送完所有資訊> +連線序號(4)
        self.tren_vo_gioi = False
        self._host_goc = None
        self.active_pet_slot = None  # SLOT TUI (1..4) cua pet dang xuat chien - tu 0x0f (marker)
        self.pet_name = None         # ten pet dang dung - tu S2C 0x0f sub=08
        self._cached_pet_list_pkt = None  # cache 0x0f de re-process khi 0x13 den sau
        self._gift_status = {}        # gtype -> status phan hoi (S2C 0x57: 01 diem danh, 04 qua 14 ngay)
        self._last_guild_pkt = None   # cache goi 0x27 (guild) de resolve ten neu toi truoc 0x69
        self.flee_mode = False        # True = dang di chuyen -> vao battle thi BO CHAY (khong danh)
        self.dungeon_complete = False  # True khi nhan goi hoan thanh dungeon (S2C 0x14 sub 0x64)
        self._machinebox_map_cam = set()   # map bi server DUNG hop may (S:065-002) = map CAM
        self._doi_kenh_tu = 0.0            # luc bat dau doi kenh (roster cua ca party dang bien dong)
        self._pb_ket_thuc_luc = 0.0    # luc nhan S:047-012 <副本結束> (pho ban TO DOI ket thuc)
        self.submit_delay = 0.5      # delay truoc khi gui combat
        self._first_turn = True      # luot dau tran -> atype=2, sau -> atype=3
        self._battle_entered = False # da gui 0x41 "vao tran" chua
        self.channels = {}           # {so_kenh: (so_nguoi, suc_chua)} - tu S2C 0x07 list
        self.current_channel = None  # kenh dang o (doc tu S2C scene/ack; None = chua biet/mac dinh)
        # True = lenh doi kenh gan nhat HONG (timeout / bi tu choi) nen `current_channel` chi con la
        # so nho lai, khong dung de ket luan "party lech kenh" duoc (xem `kenh_dang_chac`).
        self.kenh_dang_nghi_ngo = False
        self.current_channel_at = 0.0  # luc doc duoc (cu qua thi phai hoi lai - xem kenh_that)
        self._chan_event = threading.Event()
        self._chan_switch_event = threading.Event()
        # MOT ACC CHI CO MOT LENH DOI KENH DANG BAY. Co HAI duong cung ra lenh (dieu phoi tu gui +
        # `_nghe_lenh_kenh` cua chinh acc) nen khong khoa la hai luong cung gui, server tra MOT ket
        # qua, luong con lai TIMEOUT -> ghi `-1` -> dieu phoi dua kenh do vao so den OAN.
        self._chan_switch_lock = threading.Lock()
        self._chan_switch_target = None
        self._chan_switch_result = None
        self._chan_switch_luc = 0.0     # luc nhan ma tren (dieu phoi doc de biet con moi khong)
        self._ds_kenh_hoi_luc = 0.0     # luc GUI yeu cau danh sach kenh
        self._ds_kenh_nhan_luc = 0.0    # luc NHAN duoc `S:007-001` (bang `channels` moi tu day)
        self._ds_kenh_map = 0           # MAP luc nhan bang do - moi map mot danh sach kenh khac
        self._channel_scene_generation = 0
        self.server_closed = False   # True khi server CHU DONG dong ket noi (rot/bao tri/kick)
        self.disconnect_cause = 0    # ma ly do tu S:000-000 (0 = server khong noi ly do)
        self.disconnect_reason = ""  # dien giai ma tren (DISCONNECT_CAUSE)
        self._deliberate_close = False  # True khi CHINH TA dong socket (close/relogin) -> OSError ko phai rot
        self._gate_transit = False   # True khi dang gui chuoi 0x14 qua cong -> combat KHONG gui 0x32
        self._in_scene_gate = False  # True khi dang qua cong scene-walk -> in_combat() KHONG ha in_battle
        self._gate_choice_pending = False  # server dang CHO CHON trong su kien cong (resultType 6)
        self._gate_choice_try = 0          # da thu may ma trong GATE_CHOICE_CODES
        self._gate_event_logged = 0        # so buoc su kien da in trong lan qua cong nay
        self._gate_choice_sent_at = 0.0    # luc vua gui lua chon (chan 0x14 06 ngay sau)
        #   theo member-confirm (moi acc danh tran RIENG tai cong -> tin member se transit oan -> KICK)
        self.current_map = None      # map_id hien tai (doc tu broadcast 0x0c/0x07/0x03)
        self._mob_observer = None
        self._mob_observer_lock = threading.Lock()
        # Snapshot nhe cho UI ban do: entity -> (map, x, y, monotonic timestamp).
        # Khong dung de dieu khien bot; chi phan anh packet spawn/move server vua gui.
        self._world_entity_lock = threading.Lock()
        self.world_entities = {}
        self._mob_capture_lock = threading.RLock()
        self._mob_capture_target_map = None
        self._mob_capture_path = None
        self._mob_capture_file = None
        self._mob_capture_count = 0
        self._mob_capture_max_packets = 0
        self._pending_0b = []        # buffer 0x0b den TRUOC khi co self_entity (race login)
        self._pending_03 = None      # cache 0x03 self-spawn (resolve ten neu toi TRUOC 0x69)
        self.party_leader = None     # entity chu party (tu 0x0d sub=06)
        # Chi True trong khoang tu luc CHINH MINH vao doi den lan di dau tien: client teleport
        # member toi cho leader DUNG MOT LAN o `AddMember` (Team.lua:176), khong bam lien tuc.
        self._cho_bam_leader = False
        self.party_members = []      # list entity cac member theo thu tu (= slot B2)
        self._dg_enter_result = None      # ma S:097-001 <進入結果> lan vao DG gan nhat
        self._dg_enter_result_luc = 0.0
        self._dg_enter_event = threading.Event()   # server da tra loi S:097-001 chua
        # Ban do entity -> ID DOI TRUONG cua MOI party thay duoc quanh map (S:013-006 phat theo
        # map, mang nhieu party mot luc) = ban sao cua `Team.members` ben client Lua. Dung de
        # biet MINH dang ket trong party NAO (roi cho dung) va nguoi kia da o party chua.
        self.team_of = {}
        self.team_of_at = 0.0        # luc cap nhat gan nhat (roster cu thi khong tin nua)
        self.party_idx = None        # chi so party cua bot (tu config.ACCOUNT_PARTY) - de nhan moi cung party
        self.entity_names = {}       # entity(bytes) -> set(str) - TAT CA strings tim duoc tu 0x03/0x27
        self.entity_meta = {}        # entity(bytes) -> last seen scene/channel; dung loc nguoi dung canh minh
        self._run_around_lock = threading.RLock()
        self._run_around_generation = 0
        self._dg_pursuit_paused = False
        self._area_combat_mode = None
        self._running_route = False   # dang chay auto run-around
        self.chuyen_map_luc = 0.0     # lan cuoi acc nay teleport (doi map) - xem `go_to_town`
        self._luot_da_gui = None      # (gen, turn, luc) - luot gan nhat DA gui lenh danh (moi acc)
        self._luot_cham_da_bao = None # (gen, turn) da bao "LUOT CHAM" roi -> khong bao lai
        self._di_gioi_anchor = None   # TAM run-around DG = diem tele VAO Di Gioi (co dinh). Sau
        #   disconnect -> relogin, self.pos co the bi 0x03 keo ve rìa map -> run-around anchor theo
        #   pos se chay xuyen tuong. DG dung anchor CO DINH nay thay vi self.pos.
        self.pos = None              # vi tri hien tai (x,y) cua minh - doc tu S2C 0x03 self
        self._position_generation = 0  # tang khi server xac nhan self pos qua S2C 0x03
        self.train_block_stats_enabled = False
        self._block_stats_gen = None   # generation da ghi train_block_stats (1 lan/tran, tracker moi)
        self.train_block_map_id = None
        self.train_block_spot = None
        self.digioi_minutes = 0      # so phut DI GIOI hom nay (tu S2C 0x55 id=0x1b)
        self._last_digioi_ts = 0.0   # thoi diem nhan timer 0x1b gan nhat (0 = chua bao gio)
        self.role_counts = {}        # S2C 0x55 RoleCount: sid -> (value, max)
        self._server_online_seconds = None  # RoleCount id=10: so giay online hom nay server da tinh
        self._server_online_ts = 0.0
        self.shop_ho_phu_count = None # 0x0456 = so lan da mua Di Gioi Ho Phu hom nay
        self.shop_ho_phu_max = 3
        self.shop_thien_chau_count = None # 0x002b = so lan da mua Hop Thien Chau hom nay
        self.shop_thien_chau_max = 1
        self.shop_bao_hop_count = None # 0x0016 = so lan da mua Trieu Goi Bao Hop hom nay
        self.shop_bao_hop_max = 1
        self.dungeon_runs_today = None  # so luot dungeon da danh hom nay (S2C 0x55 stat 0x9b)
        self.gold = None             # Vang premium tren UI (moc user xac nhan: 707)
        self.money = None            # Dong/tien thuong (S2C 0x1a id=4; moc live 2,448,124)
        self.xu = None               # alias noi bo cu cua sid=4; giu de cac ham shop/gacha khong vo
        self.premium_xu = None       # dong tien premium tren UI; chua xac dinh packet
        self.currency_values = {}    # probe: toan bo sid/value S2C 0x1a de doi chieu UI game
        self._decompose_seq = 0      # tang moi khi nhan S2C 0x59 (xac nhan phan giai 1 cuon xong)
        self.furnace_shop = None     # ket qua soi lo (熔爐): {base_rate, active_rate, tabs:{kind:[items]}}
        self._furnace_seq = 0        # tang moi khi nhan S2C 0x59 sub01 (xac nhan soi lo xong)
        self._fashion_deposit_seq = 0  # tang moi khi nhan S2C 0x59 sub02 (tha do thoi trang xong)
        self._send_lock = threading.Lock()   # serial hoa sendall (nhieu thread gui: bot + GUI mua ho lo)
        self._bag_slot_price = None    # (money, kind) gia mua slot tui (0x54 sub01 sellId=3)
        self._bag_slot_price_seq = 0
        self._bag_slot_buy_seq = 0
        self._bag_slot_buy_result = 0
        # O RIENG cho TIEN TRANG (sellId=1). Dung chung o voi tui thi gia hai loai de len nhau.
        self._bank_slot_price = None
        self._bank_slot_price_seq = 0
        self._bank_slot_buy_seq = 0
        self._bank_slot_buy_result = 0
        self.bag_counts = {}         # tid (int) -> tong so luong (gom moi slot) - cho decompose/owns
        # slot -> du lieu MON CU THE (cuong hoa / da / dong phu / he...). Xem thing_data_info().
        # bag_slots chi co [id, so luong]; may thu nay nam trong 29 byte ma truoc day bot vut di.
        self.bag_items = {}
        # Lenh tui do bam TAY luc dang trong tran -> xep hang, het tran moi gui (xem queue_bag_cmd)
        self._bag_queue = []
        self._bag_flush_running = False   # dang co thread xa hang doi -> khong xa chong len
        # slot (int) -> [tid, count]. Snapshot tu S2C 0x17 sub05 (S:023-005) - chu thich cu ghi
        # "0x16 sub0400" la SAI (0x16=22 la MapNpc, khong dinh gi den tui). Use item = gui slot nay.
        # Server TU DAY snapshot, KHONG co lenh nao xin lai (da tra het 66 lenh C:023-*).
        self.bag_slots = {}
        self.pet_equipped = {}       # followIndex (1..4) -> [tid] do da mac cho pet (S2C 0x17 sub17)
        self._equip_seq = 0          # tang moi lan MAC XONG (char hoac pet) -> de XAC NHAN lenh
        # fitType (vi tri mac) -> tid dang mac. CAN de xu ly THAY DO: server KHONG gui goi cap nhat
        # tui cho mon bi thay ra (da doc protocolTable[23][17]: client TU dat lai vao tui tu trang
        # thai do dang mac cua chinh no). Khong co bang nay thi mon cu bien mat khoi tui.
        self.equip_by_fit = {}        # do cua NHAN VAT
        self.pet_equip_by_fit = {}    # followIndex -> {fitType: tid}
        # slot tui (1..4) -> ban ghi login cua TUNG pet (pet_login_stats.parse_record) + level/pid.
        # Co ban ghi nay thi tinh duoc HP/SP/AGI cua CA pet KHONG xuat chien.
        self.pet_login_records = {}
        self.equipped_items = []     # ThingData rut gon tu S2C 0x17 sub0b00 luc login.
        # SO PHUC THAN CON LAI (godMission trong client): tu S2C 0x18 sub0800
        # <設定衰神福神> [roleId i64][kind u16][count i32]. None = server chua gui.
        self.god_mission = None
        # "Tran nay minh da chet chua" - xoa moi khi vao tran moi (0x34). Xem _chot_minh_chet.
        self.chet_tran_nay = False
        # True = CO SU KIEN can xu ly Phuc Than NGAY (buff tut < PHUC_THAN_LOW hoac ngoc HONG),
        # khong phai cho het chu ky. Handler goi tin chi BAT co (chay o thread doc goi, khong duoc
        # gui/sleep o day); vong lap trong run_party_digioi TIEU THU khi khong con trong tran.
        self.phuc_than_pending = False
        self._active_pet_login = None
        self._pet_login_logged = None   # chu ky dong log PET login gan nhat
        self._collect_style_flags = {}
        self._collect_card_equipped = []
        self._collect_card_levels = {}
        self._bag_time = 0.0         # moc nhan snapshot tui gan nhat (cho log_bag_delayed adaptive)
        self._pending_confirm_slot = None  # slot dang cho S2C 0x17 sub09 xac nhan (probe confirm-gated)
        self._use_confirmed = False        # True khi nhan confirm cho _pending_confirm_slot
        self._no_item = set()        # (target,kind) het thuoc -> skip toi TRAN SAU (reset khi 0x34)
        self._quest_cells = set()    # o nhiem vu hang ngay DA HOAN THANH (S2C 0x5b 02 00 01 01 00 [cell])
        self._quest_missions = {}    # cell(1-9) -> missionId THAT tu S:91-1/91-4 (client lay tu day,
                                     # bot truoc hardcode 0x2f..0x37 - van la fallback)
        self.world_boss_count = None # MarkManager mission 12207 step = so luot boss the gioi da danh
        self.world_boss_max = WORLD_BOSS_MAX_ATTEMPTS
        self._world_boss_progress_loaded = False
        self._world_boss_progress_ts = 0.0
        self._claimed_lines = set()  # hang/cot DA NHAN thuong - suy tu BitFlag (giong client)
        # MOI bang 3x3 server gui (S:91-1), khong rieng bang daily: gid -> {cells:set, missions:{cell:mid}}
        # Bang EVENT doi theo thang (file .dat bi ghi de: panel 10 tu "thu thap chu" -> "Mung Game Ra
        # Mat Hai Thang") nhung CO CHE khong doi -> parse tong quat, khong phai sua code moi lan.
        self._quest_grids = {}
        self._claimed_by_grid = {}   # gid -> set(line DA NHAN)
        # DOI THUONG SU KIEN (0x7c): server gui toan bo danh sach -> cache ra JSON cho GUI.
        self._activities = {}     # activityId -> {title, kind, open, missions[]}
        self.char_attrs = {}      # CHI SO GOC tu 0x08: EAttribute -> gia tri (dung cho thanh tuu)
        # S:008-013 <設定主角技能>: he nhan vat + diem skill con lai + CAP tung skill.
        self.char_element = None      # 1..4 = he; hoc skill KHAC he ton GAP DOI learnPt
        self.char_skill_point = None  # diem skill con lai (server chot)
        self.char_skill_lv = {}       # skill_id -> cap HIEN TAI (khong co = chua hoc)
        # Chi so char doc tu goi 0x05 sub03 (Role.ReceivePlayerData). EAttribute -> gia tri.
        #   char_base  = so GOC (chua cong do) | char_equip = phan CONG TU TRANG BI
        self.char_base = {}
        self.char_equip = {}
        self.max_friend_count = None   # so ban TOI DA tung co (S:014-017) - dieu kien thanh tuu
        self.mark_flags = {}      # CO NHIEM VU (0x18 sub07/05): chi so BYTE -> gia tri byte
        self.pet_faith = {}          # pet_id -> TRUNG THANH (0..100), doc tu goi pet list
        self.pet_special_skill = {}  # pet_id -> DA MO dac ky chua (bool)
        self._mark_flags_loaded = False
        self._acts_expired = set()   # id su kien server bao DA HET (duoc xoa khoi cache)
        self._activity_done = {}  # missionId -> so lan DA LAM (S:124-001)
        self._activity_got = {}   # missionId -> so lan DA DOI (S:124-002)
        # TIEN SU KIEN (eResourceType.ActivityCoin = 1): KHONG nam trong tui -> doc rieng
        # (S:124-010 ten, S:124-011 so luong). Nguyen lieu su kien (Ngoc Thuc / Trang ...) thuong
        # la loai nay; moi cost/award deu co `kind`: 1 = tien su kien, 2 = vat pham tui.
        self._coin_names = {}     # coinId -> ten
        self._coin_quant = {}     # coinId -> so luong dang co
        self._claimed_loaded = False # da biet trang thai "da nhan" (de claim_daily_quests cho truoc)
        self._bitflag_bytes = bytearray()  # S2C 0x51 sub0200: bang BitFlag giong client
        self._bitflags_loaded = False
        self._online_gift_pending = None   # moc phut vua gui claim, do 0x57 response khong tra id
        self._online_gift_pending_ts = 0.0
        self._online_gift_next_log = None
        self._online_gift_last_log = 0.0
        self.mission_steps = {}  # mission_id -> step, tu S2C 0x18; dung de tinh luot pho ban doi
        self.mission_steps_loaded = False
        # (KHONG con `_team_dungeon_until`: "dang trong PB to doi" doc MAP THAT qua
        #  `in_team_dungeon()`. Moc `now + 20 phut` khong phai trang thai - xem ham do.)
        self._active_team_dungeon_level = None
        self._team_dungeon_end_seq = 0
        # Callback do coordinator cam TRUOC khi goi do_team_dungeon: True = co dong doi ROT.
        # Leader phai DUNG danh ngay (xem _td_party_gone).
        self._td_party_broken = None
        self._team_dungeon_reinforcement_seq = 0
        self._last_dialog_evt = 0.0  # lan cuoi nhan goi 0x14 lien quan thoai (de biet canh da HET that su chua)
        self._genuine_end_seen = 0.0  # thoi diem nhan goi 0x14 sub0800 tail=03/04 (ket tran THAT, moi context)
        self._battle_end_grace_until = 0.0  # < time.time() = vua nhan goi ket tran THAT -> 0x35 khong duoc set lai in_battle
        # THE HE tran ung voi grace o tren. Grace chi duoc chan goi 0x35 TAN DU CUA CHINH TRAN
        # DA KET (cung generation). Tran MOI da START (generation tang) -> grace PHAI HET hieu
        # luc ngay, khong thi luot 1 cua tran moi bi bo IM LANG (bug 40NPC: END 20:49:09, START
        # tran moi 20:49:11 -> ca 4 bot mat luot 1, cho server timeout 37s moi danh o luot 2).
        self._battle_end_grace_gen = -1
        self._battle_start_seq = 0     # tang moi S2C 0x34; 40NPC doi generation moi thay vi canh timing bool
        self._npc40_prompt_seq = 0     # tang khi co du cap 0x41 0a0001 + dialog 0x14 0100...0300
        self._npc40_prompt_pending = False
        self._npc40_prompt_pending_at = 0.0
        self._npc40_last_defeated = False
        self._npc40_hp_snap = None     # (defeated, alive, total) chot gan nhat khi allies con du lieu
        # HAN DUNG dich vu theo thoi gian (S:023-135) -> {"ba_dau"/"da_thu_ho"/"the_vip": datetime}
        self.han_dung = {}
        self._server_time_span = None   # gio server - gio may (dat o `_nho_gio_server`)
        self._npc40_bo_thuong = False  # thoat som (thua 2 tran, chua toi 22h) -> doi thuong se khong an
        self._npc40_last_alive = 0
        self._npc40_last_total = 0
        self._npc40_last_dialog = ""   # hex page dialog NPC 40 (0x14 0100...) gan nhat -> biet fresh/giua-event
        self._npc40_done = False       # run_loop bao: het gio event / thua 2 tran -> di doi thuong + thoat
        self._npc40_started = False
        self._npc40_stop = threading.Event()
        self._npc40_thread = None
        # --- LOAN DAU LOI DAI (solo, khong party). Xem bot/loandau.py + documents/LOAN_DAU.md ---
        self._loandau_registered_seq = 0   # tang moi `0x14 08 2a` = dang ky xong, bat dau cho ghep
        self._loandau_create_seq = 0       # tang moi `0x0b sub 250` = TAO TRAN (moc vao tran that)
        self._loandau_end_seq = 0          # tang moi `0x14 08 26` = tran/phien ket thuc
        self._loandau_dialog = ""          # hex page dialog gan nhat -> biet da toi buoc chon chua
        self._loandau_wins = 0             # so tran thang, doc tu `0x25` sub01 (lo cuoi moi vong)
        self._loandau_done = False         # run_loop bao: het gio -> di doi thuong + thoat
        self._loandau_started = False
        self._loandau_stop = threading.Event()
        self._loandau_thread = None
        # Vai tro pet MAC DINH khi khong o trong hoat dong nao (decorator _pet_role tra ve day).
        # run_party_digioi dat "quest" cho mode event (event dung chung pet voi quest/PB).
        self.default_pet_role = "train"
        self._pet_switch_fail = {}   # pet_id -> so lan doi HUT (xem PET_SWITCH_MAX_TRY)
        self._o5_team_fn = None      # hook (set boi run_party_digioi): xu ly o5 pho ban to doi - BUOC CUOI
                                     #   claim_daily_quests. Nhan o5_done (bool). Leader phoi hop ca party.
        self.friend_entities = []    # entity 8B cua ban be (S2C 0x0e 05 push luc login)
        self.friend_status = {}      # entity hex -> trailer[18] (功能標記): bit0x01=DA TANG, bit0x02=CO QUA nhan, bit0x04=DA NHAN
        self.friend_online = {}      # entity hex -> bool online (S2C 0x0e 05 / 0x0e 10)
        self._gift_recv = 0          # dem qua ban tang da nhan (S2C 0x0e 0d xac nhan nhan 1 qua)
        self.vantieu_started = None  # so luot van tieu DA gui hom nay (S2C 0x55 sid=0x08)
        self.vantieu_max = 3         # gioi han van tieu/ngay (server bao kem, mac dinh 3)
        self.vantieu_slots = {}      # slot -> {"end": OLE date ket thuc, "pet": id} (tu panel 0x56 0300)
        self._vantieu_claim_pending_slot = None
        self._vantieu_claim_result = None  # (slot, result_code) tu S2C 0x56/0500
        self._vantieu_claim_event = threading.Event()
        self._vantieu_claim_retry_after = 0.0
        # BOSS QUAN DOAN (server day luc login): count = so lan da danh hom nay (0x27 70), next = gio
        # danh tiep duoc = cooldown end epoch (0x27 76 OLE date). Server tu track -> khoi doan local.
        self.legion_boss_count = 0   # so lan DA danh boss QD hom nay (S2C 0x55 id 0x2a cur)
        self.legion_boss_max = 3     # gioi han/ngay (0x55 id 0x2a max = 3)
        self.legion_boss_next = 0.0  # gio danh tiep duoc (cooldown, S2C 0x27 76 OLE)
        # Gio duoc THU DONATE lai. Server bao "chua du 24h trong quan doan" (S:039-015 ma 32) ->
        # hoan 12h. Client khong co cach nao biet truoc, chi gui roi doc ma loi.
        self.legion_donate_next = 0.0
        # CO QUAN DOAN hay khong. MAC DINH False (KHONG phai None) - BUG THAT xac nhan qua capture
        # THAT SU: acc KHONG co quan doan thi server KHONG BAO GIO gui goi 0x27 sub=02 (guild info)
        # ca - KHAC voi gia dinh ban dau la "gui goi voi guild_len=0". Neu de mac dinh None se KET
        # LUAN SAI (mai mai None, khong bao gio thanh False) khien do_legion_boss() van chay logic cu.
        # Chi set True khi THUC SU nhan duoc goi 0x27 sub02 voi guild_len>0 (xem _on_player_info).
        # 0x55 id=0x2a (legion_boss_count/max) KHONG dung duoc: tra val=0/max=3 CA 2 TRUONG HOP
        # "khong co quan doan" LAN "co quan doan nhung chua danh", khong phan biet duoc.
        # Dung de do_legion_boss() BO QUA hoan toan (khong gui 0x27 7700/0x14 08000100 vao instance
        # khong hop le) khi has_legion=False - tranh lam roi trang thai map/transition truoc khi vao
        # Di Gioi (goc re bug "vao Di Gioi that bai du fresh login").
        self.has_legion = False
        # True = donate_legion DA MO PANEL QUAN DOAN va XAC NHAN acc KHONG co quan doan.
        # has_legion mac dinh False nen 'chua biet' va 'khong co' KHONG phan biet duoc -> phai co
        # co xac nhan rieng nay truoc khi DAM BAN nguyen lieu (xem _sell_donate_materials).
        self._no_legion_confirmed = False
        self.org_id = None           # ID quan doan doc tu 0x05 sub03 (0 = KHONG co, giong client)
        # Setting party "Danh boss QD" (Cai dat nang cao, mac dinh BAT) - run_party_digioi.py set
        # lai theo pcfg ngay sau login. Mac dinh True o day de test/goi truc tiep khong bi chan.
        self.fight_legion_boss = True
        # Setting party "Tu ban Noi Dat" (mac dinh BAT). run_party_digioi.py chi bat setting nay
        # cho mode train/city, va pre-route chi thuc hien khi random tele ve Ng.Thanh.
        self.auto_sell_noi_dat = True
        # TU CAT DO vao tien trang: MAC DINH TAT + list rong (giong tick mo ruong trang bi) -
        # cat do la thao tac mot chieu, khong tu y lam khi user chua chon mon nao.
        self.auto_cat_do = False
        # TU MO RONG TIEN TRANG: nguong vang (0 = TAT). Mac dinh TAT - mo o ton vang cua user,
        # giong tick mo rong tui do. Chi co tac dung luc kho DANG MO (xem `tu_mo_rong_tien_trang`).
        self.bank_expand_gold = 0
        self.bank_fail = None
        # tid cac mon CAT HONG trong phien nay (kho day + mon CHUA co stack san). Nho lai
        # de khong quay lai NPC tien trang vi chinh may mon do nua - do la ly do bot dung
        # li o tien trang (user bao 06/09). Nho THEO PHIEN: relogin la quen, vi luc do kho
        # co the da duoc don trong.
        self._cat_hong = set()
        # tid cac mon LAY RA hong trong phien (tui day + mon chua co stack san trong tui).
        self._lay_hong = set()
        # Ket qua lan moi party gan nhat, cho DIEU PHOI doc (xem invite_members):
        # {"luc", "lech_kenh": [entity], "lech_map": [entity], "tong"} hoac None.
        self._moi_bo_qua = None
        # Su kien NPC dang mo hay khong, va tien trang da mo chua. Gui lenh khi CHUA mo la
        # ban vao khoang khong - te nhat la server ngat "su kien vi pham". Xem cat_do_tien_trang.
        self.event_dang_mo = False
        self.bank_open = False
        self.bank_slots = {}        # {idx trong kho: (tid, count)} - doc tu S:030-001/004/005
        # "Tu don tui do" (Cai dat nang cao) = CONG TONG cua 3 muc con ben duoi; tat -> ca 3 ngung.
        self.auto_bag_clean = True
        self.auto_discard_junk = True        # vut item rac (Ngoc Hu) - mac dinh BAT
        self.auto_decompose_scrolls = False  # phan giai cuon vo tuong rac - mac dinh TAT (an toan:
                                             # phan giai la MAT HAN, user phai tu tick va soat list)
        self.scroll_modes = {}               # {tid: "keep"|"drop"} - override so voi mac dinh vkcd
        self.material_modes = {}             # {tid: "keep"} - nguyen lieu GIU lai (mac dinh donate het)
        self.auto_donate_materials = False   # tick "tu dong gop nguyen lieu quan doan" (GUI)
        self.vantieu_req_code = None # ma yeu cau slot ke tiep (0x56 0400, hex b0b1b2) - fallback VANTIEU_REQUESTS
        self.vantieu_req = None      # {he, doanh} decode tu DispatchBonus_C.dat (0400 effect1/effect2)
        self.vantieu_roster = {}     # index pet KHO (1-based) -> ten (S2C 0x1f 0600 luc login) -> tra PET_HEDOANH
        self.vantieu_roster_ids = {} # index pet KHO -> NPCID (pet id) - KHOA ON DINH de user tick
        self.vantieu_enable = True   # per-acc (thay config.VANTIEU_ENABLE cu, la cong tac CHUNG)
        self.vantieu_pick_ids = ()   # pet id user DA TICK. rong / tick HET = dung TAT CA (nhu cu)
        self.vantieu_unlocked = 1    # so slot DA MO (S2C 0x56 0600 [N]); slot con lai khoa = can vang
        self._dg_query = None        # raw S2C 0x54 (tra loi query luot dungeon)
        self._dg_query_event = threading.Event()
        self._daily_date = None      # ngay local cua bo dem hien tai
        self._connect_time = None    # thoi diem connect phien nay
        self._online_base = 0.0      # giay online TICH LUY hom nay (load tu file, truoc phien nay)
        self.claimed_gifts = set()   # cac moc qua online da nhan hom nay (load tu file)
        self._mail_ids = []          # [(mailid 4B, state, so dinh kem)] tu S:083-001
        self._mail_taken = set()     # mailid server XAC NHAN da nhan qua (S:083-002)
        self._event14_items = []     # itemid event "qua 14 ngay" tu S2C 0x7c sub=01 (de nhan)
        self._event14_ok = 0         # so phan nhan THANH CONG (S2C 0x7c sub=02 byte ok=01)
        self._event14_acks = []      # raw ack S2C 0x7c sub=02 (debug)
        self._event14_bagfull = False  # True neu server tra code 06 (tui day)

    # ---- ket noi + auth ----
    def connect(self):
        self.state.label = self._label
        # Setting rieng acc "Char đứng Phòng thủ..." (accounts.json heal.char_defend -> config.
        # ACCOUNT_CHAR_DEFEND). Set o day vi _username duoc caller gan TRUOC khi connect().
        self.state.char_defend = bool(getattr(config, "ACCOUNT_CHAR_DEFEND", {})
                                      .get(getattr(self, "_username", None), False))
        self.state.battle_config = dict(getattr(config, "ACCOUNT_BATTLE", {})
                                        .get(getattr(self, "_username", None), {}) or {})
        self._ui_selected_pet_id = int(getattr(config, "ACCOUNT_SELECTED_PET", {})
                                       .get(getattr(self, "_username", None), 0) or 0)
        self._daily_date = _gift_day()
        self._connect_time = time.time()
        # Qua online dung state server (0x55 RoleCount id=10 + 0x51 BitFlag), khong dem local nua.
        self._online_base = 0.0
        self.claimed_gifts = set()
        self._server_online_seconds = None
        self._server_online_ts = 0.0
        self._bitflag_bytes = bytearray()
        self._bitflags_loaded = False
        self._online_gift_pending = None
        self._online_gift_pending_ts = 0.0
        self._online_gift_next_log = None
        self._online_gift_last_log = 0.0
        self.sock = _open_game_socket(self.host, config.GAME_PORT)
        log.info("[%s] Da ket noi %s:%s", self._label, self.host, config.GAME_PORT)
        self.sock.sendall(build_auth_packet(self.user_id, self.access_token, self.server_id))
        log.info("[%s] Da gui auth (user_id=%s, server_id=%s)", self._label, self.user_id, self.server_id)
        self.running = True
        threading.Thread(target=self._recv_loop, args=(self.sock,), daemon=True).start()
        threading.Thread(target=self._heartbeat_loop, daemon=True).start()
        self._login_setup()   # chuoi setup sau auth -> char thanh combat-active (quai moi aggro)

    def machinebox_payload(self) -> bytes:
        """Payload C:065-001 <啟動機關盒> - BAT hop may tu danh (MachineBox.lua:377-389).

        Thu tu truong dung y WriteByte/WriteBoolean cua MachineBox.SetAutoFight:
          [0] nguong HP% char      -> 0x32=50: HP duoi 50% thi hop may UONG BINH len ~50%
          [1] nguong SP% char      -> 0x35=53   (day la nguong UONG BINH, KHONG phai nguong dung auto)
          [2] nguong HP% tuong     [3] nguong SP% tuong
          [4] het binh HP -> ve thanh   [5] het binh SP -> ve thanh   (deu TAT)
          [6] CHAR chet -> ve thanh     <- theo tick "Char chết về thành"
          [7] TUONG chet -> ve thanh    <- theo tick "Pet chết về thành"
          [8] (hang false)  [9] tu dung do EXP  [10] tu doi chuy
        Client THAT mac dinh BAT ca [6] va [7] (MachineBox.Initialize dong 220) nen 2 tick cung
        mac dinh BAT -> khong doi hanh vi cu. CLIENT KHONG he tu xu ly 2 co nay (ca file chi co 2
        dong WriteBoolean, khong cho nao kiem tra "chet chua") -> SERVER thi hanh.
        """
        _ve_thanh = not self.in_pb_quest_event()
        return bytes([0x32, 0x35, 0x01, 0x01, 0x00, 0x00,
                      1 if (_ve_thanh and getattr(self, "death_return_town", True)) else 0,
                      1 if (_ve_thanh and getattr(self, "pet_death_return_town", True)) else 0,
                      0x00, 0x00, 0x00])

    def in_team_dungeon(self) -> bool:
        """DANG DUNG TRONG map pho ban to doi? Doc MAP THAT, khong doan theo dong ho.

        Truoc day cau hoi nay tra loi bang `time.time() < self._team_dungeon_until` - mot moc
        `now + 20 phut` dap luc vao PB. Moc thoi gian KHONG PHAI TRANG THAI (L1b): PB xong sau 2
        phut thi 18 phut con lai acc van tu nhan "dang trong pho ban", trong khi `current_map` noi
        ro no dang dung o thanh.

        Leader co `finally: self._team_dungeon_until = 0.0` nen thoat duoc. MEMBER tu accept loi
        moi (`_on_dungeon`) chi SET moc, KHONG CO CHO NAO HA -> ket den het 20 phut.

        Ca that party 44, 14/09 (user: "lai moi dua 1 thanh, ve thanh roi ma van bao dang o PB doi,
        dieu phoi ngu qua, dang o map nao ma cung ko biet"):
            10:36:51 [tp601] (LEADER) === PHO BAN TO DOI LV110: tao + moi 4 member ===
            10:38:29 [tp601] THOAT PHO BAN TO DOI (C:047-010)      <- PB xong sau 98 giay
            10:38:30 [tp601] khong o trong pho ban to doi (map=12001) -> KHONG gui C:047-010
            10:39:39 [tp602] go_to_town: DANG TRONG pho ban to doi (map=12001) -> khong teleport
            10:49:59 [tp604] go_to_town: DANG TRONG pho ban to doi (map=12001) -> khong teleport
        12001 = Cua thanh Trac Quan. Chinh con so in trong dong log da noi no khong o PB nao, nhung
        code khong doc con so cua chinh no. Ca party nam moi dua mot thanh [12001, 14001], dieu phoi
        ra lenh gom moi 3 phut tu 10:38 den 10:54 (gen 4->9) ma khong ai di duoc.

        `current_map` cap nhat tuc thi theo goi doi scene, khong tre, khong can ai ha co.
        """
        return int(getattr(self, "current_map", 0) or 0) in TEAM_DUNGEON_MAPS

    def in_pb_quest_event(self) -> bool:
        """Dang o pha PHO BAN / QUEST / EVENT? (khac voi train thuong)

        Cac pha nay bi KEO VE THANH giua chung la VO LUOT: dang danh pho ban ma chet, server keo
        ve thanh -> mat luot PB, ca party phai lam lai. Train thi nguoc lai: ve thanh la dung
        (hoi mau, khoi nam do). Nen 2 co "chet ve thanh" chi BAT o train.
        - trong pho ban to doi: dang dung tren map PB (`in_team_dungeon`)
        - quest/event: state.quest_mode (mode event bi ep quest_mode - xem force_quest_mode)
        """
        if self.in_team_dungeon():
            return True
        return bool(getattr(getattr(self, "state", None), "quest_mode", False))

    def sync_machinebox_flags(self):
        """Gui lai 0x41 khi 2 co "chet ve thanh" DOI (train <-> PB/quest/event).

        Chi gui KHI THUC SU DOI (so voi lan gui truoc) va KHONG gui giua tran - tranh chen goi vao
        luc dang danh. Goi dinh ky tu vong keepalive, re: khong doi thi khong lam gi.
        """
        if not self.running:
            return False
        pl = self.machinebox_payload()
        if pl == getattr(self, "_machinebox_last_payload", None):
            return False
        if getattr(getattr(self, "state", None), "in_battle", False):
            return False        # dang danh -> de lan sau, khong chen goi giua tran
        try:
            self.send(0x41, b"\x01\x00" + pl)
        except OSError:
            return False
        self._machinebox_last_payload = pl
        log.info("[%s] HOP MAY: %s pha PB/quest/event -> chet ve thanh = %s/%s (char/pet)",
                 self._label, "VAO" if self.in_pb_quest_event() else "RA KHOI",
                 bool(pl[6]), bool(pl[7]))
        return True

    def _login_setup(self):
        """Chuoi C2S client THAT gui NGAY sau auth (capture login.pcap). Thieu chuoi nay ->
        char ket noi nhung KHONG combat-active -> quai tren map thuong NGO LO bot (khong aggro).
        (DG van danh duoc du thieu, nhung map thuong thi BAT BUOC.)

        LUU Y: ghi chu cu ghi "quan trong nhat la 0x41 dang ky san sang battle" la QUY SAI CONG -
        0x41 la HOP MAY TU DANH (機關盒), va chinh ghi chu do cung viet "gui lai moi 0x41 KHONG du,
        phai gui lai TOAN BO chuoi" => thu co tac dung nam o goi KHAC, chua xac dinh duoc goi nao."""
        # Chuoi nay CO CHU Y gui 0x41 0200 (C:065-002 = TAM DUNG hop may) roi cuoi chuoi moi BAT
        # lai. Server ACK lenh do bang S:065-002 -> handler bat S:065-002 phai BIET day la ACK
        # cua CHINH MINH, khong thi no tuong bi dung ngoai y muon va CHEN goi bat-lai vao GIUA
        # chuoi login (log that 22:46:51 acc dieubon: 0x41 0200 -> 0x41 start CHEN -> 0x0c 0100 ...
        # -> chuoi lai gui start lan 2). Dat moc truoc khi gui de handler bo qua.
        self._machinebox_pause_sent_at = time.time()
        seq = [(0x19, "2900f0"), (0x2b, "0400"), (0x01, "1000"), (0x7c, "0400"),
               (0x41, "0200"), (0x0c, "0100"), (0x57, "0300"), (0x01, "1000"),
               # game client gui 2 goi 0x62: 020002 (trigger server day frame 0x51 daily-reward) + 020001.
               # Thieu 020002 -> bot KHONG nhan 0x51 -> khong biet line da nhan.
               (0x62, "020002000000"), (0x62, "020001000000"),
               # BAT hop may - 2 co "chet ve thanh" theo tick cua user, xem machinebox_payload()
               (0x41, "0100" + self.machinebox_payload().hex())]
        for op, pl in seq:
            try:
                self.send(op, bytes.fromhex(pl))
            except OSError:
                return
            time.sleep(0.2)

    def combat_ready(self):
        """Sau khi DOI KENH / lap party, char co the mat combat-active -> gui LAI toan bo
        chuoi setup (gom 0x41 'san sang battle') de quai aggro lai."""
        self._login_setup()

    # Ma LUA CHON cua hop thoai su kien - doc tu client (Logic_Event_EventHandler.lua:429-436,
    # 450-453 + EventManager.SelectEvent -> C:020-009 <事件選擇> +選擇碼(1)):
    #     Co/Khong : 20 = CO, 21 = KHONG
    #     Danh sach: 30 + so thu tu (muc 1..10) | 60 + (thu tu - 9) (muc 11+)
    #     Dong/huy : 40
    # Doi chieu: bot da dung san `0x14 09 1e` (0x1e = 30 = muc dau danh sach) cho teleport boss.
    EVENT_YES, EVENT_NO, EVENT_CLOSE = 20, 21, 40

    # Thu tu ma thu khi server CHO CHON o cong (resultType 6).
    #
    # 30 DUNG DAU - KHONG con doan: capture `captures/gioikieu_20260825.pcap` (user tu bam trong
    # client that o cau Gioi kieu, cong 10 map 63000: 2 lan bam "khong danh", lan 3 bam "danh"):
    #     0x14 08 0a00 -> 0x14 09 001f (31) -> 0x14 06     khong danh
    #     0x14 08 0a00 -> 0x14 09 001f (31) -> 0x14 06     khong danh
    #     0x14 08 0a00 -> 0x14 09 001e (30) -> 0x14 06 -> 0x32   DANH -> vao tran -> qua cau
    # => cua nay la DANH SACH (30 = muc 1 = danh, 31 = muc 2 = khong danh), KHONG phai hop
    # Co/Khong. Ma 20 toi tung doan la SAI han - server tra "su kien vi pham" roi ngat ket noi.
    #
    # 20 giu lai o vi tri 2 cho cua nao that su la hop Co/Khong; 40 cuoi de dong bang, khoi treo.
    GATE_CHOICE_CODES = (30, 20, 31, 40)

    # resultType trong <一般事件> (Logic_Event_EventHandler.lua, bang EventHandler[...]):
    #   1 = Thoai      -> client tra `0x14 06` (buoc tiep)
    #   3 = Vao tran   -> danh
    #   6 = TUONG TAC  -> server DUNG LAI cho `0x14 09 + ma`
    EVENT_RESULT_TALK, EVENT_RESULT_FIGHT, EVENT_RESULT_INTERACT = 1, 3, 6

    def _on_route_dialog(self, pkt: bytes):
        """Bat luc server CHO CHON trong su kien cong (vd cau Gioi kieu tren duong Trac Quan).

        LOI CU (log 16:46, bot ket o cong idx=10 map 63000): ham nay bat theo `sub == 01` roi tra
        loi NGAY gois DAU TIEN, moi map dung mot lan. Nhung crack client cho thay `S:020-001..006`
        deu vao chung `EventManager.ReceiveCommonEvent` - sub KHONG phai loai buoc. Loai buoc nam o
        `resultType` trong payload (byte +4). Goi dau tien gan nhu luon la THOAI (resultType 1),
        nen bot bam chon vao luc server chua hoi -> roi tu khoa, khong tra loi luc server hoi that
        -> server treo o buoc chon -> cong khong sang map.

        Gio bat DUNG `resultType == 6` (tuong tac). Chi DANH DAU o day; viec gui do `_enter_gate`
        lam - de moi lenh gui van nam trong luong worker, khong chen ngang luong recv (giu nguyen
        rang buoc cua `_gate_transit`).
        """
        if not getattr(self, "_in_scene_gate", False):
            return          # hop thoai cho khac (40NPC, NPC nhiem vu) da co duong rieng
        sub = pkt[7]
        if pkt[8] != 0 or not (1 <= sub <= 6) or len(pkt) < 14:
            return
        rtype = pkt[9 + 4]
        # In toi da 8 buoc su kien moi lan qua cong: cong nao ket thi doc log la biet server gui
        # gi, KHOI phai doan. (Hai lan truoc toi doan sai nen cong van ket.)
        if self._gate_event_logged < 8:
            self._gate_event_logged += 1
            log.info("[%s] CONG map %s: buoc su kien sub=%d resultType=%d goi=%s",
                     self._label, self.current_map, pkt[7], rtype, pkt[7:].hex())
        if rtype != self.EVENT_RESULT_INTERACT:
            return          # thoai/tran/hieu ung -> vong `0x14 06` cua _enter_gate lo
        if self._gate_choice_pending:
            return
        self._gate_choice_pending = True
        log.info("[%s] CONG map %s: server CHO CHON (resultType 6) goi=%s -> se tra loi",
                 self._label, self.current_map, pkt[7:].hex())

    def _send_gate_choice(self) -> bool:
        """Tra loi mot buoc CHON o cong. True = da gui.

        Luot thu ma nam o _GATE_CHOICE_STATE (cap module) chu KHONG o client: chon SAI ma thi
        server ngat ket noi luon (log 17:44: ma 20 -> "su kien vi pham ma 5"), client bi tao lai,
        nen neu dem o client thi lan sau lai bat dau tu ma 20 -> ket vinh vien. Dem o cap module
        thi moi lan chay tien MOT buoc, chay may lan la ra ma dung, va ma dung duoc GHI NHO.
        """
        if not self._gate_choice_pending:
            return False
        self._gate_choice_pending = False
        key = getattr(self, "_gate_choice_key", None)
        st = _GATE_CHOICE_STATE.setdefault(key, {"try": 0, "ok": None})
        if st["ok"] is not None:
            code = st["ok"]
            log.info("[%s] CONG map %s: chon ma %d (ma DA BIET dung cho cong nay)",
                     self._label, self.current_map, code)
            self._gate_choice_last = code
            try:
                self.send(0x14, b"\x09\x00" + bytes([code]))
            except OSError:
                return False
            self._gate_choice_sent_at = time.time()
            return True
        i = st["try"]
        st["try"] += 1
        if i >= len(self.GATE_CHOICE_CODES):
            return False
        code = self.GATE_CHOICE_CODES[i]
        self._gate_choice_last = code
        log.info("[%s] CONG map %s: chon ma %d (dang do ma, lan thu %d/%d)",
                 self._label, self.current_map, code, i + 1, len(self.GATE_CHOICE_CODES))
        try:
            self.send(0x14, b"\x09\x00" + bytes([code]))
        except OSError:
            return False
        self._gate_choice_sent_at = time.time()
        return True

    def _gate_next_blocked(self) -> bool:
        """True = VUA chon xong, chua duoc gui `0x14 06`.

        `0x14 06` la <事件下一步>. Bam "buoc tiep" ngay sau khi chon, luc server con dang xu ly,
        cung bi tinh la SAI THU TU. Cho server bat nhip roi hang tinh."""
        return time.time() - getattr(self, "_gate_choice_sent_at", 0.0) < 3.0

    def _dump_recent(self, ly_do: str):
        """In 24 goi gan nhat, GUI va NHAN TRON CHUNG theo dung THU TU THOI GIAN.

        Truoc day CHI in o nhanh "empty data". Con 2 nhanh kia (OSError = connection reset, va
        "server im lang" = half-open) thi im -> mat rot ma khong biet vi sao. Log 15:37 dinh dung
        cho do: 5 acc deu rot, khong mot dong goi-cuoi nao de lan.

        VA phai TRON CHUNG hai chieu: truoc day in hai danh sach RIENG, moc thoi gian lai chi toi
        GIAY -> khong biet bot gui `0x32` TRUOC hay SAU khi server gui `0x35` offer cua luot do.
        Dung dieu can biet nhat khi dieu tra vu rot ma 42 `修改戰鬥封包` o loan dau (01/09).
        """
        log.warning("[%s] === GOI GAN NHAT TRUOC KHI ROT (%s) ===", self._label, ly_do)
        _goi = ([(ts, ">>gui ", op, hx) for ts, op, hx in self._recent_sends]
                + [(ts, "<<nhan", op, hx) for ts, op, hx in self._recent_recvs])
        for ts, chieu, op, hx in sorted(_goi, key=lambda x: x[0])[-24:]:
            log.warning("[%s]   %s.%03d %s 0x%02x %s", self._label,
                        time.strftime("%H:%M:%S", time.localtime(ts)), int(ts * 1000) % 1000,
                        chieu, op, hx)

    def scene_resume(self, settle: float = 0.6):
        """SAU KHI DOI SCENE (qua cong / len thuyen / thang tran phuc kich o cong) client THAT
        gui `0x0c 01 00` roi `0x14 06 00`; server tra `0x14 08 2a` -> MOI di chuyen duoc.
        Thieu buoc nay server NUOT lenh move -> char DUNG IM (bug 'qua map bien khong sail').

        Xac nhan tren CA 2 capture thuyen (thuyen_thanhchau + thuyen_thanhchau2): MOI lan
        s2c `0x14 08 2a` deu co dung chuoi `0x0c 01` -> `0x14 06` ngay truoc, va move dau tien
        chi den SAU do 0.1-0.5s. Client that KHONG he gui 0x41 (rearm) o cac cho nay.

        PHAI CHO HET TRAN HAN: `0x14 06` la `C:020-006 <事件下一步>` (buoc tiep SU KIEN). Gui luc
        server con dang giai tran -> `S:000-000` ly do **47** `戰鬥未結束事件先結束` ("su kien ket
        thuc khi tran chua xong") -> DUT KET NOI.
        Log 02/09 12:05:10 (party 3, 4 acc cung mot giay) va 11:54:28: 5/5 lan dinh ma 47 deu co
        `scene_resume` chay ngay truoc do. Cho goi o `_theo_leader_sua_pos` DA CO guard
        `not in_combat()` ma van dinh: goi NHAN cuoi cung la `0x14 08` (END tran), 23ms sau la goi
        ngat - tuc bot thay `in_battle=False` roi gui NGAY, con server thi chua giai xong.
        => guard phai nam TRONG day (phu ca 4 duong goi) va phai cho HET CA GRACE ket tran.
        """
        self._need_scene_resume = False
        _han = time.time() + 20.0
        while self.running and time.time() < _han:
            if not self.in_combat(idle_secs=1.0) and not self._in_battle_end_grace():
                break
            time.sleep(0.3)
        else:
            if self.running:
                log.warning("[%s] scene_resume: cho het tran qua 20s -> BO gui `0x14 06` "
                            "(gui luc dang giai tran = server ngat ma 47)", self._label)
            return
        try:
            self.send(0x0c, b"\x01\x00"); time.sleep(settle)
            self.send(0x14, b"\x06\x00"); time.sleep(settle)
        except OSError:
            pass

    def rearm_ready(self):
        """Gui LAI rieng 0x41 'san sang' - de DI CHUYEN duoc sau khi qua cong / danh tran phuc kich
        tai cong (server nuot move neu char chua 'ready' -> dung im du bot tuong da di - GIONG team
        dungeon lv20 phai combat_ready sau START phong). CHI 0x41 (khong full _login_setup) de tranh
        0x7c 0400 (co the anh huong thuyen) + 0x62 (side-effect daily). Comment _login_setup: 'quan
        trong nhat la 0x41'."""
        for pl in ("0200", "01003235010100000101000000"):
            try:
                self.send(0x41, bytes.fromhex(pl)); time.sleep(0.2)
            except OSError:
                return

    def send(self, opcode: int, payload: bytes):
        if not self.running or self.sock is None:
            return   # da rot ket noi -> bo qua (timer combat co the fire sau khi socket dong)
        if opcode != protocol.OP_HEARTBEAT:
            log.debug("[%s] SEND op=0x%02x: %s", self._label, opcode, payload.hex())
            self._recent_sends.append((time.time(), opcode, payload.hex()))
        try:
            with self._send_lock:
                self.sock.sendall(protocol.encode(opcode, payload))
        except OSError:
            self.running = False   # socket dong -> dung gui, dung moi vong lap
            if not self._deliberate_close:
                # ROT phat hien qua SEND: recv loop se thay running=False -> THOAT NGAY, KHONG kip
                # vao nhanh set server_closed. Thieu dong nay -> supervisor coi la "thoat binh thuong"
                # -> member CHET IM (tat), khong reconnect (bug thha/sga012/chu703 chet sau khi join).
                self.server_closed = True

    def leave_team_dungeon(self, wait: float = 6.0) -> bool:
        """THOAT PHO BAN TO DOI bang dung lenh cua client: C:047-010 <離開組隊>.

        Crack client: UIDungeon.OnClickTeamExit (nut "Thoat" cua UI to doi) -> Dungeon.SendLeaveTeam()
        -> Network.Send(47, 10), KHONG co payload. Server tra S:047-010 [roleId i64][result 1B]:
            0 tu roi to doi | 1 chu phong da | 2 mat ket noi | 3 ROI PHO BAN
            4 mat ket noi DANG NHAP LAI roi roi pho ban
        (Logic/Dungeon.lua RecivePlayerLeave.)

        VI SAO CAN: trong pho ban KHONG teleport ra duoc, nen truoc day bot dung relogin() lam
        phuong tien thoat instance ("relogin xong la ca lu tu thoat PB"). Cach do dung, nhung tu
        khi server CHAN TOC DO DANG NHAP (ma 90) thi login lai rat kho -> relogin de dong bo PB
        bien acc thanh ket vong dang nhap hang phut (log that party 6, 23:15-23:25). Gio thoat
        bang dung lenh nay, GIU NGUYEN ket noi, roi dong bo + danh lai PB theo rule retry cu.
        """
        _map0 = self.current_map
        # KHONG O TRONG PHO BAN thi TUYET DOI khong gui. Server nay kick khi nhan goi sai ngu canh
        # (da dinh voi 0x14 06 gui luc server con xu battle). Log that 17:56: leader BO QUA pho ban
        # (ca party da xong) roi RUNG, 4 member dang dung o THANH 12001 van chay nhanh "dong doi ROT
        # -> THOAT PB" -> gui C:047-010 giua thanh -> 6s sau CA 4 CUNG RUNG.
        # Chan o DAY (mot cho) vi co 6 noi goi ham nay.
        # Dung in_instance_map (ca dai 62xxx) chu KHONG dung TEAM_DUNGEON_MAPS (4 map): o trong
        # instance KHAC ma tra "da o ngoai" thi caller bo duong relogin du phong -> ket vinh vien.
        if not in_instance_map(_map0):
            log.info("[%s] khong o trong pho ban to doi (map=%s) -> KHONG gui C:047-010",
                     self._label, _map0)
            return True     # coi nhu da o ngoai - dung muc dich cua caller
        log.info("[%s] THOAT PHO BAN TO DOI (C:047-010) - khong relogin", self._label)
        try:
            self.send(0x2f, b"\x0a\x00")
        except OSError:
            return False
        # cho server day ra khoi instance (doi map). Khong doi map cung KHONG relogin bu o day:
        # caller tu quyet dinh, tranh am tham quay lai dung cach cu.
        _t0 = time.time()
        while self.running and time.time() - _t0 < wait:
            if self.current_map != _map0:
                log.info("[%s] -> da ra khoi pho ban (map %s -> %s)",
                         self._label, _map0, self.current_map)
                return True
            time.sleep(0.3)
        log.warning("[%s] -> gui C:047-010 roi ma %.0fs chua ra khoi map %s",
                    self._label, wait, _map0)
        return False

    def relogin(self):
        """Thoat game roi login lai (cung acc). Server tha DUNG CHO LOGOUT (login=logout pos)
        + gui 0x03 self-spawn -> self.pos RESYNC ve toa do THAT (het drift dead-reckoning).
        Fallback khi KET o bai (lau khong co battle): ve safe -> relogin lay lai vi tri chuan
        -> di tiep toi spot. KHONG load lai gift state (giu nguyen claim trong phien)."""
        log.info("[%s] RELOGIN: dong ket noi + login lai de resync vi tri", self._label)
        self._deliberate_close = True   # ta tu dong -> OSError recv (socket cu) KHONG phai server rot
        try:
            if self.sock:
                self.sock.close()
        except OSError:
            pass
        self.running = False
        time.sleep(1.0)
        # reset state battle/turn (tranh ket dong cu sau relogin)
        self.available = {}
        self._acted_turn = False
        self.flee_mode = False
        self.state.in_battle = False
        self.last_turn_time = 0.0
        self.pos = None   # se duoc 0x03 self-spawn resync ngay sau login
        self._pet_login_logged = None   # RELOGIN dung lai CUNG object -> khong reset thi dong log
                                        # "PET login active" cua lan login moi bi nuot
        # DONG HO DI GIOI: BO MOC CU, cho server day lai sau khi login.
        #
        # `S:085-001 <回傳玩家計數資料>` (id 0x1b = so phut DG da dung) do SERVER TU DAY - crack
        # client khong co goi xin (`protocolTable[85]` chi co ba nhanh S:085-*), va client ghi
        # "跨日時會更新資料" = qua ngay server gui lai. RELOGIN dung LAI cung object nen neu khong
        # bo moc, bot van xai so phut cua PHIEN TRUOC, con `digioi_minutes_live()` thi cong them
        # thoi gian troi LEN CHINH SO CU do.
        #
        # User 09/09: "luc login lai thi phai cap nhat lai time DG theo sv tra ve".
        #
        # Chi xoa MOC (`_last_digioi_ts`), GIU `digioi_minutes` lam so tham khao: cho nao doc moc
        # se thay "chua co dong ho" va cho goi moi, thay vi ket luan bang so cu.
        self._last_digioi_ts = 0.0
        try:
            self.sock = _open_game_socket(self.host, config.GAME_PORT)
            self.sock.sendall(build_auth_packet(self.user_id, self.access_token, self.server_id))
        except OSError as e:
            log.warning("[%s] RELOGIN that bai (ket noi): %s", self._label, e)
            # Re-login noi bo that bai vi mang/server -> de supervisor login lai tu dau.
            self.server_closed = True
            self._deliberate_close = False
            return False
        self.server_closed = False      # ket noi MOI thanh cong -> xoa co (socket cu ko lien quan)
        self._deliberate_close = False  # tu day recv moi lai bao rot that su
        self.running = True
        threading.Thread(target=self._recv_loop, args=(self.sock,), daemon=True).start()
        threading.Thread(target=self._heartbeat_loop, daemon=True).start()
        self._login_setup()
        # cho 0x03 self-spawn resync pos (toi da 6s)
        for _ in range(30):
            time.sleep(0.2)
            if self.pos is not None:
                break
        log.info("[%s] RELOGIN xong, pos=%s map=%s", self._label, self.pos, self.current_map)
        return True

    def chuyen_server_vo_gioi(self, cho=25.0):
        """Doi sang SERVER VO GIOI ma server vua chi (S:001-020) roi auth lai o do.

        Event LIEN SERVER (loan dau THU 7) gom nguoi cua nhieu server vao MOT may rieng, nen
        khong the "tele" toi map 54901 - phai NOI SANG MAY KHAC. Client that lam dung the:
        capture 05/09 co 2 IP (103.190.202.46 -> 103.190.202.65), doi may xong moi vao map.

        Tra True neu da auth duoc o may moi. That bai thi de nguyen `server_closed` cho supervisor
        login lai may CU - mat mot buoi event, khong mat acc.
        """
        bc = getattr(self, "_vo_gioi", None)
        if not bc:
            return False
        ent = getattr(self, "self_entity", None)
        if not ent or len(bytes(ent)) != 8:
            log.warning("[%s] chuyen vo gioi: chua biet entity cua char -> bo", self._label)
            return False
        # PHAI dung cap acc/pwd SERVER PHAT (S:001-002), khong phai user_id/access_token cua bot -
        # do la ly do lan chay 05/09 21:57 bi tu choi. Chua co thi khong di, di la mat ca party.
        if not self._vg_acc or not self._vg_pwd:
            log.warning("[%s] chuyen vo gioi: chua nhan duoc acc/pwd tu S:001-002 -> bo",
                        self._label)
            return False
        goi = build_unbounded_auth_packet(self._vg_acc, self._vg_pwd, bytes(ent),
                                          bc["server_id"], bc["sn"],
                                          connect_code=self._connect_code)
        log.info("[%s] CHUYEN SANG SERVER VO GIOI %s:%s ...", self._label, bc["host"], bc["port"])
        self._host_goc = getattr(self, "_host_goc", None) or self.host
        self._deliberate_close = True      # ta tu dong socket cu -> KHONG phai server rot
        try:
            if self.sock:
                self.sock.close()
        except OSError:
            pass
        self.running = False
        time.sleep(0.8)
        self.pos = None
        self.current_map = None
        self._pet_login_logged = None
        def _hong(vi_sao):
            """MOI duong that bai deu phai qua day. Diem SONG CON: tra `self.host` ve MAY GOC.

            Bug that 05/09 21:57 (party 11 "vang game"): ban dau gan `self.host = <ip vo gioi>`
            roi khi auth hong thi supervisor login lai bang `self.host` - tuc lai dam vao chinh
            may vua tu choi -> vong lap chet, acc khong bao gio ve duoc may cu.
            """
            log.warning("[%s] chuyen vo gioi THAT BAI (%s) -> ve may goc %s",
                        self._label, vi_sao, self._host_goc)
            self.host = self._host_goc
            self.tren_vo_gioi = False
            self._vo_gioi = None
            self.server_closed = True     # supervisor login lai MAY GOC
            self._deliberate_close = False
            return False

        try:
            self.sock = _open_game_socket(bc["host"], int(bc["port"]) or config.GAME_PORT)
            self.sock.sendall(goi)
        except OSError as e:
            return _hong("ket noi %s:%s: %s" % (bc["host"], bc["port"], e))
        self.host = bc["host"]
        self.tren_vo_gioi = True
        self.server_closed = False
        self._deliberate_close = False
        self.running = True
        threading.Thread(target=self._recv_loop, args=(self.sock,), daemon=True).start()
        threading.Thread(target=self._heartbeat_loop, daemon=True).start()
        # `_login_setup` gui hang loat goi; server tu choi auth la socket dong -> OSError.
        # Khong boc thi loi bay ra giua go_to_event, `self.host` ket lai o IP vo gioi.
        try:
            self._login_setup()
        except Exception as e:
            return _hong("loi khoi tao sau auth: %s" % e)
        t0 = time.time()                  # cho 0x03 self-spawn cua may moi
        while time.time() - t0 < cho and self.pos is None and self.running:
            time.sleep(0.2)
        if self.pos is None:
            return _hong("auth xong nhung %.0fs KHONG thay spawn (nghi server tu choi)" % cho)
        log.info("[%s] VO GIOI: vao duoc (pos=%s map=%s)", self._label, self.pos, self.current_map)
        return True

    def close(self):
        if (not self.running) and (not self._deliberate_close) and (not self.server_closed):
            # Client da chet truoc khi code goi close(); day la mat ket noi/runtime drop,
            # khong phai user Stop. Giu dau hieu nay de supervisor reconnect thay vi tat acc am tham.
            self.server_closed = True
        self._deliberate_close = True   # ta tu dong -> OSError trong recv KHONG phai server rot
        self.running = False
        # Timer combat giu bound-method -> giu ca GameClient trong RAM cho toi khi timer chay.
        # Huy ngay khi close, dong thoi bo cac queue callback/debug khong con dung nua.
        try:
            if self._decision_timer is not None:
                self._decision_timer.cancel()
        except Exception:
            pass
        self._decision_timer = None
        self._pending_party_invites.clear()
        self._pending_0b[:] = []
        self._bag_queue[:] = []
        self.available.clear()
        self.stop_npc40_loop()
        self.stop_loandau_loop()  # thieu -> thread loan dau con gui 0x14 06 len socket dang dong
        self.stop_floor_crawl()   # 2K: bao dung vong leo thap (thieu -> thread con bam tiep,
                                  # co the gui 0x14 06 len socket dang dong)
        self.finish_mob_packet_capture()
        self._ghi_cache_tui()    # chot lai tui do lan cuoi truoc khi tat (xem _ghi_cache_tui)
        if self.sock:
            self.sock.close()

    def _chot_minh_chet(self, opcode):
        """CHOT "tran nay MINH da chet chua" - doc HP cua CHINH minh, chot LIEN TUC trong tran.

        DUNG CACH CLIENT LAM. Server KHONG gui goi "thang/thua" nao: `S:011-000 <結束戰鬥>` chi
        mang `roleId + npcIndex` va `FightManager.FightOver` chi `SetWar(EWar.None)`. Client biet
        chet bang HP (`FightField.lua:1034`):
            if me.roleController:GetAttribute(EAttribute.Hp) <= 0 then me:SetBeh(EFightBeh.Dead)

        VI SAO KHONG DUNG `npc40.party_defeated(state.allies)`: `allies` la danh sach DONG DOI,
        chi day du khi co `0x0b` party-broadcast, va bi `clear()` moi `0x34`. Rong thi
        `bool(known) and alive == 0` tra **False** = "khong thua". Log that party 1 (06/09) tang
        11: `2K: xong tran idx=2, party song 0/0` trong khi ca party vua chet sach.
        `state.char` thi la CHINH MINH - luon co, cap nhat moi luot tu `0x33`/`0x35`.

        Chot LIEN TUC vi sau tran server hoi/hoi sinh -> doc luc do la mat dau vet.
        """
        if opcode == protocol.OP_BATTLE_START:
            self.chet_tran_nay = False      # tran MOI -> xoa dau cua tran truoc
            return
        # `getattr(self, "state")`: ham nay nam tren duong NONG (_dispatch, moi goi) nen khong
        # duoc nem gi. Test dung `GameClient.__new__(GameClient)` (khong chay __init__) de kiem
        # tung handler -> chua co `state`.
        st = getattr(self, "state", None)
        u = getattr(st, "char", None) if st is not None else None
        if u is not None and getattr(u, "hp_max", 0) > 0 and getattr(u, "hp", 1) <= 0:
            self.chet_tran_nay = True

    def _observe_npc40_packet(self, opcode, pkt):
        if not getattr(self, "_npc40_started", False):
            return
        now = time.time()
        if (getattr(self, "_npc40_prompt_pending", False)
                and now - getattr(self, "_npc40_prompt_pending_at", 0.0) > 5.0):
            self._npc40_prompt_pending = False
            self._npc40_prompt_pending_at = 0.0
        # CHOT LIEN TUC tinh trang HP dong doi TRONG luc con doc duoc.
        #
        # `state.allies` bi `clear()` moi `0x34` (vao tran moi) va co luc rong han sau khi tran ket
        # -> doc no O THOI DIEM dialog sau tran thi `known` rong, `party_defeated` tra
        # `(False, 0, 0)` -> bot KHONG BAO GIO thay "thua". Do la ly do
        # `consec_loss` khong bao gio tang va luat "thua 2 tran lien tiep" chua tung chay mot lan.
        # Do tren party.log 31/08: 1079/1079 lan ket tran cua acc THAT deu la
        # `alive=0/0 defeated=False`, ke ca van party 1 luc 21:44:45 leader `CHAR HP=0 (da chet)`.
        if self.state.allies:
            self._npc40_hp_snap = npc40.party_defeated(self.state.allies)
        if opcode == protocol.OP_BATTLE_START:
            self._battle_start_seq += 1
            self._npc40_hp_snap = None      # tran MOI -> bo chot cua tran truoc
            self._npc40_prompt_pending = False
            self._npc40_prompt_pending_at = 0.0
        # Luu page dialog NPC (0x14 sub=0100...) gan nhat -> run_loop biet dang o page nao:
        #  - fresh page1: ...[counter]4e (chua choice) -> can advance
        #  - page2 choice fresh (...0200) / prompt giua-event (...0300) -> chon LUON, KHONG advance
        if opcode == 0x14 and len(pkt) >= 9 and pkt[7:9] == b"\x01\x00":
            self._npc40_last_dialog = pkt[7:].hex()

        # 0x41 0a0001 chi ARM viec cho prompt. Live co the chen 0x14 08002a truoc page choice;
        # giu pending qua cac goi trung gian va chi xac nhan khi thay dialog ...0300 trong 5 giay.
        if npc40.is_repeat_prompt(opcode, pkt):
            self._npc40_prompt_pending = True
            self._npc40_prompt_pending_at = now
            return
        if opcode == 0x41 and len(pkt) >= 10 and pkt[7:10] == b"\x0a\x00\x00":
            self._npc40_prompt_pending = False
            self._npc40_prompt_pending_at = 0.0
            return
        if not getattr(self, "_npc40_prompt_pending", False):
            return
        if not npc40.is_repeat_dialog(opcode, pkt):
            return
        self._npc40_prompt_pending = False
        self._npc40_prompt_pending_at = 0.0

        # Uu tien doc TRUC TIEP; rong (da bi clear) thi lay chot gan nhat trong tran.
        defeated, alive, total = npc40.party_defeated(self.state.allies)
        if not total and getattr(self, "_npc40_hp_snap", None):
            defeated, alive, total = self._npc40_hp_snap
        # `state.allies` chi mang unit CHINH MINH thay -> luon ra `1/1` du party 5 acc. Doc thang
        # cac client cung party thi biet that (09/09: 77 tran deu in `alive=1/1` trong khi ca nam
        # acc dang `Nhan item: Thắng Lệnh 1`).
        _that = npc40.party_song_that(self)
        if _that is not None:
            defeated, alive, total = _that
        self._npc40_last_defeated = defeated
        self._npc40_last_alive = alive
        self._npc40_last_total = total
        self._npc40_prompt_seq += 1
        self.state.in_battle = False
        self._set_battle_end_grace()
        log.info("[%s] 40NPC: het tran, party alive=%d/%d defeated=%s prompt_seq=%d",
                 self._label, alive, total, defeated, self._npc40_prompt_seq)

    # ---------- DOI THUONG 40NPC (thoat event -> map 12003 -> NPC doi qua chien dau) ----------
    NPC40_EVENT_MAP = 10991      # map event 40NPC
    NPC40_REWARD_MAP = 12003     # map co NPC doi thuong
    NPC40_REWARD_NPC = (570, 770)  # toa do NPC doi thuong tren 12003 (tu capture)
    NPC40_MID_CITY = 12001       # Trac Quan: tele ve day roi smart-route toi 12003 (khi ko o event)

    def in_40npc_window(self, now=None):
        return npc40.in_event_window(now)

    def claim_40npc_reward(self, ev=None) -> bool:
        """Di doi 'qua chien dau 40NPC' o NPC map 12003:
          - dang o map event (10991) -> exit_event ra 12003
          - khong o event -> tele Trac Quan (12001) -> smart-route toi 12003
          - tren 12003 -> di NPC (570,770) -> 0x20 020008 -> 0x14 01000e00 -> 0x14 0600 x4 (boc capture)."""
        cur = self.current_map
        if cur == self.NPC40_EVENT_MAP:
            _ev = ev or {"exit": {"out_map": self.NPC40_REWARD_MAP}}
            if not self.exit_event(_ev):
                log.warning("[%s] doi thuong 40NPC: thoat event that bai", self._label)
                return False
        elif cur != self.NPC40_REWARD_MAP:
            if not self.go_to_town(self.NPC40_MID_CITY, 0):
                log.warning("[%s] doi thuong 40NPC: khong ve duoc Trac Quan", self._label)
                return False
            self.flee_mode = True
            if not self.follow_smart_scene_route(self.current_map, self.NPC40_REWARD_MAP, flee=True):
                log.warning("[%s] doi thuong 40NPC: khong route duoc %s -> 12003",
                            self._label, self.current_map)
                return False
        if self.current_map != self.NPC40_REWARD_MAP:
            log.warning("[%s] doi thuong 40NPC: khong toi duoc 12003 (dang %s)", self._label, self.current_map)
            return False
        # di toi NPC doi thuong roi mo dialog + doi
        self.flee_mode = True
        self.navigate_to(*self.NPC40_REWARD_NPC, flee=True)
        if not self._wait_combat_clear(idle=1.0, cap=30.0):
            return False
        self.send(0x20, b"\x02\x00\x08"); time.sleep(0.6)
        self.send(0x14, b"\x01\x00\x0e\x00"); time.sleep(0.6)   # chon muc doi thuong (option 0x0e)
        for _ in range(4):
            self.send(0x14, b"\x06\x00"); time.sleep(0.5)
        log.info("[%s] doi thuong 40NPC: da doi qua chien dau o NPC map 12003", self._label)
        return True

    # ---------- LOAN DAU LOI DAI ----------
    def _observe_loandau_packet(self, opcode, pkt):
        """Bam 4 moc cua loan dau. KHONG chan bang `_loandau_started` cho rieng `0x25`: so tran
        thang duoc ban theo LO luc mot vong ket thuc, co the toi truoc/sau khi vong chay bat dau."""
        # Doc bang getattr: mot so duong (test, client dung dang do) chua chay het __init__.
        win = loandau.parse_vs_win(opcode, pkt, getattr(self, "self_entity", None))
        if win is not None and win != getattr(self, "_loandau_wins", None):
            self._loandau_wins = win
            log.info("[%s] Loan dau: so tran thang = %d", self._label, win)
        if not getattr(self, "_loandau_started", False):
            return
        page = loandau.dialog_page(opcode, pkt)
        if page is not None:
            self._loandau_dialog = page
            return
        if loandau.is_registered(opcode, pkt):
            self._loandau_registered_seq += 1
        elif loandau.is_battle_over(opcode, pkt):
            self._loandau_end_seq += 1
            self.state.in_battle = False
            self._set_battle_end_grace()
            # Loan dau ket tran bang `0x14 08 26`, KHONG phai `0x14 sub0700` - nen cho goi
            # _flush_bag_queue o nhanh sub0700 khong bao gio chay o day. Thieu dong nay thi lenh
            # tui do user bam giua tran phai doi luoi an toan trong heartbeat (toi 15 giay).
            self._flush_bag_queue()
        elif loandau.is_battle_create(opcode, pkt):
            # Moc VAO TRAN that. Dung `0x0b sub 250` chu khong phai `0x34`: capture cho thay
            # sub 250 toi TRUOC 0.19s, va `0x34` co the tre/that thuong.
            self._loandau_create_seq += 1
            self._loandau_dat_phe(pkt)

    def _loandau_dat_phe(self, pkt):
        """Loan dau la PvP: phai suy phe tu o CUA MINH, khong duoc mac dinh dich o hang 0-1.

        Khong doc duoc o cua minh -> de `enemy_rows` RONG: bot se khong thay muc tieu nao nen
        KHONG danh, thay vi danh mo roi bi server da han (ly do 42 `修改戰鬥封包`). Bo mot tran
        con hon mat acc.
        """
        self.state.dat_phe_mac_dinh()
        o = loandau.o_cua_minh(pkt, self.self_entity)
        if o is None:
            self.state.enemy_rows = ()
            # Xoa luon quai cu: `enemy_rows` rong lam sync/0x33 khong ghi de nua, nen neu khong
            # xoa thi bot se danh theo vi tri quai cua TRAN TRUOC -> dung cai ket ma minh dang
            # tranh.
            self.state.reset_enemies(reset_quest=False, reset_protect=False)
            log.warning("[%s] Loan dau: KHONG doc duoc o cua minh trong goi tao tran"
                        " -> bo luot tran nay (khong danh mo de khoi bi da)", self._label)
            return
        row, col = o
        self.state.doi_phe_theo_hang_cua_minh(row)
        log.info("[%s] Loan dau: minh o (%d,%d) -> phe ta hang %s, dich hang %s",
                 self._label, row, col, self.state.ally_rows, self.state.enemy_rows)

    def start_loandau_loop(self, point, before_repeat=None, mot_tran=False, ev=None):
        if self._loandau_started:
            return False
        self._loandau_started = True
        self._loandau_stop.clear()
        self._loandau_thread = threading.Thread(
            target=loandau.run_loop,
            args=(self, tuple(point), self._loandau_stop, before_repeat),
            kwargs={"mot_tran": bool(mot_tran), "ev": ev},
            daemon=True,
            name="loandau-%s" % (self._label or self._username),
        )
        self._loandau_thread.start()
        return True

    def stop_loandau_loop(self):
        stop = getattr(self, "_loandau_stop", None)
        if stop is not None:
            stop.set()

    def start_npc40_loop(self, point, on_loss, before_repeat=None):
        if getattr(self, "_npc40_started", False):
            return False
        self._npc40_started = True
        self._npc40_prompt_pending = False
        self._npc40_prompt_pending_at = 0.0
        self._npc40_stop.clear()

        def _chay():
            """run_loop chay trong thread nen tri tra ve BI VUT DI -> moi loi ra `False` (di toi NPC
            loi, mo tran dau timeout, vao tran tiep timeout) truoc day lam thread chet AM THAM:
            `on_loss()` khong ai goi -> member khong biet ma tan hang -> ca party dung yen mai (P42
            02/09). Boc o day de MOI duong that bai deu bao party mot lan."""
            ok = False
            try:
                ok = npc40.run_loop(self, tuple(point), self._npc40_stop, on_loss, before_repeat)
            except Exception as exc:
                log.exception("[%s] 40NPC: vong lap loi: %s", self._label, exc)
            if not ok and self.running and not self._npc40_stop.is_set():
                log.warning("[%s] 40NPC: vong lap ket thuc that bai -> bao party ngung", self._label)
                try:
                    on_loss()
                except Exception as exc:
                    log.warning("[%s] 40NPC: loi khi bao party ngung: %s", self._label, exc)

        self._npc40_thread = threading.Thread(
            target=_chay,
            daemon=True,
            name="npc40-%s" % (self._label or self._username),
        )
        self._npc40_thread.start()
        return True

    def stop_npc40_loop(self):
        stop = getattr(self, "_npc40_stop", None)
        if stop is not None:
            stop.set()
        self._npc40_prompt_pending = False
        self._npc40_prompt_pending_at = 0.0

    def in_combat(self, idle_secs: float = 4.0) -> bool:
        """Dang trong tran. Moc CHUAN = state.in_battle (set MOI luot 0x35 + 0x34 START, HA o 0x14
        sub0700/sub0800 END that). KHONG ep False theo enemy rong hay idle: server co the con dang
        giai quyet animation/ket tran; gui dialog trong khoang nay se bi dong ket noi."""
        busy = (time.time() - self.last_turn_time) < idle_secs
        # CHI suy luan theo member khi DUNG LA MOT TRAN PARTY (battle_tracker.generation != 0).
        # Tran party moi co chuyen "member xac nhan ket tran thay minh"; con moi acc danh tran
        # RIENG (vd vua relogin, moi dua aggro quai cua no o map train) thi END cua dua khac
        # KHONG lien quan gi. Truoc day khong phan biet -> acc DANG danh bi ha in_battle oan ->
        # tuong het tran -> chay tiep flow "ve thanh" -> bat flee_mode -> trong tran no BO CHAY
        # thay vi danh -> mau quai dung im, tran khong bao gio xong (bug that 17:56-17:58 ban APK).
        # Them since=last_turn_time: chi tinh END xay ra SAU luot danh gan nhat cua chinh minh.
        if (self.state.in_battle and not getattr(self.state, "boss_mode", False)
                and not getattr(self, "_in_scene_gate", False)
                and getattr(getattr(self, "battle_tracker", None), "generation", 0)
                and _recent_battle_end(self.party_idx, within=3.0, map_id=self.current_map,
                                       since=self.last_turn_time)):
            # BOSS (boss the gioi / dungeon): moi acc danh trận RIENG cua no -> member khac ket tran
            # KHONG lien quan -> KHONG suy luan theo member (boss_mode). Boss loop tu quan ly ket tran.
            # Truoc day chi ap dung cho pho ban to doi -> tran PHUC KICH O
            # CONG khi keo party: leader KHONG nhan goi END rieng, member co -> leader ket 35s
            # SAFETY (log 10:27:20 quai chet -> 10:27:54 moi qua cong).
            # LOC THEO MAP: 1 member CHET giua tran cung ban sub0800 roi bay ve thanh (map khac) -
            # tin goi do thi ha in_battle OAN luc tran VAN chay -> gui 0x14 06 dung luc server giai
            # tran -> DONG KET NOI (dinh that 10:43:32, taomam chet ve 12003). Chi tin member CON
            # CUNG MAP. Suy luan NOI BO, khong gui goi nao.
            log.info("[%s] MEMBER trong party da xac nhan ket tran THAT -> "
                     "leader ha in_battle theo (khong doi SAFETY)", self._label)
            self.state.in_battle = False
            self._set_battle_end_grace()
        # KHONG reset _battle_entered/_first_turn: client THAT gui 0x41 + atype=2
        # chi 1 LAN/phien (join he thong battle), 6 tran sau van atype=3, khong gui lai 0x41
        return self.state.in_battle or busy

    def set_train_block_stats_context(self, map_id=None, spot=None, enabled=False):
        self.train_block_stats_enabled = bool(enabled and map_id is not None and spot is not None)
        self.train_block_map_id = int(map_id) if map_id is not None else None
        self.train_block_spot = tuple(int(x) for x in spot) if spot is not None else None

    def get_ground_store(self):
        return _ground_store()

    def known_party_entities(self):
        with _PARTY_LOCK:
            return set(_PARTY_ENTITIES.get(self.party_idx, set()))

    def begin_mob_observation(self, observer) -> None:
        with self._mob_observer_lock:
            self._mob_observer = observer

    def end_mob_observation(self, observer) -> None:
        with self._mob_observer_lock:
            if self._mob_observer is observer:
                self._mob_observer = None

    def arm_mob_packet_capture(self, map_id, path=None, max_packets=50000):
        self.finish_mob_packet_capture()
        if path is None:
            folder = getattr(config, "MOB_PACKET_CAPTURE_DIR", os.getcwd())
            os.makedirs(folder, exist_ok=True)
            stamp = time.strftime("%Y%m%d_%H%M%S")
            path = os.path.join(folder, f"mob_packets_{int(map_id)}_{stamp}.jsonl")
        with self._mob_capture_lock:
            self._mob_capture_target_map = int(map_id)
            self._mob_capture_path = os.path.abspath(path)
            self._mob_capture_count = 0
            self._mob_capture_max_packets = max(1, int(max_packets))
        log.info("[%s] arm packet capture map %s -> %s", self._label, map_id, path)
        return os.path.abspath(path)

    def _capture_mob_packet(self, opcode, pkt):
        with self._mob_capture_lock:
            if (self._mob_capture_target_map is None
                    or self.current_map != self._mob_capture_target_map
                    or self._mob_capture_count >= self._mob_capture_max_packets):
                return
            if self._mob_capture_file is None:
                folder = os.path.dirname(self._mob_capture_path)
                if folder:
                    os.makedirs(folder, exist_ok=True)
                self._mob_capture_file = open(
                    self._mob_capture_path, "w", encoding="utf-8"
                )
            record = {
                "time": time.monotonic(),
                "map_id": int(self.current_map),
                "opcode": int(opcode),
                "length": len(pkt),
                "packet": bytes(pkt).hex(),
            }
            self._mob_capture_file.write(json.dumps(record, separators=(",", ":")) + "\n")
            self._mob_capture_file.flush()
            self._mob_capture_count += 1

    def finish_mob_packet_capture(self):
        with self._mob_capture_lock:
            path = self._mob_capture_path
            count = self._mob_capture_count
            if self._mob_capture_file is not None:
                self._mob_capture_file.close()
            self._mob_capture_target_map = None
            self._mob_capture_path = None
            self._mob_capture_file = None
            self._mob_capture_count = 0
            self._mob_capture_max_packets = 0
        if path:
            log.info("[%s] packet capture xong: %d goi -> %s", self._label, count, path)
        return path, count

    def _observe_mob_packet(self, opcode: int, pkt: bytes) -> None:
        now = time.monotonic()
        seen = None
        try:
            if opcode == 0x07 and len(pkt) >= 23 and pkt[7:9] == b"\x00\x00":
                seen = (pkt[9:17], int.from_bytes(pkt[17:19], "little"),
                        int.from_bytes(pkt[19:21], "little"), int.from_bytes(pkt[21:23], "little"))
            elif opcode == 0x06 and len(pkt) >= 22 and pkt[7:9] == b"\x01\x00" and self.current_map is not None:
                seen = (pkt[9:17], int(self.current_map), int.from_bytes(pkt[18:20], "little"),
                        int.from_bytes(pkt[20:22], "little"))
            elif opcode == 0x16 and len(pkt) >= 15 and pkt[7:9] == b"\x02\x00" and self.current_map is not None:
                slot = pkt[9:11]
                seen = (b"\x16\x02" + slot + b"\x00" * 4, int(self.current_map),
                        int.from_bytes(pkt[11:13], "little"), int.from_bytes(pkt[13:15], "little"))
            if seen and 0 < seen[2] < 20000 and 0 < seen[3] < 20000:
                with self._world_entity_lock:
                    self.world_entities[bytes(seen[0])] = (seen[1], seen[2], seen[3], now)
                    if len(self.world_entities) > 1000:
                        cutoff = now - 30.0
                        self.world_entities = {e: v for e, v in self.world_entities.items() if v[3] >= cutoff}
        except Exception:
            seen = None
        with self._mob_observer_lock:
            observer = self._mob_observer
        if observer is None:
            return
        try:
            if opcode == 0x07 and len(pkt) >= 23 and pkt[7:9] == b"\x00\x00":
                observer.observe_spawn(
                    pkt[9:17], int.from_bytes(pkt[17:19], "little"),
                    int.from_bytes(pkt[19:21], "little"),
                    int.from_bytes(pkt[21:23], "little"), now,
                )
            elif opcode == 0x06 and len(pkt) >= 22 and pkt[7:9] == b"\x01\x00" \
                    and self.current_map is not None:
                observer.observe_move(
                    pkt[9:17], int(self.current_map),
                    int.from_bytes(pkt[18:20], "little"),
                    int.from_bytes(pkt[20:22], "little"), now,
                )
            elif opcode == 0x16 and len(pkt) >= 15 and pkt[7:9] == b"\x02\x00" \
                    and self.current_map is not None:
                slot = pkt[9:11]
                observer.observe_move(
                    b"\x16\x02" + slot + b"\x00" * 4,
                    int(self.current_map),
                    int.from_bytes(pkt[11:13], "little"),
                    int.from_bytes(pkt[13:15], "little"), now,
                )
            elif opcode == 0x0f and len(pkt) >= 17 and pkt[7:9] == b"\x07\x00":
                observer.mark_player(pkt[9:17])
            elif opcode == 0x27 and len(pkt) >= 22 and pkt[7:9] == b"\x09\x00":
                offset = 9
                while offset + 13 <= len(pkt):
                    name_bytes = pkt[offset + 12]
                    end = offset + 13 + name_bytes
                    if end > len(pkt):
                        break
                    observer.mark_player(pkt[offset:offset + 8])
                    offset = end
        except Exception as exc:
            log.debug("[%s] bo qua loi mob observer op=0x%02x: %s",
                      self._label, opcode, exc)

    def _record_train_block_stats(self, enemy_slots):
        if not (self.train_block_stats_enabled and self.train_block_map_id and self.train_block_spot):
            return
        if self.current_map != self.train_block_map_id:
            return
        try:
            from . import train_block_stats
            train_block_stats.record_battle(self.train_block_map_id, self.train_block_spot,
                                            enemy_slots, dict(self.state.enemy_pos_tids))
        except Exception as e:
            log.debug("[%s] loi ghi thong ke block quai: %s", self._label, e)

    # ---- heartbeat ----
    def _heartbeat_loop(self):
        prio = 0
        while self.running:
            # Uu tiên thread nếu acc này đang trong PB (heartbeat KHÔNG được trễ -> tránh server đá).
            want = 1 if self.in_team_dungeon() else (
                -1 if _TD_ACTIVE > 0 else 0)
            if want != prio:
                _set_thread_prio(want)
                prio = want
            time.sleep(15)
            # LUOI AN TOAN cho hang doi lenh tui do: `in_battle` duoc HA o 8 CHO khac nhau (ket
            # tran that, bo chay, doi map, reset...). Bam vao mot nhanh la kieu loi toi da mac
            # nhieu lan - vao dung nhanh do thi chay, vao nhanh khac thi lenh ket vinh vien.
            # Moc nhanh (0x14 sub0700) van giu de user khong phai cho lau; day chi la bat cac
            # truong hop con lai.
            try:
                if self._bag_queue and not self.state.in_battle:
                    self._flush_bag_queue()
            except Exception:
                pass
            try:
                self.send(protocol.OP_HEARTBEAT, b"\x00\x00")
            except OSError:
                break

    # ---- recv ----
    def _recv_loop(self, sock):
        # Wrapper mong: dam bao GIAM _TD_ACTIVE + tra priority ve thuong o finally (khong leak counter
        # du thread thoat kieu gi). Than vong lap that o _recv_loop_impl (giu nguyen logic cu).
        self._recv_prio = 0
        self._recv_td_counted = False
        self._last_recv_ts = time.time()   # moc nhan goi gan nhat (phat hien half-open o _recv_loop_impl)
        try:
            self._recv_loop_impl(sock)
        finally:
            if self._recv_td_counted:
                _td_active_inc(-1)
                self._recv_td_counted = False
            _set_thread_prio(0)

    def _recv_loop_impl(self, sock):
        while self.running and sock is self.sock:
            # Uu tien thread theo trang thai PB (leader+member deu doc map that qua in_team_dungeon):
            #   dang danh PB -> CAO; co party KHAC dang danh PB -> THAP; con lai -> thuong.
            in_td = self.in_team_dungeon()
            if in_td != self._recv_td_counted:
                _td_active_inc(1 if in_td else -1)
                self._recv_td_counted = in_td
            want = 1 if in_td else (-1 if _TD_ACTIVE > 0 else 0)
            if want != self._recv_prio:
                _set_thread_prio(want)
                self._recv_prio = want
            try:
                data = sock.recv(8192)
            except socket.timeout:
                # recv timeout (RECV_SOCK_TIMEOUT giay khong co goi). Neu KHONG nhan goi nao suot
                # >RECV_DEAD_SECS -> server half-open (rot kieu khong RST/FIN) -> acc dung hinh ->
                # coi nhu ROT de supervisor relogin. Chua qua nguong = luc im binh thuong -> cho tiep.
                if sock is not self.sock or self._deliberate_close:
                    break
                if time.time() - self._last_recv_ts > RECV_DEAD_SECS:
                    log.warning("[%s] Server im >%ds (half-open/rot khong RST) -> coi nhu ROT, relogin",
                                self._label or self._username, int(RECV_DEAD_SECS))
                    self._dump_recent("server im lang")
                    self.server_closed = True
                    self.running = False
                    break
                continue
            except OSError as e:
                if sock is not self.sock:
                    break
                # OSError = socket loi. Neu KHONG phai ta tu dong (close/relogin) -> SERVER ROT that
                # (connection reset...) -> danh dau server_closed de supervisor RECONNECT (giong nhanh
                # empty-data). Truoc day nhanh nay KHONG set -> nick rot kieu reset "chet am tham".
                if not self._deliberate_close:
                    log.warning("[%s] Server dong ket noi (OSError: %s)", self._label or self._username, e)
                    self._dump_recent("OSError %s" % e)
                    self.server_closed = True
                self.running = False   # rot ket noi -> dung MOI vong lap (tranh loop mai tren socket chet)
                break
            if not data:
                if sock is not self.sock or self._deliberate_close:
                    break
                log.warning("[%s] Server dong ket noi", self._label or self._username)
                self._dump_recent("server dong (empty data)")
                self.server_closed = True   # server CHU DONG dong (rot/bao tri/kick) - khong phai STOP
                self.running = False   # rot ket noi -> dung MOI vong lap
                break
            self._last_recv_ts = time.time()   # nhan duoc goi -> reset dong ho half-open
            # NHIP TIM cho watcher: con nhan goi = con song, ke ca dang lam viec lau (boss 15').
            # Lay tu day thay vi bat moi vong lap tu goi task_heartbeat -> khong the quen.
            # Throttle 5s: chi cham vao dict co khoa khi that su can.
            if self._last_recv_ts - getattr(self, "_hb_ts", 0.0) > 5.0:
                self._hb_ts = self._last_recv_ts
                task_heartbeat(self._username)
            self.recv_buf += protocol.xor(data)
            pkts, consumed = protocol.parse_stream(self.recv_buf)
            self.recv_buf = self.recv_buf[consumed:]
            for opcode, pkt in pkts:
                from .packet_trace import record as _record_server_packet
                _record_server_packet(self._username or self._label, opcode, pkt)
                self._recent_recvs.append((time.time(), opcode, pkt.hex()[:60]))
                try:
                    self._dispatch(opcode, pkt)
                except Exception as e:
                    # 1 goi loi KHONG duoc lam chet recv thread / nuot cac goi sau trong batch
                    # (vd response 0x57 nhan qua) -> bat rieng tung goi.
                    log.warning("[%s] Loi xu ly goi 0x%02x (bo qua): %s", self._label, opcode, e)
                finally:
                    self._capture_mob_packet(opcode, pkt)

    def _apply_role_counts(self, body: bytes):
        """Doc S2C 0x55 RoleCount body: 0100 + count + records."""
        if len(body) < 16 or body[:2] != b"\x01\x00":
            return
        cnt = int.from_bytes(body[2:6], "little")
        off, n = 6, 0
        while n < cnt and off + 10 <= len(body):
            sid = int.from_bytes(body[off:off + 2], "little")
            val = int.from_bytes(body[off + 2:off + 6], "little")
            mx = int.from_bytes(body[off + 6:off + 10], "little")
            self.role_counts[sid] = (val, mx)
            if sid == 0x1b:                   # so phut Di Gioi
                self.digioi_minutes = val & 0xFFFF
                self._last_digioi_ts = time.time()
            elif sid == ONLINE_GIFT_ROLECOUNT: # qua online: so giay online hom nay server da tinh
                self._server_online_seconds = int(val)
                self._server_online_ts = time.time()
            elif sid == 0x08:                 # van tieu: so luot DA gui hom nay + gioi han
                self.vantieu_started = val
                self.vantieu_max = mx or 3
            elif sid == 0x2a:                 # BOSS QUAN DOAN: so lan DA danh hom nay + gioi han (X/3)
                self.legion_boss_count = val   # xac nhan: 0x55 id 0x2a cur=0/1/2, max=3 (X/3)
                self.legion_boss_max = mx or 3
            elif sid == 0x0456:               # Shop: Di Gioi Ho Phu 0xff8c (Moi ngay X/3)
                self.shop_ho_phu_count = val
                self.shop_ho_phu_max = mx or 3
            elif sid == 0x002b:               # Shop: Hop Thien Chau 0xb68a (Moi ngay X/1)
                self.shop_thien_chau_count = val
                self.shop_thien_chau_max = mx or 1
            elif sid == 0x0016:               # Shop: Trieu Goi Bao Hop 0xb554 (Moi ngay X/1)
                self.shop_bao_hop_count = val
                self.shop_bao_hop_max = mx or 1
            # KHONG doc 0x9b lam "luot dungeon": login bulk gui 0x9b=9 (KHONG khop thuc te
            # 1-2 luot) -> sai -> dungeon dem THUAN LOCAL (checkin_state.json).
            off += 10
            n += 1

    def _set_world_boss_progress(self, cur: int, mx: int = WORLD_BOSS_MAX_ATTEMPTS, source: str = ""):
        mx = int(mx or WORLD_BOSS_MAX_ATTEMPTS)
        cur = max(0, int(cur))
        self.world_boss_count = cur
        self.world_boss_max = mx
        self._world_boss_progress_loaded = True
        self._world_boss_progress_ts = time.time()
        if source:
            log.debug("[%s] Boss the gioi progress %d/%d (%s)", self._label, cur, mx, source)

    def _sync_world_boss_from_mission_steps(self, source: str = ""):
        # Client game hien thi World Boss 0/5 tu MarkManager.GetMission(12207).step.
        cur = int(self.mission_steps.get(WORLD_BOSS_MISSION_ID, 0) or 0)
        cur = max(0, min(WORLD_BOSS_MAX_ATTEMPTS, cur))
        self._set_world_boss_progress(cur, WORLD_BOSS_MAX_ATTEMPTS, source or "mission-step")

    def _on_daily_quest_packet(self, pkt: bytes):
        """S2C 0x5b (op 91) Jiugongge/daily bingo - parse dung 4 sub nhu client Lua:
          0100 = FULL grid  [count]<<[gridId u16][diff 1B] 9*<<[missionId u16][progress u32][done 1B]>>>>
                 -> nguon missionId THAT (thay hardcode 0x2f..0x37) + o da xong luc login.
          0400 = update 1 o [gridId u16][cell 1B][missionId u16][progress u32][done 1B]
          0200 = ket qua ghi nhan o [result 1B][gridId u16][index 1B] (1 = xong)
          0300 = ket qua CLAIM line [result 1B][gridId u16][index 1B] -> _claimed_lines
                 (nguon xac nhan "da nhan" giua phien - chinh xac hon marker 0x51 dang do)
        Chi quan tam grid daily (gridId == 1)."""
        body = pkt[7:]
        if len(body) < 2:
            return
        sub = body[:2]
        if sub == b"\x02\x00":
            if len(body) >= 6 and body[2:5] == b"\x01\x01\x00":
                self._quest_cells.add(body[5])
        elif sub == b"\x01\x00" and len(body) >= 3:
            off = 3
            for _ in range(body[2]):
                if off + 3 + 9 * 7 > len(body):
                    break
                gid = int.from_bytes(body[off:off + 2], "little")
                off += 3
                # Luu MOI bang (bang event dung chung co che) - xem claim_event_boards().
                _g = self._quest_grids.setdefault(gid, {"cells": set(), "missions": {}})
                for c in range(1, 10):
                    mid = int.from_bytes(body[off:off + 2], "little")
                    done = body[off + 6]
                    off += 7
                    if mid:
                        _g["missions"][c] = mid
                    if done:
                        _g["cells"].add(c)
                    if gid == 1:
                        if mid:
                            self._quest_missions[c] = mid
                        if done:
                            self._quest_cells.add(c)
        elif sub == b"\x04\x00" and len(body) >= 12:
            gid = int.from_bytes(body[2:4], "little")
            cell = body[4]
            if 1 <= cell <= 9:
                mid = int.from_bytes(body[5:7], "little")
                _g = self._quest_grids.setdefault(gid, {"cells": set(), "missions": {}})
                if mid:
                    _g["missions"][cell] = mid
                if body[11]:
                    _g["cells"].add(cell)
                if gid == 1:
                    if mid:
                        self._quest_missions[cell] = mid
                    if body[11]:
                        self._quest_cells.add(cell)
        elif sub == b"\x03\x00" and len(body) >= 6:
            # Ghi nhan CLAIM theo TUNG BANG (khong chi bang daily) - bang event dung chung co che.
            _gid = int.from_bytes(body[3:5], "little")
            if body[2] == 1 and 1 <= body[5] <= 7:
                self._claimed_by_grid.setdefault(_gid, set()).add(body[5])
                if _gid == 1:
                    self._claimed_lines.add(body[5])

    # getFlag cua 7 phan thuong BANG 1 (JiugonggeInfo_C.dat: awards[i].getFlag) - line L -> 1540+L.
    # Client: award "DA NHAN" <=> BitFlag.Get(getFlag) (Logic_Jiugongge.SetJiugonggeState:
    #   canGetAward = 2 neu BitFlag.Get(...), = 1 neu du 3 o isCompleted, con lai = 0).
    _Q_LINE_FLAG = {L: 1540 + L for L in range(1, 8)}

    def _refresh_quest_claimed_from_bitflags(self):
        """Trang thai "DA NHAN" cua 7 hang/cot bingo - LAY DUNG NHU CLIENT (BitFlag / 永標).

        Truoc day bot quet PATTERN BYTE trong frame 0x51 (`c0 ?? 03000000 [mask] 01000000`, line L =
        bit L+3) - chinh comment cu da ghi "CHUA khop het server, CON DANG DO". Doi chieu capture:
        acc da nhan DU 7 line ma pattern KHONG match duoc -> bot tuong CHUA nhan gi -> claim lai het
        (server reject, ton goi + log rac); nguy hiem hon la neu match NHAM se BO QUA line chua nhan
        -> MAT qua. BitFlag la nguon that (S:081-002 full bang + S:081-001 update le) va bot da parse
        san; _bitflag_get() trung khop CheckFlag() cua client (byte=(id-1)//8, bit=(id-1)%8).
        """
        if not self._bitflags_loaded:
            return
        got = {L for L, fid in self._Q_LINE_FLAG.items() if self._bitflag_get(fid)}
        # HOP voi xac nhan giua phien (S:91-3 -> _claimed_lines) de khong mat thong tin vua claim.
        self._claimed_lines |= got
        self._claimed_by_grid.setdefault(1, set()).update(got)
        self._claimed_loaded = True
        # CAC BANG KHAC (event...): co "da nhan" doc tu jiugongge.json (crack_jiugongge.py).
        for gid, info in (getattr(config, "JIUGONGGE", {}) or {}).items():
            if gid == 1:
                continue
            aw = (info or {}).get("awards") or []
            got2 = {L for L in range(1, 8)
                    if L <= len(aw) and self._bitflag_get(aw[L - 1].get("flag"))}
            if got2:
                self._claimed_by_grid.setdefault(gid, set()).update(got2)

    def _bitflag_get(self, flag_id: int):
        """Tra ve True/False neu da co full BitFlag, None neu server chua sync."""
        try:
            flag_id = int(flag_id)
        except (TypeError, ValueError):
            return None
        if flag_id <= 0 or not self._bitflags_loaded:
            return None
        idx = (flag_id - 1) // 8
        if idx >= len(self._bitflag_bytes):
            return False
        return bool(self._bitflag_bytes[idx] & (1 << ((flag_id - 1) % 8)))

    def _bitflag_set(self, flag_id: int, value: bool):
        try:
            flag_id = int(flag_id)
        except (TypeError, ValueError):
            return
        if flag_id <= 0:
            return
        idx = (flag_id - 1) // 8
        while len(self._bitflag_bytes) <= idx:
            self._bitflag_bytes.append(0)
        mask = 1 << ((flag_id - 1) % 8)
        if value:
            self._bitflag_bytes[idx] |= mask
        else:
            self._bitflag_bytes[idx] &= (~mask) & 0xFF

    def _refresh_online_claimed_from_bitflags(self):
        flags = _load_online_gift_flags()
        claimed = {m for m, flag in flags.items() if self._bitflag_get(flag) is True}
        self.claimed_gifts = claimed
        return claimed

    def _apply_bitflags(self, pkt: bytes):
        """Doc S2C 0x51 BitFlag: sub0100 update le, sub0200 full table."""
        if len(pkt) < 9:
            return
        sub = int.from_bytes(pkt[7:9], "little")
        if sub == 0x02 and len(pkt) >= 11:
            size = int.from_bytes(pkt[9:11], "little")
            if size >= 0 and 11 + size <= len(pkt):
                self._bitflag_bytes = bytearray(pkt[11:11 + size])
                self._bitflags_loaded = True
                self._refresh_online_claimed_from_bitflags()
                self._refresh_quest_claimed_from_bitflags()
        elif sub == 0x01 and len(pkt) >= 13:
            count = int.from_bytes(pkt[9:13], "little")
            off = 13
            for _ in range(count):
                if off + 3 > len(pkt):
                    break
                self._bitflag_set(int.from_bytes(pkt[off:off + 2], "little"), bool(pkt[off + 2]))
                off += 3
            if self._bitflags_loaded:
                self._refresh_online_claimed_from_bitflags()
                self._refresh_quest_claimed_from_bitflags()

    def _metrics_battle_start(self):
        now = time.time()
        if self.exp_stats_started_at is None:
            self.exp_stats_started_at = now
        if self._metrics_battle_started_at is None:
            self._metrics_battle_started_at = now
            self._metrics_battle_char_exp = 0
            self._metrics_battle_pet_exp = 0
            self._metrics_battle_char_exp_start = self.char_exp

    def _metrics_exp_gain(self, who, amount):
        amount = max(0, int(amount or 0))
        if not amount:
            return
        if self.exp_stats_started_at is None:
            self.exp_stats_started_at = time.time()
        if who == "character":
            self.exp_stats_char_total += amount
            self._metrics_battle_char_exp += amount
        else:
            self.exp_stats_pet_total += amount
            self._metrics_battle_pet_exp += amount

    def _metrics_battle_end(self):
        started = self._metrics_battle_started_at
        if started is None:
            return
        # Mot so member khong nhan goi thuong 20-042 nhu leader. Neu server da cap nhat
        # tong EXP nhan vat nhung goi delta khong vao nhanh parser, lay chenh lech dau/cuoi
        # tran lam fallback. Chi dung khi chua dem duoc EXP de tranh cong hai lan.
        _exp_start = self._metrics_battle_char_exp_start
        if (not self._metrics_battle_char_exp and _exp_start is not None
                and self.char_exp is not None and int(self.char_exp) > int(_exp_start)):
            _fallback_gain = int(self.char_exp) - int(_exp_start)
            self._metrics_battle_char_exp = _fallback_gain
            self.exp_stats_char_total += _fallback_gain
            _row = {"type": "exp", "time": time.strftime("%H:%M:%S"),
                    "who": "character", "kind": 1, "exp": _fallback_gain,
                    "source": "character total at battle end"}
            self.combat_exp_log.appendleft(dict(_row))
            self.activity_log.appendleft(dict(_row))
        self.last_battle_seconds = max(0.1, time.time() - started)
        self.last_battle_char_exp = int(self._metrics_battle_char_exp)
        self.last_battle_pet_exp = int(self._metrics_battle_pet_exp)
        self.activity_log.appendleft({
            "type": "battle_summary", "time": time.strftime("%H:%M:%S"),
            "message": "Trận %.1fs • %s: %s EXP • %s: %s EXP" % (
                self.last_battle_seconds, self.char_name or self._username,
                ("+%d" % self.last_battle_char_exp) if self.last_battle_char_exp else "chưa xác nhận",
                getattr(self, "pet_name", "") or "Pet đang ra trận",
                ("+%d" % self.last_battle_pet_exp) if self.last_battle_pet_exp else "chưa xác nhận"),
            "char_exp": self.last_battle_char_exp, "pet_exp": self.last_battle_pet_exp,
            "seconds": self.last_battle_seconds,
        })
        self.exp_stats_battles += 1
        self._metrics_battle_started_at = None
        self._metrics_battle_char_exp_start = None
        log.info("[%s] THONG KE TRAN: %.1fs, nhan vat +%d EXP, pet +%d EXP",
                 self._label, self.last_battle_seconds, self.last_battle_char_exp,
                 self.last_battle_pet_exp)

    def exp_rate_snapshot(self):
        started = self.exp_stats_started_at
        elapsed = max(0.0, time.time() - started) if started else 0.0
        hours = elapsed / 3600.0
        return {
            "session_seconds": int(elapsed),
            "battles": int(self.exp_stats_battles),
            "last_battle_seconds": self.last_battle_seconds,
            "last_battle_char_exp": int(self.last_battle_char_exp),
            "last_battle_pet_exp": int(self.last_battle_pet_exp),
            "char_exp_total": int(self.exp_stats_char_total),
            "pet_exp_total": int(self.exp_stats_pet_total),
            "battles_per_hour": (self.exp_stats_battles / hours) if hours > 0 else 0.0,
            "char_exp_per_hour": int(self.exp_stats_char_total / hours) if hours > 0 else 0,
            "pet_exp_per_hour": int(self.exp_stats_pet_total / hours) if hours > 0 else 0,
        }

    def _dispatch(self, opcode: int, pkt: bytes):
        log.debug("[%s] RECV op=0x%02x len=%d %s", self._label, opcode, len(pkt), pkt.hex())
        # Metrics theo start/end cua battle tracker, khong theo broadcast 0x34 (co noise
        # va member co the duoc bootstrap ma khong nhan start legacy).
        # Giu ma ack Boss QD de khong gui lenh vao instance khi lenh mo da bi tu choi.
        if opcode == 0x27 and len(pkt) >= 10 and pkt[7:9] == b"\x77\x00":
            self._legion_start_result = int(pkt[9])
            self._legion_ack_seq = int(getattr(self, "_legion_ack_seq", 0)) + 1
            log.info("[%s] Boss QD: ACK mo raw=%d success=%s (probe 1=true, 0=false)",
                     self._label, self._legion_start_result, self._legion_start_result == 1)
        elif opcode == 0x14 and len(pkt) >= 10 and pkt[7:9] == b"\x08\x00" \
                and getattr(self, "_legion_enter_pending", False):
            self._legion_enter_result = int(pkt[9])
            log.info("[%s] Boss QD: server tra ket qua vao=%d", self._label, self._legion_enter_result)
        self._chot_minh_chet(opcode)
        # `S:006-001 <玩家移動> +玩家ID(8) +面朝向(1) +座標X(2) +座標Y(2)`
        #
        # Server KHONG echo nuoc di cua CHINH MINH (da do tren pcap: 244 lenh gui / 0 goi tra ve
        # mang entity minh) - nhung no BAO vi tri cua NGUOI KHAC trong cung scene. Ma "nguoi khac"
        # o day gom ca CAC ACC BOT KHAC trong CUNG TIEN TRINH.
        #
        # => Acc A khong biet vi tri that cua chinh no, nhung acc B dung canh THAY A. Do la nguon
        #    toa do THAT duy nhat co san lien tuc, thay cho dead-reckoning (thu chi dung cho toi khi
        #    mot lenh move bi tran nuot la sai vinh vien).
        #
        # Chi nhan duoc goi nay khi nguoi do o cung scene voi minh -> cung map, cung kenh.
        if (opcode == 0x06 and len(pkt) >= 22 and pkt[7:9] == b"\x01\x00"
                and self.current_map is not None):
            try:
                _e = pkt[9:17]
                if not self.self_entity or _e != self.self_entity:
                    _px = int.from_bytes(pkt[18:20], "little")
                    _py = int.from_bytes(pkt[20:22], "little")
                    if 0 < _px < 20000 and 0 < _py < 20000:
                        with _PARTY_LOCK:
                            _peers = list((_PARTY_CLIENTS.get(self.party_idx) or {}).values())
                        for _pc in _peers:
                            if _pc is self or not getattr(_pc, "running", False):
                                continue
                            if getattr(_pc, "self_entity", None) != _e:
                                continue
                            if int(getattr(_pc, "current_map", 0) or 0) != int(self.current_map):
                                continue
                            if _pc.pos != (_px, _py):
                                log.debug("[%s] pos THAT cua '%s' = (%d,%d) [no tu nho: %s] - thay "
                                          "qua S:006-001", self._label,
                                          getattr(_pc, "_label", "?"), _px, _py, _pc.pos)
                            _pc.pos = (_px, _py)
                            _pc._position_generation += 1
                            _pc._pos_valid_for_map = _pc.current_map
                            break
            except Exception as e:
                log.debug("[%s] loi doc S:006-001 cua acc khac: %s", self._label, e)
        self._observe_team_dungeon_packet(opcode, pkt)
        self._observe_npc40_packet(opcode, pkt)
        self._observe_loandau_packet(opcode, pkt)
        # Su kien cong (cau Gioi kieu...) phai nghe o DAY, KHONG nhet trong _observe_npc40_packet:
        # ham do mo dau bang `if not self._npc40_started: return` nen chi chay khi DANG lam nhiem vu
        # 40 NPC. Qua cong thi co do TAT -> handler khong bao gio duoc goi. Day la ly do that su bot
        # ket o cong idx=10 map 63000 (log 16:46 va 17:12), ke ca ban dau tien cua toi.
        # `S:020-001..006 <一般事件>` deu la buoc su kien -> dua CA 6 sub vao, ham tu loc resultType.
        if opcode == 0x14 and len(pkt) >= 9 and pkt[8] == 0 and 1 <= pkt[7] <= 6:
            self._on_route_dialog(pkt)
            # SU KIEN DANG MO. Chi khi co co nay moi duoc gui `C:020-009 <事件選擇>` (0x14 sub09).
            # Gui khi KHONG co su kien nao mo = server NGAT NGAY "su kien vi pham" (ma 5) - da lam
            # rot acc tp605 luc 14:57:46 ngay 04/09.
            self.event_dang_mo = True
        elif opcode == 0x14 and len(pkt) >= 9 and pkt[7:9] == b"\x08\x00":
            self.event_dang_mo = False      # S:020-008 <事件結束>: su kien da dong
        self._observe_mob_packet(opcode, pkt)
        self._track_battle_packet(opcode, pkt)
        # Pho ban to doi: theo doi thoai NPC de biet canh da HET that su chua (_adv_dialog_until_idle)
        # va tin hieu ket tran that (mot so canh boss tu dong xu ly, khong bao gio bat in_battle=True).
        if self.in_team_dungeon() and opcode == 0x14:
            # CHI cac sub THAT SU lien quan thoai (0100=ack dong thoai, 1000=cutscene loop,
            # 0d00=mo canh) moi duoc coi la "con dang thoai" -> reset dong ho im lang. Cac sub
            # khac (0800 noise, 2c00...) lap lai lien tuc nhung khong lien quan.
            if pkt[7:9] in (b"\x01\x00", b"\x10\x00", b"\x0d\x00"):
                self._last_dialog_evt = time.time()
            # sub0800 tail=03/04 = tin hieu KET TRAN THAT (bat ke in_battle_TRUOC dang gia tri gi).
            elif pkt[7:9] == b"\x08\x00" and len(pkt) >= 10 and pkt[9] in (0x03, 0x04):
                self._genuine_end_seen = time.time()
        # Hoan thanh dungeon: S2C 0x14 sub 0x64 (man tong ket) -> set co de do_daily_dungeon biet xong
        if opcode == 0x14 and len(pkt) >= 8 and pkt[7] == 0x64:
            self.dungeon_complete = True
            self._log_battle_rewards(pkt)   # (20-100) log vat pham/exp/vang... nhan duoc cuoi tran
        # (20-042) EXP nhan duoc: [kind 1B][exp u32]. Giu log RIENG tung client cho UI.
        # kind=1 da xac nhan char; kind=2 gan nhan pet kem RAW de doi chieu capture that.
        if opcode == 0x14 and len(pkt) >= 14 and pkt[7] == 0x2a:
            _pairs = []
            for _off in range(9, len(pkt) - 4, 5):
                _kind = pkt[_off]
                _value = int.from_bytes(pkt[_off + 1:_off + 5], "little")
                if 1 <= _kind <= 5 and 0 < _value < 2_000_000_000:
                    _pairs.append((_off, _kind, _value))
            if not _pairs:
                for _off in range(9, len(pkt) - 4):
                    _kind = pkt[_off]
                    _value = int.from_bytes(pkt[_off + 1:_off + 5], "little")
                    if 1 <= _kind <= 5 and 0 < _value < 2_000_000_000:
                        _pairs.append((_off, _kind, _value))
                        break
            for _off, _exp_kind, _exp in _pairs:
                _who = "character" if _exp_kind == 1 else ("pet" if _exp_kind == 2 else "unknown")
                _row = {"type": "exp", "time": time.strftime("%H:%M:%S"), "who": _who,
                        "kind": int(_exp_kind), "exp": int(_exp),
                        "pet": str(getattr(self, "pet_name", "") or "")}
                self.combat_exp_log.appendleft(dict(_row))
                self.activity_log.appendleft(dict(_row))
                if _who != "unknown":
                    self._metrics_exp_gain(_who, _exp)
                log.info("[%s] KET TRAN EXP: kind=%d who=%s +%d", self._label,
                         _exp_kind, _who, _exp)
            if not _pairs:
                log.warning("[%s] EXP 20-042 chua nhan dang duoc payload: %s", self._label, pkt.hex())
        # KET TRAN that: S2C 0x14 sub 0700 (man tong ket battle) -> ban DUNG 1 lan/tran luc thang.
        # Day moi la moc ket tran dang tin (0x34 ban that thuong, 1 lan/nhieu tran). Reset quest_mode
        # + enemies o DAY -> quest_mode latch luc start (>5) GIU NGUYEN ca tran du quai con <=5.
        if (opcode == 0x14 and len(pkt) >= 9 and pkt[7:9] == b"\x07\x00"
                and not self.battle_tracker.active):
            self._genuine_end_seen = time.time()
            log.info("[%s] nhan goi KET TRAN THAT 0x14 sub0700 (WIN) in_battle_truoc=%s "
                     "raw=%s -> in_battle=False",
                     self._label, self.state.in_battle, pkt.hex())
            # Pho ban to doi: quest_mode duoc EP CO DINH suot ca dungeon (do_team_dungeon_lv20),
            # KHONG duoc reset ve auto-latch giua cac tran con trong dungeon.
            _in_team_dungeon = self.in_team_dungeon()
            self.state.reset_enemies(reset_quest=not _in_team_dungeon)
            self.state.in_battle = False
            self._metrics_battle_end()
            self._heal_after_battle()   # hoi HP/SP NGAY khi ket tran (khong doi tick keepalive)
            self._flush_bag_queue()     # lenh tui do user bam giua tran -> gui bay gio
        # KET TRAN khi BO CHAY: flee KHONG sinh 0x14 sub0700 (man THANG) ma chuoi 0x14 0c00 -> 0900 ->
        # 0800 (xac nhan capture flee.pcap). -> cung ha in_battle de go_to_town teleport duoc sau flee.
        # (Neu flee chua thanh cong/dang giua tran, luot 0x35 sau tu set lai in_battle=True.)
        # DBG: log CA khi in_battle DA la False truoc do (nghi ngo: sub nay co the la scene-transition
        # khac, KHONG phai ket tran that - user nghi dung luc nay in_battle con False ma van log).
        if (opcode == 0x14 and len(pkt) >= 9
                and pkt[7:9] in (b"\x0c\x00", b"\x09\x00", b"\x08\x00")
                and not self.battle_tracker.active):
            was_true = self.state.in_battle
            now = time.time()
            scene_end_like = (
                pkt[7:9] == b"\x08\x00" and len(pkt) >= 10 and pkt[9] in (0x03, 0x04)
            )
            if scene_end_like:
                self._genuine_end_seen = now
            self.state.in_battle = False
            # KET TRAN THAT (xac nhan tu capture): sub0800 + byte cuoi=04 + dang THUC SU o
            # in_battle=True truoc do. Cac occurrence sub0800/0900 khac (login, cho, idle...)
            # deu co in_battle_TRUOC=False san -> chi la scene-transition noise, khong lien quan tran.
            # Bug that: server con gui 0x35 DU (broadcast cho member khac chua xong luot) SAU KHI
            # tran cua leader da ket that -> 0x35 handler set lai in_battle=True oan.
            # -> mo grace period ngan de 0x35 KHONG duoc phep set lai in_battle trong luc nay.
            if was_true and pkt[7:9] == b"\x08\x00":
                self._metrics_battle_end()
                # tail byte (pkt[9]) KHONG phai hang so co dinh (thay ca 03 lan 04 o cac lan
                # ket tran that khac nhau) -> co ve la bo dem tang dan, KHONG dung lam dieu
                # kien. Chi can in_battle_TRUOC=True la du tin cay (moi lan False truoc do
                # deu la noise, khong lien quan tran).
                self._genuine_end_seen = now
                self._set_battle_end_grace()
                _mark_battle_end(self.party_idx, who=self._label, map_id=self.current_map)
                log.info("[%s] XAC NHAN ket tran THAT (sub0800, in_battle_TRUOC=True) -> "
                         "grace 3s chan 0x35 set lai in_battle + bao party (leader dua vao de ha nhanh)",
                         self._label)
                # BUG THAT (xac nhan qua pcap+log thuc te): nhanh nay LA mot mac ket tran THAT
                # (giong het sub0700 o tren) nhung TRUOC DAY thieu goi reset_enemies(reset_quest=True)
                # -> quest_mode/_battle_counted CUA TRAN TRUOC (vd tran dau <=6 quai, chua latch)
                # bi giu nguyen mai mai sang cac tran sau, du tran sau co >6 quai cung KHONG BAO GIO
                # duoc dem lai -> ket qua: bot chi dung combo/danh thuong (tuong nhu TRAIN mode) o
                # MOI tran sau tran dau tien trong session, bo qua han skill toan man (Hoa Lieu
                # Nguyen...) du da hoc va con SP. User phat hien qua so sanh 2 kich ban: "vao tran
                # >6 con NGAY SAU LOGIN" (dung dung, vi _battle_counted moi = False tu dau) vs "tran
                # >6 con la tran THU HAI trong session" (sai, vi tran dau da set _battle_counted=True
                # roi khong bao gio duoc reset). Sua: reset giong het nhanh sub0700.
                _in_team_dungeon = self.in_team_dungeon()
                self.state.reset_enemies(reset_quest=not _in_team_dungeon)
                self._heal_after_battle()   # hoi HP/SP NGAY khi ket tran (khong doi tick keepalive)
        # Lo (熔爐, opcode 0x59) co NHIEU sub: sub01=shop data (soi lo), sub02=ket qua mua,
        # sub03=phan giai cuon -> Vo Tuong Phien (chips). Truoc day MOI 0x59 deu tang _decompose_seq;
        # gio tach sub01 (soi lo) ra parse rieng, con lai (sub03 phan giai + sub khac) GIU NGUYEN.
        if opcode == 0x59:
            _fsub = pkt[7:9] if len(pkt) >= 9 else b""
            if _fsub == b"\x01\x00":
                self._parse_furnace_shop(pkt)       # SOI LO (S:089-001)
            elif _fsub == b"\x02\x00":
                self._fashion_deposit_seq += 1      # ket qua THA DO THOI TRANG vao S.Tam (S:095-002)
            else:
                self._decompose_seq += 1            # phan giai cuon (S:089-003) + fashion data sub05
        # INT luc login: doc base/equip/turn3 tu 0x05 roi cong collection + horse giong client game.
        if opcode == 0x05 and len(pkt) > 16:
            self._parse_org_id_0x05(pkt)   # QUAN DOAN: orgId==0 = khong co (giong client)
            self._parse_char_login_int(pkt)
            # CAP nhan vat: payload offset 21 = pkt[28] (khop capture: char lv 64). Hien o GUI.
            if len(pkt) > 28 and 1 <= pkt[28] <= 200:
                self.char_level = pkt[28]
            # SKILL DA HOC DAY DU: 0x05 co list [count 2B LE] + count*[skill 2B LE][level 1B].
            # (0x28 chi la skill BAR, thieu skill khong dat phim tat -> char danh chay). Parse o
            # day moi du. UNION (khong ghi de) de khong mat skill tu 0x28.
            self._parse_skill_list_0x05(pkt)
        # PET dang dung: S2C 0x0f sub=0008 = danh sach pet mang theo, record DAU = pet active.
        elif opcode == 0x0f and pkt[7:9] == b"\x08\x00" and len(pkt) >= 49:
            self._cached_pet_list_pkt = pkt
            self._on_pet_list(pkt)
        # Collection style/card dung chung cho char + pet. Moi update deu tinh lai max cua pet active.
        # KET QUA VAO DI GIOI: S:097-001 <進入結果> +結果(1) (`protocal.lua:14111`).
        # Server NOI THANG ly do tu choi, khong phai doan:
        #   0 成功 | 1 等級不足 | 2 時間已滿 | 3 戰鬥中 | 4 事件中 | 5 組隊中 | 6 已在該場景
        # Truoc day bot KHONG doc goi nay (grep `opcode == 0x61` ra rong) nen ban 12 lan roi tu ket
        # luan "nhieu kha nang HET GIO DI GIOI hom nay". Log 07/09 [chutam] to cao ngay dong tren:
        #   00:37:20  SOAT LAI thay CON 120 phut DG (server: da dung 0/120)
        #   00:38:31  VAO DI GIOI THAT BAI sau 12 lan -> nhieu kha nang HET GIO DI GIOI hom nay
        # Con nguyen 120/120 phut - that ra no la `(member)`, tuc ma 5 DANG TO DOI.
        elif opcode == 0x61 and len(pkt) >= 10 and pkt[7:9] == b"\x01\x00":
            self._dg_enter_result = pkt[9]
            self._dg_enter_result_luc = time.time()
            self._dg_enter_event.set()      # danh thuc `enter_di_gioi_safe` - no CHO goi nay
            _ly_do = {
                0: "OK",
                1: "CAP KHONG DU",
                2: "HET GIO DI GIOI hom nay",
                3: "DANG CHIEN DAU",
                4: "DANG TRONG SU KIEN",
                5: "DANG TO DOI",
                6: "DA O TRONG DI GIOI",
            }.get(self._dg_enter_result, "ma la")
            if self._dg_enter_result == 0:
                log.info("[%s] Vao Di Gioi: server DONG Y (S:097-001 ma 0)", self._label)
            else:
                log.warning("[%s] Vao Di Gioi BI TU CHOI: %s (S:097-001 ma %d)",
                            self._label, _ly_do, self._dg_enter_result)
        elif opcode == 0x5f and len(pkt) >= 10 and pkt[7:9] == b"\x04\x00":
            count = pkt[9]
            self._collect_style_flags = {i + 1: pkt[10 + i] for i in range(count)
                                         if 10 + i < len(pkt)}
            self._refresh_active_pet_login_stats()
            self._refresh_char_int()
            self._refresh_char_agi()
        elif opcode == 0x5f and len(pkt) >= 11 and pkt[7:9] == b"\x09\x00":
            count = pkt[10]
            self._collect_card_equipped = list(pkt[11:11 + count])
            self._refresh_active_pet_login_stats()
            self._refresh_char_int()
            self._refresh_char_agi()
        elif opcode == 0x5f and len(pkt) >= 10 and pkt[7:9] == b"\x0a\x00":
            count = pkt[9]
            self._collect_card_levels = {
                pkt[10 + i * 2]: pkt[11 + i * 2]
                for i in range(count) if 11 + i * 2 < len(pkt)
            }
            self._refresh_active_pet_login_stats()
            self._refresh_char_int()
            self._refresh_char_agi()
        # Horse login: diem thu 2 la INT base. Trang bi horse aggregate den rieng o sub0800.
        elif opcode == 0x4f and len(pkt) >= 14 and pkt[7:9] == b"\x01\x00":
            self._on_mount_data(pkt)
        elif opcode == 0x4f and len(pkt) >= 10 and pkt[7:9] == b"\x02\x00":
            self._on_mount_level(pkt)
        elif opcode == 0x4f and len(pkt) >= 12 and pkt[7:9] == b"\x03\x00":
            self._on_mount_point(pkt)
        elif opcode == 0x4f and len(pkt) >= 10 and pkt[7:9] == b"\x08\x00":
            count = pkt[9]
            for i in range(count):
                off = 10 + i * 10
                if off + 10 > len(pkt):
                    break
                kind, sign = pkt[off], pkt[off + 1]
                value = int.from_bytes(pkt[off + 2:off + 6], "little", signed=True)
                if sign == 2:
                    value = -value
                if kind == 0xd4:  # EAttribute.EquipInt (212)
                    self._mount_equip_int = value
                elif kind == 0xd6:  # EAttribute.EquipAgi (214)
                    self._mount_equip_agi = value
            self._refresh_char_int()
            self._refresh_char_agi()
        # Cap nhat INT khi cong diem (S2C 0x08: 01 00 1b 01 [val 2B])
        if opcode == 0x0e and len(pkt) >= 13 and pkt[7:9] == b"\x11\x00":
            # S:014-017 so ban TOI DA tung co (client: Social.maxRecordFriendCount)
            self.max_friend_count = int.from_bytes(pkt[9:13], "little", signed=True)
        if opcode == 0x08 and len(pkt) >= 15 and pkt[7:9] == b"\x01\x00":
            # CHI SO GOC (dung cau truc client: [kind 1B][sign 1B][value i32][arg i32]).
            # Ghi rieng, khong dung vao cac nhanh cu ben duoi.
            _v = int.from_bytes(pkt[11:15], "little", signed=True)
            self.char_attrs[pkt[9]] = -_v if pkt[10] == 2 else _v
            # Client goc: EAttribute.Exp = 36 (0x24), attrValue la EXP TONG moi. Protocol nay
            # moi la nguon server dang gui sau tran tren ban mobile hien tai; 0x14/sub2a khong
            # xuat hien trong capture that. Lay delta giua hai gia tri de log EXP tung tran.
            if pkt[9] == 0x24:
                _new_exp = -_v if pkt[10] == 2 else _v
                _old_exp = self.char_exp
                self.char_exp = _new_exp
                if _old_exp is not None and _new_exp > int(_old_exp):
                    _gain = _new_exp - int(_old_exp)
                    _row = {"type": "exp", "time": time.strftime("%H:%M:%S"),
                            "who": "character", "kind": 1, "exp": int(_gain),
                            "total": int(_new_exp), "source": "0x08/01 attr36"}
                    self.combat_exp_log.appendleft(dict(_row))
                    self.activity_log.appendleft(dict(_row))
                    self._metrics_exp_gain("character", _gain)
                    log.info("[%s] EXP NHAN VAT: +%d (tong=%d, S:008-001 attr=36)",
                             self._label, _gain, _new_exp)
        # S:008-002 <dat thuoc tinh pet>: humanKind(1), petIndex(2), attrKind(1), sign(1),
        # value i32, arg i32. EHuman.FollowNpc=4, EAttribute.Exp=36.
        if (opcode == 0x08 and len(pkt) >= 22 and pkt[7:9] == b"\x02\x00"
                and pkt[9] == 4 and pkt[12] == 0x24):
            _pet_index = int.from_bytes(pkt[10:12], "little")
            _raw_exp = int.from_bytes(pkt[14:18], "little", signed=True)
            _new_pet_exp = -_raw_exp if pkt[13] == 2 else _raw_exp
            _old_pet_exp = self.pet_exp_values.get(_pet_index)
            self.pet_exp_values[_pet_index] = _new_pet_exp
            if _old_pet_exp is not None and _new_pet_exp > int(_old_pet_exp):
                _gain = _new_pet_exp - int(_old_pet_exp)
                _pet_label = (str(getattr(self, "pet_name", "") or "")
                              if _pet_index == int(getattr(self.state, "active_pet_id", 0) or 0)
                              else "Pet %d" % _pet_index)
                _row = {"type": "exp", "time": time.strftime("%H:%M:%S"),
                        "who": "pet", "kind": 2, "exp": int(_gain),
                        "total": int(_new_pet_exp), "pet": _pet_label,
                        "pet_id": int(_pet_index), "source": "0x08/02 attr36"}
                self.combat_exp_log.appendleft(dict(_row))
                self.activity_log.appendleft(dict(_row))
                self._metrics_exp_gain("pet", _gain)
                log.info("[%s] EXP PET %s: +%d (tong=%d, S:008-002 attr=36)",
                         self._label, _pet_label or _pet_index, _gain, _new_pet_exp)
        if opcode == 0x08 and len(pkt) >= 13 and pkt[7:9] == b"\x01\x00" and pkt[9] == STAT_INT and pkt[10] == 0x01:
            self._char_int_base = int.from_bytes(pkt[11:13], "little")
            self._refresh_char_int()
        # S:008-013 <設定主角技能> +元素(1) +技能點數(2) +技能數量(2) + <<+技能ID(2) +等級(1)>>
        # NGUON DUY NHAT co CAP cua tung skill nhan vat (0x05 co [id][lv] nhung parser cu vut lv,
        # 0x28 chi la skill BAR). Can cho tu nang skill: biet cap hien tai moi tinh duoc con ton
        # bao nhieu diem. Xem KNOWLEDGE muc 7q.
        # LUU Y: chu thich giao thuc ghi 技能數量(1) nhung `Role.ReceivePlayerSkillData`
        # (Logic/Role.lua:1729) doc `ReadUInt16()` -> count la 2 BYTE. Code moi la chan ly.
        elif opcode == 0x08 and len(pkt) >= 14 and pkt[7:9] == b"\x0d\x00":
            self._on_char_skill_data(pkt)
        elif opcode == 0x08 and len(pkt) >= 13 and pkt[7:9] == b"\x01\x00" and pkt[9] == STAT_AGI and pkt[10] == 0x01:
            self._char_agi_base = int.from_bytes(pkt[11:13], "little")
            self._refresh_char_agi()
        # HP/SP LIVE: S2C 0x08 sub=0100. 0x19=HP, 0x1a=SP. Ban CA NGOAI combat -> nguon HP/SP de
        # hoi mau (0x33 chi trong tran).
        # CAU TRUC THAT (Common_protocal.lua:1127): S:008-001 <設定主角屬性>
        #     +種類(1) +正負號(1) +數值(4) +參數(4)
        # => byte thu 2 la DAU (1 duong / 2 am), KHONG phai "unit char/pet"; gia tri 4 BYTE.
        # Goi nay la 主角 = CHI NHAN VAT CHINH (pet di goi S:008-002, co them 人物種類/索引).
        # Nhanh `unit == 0x02 -> state.pet` ben duoi vi vay la SAI VE NGHIA, nhung DO CAP:
        # quet het capture trong repo -> 369/369 goi HP/SP deu sign=1, chua bao gio co sign=2
        # (goi nay la "DAT gia tri", khong phai "cong/tru" nen khong can dau am). Doc 2 byte thay
        # vi 4 cung dung chung nao gia tri < 65536. GIU NGUYEN de khong dung vao logic hoi mau;
        # neu sau nay thay HP/SP pet nhay lung tung thi day la cho dau tien can soi.
        elif opcode == 0x08 and len(pkt) >= 13 and pkt[7:9] == b"\x01\x00" and pkt[9] in (0x19, 0x1a):
            stat = pkt[9]
            unit = pkt[10]
            val = int.from_bytes(pkt[11:13], "little")
            if unit == 0x01:
                tgt = self.state.char
            elif unit == 0x02:
                tgt = self.state.pet
            else:
                tgt = None
            if tgt is not None:
                if stat == 0x19:
                    tgt.hp = val
                else:
                    tgt.sp = val
        # DUNG ITEM xac nhan: S2C 0x17 sub=0900 [slot 1B][01]... -> item o slot do dung THANH CONG.
        # Tru count slot, set co confirm (probe confirm-gated: co confirm = item DUNG DUOC).
        elif opcode == 0x17 and len(pkt) >= 11 and pkt[7:9] == b"\x09\x00":
            slot = pkt[9]
            if slot == self._pending_confirm_slot:
                self._use_confirmed = True
            # `S:023-009 <背包減少物品> +索引(1) +數量(4)` - goi MANG SO LUONG. Truoc day bot vut
            # so luong di va luon tru 1 -> moi lan tieu thu NHIEU cai mot luc la tui ao lech han.
            # Bug that (log 10:02): nang cap thu cuoi an 100 'Tang Cap Ky Don', bot chi tru 1 ->
            # tuong con 102 trong khi that su con 3 -> tinh sai "thieu", roi mo the ma khong bao
            # gio thay so dan tang (moc so sanh lech 99) -> bao "tui day" va MAT TRANG the.
            n = 1
            if len(pkt) >= 14:
                n = int.from_bytes(pkt[10:14], "little", signed=True)
                if n <= 0 or n > 10_000_000:
                    n = 1
            rec = self.bag_slots.get(slot)
            if rec:
                rec[1] = max(0, rec[1] - n)
                tid = rec[0]
                if tid in self.bag_counts:
                    self.bag_counts[tid] = max(0, self.bag_counts[tid] - n)
        # TRANG BI DANG MAC luc login: [count u8] + count * ThingData 35B.
        elif opcode == 0x17 and len(pkt) >= 10 and pkt[7:9] == b"\x0b\x00":
            self._parse_equipment_snapshot(pkt)
        # DA MAC XONG - 2 goi RIENG, deu KHAC voi sub 0b (sub 0b = danh sach do luc LOGIN, khong
        # phai tra loi cua tung lan mac):
        #   S:023-017 <穿上裝備>     [bagIndex]           -> nhan vat (sub 17 = 0x11)
        #   S:023-023 <武將穿上裝備> [petIndex][bagIndex] -> pet      (sub 23 = 0x17)
        elif opcode == 0x17 and len(pkt) >= 10 and pkt[7:9] == b"\x11\x00":
            self._on_equip_done(pkt[9], follow=0)
        elif opcode == 0x17 and len(pkt) >= 11 and pkt[7:9] == b"\x17\x00":
            self._on_equip_done(pkt[10], follow=pkt[9])
        # COI DO XONG - server XAC NHAN. Truoc day bot KHONG bat 2 goi nay: gui lenh coi thi mon
        # co ve tui (0x17 sub08 bot co bat) nhung bang "do dang mac" van hien y nguyen -> user bam
        # "Cởi ra" xong thay KHONG CO GI XAY RA (bao 26/08).
        #   S:023-016 <卸下裝備>       +vi tri do(1) +o tui(1)              -> sub 16 = 0x10 (char)
        #   S:023-021 <武將卸下裝備>   +petIdx(1) +vi tri do(1)             -> sub 21 = 0x15 (pet)
        #   S:023-022 <武將卸下裝備到背包> +petIdx(1) +vi tri do(1) +o tui(1) -> sub 22 = 0x16 (pet)
        # `o tui` PHAI dung: server KHONG gui 023-008 cho mon vua coi, chinh CLIENT bo no vao tui
        # (xem `_on_unequip_done`). Bo qua truong nay -> `bag_slots` khong biet mon do -> lan coi
        # THU HAI chon trung o cu, server nuot im.
        elif opcode == 0x17 and len(pkt) >= 10 and pkt[7:9] == b"\x10\x00":
            self._on_unequip_done(pkt[9], follow=0,
                                  o_tui=(pkt[10] if len(pkt) >= 11 else None))
        elif opcode == 0x17 and len(pkt) >= 11 and pkt[7:9] == b"\x15\x00":
            self._on_unequip_done(pkt[10], follow=pkt[9])
        elif opcode == 0x17 and len(pkt) >= 12 and pkt[7:9] == b"\x16\x00":
            self._on_unequip_done(pkt[10], follow=pkt[9], o_tui=pkt[11])
        # HAN DUNG "Ba Dau" / Da Thu Ho / The Vip Thang - xem KNOWLEDGE.md muc 7p.
        #   S:023-135 <各種到期時間> +ket qua(1) +loai(1) [+thoi gian(8) neu ket qua=1]
        #   loai 1 = Ba Dau (auto day HP/SP sau tran) | 2 = Da Thu Ho | 3 = The Vip Thang
        # sub 135 = 0x87. `thoi gian` la MOC HET HAN dang OLE Automation Date (double) - KHONG
        # phai so giay con lai, KHONG phai unix timestamp.
        elif opcode == 0x17 and len(pkt) >= 11 and pkt[7:9] == b"\x87\x00":
            self._on_han_dung(pkt)
        # S:023-024 <武將穿上裝備> [武將索引][物品資料 16B] - server bao PET DANG MAC mon nay.
        # Client dung goi NAY de dung trang thai do pet (protocolTable[23][20] = <武將所有裝備> la
        # ham RONG - no khong dung goi do). Bot can bang nay vi cung ly do nhu do char: THAY DO thi
        # phai biet mon cu de tra ve tui. ThingData mo dau bang Id (2B LE).
        elif opcode == 0x17 and len(pkt) >= 12 and pkt[7:9] == b"\x18\x00":
            _fl = pkt[9]
            _tid = int.from_bytes(pkt[10:12], "little")
            _fit = int((_load_gamedata_items().get(_tid) or {}).get("ft", 0) or 0)
            if _tid and _fit:
                self.pet_equip_by_fit.setdefault(_fl, {})[_fit] = _tid
        # DO BEN DOI (S:023-027 = sub1b00): [vi tri do 1B][damage 1B]. Moc 100/200/250 (250=HONG).
        # Bot CHI theo ngoc CHAR o vi tri 6 (ngoc Phuc Than khong deo cho pet duoc).
        elif opcode == 0x17 and len(pkt) >= 11 and pkt[7:9] == b"\x1b\x00":
            self._on_equip_damage(pkt[9], pkt[10])
        # DO HONG -> server THAY HAN ban ghi (S:023-035 = sub2300): [vi tri 1B][ThingData 35B]
        # [followIndex 1B]. Ngoc hong -> id thanh 0x59f0 (Ngoc Hu), damage=250, nhung damagedItemId
        # VAN giu id ngoc goc. followIndex != 0 = do cua PET -> bo qua.
        elif opcode == 0x17 and len(pkt) >= 45 and pkt[7:9] == b"\x23\x00":
            self._on_equip_broken(pkt)
        # THANH TUU (opcode 82): S:082-002 <成就領獎> [result 1B] (+id u16 neu result==0).
        # result 0 = nhan OK -> server cung set bit getFlag qua 0x51 delta. 1 = fail.
        elif opcode == 0x52 and len(pkt) >= 12 and pkt[7:9] == b"\x01\x00":
            # S:082-001 ket qua BAO HOAN THANH: [result 1B][id u16]
            _res = pkt[9]
            _aid = int.from_bytes(pkt[10:12], "little")
            if _res == 0:
                self._ach_report_ok = getattr(self, "_ach_report_ok", 0) + 1
                self._ach_report_bad = 0
            elif _res != 2:      # 2 = server da biet roi, binh thuong
                self._ach_report_bad = getattr(self, "_ach_report_bad", 0) + 1
                _a = _load_achievements().get(_aid) or {}
                log.warning("[%s] Thanh tuu: bao hoan thanh '%s' (id=%d) BI TU CHOI - %s",
                            self._label, _a.get("name", "?"), _aid,
                            {1: "khong co du lieu", 3: "DIEU KIEN KHONG DU (cong thuc bot sai?)",
                             4: "du lieu loi"}.get(_res, "ma %d" % _res))
        elif opcode == 0x52 and len(pkt) >= 10 and pkt[7:9] == b"\x02\x00":
            _res = pkt[9]
            if _res == 0 and len(pkt) >= 12:
                _aid = int.from_bytes(pkt[10:12], "little")
                _a = _load_achievements().get(_aid) or {}
                log.info("[%s] Thanh tuu: NHAN OK '%s' (id=%d)",
                         self._label, _a.get("name", "?"), _aid)
            elif _res != 0:
                log.warning("[%s] Thanh tuu: nhan qua THAT BAI (result=%d)", self._label, _res)
        # S:065-002 <暫停機關盒> (protocal.lua:11551): server bao HOP MAY DA DUNG -> char thoi tu
        # danh, quai di ngang qua ma khong vao tran. Truoc day bot KHONG doc goi nay -> bi dung ma
        # khong he biet, dung ngay den khi user phat hien. Bat lai va tu ARM lai (chi gui 0x41
        # start, KHONG ca _login_setup vi trong do co 0x7c/0x62 gay side-effect). Cach nhau it
        # nhat 30s de neu server co ly do dung that thi khong thanh vong gui lien tuc.
        elif opcode == 0x41 and len(pkt) >= 9 and pkt[7:9] == b"\x02\x00":
            # ACK cua CHINH MINH: _login_setup co chu y gui 0x41 0200 roi moi BAT lai o cuoi chuoi.
            # Khong loc thi bot tu chen goi bat-lai vao GIUA chuoi login (xem ghi chu o _login_setup)
            # -> vua thua, vua co nguy co lam hong trinh tu dang nhap.
            if time.time() - float(getattr(self, "_machinebox_pause_sent_at", 0.0) or 0.0) < 10.0:
                return
            _last = getattr(self, "_machinebox_rearm_at", 0.0)
            _map = int(getattr(self, "current_map", 0) or 0)
            log.warning("[%s] HOP MAY bi server DUNG (S:065-002) o map %s -> quai se khong "
                        "vao tran", self._label, _map)
            # MAP CAM HOP MAY thi KHONG duoc bat lai. Client that kiem TRUOC khi bat
            # (`_lua_dec/Logic/MachineBox.lua:360`, `SetAutoFight`):
            #     if SceneManager.CheckLimit(SceneManager.sceneId, ESceneLimit.NoMachinebox)
            #       then ShowCenterMessage(...); return;    -- KHONG gui goi
            # (`ESceneLimit.NoMachinebox = 13`, bit trong `sceneDatas[sceneId].limits`.) Va
            # client KHONG BAO GIO tu bat lai khi nhan `S:065-002` - no chi SetAutoFight(false).
            #
            # Bot khong co bang `limits`, nhung chinh `S:065-002` LA cau tra loi cua server:
            # map nay cam. Bat lai o do la cai server, va cai gia la DUT KET NOI:
            #   07/09 party 1 (40NPC, map 10991) - user: "bat dau tran thi leader dis luon":
            #     21:06:53 HOP MAY bi server DUNG (S:065-002) -> da bat lai hop may
            #     21:06:57 <<nhan 0x34            (vao tran)
            #     21:06:57 <<nhan 0x14 08 00 03   S:020-008 <事件結束>
            #     21:06:57 SERVER NGAT KET NOI: ma la 47  (戰鬥未結束事件先結束)
            #   `gamo` dinh y het 32 giay sau. Bang "goi gan nhat truoc khi rot" khong co goi
            #   GUI nao - thu phat la lenh bat-lai tu 4 giay truoc do.
            if _map and _map in self._machinebox_map_cam:
                log.info("[%s] map %s DA TUNG cam hop may -> KHONG bat lai (client that "
                         "cung khong bat)", self._label, _map)
                return
            if _map:
                self._machinebox_map_cam.add(_map)
            if time.time() - _last >= 30.0:
                self._machinebox_rearm_at = time.time()
                try:
                    self.send(0x41, b"\x01\x00" + self.machinebox_payload())
                    log.info("[%s] -> da bat lai hop may (lan dau o map %s)", self._label, _map)
                except OSError:
                    pass
        # S:000-000 <斷線> +斷線原因(1): server BAO TRUOC ly do roi moi dong ket noi.
        # Layout (xac nhan tu dump that): [header 7B][sub 2B = 00 00][cause 1B][...]
        #   ...00 00 5a...  -> 5a = 90 = dang nhap qua thuong xuyen   (1232/1574 lan trong 1 phien)
        #   ...00 00 13...  -> 13 = 19 = dang nhap trung lap          (212 lan)
        #   ...00 00 0e...  -> 0e = 14 = di chuyen qua xa             (84 lan)
        elif opcode == 0x00 and len(pkt) >= 10 and pkt[7:9] == b"\x00\x00":
            self.disconnect_cause = pkt[9]
            self.disconnect_reason = DISCONNECT_CAUSE.get(pkt[9], "ma la %d" % pkt[9])
            log.warning("[%s] SERVER NGAT KET NOI: %s (ma %d)",
                        self._label, self.disconnect_reason, pkt[9])
        # S:020-049 <武將學習特殊技> +武將索引(1): pet VUA MO duoc dac ky (skill phai lam nhiem vu
        # moi co). Bat goi nay de biet NGAY, khong phai cho login lai doc goi pet list.
        # Client: protocal.lua:3140 -> followNpc.data.specialSkillLearned = true.
        # LUU Y: goi cho INDEX vo tuong (slot mang theo), khong cho pet_id -> doi chieu qua
        # carried_pets (thu tu = marker slot doc o _on_pet_list).
        elif opcode == 0x14 and len(pkt) >= 10 and pkt[7:9] == b"\x31\x00":
            _idx = pkt[9]
            _pets = list(getattr(self.state, "carried_pets", None) or ())
            _pid = _pets[_idx - 1][0] if 1 <= _idx <= len(_pets) else None
            if _pid:
                self.pet_special_skill[_pid] = True
                self._nho_dac_ky(_pid)
            log.info("[%s] PET vua MO DAC KY (S:020-049 idx=%d, pet=%s)", self._label, _idx,
                     ("0x%04x" % _pid) if _pid else "chua biet")
        elif opcode == 0x18:
            self._on_mission_steps(pkt)
        # INVENTORY (TUI THAT): S2C 0x17 sub=0500. header [00][count 2B] + record 36B:
        #   [idx 1B][item_id 2B LE][count 4B LE][29 pad]. idx = use-id (dung item gui [idx][01]).
        #   bag_slots[idx]=[item_id, count]; bag_counts[item_id]=tong. Snapshot day -> THAY THE.
        elif opcode == 0x17 and len(pkt) >= 12 and pkt[7:9] == b"\x05\x00":
            body = pkt[9:]
            n = int.from_bytes(body[1:3], "little")
            off = 3
            new_slots = {}
            new_info = {}
            for _ in range(n):
                if off + 7 > len(body):
                    break
                idx = body[off]
                item_id = int.from_bytes(body[off + 1:off + 3], "little")
                cnt = int.from_bytes(body[off + 3:off + 7], "little")
                _td = body[off + 1:off + 36]     # 35 byte ThingData ngay sau idx
                off += 36
                if 0 < idx < 256 and item_id > 0 and 0 < cnt < 10_000_000:
                    new_slots[idx] = [item_id, cnt]
                    if len(_td) >= 35:
                        new_info[idx] = thing_data_info(_td)
            if new_slots:
                self.bag_items = new_info
                self.bag_slots = new_slots
                self.bag_counts = {}
                for it, c in self.bag_slots.values():
                    self.bag_counts[it] = self.bag_counts.get(it, 0) + c
                self._bag_time = time.time()   # moc nhan snapshot tui (cho log_bag_delayed adaptive)
                self._ghi_cache_tui()   # snapshot DAY (login) - mot trong hai moc ghi cache
        # NHAN/DROP ITEM 1 SLOT: S2C 0x17 sub=0800 (023-008 <bag set item> [slot 1B][item data][showMsg]).
        # Server chi gui SLOT thay doi (khong phai ca tui). Layout id/count giong record snapshot:
        #   [slot][item_id 2B LE][count 4B LE]. CHI log TEN item khi count TANG (nhan duoc) - dung
        #   bag_slots cu (O(1), khong diff ca tui); khong log so luong. Bo qua khi count giam (dung item).
        elif opcode == 0x17 and len(pkt) >= 16 and pkt[7:9] == b"\x08\x00":
            slot = pkt[9]
            item_id = int.from_bytes(pkt[10:12], "little")
            cnt = int.from_bytes(pkt[12:16], "little")
            if item_id > 0 and 0 < cnt < 10_000_000:
                old = self.bag_slots.get(slot)
                old_cnt = old[1] if old and old[0] == item_id else 0
                self.bag_slots[slot] = [item_id, cnt]
                self.bag_counts[item_id] = self.bag_counts.get(item_id, 0) - old_cnt + cnt
                if cnt > old_cnt:   # thuc su NHAN them (khong phai dung item/giam)
                    nm = (_load_gamedata_items().get(item_id) or {}).get("name") or ("0x%04x" % item_id)
                    self.activity_log.appendleft({"type": "loot", "time": time.strftime("%H:%M:%S"),
                                                  "item": nm.strip(), "item_id": int(item_id),
                                                  "qty": int(cnt - old_cnt)})
                    log.info("[%s] Nhan item: %s", self._label, nm.strip())
        # BAN BE / qua hang ngay: S2C 0x0e
        #   sub 05 = list ban luc login: [05 00][count 2B] + N*[entity 8B][namelen 1B][name][trailer 35B]
        #   sub 0c = status qua:        [0c 00][count 1B] + N*[entity 8B][status 1B] (03=co qua nhan, 07=da nhan)
        if opcode == 0x0e and len(pkt) >= 9:
            self._on_friend_gift(pkt)
        # NHIEM VU HANG NGAY (bingo 9 o): chi can cell done cho logic daily cu.
        if opcode == 0x5b:
            self._on_daily_quest_packet(pkt)
        # HANG/COT DA NHAN thuong: frame 0x51 (gui luc login) chua bitmask line da nhan, ngay sau
        # marker "c0 fe 03 00 00 00" la 2 byte mask (uint16 LE) - line L da nhan = bit (L+3).
        # (Tim duoc nho raw-decode frame LON ma analyze_pcap drop; verify khop tren nhieu nick.)
        if opcode == 0x51:
            self._apply_bitflags(pkt)
            if len(pkt) >= 11 and pkt[7:9] == b"\x02\x00":
                size = int.from_bytes(pkt[9:11], "little")
                self._mount_collection_count = pet_login_stats.mount_collection_count(
                    pkt[11:11 + size], _load_pet_stat_data())
                self._refresh_char_int()
                self._refresh_char_agi()
            # DU PHONG (chi khi BitFlag CHUA ve): quet pattern `c0 ?? 03000000 [mask 2B] 01000000`,
            # line L da nhan = bit (L+3). Cach nay KHONG chac (co capture khong match duoc) -> chi
            # dung tam; nguon CHINH la BitFlag o _refresh_quest_claimed_from_bitflags() (giong client).
            for i in range(0 if not self._bitflags_loaded else len(pkt), len(pkt) - 12):
                if pkt[i] == 0xc0 and pkt[i+2:i+6] == b"\x03\x00\x00\x00" and pkt[i+8:i+12] == b"\x01\x00\x00\x00":
                    mask = int.from_bytes(pkt[i+6:i+8], "little")
                    self._claimed_lines = {L for L in range(1, 8) if (mask >> (L + 3)) & 1}
                    self._claimed_loaded = True
                    break
        # Track map_id hien tai: 0x0c/0x07 = [00 00][entity 8B][map_id 2B]...
        # CHI doc map khi entity == CHINH MINH (tranh bi NHIEM map cua nguoi xung quanh ben
        # canh map khac -> doc nham 12842 thay vi 12831). self_entity None (luc login) -> tam lay.
        if opcode in (0x0c, 0x07) and len(pkt) >= 19 and pkt[7:9] == b"\x00\x00":
            ent = pkt[9:17]
            # NGUOI KHAC doi scene: client Lua lam dung the (`protocolTable[12][0]`:
            # `if roleId ~= playerId and sceneId ~= SceneManager.sceneId then ... RemoveMember`).
            # Ho sang map khac = khong con cung cho voi minh -> HUY co "da thay tan mat".
            if (self.self_entity is not None and ent != self.self_entity
                    and self.current_map is not None):
                _m = (self.entity_meta or {}).get(bytes(ent))
                if _m and _m.get("appear_at"):
                    try:
                        if int.from_bytes(pkt[17:19], "little") != int(self.current_map):
                            _m["appear_at"] = 0.0
                            _m["nearby"] = False
                    except Exception:
                        pass
            if self.self_entity is None or ent == self.self_entity:
                mid = int.from_bytes(pkt[17:19], "little")
                if mid > 1000:   # loc gia tri rac (map_id that >1000)
                    if self.current_map is not None and mid != self.current_map:
                        # DOI SCENE do SERVER DAY: bi leader KEO qua cong theo party. Minh KHONG tu
                        # goi _enter_gate nen KHONG ai chay scene_resume() -> thieu `0x14 06` ->
                        # lenh move dau tien bi tu choi: "di chuyen QUA XA (ma 14)" -> KICK.
                        # Log 18:29 map 21826: leader TU di qua cong (co resume) -> song; 4 member bi
                        # KEO qua -> khong resume -> ca 4 rot trong CUNG MOT GIAY.
                        # CHI dat co o day (dang trong recv loop) - scene_resume() co sleep, de
                        # navigate_to goi, khong duoc chan luong doc goi.
                        #
                        # LOAI TRU doi map do CHINH BOT ra lenh (teleport/thoat DG): duong do KHONG
                        # can resume, va gui `0x14 06` sai luc thi SERVER DONG KET NOI.
                        # BUG THAT do chinh cho nay (log 15:09): 3 acc vua thoat DG bang teleport
                        # (49942 -> 12003 -> 12001) roi chuyen pha train -> navigate_to ban 0x14 06
                        # -> ca 3 RUNG. Acc con dang danh boss khong di dau nen KHONG rung.
                        _tu_minh = (time.time() - getattr(self, "_self_map_change_at", 0.0)) < 15.0
                        if not _tu_minh and not getattr(self, "_in_scene_gate", False):
                            self._need_scene_resume = True
                            self._need_scene_resume_at = time.time()
                    # DOI MAP -> SO KENH CU HET GIA TRI, phai lay lai. Goi nay (`0x0c`/`0x07`
                    # ChangeScene) khong kem so kenh, nen giu so cu la KHAI BAO MOT SO KHONG CON
                    # DUNG. Nguy nhat la khi map cu la INSTANCE (Di Gioi, thap 2K, boss quan doan):
                    # o do `current_channel` la instanceId chu khong phai kenh the gioi.
                    #
                    # Ca that 08/09 party 11 (user: "no bia kenh 27 o dau ra day" -> "p11 dang o
                    # Giang Lang, lien quan deo gi den Di gioi nua, khi doi map may phai lay lai
                    # kenh chu"): 5 acc da ve Giang Lang 21011 ma van khai kenh 27/28/29/30/31 -
                    # instanceId sot lai. Dieu phoi chot 27, ca nam acc an ma 2 <沒有該分區>.
                    #
                    # `None` = CHUA RO: `_dieu_phoi_chot_kenh` gap acc chua ro kenh thi khong quyet
                    # (`if m is None or not ch: return None`), con `_leader_tu_kiem_kenh` /
                    # `refresh_current_channel` se hoi lai bang lenh that.
                    if int(mid) != int(self.current_map or 0):
                        self.current_channel = None
                    self.current_map = mid
                # TOA DO cung nam trong chinh goi nay: [entity 8B][map u16][x u16][y u16]
                # (KNOWLEDGE muc 7: 0x07 sub0000 - 0x0c ChangeScene CUNG layout, xac nhan bang
                # capture 2K: vao 12922 -> (1490,490), dung bang dong RESYNC trong log).
                # Truoc day CHI doc map, VUT x/y -> qua cong xong pos=None -> navigate_to mat smart
                # path -> di mu 30 lenh (~45s/chang). Va refresh_server_position() gui 0x0c 0100 roi
                # cho _position_generation doi, ma bien do CHI tang o 0x03 -> luon timeout.
                if len(pkt) >= 23:
                    sx = int.from_bytes(pkt[19:21], "little")
                    sy = int.from_bytes(pkt[21:23], "little")
                    if 0 < sx < 20000 and 0 < sy < 20000:
                        self.pos = (sx, sy)
                        self._position_generation += 1
                        # Toa do nay DI KEM chinh lan doi map nay -> _enter_gate KHONG duoc
                        # xoa (xem _gate_reached).
                        self._pos_valid_for_map = self.current_map
                # 0x0c ChangeScene: sau toa do la sceneTag(2) ROI MOI den instanceId(2).
                # Client Lua protocolTable[12][0]: roleId -> sceneId -> position(x,y) -> sceneTag
                # -> instanceId. Capture 2K xac nhan: sceneTag=0, instanceId=1.
                # KENH = instanceId -> pkt[25:27]. Truoc day doc pkt[23:25] = SCENETAG (luon 0
                # trong thap) nen kenh doc tu 0x0c la RAC.
                if opcode == 0x0c and len(pkt) >= 27:
                    self._note_current_channel(int.from_bytes(pkt[25:27], "little"), "0x0c")
        # S:001-001 <玩家離線> +玩家ID(8): server BAO nguoi do da roi khoi tam nhin cua minh
        # (thoat game / doi kenh / di sang scene khac). Day la ve con lai cua cap voi `0x03
        # PlayerAppear`: co goi nay thi "da thay tan mat" phai bi HUY, khong thi bot cu tuong
        # nguoi ta con dung canh minh mai (`appear_at` khong bao gio het han - va DUNG la khong
        # duoc het han theo thoi gian, xem `da_thay_tan_mat`).
        # GIO SERVER - can de tinh han dung Ba Dau (xem KNOWLEDGE.md muc 7p): client so han voi
        # `CGTimer.serverTime`, KHONG phai gio may. Hai nguon, deu la OLE Automation Date (double):
        #   S:001-000 (login): body = ketqua(1) dbId(2) sId(2) playerId(8) SERVER_TIME(8) -> pkt+22
        #   S:001-016 <收Server時間> +Server時間(8)                                       -> pkt+9
        if opcode == 0x01 and len(pkt) >= 30 and pkt[7:9] == b"\x00\x00":
            self._nho_gio_server(pkt[22:30])
        if opcode == 0x01 and len(pkt) >= 17 and pkt[7:9] == b"\x10\x00":
            self._nho_gio_server(pkt[9:17])
        # S:001-002 <登入結果>: server phat lai CAP acc/pwd rieng ("<user_id>@vtc" + ve 10 ky tu).
        # Do MOI la thu server vo gioi doi, KHONG phai user_id/access_token bot dung de login.
        if opcode == 0x01 and pkt[7:9] == b"\x02\x00":
            _kq = parse_ket_qua_login(pkt[7:])
            if _kq:
                self._vg_acc, self._vg_pwd = _kq
        # S:001-013 <登入送完所有資訊> +連線序號(4) - goi auth vo gioi mang lai so nay.
        if opcode == 0x01 and pkt[7:9] == b"\x0d\x00":
            _cc = parse_connect_code(pkt[7:])
            if _cc is not None:
                self._connect_code = _cc
        # S:001-020 <通知連無界伺服器> = server bao "event LIEN SERVER, sang may khac ma choi".
        # Loan dau THU 7 la loai nay (map 54901 nam o IP khac). Bo qua goi nay = dung im o map
        # cu mai mai - log 05/09 21:20 party 11: cho 20s roi bao CHUA TOI, ca buoi 0 tran.
        if opcode == 0x01 and pkt[7:9] == b"\x14\x00":
            _bc = parse_bao_chuyen_vo_gioi(pkt[7:])
            if _bc:
                self._vo_gioi = _bc
                log.info("[%s] SERVER BAO CHUYEN (vo gioi): %s:%s serverId=%s SN=%s",
                         self._label, _bc["host"], _bc["port"], _bc["server_id"], _bc["sn"])
        # S:001-021 <通知連回原SERVER> GSID(2) = het event lien server, ve may cu.
        if opcode == 0x01 and pkt[7:9] == b"\x15\x00":
            log.info("[%s] SERVER BAO VE MAY CU (het event lien server)", self._label)
            self._vo_gioi_ve = True
        if opcode == 0x01 and len(pkt) >= 17 and pkt[7:9] == b"\x01\x00":
            _ra = bytes(pkt[9:17])
            _m = (self.entity_meta or {}).get(_ra)
            if _m and _m.get("appear_at"):
                _m["appear_at"] = 0.0
                _m["nearby"] = False
        # 0x03 PlayerAppear: server gui cho ca self va nguoi xung quanh.
        # Chi self-spawn moi dung de cap nhat map/pos; nguoi xung quanh dung de cache ten/entity
        # cho whitelist invite.
        if opcode == 0x03 and len(pkt) >= 30 and pkt[7:9] == b"\x00\x00":
            ent = pkt[9:17]
            self._remember_entity_name_from_03(pkt)
            if self.self_entity is None or ent == self.self_entity:
                mid = int.from_bytes(pkt[28:30], "little")
                if mid > 1000:
                    # doi map -> so kenh cu het gia tri (xem ghi chu o nhanh `0x0c`). O day goi
                    # KEM so kenh nen ngay sau se `_note_current_channel` lai bang so DUNG.
                    if int(mid) != int(self.current_map or 0):
                        self.current_channel = None
                    self.current_map = mid
                ch = self._parse_channel_from_03(pkt)
                if ch is not None:
                    self._note_current_channel(ch, "0x03")
                # RESYNC vi tri THAT do server cap: 0x03 self-spawn co toa do o payload
                # offset 23/25 = pkt[30:32]/pkt[32:34] (relogin.pcap: f2 03=1010, ca 03=970).
                # Sua dead-reckoning bi lech sau khi di xa/qua cong. Login=dung cho logout.
                if len(pkt) >= 34:
                    sx = int.from_bytes(pkt[30:32], "little")
                    sy = int.from_bytes(pkt[32:34], "little")
                    if 0 < sx < 20000 and 0 < sy < 20000:
                        self.pos = (sx, sy)
                        self._position_generation += 1
                        self._pos_valid_for_map = self.current_map
                        log.info("[%s] RESYNC pos tu 0x03 = (%d,%d) map=%s",
                                 self._label, sx, sy, self.current_map)
            # TEN NHAN VAT tu 0x03 self-spawn (nguon dang tin: MOI acc co, KHONG can bang hoi).
            if self.self_entity is None:
                self._pending_03 = pkt   # chua biet self -> cache, retry khi 0x69 toi
            elif self.char_name is None and ent == self.self_entity:
                self._resolve_name_from_03(pkt)
        # KHONG co cach hoi vi tri hien tai, va server KHONG echo lai lenh move cua chinh minh.
        # DA DO BANG SO tren log 30/08: 184 goi `0x06` DA GUI, 0 goi `S:006-001` nhan ve mang
        # entity cua chinh minh (45 goi nhan ve deu la cua nguoi khac). Client that cung the:
        # `protocolTable[6][1]` chi ve NGUOI KHAC di, con avatar cua minh la vi tri LOCAL.
        # -> `self.pos` bat buoc phai la dead-reckoning; cac diem SERVER SUA LAI cho minh chi co:
        #      `S:007-000 <玩家瞬移>` / `S:012-000 <玩家更換場景>` (opcode 0x07/0x0c sub 0000)
        #      `S:013-004 <玩家離開隊伍>` (kem toa do that)
        #      `0x03` self-spawn (login / doi scene)
        # va MOT nguon nua KHONG co goi nao: MEMBER TU DI THEO LEADER (xem `_theo_leader_sua_pos`).
        if opcode == protocol.OP_STAT_UPD and not self.battle_tracker.generation:  # 0x33 legacy
            start_enemy_slots = self.state.update_0x33(pkt)
            if start_enemy_slots:
                self._record_train_block_stats(start_enemy_slots)
        elif opcode == 0x32 and not self.battle_tracker.generation:  # legacy battle action
            self.state.update_0x32(pkt)
        elif opcode == protocol.OP_FULLSTAT:      # 0x0b
            if self.self_entity is None:
                # chua biet self_entity -> buffer lai de xu khi co (tranh mat goi stat luc login)
                self._pending_0b.append(pkt)
                if len(self._pending_0b) > 20:
                    self._pending_0b.pop(0)
            # 0x0b battle (full stat): [entity][10x00][03][SLOT] -> vi tri tran cua minh.
            # Entity-based, dang tin (khong dua HP). Cap nhat moi tran (vi tri co the doi).
            if self.self_entity and len(pkt) > 100:
                idx = pkt.find(self.self_entity)
                if idx >= 0 and idx + 19 < len(pkt) and pkt[idx + 18] == 0x03:
                    slot = pkt[idx + 19]
                    if slot < 10 and slot != self.state.self_slot:
                        self.state.self_slot = slot
                        self.state.my_atype = slot
            self.state.update_0x0b(pkt)
            # Bat TEN QUAI trong tran (entity[2:4]=template_id -> npc_names). Cho dieu kien skill
            # 'quai khoang' + sau nay 'NPC nguy hiem'. Chi lam khi co self_entity (base phien).
            try:
                self.state.note_enemy_entities(pkt, config.NPC_NAMES)
            except Exception:
                pass
        elif opcode == 0x53:                      # mail (S:083-xxx)
            # S:083-001 <新增信件> +數量(4) <<+信件ID(4) +時間(8) +狀態(1) +L(2) +內容(L)
            #                        +附件數量(1) <<+附件種類(1) +du lieu>> >>
            #   狀態 0=chua doc, 1=da doc, 2=DA NHAN qua (EMailState.Unread/Read/Take)
            # Truoc day doc pkt[9:13] tuong 1 mailid (thuc ra la COUNT) -> nhieu mail chi xu 1.
            # Va byte sau title tung bi goi nham la "cat" - no la SO DINH KEM (附件數量), chinh la
            # con so quyet dinh mail nay CO QUA hay khong. Tim record ke bang OLE-time hop le
            # (~46000). Da verify voi capture 9 mail.
            if pkt[7:9] == b"\x01\x00" and len(pkt) >= 13:
                body = pkt[9:]
                cnt = int.from_bytes(body[0:4], "little")
                off = 4
                for _ in range(min(cnt, 100)):
                    if off + 15 > len(body):
                        break
                    mid = body[off:off + 4]
                    state = body[off + 12]                  # 狀態 (sau mailid 4 + time 8)
                    tlen = int.from_bytes(body[off + 13:off + 15], "little")
                    npos = off + 15 + tlen                  # 附件數量
                    if npos >= len(body):
                        break
                    n_qua = body[npos]
                    if not any(m[0] == mid for m in self._mail_ids):
                        self._mail_ids.append((mid, int(state), int(n_qua)))
                    # record ke: sau [so dinh kem 1][kind 1][itemid 4] la padding -> quet OLE-time
                    p, nxt = npos + 6, None
                    while p + 12 <= len(body):
                        try:
                            o2 = struct.unpack("<d", body[p + 4:p + 12])[0]
                        except Exception:
                            o2 = 0
                        if 45000 < o2 < 48000:
                            nxt = p; break
                        p += 1
                    if nxt is None:
                        break
                    off = nxt
            # S:083-002 <領取信件> +數量(4) <<+信件ID(4)>> = server XAC NHAN da nhan qua mail nao.
            # Chi mail co trong day moi duoc xoa (client: state len EMailState.Take moi vao
            # OnClick_RemoveEmpty). Tui DAY -> server khong tra mail do -> giu lai, khong mat qua.
            elif pkt[7:9] == b"\x02\x00" and len(pkt) >= 13:
                _n = int.from_bytes(pkt[9:13], "little")
                for _i in range(min(_n, 200)):
                    _o = 13 + _i * 4
                    if _o + 4 > len(pkt):
                        break
                    self._mail_taken.add(pkt[_o:_o + 4])
        elif opcode == 0x7c:                      # event "qua 14 ngay" (panel claim item)
            self._on_activity_model(pkt)          # DOI THUONG su kien (S:124-000/001/002)
            # sub 01 = list phan qua: [01 00][count 4B LE] + count*[itemid 4B LE][qty 4B LE]
            if pkt[7:9] == b"\x01\x00" and len(pkt) >= 13:
                cnt = int.from_bytes(pkt[9:13], "little")
                items = []
                for i in range(cnt):
                    off = 13 + i * 8
                    if off + 8 > len(pkt):
                        break
                    items.append(pkt[off:off + 4])   # itemid 4B LE
                if items:
                    self._event14_items = items
            # sub 02 = grant qua (nhan THANH CONG): [02 00][01000000][itemid][qty]
            elif pkt[7:9] == b"\x02\x00" and len(pkt) >= 11:
                self._event14_acks.append(pkt[7:].hex())
                if pkt[9] == 0x01:
                    self._event14_ok += 1
            # sub 03 = KET QUA claim: [03 00][01000000][code]; code 00=OK, 06=TUI DAY
            elif pkt[7:9] == b"\x03\x00" and len(pkt) >= 14:
                code = pkt[13]
                self._event14_acks.append("ket_qua_code=%d" % code)
                if code == 0x06:
                    self._event14_bagfull = True
        elif opcode == protocol.OP_ACTIONS and not self.battle_tracker.generation:  # 0x35 legacy
            self._on_actions(pkt)
        elif opcode == 0x13 and len(pkt) >= 11 and pkt[7:9] in (b"\x04\x00", b"\x01\x00"):
            # pet dang dung: [04 00] luc login, [01 00] khi doi pet. id = 2B LE
            pid = int.from_bytes(pkt[9:11], "little")
            if pid == 0:
                self.state.active_pet_id = None
                self.state.active_pet_confirmed = True
                self.active_pet_slot = None
                self._active_pet_login = None
                self._pet_login_logged = None
                self.pet_name = None
                self.pet_level = None
                self.pet_agi = None
                return
            self.state.active_pet_id = pid
            self.state.active_pet_confirmed = True    # goi 0x13 = su that tu server
            if self.state.pet_cfg_owner is None:
                self.state.pet_cfg_owner = pid   # pet dau tien sau login (xem state.py)
            if self._cached_pet_list_pkt is not None:
                self._on_pet_list(self._cached_pet_list_pkt)
            # pet_usable_skills = 3 skill thuong + DAC KY neu con nay DA MO (va bot co du lieu
            # skill do). Truoc day chi lay 3 skill thuong -> dac ky lam nhiem vu moi co KHONG
            # BAO GIO duoc dung.
            self.state.pet_skills = self.pet_usable_skills(pid)   # LIST (boss skill[0])
            known = pid in getattr(config, "PET_NAMES", {}) or pid in getattr(config, "PET_SKILLS", {})
            name = getattr(config, "PET_NAMES", {}).get(pid, "?")
            # TEN pet DANG DUNG = ten cua active_pet_id tu pets.json (TIN CAY, dung nhu log login).
            # KHONG dua vao parse ten tu 0x0f (de tim nham con dau list). 0x0f chi dung lay LEVEL.
            if name and name != "?":
                self.pet_name = name
            if known:
                log.info("[%s] Pet id=0x%x '%s' -> skills=%s",
                         self._label, pid, name, [hex(s) for s in sorted(self.state.pet_skills)])
            else:
                log.warning("[%s] PET MOI chua co trong pets.json: id=0x%x (hex='0x%x') "
                            "-> them vao pets.json {skills, name, boss_skill}",
                            self._label, pid, pid)
        elif opcode == 0x2f:                      # party PHO BAN (dungeon)
            self._on_dungeon(pkt)
        elif opcode == 0x54:                      # exp offline / query luot dungeon / mua slot tui
            # Mua slot tui (UISell sellId=3) dung CHUNG opcode 0x54 -> phan biet bang payload[0:2]=0300.
            if pkt[7:9] in (b"\x01\x00", b"\x02\x00") and pkt[9:11] == b"\x03\x00":
                self._on_bag_slot(pkt)
            else:
                self._dg_query = pkt[7:]              # luu raw de query_dungeon_attempts doc
                self._dg_query_event.set()
                self._on_offline_exp(pkt)
        elif opcode == 0x55 and pkt[7:9] == b"\x01\x00" and len(pkt) >= 17:
            # BANG STAT: [01 00][count 4B] + count*([id 2B][val 4B][max 4B] = 10B).
            # Login gui FULL (~1500 stat); update le gui count=1. Doc digioi/dungeon/van tieu.
            self._apply_role_counts(pkt[7:])
        elif opcode == 0x56:                      # van tieu (escort) panel/status
            self._on_vantieu(pkt)
        elif opcode == 0x1f and pkt[7:9] == b"\x06\x00":  # list pet KHO (vận tiêu) luc login
            self._on_vantieu_roster(pkt)
        elif opcode == 0x1a and len(pkt) >= 13:   # currency: [id 2B][val 4B]
            sid = int.from_bytes(pkt[7:9], "little")
            val = int.from_bytes(pkt[9:13], "little")
            self.currency_values[sid] = val
            log.info("[%s] CURRENCY probe 0x1a sid=%d value=%d raw=%s",
                     self._label, sid, val, pkt[7:].hex())
            if sid == 4:                          # id=4 -> so VANG/DONG TIEN hien co
                self.money = val
                self.xu = val                     # tuong thich logic shop cu
            elif sid == 1:                        # id=1 -> so VANG vua nhan (vd ban Noi Dat)
                if self.money is not None:
                    self.money += val
                    self.xu = self.money
                    log.info("[%s] Nhan tien +%d -> tien hien co ~%d", self._label, val, self.money)
                else:
                    log.info("[%s] Nhan vang +%d (chua co balance hien tai de cong)", self._label, val)
            # sid==2 = so vang vua bi tru (cost), bo qua
        elif opcode == 0x23 and len(pkt) >= 13 and pkt[7:9] == b"\x05\x00":
            # Xac nhan capture live 15/09/2026: S2C 0x23 sub05 [balance u32] = 707,
            # trung chinh xac icon VANG user doc trong game.
            self.gold = int.from_bytes(pkt[9:13], "little")
            log.info("[%s] VANG hien co=%d (0x23 sub05)", self._label, self.gold)
        elif opcode == 0x57:                      # qua online
            self._on_gift(pkt)
        elif opcode == 0x28:                      # skill bar char/pet
            self._on_skill_bar(pkt)
        elif opcode == 0x1E:                      # TIEN TRANG (錢莊)
            # S:030-001 <錢莊1物品> = danh sach kho -> KHO DA MO. Day la moc DUY NHAT bao tien
            # trang san sang; gui lenh cat truoc moc nay la ban vao khoang khong.
            if pkt[7:9] == b"\x01\x00":
                self.bank_open = True
                self._doc_kho_tien_trang(pkt[9:])
            elif pkt[7:9] == b"\x04\x00":         # S:030-004: them/cap nhat MOT o
                self._doc_kho_tien_trang(pkt[9:])
            elif pkt[7:9] == b"\x05\x00" and len(pkt) >= 14:
                # S:030-005 <錢莊1減少物品> +索引(1) +減少數量(4)
                _i = pkt[9]
                _n = struct.unpack_from("<I", pkt, 10)[0]
                _cu = (self.bank_slots or {}).get(_i)
                if _cu:
                    _con = int(_cu[1]) - int(_n)
                    if _con > 0:
                        self.bank_slots[_i] = (_cu[0], _con)
                    else:
                        self.bank_slots.pop(_i, None)
                    self._ghi_cache_kho()
            # S:030-007 <錢莊操作失敗> +失敗結果(1): 3 = cat that bai, 13 = tien trang DAY.
            # Khong bat thi bot cu ban tiep ca chuc mon vao mot cai kho da day.
            if pkt[7:9] == b"\x07\x00" and len(pkt) >= 10:
                self._on_bank_fail(pkt[9])
        elif opcode == 0x27:                      # player info / guild / BOSS QUAN DOAN
            # BOSS QD: 0x27 76 [OLE 8B] = gio danh tiep duoc (cooldown) - CHI day khi DA danh.
            # (LUU Y: 0x27 70 [entity][01] la FLAG hang so, KHONG phai count. COUNT X/3 doc tu bang
            # stat 0x55 id 0x2a - xem handler 0x55 ben duoi.)
            if pkt[7:9] == b"\x76\x00" and len(pkt) >= 17:
                try:
                    self.legion_boss_next = self._ole_to_dt(struct.unpack("<d", pkt[9:17])[0]).timestamp()
                except Exception:
                    pass
            # S:039-015 <軍團訊息> +訊息種類(1). Ma 32 = "加入軍團未滿24小時，無法捐獻"
            # (TextData 21215: "Gia nhap quan doan chua du 24 tieng, khong the quyen gop").
            # Client KHONG tu biet da du 24h hay chua - khong co truong thoi gian gia nhap o dau
            # ca, chi co cach GUI ROI DOC MA LOI. Bat o day de HOAN tinh nang mo hop (donate la
            # buoc bat buoc cua no) thay vi thu lai lien tuc.
            if pkt[7:9] == b"\x0f\x00" and len(pkt) >= 10:
                self._on_legion_msg(pkt[9])
            self._on_player_info(pkt)
        elif opcode == 0x69:                      # chua self_entity
            if self.self_entity is None and len(pkt) >= 17:
                self.self_entity = pkt[9:17]
                self.state.self_entity = self.self_entity
                _register_party_entity(self.party_idx, self.self_entity)  # chia se cho cung party
                _register_party_client(self.party_idx, self.self_entity, self)
                if self.char_int is not None:   # INT da nhan truoc 0x69 -> dang ky lai khi co entity
                    _register_party_int(self.party_idx, self.self_entity, self.char_int)
                # ten nhan vat: neu 0x27 (guild list) da toi TRUOC 0x69 -> resolve tu goi da cache
                self._resolve_self_name(self._last_guild_pkt)
                # fallback (acc KHONG bang hoi): resolve ten tu 0x03 self-spawn da cache
                if self.char_name is None and self._pending_03 is not None:
                    self._resolve_name_from_03(self._pending_03)
                # xu lai cac goi 0x0b da buffer (co the chua stat cua minh den truoc 0x69)
                for p in self._pending_0b:
                    self.state.update_0x0b(p)
                    try:
                        self.state.note_enemy_entities(p, config.NPC_NAMES)
                    except Exception:
                        pass
                self._pending_0b = []
        elif opcode == 0x07 and pkt[7:9] == b"\x01\x00" and len(pkt) >= 16:
            # danh sach kenh (channel list): payload bat dau '01 00 [count]'
            # (phan biet voi 0x07 broadcast di chuyen bat dau '00 00 [entity]')
            self._on_channel_list(pkt)
        elif opcode == 0x07 and pkt[7:9] == b"\x02\x00" and len(pkt) >= 10:
            # ket qua doi kenh: payload = [02 00][result 1B], result=0 OK; 1..4 la loi.
            self._on_channel_switch_result(pkt)
        # DEBUG kenh: log 0x07 (tru broadcast di chuyen 00 00) de tim "kenh hien tai"
        if __import__("os").environ.get("CHANDBG") and opcode == 0x07 and pkt[7:9] != b"\x00\x00":
            log.info("[%s] CHANDBG 0x07 %s", self._label, pkt.hex())
        elif opcode == protocol.OP_PLAYER_STATE:  # 0x0d - party
            self._on_party(pkt)
        elif opcode == protocol.OP_BATTLE_START and not self.battle_tracker.generation:  # 0x34 legacy
            # KHONG reset quest_mode o day: quest_mode reset o KET TRAN (0x14 sub0700). 0x34 ban that
            # thuong (1 lan/nhieu tran) -> reset_quest=False de KHONG mat latch khi quai con <=5.
            self.state.in_battle = True
            self._no_item.clear()        # co the drop them item -> cho phep check hoi lai
            self.state.reset_enemies(reset_quest=False, reset_protect=False)   # xoa HP quai cu, GIU quest_mode/protect latch
            self.state.allies.clear()    # tran moi -> xoa HP dong doi tran cu (tranh ket hp=0 cua
            #                              con da chet tran truoc -> 0x33 tran moi nap lai HP tuoi)
            self.state.char_spam = False  # tran moi -> reset spam (set lai neu vao tran SP day)
            self.state.pet_spam = False
            self.last_turn_time = time.time()
            # KHONG reset _first_turn: atype=2 chi cho tran DAU TIEN ca phien, sau do=3
            # (moi tran chi 1 turn; client that dung 2 cho tran dau, 3 cac tran sau)
        # 0x41 (OP_BATTLE_ENTER) KHONG dung: fire ca luc login -> false positive
        # cac opcode khac: bo qua

    @staticmethod
    def _pet_marker_to_atype(marker: int):
        """DI GIOI SOLO: byte marker (=SLOT TUI, 1..4) -> atype dung trong tran (0,1,3,4 - atype 2
        la CHAR). XAC NHAN qua 2 lan capture thuc te: (a) 4 pet slot 1,2,3,4 -> atype 0,1,3,4;
        (b) CHI 3 pet, slot TRONG KHONG LIEN TUC (1,2,4 - thieu slot 3) -> atype VAN la 0,1,4 (bam
        THEO MARKER THAT, KHONG phai theo THU TU xuat hien trong danh sach - truoc day gia dinh SAI
        la luon du 4 con lien tiep, dung index list -> sai khi thieu con/khong lien tuc).
        Cong thuc: marker<=2 -> atype=marker-1; marker>=3 -> atype=marker (giu nguyen, ne atype=2)."""
        if marker < 1 or marker > 4:
            return None
        return marker - 1 if marker <= 2 else marker

    @staticmethod
    def _pet_atype_to_marker(atype: int):
        """Nguoc voi _pet_marker_to_atype: atype pet trong tran -> target use_slot (marker pet)."""
        if atype in (0, 1):
            return atype + 1
        if atype in (3, 4):
            return atype
        return None

    def _on_pet_list(self, pkt: bytes):
        """S2C 0x0f sub=0008: danh sach pet MANG THEO. Lay con DANG XUAT CHIEN (active_pet_id),
        KHONG phai con dau list. Record: [01 marker][pet_id 2B LE][...][LEVEL @+6][...][namelen @+30][ten @+31].
        -> tim vi tri pet_id active (ngay sau marker 0x01) roi doc level/ten tai offset co dinh.
        (khop capture: Thai Van Co id=0xa051 lv 45.) active_pet_id chua biet -> dung record dau.
        DI GIOI SOLO (toi da 4 pet ra tran cung luc, co the KHONG DU 4 con / khong lien tuc): TIEN
        THE, ghi luon skill tung pet vao state.multi_pet_skills[atype] theo MARKER THAT cua record
        (xem _pet_marker_to_atype - KHONG dung thu tu xuat hien trong danh sach, da xac nhan sai
        khi thieu con giua chung), khong lien quan gi toi logic "active pet" o duoi (giu nguyen)."""
        b = pkt[7:]
        if len(b) < 35 or b[2] < 1:
            return
        # Record DAI ~254+namelen byte: [marker=SO SLOT 1B][pet_id 2B LE][exp 4B][LEVEL @+7]...
        #   [namelen @+31][ten UTF16 @+32][tail 222B]. MARKER la slot TUI (1..4, CO THE THIEU giua
        # chung neu bo bot con - da xac nhan qua capture: 3 con marker=1,2,4 khong lien tuc).
        apid = getattr(self.state, "active_pet_id", None)
        n = b[2]
        start, chosen, first = 3, None, None
        _dbg = []   # DEBUG: (marker, pid) tung record de doi chieu voi vi tri THAT trong game
        self.state.carried_pets = []   # [(pid, ten)] pet MANG THEO - GUI tab skill per-pet doc
        self.pet_levels = {}           # pid -> cap; dung lai moi lan doc (pet mang theo co the doi)
        self._pet_skill_rows = []   # AUTO NANG SKILL PET: (slot,pid,petLv,skillPoint,[skillLv*3])
        for _ in range(n):
            if start + 33 > len(b):
                break
            if first is None:
                first = start
            marker = b[start]
            pid = int.from_bytes(b[start + 1:start + 3], "little")
            _dbg.append((marker, pid))
            if pid:
                # Cap doc THANG tu record (byte +7, giong `_pet_skill_rows`). Ghep vao TEN ngay tai
                # nguon: tui do, tab battle, cache luc acc tat... deu doc `carried_pets` nen chi can
                # mot cho la du - ten pet o day chi dung de HIEN, khong cho nao lay lam khoa (moi
                # cho deu so khop bang `pid`).
                _lv = b[start + 7]
                self.pet_levels[pid] = _lv
                _login_pet_exp = int.from_bytes(b[start + 3:start + 7], "little")
                self.pet_exp_values[pid] = _login_pet_exp
                self.pet_exp_values[marker] = _login_pet_exp  # server co ban update theo slot 1..4
                self.state.carried_pets.append(
                    (pid, ten_pet_kem_lv(getattr(config, "PET_NAMES", {}).get(pid, ""), _lv)))
            # Ban ghi login CUA TUNG PET (khong rieng con xuat chien). pet_login_stats.parse_record
            # von la ham TONG QUAT - nhan offset bat ky - nhung truoc day chi duoc goi cho con
            # active, nen bot khong biet HP/SP/AGI cua 3 con con lai. Luu het o day de UI (tui do)
            # hien duoc chi so moi con. Cap doc thang tu +7.
            try:
                _rec = pet_login_stats.parse_record(b, start)
                if _rec is not None and 1 <= marker <= 4:
                    _rec["level"] = b[start + 7]
                    _rec["pid"] = pid
                    self.pet_login_records[marker] = _rec
                # TRANG BI PET doc THANG tu day: sau ten + 3 byte skillLv la 6 x ThingData(35B),
                # moi ThingData mo dau bang Id (2B LE). O thu i (0-based) = fitType i+1.
                # Truoc day bot chi trong vao S:023-024 (sub 0x18) - goi do khong phai luc nao
                # cung ve luc login, nen bang do pet TRONG TRON (user bao 26/08 "chua thay hien
                # thi do pet dang mac"). Goi 0x0f thi LUC NAO CUNG co.
                _nl2 = b[start + 31]
                _eq = start + 35 + _nl2
                if 1 <= marker <= 4 and _eq + 6 * 35 <= len(b):
                    for _k in range(6):
                        _t = int.from_bytes(b[_eq + _k * 35:_eq + _k * 35 + 2], "little")
                        if _t:
                            self.pet_equip_by_fit.setdefault(marker, {})[_k + 1] = _t
            except Exception:
                pass
            # AUTO NANG SKILL PET: block 武將資料 (client Logic_Role.FollowNpcAppear) - tu dau record:
            #   [7]=petLv, [29:31]=DIEM SKILL (u16 LE), [31]=namelen, 3 byte NGAY SAU ten = skillLv*3.
            try:
                _nl = b[start + 31]
                _sp = int.from_bytes(b[start + 29:start + 31], "little")
                _lv = list(b[start + 32 + _nl:start + 32 + _nl + 3])
                if len(_lv) == 3:
                    self._pet_skill_rows.append((marker, pid, b[start + 7], _sp, _lv))
                # TRUNG THANH + DA MO DAC KY. Thu tu truong theo Logic/Role.lua FollowNpcAppear
                # (doi chieu them protocal.lua S:015-008):
                #   +26 dieCount | +27 Faith(忠誠) | +28 canGrow | +29 SkillPoint(2) | +31 namelen
                #   +32 ten | +32+nl skillLv * maxNpcSkill(=3)
                #   roi TRANG BI PET: maxEquip(=6) x ThingData(35B) = 210 BYTE
                #   roi sublimeCount(1) | specialSkillLearned(1) | soulId(4) | hpPill(1)
                #      | spPill(1) | upgradeLv(1)
                #
                # LOI CU (sua 2026-08-23, user bao "MacLienNhat co Chu Du da hoc dac ky ma bot
                # khong thay"): doc co o +36+nl, tuc BO QUA TRON 210 BYTE TRANG BI -> roi vao
                # giua khoi trang bi, doc RAC. Ly le tu tran an luc do cung sai: "cac moc +29/+31/
                # +32+nl dung nen bang offset tin duoc" - may moc do nam TRUOC khoi dai thay doi,
                # dung o do KHONG chung minh duoc gi cho truong nam SAU no.
                #
                # Nay neo tu CUOI ban ghi: sau co dac ky con dung 8 byte CO DINH (soulId 4 +
                # hpPill 1 + spPill 1 + upgradeLv 1 + chinh no 1) -> co = record_end - 8.
                # Do dai ban ghi 254+nl da duoc dung lau va DUNG (ten/pid/faith deu chuan).
                # Kiem cheo: 32+nl (sau ten) + 3 (skillLv) + 210 + 1 (sublime) = 246+nl -> khop.
                #
                # specialSkillLearned = "DA MO dac ky chua" (dac ky phai lam nhiem vu moi co).
                # Client chi cho dung dac ky khi CO CO NAY (RoleController.lua:4786):
                #   if self.data.specialSkillLearned and skillDatas[npcDatas[id].specialSkill] then
                self.pet_faith[pid] = b[start + 27]
                self.pet_special_skill[pid] = bool(b[start + 246 + _nl])
                if self.pet_special_skill[pid]:
                    # NHO LAI: cat vao nha tro roi thi khong con doc duoc co nay nua (xem
                    # `load_dac_ky_cache`) - ma luat "khong mua Me cho tuong da hoc dac ky" van can.
                    self._nho_dac_ky(pid)
                # GUI (tab skill per-pet) doc tu STATE chu khong co client -> cho state thay chung
                self.state.pet_faith = self.pet_faith
                self.state.pet_special_skill = self.pet_special_skill
            except Exception:
                pass
            at = self._pet_marker_to_atype(marker)
            if at is not None:
                sk = self.pet_usable_skills(pid)
                if sk:
                    self.state.multi_pet_skills[at] = sk
            # KHONG break som du tim thay active_pet_id: truoc day break ngay -> cac record SAU
            # (pet khac, can cho multi_pet_skills o tren) bi BO QUA het neu active_pet_id la con
            # DAU tien (xac nhan qua thuc te: chi pet atype dau co skills, cac con sau skills=[]).
            # Van phai QUET HET n record de multi_pet_skills du 4 con.
            if apid and pid == apid and chosen is None:
                chosen = start
                # SLOT TUI cua pet dang xuat chien = marker record (1..4, user tu xep, co the
                # khong lien tuc). use_slot(target=...) hoi pet PHAI dung slot nay - truoc day
                # hardcode target=1 -> pet nam slot khac la item bay vao slot sai, server bo qua
                # (bug thuc te: "hoi pet" ca 40 vien ma vao tran pet van 1HP).
                self.active_pet_slot = marker
            start = start + 254 + b[start + 31]
        if chosen is None:
            chosen = first   # active chua biet / khong tim thay -> con dau (fallback)
            if first is not None:
                self.active_pet_slot = b[first]
        # DEBUG hoi pet sai vi tri: in danh sach pet doc duoc + vi tri chon lam target hoi item.
        # Doi chieu voi vi tri THAT cua pet dang xuat chien trong game -> neu lech = parse sai.
        # TRUNG THANH + DAC KY doc tu chinh goi nay - IN RA de KIEM CHUNG OFFSET tren goi THAT.
        # (Test truoc day dung goi TU DUNG bang chinh offset do -> lap luan vong tron, khong chung
        # minh duoc gi. Faith la moc tot: phai la 0..100, ra so la = offset lech.)
        if self.pet_faith:
            log.info("[%s] PET-LIST: trung thanh/dac ky = %s", self._label,
                     {("0x%04x" % k): (v, bool(self.pet_special_skill.get(k)))
                      for k, v in self.pet_faith.items()})
        log.info("[%s] PET-LIST parse: apid=%s records=%s -> active_pet_slot=%s",
                 self._label, hex(apid) if apid else None,
                 [(m, hex(p)) for m, p in _dbg], self.active_pet_slot)
        if chosen is None or chosen + 33 > len(b):
            return
        chosen_pid = int.from_bytes(b[chosen + 1:chosen + 3], "little")
        if apid is None and chosen_pid:
            # 0x13 (active pet) co luc khong den truoc man config skill. 0x0f record dau
            # dang duoc dung lam fallback active pet san roi, nen ap luon skill/name de UI co du lieu.
            self.state.active_pet_id = chosen_pid
            # DOAN TAM (record dau list), CHUA chac la pet dang xuat chien -> khong danh dau
            # xac nhan; switch_pet se VAN gui lenh doi thay vi tin va bo qua.
            apid = chosen_pid
        found_active = apid is not None and chosen_pid == apid
        if found_active and not getattr(self.state, "pet_skills", None):
            self.state.pet_skills = self.pet_usable_skills(chosen_pid)
            name = getattr(config, "PET_NAMES", {}).get(chosen_pid, "")
            if name and name != "?":
                self.pet_name = name
            if self.state.pet_skills:
                log.info("[%s] Pet id=0x%x '%s' -> skills=%s (tu PET-LIST)",
                         self._label, chosen_pid, name or "?",
                         [hex(s) for s in sorted(self.state.pet_skills)])
        # Cache skill/pet cho dialog dung khi acc TAT (save_skill_cache tu bo qua neu khong doi).
        try:
            if self.state.carried_pets:
                save_skill_cache(getattr(self, "_username", None), skills_snapshot(self.state))
        except Exception as e:
            log.debug("[%s] cache skill loi: %s", self._label, e)
        lvl = b[chosen + 7]   # LEVEL cua con active (truoc day b[p+6], p=pet_id_off -> = chosen+7)
        if 1 <= lvl <= 200:
            self.pet_level = lvl
        parsed = pet_login_stats.parse_record(b, chosen)
        if parsed is not None:
            self._active_pet_login = parsed
            self._refresh_active_pet_login_stats()
        # TEN: chi cho pet KHONG co trong pets.json (0x13 da set ten tin cay). Chi khi tim DUNG record
        # active + chua co ten -> tranh 0x0f ghi de ten dung bang ten con dau.
        if found_active and self.pet_name is None:
            nl = b[chosen + 31]
            if 0 < nl <= 40 and chosen + 32 + nl <= len(b):
                try:
                    nm = b[chosen + 32:chosen + 32 + nl].decode("utf-16-le").strip("\x00")
                    if nm:
                        self.pet_name = nm
                except Exception:
                    pass

    def pet_name_out(self):
        """Ten pet CHI khi CHAC CHAN dang xuat chien. Chua chac -> None.

        _on_pet_list() co nhanh DOAN TAM: goi 0x13 (pet dang dung) doi luc toi SAU man config skill,
        nen neu chua co apid thi lay luon record DAU danh sach pet mang theo lam active de co skill
        ma vao tran. Nhanh do dat self.pet_name.

        Van de: "chua co 0x13" va "KHONG xuat chien con nao" nhin giong het nhau tu goi 0x0f. Acc
        khong tha pet van bi gan ten pet -> UI hien 'ten_lv_TenPet_lvPet' (user bao 25/08), va
        _party_levels/_average_party_levels dem CA level con pet khong ra tran vao level party ->
        chon nham bai train / cap quai Di Gioi.

        state.active_pet_confirmed CHI True khi co goi 0x13 THAT tu server (ke ca 0x13 voi pid=0 =
        khong tha pet - luc do pet_name da bi xoa san). Dung dung cai do de tra loi.
        KHONG bo nhanh doan tam: no van can cho skill/hoi pet khi 0x13 toi muon.
        """
        if not getattr(getattr(self, "state", None), "active_pet_confirmed", False):
            return None
        return self.pet_name

    def auto_upgrade_pet_skills(self):
        """AUTO NANG SKILL PET (login): moi pet co DIEM SKILL -> nang skill theo thu tu index 0->1->2,
        con index nao TOI MAX moi qua con sau (rule user). Gui C:028-002 (opcode 0x1C sub02):
        [pet_slot u8] + [skillId u16 LE][newLevel u8]*n. Gia: cap 1 = learnPt, cap sau = lvUpPt.
        skillId = config.PET_SKILLS[pid] (Npc_C, dung thu tu khop skillLv). Server tu tru diem."""
        rows = getattr(self, "_pet_skill_rows", None)
        if not rows:
            return 0
        sent = 0
        for slot, pid, petlv, sp, curlv in rows:
            if sp <= 0:
                continue
            skills = list(config.PET_SKILLS.get(pid, []))[:3]
            if not skills:
                continue
            budget = sp
            targets = []            # (skillId, newLevel)
            for i, sid in enumerate(skills):
                if i >= 3:
                    break
                info = config.SKILL_INFO.get(sid) or {}
                mx = int(info.get("maxLv", 0) or 0)
                need_lv = int(info.get("needLv", 0) or 0)
                learn = int(info.get("learnPt", 0) or 0)
                lvup = int(info.get("lvUpPt", 1) or 1)
                if mx <= 0 or petlv < need_lv:
                    continue
                lv = curlv[i] if i < len(curlv) else 0
                new = lv
                while new < mx:
                    cost = learn if new == 0 else lvup
                    if budget < cost:
                        break
                    budget -= cost
                    new += 1
                if new > lv:
                    targets.append((sid, new))
                    # RULE: index 0 phai TOI MAX moi sang index 1 -> con diem ma skill nay
                    # chua max (do het diem giua chung) thi DUNG luon, khong nhay skill sau.
                    if new < mx:
                        break
            if targets:
                body = b"\x02\x00" + bytes([slot])
                for sid, nl in targets:
                    body += sid.to_bytes(2, "little") + bytes([nl])
                self.send(0x1C, body)
                sent += 1
                log.info("[%s] AUTO NANG SKILL PET slot=%d pid=0x%04x diem=%d -> %s (con ~%d)",
                         self._label, slot, pid, sp,
                         [("0x%04x" % s, nl) for s, nl in targets], budget)
        return sent

    def befriend_nearby(self, max_friends=50, recent_secs=90):
        """KET BAN nguoi xung quanh (dung trong DG dong nguoi): gui C:014-005 (opcode 0x0e sub05)
        theo TEN moi player gan day tren MAP/KENH HIEN TAI. CHI khi so ban < max_friends (game toi
        da 50 ban). Ten gui UTF-16LE, byte-len prefix (client WriteStringWithByteL). Nguon player
        quanh minh = entity_meta[e]['nearby'] (tu 0x03 PlayerAppear), ten o entity_names[e].
        Dedup: bo qua nguoi da la ban + da gui trong phien nay."""
        cur = len(getattr(self, "friend_entities", []) or [])
        if cur >= max_friends:
            log.info("[%s] ket ban xung quanh: da du %d/%d ban -> bo qua", self._label, cur, max_friends)
            return 0
        slots = max_friends - cur
        now = time.time()
        gen = getattr(self, "_channel_scene_generation", None)
        sent_set = getattr(self, "_friend_req_sent", None)
        if sent_set is None:
            sent_set = self._friend_req_sent = set()
        friend_names = set()   # ten ban HIEN CO -> khong moi lai
        for ent in (self.friend_entities or []):
            for nm in self.entity_names.get(bytes(ent), ()):
                friend_names.add(nm.casefold())
        sent = 0
        for entity, meta in list(self.entity_meta.items()):
            if sent >= slots:
                break
            if not meta.get("nearby"):
                continue
            if gen is not None and meta.get("scene_generation") != gen:
                continue    # player o map/kenh KHAC hien tai -> bo qua
            if now - meta.get("seen", 0) > recent_secs:
                continue    # khong con o gan (cu) -> bo qua
            if self.self_entity and bytes(entity) == bytes(self.self_entity):
                continue    # chinh minh
            names = self.entity_names.get(entity)
            if not names:
                continue
            name = next(iter(names)).strip()
            if not name:
                continue
            key = name.casefold()
            if key in friend_names or key in sent_set:
                continue
            nb = name.encode("utf-16-le")
            if not (0 < len(nb) <= 255):
                continue
            self.send(0x0e, b"\x05\x00" + bytes([len(nb)]) + nb)   # C:014-005 <<[byteLen][name UTF16]>>
            sent_set.add(key)
            sent += 1
            time.sleep(0.3)     # gian cach tranh server rate-limit
        if sent:
            log.info("[%s] KET BAN xung quanh: gui %d loi moi (ban hien %d/%d)",
                     self._label, sent, cur, max_friends)
        return sent

    def _refresh_active_pet_login_stats(self):
        record = getattr(self, "_active_pet_login", None)
        if not record:
            return
        data = _load_pet_stat_data()
        if not data:
            return
        style = pet_login_stats.style_bonus(data, getattr(self, "_collect_style_flags", {}))
        cards = pet_login_stats.card_bonus(
            data,
            getattr(self, "_collect_card_equipped", []),
            getattr(self, "_collect_card_levels", {}),
        )
        hp_max, sp_max = pet_login_stats.calculate(record, data, style=style, cards=cards)
        self.pet_agi = pet_login_stats.calculate_agi(
            record,
            data,
            style_agi=pet_login_stats.style_attribute(
                data, getattr(self, "_collect_style_flags", {}), 30),
            card_agi=pet_login_stats.card_attribute(
                data,
                getattr(self, "_collect_card_equipped", []),
                getattr(self, "_collect_card_levels", {}),
                30,
            ),
        )
        p = self.state.pet
        p.hp = record["hp"]
        p.sp = record["sp"]
        p.hp_max = hp_max
        p.sp_max = sp_max
        # Ham nay chay lai MOI LAN co them du lieu cong them (0x0f pet list, 0x5f sub04 co thoi
        # trang, sub09 the trang bi, cap the) - viec TINH LAI la can, nhung LOG thi thua: luc login
        # ra 4-5 dong Y HET nhau. Chi log khi ket qua THAT SU doi.
        _sig = (record["marker"], record["id"], p.hp, p.hp_max, p.sp, p.sp_max, self.pet_agi)
        if _sig != getattr(self, "_pet_login_logged", None):
            self._pet_login_logged = _sig
            log.info("[%s] PET login active slot=%d id=0x%x HP=%d/%d SP=%d/%d AGI=%d",
                     self._label, record["marker"], record["id"], p.hp, p.hp_max, p.sp, p.sp_max,
                     self.pet_agi)

    def _parse_char_login_int(self, pkt: bytes):
        body = pkt[7:]
        if len(body) < 47 or body[:2] != b"\x03\x00":
            return
        # 0x05 sub0300 co san current/max HP/SP cua char luc login. Nap ngay de heal_full()
        # truoc boss co du stat, khong phai doi den 0x33/0x0b trong tran dau tien.
        # BO CUC DAY DU - crack Logic_Role.lua Role.ReceivePlayerData (KHONG doan offset):
        #   +2  element(1) | +3 hp(4) | +7 sp(2) | +9 Int(2) | +11 Atk(2) | +13 Def(2)
        #   +15 Agi(2) | +17 Hpx(2) | +19 Spx(2) | +21 Lv(1) | +22 Exp(4) | +26 SkillPoint(2)
        #   +28 AttrPoint(2) | +30 KillNum(4) | +34 BattleHonor(2) | +36 GovRequire(1)
        #   +37 Astrolabe(2) | +39 MaxHp(4) | +43 MaxSp(2)
        #   +45 EquipAtk(4) | +49 EquipDef(4) | +53 EquipInt(4) | +57 EquipAgi(4)
        #   +61 EquipMaxHp(4) | +65 EquipMaxSp(4) | +69 EquipHpx(4) | +73 EquipSpx(4)
        # (moc +9/+15/+53/+57 khop y het cai bot dang dung tu truoc -> bo cuc nay tin duoc.)
        hp = int.from_bytes(body[3:7], "little")
        sp = int.from_bytes(body[7:9], "little")
        hp_max = int.from_bytes(body[39:43], "little")
        # MaxSp la UInt16 (2 byte). Truoc day bot doc 4 byte -> nuot luon 2 byte dau cua
        # EquipAtk -> ra so khong lo -> tu roi vao nhanh kiem tra "sp_max < 100_000" va BO QUA
        # ca khoi HP/SP login. Chay duoc lau nay chi vi co nguon khac (0x08 sub0100 id 0x19/0x1a).
        sp_max = int.from_bytes(body[43:45], "little")
        if 0 <= hp <= hp_max < 1_000_000 and 0 <= sp <= sp_max < 100_000 and hp_max > 0 and sp_max > 0:
            c = self.state.char
            old = (c.hp, c.hp_max, c.sp, c.sp_max)
            c.hp, c.hp_max, c.sp, c.sp_max = hp, hp_max, sp, sp_max
            if old != (c.hp, c.hp_max, c.sp, c.sp_max):
                log.info("[%s] CHAR login HP=%d/%d SP=%d/%d",
                         self._label, c.hp, c.hp_max, c.sp, c.sp_max)
        # HE NHAN VAT + DIEM SKILL: doc TRUOC rao `len(body) < 98` ben duoi vi ca hai nam o offset
        # rat som (+2 va +26) - chan theo do dai cua phan TRANG BI o cuoi goi la mat oan.
        #   he   (`EAttribute.Element`: 1 Dia, 2 Thuy, 3 Hoa, 4 Phong, 5 Tam) o `+2`
        #   diem skill (`EAttribute.SkillPoint = 37`) o `+26`, ngay canh AttrPoint (+28)
        # Day la nguon DUY NHAT co ngay luc login: `S:008-013` chi toi khi mo bang skill trong
        # game, con `0x08 sub0100` thi server khong day kind 24. Thieu -> dialog Skill khong mo
        # dung tab he (user bao 2 lan: "mo skill van chua focus vao he cua minh"), hien
        # "Diem skill: ?", va gia hoc skill KHAC HE bi tinh THIEU MOT NUA.
        if len(body) > 2 and 1 <= body[2] <= 5:
            self.char_element = body[2]
        if len(body) >= 28:
            self.char_skill_point = int.from_bytes(body[26:28], "little")
            _old_char_exp = self.char_exp
            self.char_exp = int.from_bytes(body[22:26], "little")
            # Full character refresh cung co the cap nhat EXP sau tran. Khong dem snapshot
            # login dau tien, va chi dem chenh lech duong tu tong da biet cua chinh account.
            if (self.exp_stats_started_at is not None and _old_char_exp is not None
                    and self.char_exp > int(_old_char_exp)):
                _gain = self.char_exp - int(_old_char_exp)
                _row = {"type": "exp", "time": time.strftime("%H:%M:%S"),
                        "who": "character", "kind": 1, "exp": int(_gain),
                        "total": int(self.char_exp), "source": "0x05/03 character refresh"}
                self.combat_exp_log.appendleft(dict(_row))
                self.activity_log.appendleft(dict(_row))
                self._metrics_exp_gain("character", _gain)
                log.info("[%s] EXP NHAN VAT: +%d (full character refresh)", self._label, _gain)
            log.info("[%s] CHAR login EXP hien tai (0x05 +22)=%d",
                     self._label, self.char_exp)
        if len(body) < 98:
            return
        self._char_int_base = int.from_bytes(body[9:11], "little")
        self._char_equip_int = int.from_bytes(body[53:57], "little", signed=True)
        self._char_agi_base = int.from_bytes(body[15:17], "little")
        self._char_equip_agi = int.from_bytes(body[57:61], "little", signed=True)
        # DAY DU chi so char (user hoi 3 lan: "hien thi day du chi so, atk, int, def, Hpx...").
        # Truoc day bot chi lay INT/AGI nen bang chi so thieu han ATK/DEF/HPx/SPx - KHONG phai
        # vi server khong gui, ma vi khong ai doc. Khoa = EAttribute (Controller_RoleController).
        def _u16(o):
            return int.from_bytes(body[o:o + 2], "little")

        def _i32(o):
            return int.from_bytes(body[o:o + 4], "little", signed=True)
        self.char_base = {27: _u16(9), 28: _u16(11), 29: _u16(13), 30: _u16(15),
                          31: _u16(17), 32: _u16(19)}
        # DIEM TIEM NANG CHUA CONG (`EAttribute.AttrPoint = 38`), o `+28` trong bo cuc tren.
        # Server con day rieng qua `0x08 sub0100` khi doi -> `char_attrs[38]`; doc o day de biet
        # NGAY LUC LOGIN, khong phai cho lan doi diem dau tien.
        if len(body) >= 30:
            self.attr_point = _u16(28)
            if isinstance(getattr(self, "char_attrs", None), dict):
                self.char_attrs.setdefault(ATTR_POINT, self.attr_point)
        if len(body) >= 77:
            self.char_equip = {28: _i32(45), 29: _i32(49), 27: _i32(53), 30: _i32(57),
                               31: _i32(69), 32: _i32(73)}
        skill_count = int.from_bytes(body[96:98], "little")
        turn3_off = 98 + skill_count * 3
        if turn3_off + 11 <= len(body):
            self._char_turn3_int = int.from_bytes(body[turn3_off + 9:turn3_off + 11], "little")
        if turn3_off + 17 <= len(body):
            self._char_turn3_agi = int.from_bytes(body[turn3_off + 15:turn3_off + 17], "little")
        self._refresh_char_int()
        self._refresh_char_agi()

    def _refresh_char_int(self):
        base = getattr(self, "_char_int_base", None)
        if base is None:
            return
        data = _load_pet_stat_data()
        style_int = pet_login_stats.style_attribute(
            data, getattr(self, "_collect_style_flags", {}), 27)
        card_int = pet_login_stats.card_attribute(
            data,
            getattr(self, "_collect_card_equipped", []),
            getattr(self, "_collect_card_levels", {}),
            27,
        )
        horse_raw = getattr(self, "_mount_base_int", 0) + getattr(self, "_mount_equip_int", 0)
        horse_int = int(horse_raw * (1 + 0.01 * getattr(self, "_mount_collection_count", 0)))
        value = (base + getattr(self, "_char_equip_int", 0)
                 + getattr(self, "_char_turn3_int", 0)
                 + style_int + card_int
                 + horse_int)
        changed = value != self.char_int
        self.char_int = value
        _register_party_int(self.party_idx, self.self_entity, value)
        if changed:
            log.info("[%s] INT thuc te=%d (base=%d equip=%d turn3=%d style=%d card=%d horse=%d)",
                     self._label, value, base, self._char_equip_int, self._char_turn3_int,
                     style_int, card_int, horse_int)

    def _refresh_char_agi(self):
        base = getattr(self, "_char_agi_base", None)
        if base is None:
            return
        data = _load_pet_stat_data()
        style_agi = pet_login_stats.style_attribute(
            data, getattr(self, "_collect_style_flags", {}), 30)
        card_agi = pet_login_stats.card_attribute(
            data,
            getattr(self, "_collect_card_equipped", []),
            getattr(self, "_collect_card_levels", {}),
            30,
        )
        horse_raw = getattr(self, "_mount_equip_agi", 0)
        horse_agi = int(horse_raw * (1 + 0.01 * getattr(self, "_mount_collection_count", 0)))
        value = (base + getattr(self, "_char_equip_agi", 0)
                 + getattr(self, "_char_turn3_agi", 0)
                 + style_agi + card_agi + horse_agi)
        changed = value != self.char_agi
        self.char_agi = value
        if changed:
            log.info("[%s] AGI char thuc te=%d (base=%d equip=%d turn3=%d style=%d card=%d horse=%d)",
                     self._label, value, base, self._char_equip_agi, self._char_turn3_agi,
                     style_agi, card_agi, horse_agi)

    # S:013-013 <組隊訊息> +訊息種類(1) - server noi RO vi sao khong lap duoc party.
    PARTY_MSG = {
        1: "doi DO da day nguoi",
        2: "doi MINH da day nguoi",
        3: "nguoi do DANG TRONG SU KIEN",
        4: "khong duoc lap doi voi doi truong LEVEL CAO hon",
        5: "khong duoc cho nguoi LEVEL THAP hon vao doi",
        6: "doi da giai tan",
        7: "MINH dang trong nhom PHO BAN",
        8: "NGUOI KIA dang trong nhom PHO BAN",
    }
    # S:013-010 <邀請組隊結果> +結果(1) +L(1) +名字(L)
    PARTY_INVITE_RESULT = {1: "DONG Y", 2: "TU CHOI", 3: "KHONG PHAN HOI"}

    def _on_party(self, pkt: bytes):
        """S2C 0x0d. sub=09 = loi moi -> accept. sub=06 = roster [leader][count][members]."""
        if len(pkt) < 9:
            return
        sub = pkt[7]
        # KET QUA LOI MOI + LY DO THAT BAI: truoc day bot DIEC hai goi nay, nen khi party khong
        # lap duoc thi khong ai biet vi sao - phai doc nguoc ca file log de doan (party 15, 27/08).
        # Client co xu ly ca hai (protocolTable[13][10] va [13][13]), chi de hien thong bao.
        if sub == 0x0a and len(pkt) >= 11:
            _kq = pkt[9]
            _nl = pkt[10]
            try:
                _ten = pkt[11:11 + _nl].decode("utf-16-le", "replace")
            except Exception:
                _ten = "?"
            log.info("[%s] PARTY: loi moi -> %s (%s)", self._label,
                     self.PARTY_INVITE_RESULT.get(_kq, "ma %d" % _kq), _ten)
            return
        if sub == 0x0d and len(pkt) >= 10:
            _ma = pkt[9]
            # MA 0 KHONG PHAI tu choi: client chi hien lai chuoi da luu (`if result == 0 then
            # ShowCenterMessage(Team.inviteStr)`), va no ve sau MOI loi moi THANH CONG. Bao
            # "server tu choi" o day la sai va lam ban log dung luc dang doc (loi cua ban va
            # 27/08 - da thay trong log party 1: 4 dong "tu choi ma 0" xen giua 4 lan DONG Y).
            if _ma:
                log.warning("[%s] PARTY: server tu choi - %s (ma %d)", self._label,
                            self.PARTY_MSG.get(_ma, "khong ro"), _ma)
            return
        # S:013-004 <玩家離開隊伍> +玩家ID(8) +坐標X(2) +坐標Y(2): server bao ai do ROI DOI, VA
        # KEM TOA DO THAT cua nguoi do.
        #
        # DAY LA TOA DO PHAI LAY. Trong party, member TU DI THEO leader -> vi tri THAT cua no la
        # cho leader dung, con `self.pos` cua bot van la so dead-reckoning tu lan cuoi TU no ra
        # lenh di. Party tan -> member het theo -> lenh move dau tien tinh tu pos cu = server thay
        # nhay ca nghin don vi -> `di chuyen QUA XA (ma 14)` -> DUT KET NOI.
        # Log 30/08 21:45:44-46: leader giai tan party o (820,1000); goi nay mang dung (820,1000);
        # bot van tuong 4 member o (2150,1810) -> ca 4 gui move -> ca 4 bi da ra CUNG MOT GIAY.
        if sub == 0x04 and len(pkt) >= 21:
            _ai = pkt[9:17]
            # XOA NGUOI DO KHOI ROSTER - y het client (`protocal.lua:1694`):
            #     protocolTable[13][4] = function(data)
            #       local roleId = data:ReadInt64(); Team.RemoveMember(roleId); ...
            # Bot truoc day chi lay TOA DO trong goi nay, con roster thi TU XOA ngay luc GUI
            # `C:013-004` (khong cho server xac nhan). Do la cho de ra "party ma": lenh roi doi
            # bi tu choi / roi mat thi server VAN giu doi, ma bot da xoa so cua minh -> no ket
            # luan "KHONG o party nao" va tu choi giai tan, trong khi cac acc khac van dang o doi
            # do. Party ma la thu BOT TU BIA (user 08/09: "cai party ma nay la bot tu bia ra day
            # chu, game client thi member thoat het hoac leader giai tan la pt giai tan roi").
            if _ai and self.self_entity:
                if _ai == self.self_entity:
                    self.party_members = []          # CHINH MINH roi doi -> roster rong
                    self.party_leader = None
                else:
                    _ds = [e for e in (self.party_members or []) if bytes(e) != bytes(_ai)]
                    if len(_ds) != len(self.party_members or []):
                        self.party_members = _ds
                        log.info("[%s] PARTY: %s ROI doi (S:013-004) -> roster con %d nguoi",
                                 self._label, _ai.hex()[:8], len(_ds))
                    if self.party_leader and bytes(self.party_leader) == bytes(_ai):
                        # DOI TRUONG roi = doi giai tan (dung nhu game).
                        self.party_members = []
                        self.party_leader = None
                        log.info("[%s] PARTY: DOI TRUONG %s roi -> doi giai tan",
                                 self._label, _ai.hex()[:8])
            if self.self_entity and _ai == self.self_entity:
                _x = int.from_bytes(pkt[17:19], "little")
                _y = int.from_bytes(pkt[19:21], "little")
                if 0 < _x < 20000 and 0 < _y < 20000 and self.pos != (_x, _y):
                    log.info("[%s] RESYNC pos tu 0x0d sub04 (roi doi) = (%d,%d) [truoc: %s]",
                             self._label, _x, _y, self.pos)
                    self.pos = (_x, _y)
                    self._position_generation += 1
                    self._pos_valid_for_map = self.current_map
            return
        # S:013-005 <玩家加入隊伍> +隊長ID(8) +玩家ID(8): server bao TUNG NGUOI vao doi.
        # Client dung goi nay de AddMember (protocolTable[13][5]); bot truoc day chi trong vao
        # roster day du sub06 nen bo lo cac lan server chi ban goi le nay.
        if sub == 0x05 and len(pkt) >= 25:
            _lead, _ai = pkt[9:17], pkt[17:25]
            if self.self_entity and (self.self_entity == _lead or self.self_entity == _ai
                                     or _ai in (self.party_members or [])
                                     or _lead == self.party_leader):
                self.party_leader = _lead
                if _ai != _lead and _ai not in (self.party_members or []):
                    self.party_members = list(self.party_members or []) + [_ai]
                log.info("[%s] PARTY: %s vao doi (leader=%s) -> roster %d nguoi", self._label,
                         _ai.hex()[:8], _lead.hex()[:8], len(self.party_members or []))
                _sync_party_joined(self.party_idx, _lead, self.party_members,
                                   nguoi_ghi=self.self_entity, label=self._label)
                if _ai == self.self_entity and _ai != _lead:
                    # CHINH MINH vua vao doi -> client teleport minh toi cho leader NGAY LUC NAY
                    # (`Team.lua:176` trong `AddMember`: `Role.players[roleId]:Teleport(
                    # leader.position)`). Do la lan DUY NHAT client bam pos theo leader.
                    self._cho_bam_leader = True
            return
        if sub == 0x09 and self.auto_accept_party and len(pkt) >= 17:
            entity = pkt[9:17]   # entity nguoi MOI (leader), KHONG set lam self_entity
            if not self.party_invite_ready:
                # DANG LAM VIEC VAT -> GIU LOI MOI, KHONG nhan. Nhan bay gio la bo do viec vat
                # (user 14/09: "dang lam do ma vao pt roi bi keo di luon thi no hong viec vat").
                #
                # Cho NHA loi moi da giu: `account_task.__exit__` - chay khi viec vat ket thuc, KE
                # CA khi no nem loi hay bi abort. Truoc day viec nha do phu thuoc `party_invite_ready`
                # gan tay o vai nhanh cu the; acc bi ABORT giua chung khong bao gio toi dong gan co
                # -> co ket False VINH VIEN -> giu moi loi moi -> party khong bao gio du (ca that
                # 08/09 party 17: chia doi 20 phut; 07/09 party 10: im 14 phut).
                #
                # Cach chua CU la de ra ngoai le "loi moi tu acc cung party -> ACCEPT NGAY du dang
                # viec vat". Nhung trong party bot thi loi moi LUON tu acc cung party, nen ngoai le
                # do luon trung - tuc hang rao nay coi nhu khong ton tai, va moi acc deu bi keo vao
                # giua luc lam viec vat (dong "GIU loi moi" xuat hien DUNG 0 LAN trong ca file log
                # 14/09). Da bo ngoai le; thay bang cho nha chac chan o tren.
                self._pending_party_invites[bytes(entity)] = time.time()
                log.info("[%s] Chua san sang vao party -> GIU loi moi entity=%s, se accept sau viec vat",
                         self._label, entity.hex()[:12])
                return
            self._accept_party_invite(entity)
        elif sub not in (0x06, 0x09):
            # DIEM MU: bot chi xu 5 sub cua 0x0d, con lai BO IM LANG. Ho 013 con
            # 001/002/003 (yeu cau to doi + ket qua), 004 (nguoi roi doi), 014 (doi doi truong)...
            # Dang truy vu "member khong bao gio nhan duoc loi moi" (party 15/16, 27/08) ma khong
            # biet server co gui gi khac khong -> log lai het, chan spam 15s/sub.
            _k = ("0d_sub_la", sub)
            if time.time() - float(getattr(self, "_0d_la_at", {}).get(_k, 0.0)) >= 15:
                if not hasattr(self, "_0d_la_at"):
                    self._0d_la_at = {}
                self._0d_la_at[_k] = time.time()
                log.info("[%s] PARTY: goi 0x0d sub=%d CHUA XU LY: %s",
                         self._label, sub, pkt[7:39].hex())
            return
        if sub == 0x06 and len(pkt) >= 18:
            # roster: [sub 06][00] << [leader 8B][count 1B][member 8B]*count >>
            #
            # MOT GOI CHUA NHIEU PARTY. Client duyet `while data.length > 0` (protocolTable[13][6]:
            # doc leaderId + count roi AddMember, LAP LAI cho toi het goi) - vi `0x0d sub06` phat
            # theo MAP, cac party khac cung map di chung mot goi.
            # Truoc day bot chi doc NHOM DAU roi `return` neu minh khong o trong do -> party cua
            # minh nam nhom thu 2 tro di la MAT TRANG CA GOI -> leader khong bao gio thay roster.
            # Party 15 (27/08): roster cuoi cung 08:48:44, sau do 35 phut leader dem `roster
            # server=0` trong khi member van accept deu -> ket vinh vien.
            leader, members = None, []
            _off = 9
            _doi_cua = {}    # entity -> ID DOI TRUONG (nhu Team.members ben client Lua)
            while _off + 9 <= len(pkt):
                _lead = pkt[_off:_off + 8]
                _count = pkt[_off + 8]
                _off += 9
                _mems = []
                for _i in range(_count):
                    if _off + 8 > len(pkt):
                        break
                    _mems.append(pkt[_off:_off + 8])
                    _off += 8
                if not _mems:
                    continue
                # GHI LAI CA CAC NHOM KHAC. Client lam dung the (protocolTable[13][6] -> AddMember
                # cho MOI nhom, `Team.members[roleId] = leaderId`) va no la nguon cho hai viec:
                #   - Team.Leave gui `members[playerId]` = doi truong cua ĐÚNG party minh dang ket.
                #   - Team.Invite chi gui C:013-007 khi `IsAlone(roleId)`; nguoi da o party thi
                #     client gui C:013-001 <要求組隊> chu KHONG gui 007.
                # Truoc day bot `break` ngay khi thay nhom cua minh -> vut het thong tin nay ->
                # member ket trong party LA thi gui 013-004 kem ID doi truong SAI (lay doi truong
                # party minh muon vao) -> server bo qua -> ket vinh vien, con leader thi cu ban
                # 013-007 vao nguoi dang o party khac -> server im lang, khong ma loi nao.
                # (party 2, 30/08: nasau f4d0d7f8 cung map 21851 + cung kenh 3 co ack 0x07 ma
                # 6 phut khong vao noi party, leader khong nhan duoc mot ma tu choi nao.)
                _doi_cua[bytes(_lead)] = bytes(_lead)
                for _m in _mems:
                    _doi_cua[bytes(_m)] = bytes(_lead)
                if self.self_entity and (self.self_entity == _lead or self.self_entity in _mems):
                    leader, members = _lead, _mems
            if _doi_cua:
                self.team_of = _doi_cua
                self.team_of_at = time.time()
            count = len(members)          # so nguoi trong nhom CUA MINH (dung cho log ben duoi)
            if members:
                # CHI nhan roster CUA PARTY MINH (self la leader HOAC trong members).
                # 0x0d sub06 phat TOAN MAP -> party khac cung map cung gui roster cua ho;
                # neu khong loc se GHI DE party_members + atype bang roster party LA.
                if self.self_entity != leader and self.self_entity not in members:
                    return
                self.party_leader = leader
                self.party_members = members
                # ROSTER SERVER LA SU THAT (giong client: S:013-006 -> Team.AddMember). Dong bo
                # _PARTY_JOINED theo roster thay vi cho member tu ghi so luc accept loi moi:
                # party co san tu truoc / reform xoa so -> so rong ma party trong game van con
                # -> leader dem "0/4" roi MOI LAI vo han (log 18:13 party 6).
                _sync_party_joined(self.party_idx, leader, members,
                                   nguoi_ghi=self.self_entity, label=self._label)
                # CHI log khi roster THAY DOI (0x0d sub06 phat lien tuc -> truoc day spam moi goi).
                _roster_sig = (leader, tuple(members))
                _roster_changed = _roster_sig != getattr(self, "_last_roster_sig", None)
                self._last_roster_sig = _roster_sig
                # slot cua minh = vi tri trong danh sach member (1-based) -> map B2 trong 0x33
                if self.self_entity in members:
                    idx = members.index(self.self_entity)
                    # atype = VI TRI BATTLE (0-4, leader LUON o giua=2). Member dien [1,3,0,4] theo thu tu.
                    FILL = [1, 3, 0, 4]
                    self.state.my_atype = FILL[idx] if idx < len(FILL) else idx
                    # slot stats trong 0x33 = VI TRI BATTLE (= atype), KHONG phai idx+1
                    self.state.self_slot = self.state.my_atype
                    if _roster_changed:
                        log.info("[%s] Party roster: %d member, minh slot=atype=%d",
                                 self._label, count, self.state.my_atype)
                else:
                    # minh LA LEADER -> luon o giua (atype=2)
                    self.state.my_atype = 2
                    self.state.self_slot = 2
                    if _roster_changed:
                        log.info("[%s] Party roster: %d member, minh LA LEADER (atype=2)",
                                 self._label, count)

    def _accept_party_invite(self, entity: bytes) -> bool:
        entity = bytes(entity)
        if _is_party_member(self.party_idx, entity):
            self.send(protocol.OP_PLAYER_STATE, b"\x08\x00\x01" + entity)
            mark_joined(self.party_idx, self.self_entity)
            log.info("[%s] Loi moi tu THANH VIEN CUNG PARTY -> ACCEPT (da join)", self._label)
            return True
        leaders = (config.leaders_for(self.party_idx)
                   if hasattr(config, "leaders_for") else getattr(config, "PARTY_LEADERS", []))
        if leaders:
            wanted = {str(name).strip().casefold() for name in leaders if str(name).strip()}
            known = self.entity_names.get(entity, set())
            if known and not any(str(name).strip().casefold() in wanted for name in known):
                log.info("[%s] TU CHOI loi moi tu entity=%s strings=%s (khong trong PARTY_LEADERS=%s)",
                         self._label, entity.hex()[:12], known, leaders)
                return False
            if not known:
                log.info("[%s] Chua biet ten entity=%s -> CHAP NHAN (chua co 0x27)",
                         self._label, entity.hex()[:12])
        self.send(protocol.OP_PLAYER_STATE, b"\x08\x00\x01" + entity)
        # NGUOI NGOAI (user choi tay/whitelist) moi bot di danh cung -> TAT flee_mode NGAY luc accept.
        # Bug that: flee_mode=True thuong truc (mode stand/di chuyen), guard chi dua vao party_members
        # (roster 0x0d sub06) - user keo vao tran TRUOC khi roster ve -> guard vo hieu -> bot BO CHAY
        # -> server kick khoi party -> user moi lai -> lap vo tan "cu battle la flee".
        self.flee_mode = False
        log.info("[%s] Nhan loi moi party -> da gui ACCEPT (tat flee_mode, danh cung nguoi moi)", self._label)
        return True

    def dang_lam_viec_vat(self) -> bool:
        """Acc co dang O GIUA mot viec vat khong - doc BAO CAO PHA, khong doc co gan tay.

        `@task_report(..., PHASE_LOGIN_CHORE)` boc quanh TUNG viec vat (nhan mail, diem danh, van
        tieu, boss the gioi, doi qua...), nen pha nay bat/tat theo dung viec that su dang chay va
        KHONG THE ket: `account_task.__exit__` chay ca khi viec nem loi.

        Khac han `party_invite_ready` - co do gan tay o vai nhanh cu the, acc bi ABORT giua chung
        thi khong bao gio toi dong gan -> ket False VINH VIEN du dang RANH (ca that 08/09 party 17:
        hai acc lap "GIU loi moi ... se accept sau viec vat" moi 6 giay suot 40 phut trong khi
        khong he lam viec vat nao).
        """
        try:
            d = get_account_task(getattr(self, "_username", "") or "")
        except Exception:
            return False
        return bool(d) and d.get("phase") == PHASE_LOGIN_CHORE

    def set_party_invite_ready(self, ready: bool = True):
        """Mo gate party thuong va xu ly lai loi moi da den trong luc login chores."""
        self.party_invite_ready = bool(ready)
        if not self.party_invite_ready or not self._pending_party_invites:
            return False
        pending = list(self._pending_party_invites)
        self._pending_party_invites.clear()
        pending.sort(key=lambda entity: 0 if _is_party_member(self.party_idx, entity) else 1)
        for entity in pending:
            if self._accept_party_invite(entity):
                log.info("[%s] Da san sang -> xu ly loi moi party da giu tu truoc", self._label)
                return True
        return False

    def _on_dungeon(self, pkt: bytes):
        """S2C 0x2f - party PHO BAN.
        sub=0x0f: loi moi [0f 00][id 4B][01 00][leader entity 8B][namelen][ten UTF-16LE]
          -> ten leader trong PARTY_LEADERS thi DONG Y: C2S 0x2f [03 00][id 4B][00]
          -> sau do tu an CHUAN BI: C2S 0x2f [0b 00]
        """
        if len(pkt) < 9:
            return
        body = pkt[7:]
        sub = int.from_bytes(body[0:2], "little")
        # PHO BAN KET THUC: `S:047-012 <副本結束> +結果(1) +副本編號(2) +數量(1) <<+獎勵ID(2) +數量(4)>>`
        #
        # Server KHONG day ai ra ca - no chi GUI KET QUA cho tung client. Client mo bang ket qua va
        # chinh cai NUT DONG bang do moi la lenh roi (`Logic/Dungeon.lua:783`):
        #     UI.Open(UIResult, ..., reward, Dungeon.LeaveSinglePlayDungeon, result == 0);
        #     function Dungeon.LeaveSinglePlayDungeon()      -- Dungeon.lua:243
        #       sendBuffer:WriteInt64(Role.playerId); Network.Send(13, 4, sendBuffer);
        #     end
        # Tuc MOI acc phai TU roi sau khi nhan goi nay.
        #
        # Bot truoc day chi doc `sub == 0x0f` (loi moi) nen member KHONG HE BIET pho ban da xong ->
        # nam lai trong map instance. Ca that 07/09 p42: leader bao XONG luc 10:35:48, den 10:40:39
        # member van `go_to_town: DANG TRONG pho ban to doi (map=62012) -> khong teleport`.
        if sub == 0x0c and len(body) >= 5:
            _kq = body[2]
            _did = int.from_bytes(body[3:5], "little")
            self.dungeon_complete = True
            self._pb_ket_thuc_luc = time.time()
            log.info("[%s] PHO BAN KET THUC (S:047-012 ket qua=%d, pho ban 0x%04x) -> tu roi nhu "
                     "client (C:013-004)", self._label, _kq, _did)
            try:
                self.leave_single_dungeon()
            except Exception as e:
                log.warning("[%s] roi pho ban sau khi ket thuc loi: %s", self._label, e)
            return
        if sub == 0x0f and self.auto_accept_party and len(body) >= 17:
            invite_id = body[2:6]
            nl = body[16]
            name = ""
            try:
                name = body[17:17 + nl].decode("utf-16-le")
            except Exception:
                pass
            leaders = (config.leaders_for(self.party_idx)
                       if hasattr(config, "leaders_for") else getattr(config, "PARTY_LEADERS", []))
            if leaders and name:
                _ldlc = {l.strip().lower() for l in leaders}   # KHONG phan biet hoa/thuong
                if name.strip().lower() not in _ldlc:
                    log.info("[%s] TU CHOI moi pho ban tu '%s' (khong trong whitelist)",
                             self._label, name)
                    return
            # Dong y vao pho ban
            self.send(0x2f, b"\x03\x00" + invite_id + b"\x00")
            # BUG THAT (xac nhan qua log thuc te): _do_team_dungeon_lv20_inner (chi LEADER goi) co
            # ep self.state.quest_mode = True luc tao pho ban, nhung nhanh nay (MEMBER tu accept loi
            # moi pho ban CUA NGUOI THAT, khong co bot-leader dieu phoi) KHONG ep quest_mode=True -> quest_mode bi khoa cung o False
            # suot ca pho ban (member khong tu bao gio dung duoc skill toan man du >6 quai va con SP).
            # Ep giong het LEADER de nhat quan cho MOI truong hop nhan pho ban.
            self.state.quest_mode = True
            log.info("[%s] Nhan moi PHO BAN tu '%s' -> da DONG Y", self._label, name or "?")
            # Da nhan pho ban -> THEO + DANH: pha PB do DIEU PHOI bat/tat cho ca party
            # (`dat_pha_pho_ban`), acc khong tu om timer 600s nua - xem `_PARTY_PB_PHA`.
            self.flee_mode = False
            # Tu an CHUAN BI sau 2.5s (cho load scene pho ban)
            threading.Timer(2.5, self._dungeon_ready).start()

    def _dungeon_ready(self):
        if not self.running:
            return
        self.send(0x2f, b"\x0b\x00")
        _mark_dungeon_ready(self.party_idx, self.self_entity)   # bao LEADER: minh da CHUAN BI that
        log.info("[%s] Pho ban: da an CHUAN BI", self._label)

    # ---- xu ly available actions / status-list (0x35) ----
    def _observe_team_dungeon_packet(self, opcode: int, pkt: bytes):
        if getattr(self, "_active_team_dungeon_level", None) != 110:
            return
        if opcode == protocol.OP_BATTLE_START:
            self._battle_start_seq += 1
            return
        tracker = getattr(self, "battle_tracker", None)
        if (opcode == 0x14 and pkt[7:9] == b"\x07\x00"
                and not getattr(tracker, "generation", 0)):
            self._team_dungeon_end_seq += 1
            return
        if opcode != 0x35:
            return
        replacement = team_dungeon_lv110.decode_reinforcement(pkt)
        if replacement is None:
            return
        old_entity, new_entity = replacement
        self._team_dungeon_reinforcement_seq += 1
        self.state.in_battle = True
        old_id = int.from_bytes(old_entity[:2], "little")
        new_id = int.from_bytes(new_entity[:2], "little")
        log.info(
            "[%s] PB110 thay quan giua tran: %s -> %s (dot %d)",
            self._label,
            config.NPC_NAMES.get(old_id, hex(old_id)),
            config.NPC_NAMES.get(new_id, hex(new_id)),
            self._team_dungeon_reinforcement_seq,
        )

    def _on_actions(self, pkt: bytes):
        """0x35/01 = client ``FightManager.RevRestoreStatus`` records.

        Every entry is ``[row][col][status_kind][skill_id u16]``.  The game
        client applies ``skill_id=0`` too (clear that status kind).  Bot also
        uses those zero-status rows for char/pet as the turn-action signal.
        """
        # TRU: dang trong grace sau KET TRAN THAT (0x14 sub0800 tail=04) VA van dung the he tran do
        # -> 0x35 nay la broadcast DU cua member khac chua xong luot, KHONG duoc set lai in_battle
        # (bug: truoc day set lai lam leader ket "tran da ket" nhung van cho toi 25s SAFETY moi ha).
        # Kiem theo THE HE (khong chi theo thoi gian): tran MOI start trong 3s do van phai danh duoc.
        if self._in_battle_end_grace():
            return
        body = pkt[7:]
        offers = []
        if len(body) >= 2 and body[:2] == b"\x01\x00":
            i = 2
            while i + 5 <= len(body):
                unit, atype, target = body[i], body[i + 1], body[i + 2]
                skill_id = body[i + 3] | (body[i + 4] << 8)
                if skill_id == 0 and unit in (config.UNIT_CHAR, config.UNIT_PET):
                    offers.append((unit, atype, target))
                i += 5
        # Apply ALL records first, including skill_id=0.  This mirrors the game
        # client's HandleStatus loop and keeps CC/protection current for targeting.
        self.state.update_0x35_status(pkt)
        if not offers:
            return  # 11-byte confirmation hoac status-list thuan -> khong phai luot ra lenh
        # 0x35 available-actions = toi luot minh -> dang trong tran
        self.state.in_battle = True
        self.last_turn_time = time.time()
        for unit, atype, target in offers:
            self.available.setdefault(unit, [])
            if (atype, target) not in self.available[unit]:
                self.available[unit].append((atype, target))
        # KHONG lay atype tu 0x35 (no liet ke ca 5 vi tri party -> khong on dinh).
        # self_slot xac dinh qua roster (FILL) hoac khop char maxHP trong update_0x33.
        # debounce: quyet dinh 0.4s sau goi 0x35 cuoi cung
        if self.auto_combat:
            self._arm_decision()

    def _battle_coordinator(self):
        party_key = self.party_idx if self.party_idx is not None else ("solo", id(self))
        if party_key != self._battle_party_key:
            self._battle_party_key = party_key
            self._battle_party_coordinator = get_party_battle(party_key)
            self.state.attach_tracker(self.battle_tracker, self._battle_party_coordinator)
        return self._battle_party_coordinator

    def _battle_account_id(self):
        return self.user_id

    # Mot luot danh binh thuong xong trong 1-3 giay (do tren party 2, 11/09: g=6 va g=7 deu ~1s).
    # Qua nguong nay la co gi do KHONG BINH THUONG -> phai ghi ra ngay ai da gui lenh, ai chua.
    LUOT_CHAM_SEC = 25.0

    def _soi_luot_cham(self):
        """Luot keo qua lau -> GHI RA ai da gui lenh danh, ai chua. Mot dong cho moi luot.

        Bot phai TU BIET va TU GHI ly do khi no dung im - khong de nguoi doc phai di doan.

        Ca that hai lan cung mot cau hoi cua user:
          * 01/09 party 22: tran keo 12 phut, chi thay leader gui lenh ("m xem bot co gui lenh
            danh ko") - khong tra loi duoc.
          * 11/09 party 2: luot 99 giay (01:48:36 gui lenh -> 01:50:15 server moi tra don danh, va
            tra ve `skill=10000` danh thuong thay cho `12003` da chon = dau hieu server het gio cho
            lenh roi tu danh) - van khong tra loi duoc.
        Ca hai lan deu mu vi `_log_battle_verbose` an log SEND/ACK cua member. Dong nay khong phu
        thuoc vao do: no doc thang moc `_luot_da_gui` cua tung client cung tien trinh (L2).
        """
        try:
            _bt = getattr(self, "battle_tracker", None)
            gen, turn = int(getattr(_bt, "generation", 0) or 0), int(getattr(_bt, "turn", 0) or 0)
            if not gen:
                return
            _troi = time.time() - float(self.last_turn_time or 0)
            if _troi < self.LUOT_CHAM_SEC:
                return
            if getattr(self, "_luot_cham_da_bao", None) == (gen, turn):
                return                       # moi luot chi bao MOT lan, khong spam
            self._luot_cham_da_bao = (gen, turn)
            _roi, _chua = [], []
            for p in self.party_peers():
                _m = getattr(p, "_luot_da_gui", None)
                if _m and _m[0] == gen and _m[1] == turn:
                    _roi.append("%s(%.0fs truoc)" % (p._label, time.time() - _m[2]))
                else:
                    _chua.append("%s[luot cuoi gui=%s]" % (
                        p._label, "%s/t%s" % (_m[0], _m[1]) if _m else "chua bao gio"))
            log.warning("[%s] LUOT CHAM: g=%d t=%d da %.0fs chua xong | DA gui lenh: %s | CHUA "
                        "gui: %s", self._label, gen, turn, _troi,
                        ", ".join(_roi) or "KHONG AI", ", ".join(_chua) or "-")
        except Exception as e:
            log.debug("[%s] soi luot cham loi (bo qua): %s", self._label, e)

    def _log_battle_verbose(self):
        """Log battle (SEND/ACK/Decision) CHI in o LEADER cho gon (party 5 acc x ~10 unit = rat nhieu
        dong). Member an di. Khong co leader (solo) -> van in.

        LOAN DAU LA NGOAI LE: moi acc danh DOC LAP, khong he lap party (log: "KHONG o party nao"),
        nen khai bao leader trong config KHONG con y nghia o day - an log member chi lam mat dau
        vet. Log 01/09 party 22: tran keo 12 PHUT ma chi thay leader gclmot gui lenh, 4 acc kia
        khong mot dong nao -> khong the biet chung co danh hay dang dung im (user hoi dung cau do:
        "m xem bot co gui lenh danh ko"). Loan dau moi acc ~2 dong/luot nen khong so ngap log.
        """
        if getattr(self, "_loandau_started", False):
            return True
        try:
            lead = config.PARTY_LEADER_ACC.get(self.party_idx)
        except Exception:
            lead = None
        return lead is None or lead == self._username

    def _track_battle_packet(self, opcode: int, pkt: bytes):
        if opcode not in (0x0B, 0x14, 0x32, 0x33, 0x34, 0x35):
            return ()
        if self.self_entity:
            self.battle_tracker.local_role_id = self.self_entity
        coordinator = self._battle_coordinator()
        account_id = self._battle_account_id()
        body = pkt[7:] if len(pkt) > 7 and pkt[6] == opcode else pkt
        if (opcode == 0x34 and body[:2] == b"\x01\x00"
                and not self.battle_tracker.active):
            snapshot = coordinator.canonical_snapshot()
            if snapshot is not None and self.battle_tracker.restore_snapshot(snapshot):
                if snapshot.turn > 0 and coordinator.open_local_turn(
                        account_id, snapshot.generation, snapshot.turn):
                    self.state.sync_from_tracker()
                    self._prepare_tracker_turn()
                    log.info(
                        "[%s] BATTLE BOOTSTRAP g=%d t=%d tu snapshot party sau local 0x34",
                        self._label, snapshot.generation, snapshot.turn,
                    )
                    return ()
        server_end = (
            opcode == 0x14
            and body[:2] == b"\x08\x00"
            and len(body) >= 3
            and body[-1] in (0x03, 0x04)
            and self.battle_tracker.active
            and getattr(self, "_active_team_dungeon_level", None) != 110
            and not any(
                row in self.state.enemy_rows and unit.alive and unit.hp > 0
                for (row, _col), unit in self.battle_tracker.units.items()
            )
        )
        events = self.battle_tracker.confirm_end() if server_end else self.battle_tracker.apply(opcode, pkt)
        if not events:
            return ()
        snapshot = self.battle_tracker.snapshot()
        accepted = tuple(
            coordinator.observe(account_id, event, snapshot=snapshot)
            for event in events
        )
        self.state.sync_from_tracker()
        for event in events:
            if event.kind == "start":
                self._metrics_battle_start()
            elif event.kind == "turn_start":
                self._metrics_battle_start()
                self._prepare_tracker_turn()
                # train_block_stats: battle tracker MOI thay nhanh 0x33 legacy -> ghi so block quai
                # o day, 1 lan/tran (theo generation). Truoc day _record_train_block_stats CHI goi o
                # nhanh 0x33 legacy (bi skip khi battle_tracker.generation != 0) -> tracker moi bat =>
                # train_block_stats.json KHONG cap nhat (bug user bao "ko ghi block sau moi tran").
                if getattr(self, "_block_stats_gen", None) != event.generation and self.state.enemy_slots:
                    self._block_stats_gen = event.generation
                    self._record_train_block_stats(list(self.state.enemy_slots))
            elif event.kind == "ack":
                if self._log_battle_verbose():
                    log.info(
                        "[%s] BATTLE ACK g=%d t=%d source=%s",
                        self._label, event.generation, event.turn, event.source,
                    )
            elif event.kind == "end":
                self._metrics_battle_end()
                self.available = {}
                if self._decision_timer:
                    self._decision_timer.cancel()
                    self._decision_timer = None
                now = time.time()
                self._genuine_end_seen = now
                self._set_battle_end_grace()
                _mark_battle_end(self.party_idx, who=self._label, map_id=self.current_map)
                if getattr(self, "_active_team_dungeon_level", None) == 110:
                    self._team_dungeon_end_seq += 1
                in_team_dungeon = self.in_team_dungeon()
                self.state.reset_enemies(reset_quest=not in_team_dungeon)
                self.state.in_battle = False
                self._heal_after_battle()
        return tuple(zip(events, accepted))

    def _prepare_tracker_turn(self):
        tracker = self.battle_tracker
        if not tracker.active:
            return
        targets = sorted({
            col
            for (row, col), unit in tracker.units.items()
            if row in self.state.enemy_rows and unit.alive and unit.hp > 0
        })
        atype = self.state.my_atype
        self.available = {}
        for unit_kind in (config.UNIT_CHAR, config.UNIT_PET):
            unit = tracker.units.get((unit_kind, atype))
            if unit is not None and unit.alive:
                self.available[unit_kind] = [(atype, target) for target in targets]
        self.last_turn_time = time.time()
        self._acted_turn = False
        if self.auto_combat and self.available:
            self._arm_decision()

    def _set_battle_end_grace(self, seconds: float = 3.0):
        """Mo grace sau KET TRAN, GAN voi the he tran hien tai (xem _battle_end_grace_gen)."""
        self._battle_end_grace_until = time.time() + seconds
        self._battle_end_grace_gen = getattr(getattr(self, "battle_tracker", None), "generation", -1)

    def _in_battle_end_grace(self) -> bool:
        """Con trong grace VA van dung the he tran da ket -> goi 0x35 la TAN DU, bo qua.
        Tran MOI da START (generation khac) -> KHONG chan, phai danh luot 1 ngay."""
        if time.time() >= getattr(self, "_battle_end_grace_until", 0.0):
            return False
        gen = getattr(getattr(self, "battle_tracker", None), "generation", -1)
        return gen == getattr(self, "_battle_end_grace_gen", -1)

    def _battle_can_send(self, source):
        tracker = self.battle_tracker
        return self._battle_coordinator().can_send(
            self._battle_account_id(), tracker.generation, tracker.turn, source=source,
        )

    def _arm_decision(self):
        if self._decision_timer:
            self._decision_timer.cancel()
        # PHO BAN TO DOI: delay gui 0x32 = RANDOM 0.5-2s (giong human, sau battle start) thay vi 0.3s
        # co dinh. Cua so 20 phut tu luc vao pho ban (leader tao / member accept) -> tu het, train ve 0.3s.
        if self.in_team_dungeon():
            import random
            delay = random.uniform(0.5, 2.0)
        else:
            delay = self.submit_delay
        self._decision_timer = threading.Timer(delay, self._make_decisions)
        self._decision_timer.start()

    def _make_decisions(self):
        # Thread Timer nay GUI lenh danh 0x32. Khi dang PB -> uu tien de lenh khong bi cac acc train
        # lam tre (tre -> lech phien/mat luot -> luot cham 25s). Timer ngan han nen set moi lan.
        if self.in_team_dungeon():
            _set_thread_prio(1)
        if self._acted_turn:
            return
        # VUA nhan goi KET TRAN THAT (grace, CUNG THE HE) -> tran DA xong: KHONG ra quyet dinh nao (ke ca
        # Hoi Sinh). Timer quyet dinh co the da ARM tu goi 0x35 luot cuoi TRUOC khi sub0800 toi, roi
        # fire SAU khi ket tran -> decide tren state tan du (quai da clear) -> cast Hoi Sinh oan
        # (bug that user gap: chinh acc da "XAC NHAN ket tran THAT" xong VAN ra lenh Hoi Sinh 1s sau).
        if self._in_battle_end_grace():
            return
        self.state.party_idx = self.party_idx   # sync de dieu phoi hoi sinh chéo account
        # DANG QUA CONG (gui chuoi 0x14): KHONG gui 0x32 danh -> tranh "vua qua cong vua danh"
        # (0x32 xen giua 0x14 -> server kick leader). Bo luot nay; transit doi map -> tran cu bo,
        # neu transit that bai (van map cu) -> luot sau danh binh thuong.
        if self._gate_transit:
            # Dang gui chuoi 0x14 -> KHONG duoc chen 0x32 (server kick leader). NHUNG cung KHONG
            # duoc BO LUOT: tran phuc kich o cong no NGAY GIUA chuoi transit -> bo luot thi CA
            # PARTY phai cho turn timeout ~30s moi danh (log 10:06:27 leader mat luot -> 10:06:59
            # ca doi moi danh -> tran keo 40s thay vi 10s). GIU `available`, THU LAI khi transit
            # xong (chuoi 0x14 chi ~2s). Neu transit THANH CONG -> doi map -> tran bien mat, lan
            # thu lai chi thay 'khong con quai song' roi thoi (vo hai).
            self._gate_retry = getattr(self, "_gate_retry", 0) + 1
            if self._gate_retry <= 40:          # ~12s (0.3s/lan)
                self._decision_timer = threading.Timer(0.3, self._make_decisions)
                self._decision_timer.start()
                return
            log.warning("[%s] cho gate transit qua lau -> bo luot nay", self._label)
            self._gate_retry = 0
            self.available = {}
            threading.Timer(1.0, self._reset_turn).start()
            return
        self._gate_retry = 0
        # Neu stats chua load (hp_max=0) -> doi toi da 1s cho 0x0b kip den
        if self.state.char.hp_max == 0 and self.state.pet.hp_max == 0:
            for _ in range(10):
                time.sleep(0.1)
                if self.state.char.hp_max != 0 or self.state.pet.hp_max != 0:
                    break
            else:
                log.warning("[%s] Stats chua load sau 1s -> bo qua luot", self._label)
                self.available = {}
                threading.Timer(1.5, self._reset_turn).start()
                return
        self._acted_turn = True
        try:
            char_opts = self.available.get(config.UNIT_CHAR, [])
            pet_opts = self.available.get(config.UNIT_PET, [])
            # CHI dieu khien pet neu 0x35 co option pet o DUNG vi tri cua minh (my_atype).
            # Pet o CUNG atype voi char (khac hang/unit). Khong co pet@my_atype = acc KHONG co pet
            # (trong tran nay) -> gui lenh pet se sai -> server da/disconnect.
            if self.state.my_atype not in {o[0] for o in pet_opts}:
                pet_opts = []
            # CHI danh khi 0x35 THAT SU co offer cho atype cua MINH. Server gui 1 goi 0x35 RIENG cho
            # TUNG unit (party 5 nguoi = toi 10 goi/luot) -> bot nhan goi cua THANH VIEN KHAC (chua
            # phai luot minh) van kich _arm_decision -> char_opts co the CHI chua atype nguoi khac.
            # KHONG loc -> _offered_targets tung FALLBACK dung target nguoi khac lam cua minh -> gui
            # atk SAI LUC/SAI DU LIEU -> server im lang bo qua -> turn khong tien -> lap y het (dung
            # nghi van user: "giua 2 lan atk khong co goi tin quai nao" - vi day KHONG PHAI turn moi
            # that, chi la offer cua thanh vien khac kich nham).
            if self.state.my_atype not in {o[0] for o in char_opts}:
                char_opts = []
            # CON DA CHET (hp_max>0 va hp<=0) -> KHONG gui lenh cho no (gui lenh cho xac chet ->
            # server coi la lenh sai -> DA/disconnect). hp tu 0x33 moi luot.
            char_dead = self.state.char.hp_max > 0 and self.state.char.hp <= 0
            pet_dead = self.state.pet.hp_max > 0 and self.state.pet.hp <= 0
            ft = self._first_turn
            # FLEE MODE: bo chay thay vi danh. PHAI dung dung my_atype (vi tri cua MINH trong
            # party) - KHONG lay char_opts[0][0] (la atype cua VI TRI DAU danh sach, co the la
            # nguoi khac) -> sai atype thi server DA/KICK (Tao Thao kick luon).
            if getattr(self, "flee_mode", False) and not self.party_members:
                # flee_mode CHI ap dung khi DANH LE (chua co party): party_members rong. DANG TRONG
                # PARTY (roster co member) -> KHONG flee du flee_mode=True: flee tran party bi server
                # KICK khoi party -> nick van khoi party -> leader thay thieu -> reform -> vo party
                # (bug: dang keo ra spot, member bi keo vao tran leader ma flee_mode con True tu luc
                # rally -> BO CHAY -> vang khoi party). Da co party -> DANH BAT CHAP (theo yeu cau).
                my_at = self.state.my_atype
                # PET flee phai CUNG atype voi CHAR. Dung option pet THO tu 0x35 (raw_pet),
                # KHONG dung pet_opts (da bi loc theo my_atype o tren) - vi my_atype co the
                # SAI/CU (vd roster khong co self -> lay tu 0x0b) -> loc nham -> bo sot pet ->
                # pet khong hanh dong -> turn khong hoan tat -> KET TRAN khong thoat duoc.
                raw_pet = self.available.get(config.UNIT_PET, [])
                pet_atypes = {o[0] for o in raw_pet}
                a = None
                if char_opts:
                    a = my_at if my_at in {o[0] for o in char_opts} else char_opts[0][0]
                    if not char_dead:   # char con song moi flee (xac chet -> khong gui)
                        # hang = hang CUA MINH (loan dau doi phe -> 0, khong phai 3 co dinh)
                        self._send_combat(combat.Decision(
                            config.UNIT_CHAR, a, a, config.SKILL_FLEE,
                            b=combat._hang_cua(self.state, config.UNIT_CHAR)))
                # Gui pet flee CHI khi 0x35 co option pet o DUNG slot char dang flee (a) VA pet con song.
                if a is not None and a in pet_atypes and not pet_dead:
                    self._send_combat(combat.Decision(
                        config.UNIT_PET, a, a, config.SKILL_FLEE,
                        b=combat._hang_cua(self.state, config.UNIT_PET)))
                log.info("[%s] BO CHAY (flee_mode, char_at=%s pet_at=%s my_atype=%s char_opts=%s pet_opts=%s)",
                         self._label, a, (a if (a is not None and a in pet_atypes) else None),
                         my_at, sorted({o[0] for o in char_opts}), sorted(pet_atypes))
                # (in_battle ha o handler 0x14 0c00/0900/0800 = man BO CHAY tu server - khong doan o day.)
                return
            if char_opts and not char_dead:
                d = combat.decide_char(self.state, char_opts, ft)
                if d is None:
                    # KHONG CON QUAI SONG (tran da ket, goi 0x35 "tan du" sau khi thang) -> KHONG
                    # gui gi (truoc day fallback danh MU cot 1 -> goi 0x32 thua sau khi da thang tran).
                    log.info("[%s] CHAR khong con quai song -> bo qua (tran da ket)", self._label)
                else:
                    self._send_combat(d)
                    if self._log_battle_verbose():
                        _off = sorted(t for a, t in char_opts if a == self.state.my_atype)
                        log.info("[%s] CHAR %s | %s | skills=%s | quai@%s | offer(my_at=%s)=%s | enemy_hp=%s",
                                 self._label, d, self.state.char,
                                 [hex(s) for s in sorted(self.state.skills_char)],
                                 self.state.enemy_slots, self.state.my_atype, _off,
                                 {k: v for k, v in sorted(self.state.enemy_hp.items()) if v > 0})
            elif char_opts and char_dead:
                log.info("[%s] CHAR HP=0 (da chet) -> KHONG gui lenh attack", self._label)
            if self.state.solo_multipet:
                # DI GIOI SOLO: toi da 4 pet CUNG luot, moi con 1 atype rieng (0,1,3,4) - KHONG
                # loc theo self.state.my_atype (do la atype cua CHAR=2, khong lien quan pet o day).
                # Duyet TAT CA atype pet co mat trong 0x35 luot nay (raw, chua bi loc o tren).
                raw_pet = self.available.get(config.UNIT_PET, [])
                pet_atypes_now = sorted({o[0] for o in raw_pet})
                for pat in pet_atypes_now:
                    opts_at = [o for o in raw_pet if o[0] == pat]
                    unit = self.state.multi_pet.get(pat)
                    if unit is not None and unit.hp_max > 0 and unit.hp <= 0:
                        continue   # con nay da chet -> khong gui lenh (giong char_dead/pet_dead)
                    skills_at = self.state.multi_pet_skills.get(pat, [])
                    if unit is None:
                        unit = Unit(f"pet_at{pat}")   # chua co du lieu HP/SP tu 0x33 -> Unit rong
                        self.state.multi_pet[pat] = unit
                    d = combat.decide_multipet(self.state, pat, skills_at, unit, opts_at)
                    if d is not None:
                        self._send_combat(d)
                        if self._log_battle_verbose():
                            log.info("[%s] PET(atype=%d) %s | skills=%s | %s", self._label, pat, d,
                                     [hex(s) for s in skills_at], unit)
            elif pet_opts and not pet_dead:
                d = combat.decide_pet(self.state, pet_opts, ft)
                if d is None:
                    log.info("[%s] PET khong con quai song -> bo qua (tran da ket)", self._label)
                else:
                    self._send_combat(d)
                    if self._log_battle_verbose():
                        log.info("[%s] PET  %s | %s", self._label, d, self.state.pet)
            elif pet_opts and pet_dead:
                log.info("[%s] PET HP=0 (da chet) -> KHONG gui lenh attack", self._label)
            self._first_turn = False
        finally:
            # reset cho luot sau
            self.available = {}
            threading.Timer(1.5, self._reset_turn).start()

    def _reset_turn(self):
        self._acted_turn = False

    def _send_combat(self, d: combat.Decision, tail: bytes = None):
        """0x32: 01 00 [unit][atype][b11=00][target][skill LE][tail].
        tail = 2 byte nonce; client THAT gui gia tri THAY DOI MOI GOI (xac nhan capture). Truoc day
        bot gui CO DINH 0000 -> khi 2 turn LIEN TIEP CUNG skill+target (vd don 1 con "trau" nhieu
        HP trong pho ban to doi) -> goi 0x32 GIONG HET byte-by-byte -> server co the coi la goi
        lap/replay -> AM THAM BO QUA -> turn khong tien -> ket cung lap lai (xac nhan qua log: tran
        co quai HP cao/nhieu turn lien danh cung 1 con bi ket, tran quai yeu target doi lien tuc thi
        khong sao). LUON random (KHONG con env RAND_TAIL) de giong client that."""
        # HANG NGUON. `d.unit` vua la "char hay pet" (3/2) vua duoc dung THANG lam hang nguon
        # trong goi - trung nhau vi tran thuong luon xep phe ta o hang 2-3. LOAN DAU thi minh co
        # the o hang 0-1: gui hang nguon 3/2 luc dang dung o hang 0 = ban ghi khong phai cua minh
        # -> server tra `S:000-000` ly do 42 `修改戰鬥封包` va DA HAN acc (su co 25/08 21:58).
        # `_hang_cua` tra dung 3/2 o mac dinh nen tran thuong khong doi mot ly nao.
        hang_nguon = combat._hang_cua(self.state, d.unit)
        source = (hang_nguon, d.atype)
        tracker = self.battle_tracker
        if tracker.generation:
            coordinator = self._battle_coordinator()
            _acc_id = self._battle_account_id()
            _key_ok, _turn_ok = coordinator.sent_state(_acc_id, tracker.generation, tracker.turn)
            if not (_key_ok and _turn_ok):
                # Phien dieu phoi lech (thuong sau relogin/reform) -> VAN GUI. Truoc day chan o day
                # = mat sach lenh danh, im lang. Log WARNING de lan sau thay ngay.
                log.warning(
                    "[%s] party-battle lech phien (khop_key=%s thay_turn=%s) g=%d t=%d -> VAN GUI "
                    "lenh danh (khong bo)", self._label, _key_ok, _turn_ok,
                    tracker.generation, tracker.turn,
                )
            if not coordinator.mark_sent(_acc_id, source, tracker.generation, tracker.turn):
                log.warning(
                    "[%s] bo SEND source=%s vi DA GUI dung luot nay roi (g=%d t=%d)",
                    self._label, source, tracker.generation, tracker.turn,
                )
                return False
            tracker.register_action(source, d.skill, (d.b, d.target))
        import random
        if tail is None:
            # HAI byte cuoi = Y HET CLIENT (`FightRoleController.lua:2659`):
            #     local checkByte = math.random(220);   -- 1..220, KHONG phai 1..255
            #     sendBuffer:WriteByte(checkByte);
            #     sendBuffer:WriteByte(math.random(256));
            # Bot truoc day random ca hai byte trong 1..0xFFFF -> byte checkByte co the > 220, thu
            # ma client THAT khong bao gio gui. Log 01/09 20:34:10 (loan dau, acc ttsau): goi pet
            # `...d532 EA bb` co checkByte = 234 -> server tra `S:000-000` ly do 42
            # `修改戰鬥封包` (sua goi chien dau) va DA HAN acc (quit = true).
            tail = bytes([random.randint(1, 220), random.randint(0, 255)])
        payload = (b"\x01\x00"
                   + bytes([hang_nguon, d.atype, getattr(d, "b", 0), d.target])
                   + struct.pack("<H", d.skill)
                   + tail)
        self.send(protocol.OP_COMBAT, payload)
        # MOC "DA GUI LENH DANH" - ghi o MOI acc, ke ca acc bi an log (xem `_log_battle_verbose`:
        # SEND/ACK chi in o leader). Khong co moc nay thi khong the tra loi cau hoi don gian nhat
        # khi mot luot keo dai: "may dua kia CO gui lenh danh khong" - la cau user hoi 01/09 (party
        # 22, tran 12 phut) va hoi lai 11/09 (party 2, luot 99 giay). Ca hai lan deu khong tra loi
        # duoc vi log member bi an, ma an log thi khong con dau vet nao khac.
        self._luot_da_gui = (int(tracker.generation or 0), int(tracker.turn or 0), time.time())
        if tracker.generation and self._log_battle_verbose():
            log.info(
                "[%s] BATTLE SEND g=%d t=%d source=%s skill=%d target=%s",
                self._label, tracker.generation, tracker.turn, source, d.skill, (d.b, d.target),
            )
        return True

    def flee_battle(self):
        """BO CHAY khoi tran: gui 0x32 skill=0x4651 cho ca char + pet, TARGET = chinh minh
        (target = vi tri tran cua minh = atype; flee.pcap: char atype=2->target=2).
        char b=3, pet b=2 (tu flee.pcap) - nhung do la HANG CUA PHE TA o tran thuong; LOAN DAU bi
        xep sang phe kia thi char ta o hang 0 / pet hang 1, gui 3/2 la nham vao o KHONG PHAI cua
        minh -> server coi la sua goi chien dau (ma 42). Lay hang dong tu `combat._hang_cua`."""
        at = self.state.my_atype
        _hc = combat._hang_cua(self.state, config.UNIT_CHAR)
        _hp = combat._hang_cua(self.state, config.UNIT_PET)
        self._send_combat(combat.Decision(unit=config.UNIT_CHAR, atype=at, target=at, skill=config.SKILL_FLEE, b=_hc))
        if self.state.active_pet_id is not None:   # chi gui pet khi CO pet (theo goi 0x13 login)
            self._send_combat(combat.Decision(unit=config.UNIT_PET, atype=at, target=at, skill=config.SKILL_FLEE, b=_hp))
        log.info("[%s] BO CHAY khoi tran (skill %d, target=atype=%d)", self._label, config.SKILL_FLEE, at)

    # ---- qua online (0x57) ----
    def request_offline_exp(self, exp_type: int = 0x1c):
        """Hoi info exp offline (type 0x1c). Neu co exp -> tu nhan (xu ly o _on_offline_exp)."""
        self.send(0x54, b"\x01\x00" + struct.pack("<H", exp_type))

    MAIL_CHUA_DOC, MAIL_DA_DOC, MAIL_DA_NHAN = 0, 1, 2   # EMailState (Logic/Social.lua)

    @task_report("nhan mail", PHASE_LOGIN_CHORE)
    def claim_mail(self):
        """Mail (opcode 0x53): nhan qua roi CHI xoa mail da nhan xong - LAM Y CLIENT.

        Goi (protocal.lua, batch - da verify capture nhan/xoa 17 mail):
          nhan:  53 01 00 [count 4B][mailid 4B]*count   (C:083-001 <領取附件>)
          xoa:   53 02 00 [count 4B][mailid 4B]*count   (C:083-002 <刪除信件>)
          doc:   53 03 00 [count 4B][mailid 4B]*count   (C:083-003 <已讀信件>)
        Server XAC NHAN nhan duoc mail nao qua `S:083-002` -> `self._mail_taken`.

        Client (UI/UIMail.lua) tach bach hai viec, bot truoc day gop lam mot:
          OnClick_TakeAll   -> chi mail `state < Take` VA `#contents > 0` (con qua chua nhan)
          OnClick_RemoveEmpty -> chi mail `state == Take` HOAC (`state == Read` va KHONG co qua)
        Tuc client KHONG BAO GIO xoa mail con qua chua nhan duoc. Bot thi gui 53 01 cho TAT CA roi
        53 02 cho TAT CA ngay sau do, khong doi xac nhan: tui DAY -> server tu choi cho nhan ->
        van xoa -> MAT QUA (user 01/09: "neu nhan qua fail do tui do full thi sau do m van xoa mail
        lam mat qua").
        """
        # KHONG xoa _mail_ids o dau (server push luc login TRUOC khi ham nay chay).
        # CHO server PUSH HET mail: cac goi 0x53 sub01 (moi mail 1 goi) den RAI RAC luc login -> neu
        # gom NGAY thi chi bat duoc mail dau (bug: nhieu mail chi nhan 1). Doi toi khi _mail_ids NGUNG
        # tang (on dinh 1.5s) hoac cap 6s.
        _t0 = time.time(); _last_n = -1; _stable = time.time()
        while self.running and time.time() - _t0 < 6:
            _n = len(self._mail_ids)
            if _n != _last_n:
                _last_n = _n; _stable = time.time()
            elif time.time() - _stable > 1.5:
                break                            # on dinh 1.5s (co mail hay khong deu thoat)
            time.sleep(0.3)
        mails = list(self._mail_ids)
        self._mail_ids = []                      # consume sau khi gom
        self._mail_taken = set()
        if not mails:
            return

        def _gui(sub, ids):
            self.send(0x53, sub + struct.pack("<I", len(ids)) + b"".join(ids))
            time.sleep(0.6)

        # 1. NHAN: chi mail CON QUA chua nhan (y OnClick_TakeAll).
        can_nhan = [mid for mid, st, nq in mails if nq > 0 and st < self.MAIL_DA_NHAN]
        if can_nhan:
            _gui(b"\x01\x00", can_nhan)
        # 2. DANH DAU DA DOC mail rong: client chi xoa mail rong khi no da o trang thai Read.
        chua_doc_rong = [mid for mid, st, nq in mails
                         if nq <= 0 and st < self.MAIL_DA_DOC]
        if chua_doc_rong:
            _gui(b"\x03\x00", chua_doc_rong)
        # 3. XOA: mail SERVER DA XAC NHAN nhan xong, + mail rong (y OnClick_RemoveEmpty).
        #    Mail con qua ma khong co trong `_mail_taken` = nhan KHONG duoc (tui day) -> GIU LAI.
        can_xoa = [mid for mid, st, nq in mails
                   if nq <= 0 or st >= self.MAIL_DA_NHAN or mid in self._mail_taken]
        con_qua = [mid for mid, st, nq in mails
                   if nq > 0 and st < self.MAIL_DA_NHAN and mid not in self._mail_taken]
        if can_xoa:
            _gui(b"\x02\x00", can_xoa)
        if con_qua:
            log.warning("[%s] Mail: %d/%d mail nhan duoc, %d mail CON QUA chua nhan duoc "
                        "-> KHONG xoa (nhieu kha nang TUI DAY, don tui roi login lai la nhan)",
                        self._label, len(mails) - len(con_qua), len(mails), len(con_qua))
        log.info("[%s] Mail: nhan %d, xoa %d, giu lai %d (tong %d)",
                 self._label, len(can_nhan), len(can_xoa), len(con_qua), len(mails))

    # ===== SLOT TUI DO (dung luong tui + mua them slot) =====
    # capacity = 50 (goc) + (Bag_1[103]+Bag_2[106]+Bag_3[124]) * 5.  Bag_x = RoleCount (0x55, da track).
    # Mua slot = UISell sellId=3 (0x54): query gia sub01, confirm sub02. Tang Bag_1 (max 30 -> +150 slot).
    # Gia = NGUYEN BAO (kind=1), tang dan moi lan.
    # ---- NHAN QUA THANH TUU (opcode 82) ---------------------------------------------------
    # Crack client (Logic/Achievement.lua + UI/UIAchievement.lua):
    #   C:082-002 <成就領獎> +成就ID(2)  -> 0x52 sub 0200 + [id u16 LE]
    #   S:082-002 [result 1B] (+id u16 neu result==0). result 0 = OK, 1 = fail.
    # UI chi cho bam nhan khi: not HaveGetFlag() and HaveCompeleteFlag(), va TUI DAY thi khong
    # nhan (Item.CheckBagIsFull) -> bot theo y het.
    # KHONG can tinh dieu kien thanh tuu: 3 trang thai deu doc tu 2 BIT trong mang forever-flags
    # (goi 0x51) ma bot da co (_bitflag_get).
    # ECondition (Logic_CheckCondition.lua) -> nguon gia tri. Chi cai cac loai thanh tuu THUC SU
    # dung (khao sat AchievementData_C.dat: 15/14/6/20/1/7-12/18).
    _COND_GOLD, _COND_ACH_SCORE = 1, 6
    _COND_INT, _COND_ATK, _COND_AGI, _COND_DEF, _COND_HPX, _COND_SPX = 7, 8, 9, 10, 11, 12
    _COND_ROLECOUNT, _COND_MISSION_FLAG = 14, 15
    _COND_FRIEND_COUNT, _COND_NOW_LEVEL = 18, 20

    def _achievement_value(self, kind: int, kind_value: int):
        """Gia tri hien tai cua dieu kien, hoac None neu bot KHONG co du lieu (-> khong doan)."""
        k = int(kind)
        if k == self._COND_MISSION_FLAG:            # 435/600 - co nhiem vu (0x18 sub07/05)
            if not self._mark_flags_loaded:
                return None
            bit = _load_mark_bitids().get(int(kind_value))
            return None if bit is None else (1 if self.mark_flag_get(bit) else 0)
        if k == self._COND_ROLECOUNT:               # 91/600
            rc = self.role_counts.get(int(kind_value))
            return None if rc is None else int(rc[0])
        if k == self._COND_ACH_SCORE:
            # Client (Achievement.InitTotalScore) chi cong diem cai DA NHAN THUONG - dung getFlag,
            # KHONG phai completeFlag (trong Lua con dong cu bi comment lai de doi sang getFlag:
            # "有領獎的再計算積分").
            if not self._bitflags_loaded:
                return None
            return sum(int(v.get("score") or 0) for v in _load_achievements().values()
                       if self._bitflag_get(v["gf"]) is True)
        if k == self._COND_GOLD:
            return self.xu
        # Chi so GOC tu 0x08 - dung y client (GetAttribute), KHONG dung so "thuc te" bot tu cong
        # them trang bi/chuyen sinh/ngua (thanh tuu chi tinh chi so goc).
        _attr = {self._COND_INT: 27, self._COND_ATK: 28, self._COND_DEF: 29,
                 self._COND_AGI: 30, self._COND_HPX: 31, self._COND_SPX: 32,
                 self._COND_NOW_LEVEL: 35}.get(k)
        if _attr is not None:
            return self.char_attrs.get(_attr)
        if k == self._COND_FRIEND_COUNT:
            return self.max_friend_count      # S:014-017; None = server chua gui -> bo qua
        return None

    def report_completed_achievements(self, wait: float = 0.25, max_send: int = 60) -> int:
        """Tu tinh dieu kien roi BAO SERVER thanh tuu da xong (C:082-001), y het client login.

        Khong lam viec nay thi cac thanh tuu dat duoc trong luc chi co bot chay se KHONG BAO GIO
        duoc danh dau -> claim_achievements() khong thay gi de nhan.
        """
        data = _load_achievements()
        if not data or not self._bitflags_loaded:
            return 0
        self._ach_report_ok = 0
        self._ach_report_bad = 0
        pend, no_data = [], 0
        for aid, v in sorted(data.items()):
            if self._bitflag_get(v["cf"]) is True:
                continue                              # server da biet roi
            cur = self._achievement_value(v.get("kind"), v.get("kind_value"))
            if cur is None:
                no_data += 1
                continue                              # thieu du lieu -> KHONG gui bua
            if not self._MISSION_OP.get(int(v.get("opr") or 3),
                                        lambda p, c: False)(cur, int(v.get("value") or 0)):
                continue
            pend.append((aid, v))
        if not pend:
            log.info("[%s] Thanh tuu: khong co cai nao vua dat (%d cai bot chua doc duoc dieu kien)",
                     self._label, no_data)
            return 0
        log.info("[%s] Thanh tuu: %d cai VUA DAT -> bao server (client that lam viec nay luc login)",
                 self._label, len(pend))
        n = 0
        for aid, v in pend[:max_send]:
            if not self.running:
                break
            # PHANH: server tu choi lien tiep = cong thuc tinh dieu kien cua bot SAI -> dung ngay,
            # khong ban tiep ca loat (tinh nang nay chua duoc test tren server that).
            if getattr(self, "_ach_report_bad", 0) >= 5:
                log.warning("[%s] Thanh tuu: server tu choi 5 cai LIEN TIEP -> DUNG bao hoan thanh "
                            "(cong thuc dieu kien cua bot co the sai)", self._label)
                break
            # C:082-001 <完成成就> [count 1B][id u16]
            self.send(0x52, b"\x01\x00" + b"\x01" + int(aid).to_bytes(2, "little"))
            n += 1
            log.info("[%s] Thanh tuu: bao hoan thanh '%s'", self._label, v["name"] or aid)
            time.sleep(wait)
        if len(pend) > max_send:
            log.info("[%s] Thanh tuu: con %d cai -> de lan login sau", self._label, len(pend) - max_send)
        time.sleep(1.0)          # cho server bat co (0x51 delta) truoc khi claim
        _ok = getattr(self, "_ach_report_ok", 0)
        log.info("[%s] Thanh tuu: bao %d cai -> server chap nhan %d", self._label, n, _ok)
        return n

    @task_report("nhan qua thanh tuu", PHASE_LOGIN_CHORE)
    def claim_achievements(self, wait: float = 0.35, max_claim: int = 40) -> int:
        """Nhan thuong cac thanh tuu DA XONG ma CHUA NHAN. Tra so cai da gui lenh nhan."""
        data = _load_achievements()
        if not data or not self._bitflags_loaded:
            return 0
        # BAO HOAN THANH TRUOC (giong client login): server khong tu danh dau, khong bao thi
        # nhung thanh tuu vua dat se khong co co -> khong co gi de nhan.
        try:
            self.report_completed_achievements()
        except Exception as e:
            log.warning("[%s] Thanh tuu: loi bao hoan thanh (bo qua): %s", self._label, e)
        if self.bag_free_slots() <= 0:      # client: tui day -> KHONG nhan (qua se roi mat)
            log.info("[%s] Thanh tuu: TUI DAY -> hoan nhan qua", self._label)
            return 0
        # BITMAP CO DU DAI KHONG: co thanh tuu chay toi bit 8000 -> can >= 1000 byte, ma goi 0x51
        # full chi ~1004 byte TONG (tru header con ~993 -> phu toi bit ~7944). Bit NGOAI bitmap thi
        # _bitflag_get tra False = "chua hoan thanh" -> bo qua AM THAM. Canh bao ro de biet ngay.
        _bits = len(self._bitflag_bytes) * 8
        _out = [aid for aid, v in data.items() if max(v["cf"], v["gf"]) > _bits]
        if _out:
            log.warning("[%s] Thanh tuu: bitmap 0x51 chi co %d bit (%d byte) -> %d/%d thanh tuu co "
                        "co NGOAI bitmap, KHONG doc duoc trang thai (bit max can = %d)",
                        self._label, _bits, len(self._bitflag_bytes), len(_out), len(data),
                        max(max(v["cf"], v["gf"]) for v in data.values()))
        pend = [(aid, v) for aid, v in sorted(data.items())
                if self._bitflag_get(v["cf"]) is True and self._bitflag_get(v["gf"]) is not True]
        n_done = sum(1 for v in data.values() if self._bitflag_get(v["cf"]) is True)
        if not pend:
            log.info("[%s] Thanh tuu: %d/%d da hoan thanh, khong co qua nao cho nhan",
                     self._label, n_done, len(data))
            return 0
        log.info("[%s] Thanh tuu: %d/%d da hoan thanh, %d cai CHUA NHAN qua -> nhan",
                 self._label, n_done, len(data), len(pend))
        items = _load_gamedata_items()
        n = 0
        for aid, v in pend[:max_claim]:
            if not self.running or self.bag_free_slots() <= 0:
                break
            self.send(0x52, b"\x02\x00" + int(aid).to_bytes(2, "little"))
            n += 1
            _it = (items.get(v["item"]) or {}).get("name", "")
            log.info("[%s] Thanh tuu: nhan qua '%s' -> %s x%d",
                     self._label, v["name"] or aid, _it or hex(v["item"]), v["count"])
            time.sleep(wait)
        if len(pend) > max_claim:
            log.info("[%s] Thanh tuu: con %d cai chua nhan -> de lan login sau",
                     self._label, len(pend) - max_claim)
        return n

    _BAG_ROLECOUNT_IDS = (103, 106, 124)

    def bag_capacity(self) -> int:
        exp = sum(self.role_counts.get(sid, (0, 0))[0] for sid in self._BAG_ROLECOUNT_IDS)
        return 50 + exp * 5

    def bag_used_slots(self) -> int:
        return len(self.bag_slots)

    def bag_free_slots(self) -> int:
        return max(0, self.bag_capacity() - len(self.bag_slots))

    def _ghi_cache_tui(self):
        """Luu tui do ra cache de XEM LAI khi acc tat. CHI ghi o HAI MOC: lúc LOGIN (snapshot day
        `0x17 sub05`) va luc DONG acc (`close`).

        KHONG ghi trong luc chay (user chot 06/09: "trong luc chay thi chi dung cache tien trang,
        cache tui do ghi lien tuc cung thua"). Ban cache nay chi de XEM khi acc DA TAT, ma luc do
        thi ban ghi lan cuoi moi la ban dung - cac ban ghi giua chung khong ai doc toi.

        Vi sao quan trong: du da tiet che 120s, 246 acc van thanh ~20 luot ghi/phut, MOI luot ghi
        LAI CA FILE. Cache tui chiem 91.5% file (596/651 KB) -> file phinh gap 10 lan va lien tuc o
        trang thai ghi do dang. Do 06/09: 6/12 lan doc file trung luc dang ghi -> `json.load` hong.
        """
        try:
            return save_bag_cache(self._username, {
                "slots": {str(int(s)): [int(v[0]), int(v[1])]
                          for s, v in (self.bag_slots or {}).items() if v and int(v[1]) > 0},
                "cap": self.bag_capacity(), "used": self.bag_used_slots(),
                # Luu ca DO DANG MAC + PET MANG THEO: khong co thi ban offline mat hang 6 o trang
                # bi va mat cac nut chon pet, nhin khac han luc acc chay - user tuong hong.
                "equip_fit": {str(int(f)): int(t) for f, t in (self.equip_by_fit or {}).items()},
                "pet_equip_fit": {str(int(w)): {str(int(f)): int(t) for f, t in (m or {}).items()}
                                  for w, m in (self.pet_equip_by_fit or {}).items()},
                "pets": [[i, (r[1] if isinstance(r, (tuple, list)) and len(r) > 1 else "")]
                         for i, r in enumerate(
                             list(getattr(self.state, "carried_pets", None) or ())[:4], 1)],
                "pet_slot": int(getattr(self, "active_pet_slot", 0) or 0),
            })
        except Exception as e:
            log.debug("[%s] ghi cache tui do loi (bo qua): %s", self._label, e)
            return False


    def _ghi_cache_kho(self):
        """Luu tien trang ra cache. Goi moi khi `bank_slots` doi - kho chi mo luc di cat do nen
        so lan ghi rat it, khong can tiet che."""
        try:
            return save_bank_cache(self._username, {
                "slots": {str(int(i)): [int(v[0]), int(v[1])]
                          for i, v in (self.bank_slots or {}).items() if v and int(v[1]) > 0},
            })
        except Exception as e:
            log.debug("[%s] ghi cache tien trang loi (bo qua): %s", self._label, e)
            return False

    # --- BO DO (outfit): luu san mot bo, khi can doi CA BO mot lan ---
    # Luu theo ID MON, khong theo o tui: o tui doi lien tuc (ban do, nhat do, sap xep) nen luu o
    # la vo nghia. He qua: hai mon TRUNG ID khac cuong hoa thi khong phan biet duoc -> uu tien
    # mon cuong hoa cao hon (xem _bag_slot_best).
    OUTFIT_FITS = (1, 2, 3, 4, 5, 6)      # 6 vi tri that (Item.IsEquip)

    def outfit_snapshot(self) -> dict:
        """Chup bo do DANG MAC -> {"char": {fit: tid}, "pets": {petIdx: {fit: tid}}}."""
        pets = {}
        for idx, m in (self.pet_equip_by_fit or {}).items():
            row = {int(f): int(t) for f, t in (m or {}).items()
                   if int(f) in self.OUTFIT_FITS and t}
            if row:
                pets[int(idx)] = row
        return {
            "char": {int(f): int(t) for f, t in (self.equip_by_fit or {}).items()
                     if int(f) in self.OUTFIT_FITS and t},
            "pets": pets,
        }

    def _bag_slot_best(self, tid: int):
        """O tui chua `tid`, uu tien mon CUONG HOA cao nhat (roi den linh da cao nhat).

        Bo do chi luu duoc ID nen khi co nhieu ban sao phai tu chon. Lay ban "xin" nhat la lua
        chon it gay bat ngo nhat - dung ban thuong trong khi dang co ban +10 la user se chui.
        """
        ung = []
        for slot, v in (self.bag_slots or {}).items():
            if v and int(v[0]) == int(tid) and int(v[1]) > 0:
                info = (getattr(self, "bag_items", None) or {}).get(slot) or {}
                ung.append((int(info.get("reinforced") or 0), int(info.get("stone_lv") or 0),
                            int(slot)))
        if not ung:
            return None
        ung.sort(key=lambda x: (-x[0], -x[1], x[2]))
        return ung[0][2]

    def apply_outfit(self, outfit: dict):
        """Mac ca bo. Tra (so lenh da gui, danh sach mon THIEU trong tui).

        MAC DE, khong coi truoc (user chot 26/08): gui thang lenh mac, server tu tra mon cu ve o
        cu - bot da xu ly o _on_equip_done. It lenh hon va khong can o tui trong.
        Mon nao DANG MAC DUNG roi thi bo qua, khong gui thua.

        O TRONG trong bo = COI mon dang deo o o do ra (user chot 27/08). Truoc day ham nay CHI
        BIET MAC nen bo de trong o Giay ma pet dang deo giay thi mac xong giay VAN CON tren nguoi
        -> bo khong bao gio ra dung hinh, va vi khong co gi de mac nen gui 0 lenh, GUI lai bao
        nham "o trong / acc mat ket noi".
        Coi do CAN o tui trong (client tu chon o roi gui len, xem unequip_item) -> tui day thi
        `unequip_item` tra False va ghi log, khong gui hut.
        """
        if not outfit:
            return 0, []
        gui, thieu = 0, []
        _viec = [(0, outfit.get("char") or {})]
        for _p, _m in (outfit.get("pets") or {}).items():
            _viec.append((int(_p), _m or {}))
        for follow, muon in _viec:
            # Khoa ve INT: bo do luu ra JSON thi khoa thanh CHUOI, tra `muon.get(5)` se truot ->
            # tuong o do trong -> COI NHAM mon ma bo dang co. Chuan hoa mot lan o day.
            muon = {int(f): int(t or 0) for f, t in (muon or {}).items()}
            dang = (self.pet_equip_by_fit.get(follow) if follow else self.equip_by_fit) or {}
            for fit, tid in sorted((f, t) for f, t in muon.items() if t):
                if int(dang.get(fit, 0)) == tid:
                    continue                      # dang mac dung mon nay roi
                slot = self._bag_slot_best(tid)
                if slot is None:
                    thieu.append((follow, fit, tid))
                    continue
                try:
                    if follow:
                        self.equip_pet_item(follow, slot)
                    else:
                        self.equip_item(slot)
                    gui += 1
                    time.sleep(0.35)              # cho server tra mon cu ve tui truoc lenh sau
                except OSError:
                    break
            # O TRONG trong bo -> COI mon dang deo o do. Lam SAU cac lenh mac de o tui vua duoc
            # giai phong (mac xong mon cu tu ve tui) khong bi tinh nham la day.
            for fit in self.OUTFIT_FITS:
                if int(muon.get(fit, 0) or 0) or not int(dang.get(fit, 0) or 0):
                    continue
                try:
                    if self.unequip_item(int(fit), follow=follow):
                        gui += 1
                        time.sleep(0.35)
                except OSError:
                    break
        if thieu:
            log.warning("[%s] Bo do: thieu %d mon trong tui: %s", self._label, len(thieu),
                        ", ".join("%s/%s 0x%04x" % ("char" if f == 0 else "pet%d" % f, p, t)
                                  for f, p, t in thieu[:6]))
        log.info("[%s] Bo do: da gui %d lenh mac%s", self._label, gui,
                 (", THIEU %d mon" % len(thieu)) if thieu else "")
        return gui, thieu

    # --- HANG DOI LENH TUI DO khi DANG TRONG TRAN ---
    # Client that CHAN cac lenh nay trong tran: Item.UnEquip / Item.Use deu co
    #     if FightField.conIdx ~= FightField.GetPlayerIdx() then ShowCenterMessage(22595); return
    # tuc trong tran CHI doi do duoc khi TOI LUOT MINH. Bot gui bua giua tran thi server nuot lenh
    # (hoac te hon - cac cho khac trong bot da dinh: gui 0x06/0x14 luc dang battle bi DA ket noi).
    # Nen user bam luc dang danh -> XEP HANG, het tran moi gui.
    def queue_bag_cmd(self, ten: str, fn) -> bool:
        """True = DA XEP HANG (dang trong tran). False = khong trong tran, cu goi thang.

        Chi de danh cho lenh TAY tu tui do. Luong tu dong cua bot da tu biet tranh battle.
        """
        if not self.state.in_battle:
            return False
        self._bag_queue.append((ten, fn))
        log.info("[%s] Tui do: dang TRONG TRAN -> xep hang '%s', het tran se gui (%d lenh cho)",
                 self._label, ten, len(self._bag_queue))
        return True

    BAG_FLUSH_TRIES = 3        # so lan thu lai mot lenh bi server nuot
    BAG_FLUSH_VERIFY = 1.2     # giay cho server phan hoi truoc khi coi la bi nuot

    def _flush_bag_queue(self):
        """Xa hang doi sau khi ket tran. Goi tu cho HA in_battle.

        Chay o THREAD RIENG: vong nay co ngu (cho server phan hoi) ma no duoc goi tu thread doc
        goi - ngu o do la chan luon viec doc socket.
        """
        if not self._bag_queue or getattr(self, "_bag_flush_running", False):
            return
        cho = self._bag_queue
        self._bag_queue = []
        self._bag_flush_running = True
        threading.Thread(target=self._bag_flush_worker, args=(cho,), daemon=True,
                         name="bagflush-%s" % (self._label or self._username)).start()

    def _bag_flush_worker(self, cho):
        """Gui tung lenh roi KIEM lenh co an khong; bi nuot thi xep lai de thu lai.

        VI SAO CAN KIEM: moc xa hang doi la `0x14 sub0700`, ma goi do KHONG phai goi ket tran
        that (no la `S:020-007 <事件換場景>`; ket tran that la `S:011-000` = `0x0b sub 0`). Dang
        TRAIN thi tran ke tiep bat dau chi ~2 giay sau, nen lenh doi do gui o moc do bi server
        NUOT IM LANG - user bam 6 lan lien khong an mot lan nao (log 26/08 22:09-22:10), trong
        khi dung yen khong train thi bam phat an ngay.
        Server co phan hoi thi `_equip_seq` tang (xem _on_equip_done/_on_unequip_done) -> lay do
        lam bang chung. Lenh khong lien quan do (vd 'Bo') thi khong co dau hieu -> khong thu lai.
        """
        try:
            for muc in cho:
                ten, fn = muc[0], muc[1]
                lan = (muc[2] if len(muc) > 2 else 1)
                log.info("[%s] Tui do: het tran -> gui lenh da xep hang '%s'%s",
                         self._label, ten, "" if lan == 1 else " (lan %d)" % lan)
                truoc = int(getattr(self, "_equip_seq", 0))
                try:
                    fn()
                except Exception as e:
                    log.warning("[%s] Tui do: lenh '%s' loi khi xa hang doi: %s",
                                self._label, ten, e)
                    continue
                if not self._la_lenh_do(ten):
                    time.sleep(0.3)
                    continue
                het = time.time() + self.BAG_FLUSH_VERIFY
                while time.time() < het and int(getattr(self, "_equip_seq", 0)) == truoc:
                    time.sleep(0.1)
                if int(getattr(self, "_equip_seq", 0)) != truoc:
                    continue
                if lan >= self.BAG_FLUSH_TRIES:
                    log.warning("[%s] Tui do: lenh '%s' KHONG an sau %d lan -> bo. Thu lai luc "
                                "dung yen (khong danh nhau).", self._label, ten, lan)
                    continue
                log.info("[%s] Tui do: lenh '%s' bi server nuot -> xep lai de thu lai",
                         self._label, ten)
                self._bag_queue.append((ten, fn, lan + 1))
        finally:
            self._bag_flush_running = False

    @staticmethod
    def _la_lenh_do(ten: str) -> bool:
        """Lenh MAC/COI do - loai co the xac nhan bang `_equip_seq`."""
        t = (ten or "").lower()
        return t.startswith("trang bị") or t.startswith("cởi ra") or t.startswith("mặc bộ")

    def bag_first_empty_slot(self):
        """O tui TRONG dau tien (1-based, theo bag_capacity). None = tui day.

        Lenh COI DO phai chi RO o nao de nhet mon vua coi vao (client tu tim, khong phai server)."""
        dung = set(self.bag_slots or {})
        for slot in range(1, self.bag_capacity() + 1):
            if slot not in dung:
                return slot
        return None

    def unequip_item(self, fit_pos: int, follow: int = 0) -> bool:
        """COI mon dang mac o vi tri `fit_pos` (fitType 1..6) ra tui. follow = 0 char, 1..4 pet.

        Crack client (Logic_Item.lua:2690-2704, ham Item.UnEquip - dung la lua chon DUY NHAT khi
        bam vao mon dang mac, xem UI_UIStatus.lua:1896):
            char: C:023-012 <卸下玩家裝備>     +vi tri do(1) +o tui trong(1)      = 0x17 sub0c
            pet : C:023-018 <卸下武將裝備>     +petIdx(1) +vi tri do(1) +o tui(1)  = 0x17 sub12
        O tui trong do CLIENT tu chon roi gui len (server khong tu tim) -> tui day la khong coi
        duoc, phai bao thay vi gui hut.
        """
        o_trong = self.bag_first_empty_slot()
        if o_trong is None:
            log.warning("[%s] Coi do: TUI DAY (%d/%d) -> khong coi duoc",
                        self._label, self.bag_used_slots(), self.bag_capacity())
            return False
        try:
            if follow:
                self.send(0x17, b"\x12\x00" + bytes([follow & 0xFF, fit_pos & 0xFF,
                                                     o_trong & 0xFF]))
            else:
                self.send(0x17, b"\x0c\x00" + bytes([fit_pos & 0xFF, o_trong & 0xFF]))
        except OSError:
            return False
        return True

    def pet_stats(self, slot: int):
        """Chi so cua pet o slot tui 1..4, KE CA con khong xuat chien. None = chua biet.

        -> {"level","hp","hp_max","sp","sp_max","agi"}.
        pet_login_stats von la ham tong quat; truoc day bot chi goi cho con dang xuat chien nen
        khong biet gi ve 3 con con lai (user hoi 26/08: "co the hien thi tat ca chi so khong, ca
        pet khong xuat chien nua").
        """
        rec = (getattr(self, "pet_login_records", None) or {}).get(int(slot))
        if not rec:
            return None
        out = {"level": rec.get("level"), "hp": rec.get("hp"), "sp": rec.get("sp"),
               "hp_max": None, "sp_max": None, "agi": None,
               # GOC tu ban ghi; cong them phan trang bi o duoi.
               "int": rec.get("int"), "atk": rec.get("atk"), "def": rec.get("def"),
               "hpx": rec.get("hpx"), "spx": rec.get("spx")}
        data = _load_pet_stat_data()
        if not data:
            return out
        try:
            flags = getattr(self, "_collect_style_flags", {})
            eq = getattr(self, "_collect_card_equipped", [])
            lv = getattr(self, "_collect_card_levels", {})
            out["hp_max"], out["sp_max"] = pet_login_stats.calculate(
                rec, data,
                style=pet_login_stats.style_bonus(data, flags),
                cards=pet_login_stats.card_bonus(data, eq, lv))
            out["agi"] = pet_login_stats.calculate_agi(
                rec, data,
                style_agi=pet_login_stats.style_attribute(data, flags, 30),
                card_agi=pet_login_stats.card_attribute(data, eq, lv, 30))
            # Int/Atk/Def/Hpx/Spx: ban ghi chi co so GOC (khac goi char - char co san truong
            # Equip* rieng). Cong them phan trang bi bang dung ham client van dung.
            _he = int((getattr(self, "char_attrs", None) or {}).get(24, 0) or 0)
            b = pet_login_stats.equipment_bonus(rec, data, _he)
            for _k, _ma in (("int", self.ATTR_INT), ("atk", self.ATTR_ATK),
                            ("def", self.ATTR_DEF), ("hpx", self.ATTR_HPX),
                            ("spx", self.ATTR_SPX)):
                if out.get(_k) is not None:
                    out[_k] = int(out[_k]) + int(b.get(_ma, 0))
        except Exception as e:
            log.debug("[%s] pet_stats(slot=%s) loi: %s", self._label, slot, e)
        return out

    def bag_slot_maxed(self) -> bool:
        """Bag_1 (103) da mua toi da (value >= max) -> khong mua them slot duoc nua."""
        v, mx = self.role_counts.get(103, (0, 30))
        return mx > 0 and v >= mx

    # `sellId` cua UISell (crack client): 3 = mo o TUI DO (`UIBag.lua:259`), 1 = mo o TIEN TRANG
    # (`UIBank.OnClick_Unlock` -> tab Bank). 56 la tab "Storage" - KHO KHAC, khong dung o day.
    SELL_TUI = 3
    SELL_TIEN_TRANG = 1

    def _on_bag_slot(self, pkt: bytes):
        """S2C 0x54 (mua o tui / o tien trang) - PHAN BIET THEO `sellId`.

        sub01 (bao gia):  [sellId 2B][kind 1B][money 4B LE]
        sub02 (ket qua):  [sellId 2B][result 1B]  (1 = OK)

        TRUOC DAY KHONG XET `sellId`: moi goi `0x54` sub01/sub02 deu ghi vao `_bag_slot_price`.
        Chi co mot loai (tui) thi khong sao, nhung them tien trang la hai loai dung chung mot o nho
        -> gia cua cai nay de len cai kia, bot co the mua nham hoac dung nham nguong.
        """
        sub = pkt[7:9]; body = pkt[9:]
        if len(body) < 2:
            return
        _sell = int.from_bytes(body[0:2], "little")
        if _sell == self.SELL_TIEN_TRANG:
            _gia, _kq = "_bank_slot_price", "_bank_slot_buy_result"
            _gs, _bs = "_bank_slot_price_seq", "_bank_slot_buy_seq"
        elif _sell == self.SELL_TUI:
            _gia, _kq = "_bag_slot_price", "_bag_slot_buy_result"
            _gs, _bs = "_bag_slot_price_seq", "_bag_slot_buy_seq"
        else:
            return          # sellId khac (vd 56 = kho Storage) - khong phai viec cua bot
        if sub == b"\x01\x00" and len(body) >= 7:
            setattr(self, _gia, (int.from_bytes(body[3:7], "little"), body[2]))   # (money, kind)
            setattr(self, _gs, getattr(self, _gs) + 1)
        elif sub == b"\x02\x00" and len(body) >= 3:
            setattr(self, _kq, body[2])
            setattr(self, _bs, getattr(self, _bs) + 1)

    def query_bag_slot_price(self, wait: float = 2.0):
        """Query gia mua slot tui (0x54 sub01 sellId=3). Tra (money, kind) hoac None."""
        seq = self._bag_slot_price_seq
        self.send(0x54, b"\x01\x00\x03\x00")
        t0 = time.time()
        while self._bag_slot_price_seq == seq and time.time() - t0 < wait and self.running:
            time.sleep(0.1)
        return self._bag_slot_price if self._bag_slot_price_seq > seq else None

    def buy_bag_slot(self, wait: float = 2.0) -> bool:
        """Mua 1 slot tui (0x54 sub02 sellId=3). Tra True neu result=1 (thanh cong)."""
        if self.bag_slot_maxed():
            return False
        seq = self._bag_slot_buy_seq
        self.send(0x54, b"\x02\x00\x02\x03\x00")
        t0 = time.time()
        while self._bag_slot_buy_seq == seq and time.time() - t0 < wait and self.running:
            time.sleep(0.1)
        return self._bag_slot_buy_seq > seq and self._bag_slot_buy_result == 1

    def bank_slot_maxed(self) -> bool:
        """Tien trang da mo toi da o (`ERoleCount.Bank_1` = 101, 錢莊開啟格數1)."""
        v, mx = self.role_counts.get(101, (0, 0))
        return mx > 0 and v >= mx

    def query_bank_slot_price(self, wait: float = 2.0):
        """Query gia mo them o TIEN TRANG (0x54 sub01 sellId=1). Tra (money, kind) hoac None."""
        seq = self._bank_slot_price_seq
        self.send(0x54, b"\x01\x00" + struct.pack("<H", self.SELL_TIEN_TRANG))
        t0 = time.time()
        while self._bank_slot_price_seq == seq and time.time() - t0 < wait and self.running:
            time.sleep(0.1)
        return self._bank_slot_price if self._bank_slot_price_seq > seq else None

    def buy_bank_slot(self, wait: float = 2.0) -> bool:
        """Mo them 1 o tien trang (0x54 sub02 sellId=1). Tra True neu result=1."""
        if self.bank_slot_maxed():
            return False
        seq = self._bank_slot_buy_seq
        self.send(0x54, b"\x02\x00\x02" + struct.pack("<H", self.SELL_TIEN_TRANG))
        t0 = time.time()
        while self._bank_slot_buy_seq == seq and time.time() - t0 < wait and self.running:
            time.sleep(0.1)
        return self._bank_slot_buy_seq > seq and self._bank_slot_buy_result == 1

    def tu_mo_rong_tien_trang(self, gioi_han: int) -> int:
        """Mo o tien trang toi khi GIA LAN KE TIEP > `gioi_han`. Tra so o da mo.

        CHI GOI KHI DA MO TIEN TRANG (user chot 10/09: "khi di cat do vao tien trang thi mo rong
        luon vi chi mo rong dc khi mo tien trang") - trong client, nut mo o nam trong chinh UIBank
        (`UIBank.OnClick_Unlock` -> `UISell.Launch(1)`), khong bam duoc tu ngoai.

        Nguong tinh y het tui do: so dien la NGUONG BAO GOM - gia == gioi_han thi VAN MUA, gia >
        gioi_han moi dung.
        """
        gioi_han = int(gioi_han or 0)
        if gioi_han <= 0:
            return 0
        da_mo = 0
        for _ in range(self.BAG_EXPAND_MAX_LAN):
            if not self.running or self.bank_slot_maxed():
                break
            gia = self.query_bank_slot_price()
            if not gia:
                log.info("[%s] Mo rong tien trang: khong hoi duoc gia -> dung", self._label)
                break
            _tien = int(gia[0])
            if _tien > gioi_han:
                log.info("[%s] Mo rong tien trang: lan sau can %d > nguong %d -> DUNG (da mo %d o)",
                         self._label, _tien, gioi_han, da_mo)
                break
            if not self.buy_bank_slot():
                log.warning("[%s] Mo rong tien trang: mo that bai (het tien / da toi da) -> dung",
                            self._label)
                break
            da_mo += 1
            log.info("[%s] Mo rong tien trang: da mo o thu %d (ton %d)", self._label, da_mo, _tien)
            time.sleep(0.4)
        return da_mo

    # Mua slot tui LIEN TUC toi khi gia lan KE TIEP vuot nguong. Cap cung de mot cau hinh sai
    # (vd de nguong khong lo) khong lam bot mua tron doi.
    BAG_EXPAND_MAX_LAN = 30

    def tu_mo_rong_tui(self, gioi_han: int) -> int:
        """Mua slot tui toi khi GIA LAN KE TIEP > `gioi_han`. Tra so slot da mua.

        User chot 02/09: "neu tick thi tu mo rong tui do den khi vang yeu cau lon hon so duoc dien,
        vi du dien 250 thi mua den khi mua xong lan 250 (lan tiep theo can 260 thi dung)".
        Tuc so dien la NGUONG BAO GOM: gia == gioi_han thi VAN MUA, gia > gioi_han moi dung.
        """
        gioi_han = int(gioi_han or 0)
        if gioi_han <= 0:
            return 0
        da_mua = 0
        for _ in range(self.BAG_EXPAND_MAX_LAN):
            if not self.running or self.bag_slot_maxed():
                break
            gia = self.query_bag_slot_price()
            if not gia:
                log.info("[%s] Mo rong tui: khong hoi duoc gia -> dung", self._label)
                break
            _tien = int(gia[0])
            if _tien > gioi_han:
                log.info("[%s] Mo rong tui: lan sau can %d > nguong %d -> DUNG (da mua %d slot, "
                         "tui %d o)", self._label, _tien, gioi_han, da_mua, self.bag_capacity())
                break
            if not self.buy_bag_slot():
                log.warning("[%s] Mo rong tui: mua that bai (het tien / da toi da) -> dung",
                            self._label)
                break
            da_mua += 1
            log.info("[%s] Mo rong tui: da mua slot thu %d (ton %d, tui %d o)",
                     self._label, da_mua, _tien, self.bag_capacity())
            time.sleep(0.4)
        return da_mua

    def _on_offline_exp(self, pkt: bytes):
        """S2C 0x54.
        sub=1: [01 00][type 2B][flag 1B][exp 4B LE] -> neu exp>0 thi gui nhan.
        sub=2: [02 00][type 2B][status 1B] -> status=1: nhan thanh cong.
        """
        if len(pkt) < 11:
            return
        body = pkt[7:]
        sub = int.from_bytes(body[0:2], "little")
        if sub == 0x01 and len(body) >= 9:
            exp_type = int.from_bytes(body[2:4], "little")
            exp = int.from_bytes(body[5:9], "little")
            if exp_type == 0x0d:
                return   # type 0x0d = VE DUNGEON (do do_daily_dungeon xu ly), KHONG phai exp offline
            if exp > 0:
                log.info("[%s] Co %d exp offline (type=0x%x) -> nhan", self._label, exp, exp_type)
                self.send(0x54, b"\x02\x00\x02" + struct.pack("<H", exp_type))
        elif sub == 0x02 and len(body) >= 5:
            status = body[4]
            if status:
                log.info("[%s] Nhan exp offline THANH CONG", self._label)

    def reset_daily_counters_if_needed(self, today=None, now=None) -> bool:
        day = _gift_day(today)
        if self._daily_date == day:
            return False
        previous = self._daily_date
        self._daily_date = day
        self._online_base = 0.0
        self._connect_time = time.time() if now is None else float(now)
        self.claimed_gifts = set()
        self._server_online_seconds = None
        self._server_online_ts = 0.0
        self._bitflag_bytes = bytearray()
        self._bitflags_loaded = False
        self._online_gift_pending = None
        self._online_gift_pending_ts = 0.0
        self._online_gift_next_log = None
        self._online_gift_last_log = 0.0
        self._quest_cells = set()
        self.world_boss_count = None
        self.world_boss_max = WORLD_BOSS_MAX_ATTEMPTS
        self._world_boss_progress_loaded = False
        self._world_boss_progress_ts = 0.0
        self._claimed_lines = set()
        self._claimed_loaded = False
        self.vantieu_started = None
        self.vantieu_slots = {}
        self.vantieu_req_code = None
        self.vantieu_req = None
        self.dungeon_runs_today = None
        self._gift_status = {}
        self._gift_recv = 0
        _save_gift_state(self._label, 0.0, set(), today=day)
        log.info("[%s] DA SANG NGAY MOI %s -> %s: reset daily counters",
                 self._label, previous, day)
        return True

    def _current_online_seconds(self):
        if self._server_online_seconds is None:
            return None
        base = max(0.0, float(self._server_online_seconds))
        if self._server_online_ts <= 0:
            return base
        return base + max(0.0, time.time() - self._server_online_ts)

    def _log_online_gift_wait(self, message, *args, interval=300.0):
        now = time.time()
        if now - self._online_gift_last_log >= interval:
            log.info(message, *args)
            self._online_gift_last_log = now

    def claim_online_gifts(self):
        """Nhan qua online theo state server: RoleCount 10 + BitFlag, khong dem/claim mu."""
        configured = getattr(config, "GIFT_MILESTONES", [])
        flags = _load_online_gift_flags()
        milestones = [int(m) for m in configured if int(m) in flags]
        if not milestones:
            return False
        if self._online_gift_pending is not None:
            if time.time() - self._online_gift_pending_ts < 8.0:
                return False
            log.warning("[%s] Qua online moc %d phut: khong thay server tra loi -> cho vong sau",
                        self._label, self._online_gift_pending)
            self._online_gift_pending = None
            self._online_gift_pending_ts = 0.0
        if not self._bitflags_loaded:
            self._log_online_gift_wait("[%s] Qua online: chua co BitFlag 0x51 tu server -> chua claim",
                                       self._label, interval=300.0)
            return False
        online_sec = self._current_online_seconds()
        if online_sec is None:
            self._log_online_gift_wait("[%s] Qua online: chua co RoleCount 10 tu server -> chua claim",
                                       self._label, interval=300.0)
            return False

        claimed = self._refresh_online_claimed_from_bitflags()
        online_min = online_sec / 60.0
        for m in milestones:
            if m in claimed:
                continue
            if online_sec >= m * 60:
                self._online_gift_pending = m
                self._online_gift_pending_ts = time.time()
                self.send(0x57, b"\x02\x00\x03" + struct.pack("<I", m) + b"\x01")
                log.info("[%s] Nhan qua online moc %d phut (server online=%.1f phut)",
                         self._label, m, online_min)
                return False

            next_state = (m, int(online_sec // 60))
            if self._online_gift_next_log != next_state and time.time() - self._online_gift_last_log >= 60.0:
                wait_min = max(0.0, m - online_min)
                log.info("[%s] Qua online: moc tiep theo %d phut, hien %.1f phut, con %.1f phut",
                         self._label, m, online_min, wait_min)
                self._online_gift_next_log = next_state
                self._online_gift_last_log = time.time()
            return False
        return True

    def _gift_claim(self, gtype: int, day: int, wait: float = 1.5) -> int:
        """Gui 1 goi nhan qua ngay 'day': C2S 0x57 02 00 [gtype] [day 4B LE] 01.
        gtype: 01=diem danh, 04=qua 14 ngay. Tra ve status (0=OK; 2=da nhan; 5=chua toi; -1 ko phan hoi)."""
        self._gift_status[gtype] = None
        self.send(0x57, b"\x02\x00" + bytes([gtype]) + struct.pack("<I", day) + b"\x01")
        t = time.time()
        while time.time() - t < wait:
            if self._gift_status.get(gtype) is not None:
                return self._gift_status[gtype]
            time.sleep(0.1)
        return -1

    def _claim_daily_gift(self, kind: str, gtype: int, max_day: int, name: str, finite: bool = False):
        """Nhan qua theo NGAY (so lan nhan: hom nay day=N -> mai N+1). 1 lan/ngay, tu dem + luu.
        finite=True (vd qua 14 ngay): nhan het max_day thi DUNG han. Status: 0=OK,2=da nhan,5=chua toi."""
        import datetime
        today = datetime.date.today().isoformat()
        st = _load_checkin(self._label, kind)
        if st.get("date") == today:
            return True
        if finite and st.get("day", 0) >= max_day:
            return True   # da nhan het (vd ngay 14) -> khong lam nua
        # 1) Biet so dem -> thu day+1 (binh thuong 1 goi la xong)
        if 0 < st.get("day", 0) < max_day:
            s1 = self._gift_claim(gtype, st["day"] + 1)
            if s1 == 0:
                _save_checkin(self._label, kind, today, st["day"] + 1)
                log.info("[%s] %s ngay %d OK", self._label, name, st["day"] + 1)
                return True
            log.info("[%s] %s ngay %d -> status=%d (0=OK,2=da nhan,5=chua toi,-1=ko phan hoi)",
                     self._label, name, st["day"] + 1, s1)
        # 2) Lan dau / desync -> quet 1..max_day
        last = st.get("day", 0)
        seen2 = False             # co thay ngay nao "da nhan" (status=2) khong
        stats = []                # status tung ngay (de chuan doan khi that bai)
        for d in range(1, max_day + 1):
            s = self._gift_claim(gtype, d)
            stats.append(s)
            if s == 0:
                _save_checkin(self._label, kind, today, d)
                log.info("[%s] %s ngay %d OK (scan)", self._label, name, d)
                return True
            if s == 2:
                last = max(last, d); seen2 = True
        # CHI danh dau "xong hom nay" khi THUC SU co ngay da nhan (status=2).
        # Neu KHONG nhan duoc + KHONG ngay nao da nhan (toan 5/-1/khac) -> KHONG luu today
        # -> lan login sau THU LAI (tranh bug: danh dau xong ma game chua nhan).
        from collections import Counter
        if seen2:
            _save_checkin(self._label, kind, today, last)
            log.info("[%s] %s: da nhan hom nay roi (ngay %d) -> luu", self._label, name, last)
        else:
            log.warning("[%s] %s: KHONG nhan duoc phan nao (status cac ngay: %s) -> KHONG danh dau, "
                        "se thu lai login sau", self._label, name, dict(Counter(stats)))
        return True

    _RC_LOGIN_SIGN_DAY = 107     # ERoleCount.LoginSingDay - so lan DA nhan thuong diem danh

    @task_report("diem danh", PHASE_LOGIN_CHORE)
    def claim_checkin(self):
        """DIEM DANH hang ngay (0x57 type=01) - lam DUNG NHU CLIENT: 1 goi, khong quet.

        Client UI_UILoginAward.lua:1994 gui SendGetAward(Login, RoleCount(107) + 1). RoleCount la
        so SERVER gui (0x55), khong phai dem tay -> khong bao gio lech, khong can scan.
        Chi khi CHUA co RoleCount 107 (goi 0x55 chua ve) moi dung cach cu de khong mat luot.
        """
        import datetime
        today = datetime.date.today().isoformat()
        st = _load_checkin(self._label, "checkin")
        if st.get("date") == today:
            # KHONG thoat im lang: khong co dong nay thi khong biet bot da diem danh hay bo sot.
            log.info("[%s] Diem danh: hom nay da lam roi (ngay %d)", self._label, st.get("day", 0))
            return True
        rc = self.role_counts.get(self._RC_LOGIN_SIGN_DAY)
        if rc is None:
            log.info("[%s] Diem danh: chua co RoleCount %d tu server -> dung cach cu (quet)",
                     self._label, self._RC_LOGIN_SIGN_DAY)
            return self._claim_daily_gift("checkin", 0x01, 40, "Diem danh")
        done = int(rc[0])                       # so ngay DA nhan (theo server)
        s = self._gift_claim(0x01, done + 1)    # y het client: nhan ngay ke tiep
        if s == 0:
            _save_checkin(self._label, "checkin", today, done + 1)
            log.info("[%s] Diem danh ngay %d OK", self._label, done + 1)
        elif s == 2:
            _save_checkin(self._label, "checkin", today, done)
            log.info("[%s] Diem danh: hom nay da nhan roi (server: %d ngay)", self._label, done)
        else:
            log.info("[%s] Diem danh ngay %d -> status=%d (0=OK,2=da nhan,5=chua toi,-1=ko phan hoi)",
                     self._label, done + 1, s)
        return True

    def claim_14day_gift(self):
        """QUA 14 NGAY user moi (0x57 type=04). Nhan het 14 ngay thi dung."""
        return self._claim_daily_gift("gift14", 0x04, 14, "Qua 14 ngay", finite=True)

    # Toan tu so sanh cua dieu kien nhiem vu (EMissionOperator trong Logic_ActivityModel.lua)
    _MISSION_OP = {1: lambda p, c: p == c, 2: lambda p, c: p > c, 3: lambda p, c: p >= c,
                   4: lambda p, c: p < c, 5: lambda p, c: p <= c, 6: lambda p, c: p != c}
    _COND_SPECIFIED_DATE_LOGIN = 31      # EMissionConditionType.SpecifiedDateLogin
    _COND_RECHARGE90, _COND_RECHARGE2990 = 3, 8

    def _mission_claimable(self, m) -> int:
        """So lan CON NHAN DUOC cua 1 muc (0 = khong nhan duoc) - COPY dung logic client.

        Client: Logic_ActivityModel.lua ~424-470 (EMissionType.Complete). Truoc day bot gui bua
        het moi muc roi de server tu choi -> hang chuc goi rac moi lan login.
        """
        mid = int(m["id"])
        if int(m.get("cond", 0)) == 1:       # EMissionType.Exchange -> viec cua do_event_exchange
            return 0
        lim = int(m.get("limit") or 0)
        complete = int(self._activity_done.get(mid, 0))
        get = int(self._activity_got.get(mid, 0))
        cond = int(m.get("cond", 0))
        opr = int(m.get("opr", 0))
        need = int(m.get("need", 0))
        cmp_ = self._MISSION_OP.get(opr, lambda p, c: False)

        get_count = get if lim > 1 else 1
        if cond == self._COND_SPECIFIED_DATE_LOGIN:
            ok = complete > 0
        elif (self._COND_RECHARGE90 <= cond <= self._COND_RECHARGE2990) or lim <= 1:
            ok = cmp_(complete, need * get_count)
        else:
            ok = cmp_(complete, need * (get_count + 1))
        if not ok:
            return 0                          # Processing: chua du dieu kien
        if get >= lim:
            return 1 if lim == 0 else 0       # limit 0 = khong gioi han
        if complete - get > 0:
            return (complete - get) if lim <= 0 else min(complete - get, lim - get)
        return 0 if complete >= lim else 0

    @task_report("qua su kien 14 ngay", PHASE_LOGIN_CHORE)
    def claim_event_14day(self):
        """Event TANG QUA 14 NGAY (opcode 0x7c) - KHAC qua 14 ngay new-user (0x57).
        Mo list (7c 0100) -> server tra cac phan claim duoc (S2C 0x7c sub=01) ->
        nhan tung phan: 7c 03 00 [itemid 4B LE][qty=01000000]. Server tu choi phan chua
        toi ngay (vo hai). Xac nhan tu capture ev14.pcap (nhan ngay 1 = item 0x044d).
        Chay moi login: phan da nhan se khong con trong list nua."""
        self._event14_items = []
        self._event14_ok = 0
        self._event14_acks = []
        self._event14_bagfull = False
        self.send(0x7c, b"\x01\x00")          # mo/query list event
        time.sleep(1.5)                       # cho list ve
        items = list(self._event14_items)
        if not items:
            return
        # BO QUA muc DOI BANG VAT PHAM (cond == 1). Goi nhan qua o day CHINH LA SendGetAward cua
        # client -> bam bua vao muc doi se TU DOI MAT nguyen lieu su kien (Trang/Ngoc...) ma user
        # khong he chon. Muc doi do tinh nang rieng lo (do_event_exchange, theo list user tick).
        by_id = {int(m["id"]): m for a in self._activities.values()
                 for m in (a.get("missions") or ())}
        todo, skipped, unknown = [], 0, 0
        for it in items:
            mid = int.from_bytes(it, "little")
            m = by_id.get(mid)
            if m is None:
                # Mission KHONG thuoc su kien dang mo (thuong la su kien DA HET HAN - S:124-001 van
                # gui tien do cua chung). Client AN han, khong bao gio bam duoc -> bot cung khong gui.
                # Ngoai le: chua nhan duoc S:124-000 => khong biet gi ca -> van thu de khoi mat luot.
                unknown += 1
                if not by_id:
                    todo.append((it, 1))
                continue
            cnt = self._mission_claimable(m)
            if cnt <= 0:
                skipped += 1                   # chua du dieu kien / da nhan het -> KHONG gui
                continue
            todo.append((it, cnt))
        for it, cnt in todo:
            if not self.running or self._event14_bagfull:
                break                          # tui day -> dung luon, khoi thu tiep
            self._evx_last_sent = (0, 0)   # KHONG phai lenh doi qua -> handler khong canh bao
            self.send(0x7c, b"\x03\x00" + it + struct.pack("<I", int(cnt)))
            time.sleep(0.5)
        time.sleep(0.6)
        if self._event14_bagfull:
            log.warning("[%s] Event 14 ngay: KHONG nhan duoc vi TUI DO DAY (server code 06) "
                        "-> Anh don bot tui roi login lai de bot nhan.", self._label)
        else:
            log.info("[%s] Event su kien: gui %d/%d muc (bo qua %d chua du dk/da nhan, "
                     "%d cua su kien da het han), nhan thanh cong %d",
                     self._label, len(todo), len(items), skipped, unknown, self._event14_ok)

    def redeem_giftcode(self, code: str):
        """NHAP GIFTCODE (C2S 0x57 sub=02). Qua thuong ve qua MAIL -> tu claim_mail() nhan.
        Format: 57 [02 00][05][len 1B = so byte UTF16][code UTF16LE][01].
        Xac nhan tu capture gift.pcap (code 'TS1106')."""
        code = (code or "").strip()
        if not code:
            return False
        cb = code.encode("utf-16-le")
        if len(cb) > 255:
            log.warning("[%s] giftcode qua dai", self._label); return False
        self.send(0x57, b"\x02\x00\x05" + bytes([len(cb)]) + cb + b"\x01")
        log.info("[%s] Nhap giftcode '%s'", self._label, code)
        time.sleep(1.2)             # cho server xu ly + day qua vao mail
        self.claim_mail()           # qua giftcode ve mail -> nhan + xoa luon
        return True

    @task_report("qua quan doan", PHASE_LOGIN_CHORE)
    def claim_legion_gift(self):
        """Nhan qua QUAN DOAN hang ngay. C2S 0x27 [69 00] -> server tra reward (0x17).
        1 lan/ngay (daily_state.json). Khong trong quan doan thi vo hai."""
        if _daily_done(self._label, "legion"):
            return
        self.send(0x7c, b"\x04\x00")   # mo panel quan doan
        time.sleep(0.5)
        self.send(0x27, b"\x69\x00")   # nhan qua quan doan
        _mark_daily(self._label, "legion")
        log.info("[%s] Nhan qua quan doan hang ngay", self._label)

    # Thuoc cao cap KHONG dung lam nguyen lieu hop (giu lai de danh boss)
    # restrict (ItemData.lua:346 --[30]) la BITMASK:
    #   1 vut la mat | 2 khong chuyen nhuong | 4 KHONG PHAI NGUYEN LIEU HOP | 8 KHONG THE BI HOP
    #   16 khong ban cho Npc | 32 khong gui ngan hang
    # Client loc item cho HOP bang DUNG bit 4 (UICompound.lua:435):
    #   if bit.band(itemDatas[id].restrict, 4) ~= 0 then return false end
    RESTRICT_NOT_COMBINE_MATERIAL = 4

    def do_combine_item(self):
        """HOP VAT PHAM (nhiem vu bingo o 7): hop 2 do an/thuoc -> ra item random.
        KEY: goi hop dung COMPOUND_ID = 0x0100 + IDX SLOT TUI (idx = vi tri item trong tui chinh,
        chinh la cai bag_slots dung de heal). Vi the cid DONG theo phien (slot doi -> cid doi) - bot
        doc idx LIVE tu bag_slots nen luon dung. Chon 2 do an/thuoc SL it nhat (don stack le), tru
        item battle (hoi sinh) + Huong Dung. C2S 0x17: 0e 00 [cid1 2B] 00 00 00 [cid2 2B] 00*8 01."""
        items = _load_gamedata_items()
        pots = []   # (qty, idx, tid) - do an/thuoc (hp/sp) trong tui
        for idx, (tid, cnt) in self.bag_slots.items():
            if cnt < 1 or idx > 0xFF:
                continue
            info = items.get(tid)
            if not info or (info.get("hp", 0) <= 0 and info.get("sp", 0) <= 0):
                continue
            if info.get("battle"):     # item hoi sinh (Phuc Hon/Tu Quang) - khong hop
                continue
            # Loc theo CO trong data (giong client), KHONG theo TEN: hardcode ten truoc day
            # ("Huong Dung Ma/Dai Duoc") sot "Bo Tay" va moi item moi cua game -> bot gui lenh hop
            # vo ich (user bao: Bo Tay hoi HP nhung KHONG hop duoc).
            if info.get("restrict", 0) & self.RESTRICT_NOT_COMBINE_MATERIAL:
                continue
            pots.append((cnt, idx, tid))
        pots.sort()   # it nhat truoc
        if len(pots) >= 2:                  # 2 loai it nhat (don stack le)
            (_, i1, t1), (_, i2, t2) = pots[0], pots[1]
        elif pots and pots[0][0] >= 2:      # chi 1 loai -> hop 2 cai cua no
            # HOP CUNG MOT O voi chinh no: OK, mien la stack du so luong (user XAC NHAN 25/08).
            # Ghi lai vi nhin code de nghi la sai (client that thi nguoi choi keo HAI stack khac
            # nhau vao bang hop) - da mot lan nghi oan cho no khi truy vu 5 acc rot luc 15:37.
            (_, i1, t1) = pots[0]; i2, t2 = i1, t1
        else:
            log.info("[%s] Hop do: khong du do an/thuoc trong tui de hop", self._label)
            return
        cid1, cid2 = 0x100 + i1, 0x100 + i2
        pkt = (b"\x0e\x00" + struct.pack("<H", cid1) + b"\x00\x00\x00"
               + struct.pack("<H", cid2) + b"\x00" * 8 + b"\x01")
        self.send(0x17, pkt)
        time.sleep(0.5)
        log.info("[%s] Hop vat pham: %s(slot%d) + %s(slot%d)", self._label,
                 items.get(t1, {}).get("name", hex(t1)), i1, items.get(t2, {}).get("name", hex(t2)), i2)

    def _world_boss_event_open(self) -> bool:
        import datetime
        vn_hour = (datetime.datetime.utcnow() + datetime.timedelta(hours=7)).hour
        if not (12 <= vn_hour < 23):          # event boss chi mo 12h-23h (gio VN)
            log.info("[%s] Boss the gioi: ngoai gio event (12h-23h VN, hien %dh) -> bo qua",
                     self._label, vn_hour)
            return False
        return True

    def query_world_boss_attempts(self, timeout: float = 6.0):
        """Tra (cur,max) luot boss the gioi tu MarkManager mission 12207 neu server da sync."""
        if self._world_boss_progress_loaded:
            return self.world_boss_count, self.world_boss_max
        started = time.time()
        deadline = time.time() + max(0.0, timeout)
        while self.running and time.time() < deadline:
            if self._world_boss_progress_loaded and self._world_boss_progress_ts >= started - 0.1:
                return self.world_boss_count, self.world_boss_max
            if self.mission_steps_loaded:
                self._sync_world_boss_from_mission_steps("mission-step cached")
                return self.world_boss_count, self.world_boss_max
            time.sleep(0.2)
        if self._world_boss_progress_loaded:
            return self.world_boss_count, self.world_boss_max
        if self.mission_steps_loaded:
            self._sync_world_boss_from_mission_steps("mission-step cached")
            return self.world_boss_count, self.world_boss_max
        return None

    def _use_world_boss_challenge_item(self) -> bool:
        for slot, (tid, cnt) in sorted(self.bag_slots.items()):
            if tid in WORLD_BOSS_CHALLENGE_TIDS and cnt > 0:
                ok = self.use_slot(slot, target=0)
                if ok:
                    if self.world_boss_max:
                        self._set_world_boss_progress(max(0, self.world_boss_max - 1),
                                                      self.world_boss_max, "Khiêu Chiến Boss")
                    log.info("[%s] Boss the gioi: dung Khiêu Chiến Boss slot=%d 0x%04x -> luot ve %d/%d",
                             self._label, slot, tid, self.world_boss_count, self.world_boss_max)
                    time.sleep(1.0)
                return ok
        return False

    @task_report("boss the gioi", PHASE_LOGIN_CHORE)
    def do_world_boss_all(self, max_loops: int = 20, cho_phep=None) -> bool:
        """Danh boss the gioi den 5/5; neu 5/5 co Khiêu Chiến Boss thi dung ve 4/5 roi danh tiep.

        `cho_phep` = ham dieu phoi cam vao, kiem o DAU MOI VONG LAP. Tran dang danh LUON DANH NOT
        (bo dang la mat luot, mat vat pham) - cai bi chan la mo tran TIEP THEO. Vong nay danh nhieu
        tran lien tiep, moi tran 70-80 giay, va no nam han trong client: acc chui vao day la DIEC
        voi moi lenh dieu phoi suot ca chuoi tran do.

        Ca that 09/09 party 3: ca nam acc trong vong nay tu 12:40 den 12:49 trong khi dieu phoi
        chot kenh dich 16 va dem 45 giay mot lan, muoi bon lan, khong ai nhuc nhich.
        """
        if not self._world_boss_event_open():
            return False
        progress = self.query_world_boss_attempts()
        if progress is None:
            log.info("[%s] Boss the gioi: chua doc duoc so luot server -> bo qua danh het luot",
                     self._label)
            return False
        did_any = False
        loops = 0
        while self.running and loops < max_loops:
            loops += 1
            if cho_phep is not None:
                try:
                    _ly_do = cho_phep()
                except Exception:
                    _ly_do = None
                if _ly_do:
                    log.info("[%s] Boss the gioi: xong tran nay thi DUNG, khong mo tran moi - %s",
                         self._label, _ly_do)
                    break
            cur = self.world_boss_count
            mx = self.world_boss_max or WORLD_BOSS_MAX_ATTEMPTS
            if cur is None:
                log.info("[%s] Boss the gioi: mat progress giua chung -> dung", self._label)
                break
            if cur < mx:
                log.info("[%s] Boss the gioi: dang %d/%d -> danh them 1 luot",
                         self._label, cur, mx)
                ok = self.do_world_boss(heal_after=True)
                if not ok:
                    break
                did_any = True
                if self.world_boss_count is None or self.world_boss_count <= cur:
                    self._set_world_boss_progress(min(mx, cur + 1), mx, "local after battle")
                continue
            if self._use_world_boss_challenge_item():
                continue
            log.info("[%s] Boss the gioi: da du %d/%d va khong co Khiêu Chiến Boss -> hoan thanh",
                     self._label, cur, mx)
            break
        if loops >= max_loops:
            log.warning("[%s] Boss the gioi: dung sau %d vong de tranh lap vo han",
                        self._label, max_loops)
        return did_any

    @_pet_role("boss")
    def do_world_boss(self, heal_after: bool = False):
        """BOSS THE GIOI (nhiem vu o 2): event teleport (0x20 02 00 08) -> map boss 0x2d ->
        engage NPC 0x3232 (0x41) -> VAO 1 tran (combat engine tu danh). CHI CAN VAO TRAN la o2
        mark (khong can thang). Co GIO EVENT -> ngoai gio teleport/engage fail (khong vao tran)
        -> bo qua. Xong thi teleport ve Trac Quan (12001) cho khoi ket map boss. Goi khi o2 chua xong."""
        if not self._world_boss_event_open():
            return False
        orig = self.current_map
        self.flee_mode = True                 # sach tran truoc khi gui goi teleport (tranh kick)
        for _ in range(20):
            if not self.running:
                return False
            if not self.in_combat():
                break
            time.sleep(1)
        if not self.running:
            return False
        time.sleep(1.0)
        self.heal_full(force=heal_after)  # auto full-run ep hoi ky; daily o2 giu hanh vi cu
        self.state.boss_mode = True
        # TAT FLEE NGAY (truoc khi teleport): tran boss co the bat dau ngay luc transit/toi noi ->
        # flee con bat la receiver BO CHAY mat tran. Khu boss chi co boss nen tat flee la an toan.
        self.flee_mode = False
        # (1) MO event boss TRUOC roi moi teleport (replay capture: 0x4d 0x0c -> 0x20 -> 0x14).
        #     Thieu 0x4d/0x0c -> server tu choi teleport (0x14 01002d00) -> tra loi 0x00 code7 -> kick.
        self.send(0x4d, b"\x03\x00\x05\x00");    time.sleep(0.4)   # mo/chon event boss
        self.send(0x0c, b"\x01\x00");            time.sleep(0.4)   # xin info
        self.send(0x20, b"\x02\x00\x08");        time.sleep(0.5)   # chon diem teleport boss
        self.send(0x14, b"\x01\x00\x2d\x00");    time.sleep(0.8)   # teleport map boss 0x2d
        self.send(0x14, b"\x09\x00\x1e");        time.sleep(0.3)
        self.send(0x14, b"\x06\x00");            time.sleep(1.2)
        # (2) engage NPC boss
        self.send(0x41, bytes.fromhex("01003232010100000101000000")); time.sleep(1.0)
        # (3) cho VAO tran (event active?) trong 12s
        entered = False
        t0 = time.time()
        while time.time() - t0 < 12:
            if not self.running:
                self.state.boss_mode = False
                return False
            if self.state.in_battle:
                entered = True
                break
            time.sleep(0.3)
        if not entered:
            log.info("[%s] Boss the gioi: khong vao duoc tran (ngoai gio event?) -> bo qua", self._label)
        else:
            log.info("[%s] Boss the gioi: DA VAO TRAN -> danh CHO HET TRAN", self._label)
            # Cho tran KET THUC THAT. KHONG dat cap thoi gian: ta CO moc ket tran chinh xac
            # (`0x14 sub0700` ha state.in_battle - xem CLAUDE.md), khong phai doan mo.
            # Cap 120s cu CAT GIUA TRAN boss khoe (1 tran thuong o 2K da ~177s) -> boss_mode tat
            # giua chung, an thuoc va teleport khi tran con chay (battle NUOT lenh 0x06/0x14).
            # `self.running` van la duong thoat khi STOP/rot ket noi.
            t0 = time.time()
            while self.running and self.state.in_battle:
                time.sleep(1)
            log.info("[%s] Boss the gioi: tran ket thuc (sau %ds)", self._label, int(time.time() - t0))
        self.state.boss_mode = False
        self._wait_combat_clear()
        self.heal_full(force=True)   # xong world boss -> hoi FULL HP/SP char+pet

        # (4) teleport ve Trac Quan (thanh chung moi server) -> flow train sau do tu route tiep
        if self.running and (orig is None or self.current_map != orig):
            self._wait_combat_clear()
            self.teleport(12001, 0)
            time.sleep(1.5)
        return bool(entered)

    LEGION_BOSS_COOLDOWN = 4 * 3600   # 4h giua cac lan (fallback neu server ko day 0x27 76 moi)
    # Rieng khi VAO INSTANCE THAT BAI (khong vao duoc tran, xem do_legion_boss): cho lau hon 4h
    # thuong - luu ben qua cac lan mo app sau, tranh thu lai qua som roi lai loi/relogin lien tuc.
    LEGION_BOSS_FAIL_COOLDOWN = 12 * 3600

    def legion_boss_available(self) -> bool:
        """Con danh boss QD duoc khong: con luot (count < max) VA het cooldown (server bao). Dung de
        keepalive quyet dinh co trigger REFORM (ve thanh danh) hay khong.

        PHAI khop DIEU KIEN DAU cua do_legion_boss(), khong thi bot doi reform de danh mot thu ma
        chinh no bo qua ngay sau do. Log that 27/08 14:35-14:36: luubhai KHONG CO QUAN DOAN van
        "boss QD den luot -> TRIGGER REFORM" -> reform gen 1, gen 2... con do_legion_boss() thi
        "Boss QD: khong co quan doan -> bo qua hoan toan" -> party bi giai tan/lap lai vo ich.
        """
        if not getattr(self, "fight_legion_boss", True):
            return False
        if self.has_legion is False:       # None = chua biet -> van cho thu
            return False
        return (self.legion_boss_count < self.legion_boss_max
                and (not self.legion_boss_next or time.time() >= self.legion_boss_next))

    @task_report("boss quan doan", PHASE_BOSS_QD)
    @_pet_role("boss")
    def do_legion_boss(self, force=False):
        """BOSS QUAN DOAN: danh SOLO truc tiep (nhu solo dungeon, KHONG teleport nhu world boss).
        3 lan/ngay, cach 4h - SERVER track het:
          - COUNT: 0x55 id 0x2a cur=so lan da danh, max=3 -> self.legion_boss_count/_max. Du max -> nghi.
          - COOLDOWN: 0x27 76 [OLE] = gio danh tiep -> self.legion_boss_next. Con cooldown -> cho.
        Replay capture: 0x27 7700 (start) -> ack -> 0x14 08000100 (vao) -> battle -> do_heal.
        Tra ve GIO CHECK LAI (epoch) neu con luot; None neu het luot hom nay (count>=max)."""
        if not force and not getattr(self, "fight_legion_boss", True):
            # Setting party "Danh boss QD" (Cai dat nang cao) tat -> bo qua hoan toan, KHONG gui
            # goi gi ca (giong nhanh has_legion=False ngay duoi).
            log.info("[%s] Boss QD: dang tat trong che do tu dong -> bo qua", self._label)
            return None
        if force and not getattr(self, "fight_legion_boss", True):
            log.info("[%s] Boss QD: lenh Daily thu cong -> cho phep chay", self._label)
        if self.has_legion is False:
            # KHONG co quan doan (xac nhan qua guild_len=0, xem __init__/_on_player_info) -> BO QUA
            # HOAN TOAN, KHONG gui 0x27 7700/0x14 08000100 vao instance khong hop le voi acc nay.
            # BUG THAT da xac nhan: gui lenh nay khi khong co quan doan lam roi trang thai map/
            # transition phia server, khien enter_di_gioi_safe() ngay sau do THAT BAI du fresh login.
            log.info("[%s] Boss QD: khong co quan doan -> bo qua hoan toan", self._label)
            return None
        if self.legion_boss_count >= self.legion_boss_max:
            log.info("[%s] Boss QD: da danh %d/%d hom nay -> nghi",
                     self._label, self.legion_boss_count, self.legion_boss_max)
            return None                       # het luot hom nay (server bao)
        now = time.time()
        # Doc them gia tri da LUU BEN tu lan chay truoc (reconnect/relogin mat het RAM) - lay
        # gia tri XA HON (server bao hoac da luu) de khong thu lai qua som sau khi vua that bai.
        if self.legion_boss_next and now < self.legion_boss_next:
            log.info("[%s] Boss QD: con cooldown den %s -> chua the vao tran",
                     self._label, time.strftime("%H:%M:%S", time.localtime(self.legion_boss_next)))
            return self.legion_boss_next      # con cooldown (server bao hoac da luu) -> check lai dung luc do
        # --- thu danh 1 luot ---
        self._wait_combat_clear()
        self.heal_full()
        self.state.boss_mode = True
        self.flee_mode = False
        self._legion_start_result = None
        self._legion_enter_result = None
        self._legion_enter_pending = True
        self.send(0x27, b"\x77\x00")                            # start boss QD (0x27 7700)
        _ack_deadline = time.time() + 5.0
        while self.running and self._legion_start_result is None and time.time() < _ack_deadline:
            time.sleep(0.05)
        if not self.running or self._legion_start_result != 1:
            self._legion_enter_pending = False
            self.state.boss_mode = False
            _code = self._legion_start_result
            set_account_activity(self._username,
                                 "Boss QĐ: ACK mở chưa thành công (%s); giữ online" % _code,
                                 phase=PHASE_BOSS_QD)
            log.warning("[%s] Boss QD: ACK raw=%s khong phai 1 -> khong gui lenh vao; "
                        "khong tu gan cooldown 15 phut", self._label, _code)
            return self.legion_boss_next or None
        log.info("[%s] Boss QD: ACK 1=true -> gui lenh vao; van cho server xac nhan battle", self._label)
        _open_ack_seq = getattr(self, "_legion_ack_seq", 0)
        self.send(0x14, b"\x08\x00\x01\x00"); time.sleep(1.0)  # vao instance boss (gate idx 1)
        entered = False
        t0 = time.time()
        while time.time() - t0 < 10:          # cho VAO tran (10s)
            if not self.running:
                self.state.boss_mode = False
                return self.legion_boss_next or now
            if self.state.in_battle:
                entered = True; break
            time.sleep(0.3)
        if not entered:
            # server TU CHOI vao tran (thuong gap nhat: chua du 24h ke tu luc vao quan doan moi
            # duoc danh boss lan dau - KHONG lien quan gi toi dang o Di Gioi hay khong, xay ra
            # BAT KY vi tri nao). BUG THAT xac nhan qua thuc te NHIEU LAN: sau 1 lan thu that bai
            # kieu nay, current_map cuc bo bi SAI VINH VIEN trong suot phien (KHONG tu sua duoc
            # du cho bao lau) -> cac lenh dua vao current_map sau do (vd enter_di_gioi_safe) deu
            # that bai lien tuc. Fix DUY NHAT hieu qua: RELOGIN (dong ket noi + dang nhap lai) ngay
            # de lay lai current_map dung tu goi 0x03 self-spawn MOI, KHONG co gang tu sua cuc bo.
            self.state.boss_mode = False
            self._legion_enter_pending = False
            log.warning("[%s] Boss QD: không vào trận; giữ online, không đoán count/cooldown", self._label)
            return self.legion_boss_next or None
        self._legion_enter_pending = False
        log.info("[%s] Boss QD: DA VAO TRAN -> danh cho het tran", self._label)
        # Nhu boss the gioi: cho moc ket tran THAT (0x14 sub0700), khong dem gio.
        t0 = time.time()
        while self.running and self.state.in_battle:
            set_account_activity(self._username, "boss QD: dang danh tran %ds" % int(time.time() - t0),
                                 phase="boss_qd")
            time.sleep(1)
        log.info("[%s] Boss QD: tran ket thuc (sau %ds)", self._label, int(time.time() - t0))
        log.info("[%s] Boss QD probe sau tran: ACK mo gan nhat=%s; co ACK 7700 moi trong tran=%s; "
                 "khong coi ACK cu la response moi", self._label, self._legion_start_result,
                 getattr(self, "_legion_ack_seq", 0) > _open_ack_seq)
        self.state.boss_mode = False
        self._wait_combat_clear()
        self.heal_full(force=True)            # xong boss QD -> hoi FULL HP/SP char+pet
        # tang count trong phien (server day 0x55 id 0x2a moi luc login/update se ghi de = chuan);
        # server day 0x27 76 (cooldown moi) luc ket tran -> legion_boss_next tu cap nhat, fallback.
        # Count/cooldown chỉ cập nhật từ server.
        log.info("[%s] Boss QD: xong luot %d/%d -> luot sau luc %s",
                 self._label, self.legion_boss_count, self.legion_boss_max,
                 time.strftime("%H:%M", time.localtime(self.legion_boss_next)))
        return None if self.legion_boss_count >= self.legion_boss_max else self.legion_boss_next

    # Nhiem vu hang ngay BINGO 3x3: 9 o (1..9). Du 1 HANG hoac COT (3 o) -> 1 qua; du 6 qua -> 1 qua
    # TONG KET. Line id: hang R1-3=1-3, cot C1-3=4-6, tong ket=7. Reward id = 0x2f + line-1.
    _Q_LINES = {1: (1, 2, 3), 2: (4, 5, 6), 3: (7, 8, 9),      # 3 hang
                4: (1, 4, 7), 5: (2, 5, 8), 6: (3, 6, 9)}      # 3 cot
    _Q_OPEN = bytes.fromhex(
        "0200090100012f0001000230000100033100010004320001000533000100063400010007350001000836000100")

    def _q_mission_id(self, cell: int) -> int:
        """missionId cua o `cell`: uu tien bang S:91-1 (nguon client that), fallback 0x2e+cell."""
        return self._quest_missions.get(cell, 0x2e + cell)

    def pet_usable_skills(self, pet_id: int) -> list:
        """Skill con pet nay THUC SU dung duoc = 3 skill thuong + DAC KY (neu DA MO).

        Y het client (Controller/RoleController.lua:4786):
            if self.data.specialSkillLearned and skillDatas[npcDatas[id].specialSkill] ~= nil
        -> phai DU CA HAI: co ID dac ky trong bang (config.PET_SPECIAL_SKILL, sinh boi
        tools/crack_npc_special_skill.py) VA co da mo cua CHINH con do (self.pet_special_skill,
        doc tu goi pet list / S:020-049). Chua mo ma cu gui la server tu choi.
        """
        pid = int(pet_id)
        out = list(getattr(config, "PET_SKILLS", {}).get(pid) or [])
        if not self.pet_special_skill.get(pid):
            return out
        _sp = (getattr(config, "PET_SPECIAL_SKILL", {}) or {}).get(pid)
        if not _sp or _sp in out:
            return out
        # CHI dua vao khi bot CO DU LIEU skill do (SP ton bao nhieu, dame hay support, danh may o).
        # Thieu ma van dua vao -> combat chon mu: khong biet cost/splash nen tinh sai luot, co the
        # gui skill khong dung duoc. Hien skills_data.json moi phu 43/661 dac ky (Skill_C.dat tren
        # may nay chua co ban day du) -> con lai TU DONG duoc dung khi ai do chay lai
        # tools/crack_skills.py voi file .dat moi, KHONG phai sua code.
        if _sp not in (getattr(config, "SKILL_INFO", {}) or {}):
            if _sp not in getattr(self, "_sp_skill_warned", ()):
                if not hasattr(self, "_sp_skill_warned"):
                    self._sp_skill_warned = set()
                self._sp_skill_warned.add(_sp)
                log.warning("[%s] pet 0x%04x DA MO dac ky %d nhung skills_data.json chua co du lieu "
                            "-> chua dam dung (chay lai tools/crack_skills.py de bo sung)",
                            self._label, pid, _sp)
            return out
        out.append(_sp)
        return out

    def mark_flag_get(self, bit_id: int) -> bool:
        """CheckFlag(MarkManager.flags, bitId) - co NHIEM VU da hoan thanh chua.

        CHI SO 1-BASED, y het functions.lua CheckFlag:
            tableIndex = (flagIndex - 1) // 8 + 1
            bit trong byte = (flagIndex - 1) % 8
        Khoa cua self.mark_flags la chi so byte SERVER GUI o S:024-007, va client luu nguyen
        (`this.flags[index] = ReadByte()`) roi tra bang tableIndex tren -> khoa do la 1-BASED.

        BUG DA SUA (21/08): truoc day dung `bitId // 8` va `bitId % 8` (0-based) -> SAI CA khoa
        byte LAN vi tri bit, doc nham sang co cua nhiem vu KHAC. Hau qua: bot tuong nhiem vu da
        xong -> gui C:082-001 bao hoan thanh thanh tuu -> server tu choi "dieu kien khong du"
        (log that: id 204/300/336/388/390, deu kind=15 MissionFlag, 5 cai lien tiep bi tu choi).
        Chu thich cu ghi "giong BitFlag 0x51" nhung code lai KHAC _bitflag_get - chinh
        _bitflag_get moi la ban lam DUNG.
        """
        if not bit_id:
            return False
        b = int(bit_id) - 1
        if b < 0:
            return False
        return bool(self.mark_flags.get(b // 8 + 1, 0) & (1 << (b % 8)))

    def city_unlocked(self, city_id: int):
        """Thanh nay DA MO teleport chua? True/False, None = chua biet (chua nhan co nhiem vu).

        SAO DUNG client (UI_UITeleport.lua:656 SetupSkyPointData):
            if warpDatas[i].mark ~= 0
               and CheckFlag(MarkManager.flags, markDatas[warpDatas[i].mark].bitId)
        Tuc moi diem dich chuyen co MOT co nhiem vu; co co do = da mo. Client BIET TRUOC, khong
        he thu roi doan nhu bot dang lam ("tele mu, khong di duoc thi moi doan la chua mo").

        Tra None khi CHUA CO du lieu co (mark_flags rong luc vua login) - caller phai phan biet
        "chua biet" voi "chua mo", khong duoc coi None la False roi bo qua thanh do oan.
        """
        if not getattr(self, "_mark_flags_loaded", False) or not self.mark_flags:
            return None
        for w in _load_warp_points():
            if int(w.get("scene", 0)) != int(city_id):
                continue
            mark = int(w.get("mark", 0) or 0)
            if not mark:
                return False        # mark = 0: client coi la KHONG BAO GIO hien trong danh sach
            return self.mark_flag_get(_load_mark_bitids().get(mark, 0))
        return None                 # khong co trong bang warp -> khong phai thanh teleport

    def _on_mission_steps(self, pkt: bytes):
        """S2C 0x18 mission-step. UI phó bản đội lấy còn lượt từ dayilyFlag trong bảng này."""
        if len(pkt) < 9:
            return
        body = pkt[7:]
        sub = int.from_bytes(body[:2], "little")
        try:
            if sub == 0x06 and len(body) >= 6:
                count = int.from_bytes(body[2:6], "little")
                off = 6
                steps = {}
                for _ in range(count):
                    if off + 4 > len(body):
                        break
                    _idx = body[off]
                    mid = int.from_bytes(body[off + 1:off + 3], "little")
                    step = body[off + 3]
                    if mid == WORLD_BOSS_MISSION_ID:
                        step = min(WORLD_BOSS_MAX_ATTEMPTS, step)
                    steps[mid] = step
                    off += 4
                self.mission_steps = steps
                self.mission_steps_loaded = True
                self._sync_world_boss_from_mission_steps("S24-6")
            elif sub == 0x07 and len(body) >= 4:
                # S:024-007 INIT co nhiem vu: [count u16] << [byteIndex u16][gia tri 1B] >>
                cnt = int.from_bytes(body[2:4], "little")
                off = 4
                flags = {}
                for _ in range(cnt):
                    if off + 3 > len(body):
                        break
                    flags[int.from_bytes(body[off:off + 2], "little")] = body[off + 2]
                    off += 3
                self.mark_flags = flags
                self._mark_flags_loaded = True
                log.debug("[%s] co nhiem vu: nhan %d byte co (S24-7)", self._label, len(flags))
            elif sub == 0x05 and len(body) >= 6:
                # S:024-005 CAP NHAT co: [count u32] << [bitIndex u16][gia tri 1B] >>
                # LUU Y: sub 05 dung CHI SO BIT (SetMissionFlag(bitIndex,...)), khac sub 07 dung
                # chi so BYTE -> phai set dung 1 bit, khong ghi de ca byte.
                cnt = int.from_bytes(body[2:6], "little")
                off = 6
                for _ in range(cnt):
                    if off + 3 > len(body):
                        break
                    bit = int.from_bytes(body[off:off + 2], "little")
                    val = body[off + 2]
                    off += 3
                    # 1-BASED nhu SetFlag trong functions.lua (xem mark_flag_get). Dung
                    # 0-based thi bit ghi vao mot noi, doc ra mot neo.
                    bidx, mask = (bit - 1) // 8 + 1, 1 << ((bit - 1) % 8)
                    cur = self.mark_flags.get(bidx, 0)
                    self.mark_flags[bidx] = (cur | mask) if val else (cur & ~mask)
            elif sub == 0x08 and len(body) >= 16:
                # S:024-008 <設定衰神福神> [roleId i64][kind u16][count i32]. count = SO LUOT
                # Phuc Than CON LAI (client: Role.player.data.godMission) -> dung de biet buff
                # con hay het thay vi nha item mu moi 30p.
                # LUU Y: chu thich trong Lua ghi "+種類(1) +次數(1)" NHUNG CODE doc ReadUInt16 +
                # ReadInt32 -> tin CODE.
                _rid = body[2:10]
                if self.self_entity is None or _rid == self.self_entity:
                    self.god_mission = int.from_bytes(body[12:16], "little", signed=True)
                    log.info("[%s] Phuc Than con lai: %s (kind=%d)", self._label, self.god_mission,
                             int.from_bytes(body[10:12], "little"))
                    if self.god_mission < PHUC_THAN_LOW:
                        self.phuc_than_pending = True   # dung item NGAY, khong cho het chu ky
            elif sub in (0x01, 0x02) and len(body) >= 5:
                mid = int.from_bytes(body[2:4], "little")
                step = body[4]
                old = int(self.mission_steps.get(mid, 0))
                if sub == 0x01:
                    new = min(255, old + step)
                    if mid == WORLD_BOSS_MISSION_ID:
                        new = min(WORLD_BOSS_MAX_ATTEMPTS, new)
                    self.mission_steps[mid] = new
                else:
                    new = max(0, old - step)
                    if new:
                        self.mission_steps[mid] = new
                    else:
                        self.mission_steps.pop(mid, None)
                self.mission_steps_loaded = True
                if mid == WORLD_BOSS_MISSION_ID:
                    self._sync_world_boss_from_mission_steps(f"S24-{sub}")
            elif sub == 0x04 and len(body) >= 4:
                mid = int.from_bytes(body[2:4], "little")
                self.mission_steps.pop(mid, None)
                self.mission_steps_loaded = True
                if mid == WORLD_BOSS_MISSION_ID:
                    self._sync_world_boss_from_mission_steps("S24-4")
        except Exception as e:
            log.debug("[%s] bo qua loi parse 0x18 mission-step: %s", self._label, e)

    def wait_mission_steps(self, timeout: float = 6.0) -> bool:
        deadline = time.time() + max(0.0, timeout)
        while self.running and not self.mission_steps_loaded and time.time() < deadline:
            time.sleep(0.2)
        return bool(self.mission_steps_loaded)

    def team_dungeon_remaining(self, level: int):
        info = TEAM_DUNGEONS.get(int(level))
        if not info:
            return None
        if not self.mission_steps_loaded:
            return None
        used = int(self.mission_steps.get(info["daily_flag"], 0))
        return max(0, int(info["daily_count"]) - used)

    def solo_dungeon_remaining(self):
        """Con MAY LUOT FREE pho ban don. None = chua co bang mission-step (chua ket luan duoc).

        Dung dung cong thuc client (`UI/UIDungeon.lua:911-920`):
            conLai = dayilyCount - MarkManager.missions[dayilyFlag].step + VIP(Dungeon)
        Client an han nut VAO khi `conLai == 0` va chi hien nut MUA - tuc no BIET TRUOC, khong thu
        roi doan nhu bot van lam (24 dong `vao FREE that bai` trong 10 phut, log 15/09).
        Bo qua phan VIP: bot khong nap VIP nen coi nhu 0 - lech ve phia AN TOAN (mua ve thay vi
        tuong con free)."""
        if not self.mission_steps_loaded:
            return None
        used = int(self.mission_steps.get(SOLO_DUNGEON_DAILY_FLAG, 0))
        return max(0, SOLO_DUNGEON_DAILY_COUNT - used)

    def _query_quests(self):
        """Mo panel nhiem vu (C2S 0x5b 02 00 09...) -> server tra o nao DA HOAN THANH
        (S2C 0x5b 02 00 01 01 00 [cell] -> handler nhet vao self._quest_cells).
        KHONG reset _quest_cells o day -> TICH LUY qua nhieu lan query (frame status TO 208B co the
        chi ve o lan mo panel DAU; query lan 2 reset se mat -> thieu o nhu o9). Reset o claim_daily_quests."""
        if self._quest_missions:    # co bang mission THAT tu S:91-1 -> build bulk dong
            _q = b"\x02\x00\x09" + b"".join(
                b"\x01\x00" + bytes([c]) + struct.pack("<H", self._q_mission_id(c))
                for c in range(1, 10))
        else:
            _q = self._Q_OPEN
        self.send(0x5b, _q)
        time.sleep(1.5)             # cho server gui status 9 o (bulk)
        # O9 (battle-50, quest DEM) trong bulk LUON tra 020003 (ko ro done) -> QUERY RIENG o9
        # (id 0x37): server tra 020001010009 neu DA xong -> handler bat. Chua xong: 020003/020004.
        self.send(0x5b, b"\x02\x00\x01\x01\x00\x09" + struct.pack("<H", self._q_mission_id(9)))
        time.sleep(0.9)
        return self._quest_cells

    # ---------- DOI THUONG SU KIEN (S:124-xxx = opcode 0x7c) ----------
    # Server GUI TOAN BO danh sach doi (khong nam trong file data nao) -> bot chi can doc goi:
    #   S:124-000 <cap nhat mau hoat dong> [so hoat dong u32] + moi hoat dong:
    #       id u8, open u8, kind u8, banner u8, rolePic u8, sort u8, hintStringId u32,
    #       ten(1+n), mo ta(1+n), soTrang u32 + moi trang: trang u8, ten(1+n), mo ta(1+n),
    #       4 x thoi gian (double OLE), soTien u32 + moi: coinId u16,
    #       soMuc u32 + moi MUC DOI:
    #           missionId u32, order u16, conditionId u8, opr u8, conditionCount u32, getLimit u8,
    #           NHAN: 5 x (kind u8, itemId u16, quant u32),  MAT: 2 x (kind u8, itemId u16, quant u32),
    #           discount u8
    #   S:124-001 [missionId u32][so lan DA LAM u32]     S:124-002 [missionId u32][so lan DA DOI u32]
    #   C:124-003 [missionId u32][so luong u32]  = GUI DOI
    # (Layout boc tu Logic_ActivityModel.GetData + chu thich protocol; da test tren 3 capture:
    #  dung DUNG 725/725 byte, ra "Tam Quoc Do x100 + Dong x5000 -> The Luu Bi x1".)
    # EVENT DOI THEO THANG -> KHONG hardcode gi: doc duoc bao nhieu ghi bay nhieu ra cache JSON de
    # GUI hien danh sach cho user tick.
    _EXCHANGE_CACHE = "event_exchange.json"
    # Ma ket qua doi qua su kien (S:124-003), lay NGUYEN VAN tu client Lua.
    _EXCHANGE_RESULT = {
        0: "thanh cong", 1: "khong co hoat dong nay", 2: "chua mo nhan thuong",
        3: "gui qua nhanh (server chan)", 4: "khong tim thay muc doi",
        5: "VUOT QUA so lan doi cho phep", 6: "TUI DO KHONG DU CHO",
        7: "khong du dieu kien", 8: "KHONG DU vat pham de doi",
        9: "tru vat pham/tien that bai", 255: "hoat dong chua mo",
    }

    def _on_activity_model(self, pkt: bytes):
        body = pkt[7:]
        if len(body) < 2:
            return
        sub, data = body[:2], body[2:]
        try:
            if sub == b"\x00\x00":
                self._parse_activity_list(data)
            elif sub in (b"\x01\x00", b"\x02\x00") and len(data) >= 4:
                # S:124-001 (so lan DA LAM dieu kien) / S:124-002 (so lan DA NHAN/DA DOI):
                #   [count u32] + count x ([missionId u32][so lan u32])  <- CO COUNT o dau!
                # (Truoc day doc data[0:4] lam missionId = doc trung vao COUNT -> sai het.)
                store = self._activity_done if sub == b"\x01\x00" else self._activity_got
                off, cnt = 4, int.from_bytes(data[0:4], "little")
                for _ in range(cnt):
                    if off + 8 > len(data):
                        break
                    store[int.from_bytes(data[off:off + 4], "little")] = int.from_bytes(
                        data[off + 4:off + 8], "little")
                    off += 8
            elif sub == b"\x03\x00" and len(data) >= 4:
                # S:124-003 KET QUA doi: [count u32] + count x [ket qua 1B]. Server KHONG kem
                # missionId -> doi chieu voi lenh vua gui (_evx_last_sent).
                cnt = int.from_bytes(data[0:4], "little")
                codes = list(data[4:4 + cnt])
                ok = sum(1 for c in codes if c == 0)
                self._evx_result = (ok, codes)      # do_event_exchange dang cho ket qua nay
                mid, _n = getattr(self, "_evx_last_sent", (0, 0))
                if not mid:
                    # Goi 0x7c sub03 con duoc claim_event_14day dung (nhan qua 14 ngay). "Da nhan
                    # roi"/"chua toi ngay" o do la BINH THUONG -> khong keu, ham do tu tong ket.
                    return
                if ok == len(codes) and codes:
                    log.debug("[%s] doi muc %d: %d/%d OK", self._label, mid, ok, len(codes))
                elif codes:
                    bad = {}
                    for c in codes:
                        if c:
                            bad[c] = bad.get(c, 0) + 1
                    log.warning("[%s] DOI QUA muc %d: %d/%d THANH CONG - loi: %s",
                                self._label, mid, ok, len(codes),
                                ", ".join("%dx %s" % (v, self._EXCHANGE_RESULT.get(k, "ma %d" % k))
                                          for k, v in sorted(bad.items())))
            elif sub == b"\x0a\x00" and len(data) >= 4:
                # S:124-010 DINH NGHIA tien su kien: [count u32] + [coinId u16][ten(1+n)][icon u16]
                off, cnt = 4, int.from_bytes(data[0:4], "little")
                for _ in range(cnt):
                    if off + 3 > len(data):
                        break
                    cid = int.from_bytes(data[off:off + 2], "little"); off += 2
                    ln = data[off]; off += 1
                    self._coin_names[cid] = data[off:off + ln].decode("utf-16-le", "replace").rstrip("\x00")
                    off += ln + 2
            elif sub == b"\x0b\x00" and len(data) >= 4:
                # S:124-011 SO LUONG tien su kien CUA NGUOI CHOI: [count u32] + [coinId u16][quant u32]
                off, cnt = 4, int.from_bytes(data[0:4], "little")
                for _ in range(cnt):
                    if off + 6 > len(data):
                        break
                    cid = int.from_bytes(data[off:off + 2], "little")
                    self._coin_quant[cid] = int.from_bytes(data[off + 2:off + 6], "little")
                    off += 6
        except Exception as e:
            log.debug("[%s] bo qua loi parse 0x7c activity: %s", self._label, e)

    def _res_name(self, kind: int, res_id: int) -> str:
        """Ten nguyen lieu: kind 1 = TIEN SU KIEN (ten tu S:124-010), kind 2 = vat pham TUI."""
        if int(kind) == 1:
            return self._coin_names.get(int(res_id)) or ("coin %d" % res_id)
        return (_load_gamedata_items().get(int(res_id)) or {}).get("name", "") or ("item %d" % res_id)

    def _res_have(self, kind: int, res_id: int) -> int:
        """So luong DANG CO: tien su kien doc S:124-011, vat pham doc tui (bag_counts)."""
        if int(kind) == 1:
            n = int(self._coin_quant.get(int(res_id), 0))
        else:
            n = int((getattr(self, "bag_counts", {}) or {}).get(int(res_id), 0))
        # Tru phan DA GUI LENH DOI trong phien nay ma server chua kip tru vao tui. Khong co so
        # sach nay thi lan lap sau doc so CU -> lap ke hoach thua -> ngoc trung gian ket trong tui.
        return n + int((getattr(self, "_evx_spent", {}) or {}).get((int(kind), int(res_id)), 0))

    def _parse_activity_list(self, d: bytes):
        off = 0

        def u8():
            nonlocal off
            v = d[off]; off += 1; return v

        def u16():
            nonlocal off
            v = int.from_bytes(d[off:off + 2], "little"); off += 2; return v

        def u32():
            nonlocal off
            v = int.from_bytes(d[off:off + 4], "little"); off += 4; return v

        def skip(n):
            nonlocal off
            off += n

        def text():
            nonlocal off
            n = u8()
            v = d[off:off + n].decode("utf-16-le", "replace").rstrip("\x00")
            off += n
            return v

        acts = []
        for _ in range(u32()):
            a = {"id": u8(), "open": u8(), "kind": u8()}
            skip(3); skip(4)                       # banner/rolePic/sort + hintStringId
            a["title"] = text(); a["desc"] = text()
            for _p in range(u32()):
                u8(); text(); text()               # trang: so + ten + mo ta
            _t = []
            for _i in range(4):                    # batDau / ketThuc / hienTu / hienDen (OLE double)
                _t.append(struct.unpack_from("<d", d, off)[0]); off += 8
            a["time"] = _t
            a["coins"] = [u16() for _ in range(u32())]
            a["missions"] = []
            for _m in range(u32()):
                m = {"id": u32(), "order": u16(), "cond": u8()}
                u8()                               # operator
                m["need"] = u32(); m["limit"] = u8()
                m["award"] = [(u8(), u16(), u32()) for _ in range(5)]
                m["cost"] = [(u8(), u16(), u32()) for _ in range(2)]
                u8()                               # discount
                a["missions"].append(m)
            acts.append(a)
        if not acts:
            return
        # LOC GIONG CLIENT (Logic_ActivityModel.GetData -> isCurrentActivity):
        #   now < ketThuc-hien  VA  isOpen != 0  VA  (now >= batDau-hien HOAC now >= batDau)
        # Server VAN gui ca su kien DA HET HAN (client an di) -> khong loc thi cache day muc doi cu.
        import datetime as _dt
        _now = _dt.datetime.now()
        _live = []
        for a in acts:
            t = a.get("time") or [0, 0, 0, 0]
            try:
                start, _end, show_from, show_to = (self._ole_to_dt(x) for x in t)
                a["active"] = bool(a["open"]) and _now < show_to and (_now >= show_from or _now >= start)
                a["until"] = show_to.strftime("%Y-%m-%d %H:%M")
            except Exception:
                a["active"] = bool(a["open"])
                a["until"] = ""
            if a["active"]:
                _live.append(a)
            else:
                # Nho lai de _save_exchange_cache biet su kien nay THAT SU het, duoc phep xoa khoi
                # file cache (phan biet voi "phien nay chua nhan duoc goi cua no").
                self._acts_expired.add(str(a["id"]))
                log.info("[%s] su kien '%s' (id=%d) DA HET HAN/chua mo -> bo qua (giong client an di)",
                         self._label, a["title"], a["id"])
        acts = _live
        if not acts:
            return
        # GOP, KHONG ghi de: server gui NHIEU goi 124-000 (moi su kien 1 goi) -> gan de thi chi con
        # su kien cuoi, mat cac su kien truoc (da dinh khi test capture: mat 'Doi Thuong', chi con
        # 'Mung Update').
        self._activities.update({a["id"]: a for a in acts})
        self._save_exchange_cache(list(self._activities.values()))
        for a in acts:
            log.info("[%s] SU KIEN '%s' (id=%d, %s): %d muc doi",
                     self._label, a["title"], a["id"],
                     "DANG MO" if a["open"] else "dong", len(a["missions"]))

    def _save_exchange_cache(self, acts):
        """Ghi danh sach doi ra JSON de GUI hien cho user TICK (giong list lo/nguyen lieu).

        Danh sach nay CHI co khi acc dang nhap (server gui) nen khong the ship san trong repo ->
        bot chay 1 lan la GUI co du lieu; EVENT DOI THANG SAU thi file tu cap nhat theo.
        """
        try:
            from ._appdir import app_dir
            items = _load_gamedata_items()

            def _nm(i):
                return (items.get(int(i)) or {}).get("name", "") or ("item %d" % i)

            out = {}
            for a in acts:
                ms = []
                for m in a["missions"]:
                    # CHI ghi phan DOI BANG VAT PHAM (client: conditionId == EMissionConditionType
                    # .Exchange = 1 -> EMissionType.Exchange). Cac muc khac la "hoan thanh dieu kien"
                    # (diem danh / tich luy ngay dang nhap...) - BOT DA TU LAM LAU NAY qua claim_checkin
                    # / claim_event_14day, khong can ghi ra file cho user tick.
                    if int(m.get("cond", 0)) != 1:
                        continue
                    # `kind`: 1 = TIEN SU KIEN (doc _coin_quant), 2 = vat pham TUI (bag_counts)
                    cost = [{"item": i, "kind": k, "name": self._res_name(k, i), "quant": q}
                            for k, i, q in m["cost"] if i]
                    award = [{"item": i, "kind": k, "name": self._res_name(k, i), "quant": q}
                             for k, i, q in m["award"] if i]
                    # KHONG ghi so lan DA DOI vao day: do la so RIENG TUNG ACC (_activity_got),
                    # ma file nay DUNG CHUNG cho moi acc -> acc ghi sau se de len acc truoc.
                    # (client: getLimit == 0 = KHONG GIOI HAN; get >= getLimit = het luot.)
                    ms.append({"id": m["id"], "cond": m["cond"], "limit": m["limit"],
                               "need": m["need"], "cost": cost, "award": award})
                if not ms:
                    continue          # su kien khong co muc doi nao (chi diem danh) -> bo qua han
                out[str(a["id"])] = {"title": a["title"], "kind": a["kind"],
                                     "open": bool(a["open"]), "until": a.get("until", ""),
                                     "missions": ms}
            path = os.path.join(app_dir(), self._EXCHANGE_CACHE)
            # GOP voi noi dung DANG CO trong file: goi nay co the moi mang 1 su kien, cac su kien
            # khac chi la CHUA NHAN DUOC goi chu khong phai da het. Chi bo nhung cai server bao
            # het han (_acts_expired) -> khong con canh ghi file rong roi ghi lai.
            try:
                with open(path, encoding="utf-8") as fh:
                    for k, v in (json.load(fh).get("activities") or {}).items():
                        if k not in out and k not in self._acts_expired:
                            out[k] = v
            except Exception:
                pass
            payload = {"_note": "AUTO-SINH khi bot nhan S:124-000 (doi thuong su kien). GUI doc file"
                                " nay de hien danh sach cho user tick. Su kien doi -> tu cap nhat."
                                " CHI chua phan DUNG CHUNG (danh sach/gia/thuong/gioi han); so lan"
                                " DA DOI la rieng tung acc nen khong ghi o day.",
                       "activities": out}
            # CHI GHI KHI NOI DUNG DOI. Moi acc login deu nhan goi nay (39 party x 5 acc = ~200 luot
            # ghi de moi dot login) -> so sanh truoc, giong thi thoi. Nho vay file chi doi khi SU KIEN
            # THAY DOI that (dung luc user can biet).
            try:
                with open(path, encoding="utf-8") as fh:
                    if json.load(fh).get("activities") == out:
                        return
            except Exception:
                pass
            tmp = path + ".tmp"
            with open(tmp, "w", encoding="utf-8") as fh:
                json.dump(payload, fh, ensure_ascii=False, indent=1)
            os.replace(tmp, path)
            log.info("[%s] cap nhat %s: %s", self._label, self._EXCHANGE_CACHE,
                     ", ".join("%s (%d muc doi)" % (v["title"], len(v["missions"]))
                               for v in out.values()))
        except Exception as e:
            log.debug("[%s] khong ghi duoc %s: %s", self._label, self._EXCHANGE_CACHE, e)

    def run_event_pre_dungeon(self) -> None:
        """DOI QUA (theo tick) -> NHAN THUONG BANG 3x3 -> (caller danh PB TO DOI).

        Dung thu tu user yeu cau. Goi o DAU _run_auto_team_dungeons_if_needed nen ap dung cho MOI
        mode (train/di gioi/event...), khong phu thuoc claim_daily_quests co chay hay khong.
        CHAY 1 LAN moi phien dang nhap: PB to doi duoc goi tu 8 cho khac nhau.
        """
        if getattr(self, "_evx_ran", False):
            return
        self._evx_ran = True
        if getattr(self, "auto_event_exchange", False):
            try:
                self.do_event_exchange(getattr(self, "event_exchange_items", None))
            except Exception as e:
                log.warning("[%s] loi doi qua su kien (bo qua): %s", self._label, e)
        try:
            self.claim_event_boards()
        except Exception as e:
            log.warning("[%s] loi claim bang su kien (bo qua): %s", self._label, e)

    @task_report("doi qua su kien", PHASE_LOGIN_CHORE)
    def do_event_exchange(self, picks, wait: float = 1.2) -> int:
        """TU DOI QUA SU KIEN theo danh sach user tick (CHI qua CUOI, xem event_exchange.py).

        picks: iterable id qua cuoi (hoac "kind:id"). Voi moi qua: TRUY NGUOC ca chuoi nguyen lieu
        (Trang -> Ngoc -> Thien Thu -> qua), tru so DANG CO va so LUOT DA DOI; du HET chuoi moi bat
        dau doi tu nguyen lieu GOC len. Thieu bat ky tang nao -> khong doi gi (tranh doi ra nguyen
        lieu trung gian roi ket, chiem slot tui).
        `wait` = GIAN CACH giua 2 lenh doi. Server chan theo KHOANG CACH THOI GIAN (ma 3
        "太頻繁" = gui qua nhanh), KHONG theo so luong trong goi. Moi qua = 4 lenh lien tiep
        (3 nguon ra ngoc + 1 doi qua) nen de 0.6s van dinh chan (log 17:32) -> 1.2s. Co che thu lai
        khi dinh ma 3 chi la luoi do phong server doi nguong.
        """
        # Khoa tick: "kind:item:missionId" (MOI - dung 1 MUC doi cu the) hoac "kind:item" (CU -
        # bat ky muc nao ra vat pham do). Giu ca 2 de config cu khong vo.
        want = set()
        for p in (picks or ()):
            try:
                if isinstance(p, str) and ":" in p:
                    parts = p.split(":")
                    want.add((int(parts[0]), int(parts[1]),
                              int(parts[2]) if len(parts) > 2 and parts[2] else 0))
                else:
                    want.add((2, int(p), 0))
            except Exception:
                pass
        if not want:
            log.info("[%s] Doi qua su kien: BAT nhung chua tick qua nao -> bo qua "
                     "(Cai dat nang cao > List qua)", self._label)
            return 0
        # TICK THUOC SU KIEN NAO? Chu ky luu luc user tick (config) phai trung chu ky su kien DANG
        # MO. Khac = su kien da doi -> KHONG doi gi ca, du mon do tinh co van con o su kien moi
        # (nguyen lieu da khac han - user phai chon lai). Chan ngay trong bot vi bot dang chay thi
        # khong ai mo GUI de bo tick.
        _sig_now = _evx.cache_signature()
        _sig_pick = getattr(self, "event_exchange_sig", "") or ""
        if _sig_now and _sig_pick != _sig_now:
            log.warning("[%s] Doi qua su kien: tick dang co thuoc SU KIEN KHAC -> KHONG doi. "
                        "Mo Cai dat nang cao > List qua de chon lai theo su kien moi.", self._label)
            return 0
        missions = [m for a in self._activities.values() for m in a.get("missions") or ()]
        if not missions:
            # Hay gap nhat: server chua gui S:124-000 (hoac goi bi lo) -> KHONG duoc im lang.
            log.warning("[%s] Doi qua su kien: chua nhan duoc danh sach doi thuong tu server "
                        "(0 su kien) -> bo qua lan nay", self._label)
            return 0
        # TUI DO PHAI VE TRUOC (ke hoach doi dua tren so luong trong tui). Dung DUNG dieu kien cua
        # log_bag_delayed(): snapshot 0x17/05 la goi TU MO TA (co count trong goi) -> nhan duoc la
        # xong; chi cho them 1.5s phong truong hop tui to server chia NHIEU goi.
        # Thuc te toi day tui da ve tu luc login -> vong nay thoat ngay, khong cho giay nao.
        _t0 = time.time()
        while self.running and time.time() - _t0 < 8.0:
            if self.bag_slots and time.time() - self._bag_time > 1.5:
                break
            time.sleep(0.3)
        if not self.bag_slots:
            log.warning("[%s] Doi qua su kien: TUI DO chua ve (khong nhan duoc snapshot 0x17/05) "
                        "-> bo qua lan nay, khong doi mo de tranh tinh sai", self._label)
            return 0
        if time.time() - _t0 > 1.0:
            log.info("[%s] Doi qua su kien: da cho tui do %.1fs", self._label, time.time() - _t0)
        # `missions` o day la ban RAW (cost/award dang tuple) -> chuyen ve dang dict cho planner.
        norm = []
        for m in missions:
            norm.append({
                "id": m["id"], "cond": m["cond"], "limit": m["limit"],
                "cost": [{"kind": k, "item": i, "quant": q, "name": self._res_name(k, i)}
                         for k, i, q in m["cost"] if i],
                "award": [{"kind": k, "item": i, "quant": q, "name": self._res_name(k, i)}
                          for k, i, q in m["award"] if i],
            })
        total = 0
        packets = 0               # so GOI that su gui len (1 goi = 1 muc, kem so lan)
        # BAT DAU = 0: gui NGAY khi co ACK cua lenh truoc (dung y user). Chi khi server that su
        # keu ma 3 moi tu noi rong - khong dat san con so doan mo.
        self._evx_gap = 0.0
        self._evx_last_ts = 0.0
        self._evx_spent = {}      # (kind,id) -> chenh lech so voi tui (am = da tieu, duong = vua nhan)
        log.info("[%s] Doi qua su kien: %d qua da tick, %d su kien dang mo",
                 self._label, len(want), len(self._activities))
        for kind, item, mid in sorted(want):
            # Qua cuoi nay co trong su kien dang mo khong? (kem dung MUC do neu user tick theo muc)
            if not any((int(a.get("kind") or 2), int(a["item"])) == (kind, item)
                       and (not mid or int(m["id"]) == mid)
                       for m in norm for a in m["award"]):
                log.info("[%s] Doi qua: '%s' KHONG co trong su kien dang mo -> bo qua "
                         "(su kien da doi? vao List qua tick lai)",
                         self._label, self._res_name(kind, item))
                continue
            done_this = 0
            # CHI 1 LAN: da tinh so luong toi da ngay tu dau. Lap them vong nua se doc `bag_counts`
            # con CU (server chua tru xong) -> ke hoach thua -> doi ra ngoc roi ket, khong thanh qua.
            for _round in range(1):
                if not self.running:
                    break
                # Lap ke hoach cho SO LUONG LON NHAT doi duoc, khong phai tung cai mot: tang gap doi
                # roi chia doi (thuan tinh toan, khong ton goi mang). Truoc day moi vong chi doi 1
                # cai -> 50 vong x nhieu goi cho 1 mon.
                why = {}
                best, best_n = None, 0
                _lo, _try = 1, 1
                while _try <= 10000:
                    _p = _evx.plan_for(kind, item, norm, self._res_have, self._activity_got,
                                       want=_try, why=(why if _try == 1 else None),
                                       only_mission=mid)
                    if not _p:
                        break
                    best, best_n, _lo = _p, _try, _try
                    _try *= 2
                _hi = _try - 1
                while _lo + 1 <= _hi:                 # tinh chinh giua [best_n+1 .. _hi]
                    _mid = (_lo + _hi + 1) // 2
                    if _mid <= best_n:
                        break
                    _p = _evx.plan_for(kind, item, norm, self._res_have, self._activity_got,
                                       want=_mid, only_mission=mid)
                    if _p:
                        best, best_n, _lo = _p, _mid, _mid
                    else:
                        _hi = _mid - 1
                plan = best
                if not plan:
                    if not done_this:
                        self._log_exchange_block(kind, item, why)
                    break
                name = self._res_name(kind, item)
                log.info("[%s] DOI QUA SU KIEN '%s' x%d -> %s", self._label, name, best_n,
                         " | ".join("muc %d x%d" % (m["id"], n) for m, n in plan))
                for m, n in plan:
                    if not self.running:
                        break
                    # CHO SERVER CAP XONG nguyen lieu cua buoc TRUOC. Cac buoc trong 1 ke hoach noi
                    # tiep nhau (Trang -> Ngoc -> Qua): ban lien tiep thi buoc cuoi toi noi luc
                    # server MOI CAP MOT PHAN ngoc -> bi tu choi, ngoc ket lai trong tui.
                    if not self._wait_materials(m, n):
                        log.warning("[%s] Doi qua: '%s' -> server chua cap du nguyen lieu cho muc "
                                    "%d sau 20s -> dung chuoi (phan da doi van giu)",
                                    self._label, name, int(m["id"]))
                        break
                    # 1 GOI cho CA n lan: C:124-003 = +任務ID(4) +次數(4), 次數 = SO LAN doi.
                    # (Truoc day ban n goi count=1 -> cham va de dinh ma loi 3 "太頻繁" cua server.)
                    # Server chan theo KHOANG CACH giua 2 lenh (ma 3 "太頻繁"), khong theo so luong
                    # trong goi -> dinh ma 3 thi CHO LAU HON roi gui lai, khong bo cuoc.
                    _res = None
                    for _attempt in range(3):
                        if not self.running:
                            break
                        # GIAN CACH TRUOC KHI GUI: doi du _evx_gap giay ke tu lenh truoc.
                        _due = self._evx_last_ts + self._evx_gap - time.time()
                        if _due > 0:
                            time.sleep(_due)
                        self._evx_last_sent = (int(m["id"]), int(n))
                        self._evx_result = None
                        packets += 1
                        self._evx_last_ts = time.time()
                        self.send(0x7c, b"\x03\x00" + struct.pack("<II", int(m["id"]), int(n)))
                        _t0 = time.time()   # cho ACK (S:124-003) roi moi xu ly tiep
                        while self.running and self._evx_result is None and time.time() - _t0 < 10.0:
                            time.sleep(0.2)
                        _res = self._evx_result
                        if _res is None or not any(c == 3 for c in _res[1]):
                            break
                        # Van bi chan -> gian cach dang dung CHUA DU: noi rong va NHO cho ca phien
                        # (cac buoc sau dung luon muc moi, khong dinh lai cung loi).
                        self._evx_gap = min(max(self._evx_gap * 2, 1.0), 6.0)   # 0 -> 1 -> 2 -> 4 -> 6
                        log.info("[%s] Doi qua: bi chan 'gui qua nhanh' -> nang gian cach len "
                                 "%.1fs roi gui lai muc %d", self._label, self._evx_gap, int(m["id"]))
                    if _res is None:
                        log.warning("[%s] Doi qua: muc %d khong co phan hoi sau 10s -> dung chuoi",
                                    self._label, int(m["id"]))
                        break
                    _ok, _codes = _res
                    if _ok < len(_codes):
                        # Da co log chi tiet ma loi o handler. Dung ngay: cac buoc tren phu thuoc
                        # buoc nay, doi tiep chi tao them nguyen lieu thua.
                        break
                    n = _ok
                    # Server tu tru nguyen lieu + gui lai S:124-002/011 -> lan lap sau tinh lai
                    # tren so LIEU MOI. Cong tam o day de vong nay khong doi qua so luot.
                    self._activity_got[int(m["id"])] = self._activity_got.get(int(m["id"]), 0) + n
                    # SO SACH TAM: tru nguyen lieu + cong phan thuong NGAY, khong doi server.
                    _sp = self._evx_spent
                    for _c in m["cost"]:
                        _k = (int(_c.get("kind") or 2), int(_c["item"]))
                        _sp[_k] = _sp.get(_k, 0) - int(_c["quant"]) * n
                    for _a in m["award"]:
                        _k = (int(_a.get("kind") or 2), int(_a["item"]))
                        _sp[_k] = _sp.get(_k, 0) + int(_a["quant"]) * n
                    total += n
                    done_this += n
        if total:
            log.info("[%s] Doi qua su kien: %d luot doi trong %d goi", self._label, total, packets)
        return total

    def _wait_res_raw(self, kind: int, res_id: int) -> int:
        """So luong THUC TE server dang ghi nhan (KHONG tru so sach tam _evx_spent)."""
        if int(kind) == 1:
            return int(self._coin_quant.get(int(res_id), 0))
        return int((getattr(self, "bag_counts", {}) or {}).get(int(res_id), 0))

    def _wait_materials(self, m, times: int, timeout: float = 20.0) -> bool:
        """Cho den khi TUI that su du nguyen lieu de doi `times` lan muc `m`.

        Nguyen lieu cua buoc sau chinh la phan thuong cua buoc truoc -> phai doi server cap xong
        moi gui, khong thi server tu choi ma minh khong biet.
        """
        need = [((int(c.get("kind") or 2), int(c["item"])), int(c["quant"]) * int(times))
                for c in (m.get("cost") or ()) if c.get("item")]
        if not need:
            return True
        t0 = time.time()
        while self.running:
            if all(self._wait_res_raw(k, i) >= q for (k, i), q in need):
                return True
            if time.time() - t0 >= timeout:
                return False
            time.sleep(0.4)
        return False

    def _log_exchange_block(self, kind, item, why):
        """Noi RO vi sao khong doi duoc - khong bao chung chung 'khong du nguyen lieu'."""
        name = self._res_name(kind, item)
        res = why.get("res")
        if not res:
            log.info("[%s] Doi qua: '%s' khong lap duoc ke hoach doi", self._label, name)
            return
        rname = self._res_name(res[0], res[1])
        if why.get("limit_only"):
            log.info("[%s] Doi qua: '%s' -> HET LUOT doi '%s' hom nay/su kien (server gioi han)",
                     self._label, name, rname)
        else:
            log.info("[%s] Doi qua: '%s' -> thieu '%s': can %d, dang co %d",
                     self._label, name, rname, int(why.get("need") or 0), int(why.get("have") or 0))

    @task_report("bang 3x3 su kien", PHASE_LOGIN_CHORE)
    def claim_event_boards(self, wait: float = 1.2) -> int:
        """NHAN THUONG cac BANG 3x3 KHAC ngoai "Nhiem vu moi ngay" (hien la bang EVENT theo thang).

        TONG QUAT - KHONG hardcode nhiem vu: EVENT DOI THEO THANG (file .dat bi ghi de, vd panel 10
        tu "thu thap chu" -> "Mung Game Ra Mat Hai Thang") nhung CO CHE khong doi:
          - Bang nao dang chay  -> server gui trong S:91-1 (self._quest_grids)
          - O nao da xong       -> co 'done' tung o trong chinh goi do
          - Hang/cot an thuong  -> thuan logic 3x3 (1-3 hang, 4-6 cot, 7 = TAT CA)
          - Line da nhan        -> 永標 theo getFlag (jiugongge.json); thieu file van chay duoc vi
                                   server tu tu choi line da nhan.
        LOC theo `kind` (jiugongge.json): 1 = nhiem vu ngay (ham khac lo), 3 = EVENT -> LAM;
        2 = nhiem vu tan thu -> BO QUA (yeu cau user). Bang LA (chua co trong data, vd event moi
        ra sau khi crack) -> VAN LAM (kha nang cao la event moi).
        Goi C:91-3 giong client: 0x5b [03 00][gridId u16][line][missionId cua O SO `line`].
        """
        grids = dict(getattr(self, "_quest_grids", {}) or {})
        if not grids:
            return 0
        meta = getattr(config, "JIUGONGGE", {}) or {}
        total = 0
        for gid, g in sorted(grids.items()):
            if gid == 1:
                continue                      # bang nhiem vu ngay: claim_daily_quests lo
            info = meta.get(gid) or {}
            kind = info.get("kind")
            if kind == 2:
                continue                      # nhiem vu tan thu -> bo qua
            cells = set(g.get("cells") or ())
            missions = dict(g.get("missions") or {})
            if not cells:
                continue
            claimed = set(self._claimed_by_grid.get(gid) or ())
            lines = [L for L, cs in self._Q_LINES.items()
                     if all(c in cells for c in cs) and L not in claimed]
            name = info.get("name") or ("bang %d" % gid)
            for L in lines:
                self.send(0x5b, b"\x03\x00" + struct.pack("<H", gid) + bytes([L])
                          + struct.pack("<H", int(missions.get(L, 0))))
                time.sleep(0.3)
                total += 1
            # TAT CA (line 7): client bat buoc ca 6 line PHAI da nhan (msg 21291).
            n6 = sum(1 for L in range(1, 7) if L in claimed or L in lines)
            if n6 >= 6 and 7 not in claimed and 7 not in lines:
                self.send(0x5b, b"\x03\x00" + struct.pack("<H", gid) + b"\x07"
                          + struct.pack("<H", int(missions.get(7, 0))))
                time.sleep(0.3)
                total += 1
            if lines or (n6 >= 6):
                log.info("[%s] '%s' (bang %d): o xong=%s -> claim %s",
                         self._label, name, gid, sorted(cells), lines or "line TAT CA")
        return total

    @task_report("nhiem vu hang ngay", PHASE_LOGIN_CHORE)
    @_pet_role("quest")
    def claim_daily_quests(self, heavy: bool = True):
        """STATUS-DRIVEN: query 9 o -> o CHUA xong (bot lam duoc) thi LAM -> re-query -> claim
        hang/cot du 3 o (0x5b 03 00 01 00 [line][id]) + TONG KET neu du 6.
          heavy=True (mac dinh): lam ca nhiem vu NANG (boss the gioi o2 - teleport di) + nhe.
          heavy=False: CHI nhiem vu NHE (gacha o4/o6, hop o7 - khong roi cho) + claim. Dung cho
            mode DI GIOI (goi sau khi VAO DG, tranh boss teleport van ra khoi DG; o1/o2/o5 nang
            se claim_daily_quests(heavy=True) goi SAU khi xong DG).
        Chay moi login: o da xong -> bo qua; gacha thieu xu lan truoc -> login sau tu retry."""
        # CHI tin trang thai server tra LUC NAY (KHONG cache): moi lan query server gui lai DAY DU o da
        # xong (020001010009...). Cache cu thua + tung POISON (parse sai o9 -> luu nham -> relogin van bao xong).
        self._quest_cells = set()
        done = self._query_quests()
        # Cho frame 0x51 (line da nhan, trigger boi 0x62 020002 luc login) toi 2s neu chua nhan
        # -> tranh claim lai line da nhan khi 0x51 ve cham. (Server khong gui -> claim het, vo hai.)
        for _ in range(10):
            if self._claimed_loaded:
                break
            time.sleep(0.2)
        # lam cac nhiem vu con thieu (gacha tu check xu, hop tu check nguyen lieu)
        acted = False
        if 6 not in done:
            self.claim_gacha_pet();  acted = True   # o 6 = gacha pet (NHE)
        if 4 not in done:
            self.claim_gacha_card(); acted = True   # o 4 = gacha card (NHE)
        if 7 not in done:
            self.do_combine_item();  acted = True   # o 7 = hop vat pham (NHE)
        if heavy and 2 not in done:
            self.do_world_boss();    acted = True   # o 2 = boss the gioi (mo event 0x4d/0x0c truoc teleport)
        # (o1 dungeon = do_daily_dungeon rieng; o5 team dungeon = chua co - deu NANG)
        if acted:
            done = self._query_quests()   # refresh sau khi lam
        # Claim hang/cot DU CA 3 o VA CHUA NHAN (frame 0x51 luc login cho biet line da nhan -> khoi claim lai).
        lines = [L for L, cells in self._Q_LINES.items()
                 if all(c in done for c in cells) and L not in self._claimed_lines]
        n = 0
        for L in lines:                       # claim tung hang/cot chua nhan
            # missionId = mission cua O SO L (client: jiugonggeSet[index].Id) - uu tien bang that
            self.send(0x5b, b"\x03\x00\x01\x00" + bytes([L]) + struct.pack("<H", self._q_mission_id(L)))
            time.sleep(0.3); n += 1
        # Client chi cho claim TONG khi ca 6 line DA NHAN (canGetAward==2, msg 21291) -> doi
        # S:91-3 xac nhan cac line vua gui (handler nhet vao _claimed_lines) toi 3s.
        if lines:
            _t0 = time.time()
            while time.time() - _t0 < 3.0 and not all(L in self._claimed_lines for L in lines):
                time.sleep(0.2)
        # du 6 hang/cot (da nhan + vua nhan) VA tong ket (line 7) chua nhan -> claim tong ket.
        # Van tinh "L in lines" du chua confirm: xac nhan that lac thi THU claim tong - server tu
        # validate, fail thi login sau bu (giu do li cua hanh vi cu).
        n_lines6 = sum(1 for L in range(1, 7) if L in self._claimed_lines or L in lines)
        if n_lines6 >= 6 and 7 not in self._claimed_lines:
            self.send(0x5b, b"\x03\x00\x01\x00\x07" + struct.pack("<H", self._q_mission_id(7)))
            time.sleep(0.3); n += 1
        log.info("[%s] Nhiem vu hang ngay: o xong=%s (%d/9), da nhan truoc=%s, claim them %d line (line %s)",
                 self._label, sorted(done), len(done), sorted(self._claimed_lines), n, lines)
        # DOI QUA + BANG 3x3 SU KIEN: chay o DAU pho ban to doi (xem run_event_pre_dungeon).
        # Goi o day chi de PHONG khi party TAT pho ban to doi -> khong ai goi ham kia.
        self.run_event_pre_dungeon()
        # 3) O5 PHO BAN TO DOI = BUOC CUOI (sau khi check + thu lam moi o khac - o khac fail van OK).
        #   Moi acc report o5 da xong chua; LEADER chi chay khi CA party deu chua xong (xem run_party_digioi).
        #   PHAI o SAU 2 buoc tren: ham nay CHAN o day rat lau (leader danh xong ca 5 tran PB moi ve).
        if heavy and self._o5_team_fn:
            try:
                self._o5_team_fn(5 in done)
            except Exception as e:
                log.warning("[%s] loi xu ly o5 pho ban to doi: %s", self._label, e)

    def _on_friend_gift(self, pkt: bytes):
        """Parse S2C 0x0e ban be:
          sub 05 (list login): [05 00][count 2B] + N*[entity 8B][namelen 1B][name][trailer 35B]
            trailer[18]: bit0x01 = DA TANG qua cho ban nay, bit0x02 = ban CO QUA cho minh nhan.
          sub 0d: xac nhan nhan 1 qua.
        Luu friend_entities (merge) + friend_status[entity]=trailer[18]."""
        body = pkt[7:]
        if len(body) < 3:
            return
        sub = body[0]
        if sub == 0x05:           # list ban (login push) - full list roi tung ban 1 goi (update)
            cnt = int.from_bytes(body[2:4], "little")
            i = 4
            new = []
            for _ in range(cnt):
                if i + 9 > len(body):
                    break
                ent = body[i:i + 8]
                nl = body[i + 8]
                try:
                    name = body[i + 9:i + 9 + nl].decode("utf-16-le") if nl else ""
                except Exception:
                    name = ""
                tr = body[i + 9 + nl:i + 9 + nl + 35]
                if len(tr) >= 19:
                    self.friend_status[ent.hex()] = tr[18]   # cap nhat status moi nhat
                    self.friend_online[ent.hex()] = bool(tr[15])
                if name:
                    self._remember_entity_name(ent, name, "0x0e/0500-friend")
                if ent not in self.friend_entities:
                    self.friend_entities.append(ent); new.append(ent)
                i += 9 + nl + 35
        elif sub == 0x0d:         # xac nhan NHAN 1 qua tu ban: [0d 00][entity 8B][01 00]
            self._gift_recv += 1
        elif sub == 0x10 and len(body) >= 11:  # update online: [10 00][entity 8B][online 1B]
            ent = body[2:10]
            self.friend_online[ent.hex()] = bool(body[10])

    @task_report("qua ban be", PHASE_LOGIN_CHORE)
    def claim_friend_gifts(self):
        """TANG qua cho ban CHUA tang + NHAN qua ban da tang minh. HOAN TOAN theo STATUS server
        (friend_status[entity]=trailer[18] tu 0x0e 05 login): bit0x01=DA TANG, bit0x02=CO QUA nhan.
          TANG:  C2S 0x0e [12 00][count][entity*N]  - chi ban CHUA tang (status & 0x01 == 0)
          NHAN:  C2S 0x0e [13 00][count][entity*N]  - ban CO QUA (0x02) VA CHUA nhan (0x04 == 0)
        KHONG can daily_mark: status doc truc tiep -> relogin se thay 'da tang/da nhan' -> tu bo qua
        (idempotent). Chay moi login -> bat duoc ca qua ban gui TRONG NGAY."""
        ents = list(self.friend_entities)
        if not ents:
            return   # chua nhan duoc list ban -> login sau thu lai
        to_send = [e for e in ents if not (self.friend_status.get(e.hex(), 0) & 0x01)]  # chua tang
        # NHAN: co qua (0x02) VA CHUA nhan (0x04 chua set) - khop client Social.ReceiveAllGift
        # (CheckFlag(flag,2) and not CheckFlag(flag,3)). Thieu check 0x04 -> gui lai lenh nhan thua
        # cho ban da nhan qua (co 0x02 van con) -> log "nhan N qua" phong.
        to_recv = [e for e in ents if (self.friend_status.get(e.hex(), 0) & 0x02)
                   and not (self.friend_status.get(e.hex(), 0) & 0x04)]
        if to_send:
            self.send(0x0e, b"\x12\x00" + bytes([len(to_send)]) + b"".join(to_send))
            time.sleep(0.5)
        self._gift_recv = 0
        if to_recv:
            self.send(0x0e, b"\x13\x00" + bytes([len(to_recv)]) + b"".join(to_recv))
            time.sleep(1.0)   # cho 0x0e 0d xac nhan
        if to_send or to_recv:
            log.info("[%s] Qua ban be: tang %d ban (chua tang), nhan %d/%d qua",
                     self._label, len(to_send), self._gift_recv, len(to_recv))

    def _dungeon_tier(self) -> int:
        """`dungeonId` cua pho ban DON theo LEVEL char (server khoa ban ghi sai dai level).

        Khong phai "tier" - day la cot `id` trong Dungeon_C.dat; ten cu giu lai vi nhieu cho goi.
        Dai level lay thang tu file (15~80 / 81~150 / 151~200 / 201~450), truoc day code dung o 4
        nen acc tu **lv 201** tro len se gui NHAM id (ban ghi 4 het o lv 200). Chua chay vi acc cao
        nhat moi lv 193 (do log 15/09), nhung dang bo toi."""
        lv = getattr(self, "char_level", 0) or 0
        for _max_lv, _id in SOLO_DUNGEON_IDS:
            if lv <= _max_lv:
                return _id
        return SOLO_DUNGEON_IDS[-1][1]

    def _run_one_dungeon(self, max_sec: int) -> bool:
        """Chay 1 luot dungeon: query -> vao -> danh boss -> nhan thuong -> ra. True neu vao duoc."""
        orig = self.current_map
        # (1) TRANH BI KICK: phai SACH tran trUOC khi gui goi vao dungeon. Neu con dang
        #     danh tren map train (navigate flee) ma gui 0x2f/0x14 -> server kick (Server dong
        #     ket noi). Giu flee BAT, cho het tran (in_combat ve False sau ~4s idle), toi 30s.
        self.flee_mode = True
        for _ in range(30):
            if not self.running:
                return False
            if not self.in_combat():
                break
            time.sleep(1)
        if not self.running:
            return False
        time.sleep(1.0)               # them 1s cho server chot "ra tran"
        self.heal_full()              # HOI FULL HP/SP truoc khi vao danh boss dungeon
        self.state.boss_mode = True
        self.dungeon_complete = False
        # (2) Chuoi vao dungeon (capture dungeon.pcap), GUI LIEN khong cho map doi:
        #   0x2f 0100 query -> 0x2f 0200020000 VAO -> 0x14 08000100 KHOI DONG tran boss
        #   -> 0x0c 0100 xin info -> 0x14 0600 confirm.
        # LUU Y: map CHI doi sang dungeon SAU KHI gui 0x14 08000100 (code cu cho map doi
        #   truoc roi moi gui 0x14 -> deadlock -> ket o map boss khong danh).
        tier = self._dungeon_tier()
        log.info("[%s] Dungeon tier: level=%s -> tier=%s", self._label, self.char_level, tier)
        self.send(0x2f, b"\x01\x00"); time.sleep(0.6)             # query pho ban
        self.send(0x2f, b"\x02\x00" + bytes([tier]) + b"\x00\x00"); time.sleep(0.6)  # VAO dungeon (theo tier)
        self.send(0x14, b"\x08\x00" + bytes([tier - 1]) + b"\x00"); time.sleep(0.4)  # khoi dong tran boss (tier-1)
        self.send(0x0c, b"\x01\x00"); time.sleep(0.4)              # xin info tran
        self.send(0x14, b"\x06\x00")                               # confirm
        # (3) Xac nhan DA vao dungeon. SOLO dungeon KHONG co nguoi xung quanh -> current_map
        #     (doc tu broadcast nguoi KHAC) KHONG cap nhat sang map dungeon -> KHONG dua vao map.
        #     Dung tin hieu IN_BATTLE (boss giao chien) lam dau hieu da vao: da sach tran truoc
        #     do nen in_battle bat LAI = chinh la tran BOSS. CHI dung in_battle, KHONG dung
        #     "map doi" lam dau hieu: vao dungeon la VAO TRAN BOSS ngay; con map doi co the chi
        #     la di qua TOWN (12001/12002...) khi het luot -> bat nham "da vao" dù boss khong co.
        entered = False
        t0 = time.time()
        while time.time() - t0 < 15:
            if not self.running:
                self.state.boss_mode = False; return False
            if self.state.in_battle:
                self.flee_mode = False   # boss giao chien -> DANH ngay (tat flee TRUOC khi timer fire)
                entered = True; break
            time.sleep(0.1)
        if not entered:
            log.info("[%s] Khong vao duoc dungeon (het luot/het vang?)", self._label)
            self.state.boss_mode = False
            # Neu bi DAY vao sanh dungeon (map doi khac orig) ma khong danh duoc -> THOAT ve map cu
            # (server het luot van teleport vao sanh 12000... -> phai ra keo lech khoi map train).
            if self.current_map is not None and orig is not None and self.current_map != orig:
                log.info("[%s] bi day vao sanh dungeon (map=%s) -> thoat ve map cu %s",
                         self._label, self.current_map, orig)
                self.leave_party(); time.sleep(0.6)
                self.send(0x14, b"\x06\x00"); time.sleep(0.6)
                for _ in range(20):
                    if not self.running or self.current_map == orig:
                        break
                    time.sleep(1)
            return False
        log.info("[%s] Da vao dungeon (in_battle=%s map=%s) -> danh boss",
                 self._label, self.state.in_battle, self.current_map)
        try:
            t0 = time.time()
            last_dbg = 0.0
            # max_sec KHONG duoc cat khi dang danh. Ta biet chinh xac dang trong tran hay khong
            # (state.in_battle theo 0x35/0x34 + 0x14 sub0700), nen dong ho chi de bat KET that su:
            # dang trong tran = dang tien trien -> GIA HAN. Cap chi no khi da `max_sec` giay LIEN
            # TUC khong co tran nao. Truoc day cat mu theo tong thoi gian -> lượt dai binh thuong
            # bi cat GIUA TRAN, thoat vong ma KHONG claim thuong, KHONG leave_party -> ket trong
            # dungeon.
            while self.running:
                if self.state.in_battle:
                    t0 = time.time()
                elif time.time() - t0 >= max_sec:
                    log.warning("[%s] dungeon: %ds KHONG co tran nao (ket?) -> bo cuoc",
                                self._label, int(max_sec))
                    break
                time.sleep(1)
                now = time.time()
                if now - last_dbg >= 6:   # log chan doan moi 6s: co trong tran ko, quai, HP
                    last_dbg = now
                    log.info("[%s] dungeon: map=%s in_battle=%s quai=%s char_hp=%s/%s pet_sp=%s",
                             self._label, self.current_map, self.state.in_battle,
                             self.state.enemy_slots, self.state.char.hp, self.state.char.hp_max,
                             self.state.pet.sp)
                if self.dungeon_complete:
                    log.info("[%s] Dungeon HOAN THANH -> nhan thuong + ra", self._label)
                    self.send(0x52, b"\x01\x00\x01\x1d\x00")   # claim/confirm tong ket
                    time.sleep(0.6)
                    self.leave_single_dungeon()   # thoat dungeon (game tu dua ve map cu)
                    break
            # cho game tu dua ve map train (current_map cap nhat lai khi thay nguoi o safe)
            for _ in range(15):
                if not self.running or self.current_map == orig:
                    break
                time.sleep(1)
        finally:
            self.state.boss_mode = False
            self.flee_mode = True    # ra khoi dungeon -> bat lai flee (con phai ve safe/lap party)
        return True

    # S:084-001 <回傳介消資料> [sellId 2B][kind 1B] - SERVER NOI RO phai tra bang gi, dung doan.
    UISELL_FAIL = 0        # + ly do (1B)
    UISELL_FREE = 254      # dung MIEN PHI  (UISell.lua nhanh `免費使用`)
    UISELL_ITEM = 255      # + itemId (2B) + so luong (1B)
    UISELL_MONEY = {1: "nguyen bao", 2: "nguyen bao khoa", 3: "dong tien"}   # + so luong (4B)

    def buy_dungeon_ticket(self, wait: float = 2.5):
        """MUA luot pho ban don (UISell sellId=13 = `Dungeon.SendResetCount`).

        C:084-001 [sellId=13][dungeonId] -> DOC `kind` trong S:084-001 roi moi gui C:084-002:
        kieu **1** (tra bang vat pham, kem bagIndex) hay kieu **2** (mien phi / tra bang tien).
        Truoc day bot bo qua `kind` va luon gui kieu 2 - sai loai lenh ma khong ai biet.
        `sellId=13` co `rolecountId=0` trong UISell_C.dat => **khong co tran luot mua**, chan duy
        nhat la TIEN (khop nhanh `result == 2` -> `No Count Limit` trong `Dungeon.ReciveResetCount`).
        Xem KNOWLEDGE.md muc "PHO BAN DON"."""
        tier = self._dungeon_tier()
        self._dg_query = None
        self.send(0x54, b"\x01\x00\x0d\x00" + bytes([tier]) + b"\x00")     # hoi gia (theo dungeonId)
        q = None
        for _ in range(int(wait / 0.2)):
            r = self._dg_query
            if r is not None and len(r) >= 5 and r[0:2] == b"\x01\x00":
                q = r
                break
            time.sleep(0.2)
        _kieu = 2          # mac dinh: mien phi / tra bang tien
        _duoi = b""        # kieu 1 con phai kem bagIndex
        if q is None:
            log.info("[%s] Mua luot dungeon: khong nhan duoc gia -> gui nhu cu (kieu 2)", self._label)
        else:
            kind = q[4]
            if kind == self.UISELL_FAIL:
                log.info("[%s] Mua luot dungeon: KHONG DUNG DUOC (ly do %s) -> dung", self._label,
                         q[5] if len(q) >= 6 else "?")
                return False
            if kind == self.UISELL_FREE:
                log.info("[%s] Mua luot dungeon: server bao MIEN PHI", self._label)
            elif kind == self.UISELL_ITEM:
                _tid = int.from_bytes(q[5:7], "little") if len(q) >= 7 else 0
                _slot = next((s for s, (t, c) in self.bag_slots.items() if t == _tid and c > 0), None)
                if _slot is None:
                    log.info("[%s] Mua luot dungeon: can vat pham %d nhung KHONG CO trong tui -> dung",
                             self._label, _tid)
                    return False
                _kieu, _duoi = 1, bytes([_slot])
                log.info("[%s] Mua luot dungeon: tra bang vat pham %d (o tui %d)",
                         self._label, _tid, _slot)
            else:
                _gia = int.from_bytes(q[5:9], "little") if len(q) >= 9 else 0
                log.info("[%s] Mua luot dungeon: gia %d %s", self._label, _gia,
                         self.UISELL_MONEY.get(kind, "tien la (kind=%d)" % kind))
        self._dg_query = None                                          # cho doi tra loi MUA
        # C:084-002 = [sub 02 00][tong loai 1B: 1=vat pham 2=diem][sellId 2B][bagIndex neu loai 1][arg 2B]
        self.send(0x54, b"\x02\x00" + bytes([_kieu]) + b"\x0d\x00" + _duoi + bytes([tier]) + b"\x00")
        for _ in range(int(wait / 0.2)):
            r = self._dg_query
            if r is not None and len(r) >= 5 and r[0:2] == b"\x02\x00":
                ok = (r[4] == 0x01)
                log.info("[%s] Mua ve dungeon -> %s (%s)", self._label,
                         "OK" if ok else "THAT BAI", r.hex())
                return ok
            time.sleep(0.2)
        log.info("[%s] Mua ve dungeon: khong nhan phan hoi -> coi nhu THAT BAI", self._label)
        return False

    @task_report("pho ban don (daily)", PHASE_LOGIN_CHORE)
    @_pet_role("pb_don")
    def do_daily_dungeon(self, max_sec: int = 360, cho_phep=None):
        """SOLO daily dungeon den khi SERVER bao o1 XONG (2/2). KHONG dem local nua (server truth
        chuan hon: dung ca khi chay song song nhieu may/ban build+dev cung nick). Moi luot: thu VE
        FREE truoc; vao loi (free da dung o may/ban khac) -> MUA ve roi vao lai. Sau moi luot
        re-query o1, done thi dung. Cap = runs_target luot vao thanh cong (tranh mua vo han).

        `cho_phep` = ham dieu phoi cam vao, kiem TRUOC KHI VAO va o dau moi luot (L3c). Luot dang
        chay luon chay not - cua chan nam TRUOC `buy_dungeon_ticket()` nen khong mat ve, mat luot.
        Vong nay `leave_party()`
        ngay dong dau roi vao mot map rieng - tuc no VUA pha doi VUA lam lech map, dung hai thu ma
        dieu phoi dang co gang sua. Dang gom party ma vao day = tu tay pha viec cua chinh minh.

        Ca that 09/09 party 1: `brub` bi dump ra 21011 va dung cho tu 13:08:28, bon acc kia cu
        ra/vao pho ban solo nen tap map chop tat giua {21011} va {21011, 62001} - dieu phoi mat ba
        phut moi ra duoc lenh gom.
        """
        if cho_phep is not None:
            try:
                _ly_do = cho_phep()
            except Exception:
                _ly_do = None
            if _ly_do:
                log.info("[%s] Dungeon: HOAN - %s", self._label, _ly_do)
                return
        runs_target = getattr(config, "DUNGEON_RUNS_PER_DAY", 2)
        # TIN HIEU SERVER THAT: o1 (solo 2 lan) DA XONG -> bo qua. Chua co trang thai -> tu query.
        if not self._quest_cells:
            try: self._query_quests()
            except Exception: pass
        if 1 in self._quest_cells:
            log.info("[%s] Dungeon: o1 (solo 2 lan) DA XONG theo server -> bo qua", self._label)
            return
        self.leave_party(); time.sleep(1.5)   # thoat party (solo moi vao duoc dungeon)
        done_runs = 0      # so luot VAO THANH CONG phien nay (cap = runs_target -> khoi mua vo han)
        bought = False     # da chuyen sang MUA ve chua (free da het)
        # BIET TRUOC con free hay khong (client lam y het: an nut VAO khi het luot, chi hien nut MUA).
        # Khong biet -> giu nguyen duong cu (thu FREE truoc, loi thi mua).
        self.wait_mission_steps(timeout=6.0)
        _free = self.solo_dungeon_remaining()
        if _free is not None:
            bought = (_free <= 0)
            log.info("[%s] Dungeon: con %d luot FREE (flag 0x%04x) -> %s", self._label, _free,
                     SOLO_DUNGEON_DAILY_FLAG, "MUA ve ngay" if bought else "vao FREE")
        while self.running and done_runs < runs_target:
            if cho_phep is not None:
                try:
                    _ly_do = cho_phep()
                except Exception:
                    _ly_do = None
                if _ly_do:
                    log.info("[%s] Dungeon: khong vao luot moi - %s", self._label, _ly_do)
                    break
            if bought:     # khong con free -> phai MUA ve truoc khi vao
                if not self.buy_dungeon_ticket():
                    log.info("[%s] Dungeon: mua ve that bai (het vang/luot) -> dung", self._label)
                    break
            ok = self._run_one_dungeon(max_sec)
            if not ok:
                if not bought:
                    # luot FREE vao loi -> free da dung (may/ban khac) -> chuyen sang MUA ve, thu lai
                    log.info("[%s] Dungeon: vao FREE that bai (free da dung o noi khac?) -> chuyen MUA ve",
                             self._label)
                    bought = True
                    continue
                # da mua ve van khong vao -> dung (tranh loop dump)
                log.info("[%s] Dungeon: da mua ve van khong vao duoc -> dung (tranh dump)", self._label)
                break
            done_runs += 1
            bought = True   # da dung 1 luot (free hoac mua) -> tu luot sau BUOC phai mua ve
            log.info("[%s] Xong dungeon luot %d (phien nay)", self._label, done_runs)
            time.sleep(2)
            self._wait_combat_clear()
            self.heal_full(force=True)   # xong battle dungeon -> hoi FULL HP/SP char+pet
            # Re-query o1: server CHI bao done khi DU 2/2 (luc 1/2 van 020004 - panel KHONG lo tien do).
            # Done -> dung; xu ly dung ca khi nick da danh 1 luot o may/ban khac (khoi danh thua).
            try: self._query_quests()
            except Exception: pass
            if 1 in self._quest_cells:
                log.info("[%s] Dungeon: o1 DA XONG (2/2 theo server) -> dung", self._label)
                break
        log.info("[%s] Hoan tat daily dungeon (%d luot phien nay)", self._label, done_runs)

    GACHA_COST = 9000   # xu / luot gacha (pet va card deu 9k)

    def _wait_xu(self, timeout: float = 3.0):
        """Cho S2C 0x1a id=4 (so xu) toi, toi da 'timeout' giay."""
        t0 = time.time()
        while self.xu is None and time.time() - t0 < timeout:
            time.sleep(0.2)

    def claim_gacha_pet(self):
        """Gacha PET hang ngay (1 lan/ngay). C2S 0x42 (draw) + 3x 0x5b (reveal) - replay client that.
        Chi gacha khi xu >= 9000; thieu xu -> bo qua, login sau thu lai.
        Goi tu claim_daily_quests khi o 6 CHUA xong (status-driven, khong gate _daily_done)."""
        self._wait_xu()
        if self.xu is None or self.xu < self.GACHA_COST:
            log.info("[%s] Gacha pet: thieu xu (%s < %d) -> bo qua",
                     self._label, self.xu, self.GACHA_COST)
            return
        self.send(0x42, bytes.fromhex("0100050101015bb22823010000"))
        time.sleep(0.5)
        for _ in range(3):
            self.send(0x5b, bytes.fromhex("0200010100063400"))
            time.sleep(0.2)
        self.xu -= self.GACHA_COST   # server khong push lai balance -> tu tru
        log.info("[%s] Gacha PET hang ngay (xu con ~%d)", self._label, self.xu)

    def claim_gacha_card(self):
        """Gacha CARD hang ngay. Tuong tu gacha pet, banner id = 5cb2.
        Goi tu claim_daily_quests khi o 4 CHUA xong (status-driven, khong gate _daily_done)."""
        self._wait_xu()
        if self.xu is None or self.xu < self.GACHA_COST:
            log.info("[%s] Gacha card: thieu xu (%s < %d) -> bo qua",
                     self._label, self.xu, self.GACHA_COST)
            return
        self.send(0x42, bytes.fromhex("0100050101025cb22823010000"))
        time.sleep(0.5)
        for _ in range(3):
            self.send(0x5b, bytes.fromhex("0200010100043200"))
            time.sleep(0.2)
        self.xu -= self.GACHA_COST
        log.info("[%s] Gacha CARD hang ngay (xu con ~%d)", self._label, self.xu)

    # Gói mua shop = opcode 0x42 (cùng họ gacha), bắn thẳng 1 gói, không cần mở shop/reveal.
    #   0100 [shop] [tab] [page] [slot] [item_id 2B] [gia 2B] [qty 1B] 0000   (capture ts_shop.pcap)
    def _shop42_payload(self, shop: int, tab: int, page: int, slot: int,
                        item_id: int, price: int, qty: int) -> bytes:
        return (
            b"\x01\x00" + bytes([shop & 0xFF, tab & 0xFF, page & 0xFF, slot & 0xFF]) +
            int(item_id).to_bytes(2, "little") +
            int(price).to_bytes(2, "little") +
            bytes([int(qty) & 0xFF]) +
            b"\x00\x00"
        )

    def buy_di_gioi_ho_phu(self):
        """Mua Di Gioi Ho Phu (0xff8c) theo counter server 0x55 sid=0x0456 (X/3)."""
        cur = self.shop_ho_phu_count
        mx = self.shop_ho_phu_max or 3
        if cur is not None and cur >= mx:
            log.info("[%s] Mua shop: Di Gioi Ho Phu da %d/%d theo server -> bo qua",
                     self._label, cur, mx)
            return
        qty = mx if cur is None else max(0, mx - cur)
        if qty <= 0:
            return
        self.send(0x42, self._shop42_payload(1, 1, 3, 1, 0xff8c, 36, qty))
        if cur is not None:
            self.shop_ho_phu_count = min(mx, cur + qty)  # server 0x55 se ghi de lai neu khac
        log.info("[%s] Mua shop: %d Di Gioi Ho Phu (server truoc do: %s/%d)",
                 self._label, qty, "?" if cur is None else cur, mx)

    def buy_hop_thien_chau(self):
        """Mua Hộp Thiên Châu (0xb68a) theo counter server 0x55 sid=0x002b (X/1)."""
        cur = self.shop_thien_chau_count
        mx = self.shop_thien_chau_max or 1
        if cur is not None and cur >= mx:
            log.info("[%s] Mua shop: Hop Thien Chau da %d/%d theo server -> bo qua",
                     self._label, cur, mx)
            return
        qty = 1 if cur is None else max(0, min(1, mx - cur))
        if qty <= 0:
            return
        self.send(0x42, self._shop42_payload(1, 1, 3, 6, 0xb68a, 39, qty))
        if cur is not None:
            self.shop_thien_chau_count = min(mx, cur + qty)
        log.info("[%s] Mua shop: %d Hop Thien Chau (server truoc do: %s/%d)",
                 self._label, qty, "?" if cur is None else cur, mx)

    def use_di_gioi_ho_phu(self) -> bool:
        """Dung 1 Di Gioi Ho Phu (0xff8c) theo slot tui live.
        Capture MuMu 12: C2S 0x17 0f00 [slot][01] 000000 [target=00] 00;
        server ACK 0x17/0900 roi tu cap nhat timer Di Gioi bang 0x55/id=0x1b."""
        ok = self.use_item(0xff8c, target=0)
        if ok:
            log.info("[%s] Dung Di Gioi Ho Phu (0xff8c) -> cho server cap nhat timer 0x55/0x1b",
                     self._label)
        else:
            log.info("[%s] Khong co Di Gioi Ho Phu (0xff8c) trong tui -> bo qua", self._label)
        return ok

    def buy_trieu_goi_bao_hop(self, xu_threshold: int):
        """Mua Trieu Goi Bao Hop (0xb554) theo counter server 0x55 sid=0x0016 (X/1)."""
        cur = self.shop_bao_hop_count
        mx = self.shop_bao_hop_max or 1
        if cur is not None and cur >= mx:
            log.info("[%s] Mua Bao Hop: da %d/%d theo server -> bo qua", self._label, cur, mx)
            return
        self._wait_xu()
        if self.xu is None:
            log.info("[%s] Mua Bảo Hộp: chưa đọc được xu -> bỏ qua", self._label)
            return
        if self.xu <= xu_threshold:
            log.info("[%s] Mua Bảo Hộp: xu (%d) chưa vượt %d -> bỏ qua", self._label, self.xu, xu_threshold)
            return
        qty = mx if cur is None else max(0, mx - cur)
        if qty <= 0:
            return
        cost = 60000 * qty
        if self.xu < cost:
            log.info("[%s] Mua Bao Hop: xu (%d) khong du %d cai (can %d) -> bo qua",
                     self._label, self.xu, qty, cost)
            return
        before = self.xu
        self.send(0x42, self._shop42_payload(1, 1, 3, 7, 0xb554, 60000, qty))
        self.xu -= cost
        if cur is not None:
            self.shop_bao_hop_count = min(mx, cur + qty)  # server 0x55 se ghi de lai neu khac
        log.info("[%s] Mua shop: %d Trieu Goi Bao Hop (server truoc do: %s/%d, xu %d > %d, con ~%d)",
                 self._label, qty, "?" if cur is None else cur, mx, before, xu_threshold, self.xu)

    def _learned(self) -> dict:
        """Cache item da hoc theo TID (template) - CHUNG mọi acc: item giong nhau = tid giong = heal giong.
        { tid_str: {hp,sp,hp_zero,sp_zero,none,unusable} }."""
        return _load_all_learned()

    def use_item(self, item_id: int, target: int = 0) -> bool:
        """Dung item theo TID bang cach tim SLOT live roi goi use_slot().
        Moi thao tac item phai gui slot tui, khong gui tid gamedata len server."""
        for slot, (tid, cnt) in sorted(self.bag_slots.items()):
            if tid == item_id and cnt > 0:
                return self.use_slot(slot, target)
        return False

    def log_bag_delayed(self, max_wait: float = 8.0):
        """In tui khi snapshot tui (0x17/05) DA VE + on dinh (ngung nhan them 1.5s) -> tranh cho cung
        8s. Goi luc login -> in tui de dinh danh item (use_login_items xu ly viec tu dung item)."""
        def _run():
            t0 = time.time()
            while self.running and time.time() - t0 < max_wait:
                time.sleep(0.3)
                if self.bag_slots and time.time() - self._bag_time > 1.5:
                    break   # da co tui + ngung nhan snapshot moi 1.5s -> du
            if self.running:
                self.log_bag()
        threading.Thread(target=_run, daemon=True).start()

    def _use_items_from_cfg(self, cfg, context_label):
        """Loi chung: dung het cac item trong 'cfg' (subset cua config.USE_LOGIN_ITEMS) dang co
        trong tui. Dung boi use_login_items() (mot lan/login) VA use_phuc_than_items() (dinh ky,
        xem ghi chu o do). context_label chi de log ('login'/'phuc than dinh ky')."""
        if not cfg:
            return
        # snapshot slot can dung (dung 1 slot lam RONG chinh no, khong doi index slot khac)
        targets = [(slot, tid, cnt) for slot, (tid, cnt) in list(self.bag_slots.items())
                   if cnt > 0 and tid in cfg]
        if not targets:
            return
        items = _load_gamedata_items()
        total = 0
        # Moi lan chi chon 1 bao ho. Tinh ca ngoc dang deo tu snapshot login de khong thay ngang cap,
        # ha Ngoc Sieu xuong Ngoc Dai, hoac mo tui oan.
        priority_tids = {tid for tid, _action in PHUC_THAN_PROTECTION_PRIORITY}
        # Ngoc da HONG (Ngoc Hu) van chiem O NGOC -> phai VUT truoc, khong thi deo ngoc moi khong
        # duoc. Day la dung thu tu client goc lam (MachineBox 檢查福神: C:023-013 roi SendUseEquip).
        self._drop_broken_gem()
        equipped_tid = self._equipped_phuc_than_tid()
        _hang_dang_deo = phuc_than_hang(equipped_tid)
        if _hang_dang_deo != 0:      # da deo cai TOT NHAT (Ngoc Ba) -> khong dong toi
            for tid, action in PHUC_THAN_PROTECTION_PRIORITY:
                if tid not in cfg:
                    continue
                if action == "equip" and phuc_than_hang(tid) >= _hang_dang_deo:
                    # Ngoc trong tui KHONG tot hon cai dang deo -> dung han: cac tid sau con kem
                    # hon nua. Thay bang cai bang/kem hon la HA CAP mot cach vo ich.
                    break
                if action == "use" and equipped_tid in PHUC_THAN_GEM_TIDS:
                    break
                _slot = next((s for s, (t, c) in self.bag_slots.items() if t == tid and c > 0), None)
                if _slot is None:
                    continue
                if action == "equip":
                    done = 1 if self.equip_item(_slot) else 0
                else:
                    done = 1 if self.use_slot(_slot, qty=1) else 0
                total += done
                rec = self.bag_slots.get(_slot)
                if rec:
                    rec[1] = max(0, rec[1] - done)
                    if rec[1] <= 0:
                        self.bag_slots.pop(_slot, None)
                if done and action == "equip":
                    old = [x for x in getattr(self, "equipped_items", [])
                           if x.get("id") not in PHUC_THAN_GEM_TIDS | {BROKEN_PHUC_THAN_TID}]
                    self.equipped_items = old + [{"id": tid, "pos": EQUIP_POS_SPEC,
                                                  "damage": 0, "damaged_item_id": 0}]
                _nm = (items.get(tid) or {}).get("name") or cfg[tid].get("name", "")
                log.info("[%s] tu %s item (%s) slot=%d tid=0x%04x ('%s') %s",
                         self._label, "trang bi" if action == "equip" else "dung",
                         context_label, _slot, tid, _nm,
                         "OK" if done else "THAT BAI (slot het?)")
                break
        for slot, tid, cnt in targets:
            if tid in priority_tids:
                continue   # nhom bao ho da chon toi da 1 item o tren
            qcfg = cfg[tid].get("qty")
            if qcfg is None:
                want, chunk = cnt, 1              # khong gioi han: dung het, tung cai 1
            else:
                want, chunk = min(cnt, int(qcfg)), 255   # gioi han qcfg/lan, batch duoc
            done = 0
            while done < want and self.running:
                q = min(want - done, chunk)
                if not self.use_slot(slot, qty=q):
                    break
                done += q
                total += q
                time.sleep(0.4)   # cho server xu ly truoc lenh ke
            # cap nhat tracking: tru so da dung; het -> xoa slot (S2C 0x17 se update lai)
            rec = self.bag_slots.get(slot)
            if rec:
                rec[1] = max(0, rec[1] - done)
                if rec[1] <= 0:
                    self.bag_slots.pop(slot, None)
            _nm = (items.get(tid) or {}).get("name") or cfg[tid].get("name", "")
            log.info("[%s] tu dung item (%s) slot=%d tid=0x%04x dung %d/%d ('%s')",
                     self._label, context_label, slot, tid, done, cnt, _nm)
        if total:
            log.info("[%s] Tu dung item (%s): tong %d cai (%d slot)",
                     self._label, context_label, total, len(targets))

    def _parse_equipment_snapshot(self, pkt: bytes):
        """Luu phan ThingData can cho quyet dinh Phuc Than ngay sau login."""
        count = pkt[9]
        off = 10
        equipped = []
        for _ in range(count):
            if off + 35 > len(pkt):
                break
            raw = pkt[off:off + 35]
            _tid = int.from_bytes(raw[0:2], "little")
            # Client suy VI TRI tu fitType cua item (S:023-011 khong gui vi tri). Bot chi can O
            # NGOC nen chi danh dau pos=6 cho 3 tid ngoc; do khac khong dung pos.
            equipped.append({
                "id": _tid,
                "pos": (EQUIP_POS_SPEC
                        if _tid in PHUC_THAN_GEM_TIDS | {BROKEN_PHUC_THAN_TID} else 0),
                "damaged_item_id": int.from_bytes(raw[27:29], "little"),
                # DU cac truong cua MON CU THE: he, linh da (de tinh cong tu do) va cuong hoa /
                # dong phu (de hien trong tui do). Truoc day chi lay damage -> thay do xong khong
                # tinh lai duoc chi so, va bam vao mon dang mac cung khong thay cuong hoa/dong phu.
                **thing_data_info(raw),
            })
            off += 35
        self.equipped_items = equipped
        # BANG THEO VI TRI MAC. Khong co bang nay thi lan THAY DO dau tien se lam MAT mon cu:
        # server khong gui goi cap nhat tui cho no (client tu dat lai - protocolTable[23][17]),
        # nen phai biet truoc dang mac gi o vi tri do.
        self.equip_by_fit = {}
        for it in equipped:
            fit = int((_load_gamedata_items().get(it["id"]) or {}).get("ft", 0) or 0)
            if fit:
                self.equip_by_fit[fit] = it["id"]

    def _gem_record(self):
        """Ban ghi do dang deo o O NGOC (vi tri 6). None = chua biet (chua nhan snapshot login)."""
        for item in getattr(self, "equipped_items", []):
            if item.get("pos") == EQUIP_POS_SPEC:
                return item
        # Snapshot cu (chua co "pos") -> suy theo tid: ca 3 tid ngoc deu fitType=6
        for item in getattr(self, "equipped_items", []):
            if item.get("id") in PHUC_THAN_GEM_TIDS | {BROKEN_PHUC_THAN_TID}:
                return item
        return None

    def _on_equip_damage(self, pos: int, damage: int):
        """S:023-027: do ben cua do o `pos` doi. Chi quan tam O NGOC (6) cua CHAR."""
        if pos != EQUIP_POS_SPEC:
            return
        rec = self._gem_record()
        if rec is None:
            return
        old = rec.get("damage", 0)
        rec["damage"] = damage
        if damage != old:
            _nm = (_load_gamedata_items().get(rec.get("id", 0)) or {}).get("name", "?")
            log.info("[%s] NGOC '%s' do ben: %d -> %d%s", self._label, _nm, old, damage,
                     "  (HONG)" if damage >= 250 else "")
        if damage >= 250:
            self.phuc_than_pending = True   # vut + deo ngoc moi NGAY (mat he so EXP tung giay)

    def _on_equip_broken(self, pkt: bytes):
        """S:023-035: do hong -> thay HAN ban ghi. Chi xu ly do CHAR (followIndex 0) o O NGOC."""
        pos = pkt[9]
        raw = pkt[10:45]
        follow = pkt[45] if len(pkt) >= 46 else 0
        if follow != 0 or pos != EQUIP_POS_SPEC:
            return
        rec = {
            "pos": pos,
            "id": int.from_bytes(raw[0:2], "little"),
            "damage": raw[6],
            "damaged_item_id": int.from_bytes(raw[27:29], "little"),
        }
        others = [x for x in getattr(self, "equipped_items", [])
                  if x.get("pos") != EQUIP_POS_SPEC
                  and x.get("id") not in PHUC_THAN_GEM_TIDS | {BROKEN_PHUC_THAN_TID}]
        self.equipped_items = others + [rec]
        _was = (_load_gamedata_items().get(rec["damaged_item_id"]) or {}).get("name", "?")
        log.info("[%s] NGOC HONG: o ngoc thanh 0x%04x (truoc la '%s') -> se vut + deo ngoc moi",
                 self._label, rec["id"], _was)
        self.phuc_than_pending = True

    def discard_equipped(self, pos: int) -> bool:
        """Vut do DANG MAC theo VI TRI. C:023-013 <丟棄玩家裝備> +背包索引(1) = 0x17 sub0d00 [pos].
        Dung cho Ngoc Hu: no nam o O DO (vi tri 6), KHONG nam trong tui nen discard_item (theo slot
        tui) khong bao gio thay - day la cach client goc lam (MachineBox routine 檢查福神)."""
        if not self.running:
            return False
        self.send(0x17, b"\x0d\x00" + bytes([pos & 0xFF]))
        return True

    def _drop_broken_gem(self) -> bool:
        """Ngoc o vi tri 6 da HONG (damage>=250 / id=Ngoc Hu) -> vut di de deo ngoc moi duoc.
        Theo dung client: kiem damagedItemId de chac chan no TUNG la ngoc Phuc Than."""
        rec = self._gem_record()
        if rec is None:
            return False
        broken = rec.get("damage", 0) >= 250 or rec.get("id") == BROKEN_PHUC_THAN_TID
        was_gem = (rec.get("damaged_item_id") in PHUC_THAN_GEM_TIDS
                   or rec.get("id") in PHUC_THAN_GEM_TIDS | {BROKEN_PHUC_THAN_TID})
        if not (broken and was_gem):
            return False
        if not self.discard_equipped(EQUIP_POS_SPEC):
            return False
        log.info("[%s] da vut ngoc HONG o o ngoc (vi tri %d)", self._label, EQUIP_POS_SPEC)
        self.equipped_items = [x for x in getattr(self, "equipped_items", [])
                               if x is not rec]
        time.sleep(0.4)
        return True

    def _equipped_phuc_than_tid(self) -> int:
        for item in getattr(self, "equipped_items", []):
            tid = item.get("id", 0)
            if tid in PHUC_THAN_GEM_TIDS and item.get("damage", 0) < 250:
                return tid
        return 0

    def use_login_items(self):
        """Login: tu dung item co tid nam trong config.USE_LOGIN_ITEMS (template -> dung mọi acc),
        tuong tu decompose_junk_scrolls/donate_legion. 2 kieu (theo config, xem use_items.json):
          - qty None (chi ten): dung HET ca stack, TUNG CAI 1 (item chi cho 1/lenh, vd Tang O).
          - qty N: dung TOI DA N cai/login (co>N -> dung N de lai du; co<N -> dung het). Batch 255/lenh.
        use_slot: C2S 0x17 0f00 [slot][qty] 000000 [target] 00 (qty verify tu capture).
        Item danh dau "phuc_than" trong use_items.json KHONG nam o day (xem use_phuc_than_items -
        nhom nay dung theo dinh ky rieng, KHONG phai 1 lan luc login/kem theo cong tac bat/tat)."""
        cfg = {tid: v for tid, v in (getattr(config, "USE_LOGIN_ITEMS", {}) or {}).items()
               if not v.get("phuc_than")}
        self._use_items_from_cfg(cfg, "login")

    # --- THU CUOI (座騎): nang cap + boi duong. Opcode 79 = 0x4f. Xem KNOWLEDGE.md muc
    # "BOI DUONG THU CUOI" va documents/THU_CUOI.md. LUON BAT, khong co o tick trong setting. ---
    MOUNT_KIND_TEN = {1: "Cong", 2: "Tri", 3: "Phong", 4: "HP", 5: "SP"}
    MOUNT_ACK_WAIT = 3.0     # cho server xac nhan (S:079-002 / S:079-003)
    MOUNT_MAX_FEED = 400     # tran an toan so lenh boi duong / 1 lan login (chong lap vo han)
    MOUNT_GAP = 0.12         # nghi giua 2 vien - chi de khong doi goi server

    def _mount_item_name(self, tid: int):
        g = (_load_gamedata_items() or {}).get(int(tid)) or {}
        return g.get("name") or ("0x%04x" % tid)

    # --- THE DOI / hop chon qua (specialAbility 219 = EItemUseKind.Exchange) ---
    EXCHANGE_ACK_WAIT = 3.0

    def open_exchange_card(self, tid: int, index: int, cho_xac_nhan: bool = True) -> bool:
        """Mo MOT the doi `tid`, chon muc thu `index` (1-based).

        cho_xac_nhan=True  -> True khi tui THAT SU doi (dung cho luong tu dong: phai biet chac
                              da nhan duoc do moi tinh tiep).
        cho_xac_nhan=False -> True ngay khi GUI XONG goi. Tui do cua bot dung kieu nay, GIONG
                              use_slot/equip_item: no tu doi chieu trang thai truoc/sau
                              (_state_fp) de bao "xong" hay "da gui nhung chua thay doi".
                              Truoc day o day tra False khi het 3s cho -> hop thoai bao
                              "Khong gui duoc lenh (o trong / acc mat ket noi)" trong khi goi DA
                              gui va o VAN co 21 the -> user tuong bot hong (anh chup 10:0x).

        Goi RIENG, khong phai goi dung item thuong (Logic_Item.lua:2033):
            C:090-001 <兌換> +兌換物品ID(2) +選取數量(1) <<+選取索引(1)>>  = 0x5a sub01
        So muc phai chon = itemData.elementValue; cac hop kieu nay deu la 1.

        Client con chan truoc khi gui: `GetBagLeftCount(Bag) < elementValue` -> bao "tui day".
        Bot cung phai check, khong thi gui hut ma tuong da mo.
        """
        rows = (getattr(config, "EXCHANGE", {}) or {}).get(int(tid))
        if not rows:
            return False
        muc = next((r for r in rows if r["i"] == int(index)), None)
        if muc is None:
            return False
        slot = self._bag_slot_of(int(tid))
        if slot is None:
            return False
        nhan_tid, nhan_n = int(muc["id"]), int(muc["n"])
        cu = int((self.bag_counts or {}).get(nhan_tid, 0))
        the_cu = int((self.bag_counts or {}).get(int(tid), 0))
        try:
            self.send(0x5a, b"\x01\x00" + struct.pack("<HBB", int(tid), 1, int(index)))
        except OSError:
            return False
        if not cho_xac_nhan:
            return True
        t0 = time.time()
        while time.time() - t0 < self.EXCHANGE_ACK_WAIT:
            # Nhan bang HAI dau hieu: nhan duoc do MOI, HOAC the bi tru. Chi bam vao "nhan duoc
            # do" la du neu tui ao chuan, nhung tui ao tung lech (goi giam 0x17 sub09 mang so
            # luong ma truoc day bot tru 1) -> moc so sanh sai -> bot tuong that bai trong khi
            # server DA an the. Dau hieu "the bi tru" khong dinh loi do.
            if int((self.bag_counts or {}).get(nhan_tid, 0)) >= cu + nhan_n:
                return True
            if int((self.bag_counts or {}).get(int(tid), 0)) < the_cu:
                return True
            time.sleep(0.1)
        return False

    def _exchange_index_for(self, tid: int, want_tid: int):
        """Muc nao cua the `tid` cho ra `want_tid`? -> (index, so_luong) hoac None."""
        for r in (getattr(config, "EXCHANGE", {}) or {}).get(int(tid)) or []:
            if int(r["id"]) == int(want_tid):
                return int(r["i"]), int(r["n"])
        return None

    def _cards_giving(self, want_tid: int):
        """Cac the doi DANG CO TRONG TUI ma mo ra duoc `want_tid`.

        -> [(card_tid, index, so_luong_moi_lan, so_the_dang_co)], the cho NHIEU nhat dung truoc
        (mo it lan hon = it goi hon).
        """
        out = []
        for card_tid in (getattr(config, "EXCHANGE", {}) or {}):
            co = int((self.bag_counts or {}).get(int(card_tid), 0))
            if co <= 0:
                continue
            hit = self._exchange_index_for(card_tid, want_tid)
            if hit:
                out.append((int(card_tid), hit[0], hit[1], co))
        out.sort(key=lambda x: -x[2])
        return out

    def _mount_open_cards_for(self, want_tid: int, thieu: int, dang_co: int) -> int:
        """Mo the doi de bu `thieu` item `want_tid` - CHI KHI MO LA DU. Tra ve so luong sau do.

        User noi ro: "neu mo ma DU thi mo ra de nang cap".

        Bug that (log 10:04): thieu 142 nhung chi co 17 the (17*5 = 85, khong bao gio du) - bot
        van mo het 17 the. Toi da bo mat chu "ma du" trong yeu cau.

        Do KHONG mat (dan van nam trong tui, dung duoc cho lan nang cap sau) - cai mat la QUYEN
        CHON: the con doi duoc thu khac (vd 'Tui Toa Ky Dan'), mo roi thi het duong lui.

        Vi du dung (user): "dang co 33/50 -> thieu 17 -> co 10 the -> mo 4 cai de lay 20".
        """
        nguon = self._cards_giving(want_tid)
        co_the_bu = sum(moi_lan * co_the for _t, _i, moi_lan, co_the in nguon)
        if co_the_bu < thieu:
            if nguon:
                log.info("[%s] Thu cuoi: thieu %d '%s' nhung the doi chi bu duoc %d -> KHONG mo "
                         "(mo cung khong du, giu the lai)",
                         self._label, thieu, self._mount_item_name(want_tid), co_the_bu)
            return dang_co
        for card_tid, index, moi_lan, co_the in nguon:
            if thieu <= 0:
                break
            can_mo = -(-thieu // moi_lan)          # tran tren
            mo = min(can_mo, co_the)
            log.info("[%s] Thu cuoi: thieu %d '%s' -> mo %d/%d '%s' (moi cai duoc %d)",
                     self._label, thieu, self._mount_item_name(want_tid), mo, co_the,
                     self._mount_item_name(card_tid), moi_lan)
            duoc = 0
            for _ in range(mo):
                if not self.running or not self.open_exchange_card(card_tid, index):
                    break
                duoc += moi_lan
                time.sleep(self.MOUNT_GAP)
            if duoc < mo * moi_lan:
                log.warning("[%s] Thu cuoi: mo the '%s' khong tron (duoc %d/%d) - tui day?",
                            self._label, self._mount_item_name(card_tid), duoc, mo * moi_lan)
            dang_co += duoc
            thieu -= duoc
        return dang_co

    def _mount_wait_ack(self, ev) -> bool:
        """Cho server xac nhan bang EVENT chu khong poll.

        Truoc day vong cho poll moi 0.2s -> KE CA khi server tra loi tuc thi van phai doi het nhip
        poll. Cong voi MOUNT_GAP thi moi vien ton ~0.55s: 60 vien = 33s, nhin nhu bot bi DO
        (user bao). Dung Event thi chi ton dung thoi gian di-ve that cua goi."""
        return ev.wait(self.MOUNT_ACK_WAIT)

    def _on_mount_data(self, pkt: bytes):
        """S:079-001 <座騎資料> +cap(1) +diem(2)*6 +so trang bi(1) <<+do(16)>> +NPCID(2).

        Truoc day CHI lay diem INT (pkt[12:14] = kind 2) de tinh INT chon quan su. Nay luu CA CAP
        va CA 6 DIEM de con tu nang cap / boi duong thu cuoi (do_mount_upgrade)."""
        self.mount_level = pkt[9]
        self.mount_points = {
            k: int.from_bytes(pkt[10 + (k - 1) * 2:12 + (k - 1) * 2], "little")
            for k in range(1, 7) if 12 + (k - 1) * 2 <= len(pkt)
        }
        self._mount_base_int = pet_login_stats.mount_base_int(
            self.mount_points.get(2, 0), _load_pet_stat_data())
        self._refresh_char_int()
        self._refresh_char_agi()

    def _on_mount_level(self, pkt: bytes):
        """S:079-002 <設定座騎等級> +cap(1): server XAC NHAN nang cap thu cuoi xong."""
        self.mount_level = pkt[9]
        self._mount_level_ev.set()
        log.info("[%s] Thu cuoi: LEN CAP %d", self._label, self.mount_level)

    def _on_mount_point(self, pkt: bytes):
        """S:079-003 <設定座騎點數> +loai(1) +diem(2): server XAC NHAN boi duong xong.

        Diem la gia tri TUYET DOI moi -> bot KHONG tu doan cong don (1 vien = 1 diem chi la suy
        luan; cu doc thang so server bao thi khong bao gio lech)."""
        k = pkt[9]
        self.mount_points[k] = int.from_bytes(pkt[10:12], "little")
        ev = self._mount_point_ev.get(k)
        if ev is not None:
            ev.set()
        if k == 2:      # INT doi -> tinh lai INT char (dung de chon quan su)
            self._mount_base_int = pet_login_stats.mount_base_int(
                self.mount_points[k], _load_pet_stat_data())
            self._refresh_char_int()

    def _bag_slot_of(self, tid: int):
        """Slot tui dang chua tid (client gui BAG INDEX chu khong phai item id). None = khong co."""
        for slot, v in (self.bag_slots or {}).items():
            if v and int(v[0]) == int(tid) and int(v[1]) > 0:
                return int(slot)
        return None

    def mount_attr_level(self, kind: int):
        """Doi DIEM cong don -> CAP cua chi so, y het Mounts.GetAttributeProgress cua client:
        duyet tung cap, tru dan `need` cua cap do khi con du diem. Tra (cap, diem con du)."""
        grow = getattr(config, "MOUNTS_GROW", {}) or {}
        point = int((self.mount_points or {}).get(kind, 0))
        lv = 0
        for c in sorted(grow):
            a = (grow[c].get("attrs") or {}).get(kind)
            if not a:
                break
            need = a["need"]
            if point < need:
                break
            point -= need
            lv = c
        return lv, point

    def _mount_level_up_once(self) -> bool:
        """Mot lan nang cap thu cuoi. True = server DA xac nhan len cap."""
        grow = getattr(config, "MOUNTS_GROW", {}) or {}
        r = grow.get(self.mount_level)
        if not r or not r["up_item"]:
            return False                       # het cap (cap 15 co up_item = 0)
        if self.mount_level + 1 not in grow:
            return False
        co = int((self.bag_counts or {}).get(r["up_item"], 0))
        if co < r["up_count"]:
            # Thieu dan -> thu MO THE DOI trong tui cho du (vd "Ve Boi Duong Toa Ky" 0x7de7 ->
            # muc 1 = Tang Cap Ky Don x5). Chi mo DUNG SO CAN, khong mo het.
            co = self._mount_open_cards_for(r["up_item"], r["up_count"] - co, co)
        if co < r["up_count"]:
            log.info("[%s] Thu cuoi cap %d: can %d '%s' de len cap, dang co %d -> bo qua",
                     self._label, self.mount_level, r["up_count"],
                     self._mount_item_name(r["up_item"]), co)
            return False
        slot = self._bag_slot_of(r["up_item"])
        if slot is None:
            return False
        # KHONG tu kiem VANG: bot khong theo doi vang, ma gui hut thi server tu choi, KHONG mat gi.
        # Server xac nhan bang S:079-002 -> khong thay = thieu vang / cham tran VIP -> thoi.
        cu = self.mount_level
        self._mount_level_ev.clear()
        self.send(0x4f, b"\x03\x00" + bytes([slot & 0xFF]))
        if self._mount_wait_ack(self._mount_level_ev) and self.mount_level != cu:
            return True
        log.info("[%s] Thu cuoi: gui len cap %d->%d nhung server KHONG xac nhan "
                 "(thieu vang? cham tran VIP?) -> thoi", self._label, cu, cu + 1)
        return False

    def _mount_feed_once(self, kind: int) -> bool:
        """Mot lan boi duong chi so `kind`. True = server DA xac nhan diem tang."""
        grow = getattr(config, "MOUNTS_GROW", {}) or {}
        lv, _du = self.mount_attr_level(kind)
        # Luat CLIENT (Mounts.AttributeUp): het bang thi thoi, VA cap chi so KHONG duoc vuot cap
        # thu cuoi -> muon boi duong tiep phai nang cap thu cuoi truoc.
        if lv + 1 not in grow:
            return False
        if lv >= self.mount_level:
            return False
        a = (grow[lv + 1].get("attrs") or {}).get(kind)
        if not a or not a["item"]:
            return False
        slot = self._bag_slot_of(a["item"])
        if slot is None:
            return False
        cu = int((self.mount_points or {}).get(kind, 0))
        ev = self._mount_point_ev.setdefault(kind, threading.Event())
        ev.clear()
        self.send(0x4f, b"\x04\x00" + bytes([kind & 0xFF, slot & 0xFF]))
        return self._mount_wait_ack(ev) and int((self.mount_points or {}).get(kind, 0)) != cu

    @task_report("thu cuoi", PHASE_LOGIN_CHORE)
    def do_mount_upgrade(self):
        """LUON CHAY sau use_login_items(): nang cap thu cuoi roi boi duong 5 chi so.

        1) Du 'Tang Cap Ky Don' + vang -> nang cap (lap toi khi het dieu kien).
        2) Trong tui co Cong/Tri/Phong/HP/SP Ky Don va chi so do CHUA MAX -> dung het.

        "Chua max" = con cap tiep trong bang VA cap chi so < cap thu cuoi (luat cua client).
        Vi the phai nang cap TRUOC roi moi boi duong: nang cap mo them tran cho chi so.
        """
        if not (getattr(config, "MOUNTS_GROW", {}) or {}):
            log.info("[%s] Thu cuoi: thieu mounts_grow.json -> bo qua", self._label)
            return
        if not self.mount_level:
            log.info("[%s] Thu cuoi: chua nhan duoc S:079-001 (cap/diem) -> bo qua", self._label)
            return
        if self.state.in_battle:
            return
        lv0 = self.mount_level
        while self.running and self._mount_level_up_once():
            time.sleep(self.MOUNT_GAP)
        if self.mount_level != lv0:
            log.info("[%s] Thu cuoi: cap %d -> %d", self._label, lv0, self.mount_level)

        tong = 0
        for kind in (1, 2, 3, 4, 5):
            lv_cu, _ = self.mount_attr_level(kind)
            n = 0
            while self.running and n < self.MOUNT_MAX_FEED and self._mount_feed_once(kind):
                n += 1
                tong += 1
                time.sleep(self.MOUNT_GAP)
            if n:
                lv_moi, du = self.mount_attr_level(kind)
                log.info("[%s] Thu cuoi %s: dung %d vien -> cap %d->%d (du %d diem)",
                         self._label, self.MOUNT_KIND_TEN[kind], n, lv_cu, lv_moi, du)
        if not tong and self.mount_level == lv0:
            log.info("[%s] Thu cuoi: khong co gi de lam (cap %d, diem %s)",
                     self._label, self.mount_level,
                     {self.MOUNT_KIND_TEN[k]: self.mount_points.get(k, 0) for k in (1, 2, 3, 4, 5)})

    def use_phuc_than_items(self, use_normal=True, use_dai=False):
        """Dung dinh ky (KHONG phai 1 lan luc login) cac item danh dau "phuc_than": true trong
        use_items.json - CHI khi party bat cong tac "Su dung Phuc Than" (xem run_party_digioi.py,
        goi ham nay moi X phut thay vi 1 lan). Tach rieng khoi use_login_items() vi nhom item nay
        can dinh ky check lai (vd nhat/mua them giua chung), khong phai loai dung 1 lan roi thoi."""
        cfg = {tid: v for tid, v in (getattr(config, "USE_LOGIN_ITEMS", {}) or {}).items()
               if v.get("phuc_than")}
        # ITEM TIEU HAO (Phuc Than / Dai Phuc Than): client goc chi dung khi buff HET
        # (Role.player.data.godMission < 1). Bot theo huong do nhung nguong rong hon theo yeu cau
        # user: chi dung khi CON < PHUC_THAN_LOW, va toi da PHUC_THAN_USE_MAX cai/luot (truoc day
        # nha mu 25+50 cai moi 30 phut, khong xet con bao nhieu).
        # god_mission is None = server chua gui 0x18 sub0800 -> VAN dung (nhung van cap 10) de
        # khong mat tinh nang o server khong gui goi nay.
        self.phuc_than_pending = False   # da xu ly (ha co truoc khi lam, tranh lap vo han)
        _gm = self.god_mission
        if _gm is not None and _gm >= PHUC_THAN_LOW:
            log.info("[%s] Phuc Than con %d (>= %d) -> CHUA dung them item",
                     self._label, _gm, PHUC_THAN_LOW)
            cfg = {tid: v for tid, v in cfg.items() if tid in PHUC_THAN_GEM_TIDS}
        else:
            # Cap PHUC_THAN_USE_MAX la TONG (khong phai moi loai). Uu tien loai CO GIA TRI CAO
            # truoc (Dai Phuc Than > Phuc Than) = dung it item hon cho cung so luot buff.
            _budget = PHUC_THAN_USE_MAX
            _have = {}
            for _s, (_t, _n) in self.bag_slots.items():
                if _n > 0 and _t in cfg and _t not in PHUC_THAN_GEM_TIDS:
                    _have[_t] = _have.get(_t, 0) + _n
            _new = {}
            def _order(kv):
                try:
                    return PHUC_THAN_CONSUMABLE_ORDER.index(kv[0])
                except ValueError:
                    return len(PHUC_THAN_CONSUMABLE_ORDER)
            for _t, _v in sorted(cfg.items(), key=_order):
                if _t in PHUC_THAN_GEM_TIDS:
                    _new[_t] = _v
                    continue
                _take = min(_budget, _have.get(_t, 0))
                if _take > 0:
                    _new[_t] = dict(_v, qty=_take)
                    _budget -= _take
            cfg = _new
            if _gm is not None:
                log.info("[%s] Phuc Than con %d (< %d) -> dung them toi da %d cai (tong)",
                         self._label, _gm, PHUC_THAN_LOW, PHUC_THAN_USE_MAX)
        self._use_items_from_cfg(cfg, "phuc than dinh ky")
        self.discard_junk_items()

    # tid item RAC KHONG dung duoc, chi ton slot -> vut bo cho gon tui. 0x59f0 = Ngoc Hu (Ngoc Sieu
    # Phuc Than HET DO BEN sau khi dung se tu doi thanh item nay - KHONG check do ben, den gio la
    # thay Ngoc moi, con Ngoc Hu cu thi vut luon).
    DISCARD_JUNK_TIDS = {0x59f0}

    def discard_item(self, slot: int, qty: int = 1) -> bool:
        """Vut bo item trong tui. C2S 0x17 sub=0300 [slot 1B][qty 4B LE]. Xac nhan discard.pcap:
        server ack 0x17 sub=0900 (echo slot+qty) + 0x17 sub=1a00 [tid 2B LE][01] (bao tid da vut)."""
        if not self.running:
            return False
        self.send(0x17, b"\x03\x00" + bytes([slot & 0xFF]) + int(qty).to_bytes(4, "little"))
        return True

    def discard_junk_items(self):
        """Quet tui, vut bo HET cac tid trong DISCARD_JUNK_TIDS (vd Ngoc Hu). Khong confirm rieng
        (server KHONG gui lai 0x16 refresh bag) -> tu tru bag_slots ngay sau khi gui lenh."""
        if not (getattr(self, "auto_bag_clean", True)
                and getattr(self, "auto_discard_junk", True)):
            return
        total = 0
        for slot, (tid, cnt) in list(self.bag_slots.items()):
            if cnt > 0 and tid in self.DISCARD_JUNK_TIDS:
                self.discard_item(slot, cnt)
                _nm = (_load_gamedata_items().get(tid) or {}).get("name", "")
                log.info("[%s] vut bo item rac slot=%d tid=0x%04x ('%s') x%d",
                         self._label, slot, tid, _nm, cnt)
                self.bag_slots.pop(slot, None)
                total += 1
                time.sleep(0.3)
        if total:
            log.info("[%s] Vut bo item rac: tong %d slot", self._label, total)

    def log_bag(self):
        """In tui theo SLOT, moi slot ghi ro la item KHAI (items_known.json) / HOC (probe) / CHUA BIET.
        De m doi chieu xem bot hieu dung khong, roi dien tiep items_known.json."""
        if not self.bag_slots:
            log.info("[%s] bag: chua nhan S2C 0x16 inventory", self._label)
            return
        known = _load_known_items()
        gdata = _load_gamedata_items()
        learned = self._learned()
        n_known = n_gdata = n_learn = n_unknown = 0
        _la = []      # item bot CHUA BIET - phan duy nhat con dang in ra
        for slot in sorted(self.bag_slots):
            tid, cnt = self.bag_slots[slot]
            k = known.get(tid); g = gdata.get(tid); lv = learned.get(str(tid)) or {}
            if k:
                n_known += 1
                eff = [s for s in ["+%dHP" % k["hp"] if k.get("hp") else "",
                                   "+%dSP" % k["sp"] if k.get("sp") else "", k.get("type", "")] if s]
                tag = "KHAI: %s %s" % (k.get("name", ""), " ".join(eff) or "(?)")
            elif g:
                n_gdata += 1
                eff = " ".join([s for s in ["+%dHP" % g["hp"] if g.get("hp") else "",
                                            "+%dSP" % g["sp"] if g.get("sp") else ""] if s])
                bt = " [CHI TRONG TRAN]" if g.get("battle") else ""
                tag = "gamedata: %s %s%s" % (g.get("name", ""), eff, bt)
            elif lv.get("hp", 0) > 0 or lv.get("sp", 0) > 0:
                n_learn += 1
                tag = "HOC: +%dHP +%dSP" % (lv.get("hp", 0), lv.get("sp", 0))
            elif lv.get("none"):
                n_learn += 1; tag = "HOC: khong hoi (none)"
            elif lv.get("unusable"):
                n_learn += 1; tag = "HOC: ko dung duoc"
            else:
                n_unknown += 1
                _la.append("slot %d: id=0x%04x x %d" % (slot, tid, cnt))
        # KHONG in ca tui nua: gio da co CUA SO TUI DO rieng (nut 🎒 canh nut Skill), in het ra log
        # chi lam troi log - moi acc ~25 dong moi lan login, 5 acc/party.
        # VAN GIU canh bao item CHUA BIET: do la cach lau nay phat hien item moi cua game de bo sung
        # du lieu. Khong co item la thi im hoan toan.
        if _la:
            log.warning("[%s] TUI: %d item bot CHUA BIET (%d/%d slot) -> %s",
                        self._label, n_unknown, n_unknown, len(self.bag_slots), "; ".join(_la[:10]))

    # ---------- HOI MAU: closed-loop tren HP/SP live (S2C 0x08) + self-calibrate ----------
    def _heal_threshold(self, kind: str) -> float:
        """Nguong hoi mau cho acc nay. kind: hp_char/sp_char/hp_pet/sp_pet.
        Uu tien config.ACCOUNT_HEAL[username][kind]; thieu -> HP_THRESHOLD/SP_THRESHOLD chung."""
        glob = getattr(config, "SP_THRESHOLD", 0.0) if kind.startswith("sp") \
            else getattr(config, "HP_THRESHOLD", 0.4)
        over = getattr(config, "ACCOUNT_HEAL", {}).get(self._username, {})
        return over.get(kind, glob)

    def use_slot(self, slot: int, target: int = 0, qty: int = 1) -> bool:
        """Dung item o SLOT. C2S 0x17: 0f 00 [slot 1B][qty 1B] 00 00 00 [target 1B] 00.
        qty = so luong dung 1 lenh (1..255; verify capture: dung 22 -> byte=0x16). Heal qty=1.
        Server confirm S2C 0x17 sub=09 (= dung duoc). Tra False neu slot het."""
        rec = self.bag_slots.get(slot)
        if rec is not None and rec[1] <= 0:
            return False
        qty = max(1, min(int(qty), 255))
        payload = b"\x0f\x00" + bytes([slot & 0xFF, qty]) + b"\x00\x00\x00" + bytes([target & 0xFF]) + b"\x00"
        self.send(0x17, payload)
        if rec is not None:
            tid = int(rec[0])
            nm = (_load_gamedata_items().get(tid) or {}).get("name") or ("0x%04x" % tid)
            self.activity_log.appendleft({"type": "use", "time": time.strftime("%H:%M:%S"),
                                          "item": str(nm).strip(), "item_id": tid,
                                          "qty": qty, "target": "pet" if target else "character"})
        return True

    def equip_item(self, slot: int) -> bool:
        """Trang bi item o SLOT (KHAC use_slot: item deo len nguoi, khong phai tieu hao). C2S 0x17:
        0b 00 [slot 1B] (chi 3 byte, khong qty/target - xac nhan tu capture that). Server ack S2C
        0x17 sub=11 echo lai dung slot. KHONG co goi nao bao lai "da deo vao dau"/cap nhat tui do
        (0x16 KHONG gui lai) -> caller (vd use_phuc_than_items) tu coi nhu thanh cong + tu don
        bag_slots (giong use_slot lam sau khi dung), KHONG dua vao ket qua tra ve de biet chac."""
        rec = self.bag_slots.get(slot)
        if rec is not None and rec[1] <= 0:
            return False
        self.send(0x17, b"\x0b\x00" + bytes([slot & 0xFF]))
        return True

    def equip_pet_item(self, pet_idx: int, slot: int) -> bool:
        """Trang bi item o SLOT cho PET (vo tuong) thu `pet_idx` (1..4).

        KHAC HAN lenh cua nhan vat: C:023-011 <穿上玩家裝備> chi co [bagIndex] - KHONG co cho de
        noi "cho con nao". Do pet la goi RIENG:
            C:023-017 <穿上武將裝備> +武將索引(1) +背包索引(1)
        Server bao lai bang S:023-023 <武將穿上裝備> [武將索引][背包索引] (xem _on_pet_equip).
        """
        rec = self.bag_slots.get(int(slot))
        if rec is not None and rec[1] <= 0:
            return False
        try:
            self.send(0x17, b"\x11\x00" + bytes([int(pet_idx) & 0xFF, int(slot) & 0xFF]))
        except OSError:
            return False
        return True

    # Ma chi so trong du lieu ITEM (Data_ItemData.lua GetAttributeName + TextData_C.dat):
    #   207 HP | 208 SP | 210 Atk | 211 Def | 212 Int | 214 Agi | 217 Thuyen toc
    #   218 The chat (HPx) | 219 Nang luong (SPx)
    ATTR_HP, ATTR_SP, ATTR_ATK, ATTR_DEF = 207, 208, 210, 211
    ATTR_INT, ATTR_AGI, ATTR_HPX, ATTR_SPX = 212, 214, 218, 219

    def char_stat_full(self) -> dict:
        """{ma EAttribute: tong} chi so char = GOC + CONG TU DO. {} neu chua nhan goi 0x05 sub03.

        27 Int | 28 Atk | 29 Def | 30 Agi | 31 Hpx | 32 Spx.
        INT/AGI thi lay thang self.char_int/char_agi neu co - hai so do da cong them ca suu tap,
        the va thu cuoi (xem _refresh_char_agi), khong chi rieng trang bi.
        """
        base = getattr(self, "char_base", None) or {}
        if not base:
            return {}
        eq = dict(getattr(self, "char_equip", None) or {})
        # Server ban lai phan CONG TU DO qua 0x08 sub0100 moi khi doi (id 210/211/212/214/218/219
        # - xem EQUIP_ATTR). So do MOI NHAT va DA gom du linh da, LONG, bo do. char_equip chi la
        # anh chup luc login -> thay do xong la cu. Uu tien so server.
        attrs = getattr(self, "char_attrs", None) or {}
        for _ma, _dich in ((210, 28), (211, 29), (212, 27), (214, 30), (218, 31), (219, 32)):
            if _ma in attrs:
                eq[_dich] = int(attrs[_ma])
        # SO AM LA BINH THUONG - user xac nhan 26/08: "so am duoc, vi item co am ma".
        # Vd char phap su: ATK -6, DEF -2 (gay tang INT nhung tru ATK/DEF). TUYET DOI khong kep
        # ve 0 hay coi la loi parse - lam vay la bao sai so cho user.
        out = {k: int(base.get(k, 0)) + int(eq.get(k, 0)) for k in base}
        if getattr(self, "char_int", None) is not None:
            out[27] = int(self.char_int)
        if getattr(self, "char_agi", None) is not None:
            out[30] = int(self.char_agi)
        return out

    # ---------------- TANG DIEM TIEM NANG (xem KNOWLEDGE.md muc 7o) ----------------

    def attr_point_left(self):
        """So diem tiem nang CHUA CONG. None = chua biet (chua nhan goi login/0x08).

        Hai nguon, deu la `EAttribute.AttrPoint = 38`:
          - goi login `0x05 sub0300` (+28) -> `self.attr_point`,
          - server day rieng `0x08 sub0100` moi khi doi -> `char_attrs[38]` (MOI HON, uu tien).
        """
        v = (getattr(self, "char_attrs", None) or {}).get(ATTR_POINT)
        if v is None:
            v = getattr(self, "attr_point", None)
        return None if v is None else max(0, int(v))

    def char_diem_goc(self) -> dict:
        """{ma EAttribute: diem GOC} - phan CONG DIEM tac dong toi. {} neu chua co goi login.

        KHAC `char_stat_full()` (goc + trang bi/thu cuoi/the/suu tap): rule tang diem chot theo
        GOC (user 31/08), vi do la thu diem thuc su tac dong toi va khong bi thao/deo do lam nhieu.
        """
        return dict(getattr(self, "char_base", None) or {})

    # Tab skill phai gui `C:028-005` (bo cuc khac han) thay vi `C:028-001` - xem KNOWLEDGE 7q.
    SKILL_TAB_028_005 = ("Turn2",)

    # EAttribute.Element = 24 (1 Dia, 2 Thuy, 3 Hoa, 4 Phong) - `RoleController.lua:36`.
    ATTR_ELEMENT = 24

    def he_nhan_vat(self):
        """He cua nhan vat (1..4) hoac None.

        `S:008-013` co mang he, NHUNG goi do server khong gui san luc login - phai vao bang skill
        moi co. Trong khi `char_attrs[24]` da co san tu `0x08 sub0100` ngay luc login. Thieu cai
        nay thi dialog Skill khong biet mo tab he nao (user 01/09: "t bao khi mo skill thi tu chon
        tab co he cua nhan vat co ma"), va tinh gia hoc skill khac he cung sai.
        """
        if self.char_element:
            return int(self.char_element)
        v = (self.char_attrs or {}).get(self.ATTR_ELEMENT)
        return int(v) if v else None

    def skill_point_left(self):
        """Diem skill con lai. `S:008-013` la nguon chot; `char_attrs[37]` la du phong."""
        if self.char_skill_point is not None:
            return int(self.char_skill_point)
        v = (self.char_attrs or {}).get(37)
        return int(v) if v is not None else None

    def skill_cap_hien_tai(self, sid: int) -> int:
        return int((self.char_skill_lv or {}).get(int(sid), 0) or 0)

    def _tien_quyet(self, sid: int):
        """[skill tien quyet] cua `sid`. Client chi can HOC MOT trong so nay (cap > 0) la cho cong
        (`checkCount > 0 and checkCount == failCount` moi chan)."""
        info = (getattr(config, "SKILL_INFO", {}) or {}).get(int(sid)) or {}
        return [x for x in (int(info.get("pre") or 0), int(info.get("pre2") or 0)) if x]

    def _tien_quyet_da_du(self, sid: int) -> bool:
        tq = self._tien_quyet(sid)
        return (not tq) or any(self.skill_cap_hien_tai(x) > 0 for x in tq)

    def _chuoi_hoc_toi(self, sid: int, _sau=None):
        """Chuoi skill PHAI HOC TRUOC de mo duong toi `sid` (khong ke chinh no), re nhat.

        User chot 01/09: "neu de AAA cap 5 ma AAA chua duoc hoc, can hoc BBB truoc, de hoc BBB thi
        can hoc CCC truoc thi m cu lan theo skill tree de tien dan den skill mong muon".
        Moi mat xich chi can CAP 1 (client chi kiem `> 0`). Nhanh nao re hon thi di nhanh do.
        """
        sid = int(sid)
        _sau = _sau or set()
        if sid in _sau:                    # vong lap du lieu -> bo nhanh nay
            return None
        if self._tien_quyet_da_du(sid):
            return []
        re_nhat = None
        for tq in self._tien_quyet(sid):
            info = (getattr(config, "SKILL_INFO", {}) or {}).get(tq)
            if not info or not info.get("tree") or info.get("tree") in self.SKILL_TAB_028_005:
                continue
            trc = self._chuoi_hoc_toi(tq, _sau | {sid})
            if trc is None:
                continue
            duong = trc + [tq]
            # gia = tong learnPt cua ca chuoi (moi mat xich chi hoc cap 1)
            gia = sum((getattr(config, "SKILL_INFO", {}).get(x) or {}).get("learnPt", 0)
                      for x in duong)
            if re_nhat is None or gia < re_nhat[0]:
                re_nhat = (gia, duong)
        return None if re_nhat is None else re_nhat[1]

    def _skill_co_the_nang(self, sid: int, den_cap: int):
        """(chi_phi, ly_do_tu_choi). chi_phi None = KHONG nang duoc."""
        info = (getattr(config, "SKILL_INFO", {}) or {}).get(int(sid))
        if not info:
            return None, "khong co trong skills_data.json"
        tab = info.get("tree")
        if not tab:
            # Khong biet skill nay o tab nao -> co the la skill 2 chuyen/dac ky -> KHONG dung toi.
            return None, "khong ro thuoc tab nao (chi nang skill cua cay skill thuong)"
        if tab in self.SKILL_TAB_028_005:
            return None, "skill 2 chuyen (phai dung C:028-005, bot chua ho tro)"
        cap = self.skill_cap_hien_tai(sid)
        den_cap = int(den_cap)
        max_lv = int(info.get("maxLv") or 0)
        if max_lv and den_cap > max_lv:
            den_cap = max_lv                     # cat bot thay vi bo han: nang toi tran la hop y
        if den_cap <= cap:
            return None, "da dat cap %d" % cap
        can_lv = int(info.get("needLv") or 0)
        cua_toi = int(getattr(self, "char_level", 0) or 0)
        if can_lv and cua_toi and cua_toi < can_lv:
            return None, "can level %d (dang %d)" % (can_lv, cua_toi)
        if cap == 0 and not self._tien_quyet_da_du(sid):
            return None, "chua hoc skill tien quyet"
        # GIA: cap 0->1 an `learnPt` (GAP DOI neu KHAC HE), cac cap sau moi cap an `lvUpPt`.
        lvup = int(info.get("lvUpPt") or 1)
        if cap == 0:
            gia = int(info.get("learnPt") or 0)
            el = int(info.get("element") or 0)
            _he = self.he_nhan_vat()
            if 1 <= el <= 4 and _he and el != _he:
                gia *= 2
            gia += (den_cap - 1) * lvup
        else:
            gia = (den_cap - cap) * lvup
        return (gia, den_cap), None

    def nang_skill_char(self, rules, de_danh: int = 0):
        """Tu nang skill NHAN VAT theo bang rule (tu tren xuong). Tra (da_nang, thong_bao).

        `rules` = [(skill_id, cap_dich), ...]. `de_danh` = so diem GIU LAI khong tieu.
        Gui MOT goi `C:028-001` chua NHIEU skill: `[01 00] << [id u16][CAP DICH u8] >>` - luu y
        gui CAP DICH chu khong phai so cap cong them (nguoc voi `C:008-001` tang diem tiem nang).
        Xem KNOWLEDGE muc 7q.
        """
        con = self.skill_point_left()
        if con is None:
            return [], "chua doc duoc diem skill (chua nhan S:008-013)"
        ngan_sach = max(0, int(con) - max(0, int(de_danh or 0)))
        if ngan_sach <= 0:
            return [], "con %s diem, de danh %s -> khong tieu" % (con, de_danh)
        chon, bo_qua = [], []
        # Mat xich vua xep vao goi phai duoc coi la DA HOC ngay, neu khong thi mat xich ke tiep van
        # bi cham "chua hoc tien quyet" (CCC hoc roi ma BBB van bao thieu). Ghi thang vao
        # `char_skill_lv`: chung THUC SU nam trong goi sap gui, va `S:008-013` cua server se chot lai.
        def _danh_dau(_s, _cap):
            self.char_skill_lv[int(_s)] = max(self.skill_cap_hien_tai(_s), int(_cap))
            # VA CHO VAO DANH SACH SKILL DUNG TRONG TRAN. `char_skill_lv` chi la CAP; con cai
            # battle doc de chon chieu la `state.skills_char`, va no CHI duoc nap luc login (tu
            # `0x05` / bar `0x28`). Hoc mot skill MOI giua phien ma khong them vao day thi bot danh
            # het tran nay sang tran khac bang bo skill cu - dung den lan login sau moi thay.
            #
            # `S:008-013 <設定主角技能>` la goi server tra danh sach skill, nhung no CHI toi khi mo
            # bang skill trong game - ca phien 08/09 khong nhan duoc lan nao (0 dong "Skill nhan
            # vat:"), nen khong the trong cho no tu cap nhat ho.
            #
            # User 08/09: "hoc skill trong bot thi vao battle ko thay skill vua hoc... khi hoc skill
            # (user hoc hay bot tu hoc theo auto) thi m cung cap nhat lai battle".
            try:
                if int(_s) not in self.state.skills_char:
                    self.state.skills_char.append(int(_s))
                    log.info("[%s] Skill MOI '%s' -> them vao bo skill danh tran ngay (khong doi "
                             "login lai)", self._label, self._ten_skill(int(_s)))
            except Exception:
                pass

        # HOAN THANH DONG TREN MOI XUONG DONG DUOI (user chot 01/09: "da bao lam nhu ben point
        # thi phai hoan thanh duoc dong tren thi moi xuong dong duoi chu").
        # Truoc day rule nao thieu diem thi BO QUA va diem roi xuong rule duoi (re hon) -> "Hoa
        # Tien de tan cuoi ma da nang len 2 roi, tu Hoi Sinh xuong duoi la chua hoc".
        # PHAN BIET hai loai ly do khong lam duoc:
        #   - THIEU DIEM        -> tieu toi da cho dong nay roi DUNG HAN (tich tiep lan login sau)
        #   - KHONG BAO GIO DAT -> di tiep xuong dong duoi, neu khong thi mot dong hong se chan
        #     vinh vien moi dong sau (vd rule dat skill can level 90 ma acc dang level 30)
        for sid, den_cap in rules or ():
            sid = int(sid)
            # CHUA DU TIEN QUYET -> LAN THEO CAY: hoc dan CCC -> BBB roi moi toi AAA. Moi mat xich
            # chi hoc CAP 1 (du de mo duong), phan con lai danh cho chinh skill dich.
            if self.skill_cap_hien_tai(sid) == 0 and not self._tien_quyet_da_du(sid):
                _duong = self._chuoi_hoc_toi(sid)
                if _duong is None:
                    bo_qua.append("%s: khong lan duoc duong hoc (thieu skill tien quyet)"
                                  % self._ten_skill(sid))
                    continue                      # khong bao gio dat -> xuong dong duoi
                _het_diem = False
                for _tq in _duong:
                    if self.skill_cap_hien_tai(_tq) > 0:
                        continue
                    _kq, _ld = self._skill_co_the_nang(_tq, 1)
                    if _kq is None:
                        bo_qua.append("%s: can hoc truoc '%s' (%s)"
                                      % (self._ten_skill(sid), self._ten_skill(_tq), _ld))
                        _het_diem = None          # mat xich HONG -> coi nhu khong bao gio dat
                        break
                    if _kq[0] > ngan_sach:
                        bo_qua.append("%s: dang hoc dan, con thieu diem cho '%s' (can %d, con %d)"
                                      % (self._ten_skill(sid), self._ten_skill(_tq),
                                         _kq[0], ngan_sach))
                        _het_diem = True
                        break
                    ngan_sach -= _kq[0]
                    _danh_dau(_tq, 1)
                    chon.append((_tq, 1, _kq[0]))
                if _het_diem is True:
                    break                         # DUNG: dong nay chua xong thi khong xuong duoi
                if _het_diem is None:
                    continue
            ket_qua, ly_do = self._skill_co_the_nang(sid, den_cap)
            if ket_qua is None:
                if ly_do and not ly_do.startswith("da dat cap"):
                    bo_qua.append("%s: %s" % (self._ten_skill(sid), ly_do))
                continue                          # da dat cap / thieu level / tab la -> di tiep
            gia, cap_that = ket_qua
            if gia > ngan_sach:
                # Ha cap dich xuong cho vua ngan sach roi DUNG: dong nay chua toi dich, lan login
                # sau di tiep. (Hoc MOI thi khong chia nho duoc: thieu `learnPt` la khong hoc noi.)
                _ha = cap_that - 1
                while _ha > self.skill_cap_hien_tai(sid):
                    _kq, _ = self._skill_co_the_nang(sid, _ha)
                    if _kq and _kq[0] <= ngan_sach:
                        break
                    _ha -= 1
                if _ha <= self.skill_cap_hien_tai(sid):
                    bo_qua.append("%s: can %d diem, con %d -> DUNG, de danh cho dong nay"
                                  % (self._ten_skill(sid), gia, ngan_sach))
                    break
                gia, cap_that = self._skill_co_the_nang(sid, _ha)[0]
                ngan_sach -= gia
                chon.append((sid, cap_that, gia))
                _danh_dau(sid, cap_that)
                bo_qua.append("%s: moi len cap %d/%d -> DUNG, cac dong duoi cho luot sau"
                              % (self._ten_skill(sid), cap_that, int(den_cap)))
                break
            ngan_sach -= gia
            chon.append((sid, cap_that, gia))
            _danh_dau(sid, cap_that)
        if not chon:
            return [], "; ".join(bo_qua) if bo_qua else "khong co skill nao can nang"
        goi = b"".join(struct.pack("<HB", sid, cap) for sid, cap, _g in chon)
        self.send(0x1c, b"\x01\x00" + goi)
        for sid, cap, gia in chon:
            log.info("[%s] Nang skill '%s' -> cap %d (ton %d diem)",
                     self._label, self._ten_skill(sid), cap, gia)
        if self.char_skill_point is not None:
            self.char_skill_point = ngan_sach + max(0, int(de_danh or 0))
        if bo_qua:
            log.info("[%s] Nang skill: bo qua %s", self._label, "; ".join(bo_qua))
        return chon, "; ".join(bo_qua)

    def _ten_skill(self, sid: int) -> str:
        info = (getattr(config, "SKILL_INFO", {}) or {}).get(int(sid)) or {}
        return info.get("name") or ("0x%04x" % int(sid))

    def add_attr_point(self, kind: int, add: int) -> bool:
        """Cong `add` diem vao chi so `kind` (ma EAttribute) cho NHAN VAT CHINH.

        `C:008-001 <設定屬性> +武將索引(2) +種類(1) +數值(4) +參數(1)` = `0x08 sub0100`.
        Chi so 武將索引 = 0 la nhan vat chinh (pet thi ghi `role.index` - bot khong lam pet).
        數值 la SO DIEM CONG THEM, khong phai gia tri dich.

        Client chan san (`UI/UIStatus.lua:2009`): DANG TRONG TRAN thi khong cong duoc. Caller phai
        tu cho het tran; o day chi chan cho chac de khong ban goi vo ich.

        CAU TRUC DA DUOC CAPTURE XAC NHAN (`int.pcap`, tang INT id=0x1b): goi that la
        `01 00 | 00 00 | 1b | 01 00 00 00 | 00` = sub + index(2)=0 + kind(1) + value(i32) + arg(1),
        khop y het bo cuc trong `protocal.lua`. (Ham `increase_stat` cu dung chinh capture nay
        nhung khong ai goi va khong co mot rang buoc nao - da gop vao day.)
        """
        try:
            add = int(add)
            kind = int(kind)
        except (TypeError, ValueError):
            return False
        if add <= 0 or kind not in ATTR_CODE_TO_KEY:
            return False        # client cung `if add > 0` moi gui (Logic/Status.lua:389)
        if self.in_combat(idle_secs=1.0):
            log.warning("[%s] Cong diem: DANG TRONG TRAN -> khong cong duoc (client cung chan)",
                        self._label)
            return False
        con = self.attr_point_left()
        if con is not None and add > con:
            log.warning("[%s] Cong diem: muon cong %d vao %s nhung chi con %d diem -> bo",
                        self._label, add, ATTR_CODE_TO_TEN.get(kind, kind), con)
            return False
        try:
            self.send(0x08, b"\x01\x00" + struct.pack("<HBiB", 0, kind, add, 0))
        except OSError:
            return False
        # TRU NGAY tai cho: server co gui lai `0x08 sub0100` nhung khong tuc thi, ma caller
        # thuong cong LIEN TIEP nhieu o -> doc lai so cu se tuong con du diem va tieu qua tay.
        if con is not None:
            self.char_attrs[ATTR_POINT] = max(0, con - add)
            self.attr_point = self.char_attrs[ATTR_POINT]
        # Diem GOC cung tang ngay - rule duyet cac dong sau doc `char_diem_goc()`.
        try:
            self.char_base[kind] = int((self.char_base or {}).get(kind, 0)) + add
        except Exception:
            pass
        log.info("[%s] Cong diem: +%d %s (con lai %s)", self._label, add,
                 ATTR_CODE_TO_TEN.get(kind, kind),
                 "?" if con is None else con - add)
        return True

    def char_equip_bonus(self) -> dict:
        """{ma chi so: tong cong them} tu DO DANG MAC cua nhan vat. {} neu chua du du lieu.

        Dung chung ham voi pet (pet_login_stats.equipment_bonus) - no da tinh du ca linh da,
        he trang bi va bo do (suit). Bot chi can dung ThingData ma no can.
        """
        eq = [x for x in (self.equipped_items or []) if x.get("id")]
        if not eq:
            return {}
        data = _load_pet_stat_data()
        if not data:
            return {}
        rec = {"equipment": [{"id": x["id"], "element": x.get("element", 0),
                              "element_value": x.get("element_value", 0),
                              "stone_attr": x.get("stone_attr", 0),
                              "stone_lv": x.get("stone_lv", 0)} for x in eq]}
        try:
            # He cua char: EAttribute.Element = 24 (Controller_RoleController.lua), bot da thu
            # san vao char_attrs. Sai he thi chi mat phan cong THEM cua do cung he, khong hong.
            _he = int((getattr(self, "char_attrs", None) or {}).get(24, 0) or 0)
            return pet_login_stats.equipment_bonus(rec, data, _he)
        except Exception as e:
            log.debug("[%s] char_equip_bonus loi: %s", self._label, e)
            return {}

    # Ma phan CONG TU DO do SERVER tinh san (Controller_RoleController.lua EAttribute):
    #   207 EquipMaxHp | 208 EquipMaxSp | 210 EquipAtk | 211 EquipDef | 212 EquipInt
    #   214 EquipAgi | 218 EquipHpx | 219 EquipSpx
    EQUIP_ATTR = (207, 208, 210, 211, 212, 214, 218, 219)

    def _recalc_char_equip_stats(self):
        """Cap nhat phan CONG TU DO sau khi thay do.

        `_char_equip_agi`/`_char_equip_int` truoc day CHI duoc dat MOT LAN luc login (goi 0x05)
        -> thay do xong chi so va nut "Check AGI" dung nguyen so cu (user bao 26/08).

        UU TIEN SO CUA SERVER (char_attrs id 210/212/214, ban qua 0x08 sub0100). So do la tong
        server tinh, DA gom du linh da, LONG, bo do, moi thu.
        Tu tinh bang char_equip_bonus() CHI la duong lui: ham do (pet_login_stats.equipment_bonus)
        khong biet LONG (EItemKind.Feather) -> ra so THIEU. Ghi de so dung cua server bang so
        thieu do la lam hong chi so (user chi ra 26/08: "m tinh chi so nhan vat kieu gi, no phai
        lay duoc ca thong tin long chu").
        """
        attrs = getattr(self, "char_attrs", None) or {}
        co_server = self.ATTR_AGI in attrs or self.ATTR_INT in attrs
        if co_server:
            if self.ATTR_AGI in attrs:
                self._char_equip_agi = int(attrs[self.ATTR_AGI])
            if self.ATTR_INT in attrs:
                self._char_equip_int = int(attrs[self.ATTR_INT])
        else:
            b = self.char_equip_bonus()
            if not b:
                return
            log.debug("[%s] chua co so EquipAgi/EquipInt tu server -> tu tinh (CHUA co LONG)",
                      self._label)
            self._char_equip_agi = int(b.get(self.ATTR_AGI, 0))
            self._char_equip_int = int(b.get(self.ATTR_INT, 0))
        self._refresh_char_agi()
        self._refresh_char_int()

    def _cap_nhat_do_trong_ban_ghi_pet(self, follow: int, fit: int, tid=None, td=None):
        """Sua o do thu `fit` trong ban ghi login cua pet `follow` (tid=None = go do ra).

        VI SAO CAN: `pet_stats()` tinh phan cong cua trang bi tu `pet_login_records[follow]
        ["equipment"]` - ban ghi den tu goi pet-list LUC LOGIN. Ma KHONG CO lenh nao xin lai goi
        do (da tra het C:015-*: chi co them/duoi/doi ten/gui xe...). Nen thay do cho pet xong ma
        khong sua ban ghi thi CHI SO PET DUNG YEN toi tan lan login sau - dung hien tuong user
        bao 26/08: "thay cho char thi update luon, thay cho pet thi khong".
        Ben CHAR khong dinh loi nay vi co duong rieng `_recalc_char_equip_stats()`.

        ThingData trong tui (`bag_items`) va trong ban ghi pet dung Y HET 5 khoa
        (id/element/element_value/stone_attr/stone_lv) nen chep thang sang duoc, giu nguyen ca
        linh da - khong phai uoc tinh tu ban mau item.
        """
        if not fit or not (1 <= int(fit) <= 6):
            return
        # HAI ban ghi RIENG cung mo ta pet nay, deu la ket qua `parse_record` nhung la HAI object
        # khac nhau - va moi cai nuoi mot cho hien thi khac nhau:
        #   pet_login_records[follow] -> pet_stats()                  -> chi so trong TUI DO
        #   _active_pet_login         -> _refresh_active_pet_login_stats() -> `pet_agi`, tuc nut
        #                                                                "Check AGI"
        # Va chi con DANG XUAT CHIEN moi co ban ghi thu hai. Sua mot cai thi cho kia dung so cu -
        # dung hien tuong user bao 26/08: "chi so trong tui do update roi, cho check agi chua".
        rec = (getattr(self, "pet_login_records", None) or {}).get(int(follow))
        active = getattr(self, "_active_pet_login", None)
        if not isinstance(active, dict) or int(active.get("marker", -1) or -1) != int(follow):
            active = None
        dsach = [r for r in (rec, active) if isinstance(r, dict)]
        # Cung MOT object (neu sau nay gop lai) thi chi sua mot lan.
        dsach = [r for i, r in enumerate(dsach) if all(r is not x for x in dsach[:i])]
        if not dsach:
            return
        trong = {"id": 0, "element": 0, "element_value": 0, "stone_attr": 0, "stone_lv": 0}
        # `id` LUON ghi tu tid. Truoc day ham nay tu tra ThingData theo o tui roi lay ca `id` tu
        # do - o tui luc ay DA bi ghi de bang mon CU nen tra ra rong -> ghi thanh O TRONG. Ket qua
        # dung nhu user do duoc: "chi go do cu ra, chua lap do moi vao" (AGI 85 -> 89 thay vi 87).
        # Khong co ThingData thi van ghi id: mat phan linh da/he, nhung con hon mat ca mon.
        if tid:
            moi = dict(trong)
            moi["id"] = int(tid)
            if isinstance(td, dict) and int(td.get("id", 0) or 0) == int(tid):
                for k in ("element", "element_value", "stone_attr", "stone_lv"):
                    moi[k] = td.get(k, 0)
            else:
                log.debug("[%s] do pet: khong co ThingData cua 0x%04x -> chi tinh ban mau item",
                          self._label, int(tid))
        else:
            moi = dict(trong)
        for r in dsach:
            eq = r.get("equipment")
            if isinstance(eq, list) and len(eq) >= 6:
                eq[int(fit) - 1] = dict(moi)
        if active is not None:
            # `pet_agi` chi doi khi tinh lai - nut "Check AGI" doc thang bien do.
            self._refresh_active_pet_login_stats()

    def _on_unequip_done(self, fit_pos: int, follow: int = 0, o_tui: int = None):
        """DA COI XONG 1 mon (server xac nhan). Xoa khoi bang do dang mac + bump _equip_seq, VA
        TU BO MON DO VAO TUI dung o server chi (`o_tui`).

        SERVER KHONG GUI 023-008 cho mon vua coi - CHINH CLIENT chuyen no vao tui
        (`protocolTable[23][16]`):
            local fitType  = data:ReadByte();
            local bagIndex = data:ReadByte();
            Item.SetBagItem(EThings.Bag, bagIndex, Item.GetBagItem(EThings.Equip, fitType, 0), true);
            Item.DelBagItem(EThings.Equip, fitType);
        (pet: `protocolTable[23][22]` = S:023-022 <武將卸下裝備到背包>, y het + followIndex.)

        Chu thich cu o day ghi "mon tu ve tui qua goi 0x17 sub08 - bot da bat san" -> SAI, va no
        lam hong lan coi THU HAI: `bag_slots` khong bao gio biet mon vua coi nen
        `bag_first_empty_slot()` tra DUNG O CU -> lenh coi tiep tro toi o da co do -> server nuot
        im, user thay "khong co gi xay ra".
        Log 31/08: quan809 15:49:51 `Da coi do: vi tri 3 cua nhan vat (Ngự Thiên Pháp Trượng)` ma
        KHONG he co dong `Nhan item` di kem - dung la mon khong vao `bag_slots`.
        """
        table = (self.pet_equip_by_fit.setdefault(follow, {}) if follow else self.equip_by_fit)
        cu = table.pop(int(fit_pos), None)
        if o_tui is not None and cu:
            try:
                _o = int(o_tui)
                if 0 < _o < 256 and _o not in (self.bag_slots or {}):
                    self.bag_slots[_o] = [int(cu), 1]
                    self.bag_counts[int(cu)] = self.bag_counts.get(int(cu), 0) + 1
            except Exception as e:
                log.debug("[%s] coi do: ghi mon vua coi vao tui loi (bo qua): %s", self._label, e)
        if not follow:
            self.equipped_items = [x for x in (self.equipped_items or [])
                                   if int(x.get("id", 0)) != int(cu or -1)]
        self._equip_seq = int(getattr(self, "_equip_seq", 0)) + 1
        if not follow:
            self._recalc_char_equip_stats()
        else:
            self._cap_nhat_do_trong_ban_ghi_pet(follow, int(fit_pos), tid=None)
        log.info("[%s] Da coi do: vi tri %s cua %s (%s)", self._label, fit_pos,
                 "nhan vat" if not follow else "pet #%d" % follow,
                 self._mount_item_name(cu) if cu else "?")

    def _on_equip_done(self, slot: int, follow: int = 0):
        """DA MAC XONG 1 mon. Dung chung cho ca NHAN VAT lan PET (2 goi khac nhau, xu ly y het):
            S:023-017 <穿上裝備>     [背包索引]              -> nhan vat (follow = 0)
            S:023-023 <武將穿上裝備> [武將索引][背包索引]    -> pet

        VI SAO CAN: truoc day bot CHI bat S:023-011 <所有裝備> = danh sach do LUC LOGIN, khong
        phai goi tra loi cua tung lan mac. Nen mac do KHONG DE LAI DAU VET NAO: tui van hien mon
        da roi tui, va khong the biet lenh co an hay khong (user bao "van khong trang bi duoc"
        trong khi item HP/SP thi bao dung duoc - dung vay: use co goi confirm rieng, equip thi
        khong ai bat).
        Anh huong NGOAI cua so tui: use_phuc_than_items() cung deo ngoc bang duong nay -> bag_slots
        cua no cung dang sai tu truoc toi gio.

        Theo client (protocolTable[23][17] va [23][23]): item RA KHOI TUI; neu DANG mac san mon
        khac o cung vi tri thi mon CU quay VE dung o do. Bot khong theo doi do dang mac day du nen
        khong biet mon cu -> o day chi xoa slot (truong hop hay gap: o do dang TRONG). Neu la DOI
        DO thi tui hoi cu cho toi khi server gui lai snapshot - ghi ro de sau khong tuong bug moi.
        """
        slot = int(slot)
        follow = int(follow)
        rec = self.bag_slots.get(slot)
        if rec:
            tid = rec[0]
            self.bag_counts[tid] = max(0, self.bag_counts.get(tid, 0) - 1)
            # ThingData cua mon VUA MAC phai lay NGAY BAY GIO: vai dong duoi day o tui `slot` bi
            # ghi de bang mon CU (do thay do), luc do tra lai la ra mon cu / rong.
            _td_moi = (getattr(self, "bag_items", None) or {}).get(slot)
            table = (self.pet_equip_by_fit.setdefault(follow, {}) if follow else self.equip_by_fit)
            fit = int((_load_gamedata_items().get(tid) or {}).get("ft", 0) or 0)
            cu = table.get(fit) if fit else None
            if cu:
                # THAY DO: mon CU quay ve DUNG O vua lay mon moi ra (y het protocolTable[23][17]).
                self.bag_slots[slot] = [cu, 1]
                self.bag_counts[cu] = self.bag_counts.get(cu, 0) + 1
            else:
                self.bag_slots.pop(slot, None)
            if fit:
                table[fit] = tid
            if follow:
                self.pet_equipped.setdefault(follow, []).append(tid)
                self._cap_nhat_do_trong_ban_ghi_pet(follow, fit, tid=tid, td=_td_moi)
            else:
                self.equipped_items = [i for i in (getattr(self, "equipped_items", []) or [])
                                       if i.get("id") != cu]
                self.equipped_items.append({"id": tid, "pos": 0, "damage": 0,
                                            "damaged_item_id": 0})
        self._equip_seq += 1
        if not follow:
            self._recalc_char_equip_stats()   # thay do -> AGI/INT phai doi theo (ca nut Check AGI)

    def _learn_item(self, tid: int, dhp: int, dsp: int, room_hp: bool = True, room_sp: bool = True,
                    cap_hp: bool = False, cap_sp: bool = False):
        """Ghi nho (theo TID) item hoi bao nhieu HP/SP. room_*: stat do co cho do khong.
        cap_*: do XONG ma stat KICH TRAN (do hut) -> chi lay floor (max). Khong kich tran -> so CHINH XAC
        -> GHI DE (sua lai dung). none = ca 2 stat da test deu khong hoi."""
        if tid in _load_known_items() or tid in _load_gamedata_items():
            return   # DA BIET (m khai / gamedata) -> KHOA, khong tu sua/probe
        learned = self._learned()
        key = str(tid)
        cur = learned.get(key, {})
        # DA TUNG ghi nhan hoi (hp/sp>0) ma lan nay 0 -> co the loi/bi keo vao battle -> GIU NGUYEN,
        # khong downgrade (khong set *_zero/none). Item da biet la item tot, 1 lan 0 khong phu nhan.
        if dhp <= 0 and dsp <= 0 and (cur.get("hp", 0) > 0 or cur.get("sp", 0) > 0):
            return
        hp = cur.get("hp", 0)
        if dhp > 0:
            hp = max(hp, dhp) if cap_hp else dhp   # kich tran -> floor; sach -> dung that, ghi de
        sp = cur.get("sp", 0)
        if dsp > 0:
            sp = max(sp, dsp) if cap_sp else dsp
        hp_zero = cur.get("hp_zero", False) or (room_hp and dhp <= 0)
        sp_zero = cur.get("sp_zero", False) or (room_sp and dsp <= 0)
        none = (hp == 0 and sp == 0 and hp_zero and sp_zero)
        old = (cur.get("hp", 0), cur.get("sp", 0))
        learned[key] = {"hp": hp, "sp": sp, "hp_zero": hp_zero, "sp_zero": sp_zero,
                        "none": none, "unusable": cur.get("unusable", False)}
        if dhp > 0 or dsp > 0:
            fix = " (SUA tu %d/%d)" % old if old != (hp, sp) and (old[0] or old[1]) else ""
            log.info("[%s] HOC item tid 0x%04x: %dHP %dSP%s%s", self._label, tid, hp, sp,
                     " [kich tran-floor]" if (cap_hp and dhp > 0) or (cap_sp and dsp > 0) else "", fix)
        elif none:
            log.info("[%s] item tid 0x%04x dung duoc nhung KHONG hoi HP/SP -> none", self._label, tid)
        _save_all_learned()

    def _mark_unusable(self, tid: int):
        """Probe KHONG duoc confirm. 1 lan co the do LAG/mat goi -> chua khoa. >=2 lan lien tiep moi
        ghi unusable (item that su ko dung duoc). Confirm lai bat ky luc nao -> reset strike (xem _learn_item)."""
        learned = self._learned()
        key = str(tid)
        cur = learned.get(key, {})
        cur["strikes"] = cur.get("strikes", 0) + 1
        if cur["strikes"] >= 2:
            cur["unusable"] = True
            log.info("[%s] item tid 0x%04x ko confirm 2 lan -> unusable", self._label, tid)
        else:
            log.info("[%s] item tid 0x%04x ko confirm (strike %d/2, co the lag) -> thu lai sau",
                     self._label, tid, cur["strikes"])
        learned[key] = cur
        _save_all_learned()

    def _item_info(self, tid: int) -> dict:
        """Thong tin hoi cua tid. Uu tien: items_known.json (m khai) > items_gamedata.json (crack) > learned.
        2 nguon dau = LOCKED (khong probe/sua)."""
        k = _load_known_items().get(tid)
        if k is not None:
            return {"hp": k.get("hp", 0), "sp": k.get("sp", 0), "type": k.get("type", ""),
                    "none": False, "unusable": False, "locked": True}
        g = _load_gamedata_items().get(tid)
        if g is not None:
            return {"hp": g.get("hp", 0), "sp": g.get("sp", 0), "battle": g.get("battle", False),
                    "name": g.get("name", ""), "none": False, "unusable": False, "locked": True}
        return self._learned().get(str(tid)) or {}

    def _slot_for_known(self, kind: str, skip_slots) -> tuple:
        """Tim SLOT chua item DA BIET (locked hoac da hoc) hoi 'kind', count>0.
        Do QUY HIEM = TONG HP+SP hoi duoc: TONG nho nhat dung TRUOC (danh binh xin cho luc can gap).
        Vd hoi HP: A=30HP(tong30) B=20HP+14SP(tong34) C=25HP(tong25) D=12HP+10SP(tong22)
        -> thu tu dung: D(22) C(25) A(30) B(34)."""
        best = None      # (slot, tid, heal)
        best_key = None  # (tong_hp_sp, heal) - nho hon = uu tien dung truoc
        for slot, (tid, cnt) in self.bag_slots.items():
            if cnt <= 0 or slot in skip_slots:
                continue
            v = self._item_info(tid)
            if not v or v.get("none") or v.get("unusable") or v.get("battle"):
                continue   # battle=True: do hoi sinh, CHI dung trong tran -> ko hoi ngoai
            heal = v.get(kind, 0)
            if heal <= 0:
                continue
            total = v.get("hp", 0) + v.get("sp", 0)   # do quy = tong ca 2 tac dung
            key = (total, heal)
            if best_key is None or key < best_key:
                best_key, best = key, (slot, tid, heal)
        return best

    def has_hp_and_sp_items(self) -> bool:
        """Co it nhat 1 item hoi HP VA 1 item hoi SP (con so luong, DA BIET qua gamedata/khai) trong
        tui khong. Dung lam BAO HIEM cho Di Gioi SOLO: thieu 1 trong 2 loai -> KHONG chay long vong
        (danh quai lien tuc ma khong co gi hoi -> de chet/can SP giua chung, khong ai cuu vi solo)."""
        has_hp = has_sp = False
        for slot, (tid, cnt) in self.bag_slots.items():
            if cnt <= 0:
                continue
            v = self._item_info(tid)
            if not v or v.get("none") or v.get("unusable") or v.get("battle"):
                continue
            if v.get("hp", 0) > 0:
                has_hp = True
            if v.get("sp", 0) > 0:
                has_sp = True
            if has_hp and has_sp:
                return True
        return False

    # ---- DOI PET THEO VAI TRO (train / boss / quest-PB-event) -----------------------------
    # Crack client (UITeam.OnClick_FollowNpcState + protocolTable[19]):
    #   C:019-001 <跟隨武將出戰> +NPCID(2)   -> gui 0x13 sub 0100 + [pet_id u16 LE]
    #   S:019-001 / S:019-004                -> Role.SetFightNpc(npcId) = XAC NHAN doi xong
    #   C:019-002 <出戰武將收回>             -> thu pet ve (bot KHONG dung)
    # Bot CHI tu chan 2 dieu kien: DANG TRONG TRAN (client: war ~= None and not IsCanControl)
    # va pet KHONG mang theo. Cac truong hop con lai cua client (pet chet - tu hoi sinh sau tran
    # nen khong dang ke; da ha da; dang bi cuoi lam ngua - rat hiem) KHONG check truoc: server
    # khong xac nhan thi bot giu pet cu va chay tiep, khong hong gi.
    # "pb_don" TACH RA KHOI "boss" (2026-08-22): PB don cho NHIEU EXP nen user muon danh rieng
    # mot con de don exp, giong y tuong chon pet van tieu. Truoc day do_daily_dungeon dung chung
    # vai "boss" voi world boss / legion boss - nhan UI cu "Quest/PB/Event" de gay hieu nham la
    # PB don nam trong nhom quest, THUC TE no nam trong nhom boss.
    PET_ROLES = ("train", "boss", "quest", "pb_don")

    # So lan THU doi sang MOT con pet truoc khi bo cuoc. Pet HET DO TRUNG THANH thi server tu choi
    # -> switch_pet khong bao gio confirm, ma ensure_pet_role lai duoc goi MOI lan vao hoat dong
    # (decorator @_pet_role) -> thu lai vo han, moi lan phi 4s cho + log rac (user bao).
    # Dem theo PET ID chu khong theo VAI: nguyen nhan chan nam o CON PET, mot con gan 2 vai khong
    # duoc an 6 lan thu. Bo dem nam tren client nen relogin la tu reset (do trung thanh co the da
    # duoc hoi lai, hoac user da cho pet an).
    PET_SWITCH_MAX_TRY = 3

    def switch_pet(self, pid: int, wait: float = 4.0) -> bool:
        """Doi pet xuat chien sang `pid`. True = da doi xong (hoac dang dung san con do).

        KHONG doi giua tran (client cam) va KHONG doi sang pet khong mang theo. Xac nhan bang
        S2C 0x13 0100 - handler co san se cap nhat active_pet_id VA doc lai active_pet_slot tu
        goi 0x0f cache (quan trong: hoi pet phai dung slot, khong thi item bay vao slot sai).
        """
        try:
            pid = int(pid)
        except (TypeError, ValueError):
            return False
        if pid <= 0 or not self.running:
            return False
        # Bo qua goi doi CHI khi active_pet_id da duoc SERVER xac nhan (goi 0x13). Neu no chi la
        # doan tam tu record dau 0x0f thi VAN gui - doan truot ma tin la bot danh bang pet SAI
        # ma khong ai biet.
        if (pid == getattr(self.state, "active_pet_id", None)
                and getattr(self.state, "active_pet_confirmed", False)):
            return True
        # DANG TRAN = ly do TAM THOI (het tran la doi duoc) -> KHONG tinh vao so lan thu.
        if getattr(self.state, "in_battle", False):
            log.info("[%s] doi pet: DANG TRONG TRAN -> hoan", self._label)
            return False
        if self._pet_switch_gave_up(pid):
            return False
        carried = {p for p, _nm in (getattr(self.state, "carried_pets", []) or [])}
        if carried and pid not in carried:
            log.warning("[%s] doi pet: 0x%04x KHONG co trong tui pet -> bo qua", self._label, pid)
            self._pet_switch_failed(pid, "khong co trong tui pet")
            return False
        old = getattr(self.state, "active_pet_id", None)
        self.send(0x13, b"\x01\x00" + pid.to_bytes(2, "little"))
        t0 = time.time()
        while time.time() - t0 < wait and self.running:
            if getattr(self.state, "active_pet_id", None) == pid:
                nm = getattr(config, "PET_NAMES", {}).get(pid, "?")
                log.info("[%s] DOI PET: 0x%04x -> 0x%04x ('%s') slot=%s",
                         self._label, old or 0, pid, nm, getattr(self, "active_pet_slot", None))
                # Pet moi vao voi HP/SP CUA CHINH NO (khong ke thua mau con cu) -> HOI FULL
                # ngay truoc khi danh tiep (yeu cau user), khong doi nguong hoi thuong.
                try:
                    self.heal_full(force=True)
                except Exception as e:
                    log.warning("[%s] hoi full sau khi doi pet loi: %s", self._label, e)
                self._pet_switch_fail.pop(pid, None)   # doi duoc roi -> xoa bo dem
                return True
            time.sleep(0.2)
        # Khong confirm = server tu choi (HET DO TRUNG THANH / pet chet / ha da / dang bi cuoi)
        # -> giu pet cu, chay tiep.
        log.warning("[%s] doi pet sang 0x%04x KHONG duoc xac nhan (het trung thanh/pet chet/"
                    "ha da/dang cuoi?) -> giu pet cu", self._label, pid)
        self._pet_switch_failed(pid, "server khong xac nhan")
        return False

    def _pet_switch_gave_up(self, pid: int) -> bool:
        """Da thu du PET_SWITCH_MAX_TRY lan ma khong doi duoc -> thoi, giu pet hien tai."""
        return self._pet_switch_fail.get(pid, 0) >= self.PET_SWITCH_MAX_TRY

    def _pet_switch_failed(self, pid: int, ly_do: str):
        """Ghi 1 lan that bai. Cham nguong thi log RO MOT LAN roi im (khong spam moi hoat dong)."""
        n = self._pet_switch_fail.get(pid, 0) + 1
        self._pet_switch_fail[pid] = n
        if n == self.PET_SWITCH_MAX_TRY:
            faith = (getattr(self, "pet_faith", None) or {}).get(pid)
            log.warning("[%s] doi pet 0x%04x THAT BAI %d lan (%s%s) -> THOI doi, giu pet hien tai "
                        "cho toi khi login lai", self._label, pid, n, ly_do,
                        "" if faith is None else ", trung thanh=%d" % faith)

    def ensure_pet_role(self, role: str) -> bool:
        """Dam bao pet dang xuat chien la pet user gan cho `role`. Vai KHONG gan pet -> khong dung.

        pet_roles nam TRONG battle_config (cung dialog Kich ban Skill) nen khong phai them duong
        truyen config moi cho ca PC lan APK.
        """
        cfg = getattr(self.state, "battle_config", {}) or {}
        roles = cfg.get("pet_roles")
        if not isinstance(roles, dict):
            return False
        pid = roles.get(role)
        if not pid:
            return False   # vai nay khong gan pet -> giu nguyen pet dang dung
        return self.switch_pet(pid)

    def do_heal(self, force: bool = False):
        """Hoi mau NGOAI tran cho CHAR + pet, dung thuoc DA BIET (gamedata/khai).
        KHONG probe (gamedata da biet het thuoc). Hoi den NGUONG la dung.
        force=True: chi can in_battle=False la hoi (BO busy-window 4s cua in_combat) - dung cho
        hoi NGAY khi nhan goi ket tran that (_heal_after_battle): train re-aggro nhanh thi cua so
        "het busy" gan nhu khong bao gio trung tick keepalive -> hoi tre/lo (user bao thuc te)."""
        _busy = self.state.in_battle if force else self.in_combat()
        if _busy or not self.bag_slots:
            return
        c = self.state.char
        if c.hp_max > 0:
            self._heal_unit(0, c, "char", "hp_char", "hp", force=force)
            self._heal_unit(0, c, "char", "sp_char", "sp", force=force)
        if self.state.solo_multipet and self._heal_solo_multipets(force=force):
            return
        p = self.state.pet
        if p.hp_max > 0:
            # target hoi pet = SLOT TUI PET dang xuat chien (1..4, tu 0x0f marker) - KHONG phai
            # hang so 1 (user tu xep vi tri pet; hardcode 1 -> item vao slot sai, server bo qua).
            _pt = self.active_pet_slot or 1
            # Pet chet trong tran -> KET TRAN da duoc HOI SINH lai 1HP (server tu lam) -> state
            # pet.hp=0 tu 0x33 cuoi la STALE, van hoi binh thuong (coi nhu 1HP).
            if p.hp <= 0:
                p.hp = 1
            self._heal_unit(_pt, p, "pet", "hp_pet", "hp", force=force)
            self._heal_unit(_pt, p, "pet", "sp_pet", "sp", force=force)

    def _sync_solo_multipet_from_allies(self):
        """Dong bo stat pet Di Gioi solo tu allies/ally_spmax neu 0x0b da nap vao do."""
        if not self.state.solo_multipet:
            return
        for (b1, atype), src in list(self.state.allies.items()):
            if b1 != 2 or self._pet_atype_to_marker(atype) is None:
                continue
            u = self.state.multi_pet.get(atype)
            if u is None:
                u = Unit(f"pet_at{atype}")
                self.state.multi_pet[atype] = u
            if src.hp_max > 0:
                u.hp_max = src.hp_max
            if src.hp > 0 or u.hp == 0:
                u.hp = src.hp
            if src.sp >= 0:
                u.sp = src.sp
            smax = self.state.ally_spmax.get((2, atype), src.sp_max)
            if smax > 0:
                u.sp_max = smax

    def _heal_solo_multipets(self, thr_override=None, force: bool = False) -> bool:
        """Di Gioi solo: hoi HP/SP tung pet dang co stat, target item = marker pet (1..4)."""
        self._sync_solo_multipet_from_allies()
        seen = False
        for atype, unit in sorted(self.state.multi_pet.items()):
            marker = self._pet_atype_to_marker(atype)
            if marker is None or (unit.hp_max <= 0 and unit.sp_max <= 0):
                continue
            seen = True
            # Pet chet trong tran duoc server hoi sinh 1HP khi ket tran; 0x33 cuoi co the stale.
            if unit.hp_max > 0 and unit.hp <= 0:
                unit.hp = 1
            label = f"pet{marker}"
            self._heal_unit(marker, unit, label, "hp_pet", "hp",
                            thr_override=thr_override, force=force)
            self._heal_unit(marker, unit, label, "sp_pet", "sp",
                            thr_override=thr_override, force=force)
        return seen

    def _heal_after_battle(self):
        """Goi tu recv-loop NGAY khi nhan goi KET TRAN THAT (0x14 sub0700 / sub0800 tail xac nhan).
        Spawn thread rieng (KHONG block recv) doi grace ngan roi do_heal(force=True) - tranh truong
        hop keepalive (tick 5s + busy-window 4s) khong bao gio bat kip khe ho giua 2 tran."""
        if self.state.quest_mode or getattr(self.state, "boss_mode", False):
            return   # dungeon/boss flow tu quan ly heal (do_heal/heal_full rieng giua cac tran)
        if getattr(self, "_heal_after_battle_active", False):
            return
        self._heal_after_battle_active = True

        def _run():
            try:
                time.sleep(0.5)   # doi man tong ket/0x33 cuoi cap nhat HP xong (1.0 -> 0.5 theo yeu cau: hoi som hon)
                if self.running and not self.state.in_battle:
                    self.do_heal(force=True)
            finally:
                self._heal_after_battle_active = False
        self._heal_after_battle_thread = threading.Thread(target=_run, daemon=True)
        self._heal_after_battle_thread.start()

    def heal_npc40_between_battles(self):
        """Hoi FULL HP/SP sau tran (event danh theo party: 40NPC, 2K).

        KHONG can xu ly rieng "unit HP=0": HET TRAN thi SERVER tu dat con chet (char LAN pet) ve
        HP=1, khong con unit nao o HP=0 luc nay -> heal_full() phu duoc het. (Truoc day co doan
        _heal_unit(thr_override=0.01) cho char HP=0 - thua, da bo.)
        Truoc day dung do_heal(force=True) = hoi THEO SETTING -> vao tran sau voi HP/SP lung
        chung; doi sang heal_full cho dong bo voi 2K/boss/PB110 (yeu cau 2026-08-09)."""
        if self.state.in_battle:
            return
        self.heal_full(force=True)

    def heal_full(self, force: bool = False):
        """Hoi FULL HP+SP char + pet (nguong=1.0) - goi TRUOC khi danh boss (solo dungeon + world
        boss) hoac GIUA cac tran PB110. Het thuoc thi hoi duoc bao nhieu hay bay nhieu."""
        _busy = self.state.in_battle if force else self.in_combat()
        if _busy or not self.bag_slots:
            return
        log.info("[%s] Hoi FULL HP/SP...", self._label)
        c = self.state.char
        if c.hp_max > 0:
            self._heal_unit(0, c, "char", "hp_char", "hp", thr_override=1.0, force=force)
            self._heal_unit(0, c, "char", "sp_char", "sp", thr_override=1.0, force=force)
        if self.state.solo_multipet and self._heal_solo_multipets(thr_override=1.0, force=force):
            return
        p = self.state.pet
        if p.hp_max > 0:
            _pt = self.active_pet_slot or 1   # slot tui pet dang xuat chien (xem do_heal)
            if p.hp <= 0:
                p.hp = 1   # pet chet da duoc server hoi sinh 1HP luc ket tran (0x33 cuoi stale)
            self._heal_unit(_pt, p, "pet", "hp_pet", "hp", thr_override=1.0, force=force)
            self._heal_unit(_pt, p, "pet", "sp_pet", "sp", thr_override=1.0, force=force)

    def _heal_unit(self, target: int, unit, label: str, thr_key: str, kind: str, thr_override=None,
                   force: bool = False):
        """Hoi 1 con 1 stat bang thuoc DA BIET den nguong. char do qua 0x08 (chinh xac);
        pet ko do duoc -> uoc tinh theo heal (open-loop). Het thuoc nay -> tu chuyen thuoc khac.
        thr_override: ep nguong (vd 1.0 = FULL) - dung cho heal_full truoc boss.
        force=True: chi dung khi in_battle THAT (bo busy-window) - xem do_heal."""
        def _busy():
            return self.state.in_battle if force else self.in_combat()
        if _busy():
            return
        nokey = (target, kind)
        if nokey in self._no_item:
            return                 # da bao het thuoc loai nay -> cho TRAN SAU (0x34 reset) moi check
        thr = thr_override if thr_override is not None else self._heal_threshold(thr_key)
        mx = unit.hp_max if kind == "hp" else unit.sp_max
        cur = unit.hp if kind == "hp" else unit.sp
        if thr <= 0 or mx <= 0 or cur >= mx * thr:
            return
        target_val = int(mx * thr)
        healed = False
        # BATCH: biet moi thuoc hoi bao nhieu + con thieu bao nhieu -> tinh qty = ceil(thieu/heal)
        # gui 1 lenh (0x17 ho tro qty). Moi VONG = 1 batch (1 slot). Uu tien thuoc NHO nhat (de
        # danh thuoc xin). CHAR: sau batch doi 0x08 cap nhat chi so THAT roi kiem lai (bu neu server
        # nuot lenh); PET: khong do duoc -> cong optimistic theo qty*heal.
        for _ in range(12):
            if _busy():
                break
            cur = unit.hp if kind == "hp" else unit.sp
            if cur >= target_val:
                break
            found = self._slot_for_known(kind, set())
            if found is None:
                log.info("[%s] %s HET thuoc %s -> bo qua, cho tran sau (co the drop them)",
                         self._label, label, kind.upper())
                self._no_item.add(nokey)   # skip toi tran sau
                break
            slot, tid, heal = found
            rec = self.bag_slots.get(slot)
            stock = rec[1] if rec else 1
            need = target_val - cur
            qty = max(1, (need + heal - 1) // heal)   # ceil(need/heal): lo lom hon 1 thuoc nho - ko dang
            qty = min(qty, stock, 255)                # ko vuot ton kho slot / gioi han goi
            if not self.use_slot(slot, target, qty):
                break
            # tru bag OPTIMISTIC (tranh chon lai slot nay khi server chua kip gui 0x16 cap nhat tui)
            if rec:
                rec[1] = max(0, rec[1] - qty)
                if rec[1] <= 0:
                    self.bag_slots.pop(slot, None)
            healed = True
            _iname = (_load_gamedata_items().get(tid) or {}).get("name", "").strip()
            log.info("[%s] hoi %s slot=%d 0x%04x '%s' x%d (+%d/cai)%s target=%d",
                     self._label, label, slot, tid, _iname, qty, heal, kind.upper(), target)
            if target == 0:
                time.sleep(0.5)   # CHAR: cho 0x08 cap nhat chi so THAT -> vong sau kiem lai, bu neu nuot
            else:
                # PET open-loop: HP/SP that KHONG cap nhat ngoai tran -> cong optimistic de vong sau
                # biet da du (va keepalive sau KHONG hoi lai vo han; dau tran sau 0x33 cap nhat lai).
                gain = qty * heal
                if kind == "hp":
                    unit.hp = min(mx, unit.hp + gain)
                else:
                    unit.sp = min(mx, unit.sp + gain)
                time.sleep(0.2)

    def scan_furnace(self, wait: float = 3.0):
        """SOI LO thuong (熔爐): gui C:089-001 (0x59 sub01, khong payload) -> server tra S:089-001
        -> _parse_furnace_shop() luu self.furnace_shop + log 3 tab. Pha 1: CHI doc/log, chua mua.
        Tra True neu nhan duoc data trong `wait` giay."""
        self.furnace_shop = None
        seq0 = self._furnace_seq
        log.info("[%s] SOI LO: gui query (0x59 sub01)...", self._label)
        self.send(0x59, b"\x01\x00")
        t0 = time.time()
        while self._furnace_seq == seq0 and time.time() - t0 < wait and self.running:
            time.sleep(0.1)
        if self._furnace_seq == seq0:
            log.warning("[%s] SOI LO: khong nhan duoc data sau %.0fs (lo dong / khong o gan lo?)",
                        self._label, wait)
            return False
        return self.furnace_shop is not None

    # (Ghi nho protocol: kind lo thuong = 1 Vo Tuong / 2 Trang Bi / 5 Chuyen Sinh; hoang kim
    #  3/4/6 - CHUA MO, server tra 8 slot id=0. Xem FURNACE_TAB_KIND o duoi de map ten tab config.)
    # DA MUA hay chua: moi (kind,slot) co 1 BitFlag id co dinh (tu UI_UIFurnace.lua). slot 1..8.
    # base + (slot-1). Set = da mua. Doc tu bitmap 0x51 (self._bitflag_get). Mua moi item 1 lan/reset.
    FURNACE_BOUGHT_FLAG_BASE = {1: 1518, 2: 1526, 5: 7257, 3: 7067, 4: 7075, 6: 7265}

    def _furnace_bought(self, kind: int, slot: int) -> bool:
        base = self.FURNACE_BOUGHT_FLAG_BASE.get(kind)
        if base is None or not (1 <= slot <= 8):
            return False
        return self._bitflag_get(base + slot - 1)

    def _parse_furnace_shop(self, pkt: bytes):
        """Parse S2C 0x59 sub01 (熔爐 shop data). Data tu pkt[9]:
          result(1) + baseRate(double8) + activeRate(double8) + bOpen(1) + count(1)
          + <<kind(1) + isCrit(1) + itemCount(1) <<itemId(u16 LE) + quant(i32 LE)>>>>
        Luu self.furnace_shop = {base_rate, active_rate, tabs:{kind:[{index,id,quant,crit}]}}."""
        try:
            p = 9
            result = pkt[p]; p += 1
            self._furnace_seq += 1
            if result != 1:
                log.info("[%s] SOI LO: server tra that bai (result=%d)", self._label, result)
                self.furnace_shop = {}
                return
            base_rate = struct.unpack_from("<d", pkt, p)[0]; p += 8
            active_rate = struct.unpack_from("<d", pkt, p)[0]; p += 8
            _bopen = pkt[p]; p += 1
            count = pkt[p]; p += 1
            tabs = {}
            for _ in range(count):
                kind = pkt[p]; p += 1
                is_crit = pkt[p]; p += 1
                item_count = pkt[p]; p += 1
                items = []
                for j in range(1, item_count + 1):
                    item_id = struct.unpack_from("<H", pkt, p)[0]; p += 2
                    quant = struct.unpack_from("<i", pkt, p)[0]; p += 4
                    items.append({"index": j, "id": item_id, "quant": quant, "crit": is_crit,
                                  "bought": self._furnace_bought(kind, j)})
                tabs[kind] = items
            self.furnace_shop = {"base_rate": base_rate, "active_rate": active_rate, "tabs": tabs}
            # LO HOANG KIM (kind 3 GoldNpc / 4 GoldEquip / 6 GoldTurnItem): truoc day server tra
            # 8 slot id=0 (chua mo) nen bot bo qua hoan toan. Game update 03/09 MO lo hoang kim ->
            # LIET KE 1 LAN khi thay tab gold co item that, de biet list co khac lo thuong khong
            # (pool lo do SERVER gui moi lan soi, client KHONG co bang tinh de tra truoc).
            try:
                _gold = {k: [it for it in v if it.get("id")] for k, v in tabs.items()
                         if k in (3, 4, 6)}
                _gold = {k: v for k, v in _gold.items() if v}
                if _gold and not getattr(self, "_da_log_lo_hoang_kim", False):
                    self._da_log_lo_hoang_kim = True
                    _db = _load_gamedata_items()
                    for _k, _items in sorted(_gold.items()):
                        _ten = {3: "GoldNpc(vo tuong)", 4: "GoldEquip(trang bi)",
                                6: "GoldTurnItem(chuyen sinh)"}.get(_k, str(_k))
                        log.info("[%s] LO HOANG KIM tab %s (kind=%d): %s", self._label, _ten, _k,
                                 [("%s x%d" % ((_db.get(it["id"]) or {}).get("name")
                                               or ("0x%04x" % it["id"]), it["quant"]))
                                  for it in _items])
            except Exception as e:
                log.debug("[%s] loi liet ke lo hoang kim (bo qua): %s", self._label, e)
            # KHONG log liet ke 6 tab x 8 slot (~56 dong/lan soi lo, ken log vo ich - yeu cau
            # user). Item dang quan tam da co log rieng o process_furnace (THONG BAO / AUTO MUA).
            # Muon xem lai toan bo thi doc self.furnace_shop.
        except Exception as e:
            log.warning("[%s] SOI LO: loi parse (%s) raw=%s", self._label, e, pkt.hex())

    # ten tab config (per-acc) -> kind trong shop packet (ESelect: 1/2/5 lo thuong)
    FURNACE_TAB_KIND = {"vo_tuong": 1, "trang_bi": 2, "chuyen_sinh": 5}
    # LO HOANG KIM (game mo 03/09) DUNG CHUNG POOL ITEM voi lo thuong: FurnaceSlot_C.dat chi co
    # 3 nhom (415 Vo Tuong / 713 Trang Bi / 777 Chuyen Sinh) va tab gold lay tu dung 3 nhom do.
    # -> KHONG lam 3 tab config moi (user chot 03/09), moi tab phu CA HAI lo. Khac biet giua 2 lo
    # nam o bang xac suat pham (FurnaceSelect_C.dat: gold co 8 muc, sample toi 10, don ve pham cao)
    # va GIA (UI_UIFurnace.lua nhan them goldStoreRate = x2) - khong lien quan danh sach item.
    FURNACE_TAB_KIND_GOLD = {"vo_tuong": 3, "trang_bi": 4, "chuyen_sinh": 6}

    def _nho_dac_ky(self, npc_id):
        """Ghi nho vo tuong nay DA HOC dac ky (song qua viec cat vao nha tro / tat acc)."""
        try:
            self._dac_ky_biet = set(getattr(self, "_dac_ky_biet", None)
                                    or load_dac_ky_cache(getattr(self, "_username", None)))
            self._dac_ky_biet.add(int(npc_id))
            save_dac_ky_cache(getattr(self, "_username", None), [int(npc_id)])
        except Exception as e:
            log.debug("[%s] nho dac ky loi: %s", self._label, e)

    def dac_ky_da_hoc(self) -> set:
        """Set npc_id da hoc dac ky = dang mang theo (chac chan) + tung thay truoc day (cache)."""
        biet = set(getattr(self, "_dac_ky_biet", None) or ())
        if not biet:
            biet = load_dac_ky_cache(getattr(self, "_username", None))
            self._dac_ky_biet = set(biet)
        return biet | {pid for pid, co in (self.pet_special_skill or {}).items() if co}

    def vo_tuong_dang_co(self) -> set:
        """Set npc_id vo tuong acc dang so huu: 4 con MANG THEO + tat ca con o NHA TRO.

        Nha tro ve qua `S:031-006` ngay luc login (`vantieu_roster_ids`) nen luc soi lo da co du.
        """
        ids = {int(p[0]) for p in (getattr(self.state, "carried_pets", None) or ()) if p and p[0]}
        ids |= {int(v) for v in (self.vantieu_roster_ids or {}).values() if v}
        return ids

    def _lo_da_du_khoi_mua(self, item_id: int):
        """Item chuyen sinh nay DA VO DUNG voi acc -> ly do (str) de bo qua, None = van nen mua.

        User chot 01/09: mua re, nhung mua mon KHONG CAN DUNG NUA la TON SLOT TUI.
          - Kim Toa <X>: da co <X> o rb1 HOAC rb2 (mang theo hoac nha tro) -> khoi mua.
          - Me <X>: <X> da hoc dac ky -> khoi mua.
          - Tuong Tinh: KHONG gioi han (user giu nguyen).
        """
        info = _load_chuyen_sinh_map().get(int(item_id))
        if not info:
            return None
        if info["loai"] == CS_KIM_TOA:
            co = self.vo_tuong_dang_co()
            for bac in ("rb1", "rb2"):
                if info.get(bac) and info[bac] in co:
                    return "da co %s (npc 0x%04x)" % (bac, info[bac])
            return None
        if info["loai"] == CS_ME:
            hoc = self.dac_ky_da_hoc()
            for bac in ("rb1", "rb2"):
                if info.get(bac) and info[bac] in hoc:
                    return "vo tuong nay DA HOC dac ky (npc 0x%04x)" % info[bac]
        return None

    def buy_furnace_item(self, kind: int, slot: int, item_id: int, wait: float = 1.5) -> bool:
        """MUA 1 item trong lo: C:089-002 = 0x59 sub02 + [kind u8][slot u8][itemId u16 LE].
        kind/slot/itemId LAY TU goi soi (shops[kind][slot]). Server tra S:089-002 result:
        1=OK, 5=da mua, 6=thieu chips. Tra True neu gui + nhan phan hoi (khong chac thanh cong)."""
        seq0 = self._fashion_deposit_seq  # 0x59 sub02 (buy) dung chung seq voi tha do (deu sub02)
        self.send(0x59, b"\x02\x00" + bytes([kind & 0xFF, slot & 0xFF]) + struct.pack("<H", item_id))
        t0 = time.time()
        while self._fashion_deposit_seq == seq0 and time.time() - t0 < wait and self.running:
            time.sleep(0.1)
        return self._fashion_deposit_seq != seq0

    def _cho_tui_cap_nhat(self, tid: int, cu: int, wait: float = 2.0) -> bool:
        """Cho so luong `tid` trong tui LON HON `cu` (server gui 0x17 sub08 sau khi mua).

        Tra False neu het `wait` ma chua thay - caller van di tiep (co `_mua_luot` chan
        mua trung), chi la cac luat bam bag_counts se doc so cu.
        """
        t0 = time.time()
        while time.time() - t0 < wait and self.running:
            if self.bag_counts.get(int(tid), 0) > int(cu):
                return True
            time.sleep(0.1)
        log.info("[%s] LO: cho tui cap nhat 0x%04x qua %.1fs khong thay -> di tiep",
                 self._label, int(tid), wait)
        return False

    def process_furnace(self, cfg: dict):
        """SOI LO + xu ly theo config per-acc cfg = {tab: {"on": bool, "items": {tid_int: "auto"/"notify"}}}.
        tab in vo_tuong/trang_bi/chuyen_sinh. Item trong shop khop list:
          - "auto" + CHUA mua (bought=False) -> MUA luon.
          - "notify" -> gom lai bao (log) de user tu quyet.
        Tra list item can BAO (notify) de GUI popup: [{tab,kind,slot,id,name,quant}]."""
        if not self.scan_furnace() or not self.furnace_shop:
            return []
        gd = _load_gamedata_items()
        pool_ids = _load_furnace_pool_ids()
        tabs = self.furnace_shop.get("tabs", {})
        notify = []
        # DA MUA trong CHINH luot soi nay (tid). CAN vi buy_furnace_item() chi cho ack
        # S:089-002, KHONG cho goi cap nhat tui (0x17 sub08) -> sang tab hoang kim ngay sau
        # do thi bag_counts VAN la 0, luat "da co >=1 trong tui thi thoi" khong chan duoc
        # -> mua lai dung mon vua mua, ma gia hoang kim GAP DOI (user hoi 03/09: "1 Kim toa
        # co ca o lo thuong va lo hoang kim thi lan do co bi tu mua ca 2 khong").
        _mua_luot = set()
        # Moi tab config duyet CA lo thuong LAN lo hoang kim (chung pool item, xem
        # FURNACE_TAB_KIND_GOLD). `nhom` = kind lo thuong, dung cho cac luat mua theo loai; `kind`
        # la kind THAT cua goi, phai giu nguyen de gui lenh mua va tra co "da mua".
        _cap = []
        for _t, _k in self.FURNACE_TAB_KIND.items():
            _kg = self.FURNACE_TAB_KIND_GOLD.get(_t)
            _cap.append((_t, _k, _k))                      # lo thuong
            if _kg is not None:
                _cap.append((_t, _k, _kg))                 # lo hoang kim, CUNG config tab
        for tab_name, nhom, kind in _cap:
            _gold = kind != nhom
            _ten_tab = tab_name + " HOANG KIM" if _gold else tab_name
            tcfg = (cfg or {}).get(tab_name) or {}
            if not tcfg.get("on", True):   # mac dinh TICK: thieu config tab = coi nhu BAT
                continue
            wl = tcfg.get("items") or {}
            for it in tabs.get(kind, []):
                if it["id"] == 0:
                    continue
                mode = wl.get(it["id"]) or wl.get("0x%04x" % it["id"])
                if mode == "skip":
                    # User CHU Y chon "Bo qua" cho item nam trong danh sach mac-dinh-thong-bao.
                    # Can gia tri RIENG ("skip") chu khong phai "" vi "" falsy -> bi coi la CHUA
                    # cau hinh -> lai roi vao mac dinh notify (test bat duoc truoc khi len ban chay).
                    continue
                if not mode:
                    # Khong co trong config -> quyet dinh theo MAC DINH:
                    #  1. id NGOAI pool (game update them item moi) -> THONG BAO (khong tu mua item la).
                    #  2. id trong danh sach mac-dinh-thong-bao (cuon goi/K.Toa/T.Tinh/Me cua vo
                    #     tuong CO vu khi chuyen dung) -> THONG BAO.
                    #  3. con lai -> bo qua.
                    # Config cua acc luon DE LEN cai nay (da lay o `wl` phia tren).
                    if pool_ids and it["id"] not in pool_ids:
                        mode = "notify"
                    elif it["id"] in _load_furnace_default_notify_ids():
                        mode = "notify"
                    else:
                        continue
                nm = (gd.get(it["id"]) or {}).get("name") or "0x%04x" % it["id"]
                if it.get("bought"):
                    log.info("[%s] LO: %s (%s) DA MUA -> bo qua", self._label, nm, _ten_tab)
                    continue
                if mode == "auto":
                    # Luat tu mua theo TAB (bag_counts = so luong trong TUI, KHONG tinh do da mac):
                    #  - Vo tuong (kind1): co la mua luon (khong gioi han).
                    #  - Trang bi (kind2): chi mua khi trong tui CHUA CO cai nao (>=1 la thoi) -
                    #    giong K.Toa / Me ben chuyen sinh. Truoc day la <2 (tuc da co 1 van mua
                    #    them cai thu 2) -> phi chips.
                    #  - Chuyen sinh (kind5): Tuong Tinh -> mua khong gioi han; K.Toa / Me -> da co (>=1)
                    #    trong tui thi THOI.
                    _bag = self.bag_counts.get(it["id"], 0)
                    if it["id"] in _mua_luot:
                        log.info("[%s] LO: %s (%s) VUA MUA o lo kia trong luot nay -> KHONG mua lai",
                                 self._label, nm, _ten_tab)
                        continue
                    _skip = False
                    if nhom == 2 and _bag >= 1:
                        _skip = True
                    elif nhom == 5:
                        _nm = nm.strip()
                        _limited = ("Tỏa" in _nm) or _nm.endswith("Mê")   # Kim Toa / Me = gioi han 1
                        if _limited and _bag >= 1:
                            _skip = True
                    if _skip:
                        log.info("[%s] LO: %s (%s) da co %d trong tui -> KHONG tu mua",
                                 self._label, nm, _ten_tab, _bag)
                        continue
                    # Vo dung voi acc nay (da reborn / da hoc dac ky) -> mua ve chi TON SLOT TUI.
                    _thua = self._lo_da_du_khoi_mua(it["id"]) if nhom == 5 else None
                    if _thua:
                        log.info("[%s] LO: %s (%s) %s -> KHONG tu mua (mua ve chi ton slot tui)",
                                 self._label, nm, _ten_tab, _thua)
                        continue
                    log.info("[%s] LO: AUTO MUA %s (%s slot%d, tui=%d)",
                             self._label, nm, _ten_tab, it["index"], _bag)
                    _bag_truoc = self.bag_counts.get(it["id"], 0)
                    ok = self.buy_furnace_item(kind, it["index"], it["id"])
                    if ok:
                        _mua_luot.add(it["id"])
                        # CHO TUI LEN DUNG truoc khi xet lo con lai: buy_furnace_item chi cho
                        # ack S:089-002, con so luong trong tui do goi RIENG (0x17 sub08) gui
                        # sau. Khong cho thi moi luat bam bag_counts (trang bi, K.Toa/Me) deu
                        # doc so CU. `_mua_luot` van giu lam luoi thu hai neu goi tui khong toi.
                        self._cho_tui_cap_nhat(it["id"], _bag_truoc)
                    if not ok:
                        log.warning("[%s] LO: mua %s KHONG co phan hoi (thieu chips?)", self._label, nm)
                else:   # notify
                    _bag = self.bag_counts.get(it["id"], 0)
                    if it["id"] in _mua_luot:
                        continue          # vua tu mua o lo kia -> khong bao trung
                    # Me / Kim toa: chi can 1 cai -> DA CO trong tui (>=1) thi KHONG thong bao nua
                    # (du de "Thong bao"), vi co them cung vo ich.
                    if nhom == 5:
                        _nm2 = nm.strip()
                        if (("Tỏa" in _nm2) or _nm2.endswith("Mê")) and _bag >= 1:
                            continue
                    _new = pool_ids and it["id"] not in pool_ids
                    log.info("[%s] LO: CO %s (%s)%s - can BAO user quyet dinh mua",
                             self._label, nm, _ten_tab, " [ITEM LA ngoai pool]" if _new else "")
                    notify.append({"tab": tab_name, "kind": kind, "slot": it["index"],
                                   "id": it["id"], "name": nm, "quant": it["quant"],
                                   "bag": _bag, "new": bool(_new), "gold": _gold})
        return notify

    def decompose_slot(self, slot: int, wait: float = 1.2) -> bool:
        """PHAN GIAI 1 SLOT do user chon (tui do trong bot). Cung goi voi decompose_junk_scrolls
        nhung KHONG loc theo danh sach cuon rac - user tu chon nen tu chiu.
        C2S 0x59: 03 00 01 [slot 1B][01] 00 00 00 (tham chieu theo SLOT, giong use-item)."""
        seq0 = self._decompose_seq
        try:
            self.send(0x59, b"\x03\x00\x01" + bytes([int(slot) & 0xFF, 0x01]) + b"\x00\x00\x00")
        except OSError:
            return False
        t0 = time.time()
        while self._decompose_seq == seq0 and time.time() - t0 < wait and self.running:
            time.sleep(0.1)
        return self._decompose_seq > seq0

    def deposit_fashion_slot(self, slot: int, wait: float = 1.0) -> bool:
        """THA 1 MON do thoi trang o `slot` vao BO SUU TAM. C2S 0x5f sub02 [collectStyleId u16][part u8].
        Server tu tim item theo (id, part) nen KHONG gui slot - o day chi dung slot de tra ra tid."""
        rec = self.bag_slots.get(int(slot))
        if not rec:
            return False
        cid_part = _load_collect_style().get(rec[0])
        if not cid_part:
            return False          # khong phai do thoi trang -> khong tha duoc
        cid, part = cid_part
        seq0 = self._fashion_deposit_seq
        try:
            self.send(0x5f, b"\x02\x00" + struct.pack("<H", cid) + bytes([part & 0xFF]))
        except OSError:
            return False
        t0 = time.time()
        while self._fashion_deposit_seq == seq0 and time.time() - t0 < wait and self.running:
            time.sleep(0.1)
        return self._fashion_deposit_seq > seq0

    def is_fashion_item(self, tid) -> bool:
        """tid co phai do thoi trang (tha vao S.Tam duoc) khong."""
        return int(tid) in _load_collect_style()

    def deposit_fashion_to_collection(self, wait: float = 1.0):
        """THA DO THOI TRANG vao BO SUU TAM (收藏冊) -> gon tui + diem collection. Item vao S.Tam
        VAN MAC LAI DUOC (khong mat). Quet bag_slots: tid nam trong collect_style.json = do thoi
        trang -> gui C2S 0x5f sub02 [collectStyleId u16 LE][part u8]. Xac nhan capture
        ts_capture_mumu12_congty.pcap: `5f 02 00 01 00 01` -> S2C `5f 02 00 01` (thanh cong).
        THA TRUOC use_login_items (chac chan free slot; use_item doi khi de item moi lam day tui)."""
        fmap = _load_collect_style()
        if not fmap:
            return
        total = 0
        guard = 0
        while self.running and guard < 500:
            guard += 1
            # tim 1 SLOT con do thoi trang (tid nam trong map collect_style)
            target = None
            for slot, (tid, cnt) in list(self.bag_slots.items()):
                if cnt > 0 and tid in fmap:
                    target = (slot, tid)
                    break
            if target is None:
                break   # het do thoi trang trong tui
            slot, tid = target
            cid, part = fmap[tid]
            seq0 = self._fashion_deposit_seq
            # C2S 0x5f sub02: [collectStyleId u16 LE][part u8]. KHONG gui bag-slot (server tu tim
            # item theo (id,part) roi bo khoi tui).
            self.send(0x5f, b"\x02\x00" + struct.pack("<H", cid) + bytes([part & 0xFF]))
            t0 = time.time()
            while self._fashion_deposit_seq == seq0 and time.time() - t0 < wait and self.running:
                time.sleep(0.1)
            # do thoi trang la item DUY NHAT (1 cai) -> bo slot khoi tracking (S2C 0x16 update lai)
            self.bag_slots.pop(slot, None)
            self.bag_counts[tid] = max(0, self.bag_counts.get(tid, 0) - 1)
            total += 1
        if total:
            log.info("[%s] Tha %d do thoi trang vao Bo Suu Tam (gon tui + diem collection)",
                     self._label, total)

    def _decompose_scroll_tids(self) -> set:
        """Set tid cuon SE PHAN GIAI = mac dinh (khong vkcd) + override cua party.

        scroll_modes: {tid_int: "keep"|"drop"} - CHI chua muc user da doi khac mac dinh, nen
        list cuon moi cua game tu dong theo mac dinh, khong can user tick lai.

        Cuon o trang thai PHAN GIAI thi K.Toa / T.Tinh / Me cua dung con pet do cung phan giai
        (mach cua pet bo di thi giu lam gi) - "extra" trong pet_scrolls.json.
        """
        modes = getattr(self, "scroll_modes", None) or {}
        out = set()
        extra_drop, extra_keep = set(), set()
        for tid, info in _load_pet_scrolls().items():
            m = modes.get(tid)
            drop = (m == "drop") or (m is None and not info["vkcd"])
            if drop:
                out.add(tid)
            (extra_drop if drop else extra_keep).update(info.get("extra") or ())
        # Nhieu cuon co the tro CUNG mot npc goc (vd "Bi Cap X" va "Bi Cap X 80") nen dung CHUNG
        # do chuyen sinh. Neu mot cuon giu, mot cuon phan giai -> GIU LAI THANG (khong pha do).
        return out | (extra_drop - extra_keep)

    def decompose_junk_scrolls(self, wait: float = 1.2):
        """Phan giai cuon GOI PET RAC (gacha ra nhieu) -> nhan lai xu. C2S 0x59:
          03 00 01 [slot 1B][01] 00 00 00   (giong use-item: tham chieu theo SLOT, KHONG phai tid).
        AN TOAN: chi phan giai SLOT co tid nam trong CONFIG.JUNK_PET_SCROLLS (= danh sach TID cuon rac,
        template -> dung mọi acc). Tim slot trong bag_slots theo tid -> gui 0x59 voi slot do.
        So luong biet tu bag_slots[slot]; khong confirm -> dung ngay (tranh ban mu).

        DANH SACH lay tu pet_scrolls.json (807 cuon) + override cua party:
        mac dinh cuon cua tuong CO vkcd = GIU LAI, con lai = PHAN GIAI; user doi duoc ca 2 chieu
        trong "Cai dat nang cao -> Tu don tui do -> List". KHONG con lay tu items_known.json:
        nguon do bat theo type 'scroll'/'junk' nen se phan giai ca cuon user tick GIU LAI."""
        if not (getattr(self, "auto_bag_clean", True)
                and getattr(self, "auto_decompose_scrolls", False)):
            return
        junk_tids = self._decompose_scroll_tids()
        if not junk_tids:
            return
        total = 0
        guard = 0
        while self.running and guard < 1000:
            guard += 1
            # tim 1 SLOT con cuon rac (tid nam trong junk_tids)
            target = None
            for slot, (tid, cnt) in list(self.bag_slots.items()):
                if cnt > 0 and tid in junk_tids:
                    target = (slot, tid)
                    break
            if target is None:
                break   # het cuon rac trong tui
            slot, tid = target
            seq0 = self._decompose_seq
            self.send(0x59, b"\x03\x00\x01" + bytes([slot & 0xFF, 0x01]) + b"\x00\x00\x00")
            t0 = time.time()
            while self._decompose_seq == seq0 and time.time() - t0 < wait and self.running:
                time.sleep(0.1)
            if self._decompose_seq == seq0:
                # khong confirm -> coi nhu slot het, xoa khoi tracking (tranh loop) va dung slot nay
                self.bag_slots.pop(slot, None)
                continue
            # confirm -> tru count slot (S2C 0x16 cung se update lai)
            rec = self.bag_slots.get(slot)
            if rec:
                rec[1] = max(0, rec[1] - 1)
                if rec[1] <= 0:
                    self.bag_slots.pop(slot, None)
            total += 1
            _jname = ((_load_gamedata_items().get(tid) or {}).get("name")
                      or (getattr(config, "JUNK_PET_SCROLLS", {}) or {}).get(hex(tid), ""))
            log.info("[%s] phan giai cuon rac slot=%d tid=0x%04x ('%s')",
                     self._label, slot, tid, _jname)
            time.sleep(0.25)
        if total:
            log.info("[%s] Phan giai cuon rac: tong %d cuon -> nhan xu", self._label, total)

    def _cho_tui_doi(self, truoc: dict, wait: float = 2.0, it_nhat: int = 1):
        """Cho tui THAY DOI so voi snapshot `truoc` -> tra list slot MOI xuat hien.

        Mo hop = dung item; do roi ra vao O TRONG qua goi RIENG (0x17 sub08) gui SAU ack dung
        item. Khong cho thi khong biet mon nao vua roi ra -> khong the phan giai/donate dung mon.

        BO SOT DO (user bao 04/09) - ban cu `if moi: return moi` tra ve NGAY khi thay o dau tien.
        Mo 21 hop mot me thi server ban 21 goi 0x17 sub08 RAI RAC; goi dau den sau ~0.1s la ham
        thoat voi DUNG 1 o -> 20 mon con lai khong ai xu ly, nam li trong tui. Gio phai:
          - doi du `it_nhat` o (= so hop vua mo, moi hop it nhat 1 mon),
          - VA lang `_YEN` giay khong them o moi (goi den theo dot, khong deu),
          - het `wait` thi tra ve nhung gi co (khong treo mai).
        """
        _YEN = 0.4                     # khoang lang du de ket luan server ban xong dot goi
        t0 = time.time()
        lan_cuoi = t0
        n_cu = 0
        while time.time() - t0 < wait and self.running:
            moi = [s for s in self.bag_slots if s not in truoc]
            if len(moi) > n_cu:        # con o moi chay ve -> reset dong ho lang
                n_cu = len(moi)
                lan_cuoi = time.time()
            elif n_cu >= it_nhat and time.time() - lan_cuoi >= _YEN:
                return moi
            time.sleep(0.05)
        return [s for s in self.bag_slots if s not in truoc]

    def tu_mo_hop_trang_bi(self, chon=None, wait_item: float = 2.0) -> dict:
        """MO HOP/TUI TRANG BI trong tui roi phan giai / donate do roi ra.

        `chon` = {tid_hex: True} - ruong user DA TICK o "List ruong". MAC DINH KHONG TICK CAI NAO
        (user chot 03/09) -> khong tick gi thi KHONG mo gi, khong tu y dung vao do cua user.

        Tra {'mo': n, 'phan_giai': n, 'donate': n, 'bo_qua': ly_do}.

        Luat (user chot 03/09):
          - CHI chay khi CO quan doan va da vao >24h (donate la buoc bat buoc, do khong phan giai
            duoc chi con duong donate).
          - Mo toi da min(ca stack, so o trong) - va phai con >= kindCount o trong (client chan
            mo khi thieu cho: Logic_Item.lua ItemUse_48).
          - MOT LAN DUY NHAT moi luot login (user chot 04/09): mo dung MOT me roi xu ly MOT luot
            roi dung han, KHONG vet can stack va KHONG sang loai ruong khac. "Mot me" = het stack
            ruong do HOAC day tui, cai nao toi truoc. Con du de lan login sau.
          - Duyet HAI LUOT: tui THUONG truoc, tui TINH/CAO sau (khong mo de quy). Loai nao het
            hang thi xet loai ke tiep - me dau tien mo duoc la dung.
          - Mo xong thi QUET CA TUI (`_don_do_ruong_con_sot`): moi mon thuoc ruong DA TICK deu
            duoc xu ly, khong rieng mon vua roi ra (user chot 06/09). Khong con buoc "xu ly rieng
            mon vua roi" - quet ca tui da bao tron no, ma lai khong phu thuoc vao viec doan dung
            slot nao moi.
          - Mon dang KHOA thi GIU NGUYEN. Do la duong de user giu lai mon minh muon
            (user: "de user muon giu item nao thi phai khoa no lai").
        """
        kq = {"mo": 0, "phan_giai": 0, "donate": 0, "vut": 0, "bo_qua": ""}
        boxes = _load_bliss_boxes()
        if not boxes:
            kq["bo_qua"] = "khong co bliss_bag.json"
            return kq
        # Loc theo danh sach user tick. Khong tick gi = KHONG LAM GI (khong coi la "mo het").
        _tick = set()
        for k, v in (chon or {}).items():
            if not v:
                continue
            try:
                _tick.add(int(k, 16) if isinstance(k, str) and k.lower().startswith("0x") else int(k))
            except Exception:
                pass
        if not _tick:
            kq["bo_qua"] = "chua tick ruong nao trong List ruong"
            return kq
        if self.has_legion is False:
            kq["bo_qua"] = "khong co quan doan"
            return kq
        if not self.co_the_donate_quan_doan():
            _con = int(float(getattr(self, "legion_donate_next", 0)) - time.time())
            kq["bo_qua"] = "chua du 24h trong quan doan (thu lai sau %d phut)" % max(0, _con // 60)
            return kq
        # fc theo TID de biet mon roi ra co phan giai duoc khong (nguon: items_gamedata.json,
        # cung nguon voi bang trong bliss_bag.json).
        gd = _load_gamedata_items()
        # DON RAC TRUOC KHI MO. Rac tu cac lan mo TRUOC da nam san trong tui, va don truoc thi:
        #  - tui day van don duoc (nhanh "tui day" ben duoi `return` som -> khong don thi rac ket
        #    vinh vien dung luc CAN don nhat),
        #  - don xong co cho de mo me moi.
        # Quet theo TAT CA ruong da tick, khong rieng loai sap mo: moi login chi mo MOT loai
        # (user chot 04/09) nen rac tich tu tu nhieu loai khac nhau.
        self._don_do_ruong_con_sot(_tick, boxes, gd, kq)
        for luot in ("thuong", "tinh"):
            for tid, info in boxes.items():
                if tid not in _tick:
                    continue
                if (info.get("luot") or "thuong") != luot:
                    continue
                can = max(1, int(info.get("kindCount") or 1))
                if not self.running:
                    return kq
                slot = next((s for s, (t, c) in list(self.bag_slots.items())
                             if t == tid and c > 0), None)
                if slot is None:
                    continue        # loai ruong nay het hang -> xet loai ke tiep
                trong = self.bag_capacity() - self.bag_used_slots()
                if trong < can:
                    kq["bo_qua"] = "tui day (con %d o, hop can %d)" % (trong, can)
                    log.info("[%s] MO HOP: tui day (con %d o) -> dung", self._label, trong)
                    return kq
                # Mo toi da: het stack, nhung khong qua so o trong (moi lan mo an 1 o).
                n = max(1, min(int(self.bag_slots[slot][1]), trong // can))
                truoc = dict(self.bag_slots)
                log.info("[%s] MO HOP: %s x%d (o trong %d)",
                         self._label, info.get("name") or ("0x%04x" % tid), n, trong)
                if not self.use_slot(slot, qty=n):
                    return kq
                kq["mo"] += n
                # CHO do ve tui roi QUET CA TUI - khong con buoc "xu ly rieng mon vua roi ra".
                # Quet ca tui da bao tron mon vua roi, ma lai khong phu thuoc vao viec doan dung
                # slot nao moi: cho hut thi lan login sau quet lai la don duoc, khong tich tu nua.
                # (Ban cu: cho 2.5s cho 10 hop, khong thay o moi la BO CA ME - 23 lan/ngay, va mon
                #  do nam lai VINH VIEN vi khong vong nao quay lai.)
                self._cho_tui_doi(truoc, wait=max(wait_item, 1.0 + 0.15 * n), it_nhat=n)
                # MOT LAN DUY NHAT cho ca luot login (user chot 04/09: "luc login ko can mo den
                # het ruong, chi can mo 1 lan, phan giai/donate/vut bo 1 lan la dc roi";
                # "mo 1 lan la het stack ruong do hoac full tui do"). Mot me = min(ca stack,
                # so o trong) - da tinh o `n` ngay tren. Con du thi de lan login sau.
                #
                # Truoc day vong `while` vet CAN stack roi moi sang loai ke tiep -> login bi keo
                # dai (log 00:13: mo x39 xong lai mo tiep x19 cua cung mot loai).
                self._don_do_ruong_con_sot(_tick, boxes, gd, kq)
                return kq
        self._don_do_ruong_con_sot(_tick, boxes, gd, kq)
        return kq

    def _don_do_ruong_con_sot(self, tick, boxes, gd, kq):
        """Quet CA TUI, don not do thuoc cac ruong DA TICK - TRU do dang KHOA.

        User chot 06/09: "tick mo ruong nao thi nhung item trong ruong do co trong tui do cung xu
        ly luon, tuy nhien do nao o trang thai khoa thi ko xu ly, de user muon giu item nao thi
        phai khoa no lai".

        VI SAO CAN: truoc day CHI dung vao mon VUA ROI RA trong me do. Chi can MOT me hut la mon
        do nam lai VINH VIEN - khong vong nao quay lai don. Ma me hut co that:
          - 23 lan `MO HOP: khong thay do roi ra -> dung` (cho 2.5s, server tra cham hon)
          - do ca ngay 06/09: mo 4.889 hop, chi xu ly 4.610 mon -> hut 279/ngay
        Cong don: 5.407 mon rac tu ruong con nam trong tui cua 177 acc (~30 mon/acc), trong do co
        ca mon `fc=50` phan giai duoc.

        CHI dung vao id cua ruong DA TICK - khong phai ca 304 id trong bliss_bag.json. Mon dang
        KHOA thi giu nguyen: do la duong de user giu lai mon minh muon.
        """
        ids = set()
        for tid, info in (boxes or {}).items():
            if tid not in tick:
                continue
            for it in (info.get("items") or []):
                try:
                    ids.add(int(it["id"]))
                except Exception:
                    pass
        if not ids:
            return
        sot = [s for s, (t, c) in list(self.bag_slots.items()) if int(t) in ids and c > 0]
        if not sot:
            return
        log.info("[%s] MO HOP: con %d mon thuoc ruong da tick trong tui -> don not",
                 self._label, len(sot))
        self._xu_ly_do_ruong(sot, gd, kq)

    def _item_bi_khoa(self, slot) -> bool:
        """O `slot` co dang KHOA khong. Doc tu ThingData da parse san (`bag_items[slot]["lock"]`,
        byte +29 - xem KNOWLEDGE.md muc ThingData va `Logic/Item.lua:95`).

        User chot 06/09: "do nao o trang thai khoa thi ko xu ly, de user muon giu item nao thi
        phai khoa no lai". Client cung lam the o MOI duong: `Item.DropItem`/`DropEquip` (vut),
        `UICompound` (phan giai/ghep), `UIFurnace` (lo), `UIArmy.ArmyFilter` (donate).

        KHONG doc duoc ThingData -> coi la KHOA (an toan hon: thieu tin thi dung dung vao do cua
        user, xem RULE_DIEU_PHOI L13 "khong biet" khac "khong sao").
        """
        try:
            info = (self.bag_items or {}).get(slot)
        except Exception:
            return True
        if not info:
            return True
        return bool(info.get("lock"))

    def _xu_ly_do_ruong(self, slots, gd, kq):
        """Phan giai / donate / VUT cac o trong `slots` (do thuoc ruong da tick).

        Thu tu: fc>0 -> PHAN GIAI (ra manh) | donate duoc -> DONATE | KHONG CA HAI -> VUT BO
        (user chot 03/09: "do nao ko phan giai ko donate duoc thi m vut bo luon"). Vi du mon ket:
        Hoai Nam Tu / Tam Luoc / Kim Quy Kinh - sach (kind=9, material=37): fc=0 nen khong phan
        giai, material 37 nam ngoai dai 1..36 cua ArmyFilter nen cung khong donate.
        """
        donate = []
        for s in slots:
            rec = self.bag_slots.get(s)
            if not rec:
                continue
            tid = rec[0]
            r = gd.get(tid) or {}
            nm = (r.get("name") or "0x%04x" % tid).strip()
            if self._item_bi_khoa(s):
                log.info("[%s] MO HOP: %s dang KHOA -> giu nguyen", self._label, nm)
                kq["khoa"] = kq.get("khoa", 0) + 1
                continue
            if int(r.get("fc") or 0) > 0:
                log.info("[%s] MO HOP: %s -> PHAN GIAI (%s manh)", self._label, nm, r.get("fc"))
                try:
                    if self.decompose_slot(s):
                        kq["phan_giai"] += 1
                except Exception as e:
                    log.warning("[%s] MO HOP: loi phan giai %s: %s", self._label, nm, e)
            elif self._donate_quan_doan_duoc(tid, r, s):
                log.info("[%s] MO HOP: %s -> DONATE quan doan (khong phan giai duoc)",
                         self._label, nm)
                donate.append(s)
            else:
                log.info("[%s] MO HOP: %s -> VUT BO (khong phan giai, khong donate duoc)",
                         self._label, nm)
                try:
                    if self.discard_item(s, rec[1]):
                        self.bag_slots.pop(s, None)
                        kq["vut"] = kq.get("vut", 0) + 1
                except Exception as e:
                    log.warning("[%s] MO HOP: loi vut %s: %s", self._label, nm, e)
        if donate:
            try:
                # Mot lenh nhieu slot (goi cho phep <<slot>>), do dung nguyen giay lat sau.
                if self.donate_legion_equip(donate):
                    kq["donate"] += len(donate)
                    for s in donate:
                        self.bag_slots.pop(s, None)
            except Exception as e:
                log.warning("[%s] MO HOP: loi donate trang bi: %s", self._label, e)

    # DONATE QUAN DOAN duoc khong - sao y UIArmy.ArmyFilter (bo loc tui khi chon do de dong gop).
    # Mon truot bo loc nay thi CLIENT khong cho chon -> bot gui la server bo qua IM LANG.
    _DONATE_MAT_OK = set(range(1, 9)) | set(range(10, 23)) | set(range(24, 37))   # BO 9 va 23
    _DONATE_KIND_CAM = {20, 21, 22}
    _DONATE_ID_CAM = {10505, 19209, 20209, 21609, 22909, 20747, 20748, 20749, 26209, 26210, 26211,
                      26212, 26213, 26214, 26215, 26216, 26217, 26218, 26219, 16000, 21610, 19210,
                      22910, 20210, 11046}

    def _donate_quan_doan_duoc(self, tid: int, rec: dict, slot=None) -> bool:
        """rec = ban ghi items_gamedata.json (can `mat` material, `lv` level, `kd` kind).

        `slot` (neu co) de kiem KHOA: `UIArmy.ArmyFilter:2647` co `if itemSave.isLock then
        return false end` - ban sao chep cua bot truoc day THIEU dung dong nay.
        """
        if slot is not None and self._item_bi_khoa(slot):
            return False
        try:
            if int(rec.get("kd") or 0) == 53:
                return True
            if int(rec.get("lv") or 0) == 0:
                return False
            if int(tid) in self._DONATE_ID_CAM:
                return False
            if int(rec.get("mat") or 0) not in self._DONATE_MAT_OK:
                return False
            if int(rec.get("kd") or 0) in self._DONATE_KIND_CAM:
                return False
        except Exception:
            return False
        return True

    # ===== TU MO HOP/TUI TRANG BI (user chot 03/09) =====
    # Mo hop -> ra trang bi -> PHAN GIAI duoc thi phan giai lay manh trang bi; KHONG phan giai
    # duoc thi DONATE quan doan. KHONG co nhanh ban shop (user bo).
    #
    # DIEU KIEN: phai CO quan doan VA da vao > 24h. Client KHONG tu biet du 24h chua (khong co
    # truong thoi gian gia nhap o bat ky dau) - chi co cach gui roi doc ma loi: S:039-015 ma 32
    # = "Gia nhap quan doan chua du 24 tieng, khong the quyen gop" -> hoan 12h roi thu lai.
    LEGION_DONATE_RETRY = 12 * 3600

    def _on_legion_msg(self, ma: int):
        """S:039-015 <軍團訊息> +訊息種類(1). Chi quan tam ma 32 = chua du 24h -> khong donate duoc."""
        try:
            ma = int(ma)
        except Exception:
            return
        if ma != 32:
            return
        self.legion_donate_next = time.time() + self.LEGION_DONATE_RETRY
        log.info("[%s] QUAN DOAN: gia nhap chua du 24h -> chua donate duoc, hoan %d gio",
                 self._label, self.LEGION_DONATE_RETRY // 3600)

    def co_the_donate_quan_doan(self) -> bool:
        """CO quan doan VA khong con trong thoi gian hoan (vua bi tu choi vi chua du 24h)."""
        if self.has_legion is False:
            return False
        nxt = float(getattr(self, "legion_donate_next", 0.0) or 0.0)
        return (not nxt) or time.time() >= nxt

    def donate_legion_equip(self, slots, wait: float = 0.8) -> bool:
        """DONATE TRANG BI cho quan doan - DUNG CHUNG goi voi nguyen lieu:
        C:039-015 <捐獻資源> = 0x27 sub 0x0f + [tien i32 = 0] + <<slot 1B>>.

        SUA 03/09: truoc do dung C:039-053 (0x27 sub 0x35) LA SAI. Goi do ten <存入武器> = "gui vu
        khi vao kho quan doan" (kho quan bi), khong phai quyen gop -> server bo qua im lang, do
        van nam trong tui, tui day ngay (log that 18:55: mo 21 hop, "DONATE" 21 mon, ngay sau do
        "tui day (con 0 o)").
        Client dung DUNG MOT nut "Dong gop" cho ca nguyen lieu lan trang bi: UIArmy chi mo UIBag
        voi MOT bo loc `ArmyFilter`, va trang bi CO qua duoc bo loc do (material 1..36).
        """
        ds = []
        for s in slots or ():
            try:
                s = int(s)
            except Exception:
                continue
            if 0 < s < 256:
                ds.append(s)
        if not ds:
            return False
        self.send(0x7c, b"\x04\x00")            # mo panel quan doan (giong donate_legion)
        time.sleep(0.4)
        self.send(0x27, b"\x0f\x00\x00\x00\x00\x00" + bytes(ds))
        time.sleep(wait)
        return True

    def donate_legion(self, wait: float = 0.5):
        """DONATE nguyen lieu RAC cho quan doan (don tui - van tieu ra nhieu rac). C2S 0x27:
          0f 00 00 00 00 00 [slot 1B]   (giong use-item: tham chieu SLOT, KHONG phai tid).
        Game CHI cho chon slot, KHONG chon so luong -> moi lenh donate CA STACK o slot do.
        AN TOAN: chi donate SLOT co tid nam trong config.DONATE_ITEMS (danh sach TID rac, template
        -> dung mọi acc). Xac nhan qua 2 capture: 5 go ngo dong (slot 0x80) va 20 vai tho (slot
        0x74) -> deu la '0x27 0f000000000000[slot]', so luong khong nam trong goi."""
        # NGUON MOI: donate_materials.json = TAT CA nguyen lieu (20 kind), MAC DINH donate het; user
        # danh dau GIU trong GUI (material_modes[tid]='keep'). Fallback DONATE_ITEMS (list rac cu) neu
        # chua co material list. Ten lay tu ca 2 nguon.
        mats = getattr(config, "DONATE_MATERIALS", {}) or {}
        donate_names = getattr(config, "DONATE_ITEMS", {}) or {}
        keep = getattr(self, "material_modes", None) or {}
        if mats:
            donate_tids = {t for t in mats if str(keep.get(t, "")).lower() != "keep"}
        else:   # chua co material list -> giu hanh vi cu (list rac cung)
            donate_tids = set()
            for k in donate_names:
                try:
                    donate_tids.add(int(k, 16) if isinstance(k, str) else int(k))
                except Exception:
                    pass
        if not donate_tids:
            return
        # SNAPSHOT truoc: cac slot co item can donate. Donate lam RONG chinh slot do, KHONG doi
        # index cac slot khac -> duyet list snapshot la an toan (khong can quet lai giua chung).
        targets = [(slot, tid, cnt) for slot, (tid, cnt) in list(self.bag_slots.items())
                   if cnt > 0 and tid in donate_tids]
        if not targets:
            return
        self.send(0x7c, b"\x04\x00")   # mo panel quan doan (giong claim_legion_gift)
        opened_at = time.time()
        while self.running and self.has_legion is False and time.time() - opened_at < 1.5:
            time.sleep(0.1)
        if self.has_legion is False:
            # DA DO THAT (mo panel + cho) -> chac chan khong co quan doan. Danh dau de chuyen di
            # ban Noi Dat ban luon cho nguyen lieu nay (xem _sell_donate_materials).
            self._no_legion_confirmed = True
            log.info("[%s] Donate quan doan: khong co quan doan -> de danh BAN o Nha buon (%d slot "
                     "nguyen lieu)", self._label, len(targets))
            return
        self._no_legion_confirmed = False
        remain = 0.4 - (time.time() - opened_at)
        if remain > 0:
            time.sleep(remain)
        items = _load_gamedata_items()
        total = 0
        # GOM CA LOT VAO MOT LENH - dung nhu client that: C:039-015 <捐獻資源> = [tien i32] +
        # <<slot 1B>> (UIArmy.OnClick_DonateResource duyet UIBag.GetSelect() roi WriteByte TUNG
        # slot da chon vao CUNG mot sendBuffer). Truoc day bot gui tung slot mot, moi lenh nghi
        # `wait` - van tieu ra nhieu rac nen co acc donate ca chuc slot = mat vai giay moi lan login.
        _slots = []
        for slot, tid, cnt in targets:
            if not self.running:
                break
            _slots.append(slot & 0xFF)
            self.bag_slots.pop(slot, None)   # donate ca stack -> slot rong (S2C 0x17 se update lai)
            total += cnt
            _nm = ((mats.get(tid) or {}).get("name")
                   or (items.get(tid) or {}).get("name")
                   or donate_names.get(tid)
                   or donate_names.get(hex(tid), ""))
            log.info("[%s] donate quan doan slot=%d tid=0x%04x x%d ('%s')",
                     self._label, slot, tid, cnt, _nm)
        if _slots:
            self.send(0x27, b"\x0f\x00\x00\x00\x00\x00" + bytes(_slots))
            time.sleep(wait)
        if total:
            log.info("[%s] Donate quan doan: tong %d nguyen lieu rac (%d slot, 1 lenh) -> don tui",
                     self._label, total, len(_slots))

    @staticmethod
    def _decode_vantieu_req(kind: int, effect1: int, effect2: int):
        """Decode S2C 0x56/0400 or 0x56/0300 effect ids -> {he, doanh}.

        Client game tra `DispatchBonus_C.dat`: effect id -> conditionKind/value.
        Giu `vantieu_requests.json` lam fallback cho data cu/chua ship asset moi.
        """
        req = {}
        effects = getattr(config, "VANTIEU_DISPATCH_EFFECTS", {}) or {}
        for effect_id in (effect1, effect2):
            info = effects.get(str(effect_id)) or effects.get(effect_id) or {}
            if info.get("he"):
                req["he"] = info.get("he")
            if info.get("doanh"):
                req["doanh"] = info.get("doanh")
        if req:
            return req
        code = bytes([kind & 0xFF, effect1 & 0xFF, effect2 & 0xFF]).hex()
        return (getattr(config, "VANTIEU_REQUESTS", {}) or {}).get(code)

    def _on_vantieu(self, pkt: bytes):
        """S2C 0x56 panel Dispatch:
          0300: [count] + count*[slot][start OLE][end OLE][kind][innIndex][effect1][effect2]
          0400: [kind][effect1][effect2] = yeu cau slot trong hien tai.
          0500: [result] = ket qua nhan thuong.
        """
        body = pkt[7:]
        if len(body) < 3:
            return
        if body[0:2] == b"\x05\x00":          # ket qua nhan thuong van tieu
            code = body[2]
            slot = self._vantieu_claim_pending_slot
            self._vantieu_claim_result = (slot, code)
            self._vantieu_claim_pending_slot = None
            if code == 1:
                if slot is not None:
                    self.vantieu_slots.pop(slot, None)
                self._vantieu_claim_retry_after = 0.0
                log.info("[%s] Van tieu: nhan qua slot %s THANH CONG",
                         self._label, slot if slot is not None else "?")
            else:
                msg = VANTIEU_CLAIM_RESULT_TEXT.get(code, f"ma loi {code}")
                log.warning("[%s] Van tieu: nhan qua slot %s THAT BAI: %s",
                            self._label, slot if slot is not None else "?", msg)
            self._vantieu_claim_event.set()
            return
        if body[0:2] == b"\x06\x00":          # so slot DA MO (con lai khoa = can vang unlock)
            self.vantieu_unlocked = body[2]
            return
        if body[0:2] == b"\x04\x00" and len(body) >= 5:  # [kind][effect1][effect2] cho slot trong hien tai
            self.vantieu_req_code = body[2:5].hex()
            self.vantieu_req = self._decode_vantieu_req(body[2], body[3], body[4])
            log.info("[%s] Van tieu req hien tai: code=%s -> %s",
                     self._label, self.vantieu_req_code, self.vantieu_req)
            return
        if body[0:2] != b"\x03\x00":
            return
        count = body[2]
        off = 3
        for _ in range(count):
            if off + 21 > len(body):
                break
            slot = body[off]
            try:
                start_ole = struct.unpack("<d", body[off + 1:off + 9])[0]
                end_ole = struct.unpack("<d", body[off + 9:off + 17])[0]
            except Exception:
                break
            kind = body[off + 17]
            pet = body[off + 18]       # innIndex server dang cho chay trong slot nay
            effect1 = body[off + 19]
            effect2 = body[off + 20]
            req = self._decode_vantieu_req(kind, effect1, effect2)
            if start_ole <= 0:                 # slot rong (da claim)
                self.vantieu_slots.pop(slot, None)
            else:
                self.vantieu_slots[slot] = {
                    "end": end_ole,
                    "pet": pet,
                    "kind": kind,
                    "effect1": effect1,
                    "effect2": effect2,
                    "req": req,
                }
            off += 21

    def _on_vantieu_roster(self, pkt: bytes):
        """S2C 0x1f sub=0600 <客棧武將資料>: list pet KHO (nha tro) de van tieu, gui luc login.

        Bo cuc 1 ban ghi (BOC TU GOI THAT vt_kholog.pcap, doi chieu npc_names.json khop 4/4 -
        xem KNOWLEDGE.md muc van tieu; protocal.lua:6798 ghi THIEU truong exp nen dung bang nay):
            +0 index nha tro 1B (= index gui 0x56 0200) | +1 NPCID 2B LE | +3 level 1B
            +4 exp 4B | +8 HP 4B | +12 L 1B = do dai VUNG ten
            +13 vung ten L byte: UTF-16LE ket thuc \\0, PHAN DU LA RAC | +13+L trang thai 1B

        L la do dai VUNG chu KHONG phai do dai ten -> cat ten tai \\0 dau tien nhung nhay nguyen
        L byte. Parser cu khong doc L, quet toi \\0\\0 roi TU DONG BO lai bang cach do +1 - ra dung
        ten nhung gion. Nay doc tuan tu theo L.

        Ghi CA pet id: id la khoa ON DINH de user tick pet nao duoc van tieu (index nha tro XE DICH
        khi them/bot pet -> tick theo index se truot sang con khac)."""
        b = pkt[7:]
        # GOM DON theo index, KHONG thay ca bang. Client lam dung the: protocolTable[31][6] chi
        # goi Inn.SaveNpc(index,...) ghi vao O this.npcs[index]; xoa sach chi o Inn.Clear() rieng.
        # BUG THAT (user bao "chi nhan dung 1 con"): bot gan `self.vantieu_roster = roster` = THAY
        # SACH moi goi -> server chia pet ra nhieu goi thi GOI CUOI THANG, chi con 1 con (acc
        # quanmot con moi index 3, khong phai con dau -> dung la goi cuoi thang chu khong phai
        # parse hong).
        roster = dict(self.vantieu_roster or {})
        ids = dict(self.vantieu_roster_ids or {})
        moi, pos = {}, 2
        while pos + 14 <= len(b):
            index = b[pos]
            npc_id = struct.unpack_from("<H", b, pos + 1)[0]
            # `protocal.lua:6798` S:031-006 <客棧武將資料>:
            #   +客棧索引(1) +NPCID(2) +等級(1) +HP(4) +L(1) +名字(L) +武將狀態(1)
            # -> cap nam ngay sau NPCID. (Vi tri cua L thi bot do thuc nghiem ra +12, lech 4 byte so
            # voi mo ta tren; GIU NGUYEN cai da chay dung, chi lay them cap o +3.)
            npc_lv = b[pos + 3]
            ln = b[pos + 12]
            npos = pos + 13
            if npos + ln + 1 > len(b):
                break                        # cut goi -> dung, KHONG do lai
            vung = b[npos:npos + ln]
            # L la do dai VUNG ten, ten ket thuc \x00 BEN TRONG vung, phan du la RAC. Tim \x00\x00 o
            # vi tri CHAN: ten ket thuc 'i' (69 00) roi terminator 00 00 -> chuoi ...69 00 00 00,
            # bytes.find(b"\x00\x00") bat trung cap LECH -> cat mat ky tu cuoi.
            cut = len(vung) - (len(vung) % 2)
            for k in range(0, len(vung) - 1, 2):
                if vung[k:k + 2] == b"\x00\x00":
                    cut = k
                    break
            name = vung[:cut].decode("utf-16-le", "ignore")
            pos = npos + ln + 1              # +1 = byte trang thai; bo cuc CO DINH -> nhay thang
            # KHONG loai ban ghi theo NOI DUNG ten. Parser cu doi ten "toan ky tu in duoc", sai
            # mot cai la do lui `pos += 1` -> lech het phan sau (acc quanmot mat 2 con dau).
            if 1 <= index <= 30 and name:
                # TEN LAY THEO `pid` tu PET_NAMES (pets.json) - DUNG NGUON voi tui do va tab battle
                # (`gui._pet_list` / `_pet_tab_title`). Truoc day cho nay dung TEN SERVER GUI trong
                # goi, nen cung mot con pet ma nha tro hien mot kieu, tui do/battle hien kieu khac.
                # Ten server chi con la du phong khi pets.json chua co con do.
                _ten = getattr(config, "PET_NAMES", {}).get(npc_id) or name
                moi[index] = ten_pet_kem_lv(_ten, npc_lv)
                ids[index] = npc_id
        roster.update(moi)
        if moi:
            self.vantieu_roster = roster
            self.vantieu_roster_ids = ids
            log.info("[%s] Van tieu roster (kho): %s", self._label,
                     {i: "%s#%04x" % (roster[i], ids.get(i, 0)) for i in sorted(roster)})
            # Cache de dialog van tieu sua duoc khi acc DA TAT (khoi bat acc len chi de tick pet).
            try:
                save_inn_cache(getattr(self, "_username", None),
                               [[int(ids.get(i, 0)), roster[i]] for i in sorted(roster)])
            except Exception as e:
                log.debug("[%s] cache pet nha tro loi: %s", self._label, e)

    # loai (種類) trong S:023-135 -> khoa noi bo
    HAN_DUNG_LOAI = {1: "ba_dau", 2: "da_thu_ho", 3: "the_vip"}

    def _on_han_dung(self, pkt):
        """S:023-135 <各種到期時間>: luu MOC HET HAN cua Ba Dau / Da Thu Ho / The Vip Thang.

        `ket qua` = 2 la THAT BAI (kem `nguyen nhan`), KHONG phai "loai 2".
        Khong co lenh hoi (`C:023-135` khong ton tai) - server tu gui luc login va khi nap them.
        """
        body = pkt[9:]
        if len(body) < 10 or body[0] != 1:
            return                      # rong / ket qua 2 = that bai
        loai = self.HAN_DUNG_LOAI.get(body[1])
        if not loai:
            return
        try:
            ole = struct.unpack("<d", body[2:10])[0]
        except Exception:
            return
        if not ole:                     # 0 = KHONG co han (client: `time ~= 0` moi set)
            self.han_dung.pop(loai, None)
            return
        try:
            het = self._ole_to_dt(ole)
        except Exception:
            return
        self.han_dung[loai] = het
        log.info("[%s] Han dung %s: het luc %s", self._label, loai,
                 het.strftime("%d/%m/%Y %H:%M"))

    def _nho_gio_server(self, raw8):
        """Luu DO LECH gio server so voi gio may (y het `CGTimer.SetServerTime`).

        Luu DO LECH chu khong luu moc: gio server troi tiep theo dong ho may, khong dung yen o moc
        vua nhan. `raw8` = OLE Automation Date (double, little-endian).
        """
        import datetime
        try:
            ole = struct.unpack("<d", bytes(raw8))[0]
        except Exception:
            return
        if not ole or ole < 30000:        # < ~1982 -> rac, khong phai gio that
            return
        try:
            self._server_time_span = self._ole_to_dt(ole) - datetime.datetime.now()
        except Exception:
            return

    def gio_server(self):
        """Gio SERVER hien tai (datetime). None = chua nhan duoc.

        Han dung phai so voi gio SERVER, khong phai gio may: client so voi `CGTimer.serverTime`
        (`DrugBuff.GetTimeBuff`). Bot luu DO LECH luc login roi cong vao gio may, y het
        `CGTimer.SetServerTime` (`serverTimeSpan = serverTime - DateTime.Now`).
        """
        import datetime
        lech = getattr(self, "_server_time_span", None)
        if lech is None:
            return None
        return datetime.datetime.now() + lech

    def han_dung_con_lai(self, loai="ba_dau"):
        """Thoi gian CON LAI (timedelta) cua mot han dung. None = khong co / chua biet / DA HET."""
        het = (getattr(self, "han_dung", None) or {}).get(loai)
        now = self.gio_server()
        if het is None or now is None:
            return None
        con = het - now
        return con if con.total_seconds() > 0 else None

    @staticmethod
    def _ole_to_dt(ole):
        import datetime
        return datetime.datetime(1899, 12, 30) + datetime.timedelta(days=ole)

    def vantieu_candidates(self):
        """Danh sach (inn_index, ten) DUOC PHEP van tieu, da loc theo pet user TICK.

        LUAT (yeu cau user):
          - roster rong (acc KHONG co pet trong nha tro) -> [] , caller bo qua.
          - khong tick con nao, HOAC tick HET  -> dung TAT CA (y het hanh vi cu, khong doi thoi
            quen user dang chay bot).
          - tick le  -> CHI nhung con duoc tick. Sau do _match_vantieu_pet van cham diem he/doanh
            trong pham vi nay; khong con nao khop thi no tu lay con DAU danh sach (score 0).
        Tick luu theo PET ID chu khong theo index nha tro: index XE DICH khi them/bot pet -> tick
        theo index se truot sang con khac."""
        roster = self.vantieu_roster or {}
        tat_ca = [(i, roster[i]) for i in sorted(roster)]
        if not tat_ca:
            return []
        pick = set(self.vantieu_pick_ids or ())
        if not pick:
            return tat_ca
        ids = self.vantieu_roster_ids or {}
        loc = [(i, nm) for i, nm in tat_ca if ids.get(i) in pick]
        # Tick HET = coi nhu khong loc (giong "khong tick con nao").
        if not loc or len(loc) == len(tat_ca):
            return tat_ca
        return loc

    def _match_vantieu_pet(self, cands, used, req):
        """cands = list (inn_index, ten_pet). Chon con KHOP 'req' (he,doanh) nhat trong con CON TRONG.
        Score: dung ca he+doanh=2, dung 1=1, ko khop=0 (van gui de duoc qua co ban).
        Tra ve inn_index, None = het con trong. (req luon DA BIET - ma la xu ly o do_van_tieu.)"""
        def _norm(v):
            return "Huynh" if v == "Hoang" else v

        best, best_score, best_nm, best_hd = None, -1, None, None
        req_he = _norm(req.get("he"))
        req_doanh = _norm(req.get("doanh"))
        for idx, nm in cands:
            if idx in used:
                continue
            hd = config.PET_HEDOANH.get(nm, {})
            score = (_norm(hd.get("he")) == req_he) + (_norm(hd.get("doanh")) == req_doanh)
            if score > best_score:
                best, best_score, best_nm, best_hd = idx, score, nm, hd
        if best is None:
            return None
        tag = {2: "khop ca he+doanh", 1: "khop 1", 0: "KHONG khop (gui tam, qua co ban)"}[best_score]
        log.info("[%s] Van tieu match: yeu cau=%s -> slot %d '%s' %s [%s]",
                 self._label, req, best, best_nm, best_hd, tag)
        return best

    @task_report("van tieu", PHASE_LOGIN_CHORE)
    def do_van_tieu(self):
        """Van tieu (escort) opcode 0x56. Gui pet (VANTIEU_PETS = index list quan tro) ->
        ~4h sau nhan qua. Goi luc login + dinh ky.
          mo panel:  0x56 0100  -> S2C 0x56 0300 (slot + gio ket thuc OLE)
          gui pet:   0x56 0200 [pet_index]
          nhan qua:  0x56 0500 [slot]
        CLAIM theo GIO KET THUC tu server (now >= end), KHONG hardcode thoi luong.
        So luot/ngay = server RoleCount id=8.
        TRA VE: epoch thoi diem CAN GOI LAI (escort xong som nhat) hoac None (het viec hom nay)
        -> caller hen dung gio, KHONG check mu dinh ky."""
        import datetime
        # Cong tac van tieu nay la PER-ACC (bang setting Hoi HP/SP cua tung acc), khong con la o
        # tick CHUNG o Cai dat nang cao. config.VANTIEU_ENABLE chi con la mac dinh cho acc chua
        # co thiet lap rieng.
        if not getattr(self, "vantieu_enable", getattr(config, "VANTIEU_ENABLE", False)):
            return None
        retry_after = float(getattr(self, "_vantieu_claim_retry_after", 0.0) or 0.0)
        if retry_after > time.time():
            log.info("[%s] Van tieu: tam dung nhan qua sau loi truoc, check lai luc %s",
                     self._label, datetime.datetime.fromtimestamp(retry_after).strftime("%H:%M:%S"))
            return retry_after

        def _refresh_panel(delay=1.2):
            self.vantieu_slots = {}
            self.vantieu_req_code = None
            self.vantieu_req = None
            self.send(0x56, b"\x01\x00")      # mo panel
            time.sleep(delay)

        pets = list(getattr(config, "VANTIEU_PETS", []) or [])
        _refresh_panel()
        now = datetime.datetime.now()
        # 1) NHAN qua slot da xong (now >= gio ket thuc)
        claimed = False
        claim_blocked_until = 0.0
        for slot, info in list(self.vantieu_slots.items()):
            if now >= self._ole_to_dt(info["end"]):
                self._vantieu_claim_pending_slot = slot
                self._vantieu_claim_result = None
                self._vantieu_claim_event.clear()
                self.send(0x56, b"\x05\x00" + bytes([slot & 0xFF]))
                if self._vantieu_claim_event.wait(2.0):
                    ack_slot, code = self._vantieu_claim_result or (slot, None)
                    if code == 1:
                        self.vantieu_slots.pop(ack_slot or slot, None)
                        claimed = True
                    elif code == 5:
                        claim_blocked_until = time.time() + 10 * 60
                        self._vantieu_claim_retry_after = claim_blocked_until
                        log.warning("[%s] Van tieu: tui do day, anh don bot tui roi bot se thu lai sau",
                                    self._label)
                        break
                    else:
                        msg = VANTIEU_CLAIM_RESULT_TEXT.get(code, f"ma loi {code}")
                        claim_blocked_until = time.time() + 5 * 60
                        self._vantieu_claim_retry_after = claim_blocked_until
                        log.warning("[%s] Van tieu: tam dung nhan qua vi server bao %s",
                                    self._label, msg)
                        break
                else:
                    self._vantieu_claim_pending_slot = None
                    claim_blocked_until = time.time() + 60
                    self._vantieu_claim_retry_after = claim_blocked_until
                    log.warning("[%s] Van tieu: khong nhan duoc ack 0x56/0500 khi nhan slot %d -> thu lai sau",
                                self._label, slot)
                    break
        if claimed:
            _refresh_panel(0.9)
        if claim_blocked_until > 0:
            _refresh_panel(0.9)
            log.info("[%s] Van tieu: check lai luc %s",
                     self._label, datetime.datetime.fromtimestamp(claim_blocked_until).strftime("%H:%M:%S"))
            return claim_blocked_until
        # 2) GUI pet moi: CHI vao slot DA MO (1..vantieu_unlocked, KHONG tu unlock = ton vang)
        #    va trong gioi han luot/ngay (vantieu_max). slot dang chay -> bo qua.
        # cands = list (inn_index, ten_pet) de match. Uu tien ROSTER tu server (0x1f, AUTO);
        # khong co thi dung config VANTIEU_PETS_NAMES (theo thu tu slot).
        if self.vantieu_roster:
            cands = self.vantieu_candidates()      # da loc theo pet user TICK (xem ham do)
            if not cands:
                log.info("[%s] Van tieu: acc khong co pet nao trong nha tro -> bo qua", self._label)
        else:
            cands = [(i + 1, nm) for i, nm in enumerate(getattr(config, "VANTIEU_PETS_NAMES", []) or [])]
        # Smart match: 0400 = [kind][effect1][effect2] cua job slot trong hien tai. Gui xong 1 pet
        # thi refresh panel de server tra yeu cau moi cho slot tiep theo (slot 1/2 phan biet qua 0300).
        smart = bool(cands) and (
            bool(getattr(config, "VANTIEU_DISPATCH_EFFECTS", {})) or
            bool(getattr(config, "VANTIEU_REQUESTS", {}))
        )
        if pets or smart:
            daily_cap = self.vantieu_max or 3
            unlocked = self.vantieu_unlocked or 1
            # Client game hien "So luot mien phi hom nay: X/Y" bang
            # RoleCount.Get(ERoleCount.Dispatch=8) / RoleCount.Max(...). Khong doan local.
            if self.vantieu_started is None:
                log.info("[%s] Van tieu: chua co RoleCount 8 tu server -> chua gui pet moi",
                         self._label)
                started = daily_cap
            else:
                started = self.vantieu_started
            sent_before = started
            # BUG CO SAN (lo ra khi user chi tick VAI pet): `used` cu chi nho pet gui trong LAN CHAY
            # NAY, khong nho pet DANG chay o slot tu truoc -> gui lai chinh con do. Truoc day it lo
            # vi roster nhieu con nen hiem khi dung; nhung "mo 2-3 slot ma chi tick 1 pet" thi sai
            # chac chan. vantieu_slots[slot]["pet"] chinh la innIndex server dang cho chay -> loai.
            used = {info.get("pet") for info in self.vantieu_slots.values()
                    if info.get("pet")}
            if used:
                log.info("[%s] Van tieu: %d pet dang chay do -> khong gui lai: %s",
                         self._label, len(used), sorted(used))
            i = 0
            while started < daily_cap:
                unlocked = self.vantieu_unlocked or 1
                occupied = set(self.vantieu_slots)
                free_slots = [s for s in range(1, unlocked + 1) if s not in occupied]
                if not free_slots:
                    break
                if smart:
                    req = self.vantieu_req or config.VANTIEU_REQUESTS.get(self.vantieu_req_code or "")
                    if req is None:            # MA LA (hiem neu bang 20/20 du) -> GUI DAI con trong
                        log.warning("[%s] Van tieu: ma yeu cau '%s' chua co trong bang -> gui dai con "
                                    "trong. Can soi 0x56/0400 neu lap lai.",
                                    self._label, self.vantieu_req_code)
                        pet = next((idx for idx, _ in cands if idx not in used), None)
                    else:
                        pet = self._match_vantieu_pet(cands, used, req)
                    if pet is None:            # het con trong
                        break
                else:                          # gui theo index co dinh (VANTIEU_PETS)
                    if i >= len(pets):
                        break
                    pet = pets[i]; i += 1
                slot = free_slots[0]
                self.send(0x56, b"\x02\x00" + bytes([pet & 0xFF]))
                time.sleep(0.9)
                used.add(pet); started += 1
                if self.vantieu_started is not None:
                    self.vantieu_started = max(self.vantieu_started, started)
                log.info("[%s] Van tieu: gui pet #%d -> slot %d (du kien, da gui %d/%d, %d slot mo)",
                         self._label, pet, slot, started, daily_cap, unlocked)
                _refresh_panel(0.9)
            if started == sent_before:
                occupied = set(self.vantieu_slots)
                free_slots = [s for s in range(1, (self.vantieu_unlocked or 1) + 1) if s not in occupied]
                if started >= daily_cap:
                    log.info("[%s] Van tieu: da gui du %d/%d luot hom nay -> bo qua",
                             self._label, started, daily_cap)
                elif not free_slots:
                    log.info("[%s] Van tieu: %d/%d slot dang chay -> cho xong roi gui tiep",
                             self._label, len(occupied), self.vantieu_unlocked or 1)
                elif used:
                    # Ca user neu: mo 2-3 slot ma chi tick 1 pet -> con do dang chay, con lai
                    # KHONG duoc tick nen khong duoc phep gui. Phai CHO chu khong gui bua.
                    log.info("[%s] Van tieu: %d pet duoc tick deu dang van tieu -> CHO xong roi "
                             "gui tiep (con slot trong nhung khong duoc gui pet ngoai list tick)",
                             self._label, len(used))
                else:
                    log.info("[%s] Van tieu: khong co pet phu hop/con trong de gui -> bo qua",
                             self._label)
        else:
            log.info("[%s] Van tieu: chua co danh sach pet van tieu -> bo qua", self._label)
        # HEN GIO: escort dang chay xong som nhat (panel da cap nhat slot moi gui qua _on_vantieu).
        ends = [self._ole_to_dt(info["end"]).timestamp() for info in self.vantieu_slots.values()]
        if ends:
            nxt = min(ends) + 10        # +10s dem cho chac chan da xong
            log.info("[%s] Van tieu: check lai luc %s",
                     self._label, datetime.datetime.fromtimestamp(nxt).strftime("%H:%M:%S"))
            return nxt
        return None                     # khong con escort dang chay -> het viec hom nay

    def _on_gift(self, pkt: bytes):
        """S2C 0x57 sub=2: [02 00][type 1B][status 1B]. type=03 qua online, type=01 DIEM DANH.
        status=0 = thanh cong."""
        if len(pkt) < 11:
            return
        if int.from_bytes(pkt[7:9], "little") == 0x02:
            gtype = pkt[9]; status = pkt[10]
            if gtype in (0x01, 0x04):              # diem danh / qua 14 ngay (log DEBUG -> ko spam scan)
                self._gift_status[gtype] = status
                log.debug("[%s] Gift type=%d: status=%d", self._label, gtype, status)
            elif gtype == ONLINE_GIFT_KIND:        # qua online (type=03)
                pending = self._online_gift_pending
                self._online_gift_pending = None
                self._online_gift_pending_ts = 0.0
                if pending is not None and status in (0, 2):
                    flag = _load_online_gift_flags().get(int(pending))
                    if flag:
                        self._bitflag_set(flag, True)
                        if self._bitflags_loaded:
                            self._refresh_online_claimed_from_bitflags()
                    self.claimed_gifts.add(int(pending))
                if pending is not None:
                    if status == 0:
                        msg = f"moc {pending} phut THANH CONG"
                    elif status == 2:
                        msg = f"moc {pending} phut server bao DA NHAN"
                    else:
                        msg = f"moc {pending} phut status={status}"
                    log.info("[%s] Qua online: %s", self._label, msg)
                else:
                    log.info("[%s] Qua online: status=%d (khong co moc pending)", self._label, status)
            else:
                log.info("[%s] Qua online: %s", self._label,
                         "THANH CONG" if status == 0 else f"status={status}")

    # ---- parse skill DA HOC DAY DU (0x05 char-info) ----
    def _on_char_skill_data(self, pkt: bytes):
        """S:008-013: he + diem skill con lai + CAP tung skill nhan vat. Xem KNOWLEDGE 7q."""
        b = pkt[9:]
        if len(b) < 5:
            return
        el = b[0]
        diem = int.from_bytes(b[1:3], "little")
        cnt = int.from_bytes(b[3:5], "little")
        if cnt > 200 or 5 + cnt * 3 > len(b):
            return                       # goi cut / doc lech -> KHONG ghi de du lieu dang dung
        lvs = {}
        for i in range(cnt):
            p = 5 + i * 3
            sid = int.from_bytes(b[p:p + 2], "little")
            if sid:
                lvs[sid] = b[p + 2]
        self.char_element = el
        self.char_skill_point = diem
        self.char_skill_lv = lvs
        for sid in lvs:                  # 0x05/0x28 co the toi sau -> giu danh sach hop nhat
            if sid not in self.state.skills_char:
                self.state.skills_char.append(sid)
        log.info("[%s] Skill nhan vat: he=%s, con %d diem skill, %d skill da hoc",
                 self._label, el, diem, len(lvs))

    def _parse_skill_list_0x05(self, pkt: bytes):
        """Trong goi char-info 0x05 co list skill DA HOC: [count 2B LE] + count*[skill 2B LE]
        [level 1B]. (0x28 chi la skill BAR -> thieu skill khong dat phim tat.) Tim list bang
        chu ky: 1 vi tri co count C nho (1..60) + dung C entry [id trong 0x2710..0x3fff][lv 1..99].
        Lay run dau tien -> UNION vao skills_char (khong mat skill bar)."""
        payload = pkt[7:]
        n = len(payload)
        for off in range(0, n - 3):
            c = int.from_bytes(payload[off:off + 2], "little")
            if not (1 <= c <= 60) or off + 2 + c * 3 > n:
                continue
            ids = []
            lv_map = {}
            ok = True
            for k in range(c):
                p = off + 2 + k * 3
                sid = int.from_bytes(payload[p:p + 2], "little")
                lv = payload[p + 2]
                if not (0x2710 <= sid <= 0x3fff and 1 <= lv <= 99):
                    ok = False
                    break
                ids.append(sid)
                lv_map[sid] = lv
            if ok and ids:
                # GIU THU TU (skill[0]=boss fallback): append id chua co. 0x05 la list day du.
                for s in ids:
                    if s not in self.state.skills_char:
                        self.state.skills_char.append(s)
                # GIU CA CAP (goi nay von co [id][lv] nhung parser cu VUT lv di). Chi dung khi
                # `S:008-013` chua toi - goi do moi la nguon chuan, den sau thi ghi de.
                if not self.char_skill_lv:
                    self.char_skill_lv = dict(lv_map)
                log.info("[%s] Char skills (day du tu 0x05, %d): %s", self._label, len(ids),
                         [hex(s) for s in ids])
                return

    # ---- parse skill bar (0x28) ----
    def _on_skill_bar(self, pkt: bytes):
        """S2C 0x28: skill bar cua char/pet.
        Format: [01 00][unit 1B][?? 1B][skill_id 2B LE ...][0000 = terminator/slot trong]...
        unit=3: CHAR, unit=2: PET. Byte sau unit KHONG phai count tin cay (capture: =5 nhung co
        6 skill) -> DOC SKILL TOI KHI GAP 0x0000 (terminator), khong dua theo count (bug cu cat
        mat skill cuoi -> vd thieu Nem Da 0x2715 -> char danh chay).
        Quet ca CHAR va PET; validate range de bo qua block rac trong padding."""
        if len(pkt) < 12:
            return
        payload = pkt[7:]
        i = 2  # bo prefix 01 00
        seen_char = False
        while i + 2 <= len(payload):
            unit = payload[i]
            if unit not in (2, 3):
                i += 1
                continue   # padding/byte la -> truot toi block hop le
            i += 2         # bo unit + byte sau (khong dung)
            skills = []
            ok = True
            while i + 2 <= len(payload):
                sid = int.from_bytes(payload[i:i + 2], 'little')
                i += 2
                if sid == 0:
                    break  # terminator -> het skill cua unit nay
                if not (0x2710 <= sid <= 0x3fff) or len(skills) > 40:
                    ok = False
                    break  # canh rac
                if sid not in skills:
                    skills.append(sid)
            if not ok or not skills:
                continue
            if unit == 3 and not seen_char:
                for s in skills:   # gop bar 0x28 vao list (append id chua co, giu thu tu 0x05)
                    if s not in self.state.skills_char:
                        self.state.skills_char.append(s)
                seen_char = True
                log.info("[%s] Char skills (bar 0x28): %s", self._label,
                         [hex(s) for s in sorted(skills)])
            elif unit == 2:
                self.state.skills_pet = set(skills)
                if not getattr(self.state, "pet_skills", None):
                    self.state.pet_skills = list(skills)
                log.info("[%s] Pet skills (bar 0x28): %s", self._label,
                         [hex(s) for s in sorted(skills)])

    # ---- parse player info (0x27) ----
    def _resolve_self_name(self, pkt: bytes):
        """Doc TEN NHAN VAT cua minh tu goi guild 0x27: tim self_entity roi name ngay sau
        (entity 8B + name_len 1B + name UTF-16LE)."""
        if self.char_name or not self.self_entity or not pkt:
            return
        k = pkt.find(self.self_entity)
        if k < 0 or k + 9 > len(pkt):
            return
        nl = pkt[k + 8]
        if not (0 < nl <= 40) or k + 9 + nl > len(pkt):
            return
        try:
            nm = pkt[k + 9:k + 9 + nl].decode('utf-16-le')
        except Exception:
            return
        if nm:
            self._dat_nhan_log(nm)
            log.info("[%s] Ten nhan vat = '%s'", self._username, nm)

    def digioi_minutes_live(self) -> float:
        """So phut DG DA DUNG, NGOAI SUY tai thoi diem goi - giong client that.

        Client (Logic/Dungeon.lua UpdateLimitTimeDungeonTime) TU DEM LUI moi giay, chi dung
        RoleCount tu goi server 0x55 de DONG BO LAI khi co gia tri moi. Bot truoc day CHI doc
        `digioi_minutes` -> server ngung day goi (vd bi da ra khoi DG luc het gio) la con so DONG
        BANG -> khong bao gio ket luan het gio -> ca party treo (bug that 22:59-23:07: 3 acc dung
        o Quang Truong, hang rao "2/5 acc xong" cho vo tan).

        CHI dem khi DANG O TRONG map DG - user xac nhan: ra ngoai DG thi dong ho KHONG chay
        (van hien so cu).
        """
        m = float(getattr(self, "digioi_minutes", 0) or 0)
        ts = float(getattr(self, "_last_digioi_ts", 0.0) or 0.0)
        if ts:
            try:
                if self.in_di_gioi():
                    m += max(0.0, time.time() - ts) / 60.0
            except Exception:
                pass
        return m

    def _note_current_channel(self, channel, source="server"):
        try:
            channel = int(channel)
        except Exception:
            return
        if channel <= 0:
            return
        old = self.current_channel
        self.current_channel = channel
        self.current_channel_at = time.time()
        # SERVER vua xac nhan -> so kenh lai la bang chung (xem `kenh_dang_chac`).
        self.kenh_dang_nghi_ngo = False
        self._channel_scene_generation += 1
        if old != channel:
            log.info("[%s] Kenh hien tai = %s (tu %s)", self._label, channel, source)

    KENH_CACHE_SEC = 10.0    # chi de tham khao - KHONG co lenh nao hoi lai duoc (xem kenh_that)

    def kenh_that(self, max_age: float = None):
        """Kenh dang o. KHONG CO CACH HOI LAI SERVER - day la gia tri server DAY den lan cuoi.

        Da thu dung `0x0c 0100` lam cau hoi: SAI. Trong protocal.lua no la
        `C:012-001 <換場景完畢>` - goi THONG BAO "toi vao scene xong", server khong tra loi gi.
        Ep coi "khong tra loi = chua ro" lam treo ca dong bo kenh (30/08 20:01: moi acc deu
        `hoi lai kenh: server KHONG tra loi` -> leader thay `{'sga002': None, ...}` = CHUA sang
        -> party khong bao gio dong bo duoc).

        Nguon dang tin DUY NHAT cho kenh la vong `switch_channel` (`0x07 0200` -> ack co ket qua).
        Nen `switch_channel` KHONG duoc bo qua theo gia tri nho san - xem ghi chu trong do.
        """
        cur = getattr(self, "current_channel", None)
        return int(cur) if cur else None

    def refresh_current_channel(self, wait: float = 1.5):
        """Xin lai scene hien tai; chi nhan instanceId/kenh duong tu server."""
        if not self.running:
            return None
        generation = self._channel_scene_generation
        try:
            self.send(0x0c, b"\x01\x00")
        except OSError:
            return None
        deadline = time.time() + max(0.0, float(wait))
        while self.running and time.time() < deadline:
            if self._channel_scene_generation != generation:
                try:
                    channel = int(self.current_channel)
                except (TypeError, ValueError):
                    return None
                return channel if channel > 0 else None
            time.sleep(0.05)
        # `0x0c 0100` = `C:012-001 <換場景完畢>` la goi THONG BAO, KHONG phai cau hoi -> binh thuong
        # server KHONG tra loi gi. Tra gia tri nho san (hanh vi cu). Muon biet kenh chac chan thi
        # phai di qua `switch_channel` (`0x07 0200` co ack), xem `kenh_that`.
        try:
            channel = int(self.current_channel)
        except (TypeError, ValueError):
            return None
        return channel if channel > 0 else None

    def _parse_channel_from_03(self, pkt: bytes):
        """Parse instanceId/channel tu S2C 0x03 PlayerAppear theo layout client Lua.
        Name dai/ngan khac nhau, nen instanceId nam ngay sau [name_len][name UTF-16LE]."""
        if not pkt or len(pkt) < 56:
            return None
        body = pkt[7:]
        if len(body) < 50 or body[0:2] != b"\x00\x00":
            return None
        if self.self_entity is not None and body[2:10] != self.self_entity:
            return None
        name_len = body[46]
        end = 47 + name_len
        if not (0 <= name_len <= 80 and name_len % 2 == 0 and end + 2 <= len(body)):
            return None
        return int.from_bytes(body[end:end + 2], "little")

    def _dat_nhan_log(self, nm):
        """Dat `char_name` + nhan log. Ten nhan vat TRUNG voi acc khac -> CA HAI them "~username".

        Xem chu thich `_NHAN_CHU` o dau file: nhan log la thu duy nhat phan biet dong log cua acc
        nay voi acc khac, ma ten nhan vat thi khong duy nhat giua cac server.
        """
        self.char_name = nm
        u = self._username or ""
        with _NHAN_LOCK:
            chu = _NHAN_CHU.setdefault(nm, {})
            chu[u] = self
            ds = list(chu.items())
        trung = len(ds) > 1
        for _u, _c in ds:
            _c._label = _nhan_log(_u, nm, trung)
        if trung:
            log.info("[%s] ten nhan vat '%s' TRUNG voi acc khac (%s) -> nhan log = '%s' cho khoi lan",
                     u, nm, ", ".join(sorted(x for x, _ in ds if x != u)), self._label)

    def _name_from_03_body(self, body: bytes):
        def _try(off, require_zero_prefix: bool = False):
            if off < 2 or off + 1 >= len(body):
                return None
            nl = body[off]
            if not (0 < nl <= 40) or nl % 2 or off + 1 + nl > len(body):
                return None
            if require_zero_prefix and body[off - 2:off] != b"\x00\x00":
                return None
            try:
                nm = body[off + 1:off + 1 + nl].decode("utf-16-le")
            except Exception:
                return None
            return nm if (nm and nm.isprintable()) else None
        # Client Lua: Role.PlayerAppear doc name_len ngay sau serverId/turn/career.
        # Hai byte truoc name_len KHONG phai guard 0000; co the la turn/career cua nhan vat.
        nm = _try(46)   # offset co dinh cua PlayerAppear da verify voi client Lua
        if not nm:      # fallback: quet sau entity tim [0000][len][name printable]
            for off in range(12, min(len(body) - 1, 90)):
                nm = _try(off, require_zero_prefix=True)
                if nm:
                    break
        return nm

    def _remember_entity_name(
        self,
        entity: bytes,
        name: str,
        source: str = "",
        *,
        scene_id=None,
        instance_id=None,
        nearby: bool = False,
    ):
        if not entity or not name:
            return
        entity = bytes(entity)
        name = str(name).strip()
        if not name:
            return
        meta = self.entity_meta.setdefault(entity, {})
        meta["seen"] = time.time()
        meta["source"] = source or "entity-name"
        if scene_id is not None:
            try:
                meta["scene_id"] = int(scene_id)
            except Exception:
                pass
        if instance_id is not None:
            try:
                meta["instance_id"] = int(instance_id)
            except Exception:
                pass
        if nearby or source in ("0x03", "0x27/0900"):
            meta["nearby"] = True
            meta["scene_generation"] = self._channel_scene_generation
        # RIENG `0x03` PlayerAppear moi la BANG CHUNG server noi "nguoi nay o cung scene + cung
        # instance voi may". `0x27/0900` chi la danh sach ten nguoi quanh map - no cung set
        # `nearby` nen `nearby` KHONG dung de ket luan cung kenh duoc. Ghi rieng moc nay de con
        # phan biet (xem `da_thay_tan_mat`).
        if source == "0x03":
            meta["appear_at"] = time.time()
            if scene_id is not None:
                try:
                    meta["appear_scene"] = int(scene_id)
                except Exception:
                    pass
        names = self.entity_names.setdefault(entity, set())
        if name in names:
            return
        names.add(name)
        leaders = (config.leaders_for(self.party_idx)
                   if hasattr(config, "leaders_for") else getattr(config, "PARTY_LEADERS", []))
        wanted = {str(x).strip().casefold() for x in (leaders or []) if str(x).strip()}
        if name.casefold() in wanted:
            log.info("[%s] thay acc whitelist '%s' entity=%s (%s)",
                     self._label, name, entity.hex()[:12], source or "entity-name")

    def _remember_entity_name_from_03(self, pkt: bytes):
        """0x03 PlayerAppear den cho ca nguoi xung quanh. Luu ten/entity de moi whitelist."""
        body = pkt[7:]
        if len(body) < 48 or body[0:2] != b"\x00\x00":
            return
        entity = body[2:10]
        name = self._name_from_03_body(body)
        scene_id = None
        instance_id = None
        try:
            scene_id = int.from_bytes(body[21:23], "little")
            name_len = body[46]
            end = 47 + name_len
            if 0 <= name_len <= 80 and name_len % 2 == 0 and end + 2 <= len(body):
                instance_id = int.from_bytes(body[end:end + 2], "little")
        except Exception:
            pass
        if name:
            self._remember_entity_name(
                entity,
                name,
                "0x03",
                scene_id=scene_id,
                instance_id=instance_id,
                nearby=True,
            )

    def _resolve_name_from_03(self, pkt: bytes):
        """Ten nhan vat tu goi 0x03 self-spawn - gui cho MOI acc luc login (KHONG can bang hoi).
        Layout: [0000][self_entity 8B][~36B stat][name_len 1B @body[46]][name UTF-16LE].
        Guard: 2 byte truoc name_len = 0000. Verify 3/3 acc (haabo/gamo/luubay). Fallback: quet."""
        if self.char_name or not self.self_entity or not pkt or len(pkt) < 55:
            return
        body = pkt[7:]
        if len(body) < 48 or body[2:10] != self.self_entity:
            return
        nm = self._name_from_03_body(body)
        if nm:
            self._dat_nhan_log(nm)
            self._remember_entity_name(self.self_entity, nm, "0x03-self", nearby=True)
            _register_party_name(self.self_entity, nm)
            log.info("[%s] Ten nhan vat = '%s' (tu 0x03)", self._username, nm)

    def _remember_entity_names_from_27_nearby(self, pkt: bytes):
        """S2C 0x27/0900 co cac record entity + ten nguoi gan map; dung de tim whitelist."""
        off = 9
        parsed = 0
        while off + 13 <= len(pkt):
            entity = pkt[off:off + 8]
            name_len = pkt[off + 12]
            end = off + 13 + name_len
            if name_len <= 0 or name_len > 80 or end > len(pkt):
                break
            try:
                name = pkt[off + 13:end].decode("utf-16-le")
            except Exception:
                name = ""
            if name:
                self._remember_entity_name(
                    entity,
                    name,
                    "0x27/0900",
                    scene_id=self.current_map,
                    instance_id=self.current_channel,
                    nearby=True,
                )
                parsed += 1
            off = end
        if parsed:
            log.debug("[%s] 0x27/0900 parsed %d nearby players", self._label, parsed)

    def _on_player_info(self, pkt: bytes):
        """S2C 0x27 sub=0x02: danh sach thanh vien guild.
        Format: [sub 2B=0200][guild_len 1B][guild_name UTF-16LE][01][count 1B]
                [entry: entity(8B) + name_len(1B) + name(UTF-16LE name_len B) + 32B extra] x count
        Chi xu ly sub=0x02; bo qua cac sub khac (0x09 la guild-join notify, khong co ten nhan vat).
        """
        if len(pkt) < 14:
            return
        payload = pkt[7:]
        sub = int.from_bytes(payload[0:2], 'little')
        if sub == 0x09:
            self._remember_entity_names_from_27_nearby(pkt)
            return
        if sub != 0x02:
            return
        # --- TEN NHAN VAT CUA MINH: quet truc tiep self_entity trong goi roi doc name ngay sau
        # (parser entry ben duoi tinh stride khong chuan -> bo sot self; cach nay chac chan) ---
        self._last_guild_pkt = pkt   # cache de 0x69 retry neu 0x27 toi TRUOC 0x69
        self._resolve_self_name(pkt)
        guild_len = payload[2]
        self.has_legion = guild_len > 0   # guild_len=0 -> KHONG co quan doan (xem ghi chu o __init__)
        # entries bat dau sau: 2B(sub) + 1B(guild_len) + guild_len + 1B(unknown) + 1B(count) = guild_len+5
        entries_off = 3 + guild_len + 2
        if entries_off > len(payload):
            return
        off = entries_off
        parsed = 0
        while off + 9 <= len(payload):
            entity = payload[off:off + 8]
            name_len = payload[off + 8]
            if name_len == 0 or off + 9 + name_len > len(payload):
                break
            try:
                name = payload[off + 9:off + 9 + name_len].decode('utf-16-le')
            except Exception:
                name = ''
            if name:
                self._remember_entity_name(entity, name, "0x27/0200")
                # Neu la entity CUA MINH -> dung lam ten nhan vat trong log
                if self.self_entity and entity == self.self_entity and self.char_name != name:
                    self._dat_nhan_log(name)
                    _register_party_name(self.self_entity, name)   # de leader tra ten member
                    log.info("[%s] Ten nhan vat = '%s'", self._username, name)
                log.debug("[%s] guild member: %s -> '%s'", self._label, entity.hex()[:12], name)
            off += 9 + name_len + 32
            parsed += 1
        if parsed:
            log.info("[%s] 0x27 parsed %d guild members (entity_names cap nhat)", self._label, parsed)

    # ---- lenh tien ich ----
    def switch_channel(self, channel: int, wait: float = 6.0, retries: int = 2,
                       theo_lenh: bool = False) -> bool:
        """Chuyen sang sub-channel va cho server tra ket qua.
        Client game xu ly S2C 0x07/0200: result=0 OK; 1=cung kenh; 2=khong co kenh;
        3=dang trong party; 4=kenh day. Bot KHONG tu set current_channel truoc khi server xac nhan."""
        try:
            channel = int(channel)
        except Exception:
            return False
        if channel <= 0:
            return True
        # HOI LAI SERVER truoc khi ket luan "da o san kenh N". Tin so nho san la co the bo qua
        # lan doi kenh THAT SU can (xem `kenh_that`): bot tuong ca party cung kenh trong khi
        # thuc te moi nick mot kenh -> loi moi party khong bao gio toi noi, khong mot ma loi nao.
        # KHONG bo qua theo gia tri nho san. `current_channel` la so bot tu nho va no SAI duoc
        # (sot lai qua reconnect, hoac ack cu). Bo qua theo no = bo dung lan doi kenh THAT SU can:
        # ca party moi nguoi mot kenh ma bot bao cung kenh -> loi moi party khong bao gio toi noi,
        # khong mot ma tu choi nao (user kiem chung 30/08: bot hien ca 5 nick party 3 kenh 12,
        # vao game xem la 12/12/12/2/1).
        # Gui that thi RE ma CHAC: dang o san kenh do -> server tra result=1 (<cung kenh>) va
        # `_on_channel_switch_result` van coi la thanh cong. Day cung la CACH DUY NHAT xac minh
        # duoc kenh, vi khong co lenh nao hoi "toi dang o kenh nao" (xem `kenh_that`).
        # PHAI THOAT TO DOI TRUOC - client game chan thang, KHONG gui goi nao
        # (`_lua_dec/UI/UIServerArea.lua:97`, `OnClick_Area`):
        #     if not Team.IsAlone(Role.playerId) then ShowCenterMessage(...); return; end
        # Con neu van gui thi server tra `S:007-002` ma 3 <組隊不可換分區>.
        #
        # Cung mot luat `Team.IsAlone` voi `teleport()` va `enter_di_gioi()` - hai cho do da sua
        # 07/09, rieng cho nay bo sot. Hau qua (40NPC, 07/09): acc doi kenh mai khong duoc, dieu
        # phoi cu chot lai kenh dich, leader thi "chua moi N member vi chua xac nhan live dung
        # map/kenh" - ca party khong bao gio gom ve mot kenh.
        # DANG DO VIEC DOI KENH. Doi kenh phai roi doi truoc, ma doi truong roi la server GIAI TAN
        # ca doi -> roster cua ca party thanh KHONG DANG TIN trong vai giay. Dieu phoi phai biet ma
        # DUNG KET LUAN "thieu party" trong khoang do, khong thi no ra lenh lap lai party ngay giua
        # luc bot dang tu doi kenh theo lenh cua chinh no (party 52, 07/09: gen 130 -> 137 trong hai
        # phut, 38 phut chi danh duoc 14 tran).
        # MOT LENH MOT LUC. Duong dieu phoi (`_dieu_phoi_thi_hanh_kenh`) va duong acc tu nghe
        # (`_nghe_lenh_kenh`) deu goi ham nay; khong khoa thi hai lenh chong nhau, server tra MOT
        # ket qua va luong kia TIMEOUT.
        #
        # Ca that 10/09 party 2 (user: "sao lai chot kenh 8 bi full trong khi rat nhieu kenh khac
        # trong") - hai lenh cung mot acc, cung mot giay, hai gia tri `wait` khac nhau:
        #     00:09:05 [gamo] Doi kenh 3 TIMEOUT sau 4.0s      <- duong dieu phoi
        #     00:09:06 [gamo] Doi kenh 3 TIMEOUT sau 6.0s      <- duong acc tu nghe
        # Moi TIMEOUT ghi `-1`, ma `_doc_ket_qua_doi_kenh` doi xu `-1` nhu "kenh khong vao duoc" ->
        # kenh trong bi vao SO DEN. So den day dan thi nhanh (a) "kenh it nguoi nhat ma du cho ca
        # team" khong con ung vien nao -> roi xuong nhanh (b) va chot bua vao mot trong ba kenh
        # party dang dung, ca ba deu DAY:
        #     00:09:27 party lech kenh {4:1, 6:1, 8:1, 14:1, 21:1} -> CHOT kenh dich = 8
        # `blocking=False`: xep hang cung la gui thua. Lenh dang bay se tra ket qua that cho ca hai.
        if not self._chan_switch_lock.acquire(blocking=False):
            log.info("[%s] Doi kenh %s: DA CO mot lenh dang bay -> bo lenh nay (khong ghi ket qua)",
                     self._label, channel)
            return False
        try:
            return self._switch_channel_locked(channel, wait, retries, theo_lenh)
        finally:
            self._chan_switch_lock.release()

    def _switch_channel_locked(self, channel: int, wait: float, retries: int,
                               theo_lenh: bool = False) -> bool:
        self._doi_kenh_tu = time.time()
        if self.party_members:
            # DIEM NGHEN THU HAI (cung ho voi `teleport`): doi kenh cung phai ROI DOI truoc, tuc
            # cung la mot hanh dong PHA PARTY. Chi lam khi:
            #   `theo_lenh=True`  - chinh dieu phoi goi (`_dieu_phoi_thi_hanh_kenh`), hoac
            #   `nguoi_keo`       - dieu phoi giao viec di duong cho minh / cho tat ca ("*", vd
            #                       dang GOM hoac party khong co bot-leader).
            # Khong thi acc dang tu pha party dang lanh de doi kenh theo y no.
            if not theo_lenh and self.party_idx is not None:
                try:
                    _keo = nguoi_keo(self.party_idx)
                except Exception:
                    _keo = "*"
                if _keo not in ("*", self._username):
                    log.info("[%s] Doi kenh %s: KHONG roi doi (%d member) - khong phai lenh cua "
                             "dieu phoi (viec di duong dang giao cho %s)",
                             self._label, channel, len(self.party_members),
                             ("'%s'" % _keo) if _keo else "CHUA AI")
                    return False
            log.info("[%s] Doi kenh: dang o to doi (%d member) -> ROI DOI truoc (client chan "
                     "doi kenh khi con doi)", self._label, len(self.party_members))
            try:
                self.leave_party()
            except Exception as e:
                log.warning("[%s] Doi kenh: roi doi loi: %s", self._label, e)
        for attempt in range(1, max(1, int(retries)) + 1):
            self._chan_switch_event.clear()
            self._chan_switch_target = channel
            self._chan_switch_result = None
            log.info("[%s] Chuyen kenh -> %d (cho server xac nhan, lan %d/%d)",
                     self._label, channel, attempt, max(1, int(retries)))
            self.send(0x07, b"\x02\x00" + struct.pack("<H", channel))
            if not self._chan_switch_event.wait(max(0.1, float(wait))):
                # CO SERVER IM LANG khi dich TRUNG kenh dang o (khong gui ca ma 1
                # `不可換到同一區`). Log 30/08 21:09 sga001: dang o kenh 3, doi sang 3 -> TIMEOUT
                # ca 2 luot, moi 30s ton 12s vo ich. Nho san dang la kenh do -> coi nhu dung kenh.
                if self.current_channel and int(self.current_channel) == channel:
                    log.info("[%s] Doi kenh %d: server IM LANG (dich trung kenh dang o) "
                             "-> coi nhu da o kenh %d", self._label, channel, channel)
                    return True
                log.warning("[%s] Doi kenh %d TIMEOUT sau %.1fs", self._label, channel, wait)
                continue
            result = self._chan_switch_result
            if result in (0, 1):
                return True
            if result == 3:
                # `S:007-002 結果 3: 組隊不可換分區` = DANG TO DOI THI KHONG DOI KENH DUOC.
                # Day la VONG LUAN QUAN da lam party khong bao gio du: ket trong mot party (ke ca
                # party ma) -> khong doi duoc kenh -> nam khac kenh voi leader -> server khong gui
                # loi moi toi -> khong vao duoc party moi -> van ket. Truoc day bot `return False`
                # IM LANG o day nen ca vong nay vo hinh trong log.
                log.warning("[%s] Doi kenh %d bi TU CHOI: DANG TO DOI (ma 3) -> roi party roi thu lai",
                            self._label, channel)
                try:
                    self.leave_party(server_bao_dang_o_party=True)
                except Exception as e:
                    log.warning("[%s] doi kenh: loi roi party de doi kenh: %s", self._label, e)
                time.sleep(0.5)
                continue
            time.sleep(0.5)
        # HET LUOT MA CHUA VAO DUOC -> PHAI DE LAI LY DO. `return False` tran khong noi gi thi
        # dieu phoi khong biet lenh da ket thuc hay con dang bay, va no treo dich lo lung.
        # (user 08/09: "moi lan doi kenh phai duoc thanh cong hay fail va vi sao fail chu")
        #   None -> chua he nhan duoc `S:007-002` lan nao = TIMEOUT (server im lang) -> ghi -1.
        #   co ma -> giu nguyen ma that (2 khong co khu / 3 dang to doi / 4 day) de dieu phoi xu ly.
        if self._chan_switch_result is None:
            self._chan_switch_result = -1          # -1 = TIMEOUT, khong phai ma cua server
            self._chan_switch_luc = time.time()
        # LENH DOI KENH HONG -> SO KENH DANG NHO KHONG CON LA BANG CHUNG.
        #
        # `current_channel` la SO NHO SAN, khong phai so that (KNOWLEDGE.md muc 7, user kiem chung
        # 30/08: bot hien ca 5 nick party 3 o kenh 12, vao game xem la 12/12/12/2/1). Game KHONG CO
        # lenh hoi "toi dang o kenh nao" - nguon duy nhat la ack cua chinh `0x07 0200`.
        # Nen mot lenh doi kenh TIMEOUT / bi tu choi de lai dung tinh trang toi nhat: bot khong con
        # biet minh o dau, ma van om so cu nhu that.
        # Dieu phoi lay dung so cu do de ket luan "party lech kenh" roi ra lenh xe doi:
        #   00:21:26 [party 24] party lech kenh {1: 1, 2: 4} -> CHOT kenh dich = 5
        # trong khi ca party dang o CUNG kenh (user 11/09: "p24 dang o kenh 5 het, co lech kenh deo
        # dau"). Danh dau NGHI NGO de dieu phoi biet so nay khong dung de ket luan duoc.
        self.kenh_dang_nghi_ngo = True
        log.warning("[%s] Doi kenh %d THAT BAI han toan sau %d luot: %s -> so kenh dang nho (%s) "
                    "khong con la bang chung", self._label, channel, max(1, int(retries)),
                    "server IM LANG (timeout)" if self._chan_switch_result == -1
                    else "S:007-002 ma %s" % self._chan_switch_result,
                    getattr(self, "current_channel", None))
        return False

    def kenh_dang_chac(self) -> bool:
        """So kenh hien tai co phai do SERVER xac nhan khong (khong phai so nho lai tu truoc).

        Chac = lan cuoi cham vao kenh la mot ack that (`S:007-002` ma 0/1, `0x03`, `0x0c`), va tu
        do den gio khong co lenh doi kenh nao hong."""
        return bool(getattr(self, "current_channel", None)) and not getattr(
            self, "kenh_dang_nghi_ngo", False)

    def _on_channel_switch_result(self, pkt: bytes):
        """S2C 0x07 switch result: [02 00][result].
        result=0 OK; 1 same channel; 2 no area; 3 team cannot switch; 4 full."""
        result = pkt[9]
        target = self._chan_switch_target
        self._chan_switch_result = result
        # MOC de DIEU PHOI biet ma nay con MOI hay da cu. Dieu phoi doc THANG ba truong nay
        # (`_chan_switch_result`, `_chan_switch_target`, `_chan_switch_luc`) cua tung client thay
        # vi bat acc "bao cao" len - ca 5 acc nam trong MOT tien trinh, khong co gi phai bao.
        self._chan_switch_luc = time.time()
        if result == 0:
            if target:
                self._note_current_channel(target, "0x07 ack")
            log.info("[%s] Doi kenh OK -> %s", self._label, target or "?")
        elif result == 1:
            # `不可換到同一區`: server TU CHOI vi dich trung khu dang o -> suy ra dang o khu do.
            # Day la thong bao LOI cua client, khong phai "server xac nhan da doi xong".
            if target:
                self._note_current_channel(target, "0x07 ma1 (trung khu dang o)")
            log.info("[%s] Doi kenh: server bao TRUNG khu dang o -> dang o kenh %s",
                     self._label, target or "?")
        else:
            log.warning("[%s] Doi kenh %s THAT BAI: %s (result=%d)",
                        self._label, target or "?", CHANNEL_SWITCH_ERRORS.get(result, "loi khong ro"), result)
        self._chan_switch_event.set()

    def _on_channel_list(self, pkt: bytes):
        """S2C 0x07 list: payload = [01 00][count 1B][ block 6B: ch2 cur2 cap2 ]*count."""
        # DUNG DUNG `count` CUA SERVER, khong doan. Client that (`protocal.lua:1059`):
        #     local count = data:ReadByte();
        #     for i = 1, count do  index=ReadUInt16 current=ReadUInt16 max=ReadUInt16  end
        #
        # Ban cu bo qua `count`, duyet HET buffer va loc bang phong doan `0 < ch < 1000 and cap > 0`
        # -> goi thieu byte thi im lang nhan mot bang NGAN, goi thua thi nhet block rac vao. Bang
        # thieu kenh chinh la thu khien dieu phoi "khong chot lai kenh khac": no chi thay may kenh
        # dau (deu day) va khong biet con kenh trong o phia sau.
        #
        # Bang chung 08/09 - CUNG mot server ma so kenh nhan duoc nhay lung tung:
        #     545 lan "Nhan danh sach 9 kenh" · 256 lan "14 kenh" · 129 lan "62 kenh" · ... "5 kenh"
        # (user: "deo can ghi nho kenh day, m phai lay duoc chinh xac ket qua tra ve")
        _n = pkt[9] if len(pkt) > 9 else 0      # header(6)+op(1)+'01 00'(2) -> count o byte 9
        data = pkt[10:]
        if _n <= 0:
            # `count <= 0` -> client BAO LOI roi return (`string.Get(10135)` = khong doi phan khu
            # duoc luc nay; cung message no dung khi dang loan dau, `UIMiniMap.lua:298`). No KHONG
            # xoa danh sach dang co -> bot giu `channels` cu y het.
            # VAN set `_chan_event`: nguoi hoi dang cho, khong bao thi ho ngoi het timeout roi moi
            # biet - trong khi cau tra loi (KHONG CO KENH NAO) da co ngay day.
            self._ds_kenh_nhan_luc = time.time()
            self._chan_event.set()
            log.info("[%s] Danh sach kenh: server tra count=0 -> KHONG doi phan khu duoc luc nay",
                     self._label)
            return
        if len(data) < _n * 6:
            log.warning("[%s] danh sach kenh THIEU BYTE: count=%d can %d byte, chi co %d -> BO "
                        "(giu bang cu, khong chot bang so thieu)",
                        self._label, _n, _n * 6, len(data))
            return
        chans = {}
        for i in range(_n):
            ch, cur, cap = struct.unpack_from("<HHH", data, i * 6)
            if cap > 0:
                chans[ch] = (cur, cap)
        # KENH DANG O LUON PHAI CO TRONG DANH SACH - y het client:
        #     if not table.Contains(UIServerArea.instances, SceneManager.instanceId) then
        #       local aeraData = {}; aeraData.index = SceneManager.instanceId;
        #       table.insert(UIServerArea.instances, aeraData);   -- KHONG co current/maxPlayers
        # Server khong liet ke kenh minh dang dung (thuong vi no DAY) khong co nghia la kenh do
        # khong ton tai. Them vao voi suc chua KHONG BIET (None) chu dung bia so: cho chot kenh
        # doc `_con_cho(ch) is None` la "chua ro" va van cho phep giu nguyen kenh dang dung.
        _ch_now = getattr(self, "current_channel", None)
        if _ch_now and int(_ch_now) not in chans:
            chans[int(_ch_now)] = (None, None)
        if chans:
            self.channels = chans
            self._ds_kenh_nhan_luc = time.time()
            # DANH SACH KENH LA CUA MAP DANG DUNG, khong phai cua ca the gioi: moi map mot danh
            # sach khac nhau. Ghi lai map luc nhan de nguoi doc biet bang nay dung cho map nao -
            # dung bang cua map khac de chot kenh la chot mot so KHONG TON TAI o map hien tai.
            #
            # Ca that party 3, 14/09 (user: "thay chot kenh 7, ma tuong duong deo co kenh 7" ->
            # "no deo phai kenh rac ma la kenh khi no o map khac, phai la khi ca pt cung map can
            # lap pt thi moi lay danh sach kenh chu"):
            #   15:50:31 [party 3] con lech map [12001, 21001] ... minh@12001/k7
            #   15:50:35 [party 3] party lech kenh {1:1, 2:1, 3:1, 7:2} -> CHOT kenh dich = 7
            self._ds_kenh_map = int(getattr(self, "current_map", 0) or 0)
            self._chan_event.set()
            log.info("[%s] Nhan danh sach %d kenh (map %s)", self._label, len(chans),
                     self._ds_kenh_map)

    def request_channel_list(self):
        """Gui 0x07 0100 de server tra ve danh sach kenh + so nguoi.

        KHONG XOA `self.channels` o day. Bang CU vai giay van dung duoc de chon kenh; bang RONG thi
        khong dung duoc vao viec gi. Xoa truoc roi cho goi ve = tu tao mot cua so mu 1-2 giay, va
        `pick_best_channel` con goi ham nay toi 4 lan lien tiep.
        Gio `_on_channel_list` THAY THE nguyen khoi khi goi ve, nen khong co canh nua-cu-nua-moi.

        Ca that 10/09 (user: "hinh nhu no van ko tim kenh it nguoi truoc ma no chot kenh member dang
        o luon"): dem tren party.log - `bang 0 kenh` 6105 lan / `bang 58 kenh` 277 lan, tuc 94% so
        lan chot kenh la dieu phoi KHONG CO du lieu kenh nao. Khong co bang thi nhanh (a) "kenh IT
        NGUOI NHAT ma du cho ca team" khong co ung vien nao de xet -> LUON roi xuong nhanh (b) "lay
        kenh dang nhieu member nhat" - dung cai user nhin thay.
        """
        self._chan_event.clear()
        self._ds_kenh_hoi_luc = time.time()   # dieu phoi doc de biet BANG DANG CU, cho goi ve da
        self.send(0x07, b"\x01\x00")

    def pick_best_channel(self, wait=2.0, exclude=(1,), tries=4, need=1):
        """Legacy entry point: automatic channel selection is disabled globally."""
        log.info("[%s] Auto chọn phân khu đã tắt; giữ phân khu hiện tại", self._label)
        return 0

    def party_peers(self):
        """Cac client CUNG PARTY dang song trong tien trinh nay (ke ca chinh minh).

        Doc thang `_PARTY_CLIENTS` - khong acc nao phai bao cao gi (L2).
        """
        try:
            with _PARTY_LOCK:
                peers = list((_PARTY_CLIENTS.get(self.party_idx) or {}).values())
        except Exception:
            return []
        return [p for p in peers if getattr(p, "running", False)]

    def kenh_cua_party(self):
        """{kenh -> so acc} cua party NAY, chi tinh acc con song va CUNG MAP voi minh.

        Doc thang `_PARTY_CLIENTS` (client cung tien trinh) - KHONG can acc nao bao cao.
        """
        ra = {}
        try:
            with _PARTY_LOCK:
                peers = list((_PARTY_CLIENTS.get(self.party_idx) or {}).values())
            for p in peers:
                if not getattr(p, "running", False):
                    continue
                if getattr(p, "current_map", None) != self.current_map:
                    continue          # so kenh chi co nghia trong CUNG mot map
                cur = getattr(p, "current_channel", None)
                if cur:
                    ra[int(cur)] = ra.get(int(cur), 0) + 1
        except Exception:
            return {}
        return ra

    def _kenh_chung_cua_party(self):
        """Kenh MA CA PARTY dang o, None neu lech hoac chua ro. Phai co it nhat 2 acc."""
        d = self.kenh_cua_party()
        if len(d) != 1:
            return None
        ch, n = next(iter(d.items()))
        return ch if n >= 2 else None

    def _so_acc_party_o_kenh(self, ch):
        """So acc CUA CHINH PARTY dang o kenh `ch` (cung map). Chung da chiem cho san nen khi tinh
        "kenh con du cho khong" phai cong tra lai - khong thi doi kenh tai cho la bat kha thi.

        Doc thang `_PARTY_CLIENTS` (cac client cung tien trinh) - khong can goi nao.
        """
        try:
            ch = int(ch)
            with _PARTY_LOCK:
                peers = list((_PARTY_CLIENTS.get(self.party_idx) or {}).values())
            n = 0
            for p in peers:
                if not getattr(p, "running", False):
                    continue
                if getattr(p, "current_map", None) != self.current_map:
                    continue          # so kenh chi co nghia trong CUNG mot map
                cur = getattr(p, "current_channel", None)
                if cur is not None and int(cur) == ch:
                    n += 1
            return n
        except Exception:
            return 0

    def invite_entity(self, entity: bytes):
        """Moi 1 nguoi vao party BANG ENTITY. C2S 0x0d sub=07 = 07 00 [entity 8B].
        (Da xac nhan tu capture invite_dg.pcap - moi theo entity, KHONG phai index 0x52!)"""
        if not entity:
            return
        self.send(protocol.OP_PLAYER_STATE, b"\x07\x00" + bytes(entity))

    def _entity_is_visible_on_current_scene(self, entity: bytes, max_age: float = 300.0):
        """Kiem tra entity co that su dang o gan leader/cung scene+khu hien tai khong.

        Day la co che gom party: khong thay PlayerAppear/nearby cua member thi coi nhu member chua
        tap trung dung map/kenh, khong gui moi party thuong 0x0d/07.
        """
        if not entity:
            return False, "entity rong"
        eb = bytes(entity)
        meta = self.entity_meta.get(eb) or {}
        if not meta.get("nearby"):
            return False, "chua thay quanh leader"
        seen = float(meta.get("seen") or 0.0)
        if seen and time.time() - seen > max_age:
            return False, "cache quanh map qua cu"
        scene_id = meta.get("scene_id")
        if self.current_map is not None:
            if scene_id is None:
                return False, "khong biet map cua entity"
            try:
                if int(scene_id) != int(self.current_map):
                    return False, f"lech map {scene_id}!={self.current_map}"
            except Exception:
                return False, "map entity khong hop le"
        # KHONG so instanceId nua: server CHI gui nearby/PlayerAppear cho nguoi CUNG scene VA
        # CUNG instance -> da thay nhau quanh + cung map la du chac chan. So them instanceId chi
        # lam hong: con so do la instanceId (client Lua goi dung ten do), no TU DOI giua chung
        # (capture 2K: leader 1 -> 2 khi sang 12932) va moi acc cap nhat vao thoi diem khac nhau
        # -> cu so la ra "lech kenh" trong khi thuc te dung canh nhau, nhin thay nhau
        # (user xac nhan: chi co 1 kenh, thay het xung quanh, bot van bao moi dua 1 kenh).
        return True, ""

    def da_thay_tan_mat(self, entity: bytes):
        """Server DA TUNG bao "nguoi nay o cung scene + cung instance voi may" chua ("" = roi).

        `S:003 PlayerAppear` chi duoc gui cho nguoi CUNG SCENE VA CUNG INSTANCE. Nen day la bang
        chung SERVER-SIDE manh nhat ve "cung cho", manh hon moi phep so hai bien noi bo.
        Client that cung lam vay: `UITeam.UpdateList` chi liet ke tu `Role.players` - tuc chi
        nhung nguoi server DA BAO - roi loc `sceneId` + `instanceId`.

        LUAT (user chot 30/08): "cung map cung kenh la THAY NHAU roi, khong thay nhau la KHAC KENH".
        Nen ket qua cua ham nay dung de ket luan LECH KENH -> di dong bo kenh lai.

        KHONG DUOC HET HAN THEO THOI GIAN: `0x03` chi ban khi nguoi ta XUAT HIEN trong tam nhin,
        dung yen canh nhau ca tieng cung khong co goi moi. Dat han 300s la ket toi oan CA PARTY
        dang dung yen o rally (log 30/08 21:09: ca 4 member deu "lan cuoi thay ... 450s truoc"
        trong khi ho dang dung im tai cho).
        Thay vao do, co "da thay" bi HUY dung luc server bao ho di: `S:001-001 <玩家離線>` (roi
        tam nhin) va `S:012-000` doi sang scene khac. Nen "da thay" = van dang cung cho THAT.

        (KHONG dung `meta["nearby"]`: `0x27/0900` - danh sach ten quanh map - cung set co do.)
        """
        meta = (getattr(self, "entity_meta", None) or {}).get(bytes(entity)) or {}
        at = float(meta.get("appear_at") or 0.0)
        if not at:
            return "server CHUA HE bao thay nguoi nay quanh minh (chua co 0x03)"
        sc = meta.get("appear_scene")
        if sc is not None and self.current_map is not None and int(sc) != int(self.current_map):
            return "lan cuoi thay o map %s (minh dang %s)" % (sc, self.current_map)
        return ""

    def _bot_member_is_on_current_scene(self, entity: bytes):
        """Doc map/kenh live tu chinh client bot member, khong dua vao PlayerAppear cache."""
        if not entity:
            return False, "entity rong"
        eb = bytes(entity)
        with _PARTY_LOCK:
            peer = _PARTY_CLIENTS.get(self.party_idx, {}).get(eb)
        if peer is None:
            return False, "chua co client live"
        if not getattr(peer, "running", False):
            return False, "client member khong chay"
        my_map = self.current_map
        peer_map = getattr(peer, "current_map", None)
        if my_map is None or peer_map is None:
            return False, "chua biet map live"
        if int(peer_map) != int(my_map):
            return False, f"lech map live {peer_map}!={my_map}"
        # KENH: phai HOI LAI SERVER ca hai ben (`kenh_that`), khong so hai so nho san.
        #
        # Truoc day cho nay KHONG he kiem kenh - chu thich cu bao "cung map live la DU" vi tung
        # so hai gia tri NHO SAN va bi bao lech oan. Nhung bo han thi dong log lai ghi
        # "(live dung map/kenh)" trong khi chua bao gio kiem kenh -> NOI DOI, va party nam rai
        # nhieu kenh van duoc coi la du dieu kien moi (user xac nhan 30/08: bot hien ca 5 nick
        # party 3 o kenh 12, vao game xem thi la 12/12/12/2/1 -> moi mai khong ai vao doi).
        # Hoi lai server thi so sanh moi co nghia; cache 10s nen khong ban goi.
        try:
            my_ch = self.kenh_that()
            peer_ch = peer.kenh_that()
        except Exception as e:
            return False, "khong hoi duoc kenh live (%s)" % e
        if not my_ch or not peer_ch:
            return False, "chua ro kenh live (minh=%s, no=%s)" % (my_ch, peer_ch)
        if int(peer_ch) != int(my_ch):
            return False, f"lech kenh live {peer_ch}!={my_ch}"
        return True, ""

    def invite_members(self, gap: float = 1.0):
        """Leader moi TAT CA entity member cung party (tru minh) bang 0x0d sub=07.
        Chi moi bot member co client live cung map/kenh, de dam bao ca party da tap trung dung."""
        all_ents = [bytes(e) for e in _PARTY_ENTITIES.get(self.party_idx, set()) if e != self.self_entity]
        current_party = {bytes(e) for e in (self.party_members or []) if e}
        if self.party_leader:
            current_party.add(bytes(self.party_leader))
        ents = []
        skipped = []
        for e in all_ents:
            if e in current_party:
                continue
            ok, reason = self._bot_member_is_on_current_scene(e)
            if ok:
                ents.append(e)
            else:
                skipped.append((e, reason))
        # GHI LAI ket qua cho DIEU PHOI doc, KHONG chi in ra.
        #
        # Day la doi chieu TUNG CAP (minh vs tung member) nen biet CHINH XAC bao nhieu dua lech
        # va la ai - thu ma dieu phoi khong suy ra duoc tu tap gia tri kenh.
        # Party 53 (06/09): leader in dung dong nay 64 lan trong 16 phut ma khong ai lam gi -
        # "bot biet ma khong xu ly". Gio ghi lai de dieu phoi doc va ra lenh dong bo.
        _lech_kenh = [e for e, r in skipped if r.startswith("lech kenh live")]
        _lech_map = [e for e, r in skipped if r.startswith("lech map live")]
        self._moi_bo_qua = {"luc": time.time(), "lech_kenh": _lech_kenh, "lech_map": _lech_map,
                            "tong": len(skipped)} if skipped else None
        if skipped:
            now = time.time()
            last = float(getattr(self, "_last_invite_member_skip_log", 0.0) or 0.0)
            if now - last >= 15:
                self._last_invite_member_skip_log = now
                log.info("[%s] (LEADER) chua moi %d member vi chua xac nhan live dung map/kenh: %s",
                         self._label, len(skipped),
                         ["%s:%s" % (e.hex()[:8], reason) for e, reason in skipped])
        if ents:
            # In luon SO DEM va ROSTER dang thay: party 15 (27/08) moi 446 luot ma khong ai vao,
            # log cu chi in danh sach moi nen khong biet leader dang dem duoc may nguoi, cung
            # khong biet roster server dang la gi -> phai doc nguoc ca file log moi lan ra.
            log.info("[%s] (LEADER) moi %d member theo entity (live dung map/kenh): %s "
                     "| da join=%d | roster server=%d nguoi",
                     self._label, len(ents), [e.hex()[:8] for e in ents],
                     joined_member_count(self.party_idx),
                     len(current_party) - (1 if self.party_leader else 0))
        # BAO RO khi ban loi moi vao nguoi DANG O PARTY KHAC: client that khong bao gio lam the
        # (`Team.Invite` chi gui 007 khi `IsAlone`), va server nuot im lang - KHONG ma tu choi nao.
        # Day dung la trang thai party 2 (30/08) ket 6 phut ma khong ai biet vi sao. VAN GUI loi
        # moi (bo qua thi khi roster server cu/sai la khong bao gio moi nua), chi log de con thay.
        # CHUAN DOAN: server DA TUNG bao thay nguoi nay quanh minh chua (`0x03 PlayerAppear` chi
        # gui cho nguoi CUNG SCENE + CUNG INSTANCE). Chua thay bao gio = gan nhu chac chan khac
        # instance -> loi moi khong the toi noi, va server KHONG gui ma loi nao. Day la bang chung
        # server-side manh nhat con lai sau khi da loai map + kenh. LOG THOI, chua chan, de con
        # doi chieu voi thuc te trong game truoc khi dung no lam cong.
        _chua_thay = [(e, _vi) for e in ents for _vi in [self.da_thay_tan_mat(e)] if _vi]
        if _chua_thay:
            _now = time.time()
            if _now - float(getattr(self, "_last_chua_thay_log", 0.0) or 0.0) >= 15:
                self._last_chua_thay_log = _now
                log.warning("[%s] (LEADER) moi %d nguoi ma SERVER CHUA HE cho thay ho quanh minh: "
                            "%s", self._label, len(_chua_thay),
                            ["%s:%s" % (e.hex()[:8], v) for e, v in _chua_thay])
        _ket = [(e, chu) for e in ents for chu in [self.dang_o_party_khac(e)] if chu]
        if _ket:
            now = time.time()
            if now - float(getattr(self, "_last_invite_busy_log", 0.0) or 0.0) >= 15:
                self._last_invite_busy_log = now
                log.warning("[%s] (LEADER) %d nguoi DANG O PARTY KHAC (roster server) -> loi moi "
                            "0x0d/07 se bi server nuot im lang: %s", self._label, len(_ket),
                            ["%s:o party cua %s" % (e.hex()[:8], chu.hex()[:8]) for e, chu in _ket])
        for e in ents:
            self.invite_entity(e)
            time.sleep(gap)
        return len(ents)

    def _entity_names_for_log(self, entity: bytes, names=None):
        out = set()
        if names:
            out.update(str(n).strip() for n in names if str(n).strip())
        n = name_for_entity(entity)
        if n:
            out.add(str(n).strip())
        return sorted(out, key=lambda x: x.casefold())

    def _entity_label_for_log(self, entity: bytes, names=None):
        eb = bytes(entity)
        ns = self._entity_names_for_log(eb, names)
        if ns:
            return "%s:%s" % ("/".join(ns), eb.hex()[:8])
        return eb.hex()[:8]

    def _whitelist_leader_entities(self, require_nearby: bool = True):
        """Entity nhan vat ngoai bot nam trong whitelist leader, neu leader da thay ten cua ho.

        Party invite cua game gui theo entity 8B, khong gui truc tiep theo ten. Vi vay danh sach nay
        chi moi duoc nhung acc ngoai da co trong cache entity_names cua client hien tai. Client game
        loc nguoi moi theo Role.players cung sceneId/instanceId khi la party thuong. Phong PB doi
        co the moi xa neu da biet entity nen khong bat buoc require_nearby.
        """
        leaders = (config.leaders_for(self.party_idx)
                   if hasattr(config, "leaders_for") else getattr(config, "PARTY_LEADERS", []))
        wanted = {str(x).strip().casefold() for x in (leaders or []) if str(x).strip()}
        self._last_whitelist_scan = {
            "at": time.time(),
            "wanted": [str(x).strip() for x in (leaders or []) if str(x).strip()],
            "require_nearby": bool(require_nearby),
            "items": [],
        }
        if not wanted:
            return []
        with _PARTY_LOCK:
            bot_entities = set(_PARTY_ENTITIES.get(self.party_idx, set()))
        current_party = {bytes(e) for e in (self.party_members or []) if e}
        if self.party_leader:
            current_party.add(bytes(self.party_leader))
        if self.self_entity:
            current_party.add(bytes(self.self_entity))
        out = []
        for entity, names in list(self.entity_names.items()):
            eb = bytes(entity)
            all_names = self._entity_names_for_log(eb, names)
            hits = [n for n in all_names if n.casefold() in wanted]
            if not hits:
                continue
            meta = self.entity_meta.get(eb, {}) or {}
            item = {
                "entity": eb,
                "hits": hits,
                "names": all_names,
                "source": meta.get("source", "?"),
                "status": "",
            }
            if eb in current_party:
                item["status"] = "da o party roi"
                self._last_whitelist_scan["items"].append(item)
                continue
            if eb in bot_entities:
                item["status"] = "la bot member"
                self._last_whitelist_scan["items"].append(item)
                continue
            if require_nearby:
                ok, _reason = self._entity_is_visible_on_current_scene(eb)
                if not ok:
                    item["status"] = _reason or "khong dung map/kenh"
                    self._last_whitelist_scan["items"].append(item)
                    continue
            item["status"] = "se moi"
            self._last_whitelist_scan["items"].append(item)
            out.append(eb)
        return out

    def _log_no_whitelist_entity(self, context: str):
        leaders = (config.leaders_for(self.party_idx)
                   if hasattr(config, "leaders_for") else getattr(config, "PARTY_LEADERS", []))
        wanted = [str(x).strip() for x in (leaders or []) if str(x).strip()]
        wanted_lc = {w.casefold() for w in wanted}
        now = time.time()
        last = float(getattr(self, "_last_whitelist_no_entity_log", 0.0) or 0.0)
        if now - last < 30:
            return
        self._last_whitelist_no_entity_log = now
        if not wanted:
            log.info("[%s] (LEADER) whitelist rong -> khong moi them acc ngoai (%s)",
                     self._label, context)
            return
        known = []
        matched = set()
        scan = getattr(self, "_last_whitelist_scan", None)
        if isinstance(scan, dict) and now - float(scan.get("at") or 0.0) < 10.0:
            for item in scan.get("items") or []:
                hits = [str(n) for n in item.get("hits", []) if str(n).casefold() in wanted_lc]
                if not hits:
                    continue
                matched.update(n.casefold() for n in hits)
                ent = bytes(item.get("entity") or b"")
                known.append("%s:%s/%s/%s" % (
                    ",".join(hits),
                    ent.hex()[:8],
                    item.get("source", "?"),
                    item.get("status", "?"),
                ))
        if not known:
            for entity, names in list(self.entity_names.items()):
                all_names = self._entity_names_for_log(bytes(entity), names)
                hit = [n for n in all_names if n.casefold() in wanted_lc]
                if hit:
                    meta = self.entity_meta.get(bytes(entity), {})
                    matched.update(n.casefold() for n in hit)
                    known.append("%s:%s/%s" % (
                        ",".join(hit), entity.hex()[:8], meta.get("source", "?")
                    ))
        missing = [w for w in wanted if w.casefold() not in matched]
        log.info("[%s] (LEADER) whitelist %s nhung chua co entity moi duoc (%s); missing=%s; known=%s",
                 self._label, wanted, context, missing or "-", known or "-")

    def invite_whitelist_leaders(self, gap: float = 1.0) -> int:
        """Moi acc ngoai whitelist dang dung quanh leader vao party thuong.

        Khong mark joined, khong cho doi accept: acc ngoai vao hay khong khong anh huong flow bot.
        """
        ents = self._whitelist_leader_entities(require_nearby=True)
        if not ents:
            self._log_no_whitelist_entity("party thuong")
            return 0
        log.info("[%s] (LEADER) moi them %d acc whitelist ngoai party: %s",
                 self._label, len(ents), [self._entity_label_for_log(e) for e in ents])
        for e in ents:
            self.invite_entity(e)
            time.sleep(gap)
        return len(ents)

    def invite_train_party_participants(self, gap: float = 1.0):
        """Moi whitelist dang dung xung quanh truoc, sau do moi bot member train."""
        whitelist_count = 0
        try:
            whitelist_count = self.invite_whitelist_leaders(gap=gap)
        except Exception as exc:
            log.warning("[%s] (LEADER) moi whitelist truoc party train loi: %s",
                        self._label, exc)
        bot_count = self.invite_members(gap=gap)
        return whitelist_count, bot_count

    def invite_whitelist_team_dungeon(self, gap: float = 1.0) -> int:
        """Moi them acc whitelist vao PHONG PHO BAN DOI.

        Khac party thuong 0x0d/07, phong pho ban doi dung 0x2f/08. Acc ngoai co vao hay khong
        khong tinh vao so bot-ready; leader van start khi du bot member nhu cu.
        """
        ents = self._whitelist_leader_entities(require_nearby=False)
        if not ents:
            self._log_no_whitelist_entity("phong pho ban doi")
            return 0
        log.info("[%s] (LEADER) moi them %d acc whitelist vao PHO BAN DOI: %s",
                 self._label, len(ents), [self._entity_label_for_log(e) for e in ents])
        for e in ents:
            self.send(0x2f, b"\x08\x00" + bytes(e))
            time.sleep(gap)
        return len(ents)

    def _invite_team_dungeon_participants(self, bot_entities, gap: float = 1.0) -> int:
        """Moi acc whitelist truoc, sau do moi bot members vao phong pho ban.

        Whitelist khong co bot auto-ready. Moi ho truoc tao them thoi gian de vao phong va bam
        CHUAN BI trong luc cac bot members lan luot accept + auto-ready, tranh leader START ngay
        khi whitelist vua moi vao phong.
        """
        whitelist_count = self.invite_whitelist_team_dungeon(gap=gap)
        for entity in bot_entities:
            self.send(0x2f, b"\x08\x00" + bytes(entity))
            time.sleep(gap)
        return whitelist_count

    def _leave_train_party_before_team_dungeon(self, wait: float = 5.0):
        """Giai tan party train truoc khi tao lobby pho ban doi.

        Lobby 0x2f la mot doi rieng. Neu leader van con party thuong 0x0d, server van cho mo
        panel/tao phong nhung nuot im toan bo lenh moi 0x2f/08. Entity member van duoc giu trong
        registry theo account nen sau khi party thuong tan ta van moi dung bon entity cu.
        """
        had_party = bool(self.party_members or self.party_leader or self._doi_truong_dang_ket())
        if not had_party:
            log.info("[%s] (LEADER) truoc PB doi: khong con party train -> tao phong", self._label)
            return
        log.info("[%s] (LEADER) truoc PB doi: giai tan party train, cho server xac nhan roi moi "
                 "tao phong", self._label)
        self.leave_party()
        t0 = time.time()
        while self.running and time.time() - t0 < max(1.0, float(wait)):
            if not self.party_members and not self.party_leader and not self._doi_truong_dang_ket():
                break
            time.sleep(0.2)
        # Cho mot nhip sau S:013-004 de server dong trang thai party truoc khi nhan 0x2f/01.
        time.sleep(0.6)
        log.info("[%s] (LEADER) truoc PB doi: party train da tan=%s sau %.1fs", self._label,
                 not bool(self.party_members or self.party_leader or self._doi_truong_dang_ket()),
                 time.time() - t0)

    def leave_party(self, leader_entity=None, server_bao_dang_o_party=False):
        """Roi/giai tan party hien tai (de co the VAO DI GIOI - khong vao duoc khi dang trong party).
        `C:013-004 <離開隊伍> +隊長玩家ID(8)` - truong nay la ID **DOI TRUONG**, KHONG phai cua
        minh. Client gui dung vay (Logic/Team.lua:138 `Team.Leave`):
            sendBuffer:WriteInt64(this.members[Role.playerId]);   -- members[minh] = leaderId

        ### KHONG O PARTY THI KHONG DUOC GUI GOI NAY - dong dau tien cua `Team.Leave`:
            if this.members[Role.playerId] == nil then return end
        Bot lam NGUOC LAI: no gui dung luc roster local RONG (nhanh "don party ma"), va con gui
        THEM mot goi thu hai kem ID cua chinh minh - client KHONG BAO GIO gui hai goi.

        Ban 013-004 mu vao party MINH KHONG O lam server NGUNG gui loi moi cua party do toi minh.
        Do chinh la cai ket "cung map, cung kenh, moi mai khong vao, khong mot ma tu choi" da duoc
        san 3 ngay. Tuong quan 3/3 party trong log 30/08, khong sai mot truong hop nao:
          party 3  16:55 : baybay + hoathap  GUI leave -> khong vao noi | laochin + batbat khong gui -> vao ngay
          party 11 15:50 : luu0077/0101/008  GUI leave -> khong vao noi | luuchin  khong gui -> vao CA 5 vong
          party 2  15:17 : nasau            GUI leave -> khong vao noi | 3 dua con lai         -> vao binh thuong
        Dua DUY NHAT khong gui goi nay luon la dua DUY NHAT vao duoc doi.
        """
        if not self.self_entity:
            return
        # BANG CHUNG dang o party, theo thu tu dang tin:
        #   1. roster SERVER con tuoi (S:013-006 -> team_of) - dung y `this.members[playerId]`,
        #   2. roster LOCAL con nguoi (party_members) -> party_leader,
        #   3. caller chi ra doi truong, VA minh da tung thay roster (party_leader) - tuc la co
        #      that mot party dang dinh toi minh, khong phai doan mu.
        # Khong co bang chung nao thi KHONG GUI (dung nhu client).
        _chu = self._doi_truong_dang_ket()
        if _chu and leader_entity and bytes(_chu) != bytes(leader_entity):
            log.warning("[%s] Roi party: caller doan doi truong=%s nhung roster server noi minh o "
                        "party cua %s -> gui theo ROSTER", self._label,
                        bytes(leader_entity).hex()[:8], bytes(_chu).hex()[:8])
        if not _chu and self.party_members:
            _chu = self.party_leader or self.self_entity
        if not _chu and leader_entity and self.party_leader:
            _chu = leader_entity
        if not _chu and server_bao_dang_o_party:
            # SERVER VUA TU NOI la minh dang o party (`S:007-002` ma 3 <組隊不可換分區>). Do la
            # bang chung MANH NHAT co the co - manh hon ca roster, vi roster chi la thu bot nho.
            # Khong co lenh nao hoi roster (`protocal.lua` chi co C:013-001/003/004/005/006/007/
            # 008/009/010/015 - khong co truy van) nen phai doan doi truong: la CHINH MINH.
            # Dung voi ca hai the: leader thi `members[minh] == minh`; con member ma roster rong
            # thi day la party MA, gui gi cung khong lam hong them.
            #
            # Log 31/08 party 7 (14:45-14:55): ttsau bi ma 3 SAU T LAN, moi lan deu "KHONG o party
            # nao ... KHONG gui 013-004" roi thu kenh khac -> quet het kenh 2,3,4,... 67 luot,
            # khong bao gio doi duoc kenh, party khong bao gio gom lai duoc.
            _chu = self.self_entity
            log.warning("[%s] Roi party: roster rong NHUNG server vua bao DANG TO DOI -> tin "
                        "SERVER, gui 013-004 voi ID cua chinh minh", self._label)
        if not _chu:
            # 4. ACC KHAC TRONG CUNG PARTY DANG CHI VAO MINH. Bot dieu khien ca party trong MOT
            # tien trinh, nen roster cua BON KIA cung la du lieu cua bot - va no manh hon so nho
            # cua rieng minh: mot acc noi "toi o party cua X" la co that mot party dang song voi
            # X lam doi truong.
            #
            # Ca that 08/09 party 2 (user: "p2, sao ko moi vao pt dc") - 14 phut, moi 2 phut mot
            # vong, khong lan nao vao duoc:
            #   16:34:22 [gaha] PARTY: e6a1d6f8 vao doi (leader=b59fd6f8) -> roster 1 nguoi
            #   16:36:36 [gaha] (member) roster SERVER noi minh DA o party cua LEADER b59fd6f8
            #   16:48:17 [gamo] KHONG o party nao (roster server + local deu rong) -> KHONG gui 013-004
            #   16:49:38 [gamo] (LEADER) moi 4 member ... | da join=0 | roster server=0 nguoi
            # `b59fd6f8` CHINH LA gamo. Bon member deu o party cua no, con no thi tin so nho rong
            # cua minh va tu choi giai tan -> party MA giu cham 4 member, gamo moi mai vo ich.
            try:
                _ai_chi_vao_minh = None
                with _PARTY_LOCK:
                    _peers = list((_PARTY_CLIENTS.get(self.party_idx) or {}).values())
                for _c3 in _peers:
                    if _c3 is self or not getattr(_c3, "running", False):
                        continue
                    _l3 = getattr(_c3, "party_leader", None)
                    if _l3 and bytes(_l3) == bytes(self.self_entity):
                        _ai_chi_vao_minh = getattr(_c3, "_label", "?")
                        break
                if _ai_chi_vao_minh:
                    _chu = self.self_entity
                    log.warning("[%s] Roi party: so nho cua MINH rong nhung '%s' dang o party do "
                                "CHINH MINH lam doi truong -> tin no, gui 013-004 de giai tan "
                                "party ma", self._label, _ai_chi_vao_minh)
            except Exception as e:
                log.debug("[%s] roi party: loi doi chieu roster acc khac: %s", self._label, e)
        if not _chu:
            self.party_members = []
            # IN KEM ENTITY CUA CHINH MINH. Leader in ra danh sach entity no GUI LOI MOI TOI
            # (`moi N member theo entity [...]`), nhung khong o dau in entity cua tung acc, nen
            # khong the doi chieu "leader moi ai" voi "minh la ai" - tuc ca dong triu chung
            # "cung map, cung kenh, moi mai khong vao" khong the truy tiep duoc bang log.
            # Ca that p15, 11/09 (user: "p15 van ko gom dc pt"): ca 5 acc cung map 49942, cung
            # kenh 11 (ack server), cung toa do (770,830); leader moi 2 entity moi phut ma
            # `roster server=2` khong nhuc nhich, con hai member cu lap dong nay.
            log.info("[%s] KHONG o party nao (roster server + local deu rong) -> KHONG gui "
                     "013-004 (gui mu la server ngung gui loi moi toi minh) [minh=%s map=%s "
                     "kenh=%s%s]", self._label,
                     (self.self_entity or b"").hex()[:8] or "?",
                     getattr(self, "current_map", None), getattr(self, "current_channel", None),
                     "" if self.kenh_dang_chac() else " (kenh CHUA chac)")
            return
        _chu = bytes(_chu)
        self.send(protocol.OP_PLAYER_STATE, b"\x04\x00" + _chu)
        # KHONG tu xoa roster o day. Client that chi xoa khi NHAN `S:013-004` (`Team.RemoveMember`,
        # `protocal.lua:1694`) - tuc khi SERVER xac nhan. Xoa ngay luc gui la tu lam mu chinh minh:
        # lenh bi tu choi hay roi mat thi server VAN giu doi, con bot thi tuong minh da ra, roi tu
        # do ket luan "KHONG o party nao" va khong bao gio giai tan duoc nua (party 2, 08/09).
        #
        # `_on_party` nhanh `sub == 0x04` lo phan xoa - ke ca truong hop doi truong roi (doi giai
        # tan). Neu server im lang thi roster giu nguyen, va do la SU THAT chu khong phai loi.
        log.info("[%s] Roi/giai tan party cu (gui ID doi truong=%s, cho `S:013-004` xac nhan)",
                 self._label, _chu.hex()[:8])

    def leave_single_dungeon(self) -> bool:
        """RA KHOI PHO BAN SOLO. Client: `Logic/Dungeon.lua:243`

            function Dungeon.LeaveSinglePlayDungeon()
              sendBuffer:WriteInt64(Role.playerId);   -- ID CUA CHINH MINH
              Network.Send(13, 4, sendBuffer);        -- C:013-004
            end

        Cung goi `C:013-004` nhu roi party, NHUNG mang ID CUA CHINH MINH chu khong phai ID doi
        truong (`Team.Leave` moi gui `members[playerId]` = leaderId). Day la duong THOAT INSTANCE
        cua game, KHONG phai "roi party" -> khong duoc di qua guard cua `leave_party` (guard do
        chan gui MU vao party minh khong o; o day thi client THAT cung gui dung the).

        Bug 01/09: buoc nay truoc dung `leave_party()`; sau khi them guard "khong o party thi
        KHONG gui 013-004", dungeon SOLO khong co party nao -> bi chan -> KHONG BAO GIO RA khoi
        map 62001. Log: moi acc moi luot deu "Dungeon HOAN THANH -> nhan thuong + ra" roi ngay sau
        la "KHONG o party nao ... KHONG gui 013-004", sau do `go_to_town` spam teleport 2 giay/lan
        cho toi khi server da vi `ma 13` (gui goi lien tuc qua nhanh).
        """
        if not self.self_entity:
            return False
        try:
            self.send(protocol.OP_PLAYER_STATE, b"\x04\x00" + bytes(self.self_entity))
        except OSError:
            return False
        log.info("[%s] Thoat pho ban SOLO (013-004 voi ID cua chinh minh)", self._label)
        return True

    TEAM_OF_MAX_AGE = 60.0   # roster cu hon the la khong con dang tin (party doi lien tuc)

    def _doi_truong_dang_ket(self):
        """ID doi truong cua party MINH DANG O theo roster server, hoac None.

        `Team.members[Role.playerId]` cua client Lua. Chi tin roster con TUOI: S:013-006 phat lien
        tuc theo map nen roster qua 60s = da roi map/kenh do tu lau.
        """
        ent = self.self_entity
        team_of = getattr(self, "team_of", None)
        if not ent or not team_of:
            return None
        if time.time() - float(getattr(self, "team_of_at", 0.0) or 0.0) > self.TEAM_OF_MAX_AGE:
            return None
        return team_of.get(bytes(ent))

    def dang_o_party_khac(self, entity: bytes):
        """Entity kia dang o party cua NGUOI KHAC (khong phai party minh) -> ID doi truong do.

        Client chi gui C:013-007 <邀請組隊> khi `Team.IsAlone(roleId)`; nguoi da o party thi no gui
        C:013-001 <要求組隊>. Ban 007 vao nguoi dang o party = server nuot im lang, KHONG co ma loi.
        """
        team_of = getattr(self, "team_of", None)
        if not entity or not team_of:
            return None
        if time.time() - float(getattr(self, "team_of_at", 0.0) or 0.0) > self.TEAM_OF_MAX_AGE:
            return None
        chu = team_of.get(bytes(entity))
        if not chu:
            return None
        cua_minh = self._doi_truong_dang_ket() or (
            bytes(self.self_entity) if self.self_entity else None)
        if cua_minh and bytes(chu) == bytes(cua_minh):
            return None      # cung party voi minh roi
        return chu

    def set_strategist(self, entity: bytes = None):
        """Set quan su (SP regen moi turn). C2S 0x0d sub=05 = 0d 05 00 [entity].
        entity=None -> dung self_entity (party 2 nguoi target ngam = nguoi con lai)."""
        ent = entity or self.self_entity
        if not ent:
            return
        self.send(protocol.OP_PLAYER_STATE, b"\x05\x00" + ent)
        log.info("[%s] Set quan su entity=%s", self._label, ent.hex()[:12])

    def set_party_strategist(self):
        """Leader set quan su -> SP regen cho party. CHON member da JOIN co INT CAO NHAT
        (INT cao = hoi SP tot hon khi lam quan su). Chua biet INT thi lay member dau tien."""
        joined = [e for e in _PARTY_JOINED.get(self.party_idx, set()) if e != self.self_entity]
        ents = joined or [e for e in _PARTY_ENTITIES.get(self.party_idx, set()) if e != self.self_entity]
        if not ents:
            log.warning("[%s] (LEADER) khong co member de set quan su", self._label)
            return
        best = best_int_member(self.party_idx, ents)
        chosen = best or ents[0]
        ival = _PARTY_INT.get(self.party_idx, {}).get(chosen)
        with _PARTY_LOCK:
            _PARTY_STRATEGIST[self.party_idx] = bytes(chosen)   # de GUI hien "quan su"
        self.set_strategist(chosen)
        nm = name_for_entity(chosen) or chosen.hex()[:8]
        log.info("[%s] (LEADER) set quan su = member '%s' (INT=%s)%s",
                 self._label, nm, ival,
                 "" if best else " [chua biet INT -> chon dau tien]")

    def _adv_dialog_until_idle(self, min_n: int = 3, gap: float = 0.4, idle: float = 1.5,
                                max_wait: float = 25.0) -> int:
        """Bam 'next' thoai (0x14 0600) toi khi server NGUNG phan hoi (im lang qua 'idle' giay)
        thay vi dem co dinh - tranh truong hop canh thoai that su CHUA HET (con nhieu dong hon so
        vdlg hardcode) ma da dung lai -> nhan vat con ket trong hop thoai, move sau do bi bo qua.
        Luon gui toi thieu 'min_n' lan. Tra so lan da gui."""
        import random
        t0 = time.time()
        sent = 0
        self._last_dialog_evt = time.time()
        while self.running and (time.time() - t0) < max_wait:
            self.send(0x14, b"\x06\x00")
            sent += 1
            time.sleep(max(0.2, gap + random.uniform(-0.15, 0.35)))
            if sent >= min_n and (time.time() - self._last_dialog_evt) > idle:
                break
        return sent

    _REWARD_KIND = {2: "Vang", 3: "EXP tuong", 4: "Chien doanh", 6: "Skill", 7: "Diem thuoc tinh",
                    8: "Diem skill", 9: "Nguyen bao", 10: "Manh tuong", 11: "Manh skill"}

    def _log_battle_rewards(self, pkt: bytes):
        """Log phan thuong cuoi tran/dungeon: S2C 0x14 sub0x64 (20-100 <hoan thanh nhiem vu thuong>):
        [missionId u16][count i32] << [kind i32][id u16][quant i32] >>. kind: 1=vat pham 2=vang
        3=exp tuong 4=chien doanh 5=vo tuong 6=skill 7=diem tt 8=diem skill 9=nguyen bao
        10=manh tuong 11=manh skill (tu crack Common_protocal.lua 20-100). Parse phong thu:
        loi/thieu byte -> bo qua, khong crash."""
        try:
            if len(pkt) < 15:
                return
            count = int.from_bytes(pkt[11:15], "little", signed=True)
            if count <= 0 or count > 200:
                return
            gd = _load_gamedata_items()
            npc = _load_npc_names()
            o = 15
            parts = []
            for _ in range(count):
                if o + 10 > len(pkt):
                    break
                kind = int.from_bytes(pkt[o:o + 4], "little", signed=True)
                rid = int.from_bytes(pkt[o + 4:o + 6], "little")
                quant = int.from_bytes(pkt[o + 6:o + 10], "little", signed=True)
                o += 10
                if kind == 1:
                    nm = (gd.get(rid) or {}).get("name") or ("0x%04x" % rid)
                    parts.append("%s x%d" % (nm.strip(), quant))
                elif kind == 5:
                    parts.append("Vo tuong %s" % (npc.get(rid) or ("0x%04x" % rid)))
                else:
                    parts.append("%s x%d" % (self._REWARD_KIND.get(kind, "kind%d" % kind), quant))
            if parts:
                log.info("[%s] KET TRAN nhan: %s", self._label, ", ".join(parts))
        except Exception:
            pass

    def _adv_dialog(self, n: int = 3, gap: float = 0.4):
        """Bam 'next' qua doan thoai NPC: C2S 0x14 0600 (advance scene). n lan, cach 'gap' giay
        (+- jitter ngau nhien de giong nguoi that, tranh nhip gui deu tuyet doi/may moc)."""
        import random
        for _ in range(n):
            if not self.running:
                return
            self.send(0x14, b"\x06\x00")
            time.sleep(max(0.15, gap + random.uniform(-0.15, 0.35)))

    def _dialog_until_battle(self, cap_n: int = 30, gap: float = 1.0) -> bool:
        """Spam 0x14 0600 (advance dialog NPC) toi khi BATTLE bat (state.in_battle=True) HOAC toi
        khi thay tin hieu KET TRAN THAT (sub0800 tail=03/04) - mot so canh (vd boss tu dong xu
        ly, khong co pha 0x35 that) ket THAT ma KHONG BAO GIO bat in_battle=True -> truoc day
        spam THEM 0x14 0600 vao luc da ket that -> bi server kick vi spam thua.
        So lan thoai KHAC nhau moi canh (7-20) -> KHONG hardcode, spam toi khi vao tran.
        Tra True neu da vao tran (hoac da ket that); False neu het cap_n van chua (ket / loi)."""
        import random
        t0 = time.time()
        for _ in range(cap_n):
            if not self.running:
                return False
            if self.state.in_battle or self._genuine_end_seen > t0:
                return True
            self.send(0x14, b"\x06\x00")
            time.sleep(max(0.2, gap + random.uniform(-0.15, 0.4)))
            if self.state.in_battle or self._genuine_end_seen > t0:
                return True
        return self.state.in_battle

    @_pet_role("quest")
    def do_team_dungeon(self, level: int) -> bool:
        level = int(level)
        if level == 20:
            return self.do_team_dungeon_lv20()
        if level == 50:
            return self.do_team_dungeon_lv50()
        if level == 80:
            return self.do_team_dungeon_lv80()
        if level == 110:
            return self.do_team_dungeon_lv110()
        log.warning("[%s] (LEADER) pho ban to doi lv%d: chua co kich ban route/battle an toan -> bo qua",
                    self._label, level)
        return False

    def _team_dungeon_roster_ok(self, expected_members: int, wait: float = 8.0) -> bool:
        """RULE TOI THUONG 'du party moi danh': sau START (0x2f 0c00) server phat roster party
        0x0d/06 cua PHONG PHO BAN -> doi chieu so member SERVER CONG NHAN vs so bot da moi.
        Ready 4/4 (0x2f 0b00) la bot TU BAO (timer 2.5s sau accept, khong doi server xac nhan) -
        accept co the FAIL am tham server-side (vd member vua relogin lech kenh) -> ready du ma
        phong THIEU nguoi -> START danh thieu doi (log thuc te: ready 4/4 nhung roster 3 member).
        GOI SAU START: caller phai tu xoa self.party_members truoc khi gui 0x2f 0c00 (roster cu
        cua party train van con -> khong xoa se dem nham du). Tra False neu roster thieu."""
        t0 = time.time()
        while self.running and time.time() - t0 < wait:
            if len(self.party_members) >= expected_members:
                return True
            time.sleep(0.5)
        log.warning("[%s] (LEADER) roster phong pho ban chi %d/%d member sau %.1fs -> THIEU nguoi, "
                    "HUY danh de gom lai (luot chua mat - mission step chi tang khi danh xong)",
                    self._label, len(self.party_members), expected_members, time.time() - t0)
        self._td_incomplete = True   # run_party_digioi doc co nay -> ca party relogin gom lai, danh lai
        return False

    def _create_team_dungeon_room(self, dungeon_id: int, level_label: int, ready_wait: float = 9.0) -> bool:
        # CHI dem member co client CON SONG. _PARTY_ENTITIES chi duoc THEM VAO, khong bao gio xoa:
        # acc tung chay trong phien nay (roi bi Stop / go khoi party / rot han) van de lai entity
        # -> len(ents) DOI hon so bot thuc su co the ready -> "ready 3/4 sau 40s -> HUY phong,
        # relogin ca party" lap vo tan (bug that 23:45-23:47: party 4 acc nhung moi 4 member).
        # invite_members() da loc theo client song tu truoc; rieng pho ban to doi thi khong.
        with _PARTY_LOCK:
            _live = dict(_PARTY_CLIENTS.get(self.party_idx, {}))
        allowed = getattr(self, "_td_allowed_entities", None)
        ents = [e for e in _PARTY_ENTITIES.get(self.party_idx, set())
                if e != self.self_entity and getattr(_live.get(bytes(e)), "running", False)
                and (allowed is None or bytes(e) in allowed)]
        _all = len([e for e in _PARTY_ENTITIES.get(self.party_idx, set()) if e != self.self_entity])
        if _all != len(ents):
            log.warning("[%s] (LEADER) pho ban lv%s: bo qua %d entity KHONG con client song "
                        "(tong %d -> con %d)", self._label, level_label, _all - len(ents),
                        _all, len(ents))
        if not ents:
            log.warning("[%s] (LEADER) team dungeon lv%d: chua biet entity member -> bo qua",
                        self._label, level_label)
            return False
        log.info("[%s] (LEADER) === PHO BAN TO DOI LV%d: tao + moi %d member ===",
                 self._label, level_label, len(ents))
        self._td_incomplete = False   # co "phong thieu nguoi sau START" (xem _team_dungeon_roster_ok)
        get_party_battle(self.party_idx).reset_session()
        self.flee_mode = False
        self.state.quest_mode = True
        self._leave_train_party_before_team_dungeon()
        self.send(0x2f, b"\x01\x00"); time.sleep(0.6)
        self.send(0x2f, b"\x02\x00" + struct.pack("<H", int(dungeon_id)) + b"\x01"); time.sleep(1.0)
        reset_dungeon_ready(self.party_idx)
        whitelist_count = self._invite_team_dungeon_participants(ents, gap=1.0)
        ready_wait_max = max(ready_wait, 40.0)
        t0 = time.time()
        while (not _team_dungeon_can_start(
                dungeon_ready_count(self.party_idx), len(ents), time.time() - t0,
                whitelist_count) and time.time() - t0 < ready_wait_max):
            if not self.running:
                return False
            time.sleep(0.5)
        nrdy = dungeon_ready_count(self.party_idx)
        if nrdy < len(ents):
            log.warning("[%s] (LEADER) lv%d member ready %d/%d sau %.1fs -> HUY phong, relogin ca party",
                        self._label, level_label, nrdy, len(ents), time.time() - t0)
            return False
        log.info("[%s] (LEADER) lv%d member ready %d/%d sau %.1fs (whitelist=%d, grace=%ds) -> START",
                 self._label, level_label, nrdy, len(ents), time.time() - t0,
                 whitelist_count, TEAM_DUNGEON_WHITELIST_READY_GRACE)
        self.party_members = []   # xoa roster party train CU -> cho roster MOI cua phong sau START
        self.send(0x2f, b"\x0c\x00"); time.sleep(2.0)
        self.combat_ready()
        time.sleep(0.5)
        return self._team_dungeon_roster_ok(len(ents))

    def do_team_dungeon_lv50(self, ready_wait: float = 9.0) -> bool:
        """PHO BAN TO DOI LV50 - script lay tu capture ts_capture_mumu12_teamdungeon_lv50.pcap."""
        try:
            return self._do_team_dungeon_lv50_inner(ready_wait)
        finally:
            self.state.quest_mode = False

    def _do_team_dungeon_lv50_inner(self, ready_wait: float = 9.0) -> bool:
        if not self._create_team_dungeon_room(0x000E, 50, ready_wait):
            return False
        self.set_party_strategist()

        def _moves(points, battle_no):
            # TIM DUONG THONG MINH toi diem cuoi (xem _td_walk) thay vi replay tung waypoint capture.
            self._td_walk(points, tag="lv50 tran %d" % battle_no)

        def _send(op, body, delay=0.4):
            self.send(op, body)
            time.sleep(delay)

        def _advance_once():
            self._adv_dialog(1, gap=0.45)

        # Capture lv50 co 5 tran THAT. Cac goi 0x14 0800... la chuyen canh trong kich ban,
        # khong duoc tach thanh tran rieng (tach sai -> gui buoc moi khi tran cu chua bat dau/xong).
        battle_scripts = [
            [
                ("moves", [(527, 2910), (530, 2910)]),
                ("send", 0x20, b"\x02\x00\x08"),
                ("send", 0x14, b"\x01\x00\x01\x00"),
            ],
            [
                ("vdlg", 8),
                ("moves", [(422, 2926), (490, 2930), (590, 2850)]),
                ("send", 0x14, b"\x08\x00\x02\x00"),
                ("advance",),
                ("moves", [(1264, 2045), (1178, 2037), (1091, 2029), (1005, 2021),
                           (926, 2013), (839, 2005), (753, 1997), (673, 1989),
                           (587, 1981), (501, 1973), (470, 1970)]),
                ("send", 0x20, b"\x02\x00\x08"),
                ("send", 0x14, b"\x01\x00\x05\x00"),
            ],
            [
                ("vdlg", 11),
                ("moves", [(574, 1979), (510, 1970), (350, 1930), (110, 1910)]),
                ("send", 0x14, b"\x08\x00\x04\x00"),
                ("advance",),
                ("moves", [(630, 484), (630, 404), (630, 317), (630, 270)]),
                ("send", 0x20, b"\x02\x00\x08"),
                ("send", 0x14, b"\x01\x00\x14\x00"),
            ],
            [
                ("vdlg", 10),
                ("moves", [(630, 423), (804, 520), (880, 562), (956, 605),
                           (1032, 647), (1108, 689), (1110, 690)]),
                ("send", 0x14, b"\x08\x00\x08\x00"),
            ],
            [
                ("vdlg", 9),
                ("moves", [(1096, 682), (1249, 812), (1270, 830)]),
                ("send", 0x14, b"\x08\x00\x06\x00"),
                ("advance",),
                ("moves", [(2498, 373), (2567, 321), (2610, 290)]),
                ("send", 0x20, b"\x02\x00\x08"),
                ("send", 0x14, b"\x01\x00\x1f\x00"),
            ],
        ]
        n_battles = len(battle_scripts)
        for i, actions in enumerate(battle_scripts):
            if not self.running:
                return False
            if i == 0 and self._td_daily_stop("lv50 truoc tran 1"):
                return False
            if self._td_party_gone("lv50 tran %d" % (i + 1)):
                return False
            self.flee_mode = False
            log.info("[%s] (LEADER) pho ban to doi lv50 tran %d: bat dau (map=%s pos=%s in_battle=%s)",
                     self._label, i + 1, self.current_map, self.pos, self.state.in_battle)
            if i > 0:
                ok_clear = self._wait_combat_clear(idle=2.0, cap=240.0)
                log.info("[%s] (LEADER) lv50 tran %d: het cho combat (ok=%s in_battle=%s)",
                         self._label, i + 1, ok_clear, self.state.in_battle)
                self.do_heal()
                extra_t0 = time.time()
                # KHONG cap 120s khi con dang danh THAT: ta co moc ket tran chinh xac
                # (0x14 sub0700 ha state.in_battle). Cap mu cat GIUA TRAN -> lam viec tiep
                # (an thuoc/di chuyen/transit) trong luc battle NUOT lenh. Cung loi da sua o
                # boss the gioi/boss QD (9d5b0d4), 4 cho nay bi bo sot.
                while self.state.in_battle and self.running:
                    time.sleep(1.0)
                if not self.running or self.state.in_battle:
                    log.warning("[%s] (LEADER) lv50 tran %d: tran truoc chua ket that -> dung",
                                self._label, i + 1)
                    return False
                if self._td_daily_stop("lv50 sau tran %d" % i):
                    return False
            for action in actions:
                kind = action[0]
                if kind == "vdlg":
                    n_sent = self._adv_dialog_until_idle(min_n=action[1], gap=0.4, idle=1.5, max_wait=25.0)
                    log.info("[%s] (LEADER) lv50 tran %d: da spam %d lan dialog toi khi im lang",
                             self._label, i + 1, n_sent)
                elif kind == "moves":
                    _moves(action[1], i + 1)
                elif kind == "send":
                    _send(action[1], action[2])
                elif kind == "advance":
                    _advance_once()
            import random
            time.sleep(random.uniform(1.0, 1.6))
            if not self._dialog_until_battle(cap_n=45):
                log.warning("[%s] (LEADER) lv50 tran %d: spam dialog ma khong vao/ket battle -> dung",
                            self._label, i + 1)
                return False
            if not self.running:
                log.warning("[%s] (LEADER) lv50 mat ket noi sau tran %d -> fail", self._label, i + 1)
                return False
            log.info("[%s] (LEADER) pho ban to doi lv50: VAO TRAN %d/%d",
                     self._label, i + 1, n_battles)
        self._wait_combat_clear(idle=2.0, cap=240.0)
        extra_t0 = time.time()
        # KHONG cap 120s khi con dang danh THAT: ta co moc ket tran chinh xac
        # (0x14 sub0700 ha state.in_battle). Cap mu cat GIUA TRAN -> lam viec tiep
        # (an thuoc/di chuyen/transit) trong luc battle NUOT lenh. Cung loi da sua o
        # boss the gioi/boss QD (9d5b0d4), 4 cho nay bi bo sot.
        while self.state.in_battle and self.running:
            time.sleep(1.0)
        if not self.running:
            log.warning("[%s] (LEADER) lv50 mat ket noi truoc khi roi pho ban -> fail", self._label)
            return False
        self._adv_dialog_until_idle(min_n=6, gap=0.4, idle=1.5, max_wait=20.0)
        self._adv_dialog(1, gap=0.4)
        self._td_walk([(2508, 365)], tag="lv50 ra cong")
        log.info("[%s] (LEADER) === PHO BAN TO DOI LV50 XONG -> roi pho ban ===", self._label)
        self.leave_party()
        time.sleep(2.0)
        return True

    def do_team_dungeon_lv80(self, ready_wait: float = 9.0) -> bool:
        """PHO BAN TO DOI LV80 - script lay tu capture ts_capture_mumu12_teamdungeon_lv80.pcap."""
        try:
            return self._do_team_dungeon_lv80_inner(ready_wait)
        finally:
            self.state.quest_mode = False

    def _do_team_dungeon_lv80_inner(self, ready_wait: float = 9.0) -> bool:
        if not self._create_team_dungeon_room(0x000F, 80, ready_wait):
            return False
        self.scene_resume(settle=0.5)
        self.set_party_strategist()

        def _moves(points, battle_no):
            # TIM DUONG THONG MINH toi diem cuoi (xem _td_walk) thay vi replay tung waypoint capture.
            self._td_walk(points, tag="lv80 tran %d" % battle_no)

        def _send(op, body, delay=0.4):
            self.send(op, body)
            time.sleep(delay)

        def _dialog_idle(min_n, battle_no, max_wait=25.0):
            n_sent = self._adv_dialog_until_idle(min_n=min_n, gap=0.4, idle=1.5, max_wait=max_wait)
            log.info("[%s] (LEADER) lv80 tran %d: da spam %d lan dialog toi khi im lang",
                     self._label, battle_no, n_sent)

        def _battle_start(cap_n=50, gap=1.0):
            # Lv80 co nhieu 0x14 sub0700 trong doan chuyen canh TRUOC battle that.
            # Vi vay khong dung _dialog_until_battle() (ham do chap nhan _genuine_end_seen).
            import random
            for _ in range(cap_n):
                if not self.running:
                    return False
                if self.state.in_battle:
                    return True
                self.send(0x14, b"\x06\x00")
                time.sleep(max(0.2, gap + random.uniform(-0.15, 0.4)))
                if self.state.in_battle:
                    return True
            return bool(self.state.in_battle)

        def _advance_once():
            self._adv_dialog(1, gap=0.45)

        # Capture lv80 co nhieu canh "noi chuyen/chuyen canh roi di tiep", khong vao tran ngay.
        # Chi cac action "battle" moi cho in_battle=True; cac doan khac chi advance dialog/route.
        battle_scripts = [
            [
                ("moves", [(530, 4310)]),
                ("send", 0x14, b"\x08\x00\x03\x00"),
                ("advance",),
                ("moves", [(1210, 3590)]),
                ("send", 0x14, b"\x08\x00\x05\x00"),
                ("advance",),
                ("moves", [(830, 2430)]),
                ("send", 0x20, b"\x02\x00\x08"),
                ("send", 0x14, b"\x01\x00\x05\x00"),
                ("dialog", 8),
                ("moves", [(350, 2690)]),
                ("send", 0x14, b"\x08\x00\x06\x00"),
                ("advance",),
                ("moves", [(70, 3630)]),
                ("send", 0x14, b"\x08\x00\x04\x00"),
                ("advance",),
                ("moves", [(370, 4410)]),
                ("send", 0x14, b"\x08\x00\x01\x00"),
                ("battle",),
            ],
            [
                ("dialog", 9),
                ("moves", [(530, 4310)]),
                ("send", 0x14, b"\x08\x00\x03\x00"),
                ("advance",),
                ("moves", [(1210, 3590)]),
                ("send", 0x14, b"\x08\x00\x05\x00"),
                ("advance",),
                ("moves", [(830, 2430)]),
                ("send", 0x20, b"\x02\x00\x08"),
                ("send", 0x14, b"\x01\x00\x05\x00"),
                ("dialog", 6),
                ("moves", [(510, 2450)]),
                ("send", 0x20, b"\x02\x00\x08"),
                ("send", 0x14, b"\x01\x00\x06\x00"),
                ("battle",),
            ],
            [
                ("dialog", 2),
                ("moves", [(350, 2690)]),
                ("send", 0x14, b"\x08\x00\x06\x00"),
                ("advance",),
                ("moves", [(730, 3370)]),
                ("send", 0x14, b"\x08\x00\x09\x00"),
                ("advance",),
                ("moves", [(730, 790)]),
                ("send", 0x14, b"\x08\x00\x08\x00"),
                ("battle",),
            ],
            [
                ("dialog", 6),
                ("moves", [(70, 1210)]),
                ("send", 0x14, b"\x08\x00\x07\x00"),
                ("advance",),
                ("moves", [(1210, 3590)]),
                ("send", 0x14, b"\x08\x00\x05\x00"),
                ("advance",),
                ("moves", [(830, 2430)]),
                ("send", 0x20, b"\x02\x00\x08"),
                ("send", 0x14, b"\x01\x00\x05\x00"),
                ("dialog", 10),
                ("moves", [(610, 2410)]),
                ("send", 0x20, b"\x02\x00\x08"),
                ("send", 0x14, b"\x01\x00\x0b\x00"),
                ("battle",),
            ],
            [
                ("dialog", 3),
                ("moves", [(350, 2690)]),
                ("send", 0x14, b"\x08\x00\x06\x00"),
                ("advance",),
                ("moves", [(730, 3370)]),
                ("send", 0x14, b"\x08\x00\x09\x00"),
                ("advance",),
                ("moves", [(730, 790)]),
                ("send", 0x14, b"\x08\x00\x08\x00"),
                ("battle",),
            ],
        ]
        n_battles = len(battle_scripts)
        for i, actions in enumerate(battle_scripts):
            if not self.running:
                return False
            if i == 0 and self._td_daily_stop("lv80 truoc tran 1"):
                return False
            if self._td_party_gone("lv80 tran %d" % (i + 1)):
                return False
            self.flee_mode = False
            log.info("[%s] (LEADER) pho ban to doi lv80 tran %d: bat dau (map=%s pos=%s in_battle=%s)",
                     self._label, i + 1, self.current_map, self.pos, self.state.in_battle)
            if i > 0:
                ok_clear = self._wait_combat_clear(idle=2.0, cap=300.0)
                log.info("[%s] (LEADER) lv80 tran %d: het cho combat (ok=%s in_battle=%s)",
                         self._label, i + 1, ok_clear, self.state.in_battle)
                self.do_heal()
                extra_t0 = time.time()
                # Nhu cac vong khac: khong cap thoi gian khi con dang danh THAT (xem 9d5b0d4).
                while self.state.in_battle and self.running:
                    time.sleep(1.0)
                if not self.running or self.state.in_battle:
                    log.warning("[%s] (LEADER) lv80 tran %d: tran truoc chua ket that -> dung",
                                self._label, i + 1)
                    return False
                if self._td_daily_stop("lv80 sau tran %d" % i):
                    return False
            for action in actions:
                kind = action[0]
                if kind == "dialog":
                    _dialog_idle(action[1], i + 1)
                elif kind == "moves":
                    _moves(action[1], i + 1)
                elif kind == "send":
                    _send(action[1], action[2])
                elif kind == "advance":
                    _advance_once()
                elif kind == "battle":
                    import random
                    time.sleep(random.uniform(1.0, 1.6))
                    if not _battle_start(cap_n=55):
                        log.warning("[%s] (LEADER) lv80 tran %d: spam dialog ma khong vao battle -> dung",
                                    self._label, i + 1)
                        return False
                    if not self.running:
                        log.warning("[%s] (LEADER) lv80 mat ket noi sau tran %d -> fail",
                                    self._label, i + 1)
                        return False
                    log.info("[%s] (LEADER) pho ban to doi lv80: VAO TRAN %d/%d",
                             self._label, i + 1, n_battles)
        self._wait_combat_clear(idle=2.0, cap=300.0)
        extra_t0 = time.time()
        # Nhu cac vong khac: khong cap thoi gian khi con dang danh THAT (xem 9d5b0d4).
        while self.state.in_battle and self.running:
            time.sleep(1.0)
        if not self.running:
            log.warning("[%s] (LEADER) lv80 mat ket noi truoc khi roi pho ban -> fail", self._label)
            return False
        self._adv_dialog_until_idle(min_n=3, gap=0.4, idle=1.5, max_wait=20.0)
        self._td_walk([(730, 790)], tag="lv80 ra cong")
        log.info("[%s] (LEADER) === PHO BAN TO DOI LV80 XONG -> roi pho ban ===", self._label)
        self.leave_party()
        time.sleep(2.0)
        return True

    def _wait_team_dungeon_end(self, start_seq: int, timeout: float = 360.0,
                               since: float = None) -> bool:
        deadline = time.time() + timeout
        end_floor = since
        candidate_at = None
        candidate_reinforcement = getattr(self, "_team_dungeon_reinforcement_seq", 0)
        while self.running and time.time() < deadline:
            if self._team_dungeon_end_seq > start_seq:
                return True
            if self._td_party_gone("cho ket tran PB110"):
                return False
            now = time.time()
            reinforcement = getattr(self, "_team_dungeon_reinforcement_seq", 0)
            if candidate_at is not None and reinforcement != candidate_reinforcement:
                candidate_at = None
                end_floor = now
            if candidate_at is not None and now - candidate_at >= 3.0:
                return True
            # CHI coi tran ket khi CHINH LEADER da het in_battle (nhan WIN 0x14 sub0700 cua chinh no).
            # Truoc day chi dua vao _recent_battle_end (dau hieu MEMBER ket tran) -> leader nhay stage
            # sau TRONG KHI tran cua no chua xong (WIN den muon) -> gui cong stage sau giua tran ->
            # tran sau khong start (bug that PB110: "tran 3 bat dau" luc 40:32 nhung WIN tran 2 den
            # 40:56 -> tran 3 khong thay battle start -> FAIL -> relogin vo han).
            if (candidate_at is None and not self.state.in_battle and _recent_battle_end(
                    getattr(self, "party_idx", None), within=3.0,
                    map_id=getattr(self, "current_map", None), since=end_floor)):
                candidate_at = now
                candidate_reinforcement = reinforcement
            time.sleep(0.2)
        return False

    def _wait_team_dungeon_complete(self, timeout: float = 360.0) -> bool:
        deadline = time.time() + timeout
        while self.running and time.time() < deadline:
            if self.dungeon_complete or self.team_dungeon_remaining(110) == 0:
                return True
            time.sleep(0.2)
        return False

    def _advance_to_team_dungeon_complete(self, cap_n: int = 12) -> bool:
        for _ in range(cap_n):
            if not self.running:
                return False
            self._adv_dialog(1, gap=0.4)
            if self._wait_team_dungeon_complete(timeout=0.8):
                return True
        return False

    def _advance_to_team_dungeon_battle(self, cap_n: int, grace: float = 22.0) -> bool:
        start_seq = self._battle_start_seq
        end0 = self._team_dungeon_end_seq
        t0 = time.time()
        started = lambda: (self._battle_start_seq > start_seq or self.state.in_battle
                           or self._team_dungeon_end_seq > end0 or self._genuine_end_seen > t0)
        # PHASE 1: bam dialog (0x14 0600) qua doan chuyen canh de TRIGGER battle. Bounded cap_n.
        for _ in range(cap_n):
            if not self.running:
                return False
            if started():
                return True
            self._adv_dialog(1, gap=0.8)
            if started():
                return True
        # PHASE 2 (grace): 0x34 battle-start co the den MUON (server/emulator cham). CHO THU DONG -
        # CHI poll, KHONG gui them 0x14 0600. Truoc day grace van _adv_dialog moi vong -> spam 0x14
        # 0600 luc chuyen canh/da ket -> SERVER KICK ("Server dong ket noi", log 09:04). Dialog da bam
        # xong o phase 1 roi; battle-start tu server ve khong can nudge them.
        deadline = time.time() + grace
        while self.running and time.time() < deadline:
            if started():
                return True
            time.sleep(0.5)
        return started()

    def _run_team_dungeon_lv110_stage(self, actions: tuple, stage_no: int) -> bool:
        for action in actions:
            if not self.running:
                return False
            if self._td_party_gone("PB110 tran %d" % stage_no):
                return False
            kind = action[0]
            if kind == "send":
                self.send(action[1], action[2])
                time.sleep(0.4)
            elif kind == "advance":
                self._adv_dialog(action[1], gap=0.4)
            elif kind == "moves":
                # TIM DUONG THONG MINH toi diem cuoi (xem _td_walk).
                self._td_walk(action[1], tag="lv110 tran %d" % stage_no)
            elif kind == "heal_full":
                time.sleep(0.5)
                self.heal_full(force=True)
            elif kind == "battle":
                end_seq = self._team_dungeon_end_seq
                wait_since = time.time()
                if not self._advance_to_team_dungeon_battle(action[1]):
                    log.warning("[%s] (LEADER) PB110 tran %d: khong thay battle start", self._label, stage_no)
                    return False
                log.info("[%s] (LEADER) PB110: VAO TRAN %d/5", self._label, stage_no)
                if not self._wait_team_dungeon_end(end_seq, since=wait_since):
                    log.warning("[%s] (LEADER) PB110 tran %d: khong thay moc ket tran", self._label, stage_no)
                    return False
            else:
                log.warning("[%s] (LEADER) PB110 tran %d: action la %s", self._label, stage_no, kind)
                return False
        return True

    def do_team_dungeon_lv110(self, ready_wait: float = 9.0) -> bool:
        self._active_team_dungeon_level = 110
        self._team_dungeon_end_seq = 0
        self._team_dungeon_reinforcement_seq = 0
        try:
            return self._do_team_dungeon_lv110_inner(ready_wait)
        finally:
            self._active_team_dungeon_level = None
            self.state.quest_mode = False

    def _do_team_dungeon_lv110_inner(self, ready_wait: float = 9.0) -> bool:
        self.dungeon_complete = False
        if not self._create_team_dungeon_room(team_dungeon_lv110.DUNGEON_ID, 110, ready_wait):
            return False
        self.scene_resume(settle=0.5)
        self.set_party_strategist()
        for stage_no, actions in enumerate(team_dungeon_lv110.STAGES, 1):
            if self._td_daily_stop("PB110 truoc tran %d" % stage_no):
                return False
            log.info("[%s] (LEADER) PB110 tran %d: bat dau", self._label, stage_no)
            if not self._run_team_dungeon_lv110_stage(actions, stage_no):
                return False
        if not self._advance_to_team_dungeon_complete():
            log.warning("[%s] (LEADER) PB110: da bam thoai tong ket nhung khong thay mission 0x30ae",
                        self._label)
            return False
        time.sleep(0.5)
        self.state.in_battle = False
        self.heal_full(force=True)
        self._adv_dialog(1, gap=0.4)
        self._td_walk([(2124, 283)], tag="lv110 ra cong")
        log.info("[%s] (LEADER) === PHO BAN TO DOI LV110 XONG -> roi pho ban ===", self._label)
        self.leave_party()
        time.sleep(2.0)
        return True

    def do_team_dungeon_lv20(self, n_battles: int = 4, ready_wait: float = 9.0) -> bool:
        """PHO BAN TO DOI LV20 (o5 daily) - chi LEADER goi. Member da auto-accept (0x2f 0f->03) +
        auto-ready (0x2f 0b) trong _on_dungeon -> member chi di theo, KHONG can lam gi.
        Luong (capture team.pcap, xem KNOWLEDGE 7n):
          tao (0x2f 0100 / 0200010001) -> moi tung member (0x2f 0800 [entity]) -> start (0x2f 0c00)
          -> 4 tran: chuyen canh + spam dialog toi khi battle + cho het tran; set quan su sau tran 1.
        TRIGGER battle = spam 0x14 0600 (KHONG phu thuoc di chuyen). Tra True neu chay het 4 tran.

        LUU Y cho cac phien ban cao hon sau nay (lv30/40/...): kich ban tung tran (segments, so
        lan thoai, toa do move, transit) se KHAC hoan toan theo tung level -> viet ham rieng
        (vd do_team_dungeon_lv30). Phan CO CHE CHUNG van ap dung lai duoc - xem KNOWLEDGE 7n:
          - Phai goi combat_ready() (gui lai 0x41) ngay sau khi START phong, khong thi nhan vat
            se KHONG DI CHUYEN THAT du gui dung goi 0x06.
          - Dialog voi NPC: spam 0x14 0600 toi khi server IM LANG that su (whitelist sub
            0100/1000/0d00, KHONG dung kieu loai-tru vi con sub rac khac lam sai dong ho im lang).
          - Moc ket tran THAT: 0x14 sub0800 byte cuoi = 0x03/0x04 (bat ke in_battle dang gi) -
            KHONG chi dua vao in_battle=True vi co canh tu dong resolve khong bao gio bat co nay.
          - Nhan thuong TRUOC KHI leave_party() - noi dung goi nhan thuong co the khac theo tung
            pho ban, nhung thu tu (thoai tong ket -> nhan thuong -> giai tan) nen giu nguyen."""
        try:
            return self._do_team_dungeon_lv20_inner(n_battles, ready_wait)
        finally:
            # try/finally BAO DAM quest_mode luon ve False khi ham nay ket thuc, BAT KE thoat qua
            # duong nao (return som, exception, mat ket noi giua chung...) - THAY VI dua vao gan
            # `quest_mode = False` thu cong o TUNG diem return rieng le (de sot 1 cho -> quest_mode
            # KET DINH VINH VIEN True sau do, anh huong toi CA cac tran train binh thuong sau nay,
            # ke ca CAC NGAY SAU neu process chay xuyen ngay khong restart). Nghi van tu nguoi dung:
            # "check nhiem vu 5 da xong -> KHONG goi ham nay -> quest_mode khong duoc set nen khong
            # phai do duong nay" - nhung PHONG THU van dat o day cho MOI lan ham THAT SU duoc goi
            # (vd lan truoc dungeon bi rot giua chung do loi khong luong truoc).
            self.state.quest_mode = False

    def _do_team_dungeon_lv20_inner(self, n_battles: int = 4, ready_wait: float = 9.0) -> bool:
        allowed = getattr(self, "_td_allowed_entities", None)
        with _PARTY_LOCK:
            live = dict(_PARTY_CLIENTS.get(self.party_idx, {}))
        ents = [e for e in _PARTY_ENTITIES.get(self.party_idx, set())
                if e != self.self_entity and getattr(live.get(bytes(e)), "running", False)
                and (allowed is None or bytes(e) in allowed)]
        if not ents:
            log.warning("[%s] (LEADER) do_team_dungeon_lv20: chua biet entity member -> bo qua", self._label)
            return False
        log.info("[%s] (LEADER) === PHO BAN TO DOI LV20: tao + moi %d member ===", self._label, len(ents))
        self._td_incomplete = False   # co "phong thieu nguoi sau START" (xem _team_dungeon_roster_ok)
        self.flee_mode = False   # PHO BAN: leader PHAI DANH (flee_mode tu flow daily -> leader bo chay,
                                 #   ket tran sai 0x14 0c00/0900/0800 thay vi WIN 0x14 sub0700 -> hong)
        # Ep QUEST mode suot ca pho ban - KHONG dua vao auto-latch dem so quai luc bat dau tran (>5)
        # nhu binh thuong (state.py update_0x33), vi so quai co the it hon o mot so tran/level ->
        # muon danh theo quest_mode CO DINH cho toi khi xong het dungeon (hoac fail/thoat giua chung).
        self.state.quest_mode = True
        self._leave_train_party_before_team_dungeon()
        # 1. Tao pho ban to doi
        self.send(0x2f, b"\x01\x00"); time.sleep(0.6)
        self.send(0x2f, bytes.fromhex("0200010001")); time.sleep(1.0)
        # 2. Moi tung member theo ENTITY (0x2f 0800 [entity 8B]) - KHAC party-invite 0x0d 07
        reset_dungeon_ready(self.party_idx)   # xoa tin hieu ready cu (lan pho ban truoc) tranh nham
        whitelist_count = self._invite_team_dungeon_participants(ents, gap=1.0)
        # 3. Cho member auto-accept + auto-ready THAT SU (POLL dungeon_ready_count, KHONG doan gio
        # co dinh). _handle_o5_team CHI goi ham nay khi CA PARTY da bao "chua xong o5" (xem
        # run_party_digioi.py) -> luc nay moi member dang o trong vong CHO thu dong (khong lam viec
        # gi khac) nen se accept+ready ngay khi nhan duoc goi moi - nhung VAN can cho THAT SU (khong
        # doan 9s co dinh) vi do tre mang/xu ly co the > 9s -> truoc day het 9s la START LUON bat ke
        # member da ready chua, co the START ma party CHUA DU (danh mot minh, mat luot ca party).
        # CO CAP (toi da ready_wait_max giay) de KHONG "cho ca ngay" neu 1 member that su khong bao
        # gio ready duoc (vd mat ket noi giua chung) - het cap thi FAIL + relogin ca party, TUYET DOI
        # khong START mot minh.
        ready_wait_max = max(ready_wait, 40.0)
        t0 = time.time()
        while (not _team_dungeon_can_start(
                dungeon_ready_count(self.party_idx), len(ents), time.time() - t0,
                whitelist_count) and time.time() - t0 < ready_wait_max):
            if not self.running:
                self.state.quest_mode = False
                return False
            time.sleep(0.5)
        nrdy = dungeon_ready_count(self.party_idx)
        if nrdy < len(ents):
            log.warning("[%s] (LEADER) member ready %d/%d sau %.1fs -> HUY phong, relogin ca party",
                        self._label, nrdy, len(ents), time.time() - t0)
            self.state.quest_mode = False
            return False
        log.info("[%s] (LEADER) member ready %d/%d sau %.1fs (whitelist=%d, grace=%ds) -> START",
                 self._label, nrdy, len(ents), time.time() - t0,
                 whitelist_count, TEAM_DUNGEON_WHITELIST_READY_GRACE)
        self.party_members = []   # xoa roster party train CU -> cho roster MOI cua phong sau START
        self.send(0x2f, b"\x0c\x00"); time.sleep(2.0)
        # DBG: doi chieu capture nguoi that (dieusau) vs bot (cung acc) phat hien: nguoi that gui
        # 0x41 (OP_BATTLE_ENTER, "dang ky san sang battle" - da dung o _login_setup/combat_ready
        # cho map thuong) TOI 13 LAN trong ca phien pho ban to doi; bot KHONG BAO GIO gui goi nay
        # trong do_team_dungeon -> nghi day chinh la goi con thieu khien tran 4 (co che transit
        # 0x20 rieng) khong duoc cong nhan. combat_ready() da dung o noi khac sau doi kenh/lap
        # party (chinh xac tinh huong tuong tu: tao party moi cho pho ban).
        self.combat_ready()
        time.sleep(0.5)
        if not self._team_dungeon_roster_ok(len(ents)):
            self.state.quest_mode = False
            return False
        # 4. Vong battle. Moi tran: (dismiss thoai thang loi) -> [set quan su sau B1] -> DI TOI CONG
        #    (moves, _route_move dam bao toi noi - THIEU buoc nay server DA leader!) -> transit -> spam
        #    dialog toi khi battle. Moves + transit lay tu capture team.pcap (KNOWLEDGE 7n).
        # vdlg = SO LAN dialog thang loi sau battle TRUOC (cutscene co dinh - capture: B1->B2=9,
        #   B2->B3=10, B3->B4=20; thieu -> ket khong qua duoc man thang loi -> transit truot).
        segments = [
            # B1: vao thang pho ban (khong move)
            {"vdlg": 0, "pre": [], "moves": [], "transit": [(0x14, b"\x08\x00\x01\x00")]},
            # B2: thang loi B1 (9) -> set quan su -> di toi cong canh 2 -> transit
            {"vdlg": 9, "pre": [], "moves": [(243, 749), (327, 727), (411, 705), (470, 690)],
             "transit": [(0x14, b"\x08\x00\x02\x00")]},
            # B3: 0x7c 0400 -> thang loi B2 (10) -> di toi cong canh 3 -> transit
            {"vdlg": 10, "pre": [(0x7c, b"\x04\x00")], "moves": [(660, 584), (730, 550)],
             "transit": [(0x14, b"\x08\x00\x03\x00")]},
            # B4: thang loi B3 (20) -> di -> event teleport (giong boss the gioi)
            {"vdlg": 20, "pre": [], "moves": [(730, 550), (690, 450)],
             "transit": [(0x20, b"\x02\x00\x08"), (0x14, b"\x01\x00\x14\x00")]},
        ]
        for i in range(min(n_battles, len(segments))):
            if not self.running:
                self.state.quest_mode = False
                return False
            if i == 0 and self._td_daily_stop("lv20 truoc tran 1"):
                return False
            seg = segments[i]
            if self._td_party_gone("lv20 tran %d" % (i + 1)):
                self.state.quest_mode = False
                return False
            self.flee_mode = False   # giu DANH suot pho ban (khong bo chay tran nao)
            log.info("[%s] (LEADER) tran %d: bat dau (map=%s pos=%s in_battle=%s)",
                     self._label, i + 1, self.current_map, self.pos, self.state.in_battle)
            if i > 0:
                ok_clear = self._wait_combat_clear(idle=2.0, cap=240.0)   # battle truoc xong
                log.info("[%s] (LEADER) tran %d: het cho combat (ok=%s in_battle=%s)",
                         self._label, i + 1, ok_clear, self.state.in_battle)
                self.do_heal()   # xong battle truoc -> hoi HP/SP (no-op neu con in_battle)
                # QUAN TRONG: neu het cap ma in_battle VAN True (tran truoc CHUA THAT SU xong,
                # chi la cap qua ngan so voi tran thuc te dai - vd quai tru danh, char thieu SP
                # phai spam danh thuong) -> KHONG duoc lao vao gui dialog/move/transit (seg moi)
                # NGAY LUC do, vi tran cu van dang giai quyet server-side -> lenh bi server bo qua
                # hoac cham voi tran dang xu ly -> KICK ket noi (da xac nhan qua log thuc te: dung
                # ngay diem nay). Doi THEM toi khi THAT SU het tran (hoac mat ket noi) truoc khi di tiep.
                _extra_t0 = time.time()
                # KHONG cap 120s khi con dang danh THAT: ta co moc ket tran chinh xac
                # (0x14 sub0700 ha state.in_battle). Cap mu cat GIUA TRAN -> lam viec tiep
                # (an thuoc/di chuyen/transit) trong luc battle NUOT lenh. Cung loi da sua o
                # boss the gioi/boss QD (9d5b0d4), 4 cho nay bi bo sot.
                while self.state.in_battle and self.running:
                    time.sleep(1.0)
                if not self.running:
                    self.state.quest_mode = False
                    return False
                if self.state.in_battle:
                    log.warning("[%s] (LEADER) tran %d: tran truoc VAN chua ket that sau %.0fs cho them "
                                "-> dung (tranh gui lenh de kick ket noi)", self._label, i + 1,
                                240.0 + 120.0)
                    self.state.quest_mode = False
                    return False
                if self._td_daily_stop("lv20 sau tran %d" % i):
                    return False
                for op, body in seg["pre"]:                    # pre (0x7c 0400) TRUOC thoai thang loi
                    self.send(op, body); time.sleep(0.4)
                # Nguoi that xac nhan: leader KHONG di chuyen that suot ca pho ban (0x06 dung cu
                # phap nhung vo hieu). Nghi van: canh thoai thang loi CHUA HET that su (vdlg hardcode
                # co the it hon so dong thoai that) -> nhan vat con ket trong hop thoai -> move/transit
                # sau do bi bo qua. Spam TOI KHI server ngung phan hoi (thay vi dem co dinh vdlg).
                n_sent = self._adv_dialog_until_idle(min_n=seg["vdlg"], gap=0.4, idle=1.5, max_wait=25.0)
                log.info("[%s] (LEADER) tran %d: da spam %d lan dialog (vdlg hardcode=%d) "
                         "toi khi im lang", self._label, i + 1, n_sent, seg["vdlg"])
                if i == 1:
                    self.set_party_strategist()                # set quan su SAU tran 1 (sau thoai thang loi)
                # DI toi cong bang TIM DUONG THONG MINH (xem _td_walk) truoc khi transit.
                self._td_walk(seg["moves"], tag="lv20 tran %d" % (i + 1))
            for op, body in seg["transit"]:
                self.send(op, body); time.sleep(0.4)
            log.info("[%s] (LEADER) tran %d: da gui transit -> spam dialog cho battle...",
                     self._label, i + 1)
            # So sanh capture: nguoi that doi ~1.27s truoc khi bam dialog TIEP THEO ngay sau khi
            # nhan dong thoai dau tien cua canh transit (bot truoc day chi doi ~0.34s -> bi server
            # boqua/tra ve sub0800 rac thay vi tiep tuc thoai that). Doi lau hon truoc lan bam dau.
            import random
            time.sleep(random.uniform(1.0, 1.6))
            if not self._dialog_until_battle(cap_n=40):
                log.warning("[%s] (LEADER) tran %d: spam dialog ma khong vao battle -> dung "
                            "(map=%s pos=%s in_battle=%s)", self._label, i + 1,
                            self.current_map, self.pos, self.state.in_battle)
                self.state.quest_mode = False
                return False
            # _dialog_until_battle co the tra True vi _genuine_end_seen dung luc ket noi VUA mat
            # (server dong ket noi ngay sau khi gui goi ket tran that) -> PHAI check self.running
            # o day, KHONG thi ham log "VAO TRAN" + tiep tuc segment sau NHU KHONG CO CHUYEN GI,
            # trong khi thuc te da rot ket noi tu truoc do (xac nhan qua log thuc te: dong ket noi
            # xong van thay tiep "VAO TRAN 3/4" roi "xong daily login" nhu binh thuong).
            if not self.running:
                log.warning("[%s] (LEADER) mat ket noi ngay sau ket tran that (tran %d) -> BAO FAIL",
                            self._label, i + 1)
                self.state.quest_mode = False
                return False
            log.info("[%s] (LEADER) pho ban to doi lv20: VAO TRAN %d/%d", self._label, i + 1, n_battles)
        # cho tran cuoi xong (buffer chong va cham voi tran chua xong THAT da nam trong
        # _wait_combat_clear())
        self._wait_combat_clear(idle=2.0, cap=240.0)
        # Tran co the CHUA THAT SU xong sau cap (nhu i>0 o tren) -> cho THEM toi khi that su
        # het tran truoc khi claim thuong/leave_party (tranh gui lenh de kick ket noi).
        _extra_t0 = time.time()
        # KHONG cap 120s khi con dang danh THAT: ta co moc ket tran chinh xac
        # (0x14 sub0700 ha state.in_battle). Cap mu cat GIUA TRAN -> lam viec tiep
        # (an thuoc/di chuyen/transit) trong luc battle NUOT lenh. Cung loi da sua o
        # boss the gioi/boss QD (9d5b0d4), 4 cho nay bi bo sot.
        while self.state.in_battle and self.running:
            time.sleep(1.0)
        # QUAN TRONG: kiem tra lai self.running TRUOC KHI claim thuong/bao thanh cong. send() khi
        # running=False chi AM THAM khong lam gi (khong loi) -> neu KHONG check o day, ham se chay
        # het toi cuoi, log "XONG"/"NHAN THUONG" va return True GIA du thuc te da bi server ngat
        # ket noi (van game) tu truoc do va KHONG goi tin nao thuc su gui duoc.
        if not self.running:
            log.warning("[%s] (LEADER) mat ket noi truoc khi kip nhan thuong/roi pho ban -> BAO FAIL "
                        "(khong phai thanh cong that)", self._label)
            self.state.quest_mode = False
            return False
        # Capture nguoi that (dieusau) xac nhan: sau khi thang tran 4, con 1 doan thoai tong ket
        # (0x14 sub0100/1000 lap) roi man hinh thuong (dungeon_complete, giong sub=64 da dung o
        # tinh nang khac) -> client gui 0x5b 0200010100053300 (NHAN THUONG) TRUOC KHI gui 0x0d 04
        # (giai tan party). Truoc day do_team_dungeon roi party NGAY, bo qua buoc nhan thuong ->
        # bi tinh la CHUA hoan thanh du da danh xong ca 4 tran.
        self._adv_dialog_until_idle(min_n=5, gap=0.4, idle=1.5, max_wait=20.0)
        time.sleep(1.0)
        if not self.running:   # co the mat ket noi trong luc spam dialog tong ket o tren
            log.warning("[%s] (LEADER) mat ket noi truoc khi kip nhan thuong -> BAO FAIL", self._label)
            self.state.quest_mode = False
            return False
        self.send(0x5b, bytes.fromhex("0200010100053300"))   # NHAN THUONG pho ban to doi
        log.info("[%s] (LEADER) da gui goi NHAN THUONG pho ban to doi", self._label)
        time.sleep(2.0)
        log.info("[%s] (LEADER) === PHO BAN TO DOI LV20 XONG (%d tran) -> roi pho ban ===", self._label, n_battles)
        self.leave_party()     # 0x0d 04 = roi/giai tan -> thoat pho ban
        time.sleep(2.0)
        self.state.quest_mode = False   # het dungeon -> ha quest_mode ep buoc, ve mac dinh auto-latch
        return True

    def move_to(self, x: int, y: int) -> bool:
        """C2S 0x06: di chuyen nhan vat toi (x,y). Server tu di toi do.
        Dead-reckoning: server KHONG echo vi tri minh -> tu nho pos = diem vua gui di.

        TRONG TRAN THI KHONG DI, VA KHONG CONG POS - y het client (`MoveController.lua:232`):

            function MoveController.SendRolePosition()
              if FightField.isInBattle or not Role.CanControl() then return end
                                                          --戰鬥中或不可控制，不移動

        Client dung HAN vong gui vi tri khi vao tran: khong tinh them buoc nao, khong gui goi nao;
        het tran thi di tiep TU CHO DANG DUNG. Bot thi van `self.pos = (x, y)` - nhay thang toi
        dich ngay luc gui lenh, ke ca khi lenh do bi tran nuot. Tu do `pos` sai vinh vien, va moi
        thu doc `pos` deu sai theo.

        Ca that 08/09 (user: "dang di chuyen ma vao battle thi no bi mat di chuyen nhung m van tinh
        la di chuyen den safe ok, thanh ra no chua o safe va bi quai danh, con may thi nhan biet
        ngu"): bot bao "SAFE BI QUAI DANH ... diem safe nay co the hoc SAI" 32 lan cho map 21814,
        user login vao kiem thi safe hoan toan binh thuong - acc chi dang dung DO DANG giua duong.

        Tra True neu that su co gui lenh.
        """
        try:
            if self.in_combat(idle_secs=1.0):
                log.debug("[%s] move_to (%d,%d): DANG TRONG TRAN -> khong di, khong cong pos "
                          "(giong client)", self._label, x, y)
                return False
        except Exception:
            pass
        self.send(0x06, b"\x01\x00\x01" + struct.pack("<HH", x, y))
        self.pos = (x, y)
        return True

    NAV_CHO_TRAN_CAP = 180.0  # cho MOT tran giua duong toi ngan nay; qua = coi nhu ket, bo chang
    NAV_TOI_NOI = 60         # cach dich duoi nguong nay = coi la da toi (server tra toa do lam tron)
    NAV_XAC_NHAN_LAN = 3     # so vong "hoi server -> chua toi thi di bu" truoc khi chiu thua
    THEO_LEADER_LECH = 200   # lech hon the so voi leader = pos cua minh da lac, lay theo leader

    def _theo_leader_sua_pos(self):
        """MEMBER trong party TU DI THEO leader - KHONG co goi nao bao, nen `self.pos` lac dan.

        Day la nguon sai pos LON NHAT ma bot khong he biet: server khong echo lenh move cua chinh
        minh (da do: 184 goi gui / 0 goi nhan ve), con viec di theo leader thi client tu lam.
        Hau qua da thay:
          - laochin (party 3, 30/08 23:33) bao `pos=(990,480)` dung rally, `combat=True` lien tuc
            trong khi 3 member cung cho `combat=False` suot -> thuc te no o diem quai, ma duong
            tat "da o rally roi" khien no KHONG BAO GIO di;
          - 4 acc party 6 gui move tu pos cu -> `di chuyen QUA XA (ma 14)` -> dut ket noi ca lu.
        Trong party, vi tri THAT cua member bam theo leader. Client cua leader nam CUNG TIEN TRINH
        nen doc thang duoc - khong can goi nao.
        """
        try:
            # CHI bam MOT LAN, dung luc vua vao doi - y client (`Team.lua:176`, trong `AddMember`).
            # Client KHONG bam lien tuc: sau khi vao doi, member tu di bang chan cua no.
            #
            # Goi moi lan `navigate_to` la DAP toa do THAT cua member. Log 31/08 party 1 (15:17:06):
            # 4 member vua qua cong toi (3970,1210) - toa do DUNG - thi bi ghi de thanh (1237,543)
            # cua leader, lech 2800 don vi; goi ngay sau do gui tu goc sai -> CA 4 bi
            # `SERVER NGAT KET NOI: ma la 47` cung mot giay.
            if not getattr(self, "_cho_bam_leader", False):
                return False
            if not self.party_members or not self.party_leader:
                return False
            lead = bytes(self.party_leader)
            if self.self_entity is None or lead == bytes(self.self_entity):
                return False                      # chinh minh la leader -> pos cua minh la that
            with _PARTY_LOCK:
                peer = _PARTY_CLIENTS.get(self.party_idx, {}).get(lead)
            if peer is None or not getattr(peer, "running", False):
                return False
            lp = getattr(peer, "pos", None)
            if not lp or getattr(peer, "current_map", None) != self.current_map:
                return False
            if self.pos and ((self.pos[0] - lp[0]) ** 2 + (self.pos[1] - lp[1]) ** 2
                             <= self.THEO_LEADER_LECH ** 2):
                return False
            log.info("[%s] pos lac khi di theo leader: %s -> lay theo leader %s",
                     self._label, self.pos, tuple(lp))
            self.pos = (int(lp[0]), int(lp[1]))
            self._position_generation += 1
            self._pos_valid_for_map = self.current_map
            self._cho_bam_leader = False      # da bam roi - lan sau tu di bang chan cua minh
            return True
        except Exception as e:
            log.debug("[%s] sua pos theo leader loi (bo qua): %s", self._label, e)
            return False

    def navigate_to(self, x: int, y: int, moves_needed: int = None, step: float = 1.5,
                    max_iter: int = 80, flee: bool = True, abort=None,
                    require_smart_path: bool = False):
        """Di chuyen toi (x,y) tren map thuong; dinh battle giua duong -> flee=True thi BO CHAY,
        flee=False thi DANH (party da du -> keo ra spot phai danh bat chap, khong flee).
        game DI TUNG BUOC (move_to chi tien 1 doan ngan moi lan) -> diem XA can NHIEU buoc.
        moves_needed=None -> tu tinh theo KHOANG CACH (tu self.pos): ~100px/buoc, clamp [4, 30].
        Dung in_combat nguong NGAN (1.5s) - du danh hay flee deu cho HET TRAN roi di buoc tiep."""
        import math
        def _split_segment(a, b, max_len):
            ax, ay = a; bx, by = b
            dist = math.hypot(bx - ax, by - ay)
            n = max(1, int(math.ceil(dist / max_len)))
            return [
                (round(ax + (bx - ax) * i / n), round(ay + (by - ay) * i / n))
                for i in range(1, n + 1)
            ]

        # MEMBER TRONG PARTY TU DI THEO LEADER (client-side, KHONG co goi nao) -> `self.pos` cua
        # member lac ngay khi leader di chuyen. Sua TRUOC KHI tinh duong, khong thi:
        #   - smart path tinh tu diem xuat phat SAI -> di xuyen bay quai / sai huong,
        #   - lenh move dau tien nhay ca nghin don vi -> `di chuyen QUA XA (ma 14)` -> dut ket noi.
        self._theo_leader_sua_pos()
        # BI KEO SANG SCENE MOI (server day, khong tu qua cong) -> chua ai chay scene_resume().
        # Phai chay TRUOC lenh move dau tien, khong thi server tu choi "di chuyen QUA XA (ma 14)"
        # va DUT KET NOI (log 18:29: ca 4 member bi keo qua cong roi rot cung mot giay).
        # Chan khi DANG DANH: scene_resume() gui 0x14 06, ma gui goi do luc server con giai tran
        # thi server DONG KET NOI (bai hoc cu, xem _gate_wait_clear).
        # Co qua 60s thi BO: luc do da di lai binh thuong roi, gui `0x14 06` chi them rui ro.
        if (getattr(self, "_need_scene_resume", False)
                and time.time() - getattr(self, "_need_scene_resume_at", 0.0) < 60.0
                and not self.in_combat(idle_secs=1.5)):
            log.info("[%s] bi keo sang map %s (khong tu qua cong) -> chay scene_resume truoc khi di",
                     self._label, self.current_map)
            self.scene_resume()
            # ... VA XIN LAI TOA DO THAT. scene_resume chi bao server "toi vao scene xong", KHONG
            # tra vi tri. self.pos van la dead-reckoning cua map CU / cho leader dung luc keo, ma
            # server dat member canh leader O THOI DIEM KEO - leader thi da di tiep. Gui move theo
            # pos sai = server thay minh nhay ca nghin don vi -> "di chuyen QUA XA (ma 14)" + DUT
            # KET NOI. Log that 27/08 12:24:11: ca 4 member vua bi keo sang 12831, tinh duong tu
            # (1310,2410) roi rot CUNG MOT GIAY, leader luc do dang o (1180,480).
            if self.current_map is not None:
                try:
                    self.refresh_server_position(self.current_map)
                except Exception as e:
                    log.debug("[%s] xin lai toa do sau khi bi keo loi (bo qua): %s", self._label, e)

        # VUA QUA CONG -> _enter_gate dat self.pos = None ("vi tri cu vo nghia o map moi").
        # Ma smart path (Ground.mmg) CAN pos xuat phat -> pos=None thi rot xuong che do GUI MOVE MU,
        # so lenh clamp toi 30 -> mot chang ngan cung ton ~50s (log that 12:16: "da toi diem
        # (590,490) sau 30 lenh move", trong khi cung chang do co smart path chi 3 move-point).
        # -> Xin lai toa do that tu server (0x0c) TRUOC, roi moi tinh duong.
        if self.pos is None and self.current_map is not None:
            try:
                self.refresh_server_position(self.current_map)
            except Exception as e:
                log.debug("[%s] navigate_to: xin lai toa do loi (bo qua): %s", self._label, e)

        targets = [(x, y)]
        using_smart_path = False
        store = _ground_store() if self.pos and self.current_map is not None else None
        if store is not None:
            smart = store.find_world_path(self.current_map, self.pos, (x, y))
            if smart:
                if require_smart_path and math.hypot(smart[-1][0] - x, smart[-1][1] - y) > 5:
                    log.warning("[%s] SAFE NAV tu choi (%d,%d): dich nam trong/vuong collision, "
                                "Ground chi toi duoc %s", self._label, x, y, smart[-1])
                    return False
                using_smart_path = True
                segment = max(20.0, float(getattr(config, "SMART_PATH_SEGMENT", 100)))
                targets = []
                prev = self.pos
                for point in smart[1:] or [smart[-1]]:
                    targets.extend(_split_segment(prev, point, segment))
                    prev = point
                step = min(step, float(getattr(config, "SMART_PATH_STEP_WAIT", step)))
                log.info("[%s] smart path map %s: %s -> (%d,%d), %d waypoint, %d move-point",
                         self._label, self.current_map, self.pos, x, y, len(smart) - 1, len(targets))
        # Lenh tu MAP UI la dau vao tuy y cua nguoi dung. Neu Ground cua map khong ton tai,
        # dich bi tuong chan hoac khong co component noi toi thi TU CHOI TUYET DOI; khong duoc
        # roi xuong nhanh chia duong thang vi co the cat qua collision va bi anti-cheat ghi nhan.
        if require_smart_path and not using_smart_path:
            log.warning("[%s] SAFE NAV tu choi (%d,%d): map=%s pos=%s khong co Ground path",
                        self._label, x, y, self.current_map, self.pos)
            return False
        # CHAN CUOI: khong smart path (khong co Ground.mmg / khong tim ra duong) thi VAN phai chia
        # doan neu biet minh dang o dau. Truoc day nhanh di mu ban THANG mot lenh toi dich - dich
        # xa la mot lenh nhay ca nghin don vi -> `di chuyen QUA XA (ma 14)` -> DUT KET NOI, roi
        # relogin dinh chan toc do dang nhap (ma 90) -> ca party dung cho leader hang tieng.
        # (P43/P44/P45 sang 06/09: 58 lan ma 14, lan nao cung la mot lenh move nhay ~2100 don vi
        #  ngay sau khi cap ben map 18000.)
        # Loi tim duong thi cung chi duoc phep "di sai", KHONG duoc phep lam rot game.
        if not using_smart_path and self.pos:
            _seg = max(20.0, float(getattr(config, "SMART_PATH_SEGMENT", 100)))
            if math.hypot(x - self.pos[0], y - self.pos[1]) > _seg:
                targets = _split_segment(self.pos, (x, y), _seg)
                log.info("[%s] khong co smart path map %s: %s -> (%d,%d) -> chia %d buoc "
                         "(khong ban mot lenh nhay xa = ma 14)",
                         self._label, self.current_map, self.pos, x, y, len(targets))
        self.flee_mode = flee
        moves = attempts = 0
        previous = self.pos
        # DOI MAP GIUA CHUNG -> HUY LUON CHUYEN DI. Dich (x,y) la toa do cua MAP CU; o map moi no
        # la mot cho hoan toan khac, gui move toi do = nhay ca nghin don vi -> `ma 14`.
        #
        # Client lam dung the (`_lua_dec/Logic/SceneManager.lua:468-474`):
        #   if Role.player ~= nil and stopMove then Role.player:StopMove() end
        #   if CGTimer.ContainsListener(MoveController.SendRolePosition) and stopMove then
        #     CGTimer.RemoveListener(MoveController.SendRolePosition) end
        # tuc HUY dich dang di VA go luon bo gui move.
        #
        # Log 31/08 party 1 (14:05:42-45): minhminhmq dang chay `ve diem tap ket (3170,530)` cua
        # map 21814 thi `bi keo sang map 12003 (khong tu qua cong)`; navigate_to van di tiep ->
        # `SERVER NGAT KET NOI: di chuyen QUA XA (ma 14)` -> relogin ra 12003 -> ca party lech map
        # -> reform, keo nhau ve thanh. Mot lan bi keo map lam hong ca vong train.
        _map_bat_dau = self.current_map

        def _da_doi_map():
            if _map_bat_dau is None or self.current_map is None:
                return False
            if self.current_map == _map_bat_dau:
                return False
            log.warning("[%s] navigate_to: DOI MAP giua chung (%s -> %s) -> HUY chuyen di toi "
                        "(%d,%d) (dich thuoc map cu)",
                        self._label, _map_bat_dau, self.current_map, x, y)
            return True
        # Moc de biet co NGUON NAO sua pos trong luc di khong (0x03 / S:007-000 / S:012-000 /
        # S:013-004 / bam theo leader). Khong doi = khong co gi de xac nhan -> dung ngoi cho.
        _gen_truoc_khi_di = self._position_generation
        _co_tran_giua_duong = False   # co dinh tran trong luc di khong (lenh move bi nuot)
        for waypoint_index, (wx, wy) in enumerate(targets):
            if moves_needed is not None and len(targets) == 1:
                waypoint_moves = moves_needed
            elif using_smart_path:
                waypoint_moves = 1
            elif previous:
                distance = math.hypot(wx - previous[0], wy - previous[1])
                waypoint_moves = max(4, min(30, int(distance / 100) + 2))
            else:
                waypoint_moves = 30
            sent = 0
            _cho_tran = 0.0
            while attempts < max_iter and sent < waypoint_moves:
                if not self.running:
                    return False
                if abort and abort():
                    log.info("[%s] navigate_to: abort (reform moi/stop) -> dung", self._label)
                    return False
                if _da_doi_map():
                    return False
                # DUNG TRAN GIUA DUONG: DUNG YEN CHO, KHONG TINH VAO NGAN SACH BUOC DI.
                #
                # Client lam dung the - da soi `_lua_dec` (31/08):
                #   MoveController:Update()        -> `if kind == Player and FightField.isInBattle
                #                                       then return end`  (khong tien, pos DUNG IM)
                #   MoveController.SendRolePosition-> `if FightField.isInBattle ... then return end`
                #                                     (khong gui `C:006-001`)
                # `targetPosition` KHONG bi xoa va timer KHONG bi go (chi go khi het diem / doi
                # scene / vao nha / reconnect) -> het tran la di tiep toi DUNG dich cu.
                #
                # Truoc day `attempts += 1` nam TREN cho nay nen moi vong cho tran cung an mot suat
                # trong `max_iter`: tran dai la het ngan sach, thoat vong khi CHUA gui du buoc ->
                # dung giua duong ma van tuong da di xong (user 31/08: "dang chay ma gap battle thi
                # no ko den duoc diem muon den").
                if self.in_combat(idle_secs=1.0):
                    _co_tran_giua_duong = True   # lenh move bi tran NUOT -> khong duoc tu nhan da toi
                    if _cho_tran > self.NAV_CHO_TRAN_CAP:
                        log.warning("[%s] navigate_to: ket trong tran qua %.0fs -> bo chang nay",
                                    self._label, _cho_tran)
                        break
                    time.sleep(0.5)
                    _cho_tran += 0.5
                    continue
                _cho_tran = 0.0
                attempts += 1
                self.move_to(int(wx), int(wy))
                sent += 1
                moves += 1
                time.sleep(step)
            previous = (wx, wy)
            if attempts >= max_iter and waypoint_index < len(targets) - 1:
                log.warning("[%s] smart path dung som do cham max_iter=%d", self._label, max_iter)
                break
        # ==== XAC NHAN, khong tu nhan la da toi ====
        # Truoc day chi co `self.pos = (x, y)` roi in "da toi diem" - tu GAN cho minh cai dich roi
        # bao la da toi. Do la BIA: lenh move co the bi tran chien nuot, bi dia hinh chan, hoac het
        # `waypoint_moves` khi con dang giua duong.
        #
        # NHUNG KHONG CO CACH HOI VI TRI: `0x0c 0100` la `C:012-001 <換場景完畢>` - goi THONG BAO,
        # server khong tra loi. Goi no o day chi ngoi cho HET GIO 2s MOI LAN di chuyen roi van in
        # "(chua xac nhan duoc)" - phi thoi gian cua moi acc ma khong duoc gi.
        # -> Chi xac nhan khi THUC SU co nguon sua pos ve trong luc di (`_position_generation` doi):
        #    `0x03` self-spawn / `S:007-000` / `S:012-000` / `S:013-004` / bam theo leader.
        #    Khong co thi thoi, KHONG cho.
        for _vong in range(1, self.NAV_XAC_NHAN_LAN + 1):
            if _da_doi_map():
                return False
            _that = None
            try:
                if self._position_generation != _gen_truoc_khi_di:
                    _that = self.pos
            except Exception as e:
                log.debug("[%s] xac nhan toa do loi (bo qua): %s", self._label, e)
            if _that is None:
                # Khong co nguon nao sua pos -> khong khang dinh duoc gi.
                #
                # DINH TRAN GIUA DUONG thi CHAC CHAN chua toi: vao tran la lenh move bi NUOT, acc
                # dung nguyen cho cu. Truoc day nhanh nay van `self.pos = (x, y)` + "da toi diem"
                # + `return True` - tu gan cho minh cai dich roi bao la toi, dung cai ma chu thich
                # ngay tren no da cam ("XAC NHAN, khong tu nhan la da toi").
                #
                # Hau qua user chi ra 08/09: "dang di chuyen ma vao battle thi no bi mat di chuyen
                # nhung m van tinh la di chuyen den safe ok, thanh ra no chua o safe va bi quai
                # danh, con may thi nhan biet ngu" - roi bot bao "SAFE BI QUAI DANH ... diem safe
                # nay co the hoc SAI" (map 21814: 32 lan), user login vao kiem thi safe HOAN TOAN
                # BINH THUONG. Canh bao sai vi `pos` la so bot TU CONG, khong phai vi tri that.
                if _co_tran_giua_duong:
                    log.warning("[%s] navigate_to (%d,%d): dinh TRAN giua duong va server chua xac "
                                "nhan toa do -> KHONG nhan la da toi (dang o %s)",
                                self._label, x, y, self.pos)
                    return False
                self.pos = (x, y)
                log.info("[%s] da toi diem (%d,%d) sau %d lenh move", self._label, x, y, moves)
                return True
            _gen_truoc_khi_di = self._position_generation   # da tinh den lan sua nay
            _cach = math.hypot(_that[0] - x, _that[1] - y)
            if _cach <= self.NAV_TOI_NOI:
                log.info("[%s] da toi diem (%d,%d) sau %d lenh move (xac nhan %s)",
                         self._label, x, y, moves, _that)
                return True
            log.warning("[%s] TUONG da toi (%d,%d) nhung SERVER noi dang o %s (cach %d) -> di tiep "
                        "(lan %d/%d)", self._label, x, y, _that, int(_cach),
                        _vong, self.NAV_XAC_NHAN_LAN)
            if _vong >= self.NAV_XAC_NHAN_LAN:
                break
            _them = 0
            _cho_bu = 0.0
            while _them < 8 and self.running:
                if abort and abort():
                    log.info("[%s] navigate_to: abort khi di bu -> dung", self._label)
                    return False
                if _da_doi_map():
                    return False
                if self.in_combat(idle_secs=1.0):
                    if _cho_bu > self.NAV_CHO_TRAN_CAP:
                        log.warning("[%s] navigate_to: ket trong tran qua %.0fs luc di bu -> thoi",
                                    self._label, _cho_bu)
                        break
                    time.sleep(0.5)
                    _cho_bu += 0.5
                    continue
                _cho_bu = 0.0
                self.move_to(int(x), int(y))
                _them += 1
                moves += 1
                time.sleep(step)
        log.warning("[%s] KHONG toi duoc (%d,%d) sau %d lenh move - server noi dang o %s",
                    self._label, x, y, moves, self.pos)
        return False

    def follow_path(self, waypoints, step: float = 1.0, flee: bool = True, abort=None):
        """Di bo theo CHUOI WAYPOINT (capture duong di THAT trong map) toi diem quai xa.
        Moi waypoint move_to + cho HET TRAN roi di tiep.
        flee=True: ne quai (di nhanh, khong ton SP). flee=False: party DU NGUOI -> DANH quai gap
        tren duong (flee party-battle hay bi TREO -> ca party chet, nen co party thi danh thang hon).
        Dung khi navigate thang KHONG toi duoc (dia hinh/cap khoang cach). Replay tung buoc nho."""
        if not waypoints:
            return
        self.flee_mode = bool(flee)
        log.info("[%s] follow_path: %d waypoint -> (%s) [%s]", self._label, len(waypoints),
                 waypoints[-1], "FLEE" if flee else "DANH")
        for wx, wy in waypoints:
            if not self.running:
                return
            if abort and abort():   # reform moi / stop -> DUNG kéo NGAY (de keepalive xu reform/ve thanh)
                log.info("[%s] follow_path: abort (reform moi/stop) -> dung", self._label)
                return
            # CHO THOAT TRAN HOAN TOAN (flee xong) TRUOC khi di tiep - KHONG move giua battle
            # (move giua tran pha luot flee). idle_secs cao de khong nham battle co khoang nghi.
            t0 = time.time()
            while self.in_combat(idle_secs=1.0):   # in_battle chuan -> 1s sau END la di (de lau bi quai danh)
                if not self.running or time.time() - t0 > 60 or (abort and abort()):
                    break
                time.sleep(0.5)
            self.move_to(int(wx), int(wy))
            time.sleep(step)
        self.pos = tuple(waypoints[-1])
        log.info("[%s] follow_path xong -> %s", self._label, self.pos)

    def in_di_gioi(self) -> bool:
        """Dang o map Di Gioi? Doc map_id thuc te (khong dua vao so kenh)."""
        return self.current_map == config.DIGIOI_MAP_ID

    def _left_di_gioi(self) -> bool:
        """Da ra khoi Di Gioi chua (map_id da khac Di Gioi)."""
        return self.current_map is not None and self.current_map != config.DIGIOI_MAP_ID

    DIGIOI_CONG_RA = (270, 210)      # cong thoat Di Gioi (tu capture exit_new.pcap)

    def exit_di_gioi(self, step_wait: float = 2.0):
        """Di Gioi KHONG co lenh thoat: phai DI BO toi CONG (270,210). Toi cong -> map tu doi.
        Kiem tra thoat bang map_id THAT (khong dua so kenh).

        DUONG DI: uu tien SMART PATH (`navigate_to`, doc Ground.mmg - map 49942 CO du lieu) - no
        tu tim duong tu CHO DANG DUNG, chia doan 100px va cho `SMART_PATH_STEP_WAIT` (0.55s) moi
        buoc.
        Chuoi 7 buoc CO DINH replay tu capture chi con la DU PHONG (khong co Ground.mmg / smart
        path that bai): no xuat phat tu (738,648) nen dung o cho khac la di lung tung, va
        `step_wait` 2.0s/buoc = 14 giay chi de di 1 doan ngan (user 01/09: "dang nhich 1 ty roi
        dung yen 1 luc roi nhich 1 ty").
        """
        self._dg_pursuit_paused = True
        self.stop_run_around()
        _cx, _cy = self.DIGIOI_CONG_RA
        # BAT FLEE SUOT DUONG RA: dang tren duong THOAT thi dung lai danh tung bay quai la vo
        # nghia - moi tran ~30s, DG quai day dac. Truoc day ham nay khong he dat `flee_mode`, chi
        # `_wait_combat_clear` (= dung yen DANH cho xong) nen bot vua di vua danh (user 01/09:
        # "chon che do ve thanh tap trung -> login vao dang trong DG -> di chuyen ra ngoai thi no
        # phai o che do flee chu, sao no van danh").
        # `navigate_to(flee=True)` cung dat co nay, nhung phai dat o DAY de phu ca nhanh du phong
        # va cac khoang giua cac vong lap.
        self.flee_mode = True
        _lan = 0
        _t0 = time.time()
        # KHONG BO CUOC sau vai vong (user chot 01/09: "phai di ra bang duoc"). Ket lai trong DG
        # la ket VINH VIEN: DG co gio, day quai, va khong luong nao khac keo acc ra ho.
        # Chi dung khi: DA RA, hoac acc bi STOP / rot ket noi.
        while self.running:
            _lan += 1
            _smart = False
            try:
                _smart = bool(self.navigate_to(_cx, _cy, flee=True))
            except Exception as e:
                log.debug("[%s] Thoat Di Gioi: smart path loi (dung chuoi buoc cu): %s",
                          self._label, e)
            if _smart:
                log.info("[%s] Thoat Di Gioi: da toi cong (%d,%d) bang smart path (lan %d)",
                         self._label, _cx, _cy, _lan)
            else:
                log.info("[%s] Thoat Di Gioi: smart path khong toi -> di bo chuoi buoc capture "
                         "(lan %d)", self._label, _lan)
                self._di_bo_chuoi_buoc_ra_cong(step_wait)
            self.send(0x14, bytes.fromhex("08000100")); time.sleep(0.8)
            self.send(0x0c, bytes.fromhex("0100"));     time.sleep(0.5)
            self.send(0x14, bytes.fromhex("0600"));     time.sleep(1.5)
            if self._left_di_gioi():
                log.info("[%s] Da THOAT Di Gioi -> map %s (sau %d lan, %.0fs)",
                         self._label, self.current_map, _lan, time.time() - _t0)
                return True
            # CHUA RA: pos dang nho co the sai (dead-reckoning lech / spawn ngoai vung di duoc) ->
            # bam ve o di duoc roi thu lai, thay vi lap y het mot cach vo vong.
            _bam = self._bam_o_di_duoc(self.pos or (_cx, _cy), (_cx, _cy))
            if _bam and tuple(_bam) != tuple(self.pos or ()):
                log.info("[%s] Thoat Di Gioi: pos %s ngoai vung di duoc -> bam ve %s roi thu lai",
                         self._label, self.pos, tuple(_bam))
                self.pos = (int(_bam[0]), int(_bam[1]))
            if _lan % 5 == 0:
                log.warning("[%s] Thoat Di Gioi: da thu %d lan trong %.0fs ma CHUA RA (map=%s, "
                            "pos=%s) - van thu tiep, khong bo cuoc",
                            self._label, _lan, time.time() - _t0, self.current_map, self.pos)
            time.sleep(1.0)
        log.warning("[%s] Thoat Di Gioi: DUNG vi acc da stop/rot (map %s)",
                    self._label, self.current_map)
        return False

    def _di_bo_chuoi_buoc_ra_cong(self, step_wait: float = 2.0):
        """DU PHONG: replay chuoi buoc THAT tu capture (exit_new.pcap). Chi dung khi smart path
        khong dung duoc - chuoi nay xuat phat tu (738,648) nen dung cho khac la di lung tung."""
        steps = [(738, 648), (682, 609), (625, 569), (570, 530),
                 (462, 411), (417, 360), (390, 330)]
        for x, y in steps:
            # DANG TRAN THI KHONG MOVE - client cung the (`MoveController:Update` va
            # `SendRolePosition` deu `return` ngay khi `FightField.isInBattle`): gui giua tran thi
            # server nuot ma `self.pos` van nhay theo -> lech het chuoi, di mai khong toi cong.
            self._wait_combat_clear(idle=2.0, cap=120.0)
            self.move_to(x, y)
            time.sleep(step_wait)
        self.send(0x14, bytes.fromhex("04000100")); time.sleep(0.8)
        self._wait_combat_clear(idle=2.0, cap=120.0)
        self.move_to(*self.DIGIOI_CONG_RA);         time.sleep(step_wait)

    def sync_area_combat_mode(self, allow_pursuit=True):
        """DG uses existing search movement; other maps use normal auto with no roaming."""
        if not self.running or self.current_map is None:
            return
        in_dg = self.in_di_gioi()
        was_dg = bool(getattr(self, "_area_was_dg", False))
        self._area_was_dg = in_dg
        if not in_dg:
            self._dg_pursuit_paused = False
        blocked = (not allow_pursuit or self._dg_pursuit_paused
                   or getattr(self, "_daily_use_selected_pet", False)
                   or getattr(self, "_daily_hold", False)
                   or getattr(self, "_individual_safe_logout", False))
        # A party member follows its leader; never issue independent movement commands.
        follower = bool(self.party_leader and self.party_leader != self.self_entity)
        if was_dg and not in_dg and not blocked:
            self.flee_mode = False
            self._ui_auto_battle = True
        mode = "pursuit" if in_dg and not blocked else "normal"
        changed = mode != self._area_combat_mode
        self._area_combat_mode = mode
        if mode == "normal" or follower:
            if self._running_route:
                self.stop_run_around()
        elif self.has_hp_and_sp_items():
            self.flee_mode = False
            self.start_run_around()
        else:
            self.stop_run_around()  # Preserve the existing no-potions safety guard.
        if changed:
            log.info("[%s] Chế độ đánh: %s", self._label,
                     "Truy kích Dị giới" if mode == "pursuit" else "Auto bình thường")
            if not blocked and not self.in_combat() and not self.flee_mode:
                self.combat_ready()

    def start_run_around(self, stay_in_di_gioi=True):
        """Only search inside confirmed DG, never move a party follower independently."""
        if (not self.in_di_gioi() or self._dg_pursuit_paused
                or getattr(self, "_daily_use_selected_pet", False)
                or getattr(self, "_daily_hold", False)
                or getattr(self, "_individual_safe_logout", False)
                or (self.party_leader and self.party_leader != self.self_entity)):
            return
        with self._run_around_lock:
            if self._running_route:
                return
            self._run_around_generation += 1
            generation = self._run_around_generation
            self._running_route = True
            threading.Thread(target=self._run_around_loop, args=(True, generation),
                             name="dg-pursuit-%s" % self._username, daemon=True).start()

    def stop_run_around(self):
        with self._run_around_lock:
            self._running_route = False
            self._run_around_generation += 1

    def _bam_o_di_duoc(self, diem, tu_diem):
        """Bam `diem` ve O DI DUOC gan nhat theo Ground.mmg (den duoc tu `tu_diem`).

        None khi khong co du lieu dia hinh cho map hien tai -> caller giu nguyen diem cu (hanh vi
        nhu truoc, khong lam te hon).
        """
        try:
            st = _ground_store()
            if st is None or self.current_map is None:
                return None
            return st.nearest_walkable_world(self.current_map, tuple(diem), tuple(tu_diem))
        except Exception as e:
            log.debug("[%s] bam o di duoc loi (bo qua): %s", self._label, e)
            return None

    def _run_around_loop(self, stay_in_di_gioi=True, generation=None):
        if generation is None:
            generation = self._run_around_generation
        map_id = self.current_map
        def cancelled():
            return (not self.running or not self._running_route
                    or generation != self._run_around_generation
                    or self.current_map != map_id or not self.in_di_gioi()
                    or self._dg_pursuit_paused or self.flee_mode
                    or getattr(self, "_daily_use_selected_pet", False)
                    or getattr(self, "_daily_hold", False)
                    or getattr(self, "_individual_safe_logout", False)
                    or bool(self.party_leader and self.party_leader != self.self_entity))
        try:
            anchor = self._di_gioi_anchor or self.pos
            if not anchor or not self.pos:
                return  # No invented coordinates when server position is unknown.
            anchor = self._bam_o_di_duoc(anchor, self.pos)
            if anchor is None:
                log.warning("[%s] Truy kích: chưa có Ground xác minh; đứng tự đánh", self._label)
                return
            i = 0
            while not cancelled():
                if self.in_combat(getattr(config, "RUN_RESUME_IDLE", 2.0)):
                    self._soi_luot_cham();time.sleep(0.3);continue
                offsets = getattr(config, "RUN_AROUND_OFFSETS", []) or []
                if not offsets:
                    return
                dx, dy = offsets[i % len(offsets)]
                target = self._bam_o_di_duoc((anchor[0]+dx, anchor[1]+dy), self.pos)
                if target is None:
                    time.sleep(1);continue
                ok = self.navigate_to(int(target[0]), int(target[1]), flee=False,
                                      abort=cancelled, require_smart_path=True)
                if ok:
                    i += 1
                time.sleep(max(.3, float(getattr(config, "RUN_STEP_WAIT", .8))))
        except Exception as exc:
            log.warning("[%s] Truy kích dừng an toàn: %s", self._label, exc)
        finally:
            with self._run_around_lock:
                if generation == self._run_around_generation:
                    self._running_route = False

    # Cap quai Di Gioi: idx 1..15 -> [10,25,40,55,70,85,100,110,120,130,140,150,160,170,180].
    # Gói C2S 0x61 02 00 [idx] (capture digioi_level_select_20260721.pcap). Bot cu vao co dinh idx=2
    # (cap 25). self.di_gioi_level do run_party set tu config (mac dinh 2).
    DI_GIOI_LEVELS = [10, 25, 40, 55, 70, 85, 100, 110, 120, 130, 140, 150, 160, 170, 180]

    def set_di_gioi_level(self, idx: int) -> bool:
        """Doi CAP QUAI Di Gioi LIVE (dang o trong DG van doi duoc, khong can vao lai) - C2S
        0x61 02 00 [idx], idx=1..15. Xac nhan capture 21/07: sau khi vao DG, gui lien tiep
        0x61 02 00 XX doi cap ngay tai cho."""
        idx = max(1, min(int(idx), len(self.DI_GIOI_LEVELS)))
        self.di_gioi_level = idx
        self.send(0x61, bytes([0x02, 0x00, idx & 0xFF]))
        log.info("[%s] Di Gioi: doi cap quai -> idx=%d (cap %d)",
                 self._label, idx, self.DI_GIOI_LEVELS[idx - 1])
        return True

    def enter_di_gioi(self):
        """Vao map Di Gioi (map train chinh): 0x61 010001 -> 0x61 02 00 [level_idx].
        level_idx = self.di_gioi_level (mac dinh 2 = cap 25). LUU Y: KHONG vao duoc khi dang trong party."""
        # PHAI THOAT TO DOI TRUOC - y het `teleport()`. Client chan thang o
        # `UITeleport.OnClick_LimitFightArea` (`_lua_dec/UI/UITeleport.lua:501`), khong gui goi nao:
        #     if not Team.IsAlone(Role.playerId) then ShowCenterMessage(...); return true; end
        # va neu van gui thi server tra `S:097-001` ma 5 <組隊中>. Cai comment "KHONG vao duoc khi
        # dang trong party" o tren da co tu lau ma khong ai lam - hau qua la ca party 17 (07/09)
        # ket cheo: chutam tuong minh "xong DG" nen dung cho, leader bat ca party ve Tuong Duong,
        # 3 acc con lai dang danh trong DG va con 1h20m -> leader EP RELOGIN ca party.
        self._dg_pursuit_paused = False
        self._dg_enter_result = None
        self._dg_enter_event.clear()
        if self.party_members:
            log.info("[%s] Vao Di Gioi: dang o to doi (%d member) -> ROI DOI truoc (client chan "
                     "vao DG khi con doi)", self._label, len(self.party_members))
            try:
                self.leave_party()
            except Exception as e:
                log.warning("[%s] Vao Di Gioi: roi doi loi: %s", self._label, e)
        self.send(0x61, bytes.fromhex("010001"))   # mo/load zone Di Gioi
        log.info("[%s] Vao Di Gioi: gui 0x61 010001", self._label)
        time.sleep(1.5)                              # cho server load zone
        idx = max(1, min(int(getattr(self, "di_gioi_level", 2)), len(self.DI_GIOI_LEVELS)))
        self.send(0x61, bytes([0x02, 0x00, idx & 0xFF]))   # xac nhan vao + chon cap quai
        # spawn Di Gioi co dinh -> set pos (server khong echo, dung dead-reckoning tu day)
        self.pos = getattr(config, "RUN_FALLBACK_ANCHOR", (870, 740))
        self._di_gioi_anchor = self.pos   # CHOT tam run-around = diem tele vao (co dinh, ne rìa map sau relogin)
        log.info("[%s] Vao Di Gioi: gui 0x61 02 00 %02x (cap %d), spawn pos=%s",
                 self._label, idx, self.DI_GIOI_LEVELS[idx - 1], self.pos)

    # Cho server tra loi S:097-001 truoc khi ban lai. Luc 250 acc cung login, server tra loi cham
    # toi ~25s (do that 15/09: [trumuoi] gui luc 00:31:40, server DONG Y luc 00:32:06).
    DG_CHO_TRA_LOI_SEC = 30.0
    DG_GIAN_KHI_IM_SEC = 15.0      # server im hoan toan -> gian ra, KHONG ban day

    def enter_di_gioi_safe(self, tries: int = 12, wait: float = 3.0) -> bool:
        """Vao DI GIOI: gui 0x61 roi CHO `S:097-001` tra loi, khong ban lai mu.

        Ne 2 case fail truoc khi gui:
          - current_map=None  -> CHUA vao world xong (login chua xong) -> cho.
          - in_combat()       -> dang KET BATTLE (login ngay bai quai) -> cho het tran.

        VI SAO PHAI CHO (sua 15/09): ban cu ngu `wait` giay roi ban tiep bat ke server da dap chua
        -> moi acc ban 24 goi thay vi 1. Binh thuong server ranh nen khong ai thay; hom 15/09 bo
        cua hoan viec vat lam 40 acc cay pho ban don dung luc 65 acc dang vao DG, server cham lai
        va khuyet tat nay TU KHUECH DAI: cang cham cang ban, cang ban cang cham.
            dot 10:14-10:18 (server ranh) : 68 acc, 349 goi,  1 acc that bai
            dot 00:29-00:32 (server tai)  : 65 acc, 629 goi, 26 acc that bai
        Ca party.log truoc do chi 2 lan `VAO DI GIOI THAT BAI`; rieng 6 phut do la 264 lan.
        """
        for i in range(tries):
            if not self.running:        # bi STOP (GUI/close) -> thoat ngay
                return False
            if self.in_di_gioi():
                return True
            if self.current_map is None:
                log.info("[%s] cho vao world xong (map chua co)... (%d)", self._label, i + 1)
                time.sleep(wait); continue
            if self.in_combat():
                log.info("[%s] dang ket battle -> cho het tran roi vao DG... (%d)", self._label, i + 1)
                time.sleep(wait); continue
            self.enter_di_gioi()        # ham nay da `_dg_enter_event.clear()` truoc khi gui
            if not self._dg_enter_event.wait(self.DG_CHO_TRA_LOI_SEC):
                # Server IM - khong ket luan gi (co the dang nghen). Gian ra roi thu lai, TUYET DOI
                # khong ban them trong luc cho: chinh viec ban them lam server nghen nang hon.
                log.warning("[%s] Vao Di Gioi: server CHUA tra loi sau %.0fs -> gian %.0fs roi thu "
                            "lai (%d/%d)", self._label, self.DG_CHO_TRA_LOI_SEC,
                            self.DG_GIAN_KHI_IM_SEC, i + 1, tries)
                time.sleep(self.DG_GIAN_KHI_IM_SEC)
                continue
            # Server DA NOI ly do (S:097-001) -> xu ly dung theo ma, khong ban lai mu.
            ma = self._dg_enter_result
            if ma in (0, 6):
                # 0 = dong y, 6 = da o trong DG roi. Ca hai deu chi con cho server doi map.
                for _ in range(int(wait * 4)):
                    if self.in_di_gioi():
                        log.info("[%s] da VAO DI GIOI (map=%s)", self._label, self.current_map)
                        return True
                    if not self.running:
                        return False
                    time.sleep(0.5)
                log.info("[%s] Vao Di Gioi: server bao OK (ma %d) nhung map chua doi -> cho tiep",
                         self._label, ma)
                continue
            #   1 cap khong du  -> acc nay khong bao gio vao duoc, ban lai la vo ich
            #   2 het gio       -> het that, va la lan DUY NHAT duoc phep ket luan the
            if ma in (1, 2):
                log.warning("[%s] VAO DI GIOI DUNG HAN: %s (S:097-001 ma %d)", self._label,
                            "CAP KHONG DU" if ma == 1 else "HET GIO hom nay", ma)
                return False
            # Con lai (3 dang danh / 4 dang su kien / 5 dang to doi) la TAM THOI -> cho roi thu lai;
            # rieng ma 5 thi `enter_di_gioi()` da roi doi nen lan sau se qua.
            time.sleep(wait)
        # KHONG doan "het gio" nua: server co goi bao ly do, doan bua thi tren dat acc con nguyen
        # 120/120 phut ma bi danh dau la xong DG (ca that 07/09 [chutam] - xem `S:097-001` o
        # `_dispatch`). Khong nhan duoc goi nao thi noi that la khong biet.
        log.warning("[%s] VAO DI GIOI THAT BAI sau %d lan (map=%s, combat=%s, S:097-001=%s)",
                    self._label, tries, self.current_map, self.in_combat(),
                    self._dg_enter_result if self._dg_enter_result is not None else "khong nhan duoc")
        return False

    def go_to_town(self, city_id: int, flag: int = 0, tries: int = 30, wait: float = 2.0,
                   battle_grace: float = 90.0):
        """Teleport ve thanh, LAP LAI cho toi khi RA KHOI map hien tai (neu dang o bai quai/
        battle thi teleport bi chan, phai cho khoang trong giua 2 tran). Xac nhan = map da doi.
        battle_grace: giay CONG THEM vao deadline de cho thoat battle (mac dinh 90). Goi tu reform
        (thanh CHUA MO -> tele khong bao gio duoc) nen truyen battle_grace nho + tries nho de FAIL
        NHANH (~1 phut) roi chuyen sang di bo, thay vi ket 150s/lan (user: 'chi co tele 1p thoi')."""
        city_id = int(city_id)
        flag = int(flag)
        # MOC "DANG CHUYEN MAP". Teleport la thu DUY NHAT lam map doi dot ngot, nen day la cho
        # duy nhat can ghi. Dieu phoi doc moc nay de phan biet HAI thu nhin y het nhau qua tap
        # `maps`: party HONG (moi dua mot noi) va party DANG BAY theo dung lenh cua chinh no.
        # Xem `_dang_chuyen_map` ben run_party_digioi.
        self.chuyen_map_luc = time.time()
        # RA KHOI DI GIOI TRUOC MOI THU KHAC.
        #
        # Hai nhanh `return False` ngay ben duoi ("khong phai thanh teleport" / "thanh CHUA MO")
        # bo cuoc TRUOC khi toi buoc ra DG -> acc KET LAI TRONG DI GIOI vinh vien: DG co gio, day
        # quai, va ke ca doi thanh khac cung khong ai keo no ra.
        # Log 01/09 party 49 (gclm*): xong DG luc 09:41:04 ->
        #   "go_to_town: thanh 12061 CHUA MO tele -> bo qua ngay (khong spam)"
        #   "KHONG co bot-leader -> dung yen tai safe (kenh 4)"
        # roi ca 5 acc dung im o (870,740) TRONG map 49942 tu do (user 01/09: "sao mai no ko chiu
        # di ra ngoai DG"). Ra khoi DG la viec PHAI LAM bat ke ve duoc thanh nao hay khong.
        if self.in_di_gioi():
            log.info("[%s] go_to_town: dang o Di Gioi -> RA KHOI DG truoc (bat ke ve duoc thanh "
                     "%s hay khong)", self._label, city_id)
            self.exit_di_gioi()
        if not getattr(config, "is_teleport_city", lambda _city: True)(city_id):
            log.warning("[%s] go_to_town: %s KHONG phai thanh teleport (co the la map train) -> bo qua",
                        self._label, city_id)
            return False
        # THANH CHUA MO -> teleport KHONG BAO GIO an. Truoc day van lap toi 30 lan (log 15:27:
        # "Teleport -> city 12011" hang chuc lan roi moi bo cuoc). Gio biet truoc thi bo NGAY.
        if self.city_unlocked(city_id) is False:
            log.warning("[%s] go_to_town: thanh %s CHUA MO tele -> bo qua ngay (khong spam)",
                        self._label, city_id)
            return False
        log.info("[%s] Ve thanh %d (lap lai neu con battle chan teleport)...", self._label, city_id)
        # (Buoc "ra khoi Di Gioi" DA chay o DAU HAM - phai truoc ca cac nhanh `return False`.)
        # Dang o TRONG MAP EVENT (thap 2K...) -> teleport cung bi CHAN, y het Di Gioi. Phai DI BO
        # ra bang exit_event truoc. Thieu chot nay thi bot spam teleport toi het deadline:
        # log that party 2 (27/08 11:40): het 2K, chuyen pha train, ca 5 acc dang o map 12932
        # -> "PARTY co acc sai map -> ve thanh don nhau" -> "Teleport -> city 12001" MOI 2 GIAY,
        # khong bao gio ra duoc.
        self._di_bo_ra_khoi_map_event()
        ok = 0
        deadline = time.time() + tries * wait + battle_grace   # +battle_grace du cho thoat battle
        while time.time() < deadline:
            if not self.running:    # STOP / mat ket noi -> NGUNG ngay (khong spam teleport nua)
                log.info("[%s] go_to_town: dung (stop/disconnect)", self._label)
                return False
            # DANG VAO PHO BAN (vua nhan loi moi) -> NGUNG teleport ve thanh, de bot THEO + DANH
            # pho ban (tranh spam teleport + flee do xung dot voi 'city mode keo ve thanh').
            if dang_pha_pho_ban(self.party_idx):
                log.info("[%s] go_to_town: dang vao pho ban -> ngung teleport (theo + danh pho ban)",
                         self._label)
                self.flee_mode = False
                return False
            # DANG O TRONG PHO BAN TO DOI: server CHAN teleport -> gui bao nhieu lan cung vo ich.
            # Pha PB o tren chi phu luc VUA NHAN LOI MOI/dang di vao, khong phu luc DA O TRONG.
            # Bug that (party 5, 01:08-01:09): 4 acc trong map PB 62012 spam "Teleport -> city 12061"
            # MOI GIAY, ca log 1388 lan, cho toi khi het deadline moi bao "Chua ve duoc thanh".
            # Phai danh PB xong (hoac bi day ra) roi moi ve thanh duoc -> tra False cho caller lo.
            if self.in_team_dungeon():
                log.info("[%s] go_to_town: DANG TRONG pho ban to doi (map=%s) -> khong teleport, "
                         "danh xong da", self._label, self.current_map)
                self.flee_mode = False
                return False
            # VAN o DI GIOI: exit_di_gioi() chi chay MOT LAN truoc vong; that bai thi truoc day cu
            # spam teleport toi het deadline (log that: thsau map=49942 spam lien tuc roi moi bao
            # "Chua ve duoc thanh 12061"). Teleport o Di Gioi bi server tu choi -> gui vo ich.
            if self.in_di_gioi():
                log.info("[%s] go_to_town: VAN dang o Di Gioi (map=%s) -> khong teleport, ra cong "
                         "thoat da", self._label, self.current_map)
                self.flee_mode = False
                return False
            # DANG O TRONG BAT KY INSTANCE NAO (ca dai 62xxx): server CHAN teleport -> gui bao
            # nhieu lan cung vo ich, ma con la nguon spam goi -> `ma 13` (gui goi lien tuc qua
            # nhanh) roi dut ket noi.
            #
            # Nhanh `in_team_dungeon()` o tren CHI phu PHO BAN TO DOI (4 map trong
            # TEAM_DUNGEON_MAPS). Pho ban SOLO hang ngay (map 62001) khong nam trong do -> lot
            # qua het.
            # Log 01/09 08:21:20-08:23:02: 4 acc (vuhai, quantam, quanchin, quanmuoi) xong dungeon
            # solo o map 62001, spam "Teleport -> city 12001" MOI 2 GIAY suot hon 100 giay, va
            # truoc do ca 4 deu dinh `SERVER NGAT KET NOI: gui goi lien tuc qua nhanh (ma 13)`.
            if in_instance_map(self.current_map):
                # BIET MA KHONG LAM = ket vinh vien. Truoc day chi log cau nay roi `return False`,
                # khong ai di lam cai viec "ra khoi pho ban truoc" ca.
                #
                # Ca that 10/09 party 50 (user: "p50 bi lam sao"): `dk104` ket trong instance 62001
                # tu 11:39, in dong nay hang chuc lan; bon acc kia dung o map train 18444 doi no.
                # Dieu phoi thi thay "cung map/kenh nhung DOI chua du (dk104=0)" -> lap lai party
                # -> dk104 accept roi lai roi (no dang o instance khac) -> lap suot 15 phut:
                #   11:43:41 [dk104] Loi moi tu THANH VIEN CUNG PARTY -> ACCEPT (da join)
                #   11:44:14 [dk104] Roi/giai tan party cu
                #   11:45:43 ACCEPT ... 11:46:37 roi ... (lap)
                #
                # `leave_team_dungeon()` la dung lenh cua client (`C:047-010 <離開組隊>`), giu
                # nguyen ket noi. No tu bo qua khi khong o trong instance nen goi o day la an toan.
                # HAI LOAI INSTANCE, HAI LENH THOAT KHAC NHAU - va bot BIET minh dang o loai nao:
                #   PB TO DOI (`TEAM_DUNGEON_MAPS`) -> `C:047-010` (nut "Thoat" cua UI to doi)
                #   PB DON / instance khac          -> `C:013-004` mang ID CUA CHINH MINH
                #                                      (`Dungeon.LeaveSinglePlayDungeon`)
                # Gui sai loai thi server im lang, acc o lai instance mai mai.
                #
                # Ca that party 9, 13/09 (user: "P9 bi ket o map khieu chien Dau dau"):
                #   14:09:06 [lbo007] go_to_town: DANG TRONG instance (map=62001) -> THOAT PHO BAN
                #   14:09:06 [lbo007] THOAT PHO BAN TO DOI (C:047-010) - khong relogin
                #   14:09:12 [lbo007] -> gui C:047-010 roi ma 6s chua ra khoi map 62001
                #   ... lap lien tuc, BA acc cung ket, ca party cho o 21001 ...
                # 62001 KHONG nam trong `TEAM_DUNGEON_MAPS` {62002, 62011, 62012, 62013} - no la
                # PB DON. `leave_single_dungeon()` da co san va duong dungeon binh thuong van dung
                # dung no; rieng `go_to_town` (duong dieu phoi keo ve gom, them 10/09) thi tu dau
                # chi biet moi lenh to doi.
                _la_to_doi = int(self.current_map or 0) in TEAM_DUNGEON_MAPS
                log.warning("[%s] go_to_town: DANG TRONG instance (map=%s, %s) -> THOAT roi ve "
                            "thanh", self._label, self.current_map,
                            "PB TO DOI" if _la_to_doi else "PB DON")
                self.flee_mode = False
                try:
                    if _la_to_doi:
                        if self.leave_team_dungeon():
                            continue    # da ra duoc -> vong sau teleport ve thanh binh thuong
                    elif self.leave_single_dungeon():
                        time.sleep(1.0)
                        continue
                except Exception as e:
                    log.warning("[%s] go_to_town: loi thoat pho ban: %s", self._label, e)
                return False
            # DANG BATTLE -> teleport bi chan, spam teleport luc battle PHA luot FLEE -> BAT flee, cho thoat.
            # Moc chinh = state.in_battle (chinh xac: 0x34 START -> True, 0x14 sub0700 END -> False).
            # CU dung in_combat(4.0) time-based: re-aggro <4s thi in_combat LUON True -> ko bao gio toi
            # teleport -> KET battle vinh vien o map quai (flee xong dung yen, bi danh tiep). Gio tran
            # KET THUC (in_battle=False) -> teleport NGAY trong khe ho truoc khi re-aggro. in_combat(1.5)
            # chi la guard nho cho khoảnh khac ngay sau END (in_battle bao ve flee nhieu luot).
            if self.state.in_battle or self.in_combat(idle_secs=1.5):
                self.flee_mode = True
                time.sleep(1.0)
                continue
            self.teleport(city_id, flag)
            # cho 'wait' giay NHUNG van check stop/battle moi 0.2s
            end = time.time() + wait
            while time.time() < end:
                if not self.running:
                    return False
                if self.in_combat(idle_secs=1.5):
                    break   # vao tran giua chung -> ngung cho, quay lai xu ly flee
                time.sleep(0.2)
            if self.current_map == city_id:
                ok += 1
                if ok >= 2:   # 2 lan lien tiep == city_id -> on dinh (tranh nhieu luc chuyen map)
                    log.info("[%s] Da ve thanh %d", self._label, city_id)
                    return True
            else:
                ok = 0
        log.warning("[%s] Chua ve duoc thanh %d (map=%s)", self._label, city_id, self.current_map)
        return False

    def teleport(self, city_id: int, flag: int = 0):
        """flag bat buoc dung dung cho tung thanh (xem cities.json)."""
        city_id = int(city_id)
        flag = int(flag)
        if not getattr(config, "is_teleport_city", lambda _city: True)(city_id):
            log.warning("[%s] teleport: %s KHONG phai thanh teleport -> khong gui lenh",
                        self._label, city_id)
            return False
        # TELE LUON PHAI THOAT TO DOI TRUOC. Client game chan thang o `UITeleport.CheckTeleport()`
        # (`_lua_dec/UI/UITeleport.lua:673`) - dang to doi thi KHONG gui goi nao ca:
        #     if not Team.IsAlone(Role.playerId) then ShowCenterMessage(...); return true; end
        # Bot truoc day gui MU: server im lang bo qua -> vong `go_to_town` ban lai moi 2s toi het
        # deadline. Log 07/09 [dieumot]: 00:04:21 co roi doi truoc -> tele an sau 4s/2 goi;
        # 00:05:05 dang trong doi -> 19 goi trong 40s moi di. Ca log 4758 goi teleport, phan lon
        # la ban lai, va day chinh la nguon spam sinh `ma 13 (gui goi qua nhanh)`.
        # Roi doi o DAY chu khong o `go_to_town`: moi duong tele deu di qua ham nay nen khong sot.
        # Party thieu nguoi thi dieu phoi gom lai - dung L0, va no van lam vay tu truoc
        # (documents/RULE_DIEU_PHOI.md).
        if self.party_members:
            # DIEM NGHEN DUY NHAT. Roi doi de teleport la hanh dong PHA PARTY, va moi duong di deu
            # qua day (chu thich tren: "moi duong tele deu di qua ham nay nen khong sot"). Nen luat
            # "dieu phoi quyet, acc khong tu quyet" dat o DAY - mot cho - thay vi ra soat 40+ cho
            # goi teleport rai rac trong run_party_digioi.
            #
            # Suot 11/09 da phai sua di sua lai tung ca vi khong dat o day: member tu lap duong
            # (party 3), member tu di route (party 6), member tu goi reform vi sai map (party 6),
            # member tu ve thanh giua luc leader keo (party 2/7). Moi lan bit mot cho la lan sau
            # no chay ra o cho khac. User: "deo hieu sao code mai ko xong vu nay, cu thich de acc
            # quyet dinh co".
            #
            # `nguoi_keo` la lenh cua dieu phoi (xem `dat_nguoi_keo`): ten mot acc = chi acc do
            # duoc di; "*" = ai cung duoc (party khong co bot-leader, hoac dang GOM - luc do ca
            # party deu phai tu ve diem hen). Khong co lenh -> KHONG AI duoc pha party.
            _keo = nguoi_keo(self.party_idx) if self.party_idx is not None else "*"
            if _keo not in ("*", self._username):
                log.info("[%s] Teleport toi thanh %s: KHONG roi doi (%d member) - dieu phoi giao "
                         "viec di duong cho %s", self._label, city_id, len(self.party_members),
                         ("'%s'" % _keo) if _keo else "CHUA AI (chua ra lenh)")
                return False
            log.info("[%s] Teleport: dang o to doi (%d member) -> ROI DOI truoc (client chan tele "
                     "khi con doi)", self._label, len(self.party_members))
            try:
                self.leave_party()
            except Exception as e:
                log.warning("[%s] Teleport: roi doi loi: %s", self._label, e)
        payload = b"\x01\x00" + struct.pack("<H", city_id) + bytes([flag])
        # DANH DAU: lan doi map sap toi la DO BOT RA LENH -> KHONG can scene_resume, va gui
        # `0x14 06` o duong nay thi SERVER DONG KET NOI (xem _need_scene_resume).
        self._self_map_change_at = time.time()
        self.send(protocol.OP_TELEPORT, payload)
        log.info("[%s] Teleport -> city %s (flag %s)", self._label, city_id, flag)
        return True

    def _wait_combat_clear(self, idle: float = 1.0, cap: float = 90.0) -> bool:
        """Cho HET TRAN (khong co luot battle trong 'idle' giay) toi 'cap' giay.
        Tra False neu bi STOP/rot. Dung truoc khi move/transit (battle NUOT lenh 0x06/0x14)."""
        # `cap` KHONG duoc no khi con dang danh THAT (state.in_battle theo 0x35/0x34 + 0x14
        # sub0700). Truoc day cap 90s cat giua tran phuc kich o cong -> _enter_gate bo cuoc ->
        # bao "sai map" -> pha party reform lai -> reform tele giua tran -> ket vong BO CHAY
        # (bug that 17:09-17:13). cap chi con de bat cai duoi idle (khong chac chan).
        t0 = time.time()
        while self.in_combat(idle_secs=idle) and self.running:
            if self.state.in_battle:
                t0 = time.time()
            elif time.time() - t0 >= cap:
                break
            time.sleep(0.5)
        # Ra khoi vong nay KHONG co nghia la tran DA XONG THAT: `in_combat()` co duong suy luan
        # theo idle/SAFETY, ma server thi van con dang giai quyet tran. Gui lenh move ngay luc do
        # = `di chuyen QUA XA (ma 14)` -> DUT KET NOI.
        #
        # END da xac nhan van hop le sau khi doc thoai/hoi mau. Khong bat END phai moi
        # trong 2s, vi moi lan _td_walk/navigate_to goi lai se cho thua 8s.
        # Chi tin END sau luot danh gan nhat va khi tracker/metrics khong co tran moi.
        # Truoc day chot nay CHI ap dung cho pho ban to doi du chu thich
        # ghi ro y dinh la "ap dung o day (dung 1 cho) de moi noi goi ham nay deu duoc bao ve" ->
        # duong train thuong khong duoc bao ve: bam doi kenh luc dang train la ca party rung
        # (log 30/08 21:45 va 22:15, moi lan 4 acc bay cung mot giay).
        def end_confirmed():
            return (self._genuine_end_seen > 0
                    and self._genuine_end_seen >= self.last_turn_time
                    and getattr(self, "_metrics_battle_started_at", None) is None
                    and not getattr(getattr(self, "battle_tracker", None), "active", False)
                    and not self.state.in_battle)
        if self.running and not end_confirmed() and self._genuine_end_seen < time.time() - 2.0:
            for _ in range(int(WAIT_END_THAT_SEC / 0.5)):
                if not self.running or end_confirmed():
                    break
                if self.state.in_battle:      # Tran moi bat dau: caller KHONG duoc di.
                    break
                time.sleep(0.5)
        return (self.running and not self.in_combat(idle_secs=idle)
                and not getattr(getattr(self, "battle_tracker", None), "active", False))

    def _parse_org_id_0x05(self, pkt: bytes):
        """QUAN DOAN: doc orgId tu S2C 0x05 sub03 (S:005-003 <玩家資料>) - GIONG HET client.

        Client (Logic_Role.ReceivePlayerData -> RoleController:SetOrganization): `orgId == 0` = KHONG
        co quan doan (client xoa UI quan doan + hien thong bao). Cac truong TRUOC orgId deu CO DINH
        do dai nen tinh duoc offset: payload+89 -> pkt[98:102] (u32 LE).
        Da doi chieu 2 moc co san cua bot TREN CUNG GOI: INT o pkt[16], LEVEL o pkt[28] -> khop;
        capture that cho Lv=148/149, INT=229/231 va orgId=896/55 (id guild nho, hop ly).

        Truoc day bot chi DO GIAN TIEP (mo panel quan doan roi cho 0x27 sub02 - acc KHONG co quan
        doan thi server khong bao gio gui goi do). Nguon nay chac chan hon va den NGAY luc login.
        """
        if pkt[7:9] != b"\x03\x00" or len(pkt) < 102:
            return
        org = int.from_bytes(pkt[98:102], "little")
        if org > 0x7FFFFFF:   # gia tri vo ly -> layout server khac -> bo qua, giu co che do cu
            return
        if self.org_id == org:
            return
        self.org_id = org
        self.has_legion = org > 0
        self._no_legion_confirmed = (org == 0)
        log.info("[%s] Quan doan: orgId=%d -> %s (0x05 sub03, giong client)",
                 self._label, org, "CO quan doan" if org else "KHONG co quan doan")

    def _td_party_gone(self, where: str = "") -> bool:
        """CO dong doi ROT giua pho ban to doi? (coordinator cam callback _td_party_broken).

        Leader PHAI dung danh ngay: party thieu nguoi thi cac tran sau khong qua noi, danh tiep chi
        ton 10-20 phut roi van fail. Bug that (log user 14:06): 4 member bi day ra relogin giua PB110
        ma leader van danh mot minh toi tran 4. Coordinator (run_party_digioi) da co san duong xu ly
        khi do_team_dungeon tra False: _mark_team_dungeon_broken + relogin CA PARTY roi danh lai.
        """
        cb = getattr(self, "_td_party_broken", None)
        if cb is None:
            return False
        try:
            gone = bool(cb())
        except Exception:
            return False
        if gone:
            log.warning("[%s] (LEADER) DONG DOI ROT giua pho ban%s -> DUNG danh, bao FAIL de ca "
                        "party relogin danh lai", self._label, (" (%s)" % where) if where else "")
        return gone

    def _td_daily_stop(self, where: str = "") -> bool:
        """Dung Daily chi tai RANH GIOI tran: khong cat tran dang danh, khong dong socket."""
        cb = getattr(self, "_td_stop_requested", None)
        if cb is None or self.state.in_battle:
            return False
        try:
            stop = bool(cb())
        except Exception:
            stop = False
        if stop:
            log.info("[%s] DUNG DAILY sau tran hien tai%s -> dung yen, giu online",
                     self._label, (" (%s)" % where) if where else "")
        return stop

    def _td_walk(self, points, budget: float = 90.0, tag: str = "") -> bool:
        """PHO BAN TO DOI: di toi diem CUOI cua chuoi waypoint bang TIM DUONG THONG MINH.

        Truoc day replay TUNG waypoint boc tu capture bang _route_move. Moi diem ton:
        _wait_combat_clear() (trong pho ban co guard `time.sleep(2.0)` rieng) + settle 0.6s
        => ~2.6s/diem; chang 11 diem cua PB50 = ~29s -> dung hien tuong user bao "di 1 ty roi
        dung mot luc moi di tiep". Waypoint capture con la duong di cua NGUOI THAT xuat phat tu
        vi tri cua HO; bot dung cho khac se di vong hoac quay dau.

        Gio lam GIONG event 2K (floor_crawl._walk_to): cho het tran -> lay VI TRI THAT (server
        gui 0x0c/0x07 kem toa do sau MOI lan transit trong pho ban - da doi chieu capture) ->
        navigate_to() tu vi tri that toi DICH, de Ground.mmg tu tinh duong.
        Da kiem chung: ca 4 map pho ban (62002/62011/62012/62013) deu co trong Ground.mmg va
        smart path ra duong thang cho cac chang trong capture.

        Fallback: khong lay duoc pos / map khong co duong smart -> replay waypoint capture cu.
        """
        pts = [(int(p[0]), int(p[1])) for p in (points or [])]
        if not pts:
            return True
        if not self.running:
            return False
        if not self._wait_combat_clear():
            return False
        tx, ty = pts[-1]
        # Sau transit (0x14 0800 / 0x20), server gui 0x0c ChangeScene KEM toa do -> self.pos da
        # dung. Chi khi thieu moi phai xin lai (0x0c 0100).
        if self.pos is None and self.current_map is not None:
            try:
                self.refresh_server_position(self.current_map)
            except Exception as e:
                log.debug("[%s] PB %s: xin lai toa do loi (bo qua): %s", self._label, tag, e)
        smart = None
        store = _ground_store() if (self.pos and self.current_map is not None) else None
        if store is not None:
            try:
                smart = store.find_world_path(self.current_map, self.pos, (tx, ty))
            except Exception as e:
                log.debug("[%s] PB %s: find_world_path loi (bo qua): %s", self._label, tag, e)
        if smart:
            t0 = time.time()
            log.info("[%s] (LEADER) PB %s: di THONG MINH %s -> (%d,%d) (bo qua %d waypoint capture)",
                     self._label, tag, self.pos, tx, ty, len(pts))
            self.navigate_to(tx, ty, flee=False,
                             abort=lambda: (not self.running) or time.time() - t0 > budget)
            return self.running
        # Khong co smart path -> giu duong capture (da chay duoc tu truoc), khong di mu.
        log.info("[%s] (LEADER) PB %s: khong co smart path (pos=%s map=%s) -> replay %d waypoint capture",
                 self._label, tag, self.pos, self.current_map, len(pts))
        for x, y in pts:
            if not self.running:
                return False
            self._route_move(x, y)
        return self.running

    # Mot lenh `0x06` khong duoc nhay xa hon the. Client THAT khong bao gio gui toa do DICH:
    # `MoveController.SendRolePosition()` gui `Role.player.position` - VI TRI HIEN TAI sau khi da
    # cong tung buoc `moveDist = deltaTime * speed` (`_lua_dec/Controller/MoveController.lua:194`
    # va `:234`). Bot gui thang dich, nen doan cang dai thi cu nhay cang xa.
    #
    # 120 lay theo thang do DA CHAY DUOC cua smart path: log 10/09 chia doan (150,3630)->(1210,3590)
    # (1060 don vi) thanh 11 move-point ~96/buoc, va duong do khong dinh ma 14 lan nao.
    ROUTE_BUOC_TOI_DA = 120

    def _route_move(self, x: int, y: int, settle: float = 0.6, tries: int = 8):
        """Di 1 buoc route AN TOAN: cho het tran -> move -> neu vua move lai dinh tran
        (battle nuot lenh -> nhan vat KHONG toi noi) thi cho het tran roi MOVE LAI.
        Bao dam nhan vat thuc su toi (x,y) truoc khi sang buoc/cong sau.

        CHIA DOAN khi dich o xa: gui mot lenh nhay ca nghin don vi = `S:000-000` ma 14
        `<移動距離過遠>` -> DUT KET NOI.

        Ca that 10/09 party 1 (user: "p1 dang di Pb thi dis toan bo"), pho ban to doi lv80:
            02:56:35 [xGAx] da toi diem (350,2690) sau 6 lenh move
            02:56:36 [xGAx] khong co smart path (pos=(350,2690) map=62012) -> replay 1 waypoint
            02:56:36 [xGAx] >>gui 0x06 01000146002e0e
            02:56:36 [xGAx] SERVER NGAT KET NOI: di chuyen QUA XA (ma 14)
        Leader rot -> ca party thoat pho ban theo, mat luot. Chinh docstring cua ham goi no da
        canh bao "waypoint capture la duong cua NGUOI THAT xuat phat tu vi tri cua HO" - biet vay
        ma van replay mu tu cho khac.
        """
        for _ in range(tries):
            if not self.running:
                return
            if not self._wait_combat_clear():
                return
            if not self._move_chia_doan(x, y, settle):
                return
            if not self.in_combat(idle_secs=1.5):
                return   # move xong, khong dinh tran -> coi nhu da toi

    def _move_chia_doan(self, x: int, y: int, settle: float = 0.6) -> bool:
        """Di toi (x,y) bang NHIEU lenh move ngan, giong cach client that cong tung buoc.

        Tra False khi bi ngat giua chung (acc tat / dinh tran) - caller thu lai.
        """
        import math
        try:
            _tu = self.pos
            _cach = math.hypot(x - _tu[0], y - _tu[1]) if _tu else 0.0
        except Exception:
            _tu, _cach = None, 0.0
        if not _tu or _cach <= self.ROUTE_BUOC_TOI_DA:
            self.move_to(x, y)
            time.sleep(settle)
            return True
        _so = int(_cach / self.ROUTE_BUOC_TOI_DA) + 1
        log.info("[%s] route: %s -> (%d,%d) cach %.0f -> chia %d buoc (tranh ma 14)",
                 self._label, _tu, x, y, _cach, _so)
        for _i in range(1, _so + 1):
            if not self.running:
                return False
            if self.in_combat(idle_secs=1.5):
                return False        # dinh tran giua duong -> lenh bi nuot, caller cho roi thu lai
            _bx = int(round(_tu[0] + (x - _tu[0]) * _i / float(_so)))
            _by = int(round(_tu[1] + (y - _tu[1]) * _i / float(_so)))
            self.move_to(_bx, _by)
            time.sleep(settle if _i == _so else min(settle, 0.35))
        return True

    def _enter_gate(self, x: int, y: int, idx: int, timeout: float = 90.0,
                    expected_map: int = None, board_boat: bool = False,
                    on_boat: bool = False) -> bool:
        """Toi cong (x,y) + gui chuoi 0x14 04/08[idx] (giong thoat Di Gioi) -> cho MAP DOI.
        Cong trung gian khong biet map dich nen xac nhan = current_map khac map luc bat dau.
        QUAN TRONG: chi move toi cong + gui transit khi HET TRAN. Neu gui 0x06/0x14 luc dang
        battle -> server nuot lenh (khong toi cong) hoac DA ket noi -> ket cong / leader rot.
        board_boat=True: day la BEN THUYEN -> them 0x7c 04 00 (LEN THUYEN) sau 0x14 08 de server
        ghi nhan 'tren thuyen' (capture thuyen_thanhchau). Khong len thuyen thi cac cong bien sau
        se bi kick (di bo nhay cong bien)."""
        start_map = self.current_map
        expected_map = None if expected_map in (None, 0) else int(expected_map)

        def _gate_reached():
            cm = self.current_map
            if cm is None or cm == start_map:
                return False
            # Qua duoc cong SAU KHI da tra loi mot buoc chon -> ma do DUNG, ghi nho de lan sau
            # dung thang, khoi do lai tu dau.
            _last = getattr(self, "_gate_choice_last", None)
            if _last is not None:
                _st = _GATE_CHOICE_STATE.setdefault(self._gate_choice_key, {"try": 0, "ok": None})
                if _st.get("ok") != _last:
                    _st["ok"] = _last
                    log.info("[%s] CONG %s: ma %d DUNG -> ghi nho cho lan sau",
                             self._label, self._gate_choice_key, _last)
            # Qua cong -> vi tri CU vo nghia. NHUNG goi lam DOI MAP (0x0c/0x07) MANG
            # LUON toa do moi (client Lua protocolTable[12][0]/[7][0] doc position ngay
            # trong goi do) -> neu da co toa do di kem dung lan doi map nay thi GIU.
            # Xoa di la vut mat chinh thu vua nhan -> navigate_to mat smart path ->
            # di mu 30 lenh (bug 12:25).
            if getattr(self, "_pos_valid_for_map", None) != cm:
                self.pos = None
            if expected_map is not None and cm != expected_map:
                log.warning("[%s] qua cong idx=%d NHUNG sai map: %s != %s",
                            self._label, idx, cm, expected_map)
            else:
                log.info("[%s] qua cong idx=%d -> map %s", self._label, idx, cm)
            # DA SANG MAP MOI -> gui chuoi RESUME (0x0c 01 + 0x14 06) nhu client that, khong thi
            # server nuot lenh move -> dung im (bug 'qua map bien khong sail'). Xem scene_resume().
            self.scene_resume()
            return True

        def _gate_wait_clear(idle: float = 3.0, cap: float = 150.0) -> bool:
            """Cho het TRAN PHUC KICH TAI CONG. TUYET DOI KHONG gui gi trong luc cho: da test
            (10:36:45) gui 0x14 06 luc server dang giai tran (0x32 ket qua con dang ve) -> SERVER
            DONG KET NOI. Chi cho; map doi thi thoat som."""
            t_w = time.time()
            while self.running and time.time() - t_w < cap:
                if not self.in_combat(idle_secs=idle):
                    # Leader co the ha in_battle bang tin member cung map da ket tran. Rieng gate
                    # flow, neu gui 0x14 06 ngay lap tuc luc server leader con dang xa ket qua
                    # battle thi server kick (log 14:21:43 gate idx=30). Cho het grace truoc khi
                    # cho post-battle loop bam tiep dialog/qua cong.
                    if _gate_wait_grace():
                        continue
                    return self.running
                if self.current_map not in (None, start_map):
                    return self.running   # map da doi -> tran o cong xong roi
                time.sleep(0.5)
            return self.running

        def _gate_wait_grace() -> bool:
            now = time.time()
            grace_until = max(
                getattr(self, "_battle_end_grace_until", 0.0),
                getattr(self, "_genuine_end_seen", 0.0) + 4.0,
            )
            grace_left = grace_until - now
            if grace_left <= 0:
                return False
            time.sleep(min(grace_left, 3.0))
            return True

        def _gate_select_result(wait: float = 1.2):
            """Sau 0x14 08, doi ngan de bat case cong tu no battle/map doi truoc khi bam 0600."""
            t_s = time.time()
            while self.running and time.time() - t_s < wait:
                if _gate_reached():
                    return "map"
                if self.state.in_battle or self.in_combat(idle_secs=0.2):
                    return "battle"
                time.sleep(0.1)
            return None

        t0 = time.time()
        _attempt = 0
        gate_battled = False   # da tung no tran phuc kich tai cong -> KHONG gui lai 04/08 (kick)
        # Moi cong co luot thu ma chon RIENG (xem GATE_CHOICE_CODES).
        self._gate_choice_pending = False
        self._gate_choice_try = 0
        self._gate_event_logged = 0
        self._gate_choice_sent_at = 0.0
        self._gate_choice_last = None
        self._gate_choice_key = (start_map, idx)
        while True:
            if not self.running:
                return False
            # Dang danh = quai phuc kich tai cong, PHAI danh xong moi qua duoc -> khong tinh gio.
            # timeout chi de bat cong HONG (khong danh, khong doi map).
            if self.state.in_battle:
                t0 = time.time()
            elif time.time() - t0 >= timeout:
                break
            if _gate_reached():
                return True
            _attempt += 1
            log.info("[%s] _enter_gate idx=%d @(%d,%d): lan thu %d (t=%.0fs, map van %s)",
                     self._label, idx, x, y, _attempt, time.time() - t0, self.current_map)
            # CHO HET TRAN truoc khi toi cong + transit (battle nuot lenh -> ket cong / kick leader).
            # idle=5.0 (du in_battle da chuan): gate transit RAT nhay (kick leader) nen giu buffer rong hon
            # navigate/follow -> chac chan sach tran moi gui chuoi 0x14.
            if not self._wait_combat_clear(idle=5.0):
                return False
            if x or y:   # x=y=0 -> cong "vao lien" (spawn ngay tai cong) -> KHONG move, chi trigger
                self.move_to(x, y)
            # Dung tai cong: cho 0x35/0x34 (battle) kip den neu BUOC MOVE TOI CONG vua AGGRO quai moi.
            # 3.0s (KHONG 1.5s): aggro tu buoc move gui 0x34/0x35 ve cham ~1s -> 1.5s check som -> tuong
            # het tran -> transit -> tran moi ve giua transit -> SERVER KICK leader (race da gap o bai quai).
            # idle=5.0 de chac chan het tran truoc khi transit (gate nhay).
            time.sleep(3.0)
            if self.in_combat(idle_secs=5.0):
                continue   # con trong tran (hoac vua aggro) -> fight het roi moi transit
            # BEN THUYEN: quai PHUC KICH tai ben aggro CHAM (~2-5s sau khi dung tai cong). Neu len
            # thuyen (0x7c) TRUOC khi ambush no -> fight trong luc 'dang board' -> HONG trang thai
            # thuyen -> toi map bien khong sail duoc (dung im). -> Provoke + cho THEM, neu ambush no
            # thi quay lai fight het, LAN SAU moi board SACH. Dung rearm_ready (chi 0x41) de aggro -
            # KHONG combat_ready (co 0x7c -> board som ngoai y muon).
            if board_boat:
                try: self.rearm_ready()
                except Exception: pass
                time.sleep(4.0)
                if self.in_combat(idle_secs=5.0):
                    log.info("[%s] BEN THUYEN idx=%d: ambish no -> danh sach TRUOC khi len thuyen", self._label, idx)
                    continue
            # Cong tren o NUOC (giua bien, dang tren thuyen): capture chi gui 0x14 08 (KHONG 0x14 04).
            gate_is_sea = False
            try:
                _gs = _ground_store()
                if _gs is not None and self.current_map:
                    gate_is_sea = _gs.is_sea_world(self.current_map, (x, y))
            except Exception:
                pass
            # transit: bat flag de combat (luong recv) KHONG gui 0x32 xen vao giua chuoi 0x14
            self._gate_transit = True
            try:
                if board_boat:
                    # BEN THUYEN (capture): 0x14 08 idx -> 0x7c 04 00 (LEN THUYEN) -> 0c 01 -> 14 06.
                    log.info("[%s] BEN THUYEN idx=%d @(%d,%d): len thuyen (0x7c)", self._label, idx, x, y)
                    self.send(0x14, b"\x08\x00" + bytes([idx]) + b"\x00"); time.sleep(0.4)
                    self.send(0x7c, b"\x04\x00"); time.sleep(0.6)
                    _sel = _gate_select_result(1.2)
                    if _sel == "map":
                        return True
                    if _sel == "battle":
                        gate_battled = True
                    else:
                        self.send(0x0c, b"\x01\x00"); time.sleep(0.2)
                        _sel = _gate_select_result(0.8)
                        if _sel == "map":
                            return True
                        if _sel == "battle":
                            gate_battled = True
                        else:
                            self.send(0x14, b"\x06\x00"); time.sleep(1.0)
                elif gate_battled:
                    # Da no tran phuc kich o lan truoc -> server dang cho hoan tat qua cong,
                    # CHI gui 0x14 06 (gui lai 04/08 se bi kick). Xem capture thuyen_thanhchau.
                    self.send(0x14, b"\x06\x00"); time.sleep(1.0)
                else:
                    # Cong bien HOAC dang tren thuyen (on_boat): CHI 0x14 08 (khop capture). 0x14 04
                    # lam ROT KHOI THUYEN -> cac cong bien sau kick. Cong dat khi di bo: 04 + 08.
                    if not gate_is_sea and not on_boat:
                        self.send(0x14, b"\x04\x00" + bytes([idx]) + b"\x00"); time.sleep(0.3)
                    self.send(0x14, b"\x08\x00" + bytes([idx]) + b"\x00"); time.sleep(0.3)
                    _sel = _gate_select_result(1.2)
                    if _sel == "map":
                        return True
                    if _sel == "battle":
                        gate_battled = True
                    elif self._gate_choice_pending:
                        # Server DANG CHO CHON (cau Gioi kieu). Gui `0x0c 01` / `0x14 06` luc nay la
                        # SAI THU TU -> server tra loi "su kien vi pham (ma 5)" roi NGAT KET NOI.
                        # Log 17:19:45 dinh dung bay nay: 0x14 0600 xong moi 0x14 090014 -> bi da.
                        self._send_gate_choice()
                        time.sleep(1.5)
                    else:
                        self.send(0x0c, b"\x01\x00"); time.sleep(0.2)
                        _sel = _gate_select_result(0.8)
                        if _sel == "map":
                            return True
                        if _sel == "battle":
                            gate_battled = True
                        elif self._gate_choice_pending:
                            self._send_gate_choice()
                            time.sleep(1.5)
                        else:
                            self.send(0x14, b"\x06\x00"); time.sleep(1.0)
            finally:
                self._gate_transit = False
            # Mot so gate co NPC/dialog: bam thoai -> vao battle, thang xong moi qua cong.
            # Khong giu _gate_transit trong luc nay de combat loop duoc danh binh thuong.
            dialog_t0 = time.time()
            while self.running and time.time() - dialog_t0 < 10.0:
                if _gate_reached():
                    return True
                if self.state.in_battle or self.in_combat(idle_secs=1.0):
                    log.info("[%s] gate idx=%d bat battle NPC/dialog -> cho danh xong roi kiem tra map",
                             self._label, idx)
                    gate_battled = True   # vong ngoai KHONG gui lai 04/08 nua (kick leader)
                    if not _gate_wait_clear():
                        return False
                    if _gate_reached():
                        return True
                    # SAU TRAN PHUC KICH TAI CONG: server TU hoan tat qua cong khi nhan 0x14 06
                    # (capture thuyen_thanhchau: thang -> C2S 0x14 06 -> s2c 0x14 0700 + 0x03 map
                    # moi). TUYET DOI KHONG de vong ngoai gui lai 0x14 04/08 transit -> server DA
                    # ket noi (kick leader). Kien nhan gui 0x14 06 + cho 0x03 map moi.
                    post_t0 = time.time()
                    while self.running and time.time() - post_t0 < 25.0:
                        if _gate_reached():
                            return True
                        if self.state.in_battle or self.in_combat(idle_secs=1.0):
                            if not _gate_wait_clear():
                                return False
                        if _gate_wait_grace():
                            continue
                        if self._send_gate_choice():
                            time.sleep(2.0)
                            continue
                        if self._gate_next_blocked():
                            time.sleep(0.5)
                            continue
                        self.send(0x14, b"\x06\x00")
                        time.sleep(0.8)
                    break
                if _gate_wait_grace():
                    continue
                # Server DUNG o buoc CHON (cau Gioi kieu: "danh / khong danh") thi `0x14 06` vo
                # ich - phai gui `0x14 09 + ma`. Gui xong cho lau hon: chon "danh" -> vao tran.
                if self._send_gate_choice():
                    time.sleep(2.0)
                    continue
                if self._gate_next_blocked():
                    time.sleep(0.5)
                    continue
                self.send(0x14, b"\x06\x00")
                time.sleep(0.6)
        log.warning("[%s] _enter_gate idx=%d @(%d,%d): map khong doi (van %s)",
                    self._label, idx, x, y, self.current_map)
        return False

    def _exit_event_gate(self, x: int, y: int, idx: int, out_map: int = 0, timeout: float = 40.0) -> bool:
        """RA cong THOAT event map (vd 40 NPC 10991 -> 12003). KHAC _enter_gate thuong: sau khi gui
        0x14 04 (xin menu cong) phai gui LAI 0x06 DUNG YEN tren o cong voi FLAG 5 (=da toi noi) roi
        MOI gui 0x14 08 (chon) -> server chap nhan warp. Thieu buoc dung-flag5 do -> server tra
        0x14 0d (tu choi) roi KICK ket noi. Replay dung ts_exit.pcap (capture tay ra khoi 40 NPC)."""
        start_map = self.current_map
        t0 = time.time()
        _attempt = 0
        while time.time() - t0 < timeout:
            if not self.running:
                return False
            cm = self.current_map
            if cm is not None and cm != start_map:
                log.info("[%s] exit_event: qua cong idx=%d -> map %s", self._label, idx, cm)
                self.pos = None
                return (cm == out_map) if out_map else True
            _attempt += 1
            log.info("[%s] _exit_event_gate idx=%d @(%d,%d): lan thu %d (t=%.0fs, map van %s)",
                     self._label, idx, x, y, _attempt, time.time() - t0, cm)
            if not self._wait_combat_clear(idle=3.0):
                return False
            self._gate_transit = True   # chan luong combat gui 0x32 xen giua chuoi 0x14
            try:
                self.move_to(x, y); time.sleep(1.2)                        # di toi cong (flag 1)
                self.send(0x14, b"\x04\x00" + bytes([idx]) + b"\x00"); time.sleep(0.6)   # xin menu cong
                self.send(0x06, b"\x01\x00\x05" + struct.pack("<HH", x, y)); time.sleep(0.6)  # DUNG YEN tren cong (flag 5)
                self.pos = (x, y)
                self.send(0x14, b"\x08\x00" + bytes([idx]) + b"\x00"); time.sleep(0.3)   # chon
                self.send(0x0c, b"\x01\x00"); time.sleep(0.3)              # 0x0c 0100 = XAC NHAN transit (THIEU cai nay -> server tra 0x14 0d roi kick)
                self.send(0x14, b"\x06\x00"); time.sleep(1.2)             # 0x14 0600 = hoan tat transit -> warp
            finally:
                self._gate_transit = False
        log.warning("[%s] _exit_event_gate idx=%d @(%d,%d): map khong doi (van %s)",
                    self._label, idx, x, y, self.current_map)
        return False

    # ===== TU CAT DO VAO TIEN TRANG (錢莊) - user chot 04/09 =====
    # Tra tu crack client, KHONG doan:
    #   Common_protocal.lua : C:030-002 <錢莊存物品> <<+索引(1) +數量(4)>>  -> 0x1e sub0200
    #                         C:030-008 <關閉錢莊>                          -> 0x1e sub0800
    #                         S:030-007 <錢莊操作失敗> +失敗結果(1) [3 = cat that bai, 13 = DAY]
    #   UI_UIBank.lua       : `索引` la `bagIndex` cua EThings.Bag = SLOT TUI DO (giong use_slot),
    #                         va mon co `restrict & 32` thi CLIENT CHAN khong cho cat.
    #   Eve.emg scene 12263 : NPC id=1 (npcId 16004) @ (430,240); surface 1 co 3 muc ->
    #                         ma 30 "Tien bac" | ma 31 "Vat pham day du" | ma 32 "kho dau gia".
    #                         Muc CAT DO la ma 31.
    # Duong di: world_nav co canh 12001 -> 12263 qua cong 11 (1 leg, khong phai di vong).
    TIEN_TRANG_MAP = 12263
    TIEN_TRANG_POS = (390, 310)     # cho dung noi chuyen (user chi)
    TIEN_TRANG_NPC = 1              # Eve_NpcData.id trong scene - KHONG phai npcId toan cuc
    BANK_RESTRICT_CAM = 32          # restrict & 32 = mon KHONG duoc cat (client tu chan)

    NOI_DAT_TID = 0x7D2B
    NOI_DAT_SELL_CITY = 12061
    NOI_DAT_SELL_THRESHOLD = 100
    # Han gio cho CA chuyen di NPC Nha buon. Ham chay trong `pre_route_town_hop` nen ca party
    # dung cho - viec phu nay khong duoc treo party qua lau.
    NOI_DAT_HAN_GIO = 150
    NOI_DAT_NPC_ROUTE_PRE = ((322, 802), (393, 759), (410, 750), (410, 750))
    NOI_DAT_NPC_ROUTE_POST = ((606, 698), (685, 676), (764, 654), (844, 631), (850, 630), (850, 630))

    def _move_noi_dat_npc_step(self, x: int, y: int, wait: float = 0.55):
        """Replay move den NPC Nha buon o Ng.Thanh theo capture MuMu/PC (flag 0x07)."""
        if not self.running:
            return
        if not self._wait_combat_clear(idle=1.0, cap=45.0):
            # Het 45s ma van "dang trong tran" -> ghi ro. Truoc day im lang, nen log chi thay
            # dong "di NPC Nha buon" roi khong co gi nua, khong biet ket o dau.
            log.warning("[%s] Ban Noi Dat: cho het tran qua 45s tai buoc (%d,%d)",
                        self._label, x, y)
            return
        self.send(0x06, b"\x01\x00\x07" + struct.pack("<HH", int(x), int(y)))
        self.pos = (int(x), int(y))
        time.sleep(wait)

    def _noi_dat_slots(self):
        found = []
        for slot, val in list(getattr(self, "bag_slots", {}).items()):
            try:
                tid, cnt = val[0], val[1]
            except Exception:
                continue
            if int(tid) == self.NOI_DAT_TID and int(cnt) > 0:
                found.append((int(slot), int(cnt)))
        return sorted(found)

    def _donate_material_slots(self):
        """[(slot, tid, cnt)] cac slot NGUYEN LIEU dang o trang thai "dong gop" trong list quan doan.
        Muc user danh dau GIU LAI (material_modes[tid]=='keep') -> bo qua. Dung CHUNG nguon voi
        donate_legion nen list trong GUI dieu khien ca 2 (dong gop / ban)."""
        mats = getattr(config, "DONATE_MATERIALS", {}) or {}
        if not mats:
            return []
        keep = getattr(self, "material_modes", None) or {}
        out = []
        for slot, val in list(getattr(self, "bag_slots", {}).items()):
            try:
                tid, cnt = int(val[0]), int(val[1])
            except Exception:
                continue
            if cnt > 0 and tid in mats and str(keep.get(tid, "")).lower() != "keep":
                out.append((int(slot), tid, cnt))
        return sorted(out)

    def _sell_donate_materials(self, max_slots: int = 60) -> int:
        """BAN nguyen lieu quan doan tai NPC Nha buon - dung khi acc CHUA CO QUAN DOAN.

        User: tick "tu dong gop nguyen lieu" ma acc chua vao quan doan -> truoc day BO QUA HAN ->
        nguyen lieu chat day tui. Gio ghep luon vao chuyen di ban Noi Dat: item nao trong list dang
        "dong gop" thi BAN, item nao "giu lai" thi VAN GIU (dung 1 list, khong phai cau hinh rieng).
        PHAI goi TRONG luc dialog Nha buon DANG MO (xem sell_noi_dat). Tra so slot da ban.
        """
        if not getattr(self, "auto_donate_materials", False):
            return 0
        # CHI ban khi DA DO THAT va xac nhan KHONG co quan doan (donate_legion mo panel 0x7c 0400).
        # KHONG duoc dua vao mot minh has_legion: no mac dinh False nen "chua biet" cung la False
        # -> acc CO quan doan ma goi tin 0x27 sub02 chua toi se bi ban mat do dang le donate duoc.
        if not getattr(self, "_no_legion_confirmed", False) or self.has_legion is not False:
            return 0
        targets = self._donate_material_slots()[:max_slots]
        if not targets:
            return 0
        mats = getattr(config, "DONATE_MATERIALS", {}) or {}
        sold = 0
        for slot, tid, cnt in targets:
            if not self.running:
                break
            if slot < 0 or slot > 255 or cnt <= 0:
                continue
            self.send(0x1B, b"\x02\x00\x01" + bytes([slot & 0xFF])
                      + struct.pack("<H", min(cnt, 9999)) + b"\x00\x00")
            try:
                self.bag_slots.pop(slot, None)
                self.bag_counts[tid] = max(0, int(self.bag_counts.get(tid, 0)) - cnt)
            except Exception:
                pass
            sold += 1
            log.info("[%s] Ban nguyen lieu (chua co quan doan) slot=%d tid=0x%04x x%d ('%s')",
                     self._label, slot, tid, cnt, (mats.get(tid) or {}).get("name", ""))
            time.sleep(0.4)
        if sold:
            log.info("[%s] Ban nguyen lieu quan doan: %d slot (acc CHUA CO quan doan -> ban thay "
                     "vi dong gop)", self._label, sold)
        return sold

    @task_report("ban noi dat", PHASE_LOGIN_CHORE)
    def sell_noi_dat(self, max_qty: int = 9999) -> bool:
        """Ban Noi Dat (0x7d2b) o NPC Nha buon Ng.Thanh.

        Packet sell da xac nhan tu capture:
        C2S 0x1b = 02 00 01 [slot] [qty LE16] 00 00. Server ack S2C 0x17 0900...
        """
        if not (getattr(self, "auto_bag_clean", True)
                and getattr(self, "auto_sell_noi_dat", True)):
            return False
        if self.current_map != self.NOI_DAT_SELL_CITY:
            return False
        slots = self._noi_dat_slots()
        total_have = sum(cnt for _, cnt in slots)
        if total_have <= 0:
            log.info("[%s] Ban Noi Dat: khong co Noi Dat (0x7d2b) trong tui", self._label)
            return False
        if total_have <= self.NOI_DAT_SELL_THRESHOLD:
            log.info("[%s] Ban Noi Dat: co %d cai (<= %d) -> bo qua",
                     self._label, total_have, self.NOI_DAT_SELL_THRESHOLD)
            return False
        if not self._wait_combat_clear(idle=1.0, cap=60.0):
            return False

        log.info("[%s] Ban Noi Dat: co %d cai -> di NPC Nha buon Ng.Thanh", self._label, total_have)
        # HAN GIO cho ca chuyen di. `_move_noi_dat_npc_step` cho het tran toi 45s MOI BUOC, 10
        # buoc la 7.5 phut - ma ham nay chay trong `pre_route_town_hop` nen CA PARTY dung cho.
        # Ban Noi Dat la viec phu, khong dang treo party: qua han thi bo, di tiep.
        _han = time.time() + self.NOI_DAT_HAN_GIO
        for i, (x, y) in enumerate(self.NOI_DAT_NPC_ROUTE_PRE):
            if time.time() > _han:
                log.warning("[%s] Ban Noi Dat: qua %ds chua toi NPC (buoc %d) -> BO, di tiep",
                            self._label, self.NOI_DAT_HAN_GIO, i)
                return False
            self._move_noi_dat_npc_step(x, y)
        self.send(0x14, b"\x08\x00\x0a\x00")
        time.sleep(0.5)
        for i, (x, y) in enumerate(self.NOI_DAT_NPC_ROUTE_POST):
            if time.time() > _han:
                log.warning("[%s] Ban Noi Dat: qua %ds chua toi NPC (buoc post %d) -> BO, di tiep",
                            self._label, self.NOI_DAT_HAN_GIO, i)
                return False
            self._move_noi_dat_npc_step(x, y)
        if not self._wait_combat_clear(idle=1.0, cap=60.0):
            return False

        # Mo dialog Nha buon -> chon ban Noi Dat.
        self.send(0x20, b"\x02\x00\x08"); time.sleep(0.5)
        self.send(0x14, b"\x01\x00\x02\x00"); time.sleep(0.5)
        self.send(0x14, b"\x09\x00\x1f"); time.sleep(0.5)
        self.send(0x14, b"\x06\x00"); time.sleep(0.5)

        # Tu day tro di DIALOG DANG MO -> moi duong thoat deu phai dong no. Dang mo shop/thoai
        # thi server coi la DANG BAN: khong moi/nhan party duoc va TELEPORT la chet (user chot
        # 04/09). Truoc day chi dong o cuoi duong thanh cong; loi giua vong ban la thoat ham ma
        # dialog van mo.
        sold = 0
        try:
            sold = self._ban_noi_dat_vong(max_qty)
        finally:
            try:
                self.send(0x14, b"\x06\x00")
            except Exception:
                pass
        log.info("[%s] Ban Noi Dat: da ban %d/%d cai (toi da %d)",
                 self._label, sold, total_have, max_qty)
        return sold > 0

    def _ban_noi_dat_vong(self, max_qty: int) -> int:
        """Vong ban Noi Dat + ban kem nguyen lieu quan doan. Tra so cai da ban.

        PHAI goi TRONG luc dialog Nha buon dang mo, va nguoi goi (`sell_noi_dat`) chiu trach
        nhiem dong dialog trong `finally`.
        """
        sold = 0
        remaining = max(0, int(max_qty))
        for slot, cnt in self._noi_dat_slots():
            if remaining <= 0:
                break
            qty = min(cnt, remaining)
            if qty <= 0 or slot < 0 or slot > 255:
                continue
            payload = b"\x02\x00\x01" + bytes([slot & 0xFF]) + struct.pack("<H", qty) + b"\x00\x00"
            self.send(0x1B, payload)
            sold += qty
            remaining -= qty
            try:
                left = cnt - qty
                if left > 0:
                    self.bag_slots[slot] = (self.NOI_DAT_TID, left)
                else:
                    self.bag_slots.pop(slot, None)
                self.bag_counts[self.NOI_DAT_TID] = max(0, int(self.bag_counts.get(self.NOI_DAT_TID, 0)) - qty)
            except Exception:
                pass
            log.info("[%s] Ban Noi Dat: slot=%d x%d", self._label, slot, qty)
            time.sleep(0.4)
        # GHEP: acc CHUA CO quan doan -> ban luon nguyen lieu quan doan (dialog Nha buon dang mo,
        # khong ton them chuyen di). Xem _sell_donate_materials.
        try:
            self._sell_donate_materials()
        except Exception as e:
            log.warning("[%s] loi ban nguyen lieu quan doan (bo qua): %s", self._label, e)
        return sold

    # Mot ban ghi trong kho tien trang. Do tu capture 04/09 (S:030-001 dai 180B cho 5 o, 72B cho
    # 2 o -> 36B/ban ghi): `[idx 1B][itemId u16][count u32][29B con lai]`.
    # (protocal.lua ghi `+物品資料(18)` - so do KHONG khop ban dang chay, dung so do tu capture.)
    BANK_REC = 36

    def _doc_kho_tien_trang(self, data: bytes):
        """S:030-001 (ca kho) / S:030-004 (mot o) -> self.bank_slots {idx: (tid, count)}.

        Can de LAY DO RA: `C:030-001` an INDEX TRONG KHO, khong phai slot tui do.
        """
        if self.bank_slots is None:
            self.bank_slots = {}
        for off in range(0, len(data) - self.BANK_REC + 1, self.BANK_REC):
            idx = data[off]
            tid, cnt = struct.unpack_from("<HI", data, off + 1)
            if tid and cnt:
                self.bank_slots[idx] = (tid, cnt)
            else:
                self.bank_slots.pop(idx, None)
        # Server KHONG BAO GIO tu gui kho - day la LAN DUY NHAT bot nhin thay no. Ghi cache ngay
        # de con XEM LAI (va cho soi lo doi chieu) khi acc da tat.
        self._ghi_cache_kho()

    def _on_bank_fail(self, ma: int):
        """S:030-007 <錢莊操作失敗> +失敗結果(1).

        MA 3 = TUI DO DAY (khi LAY RA), MA 13 = tien trang DAY (khi CAT VAO).
        Chu thich trong protocal.lua ghi `3.存入錢莊失敗` (cat that bai) la SAI:
        chinh handler cua client hien `string.Get(80359)` = 物品欄已滿 = "tui do da day".
        Client CHI hien mot dong chu roi thoi - khong thu lai gi ca.
        """
        self.bank_fail = int(ma)
        log.warning("[%s] Tien trang: server bao LOI ma %d%s", self._label, ma,
                    " (tien trang DAY)" if int(ma) == 13 else "")

    def _cat_do_slots(self, chon):
        """[(slot, tid, qty)] cac mon TRONG LIST user tick va CAT DUOC.

        `chon` = {tid_hex: True}. Mon co `restrict & 32` bi client chan -> bot cung khong gui
        (gui la server coi la thao tac khong hop le).
        """
        gd = _load_gamedata_items()
        out = []
        for slot, val in sorted(list(getattr(self, "bag_slots", {}).items())):
            try:
                tid, cnt = int(val[0]), int(val[1])
            except Exception:
                continue
            if cnt <= 0 or chon.get("0x%04x" % tid) != CAT_DO_CAT:
                continue
            rec = gd.get(tid) or {}
            if int(rec.get("restrict", 0) or 0) & self.BANK_RESTRICT_CAM:
                log.info("[%s] Tien trang: %s KHONG duoc cat (restrict) -> bo qua",
                         self._label, rec.get("name") or ("0x%04x" % tid))
                continue
            # DA THU CAT VA HONG trong phien nay -> khong thu lai. Thieu cho nay thi mon do
            # van nam trong tui, lan tele ve Trac Quan sau lai thay "co mon can cat" -> lai di
            # NPC -> lai hong: bot di di lai lai o tien trang khong dut.
            if tid in getattr(self, "_cat_hong", ()):
                continue
            out.append((slot, tid, cnt))
        return out

    def cat_do_tien_trang(self, chon=None) -> dict:
        """Di NPC Chu tien trang o Trac Quan roi CAT do trong list vao tien trang.

        Goi tu `pre_route_town_hop` khi boc trung Trac Quan (boc trung Ng.Thanh thi van ban Noi
        Dat nhu cu). `chon` = {tid_hex: True} - list user tick, KHONG tick gi thi khong lam gi.
        Cat CA STACK cua tung mon (user chot: "nhung do co so luong thi cat max stack trong tui").

        Tra {'cat': so mon, 'so_luong': tong so luong, 'bo_qua': ly do}.
        """
        kq = {"cat": 0, "so_luong": 0, "lay": 0, "lay_so_luong": 0, "bo_qua": ""}
        # List DUNG CHUNG moi acc (cat_do_items.json). `chon` chi de test truyen tay.
        chon = _chuan_hoa_cat_do(chon or load_cat_do_items())
        if not chon:
            kq["bo_qua"] = "chua tick mon nao trong List cat do"
            return kq
        if self.current_map != self.TRAC_QUAN_CITY:
            kq["bo_qua"] = "khong o Trac Quan (dang o %s)" % self.current_map
            return kq
        can = self._cat_do_slots(chon)
        # Con phai xet chieu NGUOC LAI: co mon danh dau "lay ra" thi VAN di, du trong tui khong
        # co gi de cat. Kho co gi thi chi biet sau khi mo, nen khong the loc truoc o day.
        _co_lay = any(v == CAT_DO_LAY for v in chon.values())
        if not can and not _co_lay:
            kq["bo_qua"] = "khong co mon nao trong list o tui"
            return kq
        # TUI DAY + khong co gi de cat -> chuyen di CHAC CHAN vo ich: khong cat duoc (khong co
        # mon), lay ra cung khong duoc (het o). `_co_lay` chi nhin DANH SACH USER TICK chu khong
        # nhin thuc te, nen thieu chot nay la moi lan qua Trac Quan deu vao tien trang - mot
        # trong hai ly do bot "dung li o tien trang" (user bao 06/09).
        if not can:
            try:
                if self.bag_capacity() - self.bag_used_slots() <= 0:
                    log.info("[%s] Tien trang: tui DAY va khong co gi de cat -> khong di NPC",
                             self._label)
                    kq["bo_qua"] = "tui day, khong co gi de cat"
                    return kq
            except Exception:
                pass
        if not self._wait_combat_clear(idle=1.0, cap=60.0):
            kq["bo_qua"] = "con ket tran"
            return kq

        log.info("[%s] Tien trang: %d mon can cat%s -> di NPC Chu tien trang (map %d)",
                 self._label, len(can), ", co mon can lay ra" if _co_lay else "",
                 self.TIEN_TRANG_MAP)
        if not self.follow_smart_scene_route(self.TRAC_QUAN_CITY, self.TIEN_TRANG_MAP,
                                             safe=self.TIEN_TRANG_POS):
            kq["bo_qua"] = "khong di duoc toi map tien trang"
            return kq
        self.navigate_to(*self.TIEN_TRANG_POS, flee=True)

        # MO TIEN TRANG - chuoi CHOT TU CAPTURE THAT (captures/tien_trang_cat_do_20260904.pcap):
        #   C2S 0x20 sub0200 08     (hang so, y het sell_noi_dat - KHONG phai id NPC)
        #   C2S 0x14 sub0100 [id u16]   = `C:020 <事件觸發>` triggerKind ClickNpc=1
        #   S2C 0x14 sub0100 ...        = su kien mo
        #   C2S 0x14 sub0600            = `C:020-006 <事件下一步>`
        #   S2C 0x1e sub0100 ...        = TIEN TRANG DA MO (danh sach kho)
        # KHONG co lenh chon muc nao ca: NPC nay chi mot nhanh. Ban dau em gui
        # `0x14 sub0900 + 31` (ma "muc 2" doc tu surface) - surface la MENU TUONG TAC, thu khac
        # han. Gui no khi khong co menu nao mo -> server NGAT "su kien vi pham" (ma 5), lam rot
        # acc tp605 luc 14:57:46 ngay 04/09.
        self.bank_fail = None
        self.event_dang_mo = False
        self.bank_open = False
        self.send(0x20, b"\x02\x00\x08"); time.sleep(0.3)
        self.send(0x14, b"\x01\x00" + struct.pack("<H", self.TIEN_TRANG_NPC))
        _han = time.time() + 5.0
        while self.running and time.time() < _han and not self.event_dang_mo:
            time.sleep(0.1)
        if not self.event_dang_mo:
            log.warning("[%s] Tien trang: NPC khong mo thoai -> BO QUA (khong gui tiep)",
                        self._label)
            kq["bo_qua"] = "NPC khong mo thoai"
            self._dong_su_kien_tien_trang()     # dang ban ma tele la CHET
            self._ve_trac_quan_sau_cat_do()
            return kq
        self.send(0x14, b"\x06\x00")        # <事件下一步>
        _han = time.time() + 5.0
        while self.running and time.time() < _han and not self.bank_open:
            time.sleep(0.1)
        if not self.bank_open:
            log.warning("[%s] Tien trang: thoai xong nhung kho KHONG mo -> BO QUA", self._label)
            kq["bo_qua"] = "tien trang khong mo"
            self._dong_su_kien_tien_trang()     # su kien DA mo -> phai dong roi moi tele
            self._ve_trac_quan_sau_cat_do()
            return kq

        # MO RONG TIEN TRANG - phai lam O DAY, luc kho DANG MO. Trong client, nut mo o nam trong
        # chinh `UIBank` (`OnClick_Unlock` -> `UISell.Launch(1)`) nen khong bam duoc tu ngoai; do
        # cung la ly do khong the gop chung vao cho mo rong TUI (cho do chay luc login).
        # Mo TRUOC khi cat: cat vao kho vua rong them thi it phai bo mon hon.
        if int(getattr(self, "bank_expand_gold", 0) or 0) > 0:
            try:
                _mo = self.tu_mo_rong_tien_trang(int(self.bank_expand_gold))
                if _mo:
                    kq["mo_rong"] = _mo
            except Exception as e:
                log.warning("[%s] loi mo rong tien trang (bo qua, van cat do): %s", self._label, e)

        # Doc lai tui NGAY TRUOC khi gui: duong di co the da lam doi slot (nhat do roi, tran...).
        # MOI MON THU DUNG MOT LAN, hong thi BO QUA MON DO va di tiep (user chot 06/09).
        # Kho DAY van cat duoc mon DA CO STACK san trong kho - chi mon MOI moi hong. Ban cu
        # `break` ca lo ngay o mon hong dau tien -> vut luon phan van cat duoc.
        #
        # `bank_fail` do luong nhan goi set BAT DONG BO. Xoa co TRUOC moi lan gui roi doc SAU
        # 0.45s ngu san co: co xuat hien trong khoang do = mon vua gui hong. Server tra loi
        # cham hon 0.45s thi bi do oan cho mon KE TIEP - chap nhan, gia chi la mat 1 mon trong
        # phien (mon do vao `_cat_hong`), doi lai khong phai ngu lau gap doi moi mon.
        _hong = []
        for slot, tid, qty in self._cat_do_slots(chon):
            if not self.running:
                break
            self.bank_fail = None
            self.send(0x1E, b"\x02\x00" + bytes([slot & 0xFF]) + struct.pack("<i", int(qty)))
            time.sleep(0.45)
            _nm = (_load_gamedata_items().get(tid) or {}).get("name") or ("0x%04x" % tid)
            if self.bank_fail is not None:
                self._cat_hong.add(tid)
                _hong.append(_nm)
                log.info("[%s] Tien trang: cat %s HONG (ma %s%s) -> bo qua mon nay, thu mon sau",
                         self._label, _nm, self.bank_fail,
                         ", tien trang DAY" if self.bank_fail == 13 else "")
                continue
            kq["cat"] += 1
            kq["so_luong"] += int(qty)
            log.info("[%s] Tien trang: cat %s x%d (o #%d)", self._label, _nm, qty, slot)
        if _hong:
            kq["bo_qua"] = "%d mon cat khong duoc (%s)" % (len(_hong), ", ".join(_hong[:3]))
        # DA MO KHO ROI thi lam luon chieu NGUOC LAI trong cung chuyen di: mon user danh dau
        # "lay ra" ma dang nam trong kho -> rut ve tui. Khong tach chuyen rieng.
        #
        # XOA `bank_fail` TRUOC: vong lay do cung doc co nay de dung. Tu 06/09 vong cat KHONG
        # con `break` o mon hong nua nen co co the con SOT tu mon cat cuoi cung -> khong xoa la
        # bo luon ca chieu lay ra, du kho day chang lien quan gi den viec RUT do ve tui.
        self.bank_fail = None
        self._lay_do_tien_trang(chon, kq)
        # Dong kho ROI dong su kien (`0x1e sub0800` -> `0x14 sub0600`) - ca hai capture 04/09
        # deu co cap nay. Chi sau do moi duoc tele. Xem _dong_su_kien_tien_trang.
        self._dong_su_kien_tien_trang()
        self._ve_trac_quan_sau_cat_do()
        log.info("[%s] Tien trang: da cat %d mon (%d cai), lay ra %d mon (%d cai)%s",
                 self._label, kq["cat"], kq["so_luong"], kq["lay"], kq["lay_so_luong"],
                 " - %s" % kq["bo_qua"] if kq["bo_qua"] else "")
        return kq

    def _lay_do_tien_trang(self, chon: dict, kq: dict):
        """Rut cac mon danh dau "lay" tu kho ve tui. PHAI goi khi kho DANG MO.

        `C:030-001 <錢莊領物品> <<+索引(1) +數量(4)>>` an INDEX TRONG KHO (doc tu S:030-001),
        KHONG phai slot tui do - nham cai nay la rut nham mon.
        """
        gd = _load_gamedata_items()
        # DOI XUNG voi vong CAT (user chot 06/09): moi mon thu DUNG MOT LAN, hong thi BO QUA mon
        # do va thu mon sau. Tui day van nhan duoc mon DA CO STACK san trong tui (khong can o
        # moi), chi mon MOI moi hong - server tra ma 3 = 物品欄已滿.
        # Ban cu `break` ca lo o mon hong dau tien -> vut luon phan lay duoc; va mon hong khong
        # duoc nho lai -> lan sau ve Trac Quan lai di NPC vi chinh no.
        for idx, val in sorted((self.bank_slots or {}).items()):
            if not self.running:
                break
            tid, cnt = int(val[0]), int(val[1])
            if chon.get("0x%04x" % tid) != "lay":
                continue
            if tid in getattr(self, "_lay_hong", ()):
                continue
            self.bank_fail = None
            self.send(0x1E, b"\x01\x00" + bytes([idx & 0xFF]) + struct.pack("<I", cnt))
            time.sleep(0.45)
            _nm = (gd.get(tid) or {}).get("name") or ("0x%04x" % tid)
            if self.bank_fail is not None:
                self._lay_hong.add(tid)
                log.info("[%s] Tien trang: LAY %s HONG (ma %s%s) -> bo qua mon nay, thu mon sau",
                         self._label, _nm, self.bank_fail,
                         ", tui do DAY" if self.bank_fail == 3 else "")
                continue
            kq["lay"] += 1
            kq["lay_so_luong"] += cnt
            log.info("[%s] Tien trang: LAY RA %s x%d (kho #%d)", self._label, _nm, cnt, idx)

    def _dong_su_kien_tien_trang(self):
        """Bao server DA DONG tien trang truoc khi lam bat cu viec gi khac.

        Dang mo tien trang = SERVER coi la DANG BAN: khong moi/nhan party duoc, va TELEPORT luc
        nay la CHET (user chot 04/09). Moi duong thoat cua cat_do_tien_trang deu phai di qua day,
        ke ca cac nhanh hong giua chung.
        """
        if not getattr(self, "event_dang_mo", False) and not getattr(self, "bank_open", False):
            return
        try:
            if self.bank_open:
                self.send(0x1E, b"\x08\x00")     # C:030-008 dong kho
                time.sleep(0.3)
            self.send(0x14, b"\x06\x00")         # <事件下一步> -> ket thuc su kien
            time.sleep(0.4)
        except Exception as e:
            log.warning("[%s] Tien trang: loi khi dong su kien: %s", self._label, e)
        self.bank_open = False
        self.event_dang_mo = False

    def _ve_trac_quan_sau_cat_do(self):
        """Cat xong -> tele ve Trac Quan. Map tien trang khong co duong ve tu dong, khong ve thi
        buoc tele thanh route ke tiep xuat phat sai cho (bai hoc tu vu ket o Loi Dai Huong Dung).
        """
        if not self.running or self.current_map == self.TRAC_QUAN_CITY:
            return
        try:
            self.go_to_town(self.TRAC_QUAN_CITY, 0)
        except Exception as e:
            log.warning("[%s] Tien trang: loi tele ve Trac Quan (bo qua): %s", self._label, e)

    # ---------- MUA HP/SP tu dong (Vien Hanh Khi +62HP / Thien Kim Du +62SP) ----------
    # NPC "Loi Dai Huong Dung" o Trac Quan (12001). Route + goi mua boc tu capture
    # ts_capture_hpsp.pcap (MuMu): tele 0x44 -> qua 2 cua -> di bo toi NPC -> 0x20/0x14 mo shop ->
    # 0x1b mua theo shop-slot (01=HP tid 0x6a01, 02=SP tid 0x6a02). Gia 20 xu/cai.
    TRAC_QUAN_CITY = 12001
    HPSP_ITEM_PRICE = 20       # xu / 1 cai (ca HP lan SP)
    HP_SHOP_SLOT = 1           # Vien Hanh Khi +62HP
    SP_SHOP_SLOT = 2           # Thien Kim Du +62SP
    # Route Trac Quan spawn -> NPC (chi move + gate; scene_resume tu goi sau moi gate).
    TRAC_HPSP_ROUTE = [
        ("move", 3, 141, 1637), ("move", 3, 90, 1670), ("move", 3, 90, 1670),
        ("gate", "08000100"),
        ("move", 6, 1310, 590), ("move", 6, 1310, 590),
        ("move", 0, 1310, 409), ("move", 0, 1310, 346), ("move", 0, 1310, 282),
        ("move", 0, 1310, 219), ("move", 0, 1310, 155), ("move", 0, 1310, 110),
        ("move", 0, 1310, 110),
        ("gate", "08000500"),
        ("move", 0, 150, 1810), ("move", 7, 323, 1625), ("move", 7, 382, 1561),
        ("move", 7, 441, 1498), ("move", 7, 501, 1435), ("move", 7, 560, 1371),
        ("move", 7, 619, 1308), ("move", 7, 678, 1245), ("move", 7, 737, 1182),
        ("move", 7, 797, 1118), ("move", 7, 870, 1040), ("move", 7, 929, 977),
        ("move", 7, 988, 914), ("move", 7, 1047, 851), ("move", 7, 1106, 787),
        ("move", 7, 1166, 724), ("move", 7, 1225, 661),
        ("move", 0, 1070, 590), ("move", 0, 1070, 590),
        ("move", 1, 1010, 510), ("move", 1, 1010, 510),
    ]

    def _item_heal(self, tid: int):
        """(hp, sp) 1 item tid co the hoi. Uu tien KHAI (items_known) > gamedata > HOC (learned)."""
        k = _load_known_items().get(tid)
        if k:
            return int(k.get("hp", 0) or 0), int(k.get("sp", 0) or 0)
        g = _load_gamedata_items().get(tid)
        if g:
            return int(g.get("hp", 0) or 0), int(g.get("sp", 0) or 0)
        lv = self._learned().get(str(tid)) or {}
        return int(lv.get("hp", 0) or 0), int(lv.get("sp", 0) or 0)

    def hp_sp_reserve(self):
        """Tong HP & SP du tru = tong (so luong * luong hoi) cua MOI item hoi mau trong tui."""
        thp = tsp = 0
        for _slot, val in list(getattr(self, "bag_slots", {}).items()):
            try:
                tid, cnt = int(val[0]), int(val[1])
            except Exception:
                continue
            if cnt <= 0:
                continue
            hp, sp = self._item_heal(tid)
            thp += hp * cnt
            tsp += sp * cnt
        return thp, tsp

    def _buy_shop_slot(self, slot: int, want: int, price: int, name: str, chunk: int = 9999) -> int:
        """Mua item o shop-slot (0x1b: 01 00 [slot] [qty LE16] 00 00). Mua toi da theo 'want' NHUNG
        khong vuot kha nang chi tra (self.xu // price). Toi da 9999/lenh -> muon nhieu hon thi tu
        chia nhieu lenh (batch 'chunk'). Tu tru self.xu."""
        want = max(0, int(want))
        if want <= 0:
            return 0
        afford = (self.xu // price) if (self.xu is not None and price > 0) else want
        qty = min(want, afford)
        if qty <= 0:
            log.info("[%s] Mua %s: khong du xu (xu=%s, gia=%d) -> mua 0", self._label, name, self.xu, price)
            return 0
        bought = 0
        while bought < qty and self.running:
            q = min(qty - bought, chunk)
            payload = b"\x01\x00" + bytes([slot & 0xFF]) + struct.pack("<H", q) + b"\x00\x00"
            self.send(0x1B, payload)
            bought += q
            if self.xu is not None:
                self.xu = max(0, self.xu - q * price)
            time.sleep(0.4)
        log.info("[%s] Mua %s: %d/%d cai (con xu ~%s)", self._label, name, bought, want, self.xu)
        return bought

    def _run_trac_hpsp_route(self):
        """Replay route Trac Quan spawn -> NPC Loi Dai Huong Dung (theo capture)."""
        for step in self.TRAC_HPSP_ROUTE:
            if not self.running:
                return
            if step[0] == "move":
                _, flag, x, y = step
                if not self._wait_combat_clear(idle=1.0, cap=45.0):
                    return
                self.send(0x06, b"\x01\x00" + bytes([flag & 0xFF]) + struct.pack("<HH", int(x), int(y)))
                self.pos = (int(x), int(y))
                time.sleep(0.55)
            elif step[0] == "gate":
                if not self._wait_combat_clear(idle=1.0, cap=45.0):
                    return
                self.send(0x14, bytes.fromhex(step[1]))
                time.sleep(0.5)
                self.scene_resume()   # 0x0c 0100 + 0x14 0600: bat buoc sau doi scene moi di duoc

    def _ve_thanh_sau_mua_hpsp(self):
        """Mua HP/SP xong -> teleport ve Trac Quan. Map NPC khong co duong ve tu dong."""
        if not self.running:
            return
        if self.current_map == self.TRAC_QUAN_CITY:
            return
        try:
            self._wait_combat_clear(idle=1.0, cap=60.0)   # teleport giua tran -> server KICK
            log.info("[%s] Mua HP/SP xong (dang o map %s) -> ve Trac Quan",
                     self._label, self.current_map)
            self.go_to_town(self.TRAC_QUAN_CITY, 0)
        except Exception as e:
            log.warning("[%s] Mua HP/SP: loi ve Trac Quan sau khi mua: %s", self._label, e)

    def buy_hp_sp(self, buy_hp: bool, hp_qty: int, hp_thresh: int,
                  buy_sp: bool, sp_qty: int, sp_thresh: int):
        """Login (sau khi load tui): neu du tru HP/SP thap hon nguong -> di Trac Quan mua bo sung.
        Gop CA HP+SP trong 1 CHUYEN (cung 1 NPC), khong bay ve roi di lai. MOI lan login deu check
        (KHONG chot 1 lan/ngay) - user muon cu thieu la mua bu.
        TRA VE: True neu DA MUA nhung du tru VAN THAP hon nguong (het xu) -> caller quyet dinh (party
        thi train tiep, solo thi out). False neu du/khong can/loi trung gian (khong ep quit)."""
        if not (buy_hp or buy_sp):
            return False
        thp, tsp = self.hp_sp_reserve()
        need_hp = bool(buy_hp) and thp < int(hp_thresh)
        need_sp = bool(buy_sp) and tsp < int(sp_thresh)
        if not (need_hp or need_sp):
            log.info("[%s] Mua HP/SP: du tru du (HP=%d/nguong=%s, SP=%d/nguong=%s) -> bo qua",
                     self._label, thp, hp_thresh if buy_hp else "-", tsp, sp_thresh if buy_sp else "-")
            return False
        self._wait_xu()
        if self.xu is None:
            log.info("[%s] Mua HP/SP: chua doc duoc xu -> bo qua", self._label)
            return False
        log.info("[%s] Mua HP/SP: HP du tru=%d (%s), SP du tru=%d (%s), xu=%d -> di Trac Quan",
                 self._label, thp, "MUA" if need_hp else "du", tsp, "MUA" if need_sp else "du", self.xu)
        if not self._wait_combat_clear(idle=1.0, cap=60.0):
            return False
        if not self.go_to_town(self.TRAC_QUAN_CITY, 0):
            log.warning("[%s] Mua HP/SP: khong ve duoc Trac Quan -> bo qua", self._label)
            return False
        try:
            self._run_trac_hpsp_route()
            if not self._wait_combat_clear(idle=1.0, cap=60.0):
                return False
            # Mo dialog NPC -> vao shop (chuoi boc tu capture).
            self.send(0x20, b"\x02\x00\x08"); time.sleep(0.6)
            self.send(0x14, b"\x01\x00\x0c\x00"); time.sleep(0.5)
            self.send(0x14, b"\x09\x00\x1e"); time.sleep(0.5)
            self.send(0x14, b"\x06\x00"); time.sleep(0.5)
            if need_hp:
                self._buy_shop_slot(self.HP_SHOP_SLOT, hp_qty, self.HPSP_ITEM_PRICE,
                                    "Vien Hanh Khi +62HP")
            if need_sp:
                self._buy_shop_slot(self.SP_SHOP_SLOT, sp_qty, self.HPSP_ITEM_PRICE,
                                    "Thien Kim Du +62SP")
        finally:
            # DONG DIALOG TRUOC, roi moi tele. Dang mo shop/thoai = server coi la DANG BAN:
            # teleport luc do la CHET, va cung khong moi/nhan party duoc (user chot 04/09).
            # Truoc day dong `0x14 sub0600` nam TRONG `try` nen loi giua chung -> nhay thang
            # xuong `finally` va TELE trong khi shop van dang mo.
            try:
                self.send(0x14, b"\x06\x00")
                time.sleep(0.4)
            except Exception:
                pass
            # PHAI RA KHOI MAP NPC. Route di qua 2 cong nen ket thuc o map "Loi Dai Huong Dung",
            # KHONG phai 12001. Truoc day ham nay ket thuc tai cho -> acc dung im o cho NPC:
            # luong train phia sau bam `login_map` (doc luc login, van la map train) nen tuong minh
            # dang o dung bai, khong ai keo ve (user bao 28/08: "no dung ket o Loi dai Huong dung").
            # Loi giua chung cung phai ve -> dat trong `finally`.
            self._ve_thanh_sau_mua_hpsp()
        # Mua xong: doc lai du tru. Van thap hon nguong (het xu) -> tra True.
        thp2, tsp2 = self.hp_sp_reserve()
        still_low = (need_hp and thp2 < int(hp_thresh)) or (need_sp and tsp2 < int(sp_thresh))
        if still_low:
            log.info("[%s] Mua HP/SP: mua xong VAN THIEU (HP=%d/%s, SP=%d/%s) - co the het xu",
                     self._label, thp2, hp_thresh if buy_hp else "-", tsp2, sp_thresh if buy_sp else "-")
        return still_low

    def pre_route_town_hop(self):
        """Truoc khi teleport ve THANH DAU ROUTE: tele ve Trac Quan (12001) hoac Ng.Thanh (12061)
        TRUOC (chon ngau nhien 50-50) roi moi tele thanh route. User bao: bay ve thanh route truc
        tiep tu map la hay bi loi ngay doan tele; qua 1 thanh trung gian truoc thi on dinh."""
        import random
        # CHI chon thanh DA MO. Chua mo Nghiep Thanh -> dung Trac Quan (acc moi LUON co san).
        # Truoc day boc 50-50 mu -> acc chua mo Nghiep Thanh spam "Teleport -> city 12061" hang
        # chuc lan khong bao gio toi (log 15:27).
        _ung_vien = [(12001, 0), (12061, 2)]
        _mo = [cf for cf in _ung_vien if self.city_unlocked(cf[0]) is not False]
        city, flag = random.choice(_mo or [(12001, 0)])
        log.info("[%s] pre-route: tele trung gian ve thanh %s truoc (%d thanh da mo)",
                 self._label, city, len(_mo))
        try:
            ok = self.go_to_town(city, flag)
            # DANG GOM PARTY thi BO QUA viec vat. Cat tien trang / ban Noi Dat deu keo acc sang map
            # KHAC (12263 Chu tien trang, 12061 Nha buon) - dung luc ca party phai o mot cho.
            #
            # Ca that 07/09 party 1 (user: "leader bi chet va ve thanh, bot dieu phoi lam cai gi ma
            # deo keo pt chay lai di, thay reform vai lan moi duoc"):
            #   18:03:33 [xGAx] (LEADER) BI VAN khoi train map (vd chet) -> yeu cau CA PARTY reform
            #   18:03:37  ca party ve thanh
            #   18:03:45 [xGAx]   Tien trang: 3 mon can cat -> di NPC Chu tien trang (map 12263)
            #   18:03:45 [minh]   Ban Noi Dat: co 146 cai -> di NPC Nha buon Ng.Thanh
            #   18:03:46 [chihao] Tien trang: 2 mon can cat -> di NPC Chu tien trang (map 12263)
            #   18:03:51 [tuyet]  reform: cho leader lap duong toi map 21836 (15s)...
            # Leader vua ra lenh gom xong thi CHINH NO bo di cat do. Party tu 1 map thanh 4 map
            # [12001, 12061, 12263, 21011] -> dieu phoi lai ra lenh gom -> lap.
            if party_dang_gom(self.party_idx):
                # HOAN, khong phai bo: `pre_route_town_hop` chay lai MOI LAN di tu thanh ra train,
                # va `cat_do_tien_trang` / `sell_noi_dat` deu tu kiem "con mon nao can lam khong" -
                # nen lan sau party on dinh la lam binh thuong, khong mat gi.
                log.info("[%s] pre-route: party DANG GOM -> HOAN viec vat (cat do / ban Noi Dat), "
                         "lam o lan di train ke tiep", self._label)
                return
            if ok and city == self.NOI_DAT_SELL_CITY and getattr(self, "auto_sell_noi_dat", True):
                self.sell_noi_dat()
            # Boc trung TRAC QUAN -> di cat do vao tien trang (user chot 04/09). Boc trung
            # Ng.Thanh thi van ban Noi Dat nhu cu - moi thanh mot viec, khong lam ca hai.
            if ok and city == self.TRAC_QUAN_CITY and getattr(self, "auto_cat_do", False):
                self.cat_do_tien_trang()
        except Exception as e:
            log.warning("[%s] pre-route: loi tele trung gian (bo qua, di tiep): %s", self._label, e)

    def follow_route(self, route, step_wait: float = 0.5) -> bool:
        """Replay route tu THANH toi train map. route = {from_city, city_flag, dest_map, steps}.
        steps: {"move":[x,y]} = di 1 buoc | {"gate":idx,"x","y"} = toi cong roi gui 0x14.
        Bot CHI leader can goi (member tu bi keo theo trong party). Tra True neu toi dest_map."""
        dest = int(route.get("dest_map", 0))
        city = int(route.get("from_city", 0))
        flag = int(route.get("city_flag", 0))
        log.info("[%s] follow_route -> map %s (qua thanh %s flag %s)", self._label, dest, city, flag)
        self.flee_mode = True
        if city:
            self.pre_route_town_hop()   # tele trung gian Trac Quan/Ng.Thanh truoc (tranh loi tele truc tiep)
        if city and not self.go_to_town(city, flag):
            log.warning("[%s] follow_route: khong teleport ve thanh %s duoc", self._label, city)
            return False
        for st in route.get("steps", []):
            if not self.running:
                return False
            if "gate" in st:
                if not self._enter_gate(int(st["x"]), int(st["y"]), int(st["gate"])):
                    log.warning("[%s] follow_route: ket o cong idx=%s -> dung", self._label, st.get("gate"))
                    return False
            else:
                x, y = int(st["move"][0]), int(st["move"][1])
                self._route_move(x, y)   # cho het tran roi move (battle nuot lenh -> khong toi)
        ok = self.current_map == dest
        log.info("[%s] follow_route xong: map=%s (dich %s) -> %s",
                 self._label, self.current_map, dest, "OK" if ok else "CHUA TOI")
        return ok

    def build_smart_route(self, dest_map: int, safe):
        router = _smart_world_router()
        if router is None:
            return None
        target = None if safe is None else tuple(safe)
        return router.build_route(int(dest_map), target)

    def nearest_smart_city(self, dest_map: int, exclude_map=None, allowed=None):
        """allowed = chi duoc chon trong tap city_id nay (vd: thanh CA PARTY deu da mo)."""
        router = _smart_world_router()
        if router is None:
            return None
        return router.nearest_city(int(dest_map), exclude_city=exclude_map, allowed=allowed)

    def toa_do_that(self, wait: float = 2.0):
        """Toa do THAT do SERVER cap (`0x0c 0100` -> `0x03` self-spawn). None = server khong tra loi.

        KHAC `refresh_server_position`: ham do het gio van tra True va giu nguyen `self.pos` -
        tuc la tra ve chinh con so DEAD-RECKONING minh tu doan, nen khong dung de XAC NHAN duoc.

        LUU Y: `0x0c 0100` la `C:012-001 <換場景完畢>` - goi THONG BAO "toi vao scene xong", KHONG
        phai cau hoi. Game KHONG co lenh hoi "toi dang o dau"; server chi DAY toa do that qua
        `0x03` self-spawn / `S:012-000` khi co doi scene. Nen ham nay thuong tra None trong luc di
        lai binh thuong - do la BINH THUONG, caller phai coi None = "chua xac nhan duoc", KHONG
        duoc coi la "chua toi".
        """
        if not self.running:
            return None
        gen = self._position_generation
        try:
            self.send(0x0C, b"\x01\x00")
        except OSError:
            return None
        deadline = time.time() + max(0.0, float(wait))
        while self.running and time.time() < deadline:
            if self._position_generation != gen:
                return self.pos
            time.sleep(0.05)
        return None

    def refresh_server_position(self, source_map: int, request_timeout: float = 2.0) -> bool:
        source_map = int(source_map)
        generation = self._position_generation
        self.send(0x0C, b"\x01\x00")
        deadline = time.time() + max(0.0, float(request_timeout))
        while self.running and time.time() < deadline:
            if self._position_generation != generation:
                ok = self.current_map == source_map and self.pos is not None
                if not ok:
                    log.warning("[%s] resync pos doi sang map %s, can map %s",
                                self._label, self.current_map, source_map)
                return ok
            time.sleep(0.05)

        if self.current_map == source_map and self.pos is not None:
            log.info("[%s] request scene khong co self-spawn moi -> dung pos hien tai %s",
                     self._label, self.pos)
            return True
        log.warning("[%s] request scene khong co self-spawn va khong co pos hop le",
                    self._label)
        return False

    def build_smart_scene_route(self, source_map: int, dest_map: int, safe=None):
        router = _smart_world_router()
        if router is None:
            return None
        target = None if safe is None else tuple(safe)
        return router.build_scene_route(
            int(source_map), int(dest_map), target, start=self.pos
        )

    def follow_smart_scene_route(self, source_map: int, dest_map: int, safe=None,
                                 abort=None, flee=True,
                                 refresh_position=True) -> bool:
        source_map = int(source_map)
        dest_map = int(dest_map)
        if not self.running or (abort and abort()):
            return False
        if self.current_map != source_map:
            log.warning("[%s] scene route: dang o map %s, can bat dau tu %s",
                        self._label, self.current_map, source_map)
            return False
        if refresh_position:
            if not self.refresh_server_position(source_map):
                log.warning("[%s] scene route: khong lay duoc toa do moi map=%s",
                            self._label, source_map)
                return False
        else:
            deadline = time.time() + 20.0
            while self.running and time.time() < deadline and self.pos is None:
                if abort and abort():
                    return False
                time.sleep(0.2)
            if self.pos is None:
                log.warning("[%s] scene route: chua co toa do xuat phat map=%s",
                            self._label, self.current_map)
                return False
        route = self.build_smart_scene_route(source_map, dest_map, safe)
        if route is None:
            log.warning("[%s] scene route: khong tim thay duong %s -> %s",
                        self._label, source_map, dest_map)
            return False
        gates = ",".join(str(leg["gate"]) for leg in route["legs"])
        log.info("[%s] scene route %s -> %s: gates=%s",
                 self._label, source_map, dest_map, gates or "(none)")
        self.flee_mode = bool(flee)
        ok = execute_smart_route(self, route, abort=abort, flee=flee)
        if ok:
            log.info("[%s] scene route reached map %s", self._label, dest_map)
        return ok

    def follow_smart_route(self, dest_map: int, safe, abort=None, flee=True) -> bool:
        """Teleport to the best city, then traverse only verified scene gates."""
        dest_map = int(dest_map)
        safe = None if safe is None else (int(safe[0]), int(safe[1]))
        if not self.running or (abort and abort()):
            return False
        if self.current_map == dest_map:
            if safe is not None:
                self.navigate_to(*safe, abort=abort, flee=flee)
            return self.running and self.current_map == dest_map

        router = _smart_world_router()
        if router is None:
            return False
        route = router.build_route(dest_map, safe)
        if route is None:
            log.warning("[%s] smart route: khong tim thay duong toi map %s",
                        self._label, dest_map)
            return False

        for attempt in range(2):
            gates = ",".join(str(leg["gate"]) for leg in route["legs"])
            log.info("[%s] smart route %s: city=%s flag=%s gates=%s",
                     self._label, dest_map, route["city"], route["flag"], gates)
            self.flee_mode = bool(flee)
            if self.current_map != route["city"]:
                self.pre_route_town_hop()
                if not self.go_to_town(route["city"], route["flag"]):
                    return False

            deadline = time.time() + 20.0
            while (self.running and time.time() < deadline
                   and (self.current_map != route["city"] or self.pos is None)):
                if abort and abort():
                    return False
                time.sleep(0.2)
            if self.current_map != route["city"] or self.pos is None:
                log.warning("[%s] smart route: city spawn chua san sang map=%s pos=%s",
                            self._label, self.current_map, self.pos)
                return False

            if execute_smart_route(self, route, abort=abort, flee=flee):
                if safe is None:
                    log.info("[%s] smart route reached map %s arrival",
                             self._label, dest_map)
                else:
                    log.info("[%s] smart route reached safe (%d,%d)",
                             self._label, safe[0], safe[1])
                return True
            if self._smart_route_failure != "unexpected_scene" or attempt:
                return False
            log.warning("[%s] smart route: scene ngoai du kien %s, rebuild mot lan",
                        self._label, self.current_map)
            router.cache.invalidate(dest_map, safe)
            route = router.build_route(dest_map, safe)
            if route is None:
                return False
        return False

    def go_to_event(self, ev) -> bool:
        """Tele toi MAP EVENT roi dung yen (mode 'event', moi nick tu di rieng - khong party).
        ev = dict tu config.EVENTS: {label, select, staging_map, dest_map, steps}. Replay capture:
        0x4d [select] chon event -> server tele toi staging_map; 0x0c 0100 xin info; di chuyen (0x06)
        toi cong; 0x14 08[idx] qua cong (tu xu cinematic bang 0x14 06 trong _enter_gate) -> map event.
        Giong do_world_boss (cung 0x4d/0x0c) nhung vao instance bang di bo + cong. Tra True neu toi dich."""
        select = ev.get("select", "")
        staging = int(ev.get("staging_map", 0))
        dest = int(ev.get("dest_map", 0))
        label = ev.get("label", "?")
        self._vo_gioi = None      # xoa lenh chuyen server cu, chi nhan cai cua lan chon nay
        log.info("[%s] go_to_event '%s' -> staging %s, dest %s", self._label, label, staging, dest)
        self.flee_mode = True
        # (0) VAO EVENT PHAI KHONG CO PARTY -> roi party truoc (neu dang dinh party tu truoc thi tele
        # loi / char ket trang thai 'dang ban'). Sau khi vao xong, nguoi choi moi tay lai.
        try:
            self.leave_party(); time.sleep(0.5)
        except Exception:
            pass
        # (0b) PHAI HET TRAN MOI CHON DUOC EVENT. Server khong tele nguoi dang trong chien dau -
        # goi `0x4d` gui giua tran bi NUOT im lang, roi vong cho ben duoi het 20s va ket luan
        # "CHUA TOI" trong khi bot chua he duoc thu that su.
        #
        # Ca that 08/09 loan dau (user: "co acc ko tele vao event"):
        #   20:25:25 [daimot] go_to_event 'Loạn đấu' -> staging 0, dest 10991
        #   20:25:26 [daimot] BO CHAY (flee_mode, ...)        <- dang trong tran
        #   20:25:32 [daimot] BO CHAY (flee_mode, ...)
        #   20:25:40 [daimot] BO CHAY (flee_mode, ...)
        #   20:25:47 [daimot] go_to_event 'Loạn đấu' xong: map=21812 (dich 10991) -> CHUA TOI
        # Suot 20 giay cho do acc dang danh nhau. 44 acc dinh y het.
        try:
            if self.in_combat(idle_secs=2.0):
                log.info("[%s] go_to_event '%s': dang trong TRAN -> cho het tran roi moi chon event",
                         self._label, label)
                self._wait_combat_clear(idle=2.0, cap=180.0)
                if self.in_combat(idle_secs=2.0):
                    log.warning("[%s] go_to_event '%s': KHONG dut duoc tran -> chua the vao event, "
                                "de vong ngoai thu lai", self._label, label)
                    return False
        except Exception as e:
            log.debug("[%s] go_to_event: loi cho het tran (bo qua): %s", self._label, e)
        # (1) chon event -> server tele toi staging map (KHONG the teleport thang toi instance)
        try:
            self.send(0x4d, bytes.fromhex(select)); time.sleep(0.5)
            self.send(0x0c, b"\x01\x00"); time.sleep(0.5)
        except Exception as e:
            log.warning("[%s] go_to_event: loi chon event: %s", self._label, e)
            return False
        t0 = time.time()   # cho toi staging map (toi 20s)
        while staging and self.current_map != staging and time.time() - t0 < 20:
            if not self.running: return False
            time.sleep(1)
        # KHONG CO staging (`staging_map: 0`) = `0x4d [select]` tele THANG toi map event. Van
        # phai CHO - truoc day khong cho gi, chi ngu 1.0s roi ket luan ngay -> server tele mat
        # ~2.4s (do tren capture t7: gui 19.50, toi map 21.86) nen LUC NAO CUNG bao "CHUA TOI".
        # Hau qua that 05/09 21:15: 4 acc party 11 khong toi map event ma VAN mo NPC event tren
        # map train -> server da het ma 5 "su kien vi pham".
        if not staging and dest:
            t0 = time.time()
            while self.current_map != dest and time.time() - t0 < 20:
                if not self.running: return False
                # EVENT LIEN SERVER: server tra S:001-020 bao "sang may khac". Khong doi may thi
                # KHONG BAO GIO toi map duoc - map do khong nam tren may nay.
                if getattr(self, "_vo_gioi", None):
                    if not self.chuyen_server_vo_gioi():
                        return False
                    t0 = time.time()      # tinh lai gio cho tren may moi
                time.sleep(0.5)
        # (2) di toi cong roi qua cong -> map event.
        # DUONG DI: TIM DUONG THONG MINH (navigate_to -> Ground.mmg), KHONG chep cung tung buoc
        # `move` nhu truoc (di 1 ti roi dung cho rat lau moi di tiep - _route_move cho het tran +
        # settle moi buoc). Toa do tam cong + door index doc tu world_nav.json.
        gate = self._event_entry_gate(staging, dest) if (staging and dest) else None
        if gate is not None:
            door, center = gate
            log.info("[%s] go_to_event: di thong minh toi cong door=%s tai %s",
                     self._label, door, center)
            self.navigate_to(*center, flee=True)
            if not self._event_gate(center[0], center[1], door, dest):
                log.warning("[%s] go_to_event: ket o cong idx=%s", self._label, door)
                return False
        else:
            # world_nav khong co canh staging->dest (event moi chua co du lieu) -> dung `steps`
            # trong events.json lam duong lui, van hon la dung yen khong vao duoc.
            log.warning("[%s] go_to_event: world_nav khong co cong %s->%s -> dung steps trong json",
                        self._label, staging, dest)
            for st in ev.get("steps", []):
                if not self.running: return False
                if "gate" in st:
                    if not self._event_gate(int(st["x"]), int(st["y"]), int(st["gate"]), dest):
                        log.warning("[%s] go_to_event: ket o cong idx=%s", self._label, st.get("gate"))
                        return False
                else:
                    self._route_move(int(st["move"][0]), int(st["move"][1]))
        ok = (self.current_map == dest) if dest else True
        log.info("[%s] go_to_event '%s' xong: map=%s (dich %s) -> %s",
                 self._label, label, self.current_map, dest, "OK" if ok else "CHUA TOI")
        return ok

    def _event_entry_gate(self, staging: int, dest: int):
        """(door, (x,y)) cua cong staging->dest doc tu world_nav.json. None neu khong co du lieu."""
        router = _smart_world_router()
        if router is None:
            return None
        nav = router.nav
        for edge in nav.data.get("edges", []):
            if int(edge["scene"]) != int(staging) or int(edge["target_scene"]) != int(dest):
                continue
            gate = nav.get_gate(staging, edge["door"])
            if gate and gate.get("center"):
                return int(edge["door"]), tuple(gate["center"])
        return None

    def regroup_to_event_start(self, ev, dest: int = None) -> bool:
        """DI BO xuong map tap trung cua event. `dest` = tang gom (mac dinh dest_map = 12922).

        Trong map event KHONG teleport duoc (xem `exit._note` trong events.json), va chon lai
        event (0x4d) tu tang sau thi chua co bang chung la keo ve duoc -> cach chac chan la DI BO
        xuong theo cong. KHONG co tim duong moi: dung dung `follow_smart_scene_route` nhu
        exit_event(), chi khac DICH (dest_map thay vi exit.out_map) - world_nav du cong xuong
        (12931 -> 12003 qua 11 cong).
        Dung khi party BI LECH TANG -> gom nhau o TANG THAP NHAT ma ca doi toi duoc (xem
        _2k_regroup_target trong run_party_digioi) roi leo lai tu day. Thap nhat co the la
        12922 - luc do acc dang o NGOAI event tele vao binh thuong.
        """
        dest = int(dest or (ev or {}).get("dest_map") or 0)
        cur = int(self.current_map or 0)
        if not dest or not cur:
            return False
        if cur == dest:
            return True
        log.info("[%s] gom doi: di bo %s -> %s (map event khong teleport duoc)",
                 self._label, cur, dest)
        self.flee_mode = True   # dang di gom doi -> ne tran, khong dung lai danh
        if not self.refresh_server_position(cur):
            return False
        ok = self.follow_smart_scene_route(cur, dest, flee=True, refresh_position=False)
        if not ok:
            log.warning("[%s] gom doi: KHONG di bo duoc %s -> %s", self._label, cur, dest)
        return ok

    def start_floor_crawl(self, ev, on_done=None, heal_party=None, lost_check=None,
                          du_party=None) -> bool:
        """Bat dau leo thap (event kieu floor_crawl, vd Nhi Kieu). Chay thread rieng nhu npc40."""
        if getattr(self, "_floor_crawl_started", False):
            return False
        from . import floor_crawl
        self._floor_crawl_started = True
        self._floor_crawl_stop = threading.Event()
        self._floor_crawl_thread = threading.Thread(
            target=floor_crawl.run_floor_crawl,
            args=(self, ev, self._floor_crawl_stop, on_done, heal_party, lost_check,
                  du_party),
            daemon=True,
            name="floorcrawl-%s" % (self._label or self._username),
        )
        self._floor_crawl_thread.start()
        return True

    def stop_floor_crawl(self):
        stop = getattr(self, "_floor_crawl_stop", None)
        if stop is not None:
            stop.set()

    def event_dang_dung_trong(self):
        """Event (trong events.json) ma map HIEN TAI dang nam trong - None neu dang o ngoai.

        Phu ca thap nhieu tang (floor_crawl: dest_map..top_map) lan map event mot tang
        (dest_map / staging_map). Dung de biet khi nao PHAI di bo ra thay vi teleport.
        """
        try:
            m = int(self.current_map or 0)
        except (TypeError, ValueError):
            return None
        if not m:
            return None
        for ev in (getattr(config, "EVENTS", {}) or {}).values():
            if not isinstance(ev, dict) or not (ev.get("exit") or {}).get("out_map"):
                continue
            dest = int(ev.get("dest_map") or 0)
            top = int(((ev.get("party_battle") or {}).get("top_map")) or 0)
            if dest and top and dest <= m <= top:
                return ev
            if m in (dest, int(ev.get("staging_map") or 0)) and m:
                return ev
        return None

    def _di_bo_ra_khoi_map_event(self) -> bool:
        """Dang trong map event thi DI BO ra (exit_event). True = da ra / von o ngoai."""
        ev = self.event_dang_dung_trong()
        if ev is None:
            return True
        # DANG LEO THAP thi TUYET DOI khong tu di ra: di ra la mat het tang da leo. Vong leo thap
        # tu lo viec ra ngoai khi xong (xem `event_exit_now` trong run_party_digioi).
        if getattr(self, "_floor_crawl_started", False):
            log.info("[%s] go_to_town: dang LEO THAP (map %s) -> khong tu di ra, de vong 2K lo",
                     self._label, self.current_map)
            return False
        log.info("[%s] Dang o map event '%s' (map %s) -> teleport bi CHAN, di bo ra truoc",
                 self._label, ev.get("label") or "?", self.current_map)
        try:
            return bool(self.exit_event(ev))
        except Exception as e:
            log.warning("[%s] loi di bo ra khoi map event: %s", self._label, e)
            return False

    def exit_event(self, ev) -> bool:
        """Ra khoi event bang toa do server moi va smart scene route tu du lieu map."""
        ex = ev.get("exit") if ev else None
        if not ex:
            return False
        out_map = int(ex.get("out_map", 0))
        source_map = self.current_map
        if not out_map or source_map is None:
            log.warning("[%s] exit_event: thieu source/out map", self._label)
            return False
        source_map = int(source_map)
        log.info("[%s] exit_event smart route: %s -> %s", self._label, source_map, out_map)
        self.flee_mode = True
        if not self.refresh_server_position(source_map):
            return False
        if not self.follow_smart_scene_route(
            source_map, out_map, safe=None, flee=True, refresh_position=False
        ):
            log.warning("[%s] exit_event: khong di duoc %s -> %s tu pos=%s",
                        self._label, source_map, out_map, self.pos)
            return False
        return self.current_map == out_map

    def _event_gate(self, x: int, y: int, idx: int, dest: int, max_transit: int = 22) -> bool:
        """Qua cong VAO EVENT - KHAC _enter_gate thuong: event co CINEMATIC (battle-flow) nen sau khi
        gui 0x14 08[idx] phai gui NHIEU 0x14 0600 de CHAY HET cutscene (S2C tra 0x14 0100... tung buoc,
        toi buoc cuoi moi co 0x14 0700 END + 0x03 spawn map moi). KHONG dung som khi map vua doi ->
        char con giua cutscene = 'dang ban', khong moi party duoc. KHONG gui 0x14 04 (nhu _enter_gate)
        -> server kick. Replay dung capture tay: gui 0x14 06 lien tuc, toi map dest -> xin roster
        (0x0c) -> FLUSH them vai transit cho cutscene ket han roi moi return."""
        if not self._wait_combat_clear(idle=3.0):
            return False
        if x or y:
            self.move_to(x, y); time.sleep(1.2)
        self.send(0x14, b"\x08\x00" + bytes([idx & 0xFF]) + b"\x00"); time.sleep(0.6)
        arrived_at = None
        for i in range(max_transit):
            if not self.running:
                return False
            if self.current_map == dest:
                if arrived_at is None:
                    arrived_at = i
                    self.send(0x0c, b"\x01\x00"); time.sleep(0.4)   # xin roster sau khi doi map
                if i - arrived_at >= 3:   # da toi + FLUSH them 3 transit -> cutscene het han
                    self.pos = None
                    log.info("[%s] qua cong event idx=%d -> map %s (cutscene xong)",
                             self._label, idx, self.current_map)
                    return True
            self.send(0x14, b"\x06\x00"); time.sleep(0.6)   # DAY cinematic (tung buoc, cho server tra)
        return self.current_map == dest
