"""PARTY TRAIN DI GIOI - flow tu dong day du.

Flow moi party (slot 0 = chu party / leader, slot 1-4 = member):
  1. Login het cac acc trong party + ket noi game.
  2. Moi acc VAO DI GIOI (solo - KHONG vao duoc khi dang trong party).
  3. Leader chon KENH IT NGUOI nhat -> chia se -> ca party chuyen sang kenh do.
  4. Leader MOI 4 member (quet index nguoi gan; member tu accept qua entity cung party).
  5. Leader CHAY LONG VONG (run-around) den het gio; member tu follow + tu danh.

Chay:  python run_party_digioi.py [so_phut]   (mac dinh chay vo han)
"""
import os, sys, time, json, logging, threading, random, math
try:
    sys.stdout.reconfigure(encoding="utf-8"); sys.stderr.reconfigure(encoding="utf-8")
except Exception:
    pass
from . import config
from . import mob_spots
from . import train_pick
from . import loandau
from . import npc40
from . import train_pick as train_pick_mod   # alias: trong setup_party_runtime co tham so ten train_pick
from .mob_scanner import MobScanSession, compute_regions, scan_full_map
from .scene_fight import get_scene_fight_seed
from .train_maps_store import save_learned_regions
from .login import login
from .client import (ATTR_KEY_TO_CODE, ATTR_CODE_TO_TEN, ATTR_KINDS,
                        save_point_cache, load_point_cache,
                        load_cat_do_items, save_cat_do_items, CAT_DO_CAT, CAT_DO_LAY,
                        save_skill_char_cache, load_skill_char_cache,
                        GameClient, check_duplicate_accounts, joined_member_count, is_joined,
                        is_strategist, reset_party_joined, unmark_joined, mark_joined,
                        set_account_activity, get_account_activity, get_account_task,
                        in_instance_map, dat_pha_pho_ban, dang_pha_pho_ban,
                        dat_party_dang_gom, party_dang_gom, dat_nguoi_keo,
                        DISCONNECT_RATE_LIMIT, TEAM_DUNGEON_MAPS)

_lvl = logging.DEBUG if os.environ.get("DEBUG") else logging.INFO
try:
    # Android: "party.log" (duong dan tuong doi) ghi vao "/" - READ-ONLY tren Android (BUG THAT:
    # OSError Errno 30). Phai ghi vao thu muc rieng cua app (Context.getFilesDir(), xem _appdir.py
    # ben APK). Tren PC import nay FAIL (bot/ khong co _appdir) -> fallback "party.log" nhu cu.
    from ._appdir import app_dir as _app_dir
    _log_path = os.path.join(_app_dir(), "party.log")
except Exception:
    _log_path = "party.log"
# CHAY DUOI UNITTEST -> GHI FILE KHAC. Import module nay trong test se mo DUNG `party.log` ma bot
# THAT dang chay ghi vao, lai con `mode="w"` (truncate). Hau qua da xay ra 07/09: log cua user bi
# tron dong cua test (`[party 78] ... acc0/acc1/acc2`, mot party khong ton tai trong config) va bi
# cat cut - trong khi chinh cai log do dang duoc dung de chan doan loi that.
if "unittest" in sys.modules or os.environ.get("ATS_TEST"):
    _log_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "party_test.log")
# Android chi can log gan nhat de debug. Giu 1 file hien tai + 2 backup, moi file 8MB
# (toi da khoang 24MB) de may cam lau ngay khong day bo nho. mode="w" con don log moi lan
# process bot khoi dong lai.
from logging.handlers import RotatingFileHandler as _RotLog
_file_handler = _RotLog(_log_path, mode="w", maxBytes=8 * 1024 * 1024, backupCount=2,
                        encoding="utf-8")
logging.basicConfig(level=_lvl, format="%(asctime)s %(message)s", datefmt="%H:%M:%S",
                    handlers=[_file_handler, logging.StreamHandler()])
log = logging.getLogger("partydg")

check_duplicate_accounts(config.PARTIES)   # bao loi neu 1 user dien trung nhieu noi

MINUTES = int(sys.argv[1]) if len(sys.argv) > 1 else 0   # 0 = vo han

# Trang thai chia se theo tung party: kenh leader chon + co hieu cac buoc
_party_state = {}   # party_idx -> {"channel": ch, "channel_ready": Event, "invited": Event}
_clients = []
_threads = []   # thread tung acc - de biet khi nao TAT CA da thoat
DIGIOI_LIMIT = 120   # so phut Di Gioi/ngay (de tinh "con lai")
HO_PHU_CHECK_SEC = 180   # Di Gioi Ho Phu: check moi 3 phut (login + dinh ky)
# Phuc Than: chay theo SU KIEN (buff tut < 5 / ngoc hong -> client.phuc_than_pending). So nay chi
# la LUOI AN TOAN khi server khong gui goi - truoc day la 1800 (30 phut) va la duong CHINH nen
# phan ung rat cham (ngoc hong phut thu 1 -> mat he so EXP toi 29 phut).
PHUC_THAN_CHECK_SEC = 300


def _map_cau_hinh(raw, gia_tri_bool=False):
    """Map cau hinh tu APK: nhan CHUOI "k=v" moi dong (hoac dict, cho ban PC/test).

    APK **KHONG duoc truyen thang Map/List** qua Chaquopy: ban release bi R8 rut gon ten lop nen
    Python nhan mot object Java, khong phai dict. Hai kieu hong, ca hai deu da xay ra that:
      - `dict(raw)`            -> `TypeError: 'w' object is not iterable` (CRASH, loi APK 04/09;
                                  truoc do la `'t' object` o APK 1.1.202608181827).
      - `isinstance(raw, dict)` -> False -> tra {} IM LANG, tick cua user bi bo qua khong bao gi.
    Nen moi cho truyen map/list qua Chaquopy phai NOI CHUOI o Kotlin (giong `leaders`,
    `eventExchangeItems`, `mobElements`, `save_cat_do_items_str`) va parse lai o day.

    `gia_tri_bool=True`: gia tri "1"/"true" -> True, con lai False (dung cho `box_modes` -
    `tu_mo_hop_trang_bi` doc gia tri kieu bool; de nguyen chuoi thi "false" cung THANH TRUTHY).
    """
    if isinstance(raw, dict):
        out = dict(raw)
    elif isinstance(raw, str):
        out = {}
        for dong in raw.splitlines():
            dong = dong.strip()
            if not dong or "=" not in dong:
                continue
            k, v = dong.split("=", 1)
            out[k.strip()] = v.strip()
    else:
        return {}
    if gia_tri_bool:
        out = {k: (str(v).strip().lower() in ("1", "true", "yes", "on") if not isinstance(v, bool)
                   else v)
               for k, v in out.items()}
    return out


def _scroll_modes_map(raw):
    """{"0xc946": "drop"} (config) -> {51526: "drop"} (client). Chi chua muc user DA DOI khac
    mac dinh (mac dinh: cuon cua tuong co vkcd = keep, con lai = drop) nen cuon moi cua game tu
    theo mac dinh, khong bat user tick lai."""
    out = {}
    if isinstance(raw, dict):
        for k, v in raw.items():
            if v not in ("keep", "drop"):
                continue
            try:
                out[int(k, 16) if isinstance(k, str) and k.lower().startswith("0x") else int(k)] = v
            except Exception:
                pass
    return out


def _jitter(pt):
    """Xê dịch tọa độ ±10 ngẫu nhiên (9 khả năng) để bot không đứng cùng 1 điểm."""
    dx, dy = random.choice([-10, 0, 10]), random.choice([-10, 0, 10])
    return (pt[0] + dx, pt[1] + dy)


def _xa_diem_quai(pos, spot, nguong=60):
    """Vi tri hien tai co XA diem quai khong (de biet 'du party ma khong danh' la do dung sai cho).

    Nguong 60 > bien do _jitter (+-10) nen dung dung o spot khong bao gio bi coi la xa. pos=None
    (chua biet toa do) -> coi la KHONG xa: khong ro thi dung keo di lung tung.
    """
    if not pos or not spot:
        return False
    return abs(pos[0] - spot[0]) > nguong or abs(pos[1] - spot[1]) > nguong


def _nearest_safe(pos, safes):
    """Diem safe gan vi tri 'pos' nhat (khoang cach binh phuong). pos=None -> diem dau."""
    if not safes:
        return None
    if not pos:
        return safes[0]
    px, py = pos
    return min(safes, key=lambda s: (s[0] - px) ** 2 + (s[1] - py) ** 2)


def _resolve_train_safe(client, map_id, configured_safes):
    ground = client.get_ground_store()
    fingerprint = ground.map_fingerprint(map_id) if ground is not None else None
    if fingerprint:
        cached = mob_spots.load_safe(map_id, fingerprint)
        if cached is not None:
            return cached
    valid = [tuple(map(int, point)) for point in configured_safes or ()
             if len(point) == 2]
    return _nearest_safe(getattr(client, "pos", None), valid)


def _needs_train_safe_bootstrap(login_map, map_id, train_safes):
    return login_map == map_id and not train_safes


def _capture_arrival_safe(client, map_id, came_from_other_map):
    if not came_from_other_map or client.current_map != map_id:
        return None
    ground = client.get_ground_store()
    if ground is None:
        return None
    fingerprint = ground.map_fingerprint(map_id)
    if not fingerprint:
        return None
    cached = mob_spots.load_safe(map_id, fingerprint)
    if cached is not None:
        return cached
    # SAU WARP `pos` bi xoa (=None) va chi co lai khi server gui 0x03 resync. Map dich KHONG co
    # safe cau hinh -> khong navigate (khong cho pos). Cho pos ngan (5s); neu co -> safe = o di
    # duoc gan pos. Neu pos VAN None (vd 14861: leg khong co target_arrival + 0x03 den tre) ->
    # KHONG bo cuoc: dung SEED SceneFight lam safe (diem walkable chuan cua map, cung la noi mob
    # probe di toi). Truoc day thieu fallback nay -> 'khong lay duoc safe' -> TAT PARTY oan
    # (train rung Tan Quan 1 den noi out het).
    _t0 = time.time()
    while not client.pos and getattr(client, "running", True) and time.time() - _t0 < 5.0:
        time.sleep(0.2)
    safe = None
    if client.pos:
        arrival = tuple(map(int, client.pos))
        safe = ground.nearest_walkable_world(map_id, arrival, arrival)
    if safe is None:
        seed = get_scene_fight_seed(map_id)
        if seed is not None:
            safe = ground.nearest_walkable_world(map_id, tuple(map(int, seed)),
                                                 tuple(map(int, seed))) or tuple(map(int, seed))
    if safe is None:
        return None
    safe = tuple(map(int, safe))
    mob_spots.save_safe(map_id, fingerprint, safe)
    log.info("[%s] map %s hoc safe sau warp = %s%s",
             getattr(client, "_label", ""), map_id, safe,
             "" if client.pos else " (tu SEED SceneFight - pos chua ve)")
    return safe


def _train_route_available(smart_route, legacy_route, has_leader):
    return bool(
        smart_route
        or legacy_route
        or (has_leader and getattr(config, "SMART_WORLD_ROUTING", True))
    )


def _needs_train_mob_probe(client, map_id, train_map):
    # XOA safe+mobs = yeu cau quet lai. `pick_train_spot` UU TIEN CAO NHAT map chua co diem nao
    # trong khoang level, nen map do duoc chon ngay va party den quet - khong bi loai khoi vong
    # quay (day la ly do phai co uu tien do; thieu no thi khong ai toi = khong bao gio quet).
    # `rescan` = duong danh dau phu: giu diem cu de van train duoc, nhung toi noi thi quet lai.
    return bool(train_map.get("rescan")) or not bool(train_map.get("mobs"))


def _stationary_train_mob_probe(client, map_id, train_map=None, stop=None, seconds=None,
                                clock=time.monotonic, sleep=time.sleep):
    stop = stop or (lambda: False)
    seconds = float(seconds if seconds is not None else getattr(
        config, "MOB_PACKET_PROBE_SECONDS", 60
    ))
    ground = client.get_ground_store()
    fingerprint = ground.map_fingerprint(map_id) if ground is not None else None
    seed = get_scene_fight_seed(map_id)
    party_entities = (client.known_party_entities()
                      if hasattr(client, "known_party_entities") else set())
    session = MobScanSession(
        map_id, getattr(client, "self_entity", None), party_entities,
        quiet_seconds=0.0,
        min_samples=int(getattr(config, "MOB_SCAN_MIN_SAMPLES", 3)),
        max_patrol_diameter=float(getattr(
            config, "MOB_SCAN_MAX_PATROL_DIAMETER", 800
        )),
        merge_distance=float(getattr(config, "MOB_SCAN_MERGE_DISTANCE", 200)),
    )
    started = clock()
    completed = False
    session.begin_station(started)
    client.begin_mob_observation(session)
    path, count = None, 0
    _lb = getattr(client, "_label", "")
    stations = []          # khai bao TRUOC try: doan ve anh o duoi con dung
    try:
        channel = int(getattr(client, "current_channel", 0)
                      or getattr(config, "CHANNEL", 1) or 1)
        client.switch_channel(channel)
        # DUNG 1 CHO LA DU. Do lai tren capture map 20801: 30s dau tai MOT diem da thay DU
        # 16/16 bai quai (va 16/16 co safe). Di them 5 diem nua ton 5 phut ma con TE HON
        # (quan sat nhieu -> vung 'hazard' phinh -> kho tim safe, chi con 14/16).
        # Truoc day tuong thieu bai la do quet khong het map -> SAI: nguyen nhan that la
        # thuat toan GOM bai theo khoang cach (da sua: 1 con quai = 1 bai).
        if seed is not None:
            stations.append((int(seed[0]), int(seed[1])))
            log.info("[%s] AUTO LEARN map %s: di toi seed SceneFight %s, quan sat %.0fs",
                     _lb, map_id, seed, seconds)
            client.navigate_to(*seed, flee=True, abort=stop)
        else:
            cur = getattr(client, "pos", None)
            if cur:
                stations.append((int(cur[0]), int(cur[1])))
            log.warning("[%s] AUTO LEARN map %s: khong co SceneFight seed, quan sat tai cho %.0fs",
                        _lb, map_id, seconds)
        next_progress = 10.0
        while getattr(client, "running", False) and not stop():
            elapsed = clock() - started
            if elapsed >= seconds:
                completed = True
                break
            if elapsed >= next_progress:
                log.info("[%s] AUTO LEARN map %s: %.0f/%.0fs, %d entity ung vien",
                         _lb, map_id, elapsed, seconds, session.candidate_count())
                next_progress += 10.0
            sleep(min(1.0, max(0.0, seconds - elapsed)))
    finally:
        path, count = client.finish_mob_packet_capture()
        client.end_mob_observation(session)
    start = seed or getattr(client, "pos", None) or (0, 0)
    configured_safes = [tuple(map(int, point)) for point in
                        ((train_map or {}).get("safe", []) or [])
                        if len(point) == 2]
    fallback_safe = _nearest_safe(start, configured_safes)
    if fallback_safe is None and fingerprint:
        fallback_safe = mob_spots.load_safe(map_id, fingerprint)
    learned = compute_regions(
        session, ground, start, fallback_safe=fallback_safe,
        now=clock(), stable_only=False   # thoi diem SAU khi quet xong tat ca diem
    )
    centers = [region.center.point for region in learned]
    safes = [region.safe for region in learned]
    # ANH KET QUA SCAN (chi PC) - de mat nguoi kiem tra nhanh: dia hinh + duong chay tung con
    # quai + bbox + tam bai + safe + cac tram da dung quan sat.
    try:
        from . import scan_image
        _img = scan_image.render_scan(ground, map_id, session.bounded_traces(),
                                      centers, safes, stations=stations)
        if _img:
            log.info("[%s] AUTO LEARN map %s: anh ket qua -> %s", _lb, map_id, _img)
    except Exception as e:
        log.warning("[%s] AUTO LEARN map %s: khong ve duoc anh (bo qua): %s", _lb, map_id, e)
    complete_regions = bool(completed and centers and all(safe is not None for safe in safes))
    if complete_regions and train_map is not None:
        safes = [tuple(map(int, safe)) for safe in safes]
        centers = [tuple(map(int, center)) for center in centers]
        if save_learned_regions(config.TRAIN_MAPS_PATH, map_id, safes, centers):
            train_map["safe"] = safes
            train_map["mobs"] = centers
            for center, safe in zip(centers, safes):
                log.info("[%s] AUTO LEARN map %s: bai %s -> safe %s",
                         getattr(client, "_label", ""), map_id, center, safe)
        else:
            log.warning("[%s] AUTO LEARN map %s: khong ghi train_maps (map da co bai hoac file loi)",
                        getattr(client, "_label", ""), map_id)
    if not centers:
        log.warning("[%s] AUTO LEARN map %s: chua thay trace quai hop le "
                    "(%d packet, file %s)", getattr(client, "_label", ""),
                    map_id, count, path or "khong co")
    return centers


def _resolve_train_mob_centers(client, map_id, train_map, stop=None):
    """Use learned/configured centers; otherwise capture packets while stationary."""
    fallback = [tuple(map(int, point)) for point in (train_map.get("mobs", []) or [])]
    if not getattr(config, "MOB_SCAN_ENABLED", True):
        return fallback
    stop = stop or (lambda: False)
    if fallback:
        log.info("[%s] map %s dung %d diem quai config (khong quet map)",
                 getattr(client, "_label", ""), map_id, len(fallback))
        return fallback
    if stop():
        return []
    return _stationary_train_mob_probe(
        client, map_id, train_map=train_map, stop=stop
    )


def _wait_for_rally(event, stopped, running, st=None, label="", pidx=None):
    """CHO leader chot diem tap ket - NHUNG van NGHE duoc lenh dieu phoi.

    Ban cu: `while not event.wait(2.0)` chi thoat khi Stop / mat ket noi. Vo han, KHONG LOG MOT
    DONG NAO, va DIEC voi moi lenh cap party. Member roi vao day thi bien mat khoi log - nhin
    tuong treo, ma dieu phoi thi ra lenh gom deu deu cho khong ai nghe.

    Ca that party 50, 14/09 (user: "no van dung o thanh"):
        15:15:06 [dakba] (member) SAI MAP (o 15001, can 15861) -> ra vong chinh de nghe lenh
        ... IM HOAN TOAN 16 phut, khong mot dong log ...
        15:22:41 / 15:25:42 / 15:28:42 / 15:31:43 [party 50] REFORM gen -> 4,5,6,7 - party dang o
                                                  2 MAP khac nhau [12001, 15001] -> gom ve cung map
    dakba cho `rally_ready`; leader `dakmot` con o 12001 nen khong bao gio chot rally. Hai ben cho
    nhau, ca party dung im.

    GIO: co lenh MOI (reform_gen tang) -> tra False de acc ra vong chinh thi hanh. Thoat kieu nay
    KHONG phai acc tu quyet - no dang di THI HANH lenh, va lenh do la cua dieu phoi.
    """
    _g0 = int((st or {}).get("reform_gen", 0) or 0)
    _t0 = time.time()
    _log_luc = 0.0
    while not event.wait(2.0):
        if stopped() or not running():
            return False
        if st is not None:
            _g = int(st.get("reform_gen", 0) or 0)
            if _g > _g0:
                log.info("[%s] cho leader chot diem tap ket %.0fs thi CO LENH MOI (reform_gen %d "
                         "-> %d) -> ra vong chinh thi hanh", label or "?", time.time() - _t0,
                         _g0, _g)
                return False
            _viec = (_ke_hoach(st) or {}).get("viec")
            if _viec in (VIEC_GOM, VIEC_DONG_BO):
                log.info("[%s] cho leader chot diem tap ket %.0fs thi DIEU PHOI ra lenh '%s' -> ra "
                         "vong chinh thi hanh", label or "?", time.time() - _t0, _viec)
                return False
        # KHONG im lang: cho lau ma khong log thi nhin log tuong acc chet.
        if time.time() - _log_luc > 60.0:
            _log_luc = time.time()
            log.info("[%s] dang cho leader chot diem tap ket (%.0fs)%s", label or "?",
                     time.time() - _t0, "" if pidx is None else " [party %d]" % (pidx + 1))
    return True


def _go_town_safe(c, label, city_id=12001, flag=0):
    """SACH TRAN (flee) roi BAY VE THANH (mac dinh Trac Quan 12001) - dung khi digioi HET GIO bi
    ket o map quai. Phai cho het tran TRUOC khi teleport (teleport luc dang danh -> server KICK)."""
    c.flee_mode = True
    try:
        c._wait_combat_clear(idle=2.0, cap=15.0)   # flee het tran truoc khi teleport
    except Exception:
        pass
    try:
        c.go_to_town(city_id, flag)
    except Exception as e:
        log.warning("[%s] ve thanh (het gio DG): %s", label, e)


def _use_consumables(c):
    """Hoi HP/SP sau tran (goi NGOAI tran). Bot tu hoc item qua self-calibrate, khong can config.
    - CHAR: closed-loop tren HP/SP live (S2C 0x08) + probe item chua biet de tu hoc.
    - PET: best-effort dung item DA HOC (khong do duoc HP pet ngoai combat -> tinh theo 0x33 cuoi)."""
    c.do_heal()   # hoi char + pet, moi con tu probe/do bang HP cua chinh no

# ==== REGISTRY cho GUI dieu khien tung acc ====
account_clients = {}   # username -> GameClient (doc trang thai live)
account_stops = {}     # username -> threading.Event (GUI yeu cau dung acc nay)
account_threads = {}   # username -> Thread
account_last = {}      # username -> {"map","char"} luc CUOI truoc khi thoat (de biet thoat o dau)
account_exit_reason = {}  # username -> ly do thoat (de tong ket 1 dong khi ca party tat het)
account_furnace_notify = {}  # username -> list item lo can BAO (mode notify) de GUI popup hoi mua

# SAFE BI QUAI DANH: username -> {map, safe, so_tran, tu_luc, lan_bao}
# Diem safe la cho DUNG NGHI giua cac tran. Hoc sai thi acc dung do chiu tran vo han: hoc bang
# `nearest_walkable_outside(clearance=200, max_path=600)` - tuc chi cach cac VET TUAN TRA DA QUAN
# SAT duoc 200, ma lan quet co the chua thay het duong quai di.
#
# Ca that 07/09 party 3, map 21852 (Trai Di Lang2): `safe[2]=(2090,1170)` cach `mob[2]=(1830,1250)`
# dung 272. Ca 5 acc login ve dung safe do roi an don: 147 luot `combat=True` tai (2090,1170) trong
# 19 phut, vi tri chi dao dong 20 don vi - khong he chay di dau.
#
# User chot 07/09: KHONG tu doi safe, KHONG tu quet lai map ("t deo tin may scan lai la on") -
# chi BAO cho user tu kiem. Tinh nang chung cho MOI map.
account_safe_canh_bao = {}
SAFE_DANH_NGUONG = 8       # so tran tai safe truoc khi bao
SAFE_DANH_CUA_SO = 600.0   # ...trong bay nhieu giay
SAFE_GAN = 120             # coi la "dang dung o safe" khi cach safe duoi bay nhieu don vi

# QUANG TRUONG Trac Quan - cho server NHA NGUOI RA sau Di Gioi / sau event (map loan dau 10991 ->
# 12003). KHONG train duoc, KHONG phai thanh (`is_teleport_city(12003)` = False) nen khong the lay
# lam diem tap ket: gom ca party ve day roi cung khong co viec gi lam.
MAP_TRUNG_CHUYEN = 12003


def _ghi_nhan_tran_tai_safe(username, map_id, pos, safe, pos_dang_tin=True):
    """Acc vua VAO TRAN khi dang dung o safe -> dem. Qua nguong thi ghi canh bao cho GUI.

    Chi DEM va BAO - khong tu doi safe, khong tu quet lai map.

    `pos_dang_tin=False` -> BO QUA. `c.pos` la so DEAD-RECKONING bot tu cong sau moi lenh move; khi
    lenh bi tran nuot thi acc dung nguyen cho cu ma so do van chay tiep toi dich. Dem bang so do la
    dem tran cua mot cho KHONG PHAI safe, roi bao "safe hoc SAI" - bao oan.

    User 08/09: "hom qua t check map khac, bot bao safe bi danh, t log vao thi ko he bi danh, chung
    to cai di chuyen ra safe cua may ngu - dang di chuyen ma vao battle thi no bi mat di chuyen
    nhung m van tinh la di chuyen den safe ok".
    """
    if not safe or not pos or not map_id or not pos_dang_tin:
        return
    try:
        if math.dist(tuple(pos)[:2], tuple(safe)[:2]) > SAFE_GAN:
            return
    except Exception:
        return
    now = time.time()
    rec = account_safe_canh_bao.get(username)
    _key = (int(map_id), tuple(int(v) for v in tuple(safe)[:2]))
    if (rec is None or rec.get("key") != _key
            or now - float(rec.get("tu_luc") or 0) > SAFE_DANH_CUA_SO):
        rec = {"key": _key, "map": int(map_id), "safe": list(_key[1]),
               "so_tran": 0, "tu_luc": now, "lan_bao": 0.0}
        account_safe_canh_bao[username] = rec
    rec["so_tran"] += 1
    if rec["so_tran"] >= SAFE_DANH_NGUONG and now - float(rec.get("lan_bao") or 0) > SAFE_DANH_CUA_SO:
        rec["lan_bao"] = now
        rec["luc"] = now
        log.warning("[%s] SAFE BI QUAI DANH: %d tran trong %.0f phut ngay tai safe %s cua map %s "
                    "-> diem safe nay co the hoc SAI, user kiem lai giup",
                    username, rec["so_tran"], (now - rec["tu_luc"]) / 60.0, rec["safe"], rec["map"])


def safe_canh_bao_items(pidx):
    """[{user, map, safe, so_tran, luc}] - safe bi quai danh cua CA party pidx (cho UI)."""
    out = []
    try:
        accs = party_accounts(pidx)
    except Exception:
        return out
    for tpl in accs:
        u = tpl[0] if isinstance(tpl, (tuple, list)) else tpl
        rec = account_safe_canh_bao.get(u)
        if rec and rec.get("lan_bao"):
            out.append({"user": u, "map": rec["map"], "safe": rec["safe"],
                        "so_tran": rec["so_tran"], "luc": rec.get("luc") or rec["lan_bao"]})
    return out


def safe_canh_bao_bo_qua(username):
    """User da xem/sua xong -> bo canh bao cua acc nay."""
    account_safe_canh_bao.pop(str(username or "").strip(), None)
    return True
account_stop_reasons = {}  # username -> ai/nhanh nao set stop_ev gan nhat
account_reconnect = {}
# username -> client CON SONG, trao tay giua 2 pha (DG -> train) de KHOI dang nhap lai.
account_continue = {}     # username -> client CON SONG, trao tay giua 2 pha (DG -> train).
                          # Co gia tri = DOI PHA tai cho, KHONG phai rot: supervisor chay lai
                          # run_account NGAY voi chinh ket noi do, khong dang nhap lai.
account_forced_reconnect = set()  # survivor 40NPC bi dong de relogin cung party; KHONG tang disc_gen
account_forced_reconnect_reason = {}
account_sync_epoch = {}   # username -> epoch dang chay; != st["sync_epoch"] => ep dong bo (relogin)
# username -> so lan DA relogin vi 'ket ngoai DG nhung server con gio'. PHAI de o day (khong
# phai tren client): moi lan relogin, run_account tao GameClient MOI -> dem tren `c` se reset
# ve 0 -> RELOGIN VO HAN va party treo o hang rao 'x/5 acc xong DG' (dung bug cu can tranh).
_dg_stuck_relogin = {}
_start_cancel_generation = 0  # STOP ALL tang so nay de huy chuoi START dang do


class ResyncSignal(BaseException):
    """EP DONG BO - uu tien TUYET DOI. Ke thua BaseException (KHONG phai Exception) de KHONG bi
    cac `except Exception` sau trong barrier nuot mat -> unwind xuyen MOI vong cho vo han, ra thang
    supervisor. Supervisor coi nhu forced-reconnect -> relogin -> duong reconnect tu bam leader
    (clear sach hanh dong + tham so cu, dung nhu yeu cau: dong bo uu tien cao nhat)."""
    pass


def _resync_ck(st, username):
    """Goi trong MOI vong cho vo han (barrier report/relogin PB, reform, sync kenh, keo route, DG...).
    Khi sync_epoch bi bump (leader/GUI/watchdog ep dong bo) -> raise ResyncSignal ngay lan check ke."""
    if account_sync_epoch.get(username) != st.get("sync_epoch", 0):
        raise ResyncSignal


BARRIER_STUCK_SECS = 180.0   # barrier ket qua lau (khong nhich) -> AUTO ep dong bo (watchdog)
RESYNC_COOLDOWN = 300.0      # khong AUTO-resync cung party qua 1 lan / 5p (chong loop relogin)


RESYNC_SOFT_TRIES = 2   # so lan ep dong bo NHE truoc khi buoc phai relogin (nang)


def request_party_resync(pidx, reason="ép đồng bộ", cooldown=0.0, hard=False):
    """EP CA PARTY dong bo lai theo leader.

    NHE (mac dinh) - bump reform_gen: acc dang di duong tu DUNG (navigate_to/follow_path/
    follow_smart_route deu nhan abort=_ab co xet reform_gen), roi ca party gom ve cung thanh +
    cung kenh + lap lai party. KHONG login lai.
    NANG (hard=True) - bump sync_epoch: acc dang cho se raise ResyncSignal -> RELOGIN.

    VI SAO DOI MAC DINH: truoc day CHI co duong nang. Log that (10:55, acc chumuoi): acc VUA LOGIN
    XONG, dang dung DUNG map train 14823, chi vi party co acc khac map ma bi "EP DONG BO -> relogin
    bam leader" - trong khi no chi can DI/TELEPORT. Tu khi server chan toc do dang nhap (ma 90),
    relogin thua nhu vay lam acc ket vong dang nhap hang phut (10:55:21 -> 10:56:26 van chua vao
    duoc). User: "tai sao lai phai relogin, chi can chuyen map thoi chu".

    Van GIU duong nang lam BUOC LEO THANG: nhe RESYNC_SOFT_TRIES lan lien tiep ma party van ket
    thi moi relogin.
    cooldown>0 (auto): bo qua neu vua resync trong `cooldown` giay (chong loop).
    Tra (gen_moi, da_dung_hard) hoac None neu bi cooldown chan."""
    st = _pstate(pidx)
    now = time.time()
    with st["lock"]:
        if cooldown and now - st.get("last_resync_ts", 0.0) < cooldown:
            return None
        st["last_resync_ts"] = now
        st["team_dungeon_need_redo"] = False
        _soft = int(st.get("resync_soft_count", 0))
        if not hard and _soft >= RESYNC_SOFT_TRIES:
            hard = True            # nhe mai khong an -> leo thang
        if hard:
            st["resync_soft_count"] = 0
            st["sync_epoch"] = int(st.get("sync_epoch", 0)) + 1
            ep = st["sync_epoch"]
        else:
            st["resync_soft_count"] = _soft + 1
            ep = _bump_reform(st, "ep dong bo (nhe): " + reason)
    if hard:
        log.warning("[party %s] EP DONG BO NANG (%s) -> sync_epoch=%d, moi acc RELOGIN bam leader",
                    pidx, reason, ep)
    else:
        log.warning("[party %s] EP DONG BO NHE (%s) -> reform_gen=%d, gom ve cung map/kenh "
                    "(KHONG relogin)", pidx, reason, ep)
    return ep, hard


def _barrier_watchdog(st, pidx, t0, tag):
    """Goi moi vong cho: barrier ket > BARRIER_STUCK_SECS -> AUTO ep dong bo (cooldown chong loop).
    Khong tu raise - vong cho da co _resync_ck ngay sau se bat epoch moi va raise ResyncSignal."""
    if time.time() - t0 > BARRIER_STUCK_SECS:
        request_party_resync(pidx, "watchdog:" + tag, cooldown=RESYNC_COOLDOWN)
LOGIN_ERR1_RETRY_MIN_SEC = 60
LOGIN_ERR1_RETRY_MAX_SEC = 120


def _running_party_usernames(pidx):
    return [u for u, _p, _l, _pk in party_accounts(pidx)
            if is_account_running(u) and account_clients.get(u) is not None]


PARTY_EVENT_DUNGEON_WINDOW = 3600.0   # do dai cua so "dang trong kich ban dungeon" cho event party


def _inside_floor_crawl_tower(ev, map_id):
    """Dang DUNG SAN trong thap cua event floor_crawl (2K) chua? (dest_map <= map <= top_map)

    Dung khi login lai giua chung: game GIU nguyen vi tri trong thap. Neu con o trong thap thi
    KHONG duoc chon lai event - goi 0x4d se keo ca doi ve map cho 12921 va MAT HET tang da leo
    (xac nhan log 11:23: acc dang o 12922, sau khi chon event thi smart path chay tren 12921).
    """
    pb = (ev or {}).get("party_battle") or {}
    if pb.get("kind") != "floor_crawl":
        return False
    dest = int((ev or {}).get("dest_map") or 0)
    top = int(pb.get("top_map") or 0)
    try:
        m = int(map_id or 0)
    except (TypeError, ValueError):
        return False
    return bool(dest and top and dest <= m <= top)


def _party_same_map(st, username, cur_map, expected, stopped, label="", role="", wait=30.0,
                    pidx=None):
    """TRUOC KHI SYNC KENH: ca party phai dang o CUNG MOT MAP.

    Sync kenh khi cac acc o KHAC map la VO NGHIA: picker chot expected_map = map cua RIENG no,
    cac acc o map khac bao cao "sai map" mai -> vong sync treo (log 11:57: leader vao lai 12922
    trong khi 3 member con o 12931 -> "cho acc bao cao map (1/4)" khong bao gio xong).
    Ap dung cho MOI mode, khong rieng 2K.

    Tra True neu tat ca cung map (hoac chua doc duoc acc nao khac -> khong co gi de doi chieu).
    """
    # BOT TU THAY map cua tung acc. Ban cu bat acc ghi `presync_maps[username]` roi CHO du bao cao
    # (toi `wait` giay) - vua bat bao cao (L2) vua cho (L9), va acc ban viec khac thi khong bao gio
    # bao -> cho het gio roi quyet voi du lieu thieu.
    maps = set()
    try:
        for _t in party_accounts(pidx) if pidx is not None else ():
            _u = _t[0]
            if _u in st.get("reconnecting", ()):
                continue
            _c = account_clients.get(_u)
            if _c is not None and getattr(_c, "running", False):
                maps.add(int(getattr(_c, "current_map", 0) or 0))
    except Exception:
        pass
    if not maps:
        maps = {int(cur_map or 0)}
    if len(maps) <= 1:
        return True
    log.warning("[%s] (%s) BO QUA sync kenh: party dang o KHAC MAP %s -> sync kenh luc nay vo "
                "nghia (picker se cho bao cao map mai khong xong)", label, role, sorted(maps))
    return False


def _manual_whitelist_names(pidx, c=None):
    """Ten whitelist thoa CA BA dieu kien -> moi duoc cam doi kenh:

      1. KHONG phai acc bot nao dang chay (doi chieu account_clients TOAN BO, moi party -
         nick do co the la bot cua party khac, van la bot, van tu doi kenh duoc).
      2. La nick NGUOI CHOI dieu khien tay (he qua cua 1).
      3. DANG DUNG DO THAT: leader thay entity cua no va no o cung map hien tai.

    Bot doi kenh duoc cho chinh no nhung KHONG doi ho nick tay -> doi la bo roi ho o kenh cu,
    ho khong nhan duoc loi moi party (bug that 15:03). NHUNG chi cam khi ho CO MAT: mot nick
    khong dung do thi giu nguyen kenh chang cuu duoc ai, chi lam party nam rai nhieu kenh ->
    3/4 member khong nhan duoc loi moi -> 1/4 -> giai tan -> lap vo tan (bug that 17:25:
    member o kenh 3/3/2, leader kenh 1, whitelist ['tuyet','chihao'] deu KHONG co entity).
    """
    try:
        wanted = (config.leaders_for(pidx) if hasattr(config, "leaders_for")
                  else list(getattr(config, "PARTY_LEADERS", []) or []))
    except Exception:
        wanted = []
    bots = set()
    for u, cli in list(account_clients.items()):
        bots.add(str(u).strip().casefold())
        nm = getattr(cli, "char_name", None)
        if nm:
            bots.add(str(nm).strip().casefold())
    for u in party_accounts(pidx):
        bots.add(str(u).strip().casefold())

    cands = [str(x).strip() for x in (wanted or [])
             if str(x).strip() and str(x).strip().casefold() not in bots]
    if not cands or c is None:
        return []

    out = []
    for name in cands:
        key = name.casefold()
        ent = None
        for e, names in list((getattr(c, "entity_names", None) or {}).items()):
            if any(str(x).strip().casefold() == key for x in (names or ())):
                ent = e
                break
        if ent is None:
            continue                      # chua thay bao gio -> khong co mat -> KHONG cam
        try:
            visible, _why = c._entity_is_visible_on_current_scene(ent)
        except Exception:
            visible = False
        if visible:
            out.append(name)              # co mat that -> cam doi kenh
    return out


def _party_chet_het(pidx):
    """CA PARTY co chet trong tran vua roi khong - doc HP cua CHINH TUNG ACC.

    Server KHONG gui goi "thang/thua": `S:011-000 <結束戰鬥>` chi mang `roleId + npcIndex`, va
    `FightManager.FightOver` chi `SetWar(EWar.None)`. Client biet chet bang HP
    (`FightField.lua:1034`: `if Hp <= 0 then SetBeh(EFightBeh.Dead)`). Bot lam y het - va lam
    duoc TOT HON client vi ca 5 acc nam trong MOT tien trinh: hoi thang client cua tung dua.

    KHONG dung `npc40.party_defeated(state.allies)`: `allies` la danh sach DONG DOI, chi day du
    khi co `0x0b` party-broadcast va bi `clear()` moi `0x34`. Rong thi no tra
    `bool(known) and alive == 0` = **False** = "khong thua". Log that party 1 (06/09) tang 11:
        15:14:41 [gamo] 2K: xong tran idx=2, party song 0/0     <- ca party vua chet sach
    "0/0" khong phai "song 0 tren 0" ma la "KHONG BIET".

    Moi acc tu chot `chet_tran_nay` LIEN TUC trong tran (xem `client._chot_minh_chet`) vi sau
    tran server hoi/hoi sinh -> doc HP luc do la mat dau vet.
    """
    acc = [account_clients.get(u) for u in _active_party_usernames(pidx)]
    acc = [c for c in acc if c is not None and c.running]
    if not acc:
        return False
    return all(bool(getattr(c, "chet_tran_nay", False)) for c in acc)


def _party_left_tower(pidx, ev):
    """CO acc nao bi VANG khoi thap khong = dau hieu THUA (bay hon -> server day ve out_map).

    party_defeated() doc HP cua allies nen KHONG bat duoc ca nay: acc bay hon bi day ra khoi
    instance, HP cua no van binh thuong -> leader bao "party song 6/6" trong khi thuc te da thua
    (log 13:03: ttbay pos=(502,495) map=12003 con leader van o 12932).
    """
    for u in _active_party_usernames(pidx):
        cli = account_clients.get(u)
        if cli is None or not cli.running:
            continue
        m = int(getattr(cli, "current_map", 0) or 0)
        if m and not _inside_floor_crawl_tower(ev, m):
            log.warning("[P%s] 2K: acc '%s' da VANG khoi thap (map %s) -> coi la THUA", pidx, u, m)
            return True
    return False


def _maps_cua_party(st, pidx):
    """[map] cua cac acc CON SONG trong party - doc THANG client, khong acc nao bao cao.

    Acc dang relogin (`reconnecting`, hoac client da tat) khong tinh: map cua no la so cu."""
    ra = []
    if pidx is None:
        return ra
    try:
        accs = party_accounts(pidx)
    except Exception:
        return ra
    _rec = st.get("reconnecting", ()) if st else ()
    for _t in accs:
        _u = _t[0]
        if _u in _rec:
            continue
        _c = account_clients.get(_u)
        if _c is None or not getattr(_c, "running", False):
            continue
        _m = int(getattr(_c, "current_map", 0) or 0)
        if _m:
            ra.append(_m)
    return ra


def _cung_map_ca_party(pidx, st=None):
    """Ca party (acc con song) co dang o CUNG mot map khong - doc THANG client (L2).

    Doc SAU CUNG, khong dung so chup tu truoc: cac vong cho (ra rally, doi kenh) keo dai hang chuc
    giay, so cu la so cua tinh hinh khac. Chinh cho nay lo ra ca party 10 (10/09): `_tinh` noi
    "lech_kenh" (so cu) trong khi thuc te dang lech MAP.
    """
    _ms = {int(m) for m in _maps_cua_party(st if st is not None else _pstate(pidx), pidx) if m}
    return len(_ms) <= 1


def _2k_regroup_target(st, ev, pidx=None):
    """Tang GOM DOI khi party lech tang: TANG THAP NHAT ma ca doi toi duoc.

    - Ca doi deu trong thap -> min(map): dua o tang thap KHOI PHAI DI, dua tren di bo xuong.
    - Co acc dang o NGOAI event -> phai la cua vao dest_map (12922): acc ngoai tele vao binh
      thuong (go_to_event), acc trong thap di bo xuong day.
    """
    dest = int((ev or {}).get("dest_map") or 0)
    maps = _maps_cua_party(st, pidx)
    if maps and all(_inside_floor_crawl_tower(ev, m) for m in maps):
        return min(maps)
    return dest


def _decide_2k_resume(st, username, cur_map, ev, expected, stopped, label="", pidx=None):
    """Quyet dinh RESUME 2K o CAP PARTY (khong phai tung acc tu quyet).

    Moi acc bao map hien tai; chi RESUME khi CA PARTY deu o trong thap VA CUNG MOT TANG.
    Lech nhau -> ca doi VAO LAI tu 12921 de gom nhau (mat tang da leo, nhung con hon ket).

    Bug that (log 11:56): leader bi day ra 12003 con 3 member con o 12931 -> moi acc tu quyet:
    leader vao lai 12922, member o lai 12931 -> sync map cho vo han (1/4 mai).
    """
    if ((ev or {}).get("party_battle") or {}).get("kind") != "floor_crawl":
        return False   # event khac (40NPC...) -> KHONG dung barrier nay, tranh chan 60s vo ich
    # BOT TU THAY map cua tung acc. Ban cu: moi acc ghi `event_start_map[username]` roi CHO du bao
    # cao toi 60 giay - bat bao cao (L2) + cho (L9), ma acc ban viec khac thi khong bao gio bao.
    maps = _maps_cua_party(st, pidx)
    if not maps:
        try:
            maps = [int(cur_map or 0)]
        except (TypeError, ValueError):
            maps = [0]
    inside = [_inside_floor_crawl_tower(ev, x) for x in maps]
    same = len(set(maps)) == 1
    ok = bool(maps) and all(inside) and same
    if not ok and any(inside):
        log.warning("[%s] 2K: party KHONG cung cho (map=%s) -> ca doi VAO LAI tu dau de gom nhau",
                    label, sorted(set(maps)))
    return ok


def _set_party_quest_mode(pidx, on, label="", quiet=False):
    """Bat/tat quest_mode cho CA party (leader + member).

    Event danh theo party (40NPC, 2K) phai EP quest_mode thay vi de auto-latch quyet dinh:
    latch chi bat khi quai > 6 luc vao tran (state.py) -> tran it quai la mat skill toan man.
    Member KHONG chay vong dieu khien nao ca (bi keo vao tran cua leader) nen phai set ho.

    PHAI set kem `_team_dungeon_until`: het moi tran, handler 0x14 sub0700 goi
    reset_enemies(reset_quest=not _in_team_dungeon) -> THIEU cua so nay thi quest_mode vua ep bi
    XOA ngay sau tran DAU TIEN. Truoc day chi leader co cua so (dat trong floor_crawl) nen chi
    leader giu duoc quest_mode; member tu tran 2 tro di tut ve TRAIN mode - xac nhan qua log:
    tttam tran 1 danh 10012 (skill toan man), tran 2 danh 10005 (Nem Da, combo train).
    => GOI LAI ham nay sau MOI tran de gia han cho ca party.
    """
    n = 0
    until = (time.time() + PARTY_EVENT_DUNGEON_WINDOW) if on else 0.0
    for u in _active_party_usernames(pidx):
        cli = account_clients.get(u)
        if cli is not None and cli.running:
            cli.state.quest_mode = bool(on)
            cli._team_dungeon_until = until
            n += 1
    if not quiet:
        log.info("[%s] quest_mode=%s cho %d acc trong party", label or ("P%s" % pidx), bool(on), n)


def _active_party_usernames(pidx):
    """Acc dang duoc START, ke ca dang reconnect/chua vao world xong."""
    active = []
    for u, _p, _l, _pk in party_accounts(pidx):
        if not is_account_running(u):
            continue
        ev = account_stops.get(u)
        if ev is not None and ev.is_set():
            continue
        active.append(u)
    return active


def _dt_party_usernames(pidx):
    users = []
    for u, _p, _l, _pk in party_accounts(pidx):
        ev = account_stops.get(u)
        if ev is not None and ev.is_set():
            continue
        users.append(u)
    return users


def _login_error_code(exc):
    code = getattr(exc, "error_code", None)
    if code is not None:
        return code
    data = getattr(exc, "data", None)
    if isinstance(data, dict):
        return data.get("error_code")
    return None


def _login_error_message(exc):
    data = getattr(exc, "data", None)
    if isinstance(data, dict):
        msg = data.get("message")
        if msg is not None:
            return str(msg)
    return str(exc)


def _party_exit_summary(pidx, exclude_user):
    """Goi trong finally moi acc. Neu MOI acc khac cua party da tat -> log 1 DONG TONG KET
    o cuoi: party thoat het vi ly do gi (gom theo ly do). Chi log 1 lan/lan-chay."""
    st = _pstate(pidx)
    accs = [u for u, _p, _l, _pk in party_accounts(pidx)]
    for u in accs:
        if u == exclude_user:
            continue
        t = account_threads.get(u)
        if t is not None and t.is_alive():
            return   # con acc khac dang chay -> chua phai ca party tat
    with st["lock"]:
        if st.get("summary_done"):
            return
        st["summary_done"] = True
    # gom username theo ly do; moi nick kem MAP luc thoat -> biet vi tri ca party
    groups = {}
    for u in accs:
        r = account_exit_reason.get(u, "ket thuc binh thuong (het gio hoac GUI dung)")
        last = account_last.get(u, {})
        nm = last.get("char") or u
        mp = last.get("map")
        groups.setdefault(r, []).append(f"{nm}@map{mp}" if mp is not None else f"{nm}@?")
    parts = "; ".join(f"{r} [{', '.join(us)}]" for r, us in groups.items())
    log.warning(">>> PARTY %s DA THOAT HET vi: %s", pidx + 1, parts)
    # them 1 dong liet ke RO map tung nick (de soi nick nao sai map)
    pos = ", ".join(f"{(account_last.get(u, {}).get('char') or u)}=map{account_last.get(u, {}).get('map')}"
                    for u in accs)
    log.warning(">>> PARTY %s vi tri tung nick: %s", pidx + 1, pos)


def _dem_san_sang(pidx):
    """So MEMBER da san sang vao party - DOC THANG tung client (`_san_sang_party`).

    Bot dieu khien acc nen bot BIET dua nao da xong viec vat dau phien; khong acc nao phai ghi ten
    minh vao mot bang cap party (`ready_members` cu). Bang do con om stale qua cac lan relogin."""
    n = 0
    try:
        accs = party_accounts(pidx)
    except Exception:
        return 0
    for _t in accs:
        _c = account_clients.get(_t[0])
        if _c is not None and getattr(_c, "running", False) and getattr(_c, "_san_sang_party", False):
            n += 1
    return n


def _party_map_barrier(st, username, self_ok, expected, stopped, pidx=None, train_map=None):
    """CA PARTY co dang o train map khong - BOT TU THAY, khong acc nao bao cao.

    Ban cu: moi acc ghi map_results[username] = self_ok roi CHO du bao cao (`while True`).
    Hai cai sai cung mot luc - bat acc khai (L2) va cho acc khac (L9). Acc ban viec khac thi khong
    khai -> ca party cho mai.

    Gio doc thang `account_clients[u].current_map`. Khong bang, khong cho, tra loi ngay."""
    if pidx is None or not train_map:
        return bool(self_ok)
    try:
        accs = party_accounts(pidx)
    except Exception:
        return bool(self_ok)
    for _t in accs:
        _u = _t[0]
        if _u in st.get("reconnecting", ()):   # dang relogin -> khong ket toi
            continue
        _c = account_clients.get(_u)
        if _c is None or not getattr(_c, "running", False):
            continue
        if int(getattr(_c, "current_map", 0) or 0) != int(train_map):
            return False
    return True


def _ghi_sync_that_bai(st, username, current_map, sync_gen, expected_map, label=None):
    """Acc thay MINH sai map sau khi doi kenh -> bao HONG vong sync (khong phai "bao cao map").

    Ban cu con ghi ca `channel_map_reports[username] = (ok, map)` de leader DEM du nguoi. Do la
    bang bao cao - vi pham L2: ca party chay chung MOT tien trinh, leader doc thang
    `account_clients[u].current_map` va `kenh_that()` la biet het, khong can ai khai bao. Va bat
    khai bao thi acc nao ban viec khac se khong khai -> leader dem thieu -> TIMEOUT 60s -> reform
    -> lap lai (log that party 18, 23:36-23:43: leader dot 6 phut voi "cho acc bao cao map (1/5)"
    trong khi 4 member da o dung map tu dau).

    Chi con giu MOT viec: dat co HONG, vi do la thu leader khong the tu thay (map dung ma van hong
    thi khong co dau hieu ngoai le)."""
    map_ok = expected_map is None or current_map == expected_map
    with st["lock"]:
        if st.get("channel_sync_gen") != sync_gen:
            return False
        if not map_ok:
            st["channel_failed_reason"] = "%s map=%s, can=%s" % (
                label or username, current_map, expected_map,
            )
            st["channel_failed"].set()
    return map_ok


def _prepare_reform_channel_sync(st):
    """Khong cho member dung channel_ready cua generation truoc khi leader mo sync reform moi."""
    with st["lock"]:
        st["channel_ready"].clear()
        st["channel"] = None


def _dong_vong_sync(st):
    """DONG CUA mot vong dong bo kenh da hong.

    Bat buoc goi o MOI duong thoat that bai cua `do_channel_sync`. Khong dong thi `channel_ready`
    ket o trang thai SET vinh vien, va moi acc dang cho/dang do trong vong deo cung mot lenh da
    chet. Bug that P3 (06/09, ket 1 tieng): batbat vao kenh 15 that bai luc 02:38:31, leader
    thoat vong sync luc 02:38:32 ma khong xoa co -> batbat do lai den 03:34 van chua ra, leader
    lap "CHO du member san sang (3/4)" 28 lan.
    """
    with st["lock"]:
        st["channel_ready"].clear()
        st["channel"] = None
        st["channel_failed"].clear()
        st["channel_failed_reason"] = ""


def _event_battle_kind(mode, has_leader, ev):
    """Kieu danh cua event CO LAP PARTY: 'npc_repeat' (40NPC) | 'floor_crawl' (2K) | None."""
    battle = (ev or {}).get("party_battle") or {}
    kind = battle.get("kind")
    if mode == "event" and has_leader and kind in ("npc_repeat", "floor_crawl"):
        return kind
    return None


def _is_party_event(mode, has_leader, ev):
    """Event can LAP PARTY roi moi danh (hoan moi party + sync kenh LAI tai map event).
    Dung chung cho 40NPC lan 2K - phan khac nhau nam o buoc bat dau danh."""
    return _event_battle_kind(mode, has_leader, ev) is not None


def _is_npc_repeat_party_event(mode, has_leader, ev):
    return _event_battle_kind(mode, has_leader, ev) == "npc_repeat"


def _ev_cua_party(pcfg):
    """Ban ghi event cua party (giong duong `run_account` lay) - dung o cap dieu phoi."""
    try:
        ev = config.event_hom_nay(pcfg.get("event_key") or "")
    except Exception:
        ev = None
    if ev is None:
        _evs = getattr(config, "EVENTS", {}) or {}
        if _evs:
            ev = _evs[next(iter(_evs))]
    return ev


def _mode_can_lap_doi(pidx):
    """MODE NAY CO CAN LAP DOI KHONG - thuoc tinh cua MODE, khong phai cua tinh huong.

    `False` = moi acc tu lam viec cua no, KHONG bao gio can chung doi. Voi mode nhu vay thi TOAN BO
    duong lenh cap party tat han: khong lap doi, khong gom map, khong dong bo kenh, khong bump
    reform. Khong phai "tam thoi bo qua" - la khong co viec do ngay tu dau.

    Hien co MOT mode nhu vay: LOAN DAU (`party_battle.kind == "chaos_vs"`). Moi acc tu dang ky, tu
    ghep tran; roster cua nhau la RAC (party 24, 10/09: `daim01=0 daim02=0 ...`, party 21:
    `dieu906=4 dieu907=1 dieu908=2 dieu909=3 dieu910=4` - moi acc thay mot kieu). Ra lenh cap party
    o day khong chi vo ich ma con PHA:
      * doi kenh  -> mat cho da dang ky, acc cho ghep tran toi het gio roi thoi
        20:23:14 [haba] Loan dau: da dang ky, cho ghep tran   <- dang ky o KENH 4
        20:23:19 [haba] Doi kenh OK -> 2                      <- dieu phoi keo sang kenh 2
        20:38:14 [haba] Loan dau: cho ghep tran qua 900s khong vao -> dung
      * bump reform -> abort acc dang cho ghep tran, cung mat luot

    User 10/09: "ko phai chan, ma dieu phoi phai biet mode nay deo can lap pt". Truoc do toi di vao
    lam guard rai rac o tung ham - moi guard la mot cho co the quen, va da quen that: chan o
    `_dieu_phoi_chot_kenh` roi nhung `_dieu_phoi_quyet` van ra `viec=moi/gom/dong_bo` (party 24).
    """
    try:
        _pc = getattr(config, "PARTY_CONFIG", {}).get(pidx, {}) or {}
        if _pc.get("mode") != "event":
            return True
        _ev = config.event_hom_nay(_pc.get("event_key") or "")
        return _event_solo_battle_kind(_pc.get("mode"), _ev) not in _SOLO_BATTLE_EVENTS
    except Exception as e:
        log.debug("[party %d] loi doc mode co can lap doi: %s", pidx + 1, e)
        return True        # khong biet -> cu coi la CAN (giu hanh vi cu, khong tat nham)


def _party_40npc_ngoai_gio(pidx, pcfg):
    """Party mode 40NPC va DANG NGOAI GIO event -> KHONG con viec gi o cap party.

    Ngoai gio, moi acc chi lam mot viec SOLO: di NPC map 12003 doi 'qua chien dau 40NPC' roi thoat
    game (`claim_40npc_reward`). Khong danh, khong lap doi, khong dung chung kenh.

    Vay ma dieu phoi van chay day du: chot kenh dich, ra lenh doi kenh, va ra lenh GOM vi "lech map
    [10991, 12003]" - trong khi 12003 CHINH LA cho doi thuong, tuc lech map luc do la dung y do.

    Ca that 09/09 sau 22h (user: "mode 40npc, ngoai gio event thi chi log vao va di doi thuong roi
    out, m con phai dong bo kenh lam lon gi"):
        22:00:31 [party 49] gen 26: pha=event map=12003 viec=lam - con lech map [10991, 12003]
        22:00:37 [party 49] gen 27: viec=gom - party dang o 2 MAP khac nhau [10991, 12003]
        22:00:30 [dakbon]   DIEU PHOI GUI doi kenh 2 (dang o 1) -> ket qua 4
        22:00:33 [quanmot]  (LEADER) DIEU PHOI chot kenh 14, minh dang o 1 -> tu chuyen
    Lenh doi kenh keo acc ra khoi viec no dang lam, va con dinh ma 4 (kenh day) nen lap lai mai.
    """
    if (pcfg or {}).get("mode") != "event":
        return False
    has_leader = config.PARTY_LEADER_ACC.get(pidx) is not None
    ev = _ev_cua_party(pcfg)
    if not _is_npc_repeat_party_event("event", has_leader, ev):
        return False
    try:
        return not npc40.in_event_window()
    except Exception:
        return False


# Event SOLO: moi acc TU danh, KHONG lap party, KHONG can leader, khong sync kenh.
_SOLO_BATTLE_EVENTS = ("chaos_vs",)


def _event_solo_battle_kind(mode, ev):
    """Kieu danh cua event SOLO ('chaos_vs' = loan dau loi dai) | None.

    Day la DANG THU BA cua event. Truoc day chi co hai: 'co leader -> lap party roi danh'
    (npc_repeat/floor_crawl) va 'khong leader -> dung yen cho dieu khien tay'. Loan dau khong
    lap party nhung VAN phai tu danh -> phai tach ham rieng.

    KHONG duoc nhet 'chaos_vs' vao `_event_battle_kind`: lam vay se keo theo ca duong lap party
    + sync kenh lai + barrier cua 40NPC/2K, tuc pha duong dang chay cua hai event kia.
    """
    if mode != "event":
        return None
    kind = ((ev or {}).get("party_battle") or {}).get("kind")
    return kind if kind in _SOLO_BATTLE_EVENTS else None


def _loandau_ra_khoi_map(c, ev, label):
    """Ra khoi map loan dau (10991 -> 12003) truoc khi tat game.

    KHONG co buoc doi thuong - server TU trao (user xac nhan 25/08). Nhung van phai ra khoi map
    event: de nguyen trong 10991 thi lan login sau bot bat dau tu map event, khong phai tu thanh.
    Dang o map khac roi thi khong lam gi.
    """
    try:
        # SAN VO GIOI (lien server, THU 7): KHONG phai lam gi ca - cu tat acc.
        #
        # User chot 05/09 sau khi chay that: map event nam tren MAY KHAC, ma login lan sau luon
        # vao MAY GOC -> thoat game la ra khoi map event luon. Khac han thu 3 (map 10991 cung
        # may) - o do khong ra thi lan sau bot khoi dong tu map event chu khong tu thanh.
        #
        # Nen bo han buoc "chay ra NPC roi cho server bao ve may goc": no chi them mot cho co
        # the hong (dialog tren map la, giua luc con dang danh do) ma khong duoc gi.
        if getattr(c, "tren_vo_gioi", False):
            log.info("[%s] Loan dau: dang o san vo gioi (may khac) -> tat acc luon, login sau "
                     "server tu dua ve may goc", label)
            return True
        if int(c.current_map or 0) != int((ev or {}).get("dest_map") or 0):
            return False
        return bool(c.exit_event(ev))
    except Exception as e:
        log.warning("[%s] Loan dau: loi ra khoi map event: %s", label, e)
        return False


def _should_restart_event_party(event_party_mode, battle_active, disc_gen, handled_gen):
    return bool(event_party_mode and battle_active and disc_gen > handled_gen)


def _should_restart_mode_after_disconnect(train_on_map, reconnecting):
    """Train must regroup even when the disconnected account relogs between keepalive ticks."""
    return bool(train_on_map or reconnecting)


def _should_reform_incomplete_party(train_on_map, joined, needed, elapsed, threshold=20.0):
    return bool(train_on_map and needed > 0 and joined < needed and elapsed >= threshold)


def _party_train_tai_cho(maps, kenhs, train_map):
    """CA PARTY DA O MAP TRAIN chua? -> 'cung_kenh' | 'lech_kenh' | 'lech_map'.

    User chot 27/08: "cung o map train roi thi check kenh, neu cung kenh roi thi lap party keo ra
    train, ko cung kenh thi sync kenh thoi, ko can ve thanh". Truoc day moi lan thieu nguoi trong
    party la _do_reform() -> teleport CA PARTY ve thanh roi di bo/route len lai, du ai cung dang
    dung san o bai train -> mat vai phut va de lac them nguoi giua duong.

    Chua doc duoc kenh cua ai (None) thi KHONG dam ket luan 'cung kenh' - coi la lech de di
    duong sync kenh (sync tai cho, van khong ve thanh).
    """
    try:
        tm = int(train_map or 0)
    except (TypeError, ValueError):
        tm = 0
    ms = {int(m) for m in (maps or []) if m is not None}
    if not tm or not ms or ms != {tm}:
        return "lech_map"
    ks = [k for k in (kenhs or [])]
    if any(k is None for k in ks) or len({int(k) for k in ks if k is not None}) > 1:
        return "lech_kenh"
    return "cung_kenh"


def _should_resync_incomplete_digioi_party(
        is_digioi, digioi_solo, joined, needed, elapsed, threshold=20.0):
    return bool(is_digioi and not digioi_solo and needed > 0
                and joined < needed and elapsed >= threshold)


def _leader_tu_kiem_kenh(c, st, label=""):
    """LEADER tu gui lenh doi kenh ve dung kenh party TRUOC KHI moi.

    Member co vong retry nen 60s lai `switch_channel` mot lan -> kenh cua ho luon duoc server
    tra loi va cap nhat. LEADER thi chi doi kenh MOT LAN luc sync roi thoi, khong ai kiem lai:
    no bi day sang kenh khac (chet ve thanh, server dieu) ma `current_channel` van giu so cu ->
    bot bao "ca party cung kenh" trong khi leader dung mot noi (user log nick vao game xem 30/08:
    kenh 10 KHONG co nanam, trong khi bot bao no o 10).

    Gui lenh la CACH DUY NHAT xac minh duoc kenh (khong co truy van - xem `GameClient.kenh_that`):
    dang o dung kenh -> server tra ma 1 <trung khu dang o>; lech -> no keo ve dung kenh luon.
    """
    # `kenh_dich` (lenh dieu phoi) DUNG TREN `channel` (so picker tu chon). Hai so nay tung song
    # song va giang leader qua lai moi 8 giay - xem ghi chu o cho picker dang ky `kenh_dich`.
    ch = st.get("kenh_dich") or st.get("channel")
    if not ch:
        return True
    # Moi 30s mot lan la du: vong moi party chay moi vai giay, kiem tra moi lan thi tung luot moi
    # phai cho ack (toi 6s) -> cham han viec gom party.
    if time.time() - float(getattr(c, "_leader_kiem_kenh_at", 0.0) or 0.0) < 30.0:
        return True
    c._leader_kiem_kenh_at = time.time()
    try:
        _truoc = getattr(c, "current_channel", None)
        # wait/retries NHO: day chi la kiem tra dinh ky, khong phai lan doi kenh chinh. Cho lau
        # la chan ca vong moi party (log 30/08 21:09: 2 luot x 6s TIMEOUT moi 30s).
        ok = c.switch_channel(int(ch), wait=2.5, retries=1)
        if not ok:
            _res = getattr(c, "_chan_switch_result", None)
            if _res in (2, 4):
                # 2 = KHONG CO khu do | 4 = kenh DA DAY. Kenh chot cua party la kenh KHONG VAO
                # DUOC: giu nguyen thi ca party doi mai mot cho khong ai toi duoc.
                #
                # Log 31/08 party 1 (13:33-13:49): `st["channel"]=3` ma ca leader lan 3 member deu
                # `Doi kenh 3 THAT BAI: khong co khu do de doi (result=2)`. Member bao "-> bao
                # leader pick lai" roi nam cho `channel_ready` (khong ai set lai), con leader thi
                # "van moi, vong sau kiem lai" -> moi 13s/lan suot 16 phut, `da join=1`.
                log.warning("[%s] (LEADER) tu kiem kenh: kenh party %s KHONG VAO DUOC (%s) -> BO "
                            "kenh nay, chon lai kenh khac cho ca party", label, ch,
                            "khong ton tai" if _res == 2 else "DA DAY")
                with st["lock"]:
                    st["channel"] = None
                    st["channel_ready"].clear()
            else:
                # KHONG chan viec moi party: doi kenh hong la chuyen rieng, con moi thi cu moi.
                log.warning("[%s] (LEADER) tu kiem kenh: chua ve duoc kenh party %s (nho san %s) "
                            "-> van moi, vong sau kiem lai", label, ch,
                            getattr(c, "current_channel", None))
        elif _truoc is not None and int(_truoc) != int(ch):
            log.warning("[%s] (LEADER) tu kiem kenh: dang o kenh %s chu KHONG phai %s -> da doi ve",
                        label, _truoc, ch)
        return ok
    except Exception as e:
        log.warning("[%s] (LEADER) tu kiem kenh loi (bo qua): %s", label, e)
        return True


def _invite_party_participants(c, train_on_map, gap=1.0):
    """RULE: LUON moi acc WHITELIST TRUOC, bot member SAU.

    Whitelist la nguoi that/nick tay - khong co bot tu accept, can them thoi gian bam dong y;
    moi ho truoc thi trong luc bot lan luot accept thi ho cung kip vao. Moi sau (nhu truoc day
    o duong event/thuong) thi party co the da DU cho bot -> nguoi that KHONG con cho de vao.
    Duong train da lam dung tu truoc (invite_train_party_participants); day la lam cho duong
    con lai giong het.

    LEADER TU KIEM KENH TRUOC KHI MOI: xem `_leader_tu_kiem_kenh`. Dat o day vi day la CUA DUY
    NHAT ma leader di qua khi moi party (8 cho goi toi).

    HAM NAY CHI THI HANH - no KHONG quyet dinh co moi hay khong. Quyet dinh do la cua dieu phoi
    (`VIEC_MOI`), va nguoi doc lenh la `_moi_theo_dieu_phoi`.
    """
    try:
        _leader_tu_kiem_kenh(c, _pstate(getattr(c, "party_idx", None)),
                             label=getattr(c, "_label", "?"))
    except Exception as e:
        log.debug("[%s] leader tu kiem kenh loi (bo qua): %s", getattr(c, "_label", "?"), e)
    if train_on_map:
        return c.invite_train_party_participants(gap=gap)
    whitelist_count = 0
    try:
        whitelist_count = c.invite_whitelist_leaders(gap=gap)
    except Exception as exc:
        log.warning("[%s] (LEADER) moi whitelist truoc party loi: %s",
                    getattr(c, "_label", "?"), exc)
    return whitelist_count, c.invite_members(gap=gap)


def _invite_whitelist_followers_if_bot_party_ready(c, st, pidx, label, force=False):
    """Leader moi them acc ngoai whitelist SAU khi bot members da du.

    Acc ngoai vao hay khong KHONG tinh vao joined_member_count va khong chan flow bot.
    """
    needed = int(st.get("n_members") or 0)
    if needed > 0 and joined_member_count(pidx) < needed:
        try:
            known = {bytes(e) for e in c.known_party_entities()}
            roster = {bytes(e) for e in (getattr(c, "party_members", None) or [])}
            self_ent = bytes(c.self_entity) if getattr(c, "self_entity", None) else None
            bot_roster = {e for e in roster if e in known and e != self_ent}
        except Exception:
            bot_roster = set()
        if len(bot_roster) < needed:
            return 0
    now = time.time()
    last = float(st.get("whitelist_invite_at") or 0.0)
    if not force and now - last < 60:
        return 0
    st["whitelist_invite_at"] = now
    fn = getattr(c, "invite_whitelist_leaders", None)
    if not fn:
        return 0
    try:
        n = fn(gap=1.0)
        if n:
            log.info("[%s] (LEADER) da moi them %d acc whitelist ngoai party (khong doi accept)",
                     label, n)
        return n
    except Exception as e:
        log.warning("[%s] (LEADER) moi whitelist ngoai party loi: %s", label, e)
        return 0


def _party_is_in_train_phase(pcfg, st):
    # Android dang nhap o mode=stand, sau do nut AUTO BATTLE moi gan bai train live.
    # Tu luc co dich nay, phien phai duoc coi la train ke ca config goc van la stand.
    if st.get("ui_train_target"):
        return True
    raw_mode = pcfg.get("mode")
    if raw_mode == "train":
        return True
    if raw_mode == "digioi_train":
        return st.get("dt_phase") == "train"
    if raw_mode:
        return False
    return pcfg.get("start_city_id") in getattr(config, "TRAIN_MAPS", {})


def _average_party_levels(rows):
    levels = []
    for row in rows:
        char_level = row.get("char_level")
        if not isinstance(char_level, int) or char_level <= 0:
            return None
        levels.append(char_level)
        if row.get("pet_name"):
            pet_level = row.get("pet_level")
            if not isinstance(pet_level, int) or pet_level <= 0:
                return None
            levels.append(pet_level)
    if not levels:
        return None
    return (sum(levels) + len(levels) // 2) // len(levels)


def _party_level_rows(pidx):
    """(char_level, pet_name, pet_level) cua tung acc trong party - NGUON DUY NHAT.

    Acc dang chay thi doc thang client; acc da tat thi lay ban luu `account_last`.

    Truoc day doan nay bi CHEP HAI LAN (`_party_average_level` va `_party_levels`) - cung mot cach
    doc, chi khac cach gop cuoi. User 14/09: "da co cho tinh lv trung binh roi ma cho nay lai tinh
    lai a". Chep tay o dau la o do lech: sua nguon level o mot ham thi ham kia van doc kieu cu.
    """
    rows = []
    for username, *_ in party_accounts(pidx):
        c = account_clients.get(username)
        if c is not None:
            rows.append({
                "char_level": getattr(c, "char_level", None),
                "pet_name": c.pet_name_out(),
                "pet_level": getattr(c, "pet_level", None),
            })
        else:
            rows.append(account_last.get(username, {}))
    return rows


def _party_average_level(pidx):
    if pidx is None:
        return None
    return _average_party_levels(_party_level_rows(pidx))


def _party_levels(pidx):
    """Level CUA CA CHAR VA PET moi thanh vien -> list phang, cho train_pick.

    Cung nguon voi `_party_average_level` (`_party_level_rows`), chi khac cach gop: o day acc nao
    chua biet level thi BO QUA thay vi tra None ca cum. Chon map bang du lieu thieu van hon la
    khong chon duoc (acc dang goi ham nay chac chan da login xong nen luon co it nhat 1 level; acc
    khac lay account_last da luu tu lan chay truoc).
    """
    out = []
    for row in _party_level_rows(pidx):
        lv = row.get("char_level")
        if isinstance(lv, int) and lv > 0:
            out.append(lv)
        if row.get("pet_name"):
            plv = row.get("pet_level")
            if isinstance(plv, int) and plv > 0:
                out.append(plv)
    return out


def _acc_cho_level(pidx):
    """Acc PHAI co level truoc khi chot cap quai DG / map train = MOI acc trong party, TRU acc
    user da bam Stop.

    KHONG loc theo `is_account_running`: start_party tao thread LECH NHAU vai giay, acc chua kip
    tao thread se bi coi la "khong chay" -> barrier qua ngay bang 1 acc, dung y het bug cu.
    """
    out = []
    for username, *_ in party_accounts(pidx):
        ev = account_stops.get(username)
        if ev is not None and ev.is_set():
            continue      # user chu dong tat acc nay -> khong cho no nua
        out.append(username)
    return out


def _acc_thieu_level(pidx):
    """Acc chua san sang gop level = CHUA CO CHAR LEVEL. Pet KHONG tinh.

    Tra list mo ta `"<acc>(<thieu gi>)"` - khong phai ten tron. Ca ba cho goi deu chi dem va LOG,
    ma "thieu" ma khong noi thieu gi thi doc log van phai doan tiep.

    User 14/09: "chon bai train thi dua vao lv nhung con hien tai thoi, dua nao thieu pet thi ke
    me no di".

    Truoc day con doi ca `active_pet_confirmed`, ly do la pet level thuong cao hon char vai chuc
    (log 05/09: char ~154 / pet ~188) nen chot khi moi co char se tut hang bai. Cai gia phai tra
    qua dat: chua chot duoc bai -> `st["auto_train"]` rong -> dieu phoi KHONG BIET map train dich
    -> khong biet thanh tap ket -> ca party lap party bua o thanh di ngang qua (Trac Quan), lap
    xong teleport lam tan doi, quay vong. Mot con pet chua kip xac nhan khoa CA chuoi dieu phoi.
    Chon bai hoi thap con sua duoc; party khong bao gio hinh thanh thi khong.

    KHONG acc nao bi loai khoi phep tinh, va cung KHONG co so 0 nao duoc cong vao: `_party_levels`
    tra list PHANG cac level DANG CO MAT - char level cua moi acc, cong them pet cua acc nao dang
    tha pet. Acc thieu pet gop MOT so thay vi hai. Trung binh van la trung binh cua nhung con co
    mat, khong bi keo tut.
    """
    thieu = []
    for username in _acc_cho_level(pidx):
        c = account_clients.get(username)
        if c is None:
            lv = (account_last.get(username) or {}).get("char_level")
            if not (isinstance(lv, int) and lv > 0):
                thieu.append("%s(chua login, khong co ban luu)" % username)
            continue
        lv = getattr(c, "char_level", None)
        if not (isinstance(lv, int) and lv > 0):
            thieu.append("%s(char_level=%r)" % (username, lv))
    return thieu


# Chan chot bai lau hon bay nhieu giay thi PHAI NOI RA (moi 60s mot dong).
CHOT_BAI_BI_CHAN_BAO_SEC = 60.0


def _bao_chan_chot(pidx, st, viec, thieu):
    """Noi ro VI SAO chua chot duoc cap quai DG / map train, thay vi `return None` cam lang.

    Hai ham chot deu tra None im lang khi thieu level. Chung chay MOI 2 GIAY, nen mot lan chan keo
    dai la hang tram lan tra None ma KHONG DE LAI MOT DONG NAO - den luc di truy thi chi con nuoc
    doan.

    Ca that party 29, 14/09 (user: "p29 thay lap party o trac quan"): party start 23:55:36, mai
    00:23:07 moi co dong `TU CHON MAP` - 27 phut, khoang 810 lan tra None trong im lang. Suot do
    `auto_train` rong -> dieu phoi khong biet map train dich -> khong biet thanh tap ket -> ca
    party lap party o Trac Quan (12001) roi teleport lam tan doi, quay vong. Truy nguoc khong ra
    vi khong co du lieu: pet da xac nhan du ca 5 acc tu 23:55:41, khong exception, khong reconnect.

    Chi LOG, khong doi hanh vi.
    """
    if not thieu:
        st.pop("chot_chan_tu", None)
        st.pop("chot_chan_log", None)
        return
    _tu = float(st.get("chot_chan_tu", 0.0) or 0.0)
    if not _tu:
        st["chot_chan_tu"] = _tu = time.time()
    _lau = time.time() - _tu
    if _lau < CHOT_BAI_BI_CHAN_BAO_SEC:
        return
    if time.time() - float(st.get("chot_chan_log", 0.0) or 0.0) < CHOT_BAI_BI_CHAN_BAO_SEC:
        return
    st["chot_chan_log"] = time.time()
    log.warning("[party %d] DIEU PHOI: CHUA CHOT DUOC %s sau %.0fs - dang cho level cua %d acc: %s"
                " (ca party dung cho: khong biet map train dich thi khong biet thanh tap ket)",
                pidx + 1, viec, _lau, len(thieu), ", ".join(sorted(thieu)))


def _party_city_unlocked(pidx, city_id):
    """(danh sach acc CHUA MO thanh nay, danh sach acc CHUA BIET).

    city_unlocked() tra None = CHUA BIET (chua nhan goi co nhiem vu). Phai tach rieng: coi None la
    "chua mo" thi acc vua login se bi ket luan oan, coi la "da mo" thi lai tele mu nhu cu.
    """
    chua_mo, chua_biet = [], []
    for username, *_ in party_accounts(pidx):
        c = account_clients.get(username)
        if c is None:
            continue
        st = c.city_unlocked(city_id)
        if st is None:
            chua_biet.append(username)
        elif st is False:
            chua_mo.append(username)
    return chua_mo, chua_biet


def _party_unlocked_cities(pidx):
    """city_id ma CA PARTY (acc dang chay) deu da mo. Acc chua biet -> BO QUA thanh do cho chac."""
    out = []
    for cid in (getattr(config, "TELEPORT_CITY_IDS", None) or ()):
        chua_mo, chua_biet = _party_city_unlocked(pidx, cid)
        if not chua_mo and not chua_biet:
            out.append(cid)
    return out


def _pick_start_city(pidx, dest_city):
    """Thanh xuat phat cho party: GAN `dest_city` nhat trong so thanh CA PARTY deu mo.

    Thay cho viec co dinh gom o NGHIEP THANH - cach cu chet khi chinh Nghiep Thanh chua mo, va
    cung khong he gan (Nghiep Thanh -> Kien Nghiep 5 cong, trong khi Hoi Ke chi 2).
    """
    mo = _party_unlocked_cities(pidx)
    if not mo:
        return None
    # Dung ROUTER co san (nearest_city) chu KHONG tu tinh khoang cach: no con biet cong nao di bo
    # qua duoc (image [0,0,0] = warp event, khong di duoc). Tu dem cong se chon phai thanh ma
    # router KHONG dinh tuyen noi -> ca party ket o buoc keo.
    c = next((account_clients[u] for u, *_ in party_accounts(pidx)
              if account_clients.get(u) is not None), None)
    if c is None:
        return None
    got = c.nearest_smart_city(dest_city, allowed=mo)
    if not got:
        log.warning(">>> PARTY %s: khong thanh nao CA PARTY da mo ma di toi %s duoc (da mo: %s)",
                    pidx + 1, dest_city, sorted(mo))
        return None
    cid = int(got["city"])
    ten = (getattr(config, "TELEPORT_CITIES", None) or {}).get(cid, {}).get("name", cid)
    log.info(">>> PARTY %s: thanh xuat phat = %s (id %s, %d cong toi %s; ca party deu da mo)",
             pidx + 1, ten, cid, len(got.get("route", {}).get("legs", ())), dest_city)
    return cid


def _ai_lech_instance(pidx, song, grace=30.0):
    """AI dang o KHAC INSTANCE voi phan con lai - do bang BANG CHUNG THAY NHAU, khong bang so kenh.

    So kenh la so NHO va sai duoc (KNOWLEDGE.md muc 7: game KHONG CO lenh hoi "toi dang o kenh
    nao"). Con `0x03 PlayerAppear` thi chi server moi gui, va chi gui cho nguoi CUNG SCENE + CUNG
    INSTANCE - nen "co thay nhau khong" la cau tra loi CHAC CHAN.
    User 13/09: "biet duoc nhung nguoi xung quanh minh thi biet duoc co cung kenh hay ko".

    HOI MOI ACC, khong chi leader: `_party_khong_thay_nhau` cu chi nhin tu mat leader, nen khi
    CHINH LEADER la dua lech thi khong ai phat hien (user: "thuc ra cai party 1 la leader deo phai
    o kenh 6, no sai chu ko phai member sai").

    Cach lam: trong so acc DANG CUNG MAP, noi hai acc lai neu MOT trong hai thay dua kia -> duoc
    cac NHOM cung instance. Nhom dong nhat la instance dung; ai ngoai nhom do la lech, KE CA
    LEADER. Hoa nhau (2-2) thi khong ket luan - tra rong.

    `grace`: vua toi map thi chua kip nhan `0x03` cua nhau, doi mot lat roi hay ket luan.
    """
    _ds = [(u, c) for u, c in song if getattr(c, "current_map", None)]
    if len(_ds) < 3:
        return []                      # 2 nguoi thi khong co "da so" de dua vao
    _dem_map = {}
    for _u, c in _ds:
        _dem_map.setdefault(int(c.current_map), []).append((_u, c))
    _tren = max(_dem_map.values(), key=len)
    if len(_tren) < 3:
        return []                      # dang lech map -> nhanh khac lo, khong ket luan o day
    _luc = {}
    for _u, c in _tren:
        _t = getattr(c, "_thay_nhau_tu", None)
        if _t is None:
            _t = c._thay_nhau_tu = {}
        _luc[_u] = _t

    def _cung_party(a, b):
        """`a` va `b` co dang O CUNG MOT DOI khong - bang chung CHAC HON ca `0x03`.

        Server KHONG cho lap party xuyen instance: loi moi khong toi noi, accept khong an. Nen hai
        acc co ten trong roster cua nhau la CHAC CHAN cung instance, khong can doi goi `0x03`.

        Ca that party 3, 14/09 (user: "p3 van dung o tuong duong deo chiu di") - vong lap tu nuoi:
            17:18:25 [minh] PARTY: 94d0d7f8 vao doi (leader=4ef7d7f8) -> roster 4 nguoi   <- DU
            17:18:33 [party 3] gen 13: viec=dong_bo - cung map nhung ['minhminhmq'] KHONG THAY
                                       duoc dong doi (khac instance du cung so kenh)
            17:18:31 [minh] PARTY: 94d0d7f8 ROI doi (S:013-004) -> roster con 3
            17:18:31 [minh] PARTY: DOI TRUONG 4ef7d7f8 roi -> doi giai tan
        `minh` VUA vao party voi leader (nen chac chan cung instance) nhung chua kip nhan `0x03`
        cua ai -> bi ket luan lech -> lenh `dong_bo` -> dong bo kenh BAT BUOC roi doi -> party tan
        -> thieu nguoi -> moi lai -> du -> lai ket luan lech. Quay vong, khong bao gio di train.
        """
        try:
            _ea = getattr(a[1], "self_entity", None)
            _eb = getattr(b[1], "self_entity", None)
            if not _ea or not _eb:
                return False
            _ra = {bytes(x) for x in (getattr(a[1], "party_members", None) or ())}
            _rb = {bytes(x) for x in (getattr(b[1], "party_members", None) or ())}
            return bytes(_eb) in _ra or bytes(_ea) in _rb
        except Exception:
            return False

    def _thay(a, b):
        """acc `a` co dang THAY `b` khong (rong = thay)."""
        if _cung_party(a, b):
            return True                # cung doi -> chac chan cung instance
        _ent = getattr(b[1], "self_entity", None)
        if not _ent:
            return True                # chua biet entity -> khong ket luan xau
        try:
            return not a[1].da_thay_tan_mat(_ent)
        except Exception:
            return True

    # Nhom cac acc THAY NHAU (hai chieu deu tinh: mot ben thay la du de ket luan cung instance).
    _nhom = {u: u for u, _c in _tren}

    def _goc(u):
        while _nhom[u] != u:
            _nhom[u] = _nhom[_nhom[u]]
            u = _nhom[u]
        return u

    for i, a in enumerate(_tren):
        for b in _tren[i + 1:]:
            if _thay(a, b) or _thay(b, a):
                _nhom[_goc(a[0])] = _goc(b[0])
    _cum = {}
    for u, _c in _tren:
        _cum.setdefault(_goc(u), []).append(u)
    _to_nhat = max(_cum.values(), key=len)
    if len(_to_nhat) * 2 <= len(_tren):
        return []                      # khong co da so ro rang -> khong ket luan (L13)
    _ngoai = sorted(u for u, _c in _tren if u not in _to_nhat)
    if not _ngoai:
        return []
    # GRACE: chi ket luan khi tinh trang nay KEO DAI - vua toi map thi chua kip nhan 0x03.
    _bay = time.time()
    _ra = []
    for u in _ngoai:
        _t0 = _luc[u].setdefault("tu", _bay)
        if _bay - _t0 >= grace:
            _ra.append(u)
    for u, _c in _tren:
        if u not in _ngoai:
            _luc[u].pop("tu", None)
    return _ra


def _thieu_acc_song(pidx, song):
    """Party CHUA du acc login xong chua? (acc da TAT han thi khong tinh - no khong bao gio ve).

    Phep dem map/kenh cua dieu phoi chi nhin acc DANG SONG, nen khi moi 2/5 dua login xong thi
    "cung map/kenh" la ket luan tren mot mau khong day du - dua chua login co the o kenh khac han.
    Chua biet != khong sao (L13).
    """
    try:
        _cau_hinh = party_accounts(pidx)
    except Exception:
        return False
    if not _cau_hinh:
        return False
    # Acc da tat han (user Stop / khong con thread) thi KHONG cho: cho no la cho vinh vien.
    _con_kha_nang = [u for u, _p, _l, _k in _cau_hinh if is_account_running(u)]
    return len(song) < len(_con_kha_nang)


def _co_ai_dang_o_map(pidx, map_id, tru=None):
    """Party co ai DANG DUNG o `map_id` khong (bo qua chinh `tru`). Doc thang client, khong ai bao.

    Dung de phan biet "MINH bi van ra khoi bai" voi "CA PARTY dang di duong toi bai":
      - con nguoi o bai ma minh khong o do -> minh vang that,
      - khong ai o bai ca -> ca lu dang tren duong, chuyen binh thuong.
    """
    try:
        _m = int(map_id or 0)
    except (TypeError, ValueError):
        return False
    if not _m:
        return False
    for _u, _p, _l, _k in party_accounts(pidx):
        if tru is not None and _u == tru:
            continue
        _c = account_clients.get(_u)
        if _c is None or not getattr(_c, "running", False):
            continue
        if int(getattr(_c, "current_map", 0) or 0) == _m:
            return True
    return False


def _map_train_dich(pidx, st):
    """MAP TRAIN DICH cua party, `None` = that su chua ai biet.

    So bai train NAM O HAI CHO, va bac chan "chi lap party o diem tap ket" truoc day bam dung cai
    duoc dien MUON NHAT:
      - `auto_train`      - DIEU PHOI chot bai (`_dieu_phoi_chot_map`), co ngay tu dau
      - `train_map_dich`  - luong leader ghi, chi khi DA warp vao bai xong

    Khong phai "chua toi bai thi khong biet dich": co so map train la `_pick_start_city` tinh ra
    thanh tap ket ngay (router thuan du lieu, khong can ai di dau ca). Loi la TRA SAI O - dich da
    chot tu lau ma phep chan lai hoi cai o con rong.

    Ca that party 15, 13/09 (user: "p15, o trac quan lap pt lam lon gi the" -> "biet duoc bai train
    la biet duoc duong di den bai la luc do biet thanh tap ket roi, deo phai luc den map train moi
    biet thanh tap ket"):
        14:23:43 >>> PARTY 15: TU CHON MAP -> Dam Lay Tang Khau1 (map 21841)   <- da chot tu day
        22:57:15 [party 15] gen 4: viec=moi - cung map/kenh nhung DOI chua du (tat ca = 0)
        22:59:26 [party 15] DIEU PHOI: ca party da chung kenh 2 nhung DOI chua du -> LAP LAI PARTY
        23:01:27 ... 23:03:28 ... 23:05:29   (lap lai mai, roster khong bao gio len)
    Ca party o 12001 (Cua thanh Trac Quan) - thanh DI NGANG QUA. `_o_thanh_di_qua` tra False vi
    `train_map_dich` rong, nen bac chan khong he bat len, du bai train biet tu 8 tieng truoc.

    Thu tu doc: `train_map_dich` (leader da toi - chac chan nhat) -> `auto_train` (dieu phoi chot)
    -> `start_city_id` (user chi dinh map tay, khong `train_pick`).
    """
    # Dich do Android AUTO BATTLE chon la nguon live moi nhat. Login rieng/reconnect
    # khong duoc quay lai start_city_id=0 cua payload Login ban dau.
    _ui = st.get("ui_train_target")
    if _ui:
        try:
            return int(_ui[0])
        except (TypeError, ValueError, IndexError):
            pass
    _d = st.get("train_map_dich")
    if _d:
        try:
            return int(_d)
        except (TypeError, ValueError):
            pass
    _auto = st.get("auto_train")
    if _auto:
        try:
            return int(_auto[0])
        except (TypeError, ValueError, IndexError):
            pass
    pcfg = getattr(config, "PARTY_CONFIG", {}).get(pidx, {}) or {}
    _mode = pcfg.get("mode")
    if pcfg.get("train_pick"):
        return None                  # cho dieu phoi chot, khong doan theo config
    if _mode == "digioi_train" and st.get("dt_phase", "digioi") != "train":
        return None                  # con dang pha DG, chua co chuyen di train
    if _mode not in ("train", "digioi_train"):
        return None
    try:
        _sc = int(pcfg.get("start_city_id", getattr(config, "START_CITY_ID", 0)) or 0)
    except (TypeError, ValueError):
        return None
    return _sc or None


def _thanh_tap_ket_dich(pidx, st):
    """THANH TAP KET DICH cua party = thanh xuat phat gan map train nhat ma CA PARTY deu da mo.

    `None` = chua biet (chua co map train dich, hoac khong thanh nao ca party mo di toi duoc) ->
    nguoi goi KHONG duoc suy ra dieu gi, cu de viec chay binh thuong.

    Cache theo map train: `_pick_start_city` goi router (`nearest_smart_city`), ma vong dieu phoi
    chay moi 2 giay cho MOI party.
    """
    _train = _map_train_dich(pidx, st)
    if not _train:
        return None
    _cu = st.get("thanh_tap_ket_cache")
    if _cu and _cu[0] == _train:
        return _cu[1]
    try:
        _tp = _pick_start_city(pidx, _train)
    except Exception as e:
        log.debug("[party %d] chot thanh tap ket loi: %s", pidx + 1, e)
        _tp = None
    st["thanh_tap_ket_cache"] = (_train, _tp)
    return _tp


def _o_thanh_di_qua(pidx, st, noi):
    """`noi` co phai THANH DI NGANG QUA (khong phai diem tap ket) khong?

    Lap party o thanh trung gian la vo ich: buoc ngay sau la TELEPORT di thanh tap ket, ma
    teleport bat buoc `leave_party()` -> party vua lap lai tan.

    Ca that party 4, 13/09 (user: "p4, bon no lap pt o Trac quan lam lon gi the"):
        13:32:44 [party 4] dong_bo - cung map nhung LECH KENH [1,4,10,11]   (dang o 12001)
        13:32:47 [party 4] moi -> CHOT kenh dich = 13
        13:36:01 [party 4] dong_bo - cung map nhung LECH KENH [1,7,10,11]   (van 12001)
        13:36:16 [party 4] LAP LAI PARTY (brubb46677=0 sga008=0 ... sga012=0)
    Roster dung im o 0 suot. Trac Quan la thanh ma `pre_route_town_hop` nem acc qua (random 50-50
    Trac Quan / Nghiep Thanh) tren duong ve thanh xuat phat - CHO DI NGANG, khong phai cho tu.

    KHONG duoc coi Trac Quan / Nghiep Thanh la "trung gian" theo ID: chinh chung co the LA thanh
    tap ket cua party khac (user dan do 13/09). So voi DICH THAT, khong so voi danh sach cung.

    True chi khi CHAC CHAN: `noi` la thanh teleport, da biet dich, va `noi` khac ca dich lan map
    train. Moi truong hop con lai tra False - tha lap party thua con hon khong bao gio lap.
    """
    try:
        if not noi or not config.is_teleport_city(int(noi)):
            return False            # dang o bai train / phy ban / map thuong -> khong phai thanh
    except Exception:
        return False
    _dich = _thanh_tap_ket_dich(pidx, st)
    if not _dich:
        return False                # chua biet dich -> khong ket luan (L13)
    _train = _map_train_dich(pidx, st)
    if _train and int(noi) == int(_train):
        return False                # dang o chinh map train -> lap party ngay tai bai la dung
    return int(noi) != int(_dich)


def _nhip_cho_kenh(st):
    """Picker bao "toi VAN DANG tim kenh" - member cho theo nhip nay, khong theo dong ho tuyet doi.

    Truoc day member cat cung sau `CHO_KENH_CAP` giay (them 31/08 de chua party 14 treo 11 phut).
    Cai do lam hong dung truong hop server DONG: leader van dang kien tri quet kenh thi member da
    bo cho -> moi dua nam nguyen kenh login.
    Log 01/09 party 4 (sga011-015, server trieu_van 89 kenh, dang o Di Gioi):
        11:25:19 [thmo] KHONG kenh nao du 5 cho trong cho ca party -> RETRY  (lap toi 11:32+)
        11:26:45 [thnam] (member) cho channel_ready qua 90s -> THOI CHO
    -> leader kenh 33, con lai 2/20/71/2 (user: "party 4 bi sao ma moi dua 1 kenh").
    Con han van CAN: picker CHET han (thread ket/rot) thi nhip dung lai va member moi thoat.
    """
    st["kenh_nhip"] = time.time()


def _ve_thanh_tap_trung(c, pidx, label, dest_city, dest_flag):
    """Mode CITY: ve thanh tap trung; thanh CHUA MO tele -> ra lenh DI MAP (party keo nhau di bo).

    Truoc day mode city chi goi `go_to_town(sc, flag)` tron: thanh chua mo thi `go_to_town` bo cuoc
    NGAY ("thanh %s CHUA MO tele -> bo qua ngay") va acc DUNG IM tai cho login. Log 01/09 party 48
    (dt901-905) + party 49 (gclm*): ra khoi Di Gioi ve map 12003 (quang truong Trac Quan) roi nam
    do ca tieng (user: "party 48 49 no ko ve thanh, no dung yen o quang truong, t chon Ng thanh ma").

    KHONG tu di bo LE tung acc. Ban dau lam the va no HONG dung nhu mode train da biet tu lau: cong
    co hoi thoai (cau Gioi kieu, map 63000 cong 10) chi MOT nguoi tra loi duoc, 5 acc di le thi moi
    acc tu bam mot ma -> ket ca lu o cau (log 11:31-11:35, user: "leader chon thoi, lien quan me gi
    den 5 acc" / "m dang cho di le a").
    Dung LAI dung co che da co va da chay tot: lenh "DI MAP AAA -> BBB" (`_do_manual_route`) - no
    gom ca party ve thanh xuat phat, lap party TAM (ke ca party khong co chu PT thi picker dong vai
    leader), LEADER KEO qua tung cong con member follow, den noi thi giai tan. Y het cach mode train
    xu ly "thanh gan bai chua mo" (`_reform_via_nghiep`: gom o thanh da mo roi leader keo di bo).
    """
    dest_city = int(dest_city)

    def _toi_noi():
        # Da o thanh dich -> xoa dau lenh cu de lan sau (bi day ra khoi thanh) con ra lenh lai duoc.
        st = _pstate(pidx)
        with st["lock"]:
            if st.get("route_ve_thanh_dest") == dest_city:
                st["route_ve_thanh_dest"] = None
        return True

    try:
        if c.go_to_town(dest_city, int(dest_flag)):
            return _toi_noi()
    except Exception as e:
        log.warning("[%s] loi ve thanh %s: %s", label, dest_city, e)
    if c.current_map == dest_city:
        return _toi_noi()
    if c.city_unlocked(dest_city) is not False:
        return False        # that bai vi ly do khac (battle chan tele...) - go_to_town da lap du
    _ra_lenh_di_bo_ve_thanh(pidx, dest_city, label)
    return False


def _ra_lenh_di_bo_ve_thanh(pidx, dest_city, label):
    """Dat lenh DI MAP <thanh ca party da mo> -> <thanh user chon> cho CA PARTY.

    Dat MOT LAN cho moi dich (khong phai moi acc, khong phai moi vong): lenh la cua ca party, moi
    acc deu chay nhanh route cua no; acc nao cung dat thi cmd_gen nhay lien tuc -> route bi khoi
    dong lai giua chung mai mai.
    """
    st = _pstate(pidx)
    with st["lock"]:
        if st.get("route_ve_thanh_dest") == int(dest_city):
            return False
        st["route_ve_thanh_dest"] = int(dest_city)
    xuat_phat = _pick_start_city(pidx, dest_city)
    if not xuat_phat:
        log.warning("[%s] thanh %s CHUA MO tele va KHONG thanh nao ca party da mo di toi do duoc "
                    "-> dung tai cho", label, dest_city)
        return False
    _ten = (getattr(config, "TELEPORT_CITIES", None) or {}).get(xuat_phat, {}).get("name", xuat_phat)
    log.warning("[%s] thanh %s CHUA MO tele -> ra lenh DI MAP %s (%s) -> %s: ca party lap doi va "
                "KEO nhau di bo (khong di le)", label, dest_city, xuat_phat, _ten, dest_city)
    party_route_maps(pidx, xuat_phat, dest_city)
    return True


def _thanh_dong_acc_nhat(pidx):
    """THANH ma nhieu acc cua party dang dung nhat (doc thang `account_clients`, khong ai bao cao).

    Dung lam diem gom khi khong co route: it phai di chuyen nhat, va thuong da so party da tu ve
    day roi - chi con vai dua lac. Hoa thi lay so nho de moi acc tinh ra CUNG mot thanh.

    Tra None khi khong acc nao dang o thanh teleport duoc (dang o bai train / phy ban / DG).
    """
    _la_thanh = getattr(config, "is_teleport_city", None)
    if not callable(_la_thanh):
        return None
    dem = {}
    for _u, _p, _l, _k in party_accounts(pidx):
        _c = account_clients.get(_u)
        if _c is None or not getattr(_c, "running", False):
            continue
        m = int(getattr(_c, "current_map", 0) or 0)
        try:
            if m and _la_thanh(m):
                dem[m] = dem.get(m, 0) + 1
        except Exception:
            continue
    if not dem:
        return None
    return sorted(dem.items(), key=lambda kv: (-kv[1], kv[0]))[0][0]


def _gather_city(pidx, dest_city, gen):
    """Diem GOM cua party khi khong ve thang `dest_city` duoc: thanh GAN NHAT ma CA PARTY da mo.

    Truoc day co dinh NGHIEP THANH (12061). Hai loi user chi ra (25/08):
      1. Chinh Nghiep Thanh cung co the CHUA MO -> ke hoach chet han.
      2. Khong he "gan": Nghiep Thanh -> Kien Nghiep mat 5 cong, tu Hoi Ke chi 2.

    Chot 1 LAN moi reform gen roi giu: moi acc tu tinh se ra thanh khac nhau (danh sach thanh da mo
    doi theo acc nao dang chay), ca party se toe ra.
    Fallback 12061 chi dung khi CHUA BIET gi (chua nhan co) - giu hanh vi cu, khong lam te hon.
    """
    st = _pstate(pidx)
    with st["lock"]:
        cur = st.get("gather_city")
        if cur and st.get("gather_city_gen") == gen:
            return cur
    cid = _pick_start_city(pidx, dest_city) or 12061
    with st["lock"]:
        st["gather_city"] = cid
        st["gather_city_gen"] = gen
    return cid


def _auto_dg_level(pidx, pick_mode):
    """idx cap quai Di Gioi (1..15) suy tu level party. None neu chua biet level acc nao.

    Chot 1 LAN/phien giong _auto_train_target: ca party phai cung MOT cap quai. Co
    DIEU PHOI goi ham nay moi 2 giay; acc CHI DOC ket qua (`_doc_cap_dg`).
    """
    st = _pstate(pidx)
    with st["lock"]:
        cur = st.get("auto_dg_level")
        if cur:
            return cur
    # CHUA DU LEVEL CA PARTY -> KHONG CHOT. Dieu phoi goi lai moi 2s nen se chot ngay khi du.
    # (Truoc 05/09 khong co chot nay: `account_last` chi nam trong RAM, reset moi lan start bot,
    #  nen lan chay dau tien acc nao login xong TRUOC la chot cho ca party bang MINH NO. Bang
    #  chung: `PARTY 1: TU CHON CAP QUAI DG -> cap 150 (level party [167, 197])` - party 5 acc
    #  ma chi co 2 so. Chot 1 lan/phien nen sai la sai den luc restart bot.)
    _thieu = _acc_thieu_level(pidx)
    _bao_chan_chot(pidx, st, "CAP QUAI DG", _thieu)
    if _thieu:
        return None
    with st["lock"]:
        cur = st.get("auto_dg_level")   # dieu phoi/acc khac da chot xong trong luc minh cho
        if cur:
            return cur
        levels = _party_levels(pidx)
        if not levels:
            log.warning(">>> PARTY %s: TU CHON CAP QUAI DG nhung chua biet level acc nao -> dung "
                        "cap da luu", pidx + 1)
            return None
        tier = train_pick.desired_dg_level(pick_mode, levels)
        if not tier:
            return None
        idx = train_pick.DG_LEVELS.index(tier) + 1
        log.info(">>> PARTY %s: TU CHON CAP QUAI DG -> cap %d (muon %d, level party %s)",
                 pidx + 1, tier, train_pick.desired_level(pick_mode, levels), sorted(levels))
        st["auto_dg_level"] = idx
        return idx


CHO_DIEU_PHOI_CHOT_BAI_SEC = 60.0   # acc cho dieu phoi chot bai train (no chay moi 2 giay)


def _doc_cap_dg(pidx):
    """idx cap quai Di Gioi DIEU PHOI da chot, None = chua chot.

    CHI DOC - giong `_doc_bai_train`. Acc khong duoc tu chot: do la quyet dinh cap party (mot cap
    quai cho ca doi), va `_dieu_phoi_chot_map` lam viec do moi 2 giay.

    Truoc day acc TU goi `_auto_dg_level(pidx, pick, username, _stopped)`, ma ban co username thi
    ham do roi vao `_cho_du_level_party` - VONG CHO VO HAN, khong doc lenh dieu phoi, khong log gi
    ngoai mot dong moi 30 giay. Acc nam trong do thi DIEC: dieu phoi ra lenh gom deu deu, no khong
    nghe thay.
    """
    try:
        return _pstate(pidx).get("auto_dg_level")
    except Exception:
        return None


def _doc_bai_train(pidx):
    """(map_id, mob_index) DIEU PHOI da chot cho party, None = chua chot.

    CHI DOC. Acc khong duoc tu chot bai: chot bai la quyet dinh cap party (mot map cho ca doi),
    va `_dieu_phoi_chot_map` da lam viec do moi 2 giay.
    """
    try:
        return _pstate(pidx).get("auto_train")
    except Exception:
        return None


def _auto_train_target(pidx, pcfg):
    """(map_id, mob_index) cho party dat 'Tu chon map'. QUYET 1 LAN roi giu trong party state.

    Giu lai vi 2 le: (1) moi acc goi rieng, khong chot thi moi dua ra 1 map khac nhau; (2) co yeu to
    ngau nhien khi nhieu diem cung hop -> goi lai la ra diem khac, ca party lech nhau.
    DIEU PHOI goi ham nay moi 2 giay; acc CHI DOC ket qua (`_doc_bai_train`).
    """
    st = _pstate(pidx)
    with st["lock"]:
        cur = st.get("auto_train")
        if cur:
            return cur
    # CHUA DU LEVEL CA PARTY -> KHONG CHOT (xem _auto_dg_level).
    _thieu = _acc_thieu_level(pidx)
    _bao_chan_chot(pidx, st, "MAP TRAIN", _thieu)
    if _thieu:
        return None
    with st["lock"]:
        cur = st.get("auto_train")      # dieu phoi/acc khac da chot xong trong luc minh cho
        if cur:
            return cur
        levels = _party_levels(pidx)
        if not levels:
            log.warning(">>> PARTY %s: TU CHON MAP nhung chua biet level acc nao -> bo qua lan nay",
                        pidx + 1)
            return None
        maps = [(mid, m.get("name") or str(mid), m.get("mobs") or [])
                for mid, m in getattr(config, "TRAIN_MAPS", {}).items()]
        got = train_pick.pick_train_spot(
            pcfg.get("train_pick"), levels, maps,
            mob_min=int(pcfg.get("mob_min") or train_pick.DEFAULT_MOB_MIN),
            mob_max=int(pcfg.get("mob_max") or train_pick.DEFAULT_MOB_MAX),
            elements=pcfg.get("mob_elements") or train_pick.ALL_ELEMENTS)
        if not got:
            log.warning(">>> PARTY %s: TU CHON MAP khong tim duoc diem nao (level party %s)",
                        pidx + 1, sorted(levels))
            return None
        map_id, idx, used_level, why = got
        name = (getattr(config, "TRAIN_MAPS", {}).get(map_id) or {}).get("name", map_id)
        # In CA "muon" lan "level party" - giong dong tu chon cap quai DG. Thieu 2 so nay thi khi
        # user hoi "sao lai chon map nay" la KHONG TRA LOI DUOC tu log: khong biet bot ha level
        # xuong (do khong map nao khop bo loc) hay tai level party khac voi user tuong.
        _muon = train_pick.desired_level(pcfg.get("train_pick"), levels)
        log.info(">>> PARTY %s: TU CHON MAP -> %s (map %s) diem %s | level quai %d "
                 "(muon %s, level party %s) | %s",
                 pidx + 1, name, map_id,
                 ("CHUA QUET (se quet roi lay bai bat ky)" if idx < 0 else idx + 1),
                 used_level, _muon, sorted(levels), why)
        st["auto_train"] = (map_id, idx)
        return st["auto_train"]


# Hai lenh reform phai cach nhau it nhat bay nhieu giay. Do theo viec THUC TE mot lenh can bao lau
# de co tien trien: leader tele ve thanh roi tele ra bai mat ~10-20s (party 19, 08:04:19 -> 08:04:27),
# member di duong con lau hon. 30s = du de thay lenh truoc co an thua khong, ma van du nhanh de cuu
# party hong that (watcher deadlock chay moi 150s nen khong dinh nguong nay).
REFORM_BUMP_CACH_TOI_THIEU_SEC = 30.0


def _reform_cho_xu(st, moc):
    """Co lenh reform MOI (so voi `moc`) va CHUA BI RUT khong?

    Dung cho moi cho lay `reform_gen` de TU CHOI LAM VIEC ("dang co reform pending -> thoi khong
    lam"). Lenh da duoc DIEU PHOI RUT (L16 - `reform_gen_thoa`) thi khong con la ly do de tu choi.

    Ca that party 4, 14/09 (user: "lap pt xong deo di train"):
        16:34:46 [party 4] RUT lenh reform gen 1 - da du doi (...), cung map [21001] kenh [2]
        16:34:46 [party 4] gen 15: viec=di_train - du doi, cung map/kenh -> DI TRAIN map 21833
        16:38:51 [thmo] (LEADER) lenh 'di_train' -> SET QS + ra train
        16:38:51 [thmo] (LEADER) reform pending (acc bi dump dungeon) -> BO QUA keo ra spot
        16:38:56.. pos=(770, 610) map=21001 combat=False    <- dung im
    Leader nhan dung lenh, roi tu bo vi thay "reform pending" - trong khi lenh do da bi rut tu 4
    phut truoc. L16 sang nay chi sua vong chinh, khong ra cac cho khac doc `reform_gen`.

    KHONG dung cho cac cho ABORT ("dang di duong ma co lenh MOI -> bo dang, quay lai xu"): o do
    cau hoi la "gen co doi khong", khong phai "lenh con hieu luc khong".
    """
    try:
        _g = int(st.get("reform_gen", 0) or 0)
        if _g <= int(moc or 0):
            return False
        return int(st.get("reform_gen_thoa", 0) or 0) < _g
    except Exception:
        return False


def _bump_reform(st, reason="", uu_tien=False):
    """Tang reform_gen VA log RO ai bump. CALLER PHAI DANG GIU st["lock"].

    Truoc day co 15 cho tang thang reform_gen va KHONG cho nao noi minh la ai -> moi lan party
    "tu dung doi reform" la phai mo nguoc ca 15 cho (that: hoi 00:50 sau khi PB lv50 xong).
    Lay so dong cua NGUOI GOI bang sys._getframe(1) -> khong phai go tay 15 ly do khac nhau.
    """
    try:
        _ln = sys._getframe(1).f_lineno
    except Exception:
        _ln = 0
    # KHOANG LANG TOI THIEU GIUA HAI LAN BUMP.
    #
    # Moi bump la mot lenh ABORT gui toi MOI acc dang di duong (`_ab()` -> "ABORT di duong
    # reform"). Bump lan hai truoc khi lan mot kip thi hanh = tu huy lenh cua chinh minh, va ca
    # party khong bao gio di xong buoc nao.
    #
    # Ca that party 19, 11/09 - HAI lenh cach nhau BON giay:
    #   08:04:21 gen 19: viec=gom - party o 2 MAP khac nhau [12001, 21001] -> REFORM gen -> 3
    #   08:04:23 [quanmot] (LEADER) Da ve thanh 12001 -> Teleport -> city 21001
    #   08:04:25 gen 20: viec=moi - cung map/kenh nhung DOI chua du   -> REFORM gen -> 4
    #   08:04:25 [vumot]   (member) ABORT di duong reform: reform_gen 3 -> 4 (acc khac bump)
    #   08:04:25 [quantam] (member) ABORT di duong reform: reform_gen 3 -> 4 (acc khac bump)
    # Leader vua tele qua 12001 -> 21001 nen co DUNG MOT NHIP ca party "cung map"; dieu phoi tuong
    # gom xong va ra lenh lap party, trong khi hai member con dang tren duong. Ket qua la leader
    # di tiep mot minh (user: "deu leader 1 noi member 1 noi ... leader thi ngu van di 1 minh").
    #
    # Cac cooldown san co khong phu duoc cho nay: `KE_HOACH_GOM_COOLDOWN` chi chan gom->gom,
    # `LAP_LAI_PARTY_COOLDOWN` chi chan lap->lap. Chuoi gom->lap party thi khong ai chan.
    # `uu_tien` = lenh GOM (party dang o nhieu map - tuc doi da hong THAT). No khong bao gio bi
    # khoang lang chan: khoang lang sinh ra de chan lenh "lap lai party" de len lenh gom dang chay
    # (party 19, 08:04:21 -> 08:04:25), chu khong phai de chan chinh lenh gom.
    #
    # Ca that party 10, 11/09 - dung cai loi do:
    #   12:35:41 [party 10] REFORM gen -> 6 - chung kenh roi ma doi khong du -> lap lai party
    #   12:35:55 [party 10] gen 15: viec=gom - party dang o 2 MAP khac nhau [21841, 21844]
    #   12:35:55 [party 10] BO QUA bump reform (...gom ve cung map/kenh): lenh truoc moi ra 14s
    #   ... dieu phoi im 3 phut, khong ai gom, leader dung danh mot minh ...
    # Lenh gom bi bo MOT LAN la thoi: `_ghi_ke_hoach` chi bao khi NOI DUNG doi, ma noi dung van la
    # `viec=gom` nen khong co lan thu lai.
    _truoc = float(st.get("reform_bump_luc", 0.0) or 0.0)
    _cach = time.time() - _truoc
    if not uu_tien and _truoc and _cach < REFORM_BUMP_CACH_TOI_THIEU_SEC:
        log.info("[party %s] BO QUA bump reform tai run_party_digioi.py:%d (%s): lenh truoc moi ra "
                 "%.0fs, chua ai kip thi hanh - bump nua la ABORT chinh no",
                 (st.get("pidx", -1) + 1) if st.get("pidx") is not None else "?",
                 _ln, reason or "khong ro ly do", _cach)
        return st.get("reform_gen", 0)
    st["reform_bump_luc"] = time.time()
    st["reform_gen"] = st.get("reform_gen", 0) + 1
    log.info("[party %s] REFORM gen -> %d (bump tai run_party_digioi.py:%d)%s",
             (st.get("pidx", -1) + 1) if st.get("pidx") is not None else "?",
             st["reform_gen"], _ln, (" - " + reason) if reason else "")
    return st["reform_gen"]


def _pstate(pidx):
    if pidx not in _party_state:
        _party_state[pidx] = {"pidx": pidx,
                              "channel": None,
                              "channel_ready": threading.Event(),
                              "channel_failed": threading.Event(),
                              "channel_failed_reason": "",
                              "channel_expected_map": None,
                              "channel_sync_gen": 0,
                              "invited": threading.Event(),
                              "lock": threading.Lock(),
                              "n_members": 0,            # tong so member can cho
                              "started_train": 0,        # so acc da qua check map -> vao train (de barrier dungeon)
                              "dungeon_done": 0,         # so acc da danh xong dungeon (barrier)
                              "dailies_done": 0,         # so acc da xong daily login (barrier cho leader)
                              "o5_state": "idle",        # "idle"|"running"|"done" - member PHAI cho != "idle" (xem _handle_o5_team)
                              "o5_broke": False,         # team dungeon VO do co dis giua chung -> CA party relogin thoat instance
                              "o5_need_redo": False,     # team dungeon VO -> reconnect xong lam LAI daily (team dungeon)
                              "team_dungeon_state": {},       # level -> "idle"|"running"|"done"
                              "team_dungeon_broke": {},       # level -> co dis/fail can relogin thoat instance
                              "team_dungeon_tries": {},       # level -> so lan da thu (1 dau + 1 retry)
                              "team_dungeon_skip_all": False, # bo qua PB con lai CUA LUOT NAY (doc 1 lan roi xoa)
                              "team_dungeon_need_redo": False,
                              "leader_ok": threading.Event(),   # leader DUNG map train -> tiep tuc
                              "leader_bad": threading.Event(),  # leader SAI map -> huy ca party
                              "leader_gone": threading.Event(),  # leader da THOAT -> member ngung retry vao party
                              # Leader bam OUT RIENG: member KHONG thoat theo; ca team dung safe
                              # cho leader login/rejoin lai session hien tai.
                              "leader_manual_off": False,
                              "stop_leader_done": threading.Event(),  # STOP: leader DA ve safe -> member duoc thoat
                              "route_party_ready": threading.Event(),  # ROUTE: party da lap xong o thanh -> sap keo di
                              "route_done": threading.Event(),         # ROUTE: leader da keo xong (toi train map)
                              "route_plan_ready": threading.Event(),   # ROUTE: leader da build smart/legacy plan cho ca party
                              "route_plan": None,                      # {"gen", "city", "flag", "route"|None, "missing"?}
                              "event_exit_now": threading.Event(),  # 2K ket thuc/THUA -> CA DOI di ra khoi thap
                              # 2K: ket qua vong leo thap do LUONG LEADER bao len - "thua"/"xong"
                              # /"dut" (client chet giua chung). CHI la SU THAT, khong phai quyet
                              # dinh: `_dieu_phoi_chot_2k_xong` moi la noi bat `event_exit_now`.
                              "2k_ket_qua": None,
                              "mob_spot": None,      # diem quai leader chon (de _start_training dung lai)
                              "rally_point": None,   # safe GAN diem quai nhat -> CA PARTY ve day (gan leader)
                              "rally_ready": threading.Event(),  # leader da chon diem quai + rally_point
                              "path_done": threading.Event(),    # leader da di xong follow_path toi diem quai (member bi keo theo)
                              "reform_gen": 0,       # +1 moi khi co acc van map (chet) -> CA party reform tai cho
                              "resync_gen": 0,       # +1 khi leader moi 1p khong du party -> CA party giai tan + sync kenh lai + moi lai (event 40NPC)
                              "rally_gen": 0,        # +1 khi leader bao "lap lai party tai cho" -> MOI acc chay ra safe TRUOC khi moi (party 11, 30/08)
                              # NHIP TIM cua picker: time.time() moi vong no THU chon kenh. Member
                              # cho theo cai nay chu KHONG theo dong ho tuyet doi - xem `_nhip_cho_kenh`.
                              "kenh_nhip": 0.0,
                              # thanh dich da ra lenh DI MAP (mode city, thanh chua mo tele) - de
                              # khong acc nao ra lenh lai lien tuc. Xem `_ra_lenh_di_bo_ve_thanh`.
                              "route_ve_thanh_dest": None,
                              "kenh_dich_luc": 0.0,  # luc chot kenh dich (giu KENH_DICH_KIEN_NHAN_SEC)
                              # 2K: tang gom da chot + luc chot (giu TANG_GOM_KIEN_NHAN_SEC, dung
                              # tinh lai moi nhip keo dich tut theo buoc chan member).
                              "tang_gom": None,
                              "tang_gom_luc": 0.0,
                              "lap_party_luc": 0.0,  # lan cuoi ra lenh lap lai party (cooldown)
                              "kenh_hong": None,     # kenh ma ca party "cung so" ma khong thay nhau -> picker phai TRANH
                              # Kenh USER TU CHON bang lenh tay: picker KHONG duoc tu chon kenh khac
                              # nua. Bo ghim khi chinh kenh do hong (`kenh_hong`) hoac user ra lenh
                              # kenh khac. Log 31/08 09:57 (party 16): user chon kenh 1, doi xong,
                              # 09:58:22 picker tu "Kenh it nguoi MA DU CHO ca party (5): kenh 2"
                              # -> ca lu keo nhau ve kenh 2, phu dinh lenh cua user.
                              "kenh_ghim": None,
                              "auto_best_channel": False,
                              "manual_train_channel": None,
                              "manual_train_channel_ready": threading.Event(),
                              "train_channel_auto": False,
                              "train_channel_manual": None,
                              "daily_active": False,
                              "daily_cancel": False,
                              "daily_tasks": (),
                              "daily_task": None,
                              "daily_phase": "idle",
                              "daily_user": None,
                              "daily_message": "Chua chay Daily",
                              "daily_started_at": 0.0,
                              # Account con dang xu ly lenh Daily thu cong. Khong dung mot bool
                              # chung: acc xong som khong duoc tat Daily cua ca party.
                              "daily_pending": set(),
                              "cmd_leader_xong": threading.Event(),  # lenh tay: LEADER da ra safe + giai tan party -> member moi duoc di
                              # SO LENH ma leader da lam xong buoc tren. Member chot theo so nay,
                              # khong theo Event: co dung chung con SOT tu lenh truoc thi member
                              # doc trung khe leader chua kip xoa -> di som, party rach.
                              "cmd_leader_xong_gen": None,
                              "sync_epoch": 0,       # +1 = EP DONG BO uu tien cao nhat (request_party_resync): moi acc relogin bam leader

                              "go_claim": threading.Event(),  # 40NPC: het gio/thua 2 tran -> CA party di doi thuong + thoat
                              "cmd_gen": 0,          # +1 moi khi GUI ra lenh thu cong (doi kenh/teleport thanh)
                              "cmd": None,           # ("channel", ch) | ("city", city_id, flag) | ("route", a, b)
                              "manual_route_gen": 0,
                              "manual_route_plan": None,
                              "manual_route_plan_ready": threading.Event(),
                              "manual_route_source_results": {},
                              "manual_route_city_arrived": {},
                              "manual_route_source_done": threading.Event(),
                              "manual_route_party_ready": threading.Event(),
                              "manual_route_done": threading.Event(),
                              "reconnecting": set(),  # username dang ROT + login lai (cho reconnect resync)
                              "disc_gen": 0,          # +1 moi khi co acc rot (bao cac nick khac phan ung)
                              "event_battle_active": False,
                              "event_battle_done": threading.Event(),
                              # MODE "digioi_train" (DG roi Train): pha hien tai cua CA PARTY +
                              # danh sach acc DA XONG DG (het gio). Du CA party xong -> chuyen pha
                              # "train" -> moi acc relogin vao mode train.
                              # KE HOACH do luong DIEU PHOI ghi ra (xem muc "DIEU PHOI PARTY").
                              # Luong acc CHI DOC. None = dieu phoi chua chay nhip nao.
                              "ke_hoach": None,
                              # KENH DICH do DIEU PHOI chot. Moi acc tu soi vao day trong
                              # keepalive va tu chuyen - KHONG bat tay, khong bao cao, nen acc
                              # da lam xong van bat duoc lenh moi. Xem _dieu_phoi_chot_kenh.
                              "kenh_dich": None,
                              # Lan cuoi DIEU PHOI ra lenh gom. Dung de khong ra lenh gom don dap
                              # (moi lenh abort moi acc dang di duong).
                              "dieu_phoi_gom_luc": 0.0,
                              # Party da bat dau train chua - luat watcher "thieu nguoi qua lau"
                              # doc khoa nay. TRUOC 05/09 KHONG AI GHI no (chi doc o dung 1 cho)
                              # nen luat do CHUA TUNG CHAY LAN NAO: 0 lan trong ca ngay log.
                              "training_started": False,
                              "dt_phase": "digioi",
                              "dt_train_prepared": False,
                              "summary_done": False}  # da log dong tong ket "party thoat het" chua
    return _party_state[pidx]


def _clear_stale_manual_route(st):
    """Huy route cua phien party cu, giu cmd_gen tang dan de worker cu khong nhan nham lenh moi."""
    with st["lock"]:
        next_gen = int(st.get("cmd_gen", 0)) + 1
        st["cmd"] = None
        st["cmd_gen"] = next_gen
        st["manual_route_gen"] = next_gen
        st["manual_route_plan"] = None
        st["manual_route_source_results"] = {}
        st["manual_route_city_arrived"] = {}
        st["manual_route_plan_ready"].clear()
        st["manual_route_source_done"].clear()
        st["manual_route_party_ready"].clear()
        st["manual_route_done"].clear()


def _route_mismatch_timed_out(state, leader_map, mismatch, now, timeout=15.0):
    if not mismatch:
        state.clear()
        return False
    if state.get("leader_map") != leader_map or state.get("msg") != mismatch:
        state.update(leader_map=leader_map, msg=mismatch, since=now)
        return False
    return now - state["since"] >= timeout


DT_RECHECK_SEC = 300   # dang cho dong doi -> cu 5' soat lai gio DG / Ho Phu mot lan


def _acc_het_gio_dg(c):
    """Acc nay HET GIO Di Gioi chua - DIEU PHOI TU DOC, khong acc nao khai bao (L2).

    Tra True = het gio · False = con gio · None = CHUA BIET (chua co dong ho server).

    Ban cu dung co `c._dg_da_xong` do CHINH ACC dat khi no tu ket luan "minh xong DG". Ghi co len
    client van la BAO CAO - chi khac cho cat: acc ket luan, dieu phoi tin theo. Acc ket luan sai
    (dang dis, chua login xong, chua nhan dong ho) la ca party dung cho no.

    Ba nguon SU THAT, doc thang tu client:
      1. `S:097-001` ma 2 <時間已滿> -> server noi thang la het gio;
      2. dong ho `S:085-001` id 0x1b (so phut DA DUNG) -> con >= 1 phut la CON GIO;
      3. chua co dong ho (`_last_digioi_ts` = 0, vd vua login lai) -> CHUA BIET, phai cho.

    `S:085-001` do SERVER TU DAY (crack client khong co goi xin - `protocolTable[85]` chi co ba
    nhanh S:085-*), va client ghi "跨日時會更新資料" = qua ngay server gui lai.
    """
    if c is None or not getattr(c, "running", False):
        return None                        # acc chet/dis -> khong ket luan gi
    if getattr(c, "_dg_enter_result", None) == 2:
        return True                        # server: <時間已滿>
    if not float(getattr(c, "_last_digioi_ts", 0.0) or 0.0):
        return None                        # chua nhan dong ho -> CHUA BIET
    try:
        return (DIGIOI_LIMIT - c.digioi_minutes_live()) < 1
    except Exception:
        return None


def _dt_recheck_time_left(username, label):
    """Acc da bao xong DG: CON GIO THAT khong? Con Ho Phu thi dung. Tra True = con gio, vao lai DG.

    Doc so SERVER (RoleCount 0x1b = so phut DA DUNG), khong tin ket luan cu: sang nay da co ca
    "bi tinh het gio nhung thuc ra van con", stop/start la vao lai duoc.
    """
    c = account_clients.get(username)
    if c is None or not c.running:
        return False
    used = getattr(c, "digioi_minutes", None)
    if used is None:
        return False                       # chua co so server -> khong doan
    left = DIGIOI_LIMIT - int(used)
    if left > 0:
        log.warning("[%s] DG+Train: dang cho dong doi nhung SOAT LAI thay CON %d phut DG "
                    "(server: da dung %d/%d) -> vao lai DG danh tiep",
                    label, left, int(used), DIGIOI_LIMIT)
        return True
    # Het gio that -> con Ho Phu thi dung de duoc them gio (config phai bat).
    if not getattr(c, "use_digioi_ho_phu", False):
        return False
    if not c.use_di_gioi_ho_phu():
        return False
    # Ho Phu dung xong: server cap nhat lai 0x55/0x1b -> cho vai giay roi doc lai.
    for _ in range(10):
        time.sleep(1.0)
        used2 = getattr(c, "digioi_minutes", None)
        if used2 is not None and DIGIOI_LIMIT - int(used2) > 0:
            log.warning("[%s] DG+Train: da dung Di Gioi Ho Phu -> con %d phut -> vao lai DG",
                        label, DIGIOI_LIMIT - int(used2))
            return True
    log.info("[%s] DG+Train: da dung Ho Phu nhung server chua cong gio -> cho tiep, soat lai sau",
             label)
    return False


def _party_members_off_place(c, pidx):
    """Member nao KHAC MAP hoac KHAC KENH voi leader -> tra list mo ta; rong = ca party cung cho.

    Doc THANG tu client cua tung acc (cung tien trinh) nen chinh xac, khong phai bao cao cu.
    Khac map = leader KHONG THAY entity -> moi chac chan that bai, phai gom lai truoc.
    """
    def _ch(cli):
        """Kenh THAT (hoi lai server neu cache qua han) - so hai so nho san la sai suot ma
        khong ai biet, xem `GameClient.kenh_that`."""
        try:
            return cli.kenh_that()
        except Exception:
            return None

    out = []
    lead_map = getattr(c, "current_map", None)
    lead_ch = _ch(c)
    for u, _p, is_lead, _k in party_accounts(pidx):
        if is_lead:
            continue
        mc = account_clients.get(u)
        if mc is None or not mc.running:
            continue                       # acc chua len/da rot -> duong reconnect lo, khong tinh
        if is_joined(pidx, getattr(mc, "self_entity", None)):
            continue                       # da vao party roi
        m_map = getattr(mc, "current_map", None)
        m_ch = _ch(mc)
        _ent = getattr(mc, "self_entity", None)
        if m_map is not None and lead_map is not None and m_map != lead_map:
            out.append("%s o map %s (leader %s)" % (u, m_map, lead_map))
        elif m_ch is not None and lead_ch is not None and m_ch != lead_ch:
            out.append("%s o kenh %s (leader %s)" % (u, m_ch, lead_ch))
    return out


def _party_khong_thay_nhau(c, pidx, grace=30.0):
    """Member CUNG map + CUNG kenh (theo hai con so) ma leader CHUA HE thay quanh minh.

    `0x03 PlayerAppear` chi duoc server gui cho nguoi CUNG SCENE + CUNG INSTANCE. Chua he nhan
    duoc = hai con so kia dang noi doi, thuc te khac instance -> loi moi khong the toi noi, va
    server KHONG gui ma loi nao (dung trieu chung da duoi may ngay).

    KHONG ve thanh: cach chua la LEADER CHON KENH MOI roi dong bo lai - nhung ca party phai DA
    RA DIEM AN TOAN truoc, dung o diem quai thi doi kenh xong van hong (user chot 30/08).
    GRACE 30s: vua toi map thi chua kip nhan 0x03 cua nhau la binh thuong.
    """
    out = []
    _mem = getattr(c, "_chua_thay_tu", None)
    if _mem is None:
        _mem = c._chua_thay_tu = {}
    lead_map = getattr(c, "current_map", None)
    for u, _p, is_lead, _k in party_accounts(pidx):
        if is_lead:
            continue
        mc = account_clients.get(u)
        if mc is None or not mc.running:
            continue
        ent = getattr(mc, "self_entity", None)
        if ent is None:
            continue
        if is_joined(pidx, ent):
            _mem.pop(bytes(ent), None)
            continue
        if getattr(mc, "current_map", None) != lead_map:
            continue                       # lech map -> nhanh khac lo, khong ket luan o day
        try:
            vi = c.da_thay_tan_mat(ent)
        except Exception:
            vi = ""
        if not vi:
            _mem.pop(bytes(ent), None)
            continue
        t0 = _mem.setdefault(bytes(ent), time.time())
        if time.time() - t0 >= grace:
            out.append("%s: %s" % (u, vi))
    return out


def _dt_wait_all_digioi_done(pidx, username, label, stopped_fn):
    """MODE digioi_train: acc nay DA XONG DG -> DUNG YEN cho CA PARTY xong DG.
    Du het -> doi pha party sang "train" (moi acc relogin se chay mode train).

    Tra ve:
      True         - ca party xong DG, san sang di train
      "back_to_dg" - soat lai thay CON GIO DG -> quay lai DG danh tiep (giu ket noi)
      "lenh_moi"   - dieu phoi bump `reform_gen` -> thoi cho, di nghe lenh (giu ket noi)
      False        - BI STOP. CHI dung cho stop that: caller khong set `relogin_train` nen
                     `_ket_thuc_pha_dg()` se DONG KET NOI, acc chet han.
    CHO KHONG GIOI HAN (theo yeu cau user): chi thoat khi CA PARTY xong DG hoac bam Stop -> acc xong
    SOM khong bi tat game oan trong luc acc khac con dang DG (DG toi 2h). Acc da tat/rot han khong
    tinh vao (users = acc DANG CHAY) nen 1 acc chet cung khong ket ca party mai mai."""
    st = _pstate(pidx)
    # KHONG dat co "minh da xong DG" nua (L2 - user 09/09: "bot dieu phoi phai lam va kiem tra
    # cac acc, deo phai cac tu lam roi bao cao"). Ghi co len client van la acc TU KET LUAN roi
    # dieu phoi tin theo; acc ket luan sai (dang dis / chua nhan dong ho server) la ca party dung
    # cho no. Gio dieu phoi tu doc `_acc_het_gio_dg()` cua tung client.
    # Xong DG truoc -> DUNG CHO ca party (co the toi 2 TIENG). Danh dau CHO de watcher khong keu
    # "TREO" oan, va de biet day la cho HOP LE chu khong phai lech viec.
    set_account_activity(username, "xong Di Gioi - cho ca party xong", phase="wait")

    def _prepare_train_phase_once():
        prepared = False
        with st["lock"]:
            if not st.get("dt_train_prepared"):
                st["dt_train_prepared"] = True
                # Phase DG da dung cac flag sync/moi party nay. Sang phase train phai reset
                # nhu mot luot train moi, neu khong leader/member co the dung im o thanh vi doc
                # lai state cu cua phase DG.
                for key in ("leader_ok", "leader_bad", "leader_gone", "invited", "channel_ready",
                            "channel_failed",
                            "stop_leader_done", "route_party_ready", "route_done", "rally_ready",
                            "path_done", "route_plan_ready"):
                    st[key].clear()
                st["channel"] = None
                st["channel_expected_map"] = None
                st["mob_spot"] = None
                st["rally_point"] = None
                st["mob_path"] = None
                st["route_plan"] = None
                st["event_exit_now"].clear()
                st["2k_ket_qua"] = None
                st["o5_state"] = "idle"
                st["o5_broke"] = False
                st["o5_need_redo"] = False
                st["team_dungeon_state"] = {}
                st["team_dungeon_broke"] = {}
                st["team_dungeon_tries"] = {}
                st["team_dungeon_skip_all"] = False
                st["team_dungeon_need_redo"] = False
                st["started_train"] = 0
                st["dungeon_done"] = 0
                st["dailies_done"] = 0
                prepared = True
        if prepared:
            reset_party_joined(pidx)
        return prepared

    last_log = 0.0
    last_recheck = 0.0
    _st_gen = _pstate(pidx)
    _gen0 = _st_gen.get("reform_gen", 0)
    while True:
        if stopped_fn():
            return False
        # NGHE LENH DIEU PHOI. Vong nay co the dung yen HANG TIENG (cho dong doi xong DG); ket o
        # day ma diec voi lenh thi dieu phoi co ra lenh cung vo ich - dung cai da giet party 15
        # (07/09): dieu phoi in "LAP LAI PARTY" moi 2 phut suot 26 phut ma khong ai nghe, vi leader
        # dang ket trong mot vong cho khac.
        if _st_gen.get("reform_gen", 0) != _gen0:
            # TRA "lenh_moi" CHU KHONG PHAI False. `False` o ham nay co nghia la BI STOP, va caller
            # doi xu voi no bang cach KHONG set `relogin_train` -> `_ket_thuc_pha_dg()` goi
            # `c.close()` -> ACC CHET HAN, khong ai login lai.
            #
            # Ca that 09/09 party 2 (user: "danh DG xong thay nhieu acc tat vay"):
            #   06:02:15 [party 2] REFORM gen -> 5 - ep dong bo (nhe): watcher: party thieu nguoi qua lau
            #   06:02:16 [gamo]  dieu phoi ra lenh moi khi dang cho ca party xong DG -> thoi cho
            #   06:02:22 >>> PARTY 2 DA THOAT HET vi: het gio Di Gioi trong luc sync kenh DG
            # `sga001` im tu 06:02:21 den het log (hon 4 tieng). Sau nay 6 party di y het duong nay.
            #
            # "Co lenh dieu phoi moi" la ly do de THOI CHO va di NGHE LENH - khong phai ly do de
            # tat acc.
            log.info("[%s] dieu phoi ra lenh moi khi dang cho ca party xong DG -> thoi cho", label)
            return "lenh_moi"
        # ==== TU SOAT LAI TRONG LUC CHO (moi DT_RECHECK_SEC) ====
        # Dang cho dong doi (co the 2 TIENG) -> thoi gian chet. Truoc khi chiu dung yen, kiem tra
        # lai bang so SERVER xem CO THAT SU het gio khong, va con Ho Phu thi dung de vao danh tiep.
        # (Sang nay da co bug "bi tinh het gio nhung thuc ra van con" -> khong duoc tin ket luan cu.)
        if time.time() - last_recheck > DT_RECHECK_SEC:
            last_recheck = time.time()
            try:
                if _dt_recheck_time_left(username, label):
                    return "back_to_dg"
            except Exception as e:
                log.warning("[%s] DG+Train: loi soat lai gio DG (bo qua): %s", label, e)
        users = set(_dt_party_usernames(pidx))
        switch_to_train = False
        n_users = 0
        # BOT DOC THANG tung client (L2): no dieu khien acc nen no TU BIET acc nao het gio DG -
        # khong acc nao khai bao, ke ca kieu "ghi co len client cua chinh minh".
        #
        # `_acc_het_gio_dg` tra None = CHUA BIET (acc dang dis / chua nhan dong ho server). Chua
        # biet thi KHONG tinh la xong: tinh la xong se lam ca party bo di train trong khi acc do
        # con nguyen gio DG (ca that 09/09 `haabo` - chua vao DG lan nao van bi ghi "xong DG").
        done = {u for u in users if _acc_het_gio_dg(account_clients.get(u)) is True}
        with st["lock"]:
            if st.get("dt_phase") == "train":
                switch_to_train = True  # acc khac da chuyen pha roi -> di train luon
            # DG+Train giu nguyen nguyen tac DU PARTY: chi khi tat ca acc trong party da bao
            # xong DG moi chuyen pha train. Acc dang bi STOP thi bo qua de Stop khong treo.
            elif users and users <= done:
                st["dt_phase"] = "train"
                switch_to_train = True
                n_users = len(users)
        if switch_to_train:
            if _prepare_train_phase_once():
                log.info("[%s] DG+Train: CA PARTY (%d acc) da xong Di Gioi -> reset state DG, "
                         "CHUYEN PHA TRAIN", label, n_users or len(users))
            return True
        if time.time() - last_log > 60:
            last_log = time.time()
            # NEU RA TEN + MAP cua acc con THIEU. Truoc day chi in "(2/5 acc xong)" -> treo ca dem
            # ma khong biet dua nao ket o dau, acc ket lai KHONG in log gi (log 22:59-23:07: 3 acc
            # dung o Quang Truong, khong mot dong log).
            missing = sorted(users - done)
            detail = ", ".join(
                "%s@map%s" % (u, getattr(account_clients.get(u), "current_map", "?"))
                for u in missing
            )
            log.info("[%s] DG+Train: xong DG, DUNG YEN cho party (%d/%d acc xong) - cho khong "
                     "gioi han | CON THIEU: %s", label, len(done & users), len(users),
                     detail or "-")
        time.sleep(5)


def run_account(username, password, pidx, is_leader, is_picker=False, is_reconnect=False,
                reuse_client=None):
    # is_reconnect=True (supervisor goi lai sau khi rot): RECONNECT NHE - bo qua daily/gacha/mail/
    # vantieu (da lam phien truoc) -> vao world la di THANG toi sync kenh + gom party + keo ra bai,
    # KHONG teleport ve Trac Quan lam daily (truoc day: reconnect chay full startup -> lech nhip leader
    # -> ve thanh khong duoc keo -> "SAI MAP -> THOAT" chet luon).
    label = username
    role = "LEADER" if is_leader else "member"

    def _nhip_moi_party(train_map_on=True):
        """MOT NHIP cua moi vong LAP PARTY - phan BAT BUOC, dung chung cho moi mode.

        RANH GIOI (user chot 07/09: "cai nay la ham lap pt khi tat ca deu da cung map dung ko, con
        lam sao cung map la tuy tinh nang khac nhau thi se khac nhau nhi"):

          - "LAM SAO CUNG MAP" thi MOI TINH NANG MOT KIEU, khong gop:
                train  -> di route qua cong        DG    -> gui 0x61 vao Di Gioi
                40NPC  -> tele toi map event       2K    -> di bo xuong tang gom
          - "CUNG MAP ROI THI LAP PARTY" thi GIONG HET NHAU o moi tinh nang - va do la phan nay.

        Truoc day co NAM vong lap party viet rieng (reform / Di Gioi / train / manual x2), moi ban
        thieu mot manh khac nhau. User 07/09: "cai party nay khac deo gi luc lap pt khi train dau,
        ma tu nhien gio loi tum lum vay, co che ko dung chung dc a?" - dung, va do la ly do moi lan
        sua chi vá duoc MOT ban, bon ban kia van nguyen.

        Nhip nay lam dung ba viec khong duoc thieu o bat ky vong nao:
          1. NGHE LENH KENH cua dieu phoi (khong thi acc dung mot kenh, moi vao hu khong),
          2. CHI MOI KHI DIEU PHOI BAO MOI (`VIEC_MOI`) - moi luc khac la sai luot,
          3. gui loi moi.

        MOI LA VIEC CO LENH, KHONG PHAI MAC DINH.
        Truoc day cho nay la `if viec == VIEC_GOM: return "gom"` roi moi - tuc MOI la mac dinh, con
        chan la ngoai le, va chi biet DUNG MOT lenh. Moi lenh khac (`dong_bo`, `lam`, `di_train`,
        `ra_quai`) deu roi thang xuong moi party, ke ca khi dang dung o cho khong duoc phep lap.
        Them lenh moi vao chuoi dieu phoi la lai lot them mot duong.

        Ca that party 4, 14/09 (user: "m vua cai lon gi ma party4 lai lap party o Trac quan"):
            12:13:13 [party 4] gen 9: viec=gom - dang o thanh DI NGANG QUA 12001, chua toi thanh
                                      tap ket 21001 -> di tiep roi moi lap party
            12:13:14..40 [thmo] (LEADER) dieu phoi bao GOM (...) -> thoi moi, gom lai   <- chan DUNG
            12:13:43 [party 4] gen 10: viec=dong_bo - cung map nhung ['sga009','sga013'] KHONG THAY
                                       duoc dong doi (khac instance du cung so kenh)
            12:13:45 [thmo] (LEADER) moi 4 member theo entity ...      <- cua khong biet lenh nay
            12:13:53 [thmo] (LEADER) DU PARTY (4/4 member join)        <- lap party tai 12001
        Bac chan "thanh trung gian thi khong lap pt" van dung: no da ra lenh `gom` luc 12:13:13.
        Nhung 30 giay sau dieu phoi thay them chuyen khac (hai acc khac instance) nen doi lenh sang
        `dong_bo` - va ngay khi lenh khong con dung chu `gom`, leader moi party luon.

        Tra: "" = da moi; "gom" = dieu phoi bao gom, caller phai thoi moi va di gom;
             "cho" = chua toi luot moi (lenh khac) - caller cu chay tiep vong cua no.
        """
        _nghe_lenh_kenh()
        _kh = _ke_hoach(st)
        _viec = (_kh or {}).get("viec")
        if _viec == VIEC_GOM:
            return "gom"
        # CHI `gom` / `dong_bo` moi that su la "CHUA TOI LUOT LAP PARTY" - hai lenh do dang keo
        # acc di cho khac, moi luc do la vo ich (loi moi khong toi noi) va con dam vao chinh lenh.
        #
        # KHONG chan `lam`: no nghia la "khong con gi phai xu ly o cap party -> cu lam viec cua
        # minh", ma lap party CHINH LA viec cua leader. Chan no la party khong bao gio hinh thanh.
        # Ca that party 47 va 51, 14/09 (mode Di Gioi - dieu phoi ra `lam` vi DG khong di qua chuoi
        # gom/moi thuong):
        #   17:47:07 [vumot] (LEADER) chua toi luot moi party: dieu phoi dang ra lenh 'lam' ()
        #   17:47:15 [vumot] (LEADER) Di Gioi moi 789s chua du party (0/4) -> MOI TIEP
        # 13 phut, 5 acc dung im o 49942, roster 0/4.
        #
        # `di_train` / `ra_quai` cung khong chan: toi do la DA DU DOI roi, moi them khong hai gi.
        if _kh is not None and _viec in (VIEC_DONG_BO,):
            if time.time() - float(st.get("moi_cho_log", 0.0) or 0.0) > 60.0:
                st["moi_cho_log"] = time.time()
                log.info("[%s] (LEADER) chua toi luot moi party: dieu phoi dang ra lenh '%s' (%s)",
                         label, _viec, (_kh or {}).get("ly_do") or "")
            return "cho"
        try:
            _invite_party_participants(c, train_map_on, gap=1.0)
        except Exception:
            pass
        return ""

    def _nghe_lenh_kenh():
        """THI HANH lenh kenh cua dieu phoi. Goi o MOI vong dai, khong rieng keepalive.

        Lenh cua dieu phoi truoc day chi duoc doc o keepalive. Acc nao tut vao mot vong con
        (leader trong vong MOI party, member trong vong cho...) la DIEC vong do - va do la
        noi no o lau nhat.

        Log 07/09 party 1 (user: "t thay leader luon o 1 kenh con member o kenh khac"):
            20:39:21 [chihao] (member) DIEU PHOI chot kenh 45, minh dang o 5 -> tu chuyen
            20:39:21 [minh]   (member) DIEU PHOI chot kenh 45, minh dang o 6 -> tu chuyen
            20:42:33 [xGAx] (LEADER) chua moi 2 member vi chua xac nhan live dung map/kenh:
                     ['38d0d2f8:lech kenh live 45!=6', '0c1dd3f8:lech kenh live 45!=6']
        Hai member DA sang 45 dung lenh; leader ket trong vong moi nen van o 6, roi chinh no
        doi HO sang kenh cua no.
        """
        _kd = st.get("kenh_dich")
        if not _kd:
            return
        if int(getattr(c, "current_channel", 0) or 0) == int(_kd):
            return
        # KHONG DOI KENH KHI PARTY DANG DANH EVENT.
        #
        # Doi kenh = doi INSTANCE, va no keo theo hai thu chet nguoi giua event: `switch_channel`
        # phai ROI DOI truoc (luat `Team.IsAlone` cua client), con doi scene thi bot chay
        # `scene_resume` -> gui `C:020-006 <事件下一步>`. Gui goi EVENT luc server chua giai xong
        # tran = `S:000-000` ma 47 `戰鬥未結束事件先結束` -> DUT KET NOI.
        #
        # 07/09 (user: "vao tran van vang", "cai nay truoc chay ok ma sao may lam hong no"):
        #   21:25:49 [thsau] (LEADER) 40NPC: ca party da hoi phuc -> mo tran tiep
        #   21:25:53 [thsau] SERVER NGAT KET NOI: ma la 47
        #   21:24:07 [xGAx]  (LEADER) sync kenh: 4/5 acc da sang kenh 41   <- doi kenh giua 40NPC
        # Cung luc do co thsau/thmo/tonba/lbumot/xGAx dis ma 47 trong 32 giay.
        #
        # DUNG la loi cua chinh ban sua hom nay: truoc day lenh kenh CHI doc o keepalive (mot cho),
        # gio `_nghe_lenh_kenh()` duoc goi o CHIN vong - tan suat doi kenh tang han, nen no trung
        # vao giua tran event thuong xuyen hon nhieu.
        #
        # Ba moc an toan (event dang danh / trong tran / grace ket tran) dung CHUNG mot ham voi
        # duong dieu phoi tu gui - `_kenh_doi_duoc_ngay`. Hai ban sao thi som muon lech nhau.
        if not _kenh_doi_duoc_ngay(c, st):
            return
        log.info("[%s] (%s) DIEU PHOI chot kenh %s, minh dang o %s -> tu chuyen",
                 label, role, _kd, getattr(c, "current_channel", None))
        try:
            # Hong thi thoi - dieu phoi DOC THANG `_chan_switch_result`/`_chan_switch_target`
            # cua client nay o nhip sau. Khong bao cao gi len.
            c.switch_channel(int(_kd), wait=4.0, retries=1, theo_lenh=True)
        except Exception as e:
            log.warning("[%s] loi tu chuyen sang kenh dich %s: %s", label, _kd, e)

    has_leader = config.PARTY_LEADER_ACC.get(pidx) is not None
    _resume_2k = False   # True = login lai khi dang DUNG TRONG thap 2K -> leo tiep tai cho
    st = _pstate(pidx)
    # EP DONG BO: ghi epoch phien nay dang chay. Sau khi relogin (do resync) run_account chay lai ->
    # doc epoch MOI (da bump) -> khop -> khong raise lai. _resync_ck so sanh voi gia tri nay.
    account_sync_epoch[username] = st.get("sync_epoch", 0)
    stop_ev = account_stops.get(username)   # GUI yeu cau STOP -> thoat moi giai doan
    def _stopped():
        return stop_ev is not None and stop_ev.is_set()
    def _leader_thread_active():
        leader_acc = config.PARTY_LEADER_ACC.get(pidx)
        if not leader_acc:
            return False
        t = account_threads.get(leader_acc)
        ev = account_stops.get(leader_acc)
        return t is not None and t.is_alive() and not (ev is not None and ev.is_set())
    er = {"r": "ket thuc binh thuong (het gio hoac GUI dung)"}  # ly do thoat (de tong ket party)
    # MODE digioi_train: xong DG + ca party xong -> CAN relogin de chay pha TRAIN (khong phai "rot").
    # Dung dict de set duoc tu cac nhanh ben trong (khong vuong scope).
    _dt = {"relogin_train": False}
    def _reason(msg):
        er["r"] = msg
    # Server (IP) theo config rieng cua party
    _pc0 = getattr(config, "PARTY_CONFIG", {}).get(pidx, {})
    server_ip = _pc0.get("server_ip") or config.GAME_HOST
    server_name = _pc0.get("server", "?")
    server_id = _pc0.get("server_id", 1)
    _login_failed = False   # True neu login/vao world that bai 6 lan -> supervisor van thu lai (backoff)
    _unexpected_error = False  # True neu dinh Exception bat ngo -> cho relogin (dung de acc chet han vi loi thoang qua)
    try:
        # --- Login + cho vao world THUC SU (co self_entity VA co current_map) ---
        c = None
        ok = False
        attempt = 0
        _rl_hits = 0   # so lan bi server CHAN TOC DO dang nhap (ma 90)
        if reuse_client is not None:
            # CHUYEN PHA (DG -> train) GIU NGUYEN KET NOI: khong dang nhap lai.
            # User: "relogin thi lam kho login" - server chan toc do dang nhap (ma 90), 5 acc
            # relogin cung luc la ca party ket vong login hang phut. Ma viec o day chi la doi PHA,
            # ket noi van tot nguyen.
            c = reuse_client
            c._label = label; c._username = username
            c.party_idx = pidx
            c._o5_team_fn = (lambda o5d, _c=c:
                             _handle_o5_team(_c, st, username, label, pidx, is_leader, _stopped, o5d))
            account_clients[username] = c
            ok = True
            log.info("[%s] CHUYEN PHA train - GIU NGUYEN ket noi (khong dang nhap lai)", label)
        while not ok and attempt < 6:
            if _stopped():
                log.info("[%s] STOP truoc khi login xong", label); return
            try:
                cred = login(username, password)
                c = GameClient(cred["user_id"], cred["access_token"], host=server_ip, server_id=server_id)
                c._label = label; c._username = username
                log.info("[%s] server=%s (%s) id=%s", label, server_name, server_ip, server_id)
                c.party_idx = pidx
                # Hook o5 pho ban to doi = BUOC CUOI claim_daily_quests (xem _handle_o5_team)
                c._o5_team_fn = (lambda o5d, _c=c:
                                 _handle_o5_team(_c, st, username, label, pidx, is_leader, _stopped, o5d))
                c.submit_delay = 0.3
                # 2 co "chet -> ve thanh" cua HOP MAY: phai set TRUOC connect() vi chuoi 0x41 duoc
                # gui ngay trong connect(). Doc thang config.PARTY_CONFIG chu khong dung bien pcfg
                # (pcfg mai dong ~1153 moi co, tuc SAU connect). Mac dinh BAT = giong client that.
                _pc0 = (getattr(config, "PARTY_CONFIG", {}) or {}).get(pidx, {}) or {}
                c.death_return_town = bool(_pc0.get("death_return_town", True))
                c.pet_death_return_town = bool(_pc0.get("pet_death_return_town", True))
                # VAN TIEU per-acc (bang setting Hoi HP/SP cua acc). Mac dinh: BAT, KHONG tick con
                # nao -> vantieu_candidates() tra ve TAT CA = y het hanh vi cu.
                _vt0 = (getattr(config, "ACCOUNT_VANTIEU", {}) or {}).get(username, {}) or {}
                c.vantieu_enable = bool(_vt0.get("on", getattr(config, "VANTIEU_ENABLE", True)))
                c.vantieu_pick_ids = tuple(_vt0.get("pets") or ())
                c.connect()
                # cho self_entity + map (map=None = chua vao world xong)
                for _ in range(15):
                    if c.self_entity is not None and c.current_map is not None:
                        ok = True; break
                    time.sleep(1)
                if ok:
                    break
                # SERVER CHAN TOC DO (S:000-000 ma 90): thu lai sau ~20s van bi chan tiep -> ket
                # vong. Log that (party 6, 23:15-23:17): taot001/taot003 lap lai deu dan moi ~22s,
                # lan nao cung "DANG NHAP QUA THUONG XUYEN". Backoff o supervisor KHONG cuu duoc vi
                # vong nay nam TRONG run_account, chua he thoat ra toi do.
                # -> cho lau dan, va KHONG tinh la lan login that bai (giong nhanh error_code=1):
                #    day la server chan, khong phai sai tai khoan.
                if int(getattr(c, "disconnect_cause", 0) or 0) == DISCONNECT_RATE_LIMIT:
                    _rl = _rl_hits + 1
                    _w = min(30 * _rl, 300)
                    log.warning("[%s] chua vao world - SERVER CHAN TOC DO DANG NHAP (lan %d) "
                                "-> nghi %ds roi thu lai, KHONG tinh la fail", label, _rl, _w)
                    c.close()
                    for _ in range(_w):
                        if _stopped():
                            break
                        time.sleep(1)
                    _rl_hits = _rl
                    continue
                log.warning("[%s] chua vao world (entity=%s map=%s) -> login lai...",
                            label, c.self_entity is not None, c.current_map)
                c.close(); attempt += 1; time.sleep(5)
            except Exception as e:
                # login() (auth HTTP) / connect() LOI (server lom, mang chap) -> KHONG de nick CHET:
                # coi nhu 1 lan thu that bai, backoff 5s roi thu lai; het 6 lan -> _login_failed ben
                # duoi (supervisor reconnect vo han). Truoc day login() raise -> thoat ca vong -> thread
                # chet han (bug: nick "tat" khi server lom lam login HTTP fail).
                if _login_error_code(e) == 1:
                    wait = random.randint(LOGIN_ERR1_RETRY_MIN_SEC, LOGIN_ERR1_RETRY_MAX_SEC)
                    log.warning("[%s] login error_code=1 (%s) -> nghi %ds roi thu lai, KHONG tinh la fail",
                                label, _login_error_message(e), wait)
                    try:
                        if c is not None: c.close()
                    except Exception: pass
                    for _ in range(wait):
                        if _stopped():
                            break
                        time.sleep(1)
                    continue
                attempt += 1
                log.warning("[%s] login/connect loi (lan %d): %s -> thu lai", label, attempt, e)
                try:
                    if c is not None: c.close()
                except Exception: pass
                time.sleep(5)
        if not ok:
            _login_failed = True   # -> supervisor thu login lai (backoff), KHONG de nick chet luon
            _reason("login/vao world that bai (6 lan) -> supervisor thu lai")
            log.warning("[%s] >>> LOGIN/VAO WORLD THAT BAI sau 6 lan -> supervisor se thu lai", label)
            try:
                if c is not None: c.close()
            except Exception: pass
            return
        _clients.append(c)
        account_clients[username] = c     # GUI doc trang thai
        st["reconnecting"].discard(username)  # (reconnect) da vao world lai -> khong con "dang rot"
        # MOC "CO ACC VUA VAO WORLD" cho GUI. Bang "Chu y" (tui / Ba Dau / quan doan / du diem /
        # lo) dung lai danh sach tu nam nguon cho tung acc, qua nang de chay moi giay tren main
        # thread Tk (user 13/09: "click doi party thay do rat lau moi load ra... not responding").
        # GUI cache theo so nay: SO DOI = dung lai MOT lan, roi giu.
        #
        # Cho nay la moc duy nhat dung cho CA login dau LAN reconnect (user 14/09: "cu acc login
        # xong la cache lai 1 lan, reconnect khi dis cung cache lai, don gian la login vi bat ky
        # ly do gi cung cache lai").
        with st["lock"]:
            st["login_gen"] = int(st.get("login_gen", 0) or 0) + 1
        label = c.char_name or username   # log theo TEN NHAN VAT (neu da resolve), fallback username
        if is_leader and c.char_name:
            # Tu dong them ten nhan vat leader vao whitelist "leaders" cua party - user da cau hinh
            # account nay la leader trong Party roi thi khong can go tay lai ten o whitelist rieng.
            config.record_leader_name(pidx, c.char_name)
        login_map = c.current_map         # map LUC LOGIN (doc som, it bi pollution) - dung de check train
        log.info("[%s] (%s) vao world.", label, role)
        log.info("[%s] >>> MAP HIEN TAI = %s <<<  (dung ID nay de setup START_CITY_ID/TRAIN)",
                 label, login_map)
        # Doc party config SOM de biet mode/map ngay sau login. Neu dang login o map train thi
        # chay ve safe TRUOC cac viec login chores (qua, van tieu, shop...) de khoi dung giua bai
        # quai lau roi bi keo tran.
        pcfg = getattr(config, "PARTY_CONFIG", {}).get(pidx, {})
        # NHOM "TU DON TUI DO" phai gan NGAY O DAY (khong de xuong duoi cung voi cac config khac):
        # decompose_junk_scrolls() / discard_junk_items() / sell_noi_dat() duoc goi trong khoi
        # "viec hang ngay sau login" o TREN cho gan config cu -> luc do co van la mac dinh
        # (auto_decompose_scrolls = False) nen user TICK ma bot KHONG phan giai (bug that user gap).
        c.auto_bag_clean = bool(pcfg.get("auto_bag_clean", True))
        c.auto_discard_junk = bool(pcfg.get("auto_discard_junk", True))
        c.auto_decompose_scrolls = bool(pcfg.get("auto_decompose_scrolls", False))
        c.scroll_modes = _scroll_modes_map(pcfg.get("scroll_modes"))
        # CUNG LY DO: donate_legion() (dong gop nguyen lieu quan doan) cung nam trong khoi
        # viec-hang-ngay o TREN cho gan config cu -> gan muon thi material_modes con RONG -> bot
        # DONG GOP LUON nguyen lieu user danh dau "Giu lai" (mat do). sell_noi_dat() cung doc
        # auto_donate_materials de ban nguyen lieu khi acc chua co quan doan.
        c.auto_donate_materials = bool(pcfg.get("auto_donate_materials", True))
        c.material_modes = _scroll_modes_map(pcfg.get("material_modes"))   # {tid:'keep'} - nguyen lieu GIU
        # DOI QUA SU KIEN (config CHUNG CA PARTY, o Cai dat nang cao). Gan SOM cung ly do tren:
        # claim_daily_quests() (goi trong khoi viec-hang-ngay ben duoi) doc 2 bien nay.
        c.auto_event_exchange = bool(pcfg.get("auto_event_exchange", False))   # mac dinh TAT
        c.event_exchange_items = list(pcfg.get("event_exchange_items") or [])  # qua CUOI user tick
        # Chu ky su kien LUC USER TICK -> bot tu tu choi neu su kien da doi (xem do_event_exchange)
        c.event_exchange_sig = pcfg.get("event_exchange_sig", "") or ""
        _early_sc = pcfg.get("start_city_id", getattr(config, "START_CITY_ID", 0))
        # PARTY "TU CHON MAP" (`train_pick`): `start_city_id` trong config la 0 - so that nam o
        # `st["auto_train"]` do DIEU PHOI chot. Doc thang pcfg thi `_early_sc=0` -> `_early_tm=None`
        # -> CA khoi "ra safe truoc login chores" ben duoi bi bo qua, du acc dang dung giua bay quai.
        #
        # Ca that party 1, 13/09 (user: "log in va dung cho quai la bi danh lien tuc, deo lap duoc
        # party, truoc la login xong chay ra diem safe roi ma sao gio van bi danh"):
        #   23:16:56 [sga014] RESYNC pos tu 0x03 = (3020,1000) map=21833   <- trong bai, khong safe
        #   23:16:57 [sga015] RESYNC pos tu 0x03 = (3020,1000) map=21833
        #   23:18:30 [sga013] RECONNECT: server rot -> login lai sau 5s
        #   23:18:41 [chihao188] RECONNECT: server rot -> login lai sau 5s
        # Game cho login lai DUNG CHO logout, tuc ngay diem quai. Khong co dong "ve safe truoc
        # login chores" nao ca - vi `_early_sc` = 0.
        if not _early_sc or pcfg.get("train_pick"):
            _bai = _map_train_dich(pidx, _pstate(pidx))
            if _bai:
                _early_sc = _bai
        _early_raw_mode = pcfg.get("mode")
        _early_dt_mode = (_early_raw_mode == "digioi_train")
        _early_tm = config.TRAIN_MAPS.get(_early_sc)
        if _early_dt_mode:
            _early_mode = "digioi" if _pstate(pidx).get("dt_phase", "digioi") == "digioi" else "train"
        else:
            _early_mode = _early_raw_mode or ("train" if _early_tm else (
                "digioi" if _early_sc == config.DIGIOI_MAP_ID else ("stand" if _early_sc == 0 else "city")
            ))
        _early_train_safes = []
        if _early_tm is not None:
            _rs = _resolve_train_safe(c, _early_sc, _early_tm.get("safe", []))
            if _rs is not None:
                _early_train_safes.append(_rs)
        _login_safe_done = False
        if _early_mode == "train" and _early_tm is not None:
            c.flee_mode = True
        # RA SAFE TRUOC KHI LAM VIEC VAT - dieu kien la DANG DUNG TREN MAP TRAIN, khong phai
        # "mode == train". Truoc day chot `_early_mode == "train"` nen mode DG+Train o pha
        # `digioi` bi bo qua HOAN TOAN du no dang dung ngay tren bai quai: acc ngoi lam thanh tuu,
        # diem danh, donate quan doan, van tieu, thu cuoi... giua bay quai (party 11, 31/08 09:25
        # - `MODE=digioi start_city=23821`, khong he co dong "ve safe truoc login chores").
        if _early_tm is not None and login_map == _early_sc and _early_train_safes:
            _safe0 = _nearest_safe(c.pos, _early_train_safes)
            try:
                log.info("[%s] (%s) login o map train (mode=%s) -> ve safe %s truoc login chores",
                         label, role, _early_mode, _safe0)
                c.navigate_to(*_safe0, flee=True)
                login_map = c.current_map
                _login_safe_done = True
            except Exception as e:
                log.warning("[%s] loi ve safe ngay sau login (bo qua): %s", label, e)
        c.log_bag_delayed()   # In tui khi snapshot ve + on dinh (adaptive, toi da 8s) -> dinh danh item
        next_vantieu = None
        next_phuc_than = 0.0   # 0.0 -> kiem tra NGAY lan dau (khong cho 30p roi moi dung lan dau)
        next_ho_phu = 0.0      # Di Gioi Ho Phu: check login + moi 3p, chi khi mode Di Gioi
        c.fight_legion_boss = pcfg.get("fight_legion_boss", True)
        c.di_gioi_level = int(pcfg.get("di_gioi_level", 2))   # idx 1..15 cap quai DG (mac dinh 2=cap25)
        # TU CHON CAP QUAI DG: suy tu level party -> MOC gan nhat (bang nhau lay moc THAP hon).
        # Quyet 1 LAN cho ca party giong _auto_train_target: co random dau nhung level lay tu
        # account_last cua acc chua login co the doi giua cac lan goi -> moi acc ra 1 moc khac.
        _dgp = pcfg.get("di_gioi_pick") or ""
        if _dgp in train_pick.PICK_KEYS:
            _dg_auto = _doc_cap_dg(pidx)          # CHI DOC - dieu phoi chot (L1)
            if _dg_auto:
                c.di_gioi_level = _dg_auto
        if not is_reconnect:    # RECONNECT nhe: bo qua exp/qua/gacha/mail/vantieu (da lam phien truoc)
            if pcfg.get("claim_offline_exp", True):
                c.request_offline_exp() # NHAN EXP OFFLINE (treo may) - tu nhan neu co
            c.claim_mail()          # nhan qua mail + xoa mail da doc (qua bao tri,...)
            # NHAN QUA THANH TUU (yeu cau user: ngay SAU check mail, LUON chay - khong can tick).
            # Chi doc 2 bit trong forever-flags (0x51) nen re; tui day thi tu hoan.
            try:
                c.claim_achievements()
            except Exception as e:
                log.warning("[%s] loi nhan qua thanh tuu (bo qua): %s", label, e)
            c.claim_checkin()       # diem danh hang ngay (tu dem so lan)
            c.claim_14day_gift()    # qua 14 ngay user moi (0x57)
            c.claim_event_14day()   # event tang qua 14 ngay (0x7c) - khac cai tren
            c.claim_legion_gift()   # nhan qua quan doan hang ngay
            c.claim_friend_gifts()  # tang qua tat ca ban + nhan qua ban tang (hang ngay)
            c.decompose_junk_scrolls()  # phan giai cuon goi pet RAC (junk_scrolls.json) -> Vo Tuong Phien
            # TU MO RONG TUI DO: mua slot toi khi gia lan KE TIEP vuot nguong user dien. Dat TRUOC
            # cac viec don tui (ban Noi Dat / vut rac) de tui rong san, va truoc `use_items` de co
            # cho nhan do. Mac dinh TAT.
            if pcfg.get("auto_bag_expand") and int(pcfg.get("bag_expand_gold", 0) or 0) > 0:
                try:
                    c.tu_mo_rong_tui(int(pcfg.get("bag_expand_gold", 0) or 0))
                except Exception as e:
                    log.warning("[%s] loi tu mo rong tui do (bo qua): %s", label, e)
            # TU CONG DIEM TIEM NANG cua NHAN VAT (bang rule rieng tung acc). Chi chay MOT LAN
            # luc login, cung cho voi cac viec vat khac.
            _tu_cong_diem(c, username, label)
            # TU NANG SKILL NHAN VAT (bang rule rieng tung acc, giong Point). Dat NGAY SAU tu cong
            # diem: cung la "tieu diem theo bang rule", va cung chi chay MOT LAN luc login.
            _tu_nang_skill(c, username, label)
            _kiem_han_ba_dau(c, username, label)   # Ba Dau sap het han -> bao o man Chu y
            if pcfg.get("auto_pet_skill", True):   # AUTO NANG SKILL PET: pet co diem skill -> nang (index 0->1->2 toi max)
                try:
                    _n = c.auto_upgrade_pet_skills()
                    if _n:
                        log.info("[%s] auto nang skill pet: da gui nang cho %d pet", label, _n)
                except Exception as e:
                    log.warning("[%s] loi auto nang skill pet (bo qua): %s", label, e)
            # SOI LO + xu ly theo config per-acc (ACCOUNT_FURNACE): tab bat + item auto mua / notify.
            # Config trong -> chi soi + log 3 tab (nhu Pha 1). Notify list -> log (Pha 2 GUI popup).
            try:
                _fcfg = getattr(config, "ACCOUNT_FURNACE", {}).get(username, {})
                _notify = c.process_furnace(_fcfg)
                if _notify:
                    account_furnace_notify[username] = _notify   # GUI doc de popup hoi mua
            except Exception as e:
                log.warning("[%s] loi soi/mua lo: %s", label, e)
            if pcfg.get("auto_donate_materials", True):
                c.donate_legion()       # donate nguyen lieu cho quan doan (list edit duoc, mac dinh het) -> don tui
            # RUONG TRANG BI: mo -> phan giai duoc thi phan giai, khong thi donate quan doan.
            # DAT NGAY SAU donate nguyen lieu (user chot 03/09) de tan dung ket qua "co donate
            # duoc hay khong": do khong phan giai duoc chi con duong donate, ma donate can da vao
            # quan doan >24h (server tra S:039-015 ma 32 neu chua du) -> chua donate duoc thi mo
            # ruong ra cung vo ich, con lam day tui.
            if pcfg.get("auto_open_boxes", False):
                try:
                    _kq = c.tu_mo_hop_trang_bi(chon=pcfg.get("box_modes") or {})
                    if _kq.get("bo_qua"):
                        log.info("[%s] Ruong trang bi: bo qua (%s)", label, _kq["bo_qua"])
                    elif _kq.get("mo"):
                        log.info("[%s] Ruong trang bi: mo %d, phan giai %d, donate %d, vut %d",
                                 label, _kq["mo"], _kq["phan_giai"], _kq["donate"],
                                 _kq.get("vut", 0))
                except Exception as e:
                    log.warning("[%s] loi tu mo ruong trang bi: %s", label, e)
            # THA DO THOI TRANG vao Bo Suu Tam (gon tui + diem). DAT TRUOC use_login_items vi tha CHAC
            # CHAN free slot, con use_item doi khi de item MOI ra lam day tui (theo yeu cau user).
            try: c.deposit_fashion_to_collection()
            except Exception as e: log.warning("[%s] loi tha do thoi trang: %s", label, e)
            c.use_login_items()         # tu dung item trong list (use_items.json) -> vd tui vat lieu su kien
            # THU CUOI: nang cap + boi duong bang 5 vien ky don. CHAY NGAY SAU use_login_items()
            # (yeu cau user) vi tui vat pham vua duoc mo ra o buoc tren co the CHINH LA nguon vien
            # ky don (vd "Tui Toa Ky Dan" 0xb22c). LUON BAT, khong co o tick trong setting.
            try:
                c.do_mount_upgrade()
            except Exception as e:
                log.warning("[%s] loi nang cap thu cuoi: %s", label, e)
            # Phuc Than NGAY SAU use_login_items() - luc nay con dung an toan o thanh/diem login
            # (chua di ra bai quai) -> tranh bug dung/deo Phuc Than GIUA luc dang combat ngoai bai
            # (da tung xay ra vi next_phuc_than=0.0 chi trigger o tick dau cua vong lap chinh, co
            # the roi vao luc dang di ra spot/dang danh).
            # Mode EVENT (40NPC/2K): KHONG dung Phuc Than (yeu cau user - vao event khong an he so
            # EXP nay, dung la phi item).
            # DUNG _early_mode: bien `mode` mai ~1300 moi gan (SAU cho nay) -> dung `mode` o day
            # la UnboundLocalError, thread run_account CHET va CA PARTY thoat (bug that 00:48).
            _use_normal = bool(pcfg.get("use_phuc_than") or getattr(config, "ACCOUNT_PHUC_THAN", {}).get(username, False))
            _use_dai = bool(pcfg.get("use_dai_phuc_than") or getattr(config, "ACCOUNT_DAI_PHUC_THAN", {}).get(username, False))
            if (_use_normal or _use_dai) and _early_mode != "event":
                try: c.use_phuc_than_items(_use_normal, _use_dai)
                except Exception as e: log.warning("[%s] loi dung phuc than luc login: %s", label, e)
                next_phuc_than = time.time() + PHUC_THAN_CHECK_SEC
            # Van tieu: nhan qua xong + gui pet; tra ve gio check tiep. Cong tac "Van tieu" trong
            # Cai dat nang cao (mac dinh CO tick - giu hanh vi cu); tat -> khong lam + khong hen gio.
            # Cong tac van tieu nay o TUNG ACC (bang setting Hoi HP/SP), khong con o cap party ->
            # goi thang, chinh do_van_tieu() doc c.vantieu_enable roi tu quyet dinh.
            next_vantieu = c.do_van_tieu()
            # MUA SHOP (Cai dat nang cao, mac dinh TAT): master auto_buy_shop + list shop.
            # Dua theo RoleCount server 0x55 neu biet counter; item chua ro counter thi server tu reject.
            if pcfg.get("auto_buy_shop"):
                _shop_items = pcfg.get("shop_items") or {}
                if pcfg.get("buy_ho_phu") or _shop_items.get("ho_phu"):
                    try:
                        log.info("[%s] Mua shop: setting Ho Phu bat -> check mua 3 cai/ngay", label)
                        c.buy_di_gioi_ho_phu()
                    except Exception as e: log.warning("[%s] loi mua Ho Phu: %s", label, e)
                if pcfg.get("buy_thien_chau") or _shop_items.get("thien_chau"):
                    try:
                        c.buy_hop_thien_chau()
                    except Exception as e: log.warning("[%s] loi mua Hop Thien Chau: %s", label, e)
                if pcfg.get("buy_bao_hop") or _shop_items.get("bao_hop"):
                    try: c.buy_trieu_goi_bao_hop(int(pcfg.get("bao_hop_xu_threshold", 10000000)))
                    except Exception as e: log.warning("[%s] loi mua Bao Hop: %s", label, e)
            # MUA HP/SP (Cai dat nang cao, mac dinh TAT): neu du tru HP/SP < nguong -> di Trac Quan
            # mua Vien Hanh Khi (+62HP) / Thien Kim Du (+62SP). Gop HP+SP 1 chuyen. 1 lan/ngay/acc.
            if pcfg.get("buy_hp") or pcfg.get("buy_sp"):
                try:
                    c.buy_hp_sp(
                        pcfg.get("buy_hp", False), int(pcfg.get("hp_qty", 9999)),
                        int(pcfg.get("hp_thresh", 500000)),
                        pcfg.get("buy_sp", False), int(pcfg.get("sp_qty", 9999)),
                        int(pcfg.get("sp_thresh", 500000)),
                    )
                except Exception as e: log.warning("[%s] loi mua HP/SP: %s", label, e)
                # Chuyen mua HP/SP DOI MAP (Trac Quan -> map NPC -> ve lai Trac Quan). `login_map`
                # doc luc login van la map cu -> cac nhanh duoi bam theo no se tuong acc dang o bai
                # train va KHONG keo ve (user bao 28/08: "no dung ket o Loi dai Huong dung").
                if c.current_map is not None and c.current_map != login_map:
                    log.info("[%s] sau mua HP/SP: map %s -> %s, cap nhat lai login_map",
                             label, login_map, c.current_map)
                    login_map = c.current_map
            # BOSS QUAN DOAN ngay sau van tieu: danh solo neu con luot (server count 0x55/0x2a) + het
            # cooldown. KHONG lien quan daily quest (tick hay ko van danh). Luc login char SOLO (chua
            # lap party) -> danh duoc. Trong phien: keepalive trigger REFORM khi con luot (xem duoi).
            # Mode EVENT (40NPC): mac dinh KHONG danh boss quan doan (acc event chuyen tam cho event,
            # khong di lang thang danh boss lam tre vao event).
            if pcfg.get("mode") == "event":
                log.info("[%s] (%s) mode event -> bo qua boss quan doan (mac dinh)", label, role)
            else:
                try: c.do_legion_boss()
                except Exception as e: log.warning("[%s] loi do_legion_boss: %s", label, e)

        # MODE theo CONFIG RIENG cua party (PARTY_CONFIG[pidx]). Fallback: suy tu START_CITY_ID.
        # (pcfg da doc o tren, giu nguyen bien - khong doc lai)
        sc = pcfg.get("start_city_id", getattr(config, "START_CITY_ID", 0))
        mob_index = pcfg.get("mob_index", 0)
        city_flag = pcfg.get("city_flag", 0)
        # checkbox "Lam nhiem vu hang ngay" (bingo 9 o + dungeon). Fallback key cu "do_dungeon".
        do_daily = pcfg.get("do_daily", pcfg.get("do_dungeon", True))
        auto_world_boss = pcfg.get("auto_world_boss", True)
        auto_team_dungeon = pcfg.get("auto_team_dungeon", True)
        # Mode EVENT (40NPC): mac dinh KHONG lam daily quest (acc event chuyen tam vao event ngay,
        # khong di lang thang lam bingo/pho ban -> vao event nhanh, khong bi dump khoi map event).
        if pcfg.get("mode") == "event":
            do_daily = False
            auto_world_boss = False
            auto_team_dungeon = False
        # mode: digioi | train | city (tap trung ve thanh) | stand (dung yen) | cleanbag
        #       | digioi_train (DG TRUOC, ca party xong DG -> TRAIN map)
        raw_mode = pcfg.get("mode")
        dt_mode = (raw_mode == "digioi_train")
        if dt_mode:
            # Pha DG: chay y het mode "digioi" tren map DG. Pha train: mode "train" tren map da
            # chon (start_city_id). Pha luu o party state -> ca party cung pha.
            if _pstate(pidx).get("dt_phase", "digioi") == "digioi":
                mode, sc = "digioi", config.DIGIOI_MAP_ID
            else:
                mode = "train"
        # TU CHON MAP TRAIN (train_pick): sc/mob_index trong config = 0/-1, map+diem do bot tu tim
        # theo level party. Quyet 1 LAN cho CA PARTY (xem _auto_train_target) - moi acc tu boc thi
        # ca party toe ra cac map khac nhau.
        if (raw_mode in ("train", "digioi_train")) and pcfg.get("train_pick"):
            # CHI DOC BAI TRAIN DIEU PHOI DA CHOT - acc khong tu chot (L1). `_dieu_phoi_chot_map`
            # chay moi 2 giay va ghi `st["auto_train"]`; acc chi viec doi no co.
            #
            # Truoc day acc TU goi `_auto_train_target(..., username, _stopped)`: no tu di lay
            # level ca party, tu chot map, va neu chua duoc thi tra None -> `sc` giu nguyen 0 ->
            # `tm=None` -> `train_on_map=False` -> acc roi vao nhanh cuoi "DUNG YEN tai cho login".
            # Nhanh setup chi chay MOT LAN, nen du 20 giay sau dieu phoi chot xong bai, acc van
            # nam nguyen day.
            #
            # Ca that party 15, 13/09 (user: "p15 lai deo them gom pt" -> "lai acc doc a, acc quyet
            # dinh cai lon gi o day the"):
            #   14:23:43 >>> PARTY 15: TU CHON MAP -> Dam Lay Tang Khau1 (map 21841)
            #   17:14:15 [truchin] (member) DUNG YEN tai cho login (map=12003)
            #   ... MOT TIENG chi con `pos=(530,530) map=12003` moi 5 giay ...
            _t_bai = time.time()
            _auto = None
            while not _stopped() and c.running:
                _auto = _doc_bai_train(pidx)
                if _auto:
                    break
                if time.time() - _t_bai > CHO_DIEU_PHOI_CHOT_BAI_SEC:
                    log.warning("[%s] (%s) dieu phoi chua chot duoc bai train sau %.0fs -> vao vong "
                                "chinh cho lenh (xem log 'PARTY %d: TU CHON MAP')",
                                label, role, CHO_DIEU_PHOI_CHOT_BAI_SEC, pidx + 1)
                    break
                time.sleep(2)
            if _auto:
                sc, mob_index = _auto
        # Account reconnect vao session Android phai phuc hoi bai do AUTO BATTLE da chon,
        # khong khoi tao lai bang mode=stand/map 0 cua nut Login.
        _ui_target_setup = st.get("ui_train_target")
        if _ui_target_setup:
            try:
                sc = int(_ui_target_setup[0])
                mode = "train"
                raw_mode = "train"
            except (TypeError, ValueError, IndexError):
                pass
        tm = config.TRAIN_MAPS.get(sc)          # dict {safe, mobs} neu la map train
        if not dt_mode:
            mode = raw_mode
            if not mode:
                mode = ("train" if tm else ("digioi" if sc == config.DIGIOI_MAP_ID
                        else ("stand" if sc == 0 else "city")))
        train_on_map = (mode == "train") and (tm is not None)
        is_digioi = (mode == "digioi")
        dt_dg_finished = False   # mode digioi_train: vua HET GIO DG -> cho party roi sang train
        c.auto_sell_noi_dat = bool(pcfg.get("auto_sell_noi_dat", True) and mode in ("train", "city"))
        # TU CAT DO tien trang: chay cung cho voi ban Noi Dat (pre_route_town_hop boc 50-50) nen
        # rang buoc mode y het - khong thi mode khac cung lo di cat giua chung.
        c.auto_cat_do = bool(pcfg.get("auto_cat_do", False) and mode in ("train", "city"))
        # TU MO RONG TIEN TRANG: nguong chi co nghia khi tick bat; tat -> 0 = khong lam gi.
        c.bank_expand_gold = (int(pcfg.get("bank_expand_gold", 0) or 0)
                              if pcfg.get("auto_bank_expand", False) else 0)
        # "Tu don tui do" (Cai dat nang cao): cong tong + 2 muc con moi. Phan giai cuon MAC DINH
        # TAT vi phan giai la mat han - user phai tu tick sau khi soat list.
        # Mode event dung chung pet voi quest/PB -> vai "mac dinh" cua no la quest.
        # Mode EVENT (40NPC / 2K) LUON danh o quest_mode - khong phu thuoc co leader hay khong
        # (truoc day chi ep trong nhanh is_leader -> khong leader thi bot chay TRAIN mode).
        c.state.force_quest_mode = (mode == "event")
        if c.state.force_quest_mode:
            c.state.quest_mode = True
        c.default_pet_role = "quest" if mode == "event" else "train"
        # (nhom auto_bag_clean/discard_junk/decompose_scrolls/scroll_modes da gan SOM o tren -
        #  ngay sau khi co pcfg - vi cac ham do duoc goi trong khoi viec-hang-ngay o TREN cho nay.)
        ev = None
        train_safes = []
        if tm is not None:
            resolved_safe = _resolve_train_safe(c, sc, tm.get("safe", []))
            if resolved_safe is not None:
                train_safes.append(resolved_safe)
        if train_on_map and is_leader and _needs_train_mob_probe(c, sc, tm):
            try:
                c.arm_mob_packet_capture(
                    sc,
                    max_packets=getattr(config, "MOB_PACKET_CAPTURE_MAX_PACKETS", 50000),
                )
            except Exception as exc:
                log.warning("[%s] khong arm duoc packet capture map %s: %s", label, sc, exc)
        log.info("[%s] (%s) MODE=%s start_city=%s", label, role, mode, sc)

        def _dg_remain_minutes():
            return max(0, int(DIGIOI_LIMIT - c.digioi_minutes_live()))

        def _maybe_use_di_gioi_ho_phu(reason: str) -> bool:
            if not (is_digioi and pcfg.get("use_digioi_ho_phu")):
                return False
            remain = _dg_remain_minutes()
            if remain >= 15:
                return False
            if c.in_combat():
                return False
            before = getattr(c, "digioi_minutes", 0)
            ok = c.use_di_gioi_ho_phu()
            if not ok:
                return False
            log.info("[%s] Di Gioi Ho Phu (%s): con %d phut (<15), da gui lenh dung; "
                     "doi server cap nhat timer", label, reason, remain)
            deadline = time.time() + 8.0
            while time.time() < deadline and getattr(c, "digioi_minutes", 0) == before:
                time.sleep(0.5)
            after = getattr(c, "digioi_minutes", 0)
            after_remain = _dg_remain_minutes()
            if after != before:
                log.info("[%s] Timer Di Gioi sau Ho Phu: used %d -> %d, con %d phut",
                         label, before, after, after_remain)
            else:
                log.info("[%s] Da dung Ho Phu, chua thay 0x55/0x1b doi (used=%d, con %d phut)",
                         label, before, after_remain)
            return True

        def _dieu_phoi_dang_ra_lenh():
            """Dieu phoi dang co lenh cho party nay khong -> ly do (str) hoac None.

            LENH CUA DIEU PHOI PHAI DUOC THI HANH TUYET DOI (user 09/09: "lenh cua dieu phoi phai
            thi hanh tuyet doi"). Viec rieng (boss the gioi, pho ban) dung sau - con luot thi lan
            sau vao lai, con party lac nhau thi ca tieng khong gom lai duoc.
            """
            try:
                # KE HOACH DANG HIEU LUC noi truoc ca `kenh_dich`/`gom_dich`: dieu phoi vua chot
                # "gom" hay "dong bo" thi viec rieng dung ngay, khong doi den luc co dich cu the.
                _kh = _ke_hoach(st) or {}
                # CUNG MOT DINH NGHIA "dang co lenh" voi `dat_party_dang_gom` o vong dieu phoi
                # (VIEC_GOM, VIEC_MOI, VIEC_DONG_BO). Hai cua lech nhau la co viec vat lot qua.
                #
                # Ca that party 1, 13/09 (user: "p1, co 2 dua ko chiu tap trung ve tuong duong"):
                #   12:58:06 [party 1] gen 25: viec=moi - DOI chua du
                #   12:58:11 [thbon]   Mua ve dungeon -> OK        <- VAO LUOT MOI
                #   12:58:15 [thbon]   Da vao dungeon -> danh boss
                #   12:58:20 [thbay]   Dungeon: khong vao luot moi - dieu phoi dang ra lenh gom
                # thbay bi chan (luc do da sang 'gom'), thbon thi khong vi luc no hoi, lenh moi la
                # 'moi'. Ca party phai cho no danh not mot man boss nua moi ve duoc diem hen.
                # PB TO DOI thi hoan dung tu dau - vi no doc `party_dang_gom`, ben do CO VIEC_MOI.
                #
                # Vao pho ban don = `leave_party()` + sang map rieng, tuc VUA pha doi VUA lam lech
                # map - dung hai thu lenh 'moi' dang co gang sua.
                if _kh.get("viec") in (VIEC_GOM, VIEC_MOI, VIEC_DONG_BO):
                    return "dieu phoi dang ra lenh %s (%s)" % (_kh.get("viec"),
                                                               _kh.get("ly_do") or "")
                _kd = st.get("kenh_dich")
                if _kd and int(getattr(c, "current_channel", 0) or 0) != int(_kd):
                    return "dieu phoi dang gom ve kenh %s" % (_kd,)
                # `gom_dich[gen] = (thanh, luc)` - chi xet vong gom DANG chay (gen hien tai), va
                # co han dung GOM_DICH_HAN_SEC vi vong gom co nhieu duong thoat khong xoa duoc dich.
                _rec = (st.get("gom_dich") or {}).get(st.get("reform_gen"))
                if _rec and time.time() - float(_rec[1]) < GOM_DICH_HAN_SEC:
                    if int(getattr(c, "current_map", 0) or 0) != int(_rec[0]):
                        return "dieu phoi dang gom ve thanh %s" % (_rec[0],)
            except Exception:
                pass
            return None

        def _maybe_auto_world_boss(reason: str):
            try:
                if auto_world_boss:
                    # KHONG HOAN THEO LENH DIEU PHOI NUA (user chot 14/09).
                    #
                    # Cua hoan cu (`_dieu_phoi_dang_ra_lenh`) dat dung vao THOI DIEM LUON DANG GOM:
                    # viec nay chi chay o login chores, ma luc moi login thi 250 acc dung o 250 cho
                    # khac nhau -> dieu phoi ra lenh gom -> HOAN -> va vi chi chay MOT LAN, mat luot
                    # CA NGAY. Restart bao nhieu lan cung the.
                    #
                    # Do tren log 14/09, sau khi restart 251 acc luc 22:31:46:
                    #   PB don : 109 acc bi HOAN |  2 acc danh duoc luot
                    #   WB     : 162 acc bi HOAN | 51 acc vao danh
                    # Ca ngay: 2739 lan `Boss the gioi: HOAN`, 3224 lan `Dungeon: HOAN`;
                    # o 1 (PB don) chi 14/152 acc sang duoc -> 96% acc khong xong nhiem vu ngay.
                    #
                    # GIO AN TOAN HON TRUOC: hai viec nay bao pha `PHASE_LOGIN_CHORE`, ma dieu phoi
                    # da KHONG con tinh acc dang viec vat vao phep do lech map/kenh, con loi moi
                    # party thi duoc GIU lai den khi xong viec. Nen acc di danh khong lam party
                    # "hong" nhu truoc.
                    log.info("[%s] Boss the gioi: auto danh het luot (%s)", label, reason)
                    c.do_world_boss_all()
            except Exception as e:
                log.warning("[%s] loi auto world boss (%s): %s", label, reason, e)
            finally:
                # DANH DAU DA XONG - leader cho co nay truoc khi lap pho ban (xem
                # _wait_party_world_boss). Dat trong `finally` de loi/tat boss van danh dau,
                # khong thi ca party treo cho mot acc khong bao gio bao xong.
                with st["lock"]:
                    st.setdefault("wb_done", set()).add(username)

        def _ket_thuc_pha_dg():
            """Doi pha DG -> train: GIU ket noi neu duoc, khong thi dong nhu cu.

            CO 5 DUONG THOAT khac nhau sau _finish_digioi_train_after_dg() (het gio luc login,
            het gio giua chung, server khong cho vao lai, ...), moi duong tu dong ket noi + return.
            Lan truoc toi chi va MOT duong (4803) nen thuc te van relogin - log 16:39 di duong
            "DG da HET GIO hom nay -> khong vao" (3152) va van thay "Da gui auth".
            Gio moi duong deu goi ham nay.
            """
            if (_dt.get("relogin_train") and c is not None and getattr(c, "running", False)
                    and not getattr(c, "server_closed", False) and not _stopped()):
                account_continue[username] = c    # supervisor chay lai NGAY tren ket noi nay
                return
            try:
                c.close()
            except Exception:
                pass
            if c in _clients:
                _clients.remove(c)

        def _ve_cho_cho_pha_train(ly_do=""):
            """Cho DUNG doi cac acc khac xong DG. DANG O BAI TRAIN thi DUNG NGUYEN, chi ra safe.

            User 27/08: "login vao va da dung o map roi ma no van ve thanh". Truoc day luon
            _go_town_safe() (tele ve Trac Quan 12001) roi pha train ngay sau do lai phai reform:
            12001 -> 12061/12011 -> di route qua cong -> ve dung cho vua dung. Log 13:49:54:
            ca 5 acc login san o (1170,470) map 12831 = safe cua chinh bai train, DG het gio ->
            van bay ve thanh roi bo cong len lai.
            """
            # DANH SACH SAFE DAY DU cua map (config), KHONG dung `train_safes`: luc nay train_safes
            # moi co DUNG MOT diem (cache mob_spots hoac diem gan pos luc login) - phai sang pha
            # train moi co dong `train_safes[:] = learned_safes`. Dung mot diem do thi acc dang
            # dung SAN tren mot safe khac van bi bat chay ca chuc buoc qua vung quai, roi pha train
            # lai keo nguoc ve gan cho cu. Log 27/08 15:11: dung o safe (310,2090), chay 10 buoc
            # toi (450,1210), 22s sau chay 11 buoc nguoc lai (310,2090) - dinh tran ca luot di lan ve.
            # CON ACC DANG TRONG DI GIOI -> ve THANH dung cho, KHONG dung o bai train.
            # Safe cua bai train KHONG phai cho dung lau: quai van lang toi (map 23821 safe
            # (990,490) cach diem quai gan nhat 398). Cho DG co the ca chuc phut -> may acc xong
            # truoc dung do an dan ca buoi (user chot 31/08: "1 dua dang trong di gioi, 4 dua con
            # lai dung cho quai danh la qua ngu ngoc ... xong di gioi roi thi ve dung o thanh").
            _con_trong_dg = []
            try:
                for _u, _p, _il, _ip in party_accounts(pidx):
                    _uc = account_clients.get(_u)
                    if _uc is None or not getattr(_uc, "running", False):
                        continue
                    if getattr(_uc, "current_map", None) == config.DIGIOI_MAP_ID:
                        _con_trong_dg.append(_u)
            except Exception as e:
                log.debug("[%s] doc acc con trong DG loi (bo qua): %s", label, e)
            if _con_trong_dg:
                log.info("[%s] (%s) %s: con %s DANG TRONG DI GIOI -> ve THANH dung cho "
                         "(khong dung o bai train cho quai danh)",
                         label, role, ly_do or "xong DG", _con_trong_dg)
                _go_town_safe(c, label)
                return
            _ds = [tuple(map(int, p)) for p in ((tm or {}).get("safe") or []) if len(p) == 2]
            _ds = _ds or list(train_safes or ())
            if c.current_map == sc and _ds:
                try:
                    c.flee_mode = True
                    c._wait_combat_clear(idle=2.0, cap=15.0)
                    _s0 = _nearest_safe(c.pos, _ds)
                    # DA DUNG NGAY TREN/SAT mot safe -> dung yen, khong di dau ca.
                    if c.pos and _s0 and ((c.pos[0] - _s0[0]) ** 2
                                          + (c.pos[1] - _s0[1]) ** 2) <= 60 ** 2:
                        log.info("[%s] (%s) %s: dang dung san o safe %s cua bai train %s -> DUNG YEN",
                                 label, role, ly_do or "xong DG", _s0, sc)
                        return
                    log.info("[%s] (%s) %s: dang o bai train %s -> ra safe %s dung cho, "
                             "KHONG ve thanh", label, role, ly_do or "xong DG", sc, _s0)
                    c.navigate_to(*_s0, flee=True)
                    return
                except Exception as e:
                    log.warning("[%s] (%s) loi ra safe cho pha train (ve thanh thay the): %s",
                                label, role, e)
            _go_town_safe(c, label)

        def _finish_digioi_train_after_dg():
            _ve_cho_cho_pha_train("xong DG -> cho ca party")
            _wait_res = _dt_wait_all_digioi_done(pidx, username, label, _stopped)
            if _wait_res in ("back_to_dg", "lenh_moi"):
                # `back_to_dg`: soat lai trong luc cho thay CON GIO DG (hoac vua dung Ho Phu) ->
                #   quay lai DG danh tiep, KHONG di train.
                # `lenh_moi` : dieu phoi bump `reform_gen` trong luc cho -> thoi cho de di NGHE
                #   LENH. Truoc day duong nay tra `False` va bi hieu la BI STOP -> khong set
                #   `relogin_train` -> `_ket_thuc_pha_dg()` dong ket noi -> ACC CHET HAN (6 party
                #   09/09, xem ghi chu trong `_dt_wait_all_digioi_done`).
                #
                # Ca hai deu GIU KET NOI: supervisor chay lai run_account -> vao lai DG theo dung
                # luong dau vao (mode van la digioi_train vi dt_phase CHUA doi sang train).
                _dt["relogin_train"] = True
                return False
            if not _wait_res:
                return False
            # Daily/team dungeon can require the whole party. Run it only after every account
            # has finished DG, otherwise an early member can wait for leader while leader is
            # still trying to form the DG party.
            _maybe_auto_world_boss("sau DG, truoc pho ban doi")
            if auto_team_dungeon:
                if not _run_auto_team_dungeons_if_needed(c, st, username, label, pidx,
                                                         is_leader, _stopped, pcfg):
                    # PB HONG KHONG PHAI LY DO DE GIET PARTY. DG da het gio -> viec tiep theo LUON
                    # la TRAIN, du viec vat co xong hay khong.
                    # BUG THAT (party 19, 13:46-13:57): rule "phai du pt moi danh PB" huy tran vi
                    # roster chi 1/4 member -> ham nay `return False` -> _dt["relogin_train"] khong
                    # duoc set -> reconnectable=False -> st["leader_gone"].set() -> member thay
                    # leader chet that -> THOAT THEO -> CA PARTY CHET, phai bat tay lai.
                    # ... va KHONG duoc `return` o day: `do_daily_dungeon()` (o 1) nam ngay duoi.
                    # BUG THAT (party 19 quan_vu, 02/09): PB lv80 hong vi ca party moi lv68 ->
                    # `return` -> o 1 KHONG BAO GIO duoc lam. Pha train cung khong va lai duoc
                    # (`_do_startup_daily` chi goi `claim_daily_quests`, khong goi
                    # `do_daily_dungeon`) -> cuoi ngay 6 acc dung o `o xong=[2..9]`, thieu dung o 1.
                    log.warning("[%s] (%s) pho ban to doi khong xong -> VAN chuyen sang pha TRAIN "
                                "(khong bo party), van lam not nhiem vu ngay", label, role)
            if do_daily:
                try:
                    c.do_daily_dungeon()
                except Exception as e:
                    log.warning("[%s] loi daily dungeon (bo qua): %s", label, e)
                try:
                    c.claim_daily_quests(heavy=True)
                except Exception as e:
                    log.warning("[%s] loi claim daily quest (bo qua): %s", label, e)
            _dt["relogin_train"] = True   # supervisor chay lai -> pha TRAIN
            return True

        def _finish_digioi_train_if_time_over(reason: str) -> bool:
            if not (dt_mode and is_digioi):
                return False
            remain = max(0, int(DIGIOI_LIMIT - c.digioi_minutes_live()))
            out_of_dg = (c.current_map is not None and c.current_map != config.DIGIOI_MAP_ID
                         and not c.in_combat())
            # Theo doi thoi gian KET NGOAI DG lien tuc (reset khi dang trong DG/dang danh).
            if not out_of_dg:
                c._dg_out_since = None
            elif getattr(c, "_dg_out_since", None) is None:
                c._dg_out_since = time.time()
            # KET NGOAI DG >90s = HET GIO THAT du dong ho noi bo (digioi_minutes_live dung yen khi ra
            # ngoai) van bao con gio: server da ra 12003 va enter_di_gioi_safe() "THANH CONG GIA" (vao
            # 1 nhip roi bi da ra ngay) -> nhanh "remain>=2 -> vao lai DG" lap vo han, khong bao xong
            # (bug that: dv607@12003 treo >7 phut, ca party cho mai). -> ep coi la het gio.
            _stuck_secs = time.time() - (getattr(c, "_dg_out_since", None) or time.time())
            # SERVER moi la su that: RoleCount 0x1b = so phut DA DUNG (client Logic_Dungeon:
            # time = (limitTime - RoleCount)*60; LimitTimeDungeon_C.dat: scene 49942, limitIndex
            # 0x1b, limitTime 120). Chi coi "ket ngoai DG" la HET GIO khi SERVER cung bao gan het.
            _srv_left = max(0, DIGIOI_LIMIT - int(getattr(c, "digioi_minutes", 0) or 0))
            _stuck_out = out_of_dg and _stuck_secs > 90 and _srv_left <= 5
            # KET NGOAI DG lau MA SERVER VAN CON NHIEU GIO -> KHONG phai het gio: truoc day ep coi
            # la het gio nen acc bi khai tu oan giua luc SYNC KENH DG (bug that user bao: dung o
            # Trac Quan, bang bao con 58-59 phut; Stop/Start lai la vao DG binh thuong).
            # -> RELOGIN (dung viec Stop/Start tay ma user lam) thay vi bo pha DG.
            if not out_of_dg:
                _dg_stuck_relogin.pop(username, None)   # da vao lai duoc DG -> quen so lan cu
            if out_of_dg and _stuck_secs > 90 and _srv_left > 5:
                _n = int(_dg_stuck_relogin.get(username, 0)) + 1
                _dg_stuck_relogin[username] = _n
                if _n <= 2:
                    log.warning("[%s] (%s) KET NGOAI DG %.0fs trong luc %s nhung SERVER con %d phut "
                                "-> RELOGIN vao lai DG (lan %d/2), KHONG coi la het gio",
                                label, role, _stuck_secs, reason, _srv_left, _n)
                    c._dg_out_since = None
                    _force_supervisor_reconnect(username, c, "ket ngoai DG nhung con gio -> vao lai")
                    return True
                log.warning("[%s] (%s) KET NGOAI DG %.0fs, da relogin 2 lan van khong vao lai duoc "
                            "(server con %d phut) -> danh chiu, coi nhu xong DG",
                            label, role, _stuck_secs, _srv_left)
                _stuck_out = True
            if remain <= 0 or (out_of_dg and remain < 2) or _stuck_out:
                log.warning("[%s] (%s) DG da het trong luc %s (map=%s, con %d phut%s) -> "
                            "chuyen sang cho ca party xong DG",
                            label, role, reason, c.current_map, remain,
                            ", KET NGOAI DG >90s (server da ra lien tuc)" if _stuck_out else "")
                _reason("het gio Di Gioi trong luc %s" % reason)
                _finish_digioi_train_after_dg()
                _ket_thuc_pha_dg()
                return True
            if out_of_dg:
                log.warning("[%s] (%s) dang cho party DG nhung bi ra ngoai DG (map=%s, con %d phut) "
                            "-> vao lai DG", label, role, c.current_map, remain)
                try:
                    back_in = c.enter_di_gioi_safe()
                except Exception as e:
                    log.warning("[%s] loi vao lai DG luc cho party: %s", label, e)
                    back_in = False
                if not back_in:
                    # SERVER KHONG CHO VAO LAI = HET GIO THAT. Day moi la bang chung chac chan,
                    # khong phai dong ho noi bo. Phan biet duoc CHET (bi day ve 12003 nhung VAO LAI
                    # DUOC) voi HET GIO (khong vao lai duoc). Truoc day vut gia tri tra ve di ->
                    # lap "vao lai DG" mai mai, khong bao gio bao xong -> hang rao "2/5 acc xong"
                    # treo ca dem (bug that 22:59-23:07).
                    #
                    # NHUNG PHAI DOI CHIEU DONG HO SERVER TRUOC. "Vao khong duoc" con co nhung ly
                    # do KHAC het gio: vua relogin chua san sang, dang trong tran, server tre. Mot
                    # lan that bai ma ket luan het gio la vut ca luot DG cua acc.
                    #
                    # Ca that 09/09 party 2 (user: "p2 co dua deo them vao Di gioi kia" -> "xong cai
                    # dau lon, vua qua ngay moi, dua nao cung co 2h Di gioi") - `haabo`:
                    #   00:54:40 Boss QD: khong vao duoc tran -> RELOGIN ngay
                    #   00:54:57 (member) KHONG vao lai duoc DG (map=23822) -> coi la HET GIO DG
                    #   00:55:57 DG+Train: xong DG, DUNG YEN cho party (1/5 acc xong)
                    # Ca ngay no CHUA VAO DG lan nao (0 dong "da VAO DI GIOI"), va luc do vua sang
                    # ngay moi nen con nguyen 120 phut.
                    #
                    # PHAI LA SO SERVER THAT SU TRA VE: `digioi_minutes` mac dinh 0, nen acc chua
                    # nhan goi `0x55 id=0x1b` lan nao cung ra "con 120 phut" - tin no thi quay lai
                    # dung bug cu (server ngung day goi -> con so dong bang -> khong bao gio ket
                    # luan het gio -> ca party treo, 22:59-23:07). `_last_digioi_ts` chi duoc dat
                    # khi THUC SU parse duoc goi do.
                    try:
                        _co_timer = float(getattr(c, "_last_digioi_ts", 0.0) or 0.0) > 0
                        _dg_con = max(0, int(DIGIOI_LIMIT - c.digioi_minutes_live())) if _co_timer else None
                    except Exception:
                        _dg_con = None
                    _ma_vao = getattr(c, "_dg_enter_result", None)
                    # DANG DIS / CHUA VAO WORLD -> KHONG KET LUAN GI. Luc mat ket noi thi
                    # `enter_di_gioi_safe()` truot chac chan (`if not self.running: return False`),
                    # va do la ly do CUA DUONG TRUYEN chu khong phai cua gio DG.
                    #
                    # Ca that 09/09 `haabo` (user: "hinh nhu luc no muon vao DG thi bi dis, the la
                    # cung ko vao dc that, ko vao dc thi bot ket luan het time"):
                    #   00:54:40 Boss QD: khong vao duoc tran -> RELOGIN ngay
                    #   00:54:41 Server dong ket noi
                    #   00:54:57 (member) KHONG vao lai duoc DG (map=23822) -> coi la HET GIO DG
                    # Cach nhau 16 giay - chua login xong da ket luan mat ca luot DG, trong khi ca
                    # ngay no CHUA VAO DG lan nao va vua sang ngay moi (con nguyen 120 phut).
                    if (not getattr(c, "running", False)) or getattr(c, "current_map", None) is None:
                        log.warning("[%s] (%s) vao lai DG that bai NHUNG dang MAT KET NOI / chua "
                                    "vao world (running=%s, map=%s) -> KHONG ket luan het gio",
                                    label, role, getattr(c, "running", False),
                                    getattr(c, "current_map", None))
                        return False
                    # CHUA CO DONG HO DG CUA SERVER -> CHO, khong duoc ket luan gi.
                    #
                    # `S:085-001 <回傳玩家計數資料>` (id 0x1b = so phut DG) do SERVER TU DAY - crack
                    # client khong he co goi xin (`protocolTable[85]` chi co ba nhanh S:085-001/002/
                    # 003, khong co `C:085-*`), va chinh client ghi "跨日時會更新資料" = qua ngay thi
                    # server gui lai. Nen sau moi lan LOGIN LAI phai CHO goi do roi moi biet con bao
                    # nhieu phut (user 09/09: "luc login lai thi phai cap nhat lai time DG theo sv
                    # tra ve").
                    #
                    # Truoc do `_dg_con = None` (chua co timer) roi ROI THANG xuong nhanh ket luan
                    # HET GIO - chua biet gi da ket toi. Log `haabo` co dung dau vet: 00:53:31
                    # "Diem danh: chua co RoleCount 107 tu server" - tuc goi 0x55 CHUA VE ma bot da
                    # quyet xong so phan ca luot DG cua acc.
                    if not _co_timer and _ma_vao not in (1, 2):
                        log.warning("[%s] (%s) vao lai DG that bai NHUNG CHUA nhan dong ho DG cua "
                                    "server (0x55 id=0x1b) -> CHO goi do, khong ket luan het gio",
                                    label, role)
                        return False
                    if _dg_con and _dg_con > 2 and _ma_vao not in (1, 2):
                        log.warning("[%s] (%s) vao lai DG that bai (map=%s, S:097-001=%s) NHUNG "
                                    "server noi con %d phut -> KHONG coi la het gio, thu lai",
                                    label, role, c.current_map,
                                    _ma_vao if _ma_vao is not None else "khong tra loi", _dg_con)
                        return False
                    log.warning("[%s] (%s) KHONG vao lai duoc DG (map=%s) -> coi la HET GIO DG",
                                label, role, c.current_map)
                    _reason("het gio Di Gioi (server khong cho vao lai)")
                    _finish_digioi_train_after_dg()
                    _ket_thuc_pha_dg()
                    return True
            return False

        def _set_train_block_stats_spot(spot, enabled=False):
            if not train_on_map:
                c.set_train_block_stats_context(enabled=False)
                return
            if spot:
                c.set_train_block_stats_context(sc, spot, enabled=(enabled and is_leader))
            else:
                c.set_train_block_stats_context(enabled=False)

        # RA KHOI MAP EVENT truoc: neu login o map event (Nhi Kieu 12922 / 40 NPC 10991...) MA mode
        # KHAC event -> event map KHONG teleport thang duoc -> phai di bo ra cong ve map thuong roi moi
        # lam mode. Tim event co staging_map/dest_map == login_map -> dung 'exit' cua no.
        # TRU mode 'stand' (Login dau dung yen do) -> DUNG YEN TUYET DOI, KHONG tu chay ra.
        if mode not in ("event", "stand") and login_map is not None:
            _evx = next((_e for _e in (getattr(config, "EVENTS", {}) or {}).values()
                         if login_map in (_e.get("staging_map"), _e.get("dest_map")) and _e.get("exit")), None)
            if _evx is not None:
                try:
                    c.exit_event(_evx)
                    login_map = c.current_map   # cap nhat map sau khi ra -> teleport/route ben duoi dung
                    log.info("[%s] (%s) da ra khoi map event -> gio o map %s", label, role, login_map)
                except Exception as e:
                    log.warning("[%s] loi exit_event: %s", label, e)

        # NHIEM VU BINGO (mode KHAC digioi): VE CHO AN TOAN TRUOC roi moi lam dailies (tranh dung
        # giua o quai lam dailies; world boss tu teleport di roi ve Trac Quan; mode positioning ben
        # duoi se dua ve dung cho). Mode DIGIOI lam rieng (vao DG truoc - xem nhanh ben duoi).
        # RECONNECT thi thuong BO QUA daily (da lam phien truoc). NGOAI TRU: team dungeon VO giua chung
        # (o5_need_redo) -> ca party relogin cung nhau -> lam LAI daily (team dungeon can ca party, gio
        # deu dang reconnect nen barrier du nguoi). claim_daily_quests server-guarded -> chi lam phan
        # CHUA xong (team dungeon), cac o da xong tu skip.
        _o5_redo = bool(st.get("o5_need_redo"))
        if _o5_redo and is_reconnect:
            with st["lock"]:
                st["o5_need_redo"] = False   # lam lai 1 lan; neu vo tiep, _handle_o5_team se set lai
            log.info("[%s] (%s) reconnect do team dungeon VO -> lam LAI daily (team dungeon)", label, role)
        _td_redo = bool(st.get("team_dungeon_need_redo"))
        if _td_redo and is_reconnect:
            if not _prepare_team_dungeon_redo_after_reconnect(st, username, label, pidx, _stopped):
                try:
                    c.close()
                except Exception:
                    pass
                return
            log.info("[%s] (%s) reconnect do auto phó bản đội VỠ -> check/chạy lại auto phó bản",
                     label, role)
        # CHUYEN PHA (DG -> train) KHONG PHAI RECONNECT: `_run_account_supervised` goi lai
        # `run_account` voi `is_reconnect=not first` nen pha train luon bi coi la reconnect ->
        # bi chan het 3 viec duoi. Nhung day la LAN DAU chay pha train trong phien, chua he lam
        # boss the gioi / pho ban to doi / nhiem vu hang ngay lan nao.
        #
        # BUG THAT (log 01-02/09, mode digioi_train): o 1 (dungeon solo), o 2 (boss the gioi),
        # o 5 (pho ban to doi) KHONG BAO GIO duoc lam - bi chan ca BA cua:
        #   pha DG      -> `not is_digioi`
        #   het gio DG  -> khoi do nam sau `if not dt_mode:` (dt_mode chinh la DG+Train)
        #   pha train   -> `is_reconnect`
        # Do 700/700 luot "CHUYEN PHA train" trong log: khong mot lan nao co daily hay boss, va
        # 115/115 acc digioi_train deu dung o `o xong=[3,4,6,7,8]` (chi con cac o NHE tu keepalive).
        _chuyen_pha = reuse_client is not None
        _lan_dau = (not is_reconnect) or _chuyen_pha
        _do_startup_world_boss = bool(auto_world_boss and not is_digioi and _lan_dau)
        _do_startup_team = bool(auto_team_dungeon and not is_digioi and (_lan_dau or _td_redo))
        _do_startup_daily = bool(not is_digioi and do_daily and (_lan_dau or _o5_redo))
        if _do_startup_world_boss or _do_startup_team or _do_startup_daily:
            if mode == "city":
                try:
                    if (_ve_thanh_tap_trung(c, pidx, label, sc, city_flag)
                            and c.current_map == getattr(c, "NOI_DAT_SELL_CITY", 12061)):
                        c.sell_noi_dat()
                except Exception: pass
            elif train_on_map:
                if login_map == sc and train_safes:
                    if not _login_safe_done:
                        c.navigate_to(*_nearest_safe(c.pos, train_safes))   # dang o bai -> ra diem safe
                else:
                    try: c.teleport(12001, 0)                          # sai map -> ve Trac Quan (route keo ra sau)
                    except Exception: pass
            elif mode == "stand" and train_safes and login_map == sc:
                c.navigate_to(*_nearest_safe(c.pos, train_safes))       # stand map co safe -> ra safe
            # stand map la / khong co safe -> lam tai cho (ke me)
            if _do_startup_world_boss:
                _maybe_auto_world_boss("login, truoc pho ban doi")
            if _do_startup_team:
                if (not _run_auto_team_dungeons_if_needed(c, st, username, label, pidx,
                                                          is_leader, _stopped, pcfg)
                        and _pb_that_bai_co_phai_dung_han(c, _stopped, label, role)):
                    try: c.close()
                    except Exception: pass
                    return
            if _do_startup_daily:
                # O SO 2 cua nhiem vu hang ngay = BOSS THE GIOI -> do_world_boss() TELEPORT di roi
                # tra ve Trac Quan. Do la ly do login dung map train ma van thay "tele ve thanh"
                # (roi phai reform di route len lai bai). User chot 27/08: "chi can tele neu can
                # danh world boss thoi, con nhung cai khac thi chi can chay ra diem an toan dung".
                # -> Dang o bai train ma KHONG bat world boss thi lam nhiem vu NHE thoi (gacha,
                # hop vat pham, claim) - toan viec khong roi cho. Bat world boss thi heavy=True
                # nhu cu (luc do tele la DUNG y user).
                _heavy = bool(auto_world_boss) or not train_on_map
                if not _heavy:
                    log.info("[%s] (%s) o bai train + KHONG bat boss the gioi -> nhiem vu hang ngay "
                             "lam phan NHE thoi, khong teleport di dau", label, role)
                c.claim_daily_quests(heavy=_heavy)
        elif is_reconnect and train_on_map and train_safes:
            if login_map == sc:
                c.navigate_to(*_nearest_safe(c.pos, train_safes))   # reconnect + dang o bai -> ra safe cho keo
            else:
                # RECONNECT login lai o MAP KHAC train map (truoc khi rot member da teleport di lam
                # daily dungeon -> login = vi tri logout, van o thanh do). KHONG duoc THOAT oan.
                _rt = getattr(config, "TRAIN_ROUTES", {}).get(sc)
                _route_safe = _nearest_safe(c.pos, train_safes)
                _smart_rt = None
                if is_leader:
                    try:
                        if getattr(config, "SMART_WORLD_ROUTING", True):
                            _smart_rt = c.build_smart_route(sc, _route_safe)
                    except Exception as e:
                        log.warning("[%s] reconnect: loi build smart route: %s", label, e)
                if _train_route_available(_smart_rt, _rt, has_leader):
                    # sc la TRAIN MAP di bang ROUTE (qua cong) - KHONG phai thanh, teleport thang toi
                    # sc se FAIL (bug cu: go_to_town(20821) spam 60s roi hut). De khoi reform ben
                    # duoi (barrier + _do_reform) keo qua route; member cho leader keo (khong THOAT).
                    log.info("[%s] (%s) RECONNECT o map %s, train map %s di bang ROUTE -> de reform keo",
                             label, role, login_map, sc)
                else:
                    if getattr(config, "is_teleport_city", lambda _city: True)(sc):
                        # sc la thanh teleport TRUC TIEP -> ve thang sc roi ra safe.
                        log.info("[%s] (%s) RECONNECT o map %s != train map %s -> teleport ve train map roi ra safe",
                                 label, role, login_map, sc)
                        try: c.go_to_town(sc, city_flag)
                        except Exception as e:
                            log.warning("[%s] reconnect: teleport ve train map %s loi: %s", label, sc, e)
                        for _ in range(15):                     # cho map cap nhat sau teleport
                            if c.current_map == sc or not c.running: break
                            time.sleep(1)
                        if c.current_map == sc:
                            login_map = sc                       # -> self_map_ok=True ben duoi, khong THOAT
                            c.navigate_to(*_nearest_safe(c.pos, train_safes))
                    else:
                        log.warning("[%s] (%s) RECONNECT map train %s khong phai thanh teleport -> khong teleport thang",
                                    label, role, sc)

        # XOA (13/09) BARRIER login-dailies. No la mot vong acc TU CHO: leader tu dat dieu kien
        # "ca party xong daily thi toi moi lap party" (`dailies_done >= min(expected, _con_cho)`)
        # roi tu cho vo han cho du dieu kien do.
        #
        # Ca that party 48, 08/09 (user: "bon no ko lap pt"):
        #   06:52:36 [dtmot] (LEADER) CHO ca party xong daily (2/5, reconnecting=0)...
        #   06:53:37 [dtmot] (LEADER) CHO ca party xong daily (2/5, reconnecting=0)...
        #   06:52:51 [party 48] viec=moi - DOI chua du (dt901=4 dt902=0 dt903=0 dt904=4 dt905=0)
        # Dieu phoi ra lenh moi party; leader dang cho mot con so cua rieng no, khong nghe.
        #
        # KHONG THAY BANG DIEU KIEN KHAC (da thu 13/09: cho dieu phoi dem ho roi leader van cho -
        # van la mot vong cho, chi doi nguoi dem). Cai phai bo la HANH DONG TU CHO.
        #
        # Member dang lam daily o map/kenh khac chinh la LECH MAP / LECH KENH - dung thu ma dieu
        # phoi doc moi 2 giay va ra lenh gom. Khong can ai canh truoc ca.
        if not is_digioi and not is_reconnect and mode != "event":
            with st["lock"]:
                st["dailies_done"] += 1
            with st["lock"]:
                _o5_done = st.get("o5_state") == "done"
            if _o5_done and _clear_o5_client_flags(c):
                log.info("[%s] (%s) o5 da xong -> mo khoa teleport/reform sau pho ban", label, role)

        # Dong bo kenh: 1 dua (picker) chon kenh it nguoi -> ca lu sang cung.
        # Map-train: goi sau khi ve safe (doi kenh tren map thuong khong sao).
        def _dg_gather_giveup():
            """DG+Train: da co acc HET GIO DG (dt_done) trong khi party dang o PHA DG -> party khong
            bao gio gom du trong DG duoc nua (acc het gio dung NGOAI, khong vao lai instance duoc) ->
            BO gom/sync. Moi acc con time DG tu chay DG SOLO den het gio roi vao barrier -> sang train.
            Chi ap dung PHA DG (dt_phase='digioi'); pha train dt_done da clear nen khong dinh."""
            if not (dt_mode and _pstate(pidx).get("dt_phase") == "digioi"):
                return False
            with st["lock"]:
                # L2: doc thang dong ho tung acc, khong doc co acc tu dat.
                return any(_acc_het_gio_dg(account_clients.get(u)) is True
                           for u in _dt_party_usernames(pidx))

        def _ra_safe_truoc_khi_doi_kenh(ly_do=""):
            """DOI KENH thi phai ra DIEM AN TOAN truoc, khong duoc doi ngay giua bay quai.

            User chot 27/08. Doi kenh KHONG doi map/toa do: sang kenh moi la dung Y NGUYEN cho cu,
            nhung o kenh moi cho do co the day quai va party VUA TAN (leave_party truoc khi doi)
            -> tung acc dung le giua bai, an dan ngay khi vua vao kenh.
            """
            _manual_target = st.get("ui_train_target")
            _manual_here = bool(_manual_target and c.current_map == int(_manual_target[0]))
            if (not train_on_map or c.current_map != sc) and not _manual_here:
                return True
            if _manual_here:
                _manual_safes = list((getattr(config, "TRAIN_MAPS", {}).get(c.current_map) or {}).get("safe") or [])
                diem = _nearest_safe(c.pos, _manual_safes) if _manual_safes else None
            else:
                diem = st.get("rally_point") or (train_safes[0] if train_safes else None)
            if not diem:
                return True
            log.info("[%s] (%s) %s: ra diem an toan %s truoc khi doi kenh",
                     label, role, ly_do or "doi kenh", diem)
            try:
                # `_ra_rally_gom_lai` = cho het tran + DOC LAI TOA DO de XAC NHAN da toi, thu lai
                # 3 lan. `navigate_to` tran khong xac nhan gi -> doi kenh khi con dang giua bai
                # (user chot 30/08: "chuyen sau thi phai check lai da o safe chua").
                #
                # CHUA GIAI TAN PARTY (party con DU) thi KHONG bo chay - du party thi viec gi phai
                # chay, dinh quai cu danh cho xong roi di tiep (user chot 30/08).
                _con_party = bool(getattr(c, "party_members", None))
                _ok = _ra_rally_gom_lai(ly_do or "doi kenh", bo_chay=not _con_party)
                if not _ok:
                    log.warning("[%s] (%s) %s: CHUA ra toi diem an toan %s -> chua doi kenh",
                                label, role, ly_do or "doi kenh", diem)
                return _ok
            except Exception as e:
                log.warning("[%s] (%s) %s: loi ra safe truoc khi doi kenh: %s",
                            label, role, ly_do or "doi kenh", e)
                return False

        def do_channel_sync():
            # Gen reform luc BAT DAU sync. Neu ca party BUMP reform_gen (chuyen sang reform moi) trong
            # luc acc nay dang cho channel_ready -> picker da bo gen cu -> channel_ready gen cu KHONG
            # BAO GIO set -> acc ket "cho picker chot kenh" VO HAN (online ma im, bug that: ton4005 qua
            # barrier gen cu roi ket o do_channel_sync khi ca lu da sang gen 3). -> Cac vong cho ben
            # duoi check ham nay de THOAT khi gen doi, quay ve keepalive re-reform theo gen moi.
            _sync_reform_g0 = st["reform_gen"]
            def _sync_gen_moved():
                return st["reform_gen"] > _sync_reform_g0
            # DG+Train: chinh acc NAY het gio DG (bi day ra Quang Truong giua luc gom/sync) -> BAO
            # XONG DG cho party (mark dt_done) roi cho party + relogin train, thay vi ket o vong sync
            # ma khong bao (bug that: acc dung o Quang Truong, party cho mai khong biet no da xong).
            if _finish_digioi_train_if_time_over("sync kenh DG"):
                return True
            # DG+Train: co acc KHAC het gio DG -> KHONG gom party DG nua (tranh reform vo han "1/5"),
            # de acc con time chay DG solo den het gio. Bo qua sync, coi nhu xong.
            if _dg_gather_giveup():
                log.info("[%s] (%s) DG+Train: da co acc het gio DG -> BO gom party DG, chay DG SOLO den het gio",
                         label, role)
                return True
            # CA PARTY DA CUNG KENH -> KHONG CO VIEC GI DE DONG BO. Ra ngay, TUYET DOI khong roi doi.
            #
            # Doi kenh bat buoc phai ROI DOI truoc (server cam doi kenh khi con trong doi), nen chay
            # vong nay luc da cung kenh = tu tay pha party de "dong bo" ve dung cai kenh minh dang
            # dung.
            #
            # Ca that 10/09 party 1 (user: "p1 lai deo thay lap party"):
            #   23:18:02  bon member vao doi -> roster 4 nguoi          <- DA DU
            #   23:18:04 [chihao] (member) cap nhat kenh hien tai truoc sync: 1
            #   23:18:04 [chihao] Roi/giai tan party cu                 <- roi doi DE SYNC KENH
            #   23:18:05  roster con 0                                  <- doi tan sau 3 giay
            # Trong khi chinh dieu phoi ghi "ca party da chung kenh 1". Leader moi lai, lai tan, cuoi
            # cung no di route MOT MINH - va do la HAU QUA, khong phai dieu phoi ra lenh di mot minh.
            #
            # Doc THANG kenh cua tung client (L2), khong hoi ai.
            try:
                _ks = {int(getattr(_uc, "current_channel", 0) or 0)
                       for _u2, _p2, _l2, _k2 in party_accounts(pidx)
                       for _uc in (account_clients.get(_u2),)
                       if _uc is not None and getattr(_uc, "running", False)
                       and getattr(_uc, "current_channel", None)}
                if len(_ks) == 1 and not st.get("auto_best_channel", False):
                    log.info("[%s] (%s) sync kenh: CA PARTY DA CUNG KENH %d -> khong lam gi "
                             "(roi doi luc nay la tu pha party)", label, role, next(iter(_ks)))
                    return True
            except Exception as e:
                log.debug("[%s] sync kenh: loi doc kenh ca party (bo qua): %s", label, e)
            # CHAN TRUOC: ca party phai cung MAP thi sync kenh moi co nghia (xem _party_same_map).
            _sync_expected = int(st.get("manual_train_expected") or len(party_accounts(pidx)))
            if not _party_same_map(st, username, c.current_map, _sync_expected,
                                   _stopped, label, role, pidx=pidx):
                return False
            # Sync kenh TAI CHO (dang o bai train) -> ra safe truoc, dung doi kenh giua bay quai.
            _ra_safe_truoc_khi_doi_kenh("sync kenh")
            current_channel = c.refresh_current_channel(wait=1.5)
            log.info("[%s] (%s) cap nhat kenh hien tai truoc sync: %s",
                     label, role, current_channel if current_channel is not None else "chua biet")

            def _prepare_channel_switch():
                # Client that khong cho doi kenh khi dang trong party; server tra result=3.
                # Roi party cu truoc khi sync de bot khong tuong doi kenh OK trong khi server tu choi.
                try:
                    in_party = bool(getattr(c, "party_members", None) or getattr(c, "party_leader", None)
                                    or is_joined(pidx, c.self_entity))
                except Exception:
                    in_party = bool(getattr(c, "party_members", None) or getattr(c, "party_leader", None))
                if not in_party:
                    return
                try:
                    c.leave_party()
                    if is_leader:
                        reset_party_joined(pidx)
                    else:
                        unmark_joined(pidx, c.self_entity)
                    time.sleep(0.8)
                except Exception as e:
                    log.warning("[%s] (%s) sync kenh: loi roi party cu: %s", label, role, e)

            def _report_channel_map(sync_gen, expected_map):
                ok = _ghi_sync_that_bai(
                    st, username, c.current_map, sync_gen, expected_map, label=label,
                )
                if not ok:
                    log.warning("[%s] (%s) sync kenh: doi kenh xong SAI MAP (%s != %s)",
                                label, role, c.current_map, expected_map)
                return ok

            def _wait_channel_map_reports(sync_gen, expected_map, expected):
                _last = 0
                _t0 = time.time()   # SAFETY: co acc khong tham gia vong sync (da tan ra sau reform)
                while c.running and not _stopped():
                    if _sync_gen_moved():   # ca party sang reform gen moi -> thoat cho ngay
                        return False
                    # -> KHONG cho vo han (deadlock "1/5"): het 60s coi nhu vong sync fail, thoat ra
                    # de leader moi/reform lai (member se dong bo o vong sau) thay vi treo mai.
                    if time.time() - _t0 > 60:
                        log.warning("[%s] (%s) sync kenh/map TIMEOUT 60s -> thoat, moi/reform lai",
                                    label, role)
                        return False
                    with st["lock"]:
                        if st.get("channel_sync_gen") != sync_gen:
                            return False
                        fail = st["channel_failed"].is_set()
                        reason = st.get("channel_failed_reason") or "co acc khong ve dung map/kenh"
                        reports = {}   # DUNG BANG BAO CAO NUA - dung het bang doc thang ben duoi
                    # DOC THANG map cua tung acc thay vi CHO "bao cao": ca party chay chung MOT
                    # tien trinh, leader nam san account_clients[u].current_map. Bat member phai tu
                    # khai la thua VA de ket: member ban viec khac (dang train) thi khong chay doan
                    # bao cao -> leader dung do 60s roi timeout, bump reform, lap lai. Log that
                    # party 18: 23:36-23:43 leader dot ~6' voi "cho acc bao cao map (1/5)" trong khi
                    # 4 member dang o map 14823 - thong tin leader THAY duoc ngay tu dau.
                    _live = {}
                    _live_ch = {}
                    for _u, _up, _uil, _uip in party_accounts(pidx):
                        _uc = account_clients.get(_u)
                        if _uc is None or not getattr(_uc, "running", False):
                            continue
                        _m = getattr(_uc, "current_map", None)
                        if _m is not None:
                            _live[_u] = _m
                        try:                       # kenh THAT (hoi lai server), khong phai so nho san
                            _live_ch[_u] = _uc.kenh_that()
                        except Exception:
                            _live_ch[_u] = None
                    # DUONG TAT nay tinh acc la "xong" ma KHONG can no bao cao. Phai xet CA KENH:
                    # acc dang ban viec khac (vd train) co map DUNG nhung CHUA HE doi kenh -> truoc
                    # day leader tinh la xong -> sync bao OK -> MOI PARTY trong khi no o kenh khac
                    # -> loi moi khong toi, party khong bao gio du (user hoi 27/08: "luc dong bo
                    # kenh m da check cung kenh roi moi moi party chua").
                    # ch = 0 (giu nguyen 1 kenh) hoac chua biet kenh cua chinh minh -> khong so.
                    with st["lock"]:
                        _ch_chot = st.get("channel")
                    _ch_chot = int(_ch_chot) if _ch_chot else 0
                    for _u, _m in _live.items():           # acc DA o dung map + DUNG KENH -> tinh NGAY
                        if _u in reports:
                            continue
                        if expected_map is not None and _m != expected_map:
                            continue
                        if _ch_chot:
                            _uch = _live_ch.get(_u)
                            if _uch is None or int(_uch) != _ch_chot:
                                continue      # chua sang kenh chung -> KHONG tinh la xong
                        reports[_u] = (True, _m)
                    with st["lock"]:
                        done = len(reports) + len(st["reconnecting"]) >= expected
                    # Acc o map KHAC han: khong co cua thanh cong -> dung cho cho het 60s. Cho 10s
                    # an toan vi luc teleport current_map con la map CU trong chocc lat.
                    if not done and time.time() - _t0 > 10:
                        _sai = {_u: _m for _u, _m in _live.items()
                                if _u not in reports and expected_map is not None
                                and _m != expected_map}
                        if _sai:
                            log.warning("[%s] (%s) sync kenh/map: %d/%d acc dung cho, acc con lai o "
                                        "MAP KHAC %s (can %s) -> khong cho het 60s, regroup luon",
                                        label, role, len(reports), expected, _sai, expected_map)
                            return False
                        # DUNG MAP nhung LECH KENH -> noi ro ten acc + kenh cua no. Truoc day im
                        # lang (chi thay "cho acc bao cao map (3/5)") nen khong doan duoc vi sao.
                        _lech_ch = {_u: _live_ch.get(_u) for _u in _live
                                    if _u not in reports and _ch_chot}
                        if _lech_ch:
                            log.warning("[%s] (%s) sync kenh: %d/%d acc da sang kenh %s, con lai "
                                        "CHUA sang: %s", label, role, len(reports), expected,
                                        _ch_chot, _lech_ch)
                    if fail:
                        log.warning("[%s] (%s) sync kenh/map FAIL (%s) -> pick lai",
                                    label, role, reason)
                        return False
                    if done:
                        log.info("[%s] (%s) sync kenh/map OK: %d/%d acc o map %s",
                                 label, role, len(reports), expected, expected_map)
                        return True
                    if time.time() - _last > 15:
                        _last = time.time()
                        log.info("[%s] (%s) sync kenh/map: %d/%d acc da toi (map yeu cau=%s, "
                                 "live=%s)...", label, role, len(reports), expected, expected_map,
                                 {_u: (_m, _live_ch.get(_u)) for _u, _m in _live.items()})
                    time.sleep(1)
                return False

            _prepare_channel_switch()
            if is_picker:
                # MOI VONG SYNC: clear channel_ready + channel cu -> member CHO pick MOI (tranh dung
                # kenh cu vong truoc). channel_ready chi clear o start_party -> vong 2+ member ko cho
                # -> kenh ko sync lai. Clear o day de moi vong deu re-sync that su.
                # need = so acc cua party -> chi chon kenh con DU CHO cho CA PARTY (tranh ket instance).
                # pick tra: 0=chi 1 kenh (giu nguyen) | None=co kenh nhung khong du cho (RETRY) | int=da chuyen.
                # KIEN TRI: 30s dau thu lien tuc (3s/lan), sau do 60s/lan, cho toi khi gom du ve 1 kenh.
                need = len(party_accounts(pidx))
                t0 = time.time()
                _sync_fail = 0
                while c.running and not _stopped():
                    _nhip_cho_kenh(st)   # con song va con dang tim kenh -> member cu cho
                    # Chinh acc nay het gio DG giua luc sync -> bao done + cho party (khong ket im).
                    if _finish_digioi_train_if_time_over("sync kenh DG (picker)"):
                        return True
                    # Race: vao sync roi moi co acc KHAC het gio DG -> bo gom ngay (khoi loop "1/5").
                    if _dg_gather_giveup():
                        log.info("[%s] (%s) DG+Train: acc het gio DG giua luc sync -> BO gom party DG",
                                 label, role)
                        return True
                    if _sync_gen_moved():   # ca party sang reform gen moi -> thoat sync, re-reform
                        log.info("[%s] (picker) reform gen doi luc sync kenh -> thoat, dong bo lai", label)
                        return False
                    # KHONG GOM PARTY O MAP TRUNG CHUYEN. `expected_map` lay THANG map dang dung,
                    # nen acc dang o dau thi ca party bi keo ve day - ke ca noi khong lam gi duoc.
                    #
                    # Quang Truong 12003 la cho server NHA NGUOI RA sau event / sau Di Gioi. No
                    # KHONG phai map train, KHONG phai thanh (`is_teleport_city(12003)` = False),
                    # nen gom xong cung khong co viec gi de lam - phai di tiep (ve thanh -> ra bai
                    # train) roi moi lap party.
                    #
                    # Ca that 08/09 party 7 (user: "p7 dung o quang truong thi party lam lon gi
                    # the" -> "lap pt o Quang truong la dieu ngu ngoc va vo nghia"): ca party dung
                    # o 12003 tu 21:28 den 23:32 - HAI TIENG:
                    #   21:31:45 (LEADER) sync kenh/map: 4/5 acc da toi (map yeu cau=12003, ...)
                    #   23:32:22 (LEADER) chua moi 1 member vi chua xac nhan live dung map/kenh:
                    #                     ['f5f4e44c:lech kenh live 3!=5']
                    _mtc = int(getattr(c, "current_map", 0) or 0)
                    if _mtc and not train_on_map and _mtc == MAP_TRUNG_CHUYEN:
                        log.warning("[%s] (%s) dang o MAP TRUNG CHUYEN %s (khong train, khong phai "
                                    "thanh) -> KHONG gom party o day, di tiep da", label, role, _mtc)
                        return False
                    expected_map = c.current_map
                    with st["lock"]:
                        st["channel_ready"].clear()
                        st["channel_failed"].clear()
                        st["channel_failed_reason"] = ""
                        st["channel"] = None
                        st["channel_expected_map"] = expected_map
                        st["channel_sync_gen"] = int(st.get("channel_sync_gen", 0)) + 1
                        sync_gen = st["channel_sync_gen"]
                    # CO nick TAY trong whitelist -> TUYET DOI khong doi kenh: doi la bo roi ho
                    # o kenh cu, ho khong thay/khong nhan duoc loi moi nua (bot chi doi kenh duoc
                    # cho cac acc cua chinh no).
                    _manual_wl = _manual_whitelist_names(pidx, c)
                    if _manual_wl:
                        log.info("[%s] (%s) co nick TAY trong whitelist %s -> GIU NGUYEN kenh %s "
                                 "(doi kenh se bo roi ho)", label, role, _manual_wl, c.current_channel)
                        r = 0
                    else:
                        # TRANH kenh dang hong: ca party bao "cung kenh N" ma khong thay nhau
                        # (khac instance) -> chon lai dung N thi van the. `st["kenh_hong"]` do
                        # nhanh "khong thay nhau" dat.
                        _tru = {1}
                        _hong = st.get("kenh_hong")
                        if _hong:
                            _tru.add(int(_hong))
                        _ghim = st.get("kenh_ghim")
                        if _ghim and int(_ghim) in _tru:
                            log.warning("[%s] (%s) kenh GHIM %s bi danh dau HONG -> bo ghim, "
                                        "chon lai kenh khac", label, role, _ghim)
                            with st["lock"]:
                                st["kenh_ghim"] = None
                            _ghim = None
                        if _ghim:
                            # User da chi dinh kenh bang lenh tay -> KHONG duoc tu chon kenh khac.
                            log.info("[%s] (%s) kenh GHIM tu lenh tay = %s -> KHONG tu chon kenh khac",
                                     label, role, _ghim)
                            r = int(_ghim) if c.switch_channel(int(_ghim), theo_lenh=True) else None
                        else:
                            # DIEU PHOI DA CHOT -> THEO NO, khong tu chon cai khac (L1). Picker chi
                            # duoc tu tim kenh khi dieu phoi chua co y kien; tu tim trong khi da co
                            # lenh = hai nguon lenh giang nhau (xem ghi chu o cho dang ky ben duoi).
                            _kd0 = st.get("kenh_dich")
                            if _kd0:
                                log.info("[%s] (%s) DIEU PHOI da chot kenh %s -> picker THEO, "
                                         "khong tu chon", label, role, _kd0)
                                r = int(_kd0) if c.switch_channel(int(_kd0), theo_lenh=True) else None
                            else:
                                r = c.pick_best_channel(need=need, exclude=tuple(sorted(_tru)))
                    if r is None and getattr(c, "_chan_switch_result", None) == 3:
                        # MA 3 <組隊不可換分區>: server noi minh DANG TO DOI. Leader tu roi party roi
                        # thu lai KHONG du - party la cua CA LU, con acc nao con trong doi thi
                        # server van coi la dang to doi.
                        # => bao CA PARTY: moi acc tu roi party roi dong bo kenh (`_prepare_channel_
                        # switch` cua tung acc goi `leave_party` truoc khi doi). Do la cach duy nhat
                        # go duoc, va no cung la ket qua mong muon: ca lu thoat party + chuyen kenh.
                        # Log 31/08 party 7 (14:45-14:55): leader ttsau tu xoay mot minh -> 67 luot
                        # ma 3, quet het kenh 2,3,4,... trong 10 phut ma khong thoat.
                        log.warning("[%s] (%s) doi kenh bi chan vi DANG TO DOI (ma 3) -> bao CA "
                                    "PARTY -> de DIEU PHOI ra lenh gom", label, role)
                        return False
                    if r is None:   # co kenh nhung khong kenh nao du cho ca party -> CHO kenh trong
                        # KIEN TRI, KHONG dồn: user chot 01/09 "DG deo can don, cu random 10-15s
                        # roi thu lai cho den khi co du cho hoac het gio DG" - gom ca party ve
                        # kenh leader cung khong du cho, chi lam ca lu chen vao mot kenh dang day.
                        # Random (khong phai 60s co dinh) de nhieu party khong cung nhip hoi lai
                        # va cung nhau nhay vao dung mot kenh vua trong ra.
                        _nhip = 3.0 if time.time() - t0 <= 30 else random.uniform(10.0, 15.0)
                        if time.time() - t0 > 30:
                            log.info("[%s] (%s) chua co kenh du cho ca party (%d acc) -> thu lai "
                                     "sau %.0fs (kien tri toi khi co cho / het gio DG)",
                                     label, role, need, _nhip)
                        _nhip_cho_kenh(st)   # bao member: picker VAN DANG LAM, dung bo cho
                        time.sleep(_nhip)
                        continue
                    ch = r          # 0 (giu nguyen) hoac int (da chuyen) -> chot tam
                    # ch == 0 = server KHONG tra danh sach kenh = CHI CO 1 KENH -> GIU NGUYEN,
                    # member se bo qua doi kenh (nhanh `if not ch`) va chi bao map.
                    #
                    # TUNG ep `ch = c.current_channel` o day de "gom ca party ve kenh leader" (bug
                    # 20:55 "lech kenh live 2!=1"). BO di vi:
                    #  1. Ly do do het hieu luc: check "lech kenh live" da bi xoa - gio xac dinh
                    #     cung cho bang "co thay nhau quanh + cung map" chu khong so instanceId.
                    #  2. Con so do la instanceId, KHONG phai kenh the gioi. Trong thap no la so
                    #     instance -> ep member switch sang "kenh 2" -> server tra result=2 "khong
                    #     co kenh nay" -> member bao fail -> leader pick lai -> LOOP VO TAN
                    #     (bug that 16:05: sync lien tuc 10s/vong, khong bao gio thoat).
                    if not _report_channel_map(sync_gen, expected_map):
                        log.warning("[%s] (%s) picker doi kenh xong sai map -> pick lai", label, role)
                        time.sleep(2)
                        continue
                    with st["lock"]:
                        st["channel"] = ch
                        if ch and st.get("kenh_hong") and int(ch) != int(st["kenh_hong"]):
                            st["kenh_hong"] = None   # da sang kenh khac -> khong can tranh nua
                        # DANG KY KENH VUA CHON LAM LENH CHINH THUC (L1: chi MOT nguon lenh kenh).
                        # Khong ghi thi dieu phoi khong biet gi ve lua chon nay, no chot kenh RIENG
                        # va hai ben giang leader qua lai.
                        #
                        # Ca that 08/09 party 1 trong Di Gioi (user: "treo DG nhung co party ko
                        # danh tran nao") - leader nhay 27 -> 66 -> 27 -> 66 trong TAM giay:
                        #   01:01:23 Kenh it nguoi MA DU CHO ca party (5): kenh 27 -> chuyen sang
                        #   01:01:23 (LEADER) chon kenh 27 cho ca party (5 acc)
                        #   01:01:24 (LEADER) sync kenh/map OK: 5/5 acc o map 49942
                        #   01:01:26 (LEADER) DIEU PHOI chot kenh 66, minh dang o 27 -> tu chuyen
                        #   01:01:26 (LEADER) tu kiem kenh: dang o kenh 66 chu KHONG phai 27 -> da doi ve
                        #   01:01:34 (LEADER) DIEU PHOI chot kenh 66, minh dang o 27 -> tu chuyen
                        # Member dung yen o 27 nen leader khong bao gio moi duoc ("lech kenh live
                        # 27!=66"), party khong du, va cuoi cung leader rot xuong keepalive dung im
                        # trong DG 91 phut - het gio DG ma khong danh mot tran nao.
                        if ch:
                            st["kenh_dich"] = int(ch)
                            st["kenh_dich_luc"] = time.time()
                        st["channel_ready"].set()
                    if ch:
                        log.info("[%s] (%s) chon kenh %s cho ca party (%d acc)", label, role, ch, need)
                    else:
                        log.info("[%s] (%s) ca party giu nguyen 1 kenh (khong tach)", label, role)
                    if not _wait_channel_map_reports(sync_gen, expected_map, need):
                        _sync_fail += 1
                        # Sync fail = co member KET SAI MAP (khong bao cao). Truoc day chi retry sync
                        # noi bo MAI (member dung i o map khac o keepalive, khong ai reform) -> ca party
                        # dung ca ngay (bug that: leader loop "1/5", member dung yen map 21863). NGAY sau
                        # 1 lan timeout (~60s) -> BUMP reform_gen: keepalive CA PARTY (ke ca member
                        # idling) se _do_reform -> ve thanh regroup roi sync lai o CUNG map.
                        #
                        # KHONG gioi han theo mode nua: truoc day dieu kien la
                        # "(train_on_map or is_digioi)" nen MODE EVENT (40NPC/2K) roi thang xuong
                        # `continue` -> pick kenh khac roi lai cho 60s -> LAP VO HAN, khong bao gio
                        # lap duoc party (bug that: leader loop "cho acc bao cao map (1/5)" o map
                        # 10991, moi vong doi 1 kenh: 2 -> 6 -> ...). Bump reform_gen ap dung cho MOI
                        # mode: khong reform duoc thi cung phai thoat vong nay de tang len vong moi.
                        #
                        # 06/09 - BO `_bump_reform` o day: reform khong sua duoc gi (P3: leader
                        # lap "reform: khong co smart/legacy route -> bo qua" 28 lan trong 1 tieng),
                        # va DIEU PHOI moi la nguoi chot kenh/map. Viec DUY NHAT phai lam khi vong
                        # sync hong la DONG CUA no lai: xoa `channel_ready`/`channel` de khong con
                        # ai bam vao lenh da chet.
                        _dong_vong_sync(st)
                        log.warning("[%s] (%s) sync kenh/map FAIL %d lan -> DONG vong sync, de DIEU "
                                    "PHOI chot lai", label, role, _sync_fail)
                        return False
                    with st["lock"]:
                        st["auto_best_channel"] = False
                    break
            else:
                # cho picker CHOT kenh (co the lau neu dang doi kenh trong).
                #
                # PHAI CO HAN: phia picker da co timeout 60s, phia member truoc day cho VO HAN.
                # Member vao muon (vd vua reconnect) bat dau cho mot vong sync ma leader DA XONG va
                # khong lam lai -> `channel_ready` khong bao gio duoc set lai. Ket o day con keo
                # theo hau qua nang hon: `set_party_invite_ready(True)` nam SAU `do_channel_sync()`
                # nen cong loi moi KHONG BAO GIO MO -> leader moi bao nhieu cung vo ich.
                #
                # Log 31/08 party 14: trubon reconnect 09:13:21, xong viec vat 09:14:27
                # ("cap nhat kenh hien tai truoc sync: 1") roi im hoan toan; tu 09:15:13 den
                # 09:25:56 chi con lap "Chua san sang vao party -> GIU loi moi". Leader trumot ket
                # `da join=3 | roster server=3` suot 11 phut, moi lai 20s/lan.
                # HAN nay do theo NHIP TIM cua picker (`st["kenh_nhip"]`), KHONG theo dong ho
                # tuyet doi: server DONG thi picker kien tri quet kenh hang chuc phut la BINH
                # THUONG (user 01/09: "DG deo can don, cu random 10-15s roi thu lai cho den khi co
                # du cho hoac het gio DG"), cat cung 90s la bo party giua chung -> moi dua 1 kenh.
                # Han chi con bat khi picker IM HAN (thread ket/rot) - dung muc dich ban dau.
                CHO_KENH_CAP = 90.0
                _t_cho_kenh = time.time()

                def _het_kien_nhan():
                    _nhip = float(st.get("kenh_nhip") or 0.0)
                    return time.time() - max(_t_cho_kenh, _nhip) > CHO_KENH_CAP

                while c.running and not _stopped():
                    if _het_kien_nhan():
                        log.warning("[%s] (member) picker IM qua %.0fs (khong con nhip tim) -> THOI "
                                    "CHO, mo cong loi moi va dong bo o vong sau (khong treo cung)",
                                    label, CHO_KENH_CAP)
                        return False
                    if _finish_digioi_train_if_time_over("sync kenh DG (member)"):
                        return True
                    if _dg_gather_giveup():   # co acc KHAC het gio DG -> khoi cho picker, chay DG solo
                        return True
                    if _sync_gen_moved():     # ca party sang reform gen moi -> thoat, re-reform theo gen moi
                        log.info("[%s] (member) reform gen doi luc cho kenh -> thoat sync, dong bo lai", label)
                        return False
                    while not st["channel_ready"].wait(5):
                        if not c.running or _stopped():
                            return
                        if _het_kien_nhan():
                            log.warning("[%s] (member) picker IM qua %.0fs (khong con nhip tim) -> "
                                        "THOI CHO, mo cong loi moi va dong bo o vong sau",
                                        label, CHO_KENH_CAP)
                            return False
                        _resync_ck(st, username)   # ep dong bo -> relogin bam leader
                        if _finish_digioi_train_if_time_over("sync kenh DG (member wait)"):
                            return True
                        if _dg_gather_giveup():
                            return True
                        if _sync_gen_moved():   # gen doi giua luc cho channel_ready -> thoat (bug ton4005)
                            log.info("[%s] (member) reform gen doi luc cho channel_ready -> thoat sync", label)
                            return False
                    with st["lock"]:
                        ch = st["channel"]
                        expected_map = st.get("channel_expected_map")
                        sync_gen = st.get("channel_sync_gen", 0)
                    if not ch:
                        _report_channel_map(sync_gen, expected_map)
                        break
                    if c.switch_channel(ch, wait=4.0, retries=1, theo_lenh=True):
                        if _report_channel_map(sync_gen, expected_map):
                            log.info("[%s] (member) da chuyen sang kenh chung = %s, map=%s",
                                     label, ch, c.current_map)
                            break
                        # Doi kenh OK nhung SAI MAP (bi day ra 12003) -> THOAT ngay, khong do lai
                        # cho picker. Dieu phoi thay minh lech map la ra lenh gom/dong bo o nhip
                        # sau; do lai o day chi lam ca party cho mot dua khong ai go.
                        log.warning("[%s] (member) sang kenh %s roi nhung SAI MAP (%s) -> de DIEU "
                                    "PHOI xu ly", label, ch, c.current_map)
                        return False
                    _prepare_channel_switch()
                    if c.switch_channel(ch, wait=4.0, retries=1, theo_lenh=True):
                        if _report_channel_map(sync_gen, expected_map):
                            log.info("[%s] (member) da chuyen sang kenh chung = %s sau khi roi party cu, map=%s",
                                     label, ch, c.current_map)
                            break
                        log.warning("[%s] (member) sang kenh %s roi nhung SAI MAP (%s) -> de DIEU "
                                    "PHOI xu ly", label, ch, c.current_map)
                        return False
                    # Vao kenh khong duoc: GHI SO DEN roi THOAT ngay - dieu phoi se chot kenh khac
                    # o nhip sau. TUYET DOI khong do lai cho leader "pick lai": leader co the dang
                    # ket o vong cho member, va cho no ready la chinh minh (P3 06/09, ket 1 tieng).
                    log.warning("[%s] (member) khong doi duoc sang kenh chung %s (result=%s) -> "
                                "de DIEU PHOI chot kenh khac", label, ch,
                                getattr(c, "_chan_switch_result", None))
                    return False      # dieu phoi DOC THANG `_chan_switch_result`, khoi bao cao
                time.sleep(2)

        def _do_reform(to_spot=True):
            """CA party REFORM: ve thanh gom nhau -> leader GIAI TAN party cu + lap lai + KEO ca
            party qua route toi train map (member bi keo theo) -> RA SAFE gan quai TRUOC -> (neu
            to_spot) di tiep ra spot. Dung CHUNG cho 2 tinh huong (truoc day 2 noi code RIENG, trung
            logic nhau -> sua 1 cho quen cho kia, sinh bug "di thang xuyen tuong" o 1 nhanh):
              - LOGIN co acc sai map (goi voi to_spot=False - dung lai rally, de flow chung ben duoi
                lap party/di spot nhu binh thuong neu chua tung lap).
              - DANG CHAY co acc lech map (chet/dump/mat ket noi) - goi tu keepalive, to_spot=True
                (di thang ra spot luon vi khong con buoc nao khac lo tiep)."""
            # `sc` la snapshot luc thread login. Android login bang mode=stand/sc=0 roi
            # AUTO BATTLE moi chon bai; sau reconnect ma dung `sc` thi route thanh map 0.
            _reform_sc = _map_train_dich(pidx, st) or int(sc or 0)
            _ui_target = st.get("ui_train_target")
            _reform_tm = (getattr(config, "TRAIN_MAPS", {}) or {}).get(_reform_sc) or {}
            _reform_safes = []
            _resolved_reform_safe = _resolve_train_safe(
                c, _reform_sc, list(_reform_tm.get("safe") or []))
            if _resolved_reform_safe is not None:
                _reform_safes.append(_resolved_reform_safe)
            route2 = getattr(config, "TRAIN_ROUTES", {}).get(_reform_sc)
            route_safe = st.get("rally_point") or (
                _reform_safes[0] if _reform_safes else None
            )
            spot = (tuple(_ui_target[1:3]) if _ui_target and len(_ui_target) >= 3
                    else st.get("mob_spot"))
            _g0 = st["reform_gen"]   # gen reform DANG xu; co gen MOI hon (acc khac van) -> abort keo, quay lai xu

            # Luc BAT DAU keo party ra bai, party co DU khong. Du ma TUT giua duong thi phai dung
            # lai: quyet dinh "du 4/4" duoc chup MOT LAN roi leader loi qua 5-6 cong mat 3-4 phut,
            # khong ai kiem tra lai (party 1, 30/08: 15:03:50 roster 4 -> 15:03:55 "4/4 -> KEO qua
            # cong" -> 15:03:57 roster tut con 3 (chihao roi) -> van keo di het 23001/23000/23811/
            # 23000/23521... toi 15:07, den 15:08:08 moi bao "chua du party (3/4)").
            _keo_du_party = []    # [1] = da bat dau keo voi party DU
            _keo_thieu = []       # [t0] = moc bat dau thieu nguoi (grace, tranh nhay roster)
            KEO_THIEU_GRACE = 15.0

            def _keo_bi_tut_nguoi():
                """Ly do abort neu party tut nguoi giua duong keo; "" = van on.

                Grace 15s: roster nhay 1 nhip khi member qua cong la binh thuong, dung vi the ma
                huy ca chuyen di.
                """
                if not _keo_du_party or int(st.get("n_members") or 0) <= 0:
                    return ""
                _nj = joined_member_count(pidx)
                if _nj >= st["n_members"]:
                    _keo_thieu[:] = []
                    return ""
                if not _keo_thieu:
                    _keo_thieu.append(time.time())
                    return ""
                if time.time() - _keo_thieu[0] < KEO_THIEU_GRACE:
                    return ""
                if len(_keo_thieu) < 2:      # chi bump MOT lan cho ca chuyen keo
                    _keo_thieu.append(1)
                return "party TUT con %d/%d giua duong keo -> dung keo, gom lai" % (
                    _nj, st["n_members"])

            def _ab(_seen=[]):
                """abort cho moi buoc di duong cua reform. NOI RO vi sao bat (1 lan/reform).

                Truoc day la lambda cam: acc dang di bo giua duong tu dung, khong mot dong log ->
                khong phan biet duoc "bi bump reform_gen" voi "route hong" (that: 16:39 leader di
                4/7 cong roi dung o 18801, mat ca buoi doan xem timeout hay du lieu sai).
                """
                if _stopped():
                    why = "party da dung"
                elif not c.running:
                    why = "client khong con chay"
                elif st["reform_gen"] > _g0:
                    why = "reform_gen %d -> %d (acc khac bump)" % (_g0, st["reform_gen"])
                else:
                    why = _keo_bi_tut_nguoi()
                    if not why:
                        return False
                if not _seen:
                    _seen.append(1)
                    log.info("[%s] (%s) ABORT di duong reform: %s", label, role, why)
                return True
            plan_ready = st.setdefault("route_plan_ready", threading.Event())
            plan = None
            smart_route2 = None

            def _chot_thanh_tap_ket(vi_la_leader):
                """Chot THANH TAP KET cho ca party roi cong bo vao st["route_plan"].

                MEMBER KHONG CAN BIET DUONG DI - no vao party roi bi leader KEO theo. Xem chinh
                dong ngay duoi khoi nay:
                    smart_route2 = plan.get("route") if is_leader else None
                tuc member NEM LUON cai route vua cho. Thu duy nhat member can la `city`/`flag`:
                ca party phai gom o CUNG MOT THANH thi leader moi moi duoc (server chan invite
                khac map).

                Vay ma truoc 05/09 chi `is_leader` duoc chot, member ngoi cho VO HAN. Luong leader
                ban viec khac la ca party dung. Da xay ra that (party 10, 05/09 18:22-18:23):
                leader luumot bi `ABORT di duong reform: reform_gen 1 -> 5` roi di lam dungeon +
                sync kenh, KHONG he cong bo cho gen moi; 3 member spam "cho leader lap duong toi
                map 21812" khong dut - de cho mot thu chung no se xoa.

                VI SAO VAN CHOT MOT CHO (khong de moi acc tu tinh): danh sach thanh DA MO khac
                nhau theo acc -> moi dua tu chon la party toe ra hai thanh, leader dung o A member
                dung o B, moi mai khong ai vao doi. Cho nen: mot ban chot dung chung, ai cong bo
                truoc thi thang, va leader duoc uu tien `ROUTE_PLAN_TIEP_QUAN_SEC` giay.

                MEMBER CONG BO THI KHONG KEM ROUTE (`route: None`): route la duong LEADER SE DI BO,
                ma duong di phu thuoc thanh DA MO cua tung acc - duong cua member co the leader di
                khong duoc. Leader tu dung duong cua minh toi cung thanh do.
                """
                _sr = None
                try:
                    if getattr(config, "SMART_WORLD_ROUTING", True):
                        _sr = c.build_smart_route(_reform_sc, route_safe)
                except Exception as e:
                    log.warning("[%s] reform: loi build smart route: %s", label, e)
                    _sr = None
                if _sr:
                    _p = {"gen": _g0, "city": int(_sr["city"]), "flag": int(_sr["flag"]),
                          "route": _sr if vi_la_leader else None}
                elif route2:
                    _p = {"gen": _g0, "city": int(route2.get("from_city", 0)),
                          "flag": int(route2.get("city_flag", 0)), "route": None}
                else:
                    # KHONG CO ROUTE VAN PHAI GOM (L0). Route la duong DI BO toi bai train; gom ve
                    # cung mot THANH thi khong can duong nao ca - chi can teleport. Bo qua o day =
                    # lenh gom cua dieu phoi khong bao gio duoc thi hanh, va vong goi thi lap lai.
                    #
                    # Ca that 08/09 party 1 (user: "van deo them ve cung kenh") - vong 18 giay,
                    # leader xGAx dung mot minh o Trac Quan 12001, bon member o Truong Sa 23001:
                    #   00:53:58 (LEADER) MODE=train start_city=0
                    #   00:54:22 (LEADER) reform: khong co smart/legacy route -> bo qua
                    #   00:54:24 (LEADER) party dang o 2 MAP KHAC NHAU [12001, 23001] -> se gom
                    #   00:54:40 (LEADER) reform: khong co smart/legacy route -> bo qua
                    #   00:54:42 (LEADER) party dang o 2 MAP KHAC NHAU [12001, 23001] -> se gom
                    # `start_city=0` nen khong co `fc`, khong co route - the la no NHAN RA phai gom
                    # roi TU CHOI gom. Bug nay da duoc sua RIENG cho thap 2K (`_thi_hanh_gom`);
                    # ngoai thap thi van con nguyen.
                    #
                    # Diem gom = THANH DANG CO NHIEU ACC NHAT (it phai di chuyen nhat, va thuong la
                    # da so party da tu ve day roi); khong ai o thanh thi roi ve `_gather_city`.
                    _tg = (_thanh_dong_acc_nhat(pidx)
                           or _gather_city(pidx, _reform_sc or 0, _g0))
                    log.warning("[%s] (%s) reform: khong co smart/legacy route -> VAN GOM, "
                                "diem gom = thanh %s", label, role, _tg)
                    _p = {"gen": _g0, "city": int(_tg), "flag": 0, "route": None}
                with st["lock"]:
                    _cu = st.get("route_plan")
                    if _cu and _cu.get("gen") == _g0:
                        # Acc khac chot truoc roi -> THEO no (ca party phai cung mot thanh).
                        # Route cua minh chi dung duoc neu no dan toi DUNG thanh do; khac thanh
                        # ma van di theo la leader di mot noi, party gom mot noi.
                        _theo = _sr if (vi_la_leader and _sr
                                        and int(_sr.get("city", 0)) == int(_cu.get("city", 0))
                                        ) else None
                        return dict(_cu), _theo
                    st["route_plan"] = _p
                st["route_plan_ready"].set()
                if not vi_la_leader:
                    log.warning("[%s] (%s) reform: leader KHONG chot thanh tap ket sau %ds -> TU CHOT "
                                "thanh %s cho ca party (member khong can duong di, chi can cung thanh)",
                                label, role, int(ROUTE_PLAN_TIEP_QUAN_SEC), _p.get("city"))
                return _p, _sr

            if is_leader:
                st["route_party_ready"].clear(); st["route_done"].clear()
                plan_ready.clear()
                with st["lock"]:
                    st["route_plan"] = None
                plan, smart_route2 = _chot_thanh_tap_ket(True)
                if plan.get("missing"):
                    return
            else:
                _last_plan_log = 0
                _lead_other_since = 0.0   # thoi diem thay leader o pha KHAC reform (grace chong nhay)
                _wd0 = time.time()
                while not _ab():
                    _barrier_watchdog(st, pidx, _wd0, "reform-cho-leader-route")
                    _resync_ck(st, username)   # ep dong bo -> thoat reform, relogin bam leader
                    with st["lock"]:
                        _p = st.get("route_plan")
                        if _p and _p.get("gen") == _g0:
                            plan = dict(_p)
                            break
                    set_account_activity(username, "reform: cho leader lap route -> %s" % _reform_sc, phase="wait")
                    # DONG BO THEO LEADER: leader LIVE + dang o pha KHAC reform (khong phai boss/chore)
                    # on dinh >30s -> leader se KHONG publish route (da sang pha khac) -> BO reform,
                    # quay ve vong chinh vao dung pha leader. Khong relogin, khong cho vo tan (bug that:
                    # gclchin ket "cho leader lap route" 20 phut trong khi leader o pha PB).
                    _lph = _leader_live_phase(pidx, st)
                    if _lph is not None and _lph not in ("reform", "boss_qd", "login_chore"):
                        if not _lead_other_since:
                            _lead_other_since = time.time()
                        elif time.time() - _lead_other_since > 30:
                            log.warning("[%s] (%s) reform: leader da sang pha '%s' (khong reform) -> "
                                        "BO reform, dong bo theo leader", label, role, _lph)
                            # DUT DIEM loop reform-vs-PB: khong the tin keepalive check bat trung pha
                            # 'team_dungeon' (pha leader flip-flop team_dungeon<->train giua report-wait
                            # va pos-log -> check hay truot -> member re-reform vo tan). Abort NAY fire
                            # chac chan -> THAM GIA PB LUON tu day (report luot khong phu thuoc map).
                            if _lph == "team_dungeon" and auto_team_dungeon and not is_leader:
                                log.info("[%s] (member) -> THAM GIA PB thay vi reform (tu abort)", label)
                                try:
                                    _run_auto_team_dungeons_if_needed(
                                        c, st, username, label, pidx, is_leader, _stopped, pcfg)
                                except Exception as e:
                                    log.warning("[%s] loi tham gia PB theo leader (bo qua): %s", label, e)
                            return
                    else:
                        _lead_other_since = 0.0   # leader ve reform/whitelist/khong-live -> reset grace
                    # LEADER LAP DUONG CHAM -> MEMBER RA, DE DIEU PHOI XU. TUYET DOI KHONG TU DI.
                    #
                    # Ban cu: het han thi member TU LAP DUONG (`_chot_thanh_tap_ket`) roi di lay.
                    # Ma buoc dau cua flow "di train" la `pre_route_town_hop()` -> VE THANH. Nen
                    # het han = bon member quay dau ve thanh dung luc leader dang keo chung ra bai.
                    #
                    # Ca that party 3, 11/09 (user: "ca pt dang chay ra map train thi bon member
                    # tele ve thanh"):
                    #   09:04:02 [laochin] (member) cho leader lap duong toi map 21841 (tu lap sau 20s)
                    #   09:04:34 [nanam] (LEADER) reform: 4/4 member join lai -> KEO qua cong ra train
                    #   09:04:36 [nanam] smart route 21841: gates=1,7,8,2,2,18
                    #   09:04:53 [nanam] qua cong idx=1 -> map 21002      <- leader DANG keo
                    #   09:04:55 [batbat]  pre-route: tele trung gian ve thanh 12001 truoc
                    #   09:04:55 [hoathap] pre-route: tele trung gian ve thanh 12061 truoc
                    # Party DU 4/4, chi la leader cong bo route cham 14 giay so voi han 20s. Ket qua
                    # la leader di tiep mot minh con ca doi ve thanh -> lech map -> gom -> lap lai...
                    #
                    # Day la DUNG loai loi cua `_cho_leader_keo` (da xoa 10/09): han dai hay ngan
                    # khong phai van de - van de la ACC TU QUYET khi het han (L1/L2). Leader hong
                    # THAT thi DIEU PHOI thay: no doc map ca party moi 2 giay va co san phep bat
                    # party dung hinh (`_acc_dung_hinh`) - mot cho quyet, khong phai bon member moi
                    # dua tu di mot huong.
                    if time.time() - _wd0 > ROUTE_PLAN_TIEP_QUAN_SEC:
                        log.info("[%s] (%s) reform: leader chua cong bo duong sau %ds -> RA, de "
                                 "dieu phoi quyet (khong tu lap duong, khong tu ve thanh)",
                                 label, role, int(ROUTE_PLAN_TIEP_QUAN_SEC))
                        return
                    if time.time() - _last_plan_log > 15:
                        log.info("[%s] (%s) reform: cho leader lap duong toi map %s (%.0fs, qua "
                                 "%ds thi RA de dieu phoi quyet)...", label, role, _reform_sc,
                                 time.time() - _wd0, int(ROUTE_PLAN_TIEP_QUAN_SEC))
                        _last_plan_log = time.time()
                    plan_ready.wait(1.0)
                    time.sleep(0.2)
                if not plan:
                    return
                if plan.get("missing"):
                    log.warning("[%s] (%s) reform: leader khong co smart/legacy route -> bo qua",
                                label, role)
                    return
            fc = int(plan.get("city", 0)); ff = int(plan.get("flag", 0))
            # Bao GUI biet party dang can thanh nao -> thong bao acc nao chua mo (thanh gan
            # bai train CHI biet luc chay, GUI khong tu suy ra duoc).
            if fc:
                st["need_city"] = fc
            smart_route2 = plan.get("route") if is_leader else None
            fc_is_city = (not fc) or getattr(config, "is_teleport_city", lambda _city: True)(fc)
            c.flee_mode = True
            if is_leader:
                c.leave_party()                  # GIAI TAN party cu (neu co) -> member duoc tha
                reset_party_joined(pidx)
            if fc:
                # CHI ve thanh gom nhau. KHONG lam boss/dungeon o day nua (truoc day lam MOI VONG
                # reform cho ca member -> churn teleport + keo dai reform -> member de MAT KET NOI
                # giua chung (server chap chon) -> ca party ket reconnect-reform loop, "member khong
                # ve theo leader" (xem log party 3). Boss/dungeon da chay 1 lan o login chores roi.
                # Khop ban APK _reform_to_spot (da cat phan nay). LUU Y: mat tinh nang danh boss QD
                # mid-session-train qua reform (train mode) - danh doi lay reform gon + on dinh.
                # LAP toi khi VE DUOC thanh: go_to_town co the FAIL het ca luot (tra False, vd server
                # cham/ket map la) - truoc day BO QUA ket qua -> acc ket map khac van di tiep xuong
                # sync kenh, leader moi vo tan ma member join khong duoc (server chan invite cross-map,
                # bug thuc te log 18:22). Chua ve duoc thi KHONG duoc di tiep.
                # Fallback thanh CHUA MO phai la QUYET DINH CHUNG PARTY: chi can 1 acc tele fc fail
                # du nguong -> ca reform-gen doi diem gom sang NGHIEP THANH. Neu de tung acc tu fallback
                # rieng, leader co the dung o fc con member dung o Nghiep -> invite ket cross-map.
                def _nghiep_fallback_active():
                    with st["lock"]:
                        return st.get("reform_gather_nghiep_gen") == _g0

                def _activate_nghiep_fallback(reason=None):
                    first = False
                    with st["lock"]:
                        if st.get("reform_gather_nghiep_gen") != _g0:
                            st["reform_gather_nghiep_gen"] = _g0
                            first = True
                    if first:
                        if reason:
                            log.warning("[%s] (%s) %s -> CA PARTY gom o NGHIEP THANH roi leader keo di bo toi %s",
                                        label, role, reason, fc)
                        else:
                            log.warning("[%s] (%s) tele thanh %s FAIL nhieu lan -> CA PARTY gom o "
                                        "NGHIEP THANH roi leader keo di bo toi %s",
                                        label, role, fc, fc)

                if fc and not fc_is_city:
                    _activate_nghiep_fallback(
                        "route-plan city %s KHONG phai thanh teleport" % fc
                    )
                elif fc and c.city_unlocked(fc) is False:
                    # BIET TRUOC thanh dich chua mo -> gom ngay o diem gom, KHONG thu tele 3 lan
                    # roi moi chuyen (log 15:27: spam "Teleport -> city 12011" hang chuc lan).
                    _activate_nghiep_fallback("thanh %s CHUA MO tele voi acc nay" % fc)

                # _town_fail BEN tren client (KHONG reset moi vong _do_reform): reform_gen la bien
                # CHUNG party -> acc khac bump -> _ab() True -> _do_reform BI ABORT + goi lai lien tuc.
                # Chi reset khi VE DUOC dung diem gom cua gen nay.
                _arrival_done = False
                while not _ab() and not _arrival_done:
                    _gc = _gather_city(pidx, fc, _g0)
                    _target_city = _gc if _nghiep_fallback_active() else fc
                    _gc_name = (getattr(config, "TELEPORT_CITIES", None) or {}).get(
                        _gc, {}).get("name", _gc)
                    _target_name = _gc_name if _target_city == _gc else f"thanh {fc}"
                    while not _ab() and c.current_map != _target_city:
                        set_account_activity(username, "reform: ve %s (dang o map %s)" % (_target_name, c.current_map),
                                             phase="reform")
                        _town_ok = False
                        try:
                            if _target_city == fc:
                                # Tele TRUNG GIAN Trac Quan/Ng.Thanh (50-50) truoc roi moi ve thanh route.
                                c.pre_route_town_hop()
                                # budget NGAN (tries=6, battle_grace=0 -> ~12s/lan) de FAIL NHANH:
                                # fc chua mo thi 3 lan ~1 phut la chuyen sang gom o Nghiep Thanh.
                                _town_ok = c.go_to_town(fc, ff, tries=6, wait=2.0, battle_grace=0.0)
                            else:
                                _gc_flag = (getattr(config, "TELEPORT_CITIES", None) or {}).get(
                                    _gc, {}).get("flag", 2)
                                _town_ok = c.go_to_town(_gc, _gc_flag)
                        except Exception as e:
                            log.warning("[%s] reform: loi ve %s: %s", label, _target_name, e)
                        if _town_ok or c.current_map == _target_city:
                            break
                        if _target_city == fc:
                            c._reform_town_fail = getattr(c, "_reform_town_fail", 0) + 1
                            if getattr(c, "_reform_town_fail", 0) >= 3:
                                c._reform_town_fail = 0
                                _activate_nghiep_fallback()
                                break
                        log.warning("[%s] (%s) reform: CHUA ve duoc %s (map=%s) -> nghi 10s thu lai",
                                    label, role, _target_name, c.current_map)
                        time.sleep(10)
                    if _ab():
                        return
                    # Co acc khac vua bat fallback trong luc minh da toi fc -> quay lai outer loop
                    # de cung ve Nghiep, khong cho barrier dem nham "toi noi" khac map nhau.
                    if _nghiep_fallback_active() and _target_city != _gc:
                        continue
                    if c.current_map != _target_city:
                        continue
                    c._reform_town_fail = 0
                    c._reform_via_nghiep = (_target_city == _gc)
                    # BOSS QUAN DOAN mid-session: reform nay CO THE do boss QD toi luot (train mode
                    # khong danh trong party -> keepalive bump reform_gen ve thanh). Dang SOLO o thanh
                    # (party da giai tan o dau reform) -> DANH LUON, TRUOC khi mark "da toi" -> barrier
                    # ben duoi cho CA PARTY danh xong moi sync kenh + moi lai (khong bi keo vao party
                    # giua chung boss). Gate legion_boss_available() -> reform THUONG (displaced/startup/
                    # dungeon) KHONG chay -> tranh churn town-chore. Danh xong -> available False -> het
                    # trigger -> PHA loop "toi bai -> ve thanh" vo han (bug that: log 10:08). Truoc day
                    # dong bo PC theo APK da CAT phan nay -> mat boss QD mid-train + sinh loop.
                    if not _ab() and getattr(c, "fight_legion_boss", True):
                        try:
                            if c.legion_boss_available():
                                log.info("[%s] (%s) reform: boss QD toi luot -> danh solo o thanh",
                                         label, role)
                                c.do_legion_boss()
                        except Exception as e:
                            log.warning("[%s] reform: loi danh boss QD (bo qua): %s", label, e)
                    # BARRIER: CHO CA PARTY VE DEN CUNG diem gom TRUOC khi sync kenh + moi party.
                    # Bug thuc te (log 10:08): chubon con dang di bo ra khoi DG thi leader da sync kenh
                    # + moi -> chubon accept dung luc dang teleport -> server KHONG ghi nhan (roster chi
                    # 3 member) nhung _mark_joined van dem -> leader tuong du 4/4, keo ca party di train
                    # BO chubon lai. Phai du mat CA PARTY o thanh roi moi di tiep (reconnecting cong vao
                    # de khoi deadlock - acc rot se duoc reform lai khi quay ve).
                    if is_leader:
                        # Member co the ve thanh truoc leader. Clear generation kenh CU truoc khi
                        # leader mark arrived lam barrier du nguoi; sau khi barrier nha, member se
                        # buoc phai cho picker mo generation MOI thay vi an channel_ready stale.
                        _prepare_reform_channel_sync(st)
                    # KHONG khai bao "da toi" nua - bot TU THAY acc dang o dau qua
                    # `account_clients[u].current_map`. Chi ghi DICH DEN cua vong gom nay de acc
                    # khac biet "co nguoi dang cho o thanh nao" (`_gather_dich`).
                    with st["lock"]:
                        _gd = st.setdefault("gom_dich", {})
                        for _gk in [k for k in _gd if k < _g0]:
                            _gd.pop(_gk, None)    # don gen cu
                        _gd[_g0] = (_target_city, time.time())
                    # BARRIER "cho ca party ve den thanh" DA BO (user chot 08/09: "gio viec check cung map
                    # cung kenh la dieu phoi phai lam roi ma").
                    #
                    # No sinh ra vi mot benh khac: bot dem nguoi bang SO TU GHI (`_PARTY_JOINED` /
                    # `_mark_joined`) nen leader "tuong du 4/4" trong khi roster server chi 3, va keo ca
                    # party di train bo mot dua lai. Benh do da chua tan goc - `_thieu_doi` doc THANG
                    # `c.party_members` (roster server gui ve), khong con so tu ghi nao de tuong nham.
                    #
                    # Giu lai thi no la CAI BAY: dung trong barrier la khong chay toi vong keepalive, ma moi
                    # lenh dieu phoi (ke ca cua mo cong nhan loi moi - L0) deu doc o keepalive -> acc trong
                    # barrier DIEC hoan toan. Va no la acc TU CHO ACC KHAC, dung loai viec da bo.
                    #
                    # Ca that 08/09 party 17 (chubay, 40 phut, KHONG mot viec vat nao):
                    #   14:39:39 (member) ABORT di duong reform: reform_gen 2 -> 3 (acc khac bump)
                    #   ... im hoan toan, khong mot dong `pos=` (tuc khong vao keepalive) ...
                    #   15:15:39 Chua san sang vao party -> GIU loi moi ... se accept sau viec vat
                    #   15:20:43 [party 17] WATCH: chu707='reform: da ve Truong Sa, cho ca party (2...'
                    # Ba acc kia da lap duoc doi va di lam viec khac tu lau, nen con so "du 5 acc o thanh"
                    # khong bao gio dat duoc.
                    #
                    # GIO: ve toi diem gom la DI TIEP (sync kenh + moi/nhan loi moi). Ai chua ve thi DIEU PHOI
                    # tu thay (`_maps_cua_party`, `_thieu_doi`) va ra lenh gom tiep - do la viec cua no.
                    _arrival_done = True
                    with st["lock"]:
                        st.get("gom_dich", {}).pop(_g0, None)
                if _ab():
                    return
            # LUON re-sync kenh (khong chi switch ve kenh cu da luu) - vua ve thanh sau khi CO THE
            # da danh dungeon (solo o1 hoac team o5) -> server co the da day acc sang kenh KHAC (ngau
            # nhien). Chi switch ve kenh CU (st["channel"]) KHONG du: kenh do co the da DAY (full,
            # acc khac dang chiem) sau 1 hoi, hoac ban than viec dung 1 kenh CU thieu kiem tra lai
            # suc chua -> can PICK LAI qua do_channel_sync() (picker tu kiem tra du cho ca party
            # truoc khi chot, xem pick_best_channel) moi chac chan CA PARTY vao chung duoc 1 kenh.
            # Phan khu farm la theo TUNG MAP. Khu cua bai co the khong ton tai o thanh tap ket.
            if int(getattr(c, "current_map", 0) or 0) != int(_reform_sc or 0):
                with st["lock"]:
                    _farm_ch = st.get("train_channel_manual")
                    if (_farm_ch and st.get("kenh_ghim")
                            and int(st.get("kenh_ghim")) == int(_farm_ch)):
                        log.info("[%s] (%s) dang o map tap ket %s -> tam bo ghim phan khu farm %s",
                                 label, role, c.current_map, _farm_ch)
                        st["kenh_ghim"] = None
                    st["kenh_dich"] = None
                    st["kenh_dich_luc"] = 0.0
            do_channel_sync()
            if not is_leader:
                # Reform da xong viec rieng + ve diem gom + sync map/kenh. Tu day member moi duoc
                # accept loi moi party thuong; loi moi den som da duoc GameClient giu lai.
                c.set_party_invite_ready(True)
            if is_leader:
                # LAP LAI party TAI THANH: CHO VO HAN toi khi DU member join (khong gioi han 8 lan).
                # Member dang reconnect -> bump reform_gen -> _ab() -> thoat de keepalive reform lai khi
                # no vao. Van hoan toan -> ca party dung o thanh cho (an toan).
                _inv_t0 = time.time()
                while joined_member_count(pidx) < st["n_members"]:
                    if _ab(): return   # stop / reform moi hon -> thoat de keepalive xu lai
                    if _nhip_moi_party(True) == "gom":
                        log.info("[%s] (LEADER) reform: dieu phoi bao GOM (%s) -> thoi moi",
                                 label, (_ke_hoach(st) or {}).get("ly_do") or "lech map/kenh")
                        return
                    # KHONG cho vo han: 1 member ket SAI MAP (khong the moi) -> truoc day leader moi
                    # cac dua dung map MAI, ca team dung im ca ngay (bug that: a3570949 lech map
                    # 22000!=14001, treo 7 phut+). Sau 120s chua du -> BUMP reform_gen -> ca party
                    # (ke ca dua ket) reform lai tu thanh (dua ket se duoc teleport ve thanh gom).
                    if time.time() - _inv_t0 > 120:
                        log.warning("[%s] (LEADER) reform: 120s chua du party (%d/%d) - co member ket "
                                    "sai map -> BUMP reform_gen, gom lai tu dau",
                                    label, joined_member_count(pidx), st["n_members"])
                        return
                    time.sleep(4)
                log.info("[%s] (LEADER) reform: %d/%d member join lai -> KEO qua cong ra train map",
                         label, joined_member_count(pidx), st["n_members"])
                # BAO CAO DANG LAM VIEC. Truoc day tu day tro di leader KHONG bao cao gi nua, nen pha
                # cua no VAN NAM NGUYEN o "wait" cua vong cho party o thanh (2448), con heartbeat vong
                # mang thi tiep tuc lam bao cao "tuoi". Member cho leader lap route cung bao "wait"
                # (dung - ho cho that) => CA PARTY "wait" + tuoi => watcher ket luan DEADLOCK, sau
                # WATCH_ALLWAIT_SEC=120s ep dong bo -> bump reform_gen -> _ab() cua chinh leader bat ->
                # dang di bo giua duong thi tu dung. Log that 16:37:10 keo -> 16:39:14 "keo di bo
                # Nghiep->18001 that bai (map=18801)": party HOAN TOAN KHOE, chi vi leader quen khai
                # bao la minh dang lam viec.
                set_account_activity(username, "reform: keo party ra bai %s" % _reform_sc, phase="reform")
                try: c.set_party_strategist()
                except Exception: pass
                st["route_party_ready"].set()    # bao member: party lap xong, sap keo
                time.sleep(1.5)
                _full = st.get("n_members", 0) > 0 and joined_member_count(pidx) >= st["n_members"]
                c.flee_mode = not _full   # du party -> DANH bat chap khi keo
                if _full:
                    # Tu day _ab() theo doi luon so member: tut giua duong la DUNG KEO ngay, khong
                    # loi het 5-6 cong roi moi phat hien (xem chu thich o _keo_bi_tut_nguoi).
                    _keo_du_party.append(1)
                    _keo_thieu[:] = []
                if getattr(c, "_reform_via_nghiep", False) and c.current_map != fc:
                    # fc chua mo -> gom o Nghiep Thanh -> KEO ca party DI BO tu Nghiep Thanh toi THANH
                    # fc (gan bai). Member trong party TU FOLLOW qua cong (game keo theo leader). flee=
                    # not _full: du party -> DANH cong NPC. Toi fc roi -> keo fc->bai binh thuong ben duoi.
                    log.info("[%s] (LEADER) fc chua mo -> KEO party DI BO tu Nghiep Thanh -> thanh %s", label, fc)
                    try:
                        _walk_ok = c.follow_smart_scene_route(c.current_map, fc, None, abort=_ab, flee=not _full)
                    except Exception as e:
                        _walk_ok = False
                        log.warning("[%s] (LEADER) loi keo di bo Nghiep->fc: %s", label, e)
                    if not _walk_ok or c.current_map != fc:
                        # IN LY DO: execute_smart_route co 3 loi thoat IM LANG (aborted /
                        # unexpected_scene / gate_failed) va da ghi san vao _smart_route_failure,
                        # nhung truoc day dong log nay VUT DI -> khong ai biet vi sao dung giua
                        # duong (that: 16:39 di 4/7 cong roi dung o 18801, khong mot dong ly do).
                        log.warning("[%s] (LEADER) keo di bo Nghiep->%s that bai (map=%s, ly do=%s) "
                                    "-> de DIEU PHOI ra lenh gom", label, fc, c.current_map,
                                    getattr(c, "_smart_route_failure", None) or "khong ro")
                        c._reform_via_nghiep = False
                        c.flee_mode = True
                        st["route_done"].set()
                        return
                    c._reform_via_nghiep = False
                if smart_route2:
                    c.follow_smart_route(
                        _reform_sc, route_safe, abort=_ab, flee=not _full
                    )
                elif not route2:
                    # KHONG CO ROUTE NAO CA - va do la BINH THUONG voi mode khong-train.
                    #
                    # `route2 = TRAIN_ROUTES.get(sc)`; mode event (40NPC / 2K / loan dau) thi `sc`
                    # la map event, khong co trong `TRAIN_ROUTES` -> None. Ban cu nhay thang xuong
                    # `route2.get("steps", [])` -> `'NoneType' object has no attribute 'get'`, va
                    # loi do bi `except Exception` nuot thanh mot dong "loi reform (bo qua)".
                    #
                    # Ca that party 6 / 7 / 23, 14/09 (user: "danh xong 40NPC no ve Nghiep thanh
                    # lam gi the"):
                    #   21:19:09 [ttsau] (LEADER) reform: 4/4 member join lai -> KEO qua cong ra
                    #                    train map            <- mode EVENT ma di duong train
                    #   21:19:10 [ttsau] loi reform (bo qua): 'NoneType' object has no attribute 'get'
                    #   21:20:12 [ttbay] PARTY: ... ROI doi -> roster con 3 -> 2 ...
                    # Reform chet giua chung nen party vua gom xong lai tan, ca lu dung o Nghiep
                    # Thanh (12061 - thanh gom) thay vi quay lai map event.
                    #
                    # Gom ve cung thanh la XONG viec cua reform o mode nay: khong co bai train de
                    # keo toi. Phan "di dau tiep" do dieu phoi ra lenh o vong sau.
                    log.info("[%s] (%s) reform: da gom ve cung thanh, mode=%s khong co route train "
                             "-> DUNG o day, dieu phoi ra lenh tiep", label, role, mode)
                else:
                    # Legacy fallback: member trong party tu theo leader qua tung cong.
                    for stp in route2.get("steps", []):
                        if _ab(): break
                        t1 = time.time()
                        while c.in_combat(idle_secs=3.0) and not _ab() and time.time() - t1 < 60:
                            time.sleep(0.5)
                        if "gate" in stp:
                            if not c._enter_gate(int(stp["x"]), int(stp["y"]), int(stp["gate"])):
                                break
                        else:
                            c.move_to(int(stp["move"][0]), int(stp["move"][1])); time.sleep(0.5)
                # toi train map -> RA SAFE GAN QUAI TRUOC (KHONG di thang tu cong ra spot - duong do
                # CHUA duoc dam bao khong xuyen tuong, chi co safe/mob_path la nguoi dung da tu do
                # tinh toan de an toan). to_spot=False (goi tu login sai map) -> DUNG o day, de flow
                # chung ben duoi (lap party binh thuong + _start_training) tu di tiep ra spot.
                if c.current_map == _reform_sc and not _ab():
                    rally2 = st.get("rally_point") or (
                        _reform_safes[0] if _reform_safes else None
                    )
                    if rally2:
                        c.navigate_to(*_jitter(rally2), flee=False, abort=_ab)
                    if to_spot and not _ab():
                        path = st.get("mob_path")
                        if path:
                            c.follow_path(path, flee=False, abort=_ab)
                        elif spot:
                            c.navigate_to(*_jitter(spot), flee=False, abort=_ab)
                # Xac nhan THAT SU dang o dung map train (sc) truoc khi coi reform la thanh cong -
                # cung 1 loai bug voi _start_training (xem ghi chu o do): tran chien xen giua co the
                # day leader sang map khac ma cac buoc tren van "thanh cong" (khong bi abort).
                if c.current_map != _reform_sc:
                    # KHONG re-bump reform_gen neu drag bi abort do CO reform MOI dang cho (reform_gen
                    # da > _g0 vi acc khac bump) HOAC stop/disconnect (_ab). Truoc day bump lai ->
                    # keepalive reform lai -> drag lai abort NGAY (reform_gen van > _g0) -> "vua lap
                    # party xong da reform" lap vo tan. Reform dang cho se tu xu; chi bump khi THAT SU
                    # ket giua duong (khong phai do reform moi/stop/rot).
                    if _ab():
                        log.info("[%s] (LEADER) drag abort do co reform moi/stop -> return, KHONG re-bump", label)
                        c.flee_mode = True
                        st["route_done"].set()
                        return
                    log.warning("[%s] (LEADER) reform xong NHUNG dang o SAI MAP (%s != %s) -> "
                                "de DIEU PHOI ra lenh gom", label, c.current_map, _reform_sc)
                    c.flee_mode = True
                    st["route_done"].set()   # tha member (keepalive se reform lai tu dau)
                    return
                c.combat_ready(); c.flee_mode = False
                st["route_done"].set()
            else:
                # MEMBER KHONG CHO, KHONG BAO CAO, KHONG TU DI DAU (L1/L2).
                #
                # Trong party, member bi leader KEO theo tu dong - do la co che cua chinh client
                # (`Logic/Team.lua` AddMember -> `Teleport(leader.position)`), khong can bot lam gi.
                # Viec duy nhat member phai lam la DUNG TU Y LAM VIEC KHAC trong luc do.
                #
                # Cho nay tung la HAI VONG CHO co cua leader (`route_party_ready` / `route_done`).
                # Sang 10/09 toi bo cho-vo-han va thay bang HAN CUNG 120s - van sai, chi doi mot
                # kieu sai: mot chuyen route qua nhieu cong dai hon 2 phut la binh thuong, member
                # het han giua chuyen, chay tiep flow "di train" -> `pre_route_town_hop()` -> phai
                # ROI DOI de teleport -> party tan giua duong.
                #
                # Ca that 10/09 party 1 (user: "dang di ra map train ma may dua bay ve thanh"):
                #   22:50:40 [xGAx]   qua cong idx=1 -> map 21521       <- leader VAN DANG KEO
                #   22:50:52 [chihao] THOI CHO leader keo (route_done) sau 120s
                #   22:50:57 [chihao] pre-route: tele trung gian ve thanh 12001 truoc
                #   22:50:57 [chihao] Teleport: dang o to doi -> ROI DOI truoc
                #   22:51:10 [party 1] viec=gom - party dang o 3 MAP khac nhau  <- hau qua, khong
                #                                                                 phai nguyen nhan
                # User chot: "da bao dieu phoi quyet het, acc deo cho deo bao cao gi het".
                #
                # GIO: member ra khoi `_do_reform` luon. Khong cho co nao, khong tu ve thanh, khong
                # tu di route. Leader keo thi no theo; leader hong thi DIEU PHOI thay (no doc map
                # cua ca party moi 2 giay) va ra lenh - dung mot cho quyet (L1).
                c.flee_mode = False
                return

        def _bat_danh_neu_du_party():
            """Bat lai danh SAU khi xu ly tai cho - nhung CHI khi DU PARTY.

            Caller (reform / lenh doi kenh tay) da dat flee_mode=True de di duong, khong bat lai
            thi bot dung o diem quai ma cu bo chay. NHUNG bat vo dieu kien thi pham nguyen tac
            "DU FULL PARTY MOI TRAIN": party vua bi giai tan de lap lai, leader danh mot minh
            giua bai (user bao 27/08: "lam lon gi ma leader danh 1 minh").
            _do_reform lam dung the nay: `_full = ...; c.flee_mode = not _full`.
            """
            _full = (st.get("n_members", 0) > 0
                     and joined_member_count(pidx) >= st["n_members"])
            c.flee_mode = not _full
            if _full:
                c.combat_ready()

        def _ra_rally_gom_lai(ly_do="", bo_chay=True):
            """PARTY HONG -> acc NAY dut tran + chay ve DIEM TAP KET, roi mo i lap lai party.

            User chot 27/08: "no dang danh ma co dua ko danh thi tuc la party hong me no roi" ->
            "truong nay thi cho chay het ra diem an toan roi tim cach party lai".
            Party train danh THEO PARTY: dua danh dua khong = party da vo. Dung o diem quai ma moi
            lai thi tran noi tran, moi khong toi noi (log 27/08 20:15-20:19: leader moi ca chuc
            lan, member combat lech nhau, party khong bao gio du).
            Da dung san o rally (<=60) thi KHONG chay lai - tranh di lai vo ich qua vung quai.

            Tra True = DA XAC NHAN dung o rally. `navigate_to` bi tran chien xen ngang la ve giua
            duong nhung KHONG bao loi, nen phai DOC LAI toa do ma kiem, va thu lai vai lan.

            `bo_chay=False`: party VAN CON DU (vd di ra safe TRUOC KHI giai tan de doi kenh) - du
            party thi viec gi phai chay, dinh quai thi cu danh cho xong roi di tiep.
            """
            # `_reform_sc/_reform_safes` truoc day la bien LOCAL ben trong `_do_reform`, nen
            # helper sibling nay khong the nhin thay. Khi party vua du roi can gom lai, moi member
            # nem NameError -> thread chet -> lop ngoai hieu nham server rot va reconnect vo han.
            _rally_sc = _map_train_dich(pidx, st) or int(sc or 0)
            _rally_tm = (getattr(config, "TRAIN_MAPS", {}) or {}).get(_rally_sc) or {}
            _resolved_rally_safe = _resolve_train_safe(
                c, _rally_sc, list(_rally_tm.get("safe") or []))
            _rl = st.get("rally_point") or _resolved_rally_safe
            if not _rl or c.current_map != _rally_sc:
                return False
            def _da_toi():
                # Member trong party TU DI THEO leader -> pos cua no lac; sua theo leader truoc
                # khi so voi rally (xem `GameClient._theo_leader_sua_pos`).
                try: c._theo_leader_sua_pos()
                except Exception: pass
                p = c.pos
                if not p:
                    try: c.refresh_server_position(_rally_sc, request_timeout=2.0)
                    except Exception: pass
                    p = c.pos
                return bool(p) and (p[0] - _rl[0]) ** 2 + (p[1] - _rl[1]) ** 2 <= RALLY_BAN_KINH ** 2

            def _bao_da_ra():
                """BAO CAO da ra rally. PHAI dat O DAY - moi duong ra rally deu di qua ham nay.

                Truoc day chi ghi `rally_done` o 2 nhanh NGHE LENH (vong chinh + vong cho moi luc
                startup). Member ra rally bang duong KHAC (reform cua chinh no, sau viec vat, sau
                reconnect...) thi KHONG BAO GIO bao cao -> leader thay `rally_done=-1` -> cho het
                45s -> KHONG MOI. Do chinh la co che khien nhieu party khong lap duoc
                (log 30/08 23:30-23:50: 65 lan "CHUA du o diem tap ket", 41 lan ly do
                `chua thi hanh lenh gom (rally_done=-1 < N)`; bucket 23:40 co 0 party du 4/4).
                """
                try:
                    with st["lock"]:
                        c._rally_gen_da_lam = int(st["rally_gen"])
                except Exception:
                    pass

            try:
                if bo_chay:
                    c.flee_mode = True                  # dut tran: BO CHAY, khong danh tiep
                c._wait_combat_clear(idle=2.0, cap=60.0)
                # DUONG TAT "da o rally roi" chi duoc tin khi KHONG dinh tran. `c.pos` la so bot
                # TU NHO (dead-reckoning) va no sai duoc; sai roi thi duong tat nay lam acc KHONG
                # BAO GIO di, pos giu nguyen gia tri sai -> ket vinh vien.
                # O diem an toan that thi khong bi keo vao tran lien tuc, nen "tuong o rally MA
                # dang danh" = mau thuan -> KHONG tin pos, cu di lai cho chac.
                # Log 30/08 23:33 party 3: laochin bao pos=(990,480) [rally], combat=True lien tuc;
                # 3 member kia cung cho, combat=False suot -> thuc te laochin van dung o diem quai.
                if _da_toi():
                    if not c.in_combat(idle_secs=6.0):
                        _bao_da_ra()
                        return True                     # da o diem tap ket roi
                    # DA O RALLY MA CON DINH TRAN -> DANH CHO XONG TAI CHO, TUYET DOI KHONG DI LAI.
                    #
                    # Truoc day o day suy "o safe that thi khong bi keo tran => toa do sai => di
                    # lai cho chac". Suy the la SAI va tu tao vong lap: safe cua bai train chi cach
                    # diem quai vai tram don vi nen dung yen VAN dinh tran; di lai la bang qua vung
                    # quai -> dinh tran moi -> lai "van dinh tran" -> lai di. Moi vong lai keo them
                    # mot bay quai, khong bao gio ket thuc.
                    # Log 31/08 party 8 (10:49:25-50): lubbon va lbumot deu bao "TUONG dang o diem
                    # tap ket (650,2070) ma van dinh tran -> di lai", di xong dinh tran tiep, leader
                    # mat 24s moi giai tan duoc party de doi kenh.
                    # Dung yen danh het bay quai thi tran DUT that; con toa do co sai hay khong thi
                    # vong `for _lan` ben duoi van xu ly duoc o luot sau.
                    log.info("[%s] (%s) %s: da o diem tap ket %s nhung con dinh tran -> DANH XONG "
                             "TAI CHO (khong di lai qua vung quai)", label, role,
                             ly_do or "gom lai", _rl)
                    c._wait_combat_clear(idle=2.0, cap=120.0)
                    if _da_toi() and not c.in_combat(idle_secs=3.0):
                        _bao_da_ra()
                        return True
                log.info("[%s] (%s) %s: ve diem tap ket %s (%s)", label, role,
                         ly_do or "gom lai", _rl,
                         "bo chay" if bo_chay else "party con du -> danh binh thuong")
                for _lan in range(1, 4):
                    c.navigate_to(*_jitter(_rl), flee=bo_chay,
                                  abort=lambda: (_stopped() or not c.running))
                    if _stopped() or not c.running:
                        return False
                    c._wait_combat_clear(idle=2.0, cap=30.0)
                    if _da_toi():
                        _bao_da_ra()
                        return True
                    log.warning("[%s] (%s) %s: di ra %s NHUNG dung lai o %s (tran chien xen ngang?) "
                                "-> di tiep (lan %d/3)", label, role, ly_do or "gom lai", _rl,
                                c.pos, _lan)
                log.warning("[%s] (%s) %s: 3 lan van KHONG toi duoc diem tap ket %s (dang o %s)",
                            label, role, ly_do or "gom lai", _rl, c.pos)
                return False
            except Exception as e:
                log.warning("[%s] (%s) loi ve diem tap ket de gom party: %s", label, role, e)
                return False

        # Coi la "da toi diem tap ket". Can DU RONG de nhan sai so di duong, nhung phai DU CHAT
        # de DIEM QUAI khong lot vao - khong thi acc dung nguyen o bai quai ma bot bao "da ra safe".
        # Can duoi: `navigate_to` coi la toi khi con cach <= NAV_TOI_NOI (60), cong bien do
        # `_jitter` (+-10, cheo ~14) -> 80 la du.
        # Can tren: 200 (cu) LOT diem quai. Log 31/08 party 8 (16:11:51): lenh doi kenh tay, leader
        # lbumot dung o (760,1980) - dung diem quai, `combat=True` lien tuc - ma safe la (650,2070),
        # cach 142 < 200 -> bot bao "da o diem tap ket" roi "DA ra safe -> giai tan party", KHONG
        # HE DI MOT BUOC NAO (user: "an lenh doi kenh no van ko chiu di ve diem safe").
        RALLY_BAN_KINH = 80
        RALLY_CHO_CAP = 45.0      # cho toi da bao lau cho ca party ra safe

        def _vi_sao_chua_san_sang(cli, rally, la_leader=False, ten_acc=None, gen=0):
            """Acc nay CON VUONG gi truoc khi moi duoc vao party; "" = san sang.

            Bon dieu, thieu mot la loi moi roi vao hu khong:
              1. DA NHAN va THI HANH xong lenh gom cua dung vong nay (`rally_done` >= gen),
              2. dang o dung map train + trong ban kinh rally,
              3. khong con dinh tran,
              4. CONG NHAN LOI MOI da mo (`party_invite_ready`).
            Tra LY DO chu khong phai True/False: khong thi log timeout chi ra mot danh sach ten,
            doc xong van khong biet dua nao vuong cai gi.
            """
            if cli is None or not getattr(cli, "running", False):
                return ""                         # khong chay thi khong cho no
            if getattr(cli, "current_map", None) != sc:
                return ""                         # khac map -> viec cua nhanh reform, khong cho o day
            # DA THI HANH lenh gom chua. Khong co cai nay thi "ca lu deu phai di" chi la niem tin:
            # acc ket trong vong startup khong he nghe thay lenh, ma no TINH CO dung gan rally nen
            # leader van ket luan "ca party DA ra diem tap ket" roi moi vao hu khong (party 28,
            # 30/08 16:35-16:36: 3/4 member log "leader GOM LAI PARTY", honagtba khong dong nao).
            if not la_leader:
                # Doc THANG tren client cua no (`_rally_gen_da_lam`), khong qua bang cap party:
                # bot dieu khien acc nen bot BIET no da thi hanh lenh gom nao - khong ai phai khai.
                _da = int(getattr(cli, "_rally_gen_da_lam", -1))
                if _da < int(gen):
                    return "chua thi hanh lenh gom (%d < %d)" % (_da, gen)
            if getattr(cli, "state", None) is not None and cli.state.in_battle:
                return "con dang trong tran"
            # CONG NHAN LOI MOI con dong = moi bao nhieu lan cung chi bi GIU lai ("Chua san sang
            # vao party -> GIU loi moi"), leader dem thieu nguoi roi giai tan party vua lap.
            # Acc VUA RECONNECT phai lam lai viec vat dau phien (dungeon/barrier) truoc khi mo cong.
            # Party 28 (30/08 16:34-16:36): hoangt303 bi server da ra ma 14 (di chuyen QUA XA) lien
            # tuc; moi lan vao lai la cong dong, leader thay no DA dung o rally nen moi luon -> 3/4
            # -> "MAT PARTY giua chung" -> gom lai -> vong lai, ca party troi ra danh le.
            if not la_leader and not getattr(cli, "party_invite_ready", True):
                return "cong nhan loi moi con DONG (dang lam viec vat)"
            # Member trong party TU DI THEO leader (client-side, khong co goi nao) -> pos cua no
            # lac. Sua theo leader truoc khi so voi rally, khong thi leader ket luan "ca party da
            # ra diem tap ket" trong khi member van dung o diem quai (party 3, 30/08 23:33).
            try: cli._theo_leader_sua_pos()
            except Exception: pass
            p = getattr(cli, "pos", None)
            if not p:
                # Chua biet toa do thi XIN LAI tu server (`0x0C 0100` -> self-spawn 0x03), khong
                # ngoi doan. Doan "chua ra" thi cho het 45s vo ich; doan "da ra" thi moi party luc
                # no con dang giua bay quai.
                try:
                    cli.refresh_server_position(sc, request_timeout=2.0)
                except Exception as e:
                    log.debug("[%s] xin lai toa do cua %s loi: %s", label, cli._label, e)
                p = getattr(cli, "pos", None)
                if not p:
                    return "xin lai van khong ro toa do"
            if (p[0] - rally[0]) ** 2 + (p[1] - rally[1]) ** 2 > RALLY_BAN_KINH ** 2:
                return "con o %s, cach rally qua xa" % (tuple(p),)
            return ""

        def _cho_ca_party_ve_rally(ly_do=""):
            """CHO va KIEM CHUNG ca party da ra diem tap ket. True = da ra du (hoac khong can cho).

            Kiem theo TOA DO + het tran + CONG NHAN LOI MOI da mo, doc thang tu client tung acc.
            Khong tin "bao xong la ho da ra": bao xong ma moi ngay thi member con dang o diem quai
            giua tran, hoac vua reconnect xong con dang lam viec vat -> loi moi roi vao hu khong.
            """
            rally = st.get("rally_point") or (train_safes[0] if train_safes else None)
            if not rally:
                return True
            with st["lock"]:
                _gen = int(st["rally_gen"])
            t0 = time.time()
            while time.time() - t0 < RALLY_CHO_CAP:
                if _stopped() or not c.running:
                    return False
                chua = []
                for u, _p, _il, _ip in party_accounts(pidx):
                    _vi = _vi_sao_chua_san_sang(account_clients.get(u), rally, la_leader=_il,
                                                ten_acc=u, gen=_gen)
                    if _vi:
                        chua.append("%s: %s" % (u, _vi))
                if not chua:
                    if time.time() - t0 > 1.0:
                        log.info("[%s] (LEADER) %s: CA PARTY da ra diem tap ket %s (%.0fs) -> moi "
                                 "lai party", label, ly_do or "gom lai", rally, time.time() - t0)
                    return True
                time.sleep(1.5)
            # HET GIO CHO -> VAN MOI. "Chua xac nhan" KHONG dong nghia "chac chan chua ra": member
            # ra rally bang duong khac / dang lam viec vat thi khong bao cao, ma tu choi moi thi
            # party KHONG BAO GIO lap duoc - dung nghia chan cung.
            # (Log 30/08 23:30-23:50: 65 lan chan o day, 41 lan vi `rally_done=-1`; bucket 23:40
            #  co 0 party dat 4/4 trong khi 64 lan "MAT PARTY giua chung".)
            # Loi moi khong toi noi thi cung vo hai - vong sau moi lai.
            log.warning("[%s] (LEADER) %s: qua %.0fs van chua xac nhan du o diem tap ket %s "
                        "-> VAN MOI (khong chan) | %s", label, ly_do or "gom lai", RALLY_CHO_CAP,
                        rally, "; ".join(chua))
            return True

        def _party_tai_cho_xu_ly(ly_do="", ep_doi_kenh=False):
            """CA PARTY dang dung o MAP TRAIN roi -> xu ly TAI CHO, KHONG ve thanh. True = da xu ly.

            User chot 27/08 (ap dung cho CA reform lan luc moi di train):
              - cung map train + cung kenh -> giai tan + lap lai party ngay tai bai, keo ra spot.
              - cung map train + lech kenh  -> chi sync kenh, khong can ve thanh.
            Lech map that su moi ve thanh gom nhau (van la _do_reform nhu cu).
            """
            # DIEU PHOI QUYET CO GOM HAY KHONG - acc chi chon *cach* thi hanh (L1, xem L3o trong
            # documents/RULE_DIEU_PHOI.md: ca party 10, 10/09). Chot GOM thi phai roi xuong
            # `_do_reform()`, khong duoc tu tra `True` = "da xu ly tai cho".
            _kh_now = _ke_hoach(st) or {}
            if _kh_now.get("viec") == VIEC_GOM and not ep_doi_kenh:
                log.info("[%s] (%s) %s: DIEU PHOI chot GOM (%s) -> KHONG tu xu ly tai cho",
                         label, role, ly_do or "dong bo", _kh_now.get("ly_do") or "")
                return False

            def _doc_tinh_hinh():
                _maps, _kenhs = [], []
                for _u, _up, _uil, _uip in party_accounts(pidx):
                    _uc = account_clients.get(_u)
                    if _uc is None or not getattr(_uc, "running", False):
                        continue
                    _maps.append(getattr(_uc, "current_map", None))
                    # HOI LAI SERVER, khong doc so nho san: ket luan "ca party CUNG KENH" bang hai so
                    # nho san la sai suot ma khong ai biet (user kiem chung 30/08: bot hien ca 5 nick
                    # kenh 12, trong game la 12/12/12/2/1). `kenh_that` co cache 10s nen khong ban goi.
                    try:
                        _kenhs.append(_uc.kenh_that())
                    except Exception:
                        _kenhs.append(None)
                return _party_train_tai_cho(_maps, _kenhs, sc)

            _tinh = _doc_tinh_hinh()
            if _tinh == "lech_map":
                return False
            # `ep_doi_kenh`: hai con so kenh KHOP nhau nhung ca party khong thay nhau (khac
            # instance) -> phai di duong "lech_kenh" de CHON KENH MOI, khong duoc tin so khop.
            if ep_doi_kenh:
                _tinh = "lech_kenh"
            # MOI acc (leader lan member) tu dut tran + ve diem tap ket TRUOC khi lap lai party.
            #
            # `_ra_rally_gom_lai` chi di chuyen ACC GOI NO, ma ham nay CHI LEADER chay -> truoc day
            # member dung nguyen o diem quai, quai cu lao vao, moi luc dang trong tran thi loi moi
            # khong toi noi -> party khong bao gio du (party 11, 30/08 15:39-15:52: 4 member dung
            # (940..950, 1080..1100) combat=True lien tuc, leader lap lai party moi 21s suot 13
            # phut, chi luuchin - dua DUY NHAT khong bi quai danh - vao duoc doi).
            # Nen leader BAO CA PARTY ra safe (rally_gen) roi CHO XAC NHAN ho da ra toi noi that.
            if is_leader:
                with st["lock"]:
                    st["rally_gen"] += 1
                log.info("[%s] (LEADER) %s: bao CA PARTY ra diem tap ket truoc khi lap lai party "
                         "(rally_gen=%d)", label, ly_do or "gom lai", st["rally_gen"])
            _ra_rally_gom_lai(ly_do)
            if is_leader and not _cho_ca_party_ve_rally(ly_do):
                # Chua ra toi safe ma moi la moi luc dang trong tran -> loi moi khong toi noi.
                # Tra True = da xu ly tai cho (KHONG ve thanh); vong sau bao lai.
                return True
            if is_leader:
                c.leave_party()          # giai tan party cu de lap lai (van dung o bai train)
                reset_party_joined(pidx)
                st["invited"].clear()
            # DOC LAI tinh hinh: so kenh o tren duoc doc TRUOC khi cho ca party ra rally, ma vong
            # cho do keo dai hang chuc giay - trong lung do moi nguoi doi kenh xong het. Dung so cu
            # la di nham nhanh "lech kenh" du ca party DA cung kenh, roi sync kenh lai TU CHON kenh
            # khac -> keo ca lu di.
            #
            # Log 31/08 party 16: user chon kenh 1; 09:57:33 leader doc tinh hinh (member con o kenh
            # cu) -> "lech_kenh"; 09:57:40 CA 5 acc `Doi kenh OK -> 1`; 09:57:41 van bao "LECH KENH
            # -> chi sync kenh tai cho" -> 09:58:22 picker chon kenh 2, ca party bo kenh 1.
            if not ep_doi_kenh:
                _tinh_moi = _doc_tinh_hinh()
                if _tinh_moi != _tinh:
                    log.info("[%s] (%s) %s: doc lai sau khi gom o diem tap ket -> tinh hinh doi "
                             "'%s' => '%s'", label, role, ly_do or "dong bo", _tinh, _tinh_moi)
                    _tinh = _tinh_moi
                if _tinh == "lech_map":
                    return False
            if _tinh == "lech_kenh":
                log.warning("[%s] (%s) %s: ca party DA o map train %s nhung LECH KENH -> chi sync "
                            "kenh tai cho, KHONG ve thanh", label, role, ly_do or "dong bo", sc)
                # KHONG bump `resync_gen` o day: day la acc TU QUYET bat ca party roi doi. Ca lech
                # kenh nay DIEU PHOI da phu (`VIEC_DONG_BO` -> `resync_gen` o `_dieu_phoi_gom`),
                # va no co cooldown + chot "nguoi khac vua bump thi im".
                if not do_channel_sync():
                    # Hong vi LECH MAP thi phai GOM MAP, khong phai thu lai sync tai cho (L3o).
                    if not _cung_map_ca_party(pidx):
                        log.warning("[%s] (%s) %s: sync kenh hong vi LECH MAP -> khong thu lai tai "
                                    "cho nua, di GOM MAP", label, role, ly_do or "dong bo")
                        return False       # -> duong reform/gom cua caller
                    # Sync kenh CHUA XONG (co acc chua sang duoc, vd kenh vua day) -> KHONG duoc
                    # moi party: moi luc dang lech kenh la loi moi khong toi noi, party mai khong
                    # du. Van tra True = da xu ly tai cho, KHONG ve thanh; do_channel_sync da bump
                    # reform_gen nen keepalive se quay lai day dong bo tiep.
                    log.warning("[%s] (%s) %s: sync kenh CHUA xong -> chua moi party, dong bo lai",
                                label, role, ly_do or "dong bo")
                    _bat_danh_neu_du_party()
                    return True
            else:
                log.info("[%s] (%s) %s: ca party DA o map train %s va CUNG KENH -> lap lai party "
                         "tai cho, KHONG ve thanh", label, role, ly_do or "dong bo", sc)
            if is_leader:
                st["invited"].set()
                try:                      # MOI LAI NGAY, khong doi vong retry 60s cua keepalive
                    _invite_party_participants(c, train_on_map, gap=1.0)
                except Exception as e:
                    log.warning("[%s] (LEADER) %s: loi moi lai party tai cho: %s",
                                label, ly_do or "dong bo", e)
            _bat_danh_neu_du_party()
            return True

        via_route = False   # True neu toi train map bang KEO PARTY -> da cung kenh + da danh dungeon o thanh
        if train_on_map:
            # PHAI dung map login (toa do safe/mobs chi dung tren map do).
            self_map_ok = (
                login_map == sc
                and not _needs_train_safe_bootstrap(login_map, sc, train_safes)
            )
            def _quit():
                # NGUYEN TAC TOI THUONG: DU FULL PARTY MOI TRAIN -> n_members KHONG BAO GIO tru. Member
                # thoat (rot server / van / het gio) thi leader DUNG CHO no ve, KHONG keo le. Truoc day
                # tru n_members khi thoat -> rot server tru -> n_members ve 0 -> leader keo 1 minh (bug).
                try: c.close()
                except Exception: pass
                if c in _clients: _clients.remove(c)
            # (XOA 13/09 `_daily_then_quit`: sai map khong con la ly do de acc tu thoat - ca hai
            # nhanh leader/member deu ra vong chinh cho dieu phoi quyet, nen khong ai goi no nua.)
            # PARTY-LEVEL: chi can 1 acc sai map -> CA PARTY (ke ca dua DANG O BAI) ve thanh don nhau,
            # lap party du, KEO ca party toi train map + ra safe gan quai (member tu theo). Dung
            # CHUNG _do_reform(to_spot=False) voi nhanh "dang chay lech map" (keepalive) - truoc day
            # la 1 khoi code RIENG, DUOC ~150 dong trung logic voi _do_reform -> khi sua 1 cho (rally
            # truoc khi ra spot) de sot cho kia, gay bug "di thang xuyen tuong tu cong ra spot".
            route = getattr(config, "TRAIN_ROUTES", {}).get(sc)
            _startup_safe = st.get("rally_point") or (
                train_safes[0] if train_safes else None
            )
            smart_route = None
            # Co lenh dieu phoi MOI giua chung setup -> bo phan con lai, xuong vong chinh
            # thi hanh (xem `_wait_for_rally`).
            _ra_vong_chinh_nghe_lenh = False
            # KHONG CO BOT-LEADER thi MEMBER phai tu dung duoc duong: khong ai keo no ca.
            #
            # Ban cu chi build khi `is_leader`, nen voi party khong dat leader thi
            # `route_available` rut gon con dung `has_leader` = False -> member sai map roi vao
            # nhanh "route-less -> TAT CA PARTY".
            # Ca that (APK, user 11/09 - "den doan chay trainmap thi bao loi gi do va tat acc luon"):
            #   16:23:00 [acc2] (member) route-less + SAI MAP (o 12001, can 21864) -> TAT CA PARTY
            #   16:23:00 [acc2] STOP: route-less train + member sai map
            # Map 21864 khong co `route` cung trong train_maps.json, ma smart route thi khong ai
            # dung cho member -> tuong la khong co duong, trong khi smart router hoan toan build
            # duoc. Party CO leader khong bao gio dinh -> dung kieu "khong phai luc nao cung bi".
            if is_leader or not has_leader:
                try:
                    if getattr(config, "SMART_WORLD_ROUTING", True):
                        smart_route = c.build_smart_route(sc, _startup_safe)
                except Exception as e:
                    log.warning("[%s] startup: loi build smart route: %s", label, e)
            route_available = _train_route_available(smart_route, route, has_leader)
            if route_available and has_leader:
                expected = len(party_accounts(pidx))
                all_on_map = _party_map_barrier(st, username, self_map_ok, expected, _stopped,
                                                pidx=pidx, train_map=sc)
                if not all_on_map and _party_tai_cho_xu_ly("luc di train"):
                    # Co acc sai map, nhung xu ly TAI CHO duoc (ca party dang o bai train, chi lech
                    # kenh) -> khong ve thanh.
                    self_map_ok = (c.current_map == sc)
                    all_on_map = True
                if not all_on_map:
                    log.info("[%s] (%s) PARTY co acc sai map -> CA PARTY ve thanh don nhau roi KEO toi %s"
                             " (dung _do_reform, dung o safe - chua ra spot)", label, role, sc)
                    # DANG DANH thi KHONG duoc reform: viec dau tien cua reform la teleport, ma
                    # battle NUOT lenh -> lap "Ve thanh ... (lap lai neu con battle chan teleport)"
                    # vo tan (bug that 17:10-17:13). Cho moc ket tran THAT roi hang.
                    if c.state.in_battle:
                        log.info("[%s] (%s) dang trong tran -> danh xong roi moi reform", label, role)
                        c._wait_combat_clear()
                    _do_reform(to_spot=False)
                    c._san_sang_party = False
                    if c.current_map == sc:
                        self_map_ok = True; login_map = sc; via_route = True
                        log.info("[%s] (%s) da toi train map %s qua reform (dang o safe)", label, role, sc)
            if is_leader:
                if not self_map_ok and not c.running:
                    # LEADER MAT KET NOI (vd disconnect giua route) -> KHONG phai "sai map".
                    # Neu SERVER ROT (server_closed) -> supervisor SE reconnect leader -> KHONG set
                    # leader_bad (set = giet het member ngay); member CHO trong vong 150s ben duoi,
                    # leader reconnect + route lai + set leader_ok -> member tiep tuc. Chi set
                    # leader_bad khi KHONG reconnectable (bo cuoc that su).
                    if not _stopped() and not getattr(c, "server_closed", False):
                        c.server_closed = True
                    _srv_drop = getattr(c, "server_closed", False)
                    _reason("leader MAT KET NOI khi dang route toi train map (map cuoi %s)" % c.current_map)
                    log.warning("[%s] (LEADER) MAT KET NOI khi route toi train map %s -> %s",
                                label, sc, "supervisor reconnect, member CHO" if _srv_drop else "ca party thoat")
                    if not _srv_drop:
                        st["leader_bad"].set()
                    try: c.close()
                    except Exception: pass
                    if c in _clients: _clients.remove(c)
                    return
                if not self_map_ok:
                    if not route_available:
                        # KHONG TAT PARTY. Acc khong duoc tu quyet tat ca party - do la quyet dinh
                        # nang nhat trong file, ma can cu chi la tinh trang cua RIENG no ("minh sai
                        # map, minh khong dung duoc duong").
                        # Sai map la TRANG THAI: dieu phoi doc map ca party moi 2 giay, thay lech
                        # thi ra lenh gom. Con tat party thi user phai vao bat lai bang tay.
                        log.warning("[%s] (LEADER) SAI MAP (o %s, can %s) va khong dung duoc duong "
                                    "-> KHONG tat party, de dieu phoi quyet",
                                    label, c.current_map, sc)
                        _reason("leader sai map (o %s, can %s), khong co duong -> dieu phoi quyet"
                                % (c.current_map, sc))
                    # LEADER SAI MAP -> KHONG CO XU LY RIENG. Ra thang vong chinh, y het nhanh
                    # member ben duoi.
                    #
                    # XOA (13/09) vong `while c.running: _resync_ck + _do_reform + sleep(5)`. No la
                    # ban sao cuoi cung cua "acc tu quyet" ma nhanh member da bi xoa 11/09: acc tu
                    # chon buoc vao mot vong rieng, va trong do no DIEC voi moi lenh dieu phoi.
                    #
                    # Ca that party 48, 13/09 (user: "Party 48 ko thay lap pt"):
                    #   10:41:11 [party 48] viec=moi - DOI chua du (dt901=0 ... dt905=0)
                    #   10:41:11 [party 48] DIEU PHOI: ... -> LAP LAI PARTY
                    #   10:42:05 [dt901] Da ve thanh 18021
                    #   10:42:05..10:47:51 [dt901] KHONG o party nao ... KHONG gui 013-004
                    #                              (5 giay MOT DONG, khong dut)
                    # dt901 la LEADER. No ket trong vong nay, cu 5 giay `_do_reform` mot lan -> moi
                    # lan teleport lai `leave_party()` -> khong bao gio chay toi doan GUI LOI MOI.
                    # Dieu phoi ra lenh moi party deu dan; khong ai thi hanh -> "ko thay lap pt".
                    # Truoc do party 48 da o trang thai THIEU NGUOI (1/4) tu 07:56 den 10:30, watcher
                    # ep dong bo hang chuc lan vo ich - cung vi ly do nay.
                    #
                    # GIO: sai map la mot TRANG THAI, khong phai mot viec. Dieu phoi doc map ca party
                    # moi 2 giay va tu ra lenh gom; vong chinh thi hanh. KHONG set leader_bad (leader
                    # van song), va VAN set `leader_ok` ben duoi -> member khong treo cho.
                    _reason("leader sai map (o %s, can %s) -> ra vong chinh, dieu phoi quyet"
                            % (c.current_map, sc))
                    log.warning("[%s] (LEADER) SAI MAP (o %s, can %s) -> KHONG tu xu ly, ra vong "
                                "chinh de nghe lenh dieu phoi", label, c.current_map, sc)
                st["leader_ok"].set()   # leader ok -> member duoc tiep tuc
            else:
                if not self_map_ok and not c.running:
                    _reason("member MAT KET NOI khi dang route (map cuoi %s)" % c.current_map)
                    log.warning("[%s] (member) MAT KET NOI khi dang di chuyen toi train map -> thoat.", label)
                    _quit(); return
                if not self_map_ok:
                    if not route_available:
                        # KHONG TAT PARTY - xem ghi chu o nhanh LEADER tren. Day la dong da tat acc
                        # cua user tren APK (11/09):
                        #   16:23:00 [acc2] (member) route-less + SAI MAP (o 12001, can 21864)
                        #                   -> TAT CA PARTY
                        #   16:23:00 [acc2] STOP: route-less train + member sai map
                        log.warning("[%s] (member) SAI MAP (o %s, can %s) va khong dung duoc duong "
                                    "-> KHONG tat party, de dieu phoi quyet",
                                    label, c.current_map, sc)
                        _reason("member sai map (o %s, can %s), khong co duong -> dieu phoi quyet"
                                % (c.current_map, sc))
                    # MEMBER SAI MAP -> KHONG CO XU LY RIENG. Ra thang vong chinh.
                    #
                    # XOA (11/09) ca khoi "cho leader keo qua route": no la mot vong acc TU CHON
                    # buoc vao, va trong do no DIEC voi moi lenh cap party (chi doc `leader_gone` /
                    # `leader_bad` - hai co do luong leader ghi). Han dai hay ngan, co loi ra hay
                    # khong, deu khong phai van de: van de la acc tu quyet trang thai cua minh.
                    #
                    # Ca that party 7, 11/09 (user: "SAI MAP, cho leader keo -> acc van quyet dinh
                    # a" -> "xoa acc tu quyet, dieu phoi la nguoi quyet dinh, va va cai lon"):
                    #   10:31:36 [ttmuoi] (member) SAI MAP (o 21001, can 21814) -> vao vong cho
                    #   ... nam trong do MOT TIENG RUOI ...
                    #   11:58:26 [party 7] gen 4: viec=gom -> REFORM gen -> 2   <- khong nghe thay
                    #   11:59:17 [ttmuoi] L0: DIEU PHOI ra lenh 'gom' -> DUNG viec chinh
                    #   ... van dung im, khong mot buoc di chuyen ...
                    #
                    # GIO: sai map la mot TRANG THAI, khong phai mot viec. Dieu phoi doc map ca
                    # party moi 2 giay - no THAY acc nay lech va tu ra lenh gom; vong chinh thi
                    # hanh. Acc khong phai lam gi ca, va quan trong hon: no o trong vong chinh nen
                    # NGHE duoc moi lenh.
                    _reason("member sai map (o %s, can %s) -> ra vong chinh, dieu phoi quyet"
                            % (c.current_map, sc))
                    log.warning("[%s] (member) SAI MAP (o %s, can %s) -> KHONG tu xu ly, ra vong "
                                "chinh de nghe lenh dieu phoi", label, c.current_map, sc)
                # CO bot-leader -> doi leader quyet dinh (ok/huy). KHONG co leader -> tu di tiep.
                if has_leader:
                    # KHONG CHO leader "quyet dinh" nua. Cho o day la CHO VO HAN (ban cu ghi thang
                    # trong chu thich), chi thoat khi Stop / tu rot - tuc khong co loi ra cap party.
                    # Vi pham L9, va no khoa cheo voi chinh nhanh reform cua leader:
                    #   12:36:15..13:20:31 (44 PHUT, 327 luot) party 1 (07/09)
                    #     4 acc: reform: CHO ca party ve Trường Sa (4/5) - THIEU: brubb46677
                    #            [map=23001, da ve Trường Sa, cho ca party (37s)]
                    #     brub : (member) CHO leader quyet dinh (leader co the dang reconnect)...
                    # CA NAM DUA DEU DA O TRUONG SA (23001). brub cho leader; leader cho brub; con
                    # so "(37s)" dung im 44 phut vi brub ket trong vong nay nen khong cap nhat gi
                    # nua. User: "cho cho cai lon gi nua the, bot phai quyet dinh het chu".
                    #
                    # Gio doc MOT PHAT roi di tiep: `leader_bad` la lenh HUY that su (leader sai map
                    # / mat ket noi) nen van phai theo; con "chua quyet dinh" thi khong phai ly do de
                    # dung im - vong chinh + dieu phoi (reform_gen / VIEC_GOM) lo viec gom.
                    if st["leader_bad"].is_set():
                        _reason("leader party loi (sai map hoac mat ket noi) -> ca party bi huy")
                        log.warning("[%s] (member) LEADER party LOI (sai map / mat ket noi - xem dong "
                                    "LEADER o tren) -> ca party huy -> THOAT.", label)
                        _quit(); return
            # --- MAP-TRAIN: CA PARTY ve cung 1 SAFE = safe GAN diem quai leader chon (de gan nhau
            #     -> member vao tran chung voi leader). Leader chon diem quai SOM + bao rally_point. ---
            configured_mobs = [tuple(point) for point in tm.get("mobs", [])]
            mobs = configured_mobs
            if is_leader:
                learned_safe = _capture_arrival_safe(
                    c, sc, came_from_other_map=via_route
                )
                if learned_safe is not None:
                    train_safes[:] = [learned_safe]
                if not train_safes:
                    # KHONG TAT CA PARTY. Day la loi DU LIEU cua map (chua hoc duoc safe), khong
                    # phai ly do de tat nhung acc khac - chung van dang o day va van nghe lenh.
                    # `leader_bad` van bat de dieu phoi biet leader nay khong dung duoc.
                    st["leader_bad"].set()
                    _reason("khong lay duoc safe sau warp vao map %s" % sc)
                    log.warning("[%s] (LEADER) khong lay duoc safe sau warp vao map %s -> acc nay "
                                "dung, KHONG tat ca party (dieu phoi quyet tiep)", label, sc)
                    _quit(); return
                mobs = _resolve_train_mob_centers(c, sc, tm, stop=_stopped)
                learned_safes = [tuple(map(int, point)) for point in
                                 (tm.get("safe", []) or []) if len(point) == 2]
                if learned_safes:
                    train_safes[:] = learned_safes
                if mob_index < 0 and mobs:
                    # KHONG `import random` o day: no bien `random` thanh BIEN CUC BO cua CA HAM
                    # run_account -> moi cho dung random TRUOC dong nay (vd retry login loi 1 o
                    # dau ham) deu nem UnboundLocalError. Module da `import random` san o dau file.
                    spot = random.choice(mobs)
                else:
                    spot = mobs[mob_index] if (mobs and 0 <= mob_index < len(mobs)) else (mobs[0] if mobs else None)
                st["mob_spot"] = spot
                # BAO TRANG THAI cho dieu phoi: map train dich cua party nay la map nao. Day la
                # BAO, khong phai quyet dinh - dieu phoi doc no de biet ca party da toi noi chua
                # (`_viec_di_train`). Khong co so nay thi no khong the ra lenh DI TRAIN / RA QUAI.
                st["train_map_dich"] = int(sc)
                _set_train_block_stats_spot(spot, enabled=False)
                # CO PATH capture (diem quai XA) -> sau khi lap party leader follow_path keo ca party
                # ra spot; KHONG path -> navigate thang. DU CO PATH HAY KHONG, rally LUON la SAFE gan
                # spot (tap trung + lap party o day TRUOC), KHONG phai spot (truoc set =spot -> ca party
                # navigate thang ra spot luc chua co party -> vo ich, roi lai quay ve safe).
                path = (getattr(config, "MOB_PATHS", {}).get(sc, {}).get(tuple(spot))
                        if spot and tuple(spot) in configured_mobs else None)
                st["mob_path"] = path
                st["rally_point"] = (
                    _nearest_safe(spot, train_safes) if spot else train_safes[0]
                )
                st["rally_ready"].set()
            # member: cho leader chon (rally_point/path); khong co leader -> safe[0]
            if has_leader and not is_leader:
                if not _wait_for_rally(st["rally_ready"], _stopped, lambda: c.running,
                                       st=st, label=label, pidx=pidx):
                    if _stopped() or not c.running:
                        _quit(); return
                    # CO LENH MOI -> bo doan setup con lai (ve rally / ra spot), xuong thang VONG
                    # CHINH de thi hanh. KHONG `_quit()` (do la tat acc), va cung khong chay tiep
                    # setup bang du lieu da cu.
                    _ra_vong_chinh_nghe_lenh = True
                else:
                    _set_train_block_stats_spot(st.get("mob_spot"), enabled=False)
            # MAP-TRAIN: CA party (leader+member) ve RALLY = safe GAN spot TRUOC. KHONG follow_path
            # ngay luc nay - vi party CHUA lap (member chua join) -> keo cung vo ich (member khong bi
            # keo theo, leader chay ra spot 1 minh roi quay ve). Sau khi LAP PARTY xong, _start_training
            # moi cho leader follow_path KEO CA PARTY (da join, dang o rally) ra spot.
            rally = st.get("rally_point") or (
                train_safes[0] if train_safes else None
            )
            if rally is None and not _ra_vong_chinh_nghe_lenh:
                _reason("khong co safe de tap ket tren map %s" % sc)
                _quit(); return
            if not _ra_vong_chinh_nghe_lenh:
                log.info("[%s] (%s) MAP-TRAIN map=%s -> ve safe tap ket chung %s (lap party TRUOC, keo ra spot SAU)",
                         label, role, sc, rally)
            # `c.navigate_to(*_jitter(rally))` tran (khong flee, khong kiem lai) la SAI hai duong:
            #   - flee_mode sau login = False -> VUA DI VUA DANH tung bay quai tren duong,
            #   - navigate_to bi tran chien xen ngang thi ve giua duong ma KHONG bao loi -> acc
            #     dung o giua bai, tuong da o safe. Leader dem "ca party da ra diem tap ket" ->
            #     moi party luc no con giua bay quai -> loi moi roi vao hu khong.
            # `_ra_rally_gom_lai` bo chay + doc lai toa do + thu lai 3 lan roi moi ket luan.
            if not _ra_vong_chinh_nghe_lenh and not _ra_rally_gom_lai("login"):
                log.warning("[%s] (%s) login: CHUA ra duoc diem tap ket %s -> keepalive se gom lai",
                            label, role, rally)
            # SOLO daily dungeon o MAP-TRAIN: TAM TAT (het luot -> bi dump ve 12000, pha map-train;
            # Bat/tat bang checkbox "Danh daily dungeon" cua party (do_daily).
            # via_route -> da danh dungeon o thanh roi, BO QUA (khoi pha map-train + cho barrier).
            _rg_base = st["reform_gen"]   # moc reform_gen TRUOC dungeon: bi DUMP trong dungeon -> tang -> skip keo ra spot
            if do_daily and not via_route and not _ra_vong_chinh_nghe_lenh:
                with st["lock"]:
                    st["started_train"] += 1
                try:
                    c.do_daily_dungeon()
                except Exception as e:
                    log.warning("[%s] loi daily dungeon (bo qua): %s", label, e)
                for _ in range(15):
                    if c.current_map == sc:
                        break
                    time.sleep(1)
                if c.current_map != sc:
                    # Bi dungeon DUMP ra sanh (12000)/thanh -> KHONG bo roi, KHONG bat no chay le 1
                    # minh ve. Bump reform_gen -> CA PARTY se reform (ve thanh DON no) o keepalive
                    # ben duoi. Van +dungeon_done de barrier dungeon khong treo cho member nay.
                    log.warning("[%s] (%s) sau dungeon BI DUMP ra %s -> de DIEU PHOI ra lenh gom",
                                label, role, c.current_map)
                    with st["lock"]:
                        st["dungeon_done"] += 1
                else:
                    # VE RALLY (safe GAN mob spot leader chon), KHONG phai safe[0] co dinh: truoc day
                    # ve tm["safe"][0] -> member ket o safe[0] xa mob spot -> KHONG bi keo vao tran
                    # party cua leader -> leader danh 1 minh, member dung yen (bug thuc te: map co
                    # nhieu safe/mob, safe[0] khong gan mob leader dang danh).
                    c.navigate_to(*_jitter(rally))
                    with st["lock"]:
                        st["dungeon_done"] += 1
                log.info("[%s] (%s) xong dungeon -> cho ca party (%d/%d)...",
                         label, role, st["dungeon_done"], st["started_train"])
                _t0 = time.time()
                while True:   # CHO VO HAN cho ca party xong dungeon (reconnecting cong vao, khoi deadlock)
                    _nghe_lenh_kenh()   # lenh dieu phoi phai nghe duoc o MOI vong cho
                    if _stopped() or not c.running: _quit(); return
                    _resync_ck(st, username)   # ep dong bo -> relogin bam leader
                    with st["lock"]:
                        if (st["started_train"] > 0 and
                                st["dungeon_done"] + len(st["reconnecting"]) >= st["started_train"]):
                            break
                    if time.time() - _t0 > 30:
                        log.info("[%s] (%s) CHO ca party xong dungeon (%d/%d, reconnecting=%d)...",
                                 label, role, st["dungeon_done"], st["started_train"], len(st["reconnecting"]))
                        _t0 = time.time()
                    time.sleep(1)
                log.info("[%s] (%s) ca party xong dungeon", label, role)
            if not via_route:   # via_route -> ca party da cung kenh (di theo) -> khoi sync lai
                do_channel_sync()   # map-train: dong bo kenh sau khi ve safe (tren map thuong)
        elif is_digioi:
            # --- DI GIOI ---
            _used_ho_phu_at_login = False
            if pcfg.get("use_digioi_ho_phu"):
                _ho_phu_busy = c.in_combat()
                try:
                    _used_ho_phu_at_login = _maybe_use_di_gioi_ho_phu("login")
                except Exception as e:
                    log.warning("[%s] loi dung Di Gioi Ho Phu luc login (bo qua): %s", label, e)
                if not _ho_phu_busy:
                    next_ho_phu = time.time() + HO_PHU_CHECK_SEC   # check lai moi 3 phut
            # 0) PRE-CHECK: doc so phut DG hom nay tu BANG STAT login (0x55 id=0x1b).
            #    Da du gio (>= DIGIOI_LIMIT) -> KHOI vao (truoc day phai vao -> cho 150s moi biet).
            if (_used_ho_phu_at_login and not c.in_di_gioi()
                    and c.digioi_minutes >= DIGIOI_LIMIT):
                log.info("[%s] Da gui Di Gioi Ho Phu luc login nhung timer van %d/%d -> doi them",
                         label, c.digioi_minutes, DIGIOI_LIMIT)
                deadline = time.time() + 8.0
                while time.time() < deadline and c.digioi_minutes >= DIGIOI_LIMIT:
                    time.sleep(0.5)
            if not c.in_di_gioi() and c.digioi_minutes >= DIGIOI_LIMIT:
                log.info("[%s] (%s) DG da HET GIO hom nay (%d/%d phut, doc tu login) -> khong vao",
                         label, role, c.digioi_minutes, DIGIOI_LIMIT)
                _reason("het gio Di Gioi hom nay (doc tu login)")
                if dt_mode:
                    _finish_digioi_train_after_dg()
                    _ket_thuc_pha_dg()
                    return
                # HET GIO DG -> BAY VE THANH (Trac Quan) TRUOC: login co the o map quai (12831...) ->
                # ket tran lien tuc -> teleport boss/dungeon luc dang danh bi server KICK. Ve thanh
                # an toan roi moi lam dailies.
                _go_town_safe(c, label)
                _maybe_auto_world_boss("het gio DG luc login, truoc pho ban doi")
                if auto_team_dungeon:
                    if (not _run_auto_team_dungeons_if_needed(c, st, username, label, pidx,
                                                              is_leader, _stopped, pcfg)
                            and _pb_that_bai_co_phai_dung_han(c, _stopped, label, role)):
                        try: c.close()
                        except Exception: pass
                        if c in _clients: _clients.remove(c)
                        return
                if do_daily:
                    try: c.do_daily_dungeon()
                    except Exception as e:
                        log.warning("[%s] loi daily dungeon (bo qua): %s", label, e)
                # khong vao DG -> lam FULL nhiem vu (nhe + boss) tai cho roi dong
                if do_daily:
                    try: c.claim_daily_quests(heavy=True)
                    except Exception as e:
                        log.warning("[%s] loi claim daily quest (bo qua): %s", label, e)
                # NGUYEN TAC TOI THUONG: DU FULL PARTY MOI LAM -> KHONG tru n_members (het gio DG cung
                # ko tru; leader dung cho, ca party dung yen neu thieu - theo yeu cau).
                try: c.close()
                except Exception: pass
                if c in _clients: _clients.remove(c)
                return
            # CAP QUAI DG PHAI DOC LAI NGAY TRUOC KHI VAO - khong dung so chot hoi login.
            #
            # `c.di_gioi_level` duoc gan mot lan luc login: acc nao chay qua do khi party CHUA du
            # level thi `_auto_dg_level` tra None va no giu MAC DINH cap 25 vinh vien, con acc login
            # sau (da chot xong) thi vao dung cap that. Cap khac nhau = KHU DG khac nhau, va hai khu
            # thi khong bao gio sync kenh / lap party duoc.
            #
            # Ca that 08/09 party 26 (user: "vao DG nhung ko thay dong bo kenh va lap pt"):
            #   01:04:03 >>> PARTY 26: TU CHON CAP QUAI DG -> cap 130 (level party [143..177])
            #   01:04:10 [mhmot] Vao Di Gioi: gui 0x61 02 00 02 (cap 25)   -> map 49942
            #   01:04:12 [mhhai] ... (cap 25)                              -> map 49942
            #   01:04:17 [mhnam] Vao Di Gioi: gui 0x61 02 00 0a (cap 130)  -> map 49951
            #   04:21:11 [mhmot] (LEADER) BO QUA sync kenh: party dang o KHAC MAP [49942, 49951]
            # Leader bo qua sync kenh vi "khac map", ma khac map thi khong bao gio het -> ca party
            # treo trong DG khong danh tran nao. Chot cap luc 01:04:03 la DUNG; cai sai la bon vao
            # sau khong doc lai.
            try:
                _dgp2 = pcfg.get("di_gioi_pick") or ""
                if _dgp2 in train_pick.PICK_KEYS:
                    _dg_lv2 = _doc_cap_dg(pidx)           # CHI DOC - dieu phoi chot (L1)
                    if _dg_lv2 and int(_dg_lv2) != int(getattr(c, "di_gioi_level", 0) or 0):
                        log.info("[%s] (%s) cap quai DG cua party da chot = idx %s (minh dang giu "
                                 "%s) -> DOI THEO party",
                                 label, role, _dg_lv2, getattr(c, "di_gioi_level", None))
                        if c.in_di_gioi():
                            # DA o trong DG (khu sai) -> doi cap NGAY TAI CHO, khong phai vao lai:
                            # `set_di_gioi_level` gui dung `0x61 02 00 [idx]` va server chuyen khu
                            # (capture digioi_level_select_20260721.pcap).
                            c.set_di_gioi_level(int(_dg_lv2))
                        else:
                            c.di_gioi_level = int(_dg_lv2)
            except Exception as e:
                log.warning("[%s] loi doc lai cap quai DG (bo qua): %s", label, e)
            # 1) PHAI VAO DUOC DG TRUOC (xac nhan in_di_gioi) roi MOI chuyen kenh.
            while (not c.in_di_gioi() and not c.enter_di_gioi_safe()
                   and c.running and not _stopped()):
                # KHONG DUOC DOAN "het gio" ROI TAT ACC. `enter_di_gioi_safe` tra False cho MOI ly
                # do, ke ca "server khong tra loi gi" - do la ly do PHAI THU LAI, khong phai bang
                # chung het gio. L0 chi cho ngung vi ba le: het gio KHACH QUAN · user Stop · acc da
                # tat han.
                #
                # Quet log 08/09 (user: "tu check xem co party nao co van de ko"): 92 acc bi TAT,
                # trong do 53 lan ly do that su la:
                #   06:32:26 [vusau] VAO DI GIOI THAT BAI sau 12 lan (map=12001, combat=False,
                #                    S:097-001=khong nhan duoc)
                #   06:32:35 [vusau] (member) khong vao duoc DG (het gio?) -> TAT acc nay
                # Server IM LANG, khong he noi het gio. Ma bot BIET gio that: `digioi_minutes_live()`
                # (dong log "Di Gioi con lai: 1h48m" ngay ben duoi dung chinh no) - va `S:097-001`
                # ma 2 <時間已滿> moi la cau tra loi "het gio" cua server.
                _ma_dg = getattr(c, "_dg_enter_result", None)
                try:
                    _con_dg = max(0, int(DIGIOI_LIMIT - c.digioi_minutes_live()))
                except Exception:
                    _con_dg = None
                _het_that = (_ma_dg in (1, 2)) or (_con_dg is not None and _con_dg <= 0)
                if not _het_that:
                    log.warning("[%s] (%s) chua vao duoc DG (S:097-001=%s, con %s phut) -> KHONG "
                                "tat acc, thu lai vong sau",
                                label, role, _ma_dg if _ma_dg is not None else "khong tra loi",
                                _con_dg if _con_dg is not None else "?")
                    time.sleep(10)
                    continue          # vong `while` ngay tren -> thu vao DG lai
                log.warning("[%s] (%s) HET GIO DG that (S:097-001=%s, con %s phut) -> TAT acc nay",
                            label, role, _ma_dg, _con_dg)
                if dt_mode:
                    _finish_digioi_train_after_dg()
                    _ket_thuc_pha_dg()
                    return
                _go_town_safe(c, label)   # ve thanh truoc (thoat o quai) roi lam dailies
                _maybe_auto_world_boss("khong vao duoc DG, truoc pho ban doi")
                if auto_team_dungeon:
                    if (not _run_auto_team_dungeons_if_needed(c, st, username, label, pidx,
                                                              is_leader, _stopped, pcfg)
                            and _pb_that_bai_co_phai_dung_han(c, _stopped, label, role)):
                        try: c.close()
                        except Exception: pass
                        if c in _clients: _clients.remove(c)
                        return
                if do_daily:
                    try: c.claim_daily_quests(heavy=True)   # khong vao DG -> lam full quest roi dong
                    except Exception as e:
                        log.warning("[%s] loi claim daily quest (bo qua): %s", label, e)
                # NGUYEN TAC TOI THUONG: DU FULL PARTY MOI LAM -> KHONG tru n_members.
                try: c.close()
                except Exception: pass
                if c in _clients: _clients.remove(c)
                return
            # 2) DA o trong DG an toan -> lam nhiem vu NHE (gacha/hop + claim hang/cot du) TRUOC khi
            #    dong bo kenh. Gacha/hop KHONG di chuyen nen an toan trong DG; lam xong moi sync kenh
            #    + lap party. KHONG lam o NANG (boss teleport se van ra khoi DG) -> de SAU khi het gio DG.
            if do_daily:
                c.claim_daily_quests(heavy=False)
            # 2b) KET BAN nguoi xung quanh trong DG (DG dong nguoi) -> gom du 50 ban. CHI login moi +
            #     khi con < 50 ban (max game). Lam TRUOC sync kenh (yeu cau user). Player quanh minh
            #     lay tu 0x03 PlayerAppear (da nhan luc vao DG + lam daily nhe o tren).
            if not is_reconnect:
                try:
                    c.befriend_nearby()
                except Exception as e:
                    log.warning("[%s] loi ket ban xung quanh (bo qua): %s", label, e)
            # 3) Dong bo kenh (gom ca party ve cung instance DG). Doi kenh trong DG VAN o trong DG.
            #    SOLO -> moi acc chay rieng, KHONG can chung kenh voi ai -> bo qua.
            if pcfg.get("digioi_mode") != "solo":
                do_channel_sync()
        elif mode == "event":
            # --- EVENT: moi nick vao map rieng khi chua co party. Event 40NPC co bot leader se
            # lap party SAU KHI tat ca da vao map; event khac/no-leader van dung yen cho moi tay. ---
            _evs = getattr(config, "EVENTS", {}) or {}
            # `event_hom_nay` ap tham so theo THU (loan dau t3 va t7 khac map/select/option NPC).
            ev = config.event_hom_nay(pcfg.get("event_key") or "")
            if ev is None and _evs:
                # event_key thieu/None/sai (vd config luu truoc khi co picker) -> fallback event DAU
                # TIEN (tien khi chi co 1 event). User chon dung event trong GUI + luu lai la het.
                _k = next(iter(_evs)); ev = _evs[_k]
                log.info("[%s] (%s) mode event: event_key='%s' khong hop le -> dung event dau '%s' (%s)",
                         label, role, pcfg.get("event_key"), _k, ev.get("label"))
            # Sync kenh TRUOC go_to_event: CHI cho event STAND (moi tay). Event PARTY (40NPC) sync
            # LAI SAU khi vao map event (xem duoi) -> KHONG sync o day nua de tranh doi kenh 2 LAN
            # moi vong -> giam churn (leader nhay kenh lien tuc + relogin ca party -> server kick).
            # 2K DA KET THUC (thua/xong) -> TUYET DOI khong gom doi/vao lai nua: ra khoi thap roi
            # THOAT GAME. Cua kiem tra o vong keepalive KHONG du: leader (va acc vua relogin) chay
            # LAI nhanh event nay TRUOC khi toi keepalive, roi vao vong "gom doi" va di bo mai trong
            # thap trong khi cac nick khac da ra Quang Truong + thoat (bug that 16:02).
            if ev is not None and st["event_exit_now"].is_set():
                log.info("[%s] (%s) 2K da ket thuc -> KHONG gom doi nua, ra khoi thap roi THOAT GAME",
                         label, role)
                if _inside_floor_crawl_tower(ev, c.current_map):
                    try:
                        c.exit_event(ev)
                    except Exception as e:
                        log.warning("[%s] (%s) 2K: loi di ra khoi thap: %s", label, role, e)
                _reason("2K ket thuc -> ra khoi thap -> thoat game")
                c.close()
                return
            # Event SOLO (loan dau): moi acc tu dang ky va tu danh -> KHONG sync kenh.
            # `do_channel_sync` la mot BARRIER: leader phai doi DU ca party roi moi chon kenh, con
            # member dung cho `channel_ready`. Voi event danh chung thi doi nhau la can (phai cung
            # instance), nhung danh SOLO thi cho nhau khong duoc gi - acc nao xong truoc van phai
            # dung im cho acc dang login, mat luot dang ky (user bao 25/08).
            if not _is_party_event(mode, has_leader, ev) and not _event_solo_battle_kind(mode, ev):
                do_channel_sync()
            # ==== NGOAI GIO EVENT: CHAN O DAY, TRUOC MOI VIEC ====
            #
            # Hai cua nay TUNG nam duoi, sau ca doan `go_to_event`. Tuc acc ngoai gio van di het
            # duong vao map event roi moi toi cho ket luan "ngoai gio". Cong su kien da dong nen
            # khong bao gio vao duoc, va vong ngoai goi lai mai.
            #
            # Ca that 09/09 (user: "cac party 40npc, ngoai time event roi ma log in vao thay van co
            # log ko vao dc map event, het event roi thi vao map event lam lon gi"):
            #   22:31:37 [xGAx] (LEADER) chua vao duoc map event 10991 (dang o 12003) -> thu lai (1/5)
            # Dem tu 22:30 tro di: 3372 dong nhu vay - trong khi event 40NPC dong luc 22:00.
            #
            # 12003 la map DOI THUONG: acc da o dung cho can den roi, chi thieu moi viec ngung di
            # vao map event.
            # HET GIO EVENT -> THOAT, khong phu thuoc CO LEADER hay khong.
            #
            # `_is_npc_repeat_party_event` goi `_event_battle_kind(mode, has_leader, ev)`, ma ham
            # do tra None khi KHONG CO LEADER -> party "khong co chu PT" (acc dung yen cho nguoi
            # that moi) thi cua nay khong bao gio chay: het gio van dung im mai mai.
            #
            # Ngoai gio thi CHANG CON GI DE LAM, co leader hay khong cung the. Doc thang `ev` de
            # biet day co phai event 40NPC.
            _kind_raw = ((ev or {}).get("party_battle") or {}).get("kind")
            if mode == "event" and _kind_raw == "npc_repeat" and not c.in_40npc_window():
                log.info("[%s] (%s) 40NPC NGOAI GIO event -> huy party + di doi thuong + thoat game",
                         label, role)
                try: c.leave_party()
                except Exception: pass
                try: c.claim_40npc_reward(ev)
                except Exception as e: log.warning("[%s] loi doi thuong 40NPC (ngoai gio): %s", label, e)
                _reason("40NPC ngoai gio -> doi thuong xong -> thoat game")
                c.close(); return
            # LOAN DAU NGOAI GIO: khong vao map lam gi, thoat luon. Khung gio doc tu `ev["lich"]`
            # (THU 3 20:00-22:00, THU 7 20:30-22:30) - khong hard-code duoc vi hai ngay khac gio.
            if _event_solo_battle_kind(mode, ev) == "chaos_vs" \
                    and not loandau.in_event_window(ev=ev):
                log.info("[%s] (%s) LOAN DAU ngoai gio event -> ra khoi map + thoat game", label, role)
                _loandau_ra_khoi_map(c, ev, label)
                _reason("Loan dau ngoai gio -> ra khoi map -> thoat game")
                c.close(); return
            if ev is None:
                log.warning("[%s] (%s) mode event nhung KHONG co event nao trong events.json -> dung yen tai cho",
                            label, role)
            elif _decide_2k_resume(st, username, c.current_map, ev,
                                    len(party_accounts(pidx)), _stopped, label, pidx=pidx):
                # LOGIN LAI GIUA CHUNG 2K: CA PARTY con trong thap va CUNG TANG -> leo tiep tu day,
                # KHONG chon lai event (chon lai = bi keo ve 12921 = mat het tang da leo).
                _resume_2k = True
                log.info("[%s] (%s) 2K: ca party dang o trong thap (%s) -> leo tiep tu day",
                         label, role, config.scene_name(c.current_map))
            elif _inside_floor_crawl_tower(ev, c.current_map):
                # Con trong thap nhung PARTY LECH TANG -> gom nhau. Trong map event KHONG teleport
                # duoc, nen DI BO xuong TANG THAP NHAT ca doi toi duoc. KHONG dat _resume_2k: sau
                # khi gom van phai sync kenh (acc vao tu ngoai co the o instance khac).
                _tgt = _2k_regroup_target(st, ev, pidx)
                log.warning("[%s] (%s) 2K: party lech tang -> di bo xuong %s de gom doi",
                            label, role, config.scene_name(_tgt))
                try:
                    c.regroup_to_event_start(ev, dest=_tgt)
                except Exception as e:
                    log.warning("[%s] (%s) 2K: loi di bo gom doi: %s", label, role, e)
            else:
                try:
                    # KET QUA PHAI DOC. Truoc 05/09 vut di -> tele hong ma vong event van chay,
                    # mo NPC su kien tren MAP THUONG -> server da ma 5 "su kien vi pham"
                    # (mat 4 acc party 11 luc 21:15).
                    # VAO KHONG DUOC THI THU LAI, khong di tiep. Ban cu chi `log.error` roi chay
                    # thang xuong vong event - acc dung o MAP TRAIN suot ca ván: khong mo NPC
                    # (dung, vi mo o map thuong la bi da ma 5), nhung cung khong danh duoc gi.
                    #
                    # Ca that 08/09 (user: "co acc ko tele vao event"): 44 acc dinh, deu vi goi
                    # `0x4d` bi gui giua tran nen server nuot - nay da cho het tran truoc khi gui
                    # (xem `go_to_event`), nhung van con cac ly do khac (dang doi may vo gioi,
                    # server tre) nen phai co vong thu lai chu khong bo cuoc sau MOT lan.
                    _vao_ok = False
                    for _lan_ev in range(1, 6):
                        if _stopped() or not c.running:
                            break
                        if int(getattr(c, "current_map", 0) or 0) == int(ev.get("dest_map") or 0):
                            _vao_ok = True
                            break
                        if c.go_to_event(ev):   # tu day het cinematic roi thoat cutscene
                            _vao_ok = True
                            break
                        log.warning("[%s] (%s) chua vao duoc map event %s (dang o %s) -> thu lai "
                                    "(lan %d/5)", label, role, ev.get("dest_map"),
                                    c.current_map, _lan_ev)
                        time.sleep(10)
                    if _vao_ok:
                        c._ev_vao_hong = 0
                    else:
                        # VONG NGOAI GOI LAI HAM NAY MAI -> "5 lan" o tren khong phai tran that.
                        #
                        # Ca that 09/09 party 53 (user: "sau 23h roi con co vao map event lam lon
                        # gi"): 276 lan thu vao map 12922 tu 22:00 den 23:20 - TAM MUOI PHUT. Het
                        # 5 lan thi acc "dung yen tai safe", roi vong ngoai chay lai tu dau, lai 5
                        # lan nua. Cong su kien da dong (het gio) nen khong lan nao vao duoc.
                        #
                        # Event nao co khai bao `lich` thi da co cua ngoai gio rieng (40NPC,
                        # loan dau). 2K (`nhi_kieu`) de `lich: null` nen KHONG AI BIET gio - tran
                        # nay la luoi do chung, khong thay cho viec khai bao lich.
                        c._ev_vao_hong = int(getattr(c, "_ev_vao_hong", 0) or 0) + 1
                        log.error("[%s] (%s) KHONG vao duoc map event %s (dang o %s) -> KHONG mo "
                                  "NPC su kien (mo o map thuong = bi da ma 5) [vong %d/%d]",
                                  label, role, ev.get("dest_map"), c.current_map,
                                  c._ev_vao_hong, EV_VAO_HONG_TOI_DA)
                        if c._ev_vao_hong >= EV_VAO_HONG_TOI_DA:
                            log.warning("[%s] (%s) %d vong LIEN TIEP khong vao duoc map event %s -> "
                                        "cong su kien dong (het gio?) -> THOAT GAME, khong thu nua",
                                        label, role, c._ev_vao_hong, ev.get("dest_map"))
                            _reason("khong vao duoc map event %s sau %d vong -> thoat game"
                                    % (ev.get("dest_map"), c._ev_vao_hong))
                            c.close(); return
                except Exception as e:
                    log.warning("[%s] loi go_to_event: %s", label, e)
            c.flee_mode = False   # dung yen; bi danh thi tu danh, KHONG chu dong (cho moi tay)
        else:
            # --- CITY (tap trung ve thanh) / STAND (dung yen) / CLEANBAG ---
            # SOLO daily dungeon TRUOC (neu bat). Dungeon co the bi DUMP ve 12000 -> lam truoc
            # roi MOI ve thanh -> dam bao dung dung thanh tap trung du co bi dump.
            if do_daily:
                try:
                    c.do_daily_dungeon()
                except Exception as e:
                    log.warning("[%s] loi daily dungeon (bo qua): %s", label, e)
            if mode == "city":
                # Ve thanh SAU dungeon: neu dungeon dump ve 12000 thi teleport ve thanh lan nua.
                log.info("[%s] (%s) TAP TRUNG ve thanh %s (flag %s)%s", label, role, sc, city_flag,
                         " (dung o %s -> ve lai)" % c.current_map if c.current_map != sc else "")
                try:
                    if (_ve_thanh_tap_trung(c, pidx, label, sc, city_flag)
                            and c.current_map == getattr(c, "NOI_DAT_SELL_CITY", 12061)):
                        c.sell_noi_dat()
                except Exception as e:
                    log.warning("[%s] loi ve thanh: %s", label, e)
            elif mode == "cleanbag":
                log.info("[%s] (%s) DON TUI DO - chua lam, tam dung yen", label, role)
            elif raw_mode in ("train", "digioi_train"):
                # MODE TRAIN MA KHONG CO BAI -> day KHONG phai "dung yen tai cho login" that su.
                # `_auto_train_target` chua tra ve map luc acc nay login (party vua restart, chua
                # du du lieu level ca doi) -> sc=0 -> tm=None -> train_on_map False -> roi vao
                # chuoi nhanh cuoi. Nhanh setup chi chay MOT LAN nen du 20 giay sau dieu phoi chot
                # xong bai, acc van nam nguyen day.
                #
                # Ca that party 15, 13/09 (user: "p15 lai deo them gom pt" -> "vi sao no ko chot
                # dc bai train"):
                #   14:23:43 >>> PARTY 15: TU CHON MAP -> Dam Lay Tang Khau1 (map 21841)
                #   17:14:15 [truchin] (member) DUNG YEN tai cho login (map=12003)
                #   ... mot TIENG chi con dong `pos=(530,530) map=12003` moi 5 giay ...
                # Dieu phoi CHOT DUOC bai tu 14:23; acc login luc 17:14 doc khong ra nen dung im.
                log.warning("[%s] (%s) MODE=%s ma CHUA CO BAI TRAIN (sc=%s, train_pick=%r) -> dung "
                            "yen cho dieu phoi chot bai. Neu keo dai, xem log 'PARTY %d: TU CHON "
                            "MAP'.", label, role, raw_mode, sc, pcfg.get("train_pick"), pidx + 1)
            else:
                log.info("[%s] (%s) DUNG YEN tai cho login (map=%s)", label, role, c.current_map)
            c.flee_mode = False   # bi danh thi tu danh, KHONG chay
            do_channel_sync()

        _defer_party_invite_for_event = _is_party_event(mode, has_leader, ev)
        _solo_without_party = is_digioi and pcfg.get("digioi_mode") == "solo"
        if not is_leader and not _defer_party_invite_for_event and not _solo_without_party:
            # Da xong login chores + di toi map/diem tap ket + sync kenh. Bay gio moi mo gate
            # party thuong va xu ly loi moi da den tu truoc. Dungeon invite co gate rieng, van
            # hoat dong trong login chores de daily team dungeon khong bi khoa.
            c.set_party_invite_ready(True)
        if not is_leader:
            c._san_sang_party = True   # dau vet tren CHINH CLIENT (bot doc thang, khong ai khai)
        time.sleep(2)

        # training_started duoc gan trong nhanh "elif is_leader" (o duoi) - PHAI khoi tao truoc o
        # DAY vi vong keepalive (should_fight = training_started if is_leader else ...) doc bien
        # nay bat ke mode nao. Thieu dong nay -> Di Gioi SOLO (re vao nhanh rieng, KHONG chay qua
        # "elif is_leader") crash NGAY: "cannot access local variable 'training_started'".
        training_started = False
        startup_reform_gen_handled = 0
        # Moc reform phai co trong MOI duong vao `_start_training`. Sau doi kenh/dieu phoi,
        # account co the nhay thang vao vong chinh va bo qua khoi setup map-train ben duoi (noi
        # truoc day moi gan `_rg_base`) -> leader du PT nhung nem UnboundLocalError va dung safe.
        _rg_base = st["reform_gen"]
        # Khoi tao SOM (truoc ca phan startup) vi member co the dang ket trong vong "CHO leader moi
        # vao party" luc leader bao gom lai - vong do cung phai nghe duoc lenh.
        rally_gen_handled = st["rally_gen"]
        # Di Gioi SOLO: moi acc chay rieng le hoan toan - khong lap party, khong dong bo kenh (da
        # bo qua o buoc dong bo kenh o tren), khong cho leader/member gi ca. Ai vao duoc DG thi tu
        # chay long vong luon (xem buoc 1-2 o tren: da vao DG + lam nhiem vu nhe).
        digioi_solo = is_digioi and pcfg.get("digioi_mode") == "solo"
        event_mode = (mode == "event")
        event_party_mode = _is_party_event(mode, has_leader, ev)
        event_solo_kind = _event_solo_battle_kind(mode, ev)
        # QUYET DINH "THUA -> THOAT" CHI CO GIA TRI TRONG LAN LOGIN DO (user chot 02/09):
        # "chi muon 2 lan thua thi out o lan login do thoi, login lai thi danh lai cho den khi thua
        # 2 tran lien tiep".
        #
        # `go_claim` va `event_battle_done` song trong `_party_state[pidx]` = state theo TIEN TRINH,
        # khong phai theo lan login. `go_claim` truoc day KHONG duoc xoa o BAT KY dau (kiem het file):
        # set mot lan la moi acc login sau do doc thay -> "40NPC xong -> di doi thuong + thoat" ->
        # LOG VAO XONG OUT LUON, khong danh tran nao. `event_battle_done` co duoc xoa nhung chi o
        # nhanh reconnect va con bi gac sau `event_battle_active`, ma `_on_npc40_loss` da ha co do
        # xuong False truoc roi -> thuc te cung khong bao gio xoa sau khi thua.
        #
        # `is_reconnect` = server da giua chung -> GIU nguyen quyet dinh cua party (dung de mot acc
        # rot roi vao lai lam ca party danh tiep trong khi 4 dua kia da bo cuoc).
        if event_party_mode and not is_reconnect:
            if st["go_claim"].is_set() or st["event_battle_done"].is_set():
                log.info("[%s] (%s) EVENT: phien login MOI -> xoa co 'da thua/da xong' cua phien "
                         "truoc, danh lai tu dau", label, role)
            st["go_claim"].clear()
            st["event_battle_done"].clear()
        # Event solo VAN danh -> KHONG duoc roi vao nhanh "dung yen cho tay".
        event_stand_mode = event_mode and not event_party_mode and not event_solo_kind
        # EVENT PARTY (40NPC): kenh/instance cua MAP EVENT (vd 10991) DOC LAP voi kenh thanh -> sync
        # kenh o tren (luc con o thanh, truoc go_to_event) KHONG dam bao cung instance tren map event.
        # PHAI sync LAI SAU khi ca party da vao map event, TRUOC khi leader moi. Thieu buoc nay: moi
        # acc vao 1 instance khac nhau (vi tri spawn khac) -> moi entity khong toi -> joined=0/4 mai,
        # leader spam moi ma khong ai join -> khong danh duoc (bug user 40NPC 2026-07-29).
        if event_party_mode:
            if not _resume_2k:
                do_channel_sync()
            else:
                # RESUME 2K: ca doi von dang cung mot instance trong thap. Doi kenh luc nay la
                # RUI RO (co the bi day ra khoi instance / tach doi) va khong giai quyet gi.
                log.info("[%s] (%s) 2K resume: bo qua sync kenh (ca doi da cung instance)",
                         label, role)
            if not is_leader:
                # 40NPC chi ready party sau khi DA vao map event va sync instance tai map event.
                c.set_party_invite_ready(True)
        if event_solo_kind == "chaos_vs":
            # LOAN DAU LOI DAI: da tele toi map 10991 o tren. Moi acc TU chay - khong party,
            # khong sync kenh, khong barrier. Quest mode da bat san (force_quest_mode o mode
            # event). Xem documents/LOAN_DAU.md.
            c.flee_mode = False
            point = tuple((ev.get("party_battle") or {}).get("point") or (910, 290))

            def _before_loandau_repeat():
                # AN TOAN o day: dialog da dong (`0x14 08 26`) truoc khi ham nay duoc goi. Dung
                # item luc dialog dang mo lam server tra 08 0001 roi KICK (bai hoc 40NPC).
                if c.running and not c.state.in_battle:
                    c.heal_npc40_between_battles()

            _mot_tran = bool(pcfg.get("loandau_mot_tran"))
            if c.start_loandau_loop(point, _before_loandau_repeat, mot_tran=_mot_tran, ev=ev):
                log.info("[%s] (%s) LOAN DAU: toi %s va bat dau vong dang ky/danh%s", label, role,
                         point, " (CHI 1 TRAN roi thoat)" if _mot_tran else "")
        elif event_stand_mode:
            # EVENT: da tele toi map event o tren -> DUNG YEN HOAN TOAN, cho moi tay. Moi nick doc lap,
            # KHONG lap party/sync kenh (bo qua het nhanh leader/member ben duoi). Auto-accept moi tay
            # xu ly o client (0x2f). training_started=False -> keepalive KHONG danh chu dong.
            c.flee_mode = False
            # NOI RO VI SAO dung yen, khong thi nhin log tuong bot treo. Hay gap nhat: event CO
            # danh theo party (`party_battle.kind`) nhung party CHUA DAT LEADER -> `_is_party_event`
            # tra False -> roi vao day va cho nguoi mo i tay.
            #
            # Ca that APK, 13/09 (user: "ban apk ko thay di danh"): quanmot dung o map 12922
            # (thap Nhi Kieu) 8 phut, `pos=None ... combat=False` moi 5 giay. `event_hom_nay
            # ('nhi_kieu')` co du `kind='floor_crawl'`, chi thieu moi leader.
            _kind_ev = ((ev or {}).get("party_battle") or {}).get("kind")
            if _kind_ev and not has_leader:
                # `PARTY_LEADER_ACC[pidx]` = acc o SLOT DAU cua party; khong co = party dat
                # "Khong co chu PT" (APK: `noLeader`, PC: `no_leader_var`). In ro ca hai de khong
                # phai mo cau hinh ra doi chieu.
                log.warning("[%s] (%s) EVENT '%s' CO danh theo party (kind=%s) nhung party %d "
                            "KHONG CO LEADER (leader=%r, acc slot dau=%r) -> dung yen cho moi tay."
                            " Bo tick 'Khong co chu PT' de bot tu lap party va danh.",
                            label, role, (ev or {}).get("name") or pcfg.get("event_key") or "?",
                            _kind_ev, pidx + 1, config.PARTY_LEADER_ACC.get(pidx),
                            next((u for u, _p, _l, _k in party_accounts(pidx)), None))
            else:
                log.info("[%s] (%s) EVENT -> dung yen tai map event, cho moi tay (auto-accept)",
                         label, role)
        elif digioi_solo:
            # Di Gioi SOLO cho mang toi 4 pet ra tran CUNG LUC (khac han 1 pet binh thuong) - moi
            # con 1 atype rieng (0,1,3,4), can nhanh combat rieng (combat.decide_multipet, xem
            # state.solo_multipet trong bot/state.py + bot/client.py _make_decisions).
            c.state.solo_multipet = True
            # BAO HIEM: SOLO khong co ai cuu (khac party co quan su hoi SP + dong doi hoi mau ho) ->
            # thieu 1 trong 2 loai thuoc (HP hoac SP) trong tui thi DUNG YEN, KHONG chay long vong
            # danh quai lien tuc -> de het mau chet hoac can SP giua chung ma khong tu hoi duoc.
            if c.has_hp_and_sp_items():
                c.flee_mode = False
                c.combat_ready()
                c.start_run_around()
                log.info("[%s] Di Gioi SOLO -> tu chay long vong (khong lap party, khong dong bo kenh)", label)
            else:
                c.flee_mode = True
                log.warning("[%s] Di Gioi SOLO -> THIEU thuoc hoi HP hoac SP trong tui -> DUNG YEN "
                            "(khong chay long vong, tranh chet/can SP khong ai cuu)", label)
        # DG+Train: DA CO acc het gio DG (giveup) ngay luc setup -> KHONG lap party nua (khong the gom
        # du trong DG: acc het gio dung NGOAI, khong vao lai instance). Acc con time TU chay DG (danh/
        # dung) den HET GIO CUA MINH roi bao xong -> barrier -> train. Truoc day van vao nhanh leader/
        # member -> leader CHO member vo han (member het gio khong bao gio ready) -> ca lu IM tu do
        # (bug that: party 24, daiba het gio -> 4 acc kia log "chay DG SOLO" xong im hoan toan).
        elif is_digioi and _dg_gather_giveup() and _acc_het_gio_dg(c) is not True:
            # `_acc_het_gio_dg(c) is not True`: nhanh nay danh cho acc CON TIME (chinh chu thich
            # tren ghi vay). Acc DA HET GIO ma vao day thi no `start_run_around()` roi khong con
            # viec gi - khong vao lai DG duoc - va dung im den cuoi phien.
            #
            # Ca that 09/09 `gamo` (120/120 phut, tuc het sach gio):
            #   06:02:21 (LEADER) DG+Train: co acc het gio DG -> chay DG SOLO den het gio
            #   06:02:21 Run-around quanh (970,790) / Dung run-around
            #   [im den het log]
            if c.has_hp_and_sp_items():
                c.flee_mode = False
                c.combat_ready()
                c.start_run_around()
            else:
                c.flee_mode = True
            log.info("[%s] (%s) DG+Train: co acc het gio DG -> chay DG SOLO den het gio (khong lap party)",
                     label, role)
        # --- Leader: CHO du member san sang roi MOI, roi CAY ---
        elif is_leader:
            _dg_solo_bail = False   # True = DG+Train giveup giua chung -> bo lap party, chay DG solo
            # `via_route` = toi train map THEO PARTY (da lap party + cung kenh o thanh) -> binh
            # thuong thi khoi moi lai. NHUNG phai KIEM TRA LAI so member: member co the RUNG DOC
            # DUONG (bi dut/bi chan login) sau luc leader dem du o thanh.
            # Bug that (party 2, 18/08... 21/08 18:18): 18:18:16 "4/4 member join lai -> KEO qua cong",
            # 18:18:23 roster con 3, 18:18:59 con 2 -> leader van "da partied -> bo qua moi lai" roi
            # KEO 3 acc ra spot train, bo 2 acc lai. Vi pham rule toi thuong: PHAI DU PARTY.
            _joined_now = joined_member_count(pidx)
            if via_route and _joined_now >= st["n_members"]:
                st["invited"].set()   # bao member khoi cho moi
                log.info("[%s] (LEADER) toi train map theo party (da partied, du %d/%d) -> bo qua moi lai",
                         label, _joined_now, st["n_members"])
            else:
                if via_route:
                    log.warning("[%s] (LEADER) toi train map theo party NHUNG chi con %d/%d member "
                                "(rung doc duong) -> KHONG train thieu, cho + moi lai cho du",
                                label, _joined_now, st["n_members"])
                # XOA (13/09) vong `while _dem_san_sang(pidx) < st["n_members"]`.
                #
                # No la vong acc tu quyet NANG NHAT con lai: leader tu dat dieu kien "du member san
                # sang thi toi moi moi", tu cho den khi dieu kien do du, VA trong luc cho con TU RA
                # LENH GOM (`_do_reform(to_spot=False)` moi khi qua `READY_WAIT_REFORM_SEC` hoac
                # thay party lech map qua `READY_WAIT_SPLIT_SEC`). Tuc mot acc vua tu dinh nghia
                # trang thai dung, vua tu chon thoi diem gom - dung hai viec cua dieu phoi.
                #
                # Da thu 13/09 cach "de nguyen vong nhung gan cua thoat khi co lenh": van sai, vi
                # cai phai bo la HANH DONG TU CHO, khong phai do dai cua no.
                #
                # GIO: leader MOI luon. Member chua ve kip thi khong vao doi -> dieu phoi thay
                # thieu doi / lech map / lech kenh va ra lenh gom theo dung chuoi
                # (gom map -> gom kenh -> moi). Do la co che dung chung cho moi tinh nang, khong
                # phai mot ban sao rieng cua nhanh train.
                _dg_solo_bail = False
                if _stopped(): st["stop_leader_done"].set(); c.close(); return
                if not c.running: c.close(); return
                log.info("[%s] (LEADER) %d/%d member san sang -> MOI (theo entity), thieu thi de "
                         "dieu phoi gom", label, _dem_san_sang(pidx), st["n_members"])
                # MOI toi khi DU PARTY join (khong gioi han 6 lan): member da san sang, invite se toi.
                _t0 = time.time()
                _resync_t0 = time.time()
                _resync_log_luc = 0.0   # chan spam log "moi ... chua du party" moi vong
                while not _dg_solo_bail and joined_member_count(pidx) < st["n_members"]:
                    if _stopped(): st["stop_leader_done"].set(); c.close(); return
                    if not c.running: c.close(); return
                    # DIEU PHOI la nguoi quyet dinh party dang phai lam gi. No bao GOM (lech
                    # map/kenh) thi leader THOI moi VA DI GOM - moi tiep chi to ra 488 dong
                    # "lech map live" nhu party 19 chu khong ai vao doi duoc.
                    #
                    # PHAI `continue` SAU KHI REFORM, TUYET DOI KHONG `return`: o vong nay
                    # `return` = KET THUC run_account = TAT LUON ACC. Da xay ra that 05/09 18:09
                    # (party 10): ban dau viet `return` -> leader luumot tu tat, GUI bao leader
                    # mat, ca party dung. Cac dong `return` ngay tren deu di kem `c.close()`
                    # (Stop/mat ket noi) hoac co nhanh relogin rieng (`_finish_digioi_train_*`
                    # dat _dt["relogin_train"]) - nhanh nay khong co gi nhu the.
                    _kh = _ke_hoach(st)
                    if _nhip_moi_party(train_on_map) == "gom":
                        log.info("[%s] (LEADER) dieu phoi bao GOM (%s) -> thoi moi, gom lai",
                                 label, (_kh or {}).get("ly_do") or "lech map/kenh")
                        st["invited"].set()   # tha member khoi startup de cung nhan reform_gen
                        c.flee_mode = True
                        try:
                            _thi_hanh_gom(c, st, label, "LEADER", _kh, _do_reform)
                        except Exception as e:
                            log.warning("[%s] gom theo lenh dieu phoi loi: %s", label, e)
                        _resync_t0 = time.time(); _t0 = time.time()
                        # NGU MOT NHIP DIEU PHOI. Thieu dong nay = QUAY VONG TRAN: `_do_reform()`
                        # o map event tra ve NGAY ("khong co smart/legacy route -> bo qua") roi
                        # `continue` lap lai lap tuc. Log that party 5 (06/09): thsau quay
                        # 201.495 vong, cao diem 7.985 dong/giay - va chinh no bo doi luon LUONG
                        # DIEU PHOI, nen ke hoach dong bang o `viec=gom` suot 5 phut du ca party
                        # da chung kenh tu 13:15:37. Vong nong tu nuoi chinh no.
                        time.sleep(KE_HOACH_NHIP)
                        continue
                    if _finish_digioi_train_if_time_over("moi member vao party DG"):
                        return
                    # RACE: giveup luc dang MOI party (member vua het gio DG) -> bo moi, chay DG SOLO.
                    if _dg_gather_giveup():
                        if c.has_hp_and_sp_items():
                            c.flee_mode = False; c.combat_ready(); c.start_run_around()
                        else:
                            c.flee_mode = True
                        log.info("[%s] (LEADER) DG+Train: co acc het gio DG luc moi -> chay DG SOLO "
                                 "den het gio (bo lap party)", label)
                        _dg_solo_bail = True
                        break
                    _joined_now = joined_member_count(pidx)
                    _invite_elapsed = time.time() - _resync_t0
                    # LEADER KHONG TU QUYET GIAI TAN PARTY - xem `_phat_lenh_dong_bo`.
                    # Truoc day o day leader tu `leave_party()` + `reset_party_joined()` +
                    # `resync_gen += 1` khi moi 20s chua du. No dap chinh cai party dang gom dof:
                    #   p9  02:33:30  PARTY: c0edf0a0 vao doi -> roster 3 nguoi
                    #       02:33:30  PARTY: loi moi -> DONG Y (lubbay)
                    #       02:33:50  (LEADER) moi 24s chua du party (2/4) -> giai tan
                    #       02:33:55..57  3 member Roi/giai tan party cu
                    #   p11 02:10:32  PARTY-JOINED: 3 -> 0 (nguoi ghi=LEADER)
                    #       02:10:33  (LEADER) sync kenh/map OK: 5/5 acc o map 49942
                    #       02:10:33..34  4 member roi party -> 11 phut moi gom lai duoc
                    # Leader con dem TRE hon roster server ("2/4" trong khi roster da 3 nguoi) nen
                    # no dap ca party that su dang du dan. Gio chi DIEU PHOI duoc phat lenh dong
                    # bo - no co cooldown va chot "nguoi khac vua bump thi im", leader thi ban
                    # thang khong chot gi.
                    if _should_resync_incomplete_digioi_party(
                            is_digioi, digioi_solo, _joined_now,
                            st["n_members"], _invite_elapsed):
                        if time.time() - _resync_log_luc > 30.0:
                            _resync_log_luc = time.time()
                            log.info("[%s] (LEADER) Di Gioi moi %.0fs chua du party (%d/%d) -> "
                                     "MOI TIEP, de dieu phoi quyet co dong bo hay khong",
                                     label, _invite_elapsed, _joined_now, st["n_members"])
                    if _should_reform_incomplete_party(
                            train_on_map, _joined_now, st["n_members"], _invite_elapsed):
                        # Ca party dang dung san o bai train -> xu ly tai cho, khong ve thanh.
                        if _party_tai_cho_xu_ly("moi %.0fs chua du party" % _invite_elapsed):
                            _resync_t0 = time.time(); _t0 = time.time()
                            continue
                        log.warning("[%s] (LEADER) moi %.0fs chua du party (%d/%d) -> REFORM "
                                    "dong bo lai map + kenh", label, _invite_elapsed,
                                    _joined_now, st["n_members"])
                        st["invited"].set()   # tha member khoi startup de cung nhan reform_gen
                        with st["lock"]:
                            _startup_gen = st["reform_gen"]
                        c.flee_mode = True
                        try:
                            _do_reform()
                        except Exception as e:
                            log.warning("[%s] startup reform party loi: %s", label, e)
                        startup_reform_gen_handled = max(
                            startup_reform_gen_handled, _startup_gen
                        )
                        _resync_t0 = time.time(); _t0 = time.time()
                        continue
                    # MOI 60s van chua du party: KHONG tu giai tan nua (giong nhanh Di Gioi o tren).
                    # Member troi kenh la viec cua DIEU PHOI - no thay lech kenh thi phat lenh dong
                    # bo, va no co cooldown de khong dap party dang gom do.
                    if event_party_mode and time.time() - _resync_t0 > 60:
                        if time.time() - _resync_log_luc > 30.0:
                            _resync_log_luc = time.time()
                            log.info("[%s] (LEADER) moi 60s chua du party (%d/%d) -> MOI TIEP, de "
                                     "dieu phoi quyet co dong bo hay khong",
                                     label, joined_member_count(pidx), st["n_members"])
                    st["invited"].set()
                    time.sleep(4)
                    if time.time() - _t0 > 30:
                        log.info("[%s] (LEADER) dang moi... joined=%d/%d",
                                 label, joined_member_count(pidx), st["n_members"])
                        _t0 = time.time()
                st["invited"].set()
                log.info("[%s] (LEADER) DU PARTY (%d/%d member join)",
                         label, joined_member_count(pidx), st["n_members"])
                if not train_on_map:
                    _invite_whitelist_followers_if_bot_party_ready(
                        c, st, pidx, label, force=True
                    )
            # Bat dau train (set QS + ra cho danh). Goi khi DA co >=1 member (du quan su).
            training_started = False
            def _start_training(ep_ra_spot=False):
                """Tra True = DA THUC SU ra cho danh; False = bail giua chung (chua train).

                `ep_ra_spot`: BO QUA chot "reform pending" -> keo thang ra spot. Dung o vong retry
                60s khi party DA DU va ca party cung map+kenh: luc do thu duy nhat con thieu la
                leader dang dung o safe chu khong phai diem quai -> cu di ra, KHONG ve thanh.
                """
                c.set_party_strategist()    # set member INT cao nhat lam quan su (hoi SP)
                if event_party_mode and _event_battle_kind(mode, has_leader, ev) == "floor_crawl":
                    # 2K (Nhi Kieu): DU PARTY roi moi bat dau leo thap. Moi acc tu di 12921->12922
                    # rieng le (khong party - vao event dinh party la tele loi), toi map event moi
                    # sync kenh + lap party (nhanh _defer_party_invite_for_event o tren lo).
                    if st["event_battle_done"].is_set():
                        c.flee_mode = False
                        log.info("[%s] (LEADER) 2K da xong/dung -> khong leo lai", label)
                        return True
                    def _on_crawl_done(lost=False, ly_do="ket"):
                        # CHI BAO SU THAT, KHONG TU QUYET. Vong leo thap ket thuc vi NHIEU ly do,
                        # va `finally` cua `run_floor_crawl` goi ham nay o CA BA:
                        #   - thua tran            -> 2K het that
                        #   - leo het thap / ket   -> 2K het that
                        #   - CLIENT CHET GIUA CHUNG (bi da / user Stop) -> 2K CHUA het gi ca
                        # Ban cu bam thang `event_exit_now` o day nen ca ba deu thanh "2K xong".
                        # Bug that party 12 (06/09): leader tonmot dang o tang 5 (12928) thi dinh
                        # `SERVER NGAT KET NOI: gui goi lien tuc qua nhanh (ma 13)` luc 13:50:50
                        # -> vong leo thap dut -> co "xong" bat len -> relogin xong doc co do va
                        # keo CA 5 ACC di bo nguoc 12928->12927->...->12921->12003 roi TAT GAME.
                        # User: "danh deo danh, tu dung chay het ra ngoai roi thoat".
                        # `ly_do` do chinh vong leo bao len: xong | thua | ket | dut.
                        # KET (ket o cong / het cong len / danh thieu) va DUT (client chet) deu
                        # KHONG phai "2K het". Party 15 (06/09): leader dung 10 phut o tang 9 voi
                        # `chua du member (0/4)` -> `_enter_gate` hong -> `2K: ket o cong tang 9`
                        # -> ban truoc gan nham thanh "xong" -> ca doi ra khoi thap + tat game.
                        _het = ly_do in ("xong", "thua")
                        with st["lock"]:
                            st["event_battle_active"] = False
                            if _het:
                                st["event_battle_done"].set()
                            st["2k_ket_qua"] = ly_do
                        if _het:
                            _set_party_quest_mode(pidx, False, label)
                        log.info("[%s] (LEADER) 2K: vong leo thap ket thuc (%s) -> bao DIEU PHOI",
                                 label, ly_do)
                    def _heal_party_2k():
                        # Gia han quest_mode + cua so dungeon cho CA party sau moi tran (member
                        # khong tu gia han duoc - xem chu thich _set_party_quest_mode).
                        _set_party_quest_mode(pidx, True, label, quiet=True)
                        # HOI FULL HP/SP CA PARTY sau moi tran (song song, giong 40NPC). Leader tu
                        # hoi thi khong du: member cung an don, ma quest_mode=True lam
                        # _heal_after_battle() cua tung acc thoat som.
                        clients = [account_clients.get(u) for u in _active_party_usernames(pidx)]
                        clients = [x for x in clients if x is not None and x.running]
                        def _heal_one(cli):
                            if cli.running and not cli.state.in_battle:
                                cli.heal_full(force=True)
                        workers = [threading.Thread(target=_heal_one, args=(cli,), daemon=True)
                                   for cli in clients]
                        for w in workers: w.start()
                        for w in workers: w.join(timeout=10)
                        log.info("[%s] (LEADER) 2K: ca party (%d acc) da hoi FULL HP/SP",
                                 label, len(clients))
                    with st["lock"]:
                        st["event_battle_active"] = True
                    c.flee_mode = False
                    _set_party_quest_mode(pidx, True, label)
                    def _du_party_2k():
                        """Truoc khi LEN TANG: party con du khong? Chua du thi MOI LAI ngay tai
                        cho, khong du trong han thi THOI leo (de dieu phoi xu ly).

                        Party tan giua chung la chuyen BINH THUONG trong thap: server CAM doi kenh
                        khi dang trong doi (`result=3`), nen muon doi kenh la PHAI roi doi truoc.
                        Party 5 (06/09):
                            16:34:14  4 member: Doi kenh 2 THAT BAI: DANG TO DOI (result=3)
                            16:34:14  4 member: -> roi party roi thu lai
                            16:34:15  4 member: Doi kenh OK -> 2      <- da ra khoi doi
                            16:34:53  leader:   qua cong idx=2 -> map 12929   <- DI MOT MINH
                            16:35:38  leader:   tang 6 chi danh duoc 0/3 tran
                        Ra lenh doi kenh ma khong lap lai doi la loi cua NGUOI RA LENH.
                        """
                        if joined_member_count(pidx) >= st["n_members"]:
                            return True
                        log.warning("[%s] (LEADER) 2K: party chi con %d/%d truoc khi len tang -> "
                                    "MOI LAI truoc khi di", label,
                                    joined_member_count(pidx), st["n_members"])
                        _t0 = time.time()
                        # CHO-HOP-LE: leader CHU DONG moi lai moi vong (khong nam cho), co han
                        # `DU_PARTY_TRUOC_CONG_SEC`. Khong phai cho acc khac bao cao.
                        while (c.running and not _stopped()
                               and time.time() - _t0 < DU_PARTY_TRUOC_CONG_SEC):
                            if joined_member_count(pidx) >= st["n_members"]:
                                log.info("[%s] (LEADER) 2K: da du %d/%d -> len tang tiep", label,
                                         joined_member_count(pidx), st["n_members"])
                                return True
                            try:
                                _invite_party_participants(c, True, gap=1.0)
                            except Exception:
                                pass
                            time.sleep(2)
                        return joined_member_count(pidx) >= st["n_members"]

                    if c.start_floor_crawl(
                            ev, _on_crawl_done, _heal_party_2k,
                            lambda: _party_left_tower(pidx, ev) or _party_chet_het(pidx),
                            _du_party_2k):
                        log.info("[%s] (LEADER) 2K: du party -> bat dau leo thap tu map %s",
                                 label, c.current_map)
                elif event_party_mode:
                    if st["event_battle_done"].is_set():
                        c.flee_mode = False
                        log.info("[%s] (LEADER) 40NPC da THUA -> dung yen, khong mo lai battle", label)
                        return True
                    point = tuple(ev["party_battle"]["point"])
                    def _on_npc40_loss():
                        with st["lock"]:
                            st["event_battle_active"] = False
                            st["event_battle_done"].set()
                        _set_party_quest_mode(pidx, False, label)
                        log.warning("[%s] (LEADER) 40NPC: PARTY THUA -> chon KHONG va DUNG", label)
                    def _before_npc40_repeat():
                        _set_party_quest_mode(pidx, True, label, quiet=True)   # gia han cho ca party
                        # npc40.run_loop da chon NO + dong dialog truoc khi vao day. Luc nay moi duoc
                        # dung item; dung item khi prompt dang mo lam server tra 080001 va kick.
                        clients = [account_clients.get(u) for u in _active_party_usernames(pidx)]
                        clients = [x for x in clients if x is not None and x.running]
                        def _heal_one(cli):
                            if cli.running and not cli.state.in_battle:
                                cli.heal_npc40_between_battles()
                        workers = [threading.Thread(target=_heal_one, args=(cli,), daemon=True)
                                   for cli in clients]
                        for worker in workers: worker.start()
                        for worker in workers: worker.join(timeout=8)
                        time.sleep(0.5)  # cho server xu ly item cuoi truoc khi xac nhan dialog
                        log.info("[%s] (LEADER) 40NPC: ca party da hoi phuc -> mo tran tiep", label)
                    with st["lock"]:
                        st["event_battle_active"] = True
                    c.flee_mode = False
                    _set_party_quest_mode(pidx, True, label)
                    if c.start_npc40_loop(point, _on_npc40_loss, _before_npc40_repeat):
                        log.info("[%s] (LEADER) 40NPC: du party -> den %s va bat dau lap battle", label, point)
                elif train_on_map:
                    # CO acc bi DUMP khoi dungeon (reform_gen tang so voi truoc dungeon) -> KHONG keo ra
                    # spot (phi cong: keo xong keepalive lai reform ve thanh). De keepalive REFORM luon.
                    # Lenh reform DA BI DIEU PHOI RUT thi khong con la ly do de bo viec - xem
                    # `_reform_cho_xu` (ca that party 4, 14/09: "lap pt xong deo di train").
                    if _reform_cho_xu(st, _rg_base) and not ep_ra_spot:
                        log.info("[%s] (LEADER) reform pending (acc bi dump dungeon) -> BO QUA keo ra spot, de keepalive REFORM (ve thanh gom nhau)", label)
                        c.flee_mode = True   # ne quai trong luc cho reform
                        return False
                    # diem quai DA chon som (st["mob_spot"]) - ca party da ve safe GAN diem do roi,
                    # va DA LAP PARTY xong (goi tu day) -> gio moi keo ca party ra spot:
                    #   - CO mob_path: leader follow_path KEO ca party (da join, dang o rally) ra spot.
                    #     flee=False de gap quai danh luon (party da du, flee party-battle bi treo).
                    #   - khong path: navigate thang ra spot.
                    # CHUA O MAP TRAIN -> DI TOI MAP DA, khong navigate toi toa do cua map khac.
                    #
                    # `mob_spot` la toa do TREN MAP TRAIN. Dang dung o map khac ma navigate toi do
                    # thi acc di toi mot cho bat ky cua map hien tai, roi cua kiem ben duoi phat
                    # hien "SAI MAP" va bo cuoc - lap lai moi phut, khong bao gio qua duoc cong.
                    #
                    # Ca that party 13, 14/09 (user: "lai thay dung yen o tuong duong"):
                    #   17:02:08 [party 13] gen 11: viec=di_train -> DI TRAIN map 21841 (con o [21001])
                    #   17:04:37 [tonba] da toi diem (1820,1370) sau 4 lenh move
                    #   17:04:37 [tonba] (LEADER) toi diem quai NHUNG dang o SAI MAP (21001 != 21841)
                    #   17:05:38 ... y het, lap lai
                    # Bon member dung im o (770,610) la DUNG: server chi keo ca party khi leader
                    # QUA CONG, ma leader thi chua bao gio toi cong.
                    if c.current_map != sc:
                        log.warning("[%s] (LEADER) lenh di train nhung dang o map %s (can %s) -> "
                                    "REFORM (keo ca party toi map train) truoc", label,
                                    c.current_map, sc)
                        try:
                            # DUONG SAN CO, dung y het cac cho khac goi no: `_do_reform` giai tan +
                            # lap lai party roi KEO CA PARTY qua route toi map train (member bi keo
                            # theo khi leader qua cong). `to_spot=False` = dung lai o rally, phan
                            # ra spot do chinh ham nay lam tiep o duoi.
                            _do_reform(to_spot=False)
                        except Exception as e:
                            log.warning("[%s] (LEADER) reform di train loi: %s", label, e)
                        if c.current_map != sc:
                            c.flee_mode = True
                            log.warning("[%s] (LEADER) chua toi duoc map train %s (dang o %s) -> "
                                        "de DIEU PHOI ra lenh tiep", label, sc, c.current_map)
                            return False
                    spot = st.get("mob_spot")
                    if not spot:
                        c.flee_mode = True
                        log.warning("[%s] (LEADER) map %s khong co tam bai quai -> dung yen, "
                                    "KHONG bat combat o diem bat ky", label, sc)
                        return False
                    _set_train_block_stats_spot(spot, enabled=False)
                    path = st.get("mob_path")
                    _gs = st["reform_gen"]   # dang keo ra spot ma co dua VAN MAP (bump reform_gen) -> abort -> reform
                    _abs = lambda: _stopped() or (not c.running) or st["reform_gen"] > _gs
                    if path:
                        log.info("[%s] (LEADER) party da lap -> follow_path KEO ca party ra spot (%d buoc)",
                                 label, len(path))
                        c.follow_path(path, flee=False, abort=_abs)   # party du -> danh; van map -> abort -> reform
                    elif spot:
                        c.navigate_to(*_jitter(spot), flee=False, abort=_abs)
                    if _abs():   # bi abort (co dua van map) -> KHONG tat flee, de keepalive reform
                        log.info("[%s] (LEADER) dang keo ra spot thi co dua van map -> abort, de keepalive REFORM", label)
                        c.flee_mode = True; return False
                    # XAC NHAN THAT SU dang o dung map train (sc) TRUOC KHI coi la "toi noi". BUG THAT
                    # (xac nhan qua log thuc te): follow_path/navigate_to KHONG tu kiem tra map dich -
                    # neu 1 tran chien xen giua (vd boss the gioi) day leader ve map khac (vd thanh)
                    # NGAY SAU KHI ham tren "thanh cong" (khong bi abort), code truoc day van coi la
                    # da toi diem quai roi dung yen danh, du thuc te dang o SAI MAP - watchdog displaced
                    # (ben duoi, trong vong giu song) co grace 60s sau reform nen KHONG bat kip ngay,
                    # phai cho ~90s (watchdog relogin khac) moi phat hien. Kiem tra ngay tai day, KHONG
                    # cho watchdog, de phan ung tuc thi.
                    if c.current_map != sc:
                        log.warning("[%s] (LEADER) toi diem quai NHUNG dang o SAI MAP (%s != %s, co the bi "
                                    "tran chien xen giua day di) -> de DIEU PHOI ra lenh gom",
                                    label, c.current_map, sc)
                        c.flee_mode = True
                        return False
                    _set_train_block_stats_spot(spot, enabled=True)
                    c.combat_ready(); c.flee_mode = False   # toi noi -> TAT flee -> dung cay danh
                    log.info("[%s] (LEADER) ra diem quai %s -> dung cay danh.", label, spot)
                elif is_digioi:
                    if _dg_gather_giveup():
                        # Co acc KHAC het gio DG -> party khong gom duoc nua -> KHONG chay long vong
                        # danh 1 minh (de chet vi khong co party hoi mau). DUNG YEN burn time trong DG
                        # den khi het gio cua chinh minh -> ca party xong -> sang train (theo y user).
                        c.flee_mode = True
                        try: c.stop_run_around()
                        except Exception: pass
                        log.info("[%s] (LEADER) DG+Train: da co acc het gio DG -> DUNG YEN trong DG "
                                 "cho het gio (khong chay long vong danh le)", label)
                    else:
                        c.combat_ready(); c.flee_mode = False
                        c.start_run_around()        # DG: chay long vong tim quai
                        log.info("[%s] (LEADER) bat dau chay long vong.", label)
                else:
                    # city/stand: chi set QS, DUNG YEN (cho ban dieu khien tay di nhiem vu)
                    c.flee_mode = False
                    log.info("[%s] (LEADER) %s -> party da tu, DUNG YEN cho dieu khien tay", label, mode)
                return True
            _joined = joined_member_count(pidx)
            # Android party: TUYET DOI khong ra bai khi moi co mot member. Cho DU TOAN BO member
            # da khai bao, cung map/kenh va roster server da join, roi leader moi keo ca doi di.
            _can_start = (_joined >= st["n_members"])
            if _dg_solo_bail:
                pass   # DG giveup -> khong train party, da set solo o tren -> roi xuong main loop DG
            elif _can_start:
                time.sleep(1)
                training_started = _danh_dau_training(st, bool(_start_training()))
            else:
                # 0 member -> KHONG co quan su -> DUNG YEN ngam canh, KHONG danh (vo nghia, het SP).
                # Vong keepalive moi 60s se MOI LAI; co member join thi moi bat dau train.
                c.flee_mode = True   # ne battle neu lo dinh -> khong danh khi chua co QS
                log.info("[%s] (LEADER) chua co member (0 quan su) -> DUNG YEN cho member join...",
                         label)
        else:
            if has_leader:
                # CHO VO HAN leader moi vao party (khong timeout 120s -> tranh member tuong da join
                # roi danh le). Leader dang reconnect -> chua moi -> member dung cho o safe.
                while not st["invited"].is_set():
                    _nghe_lenh_kenh()   # lenh dieu phoi phai nghe duoc o MOI vong cho
                    if _stopped() or not c.running: break
                    # CHO LEADER MOI thi duoc, nhung CHO MOT LEADER DA TAT thi la cho ma. Doc thang
                    # `account_clients` (cung tien trinh) chu khong doi ai bao (L2). Party 50, 10/09:
                    # ba member nam trong mot vong cho tuong tu SAU IM 6 phut, dieu phoi thi dang ra
                    # lenh gom ma khong ai nghe duoc.
                    _lead_acc = config.PARTY_LEADER_ACC.get(pidx)
                    if _lead_acc:
                        _lead_cli = account_clients.get(_lead_acc)
                        if _lead_cli is None or not getattr(_lead_cli, "running", False):
                            log.warning("[%s] (member) THOI CHO loi moi: LEADER khong con chay -> "
                                        "tra quyen cho dieu phoi", label)
                            break
                    _resync_ck(st, username)   # ep dong bo -> relogin bam leader
                    # Leader BAO gom lai trong luc minh con ket o day -> ra safe NGAY. Vong chinh
                    # (noi co nhanh nghe rally_gen kia) chi chay SAU khi da vao party, nen acc nao
                    # con ket o day la acc DUY NHAT khong nghe thay lenh - dung cai ket cua
                    # honagtba (party 28, 30/08 16:35-16:36): 3/4 member log "leader GOM LAI PARTY",
                    # rieng no khong co dong nao, leader van ket luan "ca party DA ra diem tap ket".
                    if train_on_map and st["rally_gen"] > rally_gen_handled:
                        rally_gen_handled = st["rally_gen"]
                        log.info("[%s] (member) DIEU PHOI: GOM LAI PARTY (dang cho moi) -> ra diem "
                                 "tap ket", label)
                        _ra_rally_gom_lai("leader gom lai party")
                        with st["lock"]:
                            c._rally_gen_da_lam = int(rally_gen_handled)
                    st["invited"].wait(2)
            # DA vao party -> NGUNG flee, DANH tran chung (ca map-train LAN Di Gioi).
            # FLEE trong tran party bi server KICK (vd Tao Thao: member flee -> dis ngay).
            #
            # NHUNG "da vao party" KHONG co nghia la "duoc dung yen danh o BAT CU DAU". Truoc day
            # doan nay khong he kiem map: member vao doi xong la `flee_mode=False` + `combat_ready()`
            # + "dung yen tai safe, tu danh" - ke ca khi no dang o map KHAC han leader va dieu phoi
            # dang ra lenh gom.
            #
            # Ca that 07/09 party 1 (user: "leader ve Giang Lang roi ma 4 dua kia van o map khac,
            # dieu phoi ngu vai lon"):
            #   18:54:23 [xGAx] (LEADER) reform: CHO ca party ve Giang Lăng (1/5) - THIEU:
            #            minhminhmq[map=12003] brubb46677[map=21836] tuyetdo[map=21836]
            #            chihao188[map=12003]
            #   18:54:33 [minh] (member) da vao party - dung yen tai safe, tu danh   <- map 12003
            #   18:56:22 [brub] (member) da vao party - dung yen tai safe, tu danh   <- map 21836
            # Leader phai EP RELOGIN tung dua de "cuu party" (94s, 201s).
            _sai_map = bool(train_on_map and sc
                            and int(getattr(c, "current_map", 0) or 0) != int(sc))
            if _sai_map:
                # Sai map thi KHONG bat danh: cu de `flee_mode` de acc di theo lenh gom. Vong chinh
                # + dieu phoi (VIEC_GOM / reform) lo viec keo no ve.
                c.flee_mode = True
                log.warning("[%s] (member) da vao party NHUNG dang o map %s (can %s) -> KHONG dung "
                            "yen danh, di theo lenh gom", label, c.current_map, sc)
            else:
                c.flee_mode = False
                if event_party_mode:
                    c.combat_ready()
                    log.info("[%s] (member) 40NPC: da vao du party -> dung theo leader va tu danh",
                             label)
                elif train_on_map:
                    if st.get("mob_spot"):
                        c.combat_ready()   # map thuong: combat-active de quai aggro (DG khong can)
                    else:
                        c.flee_mode = True
                        log.warning("[%s] (member) map %s khong co tam bai quai -> khong bat combat",
                                    label, sc)
            if has_leader:
                if not _sai_map:
                    log.info("[%s] (member) da vao party - dung yen tai safe, tu danh", label)
            else:
                log.info("[%s] (member) KHONG co bot-leader -> dung yen tai safe (kenh %s), "
                         "auto-accept - CHO ban moi party tay", label, st.get("channel"))

        # --- Giu song ---
        out_cnt = 0
        _dg_gp_out_since = None   # moc bat dau ket NGOAI DG lien tuc (ep het gio neu keo dai)
        last_remove = time.time()
        last_retry = time.time()
        # (XOA 13/09 `_last_regroup`: rate-limit cho viec LEADER tu ra lenh gom - viec do da
        #  chuyen han cho dieu phoi, chi con lai cai dong hoen ri nay.)
        _thieu_since = 0.0    # tu luc nao party THIEU nguoi (0 = dang du) - de gom lai khi mat party
        _last_gom = 0.0       # rate-limit moi lai khi mat party giua train
        last_dg = 0.0
        last_combat = time.time()   # lan cuoi thay in_combat -> de RE-ARM combat khi ket
        last_rearm = 0.0
        _bt_truoc = False   # in_battle nhip truoc -> bat CANH LEN de dem tran tai safe
        last_relogin = time.time()  # lan cuoi RELOGIN-recovery (ket o bai 90s khong battle)
        relogin_cnt = 0
        displaced_cnt = 0           # so lan lien tiep thay KHAC map train (chet/hoi sinh/bi dump)
        last_reform = time.time()   # lan cuoi REFORM party (grace de khong trigger lien tuc o thanh)
        boss_reform_pending = False # da trigger reform de danh boss QD (chua) - tranh spam reform_gen
        # MUA HP/SP giua phien TRAIN: login da check 1 lan; sau do 2h check lai. buy_hp_sp tu doc
        # kho, du nguong -> khong di (khong roi map). Khi thieu -> acc DI TRAC QUAN mua -> off-map ->
        # reform san co keo CA PARTY ve thanh cho -> xong re-form train tiep (theo yeu cau user).
        next_buy_hpsp = time.time() + 7200
        # NHAN QUA nhiem vu hang ngay DINH KY (1h/lan). Cac o hoan thanh MUON (o5 pho ban to doi,
        # o9 danh 50 tran - xong trong luc train, hang gio sau) lam hang/cot du dieu kien SAU moc
        # claim luc login -> truoc day khong ai nhan -> reset ngay la MAT qua (ke ca qua TONG KET).
        # heavy=False: CHI query + claim, KHONG di lam nhiem vu, KHONG goi lai hook pho ban to doi.
        next_daily_claim = time.time() + 3600
        # reform_gen tang dan trong qua trinh toi-map/reconnect/keo qua cong (moi displaced/sai-map
        # += 1). Neu init handled=0 thi ngay khi bat dau train, keepalive thay reform_gen (cao) >
        # handled(0) -> REFORM NGAY du party VUA lap xong + moi acc dang dung dung map (bug that:
        # log 15:42 - party vua "da vao party" la bi xe "REFORM gen 5" tu rac reform cu).
        #  - Acc DANG dung dung train map (sc): moi reform_gen truoc do da resolved -> lay CURRENT
        #    de KHONG replay (tranh xe party vua lap).
        #  - Acc con LECH map (bi DUMP that luc setup): giu 0 de keepalive reform don no NGAY.
        #  - NHUNG neu DIEU PHOI DANG RA LENH gom/dong bo thi KHONG duoc nuot: lenh do chua thi
        #    hanh xong (party van dang o nhieu map - chinh vi the no moi con hieu luc).
        #    Ca that party 7, 11/09 (user: "dieu phoi thay 2 map khac nhau va gom lai nhung deo
        #    thay ai tap trung"):
        #      11:58:26 [party 7] gen 4: viec=gom - party o 3 MAP khac nhau -> REFORM gen -> 2
        #      11:59:12 [ttmuoi] (member) da vao party - dung yen tai safe, tu danh
        #      11:59:17 [ttmuoi] (member) L0: DIEU PHOI ra lenh 'gom' -> DUNG viec chinh, theo lenh
        #      ... im hoan toan 2 phut, khong mot buoc di chuyen ...
        #    Ba member vua duoc leader keo toi dung train map roi vao vong chinh -> dieu kien
        #    `c.current_map == sc` dung -> `reform_gen_handled = 2` = NUOT lenh gom gen 2. Chung
        #    nhan lenh "dung viec chinh" (nhanh L0) nhung khong bao gio chay `_do_reform` de di gom.
        #    Leader va mot member di ve thanh, ba dua con lai dung im -> khong bao gio tap trung.
        #
        # XOA (13/09) quyen NUOT LENH cua acc. Ban cu: `if c.current_map == sc` thi nhay
        # `reform_gen_handled` len thang `st["reform_gen"]` - tuc acc tu ket luan "toi dang dung
        # dung map, vay may lenh dang treo kia khong con y nghia" va vut sach chung. Do la acc tu
        # quyet LENH NAO NGHE, LENH NAO NUOT - nang hon moi thu khac, vi no vo hieu hoa chinh
        # duong ra lenh.
        #
        # Vet va mai khong het cung vi the: ca party 7 duoi day duoc va bang `_dang_co_lenh_gom`
        # (chi chua mot truong hop), roi lenh o dang khac lai bi nuot tiep.
        #
        # GIO: chi nuot dung nhung gen CHINH ACC NAY DA THI HANH trong startup (`_do_reform`
        # xong thi ghi lai `startup_reform_gen_handled`). Con lai deu la lenh chua ai lam - phai
        # thi hanh, khong duoc suy luan tu trang thai rieng cua minh.
        with st["lock"]:
            reform_gen_handled = startup_reform_gen_handled
        with st["lock"]:
            cmd_gen_handled = st["cmd_gen"]   # lenh thu cong (GUI) da xu ly
            _pending_cmd = st.get("cmd")
            # Neu acc reconnect/login dung luc GUI vua phat lenh DI MAP, thread moi khong duoc coi
            # cmd_gen hien tai la "da xu ly". Khong thi acc do khong report AAA, ca party cho thieu
            # nguoi roi tuong sai map -> teleport/relogin lung tung.
            if ((_pending_cmd and _pending_cmd[0] in ("route", "train")
                    and not st["manual_route_done"].is_set())
                    or (_pending_cmd and _pending_cmd[0] == "daily"
                        and st.get("daily_active"))):
                cmd_gen_handled = max(0, cmd_gen_handled - 1)
        disc_gen_handled = st["disc_gen"] # RECONNECT: gen disconnect da xu ly (init = hien tai)
        resync_gen_handled = st["resync_gen"]  # RESYNC party (event 40NPC): gen da xu ly
        # (rally_gen_handled da khoi tao SOM o tren - KHONG lam moi o day, lam moi la nuot mat
        #  lenh gom ma leader vua phat trong luc minh con dang o phan startup.)

        def _moi_theo_dieu_phoi(train_mode, ly_do=""):
            """LEADER moi party - nhung THEO LENH DIEU PHOI, khong tu quyet.

            THAY cho hai vong moi TRAN cu (`while joined_member_count(...) < n_members` chi co loi
            ra la mat ket noi / Stop). Chinh hai vong do da giet party 19 trong 2 gio 42 phut:
            leader BIET member lech map (no in "lech map live 12001!=12003" 488 lan) nhung khong
            co dong nao bien cai biet do thanh hanh dong, va ep dong bo cung khong pha duoc vi
            vong do khong doc reform_gen lan sync_epoch.

            Gio: moi vong deu hoi lai dieu phoi. Dieu phoi bao GOM thi thoi moi (no da bump
            reform_gen, luong nay quay ve keepalive de di gom). Het gio DG thi bao xong DG cho ca
            party. Va van con day du loi ra ma cac vong cho khac trong file da co.
            """
            g0 = st["reform_gen"]
            t0 = time.time()
            while joined_member_count(pidx) < st["n_members"]:
                _nghe_lenh_kenh()   # lenh dieu phoi phai nghe duoc o MOI vong cho
                if not c.running or _stopped():
                    return False
                # 1) EP DONG BO (GUI/watchdog) - vong cu KHONG doc cai nay nen ep bao nhieu cung tro.
                _resync_ck(st, username)
                # 2) DIEU PHOI da chuyen huong (gom/doi pha) -> bo moi, ve keepalive thi hanh.
                if st["reform_gen"] > g0:
                    log.info("[%s] (LEADER) dieu phoi doi huong trong luc moi party -> bo moi",
                             label)
                    return False
                _kh = _ke_hoach(st)
                if _kh and _kh.get("viec") == VIEC_GOM:
                    log.info("[%s] (LEADER) dieu phoi bao GOM (%s) -> thoi moi, di gom truoc",
                             label, _kh.get("ly_do") or "lech map/kenh")
                    return False
                # KHONG xet `viec != VIEC_MOI` o day. Toi tung them dieu kien do 10/09 va no
                # giet party 1 ngay trong dem: ke hoach dao dong moi 2 giay
                #     gen 28 viec=lam | gen 29 viec=moi | gen 30 viec=lam | gen 31 viec=gom
                # nen leader vao vong moi luc ke hoach dang la `lam` -> thoi moi ngay -> KHONG BAO
                # GIO moi duoc. Bon member dung o thanh 21011, leader di route mot minh.
                #
                # `VIEC_LAM` KHONG co nghia "cam moi" - no co nghia "khong co viec cap party phai
                # lam"; party chua du thi van phai moi. Chi `VIEC_GOM` moi la lenh doi huong.
                #
                # Mode khong co viec lap doi (loan dau) da duoc chan tu goc: `n_members = 0` nen
                # vong `while joined_member_count(pidx) < st["n_members"]` khong chay lan nao.
                # 3) HET GIO DG -> bao xong DG cho ca party, dung bat 4 dua kia cho mot thang
                #    da het gio (dung loi da lam party 19 ket).
                if _finish_digioi_train_if_time_over(ly_do or "moi party"):
                    return False
                # 4) Co acc KHAC da het gio DG -> khong gom party DG nua.
                if _dg_gather_giveup():
                    log.info("[%s] (LEADER) co acc het gio DG -> thoi moi party DG", label)
                    return False
                try:
                    if train_mode:
                        _invite_party_participants(c, True, gap=1.0)
                    else:
                        c.invite_members(gap=1.0)
                except Exception:
                    pass
                # 5) TRAN THOI GIAN: qua han ma van thieu -> tu gom mot lan roi thu lai, thay vi
                #    quay tit. (Nguong dung chung voi vong "cho member san sang".)
                if time.time() - t0 > READY_WAIT_REFORM_SEC:
                    t0 = time.time()
                    return False
                time.sleep(4)
            return True

        def _do_manual_cmd(cmd):
            """Thuc thi LENH THU CONG tu GUI (doi kenh / teleport thanh) -> roi TIEP TUC che do da
            setup. Huy party cu truoc, lam hanh dong, roi resume theo mode."""
            nonlocal mode, sc, tm, train_on_map, train_safes
            kind = cmd[0]

            if kind == "train":
                # Chuyen CHINH runtime cua thread hien tai sang train. Neu chi chay route mot lan
                # ma de bien local la stand/sc=0, keepalive sau do se khong bao ve party, va lan
                # reconnect ke tiep lai dung im o thanh.
                sc = int(cmd[2])
                mode = "train"
                tm = (getattr(config, "TRAIN_MAPS", {}) or {}).get(sc)
                train_on_map = tm is not None
                train_safes = []
                if tm is not None:
                    _safe_live = _resolve_train_safe(c, sc, tm.get("safe", []))
                    if _safe_live is not None:
                        train_safes.append(_safe_live)

            def _manual_route_reset(gen):
                with st["lock"]:
                    # Thread cua lenh CU co the chay cham hon lenh moi. Tuyet doi khong cho no
                    # quay lui generation va xoa report AAA/cac Event cua lenh moi (log Android
                    # 16/09: START TRAIN gen moi dang 1/2 report bi thread cu reset thanh 0/2).
                    current_gen = int(st.get("manual_route_gen", 0) or 0)
                    if current_gen >= int(gen):
                        return
                    st["manual_route_gen"] = gen
                    st["manual_route_plan"] = None
                    st["manual_route_source_results"] = {}
                    st["manual_route_city_arrived"] = {}
                    st["manual_route_plan_ready"].clear()
                    st["manual_route_source_done"].clear()
                    st["manual_route_party_ready"].clear()
                    st["manual_route_done"].clear()

            def _wait_event(event, desc, timeout=None, gate_follow=False):
                t0 = time.time()
                saw_route_battle = False
                continue_until = 0.0
                last_continue = 0.0
                logged_continue = False
                while not event.wait(0.5 if gate_follow else 1.0):
                    if not c.running or _stopped():
                        return False
                    if int(st.get("cmd_gen", 0) or 0) != int(cmd_gen_handled):
                        log.info("[%s] manual route: co lenh moi gen=%s (dang xu gen=%s) -> bo "
                                 "vong cho cu %s", label, st.get("cmd_gen"), cmd_gen_handled, desc)
                        return False
                    if timeout is not None and time.time() - t0 > timeout:
                        log.warning("[%s] manual route: cho %s qua %.0fs -> bo qua",
                                    label, desc, timeout)
                        return False
                    if gate_follow:
                        now = time.time()
                        if c.state.in_battle or c.in_combat(idle_secs=1.0):
                            saw_route_battle = True
                            continue_until = 0.0
                            continue
                        if saw_route_battle:
                            grace_until = max(
                                getattr(c, "_battle_end_grace_until", 0.0),
                                getattr(c, "_genuine_end_seen", 0.0) + 4.0,
                            )
                            if now < grace_until:
                                continue
                            if continue_until <= 0:
                                continue_until = now + 12.0
                            if now > continue_until:
                                saw_route_battle = False
                                continue
                            if now - last_continue >= 1.0:
                                if not logged_continue:
                                    log.info("[%s] manual route: member vua xong battle cong -> bam tiep thoai de theo leader",
                                             label)
                                    logged_continue = True
                                try:
                                    c.send(0x14, b"\x06\x00")
                                except Exception:
                                    return False
                                last_continue = now
                return True

            def _wait_manual_city_arrived(expected, timeout=150):
                # MIEN TRU vong cho diec: day la CHE DO TAY (user tu bam route), dieu phoi khong
                # can thiep, va vong co timeout huu han roi tra False - khong ket vinh vien duoc.
                t0 = time.time()
                while True:
                    if not c.running or _stopped():
                        return False
                    with st["lock"]:
                        n = len(st.get("manual_route_city_arrived", {}))
                    if n >= expected:
                        return True
                    if time.time() - t0 > timeout:
                        log.warning("[%s] manual route: cho ca party ve thanh tap ket (%d/%d) qua %.0fs",
                                    label, n, expected, timeout)
                        return False
                    time.sleep(1)

            # MIEN TRU vong cho diec: CHE DO TAY + timeout huu han (xem _wait_manual_city_arrived).
            def _manual_route_all_at_source(source, expected, timeout=30):
                with st["lock"]:
                    st.setdefault("manual_route_source_results", {})[username] = (
                        c.current_map == source
                    )
                t0 = time.time()
                while True:
                    if not c.running or _stopped():
                        return None
                    if int(st.get("cmd_gen", 0) or 0) != int(cmd_gen_handled):
                        log.info("[%s] manual route: co lenh moi trong luc report AAA -> bo lenh cu",
                                 label)
                        return None
                    with st["lock"]:
                        results = dict(st.get("manual_route_source_results", {}))
                    any_bad = any(v is False for v in results.values())
                    enough = len(results) >= expected
                    if any_bad or enough:
                        return enough and not any_bad and all(results.values())
                    if time.time() - t0 > timeout:
                        log.warning("[%s] manual route: chua du acc report AAA=%s (%d/%d), "
                                    "cho tiep de tranh teleport tap ket oan",
                                    label, source, len(results), expected)
                        t0 = time.time()
                    time.sleep(0.5)

            def _do_manual_route():
                gen = cmd_gen_handled
                source_req = int(cmd[1] or 0)
                dest = int(cmd[2])
                _manual_route_reset(gen)
                # LENH "di map AAA->BBB" LUON can 1 nguoi KEO. Party setting "khong co chu PT"
                # (has_leader=False) thi khong ai la leader -> khong ai lap plan -> ca lu dung im.
                # -> cho PICKER dong vai leader RIENG cho lenh nay (lap party, keo di), den noi
                # thi GIAI TAN party + dung yen o map dich (dung nhu setting khong-chu-PT).
                route_leader = is_leader or (not has_leader and is_picker)
                if route_leader:
                    source = source_req
                    if not source:
                        picked = c.nearest_smart_city(dest, exclude_map=dest)
                        if not picked:
                            log.warning("[%s] manual route: khong tim duoc thanh gan map dich %s",
                                        label, dest)
                            st["manual_route_plan_ready"].set()
                            st["manual_route_done"].set()
                            return
                        source = int(picked["city"])
                    gather = c.nearest_smart_city(source)
                    if not gather:
                        log.warning("[%s] manual route: khong tim duoc thanh gan map bat dau %s",
                                    label, source)
                        st["manual_route_plan_ready"].set()
                        st["manual_route_done"].set()
                        return
                    users = (list(st.get("manual_train_users") or []) if kind == "train"
                             else _active_party_usernames(pidx))
                    if not users:
                        users = _active_party_usernames(pidx)
                    expected = max(1, len(users))
                    plan = {
                        "source": int(source),
                        "dest": int(dest),
                        "city": int(gather["city"]),
                        "flag": int(gather["flag"]),
                        "expected": int(expected),
                        "users": list(users),
                    }
                    with st["lock"]:
                        st["manual_route_plan"] = plan
                    st["manual_route_plan_ready"].set()
                    log.info("[%s] manual route plan: source=%s dest=%s gather_city=%s flag=%s expected=%s",
                             label, source, dest, gather["city"], gather["flag"], expected)
                if not _wait_event(st["manual_route_plan_ready"], "leader lap route plan", timeout=60):
                    return
                with st["lock"]:
                    plan = dict(st.get("manual_route_plan") or {})
                if not plan:
                    return

                source = int(plan["source"])
                dest = int(plan["dest"])
                gather_city = int(plan["city"])
                gather_flag = int(plan["flag"])
                expected = max(1, int(plan.get("expected", 1)))
                users = list(plan.get("users") or _running_party_usernames(pidx))
                all_at_source = _manual_route_all_at_source(source, expected)
                if all_at_source is None:
                    return
                if not all_at_source:
                    c.flee_mode = True
                    try:
                        c.pre_route_town_hop()
                        c.go_to_town(gather_city, gather_flag)
                    except Exception as e:
                        log.warning("[%s] manual route: loi ve thanh tap ket %s: %s",
                                    label, gather_city, e)
                    with st["lock"]:
                        st.setdefault("manual_route_city_arrived", {})[username] = True
                    _wait_manual_city_arrived(expected)
                else:
                    log.info("[%s] manual route: ca party da o map AAA=%s -> lap party tai cho",
                             label, source)

                do_channel_sync()
                if route_leader:
                    log.info("[%s] manual route: xong sync kenh -> lap party tam de keo map",
                             label)
                if expected > 1:
                    if route_leader:
                        reset_party_joined(pidx)
                        log.info("[%s] manual route: bat dau moi party tam, can %d member join",
                                 label, expected - 1)
                        last_inv_log = 0.0
                        # Cung bo loi ra nhu `_moi_theo_dieu_phoi`: vong nay truoc day cung chi
                        # thoat khi mat ket noi/Stop -> ep dong bo khong pha duoc, va lech map thi
                        # moi mai (dung benh da giet party 19).
                        _mr_g0 = st["reform_gen"]
                        _mr_t0 = time.time()
                        while joined_member_count(pidx) < expected - 1:
                            _nghe_lenh_kenh()   # lenh dieu phoi phai nghe duoc o MOI vong cho
                            if not c.running or _stopped():
                                return
                            _resync_ck(st, username)
                            if st["reform_gen"] > _mr_g0:
                                log.info("[%s] manual route: dieu phoi doi huong -> bo moi", label)
                                return
                            if time.time() - _mr_t0 > READY_WAIT_REFORM_SEC:
                                log.warning("[%s] manual route: %ds chua du party (%d/%d) -> de DIEU PHOI ra lenh gom",
                                            label, int(READY_WAIT_REFORM_SEC),
                                            joined_member_count(pidx) + 1, expected)
                                return
                            try:
                                c.invite_members(gap=1.0)
                            except Exception:
                                pass
                            if time.time() - last_inv_log > 10:
                                log.info("[%s] manual route: CHO DU PARTY roi moi keo, joined=%d/%d",
                                         label, joined_member_count(pidx), expected - 1)
                                last_inv_log = time.time()
                            time.sleep(4)
                        _invite_whitelist_followers_if_bot_party_ready(c, st, pidx, label, force=True)
                        try:
                            c.set_party_strategist()
                        except Exception:
                            pass
                        st["manual_route_party_ready"].set()
                        log.info("[%s] manual route: party joined=%d/%d",
                                 label, joined_member_count(pidx), expected - 1)
                    else:
                        _wait_event(st["manual_route_party_ready"], "leader lap party", timeout=None)

                route_completed = False
                if route_leader:
                    route_restart = {"needed": False, "reason": ""}

                    def _route_retry(reason):
                        route_restart["needed"] = True
                        route_restart["reason"] = reason
                        with st["lock"]:
                            st["cmd"] = ("route", source_req, dest)
                            st["cmd_gen"] += 1
                        log.warning("[%s] manual route: %s -> keo lai tu dau (gen %d)",
                                    label, reason, st["cmd_gen"])

                    def _party_maps_bad():
                        if expected <= 1:
                            return None
                        leader_map = c.current_map
                        if leader_map is None:
                            return None
                        bad = []
                        for u in users:
                            cc = account_clients.get(u)
                            if cc is None or not getattr(cc, "running", False):
                                bad.append(f"{u}:off")
                                continue
                            cm = getattr(cc, "current_map", None)
                            if cm is not None and cm != leader_map:
                                bad.append(f"{u}:{cm}")
                        return ", ".join(bad) if bad else None

                    bad_since = {}

                    def abort():
                        if _stopped() or not c.running:
                            return True
                        bad = _party_maps_bad()
                        now = time.time()
                        if _route_mismatch_timed_out(
                            bad_since, c.current_map, bad, now, timeout=30.0
                        ):
                            _route_retry(
                                f"co acc lech map khi dang keo (leader map={c.current_map}, {bad})"
                            )
                            return True
                        return False

                    def _wait_party_map(target_map, timeout=20.0):
                        if expected <= 1:
                            return True
                        t_wait = time.time()
                        while time.time() - t_wait < timeout:
                            bad = _party_maps_bad()
                            if not bad and c.current_map == target_map:
                                return True
                            time.sleep(1)
                        _route_retry(
                            f"party chua cung map {target_map} sau khi keo (leader map={c.current_map}, {bad or 'unknown'})"
                        )
                        return False

                    route_flee = expected <= 1
                    if not route_flee:
                        c.flee_mode = False
                        try:
                            c.combat_ready()
                        except Exception:
                            pass
                    if c.current_map != source:
                        log.info("[%s] manual route: keo party toi AAA=%s truoc", label, source)
                        ok_source = c.follow_smart_route(source, None, abort=abort, flee=route_flee)
                    else:
                        ok_source = True
                    if not route_restart["needed"] and ok_source and c.current_map == source:
                        _wait_party_map(source)
                    st["manual_route_source_done"].set()
                    if (not route_restart["needed"]) and c.current_map == source:
                        log.info("[%s] manual route: keo party AAA=%s -> BBB=%s",
                                 label, source, dest)
                        ok_dest = c.follow_smart_scene_route(source, dest, None, abort=abort, flee=route_flee)
                        if not route_restart["needed"] and ok_dest and c.current_map == dest:
                            _wait_party_map(dest)
                            route_completed = not route_restart["needed"] and c.current_map == dest
                        elif not route_restart["needed"]:
                            _route_retry(f"leader chua toi BBB={dest} (dang map {c.current_map})")
                    elif not route_restart["needed"]:
                        log.warning("[%s] manual route: chua toi AAA=%s (dang map %s) -> khong keo BBB",
                                    label, source, c.current_map)
                        _route_retry(f"leader chua toi AAA={source} (dang map {c.current_map})")
                    st["manual_route_done"].set()
                else:
                    _wait_event(st["manual_route_source_done"], "leader keo toi AAA", timeout=None, gate_follow=True)
                    _wait_event(st["manual_route_done"], "leader keo toi BBB", timeout=None, gate_follow=True)
                    route_completed = c.current_map == dest
                c.flee_mode = False
                # Party "khong co chu PT" chi lap party TAM de keo -> den noi thi GIAI TAN,
                # moi acc dung yen tai map dich (dung nhu setting).
                if not has_leader:
                    try:
                        c.leave_party()
                        if route_leader:
                            reset_party_joined(pidx)
                        if route_completed:
                            log.info("[%s] manual route: den BBB -> giai tan party tam (khong chu PT), dung yen", label)
                        else:
                            log.info("[%s] manual route: chua den BBB -> giai tan party tam de retry/dung yen", label)
                    except Exception as e:
                        log.warning("[%s] manual route: loi giai tan party tam: %s", label, e)

            # KET BATTLE: cho thoat tran TRUOC khi doi kenh/teleport (switch_channel/leave_party
            # giua battle de bi server bo qua/loi). cap 60s.
            #
            # LENH DOI KENH KHONG DI QUA DAY: no co thu tu rieng, bat buoc theo user chot 30/08 -
            # "lead CHAY RA SAFE, GIAI TAN pt va ca lu chuyen kenh yeu cau". Doan nay giai tan
            # NGAY khi vua nhan lenh (truoc ca khi ra safe) nen party tan giua bai quai, va bat
            # flee_mode du party van con DU. Log 31/08 09:56:12 (party 19): `-> LENH THU CONG
            # ('channel', 1)` roi NGAY dong sau la `Roi/giai tan party cu`, mai 09:56:13 moi toi
            # khoi doi kenh va bao "VAN dang trong tran".
            if kind != "channel":
                c.flee_mode = True
            t0 = time.time()
            while c.in_combat(idle_secs=3.0):
                if not c.running or _stopped() or time.time() - t0 > 60:
                    break
                time.sleep(0.5)
            if kind not in ("channel", "daily") and (is_leader or (kind == "route" and not has_leader)):
                c.leave_party(); reset_party_joined(pidx)   # huy party cu
            if kind == "channel":
                ch = cmd[1]
                ok = False
                # GHIM kenh user chon: sau khi doi xong, pha sync kenh cua picker se chay tiep va
                # TU CHON "kenh it nguoi nhat" -> keo ca party sang kenh khac, phu dinh lenh tay
                # (log 31/08 09:57 party 16: chon kenh 1 -> 09:58:22 picker tu sang kenh 2).
                with st["lock"]:
                    st["kenh_ghim"] = int(ch) if ch else None
                # DANG TRAIN thi party danh lien tuc: vong cho o tren cap 60s roi BREAK va van
                # gui switch_channel GIUA TRAN -> server bo qua ma bot van bao "da doi kenh".
                # User 27/08: "dang train ma bam doi kenh thi phai ket thuc tran, chay ra diem an
                # toan roi doi kenh chu". -> KIEN TRI: het tran THAT -> ra safe -> doi; chua dut
                # duoc tran thi THU LAI, KHONG doi kenh giua tran va KHONG bao thanh cong.
                # KIEN TRI toi 5 phut chu khong bo sau vai lan: dang train thi tran noi tiep tran,
                # vai lan thu dau gan nhu chac chan roi vao giua tran. Bo som = lenh cua user bi
                # nuot im. Chi bo som khi server bao kenh KHONG TON TAI (2) / KENH DAY (4) - thu
                # lai cung the, de vong sync kenh chon kenh khac.
                _han = time.time() + 300
                _lan = 0
                # THU TU BAT BUOC (user chot 30/08): "lead chay ra safe, giai tan pt VA CA LU
                # chuyen kenh yeu cau". Member DI TRUOC LA CHET: trong party member tu di theo
                # leader nen vi tri THAT cua no la cho leader dung; chua giai tan ma da gui move
                # theo pos cu = server thay nhay ca nghin don vi -> `di chuyen QUA XA (ma 14)`.
                # Log 30/08 22:15:19-22: lenh doi kenh luc 22:15:19, member bat dau di 22:15:21 va
                # RUNG NGAY, con leader mai 22:15:24 moi giai tan party.
                #
                # Cua nay chot theo SO LENH (`cmd_gen`), KHONG theo mot Event dung chung: leader
                # xoa co o dau lenh moi, ma member co the doc co TRUOC khi leader kip xoa -> di
                # ngay bang co con SOT tu lenh truoc.
                # Log 31/08 party 8: lenh doi kenh 10:49:23; 10:49:25 lubbon in "CHO leader ra
                # safe..." roi DI LUON (10:49:26 `Doi kenh OK -> 1`), trong khi leader lbumot mai
                # 10:49:26 moi nhan lenh va 10:49:50 moi giai tan. Server tu choi ma 3 (DANG TO
                # DOI) nen lubbon TU roi party -> party rach giua luc leader con dang danh.
                if is_leader:
                    st["cmd_leader_xong"].clear()
                    with st["lock"]:
                        st["cmd_leader_xong_gen"] = None
                elif has_leader:
                    log.info("[%s] (member) manual: CHO leader ra safe + giai tan party truoc khi "
                             "doi kenh %d...", label, ch)
                    _t_cho_lead = time.time()
                    # CHO-HOP-LE: RANG BUOC THU TU CUA GAME, khong phai cho bao cao. Member di
                    # truoc khi leader giai tan doi thi vi tri THAT cua no la cho leader dung ->
                    # gui move theo pos cu = `di chuyen QUA XA (ma 14)` (log 30/08 22:15). Co han
                    # 90s va thoat ngay khi co lenh tay moi.
                    while st.get("cmd_leader_xong_gen") != cmd_gen_handled:
                        if not c.running or _stopped():
                            break
                        if st.get("cmd_gen", 0) != cmd_gen_handled:
                            break                      # co lenh tay moi -> bo lenh nay
                        if time.time() - _t_cho_lead > 90:
                            log.warning("[%s] (member) manual: cho leader qua 90s -> tu xu ly", label)
                            break
                        st["cmd_leader_xong"].wait(1.0)
                while time.time() < _han:
                    _lan += 1
                    if not c.running or _stopped():
                        break
                    # Co lenh tay MOI (user bam cai khac) -> bo lenh cu, khong giu cho nua.
                    if st.get("cmd_gen", 0) != cmd_gen_handled:
                        log.info("[%s] (%s) manual: co lenh moi -> bo lenh doi kenh %d dang thu",
                                 label, role, ch)
                        break
                    # DANH XONG TRAN roi moi di (user chot 30/08). KHONG bo chay: party VAN CON DU
                    # (giai tan la buoc sau) - du party thi viec gi phai chay, cu danh cho xong.
                    # Bo chay con cat tran nua chung dung luc server dang giai quyet, roi lenh move
                    # ngay sau do thanh `di chuyen QUA XA (ma 14)` - ca 4 acc bay cung mot giay
                    # (log 30/08 21:45 va 22:15).
                    c._wait_combat_clear(idle=2.0, cap=120.0)
                    if c.in_combat(idle_secs=3.0):
                        log.warning("[%s] (%s) manual: VAN dang trong tran -> chua doi kenh %d, "
                                    "thu lai (lan %d, con %.0fs)", label, role, ch, _lan,
                                    _han - time.time())
                        time.sleep(3)
                        continue
                    # DUNG THU TU USER CHOT 30/08: "lead CHAY RA SAFE, GIAI TAN pt va ca lu
                    # chuyen kenh yeu cau". T tung lam NGUOC (giai tan truoc roi moi ra safe) ->
                    # party tan giua bai, khong ai biet leader da toi safe hay chua.
                    #
                    # Leader di ra safe khi party VAN CON: member tu di theo nen ca lu ra cung -
                    # khong con canh "member dung o diem quai cho leader".
                    if not _ra_safe_truoc_khi_doi_kenh("lenh doi kenh tay"):
                        continue                       # chua toi safe -> KHONG giai tan, KHONG doi
                    if c.in_combat(idle_secs=3.0):     # bi keo tran tren duong ra safe
                        log.warning("[%s] (%s) manual: dinh tran luc ra safe -> thu lai (lan %d)",
                                    label, role, _lan)
                        continue
                    # DA O SAFE roi moi GIAI TAN, roi moi tha member di doi kenh.
                    if is_leader:
                        if getattr(c, "party_members", None):
                            log.info("[%s] (LEADER) manual: DA ra safe -> giai tan party roi doi kenh %d",
                                     label, ch)
                            try:
                                c.leave_party(); reset_party_joined(pidx)
                            except Exception as e:
                                log.warning("[%s] (LEADER) manual: loi giai tan party: %s", label, e)
                            time.sleep(2.0)   # cho goi 0x0d sub04 ve de MOI acc resync toa do
                        # Tha member di: het "tu di theo leader", va da co `S:013-004` (kem toa do
                        # that) de sua pos truoc khi ra lenh di.
                        with st["lock"]:
                            st["cmd_leader_xong_gen"] = cmd_gen_handled
                        st["cmd_leader_xong"].set()
                    try:
                        ok = c.switch_channel(ch, theo_lenh=True)
                        time.sleep(1.5)
                    except Exception as e:
                        log.warning("[%s] manual: loi doi kenh: %s", label, e)
                    if ok:
                        # DOI KENH XONG PHAI CHECK LAI DA O SAFE CHUA (user chot 30/08): doi kenh
                        # giu nguyen toa do, nhung o kenh moi cho do co the day quai va party VUA
                        # TAN -> dung le giua bai la an dan ngay. Chua o safe thi ra lai, roi moi
                        # de nhanh duoi lap lai party train o kenh nay.
                        if not _ra_safe_truoc_khi_doi_kenh("sau khi doi kenh %d" % ch):
                            log.warning("[%s] (%s) manual: doi kenh %d xong nhung CHUA ve duoc "
                                        "safe -> vong gom party se xu tiep", label, role, ch)
                        break
                    _res = getattr(c, "_chan_switch_result", None)
                    log.warning("[%s] (%s) manual: doi kenh %d THAT BAI (lan %d, result=%s)",
                                label, role, ch, _lan, _res)
                    if _res in (2, 4, -1):
                        # 2 = khong co kenh do | 4 = kenh DAY | -1 = server IM LANG (timeout het
                        # luot). Thu lai cung the -> de vong sync kenh ben duoi tu chon kenh khac
                        # cho CA PARTY (khong de moi acc mot noi).
                        log.warning("[%s] (%s) manual: kenh %d %s -> BO lenh tay, de sync kenh chon "
                                    "kenh khac cho ca party", label, role, ch,
                                    {2: "khong ton tai", 4: "DA DAY"}.get(_res, "server IM LANG"))
                        break
                    time.sleep(2)
                if ok:
                    with st["lock"]:
                        st["channel"] = int(ch)
                    log.info("[%s] (%s) manual: da doi kenh -> %d", label, role, ch)
                else:
                    # KHONG bo lung: xuong duoi la nhanh "tiep tuc che do" -> _party_tai_cho_xu_ly
                    # se thay party LECH KENH (acc khac doi duoc) va goi do_channel_sync de gom ca
                    # party ve MOT kenh. Tuc lenh tay that bai thi party van dong bo, chi la co the
                    # o kenh khac kenh user chon.
                    log.warning("[%s] (%s) manual: doi kenh %d THAT BAI (%d lan thu) -> GIU kenh cu "
                                "%s, de buoc dong bo kenh gom ca party lai",
                                label, role, ch, _lan, getattr(c, "current_channel", None))
            elif kind == "city":
                cid, flag = cmd[1], cmd[2]
                c.flee_mode = True
                try: c.go_to_town(cid, flag)
                except Exception as e: log.warning("[%s] manual: loi teleport thanh: %s", label, e)
                log.info("[%s] (%s) manual: da teleport ve thanh %s", label, role, cid)
            elif kind == "point":
                # Chi leader di; party dang con nguyen nen game keo member theo leader.
                # Member tuyet doi khong tu navigate de tranh tach doi/xuyen tuong.
                tx, ty = int(cmd[1]), int(cmd[2])
                if is_leader:
                    c._wait_combat_clear(idle=2.0, cap=120.0)
                    log.info("[%s] (LEADER) map tap: keo ca party toi (%d,%d)", label, tx, ty)
                    c.navigate_to(tx, ty, flee=False,
                                  abort=lambda: st.get("cmd_gen", 0) != cmd_gen_handled or _stopped(),
                                  require_smart_path=True)
                elif has_leader:
                    log.info("[%s] (member) map tap: follow leader toi (%d,%d)", label, tx, ty)
            elif kind == "route":
                if mode not in ("stand", "city"):
                    log.warning("[%s] manual route: bo qua vi mode=%s khong phai city/stand",
                                label, mode)
                else:
                    _do_manual_route()
            elif kind == "train":
                # Snapshot chi gom account da online luc bam. Account con connecting khong
                # duoc chen vao barrier route va lam leader cho vo han; online sau se rejoin
                # bang luong dong bo/reconnect binh thuong.
                if st.get("manual_train_users") and username not in st["manual_train_users"]:
                    log.info("[%s] START FARM: online sau snapshot -> cho dong bo voi leader", label)
                    return
                # AUTO BATTLE Android = MOT lenh cho CA TEAM: smart route tu thanh gan nhat,
                # lap party, leader keo toi map dich roi keo tiep toi dung bai train.
                # Bat chien dau NGAY truoc khi roi thanh: gap quai tren duong thi ca team danh
                # xong roi tiep tuc route, khong doi toi khi da dung tai bai moi bat.
                c._ui_auto_battle = True
                c.flee_mode = False
                c.combat_ready()
                tx, ty = int(cmd[3]), int(cmd[4])
                dest = int(cmd[2])
                # Fast-path: neu TOAN BO account online da o dung map train thi khong ep
                # teleport ve thanh nua. Lap PT ngay tai map hien tai roi leader keo ra bai.
                _live_clients = []
                for _u, _p, _l, _k in party_accounts(pidx):
                    if is_account_running(_u):
                        _cl = account_clients.get(_u)
                        if _cl is not None and getattr(_cl, "running", False):
                            _live_clients.append(_cl)
                _all_on_train_map = bool(_live_clients) and all(
                    int(getattr(_cl, "current_map", 0) or 0) == dest for _cl in _live_clients)
                if _all_on_train_map:
                    log.info("[%s] START TRAIN TEAM fast-path: %d account da o map %d -> bo qua phu ve thanh",
                             label, len(_live_clients), dest)
                else:
                    _do_manual_route()
                # Da ra toi map train moi danh gia do dong. Neu co khu khac vang hon va con du
                # cho, ca team ve SAFE, tan PT, chuyen dong bo, leader moi lai DU PT.
                if c.current_map == dest:
                    # Nut BAT DAU FARM chot theo cac account DANG ONLINE luc bam; account cau
                    # hinh nhung offline khong duoc lam ca team cho vo han.
                    expected = max(1, int(st.get("manual_train_expected") or
                                          len([u for u, _p, _l, _k in party_accounts(pidx)
                                               if is_account_running(u)])))
                    if is_leader:
                        chosen = None
                        with st["lock"]:
                            _auto_channel = False
                            _manual_channel = st.get("train_channel_manual")
                        if not _auto_channel:
                            if _manual_channel:
                                chosen = int(_manual_channel)
                                if chosen == int(getattr(c, "current_channel", 0) or 0):
                                    chosen = None
                                    log.info("[%s] START TRAIN TEAM: dang o dung phan khu user chon -> giu nguyen", label)
                                else:
                                    log.info("[%s] START TRAIN TEAM: user chon phan khu %s -> ve SAFE va chuyen team",
                                             label, chosen)
                            else:
                                log.info("[%s] START TRAIN TEAM: chua chon phan khu manual -> giu nguyen khu hien tai", label)
                        else:
                          try:
                            c.request_channel_list()
                            c._chan_event.wait(3.0)
                            rows = dict(getattr(c, "channels", None) or {})
                            current = int(getattr(c, "current_channel", 0) or 0)
                            current_people = rows.get(current, (None, None))[0]
                            candidates = []
                            for ch, values in rows.items():
                                cur, cap = values
                                if cur is None or cap is None or int(ch) == 1:
                                    continue
                                # Team dang o khu hien tai da chiem cho san; khu moi phai con du
                                # cho cho ca team vao.
                                free = int(cap) - int(cur)
                                if int(ch) == current or free >= expected:
                                    candidates.append((int(cur), -free, int(ch)))
                            if candidates:
                                best = min(candidates)[2]
                                best_people = next(v[0] for k, v in rows.items() if int(k) == best)
                                if best != current and (current_people is None or int(best_people) < int(current_people)):
                                    chosen = best
                                    log.info("[%s] START TRAIN TEAM: khu hien tai %s co %s nguoi, khu %s co %s "
                                             "-> ve SAFE, tan PT, chuyen khu vang hon", label, current,
                                             current_people, best, best_people)
                                else:
                                    log.info("[%s] START TRAIN TEAM: khu hien tai %s da la khu phu hop -> giu nguyen",
                                             label, current)
                          except Exception as e:
                            log.warning("[%s] START TRAIN TEAM: khong danh gia duoc do dong phan khu -> giu nguyen: %s",
                                        label, e)
                        with st["lock"]:
                            st["manual_train_channel"] = chosen
                            st["manual_train_channel_ready"].set()
                    else:
                        st["manual_train_channel_ready"].wait(15.0)
                    with st["lock"]:
                        chosen = st.get("manual_train_channel")
                        if chosen:
                            st["auto_best_channel"] = True
                            st["kenh_ghim"] = int(chosen)
                            st["kenh_dich"] = None
                    if chosen:
                        do_channel_sync()
                    # Lap/bo sung PT LUON LUON, ke ca khi khong can doi phan khu.
                    # Truoc day khoi nay nam trong `if chosen`, nen team da o dung map/khu
                    # co the bi leader keo di khi PT chua du.
                    c.set_party_invite_ready(True)
                    if has_leader:
                        if is_leader:
                            if chosen:
                                reset_party_joined(pidx)
                            invite_start = time.time()
                            while joined_member_count(pidx) < expected - 1:
                                if not c.running or _stopped() or time.time() - invite_start > 120:
                                    break
                                try:
                                    c.invite_members(gap=1.0)
                                except Exception:
                                    pass
                                time.sleep(3)
                            try:
                                c.set_party_strategist()
                            except Exception:
                                pass
                        else:
                            wait_start = time.time()
                            while (not is_joined(pidx, c.self_entity) and c.running and not _stopped()
                                   and time.time() - wait_start < 120):
                                time.sleep(1)
                if c.current_map == dest:
                    if is_leader:
                        c._wait_combat_clear(idle=2.0, cap=120.0)
                        ground = c.get_ground_store()
                        safe_target = (ground.nearest_walkable_world(dest, (tx, ty), c.pos)
                                       if ground is not None and c.pos else None)
                        if safe_target is not None:
                            tx, ty = int(safe_target[0]), int(safe_target[1])
                        log.info("[%s] (LEADER) START TRAIN TEAM: keo PT toi bai (%d,%d)",
                                 label, tx, ty)
                        c.navigate_to(tx, ty, flee=True,
                                      abort=lambda: st.get("cmd_gen", 0) != cmd_gen_handled or _stopped(),
                                      require_smart_path=True)
                    elif has_leader:
                        log.info("[%s] (member) START TRAIN TEAM: theo leader toi bai (%d,%d)",
                                 label, tx, ty)
                    c._ui_auto_battle = True
                    c.flee_mode = False
                    c.combat_ready()
            elif kind == "daily":
                tasks = tuple(cmd[1] or ())
                _daily_labels = {"legion_boss": "Boss quân đoàn", "world_boss": "Boss thế giới",
                                 "solo_dungeon": "Phụ bản đơn", "team_dungeon": "Phụ bản tổ đội",
                                 "team_dungeon_20": "Thảo Phạt Thiên Sư • Cấp 20",
                                 "team_dungeon_50": "Ngày Tàn Hoạn Quan • Cấp 50",
                                 "team_dungeon_80": "Đại Chiến Lữ Bố • Cấp 80",
                                 "team_dungeon_110": "Hỏa Thiêu Bộc Dương • Cấp 110"}
                team_levels = {int(x.rsplit("_", 1)[1]) for x in tasks
                               if str(x).startswith("team_dungeon_")
                               and str(x).rsplit("_", 1)[1].isdigit()}
                team_done = False
                log.info("[%s] (%s) DAILY QUEST tay: %s", label, role, tasks)
                c._daily_hold = False
                c._daily_use_selected_pet = True
                _selected_pet = int(getattr(c, "_ui_selected_pet_id", 0) or 0)
                if _selected_pet:
                    try:
                        c._wait_combat_clear(idle=2.0, cap=30.0)
                        c.switch_pet(_selected_pet)
                        log.info("[%s] Daily: dung pet PET & Skill id=%d", label, _selected_pet)
                    except Exception as e:
                        log.warning("[%s] Daily: khong doi duoc pet PET & Skill %d: %s",
                                    label, _selected_pet, e)
                with st["lock"]:
                    st["daily_active"] = True
                    st["daily_phase"] = "running"
                    st["daily_user"] = username
                for task_index, task in enumerate(tasks):
                    if (not c.running or _stopped() or st.get("cmd_gen", 0) != cmd_gen_handled
                            or st.get("daily_cancel", False)):
                        break
                    try:
                        with st["lock"]:
                            st["daily_task"] = task
                            st["daily_message"] = "Đang chạy %s" % _daily_labels.get(str(task), str(task))
                        if task == "legion_boss":
                            set_account_activity(username, "Daily: boss quan doan", phase="daily")
                            # Nut "Chay cac Daily da tick" la lenh thu cong ro rang cua nguoi dung.
                            # Khong de setting boss tu dong (mac dinh tat) chan im lang lenh nay.
                            c.do_legion_boss(force=True)
                        elif task == "world_boss":
                            set_account_activity(username, "Daily: boss the gioi", phase="daily")
                            c.do_world_boss_all(cho_phep=lambda: (
                                "user da bam Dung Daily" if st.get("daily_cancel", False)
                                else None))
                        elif task == "solo_dungeon":
                            set_account_activity(username, "Daily: pho ban don", phase="daily")
                            c.do_daily_dungeon(cho_phep=lambda: (
                                "user da bam Dung Daily" if st.get("daily_cancel", False)
                                else None))
                        elif (task == "team_dungeon" or str(task).startswith("team_dungeon_")) and not team_done:
                            team_done = True
                            set_account_activity(username, "Daily: pho ban to doi", phase="daily")
                            _daily_cfg = dict(pcfg)
                            _daily_cfg["auto_team_dungeon"] = True
                            # Daily tay da co barrier cho TOAN DOI sau boss the gioi. Khong cho
                            # _run_auto_team_dungeons_if_needed doi them wb_done cua luong auto:
                            # luong tay khong set co do nen leader se cho gia vo han/5 phut.
                            _daily_cfg["manual_daily_barrier_done"] = True
                            if team_levels:
                                _daily_cfg["team_dungeons"] = {
                                    lv: lv in team_levels for lv in (20, 50, 80, 110)
                                }
                            _daily_stopped = lambda: (_stopped() or st.get("daily_cancel", False))
                            c._td_stop_requested = lambda: bool(st.get("daily_cancel", False))
                            try:
                                _run_auto_team_dungeons_if_needed(
                                    c, st, username, label, pidx, is_leader,
                                    _daily_stopped, _daily_cfg)
                            finally:
                                c._td_stop_requested = None
                    except Exception as e:
                        log.warning("[%s] daily '%s' loi (bo qua task nay): %s", label, task, e)
                        with st["lock"]:
                            st["daily_message"] = "%s lỗi: %s" % (_daily_labels.get(str(task), str(task)), e)
                    # Hang rao giua cac task: leader co the bo qua boss vi cooldown va toi PB
                    # rat som, trong khi member van dang danh boss/solo. Neu khong cho nhau o
                    # day, leader tao phong va moi account dang o instance khac -> PB doi dung.
                    _step_key = "%d:%s" % (task_index, task)
                    with st["lock"]:
                        st.setdefault("daily_step_done", {}).setdefault(_step_key, set()).add(username)
                    _barrier_t0 = time.time()
                    while c.running and not _stopped() and not st.get("daily_cancel", False):
                        if st.get("cmd_gen", 0) != cmd_gen_handled:
                            break
                        with st["lock"]:
                            _participants = set(st.get("daily_participants") or ())
                            _done_step = set(st.setdefault("daily_step_done", {}).get(_step_key) or ())
                        if _participants.issubset(_done_step):
                            break
                        set_account_activity(username,
                                             "Daily: cho team xong %s (%d/%d)" % (
                                                 _daily_labels.get(str(task), str(task)),
                                                 len(_done_step), len(_participants)),
                                             phase="daily")
                        if time.time() - _barrier_t0 > 1200:
                            log.warning("[%s] Daily barrier %s qua 20 phut (%d/%d) -> di tiep",
                                        label, task, len(_done_step), len(_participants))
                            break
                        time.sleep(1)
                with st["lock"]:
                    cancelled = bool(st.get("daily_cancel", False))
                    pending = st.setdefault("daily_pending", set())
                    pending.discard(username)
                    st["daily_active"] = bool(pending)
                    if pending:
                        st["daily_phase"] = "running"
                        st["daily_message"] = "Con %d account dang chay Daily" % len(pending)
                    else:
                        st["daily_phase"] = "cancelled" if cancelled else "completed"
                        st["daily_message"] = ("Da dung Daily" if cancelled
                                               else "Da chay xong Daily da chon")
                c._daily_use_selected_pet = False
                if cancelled:
                    c._daily_hold = True
                    c._ui_auto_battle = False
                    c.flee_mode = False
                    set_account_activity(username, "Da dung Daily - dung yen tai cho", phase="idle")
                    log.info("[%s] Daily DA DUNG sau tran hien tai -> GIU ONLINE, dung yen tai cho",
                             label)
                    return
                c.flee_mode = False
            # --- TIEP TUC che do da setup ---
            if mode in ("stand", "city"):
                # stand: dung yen. city ('ve thanh dung yen'): KHONG teleport ve thanh setting nua,
                # O LAI thanh/kenh vua chuyen (ngang voi stand). -> chi dung yen.
                c.flee_mode = False
            elif is_digioi and kind != "point":
                # train DG: vao lai DG -> lap party. (kenh da chuyen o tren neu la lenh channel)
                c.flee_mode = True
                try:
                    if not c.in_di_gioi():
                        c.enter_di_gioi_safe()
                except Exception as e: log.warning("[%s] manual: loi vao lai DG: %s", label, e)
                if is_leader:
                    for _ in range(6):
                        if not c.running or _stopped(): break
                        try: c.invite_members(gap=1.0)
                        except Exception: pass
                        time.sleep(4)
                        if joined_member_count(pidx) >= st["n_members"]: break
                    _invite_whitelist_followers_if_bot_party_ready(c, st, pidx, label, force=True)
                    try: c.set_party_strategist()
                    except Exception: pass
                c.combat_ready(); c.flee_mode = False
            elif train_on_map and kind != "point":
                # train map: dua CA party ve bai + lap lai (dung lai flow reform). _do_reform ve thanh
                # gom nhau -> switch dung st['channel'] (da set kenh moi neu lenh channel) -> keo ra spot.
                # Ca party dang o san bai train thi lap lai/sync kenh TAI CHO (user 27/08).
                if not _party_tai_cho_xu_ly("lenh tay"):
                    _do_reform()

        stop_ev = account_stops.get(username)
        # Bao stop_account: ACC NAY khi STOP -> thread TU xu ly (KHONG dong socket ngay).
        #  - leader train: tu chay ve safe gan nhat roi dong.
        #  - member train co bot-leader: CHO leader ve safe (stop_leader_done) roi moi dong
        #    -> ca party thoat cung luc, KHONG bi member thoat truoc.
        if is_leader and train_on_map:
            c._return_safe_on_stop = train_safes
        elif (not is_leader) and train_on_map and has_leader:
            c._wait_leader_on_stop = True
        _exited_tower = False
        while c.running:
            if getattr(c, "_ui_mode_restart", False):
                if c.in_combat(idle_secs=2.0):
                    time.sleep(0.5)
                    continue
                c._ui_mode_restart = False
                with st["lock"]:
                    st.get("ui_mode_restart_users", set()).discard(username)
                # Doc lai mode bang supervisor tren cung socket, nhu chuyen DG -> train.
                _dt["relogin_train"] = True
                account_continue[username] = c
                return
            # Vai tro runtime thay doi sau online leader handover; khong giu tham so
            # is_leader cu suot doi thread, va khong doi username/slot/relogin.
            _previous_leader_role = is_leader
            has_leader = config.PARTY_LEADER_ACC.get(pidx) is not None
            is_leader = username == config.PARTY_LEADER_ACC.get(pidx)
            is_picker = is_leader if has_leader else username == party_accounts(pidx)[0][0]
            role = "LEADER" if is_leader else "member"
            if _previous_leader_role != is_leader:
                c._return_safe_on_stop = train_safes if is_leader and train_on_map else None
                c._wait_leader_on_stop = bool(not is_leader and has_leader and train_on_map)
            if st.get("leader_switch_pending"):
                time.sleep(0.5)
                continue
            # VE PET MAC DINH: vong lap nay chay GIUA cac hoat dong (boss/PB/quest goi tuan tu
            # trong cung thread) nen day la cho tra pet ve vai thuong. Mode event dung chung pet
            # voi quest/PB. ensure_pet_role khong gui goi nao neu dang dung dung pet -> gan nhu
            # mien phi, va switch_pet tu chan khi dang trong tran.
            try:
                c.ensure_pet_role("quest" if mode == "event" else "train")
            except Exception as e:
                log.debug("[%s] tra pet ve vai thuong loi: %s", label, e)
            # 2K KET THUC (thua/xong) -> MOI acc tu di bo ra khoi thap. Trong map event khong
            # teleport duoc nen phai di bo (exit_event -> smart scene route toi out_map).
            # Thieu buoc nay: leader di gom doi vo ich con member dung im trong thap, sync map cho
            # vo han (log 13:03-13:14 sau khi thua o tang 9).
            if (not _exited_tower and ev is not None and st["event_exit_now"].is_set()
                    and _inside_floor_crawl_tower(ev, c.current_map)):
                _exited_tower = True
                log.info("[%s] (%s) 2K ket thuc -> di bo ra khoi thap (%s -> %s) roi THOAT GAME",
                         label, role, config.scene_name(c.current_map),
                         config.scene_name(int((ev.get("exit") or {}).get("out_map") or 0)))
                try:
                    c.exit_event(ev)
                except Exception as e:
                    log.warning("[%s] (%s) 2K: loi di ra khoi thap: %s", label, role, e)
                # Ra xong thi THOAT GAME luon (giong nhanh 40NPC ngoai gio). Dung yen o 12003
                # khong lam gi thi vo ich, va con giu instance/party treo.
                _reason("2K ket thuc -> ra khoi thap -> thoat game")
                c.close()
                return
            # CHU PARTY da thoat (leader_gone) -> member cung THOAT theo (party tan, member o lai vo
            # nghia). TRU MOI CHE DO CHAY SOLO: khong lap party thuc su (moi acc chay doc lap hoan
            # toan) -> "leader" chi la vai tro danh nhan trong config, KHONG lien quan gi den viec
            # cac acc khac co chay duoc hay khong.
            #   - `digioi_solo`      : Di Gioi solo
            #   - `event_stand_mode` : event dung yen cho moi tay
            #   - `event_solo_kind`  : EVENT SOLO VAN TU DANH (loan dau) - THIEU CAI NAY den 05/09.
            #
            # Bug that 05/09 22:21 (party 11, loan dau t7): leader danh xong 1 tran truoc roi thoat,
            # 3 member DANG DANH DO bi keo thoat theo, mat luot:
            #     [luutam] (member) CHU PARTY da thoat -> member thoat theo
            #     [luutam] Loan dau: tran khong thay ket thuc -> dung
            #     [luumuoi] pos=... combat=True        <- van con trong tran
            # Loan dau la SOLO (khong lap party, khong sync kenh) nen leader khong lien quan gi.
            if ((not is_leader) and has_leader and st["leader_gone"].is_set()
                    and not digioi_solo and not event_stand_mode and not event_solo_kind):
                if _leader_thread_active():
                    log.warning("[%s] (member) leader_gone stale (leader thread van chay) -> clear, KHONG thoat",
                                label)
                    st["leader_gone"].clear()
                else:
                    log.info("[%s] (member) CHU PARTY da thoat -> member thoat theo", label)
                    _reason("chu party thoat -> member theo")
                    break
            if stop_ev is not None and stop_ev.is_set():
                log.info("[%s] (%s) -> STOP tu GUI", label, role)
                if (not is_leader) and getattr(c, "_individual_safe_logout", False):
                    try:
                        c.set_party_invite_ready(False)
                        c.leave_party()
                        time.sleep(1.0)
                    except Exception as e:
                        log.warning("[%s] OUT rieng: loi roi PT (van tiep tuc ve safe): %s", label, e)
                    safes = list((config.TRAIN_MAPS.get(int(c.current_map or 0)) or {}).get("safe") or train_safes or [])
                    dest = _nearest_safe(c.pos, safes)
                    if dest:
                        log.info("[%s] (member) OUT rieng -> ve safe gan nhat %s roi dong ket noi", label, dest)
                        try:
                            c.flee_mode = True
                            c.navigate_to(*dest)
                        except Exception as e:
                            log.warning("[%s] OUT rieng: loi chay ve safe (van OUT): %s", label, e)
                    else:
                        log.warning("[%s] OUT rieng: map %s khong co toa do safe -> roi PT roi OUT",
                                    label, c.current_map)
                    break
                if is_leader:
                    # LEADER dang cay ngoai diem quai -> chay ve diem safe GAN NHAT TRUOC,
                    # roi BAO HIEU (stop_leader_done) de member moi thoat theo.
                    if train_on_map:
                        dest = _nearest_safe(c.pos, train_safes)
                        if dest:
                            log.info("[%s] (LEADER) STOP -> chay ve safe gan nhat %s truoc khi thoat",
                                     label, dest)
                            try:
                                c.navigate_to(*dest)
                            except Exception as e:
                                log.warning("[%s] loi chay ve safe (bo qua): %s", label, e)
                    st["stop_leader_done"].set()   # leader da ve safe -> ca party duoc thoat
                    log.info("[%s] (LEADER) da ve safe -> bao member thoat", label)
                elif has_leader:
                    # MEMBER: CHO leader chay ve safe xong (stop_leader_done) roi MOI thoat
                    # -> ca lu thoat cung luc, leader khong bi bo lai ngoai diem quai.
                    log.info("[%s] (member) STOP -> cho leader ve safe roi thoat...", label)
                    if not st["stop_leader_done"].wait(60):
                        log.warning("[%s] (member) cho leader ve safe qua 60s -> thoat luon", label)
                break
            time.sleep(5)
            log.info("[%s] (%s) pos=%s map=%s combat=%s",
                     label, role, c.pos, c.current_map, c.in_combat())
            # 40NPC: MEMBER BAM KENH LEADER DA CHON (`st["channel"]`), khong cho ai "goi".
            #
            # Bam theo `st["channel"]` chu KHONG phai cho leader dang dung: kenh leader dung co the
            # het cho cho 4 dua con lai, nen leader VAN phai chon kenh (pick_best_channel, da tru
            # ra so acc cua chinh party) - so no chot moi la dich.
            # Ep sang luon, khong doi vong dong bo nao: o map su kien khac kenh la khac instance,
            # dung do ca ván la vo ich.
            # Log 31/08 party 2 (20:36-20:39): ca party relogin giua tran; 4 member vao lai TRUOC
            # va tu ve kenh cu 39, leader gamo vao lai SAU 40s roi chon kenh 34. Member da ra khoi
            # vong cho (`st["invited"]` chi co nghia "leader BAT DAU moi", KHONG phai "minh DA vao
            # doi") nen khong con cho nao doc kenh nua -> leader dot 60s
            # "sync kenh: 1/5 acc da sang kenh 34, con lai CHUA sang: {...: 39}" roi timeout, mat
            # ca ván 40NPC.
            # BO nhanh "member tu bam kenh leader chon" (`st["channel"]`).
            #
            # Do la CO CHE THU HAI cung ra lenh doi kenh, chay song song voi `kenh_dich` cua dieu
            # phoi -> vi pham L1 (chi MOT cho duoc quyet). Hai cai cung ra lenh thi khong ai chiu
            # trach nhiem hau qua, va hau qua o day la TAN DOI: server cam doi kenh khi dang trong
            # doi (`result=3`) nen doi kenh = phai roi doi truoc.
            #
            # Log that party 5 (06/09, 17:16:26) - leader dang qua cong len tang:
            #   [thbay] (member) 40NPC: leader chon kenh 2, minh dang o 1 -> BAM SANG kenh leader
            #   [thbay] Doi kenh 2 THAT BAI: DANG TO DOI thi khong doi khu duoc (result=3)
            #   [thbay] -> roi party roi thu lai
            #   [thbay] Roi/giai tan party cu
            # Ca 4 member roi doi cung luc -> leader leo tiep 12929 -> 12931 -> 12932 -> 12934 MOT
            # MINH. Dung cai da giet party 5 luc 16:34, chi khac duong vao.
            #
            # Gio CHI dieu phoi chot kenh (`st["kenh_dich"]`), va no chiu trach nhiem lap lai doi
            # sau khi doi xong (L5). Vong keepalive ben duoi da tu soi `kenh_dich`.
            # 2 co "chet ve thanh" cua HOP MAY doi theo PHA: BAT khi train, TAT khi PB/quest/event
            # (chet giua PB ma bi keo ve thanh = vo luot PB, ca party phai lam lai). Ham nay chi
            # GUI KHI THUC SU DOI va khong gui giua tran -> goi moi nhip cho re.
            try: c.sync_machinebox_flags()
            except Exception: pass
            # Pha DI GIOI khac train thuong: acc trong DG va acc DA XONG DG deu chay vong nay,
            # bao cung "train" thi watcher khong phan biet duoc trong/ngoai DG.
            set_account_activity(username,
                                 "%s map=%s combat=%s" % (
                                     "digioi" if _pstate(pidx).get("dt_phase") == "digioi" else "train",
                                     c.current_map, c.in_combat()),
                                 phase=("digioi" if _pstate(pidx).get("dt_phase") == "digioi"
                                        else "train"))
            # ==== 40NPC: run_loop (leader) bao HET GIO / THUA 2 TRAN (c._npc40_done) -> leader phat
            # tin hieu go_claim -> CA party (leader + member) di doi thuong + THOAT game. ====
            if event_party_mode:
                if is_leader and getattr(c, "_npc40_done", False):
                    st["go_claim"].set()
                if st["go_claim"].is_set():
                    log.info("[%s] (%s) 40NPC xong -> di doi thuong + thoat game", label, role)
                    try: c.leave_party()
                    except Exception: pass
                    if getattr(c, "_npc40_bo_thuong", False):
                        # Thoat som vi THUA 2 TRAN khi CHUA toi 22h: server chi cho doi thuong
                        # sau 22h nen goi luc nay chac chan khong an -> bo qua cho gon log.
                        # KHONG mat thuong: chay lai bot sau 22h thi no nhan (user xac nhan 31/08).
                        log.info("[%s] (%s) 40NPC: thoat som (thua 2 tran, chua toi 22h) -> chua "
                                 "doi thuong; chay lai bot sau 22h la nhan", label, role)
                    else:
                        try: c.claim_40npc_reward(ev)
                        except Exception as e: log.warning("[%s] loi doi thuong 40NPC: %s", label, e)
                    _reason("40NPC het gio / thua 2 tran lien tiep -> doi thuong xong -> TAT ACC")
                    c.close(); break
            # ==== LOAN DAU: run_loop bao HET GIO (qua 22h) -> thoat game. Moi acc tu xu ly, khong
            # co tin hieu party nao ca. ====
            if event_solo_kind == "chaos_vs" and getattr(c, "_loandau_done", False):
                log.info("[%s] (%s) LOAN DAU: %s, thang %d tran -> ra khoi map + thoat game",
                         label, role,
                         "danh xong 1 tran (tick chi danh 1 tran)"
                         if pcfg.get("loandau_mot_tran") else "het gio",
                         getattr(c, "_loandau_wins", 0))
                # KHONG co buoc doi thuong: server TU trao (user xac nhan 25/08). Chi can ra khoi
                # map event roi tat - de nguyen trong 10991 thi lan login sau bat dau tu map event.
                _loandau_ra_khoi_map(c, ev, label)
                _reason("Loan dau het gio -> ra khoi map -> thoat game")
                c.close(); break
            # ==== LEADER BAO "ra diem tap ket" (rally_gen) -> MEMBER chay ra safe NGAY. ====
            # `_ra_rally_gom_lai` chi di chuyen acc GOI NO, ma no nam trong `_party_tai_cho_xu_ly`
            # - ham CHI LEADER chay. Truoc day member khong he biet co lenh do: dung nguyen o diem
            # quai, quai lao vao lien tuc, moi luc dang trong tran thi loi moi khong toi noi ->
            # party khong bao gio du (party 11, 30/08 15:39-15:52: leader lap lai party moi 21s
            # suot 13 phut, 4 member combat=True o (940..950, 1080..1100), chi dua KHONG bi quai
            # danh moi vao duoc doi).
            if (not is_leader) and has_leader and train_on_map and st["rally_gen"] > rally_gen_handled:
                rally_gen_handled = st["rally_gen"]
                log.info("[%s] (member) DIEU PHOI: GOM LAI PARTY -> ra diem tap ket truoc khi cho moi",
                         label)
                _ra_rally_gom_lai("leader gom lai party")
                with st["lock"]:
                    c._rally_gen_da_lam = int(rally_gen_handled)
                continue
            # ==== RESYNC party (40NPC / Di Gioi): leader moi khong du -> giai tan + sync kenh lai.
            # Member roi party cu + sync kenh (chuyen sang kenh moi cua leader) -> auto-accept se
            # re-join khi leader moi lai. (Leader tu xu ly trong vong moi, khong vao day.) ====
            if ((not is_leader) and has_leader
                    and (event_party_mode or (is_digioi and not digioi_solo))
                    and st["resync_gen"] > resync_gen_handled):
                resync_gen_handled = st["resync_gen"]
                # LENH DA DUOC THUC HIEN XONG -> BO QUA, dung roi party nua.
                #
                # Muc dich cua resync la "leader moi mai khong du -> giai tan + sync kenh + moi
                # lai". Neu luc minh doc toi co nay ma DOI DA DU NGUOI roi thi muc dich do dat
                # roi; roi party luc nay la TU PHA cai vua lap.
                #
                # Bug that party 15 (06/09) - user: "dung yen 1 luc o tang 9 roi di ra ngoai":
                #   14:18:48 leader: moi 60s chua du party (0/4) -> GIAI TAN + sync kenh + moi lai
                #   14:18:50..53 moi lai -> 4/4 member DONG Y, roster 4 nguoi
                #   14:18:57 CA 4 member doc co resync (cham 4 giay) -> ROI PARTY VUA LAP
                #   14:18:58 leader: DU PARTY (4/4 member join)   <- leader tuong xong, di leo thap
                #   14:18:59 member: KHONG o party nao
                # Sau do leader ket trong vong leo thap, moi phut in "chua du member (0/4)" ma
                # khong lap lai duoc doi -> danh mot minh 3 phut/tran -> ket o cong -> ra khoi thap.
                # Dieu kien CU con vet "party DA DU" - qua hep. p9/p11 (07/09) chet vi party moi
                # 3-4/5: chua du nen guard truot, ma 3 dua DA VAO bi loi ra theo -> gom lai tu dau
                # (p11 mat 11 phut). Muc dich cua resync la cuu ca "leader moi mai khong ai vao";
                # voi dua DA VAO thi lenh do vo nghia, roi ra la tu pha - du hay chua du cung vay.
                # Dua CHUA vao moi la doi tuong that: no roi party ma + sync kenh roi cho moi lai.
                if is_joined(pidx, c.self_entity):
                    log.info("[%s] (member) co RE-SYNC nhung MINH DA O TRONG PARTY (%d/%d) -> BO "
                             "QUA (roi party luc nay la tu pha cai vua lap)",
                             label, joined_member_count(pidx), st["n_members"])
                    continue
                log.info("[%s] (member) leader RE-SYNC party -> roi party + sync kenh lai", label)
                try: c.leave_party()
                except Exception: pass
                do_channel_sync()   # cho channel_ready (leader da set) + chuyen kenh moi
                c.flee_mode = False
                continue
            # ==== RECONNECT reaction: co dong doi ROT (dang login lai) -> TAM DUNG + cho tat ca ve
            # -> restart mode. CHI khi party co bot-leader (khong thi nick rot da chet). Di Gioi SOLO
            # bo qua (moi acc doc lap). Team dungeon xu o phase daily rieng (relogin ca party). ====
            if (has_leader and not digioi_solo and
                    (not event_mode or event_party_mode) and st["disc_gen"] > disc_gen_handled):
                disc_gen_handled = st["disc_gen"]
                if event_party_mode:
                    if _should_restart_event_party(
                            event_party_mode, st["event_battle_active"],
                            st["disc_gen"], disc_gen_handled - 1):
                        account_forced_reconnect.add(username)
                        _reason("40NPC dang battle co dong doi rot -> relogin ca party")
                        log.warning("[%s] (%s) 40NPC dang battle co dong doi ROT -> RELOGIN cung ca party",
                                    label, role)
                        c.close()
                        break
                    continue
                # Train phai regroup theo disc_gen ke ca khi nick rot da relogin xong. Truoc day
                # ca nhanh nay phu thuoc st["reconnecting"] con phan tu; reconnect nhanh xoa marker
                # truoc nhip keepalive -> survivors nuot disc_gen, khong reform, moi nick mot noi.
                if _should_restart_mode_after_disconnect(train_on_map, st["reconnecting"]):
                    log.warning("[%s] (%s) dong doi ROT %s -> TAM DUNG cho reconnect",
                                label, role, list(st["reconnecting"]))
                    c.flee_mode = True
                    if train_on_map and train_safes:   # train: ve rally dung cho
                        rally = st.get("rally_point") or _nearest_safe(c.pos, train_safes)
                        if rally:
                            try: c.navigate_to(*_jitter(rally))
                            except Exception: pass
                    elif is_digioi:                              # digioi: dung chay long vong
                        try: c.stop_run_around()
                        except Exception: pass
                    # KHONG GIAI TAN PARTY chi vi MOT dua rot.
                    #
                    # Truoc day leader `leave_party()` ngay tai day, ly do ghi la "giai tan de sau
                    # sync kenh duoc". Nhung sync kenh CHUA CHAC xay ra: dua rot thuong vao lai
                    # dung kenh cu, va neu can doi kenh thi `_prepare_channel_switch` tu roi doi
                    # luc do. Doi lai, giai tan o day pha doi BA-BON NGUOI DANG LANH, va sau khi
                    # dua kia ve thi ca party phai lap lai tu dau - dung luc dieu phoi cung dang
                    # thay "thieu nguoi" nen no bump reform them mot lan nua.
                    #
                    # 94 lan `dong doi ROT -> TAM DUNG` trong log 14/09, moi lan la mot lan pha doi
                    # khong can thiet.
                    #
                    # Party DA HONG SAN (roster rong) thi khong co gi de giu -> don dep nhu cu.
                    if is_leader and not getattr(c, "party_members", None):
                        try: c.leave_party(); reset_party_joined(pidx)
                        except Exception: pass
                    while st["reconnecting"] and c.running and not _stopped():   # CHO tat ca ve (vo han)
                        _nghe_lenh_kenh()   # lenh dieu phoi phai nghe duoc o MOI vong cho
                        time.sleep(3)
                        try:
                            if not c.in_combat(): _use_consumables(c)
                        except Exception: pass
                    if not c.running or _stopped():
                        continue
                    log.info("[%s] (%s) tat ca da reconnect -> restart mode", label, role)
                    if train_on_map:
                        # Supervisor da phat cung reform_gen ngay luc disconnect. Danh dau gen nay
                        # dang duoc xu tai nhanh reconnect de keepalive khong reform trung them 1 lan.
                        reform_gen_handled = max(reform_gen_handled, st["reform_gen"])
                        _route_r = getattr(config, "TRAIN_ROUTES", {}).get(sc)
                        _reconnect_safe = st.get("rally_point") or (
                            train_safes[0] if train_safes else None
                        )
                        _smart_r = None
                        if is_leader:
                            try:
                                if getattr(config, "SMART_WORLD_ROUTING", True):
                                    _smart_r = c.build_smart_route(sc, _reconnect_safe)
                            except Exception as e:
                                log.warning("[%s] reconnect: loi build smart route: %s", label, e)
                        if _train_route_available(_smart_r, _route_r, has_leader):
                            try:
                                # Reconnect vao DUNG bai train va ca party cung o do -> khong ve thanh.
                                if not _party_tai_cho_xu_ly("sau reconnect"):
                                    _do_reform()
                            except Exception as e: log.warning("[%s] reconnect reform loi: %s", label, e)
                        elif c.current_map != sc:                # route-less + MINH lech map
                            # KHONG TAT PARTY: sai map sau reconnect la trang thai binh thuong, va
                            # dieu phoi doc map ca party moi 2 giay -> no thay lech va ra lenh gom.
                            log.warning("[%s] (%s) sau reconnect: KHAC map train (%s != %s) va khong "
                                        "dung duoc duong -> KHONG tat party, de dieu phoi quyet",
                                        label, role, c.current_map, sc)
                            _reason("sau reconnect sai map (%s != %s) -> dieu phoi quyet"
                                    % (c.current_map, sc))
                            continue
                        else:                                    # route-less + CA PARTY o map -> regroup TAI CHO
                            do_channel_sync()                    # (nick lech map da tu stop_party o startup cua no)
                            if is_leader:
                                c.flee_mode = True               # ne quai trong luc CHO du party
                                _moi_theo_dieu_phoi(True, "regroup tai cho sau reconnect")
                                try: c.set_party_strategist()
                                except Exception: pass
                            c.combat_ready(); c.flee_mode = False
                    elif is_digioi:                              # digioi: vao lai DG + leader re-invite
                        c.flee_mode = True
                        try:
                            if not c.in_di_gioi(): c.enter_di_gioi_safe()
                        except Exception: pass
                        if is_leader:
                            _moi_theo_dieu_phoi(False, "moi lai party DG sau reconnect")
                            _invite_whitelist_followers_if_bot_party_ready(c, st, pidx, label, force=True)
                            try: c.set_party_strategist()
                            except Exception: pass
                        c.combat_ready(); c.flee_mode = False
                    last_reform = time.time(); last_combat = time.time()
                    continue
            # EVENT: luon TAT flee khi rảnh -> nguoi choi keo bot vao battle (moi tay) thi bot DANH,
            # khong bo chay (flee_mode co the con True tu go_to_event/di chuyen truoc do).
            if event_mode and not c.in_combat() and getattr(c, "flee_mode", False):
                c.flee_mode = False
            # ==== TU SOI KENH DICH CUA DIEU PHOI ====
            # Kenh dich la TRANG THAI, khong phai cai bat tay tung vong -> acc DA LAM XONG vong
            # dong bo truoc do van bat duoc lenh moi. Day dung la cho party 53 vo (06/09):
            # vumba/vumsau lam xong vong dich=kenh4 roi ROI vong, picker doi dich sang kenh 2 sau
            # do, hai dua khong bao gio biet -> "3/5 da sang kenh 2, CHUA sang: {qv813:4, qv816:4}".
            # KHONG con cong `channel_ready`: dieu phoi la nguoi quyet, khong nhuong vong bat tay.
            # Vao khong duoc (kenh day) thi GHI SO DEN roi di lam viec khac - khong do lai cho ai
            # "pick lai" (P3 06/09: batbat do lai 1 tieng vi leader ket o vong cho member).
            _nghe_lenh_kenh()
            # ==== TU SOI TANG GOM CUA DIEU PHOI (2K) ====
            # MOI ACC deu phai thi hanh, khong rieng leader. Truoc day `_thi_hanh_gom` chi duoc goi
            # trong vong MOI cua leader -> leader lui thui di bo xuong, 4 member dung im -> CANG
            # GOM CANG LECH.
            # Log that party 8 (06/09, 17:55) - user: "ca lu co di chuyen ty nao deo dau":
            #   lbumot (LEADER) map=12928  <- da tut xuong tang 5, dang leo nguoc len MOT MINH
            #   lubhai/lubba/lubbon/lubnam map=12934 pos=(650,430)  <- dung im tang 11
            _tg = (_ke_hoach(st) or {}).get("tang_gom")
            if (_tg and not c.in_combat()
                    and int(getattr(c, "current_map", 0) or 0) != int(_tg)):
                try:
                    _ev_tg = config.event_hom_nay(pcfg.get("event_key") or "")
                except Exception:
                    _ev_tg = None
                if _ev_tg is not None:
                    log.warning("[%s] (%s) DIEU PHOI chot tang gom %s, minh dang o %s -> di bo xuong",
                                label, role, config.scene_name(int(_tg)), c.current_map)
                    try:
                        c.regroup_to_event_start(_ev_tg, dest=int(_tg))
                    except Exception as e:
                        log.warning("[%s] loi di bo ve tang gom %s: %s", label, _tg, e)
            # ==== LUAT TOI THUONG (L0): THIEU DOI thi DUNG VIEC CHINH ====
            # "du party roi lam gi thi lam; party hong thi phai gom lai BANG DUOC" - thieu du mot
            # nguoi thi viec chinh dung het. Truoc day dieu phoi CO ra lenh `VIEC_MOI` nhung
            # KHONG AI THI HANH (`grep '["viec"]'` chi thay VIEC_GOM duoc doc) - lenh roi vao hu
            # khong, con acc thi cu lam viec cua no.
            #
            # Ca that 07/09 party 23 - 32 PHUT (13:56:17 -> 14:28):
            #   4 member: (member) CHO ca party xong daily (4/5, reconnecting=0)...
            #   leader  : BATTLE ACK g=18 / BO CHAY (flee_mode) - dang danh MOT MINH
            # va dieu phoi thi in dung su that moi nhip:
            #   14:27:40 [party 22] viec=moi - DOI chua du (gclm01=0 ... gclm05=0)
            #
            # `flee_mode = True` = khong danh nua, gap quai thi chay. Khong dung `stop`/`close`:
            # acc van phai song de nhan loi moi va di gom.
            # LENH CUA DIEU PHOI LA TUYET DOI (user chot 07/09: "lenh cua dieu phoi la phai theo
            # tuyet doi, acc tu cho phep dung yen tu danh -> may thay code ngu ko").
            # BA lenh duoi day deu co nghia "party dang KHONG on, phai gom": khong acc nao duoc
            # danh/train trong luc do, du no thay minh dang dung o safe dep va doi da du.
            #   VIEC_MOI     - doi chua du
            #   VIEC_GOM     - lech map, phai ve cung cho
            #   VIEC_DONG_BO - lech kenh, dong bo tai cho
            _viec_now = (_ke_hoach(st) or {}).get("viec")
            # MO CONG NHAN LOI MOI KHONG PHU THUOC TRAN. Vao doi la goi `C:013-008`, khong bi tran
            # chan (khac teleport / doi kenh / qua cong - nhung cai do moi phai cho het tran).
            #
            # Ca that 08/09 party 6 (user: "p6 ko gom duoc pt, cung kenh cung map roi, nguyen nhan
            # vi sao"): ca party dung o map train 21814 kenh 2, roster ca 5 acc = 0, lap "LAP LAI
            # PARTY" moi 2 phut suot hon mot tieng. Ly do: chinh diem safe cua map do bi quai danh
            # lien tuc - canh bao SAFE BI QUAI DANH keu 32 lan cho map 21814, "8 tran trong 1 phut
            # ngay tai safe [3170, 530]". Acc gan nhu LUC NAO CUNG in_combat -> ca nhanh L0 duoi
            # day khong bao gio chay -> `party_invite_ready` khong bao gio duoc bat -> leader moi
            # mai ma khong ai nhan duoc.
            # MO CONG NHAN LOI MOI khi acc DANG RANH. Day la cho go co `party_invite_ready` bi
            # ket False vinh vien (acc bi ABORT truoc khi toi dong gan co - ca 08/09 party 17).
            # NHUNG khong duoc mo khi no DANG GIUA mot viec vat: nhan loi moi luc do la bo do viec
            # vat (user 14/09). Doc pha that (`dang_lam_viec_vat`), khong doc co.
            if ((not is_leader) and _viec_now in (VIEC_MOI, VIEC_GOM, VIEC_DONG_BO)
                    and not c.dang_lam_viec_vat()):
                try:
                    c.set_party_invite_ready(True)
                except Exception:
                    pass
            if _viec_now in (VIEC_MOI, VIEC_GOM, VIEC_DONG_BO) and not c.in_combat():
                if not getattr(c, "_l0_dung_viec", False):
                    c._l0_dung_viec = True
                    log.warning("[%s] (%s) L0: DIEU PHOI ra lenh '%s' -> DUNG viec chinh, theo lenh "
                                "(khong train/danh mot minh)", label, role, _viec_now)
                c.flee_mode = True
                try:
                    if is_digioi:
                        c.stop_run_around()
                except Exception:
                    pass
                # MO CONG NHAN LOI MOI. `party_invite_ready` chi duoc bat o BA nhanh cu the (xong
                # login chores / xong reform / vao map 40NPC). Member ABORT giua chung thi khong bao
                # gio toi dong do -> co ket False vinh vien, va no GIU loi moi lai thay vi accept.
                #
                # Ca that 07/09 party 10 (17:12 -> 17:26, 14 phut):
                #   [luuhai] Chua san sang vao party -> GIU loi moi entity=5910fdf4878d, se accept
                #            sau viec vat            <- lap moi 6s
                #   [luuhai] (member) ABORT di duong reform: reform_gen 2 -> 3 (acc khac bump)
                #   ... im hoan toan 14 phut ...
                #   [party 10] DIEU PHOI: DOI chua du (luu001=0 ... luu005=0) -> LAP LAI PARTY
                # Dieu phoi ra lenh lap party, leader moi that, ma member thi "de sau viec vat".
                # Thieu doi thi KHONG CON viec vat nao quan trong hon (L0) -> mo cong.
                try:
                    if not is_leader and not c.dang_lam_viec_vat():
                        c.set_party_invite_ready(True)
                except Exception:
                    pass
            elif (getattr(c, "_l0_dung_viec", False)
                  and _viec_now not in (VIEC_MOI, VIEC_GOM, VIEC_DONG_BO)):
                c._l0_dung_viec = False
                log.info("[%s] (%s) L0: dieu phoi het lenh gom -> lam tiep", label, role)
                _bat_danh_neu_du_party()
            # Hoi mau MOI MODE (train/digioi/city/stand...) - chi can ngoai combat.
            # Tu lọc theo nguong HP/SP nen dung yen/ve thanh khong thua mau thi khong dung item.
            if not c.in_combat():
                _use_consumables(c)
            # Dung Daily: giu socket/account online, khong combat/di chuyen/reform. Lenh Daily
            # moi hoac AUTO BATTLE se xoa co nay.
            if getattr(c, "_daily_hold", False):
                c.flee_mode = False
                time.sleep(1.0)
                continue
            # NHAN VAT CHET: server co the da dua xac ve thanh nhung van giu socket song, nen
            # supervisor khong xem day la disconnect va account cu dung im/le party. Chu dong
            # dua no qua CUNG luong reconnect nhu server rot. Tang disc_gen TRUOC khi dong socket
            # de cac acc con lai ngung train, ve safe va cho; sau khi nick nay vao world lai,
            # dieu phoi se dong bo kenh leader, lap lai PT roi tiep tuc route/toa do farm da luu.
            _char_stat = getattr(getattr(c, "state", None), "char", None)
            _char_dead = bool(_char_stat is not None
                              and int(getattr(_char_stat, "hp_max", 0) or 0) > 0
                              and int(getattr(_char_stat, "hp", 0) or 0) <= 0)
            if ((train_on_map or bool(getattr(c, "_ui_auto_battle", False)))
                    and _char_dead and not c.in_combat()):
                with st["lock"]:
                    st["reconnecting"].add(username)
                    st["disc_gen"] += 1
                _reason("NHAN VAT CHET -> reconnect, dong bo kenh leader va lap lai PT")
                log.warning("[%s] (%s) NHAN VAT CHET -> bao ca team ve SAFE, reconnect nick nay, "
                            "dong bo phan khu leader + lap lai PT + tiep tuc bai train",
                            label, role)
                _force_supervisor_reconnect(username, c, "nhan vat chet - hoi sinh/reconnect")
                break
            # SAFE BI QUAI DANH: dem so TRAN xay ra ngay tai diem safe. Safe la cho DUNG NGHI -
            # dung do ma tran noi tran nghia la diem safe hoc SAI (xem `_ghi_nhan_tran_tai_safe`).
            # Bat theo CANH LEN cua in_battle (khong dem moi nhip, mot tran keo dai nhieu nhip).
            _bt_now = bool(getattr(getattr(c, "state", None), "in_battle", False))
            if _bt_now and not _bt_truoc:
                try:
                    _sf = st.get("rally_point") or _nearest_safe(c.pos, train_safes)
                    # Chi dem khi toa do la THAT (server vua xac nhan cho map nay), khong dem bang
                    # so dead-reckoning - xem `_ghi_nhan_tran_tai_safe`.
                    _tin = (getattr(c, "_pos_valid_for_map", None) is not None
                            and int(getattr(c, "_pos_valid_for_map", 0) or 0)
                            == int(getattr(c, "current_map", 0) or 0))
                    _ghi_nhan_tran_tai_safe(username, getattr(c, "current_map", None), c.pos, _sf,
                                            pos_dang_tin=_tin)
                except Exception:
                    pass
            _bt_truoc = _bt_now
            # KET o bai train >40s KHONG vao tran -> co the diem quai xau (khong co quai) HOAC
            # mat combat-active sau khi keo qua cong. LEADER -> DOI diem quai khac + re-arm;
            # member -> chi re-arm (member theo tran cua leader). Tu phuc hoi, khoi restart.
            if c.in_combat():
                last_combat = time.time()
            _ui_battle = bool(getattr(c, "_ui_auto_battle", False))
            should_fight = _ui_battle or (training_started if is_leader else is_joined(pidx, c.self_entity))
            if ((train_on_map or _ui_battle) and should_fight and not getattr(c, "flee_mode", False)
                    and time.time() - last_combat > 18 and time.time() - last_rearm > 18):
                last_rearm = time.time()
                # 18s khong vao tran -> chi RE-ARM combat-active (mat sau khi qua cong) - KHONG
                # di long vong (vo ich, khong giai quyet duoc gi). Ket that su -> relogin o duoi (90s).
                try: c.combat_ready()
                except Exception: pass
            # KET o bai: >90s KHONG battle du da di long vong (re-arm 18s khong cuu) -> RELOGIN.
            # login=cho logout + goi 0x03 self-spawn -> self.pos RESYNC ve toa do THAT (het drift
            # dead-reckoning lam move_to nham huong). Chay ve rally (safe da chon) TRUOC roi relogin
            # -> tu safe (pos chuan) di lai toi spot. KHONG gioi han so lan (theo yeu cau Anh).
            # CHI leader (leader dieu huong; member theo tran leader + duoc moi lai qua vong 60s).
            if (train_on_map and is_leader and should_fight and not getattr(c, "flee_mode", False)
                    and time.time() - last_combat > 60 and time.time() - last_relogin > 60):
                last_relogin = time.time()
                relogin_cnt += 1
                rally = st.get("rally_point") or _nearest_safe(c.pos, train_safes)
                spot = st.get("mob_spot")
                log.warning("[%s] (LEADER) >90s KHONG battle -> ve safe %s + RELOGIN (lan %d) de resync vi tri",
                            label, rally, relogin_cnt)
                try:
                    c.flee_mode = True
                    if rally:
                        c.navigate_to(*_jitter(rally))   # ve safe da chon truoc khi thoat
                    # GIAI TAN party cu TRUOC khi relogin: leader van dang la leader -> 0x0d sub=04
                    # tan ca party -> 4 member duoc THA khoi party cu. Khong tan thi member van ket
                    # trong party cu -> moi lai KHONG vao (dang trong party roi) -> leader danh 1 minh.
                    c.leave_party(); time.sleep(0.8)
                    reset_party_joined(pidx)         # quen member cu -> leader tinh lai tu dau, retry 60s moi lai
                    if c.relogin():                  # thoat + login lai -> 0x03 resync pos ve dung safe
                        # MOI LAI member NGAY TAI SAFE (leader+member gan nhau) roi CHO ho join
                        # TRUOC khi keo ra spot -> member duoc keo theo. Moi truoc khi di (neu di
                        # spot truoc roi moi moi thi member ket o safe, khong duoc keo).
                        c.combat_ready(); c.flee_mode = False
                        for _ in range(4):           # moi lap lai, cho member (gio da tu do) accept
                            if not c.running or _stopped(): break
                            try: _invite_party_participants(c, True, gap=1.0)
                            except Exception: pass
                            time.sleep(3)
                            if joined_member_count(pidx) >= st["n_members"]:
                                break
                        log.info("[%s] (LEADER) sau relogin: %d/%d member join lai -> keo ra spot",
                                 label, joined_member_count(pidx), st["n_members"])
                        path = st.get("mob_path")
                        _gr = st["reform_gen"]   # dang keo (sau relogin) ma co dua van map -> abort -> reform
                        _abr = lambda: _stopped() or (not c.running) or st["reform_gen"] > _gr
                        if path:
                            c.follow_path(path, flee=False, abort=_abr)   # keo ca party ra spot (path tranh tuong)
                        elif spot:
                            c.navigate_to(*spot, abort=_abr)     # tu safe (pos CHUAN) keo party ra spot
                        c.combat_ready(); c.flee_mode = False
                        last_combat = time.time()    # cho them 90s nua truoc khi relogin tiep
                except Exception as e:
                    log.warning("[%s] loi relogin recovery (bo qua): %s", label, e)
            # DONG BO PHA THEO LEADER: leader dang o pha PB (team_dungeon) ma member lai dang reform-
            # to-train -> member THAM GIA PB (report luot + danh) thay vi reform vo vong. Bug that:
            # leader "cho report luot 4/5", 4 member sai map cu bump reform_gen loop mai (phase-sync
            # abort duoc nhung displaced re-bump lien tuc). _run_auto... BLOCK trong wait cua member
            # nen khong spam; report luot khong phu thuoc map (doc daily-flag server).
            if (not is_leader and auto_team_dungeon
                    and _leader_live_phase(pidx, st) == "team_dungeon"):
                log.info("[%s] (member) leader dang PB (team_dungeon) -> THAM GIA PB thay vi reform", label)
                try:
                    _run_auto_team_dungeons_if_needed(c, st, username, label, pidx, is_leader, _stopped, pcfg)
                except Exception as e:
                    log.warning("[%s] loi tham gia PB theo leader (bo qua): %s", label, e)
                last_reform = time.time(); displaced_cnt = 0
                continue
            # DISPLACED: dang train ma BI VAN khoi train map (99% = quai danh chet -> hoi sinh ve
            # thanh). KHONG tu ve lai le loi (party da vo) -> YEU CAU CA PARTY REFORM: acc nao tu
            # thay minh van map thi bump reform_gen -> ca party ve thanh, leader giai tan + lap lai
            # + keo ra bai. grace 60s sau reform de khong trigger lien tuc khi dang o thanh (!=sc).
            # CHI LA "BI VAN" KHI CON NGUOI DANG O BAI. Khong ai o bai = ca party dang DI DUONG
            # toi do (leader keo qua cong), va luc do khac map train la chuyen binh thuong.
            #
            # Ca that party 4, 13/09 (user: "dang di ra bai train ma sao cu co log dieu phoi ra
            # lenh gom the"):
            #   14:27:26 [tmo]  da toi diem (20,460) sau 12 lenh move
            #   14:27:26 [tmo]  _enter_gate idx=8 @(20,460): lan thu 1 (t=0s, map van 21003)
            #   14:27:27 [tha]  (member) BI VAN khoi train map (dang o 21003, vd chet)
            #   14:27:27 [bat]  (member) BI VAN khoi train map (dang o 21003, vd chet)
            #   14:27:28 [lin]  (member) BI VAN khoi train map (dang o 21003, vd chet)
            #   14:27:31 [bub]  (member) BI VAN khoi train map (dang o 21003, vd chet)
            # Ca nam dua deu o 21003 - dung sau leader cho no mo cong. Khong ai van ca, ma dong nao
            # cung keu "bi van" va nhac toi lenh gom -> doc log tuong dieu phoi dang gom lien tuc.
            if (train_on_map and c.current_map is not None and c.current_map != sc
                    and _co_ai_dang_o_map(pidx, sc, tru=username)
                    and time.time() - last_reform > 60):
                displaced_cnt += 1
                if displaced_cnt >= 2:   # 2 lan lien tiep (~10s) khac map train -> chac chan displaced
                    displaced_cnt = 0
                    # KHONG yeu cau gi ca - acc khong ra lenh cap party (L1). Dong log cu ghi
                    # "yeu cau CA PARTY reform (gen N)" trong khi than no chi la `pass`: no NOI DOI,
                    # va vi ca 5 acc cung in nen doc log tuong dang co lenh gom chay.
                    # User 13/09: "t thay van co dong lenh gom lai ma" - chinh la dong nay.
                    # Bi van khoi train map la mot TRANG THAI: dieu phoi doc map ca party moi 2
                    # giay, thay lech thi ra lenh gom.
                    log.warning("[%s] (%s) BI VAN khoi train map (dang o %s, vd chet) -> de DIEU "
                                "PHOI ra lenh gom (reform_gen hien tai %d)",
                                label, role, c.current_map, st["reform_gen"])
            else:
                displaced_cnt = 0
            # Bat ky acc nao thay reform_gen TANG (co dua van map) -> CA PARTY cung reform tai cho.
            # NGOAI RA: dang co vong GOM ve thanh (`gom_dich[gen]`) ma MINH KHONG o thanh do ->
            # phai ve gop, du minh dang o dung train map va da "handled" gen. Bug that: tttam vao
            # lai map train (relogin) -> nuot reform_gen -> ngoi im, 4 acc kia cho o thanh 21011 vo
            # tan (4/5) vi doi no.
            # CO NGUOI DANG CHO O THANH? Doc DICH cua vong gom (`gom_dich`) + map THAT cua minh,
            # khong doc danh sach ai-da-khai.
            # `gom_dich[gen] = (thanh, luc)`. CO HAN DUNG: vong gom co nhieu duong thoat (abort,
            # timeout, bo cuoc) va khong duong nao chac chan xoa duoc dich. Dich treo lai thi acc di
            # tiep ra bai train se thay `current_map != dich` -> tuong "con nguoi dang cho o thanh"
            # -> GIAI TAN PARTY quay ve gom lai (party 1, 07/09 23:56).
            with st["lock"]:
                _gd_rec = st.get("gom_dich", {}).get(st["reform_gen"])
            _gather_dich = None
            if _gd_rec:
                try:
                    _gd_city, _gd_luc = int(_gd_rec[0]), float(_gd_rec[1])
                except Exception:
                    _gd_city, _gd_luc = 0, 0.0
                if _gd_city and time.time() - _gd_luc < GOM_DICH_HAN_SEC:
                    _gather_dich = _gd_city
            _gather_wait_me = bool(_gather_dich) and int(
                getattr(c, "current_map", 0) or 0) != int(_gather_dich)
            # NGHE LENH GOM KHONG PHU THUOC `train_on_map`. Acc mode train ma CHUA CHOT DUOC BAI
            # (`tm is None` -> `train_on_map` False) roi vao nhanh "DUNG YEN tai cho login", va vi
            # dieu kien nay doi `train_on_map` nen no DIEC voi moi lenh gom - dung im vinh vien.
            #
            # Ca that party 15, 13/09 (user: "p15 lai deo them gom pt"):
            #   17:14:15 [truchin] (member) DUNG YEN tai cho login (map=12003)
            #   17:14:24 [truchin] (member) L0: DIEU PHOI ra lenh 'gom' -> DUNG viec chinh, theo lenh
            #   ... tu do den 18:18 (MOT TIENG) chi con dong `pos=(530,530) map=12003` moi 5 giay ...
            #   18:01:07..18:16:11 [party 15] REFORM gen -> 18,19,20,21,22,23 (gom, moi 3 phut)
            # Dieu phoi ra lenh deu dan, member khong he nhuc nhich. Nhanh L0 chi DUNG danh, no
            # khong di gom - viec di gom nam o dung nhanh nay.
            # LENH DIEU PHOI DA RUT (muc tieu dat) -> KHONG thi hanh lai. Acc CHI DOC con so dieu
            # phoi ghi, khong tu ket luan "minh vua lam xong cai lenh muon" (L1). Xem cho ghi
            # `reform_gen_thoa` trong `_dieu_phoi_quyet`: party 1 13/09 du 4/4 luc 23:19:49 roi tu
            # pha chinh no luc 23:19:55 de thi hanh lai lenh gen 2 da chet.
            _rg_thoa = int(st.get("reform_gen_thoa", 0) or 0)
            if _rg_thoa > reform_gen_handled:
                reform_gen_handled = _rg_thoa
            # Lenh user bam tren UI phai co uu tien cao hon reform nen. Neu reform dang treo duoc
            # xu ly truoc, no co the tiep tuc bump/continue mai va nuot START TRAIN TEAM.
            if st["cmd_gen"] > cmd_gen_handled:
                cmd_gen_handled = st["cmd_gen"]
                cmd = st.get("cmd")
                log.info("[%s] (%s) -> LENH THU CONG UU TIEN %s", label, role, cmd)
                try:
                    if cmd: _do_manual_cmd(cmd)
                except Exception as e:
                    log.warning("[%s] loi thuc thi lenh thu cong (bo qua): %s", label, e)
                last_reform = time.time()
                last_combat = time.time()
                continue
            if st["reform_gen"] > reform_gen_handled or _gather_wait_me:
                reform_gen_handled = st["reform_gen"]
                log.warning("[%s] (%s) -> REFORM party (gen %d%s)", label, role, reform_gen_handled,
                            ", gop gather dang cho" if _gather_wait_me else "")
                try:
                    # Con dang cho nhau o THANH (`gom_dich` con dich) thi phai ve gop that;
                    # con lai, ca party dung san o bai train -> xu ly tai cho, khong ve thanh.
                    #
                    _tai_cho = (not _gather_wait_me
                                and _party_tai_cho_xu_ly("reform gen %d" % reform_gen_handled))
                    if not _tai_cho:
                        _do_reform()
                except Exception as e:
                    log.warning("[%s] loi reform (bo qua): %s", label, e)
                last_reform = time.time()
                last_combat = time.time()   # reset watchdog relogin sau reform
                continue
            try:
                c.reset_daily_counters_if_needed()
                c.claim_online_gifts()   # nhan qua online khi du gio (10/20/30/60/90/180 phut)
            except Exception as e:
                log.warning("[%s] loi reset/qua online (bo qua): %s", label, e)
            # Phuc Than: dinh ky 30p/lan (KHONG phai 1 lan luc login) - CHI khi party bat cong tac
            # "Su dung Phuc Than". Danh gia moi tick (nhu claim_online_gifts) thay vi tach thread rieng.
            # BAT BUOC khong dang combat (dung/deo giua luc danh trong bai quai la vo ly + bug thuc te).
            # Chay theo SU KIEN: buff tut < 5 (0x18 sub0800) hoac ngoc HONG (0x17 sub1b00/2300)
            # -> client bat c.phuc_than_pending -> lam NGAY. Truoc day cho mu 30 phut: ngoc hong
            # phut thu 1 thi mat he so EXP toi 29 phut. Vong dinh ky chi con la LUOI AN TOAN cho
            # server khong gui goi (PHUC_THAN_CHECK_SEC).
            if ((_use_normal or _use_dai) and mode != "event"
                    and (getattr(c, "phuc_than_pending", False) or time.time() >= next_phuc_than)
                    and not c.in_combat()):
                try:
                    c.use_phuc_than_items(_use_normal, _use_dai)
                except Exception as e:
                    log.warning("[%s] loi dung item phuc than (bo qua): %s", label, e)
                next_phuc_than = time.time() + PHUC_THAN_CHECK_SEC
            # NHAN QUA nhiem vu hang ngay dinh ky (1h/lan) - xem ghi chu o cho khoi tao next_daily_claim.
            if do_daily and time.time() >= next_daily_claim and not c.in_combat():
                next_daily_claim = time.time() + 3600   # set TRUOC de loi cung khong spam
                try:
                    c.claim_daily_quests(heavy=False)
                except Exception as e:
                    log.warning("[%s] loi claim daily quest dinh ky (bo qua): %s", label, e)
            # MUA HP/SP giua phien (chi MODE TRAIN, moi 2h): kho thap -> di Trac Quan mua. buy_hp_sp
            # tu check nguong (du -> khong di). Acc di mua -> off-map -> reform keo party ve thanh cho.
            if (train_on_map and (pcfg.get("buy_hp") or pcfg.get("buy_sp"))
                    and time.time() >= next_buy_hpsp and not c.in_combat()):
                next_buy_hpsp = time.time() + 7200   # 2h (set TRUOC de loi cung khong spam)
                try:
                    _still_low = c.buy_hp_sp(
                        pcfg.get("buy_hp", False), int(pcfg.get("hp_qty", 9999)),
                        int(pcfg.get("hp_thresh", 500000)),
                        pcfg.get("buy_sp", False), int(pcfg.get("sp_qty", 9999)),
                        int(pcfg.get("sp_thresh", 500000)),
                    )
                    # Mua xong VAN THIEU (het xu): co PARTY (nhieu acc) -> train tiep, 2h sau check
                    # lai; SOLO 1 minh -> OUT game (khong tru xu vo ich, dung dam den party khac).
                    if _still_low:
                        if len(party_accounts(pidx)) > 1:
                            log.info("[%s] Mua HP/SP van thieu (het xu) - CO party -> train tiep, "
                                     "2h sau check lai", label)
                        else:
                            log.warning("[%s] Mua HP/SP van thieu (het xu) - SOLO 1 minh -> OUT game", label)
                            _quit(); return
                except Exception as e:
                    log.warning("[%s] loi mua HP/SP giua phien (bo qua): %s", label, e)
            # Di Gioi Ho Phu: chi mode Di Gioi, tick rieng, check moi 3p va chi dung khi con <15p.
            # Server se tu gui 0x55/id=0x1b sau khi dung; khong cong timer thu cong.
            if is_digioi and pcfg.get("use_digioi_ho_phu") and time.time() >= next_ho_phu:
                if not c.in_combat():
                    try:
                        _maybe_use_di_gioi_ho_phu("3p")
                    except Exception as e:
                        log.warning("[%s] loi dung Di Gioi Ho Phu (bo qua): %s", label, e)
                    next_ho_phu = time.time() + HO_PHU_CHECK_SEC
            # Van tieu: chi goi lai DUNG GIO escort xong (next_vantieu), KHONG check mu.
            if next_vantieu is not None and time.time() >= next_vantieu:
                try:
                    next_vantieu = c.do_van_tieu()
                except Exception as e:
                    log.warning("[%s] loi van tieu (bo qua): %s", label, e)
                    next_vantieu = time.time() + 600   # loi -> thu lai sau 10p
            # BOSS QUAN DOAN: con luot + het cooldown -> danh NGAY TAI BAI TRAIN.
            # do_legion_boss() KHONG teleport (khac boss the gioi): no chi gui 0x27 7700 + 0x14
            # 08000100 de vao INSTANCE boss - dung duoc tu bat ky dau. Truoc day bot bump
            # reform_gen cho CA PARTY VE THANH roi moi nick tu danh -> thua ca mot vong route,
            # va khi luat "da o bai train thi xu ly tai cho" ra doi thi reform do bi nuot ->
            # doi mai khong bao gio danh (log 27/08 14:35-14:37, party bi giai tan gen 1, gen 2).
            # Viec THAT SU can: ra safe (khoi bi quai keo tran) + roi party (boss la instance
            # SOLO) roi danh tai cho. Party thieu nguoi se duoc khoi "MAT PARTY -> GOM LAI" ben
            # duoi gom lai nhu binh thuong.
            if not pcfg.get("fight_legion_boss", True):
                pass   # setting tat -> bo qua hoan toan, KHONG lam gi
            elif not c.legion_boss_available():
                boss_reform_pending = False   # da danh xong / het luot / dang cooldown / khong QD
            elif not c.in_combat():
                # CHI DI KHI PARTY DANG YEN ON. Boss QD bat buoc `leave_party()`, tuc lam doi
                # thieu nguoi - di dung luc dieu phoi dang gom / dong bo / moi la dam vao chinh
                # viec do, va lenh cua dieu phoi luon quan trong hon boss (boss con luot ca ngay).
                # Dieu phoi bao `lam` = khong con gi phai xu ly o cap party -> luc do moi di.
                _viec_bq = (_ke_hoach(st) or {}).get("viec")
                if _viec_bq not in (None, VIEC_LAM):
                    if time.time() - float(st.get("boss_qd_cho_log", 0.0) or 0.0) > 120.0:
                        st["boss_qd_cho_log"] = time.time()
                        log.info("[%s] (%s) boss QD den luot nhung dieu phoi dang ra lenh '%s' "
                                 "-> HOAN (boss con luot, party thi khong)", label, role, _viec_bq)
                elif train_on_map:
                    if not boss_reform_pending:   # 1 lan / dot con luot (tranh lam lai lien tuc)
                        boss_reform_pending = True
                        _sf = st.get("rally_point") or (train_safes[0] if train_safes else None)
                        log.info("[%s] (%s) boss QD den luot -> ra safe %s + danh NGAY TAI BAI "
                                 "(khong ve thanh)", label, role, _sf)
                        try:
                            c.flee_mode = True
                            c._wait_combat_clear(idle=2.0, cap=20.0)
                            if _sf:
                                c.navigate_to(*_jitter(_sf), flee=True,
                                              abort=lambda: (_stopped() or not c.running))
                            # Boss QD la instance SOLO -> roi party truoc khi vao.
                            try:
                                c.leave_party()
                                if is_leader:
                                    reset_party_joined(pidx)
                                else:
                                    unmark_joined(pidx, c.self_entity)
                            except Exception as e:
                                log.warning("[%s] boss QD: loi roi party truoc khi danh: %s",
                                            label, e)
                            c.do_legion_boss()
                        except Exception as e:
                            log.warning("[%s] loi boss QD tai bai train: %s", label, e)
                        finally:
                            boss_reform_pending = False   # danh xong/loi -> vong sau tu check lai
                elif mode in ("city", "stand"):   # city/stand: nick dung yen SOLO -> danh thang (event
                    try: c.do_legion_boss()       # KHONG danh mid-session vi se roi khoi map event;
                    except Exception as e:        # digioi mid-session bo qua - login da danh)
                        log.warning("[%s] loi boss QD: %s", label, e)
            # --- MAT PARTY GIUA CHUNG -> GOM LAI NGAY (khong danh le trong luc gom) ---
            # Pha train = GOM DU PARTY roi RA TRAIN. Party tan giua chung (reform lap lai party,
            # member rot, bi keo...) thi viec duy nhat can lam la GOM CHO DU: moi lai lien tuc,
            # va thieu keo dai thi bump reform de gom/sync kenh. KHONG danh le trong luc do:
            # leader dang dung o diem quai nen quai cu vao, no danh MOT MINH (log 27/08
            # 14:36:09-14:37:57: party tan, 4 member dung o safe, leader danh 10 tran mot minh).
            # Cho 20s truoc khi coi la "mat party": lap lai party binh thuong chi mat vai giay.
            if train_on_map and int(st.get("n_members") or 0) > 0:
                if joined_member_count(pidx) >= st["n_members"]:
                    _thieu_since = 0.0
                else:
                    if not _thieu_since:
                        _thieu_since = time.time()
                    elif time.time() - _thieu_since > 20:
                        c.flee_mode = True          # khong danh le trong luc dang gom
                        if is_leader and time.time() - _last_gom > 10:
                            _last_gom = time.time()
                            # MOI LAI thi duoc (moi khong pha party, va leader la nguoi duy nhat
                            # moi duoc). Nhung dung goi do la "GOM LAI": gom la lenh cap party cua
                            # dieu phoi, con nhanh 60s ben duoi tung la cho leader tu ra lenh gom -
                            # gio chi con `pass`, tuc dong log cu hua mot viec khong ai lam.
                            log.warning("[%s] (LEADER) MAT PARTY giua chung (%d/%d) -> MOI LAI "
                                        "(con lech map/kenh thi DIEU PHOI ra lenh gom)",
                                        label, joined_member_count(pidx), st["n_members"])
                            try:
                                _invite_party_participants(c, train_on_map, gap=1.0)
                            except Exception as e:
                                log.warning("[%s] (LEADER) loi moi lai khi mat party: %s", label, e)
            # --- RETRY KENH + RE-MOI moi 60s (ca DG lan map-train) ---
            # Kenh it nguoi nhat co the KHONG du cho ca party -> co dua ket lai kenh cu.
            # Leader cu train; dua chua join thi 1p chuyen lai kenh chung 1 lan; leader 1p moi lai.
            if has_leader and time.time() - last_retry >= 60:
                last_retry = time.time()
                if is_leader:
                    nj = joined_member_count(pidx)
                    if nj < st["n_members"]:
                        # CHECK MAP + KENH TRUOC KHI MOI. Khac map thi leader KHONG THAY entity ->
                        # moi bang niem tin, lap vo han (log 17:33 party 6: leader o map 21826,
                        # 4 member o Truong Sa, cu 60s "MOI LAI" mai khong duoc).
                        _lech = _party_members_off_place(c, pidx)
                        if _lech:
                            # Dong nay tung hua "GOM LAI party truoc" roi chi `pass` - leader khong
                            # gom (dung: L1), nhung log thi noi la co. Ca that party 35, 13/09:
                            #   12:26:35 (LEADER) chua du member (2/4) NHUNG dv608 o map 56103
                            #            (leader 56001) -> KHONG moi mu, GOM LAI party truoc
                            #   12:27:35 / 12:28:38 / 12:29:38  y het, khong ai di dau
                            # Luc do dieu phoi DA nuot mat lenh gom (xem `_dieu_phoi_thi_hanh`), nen
                            # khong con ai gom that - ma doc log lai tuong dang gom.
                            log.warning("[%s] (LEADER) chua du member (%d/%d) NHUNG %s -> KHONG moi "
                                        "mu, CHO DIEU PHOI ra lenh gom",
                                        label, nj, st["n_members"],
                                        "; ".join(_lech))
                        elif _party_khong_thay_nhau(c, pidx):
                            # CUNG map + CUNG kenh (theo so) ma leader CHUA HE thay ho quanh minh
                            # -> thuc te KHAC INSTANCE, hai con so kia noi doi. Loi moi khong the
                            # toi noi va server khong gui ma loi nao.
                            # KHONG ve thanh: chi can LEADER CHON KENH MOI roi dong bo lai. Nhung
                            # ca party PHAI RA DIEM AN TOAN truoc - dung o diem quai thi doi kenh
                            # xong van hong (user chot 30/08). `_party_tai_cho_xu_ly` lam dung thu
                            # tu do: bao ca party ra rally -> CHO xac nhan da ra -> giai tan ->
                            # sync kenh -> moi lai.
                            _kt = _party_khong_thay_nhau(c, pidx)
                            with st["lock"]:
                                st["kenh_hong"] = st.get("channel")   # picker phai TRANH kenh nay
                            log.warning("[%s] (LEADER) chua du member (%d/%d): cung map+kenh %s ma "
                                        "KHONG THAY NHAU (khac instance) -> ra safe roi DOI KENH "
                                        "MOI, khong ve thanh | %s", label, nj, st["n_members"],
                                        st.get("channel"), "; ".join(_kt))
                            _party_tai_cho_xu_ly("khong thay nhau -> doi kenh moi",
                                                 ep_doi_kenh=True)
                        else:
                            log.info("[%s] (LEADER) chua du member (%d/%d), ca party cung map+kenh "
                                     "-> MOI LAI", label, nj, st["n_members"])
                            try:
                                _invite_party_participants(c, train_on_map, gap=1.0)
                            except Exception: pass
                    # DU PARTY roi ma chua train -> SET QS + RA TRAIN. Truoc day dieu kien la
                    # `nj >= 1`: CHI CAN MOT member la leader keo ra spot danh, du party con
                    # thieu 3 nguoi -> pham thang nguyen tac "gom DU party roi moi ra train"
                    # (user 27/08: "phai gom du pt roi ra train chu").
                    # DU PARTY roi ma chua train -> SET QS + RA TRAIN.
                    # Thieu party thi da xu o nhanh tren (moi lai / gom lai).
                    else:
                        # DANG TRONG TRAN thi CHO HET TRAN roi van xu, KHONG bo qua vong nay.
                        # Truoc day la `elif not c.in_combat()` -> leader dung giua bai voi party
                        # da du nhung chua duoc keo ra spot thi quai vao lien tuc, vong cuu nay
                        # KHONG BAO GIO chay: dang danh -> bo qua -> van dung do -> lai dinh tran.
                        # Party 44 (30/08) ket dung the 23 phut: 21:19:07 "DU PARTY (4/4)" ->
                        # 21:19:08 "reform pending -> BO QUA keo ra spot" -> danh le toi 21:40.
                        if c.in_combat():
                            try:
                                c._wait_combat_clear(idle=2.0, cap=60.0)
                            except Exception:
                                pass
                        # RA BAI DANH LA VIEC CUA MODE -> chi lam khi DIEU PHOI bao `VIEC_LAM`
                        # ("party on, cu lam"). Khong co lenh do thi party dang hong (gom / dong bo
                        # / moi) va leader KHONG duoc bo di.
                        #
                        # Ban cu vao nhanh nay bang BA con so cua rieng leader, khong cai nao la
                        # lenh: dong ho `last_retry >= 60`, `joined_member_count` (SO BOT TU NHO -
                        # `_thieu_doi` da bo dung tu 07/09 vi no om stale), va `c.pos` vs
                        # `st["mob_spot"]`.
                        # Ca that party 10, 11/09 (user: "p10, sao leader dung danh 1 minh"):
                        #   12:35:55 [party 10] gen 15: viec=gom - party o 2 MAP khac nhau
                        #   12:36:42 [luubon] (member) -> REFORM party (gen 6) -> DIEU PHOI chot GOM
                        #   12:36:42 [luumot] (LEADER) DU 4/4 member ma KHONG DANH -> RA DIEM QUAI
                        # Bon member di gom, leader ra bai danh mot minh. So "4/4" la so nho, con su
                        # that la party dang o hai map - chinh vi the dieu phoi moi ra lenh gom.
                        # THI HANH LENH, khong tu do. `VIEC_RA_QUAI` la lenh cua dieu phoi: no doc
                        # vi tri THAT cua ca party va ket luan "con dung o safe". `VIEC_DI_TRAIN` va
                        # `VIEC_LAM` deu dan toi `_start_training()` o nhanh duoi.
                        _viec_bay_gio = (_ke_hoach(st) or {}).get("viec")
                        if _viec_bay_gio not in (None, VIEC_LAM, VIEC_DI_TRAIN, VIEC_RA_QUAI):
                            log.info("[%s] (LEADER) chua ra bai: dieu phoi dang ra lenh '%s'",
                                     label, _viec_bay_gio)
                            _spot = None
                        elif _viec_bay_gio == VIEC_RA_QUAI:
                            _spot = st.get("mob_spot")
                        else:
                            _spot = None
                        # DUNG SAI CHO (con o safe, chua ra diem quai) -> ep ra spot. Day la chi
                        # tiet THI HANH cua lenh `VIEC_LAM`, khong phai mot quyet dinh cap party:
                        # bam VI TRI THAT thay vi co `training_started` - co do tung duoc gan True
                        # ke ca khi `_start_training()` bail giua chung, lam vong cuu chet han
                        # (party 3, 30/08: "DU PARTY (4/4)" 14:33:44 -> dung im o (1520,400) toi
                        # 14:41+ khong danh mot tran nao).
                        if _spot and _xa_diem_quai(c.pos, _spot):
                            log.warning("[%s] (LEADER) DU %d/%d member ma KHONG DANH: dang o %s, "
                                        "diem quai %s -> RA DIEM QUAI (khong ve thanh)",
                                        label, nj, st["n_members"], c.pos, _spot)
                            try:
                                # ep_ra_spot: bo chot "reform pending". Chot do so voi _rg_base -
                                # moc chup TRUOC pho ban - nen mot khi da bump la dung MAI MAI, con
                                # reform that thi bi nuot ngay sau (reform_gen_handled = gen hien
                                # tai vi leader dang dung dung map train) -> ket vinh vien.
                                training_started = _danh_dau_training(st, bool(_start_training(ep_ra_spot=True)))
                            except Exception as e:
                                log.warning("[%s] loi start training: %s", label, e)
                        elif _viec_bay_gio not in (None, VIEC_LAM, VIEC_DI_TRAIN):
                            pass          # dang co lenh khac (gom / dong bo / moi) -> theo lenh do
                        elif not training_started or _viec_bay_gio == VIEC_DI_TRAIN:
                            # `VIEC_DI_TRAIN` = dieu phoi thay CA PARTY du doi, cung map/kenh, ma
                            # chua toi map train -> keo di. Khong xet `training_started` (co cua
                            # rieng leader, tung duoc gan True oan lam vong cuu chet han - party 3,
                            # 30/08); lenh cua dieu phoi thang co nho.
                            log.info("[%s] (LEADER) lenh '%s' -> SET QS + ra train",
                                     label, _viec_bay_gio or VIEC_LAM)
                            try:
                                training_started = _danh_dau_training(st, bool(_start_training()))
                            except Exception as e:
                                log.warning("[%s] loi start training: %s", label, e)
                        elif c.flee_mode:
                            # DA o diem quai, party DU, ma van BO CHAY -> khong tran nao ket thuc
                            # duoc. flee_mode bat len o nhanh "MAT PARTY -> GOM LAI" (va vai cho
                            # khac) nhung KHONG cho nao ha xuong khi party day lai.
                            log.warning("[%s] (LEADER) DU %d/%d member, DA o diem quai ma con "
                                        "flee_mode -> TAT flee, danh tiep", label, nj,
                                        st["n_members"])
                            c.flee_mode = False
                            try: c.combat_ready()
                            except Exception: pass
                elif not is_joined(pidx, c.self_entity):
                    if st["leader_gone"].is_set():
                        pass   # chu pt da out -> KHONG retry vao party nua (vo nghia)
                    else:
                        # DON PARTY MA truoc khi retry. Server KHONG gui loi moi cho nguoi DANG
                        # O PARTY, ma bot chi gui goi roi party khi TRANG THAI LOCAL noi minh
                        # dang o party (xem do_channel_sync). Local rong + server con giu party
                        # cu = ket vinh vien: leader moi hoai, member khong bao gio nhan duoc goi.
                        # Party 15 (27/08): roster cuoi cung 08:48:44, sau do 35 phut leader gui
                        # hang tram luot moi ma 2/4 member khong nhan duoc goi nao.
                        # Chi lam khi local KHONG thay roster nao - dang o party that thi khong dung.
                        # NGOAI TRU: roster SERVER (S:013-006 -> c.team_of) noi minh dang o party
                        # cua NGUOI KHAC. Do la su that tu server, manh hon trang thai local, va
                        # la dung cai ket cua party 2 (30/08): nasau cung map 21851 + cung kenh 3
                        # (co ack 0x07) ma 6 phut khong vao noi party, leader khong nhan duoc mot
                        # ma tu choi nao - vi server khong gui loi moi cho nguoi dang o party.
                        # Entity LIVE cua acc leader trong chinh tien trinh nay (dung o ca hai cho:
                        # nhan ra "party cua leader minh" va gui dung ID doi truong khi roi).
                        _chu_ent = None
                        try:
                            for _u2, _p2, _il2, _ip2 in party_accounts(pidx):
                                if _il2:
                                    _chu_ent = getattr(account_clients.get(_u2), "self_entity", None)
                                    break
                        except Exception:
                            _chu_ent = None
                        _ket_party_la = None
                        try:
                            _ket_party_la = c._doi_truong_dang_ket()
                            if _ket_party_la and bytes(_ket_party_la) == bytes(c.self_entity):
                                _ket_party_la = None   # doi truong party 1 nguoi cua chinh minh
                            elif _ket_party_la and _chu_ent and bytes(_ket_party_la) == bytes(_chu_ent):
                                # PARTY CUA CHINH LEADER MINH - khong phai "party la". Nhanh nay chay
                                # khi so nho `is_joined` noi la chua vao, ma roster SERVER noi la roi:
                                # su that thuoc ve server, so nho sai. Roi party o day = TU DA MINH RA
                                # khoi doi vua vao, roi leader moi lai, lai roi... vong vo tan.
                                # Log 31/08 party 7 (19:22): ca 4 member "roster SERVER noi minh dang o
                                # party cua e3f4e44c -> ROI PARTY DO" - ma e3f4e44c CHINH LA ttsau,
                                # leader cua ho; ngay sau do leader bao "MAT PARTY giua chung (0/4)".
                                log.info("[%s] (member) roster SERVER noi minh DA o party cua LEADER "
                                         "%s -> so dem noi bo sai, ghi nhan da vao (KHONG roi party)",
                                         label, bytes(_chu_ent).hex()[:8])
                                _ket_party_la = None
                                try:
                                    mark_joined(pidx, c.self_entity)
                                except Exception as e:
                                    log.debug("[%s] (member) ghi nhan da vao party loi: %s", label, e)
                        except Exception:
                            _ket_party_la = None
                        if _ket_party_la:
                            log.warning("[%s] (member) roster SERVER noi minh dang o party cua %s "
                                        "-> ROI PARTY DO truoc khi cho moi lai", label,
                                        bytes(_ket_party_la).hex()[:8])
                        if _ket_party_la or not getattr(c, "party_members", None):
                            try:
                                # PHAI gui ID DOI TRUONG (C:013-004 <離開隊伍> +隊長玩家ID).
                                # Member ket ngoai party thi CHUA NHAN roster nao -> c.party_leader
                                # rong -> tu no khong biet gui ID nao. Lay entity live cua acc
                                # leader trong chinh tien trinh nay.
                                c.leave_party(leader_entity=_chu_ent)   # _chu_ent tinh o tren
                                unmark_joined(pidx, c.self_entity)
                            except Exception as e:
                                log.debug("[%s] (member) don party ma loi (bo qua): %s", label, e)
                        ch = st.get("channel")
                        if ch:
                            log.info("[%s] (member) chua vao party -> retry chuyen kenh %d", label, ch)
                            try:
                                ok = c.switch_channel(ch, theo_lenh=True)
                                if ok:
                                    with st["lock"]:
                                        sync_gen = st.get("channel_sync_gen", 0)
                                        expected_map = st.get("channel_expected_map")
                                    if _ghi_sync_that_bai(
                                            st, username, c.current_map, sync_gen,
                                            expected_map, label=label):
                                        log.info("[%s] (member) retry kenh %d -> map=%s (sync "
                                                 "gen=%s)", label, ch, c.current_map, sync_gen)
                                time.sleep(1); c.combat_ready()
                                if not ok:
                                    log.warning("[%s] (member) retry chuyen kenh %d THAT BAI", label, ch)
                            except Exception:
                                pass
            if train_on_map:
                pass   # leader da chay long vong (run-around) tu dong tim quai
            elif not is_digioi:
                pass   # city/stand: DUNG YEN, khong lam gi them
            else:
                # DG: dem nguoc thoi gian con lai (digioi_minutes tu S2C 0x55), 30s/lan
                if c.current_map == config.DIGIOI_MAP_ID and time.time() - last_dg >= 30:
                    last_dg = time.time()
                    if digioi_solo:
                        # BAO HIEM: het thuoc GIUA CHUNG (da dung dan) -> DUNG YEN lai; co thuoc
                        # tro lai (nhat item/mua them tay) -> tu chay tiep, KHONG can restart bot.
                        _ok = c.has_hp_and_sp_items()
                        if _ok and c.flee_mode:
                            c.flee_mode = False; c.combat_ready(); c.start_run_around()
                            log.info("[%s] Di Gioi SOLO: da co du thuoc HP+SP tro lai -> chay tiep", label)
                        elif not _ok and not c.flee_mode:
                            c.flee_mode = True
                            log.warning("[%s] Di Gioi SOLO: HET thuoc HP hoac SP -> DUNG YEN", label)
                    remain = max(0, int(DIGIOI_LIMIT - c.digioi_minutes_live()))
                    h, m = divmod(remain, 60)
                    log.info("[%s] Di Gioi con lai: %dh%dm (da o %d phut)",
                             label, h, m, c.digioi_minutes)
                    if remain <= 5:
                        log.warning("[%s] SAP HET GIO DI GIOI (%d phut)!", label, remain)
                    # HET GIO DG ma VAN CON TRONG map DG (server khong kick) -> CHU DONG thoat +
                    # danh solo daily dungeon roi dong acc. Truoc day chi danh dungeon khi BI DAY RA
                    # khoi DG -> con o trong DG thi ngoi i, khong bao gio danh dungeon.
                    if remain <= 0:
                        log.warning("[%s] (%s) HET GIO DG (van trong DG) -> thoat + solo daily dungeon%s",
                                    label, role, "" if (do_daily and not dt_mode) else " (doi DG+Train/tat dungeon)")
                        if not dt_mode:
                            _go_town_safe(c, label)
                            _maybe_auto_world_boss("het gio DG, truoc pho ban doi")
                            if auto_team_dungeon:
                                _run_auto_team_dungeons_if_needed(c, st, username, label, pidx,
                                                                  is_leader, _stopped, pcfg)
                        if do_daily and not dt_mode:
                            try: c.do_daily_dungeon()
                            except Exception as e:
                                log.warning("[%s] loi daily dungeon sau DG: %s", label, e)
                        dt_dg_finished = dt_mode
                        break
                # KHONG con dung map DG (chet bi day ra town / loi) lien tuc ~10s. Phan biet TIMER:
                #   - con gio (>=2 phut) -> bi day ra SOM -> VAO LAI DG ngay
                #   - het gio that -> thoat party + danh solo daily dungeon roi dong acc
                if c.current_map is not None and c.current_map != config.DIGIOI_MAP_ID and not c.in_combat():
                    out_cnt += 1
                    if _dg_gp_out_since is None:
                        _dg_gp_out_since = time.time()
                    if out_cnt >= 2:   # ~10s lien tuc ngoai DG
                        remain = max(0, int(DIGIOI_LIMIT - c.digioi_minutes_live()))
                        _back_in = False
                        # KET NGOAI DG >120s lien tuc: du dong ho noi bo con bao con gio (remain>=2) va
                        # enter_di_gioi_safe() "vao gia" (True roi bi day ra ngay) -> KHONG thu vao lai
                        # nua, EP het gio -> bao xong. Tranh loop "VAO LAI DG" vo han o 12003 lam ca
                        # party cho doi (bug that: acc @12003 khong bao xong DG).
                        _stuck_long = (time.time() - _dg_gp_out_since) > 120
                        if remain >= 2 and not _stuck_long:
                            log.warning("[%s] (%s) KHONG o trong DG (map=%s, chet/bi day ra?) "
                                        "con %d phut -> thu VAO LAI DG", label, role, c.current_map, remain)
                            try: _back_in = c.enter_di_gioi_safe()
                            except Exception: _back_in = False
                        elif _stuck_long:
                            log.warning("[%s] (%s) KET NGOAI DG >120s (map=%s, con %d phut noi bo nhung "
                                        "server da ra) -> EP HET GIO, bao xong DG", label, role,
                                        c.current_map, remain)
                        if _back_in:
                            out_cnt = 0
                            _dg_gp_out_since = None
                        else:
                            # remain<2 HOAC con gio noi bo nhung SERVER KHONG CHO VAO LAI (dong ho noi
                            # bo tre) = HET GIO THAT. Truoc day nhanh remain>=2 VUT return cua
                            # enter_di_gioi_safe() -> lap "VAO LAI DG" mai o Quang Truong (12003), KHONG
                            # bao xong DG -> ca party cho vo han (bug that: dv607@map12003 13:31).
                            log.warning("[%s] (%s) HET GIO DG that (map=%s) -> thoat party%s",
                                        label, role, c.current_map,
                                        " + solo daily dungeon" if (do_daily and not dt_mode) else "")
                            if not dt_mode:
                                _go_town_safe(c, label)
                                _maybe_auto_world_boss("het gio DG, truoc pho ban doi")
                                if auto_team_dungeon:
                                    _run_auto_team_dungeons_if_needed(c, st, username, label, pidx,
                                                                      is_leader, _stopped, pcfg)
                            if do_daily and not dt_mode:
                                c.do_daily_dungeon()
                                # XONG DG -> nhiem vu NANG (boss o2 + claim not hang/cot + tong ket).
                                # o1 dungeon vua danh o tren; o5 team dungeon chua co.
                                try: c.claim_daily_quests(heavy=True)
                                except Exception as e:
                                    log.warning("[%s] loi claim daily quest (bo qua): %s", label, e)
                            dt_dg_finished = dt_mode
                            break
                else:
                    out_cnt = 0
                    _dg_gp_out_since = None   # dang trong DG/dang danh -> reset dong ho ket-ngoai
        # MODE DG+Train: xong DG -> ve thanh DUNG YEN cho CA PARTY xong roi relogin sang pha train.
        if dt_dg_finished:
            _finish_digioi_train_after_dg()
            _ket_thuc_pha_dg()
            return
        try: c.close()
        except Exception: pass
        if c in _clients: _clients.remove(c)
    except Exception as e:
        import traceback
        _unexpected_error = True   # loi bat ngo -> KHONG de acc chet han, cho supervisor relogin lai
        _reason("LOI ngoai le: %s" % e)
        log.error("[%s] LOI: %s\n%s", label, e, traceback.format_exc())
    finally:
        # Go entity minh khoi _PARTY_JOINED: acc thoat/rot la RA KHOI party that (server tan lien
        # ket). Khong go -> lan reconnect sau leader dem "da join" STALE -> vua moi da tuong du
        # 4/4 (khong ai accept that) -> leader danh 1 minh ca phien (bug thuc te DG 09:18).
        if c is not None:
            try: unmark_joined(c.party_idx, c.self_entity)
            except Exception: pass
        # RECONNECT: server ROT (server_closed) + khong phai GUI-STOP -> supervisor se login lai.
        # Khi do KHONG set leader_gone (member phai CHO, dung thoat theo) + KHONG tong ket.
        # KHONG doi hoi has_leader: party "khong co chu PT" (vd dung yen trong DG cho moi tay) truoc
        # day rot mang la CHET LUON du con gio -> user bao "dang o DG con time ma tu out". Gio moi
        # mode deu tu login lai; dung han chi khi GUI Stop / thoat binh thuong (het gio DG...).
        # Ma 60 la tin hieu CHINH THUC cua protocol: server tat may bao tri. Day khong phai
        # rot mang tam thoi, nen cam supervisor quay vong login. Mot acc nhan duoc la tat TOAN
        # BO account dang chay trong app; cac ma khac van giu reconnect/backoff nhu cu.
        _maintenance = bool(c is not None
                            and int(getattr(c, "disconnect_cause", 0) or 0) == 60)
        if _maintenance:
            _stop_all_accounts_for_maintenance(username)
            _reason("SERVER BAO TRI (ma 60) -> da OFF tat ca account, khong reconnect")
        _forced_reconnect = username in account_forced_reconnect
        reconnectable = (not _stopped()
                         and not _maintenance
                         and (_forced_reconnect or _login_failed or _dt["relogin_train"]
                              or _unexpected_error
                              or (c is not None and getattr(c, "server_closed", False))))
        account_reconnect[username] = reconnectable
        # NOI DUNG LY DO. Cau log cua supervisor mac dinh la "server rot" -> relogin CO Y (chuyen
        # pha train sau khi xong DG + viec vat) nhin y het bi server da. Da ton mot buoi truy vu
        # "5 acc party 25 rot lien tuc" ma that ra KHONG acc nao bi rot: khong he co dong
        # "Server dong ket noi", khong co goi-cuoi nao - vi don gian la server khong dong gi ca.
        if (reconnectable and not _forced_reconnect and not _login_failed and not _unexpected_error
                and not (c is not None and getattr(c, "server_closed", False))
                and _dt["relogin_train"]):
            account_forced_reconnect_reason[username] = "chuyen pha TRAIN (relogin CO Y, khong phai rot)"
        if is_leader and not reconnectable and account_threads.get(username) is threading.current_thread():
            if c is not None and getattr(c, "_individual_safe_logout", False):
                # Nut OUT trong tab leader chi tat RIENG leader. Safe-logout worker da keo ca
                # team ve safe + giai tan PT; member phai o lai cho leader Login lai, khong duoc
                # doc nhu "party ket thuc" va thoat theo.
                with st["lock"]:
                    st["leader_manual_off"] = True
                st["leader_gone"].clear()
                log.info("[%s] LEADER OUT RIENG -> member giu online tai safe, cho leader rejoin",
                         label)
            else:
                st["leader_gone"].set()   # leader ket thuc that su -> member ngung co vao party
        # ghi lai ly do thoat (neu GUI bam STOP ma chua co ly do cu the -> ghi STOP)
        if _stopped() and er["r"].startswith("ket thuc binh thuong"):
            _reason(account_stop_reasons.get(username) or "STOP")
        # SERVER chu dong dong ket noi (rot/bao tri/kick) - KHONG phai ket thuc binh thuong/STOP
        elif (not _stopped() and er["r"].startswith("ket thuc binh thuong")
              and c is not None and getattr(c, "server_closed", False)):
            _reason("SERVER dong ket noi (rot mang/bao tri/kick) - khong phai tu thoat")
        account_exit_reason[username] = er["r"]
        if _stopped():
            account_stop_reasons.pop(username, None)
        # LUU map + ten nhan vat + LEVEL char/pet + ten pet LUC THOAT -> GUI van hien thong tin
        # nhu luc truoc khi tat (truoc day chi luu map+char -> tat la mat level/pet).
        if c is not None and getattr(c, "current_map", None) is not None:
            account_last[username] = {"map": c.current_map, "char": c.char_name or username,
                                      "char_level": getattr(c, "char_level", None),
                                      "pet_name": c.pet_name_out(),
                                      "pet_level": getattr(c, "pet_level", None)}
        account_clients.pop(username, None)
        if not reconnectable:   # reconnect thi CHUA tong ket "party thoat het" (nick se login lai)
            try:
                _party_exit_summary(pidx, username)   # neu ca party da tat -> log 1 dong tong ket
            except Exception:
                pass


# ============================================================
#  API DIEU KHIEN (cho GUI gui.py goi). Cung dung cho CLI ben duoi.
# ============================================================
def _leader_live_phase(pidx, st):
    """Pha (phase) hien tai cua LEADER neu leader dang LIVE; None neu khong co leader / leader
    dang reconnecting / activity cu (>45s khong update -> thread chet/treo, khong tin duoc).
    Dung cho member: leader la nhac truong -> member dong bo theo pha leader (chi khi leader live)."""
    lead = config.PARTY_LEADER_ACC.get(pidx)
    if not lead:
        return None
    with st["lock"]:
        if lead in st["reconnecting"]:
            return None
    act = get_account_activity(lead)
    if not act:
        return None
    _task, phase, age = act
    if age > 45:
        return None            # leader im >45s -> khong live -> member CHO (leader se tu resync khi ve)
    return phase


def party_accounts(pidx):
    """List (username, password, is_leader, is_picker) cua party pidx (bo slot trong)."""
    party = config.PARTIES[pidx]
    leader_acc = config.PARTY_LEADER_ACC.get(pidx)
    valid = [(u, p) for u, p in party if u and u.strip()]
    picker_acc = leader_acc if leader_acc else (valid[0][0] if valid else None)
    return [(u, p, u == leader_acc, u == picker_acc) for u, p in valid]


def _clear_o5_client_flags(c):
    """Ha co RIENG cua mot acc (dang o trong instance / quest_mode). Pha PB cua CA PARTY thi khong
    nam o day nua - no do dieu phoi giu (`dat_pha_pho_ban`, xem `bot/client._PARTY_PB_PHA`)."""
    active = (
        time.time() < getattr(c, "_team_dungeon_until", 0.0)
        or getattr(c.state, "quest_mode", False)
    )
    c._team_dungeon_until = 0.0
    c.state.quest_mode = False
    return active


def _team_dungeon_flags(pcfg):
    norm = getattr(config, "normalize_team_dungeons", lambda v: v)(pcfg.get("team_dungeons"))
    if not isinstance(norm, dict):
        norm = getattr(config, "DEFAULT_TEAM_DUNGEONS", {20: True, 50: True, 80: True})
    return {int(k): bool(v) for k, v in norm.items()}


def _force_supervisor_reconnect(username, c, reason):
    account_forced_reconnect.add(username)
    account_forced_reconnect_reason[username] = reason
    try:
        c.close()
    except Exception:
        pass
    return False


def _stop_all_accounts_for_maintenance(trigger_username=""):
    """Server gui disconnect cause=60: OFF moi account, tuyet doi khong reconnect.

    Chi set stop + dong socket tren snapshot de an toan voi cac supervisor dang thay doi dict.
    Nguoi dung van co the bam Login lai sau khi bao tri ket thuc; start_account se tao stop Event
    moi cho phien moi.
    """
    log.error("[BAO TRI] server gui ma 60 cho %s -> OFF TAT CA ACCOUNT, KHONG RECONNECT",
              trigger_username or "mot account")
    for user, ev in list(account_stops.items()):
        account_stop_reasons[user] = "Server bao tri (ma 60) - da off tat ca account"
        try:
            ev.set()
        except Exception:
            pass
        account_reconnect[user] = False
        account_forced_reconnect.discard(user)
        account_forced_reconnect_reason.pop(user, None)
    for _user, cli in list(account_clients.items()):
        try:
            cli.close()
        except Exception:
            pass


def _thoat_pb_ca_party(pidx, ly_do):
    """KEO CA PARTY ra khoi instance pho ban - MOT lenh, dieu phoi lam, khong ai phai tu lo.

    PB VO thi server KHONG gui goi ket thuc (`S:047-012`), nen khong acc nao tu biet ma ra. Truoc
    day moi leader tu goi `_exit_pb_or_reconnect` cho CHINH NO -> leader dung ngoai thanh, member
    con nguyen trong map 62xxx, va `go_to_town` cua ho thi BAIL vi dang trong pho ban to doi
    (user 07/09: "leader o ngoai con member van trong PB kia").

    Ca party chay trong MOT tien trinh nen keo het ra la viec lam duoc ngay (L2: khong ai phai cho
    ai bao cao). Acc nao khong con o instance thi bo qua - `leave_team_dungeon` da tu kiem."""
    ra = 0
    for _t in party_accounts(pidx):
        _c = account_clients.get(_t[0])
        if _c is None or not getattr(_c, "running", False):
            continue
        if not in_instance_map(int(getattr(_c, "current_map", 0) or 0)):
            continue
        try:
            if _c.leave_team_dungeon():
                ra += 1
        except Exception as e:
            log.debug("[%s] thoat PB (%s) loi: %s", _t[0], ly_do, e)
    if ra:
        log.info("[party %d] DIEU PHOI: %s -> keo %d acc ra khoi pho ban (C:047-010)",
                 pidx + 1, ly_do, ra)
    return ra


def _exit_pb_or_reconnect(username, c, reason):
    """RA KHOI instance pho ban roi bao "da xu ly" (tra False y het _force_supervisor_reconnect).

    Uu tien lenh THOAT cua client C:047-010 (giu nguyen ket noi). CHI relogin khi thoat khong duoc:
    tu khi server chan toc do dang nhap (ma 90), relogin de dong bo PB lam acc ket vong login hang
    phut (log that party 6, 23:15-23:25). Rule retry PB (team_dungeon_need_redo / o5_need_redo)
    KHONG doi - caller da danh dau truoc khi goi ham nay.
    """
    try:
        # leave_team_dungeon() tra True o CA HAI truong hop: vua gui lenh thoat XONG, va "von da o
        # NGOAI nen khong gui gi". Truoc day cau log duoi noi chung la "da thoat PB bang C:047-010"
        # -> nhin log tuong bot VAN gui goi giua thanh (user chan doan nham 25/08). Phan biet ra.
        _trong_pb = in_instance_map(c.current_map)
        if c.leave_team_dungeon():
            log.info("[%s] %s -> KHONG relogin (%s)", username,
                     "da thoat PB bang C:047-010" if _trong_pb else "von da o NGOAI pho ban",
                     reason)
            return False
    except Exception as e:
        log.warning("[%s] loi thoat PB (%s) -> quay ve relogin: %s", username, reason, e)
    return _force_supervisor_reconnect(username, c, reason)


# (XOA 13/09 `READY_WAIT_SPLIT_SEC`: nguong de LEADER tu quyet luc nao gom party lech map. Lech
#  map la viec cua dieu phoi - no doc map ca party moi 2 giay va ra lenh gom.)
READY_WAIT_REFORM_SEC = 120  # moi mai khong du party -> bo luot moi nay, de dieu phoi quyet tiep
TEAM_DUNGEON_MAX_TRIES = 2   # 1 lan dau + 1 lan RETRY. Qua so nay -> BO QUA HET cac PB.


def _mark_team_dungeon_broken(st, level):
    # Dem so lan da thu level nay. Team yeu thi retry vo han chi lam ket ca party ca dem
    # (log 23:45-23:47: fail -> relogin -> fail -> relogin...). User: moi PB chi retry 1 LAN,
    # van khong qua -> bo qua cac PB CON LAI CUA LUOT NAY roi di lam viec tiep theo. KHONG
    # khoa ca ngay: luot chay sau van check PB binh thuong.
    # CHI dem/clear MOT LAN moi chu ky vo (khi need_redo dang False -> True). Bug that: MOI acc goi
    # _mark deu clear recover_seen -> acc phat hien PB vo TRE (thuong leader o 4231) clear MAT nhung
    # member da qua barrier (_prepare da add seen roi vao vong cho) -> ket "4/5" vo tan, khong bao gio
    # du 5. Ngoai ra tries + 1/acc lam skip_all som. Guard bang need_redo (moi caller deu giu st.lock).
    fresh = not st.get("team_dungeon_need_redo")
    if fresh:
        tries = st.setdefault("team_dungeon_tries", {})
        tries[level] = tries.get(level, 0) + 1
        if tries[level] >= TEAM_DUNGEON_MAX_TRIES:
            st["team_dungeon_skip_all"] = True
    st.setdefault("team_dungeon_broke", {})[level] = True
    st["team_dungeon_need_redo"] = True
    st.setdefault("team_dungeon_state", {})[level] = "done"


def _pb_vo_don_lai(st, pidx, label):
    """PB vo -> DON LAI de chay lai, KHONG cho ai relogin ca.

    Ban cu la mot BARRIER diem danh: moi acc `seen.add(username)`, ai toi cuoi thi `Event.set()`,
    nhung acc nao chua toi thi ngoi trong `while not ev.is_set(): sleep(1)` va cu 30s in
    "cho ca party relogin sau PB vo (1/5)...". Do la vi pham thang L2 (bat acc bao cao) va L9
    (Event cho acc khac) - `documents/RULE_DIEU_PHOI.md`.

    Ca that 07/09 p42: PB lv50 XONG luc 10:35:48, leader rot ngay sau -> bot cham "PB vo" ->
        10:39:15..10:40:16 [luubmot] auto phó bản đội: chờ cả party relogin sau PB vỡ (1/5)...
        10:40:39 [luubhai]  go_to_town: DANG TRONG pho ban to doi (map=62012) -> khong teleport
        10:40:39 [luubhai]  (member) reform: CHUA ve duoc Hội Kê (map=62012) -> nghi 10s thu lai
    Leader cho member relogin; member khong relogin ma di reform, ma reform bi chan vi con trong
    phong. Hai ben cho nhau, khong ai ra.

    Gio KHONG cho: don lai trang thai (mot lan, idempotent duoi lock) roi ai lam viec nay di. Party
    lech nhau la viec cua dieu phoi - no da co `reform_gen`/`VIEC_GOM` de gom, va no DOC THANG
    `account_clients[u]` chu khong doi ai diem danh."""
    with st["lock"]:
        if not st.get("team_dungeon_need_redo"):
            return True   # acc khac da don roi -> khong don hai lan, khong log hai lan
        _skip_all = bool(st.get("team_dungeon_skip_all"))
        st["team_dungeon_broke"] = {}
        st["team_dungeon_need_redo"] = False
        if not _skip_all:
            st["team_dungeon_state"] = {}   # xoa -> chay lai tu dau (lan RETRY duy nhat)
    if _skip_all:
        log.warning("[%s] auto phó bản đội: RETRY vẫn KHÔNG qua -> BỎ QUA các phó bản đội còn "
                    "lại của lượt này, chuyển sang việc tiếp theo", label)
    else:
        log.info("[%s] auto phó bản đội: PB vỡ -> dọn lại, chạy lại từ đầu (lần retry duy nhất). "
                 "KHÔNG chờ ai relogin - điều phối lo gom party.", label)
    return True


def _prepare_team_dungeon_redo_after_reconnect(st, username, label, pidx, stopped_fn):
    if stopped_fn():
        return False
    return _pb_vo_don_lai(st, pidx, label)


def _thieu_level(st, members, level):
    """[(username, cap)] cac acc CHUA du cap vao PB lv`level`. PB lvN yeu cau nhan vat cap >= N.

    Acc chua doc duoc cap (chua co goi 0x05) thi KHONG tinh la thieu - tha cho thu con hon bo oan.

    Vi sao can: server khong cho acc duoi cap READY trong phong PB. Bot khong biet dieu do nen cu
    tao phong roi cho 40s, "lv80 member ready 0/4 -> HUY phong, relogin ca party", lap lai moi chu
    ky. Nang hon: `_finish_digioi_train_if_time_over` thay PB hong la `return` NGAY, bo luon
    `do_daily_dungeon()` (o 1) nam ngay duoi -> cuoi ngay 6 acc party 19 thieu o 1 (02/09).
    """
    # DOC THANG cap cua tung acc tu client - khong acc nao phai "bao cap" nua (L2).
    # Acc chua doc duoc cap (chua co goi 0x05) thi KHONG tinh la thieu - tha cho thu con hon bo oan.
    thieu = []
    for m in members:
        c = account_clients.get(m)
        _lv = getattr(c, "char_level", None) if c is not None else None
        if _lv and int(_lv) < int(level):
            thieu.append((m, int(_lv)))
    return thieu


def _handle_auto_team_dungeon(c, st, username, label, pidx, is_leader, stopped_fn, level):
    level = int(level)
    # Daily tay gom nhieu level trong MOT task ngoai. Vong ngoai chi dat `daily_task` theo muc
    # dau tien (thuong la lv20), con ham nay tu chay tiep 50/80/110. Neu khong cap nhat o day,
    # tile Android bi ket "Phu ban to doi cap 20" du log va map da sang level sau.
    # Chi leader ghi trang thai chung de bon member dang cho khong tranh nhau ghi lui level cu.
    if is_leader and st.get("daily_active"):
        with st["lock"]:
            st["daily_task"] = "team_dungeon_%d" % level
            st["daily_phase"] = "running"
            st["daily_user"] = username
            _pb_name = {20: "Thảo Phạt Thiên Sư", 50: "Ngày Tàn Hoạn Quan",
                        80: "Đại Chiến Lữ Bố", 110: "Hỏa Thiêu Bộc Dương"}.get(level, "Phụ bản tổ đội")
            st["daily_message"] = "Đang chạy %s • Cấp %d" % (_pb_name, level)
    if not c.wait_mission_steps(timeout=6.0):
        remaining = None
        log.warning("[%s] (%s) phó bản đội lv%d: chưa có status 0x18 -> bỏ qua level này",
                    label, "LEADER" if is_leader else "member", level)
    else:
        remaining = c.team_dungeon_remaining(level)
    with st["lock"]:
        pass
    has_leader = config.PARTY_LEADER_ACC.get(pidx) is not None
    if not has_leader:
        return True
    if not is_leader:
        last_log = 0.0
        _wd0 = time.time()
        with st["lock"]:
            _pb_g0 = st["reform_gen"]     # gen luc BAT DAU cho; doi = leader da goi ve thanh
        while True:
            if stopped_fn() or not c.running:
                return False
            # KHONG dat _barrier_watchdog o day! Member cho leader chay HET pho ban la CHUYEN
            # BINH THUONG va lau 10-20 phut (5 tran + di duong + thoai). Watchdog 180s tuong la
            # "ket" -> ep dong bo -> DA CA 4 MEMBER RA relogin GIUA pho ban, pha nat luot PB
            # (log that 14:03:10 member bat dau cho -> 14:06:10 = dung 180s -> "EP DONG BO").
            # Vong nay da co du duong thoat: stop/rot, st["reconnecting"], leader bao xong.
            _resync_ck(st, username)   # ep dong bo TAY (GUI) van thoat duoc
            set_account_activity(username, "PB lv%d: cho leader xu ly" % level, phase="wait")
            if time.time() - last_log > 60:
                log.info("[%s] (member) chờ leader xử lý phó bản đội lv%d...", label, level)
                last_log = time.time()
            if st["reconnecting"]:
                log.warning("[%s] (member) đồng đội rớt trong phó bản đội lv%d -> relogin thoát instance",
                            label, level)
                with st["lock"]:
                    _mark_team_dungeon_broken(st, level)
                _clear_o5_client_flags(c)
                return _exit_pb_or_reconnect(
                    username, c, "phó bản đội vỡ do đồng đội rớt"
                )
            # LEADER BI KEO DI REFORM trong luc minh dang cho -> phai THEO, khong thi ca party
            # ket (log 14:04: leader "CHO ca party ve thanh 23001 1/5", 4 member "cho leader xu ly
            # PB"). Ngoi no hay gap: acc con luot BOSS THE GIOI danh 10-20' -> sync kenh timeout
            # 60s -> bump reform_gen -> leader bo PB di reform, member khong hay biet.
            # DAT SAU nhanh "dong doi rot" o tren: nhanh do cung bump reform_gen nhung phai di
            # duong RELOGIN de ca party danh LAI PB - khong duoc de check nay cuop mat.
            with st["lock"]:
                _pb_gnow = st["reform_gen"]
            if _pb_gnow > _pb_g0:
                log.warning("[%s] (member) phó bản đội lv%d: leader chuyển sang REFORM "
                            "(reform_gen %d->%d) -> bỏ chờ, về thành cùng party",
                            label, level, _pb_g0, _pb_gnow)
                return True
            # LENH DANG CO HIEU LUC cung phai bo cho - khong chi lenh MOI HON luc minh vao vong.
            #
            # So `reform_gen` voi moc luc vao vong la acc tu chon "lenh nao nghe": lenh ra TRUOC do
            # vai giay va VAN dang chay thi bi coi la lenh cu.
            #
            # Ca that party 24, 13/09 (user: "thang daihai cu dung o trac quan, ko tap trung ve
            # cung team"):
            #   13:19:21 [daihai] (member) pho ban to doi: HOAN - dieu phoi dang ra lenh 'gom'
            #                     (party dang o 3 MAP khac nhau [12001, 21011, 21852])
            #   13:20:19 [daihai] (member) PB lv50: leader chuyen sang REFORM (1->2) -> bo cho
            #   13:20:19 [daihai] (member) cho leader xu ly pho ban doi lv80...
            #   13:21:19 / 13:22:20  y het nhau, khong dut
            # Sang lv80 no vao vong MOI voi moc gen = 2 - dung bang chinh lenh gom dang chay. Tu do
            # lenh do vinh vien la "cu", nen no dung o Trac Quan cho mot viec khong ai lam.
            if party_dang_gom(pidx) and not st.get("daily_active"):
                log.warning("[%s] (member) phó bản đội lv%d: DIEU PHOI đang ra lệnh cấp party "
                            "-> bỏ chờ leader, về cùng party", label, level)
                return True
            with st["lock"]:
                state = st.setdefault("team_dungeon_state", {}).get(level, "idle")
                broke = bool(st.setdefault("team_dungeon_broke", {}).get(level, False))
            if state == "done":
                if broke:
                    log.warning("[%s] (member) phó bản đội lv%d vỡ -> relogin thoát instance",
                                label, level)
                    _clear_o5_client_flags(c)
                    return _exit_pb_or_reconnect(
                        username, c, "phó bản đội vỡ"
                    )
                _clear_o5_client_flags(c)
                return True
            if not c.in_combat():
                try:
                    c.do_heal()
                except Exception:
                    pass
            time.sleep(2)

    members = [t[0] for t in party_accounts(pidx)]
    if len(members) < 2:
        return True
    last_log = 0.0
    _wd0 = time.time()
    _t0_doc = time.time()
    while True:
        if stopped_fn() or not c.running:
            return False
        _barrier_watchdog(st, pidx, _wd0, "leader-cho-report-PB")
        _resync_ck(st, username)   # ep dong bo -> thoat cho report, relogin bam leader
        # Dong doi rot / PB can danh lai TRONG luc leader dang cho report: truoc day leader KHONG
        # co check nay -> member da _mark_team_dungeon_broken + relogin vao hang recover, con leader
        # ket o vong cho report vo han (bug that: leader "cho report 1/5", member "cho relogin 4/5",
        # 2 hang khac nhau khong bao gio gap). -> leader cung relogin vao hang recover de CA PARTY
        # dong bo roi DANH LAI tu dau (giong het nhanh member o tren).
        with st["lock"]:
            _need_redo = bool(st.get("team_dungeon_need_redo"))
            _recon = bool(st["reconnecting"])
        if _recon or _need_redo:
            log.warning("[%s] (LEADER) đồng đội rớt / PB vỡ khi chờ report lv%d -> kéo CẢ PARTY "
                        "ra khỏi phó bản rồi đánh lại", label, level)
            with st["lock"]:
                _mark_team_dungeon_broken(st, level)
            _clear_o5_client_flags(c)
            # KEO CA PARTY ra truoc: PB vo thi khong co goi ket thuc nen khong ai tu biet duong ra.
            # Truoc day leader chi thoat cho CHINH NO -> leader dung ngoai, member ket trong 62xxx.
            _thoat_pb_ca_party(pidx, "PB lv%d vỡ" % level)
            return _exit_pb_or_reconnect(
                username, c, "phó bản đội vỡ (leader đồng bộ lại để đánh lại)"
            )
        # DOC THANG state cua tung member thay vi CHO no "report": ca party chay chung MOT tien
        # trinh, leader nam san account_clients[m]. team_dungeon_remaining() chi doc state cua
        # chinh client do va tra None khi chua co status 0x18 -> goi thang duoc, khong phai cho.
        # Bat member tu khai la thua VA de ket: member ban viec khac / vua relogin thi khong chay
        # doan report -> leader dung do "cho report (1/5)" hang phut (log that party 1, 00:33).
        _live_rem = {}
        for m in members:
            _mc = account_clients.get(m)
            if _mc is None or not getattr(_mc, "running", False):
                continue
            try:
                _r = _mc.team_dungeon_remaining(level)
            except Exception:
                _r = None
            if _r is not None:
                _live_rem[m] = _r
        reports = dict(_live_rem)
        with st["lock"]:
            _du = all(m in reports or m in st["reconnecting"] for m in members)
        if _du:
            break
        # KHONG cho vo han: acc chua co status 0x18 thi doc lai nhip sau. Nhung phai co han - het
        # han thi quyet voi nhung gi doc duoc (acc thieu coi nhu con luot).
        if time.time() - _t0_doc > 30:
            log.info("[%s] (LEADER) PB lv%d: doc duoc luot cua %d/%d acc -> quyet luon, khong cho",
                     label, level, len(reports), len(members))
            break
        set_account_activity(username, "PB lv%d: doc luot ca party" % level, phase="wait")
        time.sleep(2)

    reports = dict(_live_rem)
    if level not in (20, 50, 80, 110):
        log.warning("[%s] (LEADER) phó bản đội lv%d: đã biết trạng thái lượt nhưng chưa có script "
                    "đường đi/trận an toàn -> bỏ qua", label, level)
        with st["lock"]:
            st.setdefault("team_dungeon_state", {})[level] = "done"
        return True
    # CHUA DU CAP -> bo qua PB nay luon, dung tao phong. Server khong cho acc duoi cap ready nen
    # co co gang cung chi ra "member ready 0/4 sau 40s -> HUY phong, relogin ca party" moi chu ky.
    with st["lock"]:
        _thieu = _thieu_level(st, members, level)
    if _thieu:
        log.info("[%s] (LEADER) phó bản đội lv%d: BỎ QUA - %d acc chưa đủ cấp (%s)", label, level,
                 len(_thieu), ", ".join("%s lv%d" % (m, lv) for m, lv in _thieu))
        with st["lock"]:
            st.setdefault("team_dungeon_state", {})[level] = "done"
        return True
    need = []
    missing = []
    done = []
    for m in members:
        rem = reports.get(m)
        if rem is None:
            missing.append(m)
        elif int(rem) > 0:
            need.append(m)
        else:
            done.append(m)
    if missing:
        log.warning("[%s] (LEADER) phó bản đội lv%d: thiếu status của %s -> bỏ qua",
                    label, level, missing)
        with st["lock"]:
            st.setdefault("team_dungeon_state", {})[level] = "done"
        return True
    if len(need) == len(members):
        log.info("[%s] (LEADER) CA party (%d người) còn lượt phó bản đội lv%d -> chạy",
                 label, len(members), level)
        with st["lock"]:
            st.setdefault("team_dungeon_state", {})[level] = "running"
            st.setdefault("team_dungeon_broke", {})[level] = False
        dg0 = st["disc_gen"]
        ok = False
        broken = False
        # LEADER phai biet dong doi ROT NGAY GIUA pho ban (truoc day chi biet SAU khi danh xong:
        # leader danh mot minh het 5 tran ~15 phut roi moi bao vo - log user 14:06). Cam callback
        # de client tu dung o ranh gioi tran / vong cho ket tran (xem client._td_party_gone).
        c._td_party_broken = lambda: bool(st["reconnecting"]) or st["disc_gen"] > dg0
        try:
            ok = bool(c.do_team_dungeon(level))
            if not ok:
                log.warning("[%s] (LEADER) phó bản đội lv%d trả FAIL", label, level)
            if st["disc_gen"] > dg0 or st["reconnecting"]:
                log.warning("[%s] (LEADER) đồng đội rớt trong phó bản đội lv%d -> relogin thoát instance",
                            label, level)
        finally:
            c._td_party_broken = None   # het pho ban -> go callback (khong de ro ri sang viec khac)
            active = _clear_o5_client_flags(c)
            with st["lock"]:
                _user_stopped = bool(stopped_fn()) and bool(getattr(c, "running", False))
                broken = ((not ok) or (not c.running)
                          or st["disc_gen"] > dg0 or bool(st["reconnecting"]))
                if _user_stopped:
                    # Dung Daily la ket thuc co chu y tai ranh gioi tran, KHONG phai PB vo.
                    # Khong relogin, khong keo ca party ra ngoai, giu acc dung yen tai cho.
                    broken = False
                # PB DA DANH XONG (`ok`) thi ROT SAU DO khong phai la "vo". Van coi la `broken` de
                # thoat instance / relogin, nhung KHONG dem la mot lan thu that bai va KHONG bat
                # lam lai - server da tinh luot roi, danh lai la vo ich.
                # Ca that 07/09 p42, ba dong lien nhau:
                #   10:35:48 [luubmot] (LEADER) === PHO BAN TO DOI LV50 XONG -> roi pho ban ===
                #   10:35:50 [luu401]  RECONNECT: server rot -> login lai sau 5s (lan 1)
                #   10:35:58 [luubmot] SERVER NGAT KET NOI: DANG NHAP TRUNG LAP (ma 19)
                # -> bot danh dau PB vo, ca party vao LAI phong 62012, con leader thi dung
                # "cho ca party relogin sau PB vo (1/5)" - hai ben cho nhau, 5 phut khong ra.
                if broken and not ok:
                    _mark_team_dungeon_broken(st, level)
                st.setdefault("team_dungeon_state", {})[level] = "done"
                # CHI DANH DAU, KHONG bump ngay. Bump o day = bump sau MOI level (20/50/80) ->
                # 3 lan/vong, moi lan da member ra khoi trang thai cho de "ve thanh cung party"
                # roi vai giay sau lai moi vao PB ke tiep (log that 01:12:43-53: lv50 xong ->
                # reform_gen 0->1 -> 4 member "bo cho, ve thanh" -> 01:12:47 duoc moi vao lv80).
                # Reform chi CAN 1 LAN sau khi xong HET cac PB, vi luc do moi that su quay lai train.
                st["td_need_reform"] = True
        if stopped_fn() and getattr(c, "running", False):
            log.info("[%s] (LEADER) Dung Daily sau tran PB lv%d -> dung yen, KHONG relogin",
                     label, level)
            return False
        if broken:
            # KEO CA PARTY ra khoi instance TRUOC. PB vo thi server khong gui `S:047-012` nen khong
            # acc nao tu biet duong ra; leader chi lo minh thi member ket lai trong 62xxx.
            _thoat_pb_ca_party(pidx, "PB lv%d vỡ" % level)
            return _exit_pb_or_reconnect(
                username, c, "phó bản đội vỡ" if active else "phó bản đội fail"
            )
        return c.running
    with st["lock"]:
        st.setdefault("team_dungeon_state", {})[level] = "done"
    log.info("[%s] (LEADER) phó bản đội lv%d: không phải cả party đều còn lượt (đã hết: %s) -> bỏ qua",
             label, level, done)
    return True


def _pb_that_bai_co_phai_dung_han(c, stopped_fn, label, role):
    """PB to doi tra False: co PHAI ly do de DUNG HAN thread khong?

    `_run_auto_team_dungeons_if_needed` tra False cho CA HAI ca: GUI Stop, va PB that bai (vd
    "roster phong chi 2/4 member -> HUY danh" theo rule "phai du pt"). Caller khong phan biet
    duoc nen truoc day cu `c.close(); return` -> thread chet MA KHONG GHI LY DO -> reconnectable
    = False -> st["leader_gone"].set() -> member thay leader chet that -> THOAT THEO -> CA PARTY
    CHET, phai bat tay lai.

    BUG THAT: party 19 (13:46 PB lv20 roster 1/4) va party 35 (13:48 PB lv110 roster 2/4) - ca hai
    deu chet ca party ngay sau khi PB bi huy.

    CHI dung han khi THUC SU la Stop hoac client da chet. PB hong thi chay tiep viec khac."""
    if stopped_fn() or not getattr(c, "running", False):
        return True
    log.warning("[%s] (%s) pho ban to doi khong xong -> KHONG bo party, chay tiep viec khac",
                label, role)
    return False


WB_WAIT_SEC = 300.0     # cho toi da 5' - 1 luot boss ~20s, du cho ca 5 acc danh het luot


def _wait_party_world_boss(st, pidx, label, stopped_fn):
    """LEADER cho CA PARTY danh xong boss the gioi roi moi lap pho ban to doi.

    BUG THAT (log 15:09, party tq4xx): moi acc chay DOC LAP - ai da du 5/5 luot thi xong ngay, con
    tq402 con 0/5 nen dang danh (moi tran ~20s). Leader thay minh xong la lap phong PB va moi luon
    -> tq402 nhan loi moi + an CHUAN BI trong luc DANG TRONG TRAN boss -> khong vao duoc instance ->
    "roster phong pho ban chi 3/4 member" -> HUY danh, ca party thoat ra lam lai.

    Cho theo CO wb_done (dat trong finally cua _maybe_auto_world_boss) chu KHONG theo "dang trong
    tran": acc co the giua 2 luot boss, luc do khong o trong tran nhung van chua xong viec.
    """
    t0 = time.time()
    _log = 0.0
    while not stopped_fn():
        con = [u for u, _p, _l, _k in party_accounts(pidx)
               if is_account_running(u) and account_clients.get(u) is not None
               and u not in (st.get("wb_done") or ())]
        if not con:
            return True
        if time.time() - t0 > WB_WAIT_SEC:
            log.warning("[%s] (LEADER) cho boss the gioi qua %.0fs ma con %s -> lap pho ban luon",
                        label, WB_WAIT_SEC, con)
            return False
        if time.time() - _log > 20:
            _log = time.time()
            log.info("[%s] (LEADER) CHO %d acc danh xong boss the gioi roi moi lap pho ban: %s",
                     label, len(con), con)
        set_account_activity(st.get("leader_user") or label,
                             "cho party xong boss the gioi", phase="wait")
        time.sleep(2)
    return False


def _run_auto_team_dungeons_if_needed(c, st, username, label, pidx, is_leader, stopped_fn, pcfg):
    # DAU VET tren CHINH CLIENT (bot dieu khien acc nen bot BIET no da xong o5 chua) - khong ghi
    # vao bang cap party de acc khac phai doc "bao cao".
    # (truoc day nhan tham so `o5_done` tu acc goi len - da bo cung luc bo bang bao cao o5.
    #  Trang thai o5 la cua CA PARTY nen doc thang tu `st`, khong acc nao truyen vao nua.)
    with st["lock"]:
        c._o5_da_xong = st.get("o5_state") == "done"

    # THU TU user yeu cau: DOI QUA su kien -> NHAN THUONG BANG 3x3 -> roi moi DANH PB TO DOI.
    # Dat o DAY chu KHONG trong claim_daily_quests: mode TRAIN goi thang ham nay, con
    # claim_daily_quests chay SAU va chi khi do_daily -> tung lam doi qua khong bao gio chay.
    # TRUOC ca check auto_team_dungeon: tat PB doi thi van phai doi qua.
    try:
        c.run_event_pre_dungeon()
    except Exception as e:
        log.warning("[%s] loi doi qua/bang su kien truoc pho ban doi (bo qua): %s", label, e)
    if not pcfg.get("auto_team_dungeon", True):
        return True
    if is_leader and not pcfg.get("manual_daily_barrier_done"):
        _wait_party_world_boss(st, pidx, label, stopped_fn)
    flags = _team_dungeon_flags(pcfg)
    levels = getattr(config, "TEAM_DUNGEON_LEVELS", (20, 50, 80))
    # Bo qua CHI LUOT CHAY NAY: doc xong la XOA co ngay. Luot chay moi (login/chu ky sau) lai
    # check PB binh thuong - KHONG khoa ca ngay.
    with st["lock"]:
        _skip_now = bool(st.get("team_dungeon_skip_all"))
        if _skip_now:
            st["team_dungeon_skip_all"] = False
            st["team_dungeon_tries"] = {}
    if _skip_now:
        log.info("[%s] auto phó bản đội: retry không qua -> BỎ QUA phó bản đội LƯỢT NÀY, "
                 "chuyển sang việc tiếp theo (lượt sau vẫn check bình thường)", label)
        return True
    for level in levels:
        if not flags.get(int(level), False):
            continue
        if not _handle_auto_team_dungeon(c, st, username, label, pidx, is_leader,
                                         stopped_fn, int(level)):
            return False
    # XONG HET cac PB moi reform MOT LAN: luong PB tu giai tan party chung de vao instance, nen
    # phai lap lai party train - nhung chi can lap lai khi THUC SU quay ve train, khong phai sau
    # tung level (xem ghi chu o _handle_auto_team_dungeon).
    with st["lock"]:
        st.pop("td_need_reform", False)   # don co; lap lai party la viec cua dieu phoi (L1)
    return True


def _handle_o5_team(c, st, username, label, pidx, is_leader, stopped_fn, o5_done):
    """O5 PHO BAN TO DOI = BUOC CUOI claim_daily_quests (sau khi check + thu lam moi o khac).
    Moi acc report o5 da xong chua. LEADER cho CA party report -> CHI khi MOI nguoi deu CHUA xong o5
    -> tao + keo party vao danh (member auto-accept 0x2f 0f->03 + ready 0x2f 0b trong _on_dungeon,
    di theo leader).
    MEMBER PHAI CHO leader danh xong (o5_state != "idle" roi thanh "done") MOI duoc return -> tiep tuc
    flow rieng (go_to_town/teleport/lap party train). KHONG cho -> member tu chay tiep SONG SONG luc
    dang trong pho ban -> gui 0x06/0x14/0x44 xen vao giua tran -> server khong nhan atk hop le -> turn
    timeout lap lai ~20-25s -> KET CUNG (da xac nhan qua log thuc te: chuba tu "xong daily login ->
    sync kenh + lap party -> teleport" NGAY GIUA LUC dang danh tran 1)."""

    # THU TU user yeu cau: DOI QUA su kien -> NHAN THUONG BANG 3x3 -> roi moi DANH PB TO DOI.
    # Dat o DAY (khong phai trong claim_daily_quests) vi mode TRAIN goi thang ham nay, con
    # claim_daily_quests chay SAU va chi khi do_daily -> tung lam doi qua khong bao gio chay.
    with st["lock"]:
        pass
    # DIEU PHOI DANG RA LENH GOM -> HOAN PHO BAN. Vao PB la ca party bi keo vao INSTANCE rieng:
    # lenh gom dang chay bi bo giua chung, va acc nao chua kip vao thi ket lai ben ngoai.
    #
    # Ca that party 6, 11/09 (user: "p6 deo thay lap pt di PB luon"):
    #   09:32:23 [party 6] gen 3: viec=gom - party dang o 2 MAP khac nhau [12001, 21001]
    #   09:32:24 [ttnnam] Boss the gioi: HOAN - dieu phoi dang ra lenh gom   <- boss BIET hoan
    #   09:32:30 [ttmot]  (LEADER) === PHO BAN TO DOI LV20: tao + moi 4 member ===   <- PB thi KHONG
    # Viec vat (cat do / ban Noi Dat) va boss the gioi deu da doc co nay tu truoc; rieng duong pho
    # ban to doi thi chua ai noi cho no biet. Cung mot co, cung mot cach doc - khong them co moi.
    if party_dang_gom(pidx):
        _kh = _ke_hoach(st) or {}
        log.info("[%s] (%s) pho ban to doi: HOAN - dieu phoi dang ra lenh '%s' (%s)",
                 label, "LEADER" if is_leader else "member",
                 _kh.get("viec") or "gom", _kh.get("ly_do") or "party chua gom xong")
        return
    has_leader = config.PARTY_LEADER_ACC.get(pidx) is not None
    if not is_leader and not has_leader:
        # Party KHONG CO LEADER BOT (vd "Khong co chu PT", cho nguoi that/tay dieu khien) -> KHONG
        # AI se chay nhanh is_leader ben duoi de set o5_state="done" -> cho vo ich toi HET 600s roi
        # moi timeout thoat (xac nhan qua thuc te: claim_daily_quests() bi "treo" dung ~10 phut o
        # buoc nay, moi acc). Khong co leader thi khong co gi de cho -> bo qua NGAY.
        return
    if not is_leader:
        # CHO VO HAN leader quyet dinh + danh xong team dungeon (thoat: dong doi reconnect / o5 done /
        # Stop / tu rot). Truoc day cap 600s roi "coi nhu xong" -> co the bo giua chung.
        _t0log = time.time()
        _t0 = time.time()
        _gen0 = st.get("reform_gen", 0)
        while True:
            if stopped_fn() or not c.running:
                return
            # LOI RA CAP PARTY. Cho leader la hop le KHI PB DANG CHAY THAT; leader ket o cho khac
            # thi day thanh cai bay: member dung tai bai quai voi flee_mode -> "BO CHAY" lien tuc,
            # tuc BI QUAI DANH LE ma khong danh tra (user 07/09: "member bi quai danh le").
            #   1) dieu phoi ra lenh moi (reform_gen doi) -> phai nghe, khong duoc diec.
            #   2) qua 90s ma pha PB VAN CHUA BAT -> leader khong he vao PB -> thoi cho.
            if st.get("reform_gen", 0) != _gen0:
                log.info("[%s] (member) dieu phoi ra lenh moi khi dang cho PB -> thoi cho", label)
                _clear_o5_client_flags(c)
                return
            if time.time() - _t0 > 90 and not dang_pha_pho_ban(pidx):
                log.warning("[%s] (member) cho leader %.0fs ma pha PB chua he bat -> leader khong "
                            "vao PB, thoi cho va lam viec khac", label, time.time() - _t0)
                _clear_o5_client_flags(c)
                return
            if time.time() - _t0log > 60:
                log.info("[%s] (member) CHO leader danh xong team dungeon...", label)
                _t0log = time.time()
            # CASE 3: dong doi ROT trong luc dang danh team dungeon -> phai RA KHOI instance
            # (trong dungeon teleport/ve thanh bi chan). Truoc day dung relogin; tu khi server chan
            # toc do dang nhap (ma 90) thi relogin lam acc ket vong login -> dung C:047-010.
            if st["reconnecting"]:
                log.warning("[%s] (member) dong doi ROT trong team dungeon -> THOAT PB (C:047-010), "
                            "khong relogin", label)
                try: c.leave_team_dungeon()
                except Exception: pass
                _clear_o5_client_flags(c)
                return
            with st["lock"]:
                state = st["o5_state"]
                _broke = st["o5_broke"]
            if state == "done":
                if _broke:
                    # team dungeon VO do co dis -> member CON KET trong instance (map dungeon),
                    # go_to_town KHONG thoat duoc. TRUOC DAY relogin de ra ("relogin xong la ca lu
                    # tu thoat PB") - dung, nhung tu khi server CHAN TOC DO DANG NHAP (ma 90) thi
                    # login lai rat kho: acc ket vong dang nhap hang phut (party 6, 23:15-23:25).
                    # Gio thoat bang dung lenh cua client C:047-010, GIU NGUYEN ket noi, roi dong bo
                    # + danh lai PB theo rule retry cu (o5_need_redo).
                    log.warning("[%s] (member) team dungeon VO (co dis) -> THOAT PB (C:047-010), "
                                "khong relogin", label)
                    try: c.leave_team_dungeon()
                    except Exception: pass
                # Leader da xong (thanh cong hay fail deu vay) -> pha PB da duoc dieu phoi TAT o
                # `finally`; day chi ha not co RIENG cua acc nay (quest_mode/instance).
                _clear_o5_client_flags(c)
                return
            if not c.in_combat():   # xong 1 tran team dungeon (member auto-danh) -> hoi HP/SP
                try: c.do_heal()
                except Exception: pass
            time.sleep(2)
    members = [t[0] for t in party_accounts(pidx)]
    if len(members) < 2:
        return   # khong du party de danh pho ban to doi
    # KHONG CHO REPORT NUA. Ban cu la `while True` cho VO HAN tat ca member "report o5" - barrier
    # kinh dien (L2 + L9), va no khoa LEADER khoi moi lenh cua dieu phoi: leader ket trong vong con
    # nen khong quay lai vong chinh de nghe.
    #
    # Ca that 07/09 party 15 - 26 PHUT (14:40:59 -> 15:06:32):
    #   [trusauu] (LEADER) CHO ca party report o5 (4/5)...      <- lap lai moi 30s
    #   [trutam]  (member) CHO leader danh xong team dungeon...
    #   [truchin] im tu 14:40:40 (khong bao gio report)
    # trong khi DIEU PHOI ra lenh deu dan moi 2 phut ma khong ai nghe:
    #   14:57:37 / 14:59:37 / 15:01:38 / 15:03:38 / 15:05:38
    #     [party 15] DIEU PHOI: ca party da chung kenh 1 nhung doi chi con 0/3 -> LAP LAI PARTY
    # Hau qua nua: member dung tai bai quai voi flee_mode -> "BO CHAY" lien tuc, tuc BI QUAI DANH LE
    # (user hoi: "member bi quai danh le").
    #
    # Doc MOT PHAT: acc nao chua bao thi coi nhu DA XONG o5 -> khong danh PB to doi luot nay. Mat
    # nhieu nhat MOT luot PB (lan chay sau van check binh thuong), doi lay viec khong ket.
    # BOT DOC THANG tung client. Acc chua chay toi buoc o5 thi chua co dau vet -> coi nhu DA XONG
    # (khong danh PB to doi luot nay), khong dung cho.
    statuses = {}
    for m in members:
        _mc = account_clients.get(m)
        _v = getattr(_mc, "_o5_da_xong", None) if _mc is not None else None
        if _v is not None:
            statuses[m] = bool(_v)
    _thieu = [m for m in members if m not in statuses]
    if _thieu:
        log.info("[%s] (LEADER) o5: %d/%d acc chua bao (%s) -> coi nhu DA XONG, khong danh PB to "
                 "doi luot nay (khong dung cho)", label, len(_thieu), len(members), _thieu)
        for m in _thieu:
            statuses[m] = True
    if all(not statuses.get(m, True) for m in members):       # MOI nguoi deu chua xong
        log.info("[%s] (LEADER) CA party (%d nguoi) chua xong o5 -> PHO BAN TO DOI LV20", label, len(members))
        with st["lock"]:
            st["o5_state"] = "running"   # member biet ma CHO, khong chay tiep
            st["o5_broke"] = False       # reset moi lan danh (co set True o finally neu co dis)
        # DIEU PHOI bat pha PB cho CA party: trong pha nay khong acc nao teleport ve thanh.
        # Truoc day moi acc tu om timer 600s luc accept loi moi -> PB vo thi khong ai ha, member
        # spam "dang vao pho ban -> ngung teleport" hang phut (p51/p53 07/09, 651 dong log).
        dat_pha_pho_ban(pidx, True)
        _dg0 = st["disc_gen"]            # CASE 3: theo doi co dong doi ROT trong luc danh khong
        try:
            ok = c.do_team_dungeon_lv20()
            if ok:
                # claim_daily_quests() claim hang/cot bingo (dung o5) TRUOC KHI goi hook nay (o5 la
                # buoc cuoi) -> luc claim hang/cot, o5 con dang tinh la CHUA xong -> bo lo claim
                # hang/cot/tong ket co dinh kem o5. Goi lai claim_daily_quests(heavy=False) SAU KHI
                # danh xong pho ban de claim bu (heavy=False -> KHONG lam lai o2/goi lai hook o5).
                c.claim_daily_quests(heavy=False)
            # PHONG THIEU NGUOI sau START (roster server < so bot moi - rule "du party moi danh"):
            # leader da HUY danh truoc khi ton luot -> ca party relogin gom lai roi LAM LAI.
            if getattr(c, "_td_incomplete", False):
                # THOAT INSTANCE bang C:047-010, KHONG relogin: tu khi server chan toc do dang nhap
                # (ma 90), relogin de dong bo PB lam acc ket vong login hang phut (party 6, 23:15).
                log.warning("[%s] (LEADER) phong pho ban THIEU nguoi -> thoat PB, gom lai danh lai", label)
                try: c.leave_team_dungeon()
                except Exception: pass
            # CASE 3: co dong doi ROT trong luc danh team dungeon -> leader cung RELOGIN thoat instance
            # (giong member) truoc khi ve flow. reform_gen (finally) + train reaction se gom lai sau.
            if st["disc_gen"] > _dg0 or st["reconnecting"]:
                log.warning("[%s] (LEADER) dong doi ROT trong team dungeon -> THOAT PB (C:047-010), "
                            "khong relogin", label)
                try: c.leave_team_dungeon()
                except Exception: pass
        finally:
            # PHA PB KET THUC (xong / thieu nguoi / co dis deu vay) -> tat cho CA party trong MOT
            # lenh. Khong con canh "leader ha co cua rieng no, member om timer 600s toi het han":
            # ca that 07/09, 651 dong "dang vao pho ban -> ngung teleport" - p53 leader thoat PB
            # luc 02:40:33 ma qv813/814/815 con spam toi 02:45:41 roi bi relogin hang loat;
            # p51 (mh212) ket y het.
            dat_pha_pho_ban(pidx, False)
            _clear_o5_client_flags(c)
            if not ok:
                # PB lv20 KHONG xong -> khong co goi ket thuc -> keo ca party ra bang lenh.
                # (Xong binh thuong thi moi acc tu ra khi nhan `S:047-012`, xem client._on_dungeon.)
                _thoat_pb_ca_party(pidx, "PB lv20 vỡ")
            with st["lock"]:
                # VO do co dis (chinh leader rot = not c.running, HOAC co member rot = disc_gen/
                # reconnecting): bao member -> CA party relogin thoat instance (trong dungeon KHONG
                # teleport ra duoc -> truoc day member spam go_to_town vo tan, xem log party xGAx).
                # `ok` = PB lv20 da danh xong -> rot sau do KHONG phai "chua xong", khong lam lai
                # (xem chu thich cung y o nhanh lv50/80/110 ben tren, ca that p42 07/09).
                if (not ok) and ((not c.running) or st["disc_gen"] > _dg0 or st["reconnecting"]
                                 or getattr(c, "_td_incomplete", False)):
                    st["o5_broke"] = True
                    st["o5_need_redo"] = True   # team dungeon CHUA xong -> reconnect xong lam LAI
                st["o5_state"] = "done"   # bao member (thanh cong hay fail deu THA member ra)
                # do_team_dungeon_lv20 tu goi leave_party() (giai tan party de vao pho ban) - DAY LA
                # PARTY CHUNG voi party train, nhung KHONG co gi bao cho vong lap chinh biet can lap
                # lai -> truoc day member out het, leader chay ra bai TRAIN MOT MINH (khong reform).
                # Bump reform_gen -> co che reform co san (_do_reform, dung cho cac truong hop
                # "bi dump khoi dungeon" khac) se tu dong keo ca party tap hop + lap lai.
    else:
        with st["lock"]:
            st["o5_state"] = "done"      # khong danh -> tha member ngay
        done_list = [m for m in members if statuses.get(m, False)]
        log.info("[%s] (LEADER) o5: KHONG phai ca party chua xong (da xong: %s) -> bo qua pho ban to doi",
                 label, done_list)


def _run_account_supervised(username, password, pidx, is_leader, is_picker=False):
    """Bọc run_account: SERVER ROT (server_closed) -> login lai (backoff 5s x3 -> 30s x10 -> 60s),
    VO HAN toi khi duoc (chi dung khi GUI Stop). Ap dung MOI party, ke ca "khong co chu PT" (truoc
    day khong leader la rot CHET luon -> dung yen trong DG con gio ma tu out).
    run_account bao lai qua account_reconnect[username]."""
    st = _pstate(pidx)
    stop_ev = account_stops.get(username)
    _st = lambda: stop_ev is not None and stop_ev.is_set()
    attempt = 0
    first = True
    while True:
        account_reconnect[username] = False
        _runtime_leader = config.PARTY_LEADER_ACC.get(pidx)
        is_leader = username == _runtime_leader
        is_picker = is_leader if _runtime_leader else username == party_accounts(pidx)[0][0]
        _tiep = account_continue.pop(username, None)
        try:
            run_account(username, password, pidx, is_leader, is_picker, is_reconnect=not first,
                        reuse_client=_tiep)
        except ResyncSignal:
            # EP DONG BO: barrier sau da unwind ra day -> dong socket + coi nhu forced reconnect ->
            # relogin ngay (wait=1s) -> duong reconnect bam pha leader (clear sach state cu).
            cli = account_clients.get(username)
            if cli is not None:
                try: cli.close()
                except Exception: pass
            account_reconnect[username] = True
            account_forced_reconnect.add(username)
            account_forced_reconnect_reason[username] = "ép đồng bộ theo leader"
            log.warning("[%s] EP DONG BO -> relogin bam leader", username)
        except Exception:
            # LOI KHONG LUONG TRUOC -> truoc day luong CHET CAM: khong log, khong relogin, GUI van
            # xanh "CHAY". Acc chet giua chung nen KHONG kip vao st["reconnecting"] -> barrier
            # reform dem _n_arr + _n_rec KHONG BAO GIO du -> CA PARTY dung hinh vinh vien, chi in
            # "CHO ca party ve ... - THIEU: <acc>" moi 30s (bug that: party 38 ket 4h38').
            # Rule TOI THUONG la gom DU party -> phai CUU acc (relogin), TUYET DOI khong bo qua no.
            log.exception("[%s] LOI CHET LUONG (khong phai ResyncSignal) -> relogin de cuu party",
                          username)
            cli = account_clients.get(username)
            if cli is not None:
                try: cli.close()
                except Exception: pass
            # KHONG dat `forced`: forced = relogin 1s va BO QUA nhanh train_reform (dieu kien
            # `not forced`). Loi lap lai chac chan se thanh vong crash 1s/lan, va party cung khong
            # duoc bump reform_gen de gom lai. Luong chet thuc chat LA mot kieu rot -> di duong rot
            # binh thuong: co backoff 5s/30s/60s + disc_gen + reform_gen nhu moi lan mat ket noi.
            account_reconnect[username] = True
            account_forced_reconnect_reason[username] = "luồng lỗi bất ngờ"
        first = False
        if account_continue.get(username) is not None:
            # DOI PHA tai cho: KHONG dem la rot -> khong bump disc_gen, khong danh dau
            # st["reconnecting"] (dong doi se khong bao dong gia "dong doi ROT"), khong cho backoff.
            first = False
            continue
        if _st() or not account_reconnect.get(username):
            break   # GUI Stop / thoat binh thuong / khong reconnectable -> dung han
        forced = username in account_forced_reconnect
        account_forced_reconnect.discard(username)
        forced_reason = account_forced_reconnect_reason.pop(username, None)
        pcfg = (getattr(config, "PARTY_CONFIG", {}).get(pidx, {}) or {})
        ev = config.event_hom_nay(pcfg.get("event_key") or "")
        event_reset = (not forced and _is_party_event(
            pcfg.get("mode"), config.PARTY_LEADER_ACC.get(pidx) is not None, ev
        ) and st.get("event_battle_active", False))
        train_reform = (not forced and config.PARTY_LEADER_ACC.get(pidx) is not None
                        and _party_is_in_train_phase(pcfg, st))
        with st["lock"]:
            st["reconnecting"].add(username)
            if not forced:
                st["disc_gen"] += 1
            # (`train_reform` tinh o tren nhung KHONG con ai dung: acc mat ket noi roi login lai
            #  khong tu ra lenh cho ca party ve thanh nua - dieu phoi thay lech map/kenh va gom.)
            if event_reset:
                st["invited"].clear()
                st["event_battle_done"].clear()
        if event_reset:
            reset_party_joined(pidx)
        attempt += 1
        wait = 1 if forced else (5 if attempt <= 3 else (30 if attempt <= 13 else 60))
        # GIAN CACH TUNG ACC TRONG PARTY khi relogin HANG LOAT (ep dong bo theo leader): truoc day
        # ca 5 acc dung wait=1 -> 5 lenh dang nhap don trong ~1s -> server tra ma 90 "dang nhap qua
        # thuong xuyen" -> acc khong vao lai duoc hang phut, party train thieu nguoi.
        # Bug that (party 2, 18:17-18:21): "ep dong bo theo leader -> login lai sau 1s" cho ca party
        # -> 18:18:16 leader keo di khi roster DU 4/4 -> 18:18:23 con 3 -> 18:18:59 con 2.
        # Xep hang theo VI TRI acc trong party -> 1s, 4s, 7s, 10s, 13s.
        #
        # PHAI AP DUNG CA KHI SERVER DA LOAT (khong chi `forced`): luc do moi acc tu retry voi
        # `wait` GIONG HET NHAU (5s) -> ca party login trong ~6s -> dinh ma 90 y het.
        # Do tren party.log 31/08 (party 1): 6 lan ca party rot trong CUNG MOT GIAY (16:41:07 va
        # 17:45:48 la 4 acc, 17:55:30 la 3 acc - deu ma 61 cua server), va 2 chum ma 90 sau do
        # (15:09:36-42, 17:09:53-59) deu la 5 acc login lot trong 6 giay.
        # Dau hieu "server da loat" = dang co >=2 acc cua party trong `reconnecting`.
        # Buoc 8s (khong phai 3s nhu nhanh forced): 5 acc trong 6s DA dinh 90 roi, 3s van qua sat.
        # Trai ~37s, doi lai khong dinh 90 - ma dinh 90 la backoff `30 * attempt` (toi 300s) va
        # thuong dinh ca 5 acc.
        try:
            with st["lock"]:
                _dang_rot = len(st["reconnecting"])
        except Exception:
            _dang_rot = 0
        _gian_buoc = 3 if forced else (8 if _dang_rot >= 2 else 0)
        if _gian_buoc:
            try:
                _order = [u for u, _p, _l, _k in party_accounts(pidx)]
                _them = _gian_buoc * _order.index(username)
                if _them:
                    wait += _them
                    log.info("[%s] RECONNECT: relogin HANG LOAT (%s) -> gian them %ds cho rieng "
                             "acc nay (tranh ma 90)", username,
                             "ca party bi ep" if forced else "%d acc dang rot" % _dang_rot, _them)
            except ValueError:
                pass
        # SERVER CHAN TOC DO DANG NHAP (S:000-000 ma 90 "dang nhap qua thuong xuyen"): login lai
        # ngay chi lam server chan tiep -> VONG XOAY: dut -> login nhanh -> bi chan -> dut...
        # Do tren party.log 1 phien: 1232/1574 lan dut la ma 90 (78%). Nhung lan do backoff dang
        # la 1-5s. Voi ma nay phai CHO LAU, va cang thu nhieu cang phai cho lau hon.
        _cli_last = account_clients.get(username)
        _cause = int(getattr(_cli_last, "disconnect_cause", 0) or 0)
        _reason_extra = ""
        if _cause == DISCONNECT_RATE_LIMIT:
            wait = max(wait, min(30 * attempt, 300))
            _reason_extra = " [server chan toc do dang nhap -> gian nhip]"
        elif _cause:
            _reason_extra = " [%s]" % getattr(_cli_last, "disconnect_reason", "")
        log.warning("[%s] RECONNECT: %s%s -> login lai sau %ds (lan %d)", username,
                    forced_reason or ("bat buoc relogin ca party" if forced else "server rot"),
                    _reason_extra, wait, attempt)
        for _ in range(wait):
            if _st():
                break
            time.sleep(1)
        if _st():
            break
    st["reconnecting"].discard(username)
    if is_leader and account_threads.get(username) is threading.current_thread():
        st["leader_gone"].set()   # thoat that su (het reconnect) -> member thoat theo


def start_account(username, password, pidx, is_leader, is_picker):
    """Khoi dong 1 acc (thread). Neu thread cu con song (vd Stop xong Start LAI ngay de doi map/mode)
    -> BAO DUNG + CHO no chet han roi moi start thread MOI voi config MOI. Truoc day return False
    (bo qua) -> acc giu thread cu chay tiep config CU (bug: doi train map A->B nhung 1 so acc van
    tele ve thanh A' cu vi thread cu doc start_city_id=A tu luc dau, khong doc lai)."""
    bao_dam_dieu_phoi()   # start acc le cung phai co dieu phoi (xem bao_dam_dieu_phoi)
    t = account_threads.get(username)
    if t is not None and t.is_alive():
        ev = account_stops.get(username)
        if ev is not None:
            account_stop_reasons[username] = "start lai acc: dung thread cu truoc"
            ev.set()                     # bao thread cu dung
        t.join(timeout=12)               # cho chet han (go_to_town... co check stop -> thoat vai giay)
        if t.is_alive():
            log.warning("[%s] start_account: thread cu chua dung sau 12s -> bo qua start "
                        "(tranh 2 thread 1 acc)", username)
            return False
    account_stop_reasons.pop(username, None)
    st = _pstate(pidx)
    if is_leader:
        # Start party da clear leader_gone o dau, nhung neu leader thread CU chet muon sau do
        # (do start_account vua set stop_ev + join) thi finally cua thread cu co the set lai
        # leader_gone, lam member phien MOI vua vao party da thoat theo "chu party thoat".
        # Clear lai ngay truoc khi tao leader thread moi de cat stale signal do.
        st["leader_gone"].clear()
        st["leader_bad"].clear()
    # Start dau tien cua phien party moi: bo route con sot theo pidx tu lan chay/profile cu.
    # Supervisor reconnect khong di qua start_account nen route dang do van duoc tiep tuc.
    if not _active_party_usernames(pidx):
        _clear_stale_manual_route(st)
    # SO MEMBER CAN CHO. Mode KHONG CO VIEC LAP DOI (loan dau) thi con so nay la 0 - khong phai
    # "chan cho khoi moi", ma vi mode do THAT SU khong can cho ai: moi acc tu dang ky, tu ghep
    # tran. Dat dung o day thi MOI vong "chua du member -> moi lai / gom / reform" tu tat, khong
    # phai di vá tung cho.
    #
    # Ca that 10/09 party 19 (user: "p19 thay lap pt", "don gian mode loan dau thi di solo thoi ma
    # cung sai duoc"): dieu phoi da ra dung lenh
    #     21:44:18 [party 19] viec=lam - mode KHONG CAN LAP DOI (moi acc tu dang ky va tu danh)
    # ma leader van lap doi day 4/4 mot phut sau, roi:
    #     21:46:30 [quanmot] (LEADER) DU 4/4 member -> SET QS + ra train
    #     21:46:30 [quanmot] loi start training: cannot access local variable '_start_training'
    # - loan dau ma di duong train, va vap `UnboundLocalError` vi ham do chi duoc `def` tren duong
    # train. Goc cua ca chuoi: `n_members = 4` cho mot mode khong bao gio can 4 nguoi.
    st["n_members"] = (0 if not _mode_can_lap_doi(pidx)
                       else sum(1 for u, p, lead, _ in party_accounts(pidx) if not lead))
    # MODE digioi_train: party khoi dong LAI tu dau (chua acc nao chay) -> reset pha ve "digioi",
    # khong thi lan chay sau se bo qua DG (pha con ket o "train" tu phien truoc).
    if not _running_party_usernames(pidx):
        with st["lock"]:
            st["dt_phase"] = "digioi"
            st["dt_train_prepared"] = False
        # Khong con co "da xong DG" de xoa: dieu phoi doc thang dong ho server tung acc
        # (`_acc_het_gio_dg`), va `relogin()` da bo moc dong ho cu de cho server day lai.
    account_stops[username] = threading.Event()
    t = threading.Thread(target=_run_account_supervised, args=(username, password, pidx, is_leader, is_picker),
                         daemon=True)
    account_threads[username] = t
    _threads.append(t)
    t.start()
    return True


def setup_party_runtime(pidx, mode, server_ip, server_id, accounts,
                        city_flag=0, start_city_id=0, mob_index=-1, do_daily=True,
                        digioi_mode="party", event_key="", leaders=None, has_leader=True,
                        use_phuc_than=False, use_digioi_ho_phu=False,
                        fight_legion_boss=True, do_van_tieu=True,
                        buy_ho_phu=False, buy_bao_hop=False, bao_hop_xu_threshold=10000000,
                        di_gioi_level=2, auto_sell_noi_dat=True,
                        buy_hp=False, hp_qty=9999, hp_thresh=500000,
                        buy_sp=False, sp_qty=9999, sp_thresh=500000,
                        claim_offline_exp=True,
                        auto_team_dungeon=True, team_dungeons=None,
                        auto_buy_shop=None, buy_thien_chau=False,
                        auto_world_boss=True,
                        # THEM O CUOI: Kotlin goi THEO VI TRI (BotForegroundService.kt) nen
                        # chen vao giua se lam lech het cac tham so phia sau.
                        auto_bag_clean=True, auto_discard_junk=True,
                        auto_decompose_scrolls=False, scroll_modes=None,
                        auto_donate_materials=True, material_modes=None,
                        auto_event_exchange=False, event_exchange_items=None,
                        death_return_town=True, pet_death_return_town=True,
                        event_exchange_sig="",
                        train_pick="", mob_min=0, mob_max=0, mob_elements="",
                        di_gioi_pick="", loandau_mot_tran=False,
                        auto_bag_expand=False, bag_expand_gold=0,
                        # TU MO RUONG TRANG BI + TU CAT DO TIEN TRANG. THEM O CUOI CUNG
                        # (Kotlin goi THEO VI TRI - chen vao giua la lech het tham so phia sau).
                        # `cat_do_items` KHONG co o day: list cat la MOT file chung
                        # (cat_do_items.json), khong phai config theo party.
                        auto_open_boxes=False, box_modes=None, auto_cat_do=False,
                        # TU MO RONG TIEN TRANG. THEM O CUOI CUNG (Kotlin goi THEO VI TRI).
                        auto_bank_expand=False, bank_expand_gold=0):
    """ANDROID: Kotlin goi de POPULATE config cho 1 party luc runtime (thay vi doc accounts.json
    nhu PC). accounts = 1 CHUOI STRING duy nhat dang "u1\\x01p1\\x01battle_json\\x01heal_json\\x01u2..." (KHONG phai
    list/List<String> - da xac nhan qua logcat that: Chaquopy KHONG convert dung List<String>
    (ke ca da lam PHANG) thanh Python list khi truyen qua callAttr, "TypeError: 'ArrayList'
    object is not iterable" ngay tai list(accounts). String thi luon convert dung -> Kotlin join
    bang U+0001 (xem BotForegroundService.kt::startParty), o day tu split() lai. Goi XONG roi
    goi start_party(pidx). Cau truc PARTY_CONFIG/PARTIES/PARTY_LEADER_ACC GIONG HET
    config._load_accounts_json ban PC -> tu do run_party_digioi (coordinator CHUNG) chay y het PC."""
    pidx = int(pidx)
    if isinstance(team_dungeons, str):
        try:
            import json
            team_dungeons = json.loads(team_dungeons) if team_dungeons else None
        except Exception:
            team_dungeons = None
    config.PARTY_CONFIG[pidx] = {
        # TU CHON MAP TRAIN. mob_elements: Kotlin truyen CHUOI noi bang "," (khong truyen List -
        # R8 rut gon ten lop -> Chaquopy "TypeError: 't' object is not iterable").
        "train_pick": str(train_pick or ""),
        "di_gioi_pick": str(di_gioi_pick or ""),
        "mob_min": int(mob_min or train_pick_mod.DEFAULT_MOB_MIN),
        "mob_max": int(mob_max or train_pick_mod.DEFAULT_MOB_MAX),
        "mob_elements": ([int(x) for x in str(mob_elements).split(",") if x.strip().isdigit()]
                         or list(train_pick_mod.ALL_ELEMENTS)),
        "mode": mode, "start_city_id": int(start_city_id), "mob_index": int(mob_index),
        "city_flag": int(city_flag), "server": "", "server_ip": server_ip,
        "server_id": int(server_id), "do_daily": bool(do_daily),
        "claim_offline_exp": bool(claim_offline_exp),
        "auto_world_boss": bool(auto_world_boss),
        "auto_team_dungeon": bool(auto_team_dungeon),
        "team_dungeons": config.normalize_team_dungeons(team_dungeons),
        "digioi_mode": digioi_mode, "event_key": event_key or "",
        "loandau_mot_tran": bool(loandau_mot_tran),
        "auto_open_boxes": bool(auto_open_boxes),
        "box_modes": _map_cau_hinh(box_modes, gia_tri_bool=True),
        "auto_cat_do": bool(auto_cat_do),
        "auto_bag_expand": bool(auto_bag_expand),
        "bag_expand_gold": int(bag_expand_gold or 0),
        "auto_bank_expand": bool(auto_bank_expand),
        "bank_expand_gold": int(bank_expand_gold or 0),
        "use_phuc_than": bool(use_phuc_than), "use_digioi_ho_phu": bool(use_digioi_ho_phu),
        "fight_legion_boss": bool(fight_legion_boss),
        "do_van_tieu": bool(do_van_tieu),
        "auto_sell_noi_dat": bool(auto_sell_noi_dat),
        "death_return_town": bool(death_return_town),
        "pet_death_return_town": bool(pet_death_return_town),
        "auto_bag_clean": bool(auto_bag_clean),
        "auto_discard_junk": bool(auto_discard_junk),
        "auto_decompose_scrolls": bool(auto_decompose_scrolls),
        "scroll_modes": _map_cau_hinh(scroll_modes),
        "auto_donate_materials": bool(auto_donate_materials),
        "material_modes": _map_cau_hinh(material_modes),
        "auto_event_exchange": bool(auto_event_exchange),
        "event_exchange_sig": str(event_exchange_sig or ""),
        # APK truyen CHUOI noi bang "\n" (xem BotForegroundService.kt); PC truyen list.
        "event_exchange_items": ([x for x in event_exchange_items.split("\n") if x.strip()]
                                 if isinstance(event_exchange_items, str)
                                 else list(event_exchange_items or [])),
        "auto_buy_shop": bool(auto_buy_shop) if auto_buy_shop is not None else bool(buy_ho_phu or buy_thien_chau or buy_bao_hop),
        "shop_items": config.normalize_shop_items(None, {
            "ho_phu": bool(buy_ho_phu),
            "thien_chau": bool(buy_thien_chau),
            "bao_hop": bool(buy_bao_hop),
        }),
        "buy_ho_phu": bool(buy_ho_phu), "buy_thien_chau": bool(buy_thien_chau),
        "buy_bao_hop": bool(buy_bao_hop),
        "bao_hop_xu_threshold": int(bao_hop_xu_threshold),
        "di_gioi_level": int(di_gioi_level),
        "buy_hp": bool(buy_hp), "hp_qty": int(hp_qty), "hp_thresh": int(hp_thresh),
        "buy_sp": bool(buy_sp), "sp_qty": int(sp_qty), "sp_thresh": int(sp_thresh),
    }
    _flat = str(accounts).split("\x01") if accounts else []
    accs = []
    if len(_flat) >= 5 and len(_flat) % 5 == 0:
        # 5-tuple MOI (APK Kotlin+Python cung build -> nhat quan): u,p,battle,heal,furnace.
        import json
        for i in range(0, len(_flat) - 4, 5):
            u, p, battle_json, heal_json, furnace_json = _flat[i:i + 5]
            if not u:
                continue
            accs.append((u, p))
            try:
                bcfg = json.loads(battle_json) if battle_json else {}
                apply_account_battle(u, bcfg if isinstance(bcfg, dict) else {})
            except Exception:
                apply_account_battle(u, {})
            try:
                hcfg = json.loads(heal_json) if heal_json else {}
                apply_account_heal(u, hcfg if isinstance(hcfg, dict) else {})
            except Exception:
                apply_account_heal(u, {})
            try:
                fcfg = json.loads(furnace_json) if furnace_json else {}
                apply_account_furnace(u, fcfg if isinstance(fcfg, dict) else {})
            except Exception:
                apply_account_furnace(u, {})
    elif len(_flat) >= 4 and len(_flat) % 4 == 0:
        import json
        for i in range(0, len(_flat) - 3, 4):
            u, p, battle_json, heal_json = _flat[i], _flat[i + 1], _flat[i + 2], _flat[i + 3]
            if not u:
                continue
            accs.append((u, p))
            try:
                bcfg = json.loads(battle_json) if battle_json else {}
                apply_account_battle(u, bcfg if isinstance(bcfg, dict) else {})
            except Exception:
                apply_account_battle(u, {})
            try:
                hcfg = json.loads(heal_json) if heal_json else {}
                apply_account_heal(u, hcfg if isinstance(hcfg, dict) else {})
            except Exception:
                apply_account_heal(u, {})
    elif len(_flat) >= 3 and len(_flat) % 3 == 0:
        import json
        for i in range(0, len(_flat) - 2, 3):
            u, p, battle_json = _flat[i], _flat[i + 1], _flat[i + 2]
            if not u:
                continue
            accs.append((u, p))
            try:
                bcfg = json.loads(battle_json) if battle_json else {}
                if isinstance(bcfg, dict):
                    apply_account_battle(u, bcfg)
                else:
                    apply_account_battle(u, {})
            except Exception:
                apply_account_battle(u, {})
            apply_account_heal(u, {})
    else:
        accs = []
        for i in range(0, len(_flat) - 1, 2):
            if _flat[i]:
                accs.append((_flat[i], _flat[i + 1]))
                apply_account_battle(_flat[i], {})
                apply_account_heal(_flat[i], {})
    while len(config.PARTIES) <= pidx:
        config.PARTIES.append([])
    config.PARTIES[pidx] = accs
    if isinstance(leaders, str):
        import re
        leaders = [x.strip() for x in re.split(r"[\n,\r]+", leaders) if x.strip()]
    config.PARTY_LEADERS_BY_IDX[pidx] = list(leaders or [])
    if has_leader and accs:
        config.PARTY_LEADER_ACC[pidx] = accs[0][0]
    else:
        config.PARTY_LEADER_ACC.pop(pidx, None)
    config.ACCOUNTS = [a for party in config.PARTIES for a in party if a and a[0]]
    config.ACCOUNT_PARTY = {a[0]: i for i, party in enumerate(config.PARTIES)
                            for a in party if a and a[0]}


def party_idx_of(username):
    """pidx cua account (de map lenh thu cong username -> pidx). None neu khong biet."""
    return getattr(config, "ACCOUNT_PARTY", {}).get(username)


def _start_party_accounts(pidx, accounts, generation, stagger, skip_running):
    """Khoi dong tung acc cua party. Tach rieng de goi duoc CA khi KHONG reset state chung."""
    started = 0
    for u, p, is_leader, is_picker in accounts:
        if generation != _start_cancel_generation:
            log.info(">>> PARTY %s: huy khoi dong cac acc con lai do STOP TAT CA", pidx + 1)
            break
        if skip_running and is_account_running(u):
            continue      # START TAT CA: acc dang chay -> de yen, khong dung roi chay lai
        account_exit_reason.pop(u, None)   # xoa ly do cu
        if start_account(u, p, pidx, is_leader, is_picker):
            started += 1
            time.sleep(stagger)
    return started


def start_party(pidx, stagger=1.5, skip_running=False):
    """Khoi dong tat ca acc trong 1 party.

    skip_running=True (START TAT CA goi): acc DANG CHAY thi BO QUA, khong dung-roi-chay-lai.
    Mac dinh False cho nut "Start party" rieng: van restart de ap config moi (doi map/mode).
    """
    generation = _start_cancel_generation
    started = 0
    accounts = party_accounts(pidx)
    # TRUOC MOI THU: dam bao luong dieu phoi CHUNG con song. Dat o day (khong phai duoi nhanh
    # `if started`) vi nhanh `not _fresh` THOAT SOM - do la ly do party 53 chay khong co dieu
    # phoi suot 16 phut (06/09). Xem bao_dam_dieu_phoi.
    bao_dam_dieu_phoi()
    # Party da tat han -> tao session state MOI. Reset tung field nhu truoc de sot route_plan/
    # reform_gen cua map cu, member co the doc plan cu truoc khi leader ghi plan map moi.
    _fresh = not any(is_account_running(u) for u, *_ in accounts)   # party bat dau PHIEN MOI
    if _fresh:
        _party_state.pop(pidx, None)
        reset_party_joined(pidx)
        for u, *_ in accounts:
            account_forced_reconnect.discard(u)
            account_forced_reconnect_reason.pop(u, None)
    st = _pstate(pidx)
    # RESET state dung chung (tranh sot tu lan chay truoc: leader_bad cu -> member quit oan).
    # CHI khi party bat dau phien MOI. Party dang co acc CHAY (START TAT CA them acc con thieu,
    # hoac start lai 1 acc) ma xoa channel/team_dungeon_state/dailies_done... la PHA acc dang chay:
    # chung dang dua vao chinh nhung state do de phoi hop voi nhau.
    if not _fresh:
        return _start_party_accounts(pidx, accounts, generation, stagger, skip_running)
    for k in ("leader_ok", "leader_bad", "leader_gone", "invited", "channel_ready",
              "stop_leader_done", "route_party_ready", "route_done", "rally_ready",
              "path_done",
              # EVENT: "da thua/da xong" chi tinh trong mot phien -> phien moi phai danh lai.
              "go_claim", "event_battle_done"):
        st[k].clear()
    st["mob_spot"] = None
    st["rally_point"] = None
    st["mob_path"] = None
    st["channel"] = None
    with st["lock"]:
        st["started_train"] = 0
        st["dungeon_done"] = 0
        st["dailies_done"] = 0       # barrier: so acc da xong daily quest login (cho leader cho)
        st["team_dungeon_state"] = {}
        st["team_dungeon_broke"] = {}
        st["team_dungeon_need_redo"] = False
        st["summary_done"] = False   # cho phep log lai dong tong ket o lan chay nay
    started += _start_party_accounts(pidx, accounts, generation, stagger, skip_running)
    if started:
        # 1 watcher/party, chay nen. Tu thoat khi party dung han.
        threading.Thread(target=_party_watcher, args=(pidx,),
                         name="watch-p%d" % (pidx + 1), daemon=True).start()
    return started


# ===================== DIEU PHOI PARTY: BOT QUYET DINH =====================
# Rule user chot 05/09: "bo me cai leader quyet dinh party lam gi di, BOT la nguoi quyet dinh".
#
# VI SAO PHAI DOI: truoc day khong he co ai dieu phoi. 5 luong acc TU THUONG LUONG voi nhau qua
# mot dong co dung chung trong `st[...]` (leader_ok, channel_ready, ready_members, dt_done,
# invited, route_done...), va luong cua acc leader tinh co om nhieu quyet dinh nhat nen thanh
# "nguoi chi huy" - nhung no cung chi la mot luong dang ban login/danh/di duong nhu 4 dua kia.
# He qua tat yeu: LUONG NAO ROI VAO MOT VONG KHONG DOC DUNG CO LA CA PARTY CHET.
#
# Da xay ra that (party 19, 05/09, ket 2 GIO 42 PHUT):
#   14:05:29  4 member xong DG -> dung o 12001 cho leader bao "xong DG"
#   14:39:45  leader roi vao vong moi party tran (run_party_digioi.py:5781 cu) - vong do KHONG
#             doc gio DG, KHONG doc reform_gen, KHONG goi _resync_ck
#   ~14:59    het gio DG cua leader - KHONG AI KIEM -> khong bao gio bao "xong DG"
#   15:00:32  server da leader ve thanh 12003, member o 12001 -> leader in "lech map live
#             12001!=12003" 488 LAN roi thoi, khong lam gi
# Ep dong bo cung KHONG pha duoc vi chinh vong do khong doc sync_epoch/reform_gen.
#
# CACH LAM MOI: mot luong DIEU PHOI cho moi party. No NHIN THAY TOAN BO (ca 5 client trong
# `account_clients`: map, kenh, gio DG, dang danh khong) nen KHONG CAN barrier, KHONG CAN Event,
# KHONG CHO AI. Moi nhip no doc trang thai that roi ghi ra MOT KE HOACH; luong acc chi doc ke
# hoach ma thi hanh.
#
# Tinh chat quan trong: DEADLOCK BIEN MAT VE MAT CAU TRUC, khong phai nho va them loi thoat.
# Vi khong con cho cheo - chi co MOT cho quyet, va cho do khong bao gio cho ai.
# Bao nhieu VONG lien tiep khong vao duoc map event thi ngung han (moi vong da tu thu 5 lan).
# Luoi do cuoi cung cho event KHONG khai bao `lich` - event co lich thi da bi chan tu truoc.
EV_VAO_HONG_TOI_DA = 3

KE_HOACH_NHIP = 2.0        # nhip dieu phoi (giay). Nhanh hon watcher cu (20s) vi no ra LENH.
# Do dac 13/09 (250 acc / 50 party): quet mot vong ton 16ms, tuc 0.33ms moi party. Nen hai nguong
# duoi day KHONG phai de canh so party - chung de bat luc luong dieu phoi BI DOI CPU (GIL) hoac
# co party keo dai vong quet. Tre bao nhieu giay = ca bot mu bay nhieu giay.
DIEU_PHOI_TRE_CANH_BAO_SEC = 3.0   # nhip cach nhau qua (2s + 3s) -> khong con la dao dong binh thuong
DIEU_PHOI_QUET_LAU_SEC = 1.0       # 1 vong quet > 1s = gap 60 lan muc do dac duoc
# Party lech map/kenh LIEN TUC bao lau thi moi ra lenh GOM. Phai du dai de bao het MOT chuyen
# teleport gom (ve thanh trung gian -> thanh tap ket, tung acc lech nhip vai chuc giay): trong
# luc do lech map la BINH THUONG, ra lenh gom luc do la huy chinh viec dang lam.
KE_HOACH_LECH_MAP_SEC = 60
# LECH MAP co an han RIENG va NGAN hon lech kenh. "Khac map thi gom map" (user 09/09) - an han o
# day chi de bo qua vai giay teleport chuyen tiep, khong phai de cho acc lam xong viec rieng.
# 60 giay la han cua mot CHUYEN GOM dang chay (ve thanh trung gian -> thanh tap ket); dung no lam
# han PHAT HIEN lech thi party lech ca phut van chua duoc ra lenh.
LECH_MAP_AN_HAN_SEC = 12
# Het lech phai GIU duoc bay lau thi dong ho lech moi duoc go. Truoc day chi can MOT nhip thay
# cung map la dong ho reset ve 0 -> lech ngat quang khong bao gio dat han, tuc khong bao gio gom.
HET_LECH_CHAC_SEC = 8
KE_HOACH_DUNG_HINH_SEC = 240   # party DU nguoi, CUNG map/kenh ma khong nhuc nhich -> ket, phai pha
def _dang_danh(c):
    try:
        return bool(getattr(getattr(c, "state", None), "in_battle", False))
    except Exception:
        return False


def _dau_vet_acc(c):
    """DAU VET tien do cua MOT acc: (map, vi tri, the he tran). Doc thang client, khong ai bao cao.

    THE HE TRAN la phan bat buoc, khong phai cho co: DANH TAI CHO cung la tien do.

    Ban cu chi do (map, pos) nen coi "dung yen" = "khong lam gi". Trong Di Gioi do la phep do SAI:
    bot dung dung mot cho ban Phuc Than, danh xong lai cho con sau - map/pos KHONG BAO GIO doi.
    `_dang_danh` chi che duoc luc DANG trong tran; nghi giua hai tran vai chuc giay la dong ho chay
    tiep, 240s sau ca party bi ket luan DUNG HINH va bi `resync_gen` (= dap doi de sync kenh).

    `battle_tracker.generation` tang moi lan vao tran moi -> danh duoc tran nao la dau vet doi.
    Party dang ban ban quai vi the khong con bi ket luan la treo.
    """
    _bt = getattr(c, "battle_tracker", None)
    return (int(getattr(c, "current_map", 0) or 0),
            tuple(getattr(c, "pos", ()) or ()),
            int(getattr(_bt, "generation", 0) or 0))


def _acc_dung_hinh(st, song, han):
    """[username] cac acc KHONG nhuc nhich qua `han` giay. Tra rong = van dang chay.

    Do TUNG ACC chu khong do ca party. Ban dau ham nay do ca party (mot dau vet chung): chi can MOT
    acc dong day la coi nhu "party co tien do" -> lot luoi dung cai can bat.
    Ca that 07/09 party 23 (33 phut): leader dang DANH THAT (BATTLE ACK lien tuc) trong khi 4 member
    dung im cho no - dau vet chung doi moi nhip, dieu phoi khong thay gi bat thuong.

    Acc DANG DANH khong tinh: trong mot tran, map/vi tri dung yen la binh thuong (co tran dai vai
    phut). Acc dang relogin cung khong tinh - no khong nam trong `song`."""
    moc = st.setdefault("nhip_acc", {})
    now = time.time()
    ket = []
    _con = set()
    for u, c in song:
        _con.add(u)
        if _dang_danh(c):          # dang danh = dang lam viec -> tinh lai tu dau
            moc[u] = (None, now)
            continue
        _vet = _dau_vet_acc(c)
        _cu = moc.get(u)
        if _cu is None or _cu[0] != _vet:
            moc[u] = (_vet, now)
        elif now - _cu[1] > han:
            ket.append(u)
    for u in [u for u in moc if u not in _con]:   # acc da tat/rot -> bo moc cu
        moc.pop(u, None)
    return ket
# Hai lan dieu phoi ra lenh GOM phai cach nhau it nhat bay nhieu giay. Moi lenh gom ABORT moi acc
# dang di duong, nen ra lien tuc = ca party khong bao gio di xong buoc nao (party 17, 05/09).
KE_HOACH_GOM_COOLDOWN = 180.0
# Leader khong cong bo duong di sau bay nhieu giay -> MEMBER TU LAP. Duong di la cua PARTY, khong
# phai dac quyen leader. 20s = du cho leader binh thuong (no lap duong ngay khi vao _do_reform),
# ma khong de ca party dung 3 phut nhu party 10 (05/09 18:22-18:23).
ROUTE_PLAN_TIEP_QUAN_SEC = 20.0

# `viec` = ca party dang phai lam gi. Luong acc doc cai nay thay vi tu suy.
VIEC_LAM = "lam"      # ai vao viec nay: danh/train/DG binh thuong
VIEC_GOM = "gom"      # dang lech map/kenh -> GOM VE THANH roi keo lai (chi ngoai DG)
VIEC_MOI = "moi"      # da cung map+kenh -> leader gui loi moi, member nhan
# TRONG DI GIOI: lech kenh thi phai DONG BO TAI CHO (giai tan + sync kenh + moi lai), TUYET DOI
# khong "gom ve thanh" - tu DG ra thanh la phai DI BO RA CONG, tuc loi ca party ra khoi DG.
VIEC_DONG_BO = "dong_bo"
# CHUOI LENH DI TRAIN (user chot 11/09): "gom map -> gom kenh -> du pt thi ra lenh leader di den
# map train, toi safe gan diem duoc chon -> ra lenh leader chay ra diem quai -> trong qua trinh nay
# dieu phoi thay lech map lech kenh thi ra lenh gom lai".
#
# Truoc do hai buoc cuoi KHONG PHAI la lenh: leader tu quyet bang dong ho rieng (`last_retry >= 60`),
# so bot tu nho (`joined_member_count`) va vi tri cua rieng no. Nen no bo di ra bai ngay giua lenh
# gom (party 10, 11/09: member di gom, leader "DU 4/4 member ma KHONG DANH -> RA DIEM QUAI").
VIEC_DI_TRAIN = "di_train"   # du doi, cung map+kenh, nhung CHUA o map train -> leader keo ca party di
VIEC_RA_QUAI = "ra_quai"     # da o map train nhung leader con dung o safe -> ra diem quai roi danh


def _viec_di_train(pidx, st, song, pha):
    """DU DOI + CUNG MAP/KENH roi thi phai lam gi tiep -> (viec, ly_do).

    Hai buoc cuoi cua chuoi lenh user chot 11/09. Dieu phoi doc THANG trang thai that cua ca party
    (map hien tai, vi tri leader) - khong acc nao bao cao, khong dung so bot tu nho.

        chua o map train        -> VIEC_DI_TRAIN  (leader keo ca party di)
        o map train, con o safe -> VIEC_RA_QUAI   (leader ra diem quai roi danh)
        da o diem quai          -> VIEC_LAM       (cu danh)

    Chi ap cho pha TRAIN. DG/event co duong rieng (chay long vong / dung tang) nen giu `VIEC_LAM`.
    """
    if pha != "train":
        return VIEC_LAM, ""
    _dich = _map_train_dich(pidx, st)
    if not _dich:
        return VIEC_LAM, ""        # chua ai biet map train dich -> chua ra lenh duoc
    _ngoai = sorted({int(getattr(c, "current_map", 0) or 0) for _u, c in song
                     if getattr(c, "current_map", None)} - {_dich})
    if _ngoai:
        return VIEC_DI_TRAIN, "du doi, cung map/kenh -> DI TRAIN map %d (con o %s)" % (_dich, _ngoai)
    _spot = st.get("mob_spot")
    if _spot:
        _xa = [u for u, c in song
               if getattr(c, "pos", None) and _xa_diem_quai(c.pos, _spot)]
        if _xa:
            return VIEC_RA_QUAI, "da o map train %d -> RA DIEM QUAI %s (%s con o safe)" % (
                _dich, tuple(_spot), ", ".join(sorted(_xa)))
    return VIEC_LAM, ""


def _ke_hoach(st):
    """Ke hoach hien tai cua party (dict) hoac None neu dieu phoi chua chay lan nao."""
    with st["lock"]:
        kh = st.get("ke_hoach")
        return dict(kh) if kh else None


def _ke_hoach_gen(st):
    kh = _ke_hoach(st)
    return int(kh.get("gen", 0)) if kh else 0


def _ghi_ke_hoach(st, pidx, moi, ly_do=""):
    """Ghi ke hoach moi. Chi tang `gen` khi NOI DUNG doi -> luong acc dang thi hanh khong bi
    giat lai moi 2 giay. Tra True neu co doi."""
    with st["lock"]:
        cu = st.get("ke_hoach") or {}
        cung = all(cu.get(k) == moi.get(k) for k in ("pha", "map", "kenh", "thanh", "viec"))
        if cung and cu:
            return False
        moi = dict(moi)
        moi["gen"] = int(cu.get("gen", 0)) + 1
        moi["luc"] = time.time()
        moi["ly_do"] = ly_do
        st["ke_hoach"] = moi
    log.info("[party %d] DIEU PHOI gen %d: pha=%s map=%s kenh=%s viec=%s%s",
             pidx + 1, moi["gen"], moi.get("pha"), moi.get("map"), moi.get("kenh"),
             moi.get("viec"), (" - " + ly_do) if ly_do else "")
    _log_trang_thai(st, pidx)
    return True


def _log_trang_thai(st, pidx):
    """MOT DONG du de dung lai ca canh - in kem moi lan lenh doi.

    Truoc day muon biet "luc do party dang the nao" phai ghep 4-5 lenh grep khac nhau (log dieu
    phoi + log tung acc + roster + pha), va moi lan user bao loi la mat ca chuc phut mo nguoc.
    Cac con so o day deu la thu DIEU PHOI VUA DUNG DE QUYET - in ra de doc lai duoc quyet dinh.

    Dang:
      [party N] TRANG THAI: acc=<ten>@<map>/k<kenh><co> ... | roster leader=x/y | reform g=A thoa=B
      <co>:  ! = dang danh   * = dang lam viec le (boss QD / viec vat)
    """
    try:
        song = _acc_song(pidx)
        if not song:
            return
        _leader = config.PARTY_LEADER_ACC.get(pidx)
        _le = set(_ai_dang_lam_viec_le(song))
        _o = []
        _ros_leader = None
        for _u, c in song:
            try:
                _m = getattr(c, "current_map", None)
                _k = getattr(c, "current_channel", None)
                _n = len(getattr(c, "party_members", None) or ())
                _co = "!" if getattr(getattr(c, "state", None), "in_battle", False) else ""
                if _u in _le:
                    _co += "*"
                if _u == _leader:
                    _ros_leader = _n
                    _co += "(L)"
                _o.append("%s@%s/k%s%s" % (getattr(c, "_label", _u), _m, _k, _co))
            except Exception:
                continue
        log.info("[party %d] TRANG THAI: %s | roster leader=%s/%s | reform g=%s thoa=%s",
                 pidx + 1, " ".join(_o),
                 _ros_leader if _ros_leader is not None else "?", len(song) - 1,
                 st.get("reform_gen", 0), st.get("reform_gen_thoa", 0))
    except Exception as e:
        log.debug("[party %d] log trang thai loi (bo qua): %s", pidx + 1, e)


def _danh_dau_training(st, ok):
    """Ghi `st["training_started"]` cho luat watcher "party thieu nguoi qua lau" doc duoc.

    KHOA NAY TRUOC 05/09 KHONG AI GHI: grep ca file chi ra DUNG MOT dong, va la dong DOC
    (`if _can and _co < _can and st.get("training_started")`). `st.get(...)` luon tra None ->
    luat do chua tung chay lan nao (dem trong log ca ngay: THIEU NGUOI 0 lan, trong khi luat
    DEADLOCK chay 2227 lan). Bien `training_started` cung ten o `run_account` la bien LOCAL cua
    tung luong acc, khong lien quan gi toi `st[...]` cua party.
    """
    if ok:
        with st["lock"]:
            st["training_started"] = True
    return ok


def _acc_song(pidx):
    """[(username, client)] cua acc DANG CHAY va DA co client. Nen tang cua moi quyet dinh:
    dieu phoi doc THANG client, khong doc bao cao do chinh luong acc tu ghi (bao cao co the
    dong bang khi luong do ket - dung cai bay da lam party 19 chet)."""
    ra = []
    for u, _p, _l, _k in party_accounts(pidx):
        ev = account_stops.get(u)
        if ev is not None and ev.is_set():
            continue
        c = account_clients.get(u)
        if c is not None and getattr(c, "running", False):
            ra.append((u, c))
    return ra


def _het_gio_dg(c):
    """Acc nay HET GIO Di Gioi chua - DIEU PHOI tu ket luan, khong doi acc tu bao.

    Day la diem mau chot cua party 19: co `dt_done` nhung no do CHINH LUONG ACC ghi, ma luong do
    dang ket trong vong moi party -> khong bao gio ghi -> 4 dua kia cho mai. Dieu phoi doc thang
    dong ho nen luong acc co ket cung khong giau duoc.
    """
    try:
        con = DIGIOI_LIMIT - c.digioi_minutes_live()
    except Exception:
        return False
    if con <= 0:
        return True
    # Ra NGOAI map DG ma dong ho server cung gan het = het gio that (dong ho noi bo dung yen khi
    # o ngoai DG nen khong bao gio tu ve 0 - xem client.digioi_minutes_live).
    ngoai = (getattr(c, "current_map", None) is not None
             and c.current_map != config.DIGIOI_MAP_ID)
    if ngoai and con < 2:
        return True
    return False


def _tinh_hinh_doi(song):
    """Mo ta doi theo ROSTER SERVER cua tung acc: 'a=4 b=4 c=0'."""
    return " ".join("%s=%d" % (u, len(getattr(c, "party_members", None) or ()))
                    for u, c in sorted(song, key=lambda x: x[0]))


def _leader_dang_rot(pidx, song):
    """LEADER khong con trong danh sach acc song = no dang relogin -> DOI DA TAN, chac chan.

    User chot 07/09: "khi thang leader dis va o trang thai dang login thi chac chan tat ca deu ko
    co party, server tu giai tan party roi". Doi truong roi khoi the gioi thi server thao doi -
    khong con gi de giu.

    Bot khong the trong vao roster de biet dieu do: cac member CON SONG chi nhan duoc `0x0d` khi
    server chiu gui, va trong luc leader chua vao lai thi roster cua ho co the con giu so cu. Nen
    day la mot ket luan SUY RA TU SU KIEN, khong phai tu so dem."""
    if not song:
        return False
    _song_u = {u for u, _c in song}
    for _t in party_accounts(pidx):
        if len(_t) > 2 and _t[2] and _t[0] not in _song_u:   # _t[2] = is_leader
            return True
    return False


DOI_KENH_BIEN_DONG_SEC = 25.0   # sau khi mot acc doi kenh, roster ca party con bien dong bay lau


def _dang_doi_kenh(song):
    """CO acc nao dang do viec doi kenh khong - luc do roster KHONG DANG TIN.

    Doi kenh phai roi doi truoc (luat `Team.IsAlone`), ma DOI TRUONG roi la server giai tan ca doi.
    Nen ngay sau mot lenh doi kenh: acc do co roster 0, cac acc khac con giu SO CU chua kip cap
    nhat - ca hai deu khong phai su that. Ket luan "thieu party" luc nay la ket luan tren hau qua
    cua chinh lenh minh vua ra.

    User chot 07/09: "truoc khi bao lap lai pt thi phai thay la ko du pt thi moi lap lai chu".

    "DANG DO" = DA GUI LENH MA CHUA CO KET QUA. Khong phai "vua gui lenh trong N giay".

    `switch_channel` luon ket thuc bang MOT ket qua ro rang - `S:007-002` ma 0/1/2/3/4, hoac
    TIMEOUT - va ghi vao `_chan_switch_result` + `_chan_switch_luc`. Ban cu chi doc `_doi_kenh_tu`
    (moc BAT DAU) roi coi la "dang bay" trong tron 25 giay, ke ca khi server DA tra loi tu giay
    dau. Acc bi tu choi se thu lai lien tuc -> moc do luon moi -> "luc nao cung dang doi kenh".

    User 08/09: "giu nguyen dich cai lon me may, moi lan doi kenh phai duoc thanh cong hay fail va
    vi sao fail chu". Dung: co ket qua roi thi PHAI XU LY theo ma do (ma 2/4 -> so den, ma 3 -> ca
    party roi doi, ma 0/1 -> xong), khong duoc treo lo lung."""
    _bay = time.time()
    for _u, _c in song:
        _tu = float(getattr(_c, "_doi_kenh_tu", 0.0) or 0.0)
        if not _tu or _bay - _tu >= DOI_KENH_BIEN_DONG_SEC:
            continue
        # DA CO ket qua cho chinh lan doi nay -> lenh XONG, khong con "dang do".
        _luc = float(getattr(_c, "_chan_switch_luc", 0.0) or 0.0)
        if getattr(_c, "_chan_switch_result", None) is not None and _luc >= _tu:
            continue
        return True
    return False


# Roster vua tang trong bay nhieu giay = party DANG DUOC LAP, chua duoc dap di lap lai.
DOI_DANG_LAP_SEC = 25.0


def _doi_dang_lap(st, song):
    """Party co DANG DUOC LAP DO DANG khong (roster vua tang trong `DOI_DANG_LAP_SEC` giay).

    `_thieu_doi` chi tra loi "du chua". Do la cau hoi SAI o nhanh ra lenh lap lai party: mot party
    dang moi tung nguoi thi LUC NAO cung "chua du", nen no bi dap dung luc sap xong.

    Ca that party 3, 11/09 (user: "t bao xem truoc do co ma, doan dang di ra map train ay"):
        08:43:30 [party 3] REFORM gen -> 25 - chung kenh roi ma doi khong du -> lap lai party
        08:43:30 [party 3] DOI chua du (sga005=4 sga007=4 sga008=2 sga009=3 sga010=1)
    Bon con so do la roster DANG LEN (4,4,2,3,1) - leader moi duoc gan het doi thi bi giai tan.
    Sau do moi den man di train -> lech map -> gom -> ve thanh -> lap lai... Party 19, 6, 7, 24
    deu chet o dung cho nay.

    Do TONG roster ca party: no chi tang khi co nguoi THAT SU vao doi, va tut ve 0 khi doi tan -
    tuc phan biet duoc "dang moi" voi "dung im khong ai vao".
    """
    _tong = 0
    for _u, c in song:
        try:
            _tong += len(getattr(c, "party_members", None) or ())
        except Exception:
            pass
    _cu, _luc = st.get("roster_tong", (0, 0.0))
    if _tong > _cu:
        st["roster_tong"] = (_tong, time.time())
        return True
    if _tong < _cu:
        st["roster_tong"] = (_tong, 0.0)     # doi tut/tan -> khong con la "dang lap"
        return False
    return bool(_luc) and time.time() - float(_luc) < DOI_DANG_LAP_SEC


# Viec LE hop le: KHONG duoc keo acc dang lam do vao party roi loi di.
#   boss_qd     - boss Quan Doan la instance SOLO, BAT BUOC roi party moi vao duoc
#   login_chore - viec vat luc login (qua, van tieu, shop, thu cuoi...). User 14/09: "dang lam may
#                 cai viec vat do ko vao pt la dung, dang lam do ma vao pt roi bi keo di luon thi
#                 no hong viec vat".
# Trung `_PHASE_SOLO` cua watcher - cung mot khai niem.
VIEC_LE_HOP_LE = ("boss_qd", "login_chore")

def _ai_dang_lam_viec_le(song):
    """Acc dang lam viec LE - CHI DE IN LOG (`_log_trang_thai` danh dau `*`). KHONG quyet dinh gi.

    DIEU PHOI KHONG DUOC CHO AI CA, va cung khong can:

        leader cu MOI lien tuc
        acc dang viec vat GIU LOI MOI lai (`_pending_party_invites`), xong viec thi nhan
        neu doi dang THIEU, loi moi tu acc cung party duoc NHAN NGAY (L0 - xem `_on_party_invite`)

    Tuc chuyen "dang lam do thi chua vao party" DA duoc giai o tang acc tu truoc, dung cach: khong
    ai bi keo di giua chung, ma party van hinh thanh.

    T tung them mot bac chan o dieu phoi de lam lai chinh viec nay ("thieu nguoi vi co acc dang lam
    viec le -> CHO, khong gom"). Hau qua: viec vat thi acc nao cung lam lien tuc, nen LUC NAO cung
    co nguoi ban -> dieu phoi khong bao gio ra lenh -> ca party dung o thanh voi roster 0/4.
    Ca that 14/09 (user: "bon no lai ket o thanh deo di dau kia") - 204 lan:
        14:25:30 [party 36] gen 6: viec=lam - doi thieu nguoi vi ['taot12','taot13','taot14',
                                   'taot15'] dang lam viec LE hop le -> CHO, khong gom
        14:26:52 [party 24] TRANG THAI: daimot@12001/k2! ... | roster leader=0/4
    Va dat han cho cung khong cuu duoc: het han thi hoac keo acc di (hong viec vat) hoac cho tiep
    (ket o thanh) - ca hai deu sai. Cach dung la KHONG CHO, vi tang acc da lo roi.
    """
    out = []
    for _u, _c in song:
        try:
            d = get_account_task(_u)
        except Exception:
            continue
        if d and d.get("phase") in VIEC_LE_HOP_LE:
            out.append(_u)
    return out


# Goi roster `0x0d` toi nam acc KHONG cung luc. Bay nhieu giay la thua de no lan het.
ROSTER_LAN_SEC = 30.0


def _roster_dang_lan(st, song):
    """Cac ban sao roster CHUA NHAT TRI - tuc goi `0x0d` chua toi du moi acc.

    Roster tren SERVER la MOT su that duy nhat; `c.party_members` cua nam acc chi la nam ban sao,
    va chung toi KHONG CUNG LUC. `_thieu_doi` lay ban sao CHAM NHAT lam chuan (mot acc thieu la ket
    luan "doi chua du"), nen doi VUA DU van bi doc thanh "doi hong" trong vai giay dau.

    Day khong phai chuyen nho: ket luan do sinh ra lenh LAP LAI PARTY, ma lenh do bat leader
    `leave_party()` - tuc PHA DUNG CAI DOI VUA HINH THANH.

    Ca that party 17, 14/09 (user: 'thay log "chua ra bai, dieu phoi ra lenh moi" la sao'):
        11:49:21 [party 17] LAP LAI PARTY (chu706=4 chu707=3 chu708=4 chu709=2 chu710=1)
        11:49:31 [party 17] RUT lenh reform gen 3 - da du doi (chu706=4 ... chu710=4)
        11:49:36 [chusau] (LEADER) reform gen 3: ... -> moi lai party
        11:49:36 [chusau] Roi/giai tan party cu -> roster 3 -> 2 -> 1 -> 0
        11:49:37 [party 17] gen 25: viec=moi - DOI chua du (tat ca = 0)
    Luc 11:49:21 chu706 va chu708 DA thay du 4 nguoi - doi that su DA DU tren server, chi la
    chu710 chua nhan kip goi. Muoi giay sau moi acc deu thay 4/4. Lenh ra luc do la lenh SAI, va
    no chay tiep den tan buoc pha doi. Chu ky lap lai y het luc 11:51:34.

    Phan biet voi doi HONG THAT (party 1, 07/09: roster (4,0,0,0,0) DUNG IM 44 phut):
      - dang lan  : co acc DA thay du, acc khac chua -> lech nay TU HET trong vai giay
      - hong that : lech dung im qua `ROSTER_LAN_SEC` -> het han, doc theo ban sao cham nhat nhu cu
    """
    if len(song) < 2:
        return False
    _can = len(song) - 1
    _ros = []
    for _u, c in song:
        try:
            _ros.append(len(getattr(c, "party_members", None) or ()))
        except Exception:
            _ros.append(0)
    if not (max(_ros) >= _can > min(_ros)):
        st.pop("roster_lech_tu", None)      # nhat tri roi (du hoac hong deu the) -> quen moc
        return False
    _tu = float(st.get("roster_lech_tu", 0.0) or 0.0)
    if not _tu:
        st["roster_lech_tu"] = _tu = time.time()
    return time.time() - _tu < ROSTER_LAN_SEC


def _thieu_doi(pidx, song):
    """DOI da du chua - theo SU THAT CUA SERVER, khong theo bo dem trong cua bot.

    `joined_member_count()` doc `_PARTY_JOINED` - bang do CHINH BOT ghi khi thay acc accept. No la
    so nho, va no OM STALE: party tan ma khong ai unmark thi bot van tuong con du.

    Ca that 07/09 party 1: ca nam acc dung o Truong Sa 44 phut, dieu phoi ket luan "du doi" nen ra
    `VIEC_LAM` va im - trong khi log cua chinh leader lap lai lien tuc:
        13:26:49 [xGAx] KHONG o party nao (roster server + local deu rong)
    User: "du doi cai lon, bon no co cung party deo dau".

    `c.party_members` la roster SERVER gui ve (`0x0d`), tuc su that.

    NHUNG DOC CUA AI? LEADER - no la NGUOI MOI, roster cua no CHINH LA roster cua doi. Member chi
    nhan ban sao, va ban sao do den muon / khong day du.

    Ban cu doi MOI acc phai thay du ("chi can MOT acc thieu la doi chua du"), nen doi DA DU van bi
    doc thanh thieu - vinh vien, vi ban sao cua member khong bao gio bang leader.

    Ca that party 1, 14/09 (user: "p1, party xong deo ra cho quai"):
        13:29:01 [batbat] (LEADER) MAT PARTY giua chung (0/4) -> MOI LAI
        13:29:01..04 PARTY: ... vao doi -> roster 1 -> 2 -> 3 -> 4 nguoi      <- DU 4/4
        13:29:10 [batbat] (LEADER) chua ra bai: dieu phoi dang ra lenh 'moi'
        13:29:15/20/25 pos=(1140, 360) dung im
    Luc do roster dung im o (chihao188=2 sga008=4 sga018=4 sga019=3 sga020=1) suot 33 giay - KHONG
    phai dang lan, ma la member vinh vien khong co ban sao day du. Leader (sga008) thay 4 = du.
    Dieu phoi van ket luan "thieu" -> `viec=moi` -> cua "chua ra bai" chan leader -> ca party dung
    im giua bai train.

    Leader khong chay (dang relogin) -> KHONG doan bua: lay ban sao DAY DU NHAT con lai. Doi tan
    that thi moi ban sao deu tut, nen van bat duoc (ca 07/09 party 1 o tren: leader `xGAx` roster
    RONG -> van ket luan thieu, dung).
    """
    can = len(song) - 1          # roster khong ke chinh minh
    if can <= 0:
        return False
    _leader = config.PARTY_LEADER_ACC.get(pidx)
    for _u, c in song:
        if _u == _leader:
            return len(getattr(c, "party_members", None) or ()) < can
    # Leader khong con trong danh sach acc song -> doc ban sao day du nhat.
    _max = 0
    for _u, c in song:
        try:
            _max = max(_max, len(getattr(c, "party_members", None) or ()))
        except Exception:
            pass
    return _max < can


def _ly_do_lech(maps, kenhs, lech_kenh_that, tien_to="", mot_minh=()):
    """Cau ly do NOI DUNG SU THAT - doc thang tu client, khong qua bao cao cua ai."""
    if mot_minh:
        return "party %sco %d acc DUNG MOT MINH (cung map ma khong ai thay -> khac instance): %s" % (
            tien_to, len(mot_minh), [l for _u, l in mot_minh])
    if lech_kenh_that:
        return "party %slech kenh (%d acc dung mot minh)" % (tien_to, lech_kenh_that)
    if len(maps) > 1:
        return "party %sdang o %d MAP khac nhau %s" % (tien_to, len(maps), sorted(maps))
    return "party %slech kenh %s" % (tien_to, sorted(kenhs))


def _dieu_phoi_quyet(pidx, st, song, lech_tu):
    """Nhin trang thai THAT cua ca party -> tra (ke_hoach, ly_do, lech_tu_moi).

    Khong cho, khong khoa, khong phu thuoc bao cao cua luong acc.
    """
    pcfg = getattr(config, "PARTY_CONFIG", {}).get(pidx, {})
    raw_mode = pcfg.get("mode")
    cu = _ke_hoach(st) or {}

    # Daily thu cong co luong party/instance rieng. Dieu phoi train chen vao luc nay se coi
    # account dang cho boss/PB la dung hinh, phat lenh gom va pha phong. Mo lai khi account
    # cuoi cung da roi daily_pending.
    if st.get("daily_active") or st.get("daily_hold_after_stop"):
        st["nhip_acc"] = {}
        return ({"pha": "event", "map": None, "kenh": None, "viec": VIEC_LAM},
                "Daily dang chay/da dung tai cho -> tam khoa dieu phoi train/party", None)

    # MODE KHONG CAN LAP DOI thi dieu phoi khong co viec gi o cap party - xem `_mode_can_lap_doi`.
    # Day la CUA DAU TIEN: ba lenh duoi day (`moi` / `gom` / `dong_bo`) deu sinh ra tu chinh ham
    # nay, nen chan o `_dieu_phoi_chot_kenh` thoi la chua du (party 24, 10/09).
    if not _mode_can_lap_doi(pidx):
        return ({"pha": "event", "map": None, "kenh": None, "viec": VIEC_LAM},
                "mode KHONG CAN LAP DOI (loan dau: moi acc tu dang ky va tu danh)", None)
    # 40NPC NGOAI GIO: viec duy nhat la moi acc TU di doi thuong roi thoat - khong danh, khong lap
    # doi, khong dung chung kenh. Dieu phoi khong con gi de dieu phoi (xem `_party_40npc_ngoai_gio`).
    if _party_40npc_ngoai_gio(pidx, pcfg):
        return ({"pha": "event", "map": None, "kenh": None, "viec": VIEC_LAM},
                "40NPC ngoai gio -> moi acc tu di doi thuong roi thoat (khong gom, khong sync kenh)",
                None)
    # EVENT DA XONG -> KHONG CON VIEC GI O CAP PARTY. Moi acc di doi thuong roi THOAT.
    #
    # User 14/09: "event thi danh xong out, train deo gi o day".
    #
    # `go_claim` / `_npc40_done` = het gio hoac thua 2 tran -> `_ket_thuc` da bao party ngung danh.
    # Tu day chi con mot viec SOLO: toi NPC map 12003 doi qua roi thoat game. Dieu phoi ma van ra
    # lenh gom/moi thi acc bi keo vao vong reform va KHONG BAO GIO quay lai duoc nhanh doc co do.
    #
    # Ca that party 6 va 7, 14/09 (user: "danh xong 40NPC no ve Nghiep thanh lam gi the"):
    #   21:12:36 [ttsau] 40NPC: thua sach (khong co prompt) -> THOAT LUON       <- `_npc40_done`
    #   21:18:59 [party 7] REFORM gen -> 6 - chung kenh roi ma doi khong du     <- dieu phoi gom
    #   21:19:09 [ttsau] (LEADER) reform: 4/4 member join lai -> KEO qua cong ra train map
    #   21:19:10 [ttsau] loi reform (bo qua): 'NoneType' object has no attribute 'get'
    #   21:20:12 [ttbay] PARTY: ... ROI doi -> roster con 3 -> 2 ...
    # Ca lu quay vong o Nghiep Thanh (12061 - thanh gom) thay vi di doi thuong.
    # Doi chieu party 22 cung luc, KHONG bi dieu phoi chen vao:
    #   21:15:46 [gclmot] (LEADER) 40NPC xong -> di doi thuong + thoat game
    #   21:16:52 [gclmot] doi thuong 40NPC: da doi qua chien dau o NPC map 12003
    #   21:17:39 >>> PARTY 22 DA THOAT HET
    try:
        _xong_event = bool(st.get("go_claim") and st["go_claim"].is_set()) or any(
            getattr(c, "_npc40_done", False) for _u, c in song)
    except Exception:
        _xong_event = False
    if _xong_event:
        return ({"pha": "event", "map": None, "kenh": None, "viec": VIEC_LAM},
                "event DA XONG -> moi acc di doi thuong roi thoat (khong gom, khong moi party)",
                None)

    # ---- PHA: DG hay train ----
    pha = st.get("dt_phase", "digioi") if raw_mode == "digioi_train" else (raw_mode or "train")
    # AUTO BATTLE bien phien stand luc login thanh phien train live.
    if st.get("ui_train_target"):
        pha = "train"
    ly_do = ""
    if raw_mode == "digioi_train" and pha == "digioi":
        # CA PARTY het gio DG -> DOI PHA. Dieu phoi tu ket luan tu dong ho tung acc.
        con_gio = [u for u, c in song if not _het_gio_dg(c)]
        if song and not con_gio:
            pha = "train"
            with st["lock"]:
                st["dt_phase"] = "train"
            ly_do = "ca party (%d acc) het gio Di Gioi" % len(song)

    # ---- MAP/KENH dich + co dang lech khong ----
    # ACC DANG LAM VIEC VAT KHONG TINH VAO PHEP DO LECH MAP.
    #
    # Viec vat PHAI o map khac: ban Noi Dat o Nghiep Thanh, cat tien trang, danh boss the gioi...
    # (user 14/09: "lam viec vat thi van o map khac dc ma, nhu danh boss, cat tien trang, ban noi
    # dat"). Dem no vao `maps` thi party LUC NAO cung "lech map" -> lenh gom lien tuc -> bump
    # reform -> pha doi, trong khi thu duy nhat can lam la DOI no xong viec.
    #
    # Ca that party 13, 14/09:
    #   17:30:52 [party 13] TRANG THAI: tonba@21001/k1(L) tonbay@12263/k1 tontam@21001/k1
    #                                   tonchin@12001/k2* tonmuoi@12001/k2*    <- `*` = viec vat
    #   17:31:51 [tonchin] Teleport -> city 12061 (flag 2)      (di ban Noi Dat)
    #   17:31:52 [tonba] (LEADER) dieu phoi bao GOM (party dang o 2 MAP khac nhau [12061, 21001])
    # Ba acc kia da o Tuong Duong tu lau; hai acc nay chi dang di lam viec vat.
    #
    # KHONG PHAI "CHO": lenh van chay binh thuong tren so acc con lai (gom map / dong bo kenh /
    # moi party). Acc dang viec vat GIU loi moi lai, xong viec thi nhan (`account_task.__exit__`).
    _ban_viec_vat = set(_ai_dang_lam_viec_le(song))
    maps = {}
    _chua_biet_map = []
    for u, c in song:
        if u in _ban_viec_vat:
            continue                    # dang viec vat -> o map khac la DUNG, khong tinh lech
        m = getattr(c, "current_map", None)
        if m is not None:
            maps.setdefault(int(m), []).append(u)
        else:
            _chua_biet_map.append(u)
    lech = len(maps) > 1
    # CHI DEM KENH DO SERVER XAC NHAN. So kenh cua acc vua co lenh doi kenh HONG la so NHO LAI,
    # khong phai bang chung - lay no ra ket luan "lech kenh" la ra lenh xe doi dua tren so ao.
    # Ca that party 24, 11/09: dieu phoi doc `party lech kenh {1: 1, 2: 4}` va chot dich lien tuc,
    # trong khi ca party dang o CUNG mot kenh (user: "p24 dang o kenh 5 het, co lech kenh deo dau").
    # Truoc do la mot loat `Doi kenh N TIMEOUT` / `result=4` - moi lan hong la mot so kenh chet cung
    # trong bo nho bot. KNOWLEDGE.md muc 7 da ghi tu 30/08: `current_channel` SAI DUOC, va game
    # KHONG CO lenh hoi lai kenh.
    kenhs = set()
    _kenh_mo_ho = []
    for _u, c in song:
        if _u in _ban_viec_vat:
            continue        # cung ly do nhu `maps`: no dang o map khac, so kenh khong so duoc
        ch = getattr(c, "current_channel", None)
        if not ch:
            continue
        if hasattr(c, "kenh_dang_chac") and not c.kenh_dang_chac():
            _kenh_mo_ho.append(_u)
            continue
        kenhs.add(int(ch))
    if _kenh_mo_ho:
        log.debug("[party %d] bo qua kenh cua %s khi dem lech: lenh doi kenh gan nhat hong nen so "
                  "dang nho khong con dang tin", pidx + 1, sorted(_kenh_mo_ho))

    # ==== BAO CAO DOI CHIEU TUNG CAP tu leader (invite_members) ====
    # Tap `kenhs` o tren chi noi duoc "co bao nhieu gia tri kenh khac nhau". Leader thi doi
    # chieu TUNG CAP (minh vs tung member) nen biet CHINH XAC BAO NHIEU member lech va la ai.
    # Dung de dat cau ly do cho dung va de bat duoc ca truong hop tap `kenhs` chua kip doi.
    # (LUU Y: `kenh_that()` tra ve chinh `current_channel` - KHONG phai nguon tin cay hon.
    #  Nguyen nhan that cua su co party 53 la KHONG CO LUONG DIEU PHOI - xem bao_dam_dieu_phoi.)
    _lech_kenh_that = 0
    _mot_minh = ()
    for _u, c in song:
        _bq = getattr(c, "_moi_bo_qua", None)
        if _bq and time.time() - float(_bq.get("luc") or 0) < 30 and _bq.get("lech_kenh"):
            _lech_kenh_that = max(_lech_kenh_that, len(_bq["lech_kenh"]))
    if _lech_kenh_that:
        lech = True

    # LECH KENH tinh CHUNG mot dong ho voi lech map, KHONG phai nhanh rieng khong co an han.
    # (Ban dau viet `elif len(kenhs) > 1: VIEC_GOM` - khong an han giay nao. Party 17, 05/09
    #  18:49-18:51: acc dang teleport chuyen tiep nen kenh/map lech la CHUYEN BINH THUONG, the la
    #  dieu phoi ra lenh gom lien tuc.)
    # DU PARTY = DA CUNG INSTANCE -> KHONG CO CHUYEN "LECH KENH".
    #
    # Server khong cho o chung doi ma khac phan khu (`S:007-002` ma 3 <組隊不可換分區> la mat kia
    # cua cung mot luat). Nen roster DU la bang chung: ca party dang o cung mot cho. Con `kenhs` thi
    # dung `c.current_channel` - SO BOT TU NHO, va no sai duoc (sot lai qua reconnect, ack cu; user
    # kiem chung 30/08: bot hien ca 5 nick kenh 12, vao game xem la 12/12/12/2/1).
    #
    # Ket luan "lech kenh" tu so nho do dan toi HAI duong ra lenh, ca hai deu PHA party dang lanh:
    #   VIEC_DONG_BO -> bump `resync_gen` -> moi member `leave_party()` roi sync kenh lai
    #   `_dieu_phoi_chot_kenh` -> `kenh_dich` -> acc `switch_channel` (cung phai roi doi truoc)
    # User 07/09: "vi sao du pt roi ma van co lenh doi kenh de pt lai".
    #
    # Day la bot TU DOC roster cua tung client - khong doi acc nao bao cao.
    _du_doi = not _thieu_doi(pidx, song)
    if _du_doi and len(kenhs) > 1:
        log.debug("[party %d] du party (%s) nhung so kenh nho lai lech %s -> TIN ROSTER, bo qua",
                  pidx + 1, _tinh_hinh_doi(song), sorted(kenhs))
    lech = lech or (len(kenhs) > 1 and not _du_doi)
    # DONG HO LECH KHONG DUOC RESET BOI MOT NHIP THOANG QUA.
    #
    # Ban cu: thay cung map MOT nhip la `lech_tu = None` - dong ho ve 0. Party nao lech NGAT QUANG
    # thi khong bao gio chay du han, tuc KHONG BAO GIO duoc gom, du dieu phoi in ra "con lech map"
    # ba chuc lan.
    #
    # Ca that 09/09 party 1 (user: "p1 biet khac map ma deo gom map" -> "di train thi don gian la
    # khac map thi gom map, khac kenh thi gom kenh, du thi di train"):
    #   13:08:16 gen 23: viec=lam - con lech map [21011, 62001] -> chua lap party, cho gom xong
    #   13:08:42 gen 25: viec=moi - cung map/kenh nhung DOI chua du     <- dong ho RESET
    #   13:08:52 gen 26: viec=lam - con lech map [21011, 62001]         <- dem lai tu dau
    #   13:09:22 gen 29: viec=moi - cung map/kenh                       <- RESET lan nua
    # `brub` bi dump ra 21011 tu 13:08:28 va dung do; bon dua kia ra/vao pho ban solo lien tuc nen
    # tap `maps` chop tat giua {21011} va {21011, 62001}. Mai 13:11:33 moi ra duoc lenh gom - ba
    # phut cho mot viec dieu phoi DA BIET tu giay dau.
    #
    # Gio: het lech phai GIU duoc `HET_LECH_CHAC_SEC` thi dong ho moi duoc go.
    _het_lech_tu = st.get("het_lech_tu")
    if lech:
        _het_lech_tu = None
        if lech_tu is None:
            lech_tu = time.time()
    elif lech_tu is not None:
        if _het_lech_tu is None:
            _het_lech_tu = time.time()
        elif time.time() - float(_het_lech_tu) >= HET_LECH_CHAC_SEC:
            lech_tu = None
            _het_lech_tu = None
    with st["lock"]:
        st["het_lech_tu"] = _het_lech_tu
    # LECH MAP an han NGAN, lech kenh giu han cu: "khac map thi gom map" khong phai viec phai can
    # nhac mot phut. Teleport chuyen tiep chi mat vai giay.
    _han = LECH_MAP_AN_HAN_SEC if len(maps) > 1 else KE_HOACH_LECH_MAP_SEC
    # PHAI DANG LECH moi duoc ra lenh. Dong ho giu qua vai nhip nhieu la de KHONG MAT tuoi cua mot
    # lan lech ngat quang - khong phai de ra lenh dong bo cho party dang lanh lan.
    lech_lau = lech and lech_tu is not None and time.time() - lech_tu > _han

    # TRONG DI GIOI: van phai xu ly khi lech, nhung bang DONG BO TAI CHO chu KHONG gom ve thanh.
    #
    # Tu DG ra thanh la phai DI BO RA CONG (49942 -> 12003 -> 12002 -> 12001), tuc loi ca party
    # ra khoi DG. Da xay ra that (party 2, 06/09):
    #     00:53:27 REFORM gen -> 1 - dieu phoi: party o 3 MAP khac nhau -> gom ve cung map/kenh
    #     00:59:28 [gamo] reform: sga002/3/4/6 KET 240s khong ve duoc Truong Sa (map=49942)
    #     01:00:12 [gamo] smart path map 49942: (870,740) -> (270,210)   <- cong thoat DG
    #     01:00:29 [gamo] qua cong idx=1 -> map 12003
    # Cach dung la `resync_gen`: giai tan party + sync lai kenh + moi lai, TAT CA TRONG DG.
    # Member da phan ung san voi co nay (`is_digioi and not digioi_solo`), va leader thi cu 20s
    # lai tu sync khi party thieu nguoi (`_should_resync_incomplete_digioi_party`) - dieu phoi
    # chi can BAM NUT do khi party lech qua lau ma khong ai bam.
    if pha == "digioi":
        viec = VIEC_DONG_BO if lech_lau else VIEC_LAM
        if lech_lau and not ly_do:
            ly_do = _ly_do_lech(maps, kenhs, _lech_kenh_that, "trong DG ", _mot_minh)
    elif lech_lau:
        # LECH KENH -> DONG BO TAI CHO, khong gom ve thanh: doi kenh la xong, keo ca party ve
        # thanh chi ton them mot vong di duong. Chi lech MAP moi phai gom.
        # CUNG MAP thi lech chi co the la lech KENH -> dong bo tai cho, KHONG doi hoi leader phai
        # "bao cao" (`_lech_kenh_that`) truoc. Bat leader bao cao thi mat luon truong hop leader
        # dang ban viec khac: party 5 (06/09) ca 5 acc deu o map 12922 ma van ra lenh GOM voi ly do
        # "lech kenh [1, 5]" - gom ve thanh giua thap 2K la vo nghia, va `_do_reform` khong lam gi
        # duoc nen leader quay vong 201.495 lan.
        viec = VIEC_DONG_BO if len(maps) <= 1 else VIEC_GOM
        if not ly_do:
            ly_do = _ly_do_lech(maps, kenhs, _lech_kenh_that, "", _mot_minh)
    elif song and _leader_dang_rot(pidx, song):
        # LEADER dang login lai -> server DA thao doi. Khong doi roster bao (no toi tre, va trong
        # luc do bot ngoi im); cu coi la mat doi va chuan bi moi lai ngay khi leader vao.
        viec = VIEC_MOI
        if not ly_do:
            ly_do = "LEADER dang dis/login lai -> server da giai tan doi, phai lap lai"
        reset_party_joined(pidx)   # so nho khong duoc giu nguoi cua party da tan (L2d)
    elif song and _dang_doi_kenh(song):
        # Co acc dang do viec doi kenh -> roster dang bien dong, CHUA KET LUAN gi ca.
        viec = VIEC_LAM
        if not ly_do:
            ly_do = "co acc dang doi kenh -> cho roster on dinh roi moi quyet"
    elif song and len(maps) > 1:
        # CON LECH MAP thi CHUA DEN LUOT LAP PARTY. Thu tu bat buoc (user chot 08/09): "cung map
        # cung kenh thi party, lech map thi gom lai map, lech kenh thi gom lai kenh".
        #
        # Nhanh `_thieu_doi` ben duoi tung chay ca khi dang lech map - chi vi lech CHUA LAU nen
        # nhanh gom o tren khong bat. Ket qua la no ra lenh lap party giua luc party dang o ba noi,
        # va cau ly do thi noi doi: "cung map/kenh nhung DOI chua du".
        #
        # Ca that 08/09 party 4 (user: "gom lai thi cai dau tien phai check la co cung map hay ko"):
        #   16:16:53 [party 4] gen 23: viec=gom - party dang o 3 MAP khac nhau [12001, 12061, 23001]
        #   16:16:55 [party 4] gen 24: viec=moi - cung map/kenh nhung DOI chua du (tat ca = 0)
        #   16:16:55 [party 4] ca party da chung kenh 2 nhung DOI chua du -> LAP LAI PARTY
        # Hai giay khong the keo ca nam acc ve mot map. Lap party luc do thi duoc 3 nguoi (gen 27:
        # roster tat ca = 2), roi ca lu di train, lech map lai (gen 29) -> gom lai tu dau.
        #
        # Lech map THOANG QUA (acc dang teleport) la binh thuong -> VIEC_LAM, de dong ho `lech_lau`
        # o tren chay; qua han thi chinh no ra lenh GOM. Chi CAM ra lenh lap party luc dang lech.
        viec = VIEC_LAM
        if not ly_do:
            ly_do = "con lech map %s -> chua lap party, cho gom xong" % sorted(maps)
    elif song and _thieu_acc_song(pidx, song):
        # CHUA DU ACC LOGIN XONG -> CHUA duoc ket luan "cung map/kenh". Phep dem chi nhin acc DANG
        # SONG, nen 2 dua vao truoc cung kenh 1 bi ket luan la "ca party cung kenh" -> ra lenh MOI,
        # trong khi ba dua con lai chua login xong va co the o kenh khac han.
        #
        # Ca that party 2, 13/09 (user: "dung luat thi cung kenh roi moi den doan moi pt, o day
        # khac ma van den doan moi pt la the lon nao"):
        #   15:18:19 [party 2] gen 1: viec=moi - cung map/kenh nhung DOI chua du (sga001=0 sga002=0)
        #   15:18:19 [party 2] -> LAP LAI PARTY
        #   15:18:25 [party 2] gen 2: viec=dong_bo - cung map nhung LECH KENH [1, 2]
        # Sau giay sau, ca nam dua login xong thi lo ra lech kenh - loi moi truoc do la vo ich.
        viec = VIEC_LAM
        if not ly_do:
            ly_do = ("moi %d/%d acc login xong -> chua ket luan map/kenh, cho du roi moi quyet"
                     % (len(song), len(party_accounts(pidx))))
    elif song and _chua_biet_map:
        # CHUA BIET MAP CUA MOT NGUOI THI CHUA XONG BAC MAP - chua duoc xuong bac kenh.
        # Phep dem `maps` o tren BO QUA acc chua biet map, nen 4 dua cung map + 1 dua chua biet bi
        # ket luan la "cung map" -> tut xuong bac kenh -> `_dieu_phoi_chot_kenh` moi phat hien
        # thieu du lieu va `return None` IM LANG -> khong bao gio chot duoc kenh dich.
        #
        # Ca that party 9 (va party 4), 13/09 (user: "p4 p9 lai bi cai loi lech kenh ma deo gom
        # kenh lai"):
        #   14:46:38 [party 9] gen 8: viec=dong_bo - cung map nhung LECH KENH [1, 2]   <- kenh=None
        #   14:47:41 / 14:48:42 / ... / 14:53:48  lap 7 phut, KHONG mot dong `Chuyen kenh ->` nao
        # Trong thap 2K, acc bi keo sang scene moi thi `current_map` rong mot luc
        # ("request scene khong co self-spawn", "pos=None map=..."), du de dinh cho nay.
        #
        # Phai CUNG MAP roi moi den luot kenh (user: "phai biet cung map roi moi den doan dong bo
        # kenh chu"). Chua biet != khong sao (L13).
        viec = VIEC_LAM
        if not ly_do:
            ly_do = ("chua doc duoc map cua %s -> chua xong bac gom map, chua den luot kenh"
                     % (sorted(_chua_biet_map),))
    elif song and _thieu_doi(pidx, song) and _ai_lech_instance(pidx, song):
        # CUNG MAP + CUNG SO KENH MA KHONG THAY NHAU = KHAC INSTANCE. So kenh la so NHO va sai
        # duoc (KNOWLEDGE.md muc 7); con "ai dang dung quanh minh" (`0x03 PlayerAppear`) la bang
        # chung that - server chi gui cho nguoi CUNG SCENE + CUNG INSTANCE.
        # User 13/09: "biet duoc nhung nguoi xung quanh minh thi biet duoc co cung kenh hay ko,
        # co cai lon gi ma ko chac".
        #
        # Ca that party 1, 13/09 (user: "vi sao no ko lap duoc pt, no dung the nay gio") - 10 phut:
        #   14:24:40..14:32:46 [party 1] LAP LAI PARTY (chihao188=0 ... sga017=0)  <- roster dung 0
        #   14:32:56 [thbay] (LEADER) moi 2 nguoi ma SERVER CHUA HE cho thay ho quanh minh:
        #                    ['7c5dd8f8:chua co 0x03', '0c1dd3f8:chua co 0x03']
        # Ca party cung map 21833, cung toa do (3020..3040, 1000..1020), entity moi ĐÚNG het. Dieu
        # phoi thi thay "cung kenh 6" nen ra lenh MOI mai - loi moi khong the toi noi.
        _lech = _ai_lech_instance(pidx, song)
        viec = VIEC_DONG_BO
        if not ly_do:
            ly_do = ("cung map nhung %s KHONG THAY duoc dong doi (khac instance du cung so kenh) "
                     "-> dong bo kenh truoc khi moi (%s)" % (_lech, _tinh_hinh_doi(song)))
    elif song and len(kenhs) > 1 and _thieu_doi(pidx, song):
        # CUNG MAP ROI NHUNG CON LECH KENH -> GOM KENH TRUOC, chua den luot moi party.
        #
        # Thu tu user chot: "lech map thi dong bo map truoc, lech kenh thi dong bo kenh, du thi
        # lap party". Bac nay truoc day thieu: cung map + thieu doi la roi thang xuong `VIEC_MOI`,
        # nen dieu phoi vua bao "moi party" vua chot kenh dich - hai lenh da nhau, vi moi party thi
        # phai o trong doi con doi kenh thi phai ROI doi. Va moi nguoi khac kenh la vo ich: server
        # khong chuyen loi moi qua kenh.
        # Ca that party 7, 11/09 (user: "lech map thi phai dong bo map truoc chu, sao lai di dong
        # bo kenh"):
        #   18:01:43 gen 7: viec=moi - cung map/kenh nhung DOI chua du (tat ca = 0)
        #   18:01:43 DIEU PHOI: party lech kenh {1: 3, 2: 2} -> CHOT kenh dich = 1
        #   18:01:45 DIEU PHOI: party lech kenh {1: 3, 2: 2} -> CHOT kenh dich = 8
        # Cau ly do "cung map/kenh" con noi doi: kenh dang lech {1: 3, 2: 2}.
        #
        # KHONG doi `lech_lau`: lech kenh o day khong phai "thoang qua" - no chan dung viec dang
        # lam (moi party), nen phai xu ngay. An han cua `lech_lau` la de tranh ra lenh GOM (keo ca
        # party ve thanh) oan, con dong bo kenh tai cho thi khong ton gi.
        viec = VIEC_DONG_BO
        if not ly_do:
            ly_do = "cung map nhung LECH KENH %s -> gom kenh truoc khi moi (%s)" % (
                sorted(kenhs), _tinh_hinh_doi(song))
    elif song and _thieu_doi(pidx, song):
        # CHI LAP PARTY O THANH TAP KET HOAC MAP TRAIN (user chot 13/09). Thanh di ngang qua thi
        # chua toi luot moi: lap xong la phai teleport di tiep, ma teleport bat buoc roi doi ->
        # party vua lap lai tan, va ca chuoi quay lai tu dau.
        _noi = next(iter(maps)) if len(maps) == 1 else None
        if _noi is not None and _o_thanh_di_qua(pidx, st, _noi):
            viec = VIEC_GOM
            if not ly_do:
                ly_do = ("dang o thanh DI NGANG QUA %s, chua toi thanh tap ket %s -> di tiep roi "
                         "moi lap party (%s)" % (_noi, _thanh_tap_ket_dich(pidx, st),
                                                 _tinh_hinh_doi(song)))
        else:
            viec = VIEC_MOI
            if not ly_do:
                ly_do = "cung map/kenh nhung DOI chua du (%s)" % _tinh_hinh_doi(song)
    else:
        # DU DOI, CUNG MAP+KENH -> den luot hai buoc cuoi cua chuoi di train.
        viec, _ly_train = _viec_di_train(pidx, st, song, pha)
        if _ly_train and not ly_do:
            ly_do = _ly_train
        # RUT LENH REFORM DA DAT MUC DICH. Bump reform la lenh "doi hong, lap lai di"; doi du roi
        # thi no CHET, nhung truoc day khong ai thu no ve - no nam do cho duoc thi hanh THEM LAN
        # NUA, va lan do pha dung cai party vua lap.
        #
        # DIEU PHOI ra lenh thi DIEU PHOI rut lenh. Khong de acc tu ket luan "minh vua lam xong
        # cai ma lenh muon" - do la mo lai cua acc tu quyet (L1).
        #
        # Ca that party 1, 13/09 (user: "gom du pt, 1 luc sau lai huy pt roi pt lai"):
        #   23:19:02 [party 1] REFORM gen -> 2 - chung kenh roi ma doi khong du -> lap lai party
        #   23:19:33..44 [thbay] (LEADER) moi 4 member -> roster 1 -> 2 -> 3 -> 4
        #   23:19:49 [thbay] (LEADER) DU PARTY (4/4 member join)          <- LENH DA DAT MUC DICH
        #   23:19:50 [thbay] (LEADER) reform pending -> BO QUA keo ra spot, de keepalive REFORM
        #   23:19:55 [thbay] (LEADER) -> REFORM party (gen 2)             <- PHA party vua lap
        #   23:19:55 [thbay] reform gen 2: bao CA PARTY ra diem tap ket truoc khi lap lai party
        # Leader lam DUNG lenh: loat moi 23:19:33 chinh la noi dung cua gen 2. Nhung viec moi do
        # chay trong vong moi cua leader, khong cham vao `reform_gen_handled` (chi vong chinh ghi),
        # nen quay ve vong chinh no van thay `reform_gen 2 > handled 1` -> lam lai tu dau.
        #
        # Chi rut o DUNG nhanh nay: du doi + cung map + cung kenh. Con lech map/kenh hay thieu
        # nguoi thi lenh VAN CON HIEU LUC, khong duoc dong vao.
        with st["lock"]:
            _rg = int(st.get("reform_gen", 0) or 0)
            if _rg and int(st.get("reform_gen_thoa", 0) or 0) < _rg:
                st["reform_gen_thoa"] = _rg
                log.info("[party %d] DIEU PHOI: RUT lenh reform gen %d - da du doi (%s), cung map "
                         "%s kenh %s -> khong thi hanh lai (lam lai la pha party vua lap)",
                         pidx + 1, _rg, _tinh_hinh_doi(song), sorted(maps), sorted(kenhs))

    # PARTY DUNG HINH: du nguoi, cung map/kenh, khong lech gi ca - nhung KHONG AI NHUC NHICH.
    #
    # Dieu phoi cu chi nhin "lech map / lech kenh / thieu nguoi trong doi". Ba thu do deu BINH
    # THUONG thi no ket luan VIEC_LAM va khong lam gi nua - ke ca khi ca party dung im hang gio.
    #
    # Ca that 07/09 party 1, 44 PHUT (12:36:15 -> 13:20:31, 327 luot log):
    #   4 acc: reform: CHO ca party ve Trường Sa (4/5) - THIEU: brubb46677
    #          [map=23001, da ve Trường Sa, cho ca party (37s)]
    #   brub : (member) CHO leader quyet dinh (leader co the dang reconnect)...
    # CA NAM DUA DEU DA O TRUONG SA (23001), cung kenh, du doi. brub cho leader, leader cho brub.
    # Con so "(37s)" dung im suot 44 phut. Dieu phoi khong thay gi "lech" nen im theo.
    #
    # Do TIEN DO thay vi do "lech": doc thang (map, vi tri, dang danh) cua tung acc; y het nhau qua
    # KE_HOACH_DUNG_HINH_SEC = khong ai lam gi -> PHA THE bang lenh gom.
    # KHONG LAM DUNG MODE: ca party dam chan o THANH trong khi mode la train / Di Gioi / event.
    #
    # User 07/09: "ko lech map ko lech kenh nhung ko party va ko thuc hien dung mode duoc chon thi
    # phai xu ly chu". Dung: ba phep do cu (lech map / lech kenh / thieu nguoi) deu XANH khi ca
    # party cung dam chan mot cho SAI - vi chung chi so cac acc VOI NHAU, khong so voi VIEC PHAI LAM.
    #
    # Thanh la cho di QUA (tele trung gian, cat do, mua thuoc), khong phai cho LAM VIEC. Ca party
    # cung o thanh qua lau = khong ai lam gi -> pha the giong nhu dung hinh.
    #
    # CHI xet khi viec = VIEC_LAM (da du doi). Party CHUA du doi thi dung o thanh CHINH LA viec
    # dung - do la diem gom cua vong reform. Bump gom luc do = abort chinh vong gom vua ra lenh.
    # Ca that 08/09 party 1 (user: "ve cung kenh roi con deo them party"):
    #   00:30:12  ca 5 acc deu 'reform: da ve Trường Sa, cho ca party'   <- dang gom, dung viec
    #   00:30:50  DIEU PHOI gen 16 viec=gom - "dam chan o THANH 23001 240s" -> REFORM gen -> 8
    #   00:32:01  DIEU PHOI: chung kenh roi ma DOI chua du (tat ca = 0)   -> REFORM gen -> 9
    # Bump o 00:30:50 quang ca 5 acc ve dau vong reform nen roster ve 0, roi chinh cai roster 0 do
    # lam de cho lan bump thu hai. Mat 3 phut moi thoat (00:33:24 gen 23 viec=lam).
    if viec == VIEC_LAM and song and pha != "event":
        _o_thanh = all(
            getattr(config, "is_teleport_city", lambda _c: False)(
                int(getattr(c, "current_map", 0) or 0))
            for _u, c in song)
        if _o_thanh:
            if not st.get("o_thanh_tu"):
                st["o_thanh_tu"] = time.time()
            elif time.time() - float(st["o_thanh_tu"]) > KE_HOACH_DUNG_HINH_SEC:
                viec = VIEC_GOM
                ly_do = "ca party dam chan o THANH %s %.0fs (mode=%s) - khong lam dung viec" % (
                    sorted(maps)[0] if maps else "?",
                    time.time() - float(st["o_thanh_tu"]), raw_mode or "?")
                st["o_thanh_tu"] = time.time()
        else:
            st["o_thanh_tu"] = 0.0

    if viec == VIEC_LAM and song:
        _ket = _acc_dung_hinh(st, song, KE_HOACH_DUNG_HINH_SEC)
        # PARTY DU NGUOI + CUNG KENH thi "dong bo" KHONG CON NGHIA GI - `resync_gen` chi lam duoc
        # dung mot viec: bat moi member `leave_party()` roi moi lai. Tuc DAP party dang lanh.
        # Trong DG do chinh la cai lam leader "moi 2198s chua du party (2/4)" dem 10->11/09.
        # Phat hien dung hinh van giu (co the party ket that), nhung cach chua phai khac cach pha
        # party - va party DU thi khong con gi de dong bo.
        if _ket and pha == "digioi" and not _thieu_doi(pidx, song) and len(kenhs) <= 1:
            log.debug("[party %d] %d acc dung hinh nhung party DU + cung kenh -> khong resync "
                      "(resync chi de dap doi dang lanh)", pidx + 1, len(_ket))
            _ket = []
        if _ket:
            viec = VIEC_GOM if pha != "digioi" else VIEC_DONG_BO
            ly_do = "%d/%d acc DUNG HINH qua %.0fs: %s" % (
                len(_ket), len(song), KE_HOACH_DUNG_HINH_SEC, sorted(_ket))
            st["nhip_acc"] = {}   # ra lenh roi thi moi acc tinh lai tu dau

    # BAO CHO ACC BIET party dang gom -> viec vat (cat tien trang / ban Noi Dat) phai HOAN.
    # Viec vat keo acc sang map KHAC (12263 / 12061) dung luc ca party phai o mot cho; user VAN cho
    # phep lam, chi la khong phai luc nay (L0: du party roi lam gi thi lam).
    dat_party_dang_gom(pidx, viec in (VIEC_GOM, VIEC_MOI, VIEC_DONG_BO))
    # CHOT AI KEO CA PARTY DI DUONG. Acc khong tu quyet - khong co lenh thi no dung yen.
    #
    # Di ra bai train bat buoc phai teleport, ma teleport thi phai ROI DOI truoc (client chan khi
    # con trong doi). Nen viec nay phai giao cho DUNG MOT nguoi; hai acc cung di la party tan.
    _keo = config.PARTY_LEADER_ACC.get(pidx)
    if viec in (VIEC_GOM, VIEC_DONG_BO):
        # DANG GOM thi CA PARTY phai tu di ve diem hen - do la ca duy nhat moi acc deu phai tu
        # teleport (va roi doi la dung, vi doi dang hong nen moi phai gom).
        _keo = "*"
    elif not _keo:
        # Party khong co bot-leader (user tu dieu khien / party mot acc) -> khong ai phai cho ai.
        _keo = "*"
    elif not any(u == _keo for u, _c in song):
        # Nguoi duoc giao da tat/rot -> khong de ca party dung cho mot acc khong con chay (L0).
        log.info("[party %d] DIEU PHOI: nguoi keo '%s' khong con chay -> tam giao cho tat ca",
                 pidx + 1, _keo)
        _keo = "*"
    dat_nguoi_keo(pidx, _keo)

    kh = {"pha": pha,
          "map": (sorted(maps, key=lambda m: -len(maps[m]))[0] if maps else None),
          "kenh": (sorted(kenhs)[0] if len(kenhs) == 1 else None),
          "thanh": cu.get("thanh"),
          "viec": viec}
    # 2K NHI KIEU: gom = DI BO xuong tang, KHONG phai ve thanh. DIEU PHOI chot MOT tang cho ca
    # party (dung nguyen tac: bot ra lenh, ca party theo - khong acc nao tu tinh lay).
    if viec == VIEC_GOM and pha == "event":
        kh["tang_gom"] = _chot_tang_gom(pidx, st, song)
    if pha == "event":
        _dieu_phoi_chot_2k_xong(pidx, st, song)
    return kh, ly_do, lech_tu


def _dieu_phoi_chot_2k_xong(pidx, st, song):
    """DIEU PHOI la nguoi DUY NHAT chot "2K da het" -> bat `event_exit_now` (ra thap + tat acc).

    Truoc day luong LEADER tu bam co nay trong `_on_crawl_done`, ma ham do chay o `finally` nen
    no bat len CA KHI leader chi bi ROT MANG. Party 12 (06/09): leader dinh ma 13 o tang 5, ca
    party bi keo nguoc xuong 8 tang roi tat game giua chung.

    Dieu phoi nhin trang thai THAT:
      - "thua" / "xong"  -> het that, cho thoat.
      - "het_duong" (da danh HET tang ma khong co cong len) -> khong con gi lam o tang do nua.
        Coi nhu xong: ra khoi thap + tat acc. KHONG duoc dung im - party 1 va party 11 (06/09)
        danh xong tang 11 roi dung yen o (650,430) hang phut (user: "danh thua roi m dung do an va
        a"). Thieu du lieu world_nav thi ghi log de bo sung, chu khong bat acc dung cho.
      - "ket"  (ket o cong / danh thieu tran) -> CHUA het. Party 15 (06/09): leader
        dung 10 phut o tang 9 vi party chi con 0/4 nguoi, cong khong qua duoc. Viec can lam la
        GOM + MOI LAI roi thu cong tiep, khong phai bo ca vong 2K.
      - "dut" (client chet) -> CHUA het. Supervisor relogin, `_decide_2k_resume` leo tiep.
    """
    kq = st.get("2k_ket_qua")
    if not kq or st["event_exit_now"].is_set():
        return
    if kq in ("ket", "dut"):
        return              # chua het - de vong keepalive/dieu phoi gom & moi lai roi leo tiep
    st["event_exit_now"].set()
    log.info("[party %d] DIEU PHOI: 2K %s -> CA DOI ra khoi thap roi thoat game", pidx + 1,
             {"thua": "THUA", "xong": "da xong",
              "het_duong": "da danh het tang ma khong co cong len (thieu du lieu world_nav)"}
             .get(kq, kq))


# Da chot TANG GOM thi giu bay lau - di bo xuong vai tang mat hang chuc giay.
TANG_GOM_KIEN_NHAN_SEC = 180.0


def _chot_tang_gom(pidx, st, song):
    """Chot TANG GOM va GIU (L6) - dung tinh lai moi nhip.

    `_tang_gom_2k` lay `min(map)` cua ca doi. Tinh lai moi 2 giay thi trong luc member DANG DI
    XUONG, `min` tut theo tung buoc chan cua chinh no -> dich chay truoc mat, ca doi tut toi day
    thap, mat het tang da leo.

    Log that party 8 (06/09):
        17:47:08 gom [12932, 12934] -> dich 12932
        17:47:19 gom [12931, 12934] -> dich 12931   (member vua xuong 12931)
        17:51:05 gom [12922, 12934] -> dich 12922   (tut toi DAY thap)

    Giu dich cho toi khi: CA DOI da toi dich (het lech) · qua han · hoac khong con o trong thap.
    """
    moi = _tang_gom_2k(pidx, song)
    if moi is None:
        with st["lock"]:
            st["tang_gom"] = None
            st["tang_gom_luc"] = 0.0
        return None
    cu = st.get("tang_gom")
    if cu:
        _luc = float(st.get("tang_gom_luc", 0.0) or 0.0)
        _con = {int(getattr(c, "current_map", 0) or 0) for _u, c in song}
        if len(_con) <= 1:
            with st["lock"]:                      # da gom xong -> xoa dich
                st["tang_gom"] = None
                st["tang_gom_luc"] = 0.0
            return None
        if time.time() - _luc < TANG_GOM_KIEN_NHAN_SEC:
            return int(cu)                        # dang di xuong -> GIU, dung tut theo
        log.info("[party %d] DIEU PHOI: tang gom %s qua %.0fs van chua gom xong -> chot lai",
                 pidx + 1, cu, TANG_GOM_KIEN_NHAN_SEC)
    with st["lock"]:
        st["tang_gom"] = moi
        st["tang_gom_luc"] = time.time()
    if cu != moi:
        log.info("[party %d] DIEU PHOI: CHOT tang gom = %s (tang thap nhat ca doi toi duoc)",
                 pidx + 1, config.scene_name(int(moi)))
    return moi


def _tang_gom_2k(pidx, song):
    """Tang GOM DOI cho 2K nhi kieu: TANG THAP NHAT ma ca doi dang o. None = khong phai 2K.

    Doc THANG `current_map` cua tung client thay vi bang event_start_map cu (bang do chi duoc
    dien luc LOGIN) - nho vay bat duoc ca lech tang GIUA CHUNG, dung ca party 1 (06/09): leader
    len 12925 con 4 member o 12924, khong ai kiem.

    Co acc dang o NGOAI thap -> ve CUA VAO (`dest_map`): acc ngoai tele vao binh thuong, acc
    trong thap di bo xuong day.
    """
    pcfg = getattr(config, "PARTY_CONFIG", {}).get(pidx, {})
    try:
        ev = config.event_hom_nay(pcfg.get("event_key") or "")
    except Exception:
        ev = None
    if ((ev or {}).get("party_battle") or {}).get("kind") != "floor_crawl":
        return None
    trong, ngoai = [], 0
    for _u, c in song:
        m = int(getattr(c, "current_map", 0) or 0)
        if m and _inside_floor_crawl_tower(ev, m):
            trong.append(m)
        else:
            ngoai += 1
    if not trong:
        # KHONG AI trong thap -> khong co viec "gom tang" nao ca (ca party dang o thanh, dang di
        # toi, hoac da ra xong). Tra None, dung tra `dest_map`: cho goi dung ket qua nay de biet
        # "co dang trong thap khong", tra so la chan nham moi thu o moi noi.
        return None
    if ngoai:
        # Co dua trong thap, co dua ngoai -> ve CUA VAO: dua ngoai tele vao binh thuong, dua trong
        # thap di bo xuong day.
        return int((ev or {}).get("dest_map") or 0) or None
    return min(trong)


def _thi_hanh_gom(c, st, label, role, kh, do_reform):
    """Thi hanh lenh GOM cua dieu phoi. TRONG THAP 2K thi DI BO xuong tang, ngoai ra thi reform.

    `_do_reform` di theo route VE THANH. Trong thap 2K khong co route nao ca, nen no chi in
    "reform: khong co smart/legacy route -> bo qua" roi tra ve NGAY - lenh gom cua dieu phoi
    KHONG BAO GIO duoc thi hanh, ma vong goi thi lap lai. Party 5 (06/09) quay 201.495 vong,
    party 1 thi leader len tang 12925 mot minh ma khong ai keo xuong.

    Tang dich do DIEU PHOI chot (`kh["tang_gom"]`), khong phai acc tu tinh - moi acc doc cung
    mot so nen ca party xuong cung mot tang.
    """
    _tang = kh.get("tang_gom")
    if _tang:
        pcfg = getattr(config, "PARTY_CONFIG", {}).get(getattr(c, "party_idx", -1), {})
        try:
            ev = config.event_hom_nay(pcfg.get("event_key") or "")
        except Exception:
            ev = None
        if ev is not None:
            if int(getattr(c, "current_map", 0) or 0) == int(_tang):
                return True          # da o dung tang gom -> dung yen cho ca doi xuong
            log.warning("[%s] (%s) DIEU PHOI gom 2K: di bo xuong %s (%s)",
                        label, role, config.scene_name(int(_tang)), kh.get("ly_do") or "lech tang")
            return bool(c.regroup_to_event_start(ev, dest=int(_tang)))
    do_reform()
    return True


def _dieu_phoi_thi_hanh(pidx, st, kh, doi):
    """Bien ke hoach thanh HANH DONG THAT. Chi dieu phoi duoc goi ham nay.

    Day la cho party 19 da thieu: leader BIET member lech map (no in ra 488 lan) nhung khong ai
    bien cai biet do thanh hanh dong. Gio dieu phoi tu bump reform -> moi luong acc dang di duong
    deu bi abort qua `_ab()` (da co san) roi gom ve cung cho.

    NHUNG BUMP LA CON DAO HAI LUOI - moi bump ABORT MOI ACC DANG DI DUONG (`_ab()`). Ma trong
    luc gom thi acc dang teleport chuyen tiep, LECH MAP/KENH LA CHUYEN BINH THUONG. Bump lien
    tuc = huy chinh viec gom ma minh vua ra lenh -> vong reset vo tan.

    DA XAY RA THAT (party 17, 05/09 18:49-18:51) - loi cua chinh ban dieu phoi dau tien:
        18:49:32 DIEU PHOI gen 4: viec=gom - lech kenh [1,2]        -> REFORM gen -> 4
        18:49:36 DIEU PHOI gen 5: viec=gom - lech kenh [1,2]        -> REFORM gen -> 5
        18:50:50 DIEU PHOI gen 6: viec=gom - 4 MAP khac nhau        -> REFORM gen -> 6
        18:50:56 DIEU PHOI gen 7: viec=gom - 3 MAP khac nhau        -> REFORM gen -> 7
        [chusau] (LEADER) ABORT di duong reform: reform_gen 5 -> 6 (acc khac bump)
        [chusau] (LEADER) ABORT di duong reform: reform_gen 6 -> 7 (acc khac bump)
    Ket qua: leader ket o thanh, member dung giua bai cho quai danh (user bao).

    Nen bump phai co HAI CHOT:
      1. Chi bump khi da lech LIEN TUC qua han (`lech_lau` ben `_dieu_phoi_quyet`).
      2. NGUOI KHAC vua bump / minh vua bump -> IM. Dang co mot dot gom chay roi, de no chay xong.
    """
    _viec = kh.get("viec")
    # KHONG doi ke hoach phai VUA DOI (`doi`). Lenh gom truoc day chi duoc phat dung CAI NHIP ke
    # hoach doi; nhip do ma trung cooldown thi lenh MAT LUON, va vi ke hoach khong doi nua nen
    # khong bao gio thu lai - dieu phoi van "thay lech map" ma khong con duong ra lenh.
    #
    # Ca that party 35, 13/09 (user: "party 35 thay lech map ma ko gom map lai duoc"):
    #   12:22:47 ra lenh gom               -> dieu_phoi_gom_luc = 12:22:47
    #   12:23:26 gen 7: viec=gom           -> trong cooldown 180s -> return IM LANG
    #   (tu do ke hoach van la 'gom' nhung khong con DOI -> khong ai phat lenh nua)
    #   12:26:35 [dvsau] (LEADER) chua du member (2/4) NHUNG dv608 o map 56103 (leader 56001)
    #            -> KHONG moi mu, GOM LAI party truoc      <- lap y het moi 60s, khong ai di dau
    # Gio: con lech thi NHIP NAO CUNG XET, cooldown lo viec khong ra lenh day nhau.
    if _viec not in (VIEC_GOM, VIEC_DONG_BO):
        return
    with st["lock"]:
        luc = float(st.get("dieu_phoi_gom_luc", 0.0) or 0.0)
        # Mot dot gom dang chay (cua minh hay cua acc khac deu the) -> de no chay xong. Moi lenh
        # gom ABORT moi acc dang di duong, ra lien tuc = ca party khong di xong buoc nao (p17).
        _con = KE_HOACH_GOM_COOLDOWN - (time.time() - luc)
        if luc and _con > 0:
            # IM LANG la cach lenh gom bien mat khong dau vet (party 35). Log co cua chong spam:
            # vong chay moi 2s, khong the in moi nhip.
            if doi or time.time() - float(st.get("gom_cho_log_luc", 0.0) or 0.0) > 60.0:
                st["gom_cho_log_luc"] = time.time()
                log.info("[party %d] DIEU PHOI: van '%s' (%s) nhung dot gom truoc moi ra %.0fs "
                         "-> CHO them %.0fs roi ra lenh lai (khong dap lenh dang chay)",
                         pidx + 1, _viec, kh.get("ly_do") or "lech",
                         KE_HOACH_GOM_COOLDOWN - _con, _con)
            return
        if _viec == VIEC_DONG_BO:
            # TRONG DG: bam `resync_gen` - member roi party + sync kenh lai NGAY TAI CHO, leader
            # thay party tut nguoi thi trong 20s tu sync + moi lai. KHONG dung `reform_gen`:
            # cai do la "gom ve thanh", tu DG la phai di bo ra cong.
            st["resync_gen"] += 1
            log.info("[party %d] DIEU PHOI: %s -> DONG BO TAI CHO (resync_gen -> %d), "
                     "KHONG gom ve thanh", pidx + 1, kh.get("ly_do") or "lech",
                     st["resync_gen"])
        else:
            _bump_reform(st, "dieu phoi: %s -> gom ve cung map/kenh" % (kh.get("ly_do") or "lech"),
                         uu_tien=True)
        st["dieu_phoi_gom_luc"] = time.time()


_DIEU_PHOI_THREAD = None
_DIEU_PHOI_LOCK = threading.Lock()


def bao_dam_dieu_phoi():
    """Dam bao co DUNG MOT luong dieu phoi cho CA BOT. Goi bao nhieu lan cung khong sao.

    VI SAO KHONG DE MOI PARTY MOT LUONG (ban dau lam vay - SAI):
    `_party_watcher` TU THOAT khi party khong con acc nao chay:
        if not accs or not any(is_account_running(u) for u in accs): return
    ma `start_party` chi dung watcher moi o nhanh PHIEN MOI:
        if not _fresh: return _start_party_accounts(...)   # <- thoat luon, khong dung watcher
    Nen chi can watcher thoat mot lan roi user start lai party luc VAN CON acc chay
    (`_fresh` = False) la party do chay VINH VIEN KHONG CO AI DIEU PHOI.
    Da xay ra that (party 53, 06/09): dong DIEU PHOI cuoi cung luc 01:53:44, sau do acc van
    chay binh thuong ma khong mot nhip dieu phoi nao - leader in "lech kenh live 1!=4" 64 lan
    trong 16 phut, khong ai ra lenh dong bo.

    Mot luong cho ca bot thi khong con trang thai "party khong co dieu phoi": no quet MOI party,
    va tu hoi sinh vi `start_party`/`start_account` deu goi ham nay.
    """
    global _DIEU_PHOI_THREAD
    with _DIEU_PHOI_LOCK:
        t = _DIEU_PHOI_THREAD
        if t is not None and t.is_alive():
            return
        _DIEU_PHOI_THREAD = threading.Thread(target=_dieu_phoi_loop, name="dieu-phoi",
                                             daemon=True)
        _DIEU_PHOI_THREAD.start()
        log.info(">>> DIEU PHOI: khoi dong luong dieu phoi CHUNG cho ca bot (nhip %.1fs)",
                 KE_HOACH_NHIP)


def _dieu_phoi_loop():
    """Quet MOI party moi `KE_HOACH_NHIP` giay. KHONG BAO GIO thoat.

    Loi cua mot party khong duoc lam chet ca vong - boc try/except tung party.
    """
    lech_tu = {}
    _lan_truoc = 0.0
    _bao_tre_luc = 0.0
    while True:
        time.sleep(KE_HOACH_NHIP)
        # DONG HO NHIP. Dieu phoi la luong DUY NHAT ra lenh cho ca bot, nen no bi tre = ca bot mu
        # trong tung ay giay. Do dac 13/09 (250 acc, 50 party): ban than viec quet chi ton 16ms
        # (0.33ms/party) - so party khong bao gio la van de. Cai lam no tre la GIL: 250 luong acc
        # ban CPU thi luong nay phai xep hang, quet mot vong co the mat toi 6 giay.
        # Tung xay ra that (L10, p5 13:15-13:20): mot vong nong cua acc `continue` khong ngu ->
        # 8.000 vong/giay -> bo doi luong dieu phoi -> ke hoach dong bang 5 phut.
        _dau = time.time()
        if _lan_truoc:
            _tre = _dau - _lan_truoc - KE_HOACH_NHIP
            if _tre > DIEU_PHOI_TRE_CANH_BAO_SEC and _dau - _bao_tre_luc > 60.0:
                _bao_tre_luc = _dau
                log.warning(">>> DIEU PHOI TRE NHIP: vong truoc cach %.1fs (nhip %.1fs, tre %.1fs)"
                            " - luong dieu phoi dang bi doi CPU (GIL). Ca bot khong nhan lenh moi"
                            " trong tung ay giay.", _dau - _lan_truoc, KE_HOACH_NHIP, _tre)
        _lan_truoc = _dau
        try:
            so_party = len(getattr(config, "PARTIES", []) or [])
        except Exception:
            so_party = 0
        for pidx in range(so_party):
            try:
                song = _acc_song(pidx)
                if not song:
                    lech_tu.pop(pidx, None)
                    continue
                st = _pstate(pidx)
                if st.get("leader_switch_pending") or st.get("ui_mode_restart_users"):
                    continue
                kh, ly_do, lech_tu[pidx] = _dieu_phoi_quyet(pidx, st, song, lech_tu.get(pidx))
                doi = _ghi_ke_hoach(st, pidx, kh, ly_do)
                _dieu_phoi_thi_hanh(pidx, st, dict(kh, ly_do=ly_do), doi)
                _dich = _dieu_phoi_chot_kenh(pidx, st, song, kh)
                # CHOT roi phai GUI - chot xong de day cho acc tu doc la quay lai dung cai bang
                # thong bao da lam chet 9 phut cua party 3 (xem `_dieu_phoi_thi_hanh_kenh`).
                _dieu_phoi_thi_hanh_kenh(pidx, st, song, _dich)
                _dieu_phoi_chot_map(pidx, st)
            except Exception as e:
                log.warning("[party %d] DIEU PHOI loi (bo qua nhip nay): %s", pidx + 1, e)
        # Quet lau bat thuong -> KHONG phai do so party (0.33ms/party), ma do co party nao do
        # dang keo vong nay. In ra de con truy, thay vi doan.
        _lau = time.time() - _dau
        if _lau > DIEU_PHOI_QUET_LAU_SEC and time.time() - _bao_tre_luc > 60.0:
            _bao_tre_luc = time.time()
            log.warning(">>> DIEU PHOI: quet %d party mat %.1fs (binh thuong ~%.0fms) - co party "
                        "dang keo vong dieu phoi", so_party, _lau, so_party * 0.4)


# Dieu phoi gui lai lenh doi kenh cho CUNG mot acc sau bay lau. `switch_channel` tu no da co
# wait+retries; gui day hon la chong goi len nhau chu khong nhanh hon.
DIEU_PHOI_GUI_LAI_KENH_SEC = 12.0


def _kenh_doi_duoc_ngay(c, st):
    """Acc nay co doi kenh duoc NGAY BAY GIO khong (doc thang client, khong hoi acc).

    Doi kenh = doi INSTANCE. Gui giua tran thi server bo qua ma bot tuong da doi; gui giua event
    thi keo theo `scene_resume` -> `C:020-006` luc server chua giai xong tran -> `S:000-000` ma 47
    `<戰鬥未結束事件先結束>` -> DUT KET NOI (07/09: thsau/thmo/tonba/lbumot/xGAx dis trong 32 giay).

    Ba moc, tu chac den long: `state.in_battle` (0x35/0x34 len, `0x14 sub0700` END ha) · grace ket
    tran · `in_combat()` (idle-based, ngay sau khi tran vua mo con False nen KHONG dung mot minh).
    """
    if c is None or not getattr(c, "running", False):
        return False
    try:
        if st is not None and st.get("event_battle_active"):
            return False
    except Exception:
        pass
    try:
        if getattr(c.state, "in_battle", False) or c._in_battle_end_grace():
            return False
    except Exception:
        pass
    try:
        if c.in_combat():
            return False
    except Exception:
        pass
    return True


def _vi_sao_chua_doi_kenh(c, st):
    """Cua nao trong `_kenh_doi_duoc_ngay` dang chan - de log noi duoc LY DO, khong im lang.

    Giu DUNG THU TU voi `_kenh_doi_duoc_ngay`: lech nhau la log chi sai cho.
    """
    if c is None or not getattr(c, "running", False):
        return "acc da tat"
    try:
        if st is not None and st.get("event_battle_active"):
            return "CA PARTY dang danh event (doi kenh giua event -> ma 47 -> dut ket noi)"
    except Exception:
        pass
    try:
        if getattr(c.state, "in_battle", False):
            return "dang TRONG TRAN"
        if c._in_battle_end_grace():
            return "vua ket tran (grace) - cho server giai tran xong"
    except Exception:
        pass
    try:
        if c.in_combat():
            return "con dinh combat (idle ngan)"
    except Exception:
        pass
    return "khong ro"


def _dp_gui_doi_kenh(c, dich):
    """Mot luot gui lenh doi kenh, chay trong thread rieng cua acc do.

    Vong dieu phoi chay moi `KE_HOACH_NHIP` (2 giay) cho MOI party; `switch_channel` cho ack toi
    vai giay. Goi thang trong vong do la mot acc cham lam ke ca party khac dung hinh.
    """
    try:
        _truoc = getattr(c, "current_channel", None)
        # `theo_lenh=True`: DAY la lenh cua dieu phoi, nen duoc phep roi doi de doi kenh. Moi
        # duong khac goi `switch_channel` deu phai xin `nguoi_keo` (xem `_switch_channel_locked`).
        c.switch_channel(int(dich), wait=4.0, retries=1, theo_lenh=True)
        log.info("[%s] DIEU PHOI GUI doi kenh %s (dang o %s) -> ket qua %s",
                 getattr(c, "_label", "?"), dich, _truoc,
                 getattr(c, "_chan_switch_result", None))
    except Exception as e:
        log.warning("[%s] DIEU PHOI gui doi kenh %s loi: %s", getattr(c, "_label", "?"), dich, e)
    finally:
        c._dp_gui_kenh_dang_chay = False


def _dieu_phoi_thi_hanh_kenh(pidx, st, song, dich):
    """DIEU PHOI TU GUI lenh doi kenh cho tung acc lech - KHONG cho acc di ngang qua diem nghe.

    Truoc day dieu phoi chi GHI `st["kenh_dich"]` roi thoi; nguoi gui lenh la `_nghe_lenh_kenh()`
    trong `run_account`, ma ham do chi chay khi acc DI NGANG QUA mot trong chin diem nghe. Acc nao
    tut vao mot vong dai nam han trong `bot/client.py` (boss the gioi, van tieu, pho ban) la DIEC
    suot vong do. Ghi co len client cung la bao cao - van la bat acc tu lam (L2).

    Ca that 09/09 party 3 (user: "sao dieu phoi vo dung vai lon") - 9 phut, 14 nhip, phan bo kenh
    khong nhuc nhich mot li, va TUYET NHIEN khong co mot dong `Doi kenh` nao cua ca nam acc:
        12:40:16 [party 3] CHOT kenh dich = 16 (kenh IT NGUOI NHAT ma du cho ca team)
        12:40:33 [nanam]   Boss the gioi: DA VAO TRAN -> danh CHO HET TRAN
        12:41:03 [party 3] kenh dich 16 qua 45s van chua gom xong ({2: 1, 8: 2, 10: 2}) -> chot lai
        ... 12:41:49 / 12:42:34 / ... / 12:49:30, y het nhau ...
    Lenh chua he duoc gui di - khong phai doi kenh that bai. Bang chung co che van dung, chi la
    khong duoc chay: 13:08:59 luc acc ranh, bon member doi kenh xong trong MOT giay.

    Cung log do lo them mot lech: leader `nanam` nghe lenh CHAM HON member 74 giay (13:08:59 vs
    13:10:13) vi leader di qua diem nghe khac. Dieu phoi tu gui thi ca nam nhan cung mot nhip.

    `_nghe_lenh_kenh()` GIU LAI lam luoi do (acc doi kenh xong tu nhien bi day di noi khac), nhung
    khong con la duong chinh nua.
    """
    if not dich:
        return 0
    # CON LECH MAP THI CHUA TOI LUOT KENH. Kenh la thu THEO TUNG MAP: kenh 2 co o map nay nhung
    # khong co o map kia, nen gui lenh doi kenh cho acc dang o map khac la chac chan hong.
    #
    # Cua "dang gom map -> chua chot kenh" o `_dieu_phoi_chot_kenh` chi chan viec CHOT DICH MOI;
    # no tra ve dich CU, va cho nay van dem dich cu do gui cho MOI acc.
    #
    # Ca that 13/09, `thbay` dang o Nghiep Thanh (12061) trong khi party o 21001 (user: "cai do la
    # no chot kenh ngu, khac map ma di chot lenh"):
    #   12:55:35 [thbay] Ban Noi Dat: co 271 cai -> di NPC Nha buon Ng.Thanh
    #   12:55:38 [thbay] Doi kenh 2 THAT BAI: khong co khu do de doi (result=2)
    #   12:55:51 [thbay] Doi kenh 2 THAT BAI: khong co khu do de doi (result=2)
    #   ... lap deu moi 12 giay suot ca chuyen di ...
    # Map 12061 khong he co kenh 2. Lenh hong ma khong dong cua (L8), va no bam theo acc suot
    # luc no dang lam viec khac.
    _maps = {int(getattr(c, "current_map", 0) or 0) for _u, c in song
             if getattr(c, "current_map", None)}
    if len(_maps) > 1:
        if time.time() - float(st.get("kenh_cho_map_log", 0.0) or 0.0) > 60.0:
            st["kenh_cho_map_log"] = time.time()
            log.info("[party %d] DIEU PHOI: chua gui lenh kenh - party con o %d MAP khac nhau %s "
                     "(kenh co y nghia theo tung map)", pidx + 1, len(_maps), sorted(_maps))
        return 0
    bay = time.time()
    # ACC DANG LAM VIEC VAT -> KHONG QUAY RAY (user chot 14/09: "khi dang danh PB don va daily
    # quest thi dieu phoi tam thoi ko quay ray").
    #
    # PB don / boss the gioi nam HAN trong client, moi tran 70-80 giay. Gui lenh doi kenh vao giua
    # do thi hoac acc diec (dang trong vong tran), hoac no bo do -> mat luot ma van khong sang o 1.
    # Xong viec thi `account_task.__exit__` nha ra, luc do dieu phoi gui lai binh thuong.
    _ban = set(_ai_dang_lam_viec_le(song))
    n = 0
    for _u, c in song:
        try:
            if _u in _ban:
                continue
            if int(getattr(c, "current_channel", 0) or 0) == int(dich):
                continue
            if getattr(c, "_dp_gui_kenh_dang_chay", False):
                continue
            if bay - float(getattr(c, "_dp_gui_kenh_luc", 0.0) or 0.0) < DIEU_PHOI_GUI_LAI_KENH_SEC:
                continue
            if not _kenh_doi_duoc_ngay(c, st):
                # IM LANG O DAY = lenh dong bo kenh "ra" ma khong ai nhan, va log chi thay dieu
                # phoi lap lai "van 'dong_bo' ... CHO them Ns" - khong the biet vi sao khong ai di.
                # Ca that party 9, 13/09 (user: "p4 p9 lai bi cai loi lech kenh ma deo gom kenh
                # lai"): 14:46:38 -> 14:53:48, BAY PHUT, khong mot dong `Chuyen kenh ->` nao cua
                # ca nam acc.
                _cho = "%s_kenh_cho" % _u
                if bay - float(st.get(_cho, 0.0) or 0.0) > 60.0:
                    st[_cho] = bay
                    log.info("[party %d] DIEU PHOI: CHUA gui lenh doi kenh %s cho %s - %s",
                             pidx + 1, dich, getattr(c, "_label", _u), _vi_sao_chua_doi_kenh(c, st))
                continue
            c._dp_gui_kenh_luc = bay
            c._dp_gui_kenh_dang_chay = True
            threading.Thread(target=_dp_gui_doi_kenh, args=(c, int(dich)),
                             name="dp-kenh-p%d" % (pidx + 1,), daemon=True).start()
            n += 1
        except Exception as e:
            log.debug("[party %d] thi hanh kenh cho %s loi (bo qua): %s", pidx + 1, _u, e)
    return n


def _dieu_phoi_chot_kenh(pidx, st, song, kh=None):
    """DIEU PHOI tu chon KENH DICH cho ca party. Khong cho acc nao bao cao.

    `kh` = ke hoach VUA quyet xong o cung nhip (`_dieu_phoi_quyet`). Ham nay KHONG duoc tu ket
    luan lai tinh hinh party - mot party mot ket luan (L1).

    Kenh dich la TRANG THAI (`st["kenh_dich"]`), khong phai cai bat tay tung vong. Vong bat tay
    cu (`do_channel_sync`) chi bao ve duoc acc DANG DUNG TRONG vong cho: acc nao lam xong roi
    `break` ra la khong ai theo doi nua, doi dich sau do thi no khong bao gio biet.
    Da xay ra that (party 53, 06/09):
        02:09:11 vumba/vumsau doc dich = kenh 4 -> xong -> ROI vong dong bo
        02:09:12 picker mo vong MOI, dich = kenh 2
        02:09:22 sync kenh: 3/5 da sang kenh 2, CHUA sang: {qv813: 4, qv816: 4}
    Hai dua do KHONG HE nhan duoc lenh moi.

    CHON KENH NAO: kenh dang co NHIEU acc nhat (it phai di chuyen nhat). Khong hoi danh sach
    kenh - khong can, va hoi thi lai roi vao bay "kenh minh dang o trong co ve dong".

    CHON KENH NAO: kenh dang co NHIEU acc nhat (it phai di chuyen nhat). Roi DOC THANG ket qua
    doi kenh cua tung client (`_doc_ket_qua_doi_kenh`) de biet kenh nao vua bao DAY ma tranh, va
    de biet co phai lap lai party sau khi doi xong khong. Khong acc nao phai bao cao gi.

    DIEU PHOI LA NGUOI QUYET, khong nhuong ai: khong con nhanh "co vong bat tay chay thi thoi".
    Vong bat tay cu (`do_channel_sync`) gio chi con la CANH TAY thi hanh, khong tu chot kenh.
    """
    # MODE KHONG CAN LAP DOI -> khong chot kenh, khong lap lai doi (xem `_mode_can_lap_doi`).
    #
    # Guard nay TUNG NAM O CUOI HAM, sau ca nhanh "chung kenh ma doi khong du -> LAP LAI PARTY" -
    # cua chan dat SAU dung cai viec no phai chan (L3h). Party 21, 10/09:
    #   21:09:30 [party 21] ca party da chung kenh 1 nhung DOI chua du
    #                       (dieu906=4 dieu907=1 dieu908=2 dieu909=3 dieu910=4) -> LAP LAI PARTY
    #   21:11:31 ... 21:13:31 ... 21:15:31 ... 21:17:32   (moi 2 phut, dung bang cooldown)
    # DANG GOM MAP -> CHUA DEN LUOT KENH. Thu tu user chot: map truoc, kenh sau, roi moi lap doi.
    #
    # Ham nay co san phep "khac map thi so kenh vo nghia" (`return None` trong vong dem ben duoi),
    # nhung do la phep do cua RIENG no tren mot anh chup `song` khac - con `kh` la ket luan cua
    # dieu phoi trong CUNG nhip. Hai anh chup lech nhau vai tram ms la du de no chot kenh giua luc
    # ca party dang duoc keo ve mot map (party 7, 11/09).
    # Con `kenh_dich` cu thi GIU NGUYEN, khong xoa: gom map xong ma van lech kenh thi dung lai no.
    if kh is not None and kh.get("viec") == VIEC_GOM:
        log.debug("[party %d] chot kenh: dang gom MAP (viec=%s) -> chua den luot kenh",
                  pidx + 1, kh.get("viec"))
        return st.get("kenh_dich")
    if not _mode_can_lap_doi(pidx):
        with st["lock"]:
            if st.get("kenh_dich"):
                log.info("[party %d] DIEU PHOI: mode KHONG CAN LAP DOI -> thoi lenh doi kenh "
                         "(kenh_dich %s)", pidx + 1, st.get("kenh_dich"))
            st["kenh_dich"] = None
            st["kenh_dich_luc"] = 0.0
        return None
    # 40NPC ngoai gio: acc di doi thuong SOLO roi thoat - dung chung kenh khong de lam gi, ma lenh
    # doi kenh thi keo no ra khoi viec dang lam (xem `_party_40npc_ngoai_gio`).
    if _party_40npc_ngoai_gio(pidx, getattr(config, "PARTY_CONFIG", {}).get(pidx, {})):
        if st.get("kenh_dich"):
            log.info("[party %d] DIEU PHOI: 40NPC ngoai gio -> BO kenh dich %s (acc chi di doi "
                     "thuong roi thoat)", pidx + 1, st.get("kenh_dich"))
        with st["lock"]:
            st["kenh_dich"] = None
            st["kenh_dich_luc"] = 0.0
        return None
    # CON LECH MAP -> KHONG DUNG TOI KENH, ke ca khi chua co ke hoach (`kh is None`).
    #
    # User chot (14/09): "logic t noi tu dau: lech map thi dong bo map, lech kenh thi dong bo kenh
    # -> dang lech map thi di dong bo map di, may lay kenh lam lon gi luc do".
    #
    # MOI MAP MOT DANH SACH KENH KHAC NHAU, nen luc con lech map thi moi con so kenh deu vo nghia:
    # so `current_channel` cua acc o map khac la kenh CUA MAP DO, va bang suc chua no dang giu cung
    # la cua map do. Chot dich bang nhung so ay = chot mot kenh khong ton tai o map se toi.
    #
    # Ca that party 3, 14/09 (user: "thay chot kenh 7, ma tuong duong deo co kenh 7"):
    #   15:50:31 [party 3] gen 3: con lech map [12001, 21001] ... baybay@12001/k2 minh@12001/k7
    #   15:50:35 [party 3] party lech kenh {1:1, 2:1, 3:1, 7:2} -> CHOT kenh dich = 7
    # Cua cu chi chay khi `kh is not None`; `kh` rong la lot thang xuong.
    _maps_now = {int(getattr(c, "current_map", 0) or 0) for _u, c in song
                 if getattr(c, "current_map", None)}
    if len(_maps_now) > 1:
        # XOA DICH CU, khong phai giu lai. Ban dau cho nay `return st.get("kenh_dich")` - dieu phoi
        # thoi khong chot dich MOI, nhung dich CU van nam do va acc van doc duoc no roi tu chuyen.
        # Dich cu la kenh cua MAP CU -> o map moi no khong ton tai.
        #
        # Ca that party 4, 14/09 (user: "chua gom map da gom kenh"):
        #   16:49:53 [party 4] gen 10: viec=gom - party dang o 2 MAP khac nhau [12061, 21001]
        #   16:50:00 [thmo] (LEADER) DIEU PHOI chot kenh 5, minh dang o 2 -> tu chuyen
        #   16:50:00 [thmo] Doi kenh 5 THAT BAI: khong co khu do de doi (result=2)
        # Kenh 5 la dich chot hoi ca party con o 12001. Sang 21001 thi khong co kenh do.
        _cu = st.get("kenh_dich")
        if _cu:
            with st["lock"]:
                st["kenh_dich"] = None
                st["kenh_dich_luc"] = 0.0
            log.info("[party %d] DIEU PHOI: XOA kenh dich %s - party con o %d MAP khac nhau %s "
                     "(dich cu la kenh cua map cu; dong bo map truoc roi chot lai)",
                     pidx + 1, _cu, len(_maps_now), sorted(_maps_now))
        elif time.time() - float(st.get("chot_kenh_lech_map_log", 0.0) or 0.0) > 60.0:
            st["chot_kenh_lech_map_log"] = time.time()
            log.info("[party %d] DIEU PHOI: chua dung toi kenh - party con o %d MAP khac nhau %s "
                     "(moi map mot danh sach kenh; dong bo map truoc)",
                     pidx + 1, len(_maps_now), sorted(_maps_now))
        return None
    dem = {}
    map_chung = None
    for _u, c in song:
        m = getattr(c, "current_map", None)
        ch = getattr(c, "current_channel", None)
        if m is None or not ch:
            # IM LANG O DAY la cach party 9 ket 7 phut: dieu phoi ket luan 'dong_bo' moi nhip
            # nhung khong bao gio chot duoc dich, nen khong co lenh nao duoc gui.
            # (Bac map o `_dieu_phoi_quyet` gio da giu lai truong hop nay, day chi la luoi do.)
            if time.time() - float(st.get("chot_kenh_thieu_log", 0.0) or 0.0) > 60.0:
                st["chot_kenh_thieu_log"] = time.time()
                log.info("[party %d] DIEU PHOI: chua chot duoc kenh dich - %s chua biet %s",
                         pidx + 1, getattr(c, "_label", _u),
                         "map" if m is None else "kenh")
            return None
        if map_chung is None:
            map_chung = int(m)
        elif int(m) != map_chung:
            if time.time() - float(st.get("chot_kenh_thieu_log", 0.0) or 0.0) > 60.0:
                st["chot_kenh_thieu_log"] = time.time()
                log.info("[party %d] DIEU PHOI: chua chot kenh dich - con lech map (%s o %s, "
                         "nguoi khac o %s)", pidx + 1, getattr(c, "_label", _u), m, map_chung)
            return None                      # khac map thi so kenh vo nghia
        dem[int(ch)] = dem.get(int(ch), 0) + 1
    # PHAN KHU MANUAL la lenh cua user, uu tien tuyet doi. Truoc day command doi sang 3/7 xong
    # thi coordinator lap tuc tu chon "kenh it nguoi" va keo ca team nguoc ve 1/7, tao ping-pong
    # 30-180 giay. Khi da chon manual, bo hoan toan thuat toan auto cho toi khi user chon lai.
    _manual = int(st.get("train_channel_manual") or 0)
    if _manual > 0:
        if len(dem) == 1 and _manual in dem:
            with st["lock"]:
                st["kenh_dich"] = None
                st["kenh_dich_luc"] = 0.0
            return None
        with st["lock"]:
            if int(st.get("kenh_dich") or 0) != _manual:
                st["kenh_dich_luc"] = time.time()
            st["kenh_dich"] = _manual
        log.info("[party %d] DIEU PHOI: KHOA phan khu manual %d, hien tai %s -> khong tu chon khu khac",
                 pidx + 1, _manual, dict(sorted(dem.items())))
        return _manual
    if len(dem) <= 1:
        with st["lock"]:
            st["kenh_dich"] = None           # dang chung kenh -> khong co viec gi
            st["kenh_dich_luc"] = 0.0
        # DA CHUNG KENH MA DOI KHONG CON DU -> LAP LAI PARTY.
        # Server CAM doi kenh khi dang trong doi (`result=3`) nen member buoc phai ROI DOI moi doi
        # duoc kenh - do la luat game, member lam dung. Ra lenh doi kenh ma khong lam not phan hai
        # la loi cua NGUOI RA LENH: party 5 (06/09) tan doi luc 16:34:14 vi lenh doi kenh, roi
        # leader qua cong len tang MOT MINH luc 16:34:53.
        # KHONG dung co "no": doc THANG roster that (`joined_member_count`) la biet, va biet ca
        # khi doi tan vi ly do khac.
        # CO COOLDOWN (L6 + L7). Ham nay chay MOI 2 GIAY, ma MOI lan `_bump_reform` la ABORT moi
        # acc dang di duong (`_ab()`). Bump lien tuc = huy chinh viec lap party ma minh vua ra
        # lenh -> ca party dung im o thanh vinh vien.
        # Log that (06/09, 18:00-23:08): p28 `REFORM gen -> 8541`, p39 -> 8614, p23 -> 8035 - tam
        # NGHIN lan bump trong 5 tieng, "rat nhieu party ket o thanh ma khong di danh".
        # Chinh toi viet nhanh nay sang nay roi quen dat han - dung cai L6/L7 vua viet ra de cam.
        # DEM DOI BANG ROSTER SERVER (L2d), khong bang `joined_member_count` - so nho cua bot om
        # stale: party tan ma khong ai unmark thi no van bao du, va the la KHONG AI ra lenh lap lai
        # (ca that 07/09 party 1: 44 phut, leader lap lai "KHONG o party nao" moi phut).
        # BEN QUYET DA NOI GI THI NGHE NAY, khong tu ket luan lai (L1: mot party mot ket luan).
        #
        # Ham nay chup `song` rieng va TU di kiem map lai. Hai anh chup lech nhau vai tram ms la
        # ra hai ket luan trai nguoc - trong CUNG MOT GIAY:
        #   23:45:34 [party 6] gen 12: viec=moi ...
        #   23:45:46 [party 6] DIEU PHOI: ca party da chung kenh 1 nhung DOI chua du -> LAP LAI
        #   23:46:34 [party 6] gen 15: viec=gom - party dang o 3 MAP khac nhau [12001,23001,23811]
        # Ben quyet biet thua la dang lech map ("con lech map ... -> chua lap party, cho gom xong")
        # con ben nay thi "chung kenh roi" -> bump reform -> ABORT chinh vong gom vua ra lenh.
        # p6/p7 dem 10/09 quay nhu vay tu 23:43 den 23:47: gom -> lap lai -> lech map -> gom lai,
        # roster khong bao gio qua noi 4/5 (`taot001=4 ... taot005=1` = dang moi DO DANG thi bi cat).
        #
        # `VIEC_MOI` la lenh DUY NHAT co nghia "da cung map/kenh, gio lap doi di". Moi viec khac
        # (`gom` / `dong_bo` / `lam`) deu co nghia CHUA den luot lap party.
        _viec = (kh or {}).get("viec")
        if kh is not None and _viec != VIEC_MOI:
            log.debug("[party %d] chot kenh: dieu phoi dang bao viec=%s -> chua den luot lap "
                      "party, khong bump", pidx + 1, _viec)
            return None
        # DANG LAP DO DANG -> DE NO LAP NOT. Xem `_doi_dang_lap`: dap party dang hinh thanh la
        # cach chac chan nhat de no khong bao gio du.
        if _thieu_doi(pidx, song) and _doi_dang_lap(st, song):
            log.info("[party %d] DIEU PHOI: doi chua du (%s) nhung roster DANG LEN -> de no lap "
                     "not, khong dap di lap lai", pidx + 1, _tinh_hinh_doi(song))
            return None
        # CAC BAN SAO ROSTER CHUA NHAT TRI -> goi `0x0d` dang lan, chua doc duoc gi. Ra lenh luc
        # nay la pha dung cai doi vua hinh thanh (party 17, 14/09) - xem `_roster_dang_lan`.
        if _thieu_doi(pidx, song) and _roster_dang_lan(st, song):
            log.info("[party %d] DIEU PHOI: doi chua du (%s) nhung co acc DA thay du - goi roster "
                     "dang lan, CHO no nhat tri roi moi ket luan", pidx + 1, _tinh_hinh_doi(song))
            return None
        # THIEU NGUOI VI CO ACC DANG LAM VIEC VAT -> KHONG PHA DOI.
        #
        # Acc dang viec vat GIU loi moi lai, xong viec thi nhan (`account_task.__exit__`). Tuc doi
        # se tu du - chi cham vai giay. Bump reform luc nay la `leave_party()` ca party dang lanh
        # de "lap lai", trong khi thu duy nhat con thieu la mot dua sap xong viec vat.
        #
        # DIEU PHOI VAN RA LENH MOI nhu thuong (leader cu moi, loi moi duoc giu lai) - chi KHONG
        # bump reform. Lan truoc t cho dieu phoi NGUNG RA LENH khi co acc ban: viec vat thi acc nao
        # cung lam lien tuc nen luc nao cung co nguoi ban -> khong bao gio ra lenh -> ca party dung
        # o thanh voi roster 0/4 (14/09, 204 lan: "bon no lai ket o thanh deo di dau kia").
        #
        # Dieu phoi DOC THANG bao cao pha, khong doi acc xin phep - y het doc `current_map`.
        _ban = _ai_dang_lam_viec_le(song)
        if _thieu_doi(pidx, song) and _ban:
            if time.time() - float(st.get("viec_vat_log", 0.0) or 0.0) > 60.0:
                st["viec_vat_log"] = time.time()
                log.info("[party %d] DIEU PHOI: doi chua du (%s) nhung %s dang lam VIEC VAT - loi "
                         "moi da duoc giu lai, xong viec se vao. KHONG bump reform (pha doi dang "
                         "lanh de cho mot dua sap xong)", pidx + 1, _tinh_hinh_doi(song),
                         sorted(_ban))
            return None
        if _thieu_doi(pidx, song) and not _dang_doi_kenh(song):
            with st["lock"]:
                _luc = float(st.get("lap_party_luc", 0.0) or 0.0)
                _con_som = time.time() - _luc < LAP_LAI_PARTY_COOLDOWN
                if not _con_som:
                    st["lap_party_luc"] = time.time()
                    _bump_reform(st, "chung kenh roi ma doi khong du -> lap lai party")
            if not _con_som:
                log.info("[party %d] DIEU PHOI: ca party da chung kenh %s nhung DOI chua du (%s) "
                         "-> LAP LAI PARTY", pidx + 1, next(iter(dem), "?"),
                         _tinh_hinh_doi(song))
        return None
    # DA CHUNG KENH thi tren kia da xoa dich va tra ve roi - guard duoi day CHI danh cho
    # truong hop party CON LECH kenh. Dat no o TREN la sai: acc bi tu choi doi kenh se thu
    # lai lien tuc, tuc LUC NAO CUNG "dang doi kenh", nen ham thoat o guard va KHONG BAO
    # GIO chay toi cho xoa dich - ca party da ve chung kenh 1 tu lau ma van bam dich 27.
    # (user 08/09: "m ko thay ca lu da cung kenh 1 roi a, tim kenh khac lam cai lon gi nua")
    # DANG CO ACC BAY GIUA HAI KENH -> GIU NGUYEN DICH, khong tinh lai gi het. `current_channel`
    # cua acc do la so DO DANG (da roi doi, chua vao kenh moi) nen moi phep dem duoi day deu la
    # RAC: no vua co the ve "chung kenh" gia (-> xoa `kenh_dich`, mat luon han 45s), vua co the ve
    # mot phan bo lech khac (-> chot dich moi). Ra lenh moi trong khi lenh cu chua thi hanh xong,
    # va MOI lenh bat leader `leave_party()` them mot lan.
    #
    # Ca that 08/09 party 1 (user: "leader deo tap trung") - BA dich trong TAM giay:
    #   00:42:40  party lech kenh {5: 2, 7: 1} -> CHOT kenh dich = 5
    #   00:42:46  party lech kenh {5: 2, 7: 3} -> CHOT kenh dich = 7
    #   00:42:48  party lech kenh {5: 2, 7: 3} -> CHOT kenh dich = 3
    # Hai dong cuoi CUNG mot phan bo ma hai ket luan trai nguoc. Cung luc do ban quyet viec DA biet
    # phai dung ("gen 3 ... co acc dang doi kenh -> cho roster on dinh roi moi quyet") - chi rieng
    # ham chot kenh khong ai bao.
    if song and _dang_doi_kenh(song):
        _cu = st.get("kenh_dich")
        # ... TRU KHI chinh kenh dich do bi server bao DAY (ma 4). Acc khong vao duoc thi no retry
        # lien tuc, va retry lien tuc = LUC NAO CUNG "dang doi kenh" -> giu dich vinh vien. Day la
        # loi cua chinh guard nay khi moi them (08/09): party 3 ket 6 phut ruoi khong mot lenh moi
        #   04:21:11 [party 3] kenh dich 27 qua han nhung DA GOM DUOC 3/5 -> GIA HAN  <- lenh cuoi
        #   04:27:04 [batbat]  Doi kenh 27 THAT BAI: khu da day nguoi (result=4)
        #   04:27:14 [hoathap] Doi kenh 27 THAT BAI: khu da day nguoi (result=4)
        # User: "neu thay co dua ko ve duoc kenh do kenh day -> chon lai kenh di".
        if _cu and int(_cu) not in _doc_ket_qua_doi_kenh(song)[0]:
            return int(_cu)
        # CHUA CO DICH thi KHONG CO GI DE GIU - phai chot, khong duoc thoat.
        #
        # Guard nay sinh ra de "dang co acc bay giua hai kenh -> giu nguyen dich, khong tinh lai".
        # No chi co nghia khi DA CO dich. Chua co ma van thoat thi khong bao gio chot noi dich DAU
        # TIEN, tuc khong mot lenh doi kenh nao duoc gui - party lech kenh vinh vien.
        #
        # Ca that party 2, 13/09 (user: "p2 van ko dong bo kenh"):
        #   15:18:25 [party 2] gen 2: pha=event map=12932 kenh=None viec=dong_bo
        #                      - cung map nhung LECH KENH [1, 2] -> gom kenh truoc khi moi
        #   15:19:27 / 15:20:28 / 15:21:58 ... van 'dong_bo' ... CHO them Ns
        # sga001-004 o kenh 1, sga006 o kenh 2, CUNG map 12932, cung toa do (510,330). Khong mot
        # dong `CHOT kenh dich` nao, cung khong mot dong `Chuyen kenh ->` nao.
        #
        # So kenh cua acc dang bay co the la rac, nhung chot mot dich con hon treo: dich co han
        # (`kenh_dich_luc`) va duoc chot lai khi qua han hoac khi kenh do bao DAY.
    # LOAN DAU THI KHONG RA LENH DOI KENH. Loan dau la SOLO - khong lap party, khong can cung
    # kenh - nen viec gom kenh o day khong duoc gi ma MAT TAT CA: cho xep hang ghep tran nam o
    # kenh acc DA DANG KY, doi kenh la mat cho, roi acc dung cho toi het gio ma khong mot tran nao.
    #
    # Ca that 08/09 (user: "co acc vao roi ma ko dang ky danh") - sga003 = haba:
    #   20:23:14 [haba] Loan dau: da dang ky, cho ghep tran (thang=0)   <- dang ky o KENH 4
    #   20:23:19 [haba] Doi kenh OK -> 2                                <- dieu phoi keo sang kenh 2
    #   20:38:14 [haba] Loan dau: cho ghep tran qua 900s khong vao -> dung
    # Ba dong do cach nhau 15 phut - mat tron mot luot loan dau.
    # (Guard LOAN DAU da chuyen len DAU HAM - phai chan TRUOC ca nhanh "lap lai party", xem o do.)
    # (XOA 13/09 cua "TRONG THAP 2K THI KHONG RA LENH DOI KENH".)
    #
    # Cua do xoa luon `kenh_dich` va tra None cho MOI truong hop dang o trong thap. Nhung toi day
    # thi `len(dem) > 1` roi - tuc party DANG LECH KENH that. Xoa dich luc do = ben quyet bao
    # "dong bo kenh" con ben nay bao "khong co dich", lenh ra ma khong ai thi hanh duoc (L1: mot
    # party mot ket luan).
    #
    # Ca that party 52, 13/09 (user: "biet khac kenh roi ma van deo xu ly duoc"):
    #   15:41:04 [party 52] gen 3: pha=event map=12922 viec=dong_bo - cung map nhung LECH KENH
    #                       [1, 2] -> gom kenh truoc khi moi
    #   15:42:07 / 15:43:07 / 15:44:09 / 15:45:11 / 15:46:11 / 15:47:12  lap mai, khong mot dong
    #   `CHOT kenh dich` nao.
    #
    # LUAT CHUNG, KHONG CO NGOAI LE (user 13/09: "logic co ban o moi noi: lech map thi dong bo
    # map, lech kenh thi dong bo kenh, roi den lap party"). Con "cung kenh roi ma khac TANG" thi
    # da duoc lo o nhanh `len(dem) <= 1` ben tren (gom tang), khong dinh gi toi day.
    # DU PARTY ROI -> KHONG DUNG VAO KENH NUA (user chot 07/09: "du pt va di danh roi van di doi
    # kenh tiep, m co can code ngu the ko").
    #
    # DU PARTY = DA CUNG INSTANCE. Server khong cho o chung doi ma khac phan khu, nen chinh cai
    # roster day la bang chung manh hon moi con so bot tu nho - `current_channel` thi SAI duoc (sot
    # lai qua reconnect, ack cu; user kiem chung 30/08: bot hien ca 5 nick kenh 12, vao game xem la
    # 12/12/12/2/1), va do moi la thu khien dieu phoi tuong la "lech".
    #
    # Doi kenh luc nay khong duoc gi ma mat tat ca: `switch_channel` phai ROI DOI truoc (luat
    # `Team.IsAlone`), tuc TU TAY pha cai party vua gom xong; con doi scene giua tran thi dinh
    # `S:000-000` ma 47 (`戰鬥未結束事件先結束`).
    #
    # VONG LAP THAT 07/09 party 52 (user: "p52 nay gio co di train dc ko" - 38 phut duoc 14 tran,
    # quay qua lai bai train 12831 <-> Cu Loc 12011 moi 1-2 phut):
    #   23:37:27 DOI chua du (qv804=0 qv808=4 qv809=4 qv810=4 qv811=4)
    # `qv804` la LEADER va roster cua no = 0 vi no vua `leave_party()` DE DOI KENH; bon member con
    # thay `4` chi vi chua kip nhan goi cap nhat - doi truong roi la server giai tan CA DOI, nen so
    # 4 do la so CU. Ket qua: `_thieu_doi` bao thieu -> VIEC_MOI -> lap lai party -> chot kenh ->
    # roi doi -> ... gen 130 -> 137 trong hai phut, party bi da qua lai thanh <-> bai.
    # DANG CO DOI (du la do dang) -> THOI DOI KENH.
    #
    # Doi kenh BAT BUOC phai roi doi truoc (luat `Team.IsAlone`), ma doi truong roi la SERVER GIAI
    # TAN CA DOI. Nen ra lenh doi kenh luc dang co doi = tu tay pha cai vua gom. Chi duoc doi kenh
    # khi CHUA AI o trong doi nao - dung thu tu: gom kenh TRUOC, moi party SAU.
    # ...NHUNG CHI KHI DOI DA DU. Party DO DANG (vai nguoi vao duoc, may nguoi ket kenh khac) thi
    # no la PARTY HONG - L0 bat phai gom lai BANG DUOC, khong duoc lay "co doi" lam co de dung im.
    #
    # Ca that 09/09 party 1 (user: "p1"): ca 5 acc o map DG 49942 nhung MOI DUA MOT KENH; chihao +
    # sieugaaa vao duoc doi (roster 1) -> co `_co_party` bat -> dieu phoi IM 35 PHUT:
    #   01:45:51 [party 1] DANG CO DOI (brubb46677=0 chihao188=1 minhminhmq=0 sieugaaa=1
    #                      tuyetdo=0) -> thoi lenh doi kenh (kenh_dich 52)
    #   ... khong mot dong DIEU PHOI nao nua ...
    #   02:20:51 [xGAx] (LEADER) chua moi 3 member vi chua xac nhan live dung map/kenh:
    #                   ['c853d3f8:lech kenh live 52!=12', '38d0d2f8:lech kenh live 44!=12', ...]
    # Doi 2/5 khong danh duoc gi, ma giu no thi ba acc kia ket vinh vien. Thu tu dung van la cai
    # comment tren da ghi: gom kenh TRUOC, moi party SAU - nen party do dang phai giai tan de gom.
    # `_dang_doi_kenh` la cho phan biet HAI ca nhin giong nhau:
    #   - party 52 (07/09): leader VUA `leave_party()` DE DOI KENH -> roster no = 0 trong vai giay,
    #     `_thieu_doi` bao thieu. Do la HAU QUA cua lenh minh vua ra, khong phai party hong ->
    #     phai im (day chinh la ca sinh ra guard nay).
    #   - party 1 (09/09): 35 phut khong ai doi kenh, doi ket 2/5 -> party hong that -> phai gom.
    _co_party = any(len(getattr(_c, "party_members", None) or ()) > 0 for _u, _c in song)
    # DANG THI HANH thi GIU LENH, khong phai VUT LENH.
    #
    # Hai ca duoi day can hai hanh dong KHAC HAN, ban cu tra ve cung mot thu (`kenh_dich = None`):
    #   - doi DA DU        -> het viec that -> XOA dich.
    #   - dang doi kenh    -> lenh dang CHAY -> phai GIU dich, chi la khong quyet lai luc nay.
    # Xoa dich o ca thu hai = bo lenh giua chung. Nhip sau acc do bay xong, `_dang_doi_kenh` tat,
    # `cu` da thanh None nen cua "DA CHOT ROI THI GIU" ben duoi khong con gi de giu -> chot lai tu
    # dau bang phan bo VUA DOI -> ra dich khac -> ca party quay dau -> lap lai.
    # Ca that party 24, 00:21:26 -> 00:22:38 (72 giay, MUOI lenh doi kenh):
    #   {1:1, 2:4}        -> CHOT 5      {2:2, 4:1, 5:2} -> CHOT 4
    #   {2:2, 5:3}        -> CHOT 4      {2:2, 4:1, 5:2} -> CHOT 5
    #   {2:2, 4:3}        -> CHOT 5      {2:2, 4:1, 5:2} -> CHOT 7
    #   {2:2, 4:3}        -> CHOT 3      {2:2, 5:3}      -> CHOT 5 ...
    # Moi lenh bat acc `leave_party()` roi nhay kenh, nen party 5 dua dang DU bi xe ra va khong lan
    # nao gom xong. Sau do no dung chet 40 phut - "5/5 acc DUNG HINH" chi la HAU QUA cuoi chuoi.
    # (user 11/09: "truoc do da du party 5 dua va dang danh roi, sau do tu nhien dieu phoi lam cai
    #  gi ma no hong pt")
    if _co_party and _thieu_doi(pidx, song) and _dang_doi_kenh(song) and st.get("kenh_dich"):
        log.debug("[party %d] chot kenh: dang thi hanh lenh doi kenh -> GIU dich %s, khong quyet "
                  "lai (%s)", pidx + 1, st.get("kenh_dich"), _tinh_hinh_doi(song))
        return int(st["kenh_dich"])
    if _co_party and (not _thieu_doi(pidx, song) or _dang_doi_kenh(song)):
        with st["lock"]:
            if st.get("kenh_dich"):
                log.info("[party %d] DIEU PHOI: DA DU DOI (%s) -> thoi lenh doi kenh "
                         "(kenh_dich %s)", pidx + 1, _tinh_hinh_doi(song), st.get("kenh_dich"))
            st["kenh_dich"] = None
            st["kenh_dich_luc"] = 0.0
        return None
    # VUA HOI DANH SACH KENH -> CHO GOI VE ROI HAY CHOT. `request_channel_list` XOA `c.channels`
    # va goi `S:007-001` mat vai giay moi ve; chot trong khoang do la chot bang bang RONG hoac bang
    # cu, tuc chot bua.
    #
    # User chi ra 08/09 (party 8): "thay bot yeu cau danh sach kenh, nhung sau do deo thay chot lai
    # kenh" -> "danh sach kenh ve cham, phai cho may giay, ma may di nhanh qua truoc khi danh sach
    # ve". Log: leader xin danh sach 91 lan trong 4 phut ma dich khong doi.
    if song and any(
            float(getattr(_c, "_ds_kenh_hoi_luc", 0.0) or 0.0)
            > float(getattr(_c, "_ds_kenh_nhan_luc", 0.0) or 0.0)
            and time.time() - float(getattr(_c, "_ds_kenh_hoi_luc", 0.0) or 0.0) < DS_KENH_CHO_SEC
            for _u, _c in song):
        _cu2 = st.get("kenh_dich")
        return int(_cu2) if _cu2 else None
    hong, _ma3, _ma2 = _doc_ket_qua_doi_kenh(song)
    if hong:
        # Server bao DAY tuc bang `c.channels` dang SAI (no noi kenh 27 con 12 cho, server tu choi).
        # Ep hoi lai `S:007-001` ngay thay vi doi het `DS_KENH_LAM_MOI_SEC` - chot tiep bang so cu
        # thi rat de lai chon dung mot kenh da day.
        st["ds_kenh_luc"] = 0.0
    if _ma3:
        # MA 3 <組隊不可換分區>: server noi acc DANG TO DOI nen khong doi kenh duoc. Mot acc tu roi
        # party roi thu lai la VO ICH - party la cua CA LU, con ai con trong doi thi server van coi
        # la dang to doi. Log 31/08 party 7 (14:45-14:55): leader tu xoay mot minh -> 67 luot ma 3,
        # quet het kenh 2,3,4,... trong 10 phut ma khong thoat.
        #
        # Truoc 07/09 chinh ACC gap ma 3 tu `_bump_reform`. Gio acc khong ra lenh cap party nua
        # (L1) - dieu phoi DOC THANG `_chan_switch_result` cua tung client (`_doc_ket_qua_doi_kenh`)
        # nen no thay ma 3 khong kem gi acc, va no la cho DUY NHAT duoc quyet.
        with st["lock"]:
            _luc3 = float(st.get("ma3_gom_luc", 0.0) or 0.0)
            _som3 = time.time() - _luc3 < LAP_LAI_PARTY_COOLDOWN
            if not _som3:
                st["ma3_gom_luc"] = time.time()
                _bump_reform(st, "co acc bi ma 3 (dang to doi) -> ca party roi doi + dong bo lai")
        if not _som3:
            log.warning("[party %d] DIEU PHOI: server chan doi kenh vi DANG TO DOI (ma 3) -> ra "
                        "lenh CA PARTY roi doi roi dong bo lai", pidx + 1)
        return None
    if _ma2:
        # `result=2` <沒有該分區> = dang trong INSTANCE cua event (thap 2K): khong co khai niem
        # "khu" de doi. Ra lenh doi kenh o day la ra lenh SAI LOAI - muon cung instance thi phai
        # DI CUNG NHAU QUA CONG. Party 5 (06/09) dinh dung cho nay luc 16:34:04.
        with st["lock"]:
            st["kenh_dich"] = None
            st["kenh_dich_luc"] = 0.0
        log.info("[party %d] DIEU PHOI: server bao KHONG CO KHU do (ma 2) -> dang trong instance "
                 "event, doi kenh vo nghia -> khong ra lenh doi kenh nua", pidx + 1)
        return None
    # DA CHOT ROI THI GIU - de nguoi ta THI HANH XONG cai lenh vua ra.
    #
    # Ham nay chay MOI 2 GIAY. Chot lai tu dau moi nhip = acc vua bat dau chuyen sang kenh A thi
    # phan bo doi -> chot kenh B -> ca lu quay dau -> phan bo lai doi -> chot A... Dung kieu thrash
    # da chua cho `gom`/`reform` bang grace + cooldown, ma ham nay lai khong co.
    # Log that party 3 (06/09), 4 phut moi dong bo xong:
    #   15:38:28 {1: 1, 2: 1}       -> CHOT 1
    #   15:38:38 {1: 1, 2: 2}       -> CHOT 2      (doi y sau 10 giay)
    #   15:41:27 {1: 2, 2: 2, 4: 1} -> CHOT 1
    #   15:41:39 {1: 3, 2: 1, 4: 1} -> CHOT 2
    # Dich chi duoc doi khi: kenh do VAO SO DEN (day), hoac qua han ma van chua ai toi noi.
    cu = st.get("kenh_dich")
    # MOI KENH DEU DAY -> GIU DICH, CHO CHO TRONG. Khong lat qua lat lai.
    #
    # `hong` chua kenh vua bi server tra ma 4. Khi ca party dang o toan kenh day, MOI acc thu mot
    # kenh la kenh do vao `hong` -> dich cu luon bi loai -> chot lai -> kenh moi cung day -> ...
    # Moi lan doi dich la mot lan acc ROI DOI + doi kenh, tuc tu tay pha cai dang gom.
    #
    # Ca that 09/09 party 1 (user: "p1") - CUNG mot phan bo ma ba ket qua, moi 2 giay mot lan:
    #   01:44:47 {12: 1, 44: 1, 52: 2, 66: 1} -> CHOT 12
    #   01:44:49 {12: 1, 44: 1, 52: 2, 66: 1} -> CHOT 52
    #   01:44:51 {12: 2, 44: 1, 52: 2}        -> CHOT 52
    #   01:44:53 {12: 2, 44: 1, 52: 2}        -> CHOT 44
    # Ca nam acc deu `S:007-002 ma 4`. User chot: het kenh trong thi cu cho, dung nhay lung tung.
    if cu and dem and all(int(_ch) in hong for _ch in dem):
        with st["lock"]:
            st["kenh_dich_luc"] = time.time()      # gia han de khong roi vao nhanh "qua han"
        log.info("[party %d] DIEU PHOI: MOI kenh party dang dung deu DAY %s -> GIU kenh dich %s, "
                 "cho co cho trong (khong doi y)", pidx + 1, sorted(hong), cu)
        return int(cu)
    if cu and int(cu) not in hong:
        _luc = float(st.get("kenh_dich_luc", 0.0) or 0.0)
        if time.time() - _luc < KENH_DICH_KIEN_NHAN_SEC:
            return int(cu)                   # dang thi hanh -> GIU, dung doi y
        # QUA HAN NHUNG DANG TIEN TRIEN -> GIA HAN, dung chot lai. Chot lai luc gan xong la bat
        # ca party quay dau: log 07/09 party 1 luc 20:40:06 - `kenh dich 45 qua 45s van chua gom
        # xong ({6: 1, 45: 4})` - 4/5 acc DA sang 45, chi con mot dua, the ma no doi dich.
        if int(dem.get(int(cu), 0)) > len(song) // 2:
            with st["lock"]:
                st["kenh_dich_luc"] = time.time()
            log.info("[party %d] DIEU PHOI: kenh dich %s qua han nhung DA GOM DUOC %d/%d -> GIA "
                     "HAN, khong doi dich (%s)", pidx + 1, cu, int(dem.get(int(cu), 0)), len(song),
                     dict(sorted(dem.items())))
            return int(cu)
        log.info("[party %d] DIEU PHOI: kenh dich %s qua %.0fs van chua gom xong (%s) -> chot lai",
                 pidx + 1, cu, KENH_DICH_KIEN_NHAN_SEC, dict(sorted(dem.items())))
    # CHON KENH PHAI XET CON CHO. Ban cu chi lay "kenh dong nguoi nhat trong party" roi moi phat
    # hien no DAY (server tra ma 4) -> kenh do vao `hong` -> chot lai kenh khac -> lai day -> ...
    # Log that 40NPC (07/09), CUNG mot phan bo ma chot ba kenh khac nhau trong 12 giay:
    #   20:24:38 {5: 3, 12: 1, 13: 1} -> CHOT 13
    #   20:24:40 {5: 3, 12: 1, 13: 1} -> CHOT 5
    #   20:24:46 {5: 3, 12: 1, 13: 1} -> CHOT 13
    #   20:24:50 {5: 3, 12: 1, 13: 1} -> CHOT 12
    # Moi lan doi y la ca party quay dau -> khong bao gio gom xong (user: "phai gom bang duoc kenh
    # chu"). Va `_kenh_trong_cho_ca_party` - dung cai ham BIET kenh nao con cho - thi truoc day chi
    # chay khi MOI kenh party dang dung deu hong, tuc gan nhu khong bao gio (user: "sao cai tim
    # kenh it nguoi nhat chi chay 1 lan").
    _can = len(song)
    # LUAT CHON KENH (user chot 07/09):
    #   1. Kenh IT NGUOI NHAT ma DU CHO ca team  -> sang do.
    #   2. Khong kenh nao du cho                 -> kenh dang co NHIEU MEMBER NHAT.
    #
    # Ban cu bo qua han buoc 1: no chi lay "kenh dong member nhat" roi moi phat hien kenh do DAY
    # (server tra ma 4) -> vao `hong` -> chot lai -> lai day -> ... Log 40NPC (07/09), CUNG mot
    # phan bo ma chot ba kenh khac nhau trong 12 giay:
    #   20:24:38 {5: 3, 12: 1, 13: 1} -> CHOT 13
    #   20:24:40 {5: 3, 12: 1, 13: 1} -> CHOT 5
    #   20:24:46 {5: 3, 12: 1, 13: 1} -> CHOT 13
    #   20:24:50 {5: 3, 12: 1, 13: 1} -> CHOT 12
    # Moi lan doi y la ca party quay dau -> khong bao gio gom xong.
    #
    # `c.channels` truoc day chi nap MOT LAN trong `pick_best_channel` roi khong ai cap nhat, ma so
    # nguoi/kenh thi doi lien tuc -> phai hoi lai (cach quang, tranh spam `0x07 0100` -> ma 13).
    _lam_moi_ds_kenh(pidx, st, song)
    # CHI LAY DANH SACH KENH CUA MAP DANG DUNG (`map_chung` - toi day thi ca party da cung map,
    # vong dem o tren da `return None` neu con lech). Moi map mot danh sach kenh khac nhau.
    _bang = _bang_kenh(song, map_id=map_chung)     # {kenh: (dang_o, con_trong)}
    def _con_cho(ch):
        _c = _bang.get(int(ch))
        return None if _c is None else _c[1]      # None = chua biet suc chua

    # (a) KENH IT NGUOI NHAT MA DU CHO CA TEAM. Day la buoc DAU TIEN, khong phai duong lui.
    #
    #     User chot 07/09 va nhac lai 08/09: "phai tim kenh it nguoi nhat truoc chu, ko co kenh nao
    #     du cho ca team thi moi chon kenh nhieu member nhat".
    #
    #     TUNG co mot buoc dung TRUOC buoc nay - "uu tien kenh party dang dung cho do di lai" - do
    #     la thu TOI TU THEM VAO, khong phai luat user ra, va no de ra dung cai no dinh tranh:
    #       06:57:48 [party 15] party lech kenh {3: 4, 4: 1} -> CHOT kenh dich = 3
    #       06:58:02 [party 15] party lech kenh {3: 4, 4: 1} -> CHOT kenh dich = 4
    #     Kenh 3 (4 acc) bi bao DAY nen vao so den, the la no chot kenh 4 - noi dung MOT acc le -
    #     roi bat 4 nguoi kia di theo mot kenh cung sap day.
    #
    #     Lo "nhay kenh lien tuc" khong phai chan bang cach uu tien kenh dang dung, ma bang HAN
    #     KIEN NHAN (`KENH_DICH_KIEN_NHAN_SEC` + gia han khi da gom duoc da so) o dau ham nay.
    _ung = [(dang, ch) for ch, (dang, con) in _bang.items()
            if ch not in hong and con >= _can]
    if not _ung and getattr(hong, "doan", None):
        # Het ung vien, ma mot phan so den chi la SUY DOAN tu timeout (khong phai server noi).
        # Bo phan doan ra roi tim lai - ton nhat la them mot lan doi kenh, con giu thi ca party
        # phai chot bua vao kenh dang DAY (party 2, 10/09). Xem `_SoDen`.
        _chac = hong.chac()
        _ung = [(dang, ch) for ch, (dang, con) in _bang.items()
                if ch not in _chac and con >= _can]
        if _ung:
            log.info("[party %d] DIEU PHOI: so den co %d kenh chi TIMEOUT (khong phai server bao "
                     "day) -> bo qua chung, tim lai duoc %d kenh du cho",
                     pidx + 1, len(hong.doan), len(_ung))
    if _ung:
        dich = min(_ung)[1]
        _vi_sao = "kenh IT NGUOI NHAT ma du cho ca team (con %d cho)" % (_bang[dich][1],)
    else:
        # (b) Khong kenh nao du cho ca team -> kenh dang co NHIEU MEMBER NHAT.
        #
        # `dem` dem `current_channel` cua tung acc - va so do KHONG PHAI LUC NAO CUNG LA KENH THE
        # GIOI: trong instance (Di Gioi, thap 2K, boss quan doan) no la instanceId. Chot vao mot
        # instanceId roi ra lenh doi kenh = server tra ma 2 <沒有該分區> cho ca party.
        #
        # Ca that 08/09 party 11 (user: "no bia kenh 27 o dau ra day") - 5 acc o map 49951, moi
        # dua mot so LIEN TIEP, hoa nhau nen bot lay so nho nhat:
        #   14:05:57 [party 11] party lech kenh {27: 1, 28: 1, 29: 1, 30: 1, 31: 1}
        #                       -> CHOT kenh dich = 27 (khong kenh nao du cho -> NHIEU MEMBER NHAT)
        #   14:09:44..14:10:08  ca NAM acc: "Doi kenh 27 THAT BAI: khong co khu do (result=2)"
        #
        # Nen chi chon so nao CO MAT trong danh sach kenh that (`S:007-001`). Bang rong (chua nhan
        # duoc goi) thi moi dung tam `dem` nhu cu - khong co gi khac de dua vao.
        # NOI RO VI SAO khong co ung vien - dung de nguoi doc log phai doan.
        #
        # Ca that 10/09 party 2 (user: "sao lai chot kenh 8 bi full trong khi rat nhieu kenh khac
        # trong"): bang co 57 kenh, ma moi nhip deu roi xuong nhanh nay. Nhin log cu KHONG the biet
        # 57 kenh do deu day that, hay bi so den nuot, hay suc chua doc sai - ba nguyen nhan khac
        # han nhau. Gio in thang: bao nhieu kenh trong bang, bao nhieu bi so den, va kenh RONG NHAT
        # con may cho.
        _rong_nhat = max((con for _ch, (_d, con) in _bang.items()), default=None)
        _bi_den = sum(1 for _ch in _bang if _ch in hong)
        log.info("[party %d] DIEU PHOI: khong kenh nao du %d cho - bang %d kenh, so den %d kenh, "
                 "kenh rong nhat con %s cho", pidx + 1, _can, len(_bang), _bi_den,
                 _rong_nhat if _rong_nhat is not None else "?")
        _co_that = [ch for ch in dem if ch in _bang]
        _nguon = {ch: _n for ch, _n in dem.items() if ch in _bang} if (_bang and _co_that) else dem
        xep = [ch for ch, _n in sorted(_nguon.items(), key=lambda kv: (-kv[1], kv[0]))
               if ch not in hong]
        dich = xep[0] if xep else _kenh_trong_cho_ca_party(pidx, st, song, hong)
        _vi_sao = "khong kenh nao du cho ca team -> lay kenh dang NHIEU MEMBER NHAT"
    if not dich:
        return None                          # moi kenh party dang dung deu day -> cho nhip sau
    with st["lock"]:
        st["kenh_dich"] = dich
        st["kenh_dich_luc"] = time.time()
    if cu != dich:
        log.info("[party %d] DIEU PHOI: party lech kenh %s -> CHOT kenh dich = %d (%s)",
                 pidx + 1, dict(sorted(dem.items())), dich, _vi_sao)
    return dich


# Ra lenh LAP LAI PARTY roi thi phai cho no lam xong. Moi `_bump_reform` la abort moi acc dang
# di duong, nen bump lien tuc = tu huy viec minh vua sai. Mot vong gom + moi lai mat vai chuc giay.
LAP_LAI_PARTY_COOLDOWN = 120.0
# `gom_dich` chi dang tin bay nhieu giay. Mot vong gom ve thanh + moi lai mat vai chuc giay; qua
# han ma van con dich = vong do da thoat bang duong khong xoa duoc dich (abort/timeout/bo cuoc).
GOM_DICH_HAN_SEC = 180.0

# Da chot kenh dich thi GIU bay lau truoc khi duoc chot lai. Doi kenh mat ~1 giay, nhung acc
# dang danh/dang di duong phai cho xong viec moi chuyen duoc -> cho rong rai.
KENH_DICH_KIEN_NHAN_SEC = 45.0

# Truoc khi len tang 2K: cho bay lau de moi lai cho du party. Doi kenh xong -> member phai
# accept loi moi; 4 dua thi vai giay. Cho rong rai vi ho co the dang doi kenh do.
DU_PARTY_TRUOC_CONG_SEC = 60.0

# Ma doi kenh cu hon the thi coi nhu da lac hau (nguoi ra vao lien tuc, kenh day roi lai trong).
KENH_MA_CON_MOI_SEC = 120.0


class _SoDen(set):
    """So den kenh, co phan biet BANG CHUNG CHAC voi SUY DOAN.

    Server tra ma 2 `<沒有該分區>` / ma 4 `<分區人數已滿>` la NOI RO. Con `-1` la bot tu dat khi
    `switch_channel` het luot ma server im lang - do la KHONG BIET, khong phai "kenh day".

    Ca that 10/09 party 2 (user: "sao lai chot kenh 8 bi full trong khi rat nhieu kenh khac trong"):
    hai duong cung gui lenh doi kenh cho mot acc -> server tra MOT ket qua, luong kia timeout ->
    `-1` -> kenh do vao so den. Timeout hang loat thi so den nuot sach ung vien, nhanh "kenh it
    nguoi nhat ma du cho ca team" khong con gi de chon -> roi xuong nhanh cuoi va chot bua vao mot
    trong ba kenh party dang dung, ca ba deu DAY.

    Nguyen tac: thong tin YEU chi duoc dung khi no khong tuoc mat lua chon. Het ung vien thi bo qua
    phan suy doan va thu lai - cung lam la ton them mot lan doi kenh.
    """

    def __new__(cls, chac_va_doan, doan):
        ra = super().__new__(cls, chac_va_doan)
        return ra

    def __init__(self, chac_va_doan, doan):
        super().__init__(chac_va_doan)
        self.doan = set(doan)

    def chac(self):
        """Chi cac kenh server NOI RO la khong vao duoc."""
        return {ch for ch in self if ch not in self.doan}


def _doc_ket_qua_doi_kenh(song):
    """DOC THANG ket qua doi kenh cua tung client - KHONG bat acc nao bao cao.

    Ca 5 acc nam trong MOT tien trinh, moi client tu giu `_chan_switch_result` (ma server tra),
    `_chan_switch_target` (nham kenh nao) va `_chan_switch_luc` (luc nao). Dieu phoi chi viec doc.

    Ban dau toi bat acc goi `bao_kenh_day()` de bao len - vi pham dung luat user chot 06/09:
    "bot la nguoi dieu phoi, deo phai cho dua nao bao cao". Bot BIET HET, khong co gi phai bao.

    Ma server (`client._on_channel_switch_result`): 0 OK · 1 trung khu dang o ·
    2 KHONG CO KHU DO · 3 DANG TO DOI · 4 KENH DAY.

    -> (kenh_day, co_ma3, co_ma2)
       kenh_day = cac kenh KHONG VAO DUOC (ma con moi) -> dung chot vao do nua
       co_ma3   = co acc bi chan vi dang to doi -> doi kenh se lam TAN DOI, xong phai lap lai
       co_ma2   = server bao khong co khu do -> dang trong INSTANCE event, doi kenh vo nghia

    `kenh_day` co them thuoc tinh `.doan` = tap con CHI DUA TREN TIMEOUT. Timeout la KHONG BIET,
    khong phai bang chung kenh day - xem `_hong_chac`.
    """
    bay = time.time()
    day, ma3, ma2 = set(), False, False
    doan, chac = set(), set()
    for _u, c in song:
        r = getattr(c, "_chan_switch_result", None)
        if r is None:
            continue
        if bay - float(getattr(c, "_chan_switch_luc", 0.0) or 0.0) > KENH_MA_CON_MOI_SEC:
            continue                      # ma cu roi (nguoi ra vao lien tuc) -> bo qua
        if r in (4, -1):
            (doan if r == -1 else chac).add(int(getattr(c, "_chan_switch_target", None) or 0))
            # 4 = kenh DAY. -1 = `switch_channel` het luot ma server IM LANG (timeout).
            #
            # Timeout truoc day bi BO QUA hoan toan o day: acc thu mai khong vao duoc, con dieu
            # phoi thi khong thay ma nao nen cu giu nguyen dich - "check ket qua ma khong dung"
            # (user 08/09). Ca hai deu la KHONG VAO DUOC kenh do, nen doi xu nhu nhau: cam chot
            # vao no. `hong` chi song `KENH_MA_CON_MOI_SEC` roi tu het han, nen neu timeout chi vi
            # mang chap chon thi vai phut sau van thu lai duoc.
            t = getattr(c, "_chan_switch_target", None)
            if t:
                day.add(int(t))
        elif r == 3:
            ma3 = True
        elif r == 2:
            ma2 = True
            # MA 2 <沒有該分區> noi thang: KENH DO KHONG TON TAI. Truoc day no chi bat mot co CHUNG
            # (`ma2`) con so kenh thi khong ai ghi lai -> nhip sau chot lai DUNG kenh do, ca party
            # lai dam dau vao. Cam chot vao no y het kenh DAY.
            #
            # Ca that 08/09 party 11 (user: "server da bao ko co kenh do roi ma van co") - ca NAM
            # acc cung mot ma, cung mot kenh:
            #   14:05:57 [party 11] party lech kenh {27: 1, 28: 1, 29: 1, 30: 1, 31: 1}
            #                       -> CHOT kenh dich = 27
            #   14:09:44 [luuchin] Doi kenh 27 THAT BAI: khong co khu do de doi (result=2)
            #   14:09:47 [luumuoi] Doi kenh 27 THAT BAI: khong co khu do de doi (result=2)
            #   14:09:48 [luubay]  Doi kenh 27 THAT BAI: khong co khu do de doi (result=2)
            #   14:09:48 [luutam]  Doi kenh 27 THAT BAI: khong co khu do de doi (result=2)
            #   14:10:08 [luusau]  Doi kenh 27 THAT BAI: khong co khu do de doi (result=2)
            t = getattr(c, "_chan_switch_target", None)
            if t:
                day.add(int(t))
                chac.add(int(t))
    # `doan` chi giu kenh KHONG co bang chung chac nao. Mot acc timeout ma acc khac nhan ma 4 thi
    # kenh do la CHAC - bang chung manh thang.
    return _SoDen(day, (doan - chac) - {0}), ma3, ma2


DS_KENH_LAM_MOI_SEC = 20.0   # khoang cach toi thieu giua hai lan hoi lai danh sach kenh
# Ban danh sach kenh cu hon the thi bo (nguoi ra vao lien tuc -> so cho khong con dung). Rong rai
# hon `DS_KENH_LAM_MOI_SEC` vai lan de luon con ban dung duoc giua hai lan hoi.
DS_KENH_QUA_CU_SEC = 120.0
DS_KENH_CHO_SEC = 5.0        # hoi xong thi cho toi ngan nay cho `S:007-001` ve roi moi chot kenh


def _lam_moi_ds_kenh(pidx, st, song):
    """Hoi lai `S:007-001 <分區列表>` cho MOT acc trong party (cach quang).

    Chi can mot acc hoi la ca party co du lieu (`_suc_chua_kenh` hop tu moi client). Hoi tung acc
    moi nhip la spam `0x07 0100` -> nguy co `ma 13` (gui goi qua nhanh)."""
    if time.time() - float(st.get("ds_kenh_luc", 0.0) or 0.0) < DS_KENH_LAM_MOI_SEC:
        return
    st["ds_kenh_luc"] = time.time()
    for _u, c in song:
        if not getattr(c, "running", False):
            continue
        try:
            if c.in_combat():          # dang danh -> de acc khac hoi
                continue
            c.request_channel_list()
        except Exception as e:
            log.debug("[%s] hoi danh sach kenh loi (bo qua): %s", _u, e)
        return


def _bang_kenh(song, map_id=None):
    """{kenh: (dang_o, con_trong)} - danh sach kenh CUA MAP `map_id`, hop tu cac client trong party.

    `c.channels` = {kenh: (dang_o, suc_chua)} tu `S:007-001 <分區列表>`. Moi acc co the giu mot ban
    lay o thoi diem khac nhau -> lay ban BI QUAN NHAT (it cho nhat) cho chac.

    BO BAN QUA CU: nguoi ra vao kenh lien tuc nen so cho cua ban vai phut truoc la vo nghia. Nhung
    "cu" khac han "rong" - bang cu vai chuc giay van chon duoc kenh, con bang rong thi bat dieu phoi
    chot bua (xem `request_channel_list`).

    BO BAN CUA MAP KHAC: MOI MAP MOT DANH SACH KENH. Ban cu gop bang cua moi acc bat ke no dang o
    dau, nen acc con o map cu dem theo kenh cua map cu vao bang - va dieu phoi co the chot vao mot
    so KHONG TON TAI o map dang dung.

    Ca that party 3, 14/09 (user: "thay chot kenh 7, ma tuong duong deo co kenh 7" -> "no deo phai
    kenh rac ma la kenh khi no o map khac, phai la khi ca pt cung map can lap pt thi moi lay danh
    sach kenh chu"):
        15:50:31 [party 3] con lech map [12001, 21001] ... baybay@12001/k2 minh@12001/k7
        15:50:35 [party 3] party lech kenh {1:1, 2:1, 3:1, 7:2} -> CHOT kenh dich = 7
    """
    ra = {}
    bay = time.time()
    for _u, c in song:
        if bay - float(getattr(c, "_ds_kenh_nhan_luc", 0.0) or 0.0) > DS_KENH_QUA_CU_SEC:
            continue
        if map_id is not None:
            _m = int(getattr(c, "_ds_kenh_map", 0) or 0)
            if _m and _m != int(map_id):
                continue        # bang nay lay o MAP KHAC -> kenh trong do khong dung cho map nay
        for ch, cap in (getattr(c, "channels", None) or {}).items():
            try:
                dang, toi_da = int(cap[0]), int(cap[1])
            except Exception:
                continue
            ch = int(ch)
            con = max(0, toi_da - dang)
            if ch not in ra or con < ra[ch][1]:
                ra[ch] = (dang, con)
    return ra


def _kenh_trong_cho_ca_party(pidx, st, song, hong):
    """Moi kenh party dang dung deu trong so den -> tim kenh MOI du cho CA party.

    Dung danh sach kenh acc nao do vua nhan duoc (`c.channels` = {ch: (dang, toi_da)}).
    Khong hoi lai server o day: dieu phoi chay moi 2 giay, hoi moi nhip la spam.
    """
    can = len(song)
    tot = None
    for _u, c in song:
        for ch, cap in (getattr(c, "channels", None) or {}).items():
            try:
                dang, toi_da = int(cap[0]), int(cap[1])
            except Exception:
                continue
            if int(ch) in hong or toi_da - dang < can:
                continue
            if tot is None or dang < tot[1]:
                tot = (int(ch), dang)
    if tot:
        log.info("[party %d] DIEU PHOI: moi kenh party dang dung deu DAY %s -> doi ca party sang "
                 "kenh trong %d (%d cho dang dung)", pidx + 1, sorted(hong), tot[0], tot[1])
        return tot[0]
    return None


def _dieu_phoi_chot_map(pidx, st):
    """DIEU PHOI chot cap quai DG / map train - KHONG de luong acc nao tu chot.

    Hai ham `_auto_*` tu tra None khi chua du level ca party, ma dieu phoi goi lai moi 2 giay,
    nen no chot NGAY GIAY dau tien du du lieu. Luong acc chi doc `st["auto_dg_level"]` /
    `st["auto_train"]`.
    """
    pcfg = getattr(config, "PARTY_CONFIG", {}).get(pidx, {})
    raw_mode = pcfg.get("mode")
    try:
        _dgp = pcfg.get("di_gioi_pick") or ""
        if _dgp in train_pick.PICK_KEYS and not st.get("auto_dg_level"):
            _auto_dg_level(pidx, _dgp)
        if (raw_mode in ("train", "digioi_train") and pcfg.get("train_pick")
                and not st.get("auto_train")):
            _auto_train_target(pidx, pcfg)
    except Exception as e:
        log.warning("[party %d] DIEU PHOI chot map loi (bo qua nhip nay): %s", pidx + 1, e)


# ===================== WATCHER: QUAN SAT VIEC CUA CA PARTY =====================
# Doc bao cao "dang lam gi" cua tung acc (bot/client.py: account_task/get_account_task) roi ket
# luan o MOT CHO, thay vi de moi vong cho tu phan doan.
WATCH_EVERY_SEC = 20        # nhip quet
WATCH_STUCK_AGE = 180       # acc khong cap nhat tien do qua lau -> nghi TREO
WATCH_MISMATCH_SEC = 300    # ca party lech viec lien tuc qua lau -> ep dong bo
WATCH_SOLO_MAX = 1800       # viec le chay qua 30' -> bat thuong, bao (khong tu ep)

_PHASE_SOLO = ("login_chore", "boss_qd")     # viec LE: dong doi phai CHO, khong phai lech viec
_PHASE_TEAM = ("train", "reform", "team_dungeon", "digioi")
_PHASE_WAIT = "wait"                        # dang CHO dong doi - KHONG phai treo (xem duoi)
WATCH_ALLWAIT_SEC = 120                     # CA PARTY cung cho qua lau = deadlock that
# Bao cao "dang cho" phai con TUOI thi moi tinh la CHO THAT. Acc cho that lam moi bao cao MOI VONG
# LAP (~1-2s: reform route, reform ve thanh, PB cho leader, PB cho report) -> tuoi luon ~1s.
# Bao cao GIA = acc DA di lam viec khac ma khong ai doi pha -> tuoi tang vo han.
WATCH_WAIT_FRESH_SEC = 15                   # bien rong gap ~7 lan nhip lam moi
# Party o map train ma THIEU NGUOI lien tuc qua lau -> ep dong bo. Doc THANG so nguoi da join,
# khong qua bao cao pha, nen khong bi cac la chan "co nguoi dang lam" che mat.
WATCH_THIEU_NGUOI_SEC = 300


def _party_watcher(pidx):
    """Luong quan sat 1 party. Chi DOC trang thai, khong tham gia vao luong lam viec."""
    st = _pstate(pidx)
    mismatch_t0 = None
    allwait_t0 = None
    thieu_t0 = None
    # CHO-HOP-LE: day la LUONG QUAN SAT, chi DOC trang thai - khong acc nao dang cho no.
    while True:
        time.sleep(WATCH_EVERY_SEC)
        accs = [u for u, _p, _l, _k in party_accounts(pidx)]
        if not accs or not any(is_account_running(u) for u in accs):
            return                                  # party da dung han -> thoat luong
        with st["lock"]:
            recon = set(st["reconnecting"])
        rows = []
        for u in accs:
            if u in recon or not is_account_running(u):
                continue
            d = get_account_task(u)
            rows.append((u, d))
        live = [(u, d) for u, d in rows if d]
        if not live:
            continue

        solo = [(u, d) for u, d in live if d["phase"] in _PHASE_SOLO]
        team = [(u, d) for u, d in live if d["phase"] in _PHASE_TEAM]
        waiting = [(u, d) for u, d in live if d["phase"] == _PHASE_WAIT]
        # CHI dung cho luat (1) DEADLOCK ben duoi. KHONG dung cho la chan (3) - xem chu thich o do.
        waiting_tuoi = [(u, d) for u, d in waiting if d["age"] <= WATCH_WAIT_FRESH_SEC]
        # TREO chi tinh cho acc dang LAM. Acc dang CHO dong doi thi im lau la BINH THUONG
        # (xong DG truoc -> doi ca party, co the 2 TIENG; cho leader danh PB 10-20 phut).
        stuck = [(u, d) for u, d in live
                 if d["age"] > WATCH_STUCK_AGE and d["phase"] != _PHASE_WAIT]

        # DANG DANH PHO BAN TO DOI thi "ca party cung cho" la BINH THUONG, KHONG phai deadlock:
        # member cho leader danh la DUNG THIET KE, va 1 luot PB (4 tran + nhan thuong) lau hon
        # WATCH_ALLWAIT_SEC nhieu. Truoc day watcher ep dong bo giua chung -> ca party relogin ngay
        # sau khi vua danh XONG va NHAN THUONG (log that party 1, 00:32:43-50: nhan "Pho Ban Cap 20
        # II" xong thi 00:32:46 WATCH bao DEADLOCK -> 4 member "EP DONG BO -> relogin", leader thay
        # "dong doi rot" nen cung relogin theo). Chinh chu thich cu da noi no CO Y bat ca "member cho
        # leader danh PB" - do la nham.
        # PHAI LA CA PARTY dang trong PB moi bo qua. CHI MOT acc trong PB con lai dung cho ->
        # DUNG LA deadlock, phai pha (ca party 6: 1 acc login lai vao map PB, 4 acc kia cho mai).
        _ca_party_trong_PB = bool(live) and all(
            getattr(account_clients.get(u), "current_map", None) in TEAM_DUNGEON_MAPS
            for u, _d in live)
        if _ca_party_trong_PB:
            if allwait_t0 is not None:
                log.info("[party %d] WATCH: CA PARTY dang trong PHO BAN -> khong tinh la deadlock",
                         pidx + 1)
            allwait_t0 = None
            continue

        # 1) CA PARTY CUNG CHO = DEADLOCK THAT (khong ai lam gi de ma cho).
        # DUNG waiting_tuoi (bao cao con TUOI) chu KHONG phai waiting.
        # BUG THAT (party 19 va 35): acc roi vong cho roi di lam viec khac ma KHONG AI doi pha ->
        # bao cao ket lai o "reform: da ve thanh..., cho ca party" mai mai. Watcher khong xet tuoi
        # nen thay "ca party deu cho" -> cu 120s ep dong bo mot lan, KEO CA PARTY DANG CHAY TOT ve
        # thanh. Log: 10:05:16 leader bao "sync kenh/map OK 5/5", 10:05:24 "KEO qua cong ra train
        # map", 10:05:40 dang danh - the ma 10:07:12 watcher van tuyen bo DEADLOCK.
        if waiting_tuoi and len(waiting_tuoi) == len(live):
            if allwait_t0 is None:
                allwait_t0 = time.time()
                log.warning("[party %d] WATCH: CA PARTY DEU DANG CHO -> %s", pidx + 1,
                            ", ".join("%s='%s'" % (u, d["task"][:40]) for u, d in waiting_tuoi))
            elif time.time() - allwait_t0 >= WATCH_ALLWAIT_SEC:
                log.warning("[party %d] WATCH: ca party cho nhau %.0fs -> DEADLOCK, EP DONG BO",
                            pidx + 1, time.time() - allwait_t0)
                request_party_resync(pidx, "watcher: ca party cho nhau", cooldown=WATCH_ALLWAIT_SEC)
                allwait_t0 = None
            continue
        allwait_t0 = None

        # 2) ACC DANG LAM ma im qua lau -> nghi treo. Bao ro (ten viec + bao lau) de con truy.
        for u, d in stuck:
            log.warning("[party %d] WATCH: %s IM %.0fs khi dang '%s' (pha=%s, viec da chay %.0fs)"
                        " -> nghi TREO", pidx + 1, u, d["age"], d["task"], d["phase"], d["elapsed"])

        # 2) CO ACC LAM VIEC LE -> ca party CHO no, day KHONG phai lech viec.
        if solo:
            for u, d in solo:
                if d["elapsed"] > WATCH_SOLO_MAX:
                    log.warning("[party %d] WATCH: %s lam viec le '%s' da %.0f phut -> qua lau",
                                pidx + 1, u, d["task"], d["elapsed"] / 60.0)
            mismatch_t0 = None
            continue

        # 2b) PARTY THIEU NGUOI QUA LAU -> EP DONG BO, bat ke ai dang "lam" hay "cho".
        #
        # Day la lo hong that: luat (1) doi CA PARTY cung cho, ma leader thi luon ban (moi party
        # 13-20s/lan) nen khong bao gio du; luat (3) ben duoi thay "co acc dang cho" la bo qua ->
        # party ket 1/4 VO HAN ma khong luat nao dong toi.
        # Log 31/08 party 1 (13:33-13:49, 16 phut): st["channel"]=3 khong ton tai (result=2), 3
        # member nam cho `channel_ready` khong ai set lai, leader moi lien tuc `da join=1 |
        # roster server=1`. Watcher im lang suot.
        # Log 31/08 party 14 (09:14-09:26, 12 phut): trubon ket trong sync kenh nen cong loi moi
        # khong mo, leader `da join=3` mai.
        try:
            _can = int(st.get("n_members") or 0)
            _co = joined_member_count(pidx) + 1 if _can else 0   # +1: chinh leader
        except Exception:
            _can = _co = 0
        if _can and _co < _can and st.get("training_started"):
            if thieu_t0 is None:
                thieu_t0 = time.time()
                log.info("[party %d] WATCH: party THIEU NGUOI (%d/%d) -> bat dau tinh gio",
                         pidx + 1, _co, _can)
            elif time.time() - thieu_t0 >= WATCH_THIEU_NGUOI_SEC:
                log.warning("[party %d] WATCH: THIEU NGUOI (%d/%d) lien tuc %.0f phut -> EP DONG BO",
                            pidx + 1, _co, _can, (time.time() - thieu_t0) / 60.0)
                request_party_resync(pidx, "watcher: party thieu nguoi qua lau",
                                     cooldown=WATCH_THIEU_NGUOI_SEC)
                thieu_t0 = None
            continue
        thieu_t0 = None

        # 3) Co acc dang CHO (nhung khong phai tat ca) -> co nguoi dang lam, cho la HOP LE.
        #    Vd: 3 acc xong DG dung cho, 2 acc con dang trong DG -> khong phai lech viec.
        #    CO Y dung `waiting` (KHONG phai waiting_tuoi): day la LA CHAN. Vong cho DG
        #    ("xong Di Gioi - cho ca party xong") set bao cao DUNG 1 LAN roi ngu 5s/vong, cho co
        #    the toi 2 TIENG -> bao cao luon "gia". Neu doi sang waiting_tuoi thi acc do MAT la
        #    chan -> bo do "lech viec" no oan. Doi ngan gon: la chan de RONG, deadlock de CHAT.
        if waiting:
            mismatch_t0 = None
            continue

        # 4) DEU LAM VIEC TEAM: cung pha thi thoi, LECH PHA thi tinh gio.
        phases = {d["phase"] for _u, d in team}
        if len(phases) <= 1:
            mismatch_t0 = None
            continue
        if mismatch_t0 is None:
            mismatch_t0 = time.time()
            log.info("[party %d] WATCH: party LECH VIEC -> %s", pidx + 1,
                     ", ".join("%s=%s" % (u, d["phase"]) for u, d in team))
            continue
        if time.time() - mismatch_t0 >= WATCH_MISMATCH_SEC:
            log.warning("[party %d] WATCH: LECH VIEC lien tuc %.0f phut (%s) -> EP DONG BO",
                        pidx + 1, (time.time() - mismatch_t0) / 60.0,
                        ", ".join("%s=%s" % (u, d["phase"]) for u, d in team))
            request_party_resync(pidx, "watcher: party lech viec", cooldown=WATCH_MISMATCH_SEC)
            mismatch_t0 = None


def start_all():
    """START TAT CA = khoi dong nhung gi CHUA chay, KHONG dung-roi-chay-lai cai dang chay.

    Truoc day goi start_party() thang -> start_account() giet thread cu (join toi 12s) roi login
    lai tu dau, nen party dang chay ngon bi pha (user bao: "start rieng vai party roi start all
    thi party da chay van bi chay lai").
    """
    n = 0
    skipped = 0
    for pidx in range(len(config.PARTIES)):
        accs = [u for u, _p, _l, _k in party_accounts(pidx)]
        run = sum(1 for u in accs if is_account_running(u))
        if accs and run == len(accs):
            skipped += 1
            continue                    # ca party dang chay du -> khong dung toi
        n += start_party(pidx, skip_running=True)
    if skipped:
        log.info(">>> START TAT CA: bo qua %d party dang chay, khoi dong %d acc", skipped, n)
    return n


def redeem_giftcode_party(pidx, code):
    """Nhap GIFTCODE cho TAT CA acc DANG CHAY cua party pidx (moi acc 1 luong song song).
    Qua giftcode ve mail -> acc tu claim_mail trong redeem_giftcode."""
    code = (code or "").strip()
    configured = list(party_accounts(pidx))
    # Danh sach phan khu cua map leader dang dung. Hoi member dau danh sach co the lay nham map
    # (hoac member dang transit) nen UI thay rong/khong dung.
    targets = [u for u, _p, lead, _pk in configured
               if lead and is_account_running(u) and account_clients.get(u) is not None]
    targets += [u for u, _p, lead, _pk in configured
                if not lead and is_account_running(u) and account_clients.get(u) is not None]
    if not code:
        log.warning(">>> PARTY %s: giftcode rong -> bo qua", pidx + 1)
        return 0
    if not targets:
        log.warning(">>> PARTY %s: KHONG co acc nao dang chay -> khong nhap giftcode '%s'",
                    pidx + 1, code)
        return 0
    log.info(">>> PARTY %s: nhap giftcode '%s' cho %d acc dang chay...", pidx + 1, code, len(targets))
    def _one(u):
        c = account_clients.get(u)
        if c is None:
            return
        try:
            c.redeem_giftcode(code)
        except Exception as e:
            log.warning("[%s] loi nhap giftcode: %s", u, e)
    ths = [threading.Thread(target=_one, args=(u,), daemon=True) for u in targets]
    for t in ths:
        t.start()
    for t in ths:
        t.join(timeout=15)
    log.info(">>> PARTY %s: da gui giftcode '%s' cho %d acc", pidx + 1, code, len(targets))
    return len(targets)


def get_channel_list(pidx):
    """Hoi server danh sach kenh (co so nguoi) cho party pidx -> dict {ch: (cur, cap)}.
    Dung 1 acc DANG CHAY cua party de hoi. Tra {} neu khong co acc chay / khong lay duoc."""
    targets = [u for u, _p, _l, _pk in party_accounts(pidx)
               if is_account_running(u) and account_clients.get(u) is not None]
    if not targets:
        return {}
    c = account_clients.get(targets[0])
    try:
        c.request_channel_list()
        if c._chan_event.wait(3.0):
            return dict(c.channels)
    except Exception as e:
        log.warning(">>> PARTY %s: loi lay list kenh: %s", pidx + 1, e)
    return {}


def party_set_di_gioi_level(pidx, idx):
    """GUI ra lenh: CA party pidx doi CAP QUAI Di Gioi -> idx (1..15). Gui THANG toi cac acc dang
    chay (fire-and-forget, khong huy party/khong vao lai DG - gói 0x61 02 00 idx doi live). Cung
    luu vao PARTY_CONFIG de acc vao DG sau nay dung dung cap."""
    idx = max(1, min(int(idx), 15))
    try:
        config.PARTY_CONFIG.setdefault(int(pidx), {})["di_gioi_level"] = idx
    except Exception:
        pass
    n = 0
    for u, _p, _l, _pk in party_accounts(pidx):
        c = account_clients.get(u)
        if c is not None and getattr(c, "running", False):
            try:
                c.set_di_gioi_level(idx); n += 1
            except Exception as e:
                log.warning(">>> PARTY %s: loi doi cap DG cho %s: %s", pidx + 1, u, e)
    log.info(">>> PARTY %s: lenh DOI CAP QUAI DI GIOI -> idx %d (da gui %d acc dang chay)",
             pidx + 1, idx, n)


def party_switch_channel(pidx, channel):
    """GUI ra lenh doi kenh an toan cho ca party.

    Leader cho het combat va keo doi ve rally_point/safe khi party van con nguyen. Chi sau khi
    xac nhan toi safe moi giai tan party, tha member cung chuyen sang ``channel``, dong bo lai
    map/kenh va leader moi du member lai truoc khi tiep tuc ra bai train.
    """
    st = _pstate(pidx)
    with st["lock"]:
        channel = int(channel)
        st["train_channel_auto"] = False
        st["train_channel_manual"] = channel
        st["kenh_ghim"] = channel
        # Chot dich NGAY luc UI bam. Coordinator chay moi 2s se giup thi hanh cung dich nay,
        # khong chen mot quyet dinh auto trong luc command con cho het battle/ra safe.
        st["kenh_dich"] = channel
        st["kenh_dich_luc"] = time.time()
        st["cmd"] = ("channel", channel)
        st["cmd_gen"] += 1
    log.info(">>> PARTY %s: lenh DOI KENH -> %d (huy party + ca lu chuyen + tiep tuc che do)",
             pidx + 1, channel)


def party_move_to(pidx, x, y):
    """GUI map tap: leader giu nguyen PT va keo ca team toi toa do tren map hien tai."""
    x, y = int(x), int(y)
    if not (0 < x < 20000 and 0 < y < 20000):
        raise ValueError("toa do ngoai ban do")
    st = _pstate(pidx)
    with st["lock"]:
        st["cmd"] = ("point", x, y)
        st["cmd_gen"] += 1
    log.info(">>> PARTY %s: lenh MAP TAP -> (%d,%d), leader keo nguyen party", pidx + 1, x, y)


def party_daily_tasks(pidx, tasks):
    """GUI Android: phat danh sach daily cho moi account loop tu xu ly dung vai leader/member."""
    allowed = ("legion_boss", "world_boss", "solo_dungeon", "team_dungeon",
               "team_dungeon_20", "team_dungeon_50", "team_dungeon_80", "team_dungeon_110")
    raw = tuple(x for x in tasks if x in allowed)
    chosen = tuple([x for x in raw if x == "team_dungeon" or x.startswith("team_dungeon_")] +
                   [x for x in ("legion_boss", "solo_dungeon", "world_boss") if x in raw])
    if not chosen:
        raise ValueError("chua chon daily quest")
    st = _pstate(pidx)
    pending = {u for u, _p, _l, _k in party_accounts(pidx) if is_account_running(u)}
    with st["lock"]:
        # Daily tay thu hoi ke hoach gom/reform cua train. Neu de co cu song them mot nhip,
        # member se thay "DIEU PHOI dang gom" va bo qua ca PB20/50/80.
        st["reform_gen_thoa"] = int(st.get("reform_gen", 0) or 0)
        st["kenh_dich"] = None
        st["gom_dich"] = {}
        st["nhip_acc"] = {}
        st["cmd"] = ("daily", chosen)
        st["cmd_gen"] += 1
        st["daily_active"] = True
        st["daily_cancel"] = False
        st["daily_hold_after_stop"] = False
        st["daily_tasks"] = chosen
        st["daily_task"] = None
        st["daily_phase"] = "queued"
        st["daily_user"] = None
        st["daily_message"] = "Da xep hang Daily; dang cho account san sang"
        st["daily_started_at"] = time.time()
        st["daily_pending"] = pending
        st["daily_participants"] = set(pending)
        st["daily_step_done"] = {}
        # Moi lan bam Chay Daily la mot luot moi. Cache "done" cua PB doi tu luot truoc
        # neu khong xoa se lam member thoat cho ngay va leader khong tao phong.
        st["team_dungeon_state"] = {}
        st["team_dungeon_broke"] = {}
        st["team_dungeon_need_redo"] = False
        st["team_dungeon_skip_all"] = False
        st["team_dungeon_tries"] = {}
        st["o5_state"] = "idle"
        st["nhip_acc"] = {}
    dat_party_dang_gom(pidx, False)
    log.info(">>> PARTY %s: lenh DAILY QUEST -> %s", pidx + 1, chosen)


def party_stop_daily(pidx):
    """Dung daily tay; callback cua boss/dungeon se ket thuc sau tran dang danh."""
    st = _pstate(pidx)
    with st["lock"]:
        st["daily_cancel"] = True
        st["daily_hold_after_stop"] = True
        st["daily_phase"] = "stopping"
        st["daily_message"] = "Dang cho xong tran hien tai roi dung yen"
    for u, _p, _l, _k in party_accounts(pidx):
        c = account_clients.get(u)
        if c is not None and getattr(c, "running", False):
            c._daily_hold = True
            c._ui_auto_battle = False
    log.info(">>> PARTY %s: user yeu cau DUNG DAILY sau tran hien tai; KHONG logout", pidx + 1)


def party_train_map(pidx, map_id, x, y):
    """Android AUTO BATTLE: gom/lap PT/route ca team den map + diem train roi bat combat."""
    map_id, x, y = int(map_id), int(x), int(y)
    if map_id <= 0 or not (0 < x < 20000 and 0 < y < 20000):
        raise ValueError("map/toa do train khong hop le")
    st = _pstate(pidx)
    with st["lock"]:
        st["daily_hold_after_stop"] = False
        for u, _p, _l, _k in party_accounts(pidx):
            _c = account_clients.get(u)
            if _c is not None:
                _c._daily_hold = False
        # Mot nguon su that cho command handler lan coordinator/reconnect.
        st["ui_train_target"] = (map_id, x, y)
        _online_expected = max(1, len([u for u, _p, _l, _k in party_accounts(pidx)
                                      if is_account_running(u)]))
        st["manual_train_expected"] = len(st.get("manual_train_users") or []) or _online_expected
        st["n_members"] = max(0, st["manual_train_expected"] - 1)
        st["train_map_dich"] = map_id
        st["mob_spot"] = (x, y)
        st["rally_point"] = None
        st["thanh_tap_ket_cache"] = None
        new_gen = st["cmd_gen"] + 1
        st["cmd"] = ("train", 0, map_id, x, y)
        st["cmd_gen"] = new_gen
        st["manual_route_gen"] = new_gen
        st["manual_route_plan"] = None
        st["manual_route_source_results"] = {}
        st["manual_route_city_arrived"] = {}
        st["manual_route_plan_ready"].clear()
        st["manual_route_source_done"].clear()
        st["manual_route_party_ready"].clear()
        st["manual_route_done"].clear()
        # Phan khu manual phai ap dung NGAY TAI THANH TAP KET, truoc khi leader moi party va
        # keo ra bai. Danh dau la kenh dich ro rang de do_channel_sync khong bo qua chi vi team
        # dang tinh co cung mot kenh khac.
        _manual_channel = st.get("train_channel_manual")
        st["auto_best_channel"] = bool(_manual_channel)
        if _manual_channel:
            st["kenh_ghim"] = int(_manual_channel)
        st["manual_train_channel"] = None
        st["manual_train_channel_ready"].clear()
    log.info(">>> PARTY %s: START TRAIN TEAM -> map %d bai (%d,%d)",
             pidx + 1, map_id, x, y)


def party_teleport_city(pidx, city_id, flag=0):
    """GUI ra lenh: CA party pidx huy party + teleport ve THANH (city_id, flag) -> roi tiep tuc
    che do da setup (xu ly trong vong keepalive qua cmd_gen)."""
    st = _pstate(pidx)
    with st["lock"]:
        st["cmd"] = ("city", int(city_id), int(flag))
        st["cmd_gen"] += 1
    log.info(">>> PARTY %s: lenh TELEPORT ve thanh %s (flag %s) (huy party + ca lu teleport)",
             pidx + 1, city_id, flag)


def party_route_maps(pidx, source_map=0, dest_map=0):
    """GUI ra lenh: lap/keo party di tu map source_map toi dest_map bang smart world route.
    source_map=0 -> leader tu chon thanh gan dest_map nhat lam diem bat dau."""
    mode = (config.PARTY_CONFIG.get(int(pidx), {}) or {}).get("mode")
    if mode not in ("city", "stand"):
        log.warning(">>> PARTY %s: bo qua lenh DI MAP vi mode=%s khong phai city/stand",
                    int(pidx) + 1, mode)
        return
    source_map = int(source_map or 0)
    dest_map = int(dest_map or 0)
    if dest_map <= 0:
        log.warning(">>> PARTY %s: route map bi bo qua vi BBB khong hop le: %s",
                    pidx + 1, dest_map)
        return
    if not _active_party_usernames(pidx):
        log.warning(">>> PARTY %s: KHONG co acc nao dang chay -> khong route map %s -> %s",
                    pidx + 1, source_map or "AUTO", dest_map)
        return
    st = _pstate(pidx)
    with st["lock"]:
        new_gen = st["cmd_gen"] + 1
        st["cmd"] = ("route", source_map, dest_map)
        st["cmd_gen"] = new_gen
        # Reset NGAY khi GUI phat lenh, khong doi thread dau tien bat duoc lenh moi reset.
        # Acc dang reconnect/login sau lenh se thay route active va van xu ly cmd_gen nay.
        st["manual_route_gen"] = new_gen
        st["manual_route_plan"] = None
        st["manual_route_source_results"] = {}
        st["manual_route_city_arrived"] = {}
        st["manual_route_plan_ready"].clear()
        st["manual_route_source_done"].clear()
        st["manual_route_party_ready"].clear()
        st["manual_route_done"].clear()
    log.info(">>> PARTY %s: lenh DI MAP %s -> %s (AAA=0 la tu chon thanh gan BBB)",
             pidx + 1, source_map or "AUTO", dest_map)


def stop_account(username, reason="GUI Stop acc"):
    """Dung 1 acc: set event + dong ket noi -> thread tu ket thuc."""
    try:
        import inspect
        fr = inspect.stack()[1]
        caller = "%s:%s:%s" % (os.path.basename(fr.filename), fr.lineno, fr.function)
    except Exception:
        caller = "?"
    ev = account_stops.get(username)
    c = account_clients.get(username)
    # OUT RIENG phai la mot giao dich co thu tu: ROI PT -> het battle -> VE SAFE -> moi STOP/close.
    # Khong set stop_ev som: account co the dang o route/reform/daily va cac nhanh do thay Stop se
    # return TRUOC khi toi khoi safe-logout trong keepalive, ket qua la dung ngay bai quai roi OUT.
    if c is not None and getattr(c, "_individual_safe_logout", False):
        account_reconnect[username] = False
        account_stop_reasons[username] = reason
        if getattr(c, "_individual_safe_logout_started", False):
            log.info("[%s] OUT an toan da dang xu ly -> khong tao worker trung", username)
            return True
        c._individual_safe_logout_started = True
        log.info("[%s] OUT AN TOAN: roi PT -> ve safe -> moi dong ket noi (caller=%s)",
                 username, caller)

        def _safe_logout_worker(_u=username, _c=c, _ev=ev):
            try:
                _c.set_party_invite_ready(False)
                _c.flee_mode = True
                _is_leader_logout = bool(getattr(_c, "_individual_safe_logout_is_leader", False))
                try:
                    _c._wait_combat_clear(idle=2.0, cap=120.0)
                except Exception:
                    pass
                # Member roi PT truoc de tu di. Leader giu PT trong luc ve safe de keo cac member
                # con lai ra khoi bai quai, den safe moi giai tan doi.
                if not _is_leader_logout:
                    try:
                        _c.leave_party()
                        time.sleep(1.0)
                    except Exception as e:
                        log.warning("[%s] OUT an toan: loi roi PT: %s", _u, e)

                _map = int(getattr(_c, "current_map", 0) or 0)
                _safes = list((config.TRAIN_MAPS.get(_map) or {}).get("safe") or [])
                _pst = _pstate(int(getattr(_c, "party_idx", 0) or 0))
                _rally = _pst.get("rally_point")
                if _rally and tuple(_rally) not in [tuple(x) for x in _safes]:
                    _safes.append(tuple(_rally))
                _dest = _nearest_safe(getattr(_c, "pos", None), _safes)
                if _dest:
                    _ground = _c.get_ground_store()
                    if _ground is not None and getattr(_c, "pos", None):
                        _snap = _ground.nearest_walkable_world(_map, tuple(_dest), tuple(_c.pos))
                        if _snap is not None:
                            _dest = (int(_snap[0]), int(_snap[1]))
                    log.info("[%s] OUT an toan: dang ve safe %s (map %s)", _u, _dest, _map)
                    _has_ground = bool(_ground is not None and _ground.get(_map) is not None)
                    _ok = _c.navigate_to(*_dest, flee=True,
                                         require_smart_path=_has_ground)
                    # Cho server ap het hang doi di chuyen. Neu toa do live van con xa thi bu
                    # them mot luot; den gan safe phai dung on dinh vai giay moi close socket.
                    for _settle_try in range(2):
                        for _ in range(5):
                            if not getattr(_c, "running", False):
                                break
                            time.sleep(1.0)
                        _pos_now = getattr(_c, "pos", None)
                        _distance = (math.hypot(_pos_now[0] - _dest[0], _pos_now[1] - _dest[1])
                                     if _pos_now else 999999.0)
                        if _distance <= max(40.0, float(getattr(_c, "NAV_TOI_NOI", 40.0))):
                            break
                        log.warning("[%s] OUT an toan: sau khi cho van con cach safe %.0f -> "
                                    "di bu lan %d", _u, _distance, _settle_try + 1)
                        _ok = bool(_c.navigate_to(*_dest, flee=True,
                                                 require_smart_path=_has_ground)) or _ok
                    if _ok:
                        log.info("[%s] OUT an toan: DA TOI SAFE %s va dung on dinh -> logout",
                                 _u, _dest)
                    else:
                        log.warning("[%s] OUT an toan: chua xac nhan toi safe %s; da gui het "
                                    "duong Ground an toan + cho on dinh -> logout", _u, _dest)
                else:
                    log.warning("[%s] OUT an toan: map %s khong co safe/rally -> logout tai cho",
                                _u, _map)
                if _is_leader_logout:
                    try:
                        _c.leave_party()
                        reset_party_joined(int(getattr(_c, "party_idx", 0) or 0))
                        time.sleep(1.0)
                    except Exception as e:
                        log.warning("[%s] leader OUT an toan: loi giai tan PT tai safe: %s", _u, e)
            except Exception as e:
                log.warning("[%s] OUT an toan loi: %s", _u, e)
            finally:
                _c._individual_safe_logout_done = True
                if _ev is not None:
                    _ev.set()
                account_reconnect[_u] = False
                try:
                    _c.close()
                except Exception:
                    pass

        threading.Thread(target=_safe_logout_worker, daemon=True,
                         name="safe-logout-%s" % username).start()
        return True
    if ev is not None:
        ev.set()
    # CHAN CUNG relogin sau STOP: supervisor break neu _st() HOAC account_reconnect False. Set
    # False ngay (khong doi finally cua run_account) -> du acc dang giua chu ky login/daily, khi
    # run_account tra ve la supervisor thoat, KHONG relogin. Log ro de thay Python co nhan lenh.
    account_reconnect[username] = False
    account_stop_reasons[username] = reason
    log.info("[%s] STOP: %s (caller=%s, set stop_ev + chan relogin)", username, reason, caller)
    if c is not None:
        # KHONG dong socket ngay neu thread tu xu ly viec thoat:
        #  - leader map-train: tu chay ve safe roi dong.
        #  - member train: cho leader ve safe (stop_leader_done) roi moi dong.
        if (getattr(c, "_return_safe_on_stop", None) or getattr(c, "_wait_leader_on_stop", None)
                or getattr(c, "_individual_safe_logout", False)):
            if getattr(c, "_return_safe_on_stop", None):
                log.info("[%s] STOP -> cho thread chay ve safe roi dong", username)
            else:
                log.info("[%s] STOP -> cho leader ve safe roi member thoat theo", username)
            # WATCHDOG: thread co the KET o blocking call (navigate ve safe khong toi, dang reform/
            # danh boss, hoac member cho leader qua lau) -> STOP "khong an". Sau 25s chua thoat thi
            # FORCE dong socket -> unblock recv -> thread chac chan chet (STOP luon co tac dung).
            def _force_close_watchdog(_u=username, _c=c):
                t = account_threads.get(_u)
                limit = 75 if getattr(_c, "_individual_safe_logout", False) else 25
                for _ in range(limit):
                    if t is None or not t.is_alive():
                        return
                    time.sleep(1)
                log.warning("[%s] STOP: thread chua thoat sau %ds -> FORCE dong socket", _u, limit)
                try: _c.close()
                except Exception: pass
            threading.Thread(target=_force_close_watchdog, daemon=True).start()
        else:
            try: c.close()
            except Exception: pass
    return True


def stop_party(pidx, reason="GUI Stop party"):
    for u, p, _, _ in party_accounts(pidx):
        stop_account(u, reason=reason)


def stop_all(reason="GUI Stop tat ca"):
    global _start_cancel_generation
    _start_cancel_generation += 1
    us = list(account_stops.keys())
    log.info("STOP TAT CA: %s: %d acc -> %s", reason, len(us), us)
    for u in us:
        stop_account(u, reason=reason)


def is_account_running(username):
    t = account_threads.get(username)
    return t is not None and t.is_alive()


def furnace_notify_items(pidx):
    """[{user, tab, id, name, bag, slot, kind, new}] - thong bao lo cua CA party pidx.

    Cau noi cho UI (GUI PC dung truc tiep account_furnace_notify; APK goi ham nay qua Chaquopy).
    Tra du lieu THO - ben UI tu dung cau chu (APK da co equip_stats.json de hien chi so trang bi).
    """
    out = []
    try:
        accs = party_accounts(pidx)
    except Exception:
        return out
    for tpl in accs:
        u = tpl[0] if isinstance(tpl, (tuple, list)) else tpl
        for it in list(account_furnace_notify.get(u) or []):
            d = {"user": u}
            for k in ("tab", "id", "name", "bag", "slot", "kind", "new", "quant"):
                if k in it:
                    d[k] = it[k]
            out.append(d)
    return out


# Acc da bam "Bo qua" thong bao KHONG CO QUAN DOAN (an trong phien nay).
legion_notify_dismissed = set()


DIEM_DE_DANH_MAC_DINH = 999
# Diem skill DE DANH mac dinh - giong o Point: khong tick gi thi bot KHONG tieu diem cua user.
SKILL_DE_DANH_MAC_DINH = 999        # acc chua config -> KHONG tieu diem nao


def diem_rules_mac_dinh():
    """Cau hinh tang diem cua acc MOI: dung 1 dong "de danh 999" -> bot khong cong gi ca.

    User chot 31/08: "acc moi se mac dinh chi co dung 1 dong 'point de danh: 999', nguoi dung se
    phai tu config". Diem la thu KHONG LAY LAI DUOC (muon reset phai mua item), nen mac dinh phai
    la KHONG LAM GI.
    """
    return {"reserve": DIEM_DE_DANH_MAC_DINH, "rules": []}


def _diem_doc_cfg(cfg):
    """Chuan hoa cau hinh tang diem -> (so_de_danh, [(ma_chi_so, muc_dich)])."""
    cfg = cfg if isinstance(cfg, dict) else {}
    try:
        reserve = max(0, int(cfg.get("reserve", DIEM_DE_DANH_MAC_DINH)))
    except (TypeError, ValueError):
        reserve = DIEM_DE_DANH_MAC_DINH
    dong = []
    for r in (cfg.get("rules") or []):
        if not isinstance(r, dict):
            continue
        ma = ATTR_KEY_TO_CODE.get(str(r.get("stat") or "").strip().lower())
        try:
            dich = int(r.get("target"))
        except (TypeError, ValueError):
            continue
        if ma and dich > 0:
            dong.append((ma, dich))
    return reserve, dong


def auto_cong_diem(client, cfg, label=""):
    """Tu cong diem tiem nang theo bang rule. -> (so_diem_da_cong, so_diem_du_THUA).

    Luat user chot 31/08:
      - Dong dau CO DINH "Point de danh: N" -> LUON giu lai N diem, chi tieu phan VUOT qua N.
      - Cac dong sau la MUC DICH cua tung chi so, duyet TU TREN XUONG: dong nao chi so GOC chua
        dat thi cong cho du roi moi xuong dong tiep; dat roi thi bo qua.
      - Duyet het bang ma van con du hon so de danh -> BAO cho user (mau "Chu y"), khong tu tieu.

    Rule chot theo DIEM GOC (`char_diem_goc`), KHONG phai tong co trang bi: cong diem lam tang
    diem goc, va tong thi thao/deo do la doi - chot theo tong se lam bot do diem theo bo do.
    """
    con = client.attr_point_left()
    if con is None:
        return 0, 0
    reserve, dong = _diem_doc_cfg(cfg)
    dung_duoc = con - reserve
    if dung_duoc <= 0:
        return 0, 0
    goc = client.char_diem_goc()
    if not goc:
        return 0, 0        # chua co goi login -> chua biet diem goc, dung doan
    da_cong = 0
    for ma, dich in dong:
        if dung_duoc <= 0:
            break
        hien = int(goc.get(ma, 0))
        thieu = dich - hien
        if thieu <= 0:
            continue                       # dat muc roi -> dong tiep theo
        # DONG AGI: DEN LUOT no thi BO QUA so de danh (user chot 11/09).
        #
        # "De danh" sinh ra de giu diem cho cac dong PHIA SAU chua toi luot. Nhung AGI la dong
        # quyet dinh luot danh, va khi da toi luot no thi khong con dong nao phai giu cho nua ->
        # giu tiep la giu mai mai.
        # Vi du user dua ra: de danh 33, hpx 22, int 55, agi 44.
        #   int moi 50  -> chua toi dong agi -> VAN giu 33 (chi tieu phan vuot)
        #   int du 55   -> toi dong agi      -> tang agi len 44, khong quan tam 33 nua
        # Thu tu van nguyen: cac dong TREN phai xong het roi moi den luot agi.
        if ma == ATTR_KEY_TO_CODE.get("agi") and reserve > 0:
            _truoc = dung_duoc
            dung_duoc = max(dung_duoc, con - da_cong)     # tra lai phan dang giu
            if dung_duoc > _truoc:
                log.info("[%s] Tang diem: den dong AGI -> BO QUA %d diem de danh (khong con dong "
                         "nao phia sau phai giu cho), dung duoc %d -> %d",
                         label or getattr(client, "_label", ""), reserve, _truoc, dung_duoc)
        them = min(thieu, dung_duoc)
        if not client.add_attr_point(ma, them):
            break                          # dang trong tran / gui loi -> de vong sau
        goc[ma] = hien + them
        dung_duoc -= them
        da_cong += them
        log.info("[%s] Tang diem: %s %d -> %d (muc %d), con dung duoc %d",
                 label or getattr(client, "_label", ""), ATTR_CODE_TO_TEN.get(ma, ma),
                 hien, hien + them, dich, dung_duoc)
    return da_cong, max(0, dung_duoc)


# Acc con du diem sau khi duyet het bang rule: username -> so diem du. Hien o man "Chu y".
diem_du_notify = {}
diem_du_notify_dismissed = set()


def _tu_cong_diem(client, username, label=""):
    """Chay bang TU CONG DIEM cua acc. Goi luc login (sau khi da ra safe) va dinh ky khi train.

    Con du hon so "de danh" sau khi duyet het bang -> ghi vao `diem_du_notify` de hien o man
    "Chu y" (user chot 31/08: "bao 1 cai thong bao len chu y la 'acc xxx con du yyy diem chua
    dung'"). Cong het roi thi xoa thong bao di.
    """
    try:
        cfg = (getattr(config, "ACCOUNT_POINT", None) or {}).get(username)
        if cfg is None:
            cfg = diem_rules_mac_dinh()      # acc chua config -> chi co "de danh 999" = khong cong
        _da, _du = auto_cong_diem(client, cfg, label=label)
        if _du > 0:
            diem_du_notify[username] = _du
            log.info("[%s] Tang diem: duyet het bang ma con DU %d diem chua dung -> bao o man Chu y",
                     label or username, _du)
        else:
            diem_du_notify.pop(username, None)
        return _da
    except Exception as e:
        log.warning("[%s] tu cong diem loi (bo qua): %s", label or username, e)
        return 0


def diem_du_notify_items(pidx):
    """[{user, kind:'diem_du', diem}] - acc con du diem chua dung (da duyet het bang rule)."""
    out = []
    try:
        accs = party_accounts(pidx)
    except Exception:
        return out
    for tpl in accs:
        u = tpl[0] if isinstance(tpl, (tuple, list)) else tpl
        if u in diem_du_notify_dismissed:
            continue
        n = int(diem_du_notify.get(u) or 0)
        if n > 0:
            out.append({"user": u, "kind": "diem_du", "diem": str(n)})
    return out


def save_cat_do_items_str(items_str):
    r"""ANDROID: luu list cat vao tien trang. Nhan CHUOI noi bang "\n", KHONG phai List<String>.

    Chaquopy khong convert dung List<String> qua callAttr (ban release bi R8 rut gon ten lop ->
    "TypeError: 't' object is not iterable") - moi cho khac trong BotForegroundService cung noi
    chuoi y het.
    """
    ds = {}
    for dong in str(items_str or "").split("\n"):
        dong = dong.strip().lower()
        if not dong:
            continue
        # Dang "tid=cat" / "tid=lay". Dang CU chi co "tid" tron -> hieu la "cat".
        k, _sep, v = dong.partition("=")
        ds[k.strip()] = v.strip() or CAT_DO_CAT
    return bool(save_cat_do_items(ds))


def load_cat_do_items_str():
    r"""ANDROID: doc list cat -> chuoi cac dong "tid=cat" / "tid=lay", noi bang "\n".

    Chaquopy tra dict/list khong on dinh qua callAttr (xem save_cat_do_items_str) nen quy ve
    chuoi y het moi cho khac.
    """
    return "\n".join("%s=%s" % (k, v) for k, v in sorted(load_cat_do_items().items()))


def apply_point_config(username, point_config):
    """GUI/APK luu bang tu cong diem -> ap NGAY cho acc dang chay (khong doi restart)."""
    if isinstance(point_config, str):
        try:
            import json
            point_config = json.loads(point_config) if point_config else {}
        except Exception:
            point_config = {}
    cfg = point_config if isinstance(point_config, dict) else {}
    if not isinstance(getattr(config, "ACCOUNT_POINT", None), dict):
        config.ACCOUNT_POINT = {}
    if cfg:
        config.ACCOUNT_POINT[username] = cfg
    else:
        config.ACCOUNT_POINT.pop(username, None)
    log.info("[%s] da apply bang TU CONG DIEM moi (live)", username)
    return True


def point_info(username):
    """Bang diem cua 1 acc cho UI: goc / tong / diem du.

    KHONG quan tam pet (user chot 31/08). `char_base` = diem GOC (thu ma cong diem tac dong toi),
    `char_stat_full()` = TONG da cong trang bi/thu cuoi/the/suu tap.

    ACC DANG CHAY -> doc live va GHI CACHE. ACC DA TAT -> doc CACHE de van XEM duoc bang diem
    (them khoa `cache: True` + `ts`). CONG diem thi van phai bat acc len - phai gui goi len server.
    """
    c = account_clients.get(username)
    goc = {}
    if c is not None:
        try:
            goc = c.char_diem_goc()
        except Exception as e:
            log.debug("[%s] doc bang diem loi: %s", username, e)
            goc = {}
    if not goc:
        _cache, _ts = load_point_cache(username)
        if _cache:
            return dict(_cache, cache=True, ts=_ts)
        return {}
    try:
        tong = c.char_stat_full() or {}
        out = {
            "left": c.attr_point_left(),
            "stats": [{"key": k, "ten": t, "ma": ma,
                       "goc": int(goc.get(ma, 0)), "tong": int(tong.get(ma, goc.get(ma, 0)))}
                      for ma, k, t in ATTR_KINDS],
        }
    except Exception as e:
        log.debug("[%s] doc bang diem loi: %s", username, e)
        return {}
    try:
        save_point_cache(username, out)
    except Exception as e:
        log.debug("[%s] ghi cache bang diem loi (bo qua): %s", username, e)
    return out


def skill_char_info(username):
    """Bang SKILL NHAN VAT cua 1 acc cho UI: he + diem skill con lai + cap tung skill.

    ACC DANG CHAY -> doc live va GHI CACHE. ACC DA TAT -> doc CACHE de van XEM duoc cay skill
    (them khoa `cache: True` + `ts`). NANG skill thi van phai bat acc len (phai gui goi len server).
    """
    c = account_clients.get(username)
    if c is not None and (getattr(c, "char_skill_lv", None) or c.he_nhan_vat()):
        out = {
            # He lay tu `char_attrs[24]` khi `S:008-013` chua toi - dialog can biet mo tab he nao.
            "element": c.he_nhan_vat(),
            "left": c.skill_point_left(),
            "lv": {"0x%04x" % int(k): int(v) for k, v in (c.char_skill_lv or {}).items()},
            "char_level": getattr(c, "char_level", None),
        }
        try:
            save_skill_char_cache(username, out)
        except Exception as e:
            log.debug("[%s] ghi cache skill nhan vat loi (bo qua): %s", username, e)
        return out
    _cache, _ts = load_skill_char_cache(username)
    if _cache:
        return dict(_cache, cache=True, ts=_ts)
    return {}


def nang_skill_ngay(username, skill_id, cap_dich):
    """Hoc/nang TAY mot skill nhan vat tu dialog.

    -> True = da gui | "queued" = dang trong tran, DA XEP HANG | (False, ly_do) = khong lam duoc.

    DANG TRONG TRAN thi XEP HANG y het cong diem (`add_point`) / lenh tui do: client that chan
    thao tac bang skill khi dang danh, gui bua la server nuot.
    `de_danh=0`: day la lenh TAY cua user, khong dinh gi toi o "de danh" cua bang tu nang.
    """
    c = account_clients.get(username)
    if c is None:
        return False, "acc chưa chạy"
    sid, cap = int(skill_id), int(cap_dich)
    ten = "Nâng skill %s -> cấp %d" % (c._ten_skill(sid), cap)
    try:
        if c.queue_bag_cmd(ten, lambda: c.nang_skill_char([[sid, cap]], 0)):
            return "queued"
    except Exception as e:
        log.debug("[%s] xep hang nang skill loi (gui thang): %s", username, e)
    try:
        da, ly_do = c.nang_skill_char([[sid, cap]], 0)
    except Exception as e:
        return False, str(e)
    return (True if da else (False, ly_do or "không nâng được"))


def apply_skill_config_json(username, cfg_json):
    """ANDROID: nhu apply_skill_config nhung nhan CHUOI JSON.

    Ban PC truyen thang dict; Kotlin chi truyen duoc chuoi (giong apply_point_config, ham do tu
    json.loads o dau). Tach ham rieng de KHONG doi kieu tham so cua apply_skill_config - no la
    code DUNG CHUNG voi PC.
    """
    cfg = cfg_json
    if isinstance(cfg, str):
        try:
            cfg = json.loads(cfg) if cfg else {}
        except Exception:
            cfg = {}
    return apply_skill_config(username, cfg if isinstance(cfg, dict) else {})


def apply_skill_config(username, cfg):
    """GUI luu bang tu nang skill -> ap NGAY vao config dang chay (khong phai restart acc)."""
    if not username:
        return False
    cu = dict((getattr(config, "ACCOUNT_SKILL", None) or {}).get(username) or {})
    cu.update(cfg or {})
    if not hasattr(config, "ACCOUNT_SKILL"):
        config.ACCOUNT_SKILL = {}
    config.ACCOUNT_SKILL[username] = cu
    return True


def _tu_nang_skill(client, username, label):
    """Viec vat luc login: tu nang skill nhan vat theo bang rule cua acc."""
    cfg = (getattr(config, "ACCOUNT_SKILL", None) or {}).get(username) or {}
    rules = cfg.get("rules") or []
    if not rules:
        return
    de_danh = cfg.get("reserve")
    de_danh = SKILL_DE_DANH_MAC_DINH if de_danh is None else de_danh
    try:
        da, ly_do = client.nang_skill_char(rules, de_danh)
    except Exception as e:
        log.warning("[%s] loi tu nang skill: %s", label, e)
        return
    if not da and ly_do:
        log.info("[%s] Tu nang skill: %s", label, ly_do)


def add_point(username, stat_key, add):
    """Cong TAY `add` diem vao chi so `stat_key` ('int'/'atk'/...).

    -> True = da gui | "queued" = dang trong tran, DA XEP HANG (het tran tu gui) | False = loi.

    DANG TRONG TRAN thi XEP HANG y het lenh tui do (`queue_bag_cmd`): client that cung chan doi
    do/cong diem giua tran (`UI/UIStatus.lua:2009` - `war` phai la None/Guest), gui bua la server
    nuot. User 31/08: "lam nhu cai doi trang bi ay, de khi het tran moi gui len cong di".
    """
    c = account_clients.get(username)
    ma = ATTR_KEY_TO_CODE.get(str(stat_key or "").strip().lower())
    if c is None or not ma:
        return False
    ten = "Cộng %d %s" % (int(add), ATTR_CODE_TO_TEN.get(ma, ma))
    try:
        if c.queue_bag_cmd(ten, lambda: c.add_attr_point(ma, add)):
            return "queued"
    except Exception as e:
        log.debug("[%s] xep hang cong diem loi (gui thang): %s", username, e)
    return bool(c.add_attr_point(ma, add))


def diem_du_notify_skip(username):
    diem_du_notify_dismissed.add(username)
    return True


# Ba Dau SAP HET HAN: username -> chuoi mo ta moc het han. Chi ghi LUC LOGIN (user chot 01/09).
ba_dau_notify = {}
ba_dau_notify_dismissed = set()
BA_DAU_BAO_TRUOC = 86400.0      # con duoi 1 NGAY thi bao


def _kiem_han_ba_dau(client, username, label=""):
    """Luc LOGIN: con >0 va <1 ngay thi ghi thong bao "Ba Dau sap het han" cho man Chu y.

    User chot 01/09: "nho la gan het moi bao, khi time =0 roi thi ko bao nua" -> `han_dung_con_lai`
    da tra None khi da het han / khong co, nen chi can chot can tren.
    Chi kiem MOT LAN luc login, khong theo doi lien tuc.
    """
    try:
        con = client.han_dung_con_lai("ba_dau")
        if con is None or con.total_seconds() >= BA_DAU_BAO_TRUOC:
            ba_dau_notify.pop(username, None)      # khong co / con nhieu -> khong bao
            return
        het = (getattr(client, "han_dung", None) or {}).get("ba_dau")
        ba_dau_notify[username] = het.strftime("%H giờ %M phút ngày %d/%m/%Y")
        log.warning("[%s] Ba Dau SAP HET HAN: con %.1f gio (het luc %s) -> bao o man Chu y",
                    label or username, con.total_seconds() / 3600.0, ba_dau_notify[username])
    except Exception as e:
        log.debug("[%s] kiem han Ba Dau loi (bo qua): %s", label or username, e)


def ba_dau_notify_items(pidx):
    """[{user, kind:'ba_dau', luc}] - acc co Ba Dau sap het han (con duoi 1 ngay)."""
    out = []
    try:
        accs = party_accounts(pidx)
    except Exception:
        return out
    for tpl in accs:
        u = tpl[0] if isinstance(tpl, (tuple, list)) else tpl
        if u in ba_dau_notify_dismissed:
            continue
        luc = ba_dau_notify.get(u)
        if luc:
            out.append({"user": u, "kind": "ba_dau", "luc": luc})
    return out


def ba_dau_notify_skip(username):
    ba_dau_notify_dismissed.add(username)
    return True


def legion_notify_items(pidx):
    """[{user, kind:'legion'}] - acc KHONG o quan doan nao.

    Dung chung PC/APK (GUI PC va Chaquopy deu goi ham nay) de hai ban khong lech luat.

    `org_id` den tu `0x05 sub03` luc login (client `_on_org_id`): 0 = khong co quan doan. CHI bao
    khi DA CHAC CHAN (`_no_legion_confirmed`) - chua nhan goi thi `org_id` con None, bao luc do la
    bao lao ca luot dau cua moi acc.
    """
    out = []
    try:
        accs = party_accounts(pidx)
    except Exception:
        return out
    for tpl in accs:
        u = tpl[0] if isinstance(tpl, (tuple, list)) else tpl
        if u in legion_notify_dismissed:
            continue
        c = account_clients.get(u)
        if c is None or not getattr(c, "running", False):
            continue
        if not getattr(c, "_no_legion_confirmed", False):
            continue
        out.append({"user": u, "kind": "legion"})
    return out


# username da bam "Bo qua" thong bao tui gan day (an trong phien nay)
bag_notify_dismissed = set()
# Slot tui con it hon nguong nay -> canh bao. User chot 01/09 nang tu 5 len 10: tui gan day thi
# nhieu viec HONG AM THAM truoc khi day han - nhan qua mail that bai (xem `claim_mail`), khong
# nhat duoc do roi trong tran, khong mua duoc do o lo.
BAG_CANH_BAO_SLOT_TRONG = 10


def bag_info(username):
    """TUI DO cua 1 acc cho UI (APK). {} = chua co du lieu nao (chua chay VA chua tung cache).

    Acc TAT thi tra ban CACHE kem `"live": False` (user 06/09: "tui do la de xem lai khi
    offline"). Ban cache la CHI XEM: UI PHAI khoa moi nut khi `live` False - bam "phan giai"
    theo so cu la mat nham do. Do dung la ly do truoc day ham nay tu choi cache han; gio cache
    nhung tra kem co `live` de cho goi biet duong khoa nut.

    Moi o kem san cac co CHO PHEP (use/equip/dis/fashion) tinh bang `bot.bag_tabs` - dung luat
    cua client. De Kotlin tu suy tu items_gamedata la se lech, vi luat that nam o bag_tabs
    (vd `can_use` doc btnState NGUOC voi truc giac - xem chu thich trong file do).
    """
    c = account_clients.get(username)
    from . import bag_tabs as _bt
    from .client import _load_gamedata_items, load_bag_cache
    if c is None:
        _tui, _ts = load_bag_cache(username)
        if not _tui:
            return {}
        _slots = {int(s): v for s, v in (_tui.get("slots") or {}).items()}
        return dict(_bag_info_slots(_slots, None), live=False, ts=int(_ts),
                    cap=int(_tui.get("cap") or 0), used=int(_tui.get("used") or len(_slots)),
                    maxed=False)
    return dict(_bag_info_slots({int(s): v for s, v in (getattr(c, "bag_slots", None) or {}).items()}, c),
                live=True, ts=int(time.time()),
                cap=c.bag_capacity(), used=c.bag_used_slots(), maxed=bool(c.bag_slot_maxed()))


def bank_info(username):
    """TIEN TRANG cua 1 acc cho UI. LUON tu cache, ke ca khi acc dang chay.

    Server KHONG BAO GIO tu gui kho: bot chi thay no dung luc di NPC Trac Quan mo kho. Nen day
    la anh chup cua LAN MO KHO GAN NHAT, va acc chua tung mo kho thi coi nhu KHONG CO GI (user
    chot 06/09 - khong lam nut "doc lai kho" vi ton mot chuyen di).

    Kho LUON chi xem: khong co lenh nao cua UI tac dong vao no.
    """
    from .client import load_bank_cache
    c = account_clients.get(username)
    kho, ts = load_bank_cache(username)
    slots = {int(i): v for i, v in ((kho or {}).get("slots") or {}).items()}
    if c is not None and getattr(c, "bank_slots", None):
        slots = {int(i): v for i, v in c.bank_slots.items()}   # dang mo kho -> ban SONG moi hon
        ts = int(time.time())
    if not slots and not ts:
        return {}          # chua tung mo kho -> coi nhu khong co gi (giong bag_info khi chua cache)
    return dict(_bag_info_slots(slots, None), live=False, ts=int(ts or 0),
                used=len(slots), cap=0, maxed=False)


def _bag_info_slots(slots, c):
    """Dung `{"slots": [...]}` cho ca tui do lan tien trang. `c=None` = ban CHI XEM (acc tat /
    kho) -> cac co can client song (`fashion`, `bank`) de False, UI khoa nut theo `live`."""
    from . import bag_tabs as _bt
    from .client import _load_gamedata_items, GameClient
    _BANK_RESTRICT_CAM = GameClient.BANK_RESTRICT_CAM
    gd = _load_gamedata_items()
    o = []
    for slot, val in sorted((slots or {}).items()):
        try:
            tid, cnt = int(val[0]), int(val[1])
        except Exception:
            continue
        if cnt <= 0:
            continue
        d = gd.get(tid) or {}
        o.append({
            "slot": int(slot), "id": tid, "cnt": cnt,
            "name": d.get("name") or ("0x%04x" % tid),
            "q": int(d.get("q", 0) or 0),
            "st": int(d.get("st", 999) or 999),
            "ft": int(d.get("ft", 0) or 0),
            "kd": int(d.get("kd", 0) or 0),
            "tab": [t for t, _ten in _bt.TAB_NAMES
                    if _bt.matches_tab(t, d.get("ft"), d.get("kd"))],
            "use": bool(_bt.can_use(d.get("bs"))),
            "equip": bool(_bt.can_equip(d.get("ft"), d.get("kd"))),
            "dis": bool(_bt.can_dismantle(d.get("fc"))),
            "fashion": bool(c.is_fashion_item(tid)) if c is not None else False,
            # Mon game CAM gui ngan hang -> khong cho them vao list cat (them cung vo ich).
            # Day la co DUY NHAT con dung o ban cache: nut "Tu cat vao Tien trang" ghi thang
            # accounts.json, khong can client song (user chot 06/09).
            "bank": not (int(d.get("restrict", 0) or 0) & _BANK_RESTRICT_CAM),
        })
    return {"slots": o}


# Lenh tui do UI duoc phep goi. Khoa -> (ten hien thi, ham chay).
# Danh sach TRANG (allowlist) co chu y: UI khong duoc goi bua bat ky method nao cua client.
_BAG_LENH = {
    "use": ("Dùng", lambda c, s, a: c.use_slot(s, target=a)),
    "equip": ("Trang bị", lambda c, s, a: c.use_slot(s, target=a)),
    "decompose": ("Phân giải", lambda c, s, a: c.decompose_slot(s)),
    "discard": ("Bỏ", lambda c, s, a: c.discard_item(s, a or 1)),
    "fashion": ("Thả vào sưu tầm", lambda c, s, a: c.deposit_fashion_slot(s)),
}


def bag_cmd(username, action, slot, arg=0):
    """Chay mot lenh tui do tu UI. -> "True" | "queued" | "False: ly do".

    DANG TRONG TRAN thi XEP HANG (`queue_bag_cmd`) y het lenh cong diem / nang skill: client that
    chan thao tac tui do khi dang danh, gui bua la server nuot im lang.
    """
    c = account_clients.get(username)
    if c is None:
        return "False: acc chưa chạy"
    lenh = _BAG_LENH.get(str(action or ""))
    if lenh is None:
        return "False: lệnh không hợp lệ"
    ten, fn = lenh
    s, a = int(slot), int(arg or 0)
    try:
        if c.queue_bag_cmd("%s (ô #%d)" % (ten, s), lambda: fn(c, s, a)):
            return "queued"
        return "True" if fn(c, s, a) else "False: server không nhận"
    except Exception as e:
        return "False: %s" % e


def bag_notify_items(pidx):
    """[{user, kind:'bag', used, cap, free, maxed}] - acc con DUOI 10 slot tui trong.

    Dung chung PC/APK. Truoc day luat nay chi nam trong `gui.py` nen ban APK KHONG he co muc canh
    bao tui - dung cai bay "chep tay o dau la lech o do" trong CLAUDE.md.
    """
    out = []
    try:
        accs = party_accounts(pidx)
    except Exception:
        return out
    for tpl in accs:
        u = tpl[0] if isinstance(tpl, (tuple, list)) else tpl
        if u in bag_notify_dismissed:
            continue
        c = account_clients.get(u)
        if c is None or not getattr(c, "running", False):
            continue
        try:
            if not getattr(c, "bag_slots", None):   # chua co snapshot tui -> chua tinh duoc
                continue
            free = c.bag_free_slots()
            if free >= BAG_CANH_BAO_SLOT_TRONG:
                continue
            out.append({"user": u, "kind": "bag", "used": c.bag_used_slots(),
                        "cap": c.bag_capacity(), "free": free, "maxed": c.bag_slot_maxed()})
        except Exception:
            continue
    return out


def bag_notify_skip(username):
    """Bo qua thong bao tui cua 1 acc (an trong phien nay)."""
    if username:
        bag_notify_dismissed.add(username)
    return True


def legion_notify_skip(username):
    """Bo qua thong bao quan doan cua 1 acc (an trong phien nay)."""
    legion_notify_dismissed.add(username)
    return True


def furnace_notify_count(pidx):
    """So thong bao lo dang cho cua party (de hien badge tren nut 'Chu y')."""
    return len(furnace_notify_items(pidx))


def _furnace_notify_drop(username, tid):
    lst = account_furnace_notify.get(username)
    if not lst:
        return False
    for i, it in enumerate(list(lst)):
        if int(it.get("id", -1)) == int(tid):
            lst.pop(i)
            return True
    return False


def furnace_notify_buy(username, tid):
    """MUA item lo dang cho o acc `username`. Tra True neu server nhan lenh mua."""
    it = None
    for x in list(account_furnace_notify.get(username) or []):
        if int(x.get("id", -1)) == int(tid):
            it = x
            break
    if it is None:
        return False
    c = account_clients.get(username)
    if c is None or not getattr(c, "running", False):
        return False
    try:
        ok = bool(c.buy_furnace_item(it["kind"], it["slot"], it["id"]))
    except Exception as e:
        log.warning("[%s] mua item lo tu UI loi: %s", username, e)
        return False
    if ok:
        _furnace_notify_drop(username, tid)
    return ok


def furnace_notify_skip(username, tid):
    """BO QUA: go khoi danh sach cho, khong mua."""
    return _furnace_notify_drop(username, tid)


def account_status(username):
    """Dict trang thai live cua acc (cho GUI). running, char, map, channel, in_party, dg_remain..."""
    c = account_clients.get(username)
    running = is_account_running(username)
    pidx = getattr(c, "party_idx", None) if c is not None else party_idx_of(username)
    party_avg_level = (_party_average_level(pidx)
                       if config.PARTY_LEADER_ACC.get(pidx) == username else None)
    if c is None:
        # da tat/thoat -> GIU map + nhan vat LUC CUOI (de biet thoat o dau, dung map khong)
        # THREAD CON SONG ma chua co client = DANG LOGIN (supervisor da dong socket cu, chua kip
        # tao client moi). Truoc day tinh la "chay" -> user tuong dang danh trong khi no dang
        # login lai sau khi bi server dut.
        last = account_last.get(username, {})
        return {"running": running, "logging_in": running,
                "state": "logging_in" if running else "stopped",
                "char": last.get("char", ""), "map": last.get("map"),
                "in_party": False, "dg_remain": None, "combat": False, "channel": None,
                "strategist": False, "char_level": last.get("char_level"),
                "char_agi": last.get("char_agi"),
                "pet_name": last.get("pet_name") or "", "pet_level": last.get("pet_level"),
                "pet_agi": last.get("pet_agi"),
                "party_avg_level": party_avg_level}
    from .client import is_joined, is_strategist
    st = _party_state.get(pidx, {})
    dg_remain = None
    if c.current_map == config.DIGIOI_MAP_ID:
        dg_remain = max(0, int(DIGIOI_LIMIT - c.digioi_minutes_live()))
    account_last[username] = {"map": c.current_map, "char": c.char_name or "",
                              "char_level": getattr(c, "char_level", None),
                              "char_agi": getattr(c, "char_agi", None),
                              "pet_name": c.pet_name_out(),
                              "pet_level": getattr(c, "pet_level", None),
                              "pet_agi": getattr(c, "pet_agi", None),
                              "pet_faith": _trung_thanh_pet_dang_dung(c)}  # luu lai luc cuoi
    _ch = getattr(getattr(c, "state", None), "char", None)   # hp/sp cho UI APK (PC GUI bo qua)
    return {
        "running": running,
        "char": c.char_name or "",
        # NHAN LOG that su in ra dau dong: bang ten nhan vat, TRU khi trung ten voi acc khac thi la
        # 'ten~username' (xem `_NHAN_CHU` trong bot/client.py). APK phai mask/loc theo cai nay,
        # khong duoc theo "char" - trung ten la mask nham dong log cua acc khac.
        "log_label": getattr(c, "_label", "") or "",
        "map": c.current_map,
        # Kenh THAT cua chinh acc nay (c.current_channel - bot doc tu 0x03/0x0c), khong phai
        # st["channel"] la kenh party CHON: cai do bi clear moi vong sync nen cot "Kenh" gan nhu
        # luon rong, va giong het nhau moi acc -> nhin khong ra vu lech kenh (log 17:25: leader
        # kenh 2, member kenh 1). Con st["channel"] chi dung khi chua doc duoc kenh that.
        "channel": getattr(c, "current_channel", None) or st.get("channel"),
        # False = so kenh tren KHONG duoc server xac nhan (lenh doi kenh gan nhat hong) - UI hien
        # dau `?` de nguoi nhin biet ngay, thay vi tin mot so ao. Xem `Client.kenh_dang_chac`.
        "channel_chac": (c.kenh_dang_chac() if hasattr(c, "kenh_dang_chac") else True),
        "in_party": is_joined(pidx, c.self_entity),
        "dg_remain": dg_remain,
        "combat": c.in_combat() if running else False,
        "strategist": is_strategist(pidx, c.self_entity),
        "char_level": getattr(c, "char_level", None),
        "char_agi": getattr(c, "char_agi", None),
        "pet_name": c.pet_name_out() or "",
        "pet_level": getattr(c, "pet_level", None),
        "pet_agi": getattr(c, "pet_agi", None),
        "pet_faith": _trung_thanh_pet_dang_dung(c),
        "party_avg_level": party_avg_level,
        # --- them cho UI APK (poll qua account_status thay callback on_status) ---
        # DANG LOGIN = thread song nhung CHUA vao world. Moc "vao world xong" dung y het luc
        # connect() cho o run_account: self_entity va current_map deu phai co.
        "logging_in": bool(running and (c.self_entity is None or c.current_map is None)),
        "state": ("logging_in" if (running and (c.self_entity is None or c.current_map is None))
                  else ("running" if running else "stopped")),
        "hp": getattr(_ch, "hp", None), "sp": getattr(_ch, "sp", None),
        "hp_max": getattr(_ch, "hp_max", None), "sp_max": getattr(_ch, "sp_max", None),
    }


# Trung thanh pet duoi nguong nay -> canh bao CAM giong khi lech AGI. User chot 05/09.
# CHAT DUOI: 40 chua canh bao, 39 moi canh bao.
TRUNG_THANH_CANH_BAO = 40


def _trung_thanh_pet_dang_dung(c):
    """Trung thanh (0..100) cua pet DANG XUAT CHIEN, None neu chua biet.

    `client.pet_faith` khoa theo pet_id (doc tu goi 0x0f, offset +27) - phai tra dung con
    ACTIVE, khong duoc lay bua con dau danh sach. Chua xac nhan pet nao dang ra tran
    (`pet_name_out()` tra None) thi coi nhu chua biet, de khong canh bao oan.
    """
    try:
        if not c.pet_name_out():
            return None
        pid = getattr(getattr(c, "state", None), "active_pet_id", None)
        if not pid:
            return None
        v = (getattr(c, "pet_faith", None) or {}).get(int(pid))
        return int(v) if isinstance(v, int) else None
    except Exception:
        return None


def party_agi_report(pidx):
    """Chi tiet AGI char + pet active cua mot party; canh bao khi do lech > 10."""
    rows = []
    values = []
    for username, _password, _leader, _picker in party_accounts(pidx):
        status = account_status(username)
        char_agi = status.get("char_agi")
        pet_agi = status.get("pet_agi") if status.get("pet_name") else None
        if isinstance(char_agi, int):
            values.append(char_agi)
        if isinstance(pet_agi, int):
            values.append(pet_agi)
        faith = status.get("pet_faith") if status.get("pet_name") else None
        rows.append({
            "username": username,
            "char": status.get("char") or username,
            "char_agi": char_agi,
            "pet": status.get("pet_name") or "",
            "pet_agi": pet_agi,
            "pet_faith": faith,
        })
    low = min(values) if values else None
    high = max(values) if values else None
    spread = high - low if low is not None else None
    # TACH HAI loai canh bao, khong gop lam mot: nut hien "⚠ Check AGI (9)" - so 9 do la DO LECH
    # AGI. AGI on ma chi trung thanh thap thi hien so do la NOI DOI.
    faith_thap = [r["username"] for r in rows
                  if isinstance(r["pet_faith"], int) and r["pet_faith"] < TRUNG_THANH_CANH_BAO]
    lech_agi = spread is not None and spread > 10
    return {"rows": rows, "min": low, "max": high, "spread": spread,
            "warning": lech_agi,          # GIU nguyen nghia cu (chi lech AGI)
            "faith_thap": faith_thap,     # username co pet trung thanh < 40
            "canh_bao": lech_agi or bool(faith_thap)}   # -> to CAM cai nut


# ---- CACHE skill/pet theo account (de dialog Kich ban Skill dung duoc khi acc DA TAT) ----
# Chi phuc vu HIEN THI. Bot chay van doc du lieu THAT tu server (0x0f/0x13) - cache khong bao
# gio anh huong hanh vi. File nam canh accounts.json (PC) / files dir cua app (Android).
# ---- BO DO (outfit) theo ACCOUNT: luu file rieng canh accounts.json ----
# KHONG nhet vao accounts.json: bo do la du lieu do TUI DO quan ly (BagDialog chi co username +
# client, khong voi toi duoc hang cau hinh acc ben dialog party). Tach file thi ca GUI lan runner
# deu doc duoc, va sua bo do khong dung vao file chua mat khau.
def _outfits_path():
    try:
        from ._appdir import app_dir as _ad
        return os.path.join(_ad(), "outfits.json")
    except Exception:
        return "outfits.json"


def _migrate_outfits(row):
    """Bo do CU (chung ca char + pet) -> tach ra MOI DOI TUONG mot danh sach rieng.

    User chot 26/08: "moi con co save bo rieng". Ban cu luu {ten: {char:.., pets:{slot:..}}};
    ban moi luu {doi_tuong: {ten: {fit: tid}}} voi doi_tuong = "char" | "pet<slot>".
    Chuyen ngay khi doc de khong ai mat bo da luu.
    """
    if not row or all(k in ("char",) or str(k).startswith("pet") for k in row):
        return row or {}
    moi = {}
    for ten, bo in row.items():
        if not isinstance(bo, dict):
            continue
        if bo.get("char"):
            moi.setdefault("char", {})[str(ten)] = dict(bo["char"])
        for p, m in (bo.get("pets") or {}).items():
            if m:
                moi.setdefault("pet%s" % p, {})[str(ten)] = dict(m)
    return moi


def load_outfits(username=None, doi_tuong=None):
    """Bo do da luu.

    - khong username: {username: {doi_tuong: {ten: {fit: tid}}}}
    - co username: {doi_tuong: {ten: ...}}; them doi_tuong -> {ten: {fit: tid}}
    `doi_tuong` = "char" hoac "pet<slot>" (moi con mot danh sach rieng).
    """
    try:
        with open(_outfits_path(), encoding="utf-8") as fh:
            data = json.load(fh) or {}
    except Exception:
        data = {}
    out = {k: _migrate_outfits(v) for k, v in (data.get("accounts") or {}).items()}
    if username is None:
        return out
    row = out.get(str(username)) or {}
    if doi_tuong is None:
        return row
    return row.get(str(doi_tuong)) or {}


def save_outfit(username, doi_tuong, ten, bo):
    """Luu/ghi de mot bo do CUA MOT DOI TUONG. bo = None -> XOA bo do."""
    try:
        with open(_outfits_path(), encoding="utf-8") as fh:
            data = json.load(fh) or {}
    except Exception:
        data = {}
    accs = data.setdefault("accounts", {})
    accs[str(username)] = _migrate_outfits(accs.get(str(username)) or {})
    row = accs[str(username)]
    ds = row.setdefault(str(doi_tuong), {})
    if bo is None:
        ds.pop(str(ten), None)
    else:
        # Khoa JSON phai la CHUOI. fitType dang int -> json.dump tu doi thanh chuoi, nhung doc lai
        # se ra chuoi -> so sanh int(fit) o apply_outfit se lech neu khong ep. Ep ngay day.
        ds[str(ten)] = {str(k): int(v) for k, v in (bo or {}).items()}
    if not ds:
        row.pop(str(doi_tuong), None)
    if not row:
        accs.pop(str(username), None)
    tmp = _outfits_path() + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(data, fh, ensure_ascii=False, indent=1)
    os.replace(tmp, _outfits_path())
    return True


def _skill_cache_path():
    try:
        from ._appdir import app_dir as _ad
        return os.path.join(_ad(), "account_skills_cache.json")
    except Exception:
        return "account_skills_cache.json"


def _load_skill_cache():
    try:
        with open(_skill_cache_path(), encoding="utf-8") as fh:
            d = json.load(fh)
        return d if isinstance(d, dict) else {}
    except Exception:
        return {}


# Ghi cache nam trong bot/client.py (cho chac chan chay: _on_pet_list luc login + moi lan doi
# pet). O day chi DOC lai + dung chung ham dung snapshot de khong co 2 ban code.
from .client import save_skill_cache as save_account_skills_cache   # noqa: E402
from .client import skills_snapshot as _skills_snapshot             # noqa: E402


def account_inn_pets(username):
    """GUI/API: list pet trong NHA TRO cua acc, de render dialog chon pet van tieu.

    Tra {"pets": [[pet_id, ten], ...], "cached": 0/1}. Acc DANG CHAY -> lay LIVE tu roster server
    gui luc login; acc DA TAT -> lay CACHE lan chay gan nhat (user van tick duoc khi offline).
    Acc chua chay bao gio / khong co pet trong nha tro -> pets rong (caller hien thong bao).
    Thu tu = thu tu index nha tro; nhung KHOA la pet_id vi index xe dich khi them/bot pet."""
    username = str(username or "").strip()
    c = account_clients.get(username)
    roster = getattr(c, "vantieu_roster", None) if c is not None else None
    if roster:
        ids = getattr(c, "vantieu_roster_ids", {}) or {}
        return {"pets": [[int(ids.get(i, 0)), roster[i]] for i in sorted(roster)], "cached": 0}
    cached = _load_skill_cache().get(username)
    if isinstance(cached, dict) and cached.get("inn"):
        return {"pets": [[int(p), n] for p, n in cached["inn"]], "cached": 1}
    return {"pets": [], "cached": 1}


def account_skills(username):
    """GUI/API: skill + pet cua acc de render dialog Kich ban Skill.

    Acc DANG CHAY -> du lieu LIVE. Acc DA TAT -> lay CACHE cua lan chay gan nhat (kem "ts") de
    user van sua duoc config, khoi phai bat acc len chi de mo dialog. Cache CHI de hien thi.
    "pets": [[pid, ten, [choice...]], ...] cho tab per-pet; "active" = pet dang dung (dung de
    migrate config "pet" chung cu -> gan cho pet dang dung, pet khac auto).
    """
    c = account_clients.get(username)
    st = c.state if (c is not None and getattr(c, "state", None)) else None
    if st is not None:
        data = _skills_snapshot(st)
        save_account_skills_cache(username, data)   # tuoi -> cap nhat cache luon
        return data
    cached = _load_skill_cache().get(str(username or "").strip())
    if isinstance(cached, dict):
        return dict(cached, cached=1)
    return {"char": [], "pet": [], "pets": [], "active": 0}


def apply_account_battle(username, battle_config=None):
    """GUI/API: apply battle rule rieng acc NGAY cho acc dang chay, khong can relog.

    battle_config={} / None = ve mac dinh. Ham nay chi cap nhat runtime; GUI/Android van tu luu
    accounts.json/parties.json rieng nhu cu.
    """
    username = str(username or "").strip()
    if not username:
        return False
    if isinstance(battle_config, str):
        try:
            import json
            battle_config = json.loads(battle_config) if battle_config else {}
        except Exception:
            battle_config = {}
    cfg = battle_config if isinstance(battle_config, dict) else {}
    if not isinstance(getattr(config, "ACCOUNT_BATTLE", None), dict):
        config.ACCOUNT_BATTLE = {}
    if cfg:
        config.ACCOUNT_BATTLE[username] = cfg
    else:
        config.ACCOUNT_BATTLE.pop(username, None)
    config.ACCOUNT_CHAR_DEFEND.pop(username, None)
    c = account_clients.get(username)
    if c is not None and getattr(c, "state", None) is not None:
        c.state.battle_config = dict(cfg)
        c.state.char_defend = False
        log.info("[%s] da apply cau hinh skill/chien dau moi (live)", username)
        return True
    return False


def dangerous_npc_names():
    """GUI/API: danh sach NPC nguy hiem dung cho target battle `dangerous_npc`."""
    return list(getattr(config, "DANGEROUS_NPC_NAMES", []) or [])


def save_dangerous_npc_names(names):
    """GUI/API: luu danh sach NPC nguy hiem vao dangerous_npcs.json."""
    if isinstance(names, str):
        try:
            import json
            data = json.loads(names)
            if isinstance(data, dict):
                names = data.get("names", [])
            else:
                names = data
        except Exception:
            names = names.splitlines()
    try:
        saved = config.save_dangerous_npc_names(names)
        log.info("Da luu %d NPC nguy hiem vao dangerous_npcs.json", len(saved))
        return True
    except Exception as e:
        log.warning("Luu dangerous_npcs.json loi: %s", e)
        return False


def apply_account_heal(username, heal_config=None):
    """GUI/API: apply nguong hoi HP/SP rieng acc NGAY cho acc dang chay."""
    username = str(username or "").strip()
    if not username:
        return False
    if isinstance(heal_config, str):
        try:
            import json
            heal_config = json.loads(heal_config) if heal_config else {}
        except Exception:
            heal_config = {}
    cfg = {}
    if isinstance(heal_config, dict):
        for key in ("hp_char", "sp_char", "hp_pet", "sp_pet"):
            if key not in heal_config:
                continue
            try:
                cfg[key] = max(0.0, min(1.0, float(heal_config[key])))
            except Exception:
                pass
    if not isinstance(getattr(config, "ACCOUNT_HEAL", None), dict):
        config.ACCOUNT_HEAL = {}
    if cfg:
        config.ACCOUNT_HEAL[username] = cfg
    else:
        config.ACCOUNT_HEAL.pop(username, None)
    # VAN TIEU di chung heal_json vi cung mot dialog (bang setting Hoi HP/SP cua acc) VA vi duong
    # heal_json da duoc noi san o CA PC lan APK -> khong phai them tham so vi tri moi cho
    # setup_party_runtime (Kotlin goi theo VI TRI, them tham so giua chung la vo).
    vt = (heal_config or {}).get("vantieu") if isinstance(heal_config, dict) else None
    if not isinstance(getattr(config, "ACCOUNT_VANTIEU", None), dict):
        config.ACCOUNT_VANTIEU = {}
    if isinstance(vt, dict):
        pets = []
        for x in (vt.get("pets") or []):
            try:
                pets.append(int(x))
            except Exception:
                pass
        config.ACCOUNT_VANTIEU[username] = {"on": bool(vt.get("on", True)), "pets": pets}
    else:
        config.ACCOUNT_VANTIEU.pop(username, None)
    c = account_clients.get(username)
    if c is not None:
        _vt = config.ACCOUNT_VANTIEU.get(username) or {}
        c.vantieu_enable = bool(_vt.get("on", getattr(config, "VANTIEU_ENABLE", True)))
        c.vantieu_pick_ids = tuple(_vt.get("pets") or ())
    if username in account_clients:
        log.info("[%s] da apply nguong hoi HP/SP moi (live): %s | van tieu: %s",
                 username, cfg or "mac dinh", config.ACCOUNT_VANTIEU.get(username) or "mac dinh")
    return True


def apply_account_furnace(username, furnace_config=None):
    """GUI/API: apply config SOI LO rieng acc. furnace_config = {tab: {"on": bool, "items":
    {tid_hex/int: "auto"/"notify"}}} voi tab in vo_tuong/trang_bi/chuyen_sinh."""
    username = str(username or "").strip()
    if not username:
        return False
    if isinstance(furnace_config, str):
        try:
            import json
            furnace_config = json.loads(furnace_config) if furnace_config else {}
        except Exception:
            furnace_config = {}
    cfg = {}
    if isinstance(furnace_config, dict):
        for tab in ("vo_tuong", "trang_bi", "chuyen_sinh"):
            t = furnace_config.get(tab)
            if not isinstance(t, dict):
                continue
            items = {}
            for k, v in (t.get("items") or {}).items():
                if v not in ("auto", "notify"):
                    continue
                try:
                    tid = int(k, 16) if isinstance(k, str) and k.lower().startswith("0x") else int(k)
                    items[tid] = v
                except Exception:
                    pass
            if items:
                cfg[tab] = {"on": bool(t.get("on", True)), "items": items}
    if not isinstance(getattr(config, "ACCOUNT_FURNACE", None), dict):
        config.ACCOUNT_FURNACE = {}
    if cfg:
        config.ACCOUNT_FURNACE[username] = cfg
    else:
        config.ACCOUNT_FURNACE.pop(username, None)
    return True


def get_account_log(username, max_lines=500):
    """ANDROID: doc party.log -> loc rieng cac dong cua 1 acc, cho UI hien "log cua acc nay".
    QUAN TRONG: nhan log trong client.py (self._label) DOI TU username SANG TEN NHAN VAT ngay
    khi server tra ve (vd 'taot11' -> 'ttmot') - loc CHI theo username se BO SOT toan bo log
    sau thoi diem do (bug that: user thay log "dung lai" ngay sau dong "Ten nhan vat = ...").
    Loc theo CA username LAN char_name hien tai (qua account_status) de khong bo sot."""
    try:
        _st = account_status(username) or {}
        # NHAN LOG that su, KHONG phai char_name: hai acc khac server co the trung ten nhan vat,
        # luc do nhan la 'ten~username' con loc theo '[ten]' se HUT ca log cua acc kia (user 01/09:
        # "party 48 dung o quang truong ma cu bao battle"). Trung ten thi CHI loc theo nhan day du.
        nhan = _st.get("log_label") or _st.get("char") or ""
        tags = ["[%s]" % username]
        if nhan and nhan != username:
            tags.append("[%s]" % nhan)
        if not os.path.exists(_log_path):
            return "(chua co log - acc chua chay lan nao tren may nay)"
        # TAIL-READ: chi doc ~2MB CUOI file (khong quet ca file) -> mo log NHANH bat ke file to.
        # 2MB thua chua 500 dong cua 1 acc. Doc binary + seek de khoi nap ca file vao RAM.
        _tail = 2 * 1024 * 1024
        with open(_log_path, "rb") as f:
            f.seek(0, 2)
            _size = f.tell()
            _start = max(0, _size - _tail)
            f.seek(_start)
            _data = f.read()
        _text = _data.decode("utf-8", errors="replace")
        if _start > 0 and "\n" in _text:
            _text = _text.split("\n", 1)[1]   # bo dong dau bi cat giua chung
        lines = [ln for ln in _text.splitlines() if any(t in ln for t in tags)]
        if not lines:
            return "(chua co dong log nao cho '%s')" % username
        return "\n".join(lines[-max_lines:])
    except Exception as e:
        return "Loi doc log: %s" % e


def _run_cli():
    """Chay CLI nhu cu: khoi dong tat ca party roi cho den khi het acc / het gio."""
    import datetime as _dt
    n = start_all()
    log.info(">>> Party train dang chay (%d acc). %s",
             n, "vo han" if MINUTES == 0 else f"{MINUTES} phut")
    deadline = None if MINUTES == 0 else time.time() + MINUTES * 60
    try:
        # MIEN TRU vong cho diec: vong main: dem thread con song, khong cho acc nao ca.
        while True:
            time.sleep(5)
            if sum(1 for t in _threads if t.is_alive()) == 0:
                log.warning("=" * 60)
                log.warning(">>> TAT CA ACC DA THOAT GAME (%s). Khong con acc nao chay.",
                            _dt.datetime.now().strftime("%H:%M:%S"))
                log.warning(">>> Ly do thuong gap: sai map train / het gio DG / rot ket noi.")
                log.warning("=" * 60)
                break
            if deadline and time.time() >= deadline:
                log.info(">>> Het %d phut -> dong tat ca.", MINUTES)
                break
    except KeyboardInterrupt:
        log.info(">>> Nguoi dung dung (Ctrl+C).")
    stop_all(reason="CLI ket thuc / Ctrl+C")
    log.info(">>> Ket thuc.")


if __name__ == "__main__":
    _run_cli()
