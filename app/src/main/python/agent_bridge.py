import json
import threading
import traceback
import time
import base64
import os
import logging
import math

log = logging.getLogger(__name__)

_runner = None
_last_error = ""
_lock = threading.RLock()
_ground_ui_cache = {}
_account_settings_lock = threading.RLock()
_account_slots = {}
_selected_leader_user = None


def team_debug_json():
    """Bounded coordination snapshot: no passwords, auth or raw packet payloads."""
    import re
    from train_bot import config
    runner = _get_runner()
    st = runner._pstate(0)
    with st["lock"]:
        coordination = {}
        for key in ("dt_phase", "ui_dg_transition_pending", "ui_dg_users",
                    "ui_dg_train_target", "ui_train_target", "manual_train_users",
                    "train_channel_manual", "daily_active", "cmd_gen", "cmd",
                    "reform_gen", "n_members", "ui_train_phase", "ui_train_dispatch_gen", "manual_route_plan",
                    "manual_route_source_results", "manual_route_city_arrived"):
            value = st.get(key)
            coordination[key] = sorted(value) if isinstance(value, set) else value
        coordination["configured_mode"] = config.PARTY_CONFIG.get(0, {}).get("mode")
        for key in ("manual_route_plan_ready", "manual_route_party_ready", "manual_route_done"):
            event = st.get(key)
            coordination[key] = bool(event and event.is_set())
    accounts = []
    for user, _password, _leader, _pet in runner.party_accounts(0):
        c = runner.account_clients.get(user)
        state = runner.account_status(user) or {}
        task = runner.get_account_task(user) or {}
        accounts.append({"user": user, "online": bool(c and c.running),
                         "name": state.get("char", ""),
                         "area": config.map_display_name(int(getattr(c, "current_map", 0) or 0)),
                         "map": getattr(c, "current_map", None),
                         "position": getattr(c, "pos", None),
                         "channel": getattr(c, "current_channel", None),
                         "activity": str(task.get("task", "")),
                         "phase": str(task.get("phase", "")),
                         "waiting_seconds": task.get("elapsed", 0),
                         "party_members": len(getattr(c, "party_members", None) or []),
                         "party_invite_ready": bool(getattr(c, "party_invite_ready", False))})
    lines = []
    error = ""
    try:
        with open(runner._log_path, "rb") as f:
            f.seek(0, 2)
            offset = max(0, f.tell() - 256 * 1024)
            f.seek(offset)
            raw = f.read(256 * 1024).decode("utf-8", errors="replace")
        if offset:
            raw = raw.partition("\n")[2]
        sensitive = re.compile(r"password|passwd|access.?token|refresh.?token|authorization|bearer|credential|login.?packet|session.?key|0x(?:01|02)\b", re.I)
        lines = [line[:1500] for line in raw.splitlines()
                 if not sensitive.search(line) and "PET EXP probe" not in line][-300:]
    except FileNotFoundError:
        pass
    except Exception:
        error = "Không đọc được nhật ký team"
    return json.dumps({"schema": 1, "exported_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
                       "leader": config.PARTY_LEADER_ACC.get(0),
                       "coordination": coordination, "accounts": accounts,
                       "logs": lines, "error": error}, ensure_ascii=False, indent=2, default=str)


def server_packets_json():
    from train_bot.packet_trace import snapshot
    return json.dumps(snapshot(100), ensure_ascii=False, indent=2)


def switch_leader_json(username):
    """Online leader handover via verified leave/invite protocol, never guessed opcodes."""
    global _selected_leader_user
    runner = _get_runner()
    from train_bot import config
    st = runner._pstate(0)
    username = str(username or "").strip()
    old_user = config.PARTY_LEADER_ACC.get(0)
    live = _live_party(runner)
    clients = dict(live)
    old = clients.get(old_user)
    target = clients.get(username)
    if target is None:
        return json.dumps({"ok": False, "message": "Account được chọn phải online"}, ensure_ascii=False)
    if username == old_user:
        return json.dumps({"ok": True, "message": "Account này đã là leader"}, ensure_ascii=False)
    if any(c.in_team_dungeon() for c in clients.values()):
        return json.dumps({"ok": False, "message": "Hãy hoàn tất phụ bản trước khi đổi leader"}, ensure_ascii=False)
    # Chua co party: doi vai tro bot truc tiep, khong can ACC1 online hay SAFE.
    if not any(c.party_leader or c.party_members for c in clients.values()):
        with st["lock"]:
            if st.get("daily_active") or st.get("leader_switch_pending") or st.get("cmd"):
                return json.dumps({"ok": False, "message": "Hãy dừng/chờ luồng team hiện tại trước khi đổi leader"}, ensure_ascii=False)
            config.PARTY_LEADER_ACC[0] = username
            _save_leader(username)
            st["leader_manual_off"] = False
            st["leader_gone"].clear()
            st["reform_gen_thoa"] = st.get("reform_gen", 0)
        runner.reset_party_joined(0)
        return json.dumps({"ok": True, "message": "Đã chọn %s làm leader; không cần ACC1 online" % (target.char_name or username)}, ensure_ascii=False)
    if old is None:
        return json.dumps({"ok": False, "message": "Party vẫn còn trên server; hãy rời party trước khi chọn leader mới"}, ensure_ascii=False)
    safes = list((config.TRAIN_MAPS.get(old.current_map) or {}).get("safe") or [])
    if not safes:
        return json.dumps({"ok": False, "message": "Map hiện tại chưa có SAFE xác minh; chưa đổi leader để an toàn"}, ensure_ascii=False)
    if any(c.current_map != old.current_map or c.current_channel != old.current_channel
           for c in clients.values()):
        return json.dumps({"ok": False, "message": "Team phải cùng map và phân khu trước khi đổi leader"}, ensure_ascii=False)
    old_entity = bytes(old.self_entity or b"")
    old_roster = {bytes(e) for e in old.party_members or []} | {old_entity}
    if any(bytes(c.self_entity or b"") not in old_roster for c in clients.values()):
        return json.dumps({"ok": False, "message": "Hãy lập đủ party các account online trước khi đổi leader"}, ensure_ascii=False)
    with st["lock"]:
        if st.get("daily_active") or st.get("leader_switch_pending"):
            return json.dumps({"ok": False, "message": "Daily/đổi leader đang chạy; hãy chờ hoàn tất"}, ensure_ascii=False)
        st["leader_switch_pending"] = True
        st["cmd_gen"] += 1
        st["cmd"] = None
        st["kenh_dich"] = None
    combat_flags = {u: (bool(getattr(c, "_ui_auto_battle", False)), c.flee_mode)
                    for u, c in live}
    changed = False
    dissolved = False
    try:
        for c in clients.values():
            c._ui_auto_battle = False
            c.flee_mode = True
        deadline = time.time() + 120
        while any(c.in_combat(idle_secs=2.0) for c in clients.values()):
            if time.time() > deadline or any(not c.running for c in clients.values()):
                raise RuntimeError("Chưa hết trận hoặc có account disconnect; hủy đổi leader")
            time.sleep(1)
        if not old.pos:
            raise RuntimeError("Chưa có tọa độ leader cũ")
        safe = min(safes, key=lambda p: (p[0] - old.pos[0]) ** 2 + (p[1] - old.pos[1]) ** 2)
        if not old.navigate_to(int(safe[0]), int(safe[1]), flee=True, require_smart_path=True,
                               abort=lambda: any(not c.running for c in clients.values())):
            raise RuntimeError("Không xác nhận tới SAFE; hủy đổi leader")
        time.sleep(3)
        old.leave_party()
        dissolved = True
        deadline = time.time() + 20
        while any(c.party_leader or c.party_members for c in clients.values()):
            if time.time() > deadline:
                raise RuntimeError("Server chưa xác nhận giải tán party")
            time.sleep(1)
        runner.reset_party_joined(0)
        for c in clients.values():
            c.auto_accept_party = True
            c.set_party_invite_ready(True)
        deadline = time.time() + 90
        target_entity = bytes(target.self_entity or b"")
        while True:
            roster = {bytes(e) for e in target.party_members or []} | {target_entity}
            confirmed = (bytes(target.party_leader or b"") == target_entity
                         and all(bytes(c.self_entity or b"") in roster
                                 and bytes(c.party_leader or b"") == target_entity
                                 for c in clients.values()))
            if confirmed:
                break
            if time.time() > deadline or any(not c.running for c in clients.values()):
                raise RuntimeError("Party leader mới chưa được server xác nhận đủ thành viên")
            target.invite_members(gap=0.5)
            time.sleep(2)
        # Chot vai tro chi sau roster server xac nhan, khong doi vi tri cac slot UI.
        config.PARTY_LEADER_ACC[0] = username
        _save_leader(username)
        changed = True
        with st["lock"]:
            st["leader_manual_off"] = False
            st["leader_gone"].clear()
            st["reform_gen_thoa"] = st.get("reform_gen", 0)
        try:
            target.set_party_strategist()
        except Exception:
            pass
        message = "Đã đổi leader sang %s; team vẫn online tại SAFE" % (target.char_name or username)
    except Exception as exc:
        if dissolved and not changed:
            try:
                target.leave_party()
                time.sleep(2)
                old.invite_members(gap=0.5)
            except Exception:
                pass
        message = "Đổi leader chưa hoàn tất: %s. Giữ cấu hình leader cũ; kiểm tra party rồi bấm Bắt đầu farm." % exc
    finally:
        for u, c in live:
            c._ui_auto_battle, c.flee_mode = combat_flags[u]
        with st["lock"]:
            st["leader_switch_pending"] = False
    if changed and any(flags[0] for flags in combat_flags.values()):
        train = st.get("ui_train_target")
        if train:
            auto_battle_team_json(*train)
    return json.dumps({"ok": changed, "message": message}, ensure_ascii=False)

# EXP tuong server mobile: source PC xac nhan cong thuc
#   floor((level + 1) ** exp_power + 5)
# va moc UI YeuQuai lv155 xac nhan exp_power=3.1, required=6,290,520.
# Dung moc tong dau cap lv155 da do truc tiep lam neo, sau do tinh tien/lui de
# tranh lam mat offset lich su 10,440 EXP cua server mobile so voi bang PC.
_CHAR_EXP_ANCHOR_LEVEL = 155
_CHAR_EXP_ANCHOR_BASE = 236222752
_CHAR_EXP_POWER = 3.1


def _build_char_exp_levels(max_level=200):
    required = {
        level: int(math.pow(level + 1, _CHAR_EXP_POWER) + 5)
        for level in range(1, max_level + 1)
    }
    base = {_CHAR_EXP_ANCHOR_LEVEL: _CHAR_EXP_ANCHOR_BASE}
    for level in range(_CHAR_EXP_ANCHOR_LEVEL - 1, 0, -1):
        base[level] = base[level + 1] - required[level]
    for level in range(_CHAR_EXP_ANCHOR_LEVEL + 1, max_level + 1):
        base[level] = base[level - 1] + required[level - 1]
    return {level: (base[level], required[level]) for level in range(1, max_level + 1)}


_CHAR_EXP_LEVELS = _build_char_exp_levels()

# EXP pet thuong cua TrueBot PC (ModExp.SetExpNormal), du cap 0..201.
# Moi phan tu la EXP can trong cap do; tong tich luy dau cap duoc tinh mot lan
# ben duoi. Packet 0x0f/sub0008 tra tong EXP pet, vi vay UI can tru moc dau cap.
_PET_NORMAL_EXP_REQUIRED = (
    -6, 12, 29, 61, 111, 186, 287, 421,
    590, 799, 1052, 1353, 1705, 2113, 2579, 3109,
    3706, 4373, 5115, 5934, 6835, 7822, 8897, 10065,
    11330, 12694, 14161, 15736, 17421, 19220, 21137, 23175,
    25338, 27629, 30052, 32609, 35306, 38144, 41128, 44261,
    47547, 50988, 54588, 58351, 62280, 66379, 70650, 75098,
    79725, 84535, 89532, 94718, 100097, 105673, 111448, 117426,
    123610, 130004, 136611, 143435, 150477, 157743, 165235, 172956,
    180909, 189099, 197528, 206199, 215116, 224282, 233700, 243373,
    253306, 263500, 273959, 284687, 295686, 306960, 318512, 330345,
    342462, 354868, 367563, 380553, 393841, 407428, 421319, 435517,
    450024, 464845, 479982, 495438, 511216, 527321, 543754, 560519,
    577619, 595058, 612838, 630962, 649434, 668257, 687434, 706968,
    726862, 747119, 767743, 788736, 810102, 831843, 853963, 876465,
    899352, 922628, 946294, 970354, 994812, 1019671, 1044933, 1070601,
    1096679, 1123170, 1150076, 1177402, 1205149, 1233322, 1261922, 1290953,
    1320419, 1350322, 1380665, 1411451, 1442684, 1474366, 1506501, 1539091,
    1572139, 1605649, 1639624, 1674066, 1708979, 1744365, 1780228, 1816571,
    1853397, 1890708, 1928508, 1966799, 2005586, 2044870, 2084655, 2124944,
    2165739, 2207044, 2248862, 2291196, 2334049, 2377423, 2421322, 2465749,
    2510706, 2556197, 2602225, 2648793, 2695903, 2743559, 2791763, 2840519,
    2889829, 2939697, 2990126, 3041118, 3092676, 3144803, 3197503, 3250779,
    3304632, 3359067, 3414086, 3469692, 3525888, 3582677, 3640062, 3698046,
    3756631, 3815822, 3875620, 3936029, 3997052, 4058691, 4120949, 4183831,
    4247337, 4311472, 4376237, 4441637, 4507674, 4574351, 4641671, 4709637,
    4778252, 4847518,
)

# Pet chuyen sinh trong TrueBot: ID 45000..45999 dung bang Exp2.
_PET_REBORN_EXP_REQUIRED = (
    -6, 13, 32, 69, 130, 221, 348, 517,
    734, 1005, 1336, 1733, 2202, 2749, 3380, 4101,
    4918, 5837, 6864, 8005, 9266, 10653, 12172, 13829,
    15630, 17581, 19688, 21957, 24394, 27005, 29796, 32773,
    35942, 39309, 42880, 46661, 50658, 54877, 59324, 64005,
    68926, 74093, 79512, 85189, 91130, 97341, 103828, 110597,
    117654, 125005, 132656, 140613, 148882, 157469, 166380, 175621,
    185198, 195117, 205384, 216005, 226986, 238333, 250052, 262149,
    274630, 287501, 300768, 314437, 328514, 343005, 357916, 373253,
    389022, 405229, 421880, 438981, 456538, 474557, 493044, 512005,
    531446, 551373, 571792, 592709, 614130, 636061, 658508, 681477,
    704974, 729005, 753576, 778693, 804362, 830589, 857380, 884741,
    912678, 941197, 970304, 1000005, 1030306, 1061213, 1092732, 1124869,
    1157630, 1191021, 1225048, 1259717, 1295034, 1331005, 1367636, 1404933,
    1442902, 1481549, 1520880, 1560901, 1601618, 1643037, 1685164, 1728005,
    1771566, 1815853, 1860872, 1906629, 1953130, 2000381, 2048388, 2097157,
    2146694, 2197005, 2248096, 2299973, 2352642, 2406109, 2460380, 2515461,
    2571358, 2628077, 2685624, 2744005, 2803226, 2863293, 2924212, 2985989,
    3048630, 3112141, 3176528, 3241797, 3307954, 3375005, 3442956, 3511813,
    3581582, 3652269, 3723880, 3796421, 3869898, 3944317, 4019684, 4096005,
    4173286, 4251533, 4330752, 4410949, 4492130, 4574301, 4657468, 4741637,
    4826814, 4913005, 5000216, 5088453, 5177722, 5268029, 5359380, 5451781,
    5545238, 5639757, 5735344, 5832005, 5929746, 6028573, 6128492, 6229509,
    6331630, 6434861, 6539208, 6644677, 6751274, 6859005, 6967876, 7077893,
    7189062, 7301389, 7414880, 7529541, 7645378, 7762397, 7880604, 8000005,
    8120606, 8242413,
)


def _build_pet_exp_levels(required_by_level):
    levels = {}
    cumulative = 0
    for level, required in enumerate(required_by_level):
        if level == 0:
            levels[level] = (0, required)
            continue
        # TrueBot Getexp: required - (cumulative_end - raw_total) - 6.
        # Do do moc tru tong EXP o dau cap la cumulative + 6.
        levels[level] = (cumulative + 6, required)
        cumulative += required
    return levels


_PET_EXP_LEVELS = _build_pet_exp_levels(_PET_NORMAL_EXP_REQUIRED)
_PET_REBORN_EXP_LEVELS = _build_pet_exp_levels(_PET_REBORN_EXP_REQUIRED)

# Hai moc server mobile da doi chieu truc tiep voi UI game. Giu lai base cua
# server mobile de EXP hien tai tai cap 174/184 khong bi lech voi ban PC cu.
_PET_EXP_LEVELS.update({
    174: (141883942, 3197503),
    184: (176326366, 3756631),
})

_TEAM_DUNGEON_NAMES = {20: "Thảo Phạt Thiên Sư", 50: "Ngày Tàn Hoạn Quan",
                       80: "Đại Chiến Lữ Bố", 110: "Hỏa Thiêu Bộc Dương"}
_DAILY_AREA_NAMES = {"boss quan doan": "Boss Quân Đoàn", "boss the gioi": "Boss Thế Giới",
                     "pho ban don": "Khiêu Chiến Đậu Đậu"}

def _pet_level_exp_values(client):
    if client is None:
        return None, None, None, None, None
    state = getattr(client, "state", None)
    # UI phai bam theo pet dang chon, ke ca trong khoang ngan server chua tra
    # packet doi pet; fallback ve pet dang xuat chien neu chua co lua chon.
    pid = int(getattr(client, "_ui_selected_pet_id", 0)
              or getattr(state, "active_pet_id", 0) or 0)
    slot = int(getattr(client, "active_pet_slot", 0) or 0)
    levels = getattr(client, "pet_levels", {}) or {}
    values = getattr(client, "pet_exp_values", {}) or {}
    level = int(levels.get(pid, 0) or 0)
    current = values.get(pid, values.get(slot))
    # TrueBot dung Exp2 cho pet ID 45000..45999, pet con lai dung Exp thuong.
    exp_levels = _PET_REBORN_EXP_LEVELS if 45000 <= pid <= 45999 else _PET_EXP_LEVELS
    row = exp_levels.get(level)
    base, required = row if row is not None else (None, None)
    if current is None:
        return pid or None, level or None, None, required, None
    raw_total = max(0, int(current))
    current = max(0, raw_total - int(base)) if base is not None else None
    log.info("PET EXP probe pid=0x%x lv=%s raw_total=%s base=%s current=%s required=%s remaining=%s",
             pid, level, raw_total, base, current, required,
             max(0, required - current) if required is not None and current is not None else None)
    return pid or None, level or None, current, required, (max(0, required - current) if required is not None and current is not None else None)



def _level_exp_values(client):
    level = int(getattr(client, "char_level", 0) or 0)
    total = getattr(client, "char_exp", None)
    row = _CHAR_EXP_LEVELS.get(level)
    if total is None or row is None:
        return None, None, None
    current = max(0, int(total) - int(row[0]))
    required = int(row[1])
    return current, required, max(0, required - current)


def _account_settings_path():
    from train_bot import config
    return os.path.join(config._base_dir(), "android_account_settings.json")


def _load_account_settings():
    try:
        with open(_account_settings_path(), encoding="utf-8") as fh:
            data = json.load(fh)
            return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _save_account_setting(username, value):
    with _account_settings_lock:
        data = _load_account_settings()
        previous = data.get(str(username), {})
        data[str(username)] = {**(previous if isinstance(previous, dict) else {}), **value}
        path = _account_settings_path()
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(data, fh, ensure_ascii=False, indent=2)
        os.replace(tmp, path)


def _restore_account_settings(username):
    saved = _load_account_settings().get(str(username), {})
    if not isinstance(saved, dict):
        return {}
    from train_bot import config
    heal, battle = saved.get("heal") or {}, saved.get("battle") or {}
    if heal:
        config.ACCOUNT_HEAL[str(username)] = dict(heal)
    if battle:
        config.ACCOUNT_BATTLE[str(username)] = dict(battle)
    if not isinstance(getattr(config, "ACCOUNT_SELECTED_PET", None), dict):
        config.ACCOUNT_SELECTED_PET = {}
    config.ACCOUNT_SELECTED_PET[str(username)] = int(saved.get("pet_id", 0) or 0)
    config.ACCOUNT_PHUC_THAN[str(username)] = bool(saved.get("use_phuc_than", False))
    config.ACCOUNT_DAI_PHUC_THAN[str(username)] = bool(saved.get("use_dai_phuc_than", False))
    if not isinstance(getattr(config, "ACCOUNT_DG_HO_PHU", None), dict):
        config.ACCOUNT_DG_HO_PHU = {}
    if not isinstance(getattr(config, "ACCOUNT_AUTO_BAO_HOP", None), dict):
        config.ACCOUNT_AUTO_BAO_HOP = {}
    config.ACCOUNT_DG_HO_PHU[str(username)] = bool(saved.get("use_digioi_ho_phu", False))
    config.ACCOUNT_AUTO_BAO_HOP[str(username)] = bool(saved.get("auto_buy_bao_hop", False))
    if not isinstance(getattr(config, "ACCOUNT_DEATH", None), dict):
        config.ACCOUNT_DEATH = {}
    config.ACCOUNT_DEATH[str(username)] = dict(saved.get("death_return") or {"character": True, "pet": True})
    return saved


def apply_death_settings_json(username, character=True, pet=True):
    try:
        username = str(username or "").strip()
        if not username:
            raise ValueError("Chưa có account")
        from train_bot import config
        death = {"character": bool(character), "pet": bool(pet)}
        with _account_settings_lock:
            saved = _load_account_settings().get(username, {})
            saved["death_return"] = death
            _save_account_setting(username, saved)
        if not isinstance(getattr(config, "ACCOUNT_DEATH", None), dict):
            config.ACCOUNT_DEATH = {}
        config.ACCOUNT_DEATH[username] = death
        client = _get_runner().account_clients.get(username)
        if client is not None:
            client.death_return_town = death["character"]
            client.pet_death_return_town = death["pet"]
            if client.running:
                client.sync_machinebox_flags()
        return json.dumps({"ok": True})
    except Exception as e:
        return json.dumps({"ok": False, "message": str(e)}, ensure_ascii=False)


def _get_runner():
    global _runner
    if _runner is None:
        from train_bot import run_party_digioi
        _runner = run_party_digioi
    return _runner


def _save_leader(username):
    global _selected_leader_user
    _selected_leader_user = username
    _save_account_setting("__team__", {"leader": username})


def catalog_json():
    from train_bot import config
    maps = []
    for map_id, item in sorted(config.TRAIN_MAPS.items(), key=lambda x: (x[1].get("group", ""), x[0])):
        maps.append({"id": map_id, "name": item.get("name") or str(map_id), "group": item.get("group", ""), "mobs": item.get("mobs", [])})
    servers = []
    for key, item in config.SERVERS.items():
        servers.append({"key": key, "label": item.get("label", key), "ip": item.get("ip", ""), "id": int(item.get("id", 1))})
    return json.dumps({"maps": maps, "servers": servers}, ensure_ascii=False)


def skills_json(username):
    try:
        data = _get_runner().account_skills(str(username or "").strip())
        return json.dumps({"ok": True, "data": data}, ensure_ascii=False)
    except Exception as exc:
        return json.dumps({"ok": False, "message": "%s: %s" % (type(exc).__name__, exc)}, ensure_ascii=False)


def apply_combat_settings_json(username, pet_id=0, char_skill=0, pet_skill=0,
                               hp_percent=70, sp_percent=70, use_phuc_than=False,
                               char_mob_min=4, pet_mob_min=4, use_dai_phuc_than=False,
                               pet_hp_percent=None, pet_sp_percent=None,
                               use_digioi_ho_phu=False, auto_buy_bao_hop=False):
    """Apply pet, skill va nguong dung item ngay cho account dang online."""
    try:
        username = str(username or "").strip()
        pet_id, char_skill, pet_skill = int(pet_id or 0), int(char_skill or 0), int(pet_skill or 0)
        hp = max(0, min(100, int(hp_percent))) / 100.0
        sp = max(0, min(100, int(sp_percent))) / 100.0
        pet_hp = max(0, min(100, int(hp_percent if pet_hp_percent is None else pet_hp_percent))) / 100.0
        pet_sp = max(0, min(100, int(sp_percent if pet_sp_percent is None else pet_sp_percent))) / 100.0
        char_mob_min = max(1, min(10, int(char_mob_min or 4)))
        pet_mob_min = max(1, min(10, int(pet_mob_min or 4)))
        runner = _get_runner()
        client = runner.account_clients.get(username)
        if client is None or not getattr(client, "running", False):
            raise RuntimeError("Account chua online")
        skills = runner.account_skills(username)
        char_ids = {int(row[0]) for row in (skills.get("char") or [])}
        pet_rows = {int(row[0]): row for row in (skills.get("pets") or [])}
        effective_pet_id = pet_id or int(skills.get("active") or 0)
        if char_skill > 0 and char_skill not in char_ids:
            raise RuntimeError("Nhan vat chua hoc skill da chon")
        if pet_id and pet_id not in pet_rows:
            raise RuntimeError("Pet da chon khong nam trong danh sach mang theo")
        pet_ids = {int(row[0]) for row in (pet_rows.get(effective_pet_id, [0, "", []])[2] or [])}
        if pet_skill > 0 and pet_skill not in pet_ids:
            raise RuntimeError("Pet nay chua co skill da chon")
        char_rules = ([{"enabled": True, "condition": "always", "skill": "normal", "target": "auto"}]
                      if char_skill < 0 else [{"enabled": True, "condition": "mob", "op": "gte",
                        "value": char_mob_min, "skill": char_skill, "target": "auto"},
                       {"enabled": True, "condition": "always", "skill": "normal",
                        "target": "auto"}] if char_skill else [])
        pet_rules = ([{"enabled": True, "condition": "always", "skill": "normal", "target": "auto"}]
                     if pet_skill < 0 else [{"enabled": True, "condition": "mob", "op": "gte",
                       "value": pet_mob_min, "skill": pet_skill, "target": "auto"},
                      {"enabled": True, "condition": "always", "skill": "normal",
                       "target": "auto"}] if pet_skill else [])
        battle = {"char": char_rules,
                  "pets": {str(effective_pet_id): pet_rules} if effective_pet_id and pet_rules else {}}
        runner.apply_account_battle(username, battle)
        runner.apply_account_heal(username, {"hp_char": hp, "sp_char": sp,
                                             "hp_pet": pet_hp, "sp_pet": pet_sp})
        from train_bot import config
        config.ACCOUNT_PHUC_THAN[username] = bool(use_phuc_than)
        config.ACCOUNT_DAI_PHUC_THAN[username] = bool(use_dai_phuc_than)
        if not isinstance(getattr(config, "ACCOUNT_DG_HO_PHU", None), dict): config.ACCOUNT_DG_HO_PHU = {}
        if not isinstance(getattr(config, "ACCOUNT_AUTO_BAO_HOP", None), dict): config.ACCOUNT_AUTO_BAO_HOP = {}
        config.ACCOUNT_DG_HO_PHU[username] = bool(use_digioi_ho_phu)
        config.ACCOUNT_AUTO_BAO_HOP[username] = bool(auto_buy_bao_hop)
        client.use_digioi_ho_phu = bool(use_digioi_ho_phu)
        if not isinstance(getattr(config, "ACCOUNT_SELECTED_PET", None), dict):
            config.ACCOUNT_SELECTED_PET = {}
        config.ACCOUNT_SELECTED_PET[username] = pet_id
        client._ui_selected_pet_id = pet_id
        _save_account_setting(username, {
            "pet_id": pet_id, "char_skill": char_skill, "pet_skill": pet_skill,
            "char_mob_min": char_mob_min, "pet_mob_min": pet_mob_min,
            "heal": {"hp_char": hp, "sp_char": sp, "hp_pet": pet_hp, "sp_pet": pet_sp},
            "battle": battle, "use_phuc_than": bool(use_phuc_than),
            "use_dai_phuc_than": bool(use_dai_phuc_than),
            "use_digioi_ho_phu": bool(use_digioi_ho_phu),
            "auto_buy_bao_hop": bool(auto_buy_bao_hop),
        })

        if auto_buy_bao_hop:
            threading.Thread(target=lambda: client.buy_trieu_goi_bao_hop(0),
                             name="buy-bao-hop-%s" % username, daemon=True).start()

        def switch_selected_pet():
            if pet_id:
                try:
                    client._wait_combat_clear(idle=2.0, cap=30.0)
                    client.switch_pet(pet_id)
                except Exception:
                    traceback.print_exc()
        if pet_id:
            threading.Thread(target=switch_selected_pet, name="pet-%s" % username, daemon=True).start()
        return json.dumps({"ok": True, "message":
                           "Da ap dung: NV dung skill tu %d quai, pet tu %d quai; it hon danh thuong. Pet %s, skill NV %s, skill pet %s, HP %d%%, SP %d%%, Phuc Than %s, Dai Phuc Than %s" %
                           (char_mob_min, pet_mob_min,
                            pet_id or "tu dong", char_skill or "tu dong", pet_skill or "tu dong",
                            int(hp * 100), int(sp * 100), "BAT" if use_phuc_than else "TAT",
                            "BAT" if use_dai_phuc_than else "TAT")}, ensure_ascii=False)
    except Exception as exc:
        return json.dumps({"ok": False, "message": "%s" % exc}, ensure_ascii=False)


def account_action_json(username, action, payload="{}"):
    """Allowlisted, account-scoped shop/bag/furnace actions for Android."""
    try:
        username, action = str(username or "").strip(), str(action or "").strip()
        data = json.loads(str(payload or "{}"))
        client = _get_runner().account_clients.get(username)
        if client is None or not getattr(client, "running", False):
            raise RuntimeError("Account chưa online")
        if action in {"buy_ho_phu", "buy_bao_hop", "gacha_pet", "gacha_card",
                      "furnace_buy", "combine", "discard"} and client.in_combat(idle_secs=1.0):
            raise RuntimeError("Đang trong trận, hãy chờ kết thúc trận rồi thử lại")
        if action == "buy_ho_phu":
            client.buy_di_gioi_ho_phu(); message = "Đã gửi mua Dị Giới Hộ Phù"
        elif action == "buy_bao_hop":
            client.buy_trieu_goi_bao_hop(0); message = "Đã kiểm tra và gửi mua Túi Triệu Gọi"
        elif action == "gacha_pet":
            if not client.claim_gacha_pet(): raise RuntimeError("Không đủ tiền đồng để rút thẻ pet")
            message = "Đã rút thẻ pet"
        elif action == "gacha_card":
            if not client.claim_gacha_card(): raise RuntimeError("Không đủ tiền đồng để rút thẻ tướng")
            message = "Đã rút thẻ tướng"
        elif action == "combine":
            client.combine_slots(int(data.get("first", 0)), int(data.get("second", 0)))
            message = "Đã gửi hợp hai vật phẩm đã chọn"
        elif action == "discard":
            slot = int(data.get("slot", 0)); qty = max(1, int(data.get("qty", 1)))
            if not client.discard_item(slot, qty): raise RuntimeError("Vật phẩm đang khóa hoặc server không nhận")
            message = "Đã gửi vứt vật phẩm"
        elif action == "toggle_lock":
            slot, locked = int(data.get("slot", 0)), bool(data.get("locked", True))
            client.set_item_lock(slot, locked)
            message = "Đã khóa bảo vệ vật phẩm" if locked else "Đã mở khóa bảo vệ vật phẩm"
        elif action == "furnace_scan":
            if not client.scan_furnace(): raise RuntimeError("Server chưa trả dữ liệu lò")
            message = "Đã tải dữ liệu lò"
        elif action == "furnace_buy":
            kind, slot, item_id = int(data.get("kind", 0)), int(data.get("slot", 0)), int(data.get("item_id", 0))
            row = next((x for x in ((client.furnace_shop or {}).get("tabs", {}).get(kind, []) or [])
                        if int(x.get("index", 0)) == slot and int(x.get("id", 0)) == item_id), None)
            if row is None: raise RuntimeError("Mặt hàng đã thay đổi; hãy tải lại lò")
            if row.get("bought"): raise RuntimeError("Mặt hàng này đã mua")
            if not client.buy_furnace_item(kind, slot, item_id): raise RuntimeError("Server chưa xác nhận mua")
            message = "Đã gửi mua vật phẩm trong lò"
        else:
            raise RuntimeError("Thao tác không hợp lệ")
        return json.dumps({"ok": True, "message": message}, ensure_ascii=False)
    except Exception as exc:
        return json.dumps({"ok": False, "message": str(exc)}, ensure_ascii=False)


def start_json(payload):
    global _last_error
    try:
        data = json.loads(str(payload))
        accounts = []
        for a in data.get("accounts", []):
            if a.get("on", True) and str(a.get("u", "")).strip():
                username = str(a.get("u", "")).strip()
                _account_slots[username] = max(0, min(4, int(a.get("slot", len(_account_slots)))))
                saved = _restore_account_settings(username)
                battle = saved.get("battle") or {
                    "char": {"mode": "skill", "skill": int(a.get("char_skill", 0) or 0)},
                    "pet": {"mode": "skill", "skill": int(a.get("pet_skill", 0) or 0)}}
                accounts.extend([str(a.get("u", "")).strip(), str(a.get("p", "")), json.dumps(battle), "{}", "{}"])
        if not accounts:
            return json.dumps({"ok": False, "message": "Chưa có tài khoản được bật"}, ensure_ascii=False)
        server = data["server"]
        runner = _get_runner()
        only_user = str(data.get("only_user", "") or "").strip()
        # LOGIN RIENG khi party dang chay la REJOIN, khong phai tao session moi. Payload Android
        # cua nut Login co chu y mode=stand/map_id=0; neu dem no setup_party_runtime thi ghi de
        # mode/map train dang chay, coordinator keo ca leader ve thanh mac dinh (12061 Truong Sa)
        # roi dung im vi khong con route train. Giu nguyen toan bo runtime va chi cap nhat mat khau
        # + start dung account vua out.
        session_live = any(runner.is_account_running(row[0]) for row in runner.party_accounts(0))
        if session_live and not only_user:
            # LOGIN ALL chi bo sung acc offline; khong restart team hay reset bai farm.
            results = []
            skipped = 0
            for a in data.get("accounts", []):
                u = str(a.get("u", "")).strip()
                if not u or not a.get("on", True):
                    continue
                if runner.is_account_running(u):
                    skipped += 1
                    continue
                request = dict(data, only_user=u)
                results.append(json.loads(start_json(json.dumps(request))))
            failed = [r.get("message", "Lỗi khởi động") for r in results if not r.get("ok")]
            return json.dumps({"ok": not failed, "message":
                               "LOGIN ALL: khởi động %d account; giữ nguyên %d account đang chạy%s" %
                               (sum(bool(r.get("ok")) for r in results), skipped,
                                "; " + "; ".join(failed) if failed else "")}, ensure_ascii=False)
        if only_user and session_live:
            from train_bot import config
            requested = next((a for a in data.get("accounts", [])
                              if str(a.get("u", "")).strip() == only_user), None)
            if requested is None:
                return json.dumps({"ok": False, "message": "Khong tim thay account trong payload"},
                                  ensure_ascii=False)
            party = list(config.PARTIES[0]) if config.PARTIES else []
            found = False
            for i, row in enumerate(party):
                if row[0] == only_user:
                    party[i] = (only_user, str(requested.get("p", "")))
                    found = True
                    break
            if not found:
                party.append((only_user, str(requested.get("p", ""))))
            config.PARTIES[0] = party
            config.ACCOUNTS = [a for group in config.PARTIES for a in group if a and a[0]]
            config.ACCOUNT_PARTY[only_user] = 0
            match = next((row for row in runner.party_accounts(0) if row[0] == only_user), None)
            if match is None:
                return json.dumps({"ok": False, "message": "Account khong co trong cau hinh party"},
                                  ensure_ascii=False)
            started = 1 if runner.start_account(match[0], match[1], 0, match[2], match[3]) else 0
            st = runner._pstate(0)
            with st["lock"]:
                # Bao dieu phoi co thanh vien vua rejoin. ui_train_target/mode/kenh giu nguyen;
                # coordinator se gom cung map/kenh leader, moi lai PT va tiep tuc bai da luu.
                st["disc_gen"] += 1
                if bool(match[2]):
                    st["leader_manual_off"] = False
                    st["leader_gone"].clear()
            _last_error = ""
            return json.dumps({"ok": started > 0, "message":
                               "Da rejoin %s vao phien hien tai; giu nguyen bai train/phan khu, "
                               "dang cho leader moi lai PT" % only_user}, ensure_ascii=False)
        map_id = int(data.get("map_id", 0) or 0)
        farm_x = int(data.get("farm_x", 0) or 0)
        farm_y = int(data.get("farm_y", 0) or 0)
        mob_index = int(data.get("mob_index", -1))
        if map_id and farm_x and farm_y:
            from train_bot import config
            entry = config.TRAIN_MAPS.setdefault(map_id, {"safe": [], "mobs": [], "name": str(map_id), "group": "Tùy chỉnh"})
            entry["mobs"] = [(farm_x, farm_y)]
            mob_index = 0
        # Leader Android la account o SLOT 1 (index 0), khong phai "account dau tien con lai".
        # Neu login rieng ACC5 trong mot phien moi, setup_party_runtime truoc day tu thang ACC5
        # thanh leader vi no la phan tu dau payload -> UI ACC5 hien nham Ban do.
        leader_configured = any(
            str(a.get("u", "")).strip() and int(a.get("slot", -1)) == 0
            for a in data.get("accounts", [])
        )
        runner.setup_party_runtime(
            0, str(data.get("mode", "stand")), str(server["ip"]), int(server["id"]),
            "\x01".join(accounts), start_city_id=map_id,
            mob_index=mob_index, do_daily=False,
            has_leader=leader_configured,
            auto_world_boss=False, auto_team_dungeon=False, do_van_tieu=False,
            fight_legion_boss=False,
            di_gioi_level=max(1, min(15, int(data.get("di_gioi_level", 2) or 2))),
            auto_sell_noi_dat=False, auto_bag_clean=False, auto_discard_junk=False,
            auto_donate_materials=False, death_return_town=True, pet_death_return_town=True)
        from train_bot import config
        configured_users = {a[0] for a in config.PARTIES[0]}
        default_leader = next((str(a.get("u", "")).strip() for a in data.get("accounts", [])
                               if int(a.get("slot", -1)) == 0 and str(a.get("u", "")).strip()), None)
        global _selected_leader_user
        if _selected_leader_user is None:
            _selected_leader_user = (_load_account_settings().get("__team__") or {}).get("leader")
        config.PARTY_LEADER_ACC[0] = (_selected_leader_user if _selected_leader_user in configured_users
                                    else default_leader)
        channel = int(data.get("channel", 0) or 0)
        if channel > 0:
            st = runner._pstate(0)
            with st["lock"]:
                st["kenh_ghim"] = channel
        if only_user:
            match = next((row for row in runner.party_accounts(0) if row[0] == only_user), None)
            if match is None:
                return json.dumps({"ok": False, "message": "Account khong co trong cau hinh party"}, ensure_ascii=False)
            started = 1 if runner.start_account(match[0], match[1], 0, match[2], match[3]) else 0
        else:
            started = runner.start_party(0, stagger=1.5)
        _last_error = ""
        return json.dumps({"ok": started > 0, "message": "Đã khởi động %d tài khoản" % started}, ensure_ascii=False)
    except Exception as exc:
        _last_error = "%s: %s" % (type(exc).__name__, exc)
        traceback.print_exc()
        return json.dumps({"ok": False, "message": _last_error}, ensure_ascii=False)


def stop_all():
    global _last_error
    try:
        _get_runner().stop_all("Android Web UI")
        return json.dumps({"ok": True, "message": "Đã gửi lệnh dừng"}, ensure_ascii=False)
    except Exception as exc:
        _last_error = "%s: %s" % (type(exc).__name__, exc)
        return json.dumps({"ok": False, "message": _last_error}, ensure_ascii=False)


def safe_logout_all_json():
    """Logout ca team theo luong SAFE cua tung account; member truoc, leader sau."""
    try:
        runner = _get_runner()
        rows = list(runner.party_accounts(0))
        running = []
        for username, _password, is_leader, _strategist in rows:
            client = runner.account_clients.get(username)
            if client is not None and getattr(client, "running", False):
                running.append((username, client, bool(is_leader)))
        if not running:
            return json.dumps({"ok": True, "message": "Không có account online để logout"}, ensure_ascii=False)
        # Member ve SAFE truoc. Leader logout sau cung de party khong bi mat dau keo som.
        running.sort(key=lambda row: row[2])
        def logout_in_order():
            members = [row for row in running if not row[2]]
            for username, client, _ in members:
                client._individual_safe_logout = True
                client._wait_leader_on_stop = False
                client._individual_safe_logout_is_leader = False
                runner.stop_account(username, reason="Android: LOGOUT ALL an toan")
            deadline = time.monotonic() + 180
            while any(runner.is_account_running(u) or getattr(c, "running", False) for u, c, _ in members):
                if time.monotonic() >= deadline:
                    log.warning("LOGOUT ALL: member chưa OUT; giữ leader online, hãy thử lại")
                    return
                time.sleep(0.5)
            for username, client, is_leader in running:
                if is_leader:
                    client._individual_safe_logout = True
                    client._wait_leader_on_stop = False
                    client._individual_safe_logout_is_leader = True
                    runner.stop_account(username, reason="Android: LOGOUT ALL member da OUT")
        threading.Thread(target=logout_in_order, name="safe-logout-all", daemon=True).start()
        return json.dumps({"ok": True, "message": "Đã gửi LOGOUT ALL: %d account sẽ về SAFE rồi thoát" % len(running)}, ensure_ascii=False)
    except Exception as exc:
        return json.dumps({"ok": False, "message": "%s" % exc}, ensure_ascii=False)


def stop_one_json(username):
    try:
        username = str(username or "").strip()
        if not username:
            raise ValueError("Slot chua co username")
        runner = _get_runner()
        client = runner.account_clients.get(username)
        configured = next((row for row in runner.party_accounts(0) if row[0] == username), None)
        is_leader = bool(configured and configured[2])
        if client is not None and getattr(client, "running", False):
            client._individual_safe_logout = True
            client._wait_leader_on_stop = False
            client._individual_safe_logout_is_leader = is_leader
        runner.stop_account(username, reason="Android: dang xuat rieng account")
        message = ("%s dang ve SAFE roi moi OUT" % username
                   if client is not None and getattr(client, "running", False)
                   else "Da gui lenh dang xuat %s" % username)
        return json.dumps({"ok": True, "message": message}, ensure_ascii=False)
    except Exception as exc:
        return json.dumps({"ok": False, "message": "%s" % exc}, ensure_ascii=False)


def auto_battle_one_json(username, map_id=0, x=0, y=0):
    """Dua client dang online toi bai da chon roi bat combat; khong start/relogin thread."""
    try:
        username = str(username or "").strip()
        runner = _get_runner()
        client = runner.account_clients.get(username)
        if client is None or not getattr(client, "running", False):
            raise RuntimeError("Account dang offline; bam LOGIN truoc")
        map_id, x, y = int(map_id or 0), int(x or 0), int(y or 0)
        current_map = int(getattr(client, "current_map", 0) or 0)
        if not map_id:
            map_id = current_map
        if not (x and y):
            from train_bot import config
            spots = list((config.TRAIN_MAPS.get(map_id) or {}).get("mobs") or [])
            if spots:
                x, y = int(spots[0][0]), int(spots[0][1])
        if not (x and y):
            raise RuntimeError("Map da chon chua co diem farm; hay chon toa do tai Dieu Khien")

        target = (x, y)
        client._ui_auto_battle = False
        client.flee_mode = True

        def _route_and_fight():
            try:
                ok = False
                if int(getattr(client, "current_map", 0) or 0) != map_id:
                    ok = bool(client.follow_smart_route(map_id, target, flee=True))
                else:
                    ground = client.get_ground_store()
                    pos = getattr(client, "pos", None)
                    safe_target = ground.nearest_walkable_world(map_id, target, pos) if ground is not None and pos else None
                    if safe_target is not None:
                        ok = bool(client.navigate_to(int(safe_target[0]), int(safe_target[1]),
                                                     flee=True, require_smart_path=True))
                if not ok or int(getattr(client, "current_map", 0) or 0) != map_id:
                    return
                client._ui_auto_battle = True
                client.flee_mode = False
                client.combat_ready()
            except Exception:
                traceback.print_exc()

        threading.Thread(target=_route_and_fight, name="auto-battle-%s" % username, daemon=True).start()
        if current_map == map_id:
            message = "Dang ve duong an toan toi X %d, Y %d; den noi se tu bat battle (khong login lai)" % (x, y)
        else:
            message = "Dang dua %s tu map %d toi bai map %d, X %d Y %d; den noi moi bat battle (khong login lai)" % (username, current_map, map_id, x, y)
        return json.dumps({"ok": True, "message": message}, ensure_ascii=False)
    except Exception as exc:
        return json.dumps({"ok": False, "message": "%s" % exc}, ensure_ascii=False)


def start_farm_mode_json(mode, map_id, x, y, di_gioi_level=2):
    """Start the selected workflow without resetting account slots or battle settings."""
    mode = str(mode)
    if mode == "train":
        return auto_battle_team_json(map_id, x, y)
    if mode == "stand":
        return json.dumps({"ok": False, "message": "Đứng yên không chạy farm; hãy chọn Train hoặc Dị giới + farm"}, ensure_ascii=False)
    try:
        if mode != "digioi_train":
            raise ValueError("Chế độ không hợp lệ")
        from train_bot import config
        runner = _get_runner()
        st = runner._pstate(0)
        live = _live_party(runner)
        leader = config.PARTY_LEADER_ACC.get(0)
        if not live or leader not in dict(live):
            raise RuntimeError("Hãy đăng nhập leader và các thành viên trước")
        if any(c.in_team_dungeon() for _, c in live):
            raise RuntimeError("Hãy hoàn tất phụ bản hiện tại trước")
        map_id, x, y = int(map_id), int(x), int(y)
        di_gioi_level = max(1, min(15, int(di_gioi_level or 2)))
        if min(map_id, x, y) <= 0:
            raise ValueError("Hãy chọn bãi farm và tọa độ trước")
        with st["lock"]:
            if st.get("daily_active") or st.get("leader_switch_pending") or st.get("ui_mode_restart_users"):
                raise RuntimeError("Luồng team khác đang chạy; hãy chờ hoàn tất")
            from train_bot.workflows.lifecycle import activate_locked
            activate_locked(st, "digioi")
            entry = config.TRAIN_MAPS.setdefault(map_id, {"safe": [], "name": str(map_id)})
            entry["mobs"] = [(x, y)]
            config.PARTY_CONFIG[0].update(mode="digioi_train", start_city_id=map_id,
                                         mob_index=0, train_pick="", do_daily=False,
                                         di_gioi_level=di_gioi_level,
                                         auto_world_boss=False, auto_team_dungeon=False,
                                         fight_legion_boss=False, do_van_tieu=False)
            st["dt_phase"] = "digioi"
            st["daily_hold_after_stop"] = False
            st["ui_train_phase"] = "idle"
            st["ui_train_dispatch_gen"] = None
            st["dt_train_prepared"] = False
            st["ui_dg_users"] = {u for u, _ in live}
            st["ui_dg_train_target"] = (map_id, x, y)
            st["ui_dg_transition_pending"] = False
            st["ui_dg_handoff_started"] = False
            for _, c in live:
                c.di_gioi_level = di_gioi_level
                c._dg_train_ready_token = None
            st["ui_train_target"] = None  # Khong de coordinator ra farm khi DG chua xong.
            st["cmd"] = None
            st["cmd_gen"] += 1
            st["ui_mode_restart_users"] = {u for u, _ in live}
            for _, c in live:
                c._daily_hold = False
                c._ui_auto_battle = True
                c._ui_mode_restart = True
        dg_levels = [10, 25, 40, 55, 70, 85, 100, 110, 120, 130, 140, 150, 160, 170, 180]
        return json.dumps({"ok": True, "message": "Đã chạy Dị giới cấp %d → farm: chờ hết trận, vào Dị giới; cả team hết giờ sẽ ra bãi farm đã chọn" % dg_levels[di_gioi_level - 1]}, ensure_ascii=False)
    except Exception as exc:
        return json.dumps({"ok": False, "message": str(exc)}, ensure_ascii=False)


def auto_battle_team_json(map_id=0, x=0, y=0):
    """Nut AUTO BATTLE moi: mot lan bam dieu phoi toan bo party toi bai train."""
    try:
        runner = _get_runner()
        if runner._pstate(0).get("leader_switch_pending"):
            raise RuntimeError("Đang đổi leader; hãy chờ hoàn tất trước khi bắt đầu farm")
        if runner._pstate(0).get("ui_mode_restart_users"):
            raise RuntimeError("Đang chuyển luồng Dị giới; hãy chờ hoàn tất")
        configured = runner.party_accounts(0)
        live = _live_party(runner)
        if not configured:
            raise RuntimeError("Chua co account trong team")
        if not live:
            raise RuntimeError("Chua co account online de bat dau farm")
        live_users = {u for u, _c in live}
        leader_user = next((row[0] for row in configured if row[2]), "")
        if not leader_user:
            raise RuntimeError("Team chua co leader")
        if leader_user not in live_users:
            raise RuntimeError("Leader dang offline; hay login leader truoc khi bat dau farm")
        map_id, x, y = int(map_id or 0), int(x or 0), int(y or 0)
        if map_id <= 0 or not (x and y):
            leader_user = next((row[0] for row in configured if row[2]), configured[0][0])
            leader = runner.account_clients.get(leader_user) or live[0][1]
            near = leader.nearest_smart_city(map_id or int(getattr(leader, "current_map", 0) or 0))
            city = int((near or {}).get("city") or 0)
            if city <= 0:
                raise RuntimeError("Chua tim duoc thanh tap ket gan team leader")
            st = runner._pstate(0)
            with st["lock"]:
                st["ui_train_target"] = None
            runner.party_route_maps(0, city, city)
            return json.dumps({"ok": True, "message":
                               "Chua chon du map/toa do farm: ca team se phu ve thanh gan leader, lap PT va dung cho tai do"},
                              ensure_ascii=False)
        for _username, client in live:
            client._ui_auto_battle = True
            client.flee_mode = False
            client.combat_ready()
        st = runner._pstate(0)
        with st["lock"]:
            st["ui_train_target"] = (map_id, x, y)
            st["manual_train_expected"] = len(live)
            st["manual_train_users"] = sorted(live_users)
        runner.party_train_map(0, map_id, x, y)
        return json.dumps({"ok": True, "message":
                           "Da gui START TRAIN TEAM: %d account se ve thanh gan map %d, theo phan khu live cua leader, lap du PT va leader keo toi X %d Y %d" %
                           (len(live), map_id, x, y)}, ensure_ascii=False)
    except Exception as exc:
        return json.dumps({"ok": False, "message": "%s" % exc}, ensure_ascii=False)


def switch_channel_json(channel):
    try:
        channel = int(channel)
        if channel < 1:
            raise ValueError("Phân khu phải lớn hơn 0")
        runner = _get_runner()
        # Khong set kenh_dich o day: coordinator se lam viec do sau khi leader
        # da dua party ve safe va giai tan PT. Set som lam member tu nhay kenh le.
        runner.party_switch_channel(0, channel)
        return json.dumps({"ok": True, "message": "Đã gửi lệnh chuyển cả team sang phân khu %d" % channel}, ensure_ascii=False)
    except Exception as exc:
        return json.dumps({"ok": False, "message": "%s: %s" % (type(exc).__name__, exc)}, ensure_ascii=False)


def _live_party(runner):
    rows = []
    for username, _password, _leader, _pet in runner.party_accounts(0):
        client = runner.account_clients.get(username)
        if client is not None and getattr(client, "running", False):
            rows.append((username, client))
    return rows


def channels_json():
    """Danh sach kenh live cua map leader dang dung, kem kenh de xuat cho ca team."""
    try:
        runner = _get_runner()
        live = _live_party(runner)
        if not live:
            return json.dumps({"ok": False, "message": "Chua co bot login de hoi server"}, ensure_ascii=False)
        configured = runner.party_accounts(0)
        leader_user = next((u for u, _p, is_leader, _pet in configured if is_leader), "")
        leader = runner.account_clients.get(leader_user) or live[0][1]
        map_id = int(getattr(leader, "current_map", 0) or 0)
        rows = _channel_rows_for_map(leader, force=True, wait=True)
        if not rows:
            return json.dumps({"ok": False, "message": "Server chua tra danh sach phan khu; thu lai sau vai giay"}, ensure_ascii=False)
        need = max(1, len(configured))
        channels = []
        candidates = []
        for row in rows:
            channel = int(row["id"])
            current, capacity = int(row["current"]), int(row["capacity"])
            free = max(0, capacity - current) if current >= 0 and capacity >= 0 else -1
            item = {"id": channel, "current": current, "capacity": capacity, "free": free}
            channels.append(item)
            if free >= need and capacity >= 0:
                candidates.append(item)
        best = min(candidates, key=lambda item: (item["current"], -item["free"], item["id"])) if candidates else None
        from train_bot import config
        return json.dumps({"ok": True, "map": map_id, "map_name": config.map_display_name(map_id),
                           "team_size": need, "recommended": 0,
                           "channels": channels}, ensure_ascii=False)
    except Exception as exc:
        return json.dumps({"ok": False, "message": "%s: %s" % (type(exc).__name__, exc)}, ensure_ascii=False)


def switch_best_channel_json():
    """Legacy UI entry point cannot initiate automatic switching."""
    return json.dumps({"ok": False, "message": "Đã bỏ tự chọn phân khu. Hãy chọn phân khu manual trong tab leader."}, ensure_ascii=False)


def set_train_channel_policy_json(auto_mode=False, channel=0):
    """Chi cho phep chon kenh manual va ap dung bang flow SAFE -> tan PT -> doi -> lap PT."""
    try:
        runner = _get_runner()
        st = runner._pstate(0)
        auto_mode, channel = False, int(channel or 0)
        if channel < 1:
            raise ValueError("Chua chon phan khu")
        with st["lock"]:
            st["train_channel_auto"] = False
            st["train_channel_manual"] = channel
            st["kenh_ghim"] = channel
            st["kenh_dich"] = channel
            st["kenh_dich_luc"] = time.time()
        current = 0
        live = _live_party(runner)
        if live:
            leader_name = next((u for u, _p, lead, _pet in runner.party_accounts(0) if lead), "")
            leader = next((c for u, c in live if u == leader_name), live[0][1])
            current = int(getattr(leader, "current_channel", 0) or 0)
        if current == channel:
            return json.dumps({"ok": True, "channel": channel, "message":
                               "Da luu. Team dang o dung phan khu %d nen khong can tan PT" % channel},
                              ensure_ascii=False)
        runner.party_switch_channel(0, channel)
        return json.dumps({"ok": True, "channel": channel, "message":
                           "Da luu va gui doi phan khu %d: ve SAFE -> tan PT -> doi khu -> lap lai PT; KHONG OUT account" % channel},
                          ensure_ascii=False)
    except Exception as exc:
        return json.dumps({"ok": False, "message": "%s" % exc}, ensure_ascii=False)


def run_daily_tasks_json(tasks_json):
    try:
        if _get_runner()._pstate(0).get("leader_switch_pending"):
            raise RuntimeError("Đang đổi leader; hãy chờ hoàn tất trước khi chạy Daily")
        tasks = json.loads(str(tasks_json))
        if not isinstance(tasks, list):
            raise ValueError("Danh sach daily khong hop le")
        labels = {"legion_boss": "Boss quân đoàn", "world_boss": "Boss thế giới",
                  "solo_dungeon": "Phụ bản đơn", "team_dungeon": "Phụ bản tổ đội",
                  "team_dungeon_20": "Thảo Phạt Thiên Sư • Cấp 20",
                  "team_dungeon_50": "Ngày Tàn Hoạn Quan • Cấp 50",
                  "team_dungeon_80": "Đại Chiến Lữ Bố • Cấp 80",
                  "team_dungeon_110": "Hỏa Thiêu Bộc Dương • Cấp 110"}
        raw = [str(x) for x in tasks if str(x) in labels]
        # Thu tu Daily co dinh: PB doi truoc, sau do boss QD, PB don, cuoi cung boss TG.
        chosen = ([x for x in raw if x == "team_dungeon" or x.startswith("team_dungeon_")] +
                  [x for x in ("legion_boss", "solo_dungeon", "world_boss") if x in raw])
        if not chosen:
            raise ValueError("Chưa tick daily quest nào")
        runner = _get_runner()
        live = _live_party(runner)
        if not live:
            raise RuntimeError("Chưa có account online")
        runner.party_daily_tasks(0, chosen)
        return json.dumps({"ok": True, "message": "Đã bắt đầu: %s" %
                           ", ".join(labels[x] for x in chosen)}, ensure_ascii=False)
    except Exception as exc:
        return json.dumps({"ok": False, "message": "%s" % exc}, ensure_ascii=False)


def daily_status_json():
    try:
        runner = _get_runner()
        st = runner._pstate(0)
        with st["lock"]:
            data = {key: st.get(key) for key in ("daily_active", "daily_tasks", "daily_task",
                    "daily_phase", "daily_user", "daily_message", "daily_started_at", "daily_warnings")}
        accounts = []
        for user, client in _live_party(runner):
            try:
                solo_remaining = client.solo_dungeon_remaining()
            except Exception:
                solo_remaining = None
            team_remaining = {}
            for level in (20, 50, 80, 110):
                try:
                    team_remaining[str(level)] = client.team_dungeon_remaining(level)
                except Exception:
                    team_remaining[str(level)] = None
            accounts.append({"user": user, "name": str(getattr(client, "char_name", "") or user),
                             "legion_boss_current": getattr(client, "legion_boss_count", None),
                             "legion_boss_max": getattr(client, "legion_boss_max", None),
                             "legion_boss_next": getattr(client, "legion_boss_next", None),
                             "world_boss_current": getattr(client, "world_boss_count", None),
                             "world_boss_max": getattr(client, "world_boss_max", None),
                             "solo_dungeon_remaining": solo_remaining,
                             "team_dungeon_remaining": team_remaining,
                             "in_battle": bool(client.in_combat()),
                             "activity": str(runner.get_account_activity(user) or "")})
        labels = {"legion_boss": "Boss quân đoàn", "world_boss": "Boss thế giới",
                  "solo_dungeon": "Phụ bản đơn", "team_dungeon": "Phụ bản tổ đội",
                  "team_dungeon_20": "Thảo Phạt Thiên Sư • Cấp 20",
                  "team_dungeon_50": "Ngày Tàn Hoạn Quan • Cấp 50",
                  "team_dungeon_80": "Đại Chiến Lữ Bố • Cấp 80",
                  "team_dungeon_110": "Hỏa Thiêu Bộc Dương • Cấp 110"}
        data["daily_task_name"] = labels.get(str(data.get("daily_task") or ""), "")
        data["daily_tasks_names"] = [labels.get(str(x), str(x)) for x in (data.get("daily_tasks") or [])]
        data.update({"ok": True, "accounts": accounts})
        return json.dumps(data, ensure_ascii=False)
    except Exception as exc:
        return json.dumps({"ok": False, "message": "%s" % exc}, ensure_ascii=False)


def stop_daily_json():
    try:
        _get_runner().party_stop_daily(0)
        return json.dumps({"ok": True, "message": "Dang dung Daily an toan sau tran hien tai"}, ensure_ascii=False)
    except Exception as exc:
        return json.dumps({"ok": False, "message": "%s" % exc}, ensure_ascii=False)


def move_team_json(x, y):
    """Lenh cham ban do: chi leader di khi PT da du va team dang cung map/kenh."""
    try:
        x, y = int(x), int(y)
        runner = _get_runner()
        live = _live_party(runner)
        configured = runner.party_accounts(0)
        if not live:
            raise RuntimeError("Chua co account online")
        maps = {int(getattr(c, "current_map", 0) or 0) for _u, c in live}
        channels = {int(getattr(c, "current_channel", 0) or 0) for _u, c in live}
        if len(maps) != 1 or len(channels) != 1:
            raise RuntimeError("Team dang lech map hoac phan khu; hay gom team truoc")
        leader_user = next((u for u, _p, lead, _pet in configured if lead), live[0][0])
        leader = runner.account_clients.get(leader_user)
        if leader is None or not getattr(leader, "running", False):
            raise RuntimeError("Leader chua online; khong the keo team")
        pos = getattr(leader, "pos", None)
        ground = leader.get_ground_store()
        map_id = int(getattr(leader, "current_map", 0) or 0)
        if not pos or ground is None or ground.get(map_id) is None:
            raise RuntimeError("Map nay khong co du lieu Ground collision; da khoa di chuyen de an toan")
        requested = (x, y)
        target = ground.nearest_walkable_world(map_id, requested, tuple(pos))
        if target is None:
            raise RuntimeError("Khong tim duoc o di duoc trong vung hien tai")
        target = (int(target[0]), int(target[1]))
        path = ground.find_world_path(map_id, tuple(pos), target)
        if not path:
            raise RuntimeError("Khong co duong di hop le toi diem da cham (co the bi tuong chan)")
        # Do not reject a valid path using a fixed world-distance threshold here.
        # Ground maps can legitimately return snapped or compressed waypoints.
        # The movement worker recalculates the smart path with Ground collision,
        # requires it to exist, and splits long segments before sending movement.
        runner.party_move_to(0, target[0], target[1])
        snapped = target != requested
        message = ("Diem cham nam tren vat can; da chon o an toan gan nhat X %d, Y %d. " % target) if snapped else ""
        online_count = len(live)
        joined = runner.joined_member_count(0)
        message += ("Da ve duong %d waypoint. Leader dang di, khong cat qua tuong; "
                    "online %d account, PT co %d member theo leader.") % (len(path), online_count, joined)
        return json.dumps({"ok": True, "message": message, "requested": list(requested),
                           "target": list(target), "path": [list(p) for p in path]}, ensure_ascii=False)
    except Exception as exc:
        return json.dumps({"ok": False, "message": "%s" % exc}, ensure_ascii=False)


def status_json():
    try:
        runner = _get_runner()
        rows = []
        from train_bot import config
        from train_bot.client import party_int_for
        for username, thread in list(runner.account_threads.items()):
            client = runner.account_clients.get(username)
            pet_id, pet_level, pet_exp_current, pet_exp_total, pet_exp_remaining = _pet_level_exp_values(client)
            state = getattr(client, "state", None)
            char = getattr(state, "char", None)
            pet = getattr(state, "pet", None)
            pos = getattr(client, "pos", None) or (0, 0)
            map_id = int(getattr(client, "current_map", 0) or 0)
            rows.append({
                "user": username,
                "running": bool(thread and thread.is_alive()),
                "map": map_id,
                "map_name": config.map_display_name(map_id),
                "x": int(pos[0] or 0),
                "y": int(pos[1] or 0),
                "channel": int(getattr(client, "current_channel", 0) or 0),
                "name": str(getattr(client, "char_name", "") or ""),
                "hp": int(getattr(char, "hp", 0) or 0),
                "hp_max": int(getattr(char, "hp_max", 0) or 0),
                "sp": int(getattr(char, "sp", 0) or 0),
                "sp_max": int(getattr(char, "sp_max", 0) or 0),
                "pet_hp": int(getattr(pet, "hp", 0) or 0),
                "pet_hp_max": int(getattr(pet, "hp_max", 0) or 0),
                "pet_sp": int(getattr(pet, "sp", 0) or 0),
                "pet_sp_max": int(getattr(pet, "sp_max", 0) or 0),
                "pet_id": pet_id,
                "pet_level": pet_level,
                "pet_name": str(getattr(client, "pet_name", "") or "Pet chưa xác định"),
                "pet_exp_current": pet_exp_current,
                "pet_exp_level_total": pet_exp_total,
                "pet_exp_level_remaining": pet_exp_remaining,
                "exp_current": getattr(client, "char_exp", None),
                "exp_remaining": getattr(client, "exp_remaining", None),
                "exp_level_total": getattr(client, "exp_level_total", None),
                "legion_boss_current": getattr(client, "legion_boss_count", None),
                "legion_boss_max": getattr(client, "legion_boss_max", None),
                "world_boss_current": getattr(client, "world_boss_count", None),
                "world_boss_max": getattr(client, "world_boss_max", None),
                "solo_dungeon_remaining": client.solo_dungeon_remaining() if client is not None else None,
                "team_dungeon_remaining": ({str(level): client.team_dungeon_remaining(level)
                                             for level in (20, 50, 80, 110)}
                                            if client is not None else {}),
            })
        return json.dumps({"ok": True, "accounts": rows, "error": _last_error}, ensure_ascii=False)
    except Exception as exc:
        return json.dumps({"ok": False, "accounts": [], "error": "%s: %s" % (type(exc).__name__, exc)}, ensure_ascii=False)


def map_snapshot_json(username=""):
    """Snapshot read-only cho tab ban do: team, entity live, safe va dich train."""
    try:
        runner = _get_runner()
        live = _live_party(runner)
        if not live:
            return json.dumps({"ok": True, "map": 0, "map_name": "Chua login", "team": [], "entities": []}, ensure_ascii=False)
        configured_leader = next((user for user, _pw, lead, _pet in runner.party_accounts(0) if lead), live[0][0])
        focus_user = str(username or configured_leader)
        focus = next((client for user, client in live if user == focus_user), live[0][1])
        map_id = int(getattr(focus, "current_map", 0) or 0)
        channel = int(getattr(focus, "current_channel", 0) or 0)
        from train_bot import config
        from train_bot.client import party_int_for
        # Vi tri entity do server broadcast co the moi hon pos socket cua member dang follow.
        # Chi doc de ve UI; KHONG ghi de toa do dung cho navigate/combat.
        now = time.monotonic()
        observed = {}
        for _u, observer in live:
            if observer.current_map != map_id or observer.current_channel != channel:
                continue
            with observer._world_entity_lock:
                for ent, row in list((observer.world_entities or {}).items()):
                    if int(row[0]) != map_id or now - float(row[3]) > 15.0:
                        continue
                    if bytes(ent) not in observed or row[3] > observed[bytes(ent)][3]:
                        observed[bytes(ent)] = row
        team = []
        for username, client in live:
            pos = getattr(client, "pos", None) or (0, 0)
            entity = getattr(client, "self_entity", None)
            seen = observed.get(bytes(entity)) if entity else None
            source = "account"
            if (seen and client.current_map == map_id and client.current_channel == channel
                    and username != configured_leader):
                pos = (seen[1], seen[2])
                source = "server_entity"
            team.append({"user": username, "name": str(getattr(client, "char_name", "") or username),
                         "map": int(getattr(client, "current_map", 0) or 0),
                         "channel": int(getattr(client, "current_channel", 0) or 0),
                         "x": int(pos[0] or 0), "y": int(pos[1] or 0),
                         "position_source": source,
                         "leader": username == configured_leader,
                         "in_party": bool(runner.is_joined(0, entity)) if entity else False,
                         "strategist": bool(runner.is_strategist(0, entity)) if entity else False,
                         "int": party_int_for(0, entity) if entity else None})
        entities = []
        now = time.monotonic()
        with getattr(focus, "_world_entity_lock"):
            world = list((getattr(focus, "world_entities", None) or {}).items())
        party_entities = {bytes(getattr(c, "self_entity")) for _u, c in live if getattr(c, "self_entity", None)}
        for entity, value in world:
            emap, x, y, seen = value
            if int(emap) != map_id or now - float(seen) > 15.0 or bytes(entity) in party_entities:
                continue
            names = list((getattr(focus, "entity_names", None) or {}).get(bytes(entity), ()))
            template_id = int.from_bytes(bytes(entity)[2:4], "little") if len(bytes(entity)) >= 4 else 0
            npc_name = (getattr(config, "NPC_NAMES", None) or {}).get(template_id)
            display_name = names[0] if names else (npc_name or ("Quai #%d" % template_id if template_id else "Quai"))
            entities.append({"id": bytes(entity).hex()[:8], "template_id": template_id,
                             "name": display_name, "kind": "player" if names else "mob",
                             "x": int(x), "y": int(y)})
            if len(entities) >= 250:
                break
        st = runner._pstate(0)
        with st["lock"]:
            target = st.get("mob_spot")
        train_map = config.TRAIN_MAPS.get(map_id) or {}
        safe = list(train_map.get("safe") or [])
        collision = _ground_ui_cache.get(map_id)
        if collision is None:
            ground = focus.get_ground_store()
            gm = ground.get(map_id) if ground is not None else None
            if gm:
                left, top = ground._world_origin(gm)
                collision = {"grid_w": int(gm["grid_w"]), "grid_h": int(gm["grid_h"]),
                             "origin_x": int(left), "origin_y": int(top), "cell": 20,
                             "data": base64.b64encode(bytes(gm["grid"])).decode("ascii")}
            else:
                collision = {}
            _ground_ui_cache[map_id] = collision
        return json.dumps({"ok": True, "map": map_id, "map_name": config.map_display_name(map_id),
                           "focus_user": focus_user,
                           "channel": channel, "team": team, "entities": entities,
                           "target": list(target) if target else None,
                           "safe": [list(point) for point in safe[:20]],
                           "collision": collision}, ensure_ascii=False)
    except Exception as exc:
        return json.dumps({"ok": False, "message": "%s: %s" % (type(exc).__name__, exc)}, ensure_ascii=False)


def map_snapshots_json():
    """Snapshot rieng tung socket/account; UI cache theo username, khong muon map leader."""
    try:
        maps = {}
        for user, _client in _live_party(_get_runner()):
            row = json.loads(map_snapshot_json(user))
            if row.get("ok"):
                row["focus_user"] = user
                maps[user] = row
        return json.dumps({"ok": True, "maps": maps}, ensure_ascii=False)
    except Exception as exc:
        return json.dumps({"ok": False, "maps": {}, "message": "%s: %s" %
                           (type(exc).__name__, exc)}, ensure_ascii=False)


def accounts_dashboard_json():
    """Du lieu live cho cac tab account; tui do chi doc, khong gui lenh game."""
    try:
        runner = _get_runner()
        from train_bot import config
        st = runner._pstate(0)
        with st["lock"]:
            train_target = st.get("ui_train_target")
            channel_auto = False
            channel_manual = st.get("train_channel_manual")
        configured = runner.party_accounts(0)
        leader_user = next((u for u, _p, is_leader, _pet in configured if is_leader), "")
        leader_client = runner.account_clients.get(leader_user)
        if leader_client is None or not getattr(leader_client, "running", False):
            live = _live_party(runner)
            leader_client = live[0][1] if live else None
        channel_options = _channel_rows_for_map(leader_client, force=False, wait=False)
        result = []
        for username, _password, is_leader, _pet in configured:
            thread = runner.account_threads.get(username)
            client = runner.account_clients.get(username)
            exp_in_level, exp_level_total, exp_level_remaining = _level_exp_values(client)
            pet_id, pet_level, pet_exp_current, pet_exp_total, pet_exp_remaining = _pet_level_exp_values(client)
            online = bool(thread and thread.is_alive() and client is not None and getattr(client, "running", False))
            state = getattr(client, "state", None)
            char = getattr(state, "char", None)
            pet = getattr(state, "pet", None)
            bag = runner.bag_info(username) or {}
            pos = getattr(client, "pos", None) or (0, 0)
            map_id = int(getattr(client, "current_map", 0) or 0)
            city_at_map = config.TELEPORT_CITIES.get(map_id)
            if city_at_map:
                area_name = str(city_at_map.get("name") or config.map_display_name(map_id))
            else:
                area_name = str((config.TRAIN_MAPS.get(map_id) or {}).get("name") or
                                (getattr(config, "SCENE_NAMES", {}) or {}).get(map_id) or
                                config.map_display_name(map_id) or "Khu vực chưa có tên")
            try:
                _task_state = runner.get_account_task(username) or {}
                _task_key = str(_task_state.get("task") or "").strip().lower()
                _phase = str(_task_state.get("phase") or "")
                if _phase == "boss_qd" or _task_key in _DAILY_AREA_NAMES:
                    area_name = _DAILY_AREA_NAMES.get(_task_key, "Boss Quân Đoàn")
                elif _phase == "team_dungeon":
                    _daily_task = str(st.get("daily_task") or "")
                    try:
                        area_name = _TEAM_DUNGEON_NAMES.get(int(_daily_task.rsplit("_", 1)[-1]), area_name)
                    except Exception:
                        pass
            except Exception:
                pass
            city_name = "Chưa xác định"
            if client is not None:
                city = config.TELEPORT_CITIES.get(map_id)
                if city:
                    city_name = str(city.get("name") or config.map_display_name(map_id))
                else:
                    try:
                        near = client.nearest_smart_city(map_id)
                        near_id = int((near or {}).get("city") or 0)
                        city_name = str((config.TELEPORT_CITIES.get(near_id) or {}).get("name") or "Chưa xác định")
                    except Exception:
                        pass
            party_members = list(getattr(client, "party_members", None) or ()) if client is not None else []
            in_party = bool(party_members or getattr(client, "party_leader", None)) if client is not None else False
            if is_leader and party_members:
                in_party = True
            party_count = (len(party_members) + 1) if in_party or party_members else 0
            cities = []
            if client is not None:
                from train_bot import config
                for city_id, city in sorted(config.TELEPORT_CITIES.items(), key=lambda item: str(item[1].get("name", ""))):
                    if client.city_unlocked(city_id) is True:
                        cities.append({"id": int(city_id), "flag": int(city.get("flag", 0)),
                                       "name": str(city.get("name") or city_id)})
            saved_settings = _restore_account_settings(username)
            exp_rate = client.exp_rate_snapshot() if client is not None else {}
            furnace = None
            if client is not None and getattr(client, "furnace_shop", None) is not None:
                from train_bot.client import _load_gamedata_items
                names = _load_gamedata_items()
                raw_furnace = client.furnace_shop or {}
                furnace = {"base_rate": raw_furnace.get("base_rate"),
                           "active_rate": raw_furnace.get("active_rate"), "tabs": {}}
                for kind, items in (raw_furnace.get("tabs") or {}).items():
                    furnace["tabs"][str(kind)] = [dict(item,
                        name=(names.get(int(item.get("id", 0))) or {}).get("name") or
                             ("0x%04x" % int(item.get("id", 0)))) for item in items if item.get("id")]
            quest_cells = set(getattr(client, "_quest_cells", set()) or ()) if client is not None else set()
            result.append({
                "user": username,
                "name": str(getattr(client, "char_name", "") or username),
                "online": online,
                "logging_in": bool(thread and thread.is_alive() and not online),
                "leader": bool(is_leader),
                "map": map_id,
                "map_name": config.map_display_name(map_id),
                "area_name": area_name,
                "city_name": city_name,
                "x": int(pos[0] or 0), "y": int(pos[1] or 0),
                "channel": int(getattr(client, "current_channel", 0) or 0),
                "party_count": int(party_count),
                "party_expected": int(len(configured)),
                "channel_auto": channel_auto,
                "channel_manual": channel_manual,
                "channel_options": channel_options,
                "train_map": int(train_target[0]) if train_target else None,
                "train_map_name": config.map_display_name(int(train_target[0])) if train_target else None,
                "train_x": int(train_target[1]) if train_target else None,
                "train_y": int(train_target[2]) if train_target else None,
                "level": getattr(client, "char_level", None),
                "hp": int(getattr(char, "hp", 0) or 0),
                "hp_max": int(getattr(char, "hp_max", 0) or 0),
                "sp": int(getattr(char, "sp", 0) or 0),
                "sp_max": int(getattr(char, "sp_max", 0) or 0),
                "pet_hp": int(getattr(pet, "hp", 0) or 0),
                "pet_hp_max": int(getattr(pet, "hp_max", 0) or 0),
                "pet_sp": int(getattr(pet, "sp", 0) or 0),
                "pet_sp_max": int(getattr(pet, "sp_max", 0) or 0),
                "pet_id": pet_id,
                "pet_level": pet_level,
                "pet_name": str(getattr(client, "pet_name", "") or "Pet chưa xác định"),
                "pet_exp_current": pet_exp_current,
                "pet_exp_level_total": pet_exp_total,
                "pet_exp_level_remaining": pet_exp_remaining,
                "xu": getattr(client, "premium_xu", None),
                "currency_probe": getattr(client, "currency_values", {}),
                # EXP hien tai doc truc tiep tu 0x05/sub0300 +22. Mốc/còn lại cần bảng level.
                "gold": getattr(client, "gold", None),
                "money": getattr(client, "money", None),
                "exp_current": getattr(client, "char_exp", None),
                "exp_remaining": getattr(client, "exp_remaining", None),
                "exp_in_level": exp_in_level,
                "exp_level_total": exp_level_total,
                "exp_level_remaining": exp_level_remaining,
                "exp_rate": exp_rate,
                "legion_boss_current": getattr(client, "legion_boss_count", None),
                "legion_boss_max": getattr(client, "legion_boss_max", None),
                "legion_boss_next": getattr(client, "legion_boss_next", None),
                "world_boss_current": getattr(client, "world_boss_count", None),
                "world_boss_max": getattr(client, "world_boss_max", None),
                "daily_progress_synced": bool(getattr(client, "mission_steps_loaded", False)),
                "solo_dungeon_remaining": client.solo_dungeon_remaining() if client is not None else None,
                "team_dungeon_remaining": ({str(level): client.team_dungeon_remaining(level)
                                             for level in (20, 50, 80, 110)}
                                            if client is not None else {}),
                "combat_exp_log": list(getattr(client, "combat_exp_log", []) or []),
                "activity_log": list(getattr(client, "activity_log", []) or []),
                "skills": runner.account_skills(username),
                "heal": dict(getattr(config, "ACCOUNT_HEAL", {}).get(username, {}) or {}),
                "combat_settings": saved_settings,
                "death_return": dict(saved_settings.get("death_return") or {"character": True, "pet": True}),
                "use_phuc_than": bool(getattr(config, "ACCOUNT_PHUC_THAN", {}).get(username, False)),
                "use_dai_phuc_than": bool(getattr(config, "ACCOUNT_DAI_PHUC_THAN", {}).get(username, False)),
                "use_digioi_ho_phu": bool(saved_settings.get("use_digioi_ho_phu", False)),
                "auto_buy_bao_hop": bool(saved_settings.get("auto_buy_bao_hop", False)),
                "phuc_than_remaining": getattr(client, "god_mission", None),
                "shop": {
                    "ho_phu_used": getattr(client, "shop_ho_phu_count", None),
                    "ho_phu_max": getattr(client, "shop_ho_phu_max", 3),
                    "bao_hop_used": getattr(client, "shop_bao_hop_count", None),
                    "bao_hop_max": getattr(client, "shop_bao_hop_max", 1),
                    "gacha_pet_remaining": 0 if 6 in quest_cells else 1,
                    "gacha_card_remaining": 0 if 4 in quest_cells else 1,
                },
                "furnace": furnace,
                "cities_loaded": bool(client is not None and getattr(client, "_mark_flags_loaded", False)),
                "cities": cities,
                "bag": bag,
            })
        # UI co 5 tab co dinh. Khong nen nen danh sach configured: ACC2 login mot minh van phai
        # nam index 1, khong bi day len index 0 va hien nham thanh ACC1.
        ordered = [None] * 5
        unused = [i for i in range(5)]
        for row in result:
            slot = _account_slots.get(row.get("user"))
            if slot is None or not (0 <= int(slot) < 5) or ordered[int(slot)] is not None:
                slot = unused[0] if unused else 0
            slot = int(slot)
            ordered[slot] = row
            if slot in unused:
                unused.remove(slot)
        for i in range(5):
            if ordered[i] is None:
                ordered[i] = {"slot": i, "user": "", "name": "", "online": False,
                              "logging_in": False, "leader": False, "party_count": 0,
                              "party_expected": len(configured)}
            else:
                ordered[i]["slot"] = i
        return json.dumps({"ok": True, "accounts": ordered}, ensure_ascii=False)
    except Exception as exc:
        return json.dumps({"ok": False, "accounts": [], "message": "%s: %s" % (type(exc).__name__, exc)}, ensure_ascii=False)


def teleport_city_one_json(username, city_id):
    """Teleport rieng mot account den thanh da mo; chay nen de khong khoa UI Android."""
    try:
        username, city_id = str(username or "").strip(), int(city_id)
        runner = _get_runner()
        client = runner.account_clients.get(username)
        if client is None or not getattr(client, "running", False):
            raise RuntimeError("Account chua online")
        from train_bot import config
        city = config.TELEPORT_CITIES.get(city_id)
        if city is None:
            raise RuntimeError("Thanh nay khong co trong bang teleport")
        unlocked = client.city_unlocked(city_id)
        if unlocked is None:
            raise RuntimeError("Server chua tra danh sach thanh da mo; cho vai giay roi thu lai")
        if unlocked is not True:
            raise RuntimeError("Account chua mo thanh nay")

        def do_teleport():
            try:
                client._wait_combat_clear(idle=2.0, cap=30.0)
                client.go_to_town(city_id, int(city.get("flag", 0)))
            except Exception:
                traceback.print_exc()

        threading.Thread(target=do_teleport, name="teleport-%s" % username, daemon=True).start()
        return json.dumps({"ok": True, "message": "Da gui %s dich chuyen den %s. Account co the roi PT khi teleport." %
                           (username, city.get("name") or city_id)}, ensure_ascii=False)
    except Exception as exc:
        return json.dumps({"ok": False, "message": "%s" % exc}, ensure_ascii=False)
_channel_cache_lock = threading.RLock()
_channel_cache_by_map = {}
_channel_refreshing = set()
_channel_seen_map_by_user = {}
_channel_last_attempt_by_map = {}


def _channel_rows_for_map(client, force=False, wait=False):
    """Danh sach phan khu dung RIENG cho map hien tai; refresh khi doi map/qua 5 phut."""
    if client is None or not getattr(client, "running", False):
        return []
    map_id = int(getattr(client, "current_map", 0) or 0)
    if map_id <= 0:
        return []
    user = str(getattr(client, "_username", "") or id(client))
    now = time.time()
    with _channel_cache_lock:
        cached = _channel_cache_by_map.get(map_id) or {}
        changed_map = _channel_seen_map_by_user.get(user) != map_id
        _channel_seen_map_by_user[user] = map_id
        last_attempt = float(_channel_last_attempt_by_map.get(map_id, 0) or 0)
        refresh_after = 300.0 if cached.get("rows") else 10.0
        stale = changed_map or force or now - last_attempt >= refresh_after

    def fetch():
        try:
            client.request_channel_list()
            if client._chan_event.wait(4.0):
                response_map = int(getattr(client, "_ds_kenh_map", 0) or 0)
                if response_map != map_id:
                    log.warning("[%s] Bo danh sach phan khu map %s vi dang can map %s",
                                user, response_map, map_id)
                    return
                rows = []
                for key, value in sorted(dict(getattr(client, "channels", {}) or {}).items(),
                                         key=lambda item: int(item[0])):
                    cur, cap = value
                    # Kenh hien tai co the duoc client them vao voi suc chua chua biet. Khong de
                    # mot dong None lam hong toan bo 8+ phan khu server vua tra ve.
                    rows.append({"id": int(key),
                                 "current": int(cur) if cur is not None else -1,
                                 "capacity": int(cap) if cap is not None else -1})
                if rows:
                    with _channel_cache_lock:
                        _channel_cache_by_map[map_id] = {"at": time.time(), "rows": rows}
        except Exception:
            traceback.print_exc()
        finally:
            with _channel_cache_lock:
                _channel_refreshing.discard(map_id)

    thread = None
    if stale:
        with _channel_cache_lock:
            if map_id not in _channel_refreshing:
                _channel_refreshing.add(map_id)
                _channel_last_attempt_by_map[map_id] = now
                thread = threading.Thread(target=fetch, name="channels-map-%s" % map_id, daemon=True)
                thread.start()
    if wait and thread is not None:
        thread.join(5.0)
    with _channel_cache_lock:
        return list((_channel_cache_by_map.get(map_id) or {}).get("rows") or [])
