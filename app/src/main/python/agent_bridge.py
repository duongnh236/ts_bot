import json
import threading
import traceback
import time
import base64
import os
import logging

log = logging.getLogger(__name__)

_runner = None
_last_error = ""
_lock = threading.RLock()
_ground_ui_cache = {}
_account_settings_lock = threading.RLock()
_account_slots = {}
_selected_leader_user = None


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

# Moc EXP da doi chieu truc tiep voi UI game cua account YeuQuai.
# level -> (tong EXP tich luy tai dau cap, EXP can tu dau cap de len cap ke)
_CHAR_EXP_LEVELS = {155: (236222752, 6290520)}

# EXP pet UI hien theo EXP trong cap, packet pet login 0x0f/sub0008.
# Moc level -> tong EXP can de len cap ke, doi chieu truc tiep tu UI game.
_PET_EXP_LEVELS = {
    174: (141883942, 3197503),
    184: (176326366, 3756631),
}

_TEAM_DUNGEON_NAMES = {20: "Thảo Phạt Thiên Sư", 50: "Ngày Tàn Hoạn Quan",
                       80: "Đại Chiến Lữ Bố", 110: "Hỏa Thiêu Bộc Dương"}
_DAILY_AREA_NAMES = {"boss quan doan": "Boss Quân Đoàn", "boss the gioi": "Boss Thế Giới",
                     "pho ban don": "Khiêu Chiến Đậu Đậu"}

def _pet_level_exp_values(client):
    if client is None:
        return None, None, None, None, None
    state = getattr(client, "state", None)
    pid = int(getattr(state, "active_pet_id", 0) or 0)
    slot = int(getattr(client, "active_pet_slot", 0) or 0)
    levels = getattr(client, "pet_levels", {}) or {}
    values = getattr(client, "pet_exp_values", {}) or {}
    level = int(levels.get(pid, 0) or 0)
    current = values.get(pid, values.get(slot))
    row = _PET_EXP_LEVELS.get(level)
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
        data[str(username)] = value
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
    return saved


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
                               pet_hp_percent=None, pet_sp_percent=None):
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
        if char_skill and char_skill not in char_ids:
            raise RuntimeError("Nhan vat chua hoc skill da chon")
        if pet_id and pet_id not in pet_rows:
            raise RuntimeError("Pet da chon khong nam trong danh sach mang theo")
        pet_ids = {int(row[0]) for row in (pet_rows.get(pet_id, [0, "", []])[2] or [])}
        if pet_skill and pet_skill not in pet_ids:
            raise RuntimeError("Pet nay chua co skill da chon")
        char_rules = ([{"enabled": True, "condition": "mob", "op": "gte",
                        "value": char_mob_min, "skill": char_skill, "target": "auto"},
                       {"enabled": True, "condition": "always", "skill": "normal",
                        "target": "auto"}] if char_skill else [])
        pet_rules = ([{"enabled": True, "condition": "mob", "op": "gte",
                       "value": pet_mob_min, "skill": pet_skill, "target": "auto"},
                      {"enabled": True, "condition": "always", "skill": "normal",
                       "target": "auto"}] if pet_skill else [])
        battle = {"char": char_rules,
                  "pets": {str(pet_id): pet_rules} if pet_id else {}}
        runner.apply_account_battle(username, battle)
        runner.apply_account_heal(username, {"hp_char": hp, "sp_char": sp,
                                             "hp_pet": pet_hp, "sp_pet": pet_sp})
        from train_bot import config
        config.ACCOUNT_PHUC_THAN[username] = bool(use_phuc_than)
        config.ACCOUNT_DAI_PHUC_THAN[username] = bool(use_dai_phuc_than)
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
        })

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


def start_farm_mode_json(mode, map_id, x, y):
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
        if min(map_id, x, y) <= 0:
            raise ValueError("Hãy chọn bãi farm và tọa độ trước")
        with st["lock"]:
            if st.get("daily_active") or st.get("leader_switch_pending") or st.get("ui_mode_restart_users"):
                raise RuntimeError("Luồng team khác đang chạy; hãy chờ hoàn tất")
            entry = config.TRAIN_MAPS.setdefault(map_id, {"safe": [], "name": str(map_id)})
            entry["mobs"] = [(x, y)]
            config.PARTY_CONFIG[0].update(mode="digioi_train", start_city_id=map_id,
                                         mob_index=0, train_pick="", do_daily=False,
                                         auto_world_boss=False, auto_team_dungeon=False,
                                         fight_legion_boss=False, do_van_tieu=False)
            st["dt_phase"] = "digioi"
            st["dt_train_prepared"] = False
            st["ui_train_target"] = None  # Khong de coordinator ra farm khi DG chua xong.
            st["cmd"] = None
            st["cmd_gen"] += 1
            st["ui_mode_restart_users"] = {u for u, _ in live}
            for _, c in live:
                c._ui_auto_battle = True
                c._ui_mode_restart = True
        return json.dumps({"ok": True, "message": "Đã chạy Dị giới → farm: chờ hết trận, vào Dị giới; cả team hết giờ Dị giới sẽ ra bãi farm đã chọn"}, ensure_ascii=False)
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
                           "Da gui START TRAIN TEAM: %d account se ve thanh gan map %d, giu phan khu manual da chon, lap PT va leader keo toi X %d Y %d" %
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
                           "team_size": need, "recommended": best["id"] if best else 0,
                           "channels": channels}, ensure_ascii=False)
    except Exception as exc:
        return json.dumps({"ok": False, "message": "%s: %s" % (type(exc).__name__, exc)}, ensure_ascii=False)


def switch_best_channel_json():
    """Chon kenh vang nhat con du cho ca team, sau do dung luong safe-switch-reform."""
    data = json.loads(channels_json())
    if not data.get("ok"):
        return json.dumps(data, ensure_ascii=False)
    channel = int(data.get("recommended", 0) or 0)
    if channel < 1:
        return json.dumps({"ok": False, "message": "Khong co phan khu nao con du cho ca team"}, ensure_ascii=False)
    result = json.loads(switch_channel_json(channel))
    result.update({"channel": channel, "map": data.get("map", 0), "map_name": data.get("map_name", "")})
    return json.dumps(result, ensure_ascii=False)


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
                "use_phuc_than": bool(getattr(config, "ACCOUNT_PHUC_THAN", {}).get(username, False)),
                "use_dai_phuc_than": bool(getattr(config, "ACCOUNT_DAI_PHUC_THAN", {}).get(username, False)),
                "phuc_than_remaining": getattr(client, "god_mission", None),
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
