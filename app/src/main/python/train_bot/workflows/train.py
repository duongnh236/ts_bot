"""Train workflow implementation; explicit dependencies, no runner imports.

Moved without changing the existing game protocol/route algorithms.
"""


def _open_route_member_invites(client, st, username, label, generation, *, services):
    """Open invitation gate BEFORE waiting for the leader's party-ready event."""
    log = services.log
    set_account_activity = services.set_account_activity
    if (not client.running or st.get("cmd_gen", 0) != generation
            or getattr(client, "_individual_safe_logout", False)
            or getattr(client, "_daily_hold", False)):
        return False
    client.set_party_invite_ready(True)
    set_account_activity(username, "Farm: da tap ket, cho leader moi party", phase="wait")
    log.info("[%s] FARM: da mo nhan loi moi party tai map %s/k%s", label,
             client.current_map, getattr(client, "current_channel", None))
    return True


def _android_train_recovery_tick(c, st, username, pidx, stopped_fn, *, services):
    """Recover a lone member independently; only leader loss restarts the whole rally."""
    _nearest_safe = services._nearest_safe
    _workflow_leave_current_area = services._workflow_leave_current_area
    account_clients = services.account_clients
    config = services.config
    party_train_map = services.party_train_map
    set_account_activity = services.set_account_activity
    target = st.get("ui_train_target")
    if not target or stopped_fn() or (st.get("cmd") or (None,))[0] != "train":
        return False
    if st.get("train_channel_regroup"):
        from .channel_regroup import tick
        return tick(c, st, username, pidx, stopped_fn, services=services)
    getattr(c, "sync_machinebox_flags", lambda: None)()
    leader_user = config.PARTY_LEADER_ACC.get(pidx)
    leader = account_clients.get(leader_user)
    generation = st.get("cmd_gen")
    def abort():
        return stopped_fn() or not c.running or st.get("cmd_gen") != generation
    if username == leader_user and st.get("ui_train_phase") == "farming":
        roster = {bytes(e) for e in (getattr(c, "party_members", None) or [])}
        for member in st.get("manual_train_users", ()):
            peer = account_clients.get(member)
            if (member != leader_user and peer is not None and peer.running
                    and peer.current_map == c.current_map == int(target[0])
                    and peer.current_channel and c.current_channel
                    and peer.current_channel != c.current_channel
                    and bytes(getattr(peer, "self_entity", None) or b"") not in roster):
                from .channel_regroup import request, tick
                request(st, member, generation, "Thành viên ở khác phân khu, chưa vào party leader")
                return tick(c, st, username, pidx, stopped_fn, services=services)
    if st.get("ui_leader_recover"):
        if username != leader_user:
            if username not in st.setdefault("ui_recovery_city_arrived", set()):
                _workflow_leave_current_area(c, abort)
                destination = c.nearest_smart_city(int(target[0]), exclude_map=int(target[0]))
                if isinstance(destination, dict):
                    city, flag = destination.get("city"), destination.get("flag", 0)
                else:
                    city, flag = destination if destination else (None, None)
                if not city:
                    set_account_activity(username, "Reconnect: chưa có đường về thành gần bãi", phase="wait")
                    return True
                c.go_to_town(city, flag or 0, tries=5, wait=2.0, battle_grace=0.0)
                if not abort() and c.current_map == city:
                    with st["lock"]:
                        st.setdefault("ui_recovery_city_arrived", set()).add(username)
            set_account_activity(username, "Reconnect: về thành chờ leader online", phase="wait")
            return True
        live = [u for u in (st.get("manual_train_users") or [])
                if u != leader_user and getattr(account_clients.get(u), "running", False)]
        if all(u in st.get("ui_recovery_city_arrived", set()) for u in live):
            with st["lock"]:
                st["ui_leader_recover"] = False
                st["ui_member_recover"] = set()
                st["ui_recovery_safe_ready"] = {}
                st["manual_train_users"] = [leader_user] + live
            party_train_map(pidx, *target)
        return True
    recovering = st.get("ui_member_recover", set())
    if username in recovering:
        if leader is None or not leader.running:
            return True
        if username not in st.setdefault("ui_recovery_safe_ready", {}):
            _workflow_leave_current_area(c, abort)
            c._ui_auto_battle = True
            c.flee_mode = False
            c.combat_ready()
            if c.current_map != int(target[0]):
                set_account_activity(username, "Reconnect: tự chạy lên bãi tìm safe gần team", phase="train")
                if not c.follow_smart_route(int(target[0]), None, abort=abort, flee=False):
                    return True
            if abort() or c.current_map != leader.current_map:
                return True
            if c.current_channel != leader.current_channel:
                if not c.switch_channel(leader.current_channel, wait=6.0, retries=1, theo_lenh=True):
                    from .channel_regroup import request
                    request(st, username, generation, "Không chuyển được tới phân khu leader")
                    set_account_activity(username, "Reconnect: khu leader chưa vào được, thử lại", phase="wait")
                    return True
            safes = (getattr(config, "TRAIN_MAPS", {}).get(c.current_map) or {}).get("safe") or []
            safe = _nearest_safe(leader.pos, safes)
            if not safe:
                set_account_activity(username, "Reconnect: chưa xác định safe, không tự đoán vị trí", phase="wait")
                return True
            if not c.navigate_to(*safe, flee=False, abort=abort, require_smart_path=True):
                return True
            c.set_party_invite_ready(True)
            with st["lock"]:
                if not abort():
                    st.setdefault("ui_recovery_safe_ready", {})[username] = (c.current_map, c.current_channel, safe)
        set_account_activity(username, "Reconnect: đứng safe chờ leader ra đón", phase="wait")
        return True
    if username == leader_user and st.get("ui_train_phase") == "farming":
        ready = st.get("ui_recovery_safe_ready", {})
        for member, (map_id, channel, safe) in list(ready.items()):
            peer = account_clients.get(member)
            if not peer or not peer.running or map_id != c.current_map or channel != c.current_channel:
                continue
            set_account_activity(username, "Farm: tới safe đón thành viên reconnect", phase="train")
            if not c.navigate_to(*safe, flee=False, abort=abort, require_smart_path=True):
                return True
            c.invite_members(gap=1.0)
            if bytes(peer.self_entity or b"") not in {bytes(e) for e in (c.party_members or [])}:
                return True
            with st["lock"]:
                st.setdefault("ui_member_recover", set()).discard(member)
                st.setdefault("ui_recovery_safe_ready", {}).pop(member, None)
            c.set_party_strategist()
            c.navigate_to(int(target[1]), int(target[2]), flee=False, abort=abort, require_smart_path=True)
            return True
    if recovering:
        return True  # Healthy accounts keep server auto-combat; no native reform while member catches up.
    return False


def _train_adopt_map_channel(st, map_id, channel, generation=None, *, services):
    """Channel numbers belong to one map; never carry a city pin through a gate."""
    with st["lock"]:
        if generation is not None and st.get("cmd_gen") != generation:
            return False
        if st.get("train_channel_map") == map_id:
            return False
        st["train_channel_map"] = map_id
        st["train_channel_manual"] = int(channel or 0) or None
        st["kenh_ghim"] = None
        st["kenh_dich"] = None
        st["kenh_dich_luc"] = 0.0
        st["manual_train_full"] = None
        st["manual_train_fallback"] = None
        return True


def _train_fallback_full_channel(c, st, users, cmd, generation, label, *, services):
    """Leader-only fallback from fresh SERVER capacity; restart safe rally together."""
    account_clients = services.account_clients
    log = services.log
    time = services.time
    report = st.get("manual_train_full")
    if not report or report[0] != generation or st.get("cmd_gen") != generation:
        return False
    now = time.time()
    if now - st.get("manual_train_capacity_scan", 0) < 5:
        return False
    st["manual_train_capacity_scan"] = now
    c.request_channel_list()
    if not c._chan_event.wait(2.0):
        return False
    if getattr(c, "_ds_kenh_map", None) != c.current_map:
        return False
    blocked = int(report[1])
    candidates = []
    for channel, (population, capacity) in list(c.channels.items()):
        channel = int(channel)
        if channel == blocked or capacity <= 0:
            continue
        residents = sum(1 for u in users if (account_clients.get(u) is not None
            and account_clients[u].running and account_clients[u].current_map == c.current_map
            and account_clients[u].current_channel == channel))
        needed = len(users) - residents
        if capacity - population >= needed:
            candidates.append((population, channel))
    if not candidates:
        log.info("[%s] FARM: khu %s day; chua co khu du cho %s account, doi bang live", label, blocked, len(users))
        return False
    target = min(candidates)[1]
    with st["lock"]:
        if st.get("cmd_gen") != generation:
            return False
        st["cmd"] = tuple(cmd)
        st["cmd_gen"] += 1
        st["ui_train_dispatch_gen"] = st["cmd_gen"]
        st["ui_train_phase"] = "gather"
        st["manual_train_fallback"] = (st["cmd_gen"], c.current_map, target)
        st["manual_train_full"] = None
        st["manual_train_channel_ready"].clear()
    log.info("[%s] FARM: khu %s day -> gom LAI ca team sang khu %s (du slot theo server), roi lap party", label, blocked, target)
    return True


def _train_retry_leader_channel(c, st, username, label, generation, stopped_fn, *, services):
    """Keep the same socket and target when the leader's channel is full."""
    log = services.log
    set_account_activity = services.set_account_activity
    time = services.time
    channel = int(st.get("manual_train_channel") or 0)
    failed_attempts = 0
    if channel <= 0:
        return False
    def cancelled():
        return (not c.running or stopped_fn() or st.get("cmd_gen") != generation
                or getattr(c, "_individual_safe_logout", False))
    while c.current_channel != channel:
        if cancelled():
            return False
        switched = c.switch_channel(channel, wait=6.0, retries=1, theo_lenh=True)
        if cancelled():
            return False
        if switched and c.current_channel == channel:
            break
        code = getattr(c, "_chan_switch_result", None)
        failed_attempts += 1
        if code in (2, 4) or failed_attempts >= 2:
            plan = st.get("manual_route_plan") or {}
            if plan.get("city") and plan["city"] == getattr(c, "current_map", None):
                # Caller waits for every city-sync result, then all walk outside.
                set_account_activity(username, "Farm: khu thành không chuyển được, chờ team ra ngoài lập party", phase="wait")
                return False
            from .channel_regroup import request
            request(st, username, generation, "Server từ chối chuyển khu: %s" % code)
            set_account_activity(username, "Farm: không sang được khu leader, chờ gom lại từ safe", phase="wait")
            return False
        if code == 2:
            set_account_activity(username, "Farm: phân khu leader không hợp lệ, hãy chọn lại", phase="wait")
            log.warning("[%s] FARM: phan khu leader %s khong hop le (server ma 2)", label, channel)
            return False
        if code == 4:
            with st["lock"]:
                if st.get("cmd_gen") == generation:
                    st["manual_train_full"] = (generation, channel)
        reason = "đầy" if code == 4 else "chưa chuyển được"
        set_account_activity(username, f"Farm: phân khu leader {channel} {reason}, đang thử lại", phase="wait")
        log.info("[%s] FARM: cho phan khu leader %s, server ma %s; thu lai sau 5s, khong relogin", label, channel, code)
        if getattr(c, "_train_capacity_fallback", None) and c._train_capacity_fallback():
            return False
        for _ in range(10):
            if cancelled():
                return False
            time.sleep(0.5)
    return not cancelled()


def _farm_party_missing(pidx, users, leader, skip=(), *, services):
    """Verify exact participants against the leader's SERVER roster, not local ACK count."""
    account_clients = services.account_clients
    config = services.config
    roster = {bytes(e) for e in (getattr(leader, "party_members", None) or [])}
    missing = []
    leader_user = config.PARTY_LEADER_ACC.get(pidx)
    for user in users:
        if user == leader_user or user in skip:
            continue
        client = account_clients.get(user)
        if (client is None or not client.running
                or client.current_map != leader.current_map
                or getattr(client, "current_channel", None) != getattr(leader, "current_channel", None)
                or not getattr(client, "self_entity", None)
                or bytes(client.self_entity) not in roster):
            missing.append(user)
    return missing


def party_train_map(pidx, map_id, x, y, *, services, expected_generation=None):
    """Android AUTO BATTLE: gom/lap PT/route ca team den map + diem train roi bat combat."""
    _pstate = services._pstate
    account_clients = services.account_clients
    config = services.config
    is_account_running = services.is_account_running
    log = services.log
    party_accounts = services.party_accounts
    map_id, x, y = int(map_id), int(x), int(y)
    if map_id <= 0 or not (0 < x < 20000 and 0 < y < 20000):
        raise ValueError("map/toa do train khong hop le")
    st = _pstate(pidx)
    with st["lock"]:
        if expected_generation is not None and st.get("cmd_gen") != expected_generation:
            return False  # A newer user command owns the team.
        getattr(services, "activate_workflow", lambda *_: None)(st, "train")
        st.pop("train_channel_regroup", None)
        st["ui_member_recover"] = set()
        st["ui_leader_recover"] = False
        st["ui_recovery_safe_ready"] = {}
        st["ui_recovery_city_arrived"] = set()
        st["daily_hold_after_stop"] = False
        config.PARTY_CONFIG[pidx].update(mode="train", start_city_id=map_id, mob_index=0,
                                       train_pick="", do_daily=False, auto_world_boss=False,
                                       auto_team_dungeon=False, fight_legion_boss=False,
                                       do_van_tieu=False)
        st["dt_phase"] = "train"
        st["ui_dg_train_target"] = None
        st["ui_dg_users"] = None
        st["ui_dg_transition_pending"] = False
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
        st["ui_train_dispatch_gen"] = new_gen
        st["ui_train_phase"] = "gather"
        st["kenh_dich"] = None
        st["kenh_ghim"] = None
        st["gom_dich"] = {}
        st["reform_gen_thoa"] = st.get("reform_gen", 0)

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
        st["auto_best_channel"] = False
        st["kenh_ghim"] = None
        st["manual_train_channel"] = None
        st["manual_train_channel_ready"].clear()
    log.info(">>> PARTY %s: START TRAIN TEAM -> map %d bai (%d,%d)",
             pidx + 1, map_id, x, y)
