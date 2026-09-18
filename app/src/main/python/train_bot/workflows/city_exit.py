"""Train-only fallback: leave a full city before synchronizing and forming party."""


def gather(c, st, user, users, generation, city, destination, sync_ok, leader,
           stopped, *, services):
    def abort():
        return (st.get("cmd_gen") != generation or stopped() or not c.running
                or bool(st.get("ui_leader_recover") or st.get("train_channel_regroup")))
    def wait(check, label):
        deadline = services.time.monotonic() + 120
        while not check():
            if abort() or services.time.monotonic() >= deadline:
                services.set_account_activity(user, "Farm: chưa hoàn tất " + label, phase="wait")
                return False
            services.time.sleep(0.25)
        return not abort()
    if abort():
        return False
    with st["lock"]:
        state = st.get("train_city_sync")
        if not state or state["generation"] != generation:
            state = {"generation": generation, "results": {}, "outside": None,
                     "arrived": set(), "channel": None, "point": None}
            st["train_city_sync"] = state
        state["results"][user] = bool(sync_ok)
    def active():
        excluded = set(st.get("ui_member_recover", ())) | set(st.get("ui_kicked_users", ()))
        return [u for u in users if u not in excluded
                and getattr(services.account_clients.get(u), "running", False)]
    if not wait(lambda: all(u in state["results"] for u in active()), "đồng bộ khu trong thành"):
        return False
    if all(state["results"][u] for u in active()):
        return True
    if leader:
        route = c.build_smart_scene_route(city, destination)
        legs = (route or {}).get("legs") or []
        if not legs or int(legs[0]["target_scene"]) == city:
            services.set_account_activity(user, "Farm: chưa có Ground path ra ngoài thành", phase="wait")
            return False
        with st["lock"]:
            state["outside"] = int(legs[0]["target_scene"])
            st["kenh_ghim"] = st["kenh_dich"] = None
        services.log.info("FARM: thanh %s khong sync du khu -> ca team ra map %s lap party", city, state["outside"])
    if not wait(lambda: state["outside"] is not None, "leader chọn cổng ra thành"):
        return False
    outside = state["outside"]
    services.set_account_activity(user, "Farm: ra ngoài thành để tập trung party", phase="train")
    services._workflow_leave_current_area(c, abort)
    if abort() or not c.follow_smart_scene_route(c.current_map, outside, None, abort=abort, flee=False):
        return False
    if c.current_map != outside or abort():
        return False
    if leader:
        points = (services.config.TRAIN_MAPS.get(outside) or {}).get("safe") or []
        point = services._nearest_safe(c.pos, points) if points else c.pos
        if not point:
            return False
        if not c.navigate_to(*point, flee=False, abort=abort, require_smart_path=True):
            return False
        with st["lock"]:
            state["channel"] = int(c.current_channel or 0)
            state["point"] = tuple(c.pos)
            st["manual_train_channel"] = state["channel"]
            st["train_channel_map"] = outside
            st["train_channel_manual"] = state["channel"]
    if not wait(lambda: state["point"] is not None, "leader tập trung ngoài thành"):
        return False
    if state["channel"] <= 0:
        return False
    if c.current_channel != state["channel"]:
        if not c.switch_channel(state["channel"], wait=6.0, retries=2, theo_lenh=True):
            return False
    if abort() or c.current_channel != state["channel"]:
        return False
    if not c.navigate_to(*state["point"], flee=False, abort=abort, require_smart_path=True):
        return False
    if abort() or c.current_map != outside:
        return False
    with st["lock"]:
        state["arrived"].add(user)
    services.set_account_activity(user, "Farm: đã ra ngoài thành, chờ leader lập party", phase="wait")
    return wait(lambda: all(u in state["arrived"] for u in active()), "team ra ngoài thành")
