"""Train-only channel failure recovery. Never switch a live party in place."""


def request(st, user, generation, reason):
    with st["lock"]:
        if st.get("cmd_gen") != generation:
            return False
        if not st.get("train_channel_regroup"):
            st["train_channel_regroup"] = {"generation": generation, "phase": "safe",
                                          "user": user, "reason": reason, "arrived": set()}
        return True


def tick(c, st, user, pidx, stopped_fn, *, services):
    recovery = st.get("train_channel_regroup")
    if not recovery:
        return False
    generation = recovery["generation"]
    def abort():
        return stopped_fn() or not c.running or st.get("cmd_gen") != generation
    if st.get("cmd_gen") != generation or stopped_fn():
        with st["lock"]:
            if st.get("train_channel_regroup") is recovery:
                st.pop("train_channel_regroup", None)
        return False
    target = st.get("ui_train_target")
    if not target or (st.get("cmd") or (None,))[0] != "train":
        return False
    leader_user = services.config.PARTY_LEADER_ACC.get(pidx)
    if recovery["phase"] == "safe":
        if user != leader_user:
            services.set_account_activity(user, "Farm: chờ leader về safe rồi gom lại", phase="wait")
            return True
        city_result = c.nearest_smart_city(int(target[0]), exclude_map=int(target[0]))
        if isinstance(city_result, dict):
            city, flag = city_result.get("city"), city_result.get("flag", 0)
        else:
            city, flag = city_result if city_result else (None, None)
        if not city:
            services.set_account_activity(user, "Farm: chưa tìm được thành tập trung", phase="wait")
            return True
        # A known city is already safe; outside it require an actual configured safe.
        if c.current_map != city:
            points = (services.config.TRAIN_MAPS.get(c.current_map) or {}).get("safe") or []
            safe = services._nearest_safe(c.pos, points)
            if not safe:
                services.set_account_activity(user, "Farm: chưa có dữ liệu safe, chưa giải tán đội", phase="wait")
                return True
            services.set_account_activity(user, "Farm: đưa party về safe để gom lại phân khu", phase="train")
            if not c.navigate_to(*safe, flee=False, abort=abort, require_smart_path=True):
                return True
        if abort():
            return True
        with st["lock"]:
            recovery["city"] = int(city)
            recovery["flag"] = int(flag or 0)
            recovery["phase"] = "city"
            st["ui_train_phase"] = "regroup"
            st["kenh_dich"] = st["kenh_ghim"] = None
        services.log.info("FARM: member %s khong sync khu (%s) -> leader da ve safe; gom ca team ve thanh %s",
                          recovery["user"], recovery["reason"], city)
    if user not in recovery["arrived"]:
        services._workflow_leave_current_area(c, abort)
        if abort():
            return True
        c.go_to_town(recovery["city"], recovery["flag"], tries=5, wait=2.0, battle_grace=0.0)
        if abort() or c.current_map != recovery["city"]:
            return True
        with st["lock"]:
            recovery["arrived"].add(user)
    services.set_account_activity(user, "Farm: đã về thành, chờ đủ team chạy lại train", phase="wait")
    if user == leader_user:
        live = [u for u in st.get("manual_train_users", ())
                if getattr(services.account_clients.get(u), "running", False)]
        if live and all(u in recovery["arrived"] for u in live):
            with st["lock"]:
                if abort():
                    return True
                st.pop("train_channel_regroup", None)
                st["manual_train_users"] = list(live)
            services.party_train_map(pidx, *target, expected_generation=generation)
    return True
