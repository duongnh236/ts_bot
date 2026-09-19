"""Digioi workflow implementation; explicit dependencies, no runner imports.

Moved without changing the existing game protocol/route algorithms.
"""


def _android_dg_train_handoff(pidx, st, *, services):
    """One command after ALL DG workers resume their same-socket command loop."""
    _dt_party_usernames = services._dt_party_usernames
    account_clients = services.account_clients
    config = services.config
    log = services.log
    party_train_map = services.party_train_map
    threading = services.threading
    time = services.time
    with st["lock"]:
        target = st.get("ui_dg_train_target")
        if not target or st.get("ui_dg_handoff_started"):
            return
        session = st.get("android_workflow_session")
        token = st.get("cmd_gen", 0)
        st["ui_dg_handoff_started"] = True
        st["ui_dg_transition_pending"] = True
        st["ui_dg_transition_token"] = token
    config.PARTY_CONFIG[pidx].update(mode="stand", start_city_id=0)

    def wait_and_dispatch():
        last_log = 0
        try:
            while (st.get("cmd_gen", 0) == token and st.get("ui_dg_transition_pending")
                   and (session is None or session.active)):
                users = set(_dt_party_usernames(pidx))
                missing = []
                for user in sorted(users):
                    client = account_clients.get(user)
                    if (client is None or not client.running or client.in_di_gioi()
                            or getattr(client, "_dg_train_ready_token", None) != token):
                        missing.append(user)
                leader = config.PARTY_LEADER_ACC.get(pidx)
                if users and leader in users and not missing:
                    with st["lock"]:
                        if st.get("cmd_gen", 0) != token:
                            return
                        st["manual_train_users"] = sorted(users)
                        for user in users:
                            client = account_clients[user]
                            client.stop_run_around()
                            client.flee_mode = False
                            client._ui_auto_battle = True
                        st["ui_dg_transition_pending"] = False
                    party_train_map(pidx, *target, expected_generation=token)
                    log.info("[party %d] DG -> FARM: du %d account, bat dau gom/phan khu manual/lap party/ra bai", pidx + 1, len(users))
                    return
                if time.time() - last_log >= 10:
                    last_log = time.time()
                    log.info("[party %d] DG -> FARM: DUNG CHO account thoat DG va san sang: %s | leader=%s", pidx + 1, ", ".join(missing) or "leader offline", leader)
                time.sleep(1)
        except Exception:
            log.exception("[party %d] DG -> FARM: loi chuyen luong, giu team tai diem cho", pidx + 1)
        finally:
            with st["lock"]:
                if st.get("ui_dg_transition_token") == token:
                    st["ui_dg_transition_pending"] = False
    threading.Thread(target=wait_and_dispatch, name="dg-train-handoff", daemon=True).start()
