"""Daily workflow implementation; explicit dependencies, no runner imports.

Moved without changing the existing game protocol/route algorithms.
"""


def _daily_return_to_city(c, username, stopped_fn, *, services):
    """Daily solo preparation: each account returns independently, never waits for team."""
    _workflow_leave_current_area = services._workflow_leave_current_area
    log = services.log
    set_account_activity = services.set_account_activity
    try:
        _workflow_leave_current_area(c, stopped_fn)
        if stopped_fn():
            return False
        set_account_activity(username, "Daily: phu ve Trac Quan", phase="daily")
        if c.current_map != 12001:
            c.go_to_town(12001, 0, tries=5, wait=2.0, battle_grace=0.0)
        if stopped_fn() or c.current_map != 12001:
            raise RuntimeError("Chưa được server xác nhận tới Trác Quận")
        c.flee_mode = False
        c.combat_ready()
        return True
    except Exception as exc:
        log.warning("[%s] Daily: loi ve thanh, giu online: %s", username, exc)
        set_account_activity(username, "Daily: chua ve duoc thanh, giu online", phase="wait")
        return False


def party_daily_tasks(pidx, tasks, *, services):
    """GUI Android: phat danh sach daily cho moi account loop tu xu ly dung vai leader/member."""
    _pstate = services._pstate
    account_clients = services.account_clients
    config = services.config
    dat_party_dang_gom = services.dat_party_dang_gom
    is_account_running = services.is_account_running
    log = services.log
    party_accounts = services.party_accounts
    time = services.time
    allowed = ("legion_boss", "world_boss", "solo_dungeon", "team_dungeon",
               "team_dungeon_20", "team_dungeon_50", "team_dungeon_80", "team_dungeon_110")
    raw = tuple(x for x in tasks if x in allowed)
    chosen = tuple([x for x in raw if x == "team_dungeon" or x.startswith("team_dungeon_")] +
                   [x for x in ("legion_boss", "solo_dungeon", "world_boss") if x in raw])
    if not chosen:
        raise ValueError("chua chon daily quest")
    st = _pstate(pidx)
    pending = {u for u, _p, _l, _k in party_accounts(pidx) if is_account_running(u)}
    leader = account_clients.get(config.PARTY_LEADER_ACC.get(pidx))
    if any(x == "team_dungeon" or x.startswith("team_dungeon_") for x in chosen):
        if leader is None or not getattr(leader, "running", False):
            raise ValueError("Leader phải online để lập phòng phó bản đội")
        pending = {u for u in pending if getattr(account_clients.get(u), "running", False)}
    with st["lock"]:
        getattr(services, "activate_workflow", lambda *_: None)(st, "daily")
        config.PARTY_CONFIG[pidx].update(mode="stand", start_city_id=0, do_daily=False,
                                       auto_world_boss=False, auto_team_dungeon=False,
                                       fight_legion_boss=False, do_van_tieu=False)
        st["ui_train_target"] = None
        st["ui_train_phase"] = "idle"
        st["ui_dg_train_target"] = None
        st["ui_dg_transition_pending"] = False

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
        st["daily_resume_indices"] = {}
        st["daily_error"] = None
        st["daily_warnings"] = []
        st["daily_team_failed"] = False
        st["daily_team_rally_ready"] = set()
        st["daily_team_rally_complete"] = False
        st["daily_team_rally_error"] = None
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
