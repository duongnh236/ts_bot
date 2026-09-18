"""Dependency-free regression checks for Android orchestration changes."""
import ast
import json
import logging
import sys
import threading
import time
import unittest
from collections import deque
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

ROOT = Path(__file__).resolve().parents[1] / "app/src/main/python"
sys.path.insert(0, str(ROOT))


def function(path, name, namespace):
    namespace.setdefault("__package__", "train_bot")
    namespace.setdefault("_workflow_services", lambda: SimpleNamespace(**namespace))
    tree = ast.parse((ROOT / path).read_text())
    node = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == name)
    exec(compile(ast.Module(body=[node], type_ignores=[]), str(path), "exec"), namespace)
    return namespace[name]


class SafetyTests(unittest.TestCase):
    def test_member_catches_up_at_safe_then_leader_picks_up_without_team_restart(self):
        cfg = SimpleNamespace(PARTY_LEADER_ACC={0: "leader"}, TRAIN_MAPS={23803: {"safe": [(230, 310)]}})
        st = {"lock": threading.RLock(), "cmd_gen": 8, "cmd": ("train",),
              "ui_train_target": (23803, 550, 590), "ui_train_phase": "farming",
              "ui_member_recover": {"member"}}
        leader = SimpleNamespace(running=True, current_map=23803, current_channel=11,
            pos=(550, 590), party_members=[], navigate_to=Mock(return_value=True),
            set_party_strategist=Mock())
        member = SimpleNamespace(running=True, current_map=23802, current_channel=11,
            self_entity=b"member", combat_ready=Mock(), set_party_invite_ready=Mock(),
            navigate_to=Mock(return_value=True))
        def route(*a, **kw):
            member.current_map = 23803
            return True
        member.follow_smart_route = Mock(side_effect=route)
        leader.invite_members = Mock(side_effect=lambda **kw: leader.party_members.append(b"member"))
        restart = Mock()
        ns = {"config": cfg, "account_clients": {"leader": leader, "member": member},
              "_workflow_leave_current_area": Mock(), "_nearest_safe": lambda p, points: points[0],
              "set_account_activity": Mock(), "party_train_map": restart}
        fn = function("train_bot/run_party_digioi.py", "_android_train_recovery_tick", ns)
        self.assertTrue(fn(member, st, "member", 0, lambda: False))
        member.follow_smart_route.assert_called_once()
        member.navigate_to.assert_called_once()
        leader.navigate_to.assert_not_called()
        self.assertTrue(fn(leader, st, "leader", 0, lambda: False))
        self.assertEqual([call.args for call in leader.navigate_to.call_args_list], [(230, 310), (550, 590)])
        self.assertFalse(st["ui_member_recover"])
        self.assertEqual(st["cmd_gen"], 8)
        restart.assert_not_called()

    def test_leader_loss_returns_members_to_city_before_restart(self):
        st = {"lock": threading.RLock(), "cmd_gen": 8, "cmd": ("train",),
              "ui_train_target": (23803, 550, 590), "ui_leader_recover": True,
              "manual_train_users": ["leader", "member"]}
        c = SimpleNamespace(running=True, current_map=23803,
                            nearest_smart_city=Mock(return_value=(23001, 17)))
        c.go_to_town = Mock(side_effect=lambda *a, **kw: setattr(c, "current_map", 23001))
        leader = SimpleNamespace(running=True)
        restart = Mock()
        ns = {"config": SimpleNamespace(PARTY_LEADER_ACC={0: "leader"}),
              "account_clients": {"leader": leader, "member": c},
              "_nearest_safe": Mock(),
              "_workflow_leave_current_area": Mock(), "set_account_activity": Mock(),
              "party_train_map": restart}
        fn = function("train_bot/run_party_digioi.py", "_android_train_recovery_tick", ns)
        self.assertTrue(fn(c, st, "member", 0, lambda: False))
        restart.assert_not_called()
        self.assertTrue(fn(leader, st, "leader", 0, lambda: False))
        restart.assert_called_once_with(0, 23803, 550, 590)
        self.assertFalse(st["ui_leader_recover"])

    def test_farming_channel_change_queues_safe_flow_not_direct_switch(self):
        safe = Mock()
        fn = function("train_bot/run_party_digioi.py", "_dieu_phoi_thi_hanh_kenh",
                      {"party_switch_channel": safe})
        c = SimpleNamespace(current_map=23803, current_channel=11, switch_channel=Mock())
        st = {"cmd": ("train",), "ui_train_phase": "farming", "train_channel_map": 23803}
        self.assertEqual(fn(0, st, [("leader", c)], 2), 0)
        safe.assert_called_once_with(0, 2)
        c.switch_channel.assert_not_called()
        safe.reset_mock()
        st["cmd"] = ("channel", 2)
        self.assertEqual(fn(0, st, [("leader", c)], 2), 0)
        safe.assert_not_called()

    def test_farm_map_replaces_city_pin_without_leaving_party(self):
        fn = function("train_bot/run_party_digioi.py", "_train_adopt_map_channel", {})
        st = {"lock": threading.RLock(), "cmd_gen": 3, "train_channel_map": 23001,
              "train_channel_manual": 2, "kenh_ghim": 2, "kenh_dich": 2,
              "manual_train_full": (3, 2)}
        self.assertTrue(fn(st, 23803, 11, 3))
        self.assertEqual(st["train_channel_manual"], 11)
        self.assertEqual(st["train_channel_map"], 23803)
        self.assertIsNone(st["kenh_ghim"])
        self.assertIsNone(st["kenh_dich"])
        self.assertFalse(fn(st, 23803, 2, 3))
        self.assertEqual(st["train_channel_manual"], 11)
        self.assertFalse(fn(st, 23001, 2, 2))
        self.assertEqual(st["train_channel_map"], 23803)

    def test_full_channel_fallback_requires_fresh_capacity_for_entire_team(self):
        c = SimpleNamespace(current_map=23001, current_channel=2, running=True,
            _ds_kenh_map=23001, _chan_event=Mock(), request_channel_list=Mock(),
            channels={2: (100, 100), 3: (98, 100), 4: (94, 100)})
        c._chan_event.wait.return_value = True
        clients = {"leader": c}
        for i in range(4):
            clients[str(i)] = SimpleNamespace(running=True, current_map=23001, current_channel=2)
        st = {"cmd_gen": 8, "lock": threading.RLock(), "manual_train_full": (8, 2),
              "manual_train_channel_ready": threading.Event()}
        ns = {"log": logging.getLogger("test"), "time": SimpleNamespace(time=lambda: 100),
              "account_clients": clients}
        fn = function("train_bot/run_party_digioi.py", "_train_fallback_full_channel", ns)
        cmd = ("train", 23803, 550, 590)
        self.assertTrue(fn(c, st, list(clients), cmd, 8, "leader"))
        self.assertEqual(st["manual_train_fallback"], (9, 23001, 4))
        self.assertEqual(st["cmd"], cmd)
        self.assertEqual(st["ui_train_dispatch_gen"], 9)
        self.assertTrue(c.running)
        st.update(cmd_gen=10, manual_train_full=(10, 2), manual_train_capacity_scan=0)
        c._chan_event.wait.return_value = False
        self.assertFalse(fn(c, st, list(clients), cmd, 10, "leader"))
        self.assertEqual(st["cmd_gen"], 10)

    def test_watcher_does_not_reform_during_train_gather(self):
        task = Mock(side_effect=AssertionError("Watcher must not inspect/reform owned train"))
        ns = {"_pstate": lambda p: {"ui_train_dispatch_gen": 8, "cmd_gen": 8,
                                    "ui_train_phase": "gather"},
              "time": SimpleNamespace(sleep=Mock()), "WATCH_EVERY_SEC": 20,
              "party_accounts": lambda p: [("member", None, None, None)],
              "is_account_running": Mock(side_effect=[True, False]),
              "get_account_task": task}
        fn = function("train_bot/run_party_digioi.py", "_party_watcher", ns)
        fn(0)
        task.assert_not_called()

    def test_train_channel_full_requests_safe_regroup_without_relogin(self):
        ns = {"log": logging.getLogger("test"), "time": SimpleNamespace(sleep=Mock()),
              "set_account_activity": Mock()}
        fn = function("train_bot/run_party_digioi.py", "_train_retry_leader_channel", ns)
        st = {"cmd_gen": 8, "manual_train_channel": 2, "lock": threading.RLock()}
        c = SimpleNamespace(running=True, current_channel=1, _chan_switch_result=4)
        attempts = []
        def switch(channel, **kw):
            attempts.append(channel)
            if len(attempts) == 3:
                c.current_channel = channel
                return True
            return False
        c.switch_channel = switch
        self.assertFalse(fn(c, st, "member", "member", 8, lambda: False))
        self.assertEqual(attempts, [2])
        self.assertEqual(st["train_channel_regroup"]["phase"], "safe")
        self.assertTrue(c.running)
        c.current_channel = 1
        st["cmd_gen"] = 9
        self.assertFalse(fn(c, st, "member", "member", 8, lambda: False))
        self.assertEqual(len(attempts), 1)

    def test_daily_city_preparation_is_independent_and_keeps_online_on_failure(self):
        ns = {"log": logging.getLogger("test"), "set_account_activity": Mock(),
              "_workflow_leave_current_area": Mock()}
        fn = function("train_bot/run_party_digioi.py", "_daily_return_to_city", ns)
        c = SimpleNamespace(running=True, current_map=23803, combat_ready=Mock())
        c.go_to_town = Mock(side_effect=lambda *args, **kw: setattr(c, "current_map", 12001))
        self.assertTrue(fn(c, "member", lambda: False))
        c.go_to_town.assert_called_once()
        c.combat_ready.assert_called_once()
        c.current_map = 23803
        c.go_to_town = Mock(return_value=False)
        self.assertFalse(fn(c, "member", lambda: False))
        self.assertTrue(c.running)

    def test_selected_pet_wins_across_all_workflows_with_default_fallback(self):
        tree = ast.parse((ROOT / "train_bot/client.py").read_text())
        node = next(n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef) and n.name == "ensure_pet_role")
        ns = {}
        exec(compile(ast.Module(body=[node], type_ignores=[]), "pet-role", "exec"), ns)
        c = SimpleNamespace(_ui_selected_pet_id=123, state=SimpleNamespace(battle_config={"pet_roles": {"train": 456, "quest": 789}}), switch_pet=Mock(return_value=True))
        for role in ("train", "quest", "boss"):
            self.assertTrue(ns["ensure_pet_role"](c, role))
        self.assertEqual([call.args[0] for call in c.switch_pet.call_args_list], [123, 123, 123])
        c._ui_selected_pet_id = 0
        self.assertTrue(ns["ensure_pet_role"](c, "train"))
        c.switch_pet.assert_called_with(456)

    def test_workflows_have_single_owner_and_commands_precede_legacy_recovery(self):
        source = (ROOT / "train_bot/run_party_digioi.py").read_text()
        start = source.index("        while c.running:\n")
        self.assertLess(source.index('            if st["cmd_gen"] > cmd_gen_handled:', start),
                        source.index("            # VE PET MAC DINH:", start))
        self.assertIn('all_at_source = False if kind == "train"', source)
        self.assertNotIn('START TRAIN TEAM fast-path:', source)
        self.assertTrue('st.get("ui_train_phase") != "farming"' in source)
        self.assertIn('if st.get("cmd_gen") != gen:', source)
        self.assertTrue('st["daily_participants"] = set(pending)' in (ROOT / "train_bot/workflows/daily.py").read_text())
        self.assertIn('if not str(task).startswith("team_dungeon"):\n                        continue', source)
        self.assertIn('Daily: da xong, dung yen', source)

    def test_full_train_command_gathers_five_then_routes_only_leader(self):
        tree = ast.parse((ROOT / "train_bot/run_party_digioi.py").read_text())
        command = next(n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef) and n.name == "_do_manual_cmd")
        names = sorted({v for n in command.body if isinstance(n, ast.Nonlocal) for v in n.names})
        assignments = ast.parse("\n".join(n + " = initial.get(" + repr(n) + ")" for n in names)).body
        factory = ast.FunctionDef(name="factory", args=ast.arguments(posonlyargs=[], args=[ast.arg(arg="initial")],
                                  kwonlyargs=[], kw_defaults=[], defaults=[]),
                                  body=assignments + [command, ast.Return(value=ast.Name(id="_do_manual_cmd", ctx=ast.Load()))],
                                  decorator_list=[])
        factory_code = compile(ast.fix_missing_locations(ast.Module(body=[factory], type_ignores=[])), "actual-command", "exec")
        users = ["leader"] + ["member" + str(i) for i in range(1, 5)]
        st = {"lock": threading.RLock(), "cmd_gen": 2, "manual_route_gen": 2,
              "reform_gen": 0, "manual_train_users": users, "manual_route_city_arrived": {},
              "manual_route_plan": None, "ui_train_phase": "gather"}
        for key in ("manual_route_plan_ready", "manual_route_party_ready", "manual_route_source_done",
                    "manual_route_done", "manual_train_channel_ready"):
            st[key] = threading.Event()
        clients = {}
        for index, user in enumerate(users):
            c = SimpleNamespace(running=True, current_map=23803, current_channel=1 if index == 0 else 2,
                                self_entity=user.encode(), party_members=[], party_leader=None,
                                pos=(550, 590), party_invite_ready=False, flee_mode=False,
                                state=SimpleNamespace(in_battle=False), stop_run_around=Mock(),
                                in_combat=lambda **kw: False, in_di_gioi=lambda: False,
                                _wait_combat_clear=Mock(), combat_ready=Mock(), leave_party=Mock(),
                                nearest_smart_city=lambda *args, **kw: {"city": 23001, "flag": 0},
                                invite_members=Mock(), set_party_strategist=Mock(),
                                pre_route_town_hop=Mock(side_effect=AssertionError("random hop forbidden")))
            def town(city, flag, client=c, **kw):
                client.current_map = city
                return True
            c.go_to_town = Mock(side_effect=town)
            def switch(channel, client=c, **kw):
                client.current_channel = channel
                return True
            c.switch_channel = Mock(side_effect=switch)
            def ready(value, client=c, member=user):
                client.party_invite_ready = value
                if value and member != "leader":
                    client.party_leader = b"leader"
                    clients["leader"].party_members.append(client.self_entity)
            c.set_party_invite_ready = Mock(side_effect=ready)
            clients[user] = c
        leader = clients["leader"]
        def route(source, destination, safe, **kw):
            self.assertEqual(source, 23001)
            self.assertEqual(destination, 23803)
            self.assertEqual(set(leader.party_members), {u.encode() for u in users[1:]})
            self.assertTrue(all(c.current_channel == 1 for c in clients.values()))
            self.assertFalse(kw["flee"])
            for c in clients.values():
                c.current_map = destination
            return True
        leader.follow_smart_scene_route = Mock(side_effect=route)
        leader.navigate_to = Mock(return_value=True)
        cfg = SimpleNamespace(TRAIN_MAPS={23803: {"safe": []}}, PARTY_LEADER_ACC={0: "leader"})
        shared = {"config": cfg, "account_clients": clients, "time": SimpleNamespace(time=time.time, monotonic=time.monotonic, sleep=lambda _: time.sleep(0.003)),
                  "log": logging.getLogger("test"), "_resolve_train_safe": lambda *args: None,
                  "set_account_activity": Mock(), "joined_member_count": lambda _: 0,  # Simulate stale local ACK count.
                  "reset_party_joined": Mock(), "_invite_whitelist_followers_if_bot_party_ready": Mock(),
                  "_resync_ck": Mock(), "READY_WAIT_REFORM_SEC": 5,
                  "_route_mismatch_timed_out": lambda *args, **kwargs: False}
        for name in ("_workflow_leave_current_area", "_open_route_member_invites", "_train_retry_leader_channel", "_train_fallback_full_channel", "_train_adopt_map_channel", "_farm_party_missing"):
            function("train_bot/run_party_digioi.py", name, shared)
        errors = []
        threads = []
        for user in users:
            ns = dict(shared, c=clients[user], st=st, username=user, label=user, pidx=0,
                      cmd_gen_handled=2, is_leader=user == "leader", is_picker=user == "leader", has_leader=True,
                      _stopped=lambda: False, _nghe_lenh_kenh=Mock(), do_channel_sync=Mock(),
                      role="LEADER" if user == "leader" else "member")
            exec(factory_code, ns)
            initial = {"mode": "digioi", "raw_mode": "digioi_train", "sc": 23001,
                       "is_digioi": True, "dt_mode": True, "digioi_solo": False,
                       "_solo_without_party": False, "training_started": False}
            fn = ns["factory"](initial)
            def run(cmd=fn):
                try:
                    cmd(("train", 0, 23803, 550, 590))
                except Exception as exc:
                    errors.append(exc)
            thread = threading.Thread(target=run, daemon=True)
            threads.append(thread)
            thread.start()
        for thread in threads:
            thread.join(timeout=8)
        self.assertFalse(any(t.is_alive() for t in threads), "train deadlock")
        self.assertEqual(errors, [])
        self.assertEqual(st["ui_train_phase"], "farming")
        for c in clients.values():
            c.go_to_town.assert_called_once()
            self.assertEqual(c.go_to_town.call_args.args[0], 23001)
            c.pre_route_town_hop.assert_not_called()
        leader.follow_smart_scene_route.assert_called_once()
        leader.navigate_to.assert_called_once()
        self.assertEqual(leader.navigate_to.call_args.args, (550, 590))
        self.assertTrue(leader.navigate_to.call_args.kwargs["require_smart_path"])

    def test_route_members_open_invites_before_waiting_for_party(self):
        activity = Mock()
        client = SimpleNamespace(running=True, current_map=23001, current_channel=1,
                                 set_party_invite_ready=Mock())
        ns = {"set_account_activity": activity, "log": logging.getLogger("test")}
        fn = function("train_bot/run_party_digioi.py", "_open_route_member_invites", ns)
        self.assertTrue(fn(client, {"cmd_gen": 2}, "member", "XeTai", 2))
        client.set_party_invite_ready.assert_called_once_with(True)
        activity.assert_called_once()
        client.set_party_invite_ready.reset_mock()
        self.assertFalse(fn(client, {"cmd_gen": 3}, "member", "XeTai", 2))
        client.set_party_invite_ready.assert_not_called()
        client._individual_safe_logout = True
        self.assertFalse(fn(client, {"cmd_gen": 2}, "member", "XeTai", 2))
        client.set_party_invite_ready.assert_not_called()
        source = (ROOT / "train_bot/run_party_digioi.py").read_text()
        a = source.index("            def _do_manual_route():")
        b = source.index("            # KET BATTLE:", a)
        route = source[a:b]
        self.assertLess(route.index("_open_route_member_invites(c, st, username, label, gen)"),
                        route.index('_wait_event(st["manual_route_party_ready"]'))
        self.assertIn('st["cmd"] = tuple(cmd)', route)
        self.assertNotIn('st["cmd"] = ("route", source_req, dest)', route)
        self.assertIn('if not _wait_manual_city_arrived(expected):', route)

    def test_map_train_clears_all_dg_runtime_flags(self):
        tree = ast.parse((ROOT / "train_bot/run_party_digioi.py").read_text())
        cmd = next(n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef) and n.name == "_do_manual_cmd")
        block = next(n for n in cmd.body if isinstance(n, ast.If))
        nonlocals = {v for n in cmd.body if isinstance(n, ast.Nonlocal) for v in n.names}
        flags = {"is_digioi", "dt_mode", "digioi_solo", "_solo_without_party"}
        self.assertTrue(flags <= nonlocals)
        ns = {"cmd": ("train", 0, 23803, 550, 590), "mode": "digioi", "raw_mode": "digioi_train",
              "config": SimpleNamespace(TRAIN_MAPS={23803: {"safe": []}}), "c": SimpleNamespace(stop_run_around=Mock()),
              "_resolve_train_safe": lambda *args: None, "label": "leader", "log": logging.getLogger("test")}
        ns.update({f: True for f in flags})
        exec(compile(ast.Module(body=block.body, type_ignores=[]), "train-runtime", "exec"), ns)
        self.assertEqual(ns["sc"], 23803)
        self.assertEqual(ns["mode"], "train")
        self.assertEqual(ns["raw_mode"], "train")
        for f in flags:
            self.assertFalse(ns[f], f)
        ns["c"].stop_run_around.assert_called_once()

    def test_map_train_config_and_no_idle_relogin(self):
        source = (ROOT / "train_bot/workflows/train.py").read_text()
        a = source.index("def party_train_map(")
        section = source[a:]
        self.assertIn('config.PARTY_CONFIG[pidx].update(mode="train", start_city_id=map_id', section)
        self.assertIn('st["dt_phase"] = "train"', section)
        self.assertIn('st["ui_dg_train_target"] = None', section)
        source = (ROOT / "train_bot/run_party_digioi.py").read_text()
        a = source.index('            if (train_on_map and is_leader and should_fight')
        condition = source[a:source.index('last_relogin = time.time()', a)]
        self.assertIn('and not st.get("ui_train_target")', condition)

    def test_team_json_is_bounded_and_omits_credentials(self):
        import io
        cfg = SimpleNamespace(PARTY_LEADER_ACC={0: "leader"}, PARTY_CONFIG={0: {"mode": "train"}}, map_display_name=lambda _: "Trác Quận")
        st = {"lock": threading.RLock(), "ui_dg_users": {"leader"}}
        client = SimpleNamespace(running=True, current_map=12001, pos=(100, 200), current_channel=2, party_members=[])
        runner = SimpleNamespace(_pstate=lambda _: st, _log_path="fake.log",
                                 party_accounts=lambda _: [("leader", "secret-password", True, 0)],
                                 account_clients={"leader": client},
                                 account_status=lambda _: {"char": "Yêu Quái"},
                                 get_account_task=lambda _: {"task": "chờ member", "phase": "wait", "elapsed": 12})
        ns = {"_get_runner": lambda: runner, "json": json, "time": time}
        fn = function("agent_bridge.py", "team_debug_json", ns)
        data = ("waiting\n" * 350 + "password=secret-password\naccess_token=secret-token\n").encode()
        with patch.dict("sys.modules", {"train_bot": SimpleNamespace(config=cfg)}), patch("builtins.open", return_value=io.BytesIO(data)):
            result = fn()
        self.assertNotIn("secret-password", result)
        self.assertNotIn("secret-token", result)
        snapshot = json.loads(result)
        self.assertEqual(len(snapshot["logs"]), 300)
        self.assertEqual(snapshot["accounts"][0]["activity"], "chờ member")
        self.assertEqual(snapshot["coordination"]["ui_dg_users"], ["leader"])

    def test_farm_requires_exact_server_roster(self):
        leader = SimpleNamespace(current_map=12001, current_channel=2, party_members=[b"other"])
        member = SimpleNamespace(running=True, current_map=12001, current_channel=2, self_entity=b"member")
        ns = {"config": SimpleNamespace(PARTY_LEADER_ACC={0: "leader"}),
              "account_clients": {"member": member}}
        fn = function("train_bot/run_party_digioi.py", "_farm_party_missing", ns)
        self.assertEqual(fn(0, ["leader", "member"], leader), ["member"])
        leader.party_members = [b"member"]
        self.assertEqual(fn(0, ["leader", "member"], leader), [])
        member.running = False
        self.assertEqual(fn(0, ["leader", "member"], leader), ["member"])

    def test_dg_handoff_waits_for_all_command_loops_and_dispatches_once(self):
        st = {"lock": threading.Lock(), "cmd_gen": 7, "ui_dg_train_target": (21001, 100, 200)}
        clients = {u: SimpleNamespace(running=True, in_di_gioi=lambda: False,
                                     _dg_train_ready_token=7 if u == "leader" else None,
                                     stop_run_around=Mock()) for u in ("leader", "member")}
        callbacks = []
        def dispatch_train(*args, **kwargs):
            self.assertTrue(st["lock"].acquire(blocking=False), "DG dispatch still holds party lock")
            st["lock"].release()
            st.update(cmd_gen=8)
        dispatch = Mock(side_effect=dispatch_train)
        sleeps = []
        def sleep(_seconds):
            self.assertEqual(dispatch.call_count, 0)
            sleeps.append(True)
            clients["member"]._dg_train_ready_token = 7
        fake_threads = SimpleNamespace(Thread=lambda **kw: SimpleNamespace(start=lambda: callbacks.append(kw["target"])))
        cfg = SimpleNamespace(PARTY_CONFIG={0: {}}, PARTY_LEADER_ACC={0: "leader"})
        ns = {"config": cfg, "account_clients": clients, "threading": fake_threads,
              "time": SimpleNamespace(time=lambda: 100, sleep=sleep),
              "log": logging.getLogger("test"), "_dt_party_usernames": lambda _: list(clients),
              "party_train_map": dispatch}
        fn = function("train_bot/run_party_digioi.py", "_android_dg_train_handoff", ns)
        fn(0, st)
        self.assertEqual(cfg.PARTY_CONFIG[0]["mode"], "stand")
        callbacks[0]()
        self.assertEqual(len(sleeps), 1)
        dispatch.assert_called_once_with(0, 21001, 100, 200, expected_generation=7)
        self.assertEqual(st["manual_train_users"], ["leader", "member"])
        self.assertFalse(st["ui_dg_transition_pending"])
        fn(0, st)
        self.assertEqual(len(callbacks), 1)

    def test_dg_snapshot_excludes_offline_configured_accounts(self):
        ns = {"_pstate": lambda _: {"ui_dg_users": {"leader", "member"}},
              "party_accounts": lambda _: [(u, "", False, 0) for u in ("leader", "member", "offline")],
              "account_stops": {}}
        fn = function("train_bot/run_party_digioi.py", "_dt_party_usernames", ns)
        self.assertEqual(fn(0), ["leader", "member"])

    def test_dg_pursuit_stops_outside_and_during_daily(self):
        tree = ast.parse((ROOT / "train_bot/client.py").read_text())
        node = next(n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef) and n.name == "sync_area_combat_mode")
        ns = {"log": logging.getLogger("test")}
        exec(compile(ast.Module(body=[node], type_ignores=[]), "client.py", "exec"), ns)
        client = SimpleNamespace(_label="test", running=True, current_map=49942, _dg_pursuit_paused=False,
                                 _area_combat_mode=None, _running_route=False, party_leader=None,
                                 self_entity=b"self", flee_mode=True,
                                 in_di_gioi=lambda: True, has_hp_and_sp_items=lambda: True,
                                 in_combat=lambda: False, start_run_around=Mock(),
                                 stop_run_around=Mock(), combat_ready=Mock())
        fn = ns["sync_area_combat_mode"]
        fn(client)
        self.assertEqual(client._area_combat_mode, "pursuit")
        self.assertFalse(client.flee_mode)
        client.start_run_around.assert_called_once()
        client._running_route = True
        fn(client, allow_pursuit=False)
        self.assertEqual(client._area_combat_mode, "normal")
        client.stop_run_around.assert_called_once()
        client.stop_run_around.reset_mock()
        client.in_di_gioi = lambda: False
        client.current_map = 12001
        client.flee_mode = True
        fn(client)
        client.stop_run_around.assert_called_once()
        self.assertEqual(client.start_run_around.call_count, 1)
        self.assertFalse(client.flee_mode)
        self.assertTrue(client._ui_auto_battle)

    def test_dg_pursuit_requires_ground_and_generation(self):
        source = (ROOT / "train_bot/client.py").read_text()
        a = source.index('    def _run_around_loop(')
        b = source.index('    # Cap quai Di Gioi:', a)
        loop = source[a:b]
        self.assertIn('generation != self._run_around_generation', loop)
        self.assertIn('require_smart_path=True', loop)
        self.assertNotIn('self.move_to(', loop)

    def test_ui_account_controls_follow_connection_state(self):
        java = ROOT.parents[1] / "main/java/com/fen/tsbot"
        account = (java / "AccountManagerView.java").read_text()
        main = (java / "MainActivity.java").read_text()
        self.assertIn('boolean busy=online||connecting||loginPending[selected]', account)
        self.assertIn('outButton.setEnabled(online||connecting)', account)
        self.assertIn('currentUserField.setEnabled(!busy)', account)
        self.assertIn('currentPassField.setEnabled(!busy)', account)
        self.assertIn('dailyStop.setVisibility(active?View.VISIBLE:View.GONE)', main)
        self.assertIn('logoutAllButton.setVisibility(anyOnline?View.VISIBLE:View.GONE)', main)
        self.assertIn('accountManagerView.hasPendingLogin()||allConfiguredAccountsOnline()', main)
        self.assertNotIn('XEM PACKET SERVER JSON', main)

    def test_channel_policy_never_ranks_population(self):
        config = SimpleNamespace(PARTY_CONFIG={0: {}}, PARTY_LEADER_ACC={0: "leader"})
        ns = {"config": config, "_mode_can_lap_doi": lambda _: True,
              "_party_40npc_ngoai_gio": lambda *args: False,
              "time": time, "log": logging.getLogger("test")}
        fn = function("train_bot/run_party_digioi.py", "_dieu_phoi_chot_kenh", ns)
        clients = [("leader", SimpleNamespace(current_map=12001, current_channel=3)),
                   ("member", SimpleNamespace(current_map=12001, current_channel=7))]
        state = {"lock": threading.RLock(), "train_channel_manual": 7}
        self.assertEqual(fn(0, state, clients), 7)
        state = {"lock": threading.RLock()}
        self.assertEqual(fn(0, state, clients), 3)

    def test_no_empty_channel_fallback(self):
        fn = function("train_bot/run_party_digioi.py", "_kenh_trong_cho_ca_party", {})
        self.assertIsNone(fn(0, {}, [], {7}))
        ns = {"json": json}
        fn = function("agent_bridge.py", "switch_best_channel_json", ns)
        self.assertFalse(json.loads(fn())["ok"])

    def test_installer_fsync_uses_original_stream(self):
        source = (ROOT.parents[1] / "main/java/com/fen/tsbot/UpdateManager.java").read_text()
        self.assertIn('OutputStream output=session.openWrite("update.apk",0,total)', source)
        self.assertNotIn('new BufferedOutputStream', source)
        self.assertIn('session.fsync(output)', source)
        self.assertIn('done!=total', source)

    def test_leader_saved(self):
        save = Mock()
        ns = {"_save_account_setting": save}
        function("agent_bridge.py", "_save_leader", ns)("member2")
        self.assertEqual(ns["_selected_leader_user"], "member2")
        save.assert_called_once_with("__team__", {"leader": "member2"})

    def test_daily_recovery_does_not_disconnect(self):
        client = SimpleNamespace(_daily_use_selected_pet=True, running=True, close=Mock())
        ns = {"log": logging.getLogger("test")}
        fn = function("train_bot/run_party_digioi.py", "_force_supervisor_reconnect", ns)
        self.assertFalse(fn("member", client, "PB failed"))
        self.assertTrue(client._daily_instance_blocked)
        client.close.assert_not_called()

    def test_logout_all_members_before_leader(self):
        events = []
        clients = {u: SimpleNamespace(running=True) for u in ("leader", "member")}
        def stop(user, **kw):
            events.append(user)
            clients[user].running = False
        runner = SimpleNamespace(party_accounts=lambda _: [("leader", "", True, False), ("member", "", False, False)],
                                 account_clients=clients, stop_account=stop, is_account_running=lambda u: False)
        class Thread:
            def __init__(self, target, **kw): self.target = target
            def start(self): self.target()
        ns = {"_get_runner": lambda: runner, "json": json, "threading": SimpleNamespace(Thread=Thread),
              "time": SimpleNamespace(monotonic=lambda: 0), "log": logging.getLogger("test")}
        result = json.loads(function("agent_bridge.py", "safe_logout_all_json", ns)())
        self.assertTrue(result["ok"])
        self.assertEqual(events, ["member", "leader"])

    def test_boss_count_and_cooldown_are_server_owned(self):
        tree = ast.parse((ROOT / "train_bot/client.py").read_text())
        cls = next(n for n in tree.body if isinstance(n, ast.ClassDef) and any(getattr(f, "name", "") == "do_legion_boss" for f in n.body))
        method = next(n for n in cls.body if getattr(n, "name", "") == "do_legion_boss")
        source = ast.unparse(method)
        self.assertNotIn("self.relogin()", source)
        self.assertNotIn("self.legion_boss_count +=", source)
        self.assertNotIn("self.legion_boss_next =", source)

    def test_independent_daily_and_walk_combat(self):
        source = (ROOT / "train_bot/run_party_digioi.py").read_text()
        self.assertIn('if not str(task).startswith("team_dungeon"):', source)
        self.assertIn('route_flee = expected <= 1 and kind != "train"', source)
        self.assertIn('c.navigate_to(tx, ty, flee=False,', source)

    def test_unknown_exp_level_not_guessed(self):
        ns = {"_CHAR_EXP_LEVELS": {155: (236222752, 6290520)}}
        fn = function("agent_bridge.py", "_level_exp_values", ns)
        self.assertEqual(fn(SimpleNamespace(char_level=156, char_exp=250000000)), (None, None, None))
        self.assertEqual(fn(SimpleNamespace(char_level=155, char_exp=237360504)), (1137752, 6290520, 5152768))


if __name__ == "__main__":
    unittest.main()
