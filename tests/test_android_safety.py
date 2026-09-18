"""Dependency-free regression checks for Android orchestration changes."""
import ast
import json
import logging
import threading
import time
import unittest
from collections import deque
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

ROOT = Path(__file__).resolve().parents[1] / "app/src/main/python"


def function(path, name, namespace):
    tree = ast.parse((ROOT / path).read_text())
    node = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == name)
    exec(compile(ast.Module(body=[node], type_ignores=[]), str(path), "exec"), namespace)
    return namespace[name]


class SafetyTests(unittest.TestCase):
    def test_team_json_is_bounded_and_omits_credentials(self):
        import io
        cfg = SimpleNamespace(PARTY_LEADER_ACC={0: "leader"}, map_display_name=lambda _: "Trác Quận")
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
        st = {"lock": threading.RLock(), "cmd_gen": 7, "ui_dg_train_target": (21001, 100, 200)}
        clients = {u: SimpleNamespace(running=True, in_di_gioi=lambda: False,
                                     _dg_train_ready_token=7 if u == "leader" else None,
                                     stop_run_around=Mock()) for u in ("leader", "member")}
        callbacks = []
        dispatch = Mock(side_effect=lambda *args: st.update(cmd_gen=8))
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
        dispatch.assert_called_once_with(0, 21001, 100, 200)
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
