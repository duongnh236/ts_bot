"""Dependency-free regression checks for Android orchestration changes."""
import ast
import json
import logging
import unittest
from collections import deque
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

ROOT = Path(__file__).resolve().parents[1] / "app/src/main/python"


def function(path, name, namespace):
    tree = ast.parse((ROOT / path).read_text())
    node = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == name)
    exec(compile(ast.Module(body=[node], type_ignores=[]), str(path), "exec"), namespace)
    return namespace[name]


class SafetyTests(unittest.TestCase):
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
