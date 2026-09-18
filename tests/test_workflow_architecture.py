"""Workflow boundaries and cancellation, without importing the live runner."""
import ast
import sys
import threading
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

ROOT = Path(__file__).resolve().parents[1] / "app/src/main/python"
sys.path.insert(0, str(ROOT))
from train_bot.workflows.lifecycle import WorkflowCoordinator, activate, activate_locked


def bounded(fn):
    """A regression must fail promptly, not hang the entire test process."""
    def run(self):
        errors = []
        def worker():
            try:
                fn(self)
            except BaseException as exc:
                errors.append(exc)
        thread = threading.Thread(target=worker, daemon=True)
        thread.start()
        thread.join(3)
        self.assertFalse(thread.is_alive(), "workflow entry deadlocked with the real party Lock")
        if errors:
            raise errors[0]
    return run


class ArchitectureTests(unittest.TestCase):
    @bounded
    def test_real_entry_points_cancel_previous_flow_and_keep_old_commands(self):
        from train_bot.workflows import train, daily
        st = {"lock": threading.Lock(), "cmd_gen": 0, "manual_train_users": ["leader"]}
        for name in ("manual_route_plan_ready", "manual_route_source_done", "manual_route_party_ready",
                     "manual_route_done", "manual_train_channel_ready"):
            st[name] = threading.Event()
        services = SimpleNamespace(
            _pstate=lambda p: st, activate_workflow=activate_locked,
            account_clients={"leader": SimpleNamespace(running=True)},
            config=SimpleNamespace(PARTY_CONFIG={0: {}}, PARTY_LEADER_ACC={0: "leader"}),
            party_accounts=lambda p: [("leader", "", True, True)],
            is_account_running=lambda u: True, dat_party_dang_gom=Mock(), log=Mock(), time=time)
        dg = activate(st, "digioi")
        train.party_train_map(0, 23803, 550, 590, services=services)
        train_session = st["android_workflow_session"]
        self.assertFalse(dg.active)
        self.assertEqual(train_session.kind, "train")
        self.assertEqual(st["cmd"], ("train", 0, 23803, 550, 590))
        self.assertEqual(st["ui_train_target"], (23803, 550, 590))
        daily.party_daily_tasks(0, ["world_boss", "solo_dungeon"], services=services)
        self.assertFalse(train_session.active)
        self.assertEqual(st["android_workflow_session"].kind, "daily")
        self.assertIsNone(st["ui_train_target"])
        self.assertEqual(st["cmd"], ("daily", ("solo_dungeon", "world_boss")))
        active = st["android_workflow_session"]
        with self.assertRaises(ValueError):
            train.party_train_map(0, 0, 550, 590, services=services)
        self.assertTrue(active.active)
        command = st["cmd"]
        self.assertFalse(train.party_train_map(0, 23803, 550, 590, services=services,
                                               expected_generation=st["cmd_gen"] - 1))
        self.assertIs(st["android_workflow_session"], active)
        self.assertEqual(st["cmd"], command)

    @bounded
    def test_activation_with_real_lock_supports_both_entry_contracts(self):
        st = {"lock": threading.Lock()}
        first = activate(st, "train")
        with st["lock"]:
            second = activate_locked(st, "digioi")
        self.assertFalse(first.active)
        self.assertTrue(second.active)

    def test_switch_cancels_old_owner_without_sharing_new_state(self):
        coordinator = WorkflowCoordinator()
        dg = coordinator.start("digioi")
        dg.state["done"] = {"member"}
        train = coordinator.start("train")
        self.assertFalse(dg.active)
        self.assertFalse(coordinator.owns(dg))
        self.assertTrue(coordinator.owns(train))
        self.assertEqual(train.state, {})
        train.state["done"] = set()
        self.assertEqual(dg.state["done"], {"member"})
        daily = coordinator.start("daily")
        self.assertFalse(train.active)
        self.assertTrue(coordinator.owns(daily))

    def test_parties_and_repeated_runs_are_independent(self):
        first = {"lock": threading.RLock()}
        second = {"lock": threading.RLock()}
        a = activate(first, "train")
        b = activate(second, "digioi")
        c = activate(first, "train")
        self.assertFalse(a.active)
        self.assertTrue(b.active)
        self.assertTrue(c.active)
        self.assertEqual(c.revision, 2)
        self.assertEqual(b.revision, 1)

    def test_invalid_workflow_does_not_cancel_active_run(self):
        coordinator = WorkflowCoordinator()
        current = coordinator.start("train")
        with self.assertRaises(ValueError):
            coordinator.start("")
        self.assertTrue(coordinator.owns(current))

    def test_workflow_modules_do_not_import_runner_or_mutate_module_globals(self):
        for path in (ROOT / "train_bot/workflows").glob("*.py"):
            tree = ast.parse(path.read_text())
            for node in ast.walk(tree):
                if isinstance(node, ast.ImportFrom):
                    self.assertNotIn("run_party_digioi", node.module or "")
                if isinstance(node, ast.Import):
                    self.assertTrue(all("run_party_digioi" not in a.name for a in node.names))
                self.assertNotIsInstance(node, ast.Global)


if __name__ == "__main__":
    unittest.main()
