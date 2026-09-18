import sys
import threading
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "app/src/main/python"))
from train_bot.workflows.city_exit import gather


class CityExitTests(unittest.TestCase):
    def fixture(self):
        st = {"lock": threading.RLock(), "cmd_gen": 3}
        clients = {}
        for name, channel in (("leader", 3), ("member", 2)):
            c = SimpleNamespace(running=True, current_map=23001, current_channel=channel,
                                pos=(100, 100), build_smart_scene_route=Mock(return_value={
                                    "legs": [{"target_scene": 23002}]}))
            c.follow_smart_scene_route = Mock(side_effect=lambda source, dest, safe, client=c, **kw:
                (setattr(client, "current_map", dest) or True))
            c.navigate_to = Mock(return_value=True)
            c.switch_channel = Mock(side_effect=lambda ch, client=c, **kw:
                (setattr(client, "current_channel", ch) or True))
            clients[name] = c
        services = SimpleNamespace(time=time, account_clients=clients,
            config=SimpleNamespace(TRAIN_MAPS={}), _nearest_safe=Mock(),
            _workflow_leave_current_area=Mock(), set_account_activity=Mock(), log=Mock())
        return st, clients, services

    def run_team(self, st, clients, services, failed=False):
        results = {}
        def run(name):
            results[name] = gather(clients[name], st, name, list(clients), 3, 23001, 23803,
                                   not (failed and name == "member"), name == "leader",
                                   lambda: False, services=services)
        threads = [threading.Thread(target=run, args=(name,), daemon=True) for name in clients]
        for t in threads:
            t.start()
        for t in threads:
            t.join(5)
            self.assertFalse(t.is_alive())
        return results

    def test_failed_city_sync_walks_everyone_out_before_party(self):
        st, clients, services = self.fixture()
        self.assertEqual(self.run_team(st, clients, services, failed=True), {"leader": True, "member": True})
        self.assertEqual(st["train_city_sync"]["arrived"], set(clients))
        for c in clients.values():
            self.assertEqual(c.current_map, 23002)
            self.assertEqual(c.current_channel, 3)
            c.follow_smart_scene_route.assert_called_once()
            self.assertFalse(c.follow_smart_scene_route.call_args.kwargs["flee"])
            self.assertTrue(c.navigate_to.call_args.kwargs["require_smart_path"])

    def test_successful_city_sync_keeps_existing_flow(self):
        st, clients, services = self.fixture()
        self.assertTrue(all(self.run_team(st, clients, services).values()))
        for c in clients.values():
            c.follow_smart_scene_route.assert_not_called()
        services._workflow_leave_current_area.assert_not_called()

    def test_new_command_cancels_without_walking(self):
        st, clients, services = self.fixture()
        st["cmd_gen"] = 4
        self.assertFalse(gather(clients["member"], st, "member", list(clients), 3,
                               23001, 23803, False, False, lambda: False, services=services))
        clients["member"].follow_smart_scene_route.assert_not_called()
        self.assertNotIn("train_city_sync", st)

    def test_city_rejection_does_not_request_back_to_city_regroup(self):
        from train_bot.workflows.train import _train_retry_leader_channel
        st, clients, services = self.fixture()
        st["manual_route_plan"] = {"city": 23001}
        st["manual_train_channel"] = 3
        member = clients["member"]
        member._chan_switch_result = 4
        member.switch_channel = Mock(return_value=False)
        self.assertFalse(_train_retry_leader_channel(member, st, "member", "member", 3,
                                                     lambda: False, services=services))
        self.assertNotIn("train_channel_regroup", st)
        self.assertTrue(member.running)
