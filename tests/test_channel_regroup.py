"""Safe-before-leave ordering for split-channel train teams."""
import sys
import threading
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "app/src/main/python"))
from train_bot.workflows.channel_regroup import request, tick


class ChannelRegroupTests(unittest.TestCase):
    def test_leader_detects_split_channel_on_farm_and_enters_safe_flow(self):
        from train_bot.workflows.train import _android_train_recovery_tick
        st,leader,member,services,actions=self.fixture()
        st.pop("train_channel_regroup")
        st["ui_train_phase"]="farming"
        self.assertTrue(_android_train_recovery_tick(leader,st,"leader",0,lambda:False,services=services))
        self.assertEqual(actions,["safe","leave:leader","town:leader"])
        self.assertEqual(st["train_channel_regroup"]["phase"],"city")
        services.party_train_map.assert_not_called()

    def fixture(self, safe_ok=True):
        actions = []
        st = {"lock": threading.Lock(), "cmd_gen": 8, "cmd": ("train",),
              "ui_train_target": (23803, 550, 590), "manual_train_users": ["leader", "member"]}
        leader = SimpleNamespace(running=True, current_map=23803, current_channel=11,
            pos=(550, 590), nearest_smart_city=Mock(return_value={"city": 23001, "flag": 17}))
        member = SimpleNamespace(running=True, current_map=23803, current_channel=2)
        def navigate(*a, **kw):
            actions.append("safe")
            return safe_ok
        leader.navigate_to = Mock(side_effect=navigate)
        for name, c in (("leader", leader), ("member", member)):
            def town(*a, client=c, who=name, **kw):
                actions.append("town:"+who)
                client.current_map = a[0]
            c.go_to_town = Mock(side_effect=town)
        def leave(c, abort):
            actions.append("leave:"+("leader" if c is leader else "member"))
        def dispatch(*args, **kwargs):
            self.assertTrue(st["lock"].acquire(blocking=False), "dispatch still holds party lock")
            st["lock"].release()
            actions.append("restart")
        restart = Mock(side_effect=dispatch)
        services = SimpleNamespace(config=SimpleNamespace(PARTY_LEADER_ACC={0:"leader"},
            TRAIN_MAPS={23803:{"safe":[(230,310)]}}), _nearest_safe=lambda pos, pts: pts[0] if pts else None,
            _workflow_leave_current_area=leave, account_clients={"leader":leader,"member":member},
            party_train_map=restart, set_account_activity=Mock(), log=Mock())
        request(st,"member",8,"full")
        return st, leader, member, services, actions

    def test_no_member_leaves_until_leader_reaches_safe_and_all_city_arrivals_before_restart(self):
        st, leader, member, services, actions = self.fixture()
        self.assertTrue(tick(member,st,"member",0,lambda:False,services=services))
        self.assertEqual(actions, [])
        tick(leader,st,"leader",0,lambda:False,services=services)
        self.assertEqual(actions, ["safe","leave:leader","town:leader"])
        services.party_train_map.assert_not_called()
        tick(member,st,"member",0,lambda:False,services=services)
        tick(leader,st,"leader",0,lambda:False,services=services)
        self.assertEqual(actions[-3:], ["leave:member","town:member","restart"])
        services.party_train_map.assert_called_once_with(0,23803,550,590,expected_generation=8)

    def test_failed_safe_path_keeps_party_intact(self):
        st,leader,member,services,actions=self.fixture(False)
        tick(leader,st,"leader",0,lambda:False,services=services)
        self.assertEqual(actions,["safe"])
        self.assertEqual(st["train_channel_regroup"]["phase"],"safe")
        services.party_train_map.assert_not_called()

    def test_new_flow_cancels_regroup_without_teleport(self):
        st,leader,member,services,actions=self.fixture()
        st["cmd_gen"]=9
        self.assertFalse(tick(leader,st,"leader",0,lambda:False,services=services))
        self.assertFalse(actions)
        self.assertNotIn("train_channel_regroup",st)
