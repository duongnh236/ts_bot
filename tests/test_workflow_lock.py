import threading
import time
import unittest
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "app/src/main/python"))
from train_bot.diagnostic_lock import WorkflowLock, WorkflowLockError


class WorkflowLockTests(unittest.TestCase):
    def test_same_thread_reentry_fails_instead_of_hanging(self):
        lock = WorkflowLock("party-1", max_wait=.1)
        with lock:
            with self.assertRaisesRegex(WorkflowLockError, "lấy khóa lần hai"):
                with lock:
                    pass

    def test_contention_is_bounded_and_reports_owner(self):
        lock = WorkflowLock("party-1", max_wait=.05)
        lock.acquire()
        errors = []
        worker = threading.Thread(target=lambda: self._capture(errors, lock), daemon=True)
        worker.start(); worker.join(.5)
        lock.release()
        self.assertFalse(worker.is_alive())
        self.assertEqual(len(errors), 1)
        self.assertIn("owner thread=", str(errors[0]))

    @staticmethod
    def _capture(errors, lock):
        try:
            lock.acquire()
        except Exception as exc:
            errors.append(exc)

    def test_normal_cross_thread_handoff_is_unchanged(self):
        lock = WorkflowLock("party-1", max_wait=.5)
        seen = []
        with lock:
            worker = threading.Thread(target=lambda: self._append_under_lock(lock, seen), daemon=True)
            worker.start()
            time.sleep(.02)
            self.assertEqual(seen, [])
        worker.join(.5)
        self.assertEqual(seen, [True])

    @staticmethod
    def _append_under_lock(lock, seen):
        with lock:
            seen.append(True)
