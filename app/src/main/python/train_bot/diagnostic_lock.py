"""Fail-fast non-reentrant lock for shared workflow state.

Workflow state is deliberately non-reentrant: nested acquisition means an
ownership bug. A normal Lock hangs forever and hides the call site; this lock
keeps the same contract but turns that condition into a bounded diagnostic.
"""
import threading


class WorkflowLockError(RuntimeError):
    pass


class WorkflowLock:
    def __init__(self, name="workflow", max_wait=15.0):
        self._lock = threading.Lock()
        self._name = str(name)
        self._max_wait = float(max_wait)
        self._owner = None

    def acquire(self, blocking=True, timeout=-1):
        current = threading.get_ident()
        if self._owner == current:
            if not blocking:
                return False
            raise WorkflowLockError("%s: cùng thread lấy khóa lần hai" % self._name)
        if not blocking:
            acquired = self._lock.acquire(False)
        else:
            wait = self._max_wait if timeout is None or timeout < 0 else min(float(timeout), self._max_wait)
            acquired = self._lock.acquire(True, wait)
            if not acquired:
                raise WorkflowLockError(
                    "%s: chờ khóa quá %.1fs (owner thread=%s)" %
                    (self._name, wait, self._owner))
        if acquired:
            self._owner = current
        return acquired

    def release(self):
        current = threading.get_ident()
        if self._owner != current:
            raise WorkflowLockError("%s: thread không sở hữu khóa nhưng gọi release" % self._name)
        self._owner = None
        self._lock.release()

    def locked(self):
        return self._lock.locked()

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, exc_type, exc, tb):
        self.release()
        return False
