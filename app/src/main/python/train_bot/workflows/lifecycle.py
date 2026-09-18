"""Independent state and cancellation ownership for one party's workflow."""
from dataclasses import dataclass, field
from threading import Event, RLock


@dataclass
class WorkflowSession:
    kind: str
    revision: int
    state: dict = field(default_factory=dict)
    cancelled: Event = field(default_factory=Event)

    @property
    def active(self):
        return not self.cancelled.is_set()


class WorkflowCoordinator:
    """One active owner, fresh state for every run; unrelated parties stay separate."""
    def __init__(self):
        self._lock = RLock()
        self._revision = 0
        self.current = None

    def start(self, kind):
        if not kind:
            raise ValueError("Workflow kind is required")
        with self._lock:
            if self.current is not None:
                self.current.cancelled.set()
            self._revision += 1
            self.current = WorkflowSession(str(kind), self._revision)
            return self.current

    def owns(self, session):
        with self._lock:
            return self.current is session and session.active


def activate(st, kind):
    """Legacy-state boundary; only this adapter installs the active session."""
    with st["lock"]:
        return activate_locked(st, kind)


def activate_locked(st, kind):
    """Caller MUST hold the party lock; do not acquire a non-reentrant Lock twice."""
    coordinator = st.setdefault("android_workflow_coordinator", WorkflowCoordinator())
    session = coordinator.start(kind)
    st["android_workflow_session"] = session
    return session
