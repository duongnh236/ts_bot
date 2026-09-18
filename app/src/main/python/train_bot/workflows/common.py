"""Common workflow implementation; explicit dependencies, no runner imports.

Moved without changing the existing game protocol/route algorithms.
"""


def _workflow_leave_current_area(c, stopped_fn, *, services):
    """Finish battle, stop DG movement and release farm party before a city teleport."""
    time = services.time
    c.stop_run_around()
    c._dg_pursuit_paused = True
    c.set_party_invite_ready(False)
    c.flee_mode = False
    c._wait_combat_clear(idle=2.0, cap=120.0)
    if stopped_fn() or c.in_combat(idle_secs=2.0):
        raise RuntimeError("Chưa hết trận/đã dừng; không teleport")
    if c.in_di_gioi():
        c.exit_di_gioi()
        if stopped_fn() or c.in_di_gioi():
            raise RuntimeError("Chưa được server xác nhận thoát Dị giới")
    if c.party_members or (c.party_leader and c.party_leader != c.self_entity):
        c.leave_party()
        deadline = time.time() + 15
        while c.party_members or (c.party_leader and c.party_leader != c.self_entity):
            if stopped_fn() or time.time() > deadline:
                raise RuntimeError("Server chưa xác nhận rời party farm")
            time.sleep(0.5)
    c.flee_mode = False
