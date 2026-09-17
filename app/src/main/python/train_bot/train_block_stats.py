"""Train-map monster block statistics.

Counts the block shape of enemies seen at the start of each train-map battle,
grouped by map id and configured mob point.
"""
from __future__ import annotations

import json
import os
import threading
import time
from typing import Iterable

from ._appdir import app_dir


_LOCK = threading.Lock()
_FILE = "train_block_stats.json"
MAX_SPOT_BATTLES = 100_000   # toi da so tran ghi/diem (du thay ti le, file khoi phinh)
MAX_SPOT_MOBS = 100_000      # toi da so CON quai ghi/diem - dem RIENG voi so tran

# EElement (Logic_Fight_Skill.lua:547). Npc_C.dat dung 0 cho VO HE; so 6 (None) khong con nao dung.
ELEMENT_NAMES = {0: "Vô hệ", 1: "Địa", 2: "Thủy", 3: "Hỏa", 4: "Phong",
                 5: "Tâm", 6: "Vô hệ", 7: "Quang", 8: "Ám"}

_NPC_TABLE = None


def _path() -> str:
    return os.path.join(app_dir(), _FILE)


def spot_key(spot) -> str:
    x, y = spot
    return f"{int(x)},{int(y)}"


def _load_unlocked() -> dict:
    path = _path()
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict):
            data.setdefault("version", 1)
            data.setdefault("maps", {})
            return data
    except Exception:
        pass
    # Android ships a seed copy in assets/train_bot_data. On first write, the
    # runtime file in app_dir() takes over and keeps accumulating.
    try:
        from .config import _read_asset
        data = json.loads(_read_asset(_FILE))
        if isinstance(data, dict):
            data.setdefault("version", 1)
            data.setdefault("maps", {})
            return data
    except Exception:
        pass
    return {"version": 1, "maps": {}}


def load_stats() -> dict:
    with _LOCK:
        return _load_unlocked()


def _save_unlocked(data: dict) -> None:
    path = _path()
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


def _runs(cols: Iterable[int]) -> list[list[int]]:
    out = []
    cur = []
    for c in sorted(set(int(x) for x in cols)):
        if cur and c != cur[-1] + 1:
            out.append(cur)
            cur = []
        cur.append(c)
    if cur:
        out.append(cur)
    return out


def cells_from_enemy_slots(enemy_slots: Iterable[int]) -> list[tuple[int, int]]:
    """Convert internal enemy pos (row*10 + col) to (row, col).

    The game sends row 0/1 and position 0..4 separately. The bot stores that
    as row*10 + col for combat targeting.
    """
    cells = []
    for pos in enemy_slots:
        pos = int(pos)
        if pos >= 10:
            row, col = divmod(pos, 10)
        else:
            row, col = 0, pos
        if row in (0, 1) and 0 <= col <= 4:
            cell = (row, col)
            if cell not in cells:
                cells.append(cell)
    return sorted(cells)


def block_pattern_from_cells(cells: Iterable[tuple[int, int]]) -> str:
    """Return row-only block shape like '3x1 + 1x1'.

    Blocks are counted only by horizontal runs on each enemy row. Vertical
    alignment is intentionally ignored: two rows of 3 enemies become
    '3x1 + 3x1', not '3x2'.
    """
    rows = {0: set(), 1: set()}
    for row, col in cells:
        row = int(row)
        col = int(col)
        if row in rows and 0 <= col <= 4:
            rows[row].add(col)

    blocks = []
    for row in (0, 1):
        for run in _runs(rows[row]):
            blocks.append((run[0], row, len(run), 1))

    if not blocks:
        return "0"
    blocks.sort(key=lambda b: (b[0], b[1], -b[2], -b[3]))
    counts = {}
    order = []
    for _col, _row, width, _height in blocks:
        if width not in counts:
            counts[width] = 0
            order.append(width)
        counts[width] += 1
    return " + ".join(f"{w}x{counts[w]}" for w in order)


def block_pattern_from_slots(enemy_slots: Iterable[int]) -> str:
    return block_pattern_from_cells(cells_from_enemy_slots(enemy_slots))


def _tids_for_cells(cells, pos_tids) -> list[int]:
    """template_id cua TUNG O quai. Thieu du lieu 1 o -> tra rong, BO QUA ca tran.

    Ghi thieu con lam lech ti le he/level (cai duy nhat bang nay dung de tinh), nen tha khong ghi.
    """
    if not pos_tids:
        return []
    out = []
    for row, col in cells:
        got = pos_tids.get(row * 10 + col)
        if not got:
            return []
        out.append(int(sorted(got)[0]))
    return out


def record_battle(map_id: int, spot, enemy_slots: Iterable[int], enemy_pos_tids=None) -> dict | None:
    cells = cells_from_enemy_slots(enemy_slots)
    if not cells:
        return None
    pattern = block_pattern_from_cells(cells)
    tids = _tids_for_cells(cells, enemy_pos_tids)
    mkey = str(int(map_id))
    skey = spot_key(spot)
    now = int(time.time())

    with _LOCK:
        data = _load_unlocked()
        spot_data = (data.setdefault("maps", {})
                         .setdefault(mkey, {})
                         .setdefault("spots", {})
                         .setdefault(skey, {"total": 0, "patterns": {}}))
        # 2 bo dem DOC LAP: block dem theo TRAN, quai dem theo CON. Diem da day tran van con ghi
        # duoc quai (va nguoc lai) - neu dung chung 1 tran thi cai day truoc giet luon cai kia.
        block_full = int(spot_data.get("total", 0)) >= MAX_SPOT_BATTLES
        mob_full = sum(int(v) for v in (spot_data.get("mobs") or {}).values()) >= MAX_SPOT_MOBS
        if block_full and mob_full:
            return None

        patterns = spot_data.setdefault("patterns", {})
        if not block_full:
            spot_data["total"] = int(spot_data.get("total", 0)) + 1
            patterns[pattern] = int(patterns.get(pattern, 0)) + 1
            spot_data["last_slots"] = [f"{row}:{col}" for row, col in cells]
            spot_data["last_pattern"] = pattern
        if tids and not mob_full:
            # Dem theo CON: 3 con lv89 + 1 con lv90 -> +3 va +1 (ra dung TI LE quai cua diem).
            mobs = spot_data.setdefault("mobs", {})
            for tid in tids:
                k = str(tid)
                mobs[k] = int(mobs.get(k, 0)) + 1
        spot_data["updated_at"] = now
        _save_unlocked(data)
        return {"total": int(spot_data.get("total", 0)), "pattern": pattern,
                "count": int(patterns.get(pattern, 0))}


def get_spot_summary(map_id: int, spot) -> dict:
    data = load_stats()
    return (data.get("maps", {})
                .get(str(int(map_id)), {})
                .get("spots", {})
                .get(spot_key(spot), {"total": 0, "patterns": {}}))


def format_patterns(patterns: dict, limit: int = 6) -> str:
    rows = sorted(((str(k), int(v)) for k, v in (patterns or {}).items()),
                  key=lambda kv: (-kv[1], kv[0]))
    return ", ".join(f"{k}: {v}" for k, v in rows[:limit])


def _mobs_in_pattern(pattern: str) -> int:
    """'3x1 + 1x1' -> 4 con. Sai dinh dang -> 0."""
    total = 0
    for part in str(pattern).split("+"):
        try:
            w, c = part.strip().split("x")
            total += int(w) * int(c)
        except Exception:
            return 0
    return total


def spot_mob_range(patterns: dict):
    """(min, max) SO CON quai 1 tran. Khong loc gi - the tran nao da ghi la tinh."""
    nums = [n for n in (_mobs_in_pattern(p) for p in (patterns or {})) if n > 0]
    if not nums:
        return None
    return min(nums), max(nums)


def format_mob_range(patterns: dict) -> str:
    """'3-5', hoac '3' khi luc nao cung 3 con."""
    rng = spot_mob_range(patterns)
    if not rng:
        return ""
    lo, hi = rng
    return str(lo) if lo == hi else f"{lo}-{hi}"


def _npc_table() -> dict:
    """npc_table.json (AUTO tools/crack_npc_table.py): tid -> {name, level, element, ...}.

    File thong ke chi luu template_id; he/level tra o DAY luc hien. Khong chep he/level vao file
    thong ke: chep du lieu game sang file khac la se lech khi game doi (bai hoc Servers.kt).
    """
    global _NPC_TABLE
    if _NPC_TABLE is None:
        _NPC_TABLE = {}
        try:
            from .config import _base_dir
            with open(os.path.join(_base_dir(), "npc_table.json"), encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, dict):
                _NPC_TABLE = data
        except Exception:
            pass
        if not _NPC_TABLE:      # Android: file nam trong assets/train_bot_data
            try:
                from .config import _read_asset
                data = json.loads(_read_asset("npc_table.json"))
                if isinstance(data, dict):
                    _NPC_TABLE = data
            except Exception:
                pass
    return _NPC_TABLE


def mob_label(tid, short: bool = False) -> str:
    """tid -> 'Thủy lv110' (short=True: 'Thủy 110', cho dropdown chat cho).

    Khong tra duoc thi giu nguyen id (khong doan bua).
    """
    info = _npc_table().get(str(int(tid))) or {}
    if not info:
        return f"id {tid}"
    elem = ELEMENT_NAMES.get(int(info.get("element") or 0), "?")
    lv = int(info.get("level") or 0)
    return f"{elem} {lv}" if short else f"{elem} lv{lv}"


def mob_name(tid) -> str:
    info = _npc_table().get(str(int(tid))) or {}
    return str(info.get("name") or f"id {tid}")


def format_mobs(mobs: dict, limit: int = 8, short: bool = False) -> str:
    """'Thủy lv110: 7777, Hỏa lv112: 6666' - gop cac tid cung he+level lam mot.

    short=True -> bo so dem va chu 'lv': 'Thủy 110, Địa 112' (dung cho dropdown diem quai).
    """
    groups = {}
    for tid, n in (mobs or {}).items():
        try:
            label = mob_label(tid, short=short)
        except Exception:
            label = f"id {tid}"
        groups[label] = groups.get(label, 0) + int(n)
    rows = sorted(groups.items(), key=lambda kv: (-kv[1], kv[0]))
    if short:
        return ", ".join(k for k, _v in rows[:limit])
    return ", ".join(f"{k}: {v}" for k, v in rows[:limit])
