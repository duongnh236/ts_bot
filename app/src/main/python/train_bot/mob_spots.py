"""Atomic runtime cache for learned monster-area center points."""
from __future__ import annotations

import json
import os
import threading
import time

from ._appdir import app_dir


SCHEMA_VERSION = 1
_LOCK = threading.Lock()


def _path() -> str:
    try:
        from . import config
        configured = getattr(config, "MOB_SPOTS_CACHE_PATH", None)
        if configured:
            return configured
    except Exception:
        pass
    return os.path.join(app_dir(), "mob_spots.json")


def _load_unlocked() -> dict:
    try:
        with open(_path(), encoding="utf-8") as fh:
            data = json.load(fh)
        if isinstance(data, dict) and data.get("version") == SCHEMA_VERSION:
            data.setdefault("maps", {})
            return data
    except Exception:
        pass
    return {"version": SCHEMA_VERSION, "maps": {}}


def _write_unlocked(data: dict) -> None:
    path = _path()
    directory = os.path.dirname(path)
    if directory:
        os.makedirs(directory, exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(data, fh, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


def _points(centers) -> list[list[int]]:
    out = []
    for center in centers or ():
        point = getattr(center, "point", center)
        value = [int(point[0]), int(point[1])]
        if value not in out:
            out.append(value)
    return out


def load_progress(map_id: int, fingerprint: str) -> dict:
    with _LOCK:
        entry = _load_unlocked().get("maps", {}).get(str(int(map_id)), {})
        if entry.get("fingerprint") != str(fingerprint):
            return {}
        return json.loads(json.dumps(entry))


def load_complete_centers(map_id: int, fingerprint: str):
    entry = load_progress(map_id, fingerprint)
    if entry.get("status") != "complete" or not entry.get("centers"):
        return None
    return [tuple(map(int, point)) for point in entry.get("centers", [])]


def load_safe(map_id: int, fingerprint: str):
    safe = load_progress(map_id, fingerprint).get("safe")
    if not isinstance(safe, list) or len(safe) != 2:
        return None
    try:
        return int(safe[0]), int(safe[1])
    except (TypeError, ValueError):
        return None


def save_safe(map_id: int, fingerprint: str, safe) -> None:
    point = [int(safe[0]), int(safe[1])]
    with _LOCK:
        data = _load_unlocked()
        maps = data.setdefault("maps", {})
        key = str(int(map_id))
        entry = maps.get(key, {})
        if entry.get("fingerprint") != str(fingerprint):
            entry = {
                "fingerprint": str(fingerprint),
                "status": "incomplete",
                "coverage": {},
                "settings": {},
                "centers": [],
            }
        entry["safe"] = point
        entry["updated_at"] = int(time.time())
        maps[key] = entry
        _write_unlocked(data)


def _save(map_id, fingerprint, status, centers, coverage, settings) -> None:
    with _LOCK:
        data = _load_unlocked()
        maps = data.setdefault("maps", {})
        key = str(int(map_id))
        previous = maps.get(key, {})
        entry = {
            "fingerprint": str(fingerprint),
            "status": str(status),
            "updated_at": int(time.time()),
            "coverage": dict(coverage or {}),
            "settings": dict(settings or {}),
            "centers": _points(centers),
        }
        if previous.get("fingerprint") == str(fingerprint) and "safe" in previous:
            entry["safe"] = previous["safe"]
        maps[key] = entry
        _write_unlocked(data)


def save_progress(map_id: int, fingerprint: str, completed_stations, centers,
                  coverage, settings) -> None:
    coverage = dict(coverage or {})
    coverage["completed"] = sorted({int(i) for i in completed_stations or ()})
    _save(map_id, fingerprint, "incomplete", centers, coverage, settings)


def save_complete(map_id: int, fingerprint: str, centers, coverage, settings) -> None:
    points = _points(centers)
    _save(map_id, fingerprint, "complete" if points else "empty",
          points, coverage, settings)
