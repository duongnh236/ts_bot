"""Read random-encounter seed points from SceneFight_C.dat."""
from __future__ import annotations

from dataclasses import dataclass
import os
import struct


RECORD_SIZE = 25


@dataclass(frozen=True)
class SceneFightEntry:
    map_id: int
    point: tuple[int, int]
    level_range: tuple[int, int]


def load_scene_fight(path: str) -> dict[int, SceneFightEntry]:
    with open(path, "rb") as fh:
        data = fh.read()
    if len(data) < 4:
        raise ValueError("SceneFight_C.dat is truncated")
    count = struct.unpack_from("<I", data)[0]
    if len(data) != 4 + count * RECORD_SIZE:
        raise ValueError("SceneFight_C.dat has an invalid record count")
    entries = {}
    for index in range(count):
        record = data[4 + index * RECORD_SIZE:4 + (index + 1) * RECORD_SIZE]
        map_id, x, y, min_level, max_level = struct.unpack_from("<HHHHH", record, 5)
        entries[map_id] = SceneFightEntry(
            map_id, (x, y), (min_level, max_level)
        )
    return entries


def get_scene_fight_seed(map_id: int, path: str | None = None):
    if path is None:
        from . import config
        path = getattr(config, "SCENE_FIGHT_PATH", "")
    if not path or not os.path.isfile(path):
        return None
    try:
        entry = load_scene_fight(path).get(int(map_id))
    except (OSError, ValueError):
        return None
    return entry.point if entry else None
