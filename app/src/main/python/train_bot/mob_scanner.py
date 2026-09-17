"""Learn bounded monster patrols and reduce each roaming area to one center."""
from __future__ import annotations

from dataclasses import dataclass, field
import math
import threading
import time


Point = tuple


@dataclass
class PatrolTrace:
    sample_count: int = 0
    unique_points: list[Point] = field(default_factory=list)
    edges: set[tuple[Point, Point]] = field(default_factory=set)
    repeated_edge_count: int = 0
    last_point: Point | None = None
    last_new_at: float = 0.0

    def add(self, point: Point, now: float) -> bool:
        self.sample_count += 1
        is_new = point not in self.unique_points
        if is_new:
            self.unique_points.append(point)
            self.last_new_at = float(now)
        if self.last_point is not None and point != self.last_point:
            edge = (self.last_point, point)
            if edge in self.edges:
                self.repeated_edge_count += 1
            else:
                self.edges.add(edge)
        self.last_point = point
        return is_new

    @property
    def diameter(self) -> float:
        if len(self.unique_points) < 2:
            return 0.0
        return max(math.dist(a, b) for i, a in enumerate(self.unique_points)
                   for b in self.unique_points[i + 1:])

    def stable(self, now: float, quiet_seconds: float, min_samples: int,
               max_patrol_diameter: float) -> bool:
        return (
            self.sample_count >= min_samples
            and len(self.unique_points) >= 2
            and self.repeated_edge_count >= 1
            and self.diameter <= max_patrol_diameter
            and float(now) - self.last_new_at >= quiet_seconds
        )


@dataclass(frozen=True)
class CenterCandidate:
    point: Point
    monster_count: int
    confidence: float


@dataclass(frozen=True)
class LearnedRegion:
    center: CenterCandidate
    safe: Point | None


@dataclass(frozen=True)
class ScanResult:
    status: str
    centers: tuple[CenterCandidate, ...]
    visited: int
    total: int


class MobScanSession:
    def __init__(self, map_id: int, self_entity: bytes | None = None,
                 party_entities=None, quiet_seconds: float = 8.0,
                 min_samples: int = 3, max_patrol_diameter: float = 800.0,
                 merge_distance: float = 200.0):
        self.map_id = int(map_id)
        self.self_entity = bytes(self_entity) if self_entity else None
        self.party_entities = {bytes(e) for e in (party_entities or ()) if e}
        self.quiet_seconds = float(quiet_seconds)
        self.min_samples = int(min_samples)
        self.max_patrol_diameter = float(max_patrol_diameter)
        self.merge_distance = float(merge_distance)
        self._lock = threading.RLock()
        self._traces: dict[bytes, PatrolTrace] = {}
        self._players: set[bytes] = set()
        self._station_entities: set[bytes] = set()
        self._station_started = 0.0
        self._station_last_change = 0.0

    def begin_station(self, now: float) -> None:
        with self._lock:
            self._station_entities.clear()
            self._station_started = float(now)
            self._station_last_change = float(now)

    def mark_player(self, entity: bytes) -> None:
        entity = bytes(entity)
        with self._lock:
            self._players.add(entity)
            self._traces.pop(entity, None)
            self._station_entities.discard(entity)

    def observe_spawn(self, entity: bytes, map_id: int, x: int, y: int,
                      now: float) -> None:
        self._observe(entity, map_id, x, y, now)

    def observe_move(self, entity: bytes, map_id: int, x: int, y: int,
                     now: float) -> None:
        self._observe(entity, map_id, x, y, now)

    def _observe(self, entity: bytes, map_id: int, x: int, y: int,
                 now: float) -> None:
        entity = bytes(entity)
        if int(map_id) != self.map_id or not (0 < int(x) < 20000 and 0 < int(y) < 20000):
            return
        with self._lock:
            if (entity == self.self_entity or entity in self.party_entities
                    or entity in self._players):
                return
            is_new_entity = entity not in self._traces
            trace = self._traces.setdefault(entity, PatrolTrace())
            is_new_point = trace.add((int(x), int(y)), float(now))
            self._station_entities.add(entity)
            if is_new_entity or is_new_point:
                self._station_last_change = float(now)

    def candidate_count(self) -> int:
        with self._lock:
            return len(self._traces)

    def station_stable(self, now: float) -> bool:
        with self._lock:
            now = float(now)
            if now - self._station_last_change < self.quiet_seconds:
                return False
            if not self._station_entities:
                return now - self._station_started >= self.quiet_seconds
            return all(self._traces[e].stable(
                now, self.quiet_seconds, self.min_samples,
                self.max_patrol_diameter,
            ) for e in self._station_entities if e in self._traces)

    def stable_traces(self, now: float) -> list[PatrolTrace]:
        with self._lock:
            return [trace for trace in self._traces.values() if trace.stable(
                float(now), self.quiet_seconds, self.min_samples,
                self.max_patrol_diameter,
            )]

    def bounded_traces(self) -> list[PatrolTrace]:
        with self._lock:
            return [trace for trace in self._traces.values() if (
                trace.sample_count >= self.min_samples
                and len(trace.unique_points) >= 2
                and trace.diameter <= self.max_patrol_diameter
            )]


def _closest_pair(a: PatrolTrace, b: PatrolTrace):
    return min(((math.dist(pa, pb), pa, pb)
                for pa in a.unique_points for pb in b.unique_points),
               key=lambda item: item[0])


def _can_merge(a: PatrolTrace, b: PatrolTrace, distance: float, ground,
               map_id: int) -> bool:
    gap, pa, pb = _closest_pair(a, b)
    if gap > distance:
        return False
    if ground is None:
        return True
    path = ground.find_world_path(map_id, pa, pb)
    if not path:
        return False
    walked = sum(math.dist(p, q) for p, q in zip(path, path[1:]))
    return walked <= distance * 1.5


def _trace_groups(traces, distance: float, ground, map_id: int):
    parent = list(range(len(traces)))

    def find(index):
        while parent[index] != index:
            parent[index] = parent[parent[index]]
            index = parent[index]
        return index

    def union(first, second):
        first_root, second_root = find(first), find(second)
        if first_root != second_root:
            parent[second_root] = first_root

    for index, first in enumerate(traces):
        for other in range(index + 1, len(traces)):
            if _can_merge(first, traces[other], distance, ground, map_id):
                union(index, other)

    groups = {}
    for index, trace in enumerate(traces):
        groups.setdefault(find(index), []).append(trace)
    return list(groups.values())


def _medoid(points: list[Point]) -> Point:
    unique = list(dict.fromkeys(points))
    return min(unique, key=lambda point: (
        sum(math.dist(point, other) for other in unique), point[1], point[0]
    ))


def _bbox_center(group) -> Point:
    """TAM cua o vuong tuan tra. Moi con quai chay quanh 1 O VUONG (~200-470 don vi, nhin anh
    ve tu capture ro rang) -> tam bbox la diem GIUA bai, chuan hon medoid (medoid keo lech ve
    cho quai dung lau)."""
    points = [point for trace in group for point in trace.unique_points]
    xs = [p[0] for p in points]
    ys = [p[1] for p in points]
    return ((min(xs) + max(xs)) // 2, (min(ys) + max(ys)) // 2)


def _center_candidate(group, ground, map_id: int,
                      start: Point) -> CenterCandidate | None:
    point = _bbox_center(group)
    if ground is not None:
        point = ground.nearest_walkable_world(map_id, point, start)
        if point is None:
            return None
    confidence = min(
        1.0, 0.6 + 0.1 * sum(trace.repeated_edge_count for trace in group)
    )
    return CenterCandidate(tuple(map(int, point)), len(group), confidence)


def compute_centers(session: MobScanSession, ground, start: Point,
                    now: float | None = None,
                    stable_only: bool = True) -> list[CenterCandidate]:
    if now is None:
        import time
        now = time.monotonic()
    traces = (session.stable_traces(float(now)) if stable_only
              else session.bounded_traces())
    if not traces:
        return []
    # 1 CON QUAI = 1 BAI: moi con chay quanh 1 o vuong RIENG (xac nhan bang anh ve tu capture
    # map 20801: 16 o vuong tach bach). Gom nhom theo khoang cach lam 2 bai KE NHAU dinh lam
    # mot -> mat rat nhieu bai (200 -> chi con 7/16). Bo gom, moi trace 1 bai.
    groups = [[trace] for trace in traces]
    centers = [
        center for center in (
            _center_candidate(group, ground, session.map_id, start)
            for group in groups
        ) if center is not None
    ]
    return sorted(centers, key=lambda c: (-c.monster_count, -c.confidence,
                                          c.point[1], c.point[0]))


def compute_regions(session: MobScanSession, ground, start: Point,
                    fallback_safe: Point | None = None,
                    now: float | None = None,
                    stable_only: bool = True) -> list[LearnedRegion]:
    now = time.monotonic() if now is None else float(now)
    traces = (session.stable_traces(now) if stable_only
              else session.bounded_traces())
    if not traces:
        return []
    groups = [[trace] for trace in traces]   # 1 con quai = 1 bai (xem compute_centers)
    hazards = [point for trace in traces for point in trace.unique_points]
    regions = []
    for group in groups:
        center = _center_candidate(group, ground, session.map_id, start)
        if center is None:
            continue
        safe = None
        if ground is not None and hasattr(ground, "nearest_walkable_outside"):
            safe = ground.nearest_walkable_outside(
                session.map_id, center.point, hazards,
                clearance=200, max_path=600,
            )
        regions.append(LearnedRegion(center, safe))   # safe=None = chua tim duoc, va o duoi
    # LUOT 2 - bai KHONG tim duoc safe rieng thi muon safe cua bai HANG XOM gan nhat.
    # Truoc day roi thang ve `fallback_safe` = MOT diem duy nhat cho ca map (cho bot dung quan
    # sat, thuong o goc map) -> moi bai hong deu nhan cung 1 diem o tan dau map. Do la ly do
    # 35/111 map co cap safe-bai cach hang NGHIN (20801: 2744 va 2545; 14801: 5 bai chung 1 safe,
    # xa toi 2292) -> train o do la cu moi lan nghi lai chay ca nua map.
    # Vi sao bai o giua map hay hong: safe phai cach MOI vet tuan tra cua MOI con quai >=200
    # (clearance) va duong di tu tam bai <=600 (max_path); bai bi vay tu phia thi khong con cho
    # nao thoa. Cang nhieu bai (sau khi bo gom bai o 324228a) thi cang de bi bop.
    hoc_duoc = [r.safe for r in regions if r.safe is not None]
    if hoc_duoc:
        regions = [r if r.safe is not None else LearnedRegion(
            r.center, min(hoc_duoc, key=lambda p: math.dist(r.center.point, p)))
            for r in regions]
    regions = [r if r.safe is not None else LearnedRegion(r.center, fallback_safe)
               for r in regions]
    return sorted(regions, key=lambda region: (
        -region.center.monster_count, -region.center.confidence,
        region.center.point[1], region.center.point[0],
    ))


def _merge_center_points(points, distance: float) -> list[Point]:
    merged: list[Point] = []
    for raw in points:
        point = tuple(map(int, getattr(raw, "point", raw)))
        match = next((i for i, old in enumerate(merged)
                      if math.dist(old, point) <= distance), None)
        if match is None:
            merged.append(point)
        else:
            old = merged[match]
            merged[match] = (round((old[0] + point[0]) / 2),
                             round((old[1] + point[1]) / 2))
    return merged


def scan_full_map(client, map_id: int, seed_points=(), stop=None,
                  clock=time.monotonic, sleep=time.sleep) -> ScanResult:
    """Walk all reachable observation stations and cache only learned centers."""
    from . import config, mob_spots

    stop = stop or (lambda: False)
    ground = client.get_ground_store() if hasattr(client, "get_ground_store") else None
    if ground is None or client.pos is None:
        return ScanResult("unavailable", (), 0, 0)
    fingerprint = ground.map_fingerprint(map_id)
    if not fingerprint:
        return ScanResult("unavailable", (), 0, 0)

    cached = mob_spots.load_complete_centers(map_id, fingerprint)
    if cached is not None:
        centers = tuple(CenterCandidate(point, 0, 1.0) for point in cached)
        status = "cached"
        return ScanResult(status, centers, 0, 0)

    stride = tuple(getattr(config, "MOB_SCAN_STATION_STRIDE", (320, 240)))
    quiet = float(getattr(config, "MOB_SCAN_QUIET_SECONDS", 8.0))
    timeout = float(getattr(config, "MOB_SCAN_STATION_TIMEOUT", 90.0))
    min_samples = int(getattr(config, "MOB_SCAN_MIN_SAMPLES", 3))
    max_diameter = float(getattr(config, "MOB_SCAN_MAX_PATROL_DIAMETER", 800))
    merge_distance = float(getattr(config, "MOB_SCAN_MERGE_DISTANCE", 200))
    second_pass = bool(getattr(config, "MOB_SCAN_SECOND_PASS", True))
    settings = {
        "stride": list(map(int, stride)),
        "quiet_seconds": quiet,
        "station_timeout": timeout,
        "min_samples": min_samples,
        "max_patrol_diameter": max_diameter,
        "merge_distance": merge_distance,
        "second_pass": second_pass,
    }
    stations = ground.coverage_stations(map_id, client.pos, stride)
    if not stations:
        return ScanResult("unavailable", (), 0, 0)

    progress = mob_spots.load_progress(map_id, fingerprint)
    coverage = progress.get("coverage", {})
    completed = {int(i) for i in coverage.get("completed", [])
                 if 0 <= int(i) < len(stations)}
    provisional = [tuple(map(int, p)) for p in progress.get("centers", [])]
    session = MobScanSession(
        map_id, getattr(client, "self_entity", None),
        quiet_seconds=quiet, min_samples=min_samples,
        max_patrol_diameter=max_diameter, merge_distance=merge_distance,
    )
    low_confidence = []
    aborted = False

    def should_stop():
        return stop() or not getattr(client, "running", False) \
            or getattr(client, "current_map", map_id) != map_id

    client.begin_mob_observation(session)
    try:
        passes = [list(i for i in range(len(stations)) if i not in completed)]
        if second_pass:
            passes.append(None)
        for pass_index, indices in enumerate(passes):
            if indices is None:
                indices = list(low_confidence)
                low_confidence = []
            for index in indices:
                if should_stop():
                    aborted = True
                    break
                session.begin_station(clock())
                client.navigate_to(*stations[index], flee=True, abort=should_stop)
                if should_stop():
                    aborted = True
                    break
                deadline = clock() + timeout
                stable = session.station_stable(clock())
                while not stable and clock() < deadline and not should_stop():
                    sleep(min(0.1, max(0.0, deadline - clock())))
                    stable = session.station_stable(clock())
                if should_stop():
                    aborted = True
                    break
                if stable:
                    completed.add(index)
                else:
                    low_confidence.append(index)
                learned = compute_centers(session, ground, client.pos, now=clock())
                provisional = _merge_center_points(
                    provisional + [c.point for c in learned], merge_distance
                )
                current_coverage = {
                    "visited": len(completed),
                    "total": len(stations),
                    "pass": pass_index + 1,
                    "low_confidence": len(low_confidence),
                }
                mob_spots.save_progress(map_id, fingerprint, completed,
                                        provisional, current_coverage, settings)
            if aborted:
                break
        remaining = set(range(len(stations))) - completed
        if low_confidence:
            remaining.update(low_confidence)
        status = "incomplete" if aborted or remaining else ("complete" if provisional else "empty")
        final_coverage = {
            "visited": len(completed),
            "total": len(stations),
            "completed": sorted(completed),
            "low_confidence": len(remaining),
        }
        if status in ("complete", "empty"):
            mob_spots.save_complete(map_id, fingerprint, provisional,
                                    final_coverage, settings)
        else:
            mob_spots.save_progress(map_id, fingerprint, completed, provisional,
                                    final_coverage, settings)
        centers = tuple(CenterCandidate(point, 0, 1.0) for point in provisional)
        return ScanResult(status, centers, len(completed), len(stations))
    finally:
        client.end_mob_observation(session)
