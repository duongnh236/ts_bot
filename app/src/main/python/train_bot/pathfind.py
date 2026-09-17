"""Tim duong lien map qua cong dich chuyen (BFS tren do thi co huong MAP_GATES)."""
from collections import deque
import heapq
import math
import re
import struct
import zlib


def find_path(graph, src_map, dst_map):
    """graph: {map_id -> [(x,y,to), ...]}. Tra:
      []   neu da o dst (src == dst)
      list [(gate_x, gate_y, next_map), ...] = chuoi cong NGAN nhat
      None neu khong co duong (hoac src khong co trong graph)."""
    if src_map == dst_map:
        return []
    if src_map not in graph:
        return None
    visited = {src_map}
    q = deque([(src_map, [])])   # (map_hien_tai, duong_di_toi_no)
    while q:
        cur, path = q.popleft()
        for (x, y, to) in graph.get(cur, []):
            if to in visited:
                continue
            np = path + [(x, y, to)]
            if to == dst_map:
                return np
            visited.add(to)
            q.append((to, np))
    return None


def _value(grid, width, height, x, y):
    if x < 1 or x > width or y < 1 or y > height:
        return None
    return grid[(x - 1) * height + (y - 1)]


def _blocked(grid, width, height, x, y):
    """O (x,y) co bi CHAN khong - Y HET client, khong hon khong kem.

    `_lua_dec/Logic/Map/MapManager.lua:231`:
        return bit.band(blocks[x][y], 1) == 1 or bit.band(blocks[x][y], 4) == 4;

    BIT2 = BIEN, VA NO KHONG PHAI CHUONG NGAI. Ca `FindWay.lua` (toan bo ham tim duong cua
    client, 377 dong) chi goi `IsObstacle` - trong do khong co mot chu sea/boat/water nao.
    `MapManager.IsSea` chi duoc goi o 3 cho, khong cho nao dinh toi tim duong:
      - RoleController:3692 / Role.lua:318 -> `SetOnTheSea()` doi hinh nhan vat sang dang thuyen
      - UIAction.lua:92 -> chan lam dong tac tren bien (`--海上不可做動作`)
    Tuc: DUNG len o bien thi TU THANH "dang tren thuyen"; do la HE QUA cua viec dung o do, khong
    phai dieu kien de di toi do. Client di chung mot luoi, bo voi bien nhu nhau.

    Truoc day bot co them che do `boat=True` = "chi di duoc tren nuoc" - HOAN TOAN BIA, va no DAO
    NGUOC luat (dung tren dat thi coi nhu bi chan). Chang 18000->26000 bat dau o cho cap ben TREN
    DAT nen tra None ngay tu o xuat phat -> navigate_to mat smart path -> di mu -> lenh move dau
    tien nhay 2100 don vi -> `di chuyen QUA XA (ma 14)` -> dut ket noi. Ba leader p43/p44/p45 quay
    vong nhu the ca tieng sang 06/09 (58/58 lan ma 14 deu ngay sau `qua cong idx=1 -> map 18000`).
    """
    value = _value(grid, width, height, x, y)
    if value is None:
        return True
    return value & 1 == 1 or value & 4 == 4


def is_line_clear(grid, width, height, start, target):
    """Port MapManager.IsLineWay: kiem tra ca ceil/floor sat hai mep duong."""
    sx, sy = start
    tx, ty = target
    vx = -1 if sx >= tx else 1
    vy = -1 if sy >= ty else 1
    dx, dy = abs(sx - tx), abs(sy - ty)
    if dx == 0 and dy == 0:
        return not _blocked(grid, width, height, sx, sy)
    slope = dy / (dx + 0.01) if dx >= dy else dx / (dy + 0.01)
    if dx >= dy:
        for i in range(1, dx + 1):
            x = sx + i * vx
            for y in (sy + math.ceil(i * slope * vy), sy + math.floor(i * slope * vy)):
                if _blocked(grid, width, height, x, y):
                    return False
    else:
        for i in range(1, dy + 1):
            y = sy + i * vy
            for x in (sx + math.ceil(i * slope * vx), sx + math.floor(i * slope * vx)):
                if _blocked(grid, width, height, x, y):
                    return False
    return True


def _empty_target(grid, width, height, target, max_radius=30):
    x, y = target
    if not _blocked(grid, width, height, x, y):
        return target
    # Thu tu giong MapManager.GetNearEmpty: tren, duoi, trai, phai, bon goc.
    for dx, dy in ((0, -1), (0, 1), (-1, 0), (1, 0),
                   (-1, -1), (-1, 1), (1, -1), (1, 1)):
        if not _blocked(grid, width, height, x + dx, y + dy):
            return x + dx, y + dy
    # Cong o BIEN map (vd ben thuyen 15000 door2 @[2440,20]) co center nam tren tuong/mep,
    # 8 o ke deu chan -> vong ra xa hon (spiral) tim o dung-duoc gan nhat. Khong co -> None.
    for r in range(2, int(max_radius) + 1):
        best = None
        best_d = None
        for dx in range(-r, r + 1):
            for dy in range(-r, r + 1):
                if max(abs(dx), abs(dy)) != r:   # chi quet VIEN cua vong r
                    continue
                nx, ny = x + dx, y + dy
                if _blocked(grid, width, height, nx, ny):
                    continue
                d = dx * dx + dy * dy
                if best_d is None or d < best_d or (d == best_d and (ny, nx) < (best[1], best[0])):
                    best, best_d = (nx, ny), d
        if best is not None:
            return best
    return None


def _smooth(grid, width, height, path):
    if len(path) < 3:
        return path
    result = [path[0]]
    current = 0
    while current < len(path) - 1:
        farthest = current + 1
        for candidate in range(current + 2, len(path)):
            if is_line_clear(grid, width, height, path[current], path[candidate]):
                farthest = candidate
        result.append(path[farthest])
        current = farthest
    return result


def find_local_path(grid, width, height, start, target, smooth=True):
    """A* noi-map tren block 1-based, 4 huong. Mot luoi duy nhat, y het client (xem _blocked)."""
    if _blocked(grid, width, height, *start):
        return None
    target = _empty_target(grid, width, height, target)
    if target is None:
        return None
    if start == target:
        return [start]
    if is_line_clear(grid, width, height, start, target):
        return [start, target]

    frontier = [(math.dist(start, target), 0, start)]
    came_from = {start: None}
    cost_so_far = {start: 0}
    serial = 0
    best = start                       # o REACHABLE gan target nhat (best-effort neu target ket)
    best_d = math.dist(start, target)
    while frontier:
        _, _, current = heapq.heappop(frontier)
        if current == target:
            break
        cd = math.dist(current, target)
        if cd < best_d:
            best_d, best = cd, current
        x, y = current
        for nxt in ((x, y - 1), (x, y + 1), (x - 1, y), (x + 1, y)):
            if _blocked(grid, width, height, *nxt):
                continue
            new_cost = cost_so_far[current] + 1
            if nxt not in cost_so_far or new_cost < cost_so_far[nxt]:
                cost_so_far[nxt] = new_cost
                came_from[nxt] = current
                serial += 1
                heapq.heappush(frontier, (new_cost + math.dist(nxt, target), serial, nxt))
    if target not in came_from:
        # target KET (bi chan / pocket roi / gate center lech vao o chan - grid collision KHONG khop
        # 100% passability that, xem KNOWLEDGE 7d-RE). Di toi o REACHABLE GAN target nhat neu DU GAN
        # (<=4 block ~80px): cong trigger theo khoang cach nen toi sat la duoc. Xa hon -> that su ket.
        if best_d <= 4 and best != start:
            target = best
        else:
            return None

    path = []
    current = target
    while current is not None:
        path.append(current)
        current = came_from[current]
    path.reverse()
    return _smooth(grid, width, height, path) if smooth else path


class GroundMapStore:
    """Doc Ground.mmg mot lan va cap map collision theo scene id."""

    def __init__(self, path):
        with open(path, "rb") as fh:
            self.data = fh.read()
        self.index = {}
        pattern = re.compile(rb"([0-9]+\.map)(.{11})(.{4})(.{4})", re.DOTALL)
        for match in pattern.finditer(self.data):
            name = match.group(1)
            if match.start(1) == 0 or self.data[match.start(1) - 1] != len(name):
                continue
            offset, size = struct.unpack("<II", match.group(3) + match.group(4))
            self.index[int(name[:-4])] = (offset, size)

    def get(self, map_id):
        entry = self.index.get(int(map_id))
        if entry is None:
            return None
        offset, size = entry
        data = self.data
        if offset < 0 or size < 13 or offset + size > len(data):
            return None
        width, height = struct.unpack_from("<II", data, offset)
        chunk_count = data[offset + 8]
        p = offset + 9 + chunk_count * 6
        grid_w, grid_h = struct.unpack_from("<HH", data, p)
        p += 4
        grid_size = grid_w * grid_h
        if p + grid_size > offset + size:
            return None
        return {"width_px": width, "height_px": height, "grid_w": grid_w,
                "grid_h": grid_h, "grid": data[p:p + grid_size]}

    @staticmethod
    def _world_origin(m):
        left = math.floor((800 - m["width_px"]) * 0.5 * 0.05) * 20 \
            if m["width_px"] < 800 else 0
        top = math.floor((600 - m["height_px"]) * 0.5 * 0.05) * 20 \
            if m["height_px"] < 600 else 0
        return left, top

    def world_to_block(self, map_id, point):
        m = self.get(map_id)
        if m is None:
            return None
        left, top = self._world_origin(m)
        return (math.ceil((point[0] - left) * 0.05),
                math.ceil((point[1] - top) * 0.05))

    def is_sea_world(self, map_id, point):
        """Cell (x,y) o world co phai NUOC (bit2=sea)? Dung de _enter_gate biet cong GIUA BIEN
        (diem chuyen map tren o nuoc, dang tren thuyen) -> chi gui 0x14 08, KHONG 0x14 04 (server
        da). Xem capture thuyen_thanhchau: cac cong bien deu chi 0x14 08."""
        m = self.get(map_id)
        if m is None:
            return False
        blk = self.world_to_block(map_id, point)
        if blk is None:
            return False
        v = _value(m["grid"], m["grid_w"], m["grid_h"], blk[0], blk[1])
        return v is not None and v & 2 == 2

    def block_to_world(self, map_id, block):
        m = self.get(map_id)
        if m is None:
            return None
        left, top = self._world_origin(m)
        return block[0] * 20 - 10 + left, block[1] * 20 - 10 + top

    def reachable_blocks(self, map_id, start):
        m = self.get(map_id)
        if m is None:
            return set()
        block = self.world_to_block(map_id, start)
        block = _empty_target(
            m["grid"], m["grid_w"], m["grid_h"], block
        )
        if block is None:
            return set()
        found = {block}
        queue = deque([block])
        while queue:
            x, y = queue.popleft()
            for nxt in ((x, y - 1), (x, y + 1), (x - 1, y), (x + 1, y)):
                if nxt in found or _blocked(
                    m["grid"], m["grid_w"], m["grid_h"], *nxt
                ):
                    continue
                found.add(nxt)
                queue.append(nxt)
        return found

    def coverage_stations(self, map_id, start, stride_world=(320, 240)):
        component = self.reachable_blocks(map_id, start)
        if not component:
            return []
        stride_x = max(1, math.ceil(float(stride_world[0]) / 20.0))
        stride_y = max(1, math.ceil(float(stride_world[1]) / 20.0))
        buckets = {}
        for block in component:
            key = ((block[0] - 1) // stride_x, (block[1] - 1) // stride_y)
            buckets.setdefault(key, []).append(block)

        selected = {}
        for (bucket_x, bucket_y), blocks in buckets.items():
            center_x = bucket_x * stride_x + (stride_x + 1) / 2.0
            center_y = bucket_y * stride_y + (stride_y + 1) / 2.0
            selected[(bucket_x, bucket_y)] = min(
                blocks,
                key=lambda p: ((p[0] - center_x) ** 2 + (p[1] - center_y) ** 2,
                               p[1], p[0]),
            )

        ordered = []
        row_keys = sorted({key[1] for key in selected})
        for row_index, bucket_y in enumerate(row_keys):
            keys = sorted((key for key in selected if key[1] == bucket_y),
                          key=lambda key: key[0], reverse=bool(row_index % 2))
            ordered.extend(self.block_to_world(map_id, selected[key]) for key in keys)
        return ordered

    def nearest_walkable_world(self, map_id, point, reachable_from):
        component = self.reachable_blocks(map_id, reachable_from)
        if not component:
            return None
        target = self.world_to_block(map_id, point)
        nearest = min(component,
                      key=lambda p: ((p[0] - target[0]) ** 2 + (p[1] - target[1]) ** 2,
                                     p[1], p[0]))
        return self.block_to_world(map_id, nearest)

    def nearest_walkable_outside(self, map_id, start, hazards,
                                 clearance=200.0, max_path=600.0):
        m = self.get(map_id)
        if m is None:
            return None
        origin = _empty_target(
            m["grid"], m["grid_w"], m["grid_h"],
            self.world_to_block(map_id, start),
        )
        if origin is None:
            return None
        hazards = [tuple(map(int, point)) for point in hazards]
        limit = max(0, math.ceil(float(max_path) / 20.0))
        queue = deque([(origin, 0)])
        seen = {origin}
        current_depth = -1
        valid = []
        while queue:
            block, depth = queue.popleft()
            if depth != current_depth and valid:
                return min(valid, key=lambda item: (
                    item[0], item[1][1], item[1][0]
                ))[1]
            if depth > limit:
                break
            current_depth = depth
            point = self.block_to_world(map_id, block)
            nearest = min(
                (math.dist(point, hazard) for hazard in hazards),
                default=float("inf"),
            )
            if nearest >= float(clearance):
                valid.append((math.dist(point, start), point))
            x, y = block
            for nxt in ((x, y - 1), (x, y + 1), (x - 1, y), (x + 1, y)):
                if nxt in seen or _blocked(
                    m["grid"], m["grid_w"], m["grid_h"], *nxt
                ):
                    continue
                seen.add(nxt)
                queue.append((nxt, depth + 1))
        return min(valid, key=lambda item: (
            item[0], item[1][1], item[1][0]
        ))[1] if valid else None

    def map_fingerprint(self, map_id):
        entry = self.index.get(int(map_id))
        if entry is None:
            return None
        offset, size = entry
        if offset < 0 or size < 1 or offset + size > len(self.data):
            return None
        return f"{zlib.crc32(self.data[offset:offset + size]) & 0xffffffff:08x}"

    def find_world_path(self, map_id, start, target):
        m = self.get(map_id)
        if m is None:
            return None
        to_block = lambda p: self.world_to_block(map_id, p)
        start_block = to_block(start)
        if _blocked(m["grid"], m["grid_w"], m["grid_h"], *start_block):
            # Start bi grid danh dau 'blocked' -> player DANG dung o do that (grid collision KHONG
            # khop passability that: vd o BIEN map) -> snap ve o di-duoc
            # gan nhat de co diem bat dau. Thieu buoc nay -> find_local_path None -> route fail
            # (bug that: map 12061 pos (470,1210) block (24,61) bi coi blocked -> ket route, reform vo han).
            snapped = _empty_target(m["grid"], m["grid_w"], m["grid_h"], start_block)
            if snapped is not None:
                start_block = snapped
        blocks = find_local_path(m["grid"], m["grid_w"], m["grid_h"],
                                 start_block, to_block(target))
        if blocks is None:
            return None
        result = [self.block_to_world(map_id, block) for block in blocks]
        target_block = to_block(target)
        if blocks and blocks[-1] == target_block and not _blocked(
                m["grid"], m["grid_w"], m["grid_h"], *target_block):
            result[-1] = tuple(target)
        return result
