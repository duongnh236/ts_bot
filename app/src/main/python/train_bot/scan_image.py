"""Ve ANH KET QUA SCAN MAP (bai quai + safe) de nguoi kiem tra bang mat.

Vi sao co file nay: doc so lieu kho thay sai; ve ra anh thi phat hien ngay (vd truoc day
gom bai theo khoang cach lam 16 bai quai chi con 7 - nhin anh la lo ra lien).

KHONG dung Pillow (bot khong co, them vao phinh exe) -> tu ghi PNG bang zlib cua stdlib.
CHI ghi tren PC. Android bo qua (file nam trong filesDir noi bo, user khong xem duoc).
"""
from __future__ import annotations

import colorsys
import os
import struct
import sys
import time
import zlib

from ._appdir import app_dir


def is_android() -> bool:
    """Chaquopy (Android) co module 'java'. Ngoai ra ANDROID_ROOT luon co tren Android."""
    if "ANDROID_ROOT" in os.environ or "ANDROID_DATA" in os.environ:
        return True
    return "java" in sys.modules


class _Canvas:
    """Framebuffer RGB don gian + vai primitive (khong can thu vien ngoai)."""

    def __init__(self, width: int, height: int, bg=(10, 10, 10)):
        self.w = int(width)
        self.h = int(height)
        self.buf = bytearray(bytes(bg) * (self.w * self.h))

    def px(self, x: int, y: int, color) -> None:
        if 0 <= x < self.w and 0 <= y < self.h:
            i = (y * self.w + x) * 3
            self.buf[i:i + 3] = bytes(color)

    def fill_rect(self, x0, y0, x1, y1, color) -> None:
        c = bytes(color)
        x0, x1 = max(0, min(x0, x1)), min(self.w - 1, max(x0, x1))
        y0, y1 = max(0, min(y0, y1)), min(self.h - 1, max(y0, y1))
        if x1 < x0 or y1 < y0:
            return
        row = c * (x1 - x0 + 1)
        for y in range(y0, y1 + 1):
            i = (y * self.w + x0) * 3
            self.buf[i:i + len(row)] = row

    def rect(self, x0, y0, x1, y1, color, width=1) -> None:
        for k in range(int(width)):
            self.fill_rect(x0 + k, y0 + k, x1 - k, y0 + k, color)
            self.fill_rect(x0 + k, y1 - k, x1 - k, y1 - k, color)
            self.fill_rect(x0 + k, y0 + k, x0 + k, y1 - k, color)
            self.fill_rect(x1 - k, y0 + k, x1 - k, y1 - k, color)

    def to_png(self) -> bytes:
        raw = bytearray()
        stride = self.w * 3
        for y in range(self.h):
            raw.append(0)                       # filter byte = None
            raw += self.buf[y * stride:(y + 1) * stride]

        def chunk(tag: bytes, data: bytes) -> bytes:
            return (struct.pack(">I", len(data)) + tag + data
                    + struct.pack(">I", zlib.crc32(tag + data) & 0xFFFFFFFF))

        ihdr = struct.pack(">IIBBBBB", self.w, self.h, 8, 2, 0, 0, 0)
        return (b"\x89PNG\r\n\x1a\n" + chunk(b"IHDR", ihdr)
                + chunk(b"IDAT", zlib.compress(bytes(raw), 6))
                + chunk(b"IEND", b""))


# Font so 3x5 pixel tu ve (khong dung thu vien ngoai). Moi so = 5 hang, moi hang 3 bit.
_DIGITS = {
    "0": ("111", "101", "101", "101", "111"),
    "1": ("010", "110", "010", "010", "111"),
    "2": ("111", "001", "111", "100", "111"),
    "3": ("111", "001", "111", "001", "111"),
    "4": ("101", "101", "111", "001", "001"),
    "5": ("111", "100", "111", "001", "111"),
    "6": ("111", "100", "111", "101", "111"),
    "7": ("111", "001", "001", "001", "001"),
    "8": ("111", "101", "111", "101", "111"),
    "9": ("111", "101", "111", "001", "111"),
}


def _draw_number(cv: "_Canvas", cx: int, cy: int, value: int, dot: int, color) -> None:
    """Ve `value` (canh giua tai cx,cy). dot = kich thuoc 1 pixel font (phong to)."""
    text = str(int(value))
    gap = dot                                   # khoang cach giua 2 chu so
    total_w = len(text) * 3 * dot + (len(text) - 1) * gap
    x0 = cx - total_w // 2
    y0 = cy - (5 * dot) // 2
    for ch in text:
        rows = _DIGITS.get(ch)
        if rows:
            for ry, row in enumerate(rows):
                for rx, bit in enumerate(row):
                    if bit == "1":
                        cv.fill_rect(x0 + rx * dot, y0 + ry * dot,
                                     x0 + rx * dot + dot - 1, y0 + ry * dot + dot - 1, color)
        x0 += 3 * dot + gap


def _out_path(map_id: int) -> str:
    folder = os.path.join(app_dir(), "scan_maps")
    os.makedirs(folder, exist_ok=True)
    stamp = time.strftime("%Y%m%d_%H%M%S")
    return os.path.join(folder, f"mob_scan_{int(map_id)}_{stamp}.png")


def render_scan(ground, map_id: int, traces, centers, safes,
                stations=(), scale: int = 4) -> str | None:
    """Ve: dia hinh + diem quai tung con (mau rieng) + bbox + tam bai + safe + tram scan.
    Tra duong dan file, hoac None neu bo qua/loi (KHONG bao gio nem loi ra ngoai)."""
    if is_android():
        return None
    try:
        m = ground.get(int(map_id)) if ground is not None else None
        if not m:
            return None
        gw, gh = int(m["grid_w"]), int(m["grid_h"])
        grid = m["grid"]
        sc = max(2, int(scale))
        cv = _Canvas(gw * sc, gh * sc)
        # dia hinh: dat / tuong / nuoc
        pal = {0: (32, 32, 32), 1: (190, 190, 190), 2: (40, 90, 200), 4: (190, 190, 190)}
        for by in range(gh):
            for bx in range(gw):
                col = pal.get(grid[bx * gh + by] & 7, (180, 40, 40))
                cv.fill_rect(bx * sc, by * sc, bx * sc + sc - 1, by * sc + sc - 1, col)

        def to_px(point):
            b = ground.world_to_block(int(map_id), (int(point[0]), int(point[1])))
            if not b:
                return None
            return ((b[0] - 1) * sc + sc // 2, (b[1] - 1) * sc + sc // 2)

        # duong chay tung con quai + khung bbox
        for i, tr in enumerate(traces or ()):
            pts = list(getattr(tr, "unique_points", ()) or ())
            if not pts:
                continue
            hue = (i * 0.37) % 1.0
            col = tuple(int(255 * c) for c in colorsys.hsv_to_rgb(hue, 0.85, 0.95))
            for p in pts:
                q = to_px(p)
                if q:
                    cv.fill_rect(q[0] - 1, q[1] - 1, q[0] + 1, q[1] + 1, col)
            xs = [p[0] for p in pts]
            ys = [p[1] for p in pts]
            a, b2 = to_px((min(xs), min(ys))), to_px((max(xs), max(ys)))
            if a and b2:
                cv.rect(a[0], a[1], b2[0], b2[1], (255, 255, 255), 1)
        # tram scan (diem dung quan sat) - xanh nhat
        for s in stations or ():
            q = to_px(s)
            if q:
                cv.rect(q[0] - 2 * sc, q[1] - 2 * sc, q[0] + 2 * sc, q[1] + 2 * sc,
                        (120, 200, 255), 1)
        # SAFE = o vuong xanh la
        for s in safes or ():
            if not s:
                continue
            q = to_px(s)
            if q:
                cv.rect(q[0] - 3 * sc, q[1] - 3 * sc, q[0] + 3 * sc, q[1] + 3 * sc,
                        (0, 255, 0), 2)
        # TAM BAI QUAI = o vuong trang dac + SO THU TU bai (dung so nay chon o dropdown "Quai"
        # trong config, ung voi mob_index). Safe khong danh so.
        dot = max(2, sc // 2)                    # kich thuoc 1 pixel font
        half = max(3 * sc, 4 * dot)              # o phai du rong cho 2 chu so
        for i, c in enumerate(centers or (), start=1):
            q = to_px(c)
            if not q:
                continue
            cv.fill_rect(q[0] - half, q[1] - half, q[0] + half, q[1] + half, (255, 255, 255))
            _draw_number(cv, q[0], q[1], i, dot, (0, 0, 0))
        path = _out_path(map_id)
        with open(path, "wb") as fh:
            fh.write(cv.to_png())
        return path
    except Exception:
        return None
