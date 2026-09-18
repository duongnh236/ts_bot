"""Bounded S2C trace for Android debugging; never records outgoing credentials."""
import json
import logging
import os
import threading
import time
from logging.handlers import RotatingFileHandler

_lock = threading.RLock()
_handler = None


def _path():
    from ._appdir import app_dir
    return os.path.join(app_dir(), "server_packets.jsonl")


def record(username, opcode, packet):
    global _handler
    try:
        # Login/server transfer may contain access credentials. Preserve metadata only.
        redacted = int(opcode) == 0x01
        row = {"timestamp": time.time(), "direction": "S2C", "account": str(username),
               "opcode": "0x%02X" % opcode, "length": len(packet),
               "redacted": redacted, "payload_hex": None if redacted else packet[7:].hex(),
               "frame_hex": None if redacted else packet.hex(),
               "format": "decrypted protocol frame; unknown fields not guessed"}
        with _lock:
            if _handler is None:
                _handler = RotatingFileHandler(_path(), maxBytes=4 * 1024 * 1024,
                                              backupCount=2, encoding="utf-8")
                _handler.setFormatter(logging.Formatter("%(message)s"))
            _handler.emit(logging.LogRecord("packet_trace", logging.INFO, "", 0,
                                           json.dumps(row, ensure_ascii=False), (), None))
    except Exception:
        # Debug trace must never interrupt game packet handling.
        pass


def snapshot(limit=100):
    limit = max(1, min(200, int(limit)))
    with _lock:
        if _handler is not None:
            _handler.flush()
        path = _path()
        if not os.path.exists(path):
            return {"packets": [], "note": "Chua nhan packet trong ban capture nay"}
        with open(path, "rb") as stream:
            stream.seek(0, 2)
            start = max(0, stream.tell() - 512 * 1024)
            stream.seek(start)
            raw = stream.read()
        lines = raw.decode("utf-8", errors="replace").splitlines()
        if start:
            lines = lines[1:]
        rows = []
        for line in lines[-limit:]:
            try:
                rows.append(json.loads(line))
            except ValueError:
                pass
        return {"packets": rows, "note": "Preview toi da 200 packet. File JSONL xoay vong 12MB; opcode 0x01 an thong tin xac thuc."}
