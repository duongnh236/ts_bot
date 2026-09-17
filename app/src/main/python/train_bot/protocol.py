"""Protocol TS Online: XOR ma hoa, dong goi/giai goi packet.

Header: c0 91 [len_lo len_hi] 00 00 [opcode] [payload]
- len = 2 byte LE = tong kich thuoc packet (ke ca header)
- toan bo packet duoc XOR voi 0xAD truoc khi gui qua TCP
"""
import struct
from .config import XOR_KEY

MAGIC = b"\xc0\x91"


def xor(data: bytes) -> bytes:
    """XOR 2 chieu (encode = decode)."""
    return bytes(b ^ XOR_KEY for b in data)


def build_packet(opcode: int, payload: bytes) -> bytes:
    """Tao 1 packet hoan chinh (chua XOR).

    Frame: c0 91 [len LE 2B] 00 00 [opcode] [payload]
    len = 7 (header) + len(payload)
    """
    total = 7 + len(payload)
    return MAGIC + struct.pack("<H", total) + b"\x00\x00" + bytes([opcode]) + payload


def encode(opcode: int, payload: bytes) -> bytes:
    """Tao packet va XOR san sang gui."""
    return xor(build_packet(opcode, payload))


def parse_stream(decoded: bytes):
    """Tach nhieu packet trong 1 buffer da giai XOR.

    Returns: list (opcode, full_packet_bytes), so byte da tieu thu.
    """
    out = []
    i = 0
    n = len(decoded)
    while i + 7 <= n:
        if decoded[i:i + 2] != MAGIC:
            i += 1
            continue
        plen = struct.unpack_from("<H", decoded, i + 2)[0]
        if plen < 7 or i + plen > n:
            break
        pkt = decoded[i:i + plen]
        out.append((pkt[6], pkt))
        i += plen
    return out, i


# ---- Opcodes ----
OP_LOGIN = 0x01          # C2S auth / S2C "your turn"
OP_HEARTBEAT = 0x0A
OP_FULLSTAT = 0x0B
OP_MOB_INFO = 0x0C
OP_PLAYER_STATE = 0x0D   # party commands
OP_TELEPORT = 0x44
OP_INVITE = 0x52
OP_COMBAT = 0x32
OP_STAT_UPD = 0x33
OP_BATTLE_START = 0x34   # party battle start
OP_ACTIONS = 0x35        # available actions / confirmation
OP_BATTLE_ENTER = 0x41
