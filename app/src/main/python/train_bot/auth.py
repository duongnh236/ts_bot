"""Tao goi auth TCP (opcode 0x01).

Cau truc (xac nhan tu 2 lan capture):
  prefix(20B) + UTF16LE(user_id) + UTF16LE('f') + UTF16LE(access_token)

prefix = c0 91 [len] 00 00 01 | 00 00 02 01 01 00 00 00 00 00 | 19 14 00
  - byte[18]=0x14=20 = len(user_id)*2  (user_id 10 chu so)
  - byte[17]=0x19=25 = hang so (token luon dinh dang 51 ky tu)
len duoc tinh lai theo do dai chuoi thuc te.
"""
import struct
from .protocol import xor, OP_LOGIN

# 13 byte payload-prefix sau opcode (truoc chuoi credential)
# 00 00 | 02 01 [SERVER_ID] 00 00 00 00 00 | 19 14 00
# byte thu 5 (index 4) = SERVER ID: Trieu Van=1 (.98), Tao Thao=2 (.99). Sai -> KHONG vao world.


def build_auth_packet(user_id: str, access_token: str, server_id: int = 1) -> bytes:
    """Tra ve packet auth da XOR, san sang gui. server_id theo server (xem servers.json)."""
    prefix = bytes([0x00, 0x00, 0x02, 0x01, server_id & 0xFF,
                    0x00, 0x00, 0x00, 0x00, 0x00, 0x19, 0x14, 0x00])
    cred = (user_id + "f" + access_token).encode("utf-16-le")
    payload = prefix + cred
    total = 7 + len(payload)
    frame = b"\xc0\x91" + struct.pack("<H", total) + b"\x00\x00" + bytes([OP_LOGIN]) + payload
    return xor(frame)


# ===== VO GIOI (無界) - server RIENG cho event lien server =====
# Cau truc lay THANG tu client, KHONG doan (Logic/Network.lua:428 `Network.OnConnect`):
#   C:001-000 <登入> +版本編號(2) +伺服器ID(2) +連線碼(4) +登入方式(1)
#   登入方式 255 (ELogin.Unbounded): +L(1)+帳號(L) +L(1)+密碼(L) + RoleID(8) + SN(4)
#
# KHAC goi auth thuong o 3 diem, sai mot cai la server tu choi:
#   1. loginKind = 255 (thuong la 25 = ELogin.VNSDK)
#   2. do dai chuoi la MOT BYTE (`WriteStringWithByteL`), con nhanh VNSDK dung HAI byte
#      (`WriteStringWithWordL`) - do la ly do goi thuong co `14 00` con goi nay chi co `1c`
#   3. co them RoleID (entity cua char) + SN, hai thu goi thuong khong co
#
# serverId va SN lay tu chinh goi server bao chuyen (S:001-020, xem client._on_bao_chuyen_vo_gioi).
LOGIN_VO_GIOI = 0xFF          # ELogin.Unbounded (Logic/Network.lua:19)
VERSION = 0x0102              # 2 byte `02 01` trong goi auth thuong


def build_unbounded_auth_packet(user_id, access_token, role_id, server_id,
                                sn, connect_code=0, version=VERSION):
    """Goi auth toi SERVER VO GIOI. `role_id` = entity 8 byte cua char (Role.playerId)."""
    acc = str(user_id).encode("utf-16-le")
    pwd = str(access_token).encode("utf-16-le")
    if len(acc) > 255 or len(pwd) > 255:
        raise ValueError("chuoi qua dai cho do dai 1 byte (acc=%d pwd=%d)" % (len(acc), len(pwd)))
    if len(role_id) != 8:
        raise ValueError("role_id phai 8 byte, nhan %d" % len(role_id))
    payload = (b"\x00\x00"                                   # sub 000
               + struct.pack("<H", version)
               + struct.pack("<H", int(server_id) & 0xFFFF)
               + struct.pack("<I", int(connect_code) & 0xFFFFFFFF)
               + bytes([LOGIN_VO_GIOI])
               + bytes([len(acc)]) + acc
               + bytes([len(pwd)]) + pwd
               + bytes(role_id)
               + struct.pack("<I", int(sn) & 0xFFFFFFFF))
    total = 7 + len(payload)
    frame = b"\xc0\x91" + struct.pack("<H", total) + b"\x00\x00" + bytes([OP_LOGIN]) + payload
    return xor(frame)


def parse_ket_qua_login(body):
    """S:001-002 <登入結果> -> (account, password) DE DUNG CHO SERVER VO GIOI, None neu khong doc duoc.

    Day la manh con thieu khien lan chay 05/09 21:57 that bai: bot gui `user_id` (chuoi so) va
    `access_token` (51 ky tu) cua RIENG no, trong khi server vo gioi doi CAP MA CHINH SERVER GOC
    vua phat lai luc login:
        acc = "1623021930@vtc"   (user_id + @vtc)
        pwd = "30QH8R3A49"       (ve 10 ky tu, KHAC hoan toan access_token)
    Do tren `captures/loandau_t7_login_20260905.pcap`:
        S2C 0x01 0200 | 00 00 01 00 08 00 | roleId(8) | serverTime(8) | 00
                      | L(1)=1c + acc(28) | L(1)=14 + pwd(20) | 00
    """
    if len(body) < 26 or body[0] != 0x02 or body[1] != 0x00:
        return None
    i = 24                       # byte do dai cua chuoi ACCOUNT
    n = body[i]
    if not n or i + 1 + n + 1 > len(body):
        return None
    j = i + 1 + n
    m = body[j]
    if not m or j + 1 + m > len(body):
        return None
    try:
        return (body[i + 1:j].decode("utf-16-le"),
                body[j + 1:j + 1 + m].decode("utf-16-le"))
    except UnicodeDecodeError:
        return None


def parse_connect_code(body):
    """S:001-013 <登入送完所有資訊> +連線序號(4). Goi auth vo gioi mang lai so nay.

    Login MOI thi connectCode = 0 (capture: `00000000`), nhung khi chuyen sang may vo gioi
    client gui so cua PHIEN DANG CHAY (capture t7: b9190000 = 6585). Khong luu lai thi bot chi
    biet gui 0 -> nghi day la mot trong hai ly do server vo gioi tu choi.
    """
    if len(body) < 6 or body[0] != 0x0D or body[1] != 0x00:
        return None
    return struct.unpack_from("<I", body, 2)[0]


def parse_bao_chuyen_vo_gioi(body):
    """S:001-020 <通知連無界伺服器> ServerId(2)+L(1)+IP(L)+port(2)+SN(4).

    `body` = than goi SAU opcode (tinh ca 2 byte sub). Tra dict hoac None neu khong phai.
    """
    if len(body) < 10 or body[0] != 0x14 or body[1] != 0x00:
        return None
    server_id = struct.unpack_from("<H", body, 2)[0]
    n = body[4]
    if 5 + n + 6 > len(body):
        return None
    try:
        host = body[5:5 + n].decode("utf-16-le")
    except UnicodeDecodeError:
        return None
    port, sn = struct.unpack_from("<HI", body, 5 + n)
    return {"server_id": server_id, "host": host, "port": port, "sn": sn}
