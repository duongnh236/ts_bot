"""Loan dau loi dai (亂鬥擂台) - vong dang ky + danh, moi acc chay DOC LAP (khong party).

Nguon: `captures/loandau_20260825.pcap` + crack client. Xem `documents/LOAN_DAU.md`.

Cung map / cung NPC voi event 40 NPC (10991, diem (910,290)); khac dung 2 cho:
    chon event  `0x4d 03000300`   (40NPC la `03000400`)
    option NPC  `0x14 0100 03 00` (40NPC la `...0500`) - byte 03 = triggerID 3 = 亂鬥

KHAC 40NPC o cho quan trong: day la PvP, doi thu la NGUOI CHOI, va giua 2 tran KHONG phai
di lai (capture: lenh di chuyen cuoi cung o t=12.8s, ca 2 tran deu dang ky tu dung cho do).
"""
from __future__ import annotations

import datetime
import logging
import time


log = logging.getLogger("bot")

OP_DIALOG = 0x14
OP_EVENT = 0x20
OP_BATTLE = 0x0b
OP_VS_WIN = 0x25            # S:037-001 <團p勝場數> +玩家ID(8) +勝場數(2)

OPEN_EVENT = b"\x02\x00\x08"
# Option NPC MAC DINH = thu 3 (triggerID 3 = 亂鬥). THU 7 dung option KHAC (01) - xem `lich`
# trong events.json va `bien_the_hom_nay`. Doi tham so thay vi hang so cung: capture t7
# (05/09/2026) cho thay moi thu 7 mot bo tham so rieng.
OPEN_NPC = b"\x01\x00\x03\x00"
ADVANCE = b"\x06\x00"
CHOOSE_YES = b"\x09\x00\x1e"
CHOOSE_NO = b"\x09\x00\x1f"

# S:020-008 <事件結束> + kind(1). Hai kind gap trong capture:
END_REGISTERED = 0x2a       # phien dialog dong ngay sau khi chon -> DA DANG KY, bat dau cho
END_BATTLE = 0x26           # tran / phien thi dau ket thuc
BATTLE_CREATE = b"\xfa\x00"  # 0x0b sub 250 = <現行戰鬥現況> = TAO TRAN

# Cho ghep tran RAT lech nhau: capture do 336.6s (tran 1) va 0.6s (tran 2) - khong phai hang cho
# theo tung nguoi ma la cho toi luot mo man cua sanh. Nen moc dung that su la 22:00 (het gio
# event), con so duoi day chi la chot chan de khong treo vinh vien neu server im.
WAIT_BATTLE_SEC = 900.0
# So lan `0x14 0600` toi da sau khi chon. Capture can 3 lan; poll SAT sau moi lan va dung NGAY
# khi nhan `08 2a` - advance THUA lam lech state va bi server kick (bai hoc 40NPC 2026-07-29).
MAX_ADVANCE = 8


# Lich MAC DINH khi events.json khong khai `lich` (giu hanh vi cu: chi thu 3).
# LICH DU PHONG khi `events.json` khong doc duoc (asset APK loi, file hong...). PHAI DU CA BA
# NGAY, khong chi thu 3: fallback thieu ngay thi mat han loan dau ngay do ma KHONG BAO GI CA -
# dung bai hoc "Servers.kt FALLBACK thieu server" trong CLAUDE.md (PC 17 server, APK 16, khong ai
# biet cho toi khi user hoi).
#
# `thu` = `datetime.weekday()`: 0=T2, 1=T3, 2=T4, 3=T5, 4=T6, 5=T7, 6=CN.
# Nhin so de tuong thieu ngay - `thu: 3` LA THU NAM, `thu: 5` LA THU BAY.
#
# Tham so rieng tung ngay (select / dest_map / npc_option) van lay tu `events.json`; o day chi
# giu THU + KHUNG GIO de con biet "hom nay co loan dau khong".
LICH_MAC_DINH = [
    {"thu": 1, "tu": "20:00", "den": "22:00"},   # THU BA  - loi dai ti vo (map 10991)
    {"thu": 3, "tu": "20:00", "den": "22:00"},   # THU NAM - 團P, cung map/NPC voi thu 3
    {"thu": 5, "tu": "20:30", "den": "22:30"},   # THU BAY - loi dai dau tran (map 54901)
]


def _gio_phut(s, mac_dinh):
    """"20:30" -> (20, 30). Sai dinh dang -> tra `mac_dinh` (khong lam do ca vong chay)."""
    try:
        g, p = str(s).split(":")
        return int(g), int(p)
    except Exception:
        return mac_dinh


def _buoi_hom_nay(ev=None, now=None):
    """Muc `lich` khop HOM NAY (theo THU), None neu hom nay khong co loan dau.

    KHONG loc theo gio - chi loc theo THU. Cho `in_event_window` lo phan gio.
    """
    now = now or datetime.datetime.now()
    lich = ((ev or {}).get("lich") or LICH_MAC_DINH)
    for buoi in lich:
        try:
            if int(buoi.get("thu")) == now.weekday():
                return buoi
        except (TypeError, ValueError):
            continue
    return None


def in_event_window(now=None, ev=None):
    """Loan dau dang mo khong. Lich doc tu `ev["lich"]` (events.json).

    THU 3 va THU 7 KHAC KHUNG GIO (t3 20:00-22:00, t7 20:30-22:30) nen phai so ca PHUT -
    `20 <= now.hour < 22` nhu ban cu khong bieu dien duoc 20:30.
    """
    now = now or datetime.datetime.now()
    buoi = _buoi_hom_nay(ev, now)
    if not buoi:
        return False
    dau = _gio_phut(buoi.get("tu"), (20, 0))
    cuoi = _gio_phut(buoi.get("den"), (22, 0))
    return dau <= (now.hour, now.minute) < cuoi


def bien_the_hom_nay(ev, now=None):
    """Ban SAO cua `ev` da AP tham so cua buoi HOM NAY (select / dest_map / party_battle...).

    VI SAO CAN: cung mot "Loan dau" nhung thu 3 va thu 7 la HAI SANH KHAC NHAU. Do tren
    `captures/loandau_t7_20260905.pcap` (ghep luong TCP theo seq, 0 byte sot):

        |                | THU 3            | THU 7                |
        | chon event 0x4d| 03000300 (id 3)  | 03005a00 (id 90)     |
        | map            | 10991            | 54901                |
        | diem NPC       | (910, 290)       | (1630, 430)          |
        | option NPC     | 0x14 0100 03 00  | 0x14 0100 01 00      |

    Map 54901 nam trong dai 54901-54905 "Loi dai dau tran" - KHAC dai 10991 "Loi dai ti vo"
    cua thu 3 (xem documents/LOAN_DAU.md muc 1). Luong dang ky thi GIONG HET ve hinh.

    Muc `lich` chi can khai thu nao KHAC mac dinh; khong khai thi dung gia tri goc cua ev
    (thu 3 vi vay khong phai lap lai gi).
    """
    if not ev:
        return ev
    buoi = _buoi_hom_nay(ev, now)
    if not buoi:
        return dict(ev)
    ra = dict(ev)
    for k, v in buoi.items():
        if k in ("thu", "tu", "den"):
            continue
        if k == "party_battle":
            pb = dict(ra.get("party_battle") or {})
            pb.update(v or {})
            ra["party_battle"] = pb
        else:
            ra[k] = v
    return ra


def npc_option(ev=None):
    """Goi `0x14` mo muc loan dau o NPC. Doc `party_battle.npc_option` cua bien the hom nay."""
    pb = ((ev or {}).get("party_battle") or {})
    hex_str = pb.get("npc_option")
    if hex_str:
        try:
            return bytes.fromhex(str(hex_str))
        except ValueError:
            log.warning("Loan dau: npc_option '%s' khong phai hex -> dung mac dinh", hex_str)
    return OPEN_NPC


# ---------- nhan dang goi (ham thuan, de test) ----------

def _body(pkt):
    """Than goi: header 7 byte `c0 91 [len 2B] 00 00 [opcode]`."""
    return pkt[7:]


def is_event_end(opcode, pkt, kind):
    b = _body(pkt)
    return opcode == OP_DIALOG and len(b) >= 3 and b[0] == 0x08 and b[1] == 0x00 and b[2] == kind


def is_registered(opcode, pkt):
    """Da dang ky xong, dang cho ghep tran."""
    return is_event_end(opcode, pkt, END_REGISTERED)


def is_battle_over(opcode, pkt):
    return is_event_end(opcode, pkt, END_BATTLE)


def dialog_page(opcode, pkt):
    """Hex page dialog NPC (`0x14 0100...`), None neu khong phai. Page ket bang '0200'/'0300'
    la DA o buoc chon -> chon luon, khong advance (cung quy tac 40NPC)."""
    b = _body(pkt)
    if opcode == OP_DIALOG and len(b) >= 2 and b[0] == 0x01 and b[1] == 0x00:
        return b.hex()
    return None


def is_choice_page(page):
    return bool(page) and (page.endswith("0200") or page.endswith("0300"))


def is_battle_create(opcode, pkt):
    return opcode == OP_BATTLE and _body(pkt)[:2] == BATTLE_CREATE


def o_cua_minh(pkt, self_entity):
    """(hang, cot) cua CHINH MINH trong goi TAO TRAN, None neu khong doc duoc.

    Dau truong xep char (kind=2) o hang 0 va 3, pet (kind=4) o hang 1 va 2 -> hai phe la {0,1}
    va {3,2}, va server xep minh vao phe NAO CUNG DUOC (capture 25/08: (0,1) tran 1, (0,0) tran
    2). Danh nham vao phe minh -> `S:000-000` ly do 42 `修改戰鬥封包` -> DA HAN acc.
    Dung chinh BattleTracker de doc, khong tu do offset lai.
    """
    if not self_entity:
        return None
    from .battle_tracker import BattleTracker
    tracker = BattleTracker(self_entity)
    try:
        tracker.apply(OP_BATTLE, _body(pkt))
    except Exception:
        return None
    for o, unit in tracker.units.items():
        if unit.role_id == self_entity:
            return o
    return None


def parse_vs_win(opcode, pkt, self_entity):
    """So tran THANG cua CHINH MINH tu `0x25` sub01, None neu goi cua nguoi khac.

    Server ban theo LO dung luc mot vong dau ket thuc (capture: 8 nguoi luc 338.4s, 10 nguoi
    luc 477.8s va 791.8s), trong lo co ca id cua minh.
    """
    b = _body(pkt)
    if opcode != OP_VS_WIN or len(b) < 12 or b[0] != 0x01 or b[1] != 0x00:
        return None
    if not self_entity or b[2:10] != self_entity:
        return None
    return int.from_bytes(b[10:12], "little")


# ---------- vong chay ----------

def _active(client, stop_event):
    return client.running and not stop_event.is_set()


def _cho(client, stop_event, sleep_fn, dieu_kien, han_giay, buoc=0.2):
    """Cho `dieu_kien()` thanh True. False neu het han / bi stop / rot.

    Vong nay CHI ngu - heartbeat `0x0a` chay o thread rieng (_heartbeat_loop, 15s/lan) nen cho
    lau bao nhieu cung khong lam rot ket noi. Da do: cho 336s van song.
    """
    het = time.time() + han_giay
    while time.time() < het:
        if not _active(client, stop_event):
            return False
        if dieu_kien():
            return True
        sleep_fn(buoc)
    return dieu_kien()


def dang_ky(client, stop_event, sleep_fn, poll_interval=0.4, max_advance=MAX_ADVANCE, ev=None):
    """Mo NPC -> chon loan dau -> chon CO -> advance toi khi nhan `08 2a`. True = da dang ky.

    `ev` (event dict cua HOM NAY) quyet dinh byte option NPC: thu 3 la 03, thu 7 la 01.
    Chuoi con lai GIONG HET nhau o ca hai ngay (do tren capture t7 05/09):
        0x20 020008 -> 0x14 0100xx00 -> page ...3930 -> 0600 -> page (choice) -> 09001e
        -> 0600 x3 -> 0x14 0d00 -> 0x14 08 2a
    """
    moc = client._loandau_registered_seq
    client._loandau_dialog = ""
    client.send(OP_EVENT, OPEN_EVENT)
    sleep_fn(0.6)
    client.send(OP_DIALOG, npc_option(ev))
    sleep_fn(0.8)
    if not is_choice_page(getattr(client, "_loandau_dialog", "")):
        client.send(OP_DIALOG, ADVANCE)
        sleep_fn(0.8)
    if not _active(client, stop_event):
        return False
    client.send(OP_DIALOG, CHOOSE_YES)
    for _ in range(max_advance):
        # Poll SAT 0.1s sau moi advance, dung NGAY khi da dang ky. Ngu ca poll_interval roi moi
        # check se gui THEM advance thua -> lech state -> server kick (bai hoc 40NPC).
        cho = 0.0
        while cho < max(0.1, poll_interval):
            if not _active(client, stop_event):
                return False
            if client._loandau_registered_seq > moc:
                return True
            sleep_fn(0.1)
            cho += 0.1
        client.send(OP_DIALOG, ADVANCE)
    return client._loandau_registered_seq > moc


def run_loop(client, point, stop_event, before_repeat=None, sleep_fn=time.sleep,
             poll_interval=0.4, wait_battle_sec=WAIT_BATTLE_SEC, window_fn=None,
             mot_tran=False, ev=None):
    """Vong loan dau cua MOT acc. Tra ve khi het gio / bi stop / rot.

    `before_repeat` goi giua 2 tran (hoi HP/SP) - dung o day la AN TOAN vi dialog da dong
    (`08 26`); tuyet doi khong hoi luc dialog dang mo (bai hoc 40NPC: bi kick).

    `mot_tran` (tick "Chi danh 1 tran" trong GUI, mac dinh TAT): danh XONG tran dau tien la dung,
    khong dang ky tran tiep. Ket thuc y het nhanh HET GIO - dat `_loandau_done` de vong chinh
    `run_party_digioi` ra khoi map event roi TAT ACC.
    """
    lbl = getattr(client, "_label", "?")
    # Khung gio doc theo `ev` (lich thu 3 / thu 7 khac nhau). Caller truyen window_fn rieng thi
    # ton trong (test dung duong do).
    if window_fn is None:
        def window_fn():
            return in_event_window(ev=ev)
    # ===== CHOT SINH TU: KHONG DUNG MAP EVENT thi TUYET DOI khong mo NPC event =====
    # Mo NPC su kien khi dang dung o map THUONG -> server da het ma 5 "su kien vi pham"
    # (S:000-000 code 05). Da mat 4 acc party 11 luc 05/09 21:15: `go_to_event` bao
    # "map=23882 (dich 54901) -> CHUA TOI" nhung KHONG AI DOC ket qua do, vong nay van chay,
    # di toi (1630,430) TREN MAP TRAIN roi mo NPC -> ca 4 bi da trong 3 giay.
    # Bo mot buoi event con hon mat acc.
    _dich = int((ev or {}).get("dest_map") or 0)
    if _dich and int(getattr(client, "current_map", 0) or 0) != _dich:
        log.error("[%s] Loan dau: DANG O MAP %s, KHONG phai map event %s -> KHONG mo NPC "
                  "(mo o map thuong = server da ma 5 'su kien vi pham')",
                  lbl, getattr(client, "current_map", None), _dich)
        return False
    if not client.navigate_to(int(point[0]), int(point[1]), flee=False):
        return False
    if not _active(client, stop_event):
        return False
    # Cho scene on truoc khi mo NPC: mo luc scene chua settle -> `0x14 08 01` -> `0x00` KICK
    # (su co that cua 40NPC 2026-07-29).
    client._wait_combat_clear(idle=1.0, cap=20.0)
    sleep_fn(3.0)
    if not _active(client, stop_event):
        return False
    # CHI rearm (`0x41`), KHONG combat_ready(): gui lai chuoi login ngay truoc NPC event lam
    # server loan state -> kick (40NPC).
    client.rearm_ready()

    while _active(client, stop_event) and window_fn():
        moc_tran = client._loandau_create_seq
        if not dang_ky(client, stop_event, sleep_fn, poll_interval, ev=ev):
            log.warning("[%s] Loan dau: dang ky that bai/timeout", lbl)
            return False
        log.info("[%s] Loan dau: da dang ky, cho ghep tran (thang=%d)",
                 lbl, getattr(client, "_loandau_wins", 0))

        # Cho vao tran. Het gio giua chung -> dung luon, khong cho tiep.
        vao = _cho(client, stop_event, sleep_fn,
                   lambda: client._loandau_create_seq > moc_tran or not window_fn(),
                   wait_battle_sec)
        if not window_fn():
            break
        if not vao or client._loandau_create_seq <= moc_tran:
            log.warning("[%s] Loan dau: cho ghep tran qua %.0fs khong vao -> dung", lbl,
                        wait_battle_sec)
            return False

        moc_ket = client._loandau_end_seq
        if not _cho(client, stop_event, sleep_fn,
                    lambda: client._loandau_end_seq > moc_ket, wait_battle_sec):
            log.warning("[%s] Loan dau: tran khong thay ket thuc -> dung", lbl)
            return False
        log.info("[%s] Loan dau: het tran (thang=%d)", lbl, getattr(client, "_loandau_wins", 0))

        if mot_tran:
            log.info("[%s] Loan dau: tick 'chi danh 1 tran' -> XONG, khong dang ky tiep", lbl)
            client._loandau_done = True
            return True

        if before_repeat is not None and _active(client, stop_event) and window_fn():
            try:
                before_repeat()
            except Exception as exc:
                log.warning("[%s] Loan dau: loi hoi phuc giua 2 tran: %s", lbl, exc)

    if not window_fn():
        log.info("[%s] Loan dau: het gio (qua 22h) -> di doi thuong", lbl)
        client._loandau_done = True
        return True
    return False
