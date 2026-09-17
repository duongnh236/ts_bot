"""Event kieu LEO THAP nhieu tang (Nhi Kieu / '2K': 12922 -> 12959).

Boc tu capture that `captures/nhikieu_2k_tang1_9_20260809.pcap` (leader 13e8e44c + 4 member,
di lien tuc 12921 -> 12932 roi THUA):

- Tang LIEN TIEP: 12921(cho) -> 12922 -> ... . Map id doc tu S2C 0x0c (0x03 KHONG bat moi lan
  doi tang -> dung 0x03 de theo tang se sot).
- MOI tuong tac tren tang = C2S `0x14 0800 [idx]`, DUNG CHUNG opcode cho ca danh quai lan cong:
  * idx cua CONG len tang: lay tu world_nav.json - da doi chieu capture, KHOP 10/10 tang, ke ca
    tang 12930 dung door 3 (khong phai 2).
  * idx DANH QUAI: cac idx con lai. Quan sat 11/11 tang: nam trong 3..6, tru dung idx cua cong.
    (vd 12930 gui 4,5,6 vi cong la 3; cac tang khac gui 3,4,5 vi cong la 1/2.)
- THUA: KHONG dua vao "bi day ve 12003" - do la truong hop bay hon; thua binh thuong van dung
  yen tai cho. Dung `npc40.party_defeated(state.allies)` - dung ham repo da dung cho 40NPC.
- Moc bat dau/ket thuc tran: theo KNOWLEDGE muc 5 + CLAUDE.md -> bam `state.in_battle`
  (`_wait_combat_clear`), KHONG tu che moc moi.
"""

from __future__ import annotations

import logging
import time

from . import npc40

log = logging.getLogger("bot")

_BATTLE_IDX_RANGE = range(3, 9)   # idx danh quai quan sat duoc (3..6); quet rong hon 1 chut
_DIALOG_CAP = 15                  # so lan bam 0x14 0600 toi da de day thoai NPC vao tran
_DIALOG_OPEN_WAIT = 3.0           # cho THOAI mo sau khi bam idx; khong mo = diem da het quai
_POS_REFRESH_TRIES = 4            # so lan xin lai toa do sau khi qua cong (server hay im)
_DUNGEON_WINDOW = 3600.0          # cua so "dang trong kich ban dungeon" (gia han moi tang)
_MAX_FLOOR_SECONDS = 900.0
_WALK_BUDGET = 120.0              # ngan sach di 1 chang (log that: 45s/30 lenh move cho 1 diem
                                  # -> 45s la sat nut, bi cat giua duong)


def _nav():
    from .client import _smart_world_router

    router = _smart_world_router()
    return None if router is None else router.nav


def _up_gate(scene: int):
    """(next_scene, door, (x,y)) cua cong LEN TANG, suy tu world_nav.json.

    KHONG gia dinh `scene + 1`: thap co lo hong (12940 khong ton tai, 12939 noi thang 12944).
    Tra None neu khong co canh len (12934/12939/12943/12949/12954 chi co cong xuong).
    """
    nav = _nav()
    if nav is None:
        return None
    best = None
    for edge in nav.data.get("edges", []):
        if int(edge["scene"]) != int(scene) or int(edge["target_scene"]) <= int(scene):
            continue
        gate = nav.get_gate(scene, edge["door"])
        if not (gate and gate.get("center")):
            continue
        cand = (int(edge["target_scene"]), int(edge["door"]), tuple(gate["center"]))
        if best is None or cand[0] < best[0]:
            best = cand
    return best


def _active(client, stop_event):
    return client.running and not stop_event.is_set()


def _fight_one(client, idx: int, stop_event, heal_party=None, lost_check=None):
    """Gui `0x14 0800 [idx]` roi cho xem co vao tran khong.

    Tra: "won" (danh xong, con song) | "lost" (party chet het) | None (idx nay khong ra tran).
    """
    # Chuoi THAT (capture): 0x14 0800[idx] -> S2C 0x14 0100 = THOAI MO -> phai bam 0x14 0600
    # nhieu lan -> moi ra S2C 0x34 = BATTLE START. Truoc day chi gui idx roi ngoi cho in_battle
    # -> khong bao gio vao tran, roi gui idx ke TRONG LUC THOAI DANG MO -> SERVER DA (xac nhan
    # log 11:18:16: gui '0x14 08000400' xong la "Server dong ket noi").
    prev_evt = getattr(client, "_last_dialog_evt", 0.0)   # CHOT MOC TRUOC khi gui. Khong so voi
    t_send = time.time()                                  # time.time() luc gui: Windows ~15ms/tick
                                                          # -> 2 lenh lien nhau co the ra CUNG so.
    client.send(0x14, b"\x08\x00" + bytes([idx & 0xFF]) + b"\x00")
    # Diem nay con quai khong? Con -> server MO THOAI (0x14 0100/1000/0d00 -> _last_dialog_evt).
    # Het quai (da danh truoc do, vd login lai giua tang) -> khong co thoai -> bo qua NGAY, thay vi
    # spam 15 lan 0x14 0600 roi cho tiep (~20s cho moi diem da chet - log that ket ca phut o tang 8).
    while _active(client, stop_event) and time.time() - t_send < _DIALOG_OPEN_WAIT:
        if client.state.in_battle or getattr(client, "_last_dialog_evt", 0.0) > prev_evt:
            break
        time.sleep(0.3)
    else:
        return None
    if not client._dialog_until_battle(cap_n=_DIALOG_CAP, gap=0.7):
        # Khong vao tran -> idx nay khong phai diem danh quai. Don thoai lo mo roi bo qua.
        client._adv_dialog_until_idle(min_n=2, gap=0.4, idle=1.2, max_wait=8.0)
        return None
    if not _active(client, stop_event):
        return None
    client._wait_combat_clear(idle=3.0, cap=_MAX_FLOOR_SECONDS)
    # Sau tran co THOAI TONG KET (0x14 0100/1000/0d00) - phai bam het roi moi duoc gui idx ke,
    # neu khong idx ke roi vao luc thoai dang mo -> server DA.
    client._adv_dialog_until_idle(min_n=3, gap=0.5, idle=1.5, max_wait=25.0)
    # allies bi clear() moi 0x34 -> phai doc NGAY sau khi ket tran, truoc tran ke tiep.
    # DOC THUA TRUOC khi hoi mau: hoi xong thi HP len lai -> mat dau hieu party da chet het.
    defeated, alive, total = npc40.party_defeated(client.state.allies)
    # party_defeated chi doc HP -> KHONG bat duoc acc BAY HON (bi day ra khoi thap, HP van binh
    # thuong). lost_check() kiem "co ai vang khoi thap khong" - do moi la dau hieu thua that o 2K.
    if not defeated and lost_check is not None:
        try:
            defeated = bool(lost_check())
        except Exception:
            pass
    log.info("[%s] 2K: xong tran idx=%d, party song %d/%d%s", client._label, idx, alive, total,
             " (co acc vang khoi thap -> THUA)" if defeated and alive else "")
    # HOI FULL HP/SP ca party sau MOI tran - KE CA TRAN THUA: thua thi 2K dung, nhung acc con
    # phai di lam viec tiep theo (train/daily...) nen van can day mau. Doc `defeated` TRUOC khi
    # hoi vi hoi xong HP len lai -> khong con doc duoc dau hieu party chet sach.
    # Bat buoc tu goi o day: quest_mode=True lam _heal_after_battle() thoat som
    # (client.py: "dungeon/boss flow tu quan ly heal").
    if heal_party is not None:
        heal_party()
    else:
        client.heal_full(force=True)
    return "lost" if defeated else "won"


def _battle_idx(ev, scene: int):
    """Cac idx de KICH TRAN o tang nay. Mac dinh 3..8; tang nao khac thi khai o events.json.

    Tang CHOT (12934/12939/12943/12949/12954 - chi co cong xuong trong world_nav, cong len chi
    hien SAU KHI THANG) dung idx khac han. Capture 06/09 tang 11 (12934):
        move -> (650,430)   roi   C2S 0x14 0800 02    <- idx 2, NGOAI khoang 3..8
    Nen bot quet 3..8 la khong bam trung diem nao -> dung im. Khong mo rong khoang mac dinh xuong
    2 duoc: o tang thuong idx 2 THUONG LA CONG (vd 12933 door=2) -> bam vao la qua cong som.
    """
    m = (ev.get("party_battle") or {}).get("battle_idx") or {}
    v = m.get(str(int(scene)))
    return [int(x) for x in v] if v else list(_BATTLE_IDX_RANGE)


def _battle_points(ev, scene: int):
    """[(x,y)] cac diem DANH QUAI cua tang, doc tu events.json (khong hardcode).

    TANG NAO CUNG phai di toi diem roi moi bam idx (xac nhan tu capture: moi idx deu co 4-10
    goi 0x06 di truoc). Thu tu VI TRI co dinh; so idx doi tuy tang -> diem thu k dung cho lan
    danh thu k.
    """
    pts = (ev.get("party_battle") or {}).get("battle_points") or {}
    p = pts.get(str(int(scene))) or pts.get("default") or []
    return [tuple(x) for x in p]


def _floor_label(ev, scene: int):
    """'tang 9 - Thang Thap (12932)'. Ten map lay THEO GAME (config.scene_name, boc tu
    Data/TextData_C.dat). N suy tu floor_base: 12924 = tang 1; 12921/12922/12923 la khu vao
    ("Thap Luyen"/"Thong Dao"/"Dai Dien" - khong danh so tang)."""
    from . import config

    base = int((ev.get("party_battle") or {}).get("floor_base") or 0)
    n = int(scene) - base if base else 0
    where = config.scene_name(scene)
    return ("tang %d - %s" % (n, where)) if n >= 1 else ("khu vao - %s" % where)


def _walk_to(client, point, stop_event):
    """Di toi diem bang TIM DUONG THONG MINH, ngan sach ngan roi thoi (best-effort)."""
    if not point:
        return
    t0 = time.time()
    try:
        client.navigate_to(*point, flee=False,
                           abort=lambda: stop_event.is_set() or time.time() - t0 > _WALK_BUDGET)
    except Exception as e:
        log.debug("[%s] 2K: di toi %s loi (bo qua): %s", client._label, point, e)


def _back_gate_center(scene: int, prev_scene: int):
    """Toa do cong tren `scene` dan NGUOC ve `prev_scene` = cho buoc ra sau khi qua cong."""
    nav = _nav()
    if nav is None:
        return None
    for edge in nav.data.get("edges", []):
        if int(edge["scene"]) != int(scene) or int(edge["target_scene"]) != int(prev_scene):
            continue
        gate = nav.get_gate(scene, edge["door"])
        if gate and gate.get("center"):
            return tuple(gate["center"])
    return None


def _fix_pos_after_gate(client, prev_scene, stop_event):
    """Sau khi qua cong: bao dam client.pos co gia tri THAT, neu khong smart path bi vo hieu.

    _enter_gate dat pos=None ("vi tri cu vo nghia o map moi"). Server KHONG phai luc nao cung ban
    0x03 self-spawn ngay: log 12:25 di LEN 12930 -> "request scene khong co self-spawn va khong co
    pos hop le" -> navigate_to roi ve gui move MU 30 lenh (~45s). (Di XUONG thi co pos ngay nen
    chang do chay smart path binh thuong.)

    -> Kien tri xin lai vai lan; van khong duoc thi lay CONG DOI UNG lam moc: qua cong thi buoc ra
    o cai cong dan NGUOC lai chinh cho vua di. Xac nhan bang log: xuong 12931 -> 12930 ra dung
    (790,190) = cong cua 12930 dan ve 12931. KHONG duoc lay diem quai 1 - chuyen tang xong con
    phai di mot doan moi toi diem quai.
    """
    for _ in range(_POS_REFRESH_TRIES):
        if not _active(client, stop_event) or client.pos is not None:
            return
        try:
            if client.refresh_server_position(client.current_map):
                return
        except Exception:
            pass
        time.sleep(1.0)
    if client.pos is None:
        back = _back_gate_center(client.current_map, prev_scene)
        if back:
            client.pos = back
            log.warning("[%s] 2K: server khong tra toa do sau khi qua cong -> lay cong doi ung %s "
                        "lam moc (tranh di mu 30 lenh)", client._label, back)


def run_floor_crawl(client, ev, stop_event, on_done=None, heal_party=None, lost_check=None,
                    du_party=None):
    """Leo tu tang hien tai len `top_map`. Chay thread rieng (giong npc40.run_loop).

    MEMBER KHONG chay ham nay: trong party, member tu dong di theo leader va khong tu di chuyen
    duoc (KNOWLEDGE muc 'Di chuyen': 0x06 bi vo hieu khi o trong party).
    """
    label = client._label
    top = int((ev.get("party_battle") or {}).get("top_map") or 0)
    if not top:
        log.warning("[%s] 2K: thieu top_map trong events.json -> khong leo", label)
        return
    client.flee_mode = False   # VAO LA DANH (khac go_to_event dat flee_mode=True)
    # EP quest_mode suot ca thap (giong pho ban to doi): KHONG de auto-latch quyet dinh - latch
    # chi bat khi quai > 6 luc vao tran (state.py), tran 2K it quai hon la mat skill toan man.
    client.state.quest_mode = True
    # _team_dungeon_until = cua so "dang chay kich ban dungeon". BAT BUOC cho 2K vi:
    #  - recv-loop CHI cap nhat _last_dialog_evt trong cua so nay -> thieu thi
    #    _adv_dialog_until_idle() khong biet thoai da het chua.
    #  - 0x14 sub0700 goi reset_enemies(reset_quest=not _in_team_dungeon) -> thieu thi quest_mode
    #    vua ep bi XOA ngay sau tran DAU TIEN.
    client._team_dungeon_until = time.time() + _DUNGEON_WINDOW
    lost = False
    # LY DO ket thuc vong leo - PHAI noi dung su that. `finally` chay o MOI duong thoat, nen neu
    # chi bao "xong/thua" thi KET O CONG va ROT MANG cung thanh "xong": ca party bi keo ra khoi
    # thap roi tat game giua chung (party 12 va party 15, 06/09).
    #   "xong" = da toi `top_map`        -> 2K het that
    #   "thua" = thua tran               -> 2K het that
    #   "ket"  = ket o cong / danh thieu tran -> CHUA het, dieu phoi xu ly (gom, moi lai)
    #   "het_duong" = da danh HET tang ma khong co cong len -> khong con gi lam o day nua. KHONG
    #                 duoc dung im an va (party 1/2 06/09 dung yen o (650,430) sau khi danh xong).
    #   "dut"  = client chet giua chung  -> CHUA het, relogin roi leo tiep
    ly_do = "ket"
    try:
        while _active(client, stop_event):
            scene = int(client.current_map or 0)
            if scene >= top:
                log.info("[%s] 2K: da toi tang cao nhat %s -> XONG", label, scene)
                ly_do = "xong"
                break
            client._team_dungeon_until = time.time() + _DUNGEON_WINDOW   # gia han moi tang
            # LAY cong len TRUOC de biet idx nao la cong (khoi bam nham), nhung KHONG duoc thoat
            # o day: TANG CHOT chua co cong len trong world_nav vi cong chi HIEN SAU KHI THANG
            # (user xac nhan 06/09). Ban cu `break` ngay -> bot len tang 11 roi DUNG IM, khong
            # danh mot tran nao (party 1 va party 11, 06/09: "len dinh thap thi ko thay danh
            # tiep"). Phai DANH HET TANG roi moi ket luan.
            up = _up_gate(scene)
            nxt, door, center = up if up else (None, None, None)
            points = _battle_points(ev, scene)
            log.info("[%s] 2K: %s -> len %s (cong door=%s tai %s), %d diem danh quai, idx %s",
                     label, _floor_label(ev, scene), nxt, door, center, len(points),
                     _battle_idx(ev, scene))
            # Duyet idx tang dan, BO idx cua cong. Truoc moi lan danh: DI TOI diem tuong ung.
            # `k` = chi so DIEM, tang theo TUNG LAN THU chu KHONG theo so tran THANG. Truoc day
            # dung points[fought] (chi tang khi thang): login lai giua tang, con 1 da chet -> khong
            # vao tran -> fought=0 mai -> moi idx deu quay lai DIEM 1, hai con con song o diem 2/3
            # KHONG BAO GIO duoc danh (log that tang 8 - 12931).
            fought = 0
            k = 0
            for idx in _battle_idx(ev, scene):
                if not _active(client, stop_event) or k >= len(points):
                    break
                if idx == door:
                    continue
                _walk_to(client, points[k], stop_event)
                k += 1
                res = _fight_one(client, idx, stop_event, heal_party, lost_check)
                if res == "lost":
                    log.warning("[%s] 2K: PARTY THUA o %s (idx=%d) -> KET THUC 2K",
                                label, _floor_label(ev, scene), idx)
                    lost = True
                    ly_do = "thua"
                    break
                if res == "won":
                    fought += 1
            if lost or not _active(client, stop_event):
                break
            if fought < len(points):
                log.warning("[%s] 2K: %s chi danh duoc %d/%d tran -> van thu qua cong",
                            label, _floor_label(ev, scene), fought, len(points))
            # DANH XONG ma van khong co cong len -> gio moi duoc ket luan. Cong cua tang chot chi
            # hien sau khi thang, nen hoi lai world_nav mot lan nua (du lieu tinh, nhung neu sau
            # nay bo sung thi chay duoc ngay).
            if up is None:
                up = _up_gate(scene)
                if up is None:
                    log.warning("[%s] 2K: %s da danh %d/%d tran ma VAN khong co cong len trong "
                                "world_nav -> HET DUONG. Capture doan 'thang xong -> cong hien' "
                                "roi bo sung world_nav.json de leo tiep.",
                                label, _floor_label(ev, scene), fought, len(points))
                    ly_do = "het_duong"
                    break
                nxt, door, center = up
            # PHAI DU PARTY MOI DUOC LEN TANG. Qua cong mot minh = hong ca vong: member bi bo
            # lai tang duoi, leader leo tiep va danh khong noi (party 5 06/09: leader qua cong luc
            # 16:34:53, tang 6 "chi danh duoc 0/3 tran").
            # Party tan giua chung la chuyen BINH THUONG o day: server CAM doi kenh khi dang trong
            # doi (result=3), nen muon doi kenh thi PHAI roi doi truoc. Ra lenh doi kenh ma khong
            # lap lai doi la loi cua nguoi RA LENH, khong phai cua member.
            if du_party is not None and not du_party():
                log.warning("[%s] 2K: %s chua du party -> KHONG len tang mot minh",
                            label, _floor_label(ev, scene))
                break
            _walk_to(client, center, stop_event)   # di toi CONG (toa do tu world_nav)
            # Qua cong len tang: cung dang `0x14 0800 [idx]`, dung _enter_gate de cho map doi that.
            client._in_scene_gate = True
            try:
                ok = client._enter_gate(center[0], center[1], door, expected_map=nxt)
            finally:
                client._in_scene_gate = False
            if not ok:
                log.warning("[%s] 2K: KET o cong %s (door=%s) -> dung leo (CHUA xong thap)",
                            label, _floor_label(ev, scene), door)
                break
            log.info("[%s] 2K: da len %s", label, _floor_label(ev, client.current_map))
            _fix_pos_after_gate(client, scene, stop_event)   # co pos -> smart path, khong thi di mu
    finally:
        client.state.quest_mode = False   # KHONG de ket dinh sang cac tran train sau nay
        client._team_dungeon_until = 0.0
        if not client.running:
            ly_do = "dut"                 # client chet -> moi ly do khac deu vo nghia
        if on_done is not None:
            try:
                on_done(lost, ly_do)
            except Exception:
                pass
