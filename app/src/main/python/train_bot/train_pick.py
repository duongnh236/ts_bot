"""Tu chon MAP + DIEM TRAIN theo level party, so quai va he quai.

Luong: level party -> level quai MONG MUON -> loc map chua level do -> chon diem.

Uu tien diem CHUA CO DU LIEU (de gom thong ke he/level), het diem trong moi loc theo cau hinh.
Khong ra diem nao thi ha level mong muon xuong 1 roi tim lai.

Level cua map doc tu TEN MAP (quy uoc user tu dat): '129-130', '126', '4x'.
Level/he cua quai doc tu train_block_stats.json + npc_table.json (KHONG chep lai vao dau).
"""
from __future__ import annotations

import random
import re

from . import train_block_stats

# Cach suy level quai mong muon tu level party. Khoa nay luu trong accounts.json.
# (khoa, nhan DAI cho dropdown Map, nhan NGAN cho o hep nhu 'Cap quai DG').
PICK_MODES = [
    ("avg-20", "Tự chọn map: level TB -20", "Tự chọn: TB -20"),
    ("avg-25", "Tự chọn map: level TB -25", "Tự chọn: TB -25"),
    ("avg-30", "Tự chọn map: level TB -30", "Tự chọn: TB -30"),
    ("min+29", "Tự chọn map: level thấp nhất +29", "Tự chọn: thấp nhất +29"),
    ("max-29", "Tự chọn map: level cao nhất -29", "Tự chọn: cao nhất -29"),
]
PICK_KEYS = [row[0] for row in PICK_MODES]


def pick_label(key, short=False):
    i = 2 if short else 1
    return next((row[i] for row in PICK_MODES if row[0] == key), "")


def pick_key(label):
    """Nhan (dai HOAC ngan) -> khoa. '' neu khong phai muc tu chon."""
    return next((row[0] for row in PICK_MODES if label in (row[1], row[2])), "")
DEFAULT_PICK = "avg-25"        # mac dinh khi doi sang mode train

# 7 he cua game + VO HE. So 6 (EElement.None) khong npc nao dung nen khong dua vao.
ELEMENTS = [(1, "Địa"), (2, "Thủy"), (3, "Hỏa"), (4, "Phong"),
            (5, "Tâm"), (7, "Quang"), (8, "Ám"), (0, "Vô hệ")]
ALL_ELEMENTS = [e for e, _n in ELEMENTS]
DEFAULT_MOB_MIN = 3
DEFAULT_MOB_MAX = 4


# Cac MOC cap quai Di Gioi (goi 0x61 02 00 idx; idx = vi tri trong list + 1).
DG_LEVELS = [10, 25, 40, 55, 70, 85, 100, 110, 120, 130, 140, 150, 160, 170, 180]


def nearest_tier(level, tiers=None):
    """Moc gan `level` nhat. BANG NHAU thi lay moc THAP HON (user chot).

    VD level 115, moc 110 va 120 deu cach 5 -> tra 110.
    """
    tiers = sorted(int(t) for t in (tiers or DG_LEVELS))
    if not tiers:
        return None
    level = int(level)
    # key: khoang cach truoc, roi -t de khi bang nhau cai THAP HON thang (min lay cai dau tien khi
    # sap xep tang dan theo key; -t lam moc thap co key lon hon... nen dung t truc tiep).
    return min(tiers, key=lambda t: (abs(t - level), t))


def desired_dg_level(pick_mode, levels, tiers=None):
    """Level party -> level quai mong muon -> MOC Di Gioi gan nhat. None neu chua tinh duoc."""
    want = desired_level(pick_mode, levels)
    if want is None:
        return None
    return nearest_tier(want, tiers)


def map_level_range(name):
    """Ten map -> (level thap nhat, level cao nhat). Doc duoc ca 111 map hien co.

    '129-130' -> (129,130) | '126' -> (126,126) | '4x' -> (40,49). Khong doc duoc -> None.
    """
    s = str(name or "").strip()
    m = re.search(r"(\d+)\s*-\s*(\d+)\s*$", s)
    if m:
        lo, hi = int(m.group(1)), int(m.group(2))
        return (lo, hi) if lo <= hi else (hi, lo)
    m = re.search(r"(\d+)\s*[xX]\s*$", s)
    if m:
        d = int(m.group(1)) * 10
        return d, d + 9
    m = re.search(r"(\d+)\s*$", s)
    if m:
        return int(m.group(1)), int(m.group(1))
    return None


_SOUL_PREFIX = re.compile(r"^\s*LH\s*-")


def is_soul_map(name):
    """Map 'LH-...' = quai LINH HON, cuc khoe, phai reborn 2 lan moi danh duoc.

    BOT TU CHON thi bo qua han. User chon TAY van vao binh thuong (khong chan o day).
    """
    return bool(_SOUL_PREFIX.match(str(name or "")))


def desired_level(pick_mode, levels):
    """Level quai mong muon. levels = level CUA CA CHAR VA PET moi thanh vien.

    Trung binh lam tron giong _average_party_levels ben run_party_digioi (cong len len // 2).
    """
    nums = [int(x) for x in (levels or []) if isinstance(x, int) and x > 0]
    if not nums or pick_mode not in PICK_KEYS:
        return None
    if pick_mode == "min+29":
        return max(1, min(nums) + 29)
    if pick_mode == "max-29":
        return max(1, max(nums) - 29)
    avg = (sum(nums) + len(nums) // 2) // len(nums)
    return max(1, avg - int(pick_mode.split("-")[1]))


def spot_profile(spot_data):
    """Rut gon 1 diem train: level quai, he quai, so con hay gap nhat.

    Tra None o 'levels'/'elements' khi diem CHUA CO du lieu quai -> nhanh gom du lieu dung cai nay.
    """
    spot_data = spot_data or {}
    mobs = spot_data.get("mobs") or {}
    levels, elements = [], set()
    table = train_block_stats._npc_table()
    for tid in mobs:
        info = table.get(str(tid))
        if not info:
            continue
        lv = info.get("level")
        if isinstance(lv, int) and lv > 0:
            levels.append(lv)
        elements.add(int(info.get("element") or 0))
    patterns = spot_data.get("patterns") or {}
    return {
        "levels": (min(levels), max(levels)) if levels else None,
        "elements": elements or None,
        "patterns": patterns,
        "battles": sum(int(v) for v in patterns.values()),
        "has_data": bool(levels),
    }


# Ti le so tran phai roi vao khoang min/max thi diem moi duoc coi la HOP (user chot 26/08).
MATCH_SHARE = 0.40

def _la_block_cham(pattern) -> bool:
    """'1x2' / '1x3' / '1x4' / '3x1 + 1x2' -> True. '1x1', '2x1', '3x1', '4x1' -> False.

    Dang 'WxC' = C khoi, moi khoi W quai (doc y het _mobs_in_pattern de khong lech cach hieu).
    W == 1 va C >= 2 = nhieu khoi le -> phai danh tung khoi mot -> RAT LAU. User chot: loai.

    LUU Y '1x2' KHAC '2x1' (user nhac): 1x2 = HAI khoi, moi khoi 1 quai -> loai.
    2x1 = MOT khoi 2 quai -> danh mot lan, GIU. '1x1' = mot con le, cung GIU.
    """
    for part in str(pattern).split("+"):
        try:
            w, c = part.strip().split("x")
            w, c = int(w), int(c)
        except Exception:
            continue
        if w == 1 and c >= 2:
            return True
    return False


# Block cham chiem duoi ti le nay = NHIEU, khong loai diem (user chot 30/08: "duoi 1% la nhieu").
# CO Y de RAT THAP: luat goc cua user van la "co block 1x2/1x3 thi loai luon diem do"; nguong nay
# chi de bo qua ghi nhan LE TE (vai tran tren hang tram), khong phai de noi tay luat.
SLOW_BLOCK_MAX_SHARE = 0.01


def slow_block_share(patterns) -> float:
    """Ti le tran roi vao block cham (1x2/1x3/1x4). 0.0 neu chua co du lieu."""
    patterns = patterns or {}
    tong = sum(int(v) for v in patterns.values())
    if not tong:
        return 0.0
    return sum(int(v) for k, v in patterns.items() if _la_block_cham(k)) / tong


def has_slow_block(patterns) -> bool:
    """Diem co QUA NHIEU block 1x2/1x3/1x4 -> loai (danh qua lau).

    User chot 26/08: "khong can nguong dau" - loai ngay khi TUNG ghi nhan mot lan. Nhung thuc te
    30/08 cho thay chot tuyet doi do loai oan diem TOT NHAT: 'Trai Pham Thanh3 145-146' diem 0 co
    `4x1: 426 / 439 tran` = 97% dung y user muon, ma bi loai chi vi DUNG MOT tran `1x3` (0.2%).
    Party 1 vi the phai tut xuong map 142-143.
    Nen: van GIU luat "co block cham thi loai", chi bo qua khi no le te duoi
    `SLOW_BLOCK_MAX_SHARE` (user chot 30/08: duoi 1%) - tuc quai lang thang lac vao / ghi nhan le.
    """
    return slow_block_share(patterns) >= SLOW_BLOCK_MAX_SHARE


def mob_share_in_range(patterns, mob_min, mob_max):
    """Ti le tran co so quai nam trong [mob_min, mob_max]. 0.0 neu chua co du lieu.

    THAY CHO cach cu "lay dang xuat hien nhieu nhat roi so": game spawn moi diem theo DUNG HAI
    muc, gan nhu 50/50 (do that: diem 4 'Rung Doi Phuong2' co 4x1:9386 vs 2x1:9295 - chenh 0.5%).
    Lay dang dong nhat thi con so do la TUNG DONG XU - may nay ra 4, may kia ra 2, cung mot cau
    hinh lai chon hai map khac nhau (user gap 26/08, mat ca buoi truy).
    Con mot le nua: cai user NHIN tren dropdown la khoang min-max cua CA CAC DANG
    (format_mob_range), nen loc theo mot dang don la loc theo con so user khong he thay.
    """
    patterns = patterns or {}
    tong = sum(int(v) for v in patterns.values())
    if not tong:
        return 0.0
    trong = sum(int(v) for k, v in patterns.items()
                if mob_min <= train_block_stats._mobs_in_pattern(k) <= mob_max)
    return trong / tong


def vi_sao_loai(prof, level, mob_min, mob_max, elements):
    """"" = diem HOP; khac rong = LY DO bi loai (de log noi duoc vi sao chon map thap hon).

    Truoc day chi tra True/False nen log ghi tron "co N diem nhung BO LOC loai het" - user hoi
    "tai sao no chon map 142-143" thi phai ngoi do bang tay lai tung diem.
    """
    lv = prof.get("levels")
    if not lv or not (lv[0] <= level <= lv[1]):
        return "level quai %s khong chua %d" % (lv, level)
    if has_slow_block(prof.get("patterns")):
        return "block cham (1x2/1x3/1x4) chiem %d%% (nguong %d%%)" % (
            round(slow_block_share(prof.get("patterns")) * 100),
            round(SLOW_BLOCK_MAX_SHARE * 100))
    if prof.get("patterns"):
        _ty = mob_share_in_range(prof["patterns"], mob_min, mob_max)
        if _ty < MATCH_SHARE:
            return "chi %d%% tran co %d-%d quai (can >=%d%%)" % (
                round(_ty * 100), mob_min, mob_max, round(MATCH_SHARE * 100))
    want = set(elements or ALL_ELEMENTS)
    got = prof.get("elements")
    # Diem phai TOAN quai thuoc he da tick: tick nghia la "chi danh nhung he nay".
    if got and not got <= want:
        return "co he %s ngoai he da tick" % sorted(got - want)
    return ""


def spot_matches(prof, level, mob_min, mob_max, elements):
    """Diem co hop cau hinh khong (chi goi khi diem DA CO du lieu)."""
    return not vi_sao_loai(prof, level, mob_min, mob_max, elements)


def _maps_chua_quet(maps, level):
    """[map_id] cua map CO KHOANG LEVEL chua `level` nhung CHUA CO DIEM NAO (chua quet quai).

    Uu tien cao nhat khi chon map: co map chua quet thi den do quet truoc da - vua co them bai
    train, vua la cach DUY NHAT de map moi config vao duoc dung. `_needs_train_mob_probe` chi
    chay khi party DA DUNG TREN map, nen map khong bao gio duoc chon = khong bao gio duoc quet.
    """
    out = []
    for map_id, name, mobs in maps:
        if is_soul_map(name) or (mobs or []):
            continue
        rng = map_level_range(name)
        if rng and rng[0] <= level <= rng[1]:
            out.append(int(map_id))
    return out


def _spots_of_maps(maps, level):
    """[(map_id, spot_index, spot_xy)] cua moi map co chua `level` trong khoang ten map."""
    out = []
    for map_id, name, mobs in maps:
        if is_soul_map(name):
            continue
        rng = map_level_range(name)
        if not rng or not (rng[0] <= level <= rng[1]):
            continue
        for i, xy in enumerate(mobs or []):
            out.append((int(map_id), i, xy))
    return out


def pick_train_spot(pick_mode, levels, maps, mob_min=DEFAULT_MOB_MIN, mob_max=DEFAULT_MOB_MAX,
                    elements=None, stats=None, rng=None):
    """-> (map_id, spot_index, level_da_dung, ly_do) hoac None.

    maps = [(map_id, ten_map, [diem...])]. Ha dan level mong muon cho toi khi ra diem.
    """
    want = desired_level(pick_mode, levels)
    if want is None:
        return None
    rng = rng or random
    stats = stats if stats is not None else train_block_stats.load_stats()
    all_spots = stats.get("maps", {})

    def _profs(level):
        """[(map_id, idx, prof)] cua moi diem thuoc level do."""
        out = []
        for map_id, idx, xy in _spots_of_maps(maps, level):
            sd = (all_spots.get(str(map_id), {}).get("spots", {})
                  .get(train_block_stats.spot_key(xy)))
            out.append((map_id, idx, spot_profile(sd)))
        return out

    def _it_tran_nhat(ds):
        """Diem CO IT TRAN NHAT (user: de fill day train_block_stats.json). Hoa -> random."""
        it = min(p["battles"] for _m, _i, p in ds)
        return rng.choice([(m, i) for m, i, p in ds if p["battles"] == it])

    # Vet duong ha level. Thieu no thi khi bot chon level 122 trong khi muon 130, KHONG AI biet vi
    # sao - phai ngoi do nguoc bang tay (da xay ra that 26/08, mat ca buoi).
    vet = []

    def _ly_do(chinh):
        return chinh + (" | tut tu %d: %s" % (want, ", ".join(vet[:8])) if vet else "")

    # LUAT USER 26/08: CHI HA level, TOI DA -5. "Tang len kho danh" nen khong tim len nua.
    khoang = [lv for lv in range(want, want - 6, -1) if lv >= 1]
    for level in khoang:
        # UU TIEN 1: map CHUA QUET (khong co diem nao). Den do quet truoc - vua them bai train,
        # vua la cach duy nhat map moi config vao duoc dung (khong chon = khong ai toi = khong
        # bao gio quet). Tra idx = -1 = "chua co diem", caller quet xong roi random 1 bai.
        _chua_quet = _maps_chua_quet(maps, level)
        if _chua_quet:
            return rng.choice(_chua_quet), -1, level, _ly_do("map CHUA QUET quai -> den quet truoc")
        ds = _profs(level)
        if not ds:
            vet.append("%d khong map" % level)
            continue
        chua_data = [(m, i) for m, i, p in ds if not p["has_data"]]
        if chua_data:                     # LUAT: uu tien diem CHUA co du lieu (de fill thong ke)
            map_id, idx = rng.choice(chua_data)
            return map_id, idx, level, _ly_do("chua co du lieu quai (gom du lieu)")
        ok = [(m, i, p) for m, i, p in ds
              if spot_matches(p, level, mob_min, mob_max, elements)]
        if ok:                            # LUAT: nhieu diem hop -> lay diem IT TRAN NHAT
            map_id, idx = _it_tran_nhat(ok)
            return map_id, idx, level, _ly_do("khop level/so quai/he (diem it tran nhat)")
        # NOI RO vi sao loai: gom theo ly do + dem. Khong thi log chi ghi "BO LOC loai het" va
        # user phai ngoi do lai tung diem bang tay de biet vuong so quai hay vuong he.
        _dem = {}
        for _m, _i, _p in ds:
            _ly = vi_sao_loai(_p, level, mob_min, mob_max, elements)
            if _ly:
                _dem[_ly] = _dem.get(_ly, 0) + 1
        _chi_tiet = "; ".join("%dx %s" % (n, ly) for ly, n in
                              sorted(_dem.items(), key=lambda kv: -kv[1])[:3])
        vet.append("%d co %d diem nhung BO LOC loai het (%s)" % (level, len(ds), _chi_tiet))

    # LUAT: het ca khoang [want-5, want] ma khong diem nao hop -> lay diem IT TRAN NHAT trong CA
    # khoang do (van train duoc, va gom them du lieu cho nhung diem con thieu).
    theo_level = {lv: _profs(lv) for lv in khoang}
    ca_khoang = [(lv, m, i, p) for lv, ds in theo_level.items() for m, i, p in ds]
    if ca_khoang:
        it = min(p["battles"] for _lv, _m, _i, p in ca_khoang)
        lv, map_id, idx, _p = rng.choice([r for r in ca_khoang if r[3]["battles"] == it])
        return map_id, idx, lv, _ly_do(
            "khong diem nao hop trong %d..%d -> lay diem it tran nhat" % (khoang[-1], want))

    # LUAT: ca khoang [want-5, want] KHONG CO MAP NAO (khac han "co map ma khong hop") -> HA TIEP
    # level cho toi khi gap map GAN NHAT, lay diem it tran nhat. KHONG BAO GIO tim len: user
    # "tang len kho danh".
    for level in range(khoang[-1] - 1, 0, -1):
        ds = _profs(level)
        if ds:
            map_id, idx = _it_tran_nhat(ds)
            return map_id, idx, level, _ly_do(
                "khong map nao o %d..%d -> ha tiep, map gan nhat o level %d (diem it tran nhat)"
                % (khoang[-1], want, level))

    # LUAT: level muon THAP HON map thap nhat cua game -> lay luon MAP THAP NHAT.
    # Vd party level 39 chon "TB -30" -> muon lv9, map thap nhat la lv28. Khong co nhanh nay thi
    # bot khong chon duoc map nao (bug 25/08).
    lo = None
    for _mid, _name, _mobs in maps:
        r = map_level_range(_name)
        if r and not is_soul_map(_name):
            lo = r[0] if lo is None else min(lo, r[0])
    if lo is not None:
        ds = _profs(lo)
        if ds:
            map_id, idx = _it_tran_nhat(ds)
            return map_id, idx, lo, _ly_do(
                "muon lv%d THAP HON map thap nhat cua game (lv%d) -> lay map thap nhat "
                "(diem it tran nhat)" % (want, lo))
    return None
