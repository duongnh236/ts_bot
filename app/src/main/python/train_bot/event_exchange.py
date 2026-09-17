"""Lap ke hoach DOI QUA SU KIEN theo chuoi nguyen lieu (truy nguoc tu qua CUOI).

Su kien kieu "Mung 2 Thang" co CHUOI DOI nhieu tang:
    Trang Tao Thao/Dien Vi/Hua Chu x50  -> Ngoc Nguy x1        (nguyen lieu goc -> trung gian)
    Ngoc Nguy x30                       -> Tranh Ba Thien Thu x1
    Tranh Ba Thien Thu x6               -> Tam Quoc Tranh Ba x3 (QUA CUOI)

PHAN LOAI (tu dong, KHONG hardcode ten - su kien thang sau doi van chay):
  - QUA CUOI    = tai nguyen CO trong phan thuong nhung KHONG bi muc doi nao tieu thu.
  - NGUYEN LIEU = bi it nhat 1 muc doi tieu thu (goc = khong muc nao tao ra no).
GUI chi hien QUA CUOI cho user tick.

KHOA TAI NGUYEN = (kind, id): client co 2 loai (eResourceType) - 1 = TIEN SU KIEN (khong nam trong
tui, doc S:124-011), 2 = VAT PHAM TUI. Id cua 2 loai co the TRUNG nhau nen phai kem kind.

TRUY NGUOC: tick 1 qua cuoi -> tinh con thieu bao nhieu o TUNG TANG (da tru so dang co). Mot loai
ngoc doi duoc tu NHIEU loai trang -> chon dai, uu tien loai dang co san nhieu nhat.

CHI DOI KHI DU TOAN BO CHUOI: thieu bat ky tang nao -> KHONG doi gi ca. Tranh canh doi ra nguyen
lieu trung gian roi tac o do, vua khong duoc qua vua CHIEM SLOT TUI.

GIOI HAN LUOT: `limit` cua tung muc (limit == 0 = KHONG gioi han, theo client) tru so lan da doi
(server gui S:124-002), va tru tiep so lan da dung TRONG CHINH ke hoach nay.
"""
from __future__ import annotations

import math

MAX_DEPTH = 8


def _key(res):
    """(kind, id) tu 1 muc cost/award."""
    return int(res.get("kind") or 2), int(res.get("item") or 0)


def build_graph(missions):
    """Tra (producers, consumed):
      producers[(kind,id)] = [(mission, so luong nhan duoc moi lan doi)]
      consumed = set((kind,id) bi tieu thu)
    CHI xet muc DOI BANG VAT PHAM (cond == 1); muc "hoan thanh dieu kien" (diem danh...) khong
    thuoc chuoi nguyen lieu.
    """
    producers, consumed = {}, set()
    for m in missions or ():
        if int(m.get("cond", 0)) != 1:
            continue
        for c in m.get("cost") or ():
            if c.get("item"):
                consumed.add(_key(c))
        for a in m.get("award") or ():
            if a.get("item"):
                producers.setdefault(_key(a), []).append((m, int(a.get("quant") or 1)))
    return producers, consumed


def _chain_depth(res, producers, depth=0, seen=None):
    """So TANG doi phai qua de ra duoc `res` (nguyen lieu goc = 0)."""
    if depth > MAX_DEPTH or res not in producers:
        return depth
    seen = (seen or set()) | {res}
    best = depth
    for m, _per in producers[res]:
        for c in m.get("cost") or ():
            if not c.get("item"):
                continue
            k = _key(c)
            if k in seen:
                continue
            best = max(best, _chain_depth(k, producers, depth + 1, seen))
    return best


def final_items(missions):
    """[(kind, id, ten, mission)] MOI MUC DOI ra qua cuoi - GUI hien de tick.

    MOI MUC MOT DONG, khong gop theo vat pham: 1 qua co the co NHIEU muc doi voi nguyen lieu /
    gia / gioi han KHAC NHAU (vd Quoc Khanh: 2 muc Xu Vang an 2 cap chu khac nhau; 2 muc Hoan Cot
    Hoan gia 20+20 va 30+30). Gop lai la tuoc quyen chon cua user va hien sai so muc so voi game.

    Sap: DO SAU CHUOI giam dan (qua xin nhat len dau) -> ten -> RE TRUOC (tong nguyen lieu it hon
    len truoc, de user thay ngay muc hoi).
    Khong hardcode ten -> su kien thang sau doi vat pham van tu sap dung.
    """
    producers, consumed = build_graph(missions)
    out = []
    for res, plist in producers.items():
        if res in consumed:
            continue
        depth = _chain_depth(res, producers)
        for m, per in plist:
            name = next((a.get("name") or "" for a in (m.get("award") or ()) if _key(a) == res), "")
            cost = sum(int(c.get("quant") or 0) for c in (m.get("cost") or ()) if c.get("item"))
            out.append((depth, name, cost / max(1, per), res[0], res[1], m))
    out.sort(key=lambda t: (-t[0], t[1], t[2]))
    return [(k, i, n, m) for _d, n, _c, k, i, m in out]


def _remaining(m, got, used):
    """So lan CON duoc doi. limit == 0 = khong gioi han (theo client)."""
    lim = int(m.get("limit") or 0)
    left = (10 ** 9) if lim <= 0 else max(0, lim - int(got.get(int(m["id"]), 0)))
    return left - int(used.get(int(m["id"]), 0))


def _ensure(res, qty, stock, used, producers, got, depth=0, why=None):
    """Bao dam co du `qty` cua tai nguyen `res` = (kind,id).

    stock/used bi SUA TRUC TIEP (caller tu sao chep khi muon thu). Tra list [(mission, so lan)]
    theo THU TU CHAY (nguyen lieu goc truoc), hoac None neu khong the.
    """
    if depth > MAX_DEPTH:
        return None
    have = int(stock.get(res, 0))
    if have >= qty:
        stock[res] = have - qty
        return []
    short = qty - have
    stock[res] = 0

    def _in_stock(mp):
        """Uu tien muc ma nguyen lieu dang co san nhieu nhat -> do phai doi nhieu tang ben duoi."""
        m = mp[0]
        vals = [int(stock.get(_key(c), 0)) for c in (m.get("cost") or ()) if c.get("item")]
        return min(vals) if vals else 0

    steps = []
    for m, per in sorted(producers.get(res, []), key=_in_stock, reverse=True):
        if short <= 0:
            break
        can = _remaining(m, got, used)
        if can <= 0 or per <= 0:
            continue
        times = min(int(math.ceil(short / per)), can)
        while times > 0:
            t_stock, t_used = dict(stock), dict(used)
            t_used[int(m["id"])] = t_used.get(int(m["id"]), 0) + times
            sub, ok = [], True
            for c in m.get("cost") or ():
                if not c.get("item"):
                    continue
                s = _ensure(_key(c), int(c["quant"]) * times, t_stock, t_used,
                            producers, got, depth + 1, why)
                if s is None:
                    ok = False
                    break
                sub.extend(s)
            if ok:
                stock.clear(); stock.update(t_stock)
                used.clear(); used.update(t_used)
                steps.extend(sub)
                steps.append((m, times))
                got_qty = per * times
                if got_qty > short:                 # doi du ra -> giu trong kho ao
                    stock[res] = stock.get(res, 0) + (got_qty - short)
                short -= got_qty
                break
            times -= 1
    if short > 0:
        # Ghi lai cho tac nghen SAU CUNG (= tang thap nhat khong du) de bao cho user biet chinh xac
        # thieu gi, thay vi chi noi "khong du nguyen lieu".
        # Giu tang SAU NHAT (nguyen lieu goc), khong phai tang ngoai cung: bao "thieu Tam Quoc
        # Tranh Ba" thi vo nghia - phai bao "thieu Trang Tao Thao: can X, co Y".
        if why is not None and depth >= int(why.get("depth", -1)):
            why["depth"] = depth
            why["res"] = res
            why["need"] = qty
            why["have"] = have
            why["limit_only"] = bool(producers.get(res)) and all(
                _remaining(m, got, used) <= 0 for m, _p in producers.get(res, []))
        return None
    return steps


def plan_for(kind, item, missions, have_fn, got, want=1, why=None, only_mission=None):
    """Ke hoach doi THEM `want` cai qua cuoi (kind,item).

    have_fn(kind, id) -> so luong dang co (tien su kien hay vat pham tui deu duoc).
    got: {missionId: so lan DA doi} (rieng tung acc, tu S:124-002).
    Tra [(mission, so lan)] theo dung THU TU CHAY, hoac None neu KHONG DU (luc do KHONG doi gi).
    """
    producers, _consumed = build_graph(missions)
    goal = (int(kind), int(item))
    if goal not in producers:
        return None
    if only_mission:
        # User tick 1 MUC cu the -> chi duoc dung dung muc do de ra qua cuoi. Cac tang NGUYEN LIEU
        # ben duoi van tu do chon muc (vd ngoc doi tu nhieu loai trang).
        producers = dict(producers)
        producers[goal] = [(m, per) for m, per in producers[goal]
                           if int(m["id"]) == int(only_mission)]
        if not producers[goal]:
            return None
    stock = {}
    for m in missions or ():
        if int(m.get("cond", 0)) != 1:
            continue
        for res in list(m.get("cost") or ()) + list(m.get("award") or ()):
            if not res.get("item"):
                continue
            k = _key(res)
            if k not in stock:
                try:
                    stock[k] = int(have_fn(k[0], k[1]) or 0)
                except Exception:
                    stock[k] = 0
    stock[goal] = 0        # muon THEM `want` cai nua -> khong tinh so dang co
    steps = _ensure(goal, int(want), stock, {}, producers, got or {}, why=why)
    if steps is None:
        return None
    merged = []
    for m, n in steps:
        if merged and merged[-1][0] is m:
            merged[-1] = (m, merged[-1][1] + n)
        else:
            merged.append((m, n))
    return merged


def options_from_cache(path=None):
    """Danh sach QUA CUOI cho GUI tick, doc tu cache do bot ghi luc dang nhap.

    Tra list chuoi "key\tnhan" (key = "kind:itemId"). Dung dinh dang chuoi de Chaquopy (APK)
    doc duoc truc tiep, khong phai dung 1 ban Kotlin chep tay -> khong the lech voi ban PC.
    """
    import json
    import os
    if not path:
        try:
            from ._appdir import app_dir
            path = os.path.join(app_dir(), "event_exchange.json")
        except Exception:
            path = "event_exchange.json"
    try:
        with open(path, encoding="utf-8") as fh:
            data = json.load(fh)
    except Exception:
        return []
    out = []
    acts = data.get("activities") or {}
    for act in (acts.values() if isinstance(acts, dict) else acts):   # cache ghi dang {id: act}
        missions = act.get("missions") or []
        for kind, item, name, m in final_items(missions):
            cost = " + ".join("%s x%d" % (c.get("name") or c.get("item"), c.get("quant") or 1)
                              for c in (m.get("cost") or ()) if c.get("item"))
            lim = int(m.get("limit") or 0)
            out.append("%d:%d:%d\t%s  [%s]  ← cần %s%s" % (
                kind, item, int(m["id"]), name, act.get("title") or "", cost,
                "" if lim <= 0 else "  (tối đa %d lần)" % lim))
    return out


def _cache_path(path=None):
    import os
    if path:
        return path
    try:
        from ._appdir import app_dir
        return os.path.join(app_dir(), "event_exchange.json")
    except Exception:
        return "event_exchange.json"


def cache_signature(path=None):
    """Chu ky cua su kien dang mo = tap KEY qua cuoi. Doi key = SU KIEN MOI.

    Khong dung mtime/hash ca file: bot ghi lai file khi tien do thay doi cung khong phai su kien
    moi. Chi khi DANH SACH QUA CUOI khac di moi coi la su kien moi.
    """
    keys = sorted(line.split("\t", 1)[0] for line in options_from_cache(path))
    return "|".join(keys)


def is_new_event(path=None, sig_path=None):
    """True DUY NHAT 1 lan khi su kien vua doi (so voi chu ky da luu). Tu ghi lai chu ky moi.

    Chu ky luu o file rieng canh cache -> khong phai doi cau truc file config (PC lan APK dung
    chung ham nay, khong ban nao chep tay logic).
    """
    import io as _io
    import os
    cur = cache_signature(path)
    if not cur:                      # chua co cache (bot chua chay lan nao) -> khong doi gi
        return False
    if not sig_path:
        sig_path = os.path.join(os.path.dirname(_cache_path(path)) or ".", "event_exchange_sig.txt")
    try:
        old = _io.open(sig_path, encoding="utf-8").read().strip()
    except Exception:
        old = ""
    if old == cur:
        return False
    try:
        _io.open(sig_path, "w", encoding="utf-8").write(cur)
    except Exception:
        pass
    return bool(old)                 # lan DAU (chua co chu ky) chi ghi lai, khong bo tick cua user
