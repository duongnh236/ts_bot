"""Phan loai item vao 4 TAB tui do - sao DUNG logic client, khong tu nghi ra.

Nguon (lua_decrypted_all):
    EThingsCategory (Logic_Item.lua:32)      All=1 Equip=2 Props=3 Material=4
    Item.ConditionEquip     (Logic_Item.lua:3243)
    Item.ConditionProps     (Logic_Item.lua:3249)
    Item.ConditionMaterial  (Logic_Item.lua:3258)
    Item.IsTypeOfEquips     (Logic_Item.lua:1046)
    EItemFitType            (Data_ItemData.lua:1)
    EItemKind               (Data_ItemData.lua)

Chi can 2 truong cua Item_C.dat: `fitType` va `kind`. Ca hai da co san trong ban doc
(tools/crack_furnace_notify.read_items doc du 55 truong) - chi la items_gamedata.json truoc day
khong ghi ra.
"""
from __future__ import annotations

ALL = 1
EQUIP = 2
PROPS = 3
MATERIAL = 4

TAB_NAMES = [(ALL, "Tất cả"), (EQUIP, "Trang bị"), (PROPS, "Vật phẩm"), (MATERIAL, "Nguyên liệu")]

# IsTypeOfEquips(fitType): 1..6 = trang bi that, 7..11 = thoi trang, 100 = ao choang.
# (Danh sach GO NGUYEN theo Contains(...) trong Logic_Item.lua:1047 - khong rut gon thanh khoang
#  vi 12..99 KHONG phai trang bi.)
_FIT_EQUIP = frozenset({1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 100})

# ConditionEquip: ngoai fitType con nhan them cac kind nay (vk linh tinh / trang bi vo tuong).
_KIND_EQUIP = frozenset({12, 13, 14, 39, 56, 57, 59, 62})   # ..., SoulWeapon, NpcEquip, SoulEquip


def can_use(btn_state) -> bool:
    """Co nut "Su dung" khong. btnState (truong [48]) = LY DO nut bi KHOA -> 0 = khong co ly do.

    CAN THAN: ham Item.CheckItemUseState() ben client tra TRUE khi btnState > 0, doc luot qua rat
    de hieu NGUOC. Ten ham la "check use state" nhung y la "dang o trang thai KHONG dung duoc"
    (Data_ItemData:GetCantUseText cung lay itemHintDatas[btnState] lam ly do).
    Da kiem bang so lieu tren ca file: 1208/1208 nguyen lieu deu btnState > 0 (dung khong duoc),
    con moi mon hoi HP deu btnState = 0.
    """
    return not int(btn_state or 0)


def can_dismantle(furnace_count) -> bool:
    """Co nut "Phan giai" khong - Item.IsDismantle (Logic_Item.lua:2519): furnaceCount > 0."""
    return int(furnace_count or 0) > 0


def can_equip(fit_type, kind) -> bool:
    """Co nut "Trang bi" khong. CHI theo fitType: cac kind trong _KIND_EQUIP (vd cuon vo tuong) hien
    o tab Trang bi nhung KHONG mac len nguoi duoc."""
    return is_equip_fit(fit_type)


def is_equip_fit(fit_type) -> bool:
    return int(fit_type or 0) in _FIT_EQUIP


def _is_material_kind(kind) -> bool:
    k = int(kind or 0)
    return 24 <= k <= 35 or 40 <= k <= 46


def category_of(fit_type, kind) -> int:
    """Tab CHINH cua item (ngoai tab 'Tat ca'). 0 = khong thuoc tab con nao.

    Thu tu y HET client: Equip truoc, roi Material, con lai la Props. Client kiem tra RIENG tung
    tab nen mot item CO THE thuoc 2 tab (vd fitType trang bi + kind nam trong khoang nguyen lieu);
    ham nay tra tab DAU TIEN khop - dung de hien nhan, con loc tab thi dung matches_tab().
    """
    if is_equip_fit(fit_type) or int(kind or 0) in _KIND_EQUIP:
        return EQUIP
    if _is_material_kind(kind):
        return MATERIAL
    return PROPS


def matches_tab(tab, fit_type, kind) -> bool:
    """Item co hien o `tab` khong - kiem RIENG tung tab, y het client (khong dung category_of)."""
    tab = int(tab)
    if tab == ALL:
        return True
    if tab == EQUIP:
        return is_equip_fit(fit_type) or int(kind or 0) in _KIND_EQUIP
    if tab == MATERIAL:
        return _is_material_kind(kind)
    if tab == PROPS:
        # ConditionProps: KHONG phai trang bi VA KHONG nam trong khoang nguyen lieu.
        # (Client chi loai theo fitType, khong loai theo _KIND_EQUIP - giu dung nhu vay.)
        return not is_equip_fit(fit_type) and not _is_material_kind(kind)
    return False
