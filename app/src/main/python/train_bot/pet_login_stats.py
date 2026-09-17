"""Decode the active pet login record and reproduce the game's max HP/SP formula."""
from __future__ import annotations

import math


STONE_VALUES = (1, 2, 3, 4, 5, 7, 9, 11, 13, 15, 18, 21, 24, 27, 30)
STONE_ATTRS = {1: 212, 4: 218, 5: 219, 6: 214}


def parse_record(body: bytes, off: int) -> dict | None:
    if off + 33 > len(body):
        return None
    name_len = body[off + 31]
    equip_off = off + 35 + name_len
    suffix = equip_off + 6 * 35
    if suffix + 9 > len(body):
        return None
    equipment = []
    for i in range(6):
        raw = body[equip_off + i * 35:equip_off + (i + 1) * 35]
        equipment.append({
            "id": int.from_bytes(raw[0:2], "little"),
            "element": raw[7],
            "element_value": raw[8],
            "stone_attr": raw[16],
            "stone_lv": raw[17],
        })
    return {
        "marker": body[off],
        "id": int.from_bytes(body[off + 1:off + 3], "little"),
        "level": body[off + 7],
        "hp": int.from_bytes(body[off + 8:off + 12], "little"),
        "sp": int.from_bytes(body[off + 12:off + 14], "little"),
        # Int/Atk/Def: thu tu truong theo Logic_Role.lua Role.FollowNpcAppear
        #   +3 Exp(4) +7 Lv(1) +8 Hp(4) +12 Sp(2) +14 Int(2) +16 Atk(2) +18 Def(2)
        #   +20 Agi(2) +22 Hpx(2) +24 Spx(2) +26 dieCount +27 Faith +31 namelen
        # 3 moc +20/+22/+24 duoi day von da dung tu truoc -> bo cuc nay tin duoc.
        "int": int.from_bytes(body[off + 14:off + 16], "little"),
        "atk": int.from_bytes(body[off + 16:off + 18], "little"),
        "def": int.from_bytes(body[off + 18:off + 20], "little"),
        "agi": int.from_bytes(body[off + 20:off + 22], "little"),
        "hpx": int.from_bytes(body[off + 22:off + 24], "little"),
        "spx": int.from_bytes(body[off + 24:off + 26], "little"),
        "equipment": equipment,
        "hp_pill": body[suffix + 6],
        "sp_pill": body[suffix + 7],
    }


def style_attribute(data: dict, flags: dict[int, int], wanted: int) -> int:
    points = 0
    styles = data.get("styles", {})
    for style_id, flag in flags.items():
        saved = styles.get(str(style_id))
        if not saved:
            continue
        item_ids, scores = saved
        complete = True
        for index, item_id in enumerate(item_ids):
            if not item_id:
                continue
            owned = bool(flag & (1 << index))
            if owned:
                points += scores[index]
            else:
                complete = False
        if complete:
            points += scores[5]
    total = 0
    for score, attrs in data.get("style_values", []):
        if score <= points:
            for kind, value in attrs:
                if kind == wanted:
                    total += value
    return total


def style_bonus(data: dict, flags: dict[int, int]) -> tuple[int, int]:
    return style_attribute(data, flags, 31), style_attribute(data, flags, 32)


def card_attribute(data: dict, equipped: list[int], levels: dict[int, int], wanted: int) -> int:
    total = 0
    cards = data.get("cards", {})
    for card_id in equipped:
        if not card_id:
            continue
        level = max(1, levels.get(card_id, 0))
        for kind, value, grow in cards.get(str(card_id), []):
            if kind == wanted:
                total += value + grow * (level - 1)
    return total


def card_bonus(data: dict, equipped: list[int], levels: dict[int, int]) -> tuple[int, int]:
    return (card_attribute(data, equipped, levels, 31),
            card_attribute(data, equipped, levels, 32))


def equipment_bonus(record: dict, data: dict, element: int) -> dict[int, int]:
    result = {207: 0, 208: 0, 212: 0, 214: 0, 218: 0, 219: 0}
    items = data.get("items", {})
    suit_counts = {}
    loaded = []
    spirituality = 0
    for saved in record["equipment"]:
        item = items.get(str(saved["id"]))
        if not item:
            continue
        loaded.append((saved, item))
        suit_id = item.get("s", 0)
        if suit_id:
            suit_counts[suit_id] = suit_counts.get(suit_id, 0) + 1
        if item.get("e") == element or saved["element"] == element:
            spirituality += 1
    for saved, item in loaded:
        for kind, value in item.get("a", []):
            extra = 0
            if value > 0:
                if item.get("e") in (element, 5):
                    extra += max(0, item.get("ev", 0))
                if saved["element"] in (element, 5):
                    extra += max(0, saved["element_value"] - 100)
                if spirituality >= 5:
                    extra += 3
            result[kind] = result.get(kind, 0) + value + extra
        stone_kind = STONE_ATTRS.get(saved["stone_attr"])
        stone_lv = saved["stone_lv"]
        if stone_kind and 1 <= stone_lv <= len(STONE_VALUES):
            result[stone_kind] += STONE_VALUES[stone_lv - 1]
    for suit_id, count in suit_counts.items():
        for need, kind, value in data.get("suits", {}).get(str(suit_id), []):
            if count >= need:
                result[kind] = result.get(kind, 0) + value
    return result


def calculate(record: dict, data: dict, style=(0, 0), cards=(0, 0)) -> tuple[int, int]:
    element, raw_turn = data.get("npcs", {}).get(str(record["id"]), [0, 0])
    equip = equipment_bonus(record, data, element)
    hpx = max(0, record["hpx"] + equip[218] + style[0] + cards[0])
    spx = max(0, record["spx"] + equip[219] + style[1] + cards[1])
    level = record["level"]
    turn = 1 if raw_turn == 2 else 0
    hp_base = 180 + 4 * hpx if turn else 80 + 2 * hpx
    sp_base = 110 if turn else 60
    hp = math.floor(level + 2 * hpx * level ** 0.35 + 0.5)
    hp += hp_base + equip[207] + record["hp_pill"] * 50
    sp = math.floor(level + 2 * spx * level ** 0.25 + 0.5)
    sp += sp_base + equip[208] + record["sp_pill"] * 10
    return max(1, hp), max(1, sp)


def calculate_agi(record: dict, data: dict, style_agi=0, card_agi=0) -> int:
    element = data.get("npcs", {}).get(str(record["id"]), [0, 0])[0]
    equip = equipment_bonus(record, data, element)
    return max(0, record.get("agi", 0) + equip[214] + style_agi + card_agi)


def mount_base_int(points: int, data: dict) -> int:
    value = 0
    remaining = points
    for _level, add, need in sorted(data.get("mount_int_grow", [])):
        if remaining < need:
            break
        value = add
        remaining -= need
    return value


def mount_collection_count(raw_flags: bytes, data: dict) -> int:
    count = 0
    for flag in data.get("mount_flags", []):
        index, bit = divmod(flag, 8)
        if index < len(raw_flags) and raw_flags[index] & (1 << bit):
            count += 1
    return count
