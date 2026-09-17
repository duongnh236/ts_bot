"""Captured route and packet helpers for team dungeon level 110."""

DUNGEON_ID = 0x0010
MISSION_ID = 0x30AE


def decode_reinforcement(pkt: bytes):
    body = pkt[7:]
    if len(body) != 19 or body[:3] != b"\x06\x00\x01":
        return None
    return body[3:11], body[11:19]


STAGES = (
    (
        ("send", 0x14, bytes.fromhex("08000800")),
        ("send", 0x0C, bytes.fromhex("0100")),
        ("battle", 23),
    ),
    (
        ("heal_full",),
        ("advance", 10),
        ("moves", ((490, 2410), (222, 2446), (126, 2459), (50, 2470), (50, 2470))),
        ("send", 0x14, bytes.fromhex("08000600")),
        ("advance", 1),
        ("send", 0x14, bytes.fromhex("08000900")),
        ("battle", 7),
    ),
    (
        ("heal_full",),
        ("advance", 9),
        (
            "moves",
            (
                (733, 350),
                (710, 350),
                (452, 226),
                (366, 185),
                (280, 143),
                (210, 110),
                (210, 110),
            ),
        ),
        ("send", 0x14, bytes.fromhex("08000200")),
        ("advance", 1),
        (
            "moves",
            ((2796, 2314), (2721, 2255), (2647, 2195), (2590, 2150), (2590, 2150)),
        ),
        ("send", 0x14, bytes.fromhex("08000a00")),
        ("battle", 15),
    ),
    (
        ("heal_full",),
        ("advance", 12),
        (
            "moves",
            (
                (2623, 2176),
                (2796, 2313),
                (2871, 2372),
                (2946, 2432),
                (3020, 2491),
                (3070, 2530),
                (3070, 2530),
            ),
        ),
        ("send", 0x14, bytes.fromhex("08000500")),
        ("advance", 1),
        ("moves", ((430, 370), (430, 370))),
        ("send", 0x14, bytes.fromhex("08000b00")),
        ("battle", 15),
    ),
    (
        ("heal_full",),
        ("advance", 15),
        ("moves", ((430, 370), (228, 459), (141, 498), (70, 530), (70, 530))),
        ("send", 0x14, bytes.fromhex("08000300")),
        ("advance", 1),
        ("moves", ((2268, 219), (2181, 258), (2110, 290), (2110, 290))),
        ("send", 0x41, bytes.fromhex("01006464010100000101000000")),
        ("heal_full",),
        ("moves", ((2110, 290),)),
        ("send", 0x14, bytes.fromhex("08000c00")),
        ("advance", 1),
        ("send", 0x41, bytes.fromhex("0200")),
        ("battle", 12),
    ),
)
