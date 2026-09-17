"""Thu muc goc de doc/ghi file JSON runtime (items_learned.json, items_known.json,
items_gamedata.json...) tren Android. Khac bot/_appdir.py (goc dung thu muc canh .exe/project) -
Android dung Context.getFilesDir() (thu muc rieng tu app, ghi duoc, ton tai qua cac lan chay).
Cac noi goi ham nay trong client.py (copy tu bot/client.py) DEU boc trong try/except Exception ->
neu Chaquopy/Context chua san sang, tu dong fallback ve duong dan tuong doi (khong crash)."""
from com.chaquo.python import Python


def app_dir() -> str:
    ctx = Python.getPlatform().getApplication()
    return ctx.getFilesDir().getAbsolutePath()
