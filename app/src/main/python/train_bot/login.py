"""HTTP login -> lay user_id + access_token."""
import urllib.parse
import urllib.request
import urllib.error
import json
import time
import logging
from . import config

log = logging.getLogger("login")


import hashlib


class LoginError(RuntimeError):
    def __init__(self, data):
        self.data = data or {}
        self.error_code = self.data.get("error_code")
        super().__init__(f"Login that bai: {self.data}")

def _device_id_for(username: str) -> str:
    """Tao device_id DUY NHAT cho moi account (32 hex) - tranh server coi 2 acc chung device."""
    return hashlib.md5(("dev_" + username).encode()).hexdigest()


def _tracking_id_for(username: str) -> str:
    """tracking_id dang UUID duy nhat moi account."""
    h = hashlib.md5(("trk_" + username).encode()).hexdigest()
    return f"{h[:8]}-{h[8:12]}-{h[12:16]}-{h[16:20]}-{h[20:32]}"


def login(username: str = None, password: str = None, device_id: str = None) -> dict:
    """Goi API login, tra ve {user_id, access_token, username}.

    Raise RuntimeError neu that bai.
    """
    username = username or config.USERNAME
    password = password or config.PASSWORD
    device_id = device_id or _device_id_for(username)

    params = {
        "username": username,
        "password": password,
        "device_id": device_id,
        "agency_id": "1",
        "device_os_version": "Samsung SM-A528B 12",
        "client_version": "1.1",
        "lang": "vi",
        "device_os": "android",
        "local_agency_id": "1",
        "tracking_id": _tracking_id_for(username),
        "carrier": "",
    }
    body = urllib.parse.urlencode(params).encode()
    req = urllib.request.Request(
        config.LOGIN_URL,
        data=body,
        headers={"Content-Type": "application/x-www-form-urlencoded",
                 "User-Agent": "okhttp/4.12.0"},
    )
    # RETRY khi can cong ephemeral (WinError 10048: 100 acc login don dap -> cong TIME_WAIT chua
    # kip giai phong) hoac loi mang tam. Cho tang dan (cong tu giai phong sau ~120s TIME_WAIT).
    data = None
    last_err = None
    for attempt in range(6):
        try:
            with urllib.request.urlopen(req, timeout=15) as resp:
                data = json.loads(resp.read().decode())
            break
        except (urllib.error.URLError, OSError) as e:
            last_err = e
            # WinError 10048 = address in use (can cong); 10055/10060 = het buffer/timeout -> deu retry duoc
            wait = min(30, 3 * (attempt + 1))
            log.warning("[%s] login loi (%s) -> thu lai sau %ds (lan %d/6)", username, e, wait, attempt + 1)
            time.sleep(wait)
    if data is None:
        raise RuntimeError(f"Login that bai sau 6 lan thu (can cong/loi mang): {last_err}")

    if not data.get("status"):
        raise LoginError(data)

    d = data["data"]
    return {
        "user_id": d["user_id"],
        "access_token": d["access_token"],
        "username": d["username"],
    }


if __name__ == "__main__":
    print(login())
