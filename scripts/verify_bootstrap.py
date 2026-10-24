"""Verify login and forced rotation without changing the operator's bootstrap password."""

import getpass
import re
import sys
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from modules.config import ROOT  # noqa: E402 - standalone script resolves its own checkout


def main():
    password = getpass.getpass("Bootstrap password for verification: ")
    s = requests.Session()
    s.verify = str(ROOT / "tls/server.crt")
    login = s.get("https://localhost/login", timeout=10)
    login.raise_for_status()
    token = re.search(r'name="csrf" value="([^"]+)"', login.text).group(1)
    result = s.post(
        "https://localhost/login",
        data={"username": "localadmin", "password": password, "csrf": token},
        allow_redirects=False,
        timeout=10,
    )
    assert result.status_code == 302 and result.headers["Location"].endswith("/dashboard"), (
        "Login failed"
    )
    guarded = s.get("https://localhost/dashboard", allow_redirects=False, timeout=10)
    assert guarded.status_code == 302 and guarded.headers["Location"].endswith("/password"), (
        "Rotation gate failed"
    )
    page = s.get("https://localhost/password", timeout=10)
    page.raise_for_status()
    assert password not in page.text, "Credential leak"
    token = re.search(r'name="csrf" value="([^"]+)"', page.text).group(1)
    logout = s.post(
        "https://localhost/logout", data={"csrf": token}, allow_redirects=False, timeout=10
    )
    assert logout.status_code == 302, "Logout failed"
    print(
        "Live login, forced password rotation, credential-free page and logout verified. Bootstrap password remains unchanged."
    )


if __name__ == "__main__":
    main()
