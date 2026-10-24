import json
import os
import subprocess
import sys
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from modules.config import ROOT  # noqa: E402 - standalone script resolves its own checkout


def main():
    import socket

    names = {"DNS:" + socket.getfqdn(), "DNS:localhost", "IP:127.0.0.1"}
    try:
        request = urllib.request.Request(
            "http://169.254.169.254/opc/v2/vnics/", headers={"Authorization": "Bearer Oracle"}
        )
        with urllib.request.urlopen(request, timeout=3) as response:
            for vnic in json.load(response):
                for field in ("privateIp", "publicIp"):
                    if vnic.get(field):
                        names.add("IP:" + vnic[field])
        # IMDS does not always include the public NAT address. Explicit private config can add it.
    except Exception:
        pass
    for value in os.environ.get("AUDIT_TLS_SAN", "").split(","):
        if value.strip():
            names.add(value.strip())
    subprocess.run(
        [
            "openssl",
            "req",
            "-x509",
            "-newkey",
            "rsa:3072",
            "-sha256",
            "-days",
            "90",
            "-nodes",
            "-keyout",
            str(ROOT / "tls/server.key"),
            "-out",
            str(ROOT / "tls/server.crt"),
            "-subj",
            "/CN=HeatWave Audit",
            "-addext",
            "subjectAltName=" + ",".join(sorted(names)),
        ],
        check=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    (ROOT / "tls/server.key").chmod(0o600)
    (ROOT / "tls/server.crt").chmod(0o644)


if __name__ == "__main__":
    main()
