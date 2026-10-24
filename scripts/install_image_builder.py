#!/usr/bin/env python3
"""Install the rootless image builder without changing web/MySQL configuration."""

import grp
import os
import pwd
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from modules.config import ROOT  # noqa: E402
from modules.image_builds import ACTIVE, inventory  # noqa: E402
from scripts.install_services import install, quoted, unit_path  # noqa: E402


def main():
    user = pwd.getpwuid(os.getuid()).pw_name
    if user == "root":
        raise SystemExit("Run as the non-root application owner with sudo access.")
    if not (ROOT / ".runtime.env").is_file():
        raise SystemExit("Complete application setup before installing the image builder.")
    if any(row["status"] in ACTIVE - {"queued"} for row in inventory()):
        raise SystemExit("Wait for the active image build to finish before restarting the builder.")
    for name in ("image-builds", "image-builder"):
        path = ROOT / ".data" / name
        path.mkdir(mode=0o700, exist_ok=True)
        path.chmod(0o700)
    install(
        "heatwave-audit-image-builder.service",
        "[Unit]\nDescription=HeatWave Audit rootless image builder\nAfter=network-online.target\nWants=network-online.target\n"
        "[Service]\nUser="
        + user
        + "\nGroup="
        + grp.getgrgid(pwd.getpwuid(os.getuid()).pw_gid).gr_name
        + "\nWorkingDirectory="
        + unit_path(ROOT)
        + "\n"
        "Environment=AUDIT_IMAGE_RUNTIME_DIR=/run/heatwave-audit-image-builder\n"
        "RuntimeDirectory=heatwave-audit-image-builder\nRuntimeDirectoryMode=0700\nRuntimeDirectoryPreserve=yes\n"
        "ExecStart=/usr/bin/bash " + quoted(ROOT / "scripts/start_image_builder.sh") + "\n"
        "UMask=0077\nPrivateTmp=true\nProtectSystem=strict\nReadWritePaths=" + quoted(ROOT) + "\n"
        "Delegate=yes\nRestart=on-failure\nRestartSec=5\nTimeoutStopSec=30\n"
        "[Install]\nWantedBy=multi-user.target\n",
    )
    subprocess.run(["sudo", "systemctl", "daemon-reload"], check=True)
    subprocess.run(
        ["sudo", "systemctl", "enable", "heatwave-audit-image-builder.service"], check=True
    )
    subprocess.run(
        ["sudo", "systemctl", "restart", "heatwave-audit-image-builder.service"], check=True
    )


if __name__ == "__main__":
    main()
