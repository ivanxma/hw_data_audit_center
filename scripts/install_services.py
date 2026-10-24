"""Write app-relative runtime files; install OL9 systemd units via sudo."""

import argparse
import grp
import json
import os
import pwd
import shlex
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from modules.config import ROOT  # noqa: E402 - standalone script resolves its own checkout
from modules.runtime_env import read_runtime  # noqa: E402
from modules.validation import ocid  # noqa: E402


def quoted(value):
    return '"' + str(value).replace("\\", "\\\\").replace('"', '\\"').replace("%", "%%") + '"'


def unit_path(value):
    """WorkingDirectory does not use ExecStart's word quoting."""
    path = str(value)
    if not path.startswith("/") or any(ord(c) < 32 or ord(c) == 127 for c in path):
        raise ValueError("A systemd path must be absolute and contain no control characters.")
    return path.replace("%", "%%")


def install(name, body):
    path = ROOT / ".data" / name
    path.write_text(body)
    path.chmod(0o600)
    subprocess.run(["systemd-analyze", "verify", str(path)], check=True)
    subprocess.run(
        ["sudo", "install", "-m", "0644", str(path), "/etc/systemd/system/" + name], check=True
    )


def runtime_settings(root, environ):
    previous = read_runtime(root / ".runtime.env")
    keys = (
        "AUDIT_CONFIG_FILE",
        "AUDIT_HTTPS_PORT",
        "SSL_CERT_FILE",
        "SSL_KEY_FILE",
        "AUDIT_MYSQLBINLOG",
    )
    values = {k: environ.get(k, previous.get(k, "")) for k in keys}
    if not values["AUDIT_CONFIG_FILE"]:
        raise ValueError("Set AUDIT_CONFIG_FILE to the private deployment JSON created on this VM.")
    private = Path(values["AUDIT_CONFIG_FILE"]).expanduser().resolve()
    config = json.loads(private.read_text())
    if not isinstance(config, dict) or not config.get("region"):
        raise ValueError("Private deployment JSON must define the VM region and compartment_id.")
    ocid(config.get("compartment_id"), "compartment")
    port = values["AUDIT_HTTPS_PORT"] or "443"
    if not port.isdigit() or not 1 <= int(port) <= 65535:
        raise ValueError("AUDIT_HTTPS_PORT must be between 1 and 65535.")
    values.update(
        AUDIT_CONFIG_FILE=str(private),
        AUDIT_HTTPS_PORT=port,
        HOST="0.0.0.0",
        AUDIT_SESSION_COOKIE_SECURE="1",
    )
    if bool(values["SSL_CERT_FILE"]) != bool(values["SSL_KEY_FILE"]):
        raise ValueError("Set SSL_CERT_FILE and SSL_KEY_FILE together.")
    for key in ("SSL_CERT_FILE", "SSL_KEY_FILE", "AUDIT_MYSQLBINLOG"):
        if values[key]:
            path = Path(values[key]).expanduser().resolve()
            if not path.is_file():
                raise ValueError(key + " must reference an existing file.")
            values[key] = str(path)
    return private, {k: v for k, v in values.items() if v}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check-config", action="store_true")
    args = parser.parse_args()
    private, values = runtime_settings(ROOT, os.environ)
    if args.check_config:
        return
    user = pwd.getpwuid(os.getuid()).pw_name
    group = grp.getgrgid(pwd.getpwuid(os.getuid()).pw_gid).gr_name
    if user == "root":
        raise SystemExit("Run setup as the intended non-root application owner with sudo access.")
    cfg = ROOT / "etc/my.cnf"
    cfg.write_text(
        "[mysqld]\nbasedir="
        + str(ROOT / ".embedded/mysql-server/current")
        + "\ndatadir="
        + str(ROOT / ".data/mysql")
        + "\nsocket="
        + str(ROOT / ".data/run/mysql.sock")
        + "\npid-file="
        + str(ROOT / ".data/run/mysqld.pid")
        + "\nlog-error="
        + str(ROOT / ".data/log/mysqld.err")
        + "\ntmpdir="
        + str(ROOT / ".data/tmp")
        + "\nskip-networking\nmysqlx=0\n"
    )
    cfg.chmod(0o600)
    runtime = ROOT / ".runtime.env"
    runtime.write_text("\n".join(k + "=" + shlex.quote(v) for k, v in values.items()) + "\n")
    runtime.chmod(0o600)
    common = (
        "[Service]\nUser="
        + user
        + "\nGroup="
        + group
        + "\nWorkingDirectory="
        + unit_path(ROOT)
        + "\nUMask=0077\nNoNewPrivileges=true\nPrivateTmp=true\nProtectSystem=strict\nReadWritePaths="
        + quoted(ROOT)
        + " "
        + quoted(private.parent)
        + "\nRestrictSUIDSGID=true\nRestart=on-failure\nRestartSec=5\n"
    )
    install(
        "heatwave-audit-local-mysql.service",
        "[Unit]\nDescription=HeatWave Audit socket-only MySQL\nAfter=network.target\n"
        + common
        + "ExecStart=/usr/bin/bash "
        + quoted(ROOT / "start_mysql.sh")
        + "\nLimitNOFILE=65535\n[Install]\nWantedBy=multi-user.target\n",
    )
    install(
        "heatwave-audit-https.service",
        "[Unit]\nDescription=HeatWave Audit HTTPS\nAfter=network-online.target heatwave-audit-local-mysql.service\nWants=network-online.target\nRequires=heatwave-audit-local-mysql.service\n"
        + common
        + "ExecStart=/usr/bin/bash "
        + quoted(ROOT / "start_https.sh")
        + "\nAmbientCapabilities=CAP_NET_BIND_SERVICE\n[Install]\nWantedBy=multi-user.target\n",
    )


if __name__ == "__main__":
    main()
