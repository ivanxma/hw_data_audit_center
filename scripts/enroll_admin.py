"""Local, interactive bootstrap. Never save or print the submitted password."""

import argparse
import getpass
import json
import os
import re
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from modules.config import ROOT  # noqa: E402 - standalone script resolves its own checkout
from modules.mysql_util import escape_sql_literal  # noqa: E402 - standalone script resolves its own checkout


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--environment", action="store_true")
    args = parser.parse_args()
    profiles = ROOT / "profiles.json"
    if profiles.exists():
        raise SystemExit(
            "Local admin is already enrolled. Use authenticated password change; no implicit credential reset."
        )
    username = os.environ.get("LOCAL_MYSQL_ADMIN_USER", "localadmin")
    if not re.fullmatch(r"[A-Za-z][A-Za-z0-9_]{0,31}", username):
        raise SystemExit("Invalid local administrator name.")
    password = (
        os.environ.get("LOCAL_MYSQL_ADMIN_PASSWORD")
        if args.environment
        else getpass.getpass("Initial localadmin password: ")
    )
    if not password:
        raise SystemExit("An explicit bootstrap password is required.")
    if not args.environment and password != getpass.getpass("Confirm initial password: "):
        raise SystemExit("Passwords do not match.")
    text = (ROOT / ".data/log/mysqld.err").read_text()
    match = re.search(r"A temporary password is generated for root@localhost: (\S+)", text)
    if not match:
        raise SystemExit(
            "Initial root credential not found. Inspect local setup state without resetting accounts."
        )
    env = dict(os.environ)
    env["MYSQL_PWD"] = match.group(1)
    sql = (
        "ALTER USER CURRENT_USER() IDENTIFIED BY "
        + escape_sql_literal(password)
        + ";\nRENAME USER "
        + escape_sql_literal("root")
        + "@'localhost' TO "
        + escape_sql_literal(username)
        + "@'localhost';\n"
        + (ROOT / "sql/repository.sql").read_text()
    )
    proc = subprocess.run(
        [
            str(ROOT / ".embedded/mysql-server/current/bin/mysql"),
            "--no-defaults",
            "--protocol=SOCKET",
            "--socket=" + str(ROOT / ".data/run/mysql.sock"),
            "--user=root",
            "--connect-expired-password",
        ],
        input=sql,
        text=True,
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
    )
    if proc.returncode:
        raise SystemExit(
            "Enrollment failed. Check local MySQL setup; credential details are suppressed."
        )
    profiles.write_text(
        json.dumps(
            {
                "admin_users": [username],
                "rotation_required": [username],
                "profiles": {
                    "local-admin-profile": {"mode": "socket", "database": "audit_control"}
                },
            },
            indent=2,
        )
    )
    profiles.chmod(0o600)
    # Remove temporary bootstrap material from the private server log after account rename.
    log = ROOT / ".data/log/mysqld.err"
    log.write_text(
        re.sub(
            r"(A temporary password is generated for root@localhost: )\S+",
            r"\1[removed after enrollment]",
            text,
        )
    )
    log.chmod(0o600)
    print(
        "Local administrator enrolled. First login requires a password change and signs out afterward."
    )


if __name__ == "__main__":
    main()
