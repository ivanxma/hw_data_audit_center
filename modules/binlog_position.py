"""Bounded UTC transaction-boundary inspection; never persist binlog row payloads."""

import os
import re
import selectors
import subprocess
import tempfile
import time
from pathlib import Path

from .capture_start import gtid_set, utc_datetime, validate_gtid_boundary
from .config import ROOT
from .heatwave import connect
from .repository import query


def option_value(value):
    return (
        '"'
        + str(value)
        .replace("\\", "\\\\")
        .replace('"', '\\"')
        .replace("\n", "\\n")
        .replace("\r", "\\r")
        + '"'
    )


class TransactionBoundary:
    def __init__(self, baseline, requested):
        self.executed = baseline["gtids"]
        self.requested_us = int(requested.timestamp() * 1_000_000)
        self.position = None
        self.timestamp = None
        self.found = None

    def line(self, line, filename):
        if match := re.fullmatch(r"# at ([0-9]+)", line.strip()):
            self.position = int(match[1])
            self.timestamp = None
        if match := re.search(r"immediate_commit_timestamp=([0-9]+)", line):
            self.timestamp = int(match[1])
        if match := re.match(r"SET @@SESSION.GTID_NEXT\s*=\s*'([^']+)'", line):
            transaction = match[1]
            if transaction == "AUTOMATIC":
                return
            transaction = gtid_set(transaction)
            if self.timestamp is None or self.position is None:
                raise ValueError(
                    "This binlog lacks a supported GTID commit timestamp. Review the retained baseline and source version before historical positioning."
                )
            if self.timestamp >= self.requested_us:
                self.found = {
                    "file": filename,
                    "pos": self.position,
                    "gtids": self.executed,
                    "transaction": transaction,
                    "commit_us": self.timestamp,
                }
            else:
                self.executed = gtid_set(
                    self.executed + ("," if self.executed else "") + transaction
                )


class ExecutedSetBoundary(TransactionBoundary):
    """Only a real transaction-order prefix is a safe historical schema boundary."""

    def __init__(self, baseline, requested):
        self.executed = gtid_set(baseline["gtids"]) if baseline["gtids"] else ""
        self.requested_set = gtid_set(requested)
        self.requested_us = 2**63
        self.position = self.timestamp = self.found = None

    def line(self, line, filename):
        super().line(line, filename)
        if self.executed == self.requested_set:
            self.found = {
                "file": filename,
                "pos": self.position,
                "gtids": self.executed,
                "commit_us": self.timestamp,
            }


def read_metadata(command, parser, filename, deadline, max_bytes=64 * 1024 * 1024):
    """Discard all non-boundary lines in memory. Raw tool stderr is never surfaced."""
    with subprocess.Popen(
        command, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, env={**os.environ, "TZ": "UTC"}
    ) as process:
        poller = selectors.DefaultSelector()
        poller.register(process.stdout, selectors.EVENT_READ)
        pending, used = b"", 0
        try:
            while time.monotonic() < deadline:
                if not poller.select(timeout=min(0.2, max(0, deadline - time.monotonic()))):
                    continue
                chunk = os.read(process.stdout.fileno(), 65536)
                if not chunk:
                    if process.wait(timeout=2) != 0:
                        raise ValueError(
                            "Binlog inspection failed. Check replication access, retained files and TLS trust."
                        )
                    return
                used += len(chunk)
                if used > max_bytes:
                    raise ValueError(
                        "Position inspection exceeded its bounded read size. Review the retained evidence or establish a later baseline."
                    )
                pending += chunk
                while b"\n" in pending:
                    line, pending = pending.split(b"\n", 1)
                    parser.line(line.decode(errors="replace"), filename)
                    if parser.found:
                        return
                if len(pending) > 2 * 1024 * 1024:
                    raise ValueError("Binlog inspection encountered an oversized metadata line.")
            raise ValueError(
                "Position inspection timed out. Review the retained evidence or establish a later baseline."
            )
        finally:
            poller.close()
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=2)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=2)


def resolve_boundary(flow, cloud, baseline, value, *, mode="datetime"):
    requested = utc_datetime(value) if mode == "datetime" else None
    if requested and requested.timestamp() < baseline.get("ts_sec", float("inf")):
        raise ValueError(
            "Requested datetime predates the retained schema baseline. Earlier data needs matching historical schema evidence."
        )
    binary = Path(
        os.environ.get("AUDIT_MYSQLBINLOG", ROOT / ".embedded/mysql-server/current/bin/mysqlbinlog")
    )
    if not binary.is_file():
        raise ValueError(
            "mysqlbinlog is not installed on the OL9 VM. Configure AUDIT_MYSQLBINLOG for historical position validation."
        )
    with connect(flow["source"], cloud) as db:
        state = query(
            db,
            "SELECT @@GLOBAL.gtid_executed AS executed,UNIX_TIMESTAMP(NOW(6)) AS observed",
            one=True,
        )
        if requested and requested.timestamp() > float(state["observed"]):
            raise ValueError(
                "Capture datetime must not be later than the source server's current UTC time."
            )
        if mode == "gtid":
            target_set = validate_gtid_boundary(db, value, baseline["gtids"])
            if target_set == (gtid_set(baseline["gtids"]) if baseline["gtids"] else ""):
                return baseline
        files = query(db, "SHOW BINARY LOGS")
        start = next(
            (i for i, row in enumerate(files) if row["Log_name"] == baseline["file"]), None
        )
        if start is None:
            raise ValueError(
                "The baseline binlog has expired. Historical transaction order cannot be verified; use a new baseline or restore retained evidence."
            )
        selected = files[start:]
        source, tls = flow["source"], cloud.tls_profile()
        settings = {
            "host": source["host"],
            "port": source["port"],
            "user": source["username"],
            "password": cloud.secret(source["secret_ocid"]),
            "ssl-mode": tls["ssl_mode"],
        }
        if tls.get("ssl_ca"):
            settings["ssl-ca"] = tls["ssl_ca"]
        parser = (
            TransactionBoundary(baseline, requested)
            if mode == "datetime"
            else ExecutedSetBoundary(baseline, target_set)
        )
        deadline = time.monotonic() + 30
        temp_root = ROOT / ".data/tmp"
        temp_root.mkdir(parents=True, exist_ok=True, mode=0o700)
        with tempfile.TemporaryDirectory(prefix="binlog-position-", dir=temp_root) as directory:
            defaults = Path(directory) / "client.cnf"
            with defaults.open("x") as handle:
                defaults.chmod(0o600)
                handle.write(
                    "[client]\n"
                    + "\n".join(k + "=" + option_value(v) for k, v in settings.items())
                    + "\n"
                )
            for index, row in enumerate(selected):
                if index >= 128:
                    raise ValueError(
                        "Position inspection reached its 128 scanned-file limit before finding the boundary. "
                        "Review the retained evidence or establish a later baseline."
                    )
                name = row["Log_name"]
                if not re.fullmatch(r"[A-Za-z0-9_.-]+", name) or name.startswith("-"):
                    raise ValueError("Unexpected binary log name.")
                read_metadata(
                    [
                        str(binary),
                        "--defaults-file=" + str(defaults),
                        "--read-from-remote-server",
                        "--base64-output=DECODE-ROWS",
                        "--start-position=" + str(baseline["pos"] if index == 0 else 4),
                        "--stop-position=" + str(row["File_size"]),
                        name,
                    ],
                    parser,
                    name,
                    deadline,
                )
                if parser.found:
                    break
        if mode == "gtid" and not parser.found:
            raise ValueError(
                "The GTID set is not a complete transaction-order prefix after the schema baseline. Do not skip arbitrary transactions; choose a verified boundary or UTC datetime."
            )
        boundary = parser.found or {
            "file": selected[-1]["Log_name"],
            "pos": selected[-1]["File_size"],
            "gtids": parser.executed,
            "commit_us": None,
        }
        # A source may purge logs while the bounded scanner runs. Check again.
        if boundary["gtids"]:
            validate_gtid_boundary(db, boundary["gtids"], baseline["gtids"])
        return {**boundary, "requested_utc": requested.isoformat() if requested else None}


def resolve_datetime(flow, cloud, baseline, value):
    return resolve_boundary(flow, cloud, baseline, value)


def resolve_gtid(flow, cloud, baseline, value):
    from .gtid_position import locate_gtid

    with connect(flow["source"], cloud) as db:
        return locate_gtid(db, value, baseline)
