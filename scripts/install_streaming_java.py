#!/usr/bin/env python3
"""Install the pinned Java client dependencies for VM-side Streaming checks/exploration."""

import os
import shutil
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from modules.config import ROOT  # noqa: E402


def main():
    os.umask(0o077)
    if not shutil.which("java") or not shutil.which("mvn"):
        raise SystemExit("Install java-21-openjdk-headless and maven on the OL9 VM first.")
    base = ROOT / ".data/java"
    base.mkdir(mode=0o700, exist_ok=True)
    pending = base / "streaming-libs.pending"
    shutil.rmtree(pending, ignore_errors=True)
    pending.mkdir(mode=0o700)
    subprocess.run(
        [
            "mvn",
            "-B",
            "-q",
            "-f",
            str(ROOT / "deploy/streaming-java/pom.xml"),
            "-Dmaven.repo.local=" + str(base / "m2"),
            "dependency:copy-dependencies",
            "-DincludeScope=runtime",
            "-DoutputDirectory=" + str(pending),
        ],
        check=True,
    )
    previous = base / "streaming-libs.previous"
    shutil.rmtree(previous, ignore_errors=True)
    target = base / "streaming-libs"
    if target.exists():
        target.rename(previous)
    pending.rename(target)
    print("OCI Streaming Java principal client dependencies installed.")


if __name__ == "__main__":
    main()
