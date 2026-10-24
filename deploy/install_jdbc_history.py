"""Install the JDBC storage module matching the MySQL connector in the base image."""

import hashlib
import os
import re
import urllib.request
from pathlib import Path


def main():
    version = os.environ["DEBEZIUM_VERSION"]
    if not re.fullmatch(r"\d+\.\d+\.\d+\.Final", version):
        raise ValueError("A released Debezium base version is required.")
    folder = Path("/kafka/connect/debezium-connector-mysql")
    if not (folder / ("debezium-connector-mysql-" + version + ".jar")).is_file():
        raise ValueError("Base connector version does not match DEBEZIUM_VERSION.")
    name = "debezium-storage-jdbc-" + version + ".jar"
    url = (
        "https://repo.maven.apache.org/maven2/io/debezium/debezium-storage-jdbc/"
        + version
        + "/"
        + name
    )
    with urllib.request.urlopen(url, timeout=60) as response:
        content = response.read()
    with urllib.request.urlopen(url + ".sha512", timeout=30) as response:
        checksum = response.read().decode().strip().split()[0]
    if (
        not re.fullmatch(r"[0-9a-fA-F]{128}", checksum)
        or hashlib.sha512(content).hexdigest() != checksum.lower()
    ):
        raise ValueError("JDBC storage module checksum mismatch.")
    path = folder / name
    path.write_bytes(content)
    path.chmod(0o644)
    print("Installed matching Debezium JDBC history module: " + version)


if __name__ == "__main__":
    main()
