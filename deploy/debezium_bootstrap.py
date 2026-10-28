"""Run Debezium Engine with native OCI Streaming and workload-principal secrets."""

import json
import os
import signal
import subprocess
import time
from pathlib import Path

import requests

from modules.connector_health import MARKER, summarize
from modules.process_errors import MARKER as ERROR_MARKER, describe
from modules.workload_secrets import WorkloadSecrets


def properties(value):
    raw = value.encode("utf-16-be")
    return "".join("\\u%04x" % int.from_bytes(raw[i:i + 2], "big") for i in range(0, len(raw), 2))


def main():
    os.umask(0o077)
    config = json.loads(os.environ["AUDIT_WORKER_CONFIG"])
    if config.get("runtime_protocol") != 3:
        raise ValueError("Native capture requires runtime protocol 3.")
    secret = WorkloadSecrets(config)
    runtime = Path("/tmp/audit-runtime")
    runtime.mkdir(mode=0o700, exist_ok=True)
    credentials = runtime / "credentials.properties"
    credentials.write_text(
        "source_password=" + properties(secret(config["source_secret_ocid"])) + "\n"
        + "history_password=" + properties(secret(config["history_secret_ocid"])) + "\n"
    )
    credentials.chmod(0o600)
    config["credentials_file"] = str(credentials)
    configuration = runtime / "native-runtime.json"
    configuration.write_text(json.dumps(config))
    configuration.chmod(0o600)
    proc = subprocess.Popen([
        "java", "-Dorg.slf4j.simpleLogger.defaultLogLevel=warn", "-jar",
        "/opt/native/native-capture.jar", str(configuration),
    ])
    signal.signal(signal.SIGTERM, lambda *_: proc.terminate())
    signal.signal(signal.SIGINT, lambda *_: proc.terminate())
    name = config["connector_name"]
    root = "http://127.0.0.1:8083/capture"
    try:
        for _ in range(120):
            if proc.poll() is not None:
                raise RuntimeError("Native capture runtime exited before initialization.")
            try:
                response = requests.get(root, timeout=5)
                if response.ok:
                    break
            except requests.RequestException:
                pass
            time.sleep(2)
        else:
            raise RuntimeError("Native capture did not become ready.")
        if config.get("initial_capture"):
            from modules.initial_capture import bootstrap

            bootstrap(config, secret)
        else:
            response = requests.put(root + "/" + name + "/config", json=config["connector"], timeout=30)
            response.raise_for_status()
        while proc.poll() is None:
            if config.get("capture_control"):
                from modules.capture_runtime import poll

                try:
                    outcome = poll(config, secret)
                    if outcome:
                        print("AUDIT_CAPTURE_REQUEST " + json.dumps(outcome), flush=True)
                except Exception as error:
                    print(ERROR_MARKER + json.dumps(describe(error, "target_connect")), flush=True)
            try:
                response = requests.get(root + "/" + name + "/status", timeout=5)
                response.raise_for_status()
                health = summarize(response.json())
            except (requests.RequestException, ValueError, TypeError):
                health = {"state": "UNAVAILABLE", "tasks": 0, "checked_at": time.time()}
            print(MARKER + json.dumps(health), flush=True)
            time.sleep(15)
        return proc.returncode
    finally:
        if proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=30)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=5)
        credentials.unlink(missing_ok=True)
        configuration.unlink(missing_ok=True)


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:
        print("AUDIT_CAPTURE_BOOTSTRAP_FAILURE type=" + type(error).__name__, flush=True)
        print(ERROR_MARKER + json.dumps(describe(error, "configuration")), flush=True)
        raise SystemExit(1)
