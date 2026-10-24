"""Resolve Vault references only inside the workload; Connect persists file references."""

import json
import os
import subprocess
import time
from pathlib import Path

import requests

from modules.connector_health import MARKER, summarize
from modules.process_errors import MARKER as ERROR_MARKER
from modules.process_errors import describe
from modules.transport import (
    backend,
    jaas,
    mechanism,
    principal_jaas,
    producer_settings,
    state_topics,
    uses_principal,
)
from modules.workload_secrets import WorkloadSecrets


def properties(value):
    # Java properties operate on UTF-16 code units; escape spaces and non-ASCII too.
    units = value.encode("utf-16-be")
    return "".join(
        "\\u%04x" % int.from_bytes(units[i : i + 2], "big") for i in range(0, len(units), 2)
    )


def main():
    config = json.loads(os.environ["AUDIT_WORKER_CONFIG"])
    secret = WorkloadSecrets(config)
    q = config["queue"]
    jaas_value = (
        principal_jaas(q) if uses_principal(q) else jaas(q, q["username"], secret(q["secret_ocid"]))
    )
    topics = state_topics(q, config["connector_name"])
    runtime = Path("/tmp/audit-runtime")
    runtime.mkdir(mode=0o700, exist_ok=True)
    path = runtime / "credentials.properties"
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w") as f:
        f.write("source_password=" + properties(secret(config["source_secret_ocid"])) + "\n")
        f.write("jaas=" + properties(jaas_value) + "\n")
        if backend(q) == "oci_streaming":
            f.write("history_password=" + properties(secret(config["history_secret_ocid"])) + "\n")
    name = config["connector_name"]
    env = dict(os.environ)
    if uses_principal(q):
        # Append Oracle's authentication provider to the Connect worker classpath.
        env["CLASSPATH"] = "/opt/streaming-libs/*" + (
            ":" + env["CLASSPATH"] if env.get("CLASSPATH") else ""
        )
    env.update(
        {
            "CONNECT_REST_HOST_NAME": "127.0.0.1",
            "CONNECT_LISTENERS": "http://127.0.0.1:8083",
            "ADVERTISED_HOST_NAME": "127.0.0.1",
            "BOOTSTRAP_SERVERS": q["bootstrap_servers"],
            "GROUP_ID": name,
            "CONFIG_STORAGE_TOPIC": topics["config"],
            "OFFSET_STORAGE_TOPIC": topics["offset"],
            "STATUS_STORAGE_TOPIC": topics["status"],
            "CONNECT_CONFIG_PROVIDERS": "file",
            "CONNECT_CONFIG_PROVIDERS_FILE_CLASS": "org.apache.kafka.common.config.provider.FileConfigProvider",
            "CONNECT_SECURITY_PROTOCOL": "SASL_SSL",
            "CONNECT_SASL_MECHANISM": mechanism(q),
            "CONNECT_SASL_JAAS_CONFIG": "${file:/tmp/audit-runtime/credentials.properties:jaas}",
            "CONNECT_PRODUCER_SECURITY_PROTOCOL": "SASL_SSL",
            "CONNECT_PRODUCER_SASL_MECHANISM": mechanism(q),
            "CONNECT_PRODUCER_SASL_JAAS_CONFIG": "${file:/tmp/audit-runtime/credentials.properties:jaas}",
            "CONNECT_CONSUMER_SECURITY_PROTOCOL": "SASL_SSL",
            "CONNECT_CONSUMER_SASL_MECHANISM": mechanism(q),
            "CONNECT_CONSUMER_SASL_JAAS_CONFIG": "${file:/tmp/audit-runtime/credentials.properties:jaas}",
            "CONFIG_STORAGE_REPLICATION_FACTOR": str(q.get("replication_factor", 3)),
            "OFFSET_STORAGE_REPLICATION_FACTOR": str(q.get("replication_factor", 3)),
            "STATUS_STORAGE_REPLICATION_FACTOR": str(q.get("replication_factor", 3)),
            "CONNECT_TOPIC_CREATION_ENABLE": "false",
            "CONNECT_EXACTLY_ONCE_SOURCE_SUPPORT": "disabled",
        }
    )
    for key, value in producer_settings(q).items():
        env["CONNECT_PRODUCER_" + key.upper().replace(".", "_")] = value
    proc = subprocess.Popen(["/docker-entrypoint.sh", "start"], env=env)
    import signal

    def stop(*_):
        proc.terminate()

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    try:
        for _ in range(120):
            if proc.poll() is not None:
                raise RuntimeError("Connect exited.")
            try:
                response = requests.put(
                    "http://127.0.0.1:8083/connectors/" + name + "/config",
                    json=config["connector"],
                    timeout=5,
                )
                if response.ok:
                    break
            except requests.RequestException:
                pass
            time.sleep(2)
        else:
            raise RuntimeError("Connect did not become ready.")
        while proc.poll() is None:
            if config.get("capture_control"):
                from modules.capture_runtime import poll

                try:
                    outcome = poll(config, secret)
                    if outcome:
                        print("AUDIT_CAPTURE_REQUEST " + json.dumps(outcome), flush=True)
                except Exception as error:
                    print(ERROR_MARKER + json.dumps(describe(error, "target_connect")), flush=True)
                    print(
                        "AUDIT_CAPTURE_CONTROL unavailable; pending requests have not been acknowledged.",
                        flush=True,
                    )
            try:
                status = requests.get(
                    "http://127.0.0.1:8083/connectors/" + name + "/status", timeout=5
                )
                status.raise_for_status()
                health = summarize(status.json())
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
        path.unlink(missing_ok=True)


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:
        print(ERROR_MARKER + json.dumps(describe(error, "configuration")), flush=True)
        print(
            "Debezium startup failed. Check workload identity, Vault permissions, queue topics and source settings.",
            flush=True,
        )
        raise SystemExit(1)
