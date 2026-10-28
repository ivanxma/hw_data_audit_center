"""Bounded history writer; target persistence always precedes queue acknowledgement."""

import json
import logging
import os
import signal

from modules.ingestion import BatchSettings, BatchWriter
from modules.mysql_util import open_connection
from modules.process_errors import MARKER, describe
from modules.native_streaming import NativeConsumer
from modules.repository import named_lock
from modules.workload_secrets import WorkloadSecrets


def run(status):
    flow = json.loads(os.environ["AUDIT_WORKER_CONFIG"])
    if flow.get("writer_protocol") != 3:
        raise ValueError("Deploy this image with writer protocol 3 configuration.")
    settings = BatchSettings.from_config(flow.get("batch", {}))
    status["stage"] = "secrets"
    secret = WorkloadSecrets(flow)
    fingerprint_key = secret(flow["fingerprint_secret_ocid"]).encode()
    if not fingerprint_key:
        raise ValueError("Fingerprint secret is empty.")
    target = flow["target"]
    status["stage"] = "target_connect"
    db, _ = open_connection(
        {
            "mode": "tcp",
            "host": target["host"],
            "port": target["port"],
            "database": flow["target_schema"],
            "ssl_mode": target["ssl_mode"],
            "ssl_ca": target.get("ssl_ca"),
            "read_timeout": 30,
            "write_timeout": 30,
        },
        target["username"],
        secret(target["secret_ocid"]),
    )
    consumer = None
    stopping = False

    def stop(*_):
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    try:
        status["stage"] = "queue_connect"
        with named_lock(db, "writer", flow["target_schema"], flow["id"]):
            from modules.message_fragments import initialize, recover_committed

            initialize(db, flow["target_schema"])
            recover_committed(db, flow)
            consumer = NativeConsumer(
                flow["queue"], resource_principal=True, poll_policy=flow.get("message_policy")
            )
            writer = BatchWriter(consumer, db, flow, fingerprint_key, settings)
            status["writer"] = writer
            writer.run(lambda: stopping)
    finally:
        if consumer is not None:
            consumer.close()
        db.close()


def main():
    status = {"stage": "configuration"}
    try:
        run(status)
    except Exception as error:
        writer = status.get("writer")
        stage = writer.stage if writer else status["stage"]
        logging.error("%s%s", MARKER, json.dumps(describe(error, stage)))
        raise


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    try:
        main()
    except Exception:
        # Diagnostic text comes only from safe codes; never print raw exceptions.
        raise SystemExit(1)
