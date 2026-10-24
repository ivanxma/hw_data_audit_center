"""Bounded history writer; target persistence always precedes queue acknowledgement."""

import json
import logging
import os
import signal

from modules.ingestion import BatchSettings, BatchWriter
from modules.mysql_util import open_connection
from modules.queue_service import create_consumer
from modules.workload_secrets import WorkloadSecrets


def main():
    flow = json.loads(os.environ["AUDIT_WORKER_CONFIG"])
    if flow.get("writer_protocol") != 2:
        raise ValueError("Deploy this image with writer protocol 2 configuration.")
    settings = BatchSettings.from_config(flow.get("batch", {}))
    secret = WorkloadSecrets(flow)
    fingerprint_key = secret(flow["fingerprint_secret_ocid"]).encode()
    if not fingerprint_key:
        raise ValueError("Fingerprint secret is empty.")
    target = flow["target"]
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
        consumer = create_consumer(
            flow["queue"], secret, "audit-writer-" + str(flow["id"]), resource_principal=True
        )
        BatchWriter(consumer, db, flow, fingerprint_key, settings).run(lambda: stopping)
    finally:
        if consumer is not None:
            consumer.close()
        db.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    try:
        main()
    except Exception as error:
        # Do not print raw API/DB exceptions, credentials or row images.
        logging.error(
            "history_writer_stopped error_type=%s; review queue, target, retention and source evidence",
            type(error).__name__,
        )
        raise SystemExit(1)
