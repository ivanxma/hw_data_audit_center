"""Exercise real MySQL partitioning and deduplication in a disposable local validation schema."""

import getpass
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from modules.config import local_profile  # noqa: E402 - standalone script resolves its own checkout
from modules.events import decode, write_event  # noqa: E402 - standalone script resolves its own checkout
from modules.heatwave import prepare_target  # noqa: E402 - standalone script resolves its own checkout
from modules.mysql_util import open_connection  # noqa: E402 - standalone script resolves its own checkout
from modules.partitions import list_partitions, prepare  # noqa: E402 - standalone script resolves its own checkout
from modules.repository import query  # noqa: E402 - standalone script resolves its own checkout
from modules.schema_history import write_schema_event  # noqa: E402 - standalone script resolves its own checkout


def main():
    password = getpass.getpass("Local administrator password for storage verification: ")
    db, _ = open_connection(local_profile(), "localadmin", password)
    schema = "audit_storage_validation"
    # Only app-created scratch objects are removed by this check.
    if query(
        db,
        "SELECT SCHEMA_NAME AS name FROM information_schema.SCHEMATA WHERE SCHEMA_NAME=%s",
        (schema,),
    ):
        raise SystemExit("Validation schema exists; refusing to overwrite it.")
    query(db, "CREATE DATABASE audit_storage_validation")
    try:
        flow = {
            "id": 1,
            "source_schema": "sales",
            "source_table": "orders",
            "target_schema": schema,
            "target_table": "history",
            "lineage_id": "validation-lineage",
            "source_identity": "validation-source",
            "queue_incarnation": "validation-queue",
        }
        prepare_target(db, flow)
        source = {
            "db": "sales",
            "table": "orders",
            "ts_ms": int(datetime.now(timezone.utc).timestamp() * 1000),
            "file": "binlog.000001",
            "pos": 100,
            "row": 0,
        }
        payload = {
            "op": "u",
            "source": source,
            "before": {"id": 1042, "state": "pending"},
            "after": {"id": 1042, "state": "paid"},
        }
        first = decode(
            json.dumps(payload),
            b'{"id":1042}',
            "audit.orders",
            0,
            10,
            flow,
            b"local-validation-key",
        )
        duplicate = decode(
            json.dumps(payload),
            b'{"id":1042}',
            "audit.orders",
            0,
            11,
            flow,
            b"local-validation-key",
        )
        write_event(db, flow, first)
        write_event(db, flow, duplicate)
        assert (
            query(db, "SELECT COUNT(*) AS n FROM audit_storage_validation.history", one=True)["n"]
            == 1
        )
        row = query(
            db,
            "SELECT before_image,after_image FROM audit_storage_validation.history WHERE key_hash=%s",
            (first["key_hash"],),
            one=True,
        )
        assert json.loads(row["before_image"])["state"] == "pending"
        assert json.loads(row["after_image"])["state"] == "paid"
        schema_event = {
            "databaseName": "sales",
            "ddl": "ALTER TABLE sales.orders MODIFY state VARCHAR(50)",
            "source": source,
            "tableChanges": [
                {
                    "id": "sales.orders",
                    "type": "ALTER",
                    "table": {
                        "columns": [
                            {"name": "id", "typeName": "INT"},
                            {"name": "state", "typeName": "VARCHAR", "length": 50},
                        ]
                    },
                }
            ],
        }
        write_schema_event(db, flow, json.dumps(schema_event), "audit_1", 0, 1)
        schema_event["source"] = {**source, "pos": 200}
        schema_event["tableChanges"][0]["table"]["columns"][1]["length"] = 100
        write_schema_event(db, flow, json.dumps(schema_event), "audit_1", 0, 2)
        assert (
            query(
                db,
                "SELECT COUNT(*) AS n FROM audit_storage_validation.audit_schema_changes",
                one=True,
            )["n"]
            == 2
        )
        parts = list_partitions(db, schema, "history")
        assert any(p["partition_name"] == "p_future" for p in parts)
        assert prepare(db, schema, "history") == []
        print(
            "Real MySQL checks passed: monthly partitions, indexed key lookup, complete images, new-offset source deduplication and semantic schema history."
        )
    finally:
        query(db, "DROP DATABASE audit_storage_validation")
        db.close()


if __name__ == "__main__":
    main()
