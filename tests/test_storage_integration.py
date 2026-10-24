"""Opt-in tests on a disposable socket-only MySQL; never use a production account."""

import json
import os
import unittest
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from unittest.mock import patch

from modules.audit_service import browse, overview
from modules.events import decode, write_batch
from modules.heatwave import prepare_target
from modules.mysql_util import open_connection
from modules.partitions import month
from modules.repository import query
from modules.retention import check_ingestion, cleanup
from modules.schema_history import write_schema_event


@unittest.skipUnless(
    os.environ.get("AUDIT_TEST_MYSQL_SOCKET"),
    "Set AUDIT_TEST_MYSQL_SOCKET for disposable MySQL integration",
)
class StorageTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.profile = {"mode": "socket", "socket": os.environ["AUDIT_TEST_MYSQL_SOCKET"]}
        cls.db, _ = open_connection(cls.profile, "root", "")
        if query(cls.db, "SELECT @@skip_networking AS disabled", one=True)["disabled"] != 1:
            cls.db.close()
            raise RuntimeError("Integration server must have networking disabled.")
        cls.schema = "audit_test_" + uuid.uuid4().hex[:16]
        query(cls.db, f"CREATE DATABASE `{cls.schema}`")

    @classmethod
    def tearDownClass(cls):
        query(cls.db, f"DROP DATABASE `{cls.schema}`")
        cls.db.close()

    def setUp(self):
        suffix = uuid.uuid4().hex[:8]
        self.flow = {
            "id": int(suffix, 16),
            "source_identity": "integration-source",
            "lineage_id": suffix,
            "queue_incarnation": "integration-queue",
            "source_schema": "sales",
            "source_table": "orders",
            "target_schema": self.schema,
            "target_table": "history_" + suffix,
        }
        prepare_target(self.db, self.flow)

    def event(self, pos, when=None):
        when = when or datetime.now(timezone.utc).replace(microsecond=0)
        source = {
            "db": "sales",
            "table": "orders",
            "file": "binlog.000001",
            "pos": pos,
            "row": 0,
            "ts_ms": int(when.timestamp() * 1000),
        }
        envelope = {
            "op": "u",
            "source": source,
            "before": {"id": 1, "state": "pending"},
            "after": {"id": 1, "state": "paid"},
        }
        return decode(json.dumps(envelope), b'{"id":1}', "orders", 0, pos, self.flow, b"test-hmac")

    def count(self):
        return query(
            self.db,
            f"SELECT COUNT(*) AS n FROM `{self.schema}`.`{self.flow['target_table']}`",
            one=True,
        )["n"]

    def test_chart_week_boundaries_and_global_range_counts(self):
        sunday = datetime(2026, 10, 18, 23, 59, tzinfo=timezone.utc)
        monday = datetime(2026, 10, 19, 0, 0, tzinfo=timezone.utc)
        write_batch(self.db, self.flow, [self.event(501, sunday), self.event(502, monday)])
        flow = {**self.flow, "source_id": 7, "source": {"name": "Orders"}, "target": {}}

        @contextmanager
        def connection(*args):
            yield self.db

        filters = {"from": "2026-10-18", "to": "2026-10-20", "interval": "week"}
        with patch("modules.audit_service.connect", connection):
            result = browse(flow, None, filters)
            self.assertEqual(result["total"], 2)
            self.assertEqual([r["n"] for r in result["chart"]], [1, 1])
            dates = [
                datetime.fromtimestamp(int(r["bucket"]), timezone.utc).day for r in result["chart"]
            ]
            self.assertEqual(dates, [12, 19])
            summary = overview(
                [flow], None, {**filters, "from": "2026-10-19", "operation": "UPDATE"}
            )
            self.assertEqual(summary["total"], 1)
            self.assertEqual(summary["unavailable"], 0)

    def test_batch_replay_and_conflict_are_atomic(self):
        one, two = self.event(100), self.event(200)
        self.assertEqual(write_batch(self.db, self.flow, [one, two]), 2)
        replay = {**one, "queue_offset": 900, "capture_fingerprint": "f" * 64}
        self.assertEqual(write_batch(self.db, self.flow, [replay, two]), 0)
        self.assertEqual(self.count(), 2)
        conflict = {**one, "after_image": {"id": 1, "state": "unexpected"}}
        with self.assertRaises(ValueError):
            write_batch(self.db, self.flow, [self.event(300), conflict])
        self.assertEqual(self.count(), 2)
        self.assertTrue(self.db.autocommit)

    def test_payload_failure_rolls_back_ledger(self):
        event = self.event(300)
        bad = {**event, "operation": "x" * 100}  # Exceeds VARCHAR(16), after ledger insert.
        with self.assertRaises(Exception):
            write_batch(self.db, self.flow, [bad])
        self.assertIsNone(
            query(
                self.db,
                f"SELECT event_id FROM `{self.schema}`.audit_event_dedup WHERE event_id=%s",
                (event["event_id"],),
                one=True,
            )
        )
        self.assertEqual(write_batch(self.db, self.flow, [event]), 1)

    def test_cleanup_retains_ledger_and_blocks_replay(self):
        today = datetime.now(timezone.utc).date()
        old = datetime.combine(month(today, -5), datetime.min.time(), tzinfo=timezone.utc)
        event = self.event(400, old)
        write_batch(self.db, self.flow, [event])
        policy = {
            "enabled": True,
            "retain_months": 3,
            "legal_hold": False,
            "archive_required": False,
        }
        job_id = cleanup(self.db, self.flow, policy, "integration-test")
        self.assertTrue(job_id)
        self.assertEqual(self.count(), 0)
        self.assertIsNotNone(
            query(
                self.db,
                f"SELECT event_id FROM `{self.schema}`.audit_event_dedup WHERE event_id=%s",
                (event["event_id"],),
                one=True,
            )
        )
        with self.assertRaises(ValueError):
            write_batch(self.db, self.flow, [event])
        self.assertEqual(self.count(), 0)
        self.assertIsNone(cleanup(self.db, self.flow, policy, "integration-test"))

    def test_cleanup_resumes_frozen_manifest_after_ddl_failure(self):
        policy = {
            "enabled": True,
            "retain_months": 3,
            "legal_hold": False,
            "archive_required": False,
        }
        real_query = query

        def fail_ddl(db, sql, params=(), one=False):
            if "DROP PARTITION" in sql:
                raise RuntimeError("simulated metadata lock timeout")
            return real_query(db, sql, params, one)

        with (
            patch("modules.retention.query", side_effect=fail_ddl),
            self.assertRaises(RuntimeError),
        ):
            cleanup(self.db, self.flow, policy, "integration-test")
        job = query(
            self.db,
            f"SELECT * FROM `{self.schema}`.audit_cleanup_jobs WHERE flow_id=%s",
            (self.flow["id"],),
            one=True,
        )
        self.assertEqual(job["status"], "failed")
        self.assertEqual(cleanup(self.db, self.flow, policy, "integration-test"), job["job_id"])
        done = query(
            self.db,
            f"SELECT * FROM `{self.schema}`.audit_cleanup_jobs WHERE job_id=%s",
            (job["job_id"],),
            one=True,
        )
        self.assertEqual(done["status"], "complete")
        self.assertEqual(json.loads(done["plan"]), json.loads(job["plan"]))

    def test_schema_replay_at_new_offset_and_out_of_order_fail(self):
        source = {"file": "binlog.000001", "pos": 100}
        envelope = {
            "databaseName": "sales",
            "source": source,
            "ddl": "ALTER TABLE orders ADD x INT",
            "tableChanges": [
                {
                    "id": "sales.orders",
                    "type": "ALTER",
                    "table": {
                        "columns": [
                            {"name": "id", "typeName": "INT"},
                            {"name": "x", "typeName": "INT"},
                        ]
                    },
                }
            ],
        }
        raw = json.dumps(envelope)
        write_schema_event(self.db, self.flow, raw, "schema", 0, 10)
        write_schema_event(self.db, self.flow, raw, "schema", 0, 11)
        result = query(
            self.db,
            f"SELECT COUNT(*) AS n FROM `{self.schema}`.audit_schema_changes WHERE flow_id=%s",
            (self.flow["id"],),
            one=True,
        )
        self.assertEqual(result["n"], 1)
        envelope["source"]["pos"] = 200
        with self.assertRaises(ValueError):
            write_schema_event(self.db, self.flow, json.dumps(envelope), "schema", 0, 9)

    def test_snapshot_schema_reset_create_replay_and_legacy_repair(self):
        from modules.fingerprints import digest

        source = {"file": "binlog.000001", "pos": 198, "snapshot": "true"}
        envelope = {
            "databaseName": "sales",
            "source": source,
            "ddl": "DROP TABLE orders",
            "tableChanges": [{"id": "sales.orders", "type": "DROP", "table": None}],
        }
        raw_drop = json.dumps(envelope)
        write_schema_event(self.db, self.flow, raw_drop, "schema", 0, 1)
        # Reproduce the old application's persisted snapshot identity/label.
        legacy = digest(
            [
                self.flow["lineage_id"],
                self.flow["source_identity"],
                source["file"],
                source["pos"],
                None,
                "sales.orders",
                0,
            ]
        )
        query(
            self.db,
            f"UPDATE `{self.schema}`.audit_schema_changes SET event_id=%s,"
            "change_kind='DROP',fidelity='baseline' WHERE flow_id=%s",
            (legacy, self.flow["id"]),
        )
        write_schema_event(self.db, self.flow, raw_drop, "schema", 0, 1)
        envelope["ddl"] = "CREATE TABLE orders (id INT)"
        envelope["tableChanges"] = [
            {
                "id": "sales.orders",
                "type": "CREATE",
                "table": {"columns": [{"name": "id", "typeName": "INT"}]},
            }
        ]
        raw_create = json.dumps(envelope)
        write_schema_event(self.db, self.flow, raw_create, "schema", 0, 7)
        # A target commit followed by a missing queue ack must remain replayable.
        write_schema_event(self.db, self.flow, raw_drop, "schema", 0, 1)
        write_schema_event(self.db, self.flow, raw_create, "schema", 0, 7)
        rows = query(
            self.db,
            f"SELECT change_kind,fidelity FROM `{self.schema}`.audit_schema_changes "
            "WHERE flow_id=%s ORDER BY change_kind",
            (self.flow["id"],),
        )
        self.assertEqual(
            rows,
            [
                {"change_kind": "SNAPSHOT", "fidelity": "baseline"},
                {"change_kind": "SNAPSHOT_RESET", "fidelity": "snapshot_reset"},
            ],
        )
        envelope["tableChanges"][0]["table"]["columns"][0]["typeName"] = "VARCHAR"
        with self.assertRaisesRegex(ValueError, "conflict"):
            write_schema_event(self.db, self.flow, json.dumps(envelope), "schema", 0, 7)

    def test_capture_preview_requires_confirmation_and_is_consumed_once(self):
        from modules import capture_requests

        flow = {**self.flow, "connector_name": "integration-capture"}
        plan = {"partition": {"server": "audit_1"}, "offset": {"gtids": "example"}}
        rid = capture_requests.enqueue(
            self.db, flow, {"mode": "gtid"}, plan, "operator", preview=True
        )
        self.assertIsNone(
            capture_requests.pending(self.db, flow["id"], flow["connector_name"], self.schema)
        )
        with self.assertRaises(ValueError):
            capture_requests.confirm(self.db, flow, rid, "other-user", lambda p: None)
        seen = []
        capture_requests.confirm(self.db, flow, rid, "operator", lambda p: seen.append(p))
        self.assertEqual(seen, [plan])
        self.assertEqual(
            capture_requests.pending(self.db, flow["id"], flow["connector_name"], self.schema)[
                "request_id"
            ],
            rid,
        )
        with self.assertRaises(ValueError):
            capture_requests.confirm(self.db, flow, rid, "operator", lambda p: None)

    def test_expiry_gate_waits_for_existing_writer_transaction(self):
        # FOR SHARE protects a batch from a cutoff being activated during its commit.
        other, _ = open_connection(self.profile, "root", "")
        try:
            self.db.autocommit = False
            check_ingestion(self.db, self.flow, [self.event(500)])
            query(other, "SET SESSION innodb_lock_wait_timeout=1")
            with self.assertRaises(Exception):
                query(
                    other,
                    f"UPDATE `{self.schema}`.audit_retention_state SET expiry_cutoff=%s WHERE flow_id=%s",
                    ("2026-01-01", self.flow["id"]),
                )
        finally:
            self.db.rollback()
            self.db.autocommit = True
            other.close()

    def test_connection_migration_preserves_fingerprint_namespace(self):
        from modules.repository import migrate_connections

        query(self.db, f"USE `{self.schema}`")
        query(
            self.db,
            "CREATE TABLE connections (id BIGINT PRIMARY KEY, db_system_ocid VARCHAR(255) NOT NULL, ssl_ca VARCHAR(500), ssl_mode VARCHAR(30))",
        )
        query(
            self.db,
            "INSERT INTO connections VALUES (1,'legacy-source-identity','','VERIFY_IDENTITY')",
        )
        migrate_connections(self.db)
        self.assertEqual(
            query(self.db, "SELECT server_identity FROM connections", one=True)["server_identity"],
            "legacy-source-identity",
        )
        columns = query(self.db, "SHOW COLUMNS FROM connections")
        self.assertEqual({c["Field"] for c in columns}, {"id", "server_identity"})
        migrate_connections(self.db)
        self.assertEqual(query(self.db, "SELECT COUNT(*) AS n FROM connections", one=True)["n"], 1)

    def test_queue_resource_migration_and_foreign_key(self):
        from modules.config import ROOT
        from modules.repository import get_flow, migrate_resources

        if query(
            self.db,
            "SELECT SCHEMA_NAME AS name FROM information_schema.SCHEMATA WHERE SCHEMA_NAME='audit_control'",
            one=True,
        ):
            self.skipTest("Disposable server already has a control schema; refuse to overwrite it.")
        old_sql = (
            (ROOT / "sql/repository.sql")
            .read_text()
            .replace(" queue_connection_id BIGINT,\n", "")
            .replace(" connect_harness_id VARCHAR(255),\n", "")
            .replace(
                " CONSTRAINT queue_connection_ref FOREIGN KEY (queue_connection_id) REFERENCES queue_connections(id),\n",
                "",
            )
        )
        try:
            for statement in old_sql.split(";"):
                if statement.strip():
                    query(self.db, statement)
            migrate_resources(self.db)
            migrate_resources(self.db)
            source = query(
                self.db,
                "INSERT INTO connections(name,role,host,username,database_name,secret_ocid,server_identity) VALUES ('source','source','source.example.invalid','reader','sales','secret','source')",
            )
            target = query(
                self.db,
                "INSERT INTO connections(name,role,host,username,database_name,secret_ocid,server_identity) VALUES ('target','target','target.example.invalid','writer','history','secret','target')",
            )
            queue = query(
                self.db,
                "INSERT INTO queue_connections(name,kind,resource_ocid,username,secret_ocid) VALUES ('queue','oci_streaming','pool','user','secret')",
            )
            flow = query(
                self.db,
                "INSERT INTO flows(name,source_id,target_id,source_schema,source_table,target_schema,target_table,topic,transport,connector_name,queue_connection_id,connect_harness_id) VALUES ('mapping',%s,%s,'sales','orders','history','orders_history','orders','streaming','capture',%s,'harness')",
                (source, target, queue),
            )
            loaded = get_flow(self.db, flow)
            self.assertEqual(loaded["queue_connection"]["id"], queue)
            with self.assertRaises(Exception):
                query(self.db, "DELETE FROM queue_connections WHERE id=%s", (queue,))
        finally:
            query(self.db, "DROP DATABASE IF EXISTS audit_control")
            query(self.db, f"USE `{self.schema}`")
