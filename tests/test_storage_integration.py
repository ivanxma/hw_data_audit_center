"""Opt-in tests on a disposable socket-only MySQL; never use a production account."""

import json
import base64
import hashlib
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

    def fragment_fixture(self, topic="orders", marker="large"):
        from test_message_fragments import framed

        source = {
            "db": "sales",
            "table": "orders",
            "file": "binlog.1",
            "pos": 876,
            "row": 0,
            "ts_ms": 1793000000000,
        }
        value = json.dumps(
            {
                "op": "u",
                "source": source,
                "before": {"id": 1, "state": "pending", "large": "é" * 180000},
                "after": {"id": 1, "state": marker, "large": "界" * 220000},
            },
            ensure_ascii=False,
        ).encode()
        key = b'{"id":1}'
        return key, value, framed(topic, key, value)

    def test_fragment_custody_survives_new_connection_and_out_of_order_parts(self):
        from modules.message_fragments import frame, accept, commit_groups
        from test_message_fragments import framed

        key, value, parts = self.fragment_fixture()
        self.assertGreater(len(parts), 2)
        self.assertIsNone(accept(self.db, self.flow, "orders", 0, 0, frame(parts[1])))
        other, _ = open_connection(self.profile, "root", "")
        try:
            assembled = None
            for offset, index in enumerate([0, *range(2, len(parts))], 1):
                assembled = accept(other, self.flow, "orders", 0, offset, frame(parts[index]))
            self.assertEqual(assembled["key"], key)
            self.assertEqual(assembled["value"], value)
            event = decode(
                value,
                key,
                "orders",
                0,
                len(parts) - 1,
                self.flow,
                b"test-hmac",
                message_id=assembled["message_id"],
            )
            self.assertEqual(write_batch(other, self.flow, [event]), 1)
            commit_groups(other, self.flow, [assembled["group_id"]])
            for offset, raw in enumerate(parts, 100):
                self.assertIsNone(accept(other, self.flow, "orders", 0, offset, frame(raw)))
            changed = json.loads(parts[0])
            payload = bytearray(base64.b64decode(changed["_audit_fragment_v1"]["data"]))
            payload[-1] ^= 1
            changed["_audit_fragment_v1"]["data"] = base64.b64encode(payload).decode()
            changed["_audit_fragment_v1"]["chunk_sha256"] = hashlib.sha256(payload).hexdigest()
            with self.assertRaisesRegex(ValueError, "conflicts"):
                accept(other, self.flow, "orders", 0, 200, frame(json.dumps(changed)))
        finally:
            other.close()
        self.assertEqual(self.count(), 1)
        self.assertEqual(
            query(
                self.db,
                f"SELECT COUNT(*) AS n FROM `{self.schema}`.audit_message_fragments WHERE group_id=%s AND payload IS NOT NULL",
                (assembled["group_id"],),
                one=True,
            )["n"],
            0,
        )

    def test_fragment_completion_write_failure_replays_without_losing_parts(self):
        from modules.ingestion import BatchWriter
        from test_ingestion import message
        from unittest.mock import MagicMock

        key, value, parts = self.fragment_fixture()
        flow = {**self.flow, "topic": "orders", "schema_topic": "schema"}
        consumer = MagicMock()
        writer = BatchWriter(consumer, self.db, flow, b"test-hmac")
        writer.owned = {("orders", 0)}
        for offset, raw in enumerate(parts[:-1]):
            writer.add(message(offset, value=raw))
        writer.flush()
        self.assertEqual(self.count(), 0)
        consumer.acknowledge.reset_mock()
        writer.add(message(len(parts) - 1, value=parts[-1]))
        with patch("modules.ingestion.write_batch", side_effect=RuntimeError("target interrupted")):
            with self.assertRaises(RuntimeError):
                writer.flush()
        consumer.acknowledge.assert_not_called()
        checkpoint = query(
            self.db,
            f"SELECT next_offset FROM `{self.schema}`.audit_delivery_checkpoints WHERE flow_id=%s",
            (flow["id"],),
            one=True,
        )
        self.assertEqual(checkpoint["next_offset"], len(parts) - 1)
        restarted = BatchWriter(consumer, self.db, flow, b"test-hmac")
        restarted.owned = {("orders", 0)}
        restarted.add(message(len(parts) - 1, value=parts[-1]))
        restarted.flush()
        self.assertEqual(self.count(), 1)
        for offset, raw in enumerate(parts, len(parts)):
            restarted.add(message(offset, value=raw))
        restarted.flush()
        self.assertEqual(self.count(), 1)
        row = query(
            self.db,
            f"SELECT before_image,after_image FROM `{self.schema}`.`{flow['target_table']}`",
            one=True,
        )
        self.assertEqual(json.loads(row["after_image"])["large"], "界" * 220000)
        self.assertEqual(json.loads(row["before_image"])["large"], "é" * 180000)

    def test_fragment_receipt_recovery_after_completion_checkpoint(self):
        from modules.message_fragments import frame, accept, note_event_time, recover_committed
        from modules.checkpoints import save

        key, value, parts = self.fragment_fixture()
        assembled = None
        for offset, raw in enumerate(parts):
            assembled = accept(self.db, self.flow, "orders", 0, offset, frame(raw))
        event = decode(
            value,
            key,
            "orders",
            0,
            len(parts) - 1,
            self.flow,
            b"test-hmac",
            message_id=assembled["message_id"],
        )
        note_event_time(self.db, self.flow, assembled["group_id"], event["event_time"])
        write_batch(self.db, self.flow, [event])
        save(self.db, self.flow, {("orders", 0): len(parts)})
        recover_committed(self.db, self.flow)
        row = query(
            self.db,
            f"SELECT state,event_time FROM `{self.schema}`.audit_message_assemblies WHERE group_id=%s",
            (assembled["group_id"],),
            one=True,
        )
        self.assertEqual(row["state"], "committed")
        self.assertEqual(row["event_time"], event["event_time"])
        self.assertIsNone(accept(self.db, self.flow, "orders", 0, 100, frame(parts[0])))

    def test_fragment_pending_budget_and_full_checksum_are_enforced(self):
        from modules.message_fragments import frame, accept

        key, value, parts = self.fragment_fixture()
        policy = {
            **self.flow,
            "message_policy": {"max_event_bytes": 1024 * 1024, "max_pending_bytes": 1024 * 1024},
        }
        self.assertIsNone(accept(self.db, policy, "orders", 0, 0, frame(parts[0])))
        key, value2, parts2 = self.fragment_fixture(marker="different")
        with self.assertRaisesRegex(ValueError, "custody budget"):
            accept(self.db, policy, "orders", 0, 1, frame(parts2[0]))

    def test_grouped_targets_route_replay_and_preserve_cross_table_keys(self):
        from modules.target_routes import write, tables, relation, ensure

        for mode, expected in [("flow", 1), ("database", 2), ("table", 3)]:
            f = {
                **self.flow,
                "id": self.flow["id"] + {"flow": 1, "database": 2, "table": 3}[mode],
                "target_table": self.flow["target_table"] + "_" + mode,
                "lineage_id": self.flow["lineage_id"] + "-" + mode,
                "source_schema": "*",
                "source_table": "*",
                "target_grouping": mode,
            }
            prepare_target(self.db, f)
            events = []
            for pos, (schema, table) in enumerate(
                [("sales", "orders"), ("sales", "customers"), ("warehouse", "orders")], 100
            ):
                payload = {
                    "op": "c",
                    "before": None,
                    "after": {"id": 1, "state": "new"},
                    "source": {
                        "db": schema,
                        "table": table,
                        "file": "binlog.1",
                        "pos": pos,
                        "row": 0,
                        "ts_ms": 1793000000000,
                    },
                }
                events.append(decode(json.dumps(payload), b'{"id":1}', "data", 0, pos, f, b"test"))
            self.assertEqual(write(self.db, f, events), 3)
            self.assertEqual(write(self.db, f, events), 0)
            self.assertEqual(len(tables(self.db, f)), expected)
            self.assertEqual(len({e["key_hash"] for e in events}), 3)
            rows = query(
                self.db,
                "SELECT source_schema,source_table FROM "
                + relation(self.db, f)
                + " ORDER BY source_schema,source_table",
            )
            self.assertEqual(len(rows), 3)

            @contextmanager
            def connection(*args):
                yield self.db

            read_flow = {
                **f,
                "source_id": 7,
                "source": {"name": "Example source", "server_identity": "integration-source"},
                "target": {},
            }
            filters = {"from": "2026-10-25", "to": "2026-10-27", "interval": "day"}
            with patch("modules.audit_service.connect", connection):
                result = browse(read_flow, None, filters)
                self.assertEqual(result["total"], 3)
                narrowed = browse(
                    read_flow,
                    None,
                    {**filters, "source_database": "sales", "source_table_filter": "orders"},
                )
                self.assertEqual(narrowed["total"], 1)
                summary = overview([read_flow], None, filters)
                self.assertEqual(summary["total"], 3)
                self.assertEqual(len(summary["rows"]), 3)
                only_sales = overview([read_flow], None, {**filters, "source_schema": "sales"})
                self.assertEqual(only_sales["total"], 2)
            if mode != "flow":
                self.assertEqual(
                    ensure(self.db, f, "sales", "orders"), ensure(self.db, f, "sales", "orders")
                )
                claims = query(
                    self.db,
                    f"SELECT flow_id FROM `{self.schema}`.audit_target_route_claims WHERE flow_id=%s",
                    (f["id"],),
                )
                self.assertEqual(len(claims), expected)

    def test_generated_target_refuses_unowned_existing_table(self):
        from modules.target_routes import table_name, ensure

        f = {**self.flow, "source_table": "*", "target_grouping": "table"}
        target = table_name(f, "sales", "orders")
        query(self.db, f"CREATE TABLE `{self.schema}`.`{target}` (id INT PRIMARY KEY)")
        with self.assertRaisesRegex(ValueError, "ownership"):
            ensure(self.db, f, "sales", "orders")

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
            .replace(" stream_bindings JSON,\n", "")
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
                "INSERT INTO queue_connections(name,kind,resource_ocid) VALUES ('queue','oci_streaming','pool')",
            )
            flow = query(
                self.db,
                "INSERT INTO flows(name,source_id,target_id,source_schema,source_table,target_schema,target_table,topic,transport,connector_name,queue_connection_id) VALUES ('mapping',%s,%s,'sales','orders','history','orders_history','orders','streaming','capture',%s)",
                (source, target, queue),
            )
            loaded = get_flow(self.db, flow)
            self.assertEqual(loaded["queue_connection"]["id"], queue)
            with self.assertRaises(Exception):
                query(self.db, "DELETE FROM queue_connections WHERE id=%s", (queue,))
        finally:
            query(self.db, "DROP DATABASE IF EXISTS audit_control")
            query(self.db, f"USE `{self.schema}`")
