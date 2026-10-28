import json
import re
import unittest
from unittest.mock import MagicMock, patch

from modules.capture_scope import filters, inventory, matches, overlaps
from modules.events import decode
from modules.mappings import validate_mapping
from modules.target_routes import table_name, write

BASE = {
    "id": 19,
    "source_schema": "sales",
    "source_table": "*",
    "target_schema": "history",
    "target_table": "audit_history",
    "source_identity": "source",
    "lineage_id": "lineage",
    "queue_incarnation": "pool",
}


class ScopeTests(unittest.TestCase):
    def test_database_and_server_scopes_exclude_audit_and_system_schemas(self):
        flow = {**BASE, "source_schema": "*", "source_exclusions": json.dumps(["history"])}
        db, table = filters(flow)
        for schema in ["sales", "warehouse"]:
            self.assertTrue(matches(flow, schema, "orders"))
            self.assertIsNotNone(re.fullmatch(db, schema))
            self.assertIsNotNone(re.fullmatch(table, schema + ".orders"))
        for schema in ["mysql", "sys", "information_schema", "performance_schema", "history"]:
            self.assertFalse(matches(flow, schema, "orders"))
            self.assertIsNone(re.fullmatch(db, schema))
        self.assertFalse(matches(BASE, "warehouse", "orders"))

    def test_literals_are_not_patterns_and_overlaps_are_physical(self):
        flow = {**BASE, "source_schema": "sales$", "source_table": "order$"}
        db, table = filters(flow)
        self.assertIsNotNone(re.fullmatch(table, "sales$.order$"))
        self.assertIsNone(re.fullmatch(table, "sales.orders"))
        self.assertTrue(overlaps(BASE, {**BASE, "source_table": "orders"}))
        self.assertFalse(overlaps(BASE, {**BASE, "source_schema": "other"}))
        self.assertFalse(
            overlaps(
                {**BASE, "source_schema": "*", "source_exclusions": ["history"]},
                {**BASE, "source_schema": "history"},
            )
        )

    def test_multi_table_fingerprints_keep_equal_keys_distinct(self):
        def event(table):
            value = {
                "op": "c",
                "before": None,
                "after": {"id": 1},
                "source": {
                    "db": "sales",
                    "table": table,
                    "file": "binlog.1",
                    "pos": 200,
                    "row": 0,
                    "ts_ms": 1793000000000,
                },
            }
            return decode(json.dumps(value), '{"id":1}', "data", 0, 1, BASE, b"stable")

        a, b = event("orders"), event("customers")
        self.assertNotEqual(a["key_hash"], b["key_hash"])
        self.assertNotEqual(a["event_id"], b["event_id"])
        with self.assertRaises(ValueError):
            decode(
                json.dumps(
                    {"op": "c", "after": {"id": 1}, "source": {"db": "history", "table": "x"}}
                ),
                '{"id":1}',
                "data",
                0,
                1,
                {**BASE, "source_schema": "*", "source_exclusions": ["history"]},
                b"stable",
            )

    def test_grouping_routes_are_stable_distinct_and_valid_identifiers(self):
        f = {**BASE, "target_grouping": "flow"}
        self.assertEqual(table_name(f, "sales", "orders"), "audit_history")
        d = {**f, "target_grouping": "database"}
        self.assertEqual(table_name(d, "sales", "orders"), table_name(d, "sales", "customers"))
        self.assertNotEqual(table_name(d, "sales", "orders"), table_name(d, "other", "orders"))
        t = {**d, "target_grouping": "table", "target_table": "x" * 64}
        self.assertNotEqual(table_name(t, "sales", "orders"), table_name(t, "sales", "customers"))
        name = table_name(t, "sales", "orders")
        self.assertLessEqual(len(name), 64)
        self.assertRegex(name, r"^[A-Za-z_][A-Za-z0-9_$]*$")
        self.assertNotEqual(name, table_name({**t, "id": 20}, "sales", "orders"))

    def test_partial_group_write_cannot_acknowledge_failed_route(self):
        f = {**BASE, "target_grouping": "table"}
        events = [{"source_schema": "sales", "source_table": n} for n in ["orders", "customers"]]
        with (
            patch(
                "modules.target_routes.ensure", side_effect=["orders_history", "customers_history"]
            ),
            patch(
                "modules.events.write_batch", side_effect=[1, RuntimeError("target unavailable")]
            ) as writer,
        ):
            with self.assertRaisesRegex(RuntimeError, "unavailable"):
                write(MagicMock(), f, events)
            self.assertEqual(writer.call_count, 2)
            self.assertEqual(writer.call_args.args[1]["retention_table"], "audit_history")

    def test_overlap_and_feedback_rejected_across_connection_aliases(self):
        row = {
            **BASE,
            "id": 8,
            "source_system": "source",
            "target_system": "target",
            "target_table": "old",
        }
        f = {
            **BASE,
            "source": {"server_identity": "source"},
            "target": {"server_identity": "target"},
            "source_table": "orders",
        }
        with (
            patch("modules.mappings.query", return_value=[row]),
            self.assertRaisesRegex(ValueError, "already registered"),
        ):
            validate_mapping(MagicMock(), f)
        with patch("modules.mappings.query", return_value=[]), self.assertRaises(ValueError):
            validate_mapping(
                MagicMock(),
                {
                    **f,
                    "source_schema": "*",
                    "source_table": "*",
                    "target": {"server_identity": "source"},
                },
            )

    def test_inventory_never_silently_truncates_or_reads_rows(self):
        rows = [
            {"source_schema": "sales", "source_table": "t" + str(i), "has_primary": 1}
            for i in range(3)
        ]
        with patch("modules.repository.query", return_value=rows) as q:
            with self.assertRaisesRegex(ValueError, "1000"):
                inventory(MagicMock(), BASE, maximum=2)
            self.assertNotIn("SELECT *", q.call_args.args[1])
