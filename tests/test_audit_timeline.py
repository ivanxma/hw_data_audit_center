import unittest
from datetime import datetime
from unittest.mock import MagicMock, patch

import test_core

from modules.audit_timeline import browse


class TimelineValidationTests(unittest.TestCase):
    def test_invalid_kind_and_sort_order_are_rejected_before_database_access(self):
        for filters in ({"event_kind": "arbitrary"}, {"direction": "asc; drop table"}):
            with self.subTest(filters=filters), self.assertRaises(ValueError):
                browse({}, None, filters)


class TimelineWebTests(unittest.TestCase):
    setUp = test_core.WebTests.setUp
    auth = test_core.WebTests.auth

    def render(self, export=False):
        self.auth()
        flow = {"id": 4, "source_name": "Source", "source_schema": "sales", "source_table": "*"}
        event = {
            "event_id": "event-schema",
            "event_kind": "schema",
            "event_time": datetime(2026, 10, 28, 12),
            "source_schema": "sales",
            "source_table": "orders",
            "operation": "DDL_CREATE",
            "operation_label": "CREATE TABLE",
            "time_basis": "source",
            "ddl": "CREATE TABLE orders (id INT) /* <script>untrusted</script> */",
            "fidelity": "baseline",
            "before_definition": None,
            "after_definition": {"columns": {}},
            "semantic_delta": [],
            "source_metadata": {"file": "binlog.1", "pos": 100, "gtid": "fixture"},
            "topic": "audit_4",
            "partition_id": 0,
            "queue_offset": 1,
        }
        result = {
            "event_kind": "all",
            "direction": "asc",
            "rows": [event],
            "total": 1,
            "page": 1,
            "counts": [{"operation": "DDL_CREATE", "n": 1}],
            "chart": [],
            "interval": 3600,
            "from": "2026-10-28T00:00:00",
            "to": "2026-10-29T00:00:00",
        }
        with (
            patch.object(self.mod.CONNECTIONS, "borrow", return_value=MagicMock()),
            patch.object(self.mod.repo, "flows", return_value=[flow]),
            patch.object(self.mod.repo, "get_flow", return_value=flow),
            patch.object(self.mod, "Cloud"),
            patch.object(self.mod, "browse_timeline", return_value=result),
        ):
            return self.client.get("/audit?flow_id=4" + ("&export=csv" if export else ""))

    def test_combined_view_has_kind_filters_source_coordinates_and_escaped_ddl(self):
        response = self.render()
        self.assertEqual(response.status_code, 200)
        text = response.get_data(as_text=True)
        for value in [
            "All changes",
            "Schema changes only",
            "Row changes only",
            "Oldest first",
            "Newest first",
            "CREATE TABLE",
            "Schema details",
            "Before definition",
            "GTID: fixture",
        ]:
            self.assertIn(value, text)
        self.assertIn("No earlier definition has been recorded", text)
        self.assertNotIn("This is a capture-initialization observation", text)
        self.assertNotIn("<script>untrusted</script>", text)
        self.assertIn("&lt;script&gt;untrusted&lt;/script&gt;", text)

    def test_csv_includes_schema_statement_and_definition_fields(self):
        response = self.render(export=True)
        self.assertEqual(response.status_code, 200)
        text = response.get_data(as_text=True)
        self.assertIn(
            "ddl,before_definition,after_definition,semantic_delta,source_metadata,time_basis", text
        )
        self.assertIn("DDL_CREATE", text)
        self.assertIn("CREATE TABLE orders", text)
