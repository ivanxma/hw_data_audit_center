import unittest
from contextlib import contextmanager
from datetime import datetime
from unittest.mock import MagicMock, patch

from modules.audit_service import chart_window, overview


class AuditChartTests(unittest.TestCase):
    def test_custom_ranges_and_minute_hour_window(self):
        start, end, interval = chart_window(
            {"from": "2026-01-01", "to": "2026-10-24", "interval": "day"}
        )
        self.assertEqual(start, datetime(2026, 1, 1))
        self.assertEqual(interval, 86400)
        self.assertEqual(end, datetime(2026, 10, 24))
        start, end, interval = chart_window({"interval": "minute"})
        self.assertEqual((end - start).total_seconds(), 3600)
        self.assertEqual(interval, 60)
        with self.assertRaisesRegex(ValueError, "one hour"):
            chart_window({"from": "2026-10-23", "to": "2026-10-24", "interval": "minute"})
        with self.assertRaisesRegex(ValueError, "1,000"):
            chart_window({"from": "2020-01-01", "to": "2026-01-01", "interval": "hour"})
        self.assertEqual(chart_window({"interval": "week"})[2], 604800)

    def test_global_failures_remain_unknown_and_filters_are_parameterized(self):
        flows = [
            {
                "id": n,
                "source_id": 2,
                "source": {"name": "Orders"},
                "source_schema": "sales",
                "source_table": "orders",
                "target": {"database_name": "audit"},
                "target_schema": "audit",
                "target_table": "history",
            }
            for n in [1, 2]
        ]

        @contextmanager
        def connect(*args):
            yield MagicMock()

        with (
            patch("modules.audit_service.connect", connect),
            patch(
                "modules.audit_service.query", side_effect=[{"n": 4}, RuntimeError("private-value")]
            ) as query,
        ):
            result = overview(
                flows,
                MagicMock(),
                {"from": "2026-01-01", "to": "2026-10-24", "operation": "UPDATE"},
            )
        self.assertEqual(result["total"], 4)
        self.assertEqual(result["unavailable"], 1)
        self.assertIsNone(result["rows"][1]["count"])
        self.assertNotIn("private-value", str(result))
        self.assertEqual(query.call_args.args[2][-1], "UPDATE")
        self.assertNotIn("UPDATE", query.call_args.args[1])
