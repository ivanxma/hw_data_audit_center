import unittest
from unittest.mock import MagicMock, patch

from modules.binlog_position import ExecutedSetBoundary, TransactionBoundary, option_value
from modules.capture_history import baseline_record
from modules.capture_start import gtid_set, selection, utc_datetime, validate_gtid_boundary

SID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"


class CaptureStartTests(unittest.TestCase):
    def test_gtid_intervals_are_canonical_and_not_source_filters(self):
        self.assertEqual(gtid_set(SID.upper() + ":5-9:1-3:4," + SID + ":11"), SID + ":1-9:11")
        for value in [
            SID,
            SID + ":0",
            SID + ":9-2",
            SID + ":tag:1",
            SID + ":1;DROP TABLE x",
            SID + ":9223372036854775808",
        ]:
            with self.subTest(value=value), self.assertRaises(ValueError):
                gtid_set(value)

    def test_choices_and_utc_are_explicit(self):
        self.assertEqual(selection(), {"mode": "start"})
        self.assertEqual(selection({"mode": "end", "value": "ignored"}), {"mode": "end"})
        self.assertEqual(
            selection({"mode": "datetime", "value": "2026-10-24T10:00:00+01:00"})["value"],
            "2026-10-24T09:00:00+00:00",
        )
        with self.assertRaises(ValueError):
            selection({"mode": "arbitrary"})

    def test_datetime_resolves_before_the_first_matching_transaction(self):
        baseline = {"gtids": SID + ":1-11"}
        parser = TransactionBoundary(baseline, utc_datetime("2026-10-24T09:00:00Z"))
        boundary = int(utc_datetime("2026-10-24T09:00:00Z").timestamp() * 1_000_000)
        for pos, transaction, timestamp in [(198, 12, boundary - 1), (400, 13, boundary)]:
            for line in [
                f"# at {pos}",
                f"# immediate_commit_timestamp={timestamp}",
                f"SET @@SESSION.GTID_NEXT= '{SID}:{transaction}'/*!*/;",
            ]:
                parser.line(line, "binlog.000001")
        self.assertEqual(parser.found["gtids"], SID + ":1-12")
        self.assertEqual(parser.found["pos"], 400)
        self.assertEqual(parser.found["transaction"], SID + ":13")

    def test_unsupported_timestamp_fails_instead_of_approximating(self):
        parser = TransactionBoundary({"gtids": SID + ":1"}, utc_datetime("2026-10-24T09:00:00Z"))
        parser.line("# at 198", "binlog.000001")
        with self.assertRaisesRegex(ValueError, "timestamp"):
            parser.line(f"SET @@SESSION.GTID_NEXT= '{SID}:2'/*!*/;", "binlog.000001")

    def test_gtid_boundary_must_include_intervening_transactions(self):
        for desired, matches in [(SID + ":1-13", True), (SID + ":1-11:13", False)]:
            parser = ExecutedSetBoundary({"gtids": SID + ":1-11"}, desired)
            for number in [12, 13]:
                for line in [
                    f"# at {number * 100}",
                    "# immediate_commit_timestamp=1792831473000000",
                    f"SET @@SESSION.GTID_NEXT= '{SID}:{number}'/*!*/;",
                ]:
                    parser.line(line, "binlog.000001")
            self.assertEqual(bool(parser.found), matches)

    def test_schema_baseline_must_match_table_source_and_snapshot(self):
        flow = {"id": 1, "source_schema": "sales", "source_table": "orders"}
        event = {
            "source": {"server": "audit_1"},
            "position": {
                "snapshot": "INITIAL",
                "file": "binlog.000001",
                "pos": 198,
                "gtids": SID + ":1-11",
                "ts_sec": 1,
            },
            "tableChanges": [{"id": '"sales"."orders"', "type": "DROP", "table": None}],
        }
        with self.assertRaises(ValueError):
            baseline_record([event], flow)
        event["tableChanges"][0].update(type="CREATE", table={"columns": [{"name": "id"}]})
        self.assertEqual(baseline_record([event], flow)["pos"], 198)
        event["source"]["server"] = "other"
        with self.assertRaises(ValueError):
            baseline_record([event], flow)

    def test_purged_future_and_uncovered_gtids_are_rejected(self):
        for failed in ["executed", "retained", "covered"]:
            flags = {"executed": 1, "retained": 1, "covered": 1, failed: 0}
            with (
                patch("modules.repository.query", return_value=flags),
                self.assertRaises(ValueError),
            ):
                validate_gtid_boundary(MagicMock(), SID + ":1-20", SID + ":1-11")

    def test_defaults_file_values_preserve_special_characters(self):
        self.assertEqual(option_value('a"b\\c\nd'), '"a\\"b\\\\c\\nd"')
