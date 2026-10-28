import re
import unittest
from unittest.mock import MagicMock, patch

from modules.capture_start import gtid_set
from modules.gtid_position import locate_gtid

A = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
B = "bbbbbbbb-cccc-dddd-eeee-ffffffffffff"


def members(value):
    result = set()
    if not value:
        return result
    for group in value.split(","):
        sid, *intervals = group.split(":")
        for interval in intervals:
            bounds = [int(n) for n in interval.split("-")]
            result.update((sid.lower(), n) for n in range(bounds[0], bounds[-1] + 1))
    return result


def event(pos, kind, info="", length=10):
    return {"Pos": pos, "End_log_pos": pos + length, "Event_type": kind, "Info": info}


class Binlogs:
    def __init__(self, files, purged="", executed=None):
        self.logs = files
        self.purged = purged
        self.executed = executed or A + ":1-1000"
        self.calls = []

    def query(self, db, sql, params=(), one=False):
        self.calls.append((sql, params))
        if "AS purged" in sql:
            return {"purged": self.purged, "executed": self.executed}
        if "AS retained" in sql:
            requested, repeated, baseline, covered = params
            return {
                "executed": int(members(requested) <= members(self.executed)),
                "retained": int(members(self.purged) <= members(repeated)),
                "covered": int(members(baseline) <= members(covered)),
            }
        if "GTID_SUBSET" in sql:
            return {"covered": int(members(params[0]) <= members(params[1]))}
        if sql == "SHOW BINARY LOGS":
            return [{"Log_name": name, "File_size": rows[-1]["End_log_pos"]} for name, rows in self.logs.items()]
        if sql.startswith("SHOW BINLOG EVENTS"):
            name = re.search(r"IN '([^']+)'", sql)[1]
            start = re.search(r" FROM ([0-9]+)", sql)
            limit = int(re.search(r"LIMIT ([0-9]+)", sql)[1])
            rows = self.logs[name]
            return [r for r in rows if not start or r["Pos"] >= int(start[1])][:limit]
        raise AssertionError(sql)


class GtidPositionTests(unittest.TestCase):
    def locate(self, logs, requested, **kwargs):
        with patch("modules.gtid_position.query", side_effect=logs.query), patch(
            "modules.repository.query", side_effect=logs.query
        ):
            return locate_gtid(MagicMock(), requested, **kwargs)

    def test_more_than_128_retained_files_uses_headers_near_requested_gtid(self):
        logs = Binlogs({
            f"binlog.{n:06}": [event(4, "Previous_gtids", A + f":1-{n}")]
            for n in range(1, 301)
        })
        result = self.locate(logs, A + ":1-32", baseline={"gtids": A + ":1-10"})
        self.assertEqual(result["file"], "binlog.000032")
        self.assertLess(len(logs.calls), 30)
        self.assertFalse(any(" FROM " in sql for sql, _ in logs.calls))

    def test_multiple_uuid_groups_and_intervals_match_cumulative_commit_boundary(self):
        prior = A + ":1-10:20-25," + B + ":1-4"
        wanted = gtid_set(prior + "," + B + ":5")
        logs = Binlogs({"binlog.000001": [
            event(4, "Previous_gtids", prior),
            event(14, "Gtid", f"SET @@SESSION.GTID_NEXT= '{B}:5'"),
            event(24, "Query", "use `sales`; BEGIN"),
            event(34, "Update_rows"), event(44, "Xid"),
            event(54, "Gtid", f"SET @@SESSION.GTID_NEXT= '{A}:26'"),
            event(64, "Query", "BEGIN"), event(74, "Xid"),
        ]}, executed=wanted + "," + A + ":26")
        result = self.locate(logs, wanted)
        self.assertEqual(result["pos"], 54)
        self.assertEqual(result["gtids"], wanted)

    def test_purged_transactions_must_all_be_excluded_from_replay(self):
        purged = A + ":1-20," + B + ":1-5"
        logs = Binlogs({"binlog.000001": [event(4, "Previous_gtids", purged)]},
                      purged=purged, executed=A + ":1-32," + B + ":1-10")
        for invalid in (A + ":1-19," + B + ":1-5", A + ":1-32", A + ":1-33," + B + ":1-5"):
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                self.locate(logs, invalid)
        self.assertEqual(self.locate(logs, purged)["source_range"]["purged"], purged)

    def test_holes_that_are_not_a_transaction_prefix_are_rejected(self):
        logs = Binlogs({"binlog.000001": [
            event(4, "Previous_gtids", A + ":1-10"),
            event(14, "Gtid", f"SET @@SESSION.GTID_NEXT= '{A}:11'"), event(24, "Xid"),
            event(34, "Gtid", f"SET @@SESSION.GTID_NEXT= '{A}:12'"), event(44, "Xid"),
        ]})
        with self.assertRaisesRegex(ValueError, "prefix"):
            self.locate(logs, A + ":1-10:12")

    def test_boundary_inside_a_file_does_not_continue_into_later_files(self):
        logs = Binlogs({
            "binlog.000001": [
                event(4, "Previous_gtids", A + ":1-29"),
                event(14, "Gtid", f"SET @@SESSION.GTID_NEXT= '{A}:30'"),
                event(24, "Query", "use `sales`; CREATE TABLE orders (id INT)"),
                event(34, "Gtid", f"SET @@SESSION.GTID_NEXT= '{A}:31'"),
                event(44, "Query", "BEGIN"), event(54, "Xid"),
                event(64, "Gtid", f"SET @@SESSION.GTID_NEXT= '{A}:32'"),
                event(74, "Query", "BEGIN"), event(84, "Xid"),
                event(94, "Gtid", f"SET @@SESSION.GTID_NEXT= '{A}:33'"),
                event(104, "Query", "BEGIN"), event(114, "Xid"),
            ],
            "binlog.000002": [event(4, "Previous_gtids", A + ":1-33")],
        })
        result = self.locate(logs, A + ":1-32", baseline={"gtids": A + ":1-30"})
        self.assertEqual((result["file"], result["pos"]), ("binlog.000001", 94))

    def test_initial_capture_rejects_historical_ddl_while_existing_capture_can_use_history(self):
        logs = Binlogs({"binlog.000001": [
            event(4, "Previous_gtids", A + ":1-10"),
            event(14, "Gtid", f"SET @@SESSION.GTID_NEXT= '{A}:11'"),
            event(24, "Query", "use `sales`; ALTER TABLE orders ADD amount INT /* xid=4 */"),
        ]})
        with self.assertRaisesRegex(ValueError, "historical schema"):
            self.locate(logs, A + ":1-10", check_schema=True)
        self.assertEqual(self.locate(logs, A + ":1-10")["gtids"], A + ":1-10")

    def test_initial_capture_accepts_row_changes_without_row_queries(self):
        logs = Binlogs({"binlog.000001": [
            event(4, "Previous_gtids", A + ":1-10"),
            event(14, "Gtid", f"SET @@SESSION.GTID_NEXT= '{A}:11'"),
            event(24, "Query", "use `sales`; BEGIN"), event(34, "Table_map"),
            event(44, "Write_rows"), event(54, "Xid"),
        ]})
        self.locate(logs, A + ":1-10", check_schema=True)
        self.assertFalse(any("SELECT *" in sql for sql, _ in logs.calls))

    def test_schema_baseline_and_missing_header_still_reject_unsafe_replay(self):
        logs = Binlogs({"binlog.000001": [event(4, "Previous_gtids", A + ":1-10")]})
        with self.assertRaisesRegex(ValueError, "schema baseline"):
            self.locate(logs, A + ":1-10", baseline={"gtids": A + ":1-20"})
        logs = Binlogs({"binlog.000001": [event(4, "Format_desc")]})
        with self.assertRaisesRegex(ValueError, "Previous_gtids"):
            self.locate(logs, A + ":1-10")

    def test_encrypted_compressed_or_anonymous_history_is_not_silently_trusted(self):
        for kind in ("Anonymous_Gtid", "Transaction_payload", "Incident"):
            logs = Binlogs({"binlog.000001": [
                event(4, "Previous_gtids", A + ":1-10"), event(14, kind),
            ]})
            with self.subTest(kind=kind), self.assertRaisesRegex(ValueError, "Unsupported"):
                self.locate(logs, A + ":1-10", check_schema=True)
