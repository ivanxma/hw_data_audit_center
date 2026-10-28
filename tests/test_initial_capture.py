import copy
import json
import unittest
from unittest.mock import MagicMock, patch

from modules.initial_capture import bootstrap, create_stopped

A = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
B = "bbbbbbbb-cccc-dddd-eeee-ffffffffffff"
VALUE = A + ":1-32," + B + ":1-18:21-25"


class InitialCaptureTests(unittest.TestCase):
    def setUp(self):
        self.config = {
            "connector_name": "capture-7",
            "connector": {"topic.prefix": "audit_7", "snapshot.mode": "no_data"},
            "initial_capture": {"mode": "gtid", "value": VALUE},
            "initial_capture_table": "orders",
            "capture_control": {
                "flow_id": 7, "target": {"database": "history"}, "source": {"database": "sales"},
            },
        }
        self.target, self.source = MagicMock(), MagicMock()
        self.open = self.enterContext(patch("modules.initial_capture.open_db"))
        self.open.side_effect = lambda target, secret: self.target if target["database"] == "history" else self.source
        self.enterContext(patch("modules.initial_capture.named_lock"))
        self.enterContext(patch("modules.initial_capture.create_stopped"))
        self.locate = self.enterContext(patch("modules.initial_capture.locate_gtid", return_value={
            "file": "binlog.000005", "pos": 100, "gtids": VALUE,
        }))
        self.query = self.enterContext(patch("modules.initial_capture.query", side_effect=self.sql))
        self.api = MagicMock()
        self.current = {"offsets": []}
        self.row = None
        self.api.offsets.side_effect = lambda: copy.deepcopy(self.current)
        self.api.patch.side_effect = self.patch_offsets
        self.api.call.side_effect = self.call
        self.marker = self.enterContext(patch("modules.initial_capture.Path")).return_value
        self.marker.is_file.return_value = True
        self.marker.read_text.side_effect = lambda: self.connector["audit.initial.token"]

    def sql(self, db, sql, params=(), one=False):
        if sql.startswith("SELECT * FROM `history`.audit_initial_capture"):
            return copy.deepcopy(self.row)
        if sql.startswith("INSERT INTO"):
            self.row = {"connector_name": params[1], "selection": params[2], "state": "prepared", "planned_offset": None}
        if sql.startswith("UPDATE "):
            self.row["state"] = params[0]
            if params[1]:
                self.row["planned_offset"] = params[1]
        if "GTID_SUBSET" in sql:
            return {"covered": 1}

    def patch_offsets(self, document):
        self.current = copy.deepcopy(document)

    def call(self, method, path, body=None):
        if method == "PUT" and path == "/config" and body.get("snapshot.mode") == "custom":
            self.connector = body
            # Schema capture emits its own offset and drops our seed marker;
            # no row events are streamed in this custom snapshot mode.
            self.current["offsets"][0]["offset"].pop("audit_initial_seed", None)
        return {"tasks": [{"state": "RUNNING"}]}

    def run_bootstrap(self):
        bootstrap(self.config, MagicMock(), self.api)

    def test_seed_schema_only_complete_then_restart_never_reseeds(self):
        self.run_bootstrap()
        self.assertEqual(self.api.patch.call_count, 2)
        initial = self.api.patch.call_args_list[0].args[0]["offsets"][0]
        self.assertEqual(initial["offset"]["gtids"], VALUE)
        self.assertEqual(initial["offset"]["snapshot"], "INITIAL")
        self.assertNotIn("snapshot", self.current["offsets"][0]["offset"])
        self.assertEqual(self.row["state"], "complete")
        self.source.rollback.assert_called_once()
        self.source.close.assert_called_once()
        self.assertTrue(any("LIMIT 0" in c.args[1] for c in self.query.call_args_list))
        self.locate.assert_called_once_with(self.source, VALUE, check_schema=True)
        self.api.reset_mock()
        self.locate.reset_mock()
        self.run_bootstrap()
        self.api.patch.assert_not_called()
        self.api.stop.assert_not_called()
        self.api.resume.assert_not_called()
        self.locate.assert_not_called()

    def test_schema_completion_waits_for_published_durable_offset_before_stopping(self):
        checks = []
        original_call = self.call
        def call(method, path, body=None):
            result = original_call(method, path, body)
            if method == "PUT" and path == "/config" and body.get("snapshot.mode") == "custom":
                self.current["offsets"][0]["offset"]["audit_initial_seed"] = "pending-publication"
            return result
        def offsets():
            if hasattr(self, "connector") and "audit_initial_seed" in self.current["offsets"][0]["offset"]:
                checks.append("wait")
                if len(checks) == 3:
                    self.current["offsets"][0]["offset"].pop("audit_initial_seed")
            return copy.deepcopy(self.current)
        def stop():
            self.assertNotIn("audit_initial_seed", self.current["offsets"][0]["offset"])
        self.api.call.side_effect = call
        self.api.offsets.side_effect = offsets
        self.api.stop.side_effect = stop
        with patch("modules.initial_capture.time.sleep"):
            self.run_bootstrap()
        self.assertEqual(len(checks), 3)
        self.assertEqual(self.row["state"], "complete")

    def test_existing_offsets_without_initialization_evidence_are_never_overwritten(self):
        self.current = {"offsets": [{"offset": {"gtids": VALUE}}]}
        with self.assertRaisesRegex(ValueError, "Existing connector offsets"):
            self.run_bootstrap()
        self.api.patch.assert_not_called()
        self.api.resume.assert_not_called()

    def test_completed_initialization_with_lost_offsets_cannot_silently_start_at_end(self):
        self.run_bootstrap()
        self.current = {"offsets": []}
        self.api.reset_mock()
        with self.assertRaisesRegex(ValueError, "checkpoint is missing"):
            self.run_bootstrap()
        self.api.patch.assert_not_called()
        self.api.call.assert_not_called()

    def test_expired_or_schema_unsafe_boundary_stays_stopped_and_releases_source_lock(self):
        self.locate.side_effect = ValueError("Required binlog transactions have been purged.")
        with self.assertRaisesRegex(ValueError, "purged"):
            self.run_bootstrap()
        self.api.patch.assert_not_called()
        self.api.resume.assert_not_called()
        self.api.stop.assert_called_once()
        self.source.rollback.assert_called_once()
        self.source.close.assert_called_once()

    def test_crash_after_seed_is_reconciled_without_a_second_seed(self):
        def fail_after_patch(document):
            self.patch_offsets(document)
            raise RuntimeError("simulated worker exit")
        self.api.patch.side_effect = fail_after_patch
        with self.assertRaisesRegex(RuntimeError, "worker exit"):
            self.run_bootstrap()
        self.assertEqual(self.row["state"], "prepared")
        original = copy.deepcopy(self.current)
        self.api.patch.side_effect = self.patch_offsets
        self.run_bootstrap()
        self.assertEqual(self.api.patch.call_count, 2)  # Original seed and final completion only.
        self.assertEqual(self.current["offsets"][0]["offset"]["gtids"], original["offsets"][0]["offset"]["gtids"])

    def test_missing_seeded_checkpoint_is_not_silently_recreated(self):
        self.row = {"connector_name": "capture-7", "selection": self.config["initial_capture"],
                    "state": "seeded", "planned_offset": {"partition": {"server": "audit_7"}, "offset": {"gtids": VALUE}}}
        with self.assertRaisesRegex(ValueError, "previously seeded checkpoint is missing"):
            self.run_bootstrap()
        self.api.patch.assert_not_called()

    def test_schema_ready_crash_retains_progress_instead_of_rewinding(self):
        self.run_bootstrap()
        self.row["state"] = "schema_ready"
        self.current["offsets"][0]["offset"].update(gtids=VALUE + "," + A + ":33", pos=200)
        advanced = copy.deepcopy(self.current)
        self.api.reset_mock()
        self.run_bootstrap()
        self.assertEqual(self.current, advanced)
        self.api.patch.assert_not_called()
        self.assertEqual(self.row["state"], "complete")

    def test_initial_selection_cannot_be_changed_after_initialization(self):
        self.run_bootstrap()
        self.config["initial_capture"]["value"] = A + ":1-100"
        self.api.reset_mock()
        with self.assertRaisesRegex(ValueError, "durable initialization"):
            self.run_bootstrap()
        self.api.patch.assert_not_called()
        self.api.resume.assert_not_called()

    def test_creates_connector_stopped_before_any_initial_offset_patch(self):
        with patch("modules.initial_capture.requests.get") as get, patch("modules.initial_capture.requests.post") as post:
            get.return_value.status_code = 404
            create_stopped(self.api, "capture-7", self.config["connector"])
            self.assertEqual(post.call_args.kwargs["json"]["initial_state"], "STOPPED")
            self.api.stop.assert_called_once()
