import json
import unittest
from unittest.mock import MagicMock, patch

import test_core

from modules.capture_runtime import apply_request, validate_checkpoint_coverage
from modules.connector_health import summarize

PLAN = {
    "partition": {"server": "audit_1"},
    "offset": {"file": "binlog.000001", "pos": 198, "gtids": "x:1-11", "row": 0, "event": 0},
}
OLD = {"offsets": [{"partition": {"server": "audit_1"}, "offset": {"gtids": "x:1-20"}}]}


class CaptureRuntimeTests(unittest.TestCase):
    def test_stop_save_patch_verify_mark_resume_order(self):
        api = MagicMock()
        api.offsets.side_effect = [OLD, {"offsets": [PLAN]}]
        steps = []
        api.stop.side_effect = lambda: steps.append("stop")
        api.patch.side_effect = lambda _: steps.append("patch")
        api.resume.side_effect = lambda: steps.append("resume")
        with patch(
            "modules.capture_runtime.advance",
            side_effect=lambda d, s, r, state, **kw: steps.append(state),
        ):
            apply_request(
                MagicMock(),
                "audit",
                {"request_id": "r", "state": "pending", "planned_offset": PLAN},
                api,
                lambda *_: steps.append("source-check"),
            )
        self.assertEqual(
            steps, ["stop", "stopped", "source-check", "patch", "applied", "resume", "complete"]
        )

    def test_crash_after_patch_reconciles_without_repatch(self):
        api = MagicMock()
        check = MagicMock()
        api.offsets.return_value = {"offsets": [PLAN]}
        with patch("modules.capture_runtime.advance"):
            apply_request(
                MagicMock(),
                "audit",
                {
                    "request_id": "r",
                    "state": "stopped",
                    "planned_offset": json.dumps(PLAN),
                    "previous_offsets": json.dumps(OLD),
                },
                api,
                check,
            )
        api.patch.assert_not_called()
        api.resume.assert_called_once()
        check.assert_called_once_with(PLAN, OLD)

    def test_forward_jump_cannot_skip_uncaptured_schema_changes(self):
        for checkpoint in ({"offsets": []}, OLD):
            with (
                patch("modules.capture_runtime.query", return_value={"covered": 0}),
                patch("modules.capture_runtime.validate_gtid_boundary") as retention,
                self.assertRaisesRegex(ValueError, "SCHEMA_HISTORY_COVERAGE"),
            ):
                validate_checkpoint_coverage(MagicMock(), PLAN, checkpoint)
            retention.assert_not_called()
        with (
            patch("modules.capture_runtime.query", return_value={"covered": 1}) as sql,
            patch("modules.capture_runtime.validate_gtid_boundary") as retention,
        ):
            validate_checkpoint_coverage(MagicMock(), PLAN, OLD)
            self.assertEqual(sql.call_args.args[2], ("x:1-11", "x:1-20"))
            retention.assert_called_once()

    def test_applied_request_never_rewinds_progress_after_restart(self):
        api = MagicMock()
        with patch("modules.capture_runtime.advance"):
            apply_request(
                MagicMock(),
                "audit",
                {"request_id": "r", "state": "applied", "planned_offset": PLAN},
                api,
                MagicMock(),
            )
        api.offsets.assert_not_called()
        api.patch.assert_not_called()
        api.stop.assert_not_called()
        api.resume.assert_called_once()

    def test_ambiguous_progress_and_failed_validation_leave_task_stopped(self):
        for state, current in [("stopped", {"offsets": []}), ("pending", OLD)]:
            api = MagicMock()
            api.offsets.return_value = current
            with patch("modules.capture_runtime.advance"), self.assertRaises(ValueError):
                apply_request(
                    MagicMock(),
                    "audit",
                    {
                        "request_id": "r",
                        "state": state,
                        "planned_offset": PLAN,
                        "previous_offsets": OLD,
                    },
                    api,
                    MagicMock(side_effect=ValueError("expired")),
                )
            api.patch.assert_not_called()
            api.resume.assert_not_called()

    def test_initial_snapshot_is_not_reset(self):
        api = MagicMock()
        api.offsets.return_value = {"offsets": [{"offset": {"snapshot": "INITIAL"}}]}
        with (
            patch("modules.capture_runtime.advance"),
            self.assertRaisesRegex(ValueError, "SNAPSHOT"),
        ):
            apply_request(
                MagicMock(),
                "audit",
                {"request_id": "r", "state": "pending", "planned_offset": PLAN},
                api,
                MagicMock(),
            )
        api.patch.assert_not_called()
        self.assertEqual(
            summarize({"connector": {"state": "STOPPED"}, "tasks": []})["state"], "STOPPED"
        )


class CaptureWebTests(unittest.TestCase):
    setUp = test_core.WebTests.setUp
    auth = test_core.WebTests.auth

    def test_requires_authenticated_admin_and_csrf(self):
        self.assertEqual(self.client.get("/flows/1/capture-start").status_code, 302)
        self.auth()
        self.assertEqual(
            self.client.post("/flows/1/capture-start", data={"action": "apply"}).status_code, 400
        )
        with patch.object(self.mod.CONNECTIONS, "borrow", return_value=MagicMock()):
            self.assertEqual(self.client.get("/flows/1/capture-start").status_code, 403)

    def test_initial_gtid_check_requires_admin_and_csrf(self):
        self.auth()
        self.assertEqual(self.client.post("/configuration/gtid-check").status_code, 400)
        with patch.object(self.mod.CONNECTIONS, "borrow", return_value=MagicMock()):
            self.assertEqual(self.client.post(
                "/configuration/gtid-check", data={"csrf": "test-form-token"}
            ).status_code, 403)

    def test_rejected_gtid_check_still_returns_purged_and_executed_information(self):
        self.auth()
        limits = {"purged": "source-a:1-20,source-b:1-5", "executed": "source-a:1-100,source-b:1-40"}
        value = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:1-10"
        with (
            patch.object(self.mod.CONNECTIONS, "borrow", return_value=MagicMock()),
            patch.object(self.mod, "ROOT") as root,
            patch.object(self.mod, "Cloud"),
            patch("modules.capture_pages.repo.query", return_value={"id": 1}),
            patch("modules.capture_pages.connect"),
            patch("modules.gtid_position.source_range", return_value=limits),
            patch("modules.gtid_position.locate_gtid", side_effect=ValueError("Required binlog transactions have been purged.")),
        ):
            root.__truediv__.return_value.exists.return_value = True
            root.__truediv__.return_value.read_text.return_value = '{"admin_users":["operator"]}'
            response = self.client.post("/configuration/gtid-check", data={
                "csrf": "test-form-token", "source_id": "1", "gtid": value,
            }, headers={"Accept": "application/json"})
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json["source_range"], limits)
        self.assertIn("purged", response.json["message"])
        self.assertNotIn("never-render-this-password", response.get_data(as_text=True))

    def test_initial_form_explains_snapshot_and_preserves_password_secrecy(self):
        from flask import g, render_template

        with self.app.test_request_context("/flows/1/capture-start"):
            g.record = {"username": "operator", "csrf": "form"}
            page = render_template(
                "capture_start.html",
                flow={
                    "id": 1,
                    "name": "flow",
                    "source_schema": "sales",
                    "source_table": "orders",
                    "status": "draft",
                    "connector_ocid": None,
                },
                choice={"mode": "start"},
                preview=None,
                last=None,
                message=None,
                gtid_range={"purged": "source-a:1-20", "executed": "source-a:1-32"},
            )
        self.assertIn("snapshot current rows", page)
        self.assertIn("Datetime", page)
        self.assertIn("Normal Start, Stop and Resume keep the existing checkpoint", page)
        self.assertIn("Purged GTIDs", page)
        self.assertIn("source-a:1-20", page)
        self.assertIn("source-a:1-32", page)
        self.assertNotIn("never-render-this-password", page)
