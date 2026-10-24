import json
import tempfile
import time
import unittest
from pathlib import Path
from types import SimpleNamespace as Obj
from unittest.mock import MagicMock, patch

import test_core

from modules.connector_health import from_logs, summarize
from modules.flow_status import inspect_flow
from modules.workflows import WorkflowService


class FlowStatusTests(unittest.TestCase):
    def setUp(self):
        self.cloud = MagicMock()
        self.flow = {
            "id": 1,
            "status": "deploying",
            "connector_ocid": "capture",
            "processor_ocid": "writer",
        }
        self.instances = {
            r: Obj(lifecycle_state="ACTIVE", containers=[Obj(container_id=r + "-process")])
            for r in ("capture", "writer")
        }
        self.processes = {
            r + "-process": Obj(
                container_instance_id=r,
                lifecycle_state="ACTIVE",
                lifecycle_details="CONTAINER_RUNNING",
                exit_code=0,
            )
            for r in self.instances
        }
        self.cloud.owned_container.side_effect = lambda rid, fid: self.instances[rid]
        self.cloud.containers.get_container.side_effect = lambda rid: Obj(data=self.processes[rid])
        self.cloud.check.side_effect = lambda obj: obj
        self.cloud.containers.retrieve_logs.return_value.data.content = (
            "AUDIT_CONNECTOR_HEALTH " + json.dumps({"state": "RUNNING", "checked_at": time.time()})
        ).encode()
        self.cloud.config = {"debezium": {"image_url": "new-image"}}
        self.processes["capture-process"].image_url = "old-image"

    def test_live_processes_replace_stale_deploying_display(self):
        self.assertEqual(inspect_flow(self.cloud, self.flow)["status"], "active")

    def test_running_process_with_failed_task_is_not_active(self):
        health = summarize(
            {
                "connector": {"state": "RUNNING"},
                "tasks": [{"state": "FAILED", "trace": "private-data"}],
            }
        )
        self.assertNotIn("private-data", json.dumps(health))
        self.cloud.containers.retrieve_logs.return_value.data.content = (
            "AUDIT_CONNECTOR_HEALTH " + json.dumps(health)
        ).encode()
        result = inspect_flow(self.cloud, self.flow)
        self.assertEqual(result["status"], "attention")
        self.assertEqual(result["workloads"][0]["task_state"], "FAILED")

    def test_stale_or_missing_task_report_never_establishes_health(self):
        self.assertEqual(
            from_logs('AUDIT_CONNECTOR_HEALTH {"state":"RUNNING","checked_at":1}', now=200),
            "UNAVAILABLE",
        )
        self.assertEqual(
            from_logs("Unable to find class io.debezium.storage.jdbc.history.JdbcSchemaHistory"),
            "MISSING_JDBC_MODULE",
        )
        self.assertEqual(
            summarize({"connector": {"state": "RUNNING"}, "tasks": []})["state"], "STARTING"
        )
        self.cloud.containers.retrieve_logs.side_effect = RuntimeError("access denied")
        self.assertEqual(inspect_flow(self.cloud, self.flow)["status"], "pending")

    def test_active_instance_with_exited_process_needs_attention(self):
        self.processes["capture-process"].lifecycle_details = "CONTAINER_TERMINATED"
        self.processes["capture-process"].exit_code = 1
        result = inspect_flow(self.cloud, self.flow)
        self.assertEqual(result["status"], "attention")
        self.assertEqual(result["workloads"][0]["exit_code"], 1)

    def test_creating_is_progress_and_missing_writer_is_partial(self):
        self.instances["writer"].lifecycle_state = "CREATING"
        self.assertEqual(inspect_flow(self.cloud, self.flow)["status"], "deploying")
        self.flow["processor_ocid"] = None
        self.assertEqual(inspect_flow(self.cloud, self.flow)["status"], "pending")

    def test_stopped_and_retired_are_preserved(self):
        for obj in self.instances.values():
            obj.lifecycle_state = "INACTIVE"
        self.assertEqual(inspect_flow(self.cloud, self.flow)["status"], "stopped")
        self.flow["status"] = "retired"
        self.assertEqual(inspect_flow(self.cloud, self.flow)["status"], "retired")

    def test_inactive_writer_explains_selected_image_is_not_deployed(self):
        self.instances["writer"].lifecycle_state = "INACTIVE"
        self.processes["writer-process"].image_url = "old-image"
        self.cloud.config["processor"] = {"image_url": "fixed-image"}
        result = inspect_flow(self.cloud, self.flow)
        self.assertEqual(result["status"], "attention")
        self.assertTrue(result["workloads"][1]["update_available"])
        self.assertIn("Update history writer image", result["workloads"][1]["guidance"])

    def test_unknown_process_is_not_healthy_and_wrong_attachment_is_refused(self):
        self.processes["capture-process"].lifecycle_details = "UNRECOGNIZED"
        self.assertEqual(inspect_flow(self.cloud, self.flow)["status"], "starting")
        self.processes["capture-process"].container_instance_id = "other"
        with self.assertRaises(ValueError):
            inspect_flow(self.cloud, self.flow)

    def test_running_connector_update_stops_only_capture_then_replaces(self):
        service = WorkflowService(MagicMock(), lambda: self.cloud, "operator")
        self.cloud.create_debezium.return_value = "replacement"
        self.cloud.container_action.side_effect = lambda *args: setattr(
            self.instances["capture"], "lifecycle_state", "INACTIVE"
        )
        with patch.object(service, "_ready"), patch.object(service, "_update") as update:
            self.assertEqual(
                service._operate(self.cloud, self.flow, "update_debezium"), "deploying"
            )
            self.cloud.create_debezium.assert_called_once_with(self.flow)
            self.cloud.container_action.assert_called_once_with("capture", "stop", 1)
            update.assert_called_with(1, connector_ocid="replacement", status="deploying")
            self.assertEqual(self.instances["writer"].lifecycle_state, "ACTIVE")
            self.assertEqual(self.flow["processor_ocid"], "writer")
            self.cloud.containers.delete_container_instance.assert_not_called()

    def test_replacement_waits_for_confirmed_shutdown_and_can_retry(self):
        service = WorkflowService(MagicMock(), lambda: self.cloud, "operator")
        with (
            patch.object(service, "_ready"),
            patch.object(service, "_update") as update,
            patch("modules.workflows.time.monotonic", side_effect=[0, 61]),
        ):
            self.assertEqual(service._operate(self.cloud, self.flow, "update_debezium"), "stopping")
            self.cloud.create_debezium.assert_not_called()
            update.assert_called_once_with(1, status="stopping", last_action="update_debezium")
        self.instances["capture"].lifecycle_state = "INACTIVE"
        self.cloud.container_action.reset_mock()
        self.cloud.create_debezium.return_value = "replacement"
        with patch.object(service, "_ready"), patch.object(service, "_update"):
            self.assertEqual(
                service._operate(self.cloud, self.flow, "update_debezium"), "deploying"
            )
            self.cloud.container_action.assert_not_called()

    def test_failed_preflight_never_stops_workloads(self):
        service = WorkflowService(MagicMock(), lambda: self.cloud, "operator")
        self.cloud.validate_workload.side_effect = ValueError("image unavailable")
        with patch.object(service, "_ready"), self.assertRaisesRegex(ValueError, "unavailable"):
            service._operate(self.cloud, self.flow, "update_debezium")
        self.cloud.container_action.assert_not_called()
        self.cloud.create_debezium.assert_not_called()

    def test_unconfirmed_or_failed_shutdown_never_creates_replacement(self):
        service = WorkflowService(MagicMock(), lambda: self.cloud, "operator")
        self.cloud.container_action.side_effect = lambda *args: setattr(
            self.instances["capture"], "lifecycle_state", "FAILED"
        )
        with (
            patch.object(service, "_ready"),
            patch.object(service, "_update"),
            self.assertRaises(ValueError),
        ):
            service._operate(self.cloud, self.flow, "update_debezium")
        self.cloud.create_debezium.assert_not_called()

    def test_intentional_capture_pause_keeps_writer_running_without_attention(self):
        self.instances["capture"].lifecycle_state = "INACTIVE"
        self.flow.update(last_action="stop", status="stopping")
        result = inspect_flow(self.cloud, self.flow)
        self.assertEqual(result["status"], "paused")
        self.assertIn("drains queued records", result["workloads"][1]["guidance"])
        self.processes["writer-process"].lifecycle_details = "CONTAINER_TERMINATED"
        self.assertEqual(inspect_flow(self.cloud, self.flow)["status"], "attention")

    def test_same_image_update_cannot_create_duplicate_container(self):
        self.instances["capture"].lifecycle_state = "INACTIVE"
        self.cloud.config["debezium"]["image_url"] = "old-image"
        service = WorkflowService(MagicMock(), lambda: self.cloud, "operator")
        with self.assertRaisesRegex(ValueError, "newly built"):
            service._operate(self.cloud, self.flow, "update_debezium")
        self.cloud.create_debezium.assert_not_called()
        self.cloud.container_action.assert_not_called()

    def test_writer_update_keeps_capture_and_existing_history(self):
        service = WorkflowService(MagicMock(), lambda: self.cloud, "operator")
        self.cloud.config["processor"] = {"image_url": "new-writer"}
        self.processes["writer-process"].image_url = "old-writer"
        self.cloud.create_processor.return_value = "replacement-writer"
        self.cloud.container_action.side_effect = lambda *args: setattr(
            self.instances["writer"], "lifecycle_state", "INACTIVE"
        )
        with patch.object(service, "_ready"), patch.object(service, "_update") as update:
            self.assertEqual(service._operate(self.cloud, self.flow, "update_writer"), "deploying")
            update.assert_called_with(1, processor_ocid="replacement-writer", status="deploying")
            self.cloud.create_debezium.assert_not_called()
            self.cloud.container_action.assert_called_once_with("writer", "stop", 1)
            self.assertEqual(self.instances["capture"].lifecycle_state, "ACTIVE")
            self.cloud.containers.delete_container_instance.assert_not_called()


class FlowStatusWebTests(unittest.TestCase):
    setUp = test_core.WebTests.setUp
    auth = test_core.WebTests.auth

    def test_stop_and_update_actions_require_authentication_and_csrf(self):
        for action in ("stop", "stop_all", "update_debezium", "update_writer"):
            self.assertEqual(self.client.post("/flows/1/" + action).status_code, 400)
        self.auth()
        with (
            tempfile.TemporaryDirectory() as directory,
            patch.object(self.mod.CONNECTIONS, "borrow", return_value=MagicMock()),
            patch("modules.workflows.WorkflowService.action", return_value="stopping") as operate,
            patch.object(self.mod, "ROOT", Path(directory)),
        ):
            self.assertEqual(self.client.post("/flows/1/stop_all").status_code, 400)
            operate.assert_not_called()
            self.assertEqual(
                self.client.post("/flows/1/stop_all", data={"csrf": "test-form-token"}).status_code,
                403,
            )
            operate.assert_not_called()
            (Path(directory) / "profiles.json").write_text(
                json.dumps({"admin_users": ["operator"]})
            )
            for action in ("stop", "stop_all", "update_debezium", "update_writer"):
                response = self.client.post("/flows/1/" + action, data={"csrf": "test-form-token"})
                self.assertEqual(response.status_code, 302)
                operate.assert_called_with(1, action)

    def test_status_requires_login_and_never_mutates_repository(self):
        self.assertEqual(self.client.get("/flows/1/status").status_code, 302)
        self.auth()
        with (
            patch.object(self.mod.CONNECTIONS, "borrow", return_value=MagicMock()),
            patch.object(self.mod.repo, "query", return_value={"id": 1}) as query,
            patch(
                "modules.flow_status.snapshot",
                return_value={"id": 1, "status": "attention", "workloads": []},
            ) as snapshot,
        ):
            response = self.client.get("/flows/1/status")
            self.assertEqual(response.status_code, 200)
            self.assertEqual(response.json["status"], "attention")
            self.assertTrue(query.call_args.args[1].startswith("SELECT"))
            query.assert_called_once()
            snapshot.assert_called_once()
