import unittest
from unittest.mock import MagicMock, patch

from modules.oci_service import Cloud
from modules.workflows import WorkflowService


class WorkflowTests(unittest.TestCase):
    def setUp(self):
        self.db = MagicMock()
        self.cloud = MagicMock()
        self.service = WorkflowService(self.db, lambda: self.cloud, "test-operator")
        self.flow = {
            "id": 1,
            "status": "draft",
            "connector_ocid": None,
            "processor_ocid": None,
            "source": {},
            "target": {},
            "source_schema": "sales",
            "source_table": "orders",
        }

    def test_locked_read_and_partial_deployment_retry(self):
        calls = []
        lock = MagicMock()
        lock.__enter__.side_effect = lambda: calls.append("lock")
        self.cloud.create_debezium.return_value = "connector-id"
        self.cloud.create_processor.side_effect = [
            RuntimeError("temporary failure"),
            "processor-id",
        ]
        with (
            patch("modules.workflows.repo.named_lock", return_value=lock),
            patch(
                "modules.workflows.repo.get_flow",
                side_effect=lambda *args: calls.append("read") or self.flow,
            ),
            patch("modules.workflows.repo.query"),
            patch("modules.workflows.repo.record"),
            patch.object(self.service, "_ready"),
            patch("modules.workflows.connect"),
            patch("modules.workflows.prepare_target"),
        ):
            with self.assertRaises(RuntimeError):
                self.service.action(1, "deploy")
            self.assertEqual(calls[:2], ["lock", "read"])
            self.assertEqual(self.service.action(1, "deploy"), "deploying")
        self.cloud.create_debezium.assert_called_once()
        self.assertEqual(self.cloud.create_processor.call_count, 2)

    def test_stop_cannot_unretire_a_flow(self):
        self.flow.update(status="retired", connector_ocid="connector-id")
        self.assertEqual(self.service._operate(self.cloud, self.flow, "stop"), "retired")
        self.cloud.container_action.assert_called_once_with("connector-id", "stop", 1)

    def test_normal_stop_leaves_writer_running_and_maintenance_stop_stops_both(self):
        self.flow.update(status="active", connector_ocid="capture", processor_ocid="writer")
        self.assertEqual(self.service._operate(self.cloud, self.flow, "stop"), "stopping")
        self.cloud.container_action.assert_called_once_with("capture", "stop", 1)
        self.cloud.container_action.reset_mock()
        self.assertEqual(self.service._operate(self.cloud, self.flow, "stop_all"), "stopping")
        self.assertEqual(
            [call.args for call in self.cloud.container_action.call_args_list],
            [("capture", "stop", 1), ("writer", "stop", 1)],
        )

    def test_refresh_preserves_capture_pause_intent(self):
        self.flow.update(
            status="stopping", last_action="stop", connector_ocid="capture", processor_ocid="writer"
        )
        with (
            patch("modules.workflows.repo.named_lock"),
            patch("modules.workflows.repo.get_flow", return_value=self.flow),
            patch("modules.workflows.repo.record"),
            patch.object(self.service, "_operate", return_value="paused"),
            patch.object(self.service, "_update") as update,
        ):
            self.assertEqual(self.service.action(1, "refresh"), "paused")
            update.assert_called_once_with(1, status="paused")

    def test_refresh_does_not_forget_an_unreadable_container(self):
        self.flow.update(status="retired", connector_ocid="connector-id")
        self.cloud.owned_container.side_effect = RuntimeError("not authorized or not found")
        with patch.object(self.service, "_update") as update, self.assertRaises(RuntimeError):
            self.service._operate(self.cloud, self.flow, "refresh")
        update.assert_not_called()

    def test_wrong_flow_ownership_blocks_mutation(self):
        cloud = Cloud.__new__(Cloud)
        cloud.compartment = "scope"
        cloud.containers = MagicMock()
        instance = MagicMock()
        instance.compartment_id = "scope"
        instance.freeform_tags = {"managed-by": "heatwave-audit", "flow-id": "different"}
        cloud.containers.get_container_instance.return_value.data = instance
        with self.assertRaises(ValueError):
            cloud.container_action("resource", "stop", 1)
        cloud.containers.stop_container_instance.assert_not_called()

    def test_unknown_action_cannot_fall_through_to_stop(self):
        with self.assertRaises(ValueError):
            self.service.action(1, "invalid")
        self.cloud.container_action.assert_not_called()
