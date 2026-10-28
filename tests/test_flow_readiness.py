import unittest
from datetime import datetime, timezone
from types import SimpleNamespace as Obj
from unittest.mock import MagicMock, patch

from test_service_catalog import cloud

from modules.config import fingerprint_secret
from modules.heatwave import validate_source
from modules.queue_connections import prepare_schema_topic
from modules.workflows import SourceNotReady, WorkflowService


class FlowReadinessTests(unittest.TestCase):
    def setUp(self):
        self.cloud = cloud()
        self._streams = {}
        self.cloud.streams.get_stream.side_effect = lambda identifier: Obj(data=self._streams[identifier])
        self.cloud.validate_connection = MagicMock()
        self.cloud.config = {"region": "example-region", "fingerprint_secret_ocid": "ocid1.vaultsecret.test.key"}
        self.cloud.vault.get_secret.return_value.data = Obj(compartment_id="scope")
        self.cloud.streams.get_connect_harness.return_value.data = Obj(
            compartment_id="scope", lifecycle_state="ACTIVE"
        )
        self.cloud.streams.get_stream_pool.return_value.data = Obj(
            compartment_id="scope", id="pool",
            lifecycle_state="ACTIVE",
            private_endpoint_settings=None,
            kafka_settings=Obj(bootstrap_servers="stream.example.invalid:9092"),
        )
        self.flow = {
            "id": 42,
            "topic": "orders",
            "transport": "streaming",
            "status": "draft",
            "source": {},
            "target": {},
            "source_schema": "sales",
            "source_table": "orders",
            "connector_name": "orders",
            "connect_harness_id": "ocid1.connectharness.test.harness",
            "queue_connection": {
                "kind": "oci_streaming",
                "stream_pool_id": "pool",
                "bootstrap_servers": "stream.example.invalid:9092",
                "incarnation": "pool",
            },
        }

    def listing(self, state="ACTIVE", partitions=1):
        stream = Obj(id="stream-" + str(len(self._streams)), compartment_id="scope", stream_pool_id="pool", lifecycle_state=state, partitions=partitions, messages_endpoint="https://example.invalid", time_created=datetime.now(timezone.utc), retention_in_hours=168)
        self._streams[stream.id] = stream
        return Obj(data=[stream])

    def test_missing_schema_stream_names_resource_and_preparation_action(self):
        with patch(
            "oci.pagination.list_call_get_all_results", side_effect=[self.listing(), Obj(data=[])]
        ):
            with self.assertRaisesRegex(ValueError, "audit_42"):
                self.cloud.validate_flow(self.flow)

    def test_schema_stream_requires_active_one_partition_and_same_compartment(self):
        for schema, text in (
            (self.listing("CREATING"), "CREATING"),
            (self.listing(partitions=2), "exactly one partition"),
            (Obj(data=[Obj(compartment_id="foreign", lifecycle_state="ACTIVE")]), "outside the VM"),
        ):
            with (
                self.subTest(text=text),
                patch(
                    "oci.pagination.list_call_get_all_results", side_effect=[self.listing(), schema]
                ),
            ):
                with self.assertRaisesRegex(ValueError, text):
                    self.cloud.validate_flow(self.flow)

    def test_ready_streams_continue_to_explicit_fingerprint_configuration_check(self):
        self.cloud.config = {"region": "example-region"}
        with patch("oci.pagination.list_call_get_all_results", return_value=self.listing()):
            with self.assertRaisesRegex(ValueError, "fingerprint_secret_ocid"):
                self.cloud.validate_flow(self.flow)
        for value in ({}, {"fingerprint_secret_ocid": {}}, {"fingerprint_secret_ocid": ""}):
            with self.assertRaises(ValueError):
                fingerprint_secret(value)

    def test_preparation_creates_only_missing_schema_topic(self):
        with (
            patch(
                "modules.queue_connections.topics",
                return_value=[{"name": "orders", "partitions": 4, "state": "ACTIVE"}],
            ),
            patch("modules.queue_connections.create_topic") as create,
        ):
            message = prepare_schema_topic("db", self.cloud, self.flow, "operator")
        create.assert_called_once_with(
            "db", self.cloud, self.flow["queue_connection"], "audit_42", 1, actor="operator"
        )
        self.assertIn("creation submitted", message)

    def test_preparation_reuses_active_or_creating_topic_without_changing_it(self):
        for state in ("ACTIVE", "CREATING", "DELETING"):
            with (
                self.subTest(state=state),
                patch(
                    "modules.queue_connections.topics",
                    return_value=[{"name": "audit_42", "partitions": 1, "state": state}],
                ),
                patch("modules.queue_connections.create_topic") as create,
            ):
                if state == "DELETING":
                    with self.assertRaisesRegex(ValueError, state):
                        prepare_schema_topic("db", self.cloud, self.flow, "operator")
                else:
                    self.assertIn(
                        "audit_42", prepare_schema_topic("db", self.cloud, self.flow, "operator")
                    )
                create.assert_not_called()

    def test_deployment_prepares_schema_stream_once_and_waits_for_visibility(self):
        with (
            patch("modules.queue_connections.topics", side_effect=[[], [], [{"name": "audit_42", "partitions": 1, "state": "ACTIVE"}]]),
            patch("modules.queue_connections.create_topic") as create,
            patch("modules.queue_connections.time.sleep"),
        ):
            result = prepare_schema_topic("db", self.cloud, self.flow, "operator", wait=True)
        create.assert_called_once()
        self.assertIn("ACTIVE", result)

    def test_source_configuration_failure_precedes_queue_and_target_deployment(self):
        service = WorkflowService(MagicMock(), lambda: self.cloud, "operator")
        self.cloud.validate_flow = MagicMock()
        with (
            patch("modules.workflows.validate_mapping"),
            patch("modules.workflows.validate_source", return_value={"ok": False, "checks": []}),
            patch("modules.workflows.prepare_target") as target,
        ):
            with self.assertRaises(SourceNotReady):
                service._operate(self.cloud, self.flow, "deploy")
        self.cloud.validate_flow.assert_not_called()
        target.assert_not_called()

    def test_heatwave_flush_tables_substitutes_reload_but_other_grants_still_required(self):
        for extra, allowed in (("FLUSH_TABLES", True), ("RELOAD", True), ("FLUSH_STATUS", False)):
            grants = (
                "GRANT SELECT, REPLICATION SLAVE, REPLICATION CLIENT, SHOW DATABASES, LOCK TABLES, "
                + extra
                + " ON *.* TO 'FLUSH_TABLES'@'%'"
            )
            with (
                self.subTest(extra=extra),
                patch("modules.heatwave.connect"),
                patch(
                    "modules.repository.query",
                    side_effect=[
                        [],
                        [{"column_name": "id", "column_key": "PRI"}],
                        [{"grants": grants}],
                    ],
                ),
            ):
                result = validate_source({}, "sales", "orders", self.cloud)
            check = next(c for c in result["checks"] if c["name"] == "connector_privileges")
            self.assertEqual(check["ok"], allowed)
