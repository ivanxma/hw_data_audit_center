import unittest
from types import SimpleNamespace as Obj
from unittest.mock import MagicMock, patch

import oci

from modules.database_objects import discover, new_audit_name
from modules.oci_service import Cloud
from modules.resource_catalog import build_spec, create, delete_selected, inventory
from modules.transport import transport_profile


def cloud():
    value = Cloud.__new__(Cloud)
    value.compartment = "scope"
    value.config = {}
    value.streams = MagicMock()
    value.streams.list_streams.__name__ = "list_streams"
    value.kafka = MagicMock()
    value.vault = MagicMock()
    value.network = MagicMock()
    return value


class CatalogScopeTests(unittest.TestCase):
    def test_inventory_is_scoped_and_rejects_foreign_results(self):
        c = cloud()
        row = Obj(
            id="stream",
            name="orders",
            lifecycle_state="ACTIVE",
            compartment_id="other",
            freeform_tags={},
        )
        with patch(
            "oci.pagination.list_call_get_all_results", return_value=Obj(data=[row])
        ) as listing:
            with self.assertRaises(ValueError):
                inventory(c, "stream")
        self.assertEqual(listing.call_args.kwargs["compartment_id"], "scope")

    def test_delete_checks_every_selected_resource_before_any_mutation(self):
        c = cloud()
        good = Obj(id="one", compartment_id="scope", freeform_tags={"managed-by": "heatwave-audit"})
        foreign = Obj(
            id="two", compartment_id="other", freeform_tags={"managed-by": "heatwave-audit"}
        )
        c.streams.get_stream.side_effect = [Obj(data=good), Obj(data=foreign)]
        with (
            patch("modules.resource_catalog.repo.named_lock"),
            patch("modules.resource_catalog.in_use", return_value=False),
        ):
            with self.assertRaises(ValueError):
                delete_selected(MagicMock(), c, "stream", ["one", "two"], "operator")
        c.streams.delete_stream.assert_not_called()

    def test_in_use_and_unmanaged_resources_cannot_be_deleted(self):
        c = cloud()
        row = Obj(id="one", compartment_id="scope", freeform_tags={})
        c.streams.get_stream.return_value.data = row
        with patch("modules.resource_catalog.repo.named_lock"):
            with self.assertRaises(ValueError):
                delete_selected(MagicMock(), c, "stream", ["one"], "operator")
        row.freeform_tags = {"managed-by": "heatwave-audit"}
        with (
            patch("modules.resource_catalog.repo.named_lock"),
            patch("modules.resource_catalog.in_use", return_value=True),
        ):
            with self.assertRaises(ValueError):
                delete_selected(MagicMock(), c, "stream", ["one"], "operator")
        c.streams.delete_stream.assert_not_called()

    def test_forged_compartment_cannot_provision(self):
        c = cloud()
        c.provision = MagicMock()
        with self.assertRaises(ValueError):
            create(
                MagicMock(), c, "stream_pool", {"name": "pool", "compartment_id": "other"}, "actor"
            )
        c.provision.assert_not_called()

    def test_stream_creation_uses_verified_pool_without_exclusive_compartment_field(self):
        c = cloud()
        c.registry = MagicMock()
        c.mysql = MagicMock()
        c.client = MagicMock()
        c.streams.get_stream_pool.return_value.data = Obj(compartment_id="scope")
        c.streams.create_stream.return_value.data = Obj(
            id="created-stream", compartment_id="scope", stream_pool_id="pool"
        )
        c.streams.list_streams.return_value = oci.response.Response(200, {}, [], None)
        spec = build_spec("stream", {"name": "events", "stream_pool_id": "pool"})
        self.assertEqual(c.provision("stream", spec, retry_token="retry"), "created-stream")
        c.streams.get_stream_pool.assert_called_once_with("pool")
        details = c.streams.create_stream.call_args.args[0]
        self.assertEqual(details.stream_pool_id, "pool")
        self.assertIsNone(details.compartment_id)
        c.streams.get_stream_pool.return_value.data.compartment_id = "foreign"
        with self.assertRaises(ValueError):
            c.provision("stream", spec)
        self.assertEqual(c.streams.create_stream.call_count, 1)

    def test_stream_creation_passes_the_real_sdk_argument_validation(self):
        c = cloud()
        c.registry = MagicMock()
        c.mysql = MagicMock()
        c.client = MagicMock()
        c.streams.get_stream_pool.return_value.data = Obj(compartment_id="scope")
        c.streams.list_streams.return_value = oci.response.Response(200, {}, [], None)
        sdk = oci.streaming.StreamAdminClient.__new__(oci.streaming.StreamAdminClient)
        sdk.base_client = MagicMock()
        sdk.retry_strategy = oci.retry.NoneRetryStrategy()
        sdk.base_client.get_preferred_retry_strategy.return_value = sdk.retry_strategy
        sdk.base_client.call_api.return_value.data = Obj(
            id="created-stream", compartment_id="scope", stream_pool_id="pool"
        )
        c.streams.create_stream = sdk.create_stream
        spec = build_spec("stream", {"name": "events", "stream_pool_id": "pool"})
        self.assertEqual(c.provision("stream", spec, retry_token="request-token"), "created-stream")
        args = sdk.base_client.call_api.call_args.kwargs
        self.assertNotIn("opc-retry-token", args["header_params"])
        self.assertEqual(args["header_params"]["opc-request-id"], "request-token")
        self.assertEqual(args["body"].freeform_tags["audit-request"], "request-token")
        self.assertIsNone(args["body"].compartment_id)

    def test_lost_stream_create_response_is_reconciled_without_another_create(self):
        c = cloud()
        details = oci.streaming.models.CreateStreamDetails(
            name="events",
            stream_pool_id="pool",
            partitions=1,
            retention_in_hours=168,
            freeform_tags={"managed-by": "heatwave-audit"},
        )
        c.streams.list_streams.return_value = oci.response.Response(200, {}, [], None)
        c.streams.create_stream.side_effect = TimeoutError("lost response")
        with self.assertRaises(TimeoutError):
            c._create_stream(details, "request-token")
        existing = Obj(
            id="accepted-stream",
            compartment_id="scope",
            stream_pool_id="pool",
            name="events",
            lifecycle_state="CREATING",
            partitions=1,
            retention_in_hours=168,
            freeform_tags={"managed-by": "heatwave-audit", "audit-request": "request-token"},
        )
        c.streams.list_streams.return_value.data = [existing]
        self.assertEqual(c._create_stream(details, "request-token"), "accepted-stream")
        c.streams.create_stream.assert_called_once()
        for change in ({"compartment_id": "foreign"}, {"freeform_tags": {}}, {"partitions": 2}):
            with self.subTest(change=change):
                c.streams.list_streams.return_value.data = [Obj(**{**vars(existing), **change})]
                with self.assertRaises(ValueError):
                    c._create_stream(details, "request-token")
                c.streams.create_stream.assert_called_once()

    def test_resource_creation_submits_normal_resources_and_records_result(self):
        c = cloud()
        c.provision = MagicMock(return_value="new-pool")
        with (
            patch("modules.resource_catalog.repo.named_lock") as lock,
            patch("modules.resource_catalog.repo.query", side_effect=[None, None, None]) as query,
            patch("modules.resource_catalog.repo.record"),
        ):
            result = create(
                MagicMock(), c, "stream_pool", {"name": "pool", "compartment_id": "scope"}, "actor"
            )
            self.assertEqual(result, "new-pool")
            c.provision.assert_called_once()
            self.assertEqual(
                query.call_args.args[2],
                ("new-pool", "submitted", c.provision.call_args.kwargs["retry_token"]),
            )
            lock.return_value.__exit__.assert_called_once()

    def test_stream_and_kafka_specs_preserve_service_contracts(self):
        spec = build_spec(
            "stream",
            {"name": "orders", "stream_pool_id": "pool", "partitions": "8", "retention": "168"},
        )
        self.assertEqual(spec["partitions"], 8)
        self.assertEqual(spec["retention_in_hours"], 168)
        with self.assertRaises(ValueError):
            build_spec("stream", {"name": "bad", "retention": "200"})
        with self.assertRaises(ValueError):
            build_spec(
                "kafka_cluster",
                {"name": "cluster", "node_count": "1", "cluster_type": "PRODUCTION"},
            )
        self.assertFalse(build_spec("repository", {"name": "images"})["is_public"])

    def test_mapping_uses_selected_connection_and_harness(self):
        profile = transport_profile(
            {"streaming": {"kind": "oci_streaming", "username": "global"}},
            {
                "transport": "streaming",
                "connector_name": "reader",
                "queue_connection": {
                    "kind": "oci_streaming",
                    "username": "selected",
                    "stream_pool_id": "pool",
                },
                "connect_harness_id": "ocid1.connectharness.test.id",
            },
        )
        self.assertEqual(profile["username"], "selected")
        self.assertEqual(profile["connect_topics"]["config"], "ocid1.connectharness.test.id-config")


class ObjectDiscoveryTests(unittest.TestCase):
    def test_schemas_hide_system_objects(self):
        c = cloud()
        with (
            patch("modules.database_objects.connect"),
            patch(
                "modules.database_objects.query",
                return_value=[{"name": "mysql"}, {"name": "sales"}, {"name": "information_schema"}],
            ),
        ):
            self.assertEqual(discover({"role": "source"}, c), {"schemas": ["sales"]})

    def test_target_dropdown_distinguishes_audit_layout(self):
        from modules.heatwave import AUDIT_COLUMNS

        tables = [{"name": "business"}, {"name": "audit"}]
        columns = [{"object_name": "audit", "name": name} for name in AUDIT_COLUMNS] + [
            {"object_name": "business", "name": "id"}
        ]
        keys = [
            {"object_name": "audit", "name": "event_time"},
            {"object_name": "audit", "name": "event_id"},
        ]
        with (
            patch("modules.database_objects.connect"),
            patch("modules.database_objects.query", side_effect=[tables, columns, keys]),
        ):
            result = discover({"role": "target"}, cloud(), "history")["tables"]
        self.assertFalse(result[0]["compatible"])
        self.assertTrue(result[1]["compatible"])

    def test_generated_audit_names_are_stable_and_bounded(self):
        one = new_audit_name("s" * 64, "t" * 64)
        self.assertLessEqual(len(one), 64)
        self.assertEqual(one, new_audit_name("s" * 64, "t" * 64))
        self.assertNotEqual(one, new_audit_name("s" * 64, "u" * 64))
