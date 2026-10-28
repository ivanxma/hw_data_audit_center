import unittest
from datetime import datetime, timezone
from types import SimpleNamespace as Obj
from unittest.mock import MagicMock, patch

from modules.streaming_service import resolve_streams


class StreamingServiceTests(unittest.TestCase):
    def setUp(self):
        self.cloud = MagicMock()
        self.cloud.check.side_effect = lambda value: value
        self.cloud.config = {"region": "example-region"}
        self.cloud.compartment = "compartment"
        self.cloud.streams.get_stream_pool.return_value.data = Obj(
            id="pool", lifecycle_state="ACTIVE", private_endpoint_settings=None)
        self.flow = {"id": 7, "topic": "rows"}
        self.queue = {"kind": "oci_streaming", "stream_pool_id": "pool"}

    def stream(self, name, partitions=1):
        return Obj(id=name + "-id", name=name, partitions=partitions, stream_pool_id="pool",
                   lifecycle_state="ACTIVE", messages_endpoint="https://stream.example.invalid",
                   time_created=datetime.now(timezone.utc), retention_in_hours=168)

    def listing(self, _method, **kwargs):
        return Obj(data=[self.stream(kwargs["name"])])

    def details(self, value):
        return Obj(data=self.stream(value[:-3]))

    def test_native_metadata_has_no_broker_or_harness_configuration(self):
        self.cloud.streams.get_stream.side_effect = self.details
        with patch("modules.streaming_service.oci.pagination.list_call_get_all_results", side_effect=self.listing):
            result = resolve_streams(self.cloud, self.flow, self.queue)
        self.assertEqual(set(result["streams"]), {"rows", "audit_7"})
        self.assertEqual(set(result), {"kind", "region", "stream_pool_id", "incarnation", "streams"})
        self.cloud.streams.get_connect_harness.assert_not_called()

    def test_recreated_stream_cannot_silently_reuse_a_delivery_checkpoint(self):
        self.flow["stream_bindings"] = '{"rows":"old-stream-id","audit_7":"audit_7-id"}'
        self.cloud.streams.get_stream.side_effect = self.details
        with patch("modules.streaming_service.oci.pagination.list_call_get_all_results", side_effect=self.listing):
            with self.assertRaisesRegex(ValueError, "identity changed"):
                resolve_streams(self.cloud, self.flow, self.queue)

    def test_schema_stream_rejects_multiple_partitions(self):
        self.cloud.streams.get_stream.side_effect = lambda value: Obj(data=self.stream(value[:-3], 2))
        with patch("modules.streaming_service.oci.pagination.list_call_get_all_results", side_effect=self.listing):
            with self.assertRaisesRegex(ValueError, "one partition"):
                resolve_streams(self.cloud, self.flow, self.queue)
