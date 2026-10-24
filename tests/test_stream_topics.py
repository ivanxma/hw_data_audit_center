import unittest
from datetime import datetime
from types import SimpleNamespace as Obj
from unittest.mock import MagicMock, patch

import oci
from test_service_catalog import cloud

from modules.fingerprints import digest
from modules.queue_connections import create_topic, topics
from modules.resource_catalog import build_spec, in_use


class StreamTopicTests(unittest.TestCase):
    def setUp(self):
        self.cloud = cloud()
        self.row = {"id": 7, "kind": "oci_streaming", "resource_ocid": "pool"}
        self.profile = {"kind": "oci_streaming", "stream_pool_id": "pool"}
        self.form = {"name": "events", "stream_pool_id": "pool", "compartment_id": "scope"}
        self.stream = Obj(
            id="stream",
            name="events",
            compartment_id="scope",
            stream_pool_id="pool",
            lifecycle_state="ACTIVE",
            partitions=1,
            retention_in_hours=168,
            freeform_tags={"managed-by": "heatwave-audit"},
        )
        self.request = {
            "request_token": "original-request",
            "kind": "stream",
            "name": "events",
            "spec_digest": digest(build_spec("stream", self.form)),
            "resource_ocid": "stream",
            "status": "submitted",
            "created_at": datetime.now(),
        }
        self.cloud.provision = MagicMock()
        self.cloud.streams.get_stream.return_value.data = self.stream

    def list_client(self, rows):
        # Use the actual SDK argument validation and pagination, without network calls.
        sdk = oci.streaming.StreamAdminClient.__new__(oci.streaming.StreamAdminClient)
        sdk.base_client = MagicMock()
        sdk.retry_strategy = oci.retry.NoneRetryStrategy()
        sdk.base_client.get_preferred_retry_strategy.return_value = sdk.retry_strategy
        sdk.base_client.call_api.return_value = oci.response.Response(200, {}, rows, None)
        self.cloud.streams.list_streams = sdk.list_streams
        return sdk

    def test_topic_inventory_uses_sdk_contract_and_hides_deleted_streams(self):
        deleted = Obj(**{**vars(self.stream), "lifecycle_state": "DELETED"})
        sdk = self.list_client([self.stream, deleted])
        with patch("modules.queue_connections.profile", return_value=self.profile):
            self.assertEqual(
                topics(self.cloud, self.row),
                [{"name": "events", "partitions": 1, "state": "ACTIVE"}],
            )
        params = sdk.base_client.call_api.call_args.kwargs["query_params"]
        self.assertEqual(params["compartmentId"], "scope")
        self.assertEqual(params["streamPoolId"], "pool")

    def test_topic_inventory_rejects_foreign_streams(self):
        self.stream.compartment_id = "other"
        self.list_client([self.stream])
        with patch("modules.queue_connections.profile", return_value=self.profile):
            with self.assertRaisesRegex(ValueError, "outside the VM compartment"):
                topics(self.cloud, self.row)

    def test_deleted_streams_do_not_keep_a_pool_in_use(self):
        self.stream.lifecycle_state = "DELETED"
        self.list_client([self.stream])
        with patch("modules.resource_catalog.repo.query", return_value=None):
            self.assertFalse(in_use(MagicMock(), self.cloud, "stream_pool", "pool", Obj()))
            self.stream.lifecycle_state = "DELETING"
            self.assertTrue(in_use(MagicMock(), self.cloud, "stream_pool", "pool", Obj()))

    def submit_again(self):
        with (
            patch("modules.queue_connections.profile", return_value=self.profile),
            patch("modules.resource_catalog.repo.named_lock"),
            patch("modules.resource_catalog.repo.query", return_value=self.request) as query,
            patch("modules.resource_catalog.repo.record"),
        ):
            create_topic(MagicMock(), self.cloud, self.row, "events")
        return query

    def test_repeating_accepted_topic_completes_registration_without_duplicate_creation(self):
        for state in ("ACTIVE", "CREATING"):
            with self.subTest(state=state):
                self.stream.lifecycle_state = state
                query = self.submit_again()
                self.assertIn("INSERT INTO managed_topics", query.call_args.args[1])
                self.assertEqual(query.call_args.args[2], (7, "events"))
        self.cloud.provision.assert_not_called()

    def test_repeat_topic_cannot_reuse_foreign_changed_or_unavailable_stream(self):
        original = vars(self.stream).copy()
        for changes in (
            {"compartment_id": "other"},
            {"stream_pool_id": "different-pool"},
            {"name": "different-name"},
            {"partitions": 2},
            {"retention_in_hours": 24},
            {"freeform_tags": {}},
            {"freeform_tags": {"managed-by": "heatwave-audit", "audit-app-id": "other-app"}},
            {"lifecycle_state": "DELETING"},
            {"lifecycle_state": "DELETED"},
        ):
            with self.subTest(changes=changes):
                self.cloud.streams.get_stream.return_value.data = Obj(**{**original, **changes})
                with self.assertRaises(ValueError):
                    self.submit_again()
        self.cloud.provision.assert_not_called()

    def test_not_authorized_is_not_treated_as_a_missing_stream_to_recreate(self):
        self.cloud.streams.get_stream.side_effect = oci.exceptions.ServiceError(
            404, "NotAuthorizedOrNotFound", {}, "unavailable"
        )
        with self.assertRaises(oci.exceptions.ServiceError):
            self.submit_again()
        self.cloud.provision.assert_not_called()
