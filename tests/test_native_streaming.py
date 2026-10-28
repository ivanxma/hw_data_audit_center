import base64
import unittest
from datetime import datetime, timezone
from types import SimpleNamespace as Obj
from unittest.mock import MagicMock, patch

from modules.native_streaming import NativeConsumer, NativeMessage


class NativeStreamingTests(unittest.TestCase):
    def setUp(self):
        self.client = MagicMock()
        self.client.create_cursor.return_value.data.value = "cursor"
        self.flow = {
            "id": 7,
            "topic": "rows",
            "schema_topic": "audit_7",
            "target_schema": "history",
        }
        stream = {
            "id": "stream-id",
            "partitions": 1,
            "messages_endpoint": "https://stream.example.invalid",
            "time_created": datetime.now(timezone.utc).isoformat(),
            "retention_in_hours": 168,
        }
        self.queue = {
            "kind": "oci_streaming",
            "region": "example-region",
            "streams": {"rows": stream, "audit_7": stream},
        }

    def consumer(self):
        return NativeConsumer(self.queue, client_factory=lambda endpoint: self.client)

    def test_restart_uses_after_last_actual_offset_and_never_resets_on_expiry(self):
        consumer = self.consumer()
        with patch("modules.native_streaming.query", return_value={"next_offset": 46}):
            lanes = consumer.attach(MagicMock(), self.flow)
        self.assertEqual(lanes[0].offset, 46)
        details = self.client.create_cursor.call_args_list[0].args[1]
        self.assertEqual((details.type, details.offset), ("AFTER_OFFSET", 45))
        self.client.create_cursor.side_effect = RuntimeError("expired offset")
        with (
            patch("modules.native_streaming.query", return_value={"next_offset": 46}),
            self.assertRaises(RuntimeError),
        ):
            self.consumer().attach(MagicMock(), self.flow)

    def test_unknown_origin_is_not_silently_reset_after_retention(self):
        self.queue["streams"]["rows"]["time_created"] = "2000-01-01T00:00:00+00:00"
        with (
            patch("modules.native_streaming.query", return_value=None),
            self.assertRaisesRegex(ValueError, "origin may have expired"),
        ):
            self.consumer().attach(MagicMock(), self.flow)
        self.client.create_cursor.assert_not_called()

    def test_native_cursor_and_message_values_are_decoded_without_a_jvm(self):
        consumer = self.consumer()
        with patch("modules.native_streaming.query", return_value={"next_offset": 1}):
            consumer.attach(MagicMock(), self.flow)
        record = Obj(
            partition="0",
            offset=45,
            key=base64.b64encode(b'{"id":1}').decode(),
            value=base64.b64encode(b'{"payload":{"op":"c"}}').decode(),
        )
        self.client.get_messages.return_value = Obj(
            data=[record], headers={"opc-next-cursor": "next"}
        )
        message = consumer.poll(0.1)
        self.assertEqual(message.key(), b'{"id":1}')
        self.assertEqual(message.offset(), 45)
        self.assertEqual(consumer.acknowledged[("rows", 0)], 1)
        consumer.acknowledge({("rows", 0): 46})
        self.assertEqual(consumer.acknowledged[("rows", 0)], 46)

    def test_malformed_binary_values_are_not_acknowledged(self):
        message = NativeMessage(
            "rows", Obj(key="invalid!", value="invalid!", partition="0", offset=0)
        )
        with self.assertRaises(ValueError):
            message.value()

    def test_idle_backoff_reduces_get_requests_and_fetches_larger_batches(self):
        now = [0.0]
        consumer = NativeConsumer(
            self.queue, client_factory=lambda endpoint: self.client, clock=lambda: now[0]
        )
        self.client.get_messages.return_value = Obj(data=[], headers={"opc-next-cursor": "same"})
        with patch("modules.native_streaming.query", return_value={"next_offset": 1}):
            consumer.attach(MagicMock(), self.flow)
        with patch(
            "modules.native_streaming.time.sleep",
            side_effect=lambda delay: now.__setitem__(0, now[0] + delay),
        ):
            for _ in range(50):
                consumer.poll(0.1)
        self.assertLessEqual(consumer.metrics()["get_requests"], 10)
        self.assertGreaterEqual(consumer.metrics()["get_requests"], 6)
        self.assertEqual(consumer.metrics()["empty_gets"], consumer.metrics()["get_requests"])
        self.assertEqual(self.client.get_messages.call_args.kwargs["limit"], 1000)

    def test_get_throttle_retries_same_cursor_with_backoff(self):
        import oci
        from collections import deque

        now = [0.0]
        consumer = NativeConsumer(
            self.queue, client_factory=lambda endpoint: self.client, clock=lambda: now[0]
        )
        with patch("modules.native_streaming.query", return_value={"next_offset": 1}):
            consumer.attach(MagicMock(), self.flow)
        consumer.lanes = deque([("rows", 0)])
        self.client.get_messages.side_effect = [
            oci.exceptions.ServiceError(429, "TooManyRequests", {}, "rate"),
            Obj(data=[], headers={"opc-next-cursor": "next"}),
        ]
        with patch(
            "modules.native_streaming.time.sleep",
            side_effect=lambda delay: now.__setitem__(0, now[0] + delay),
        ):
            consumer.poll(0.1)
            self.assertEqual(consumer.cursors[("rows", 0)], "cursor")
            now[0] = 0.6
            consumer.poll(0.1)
        self.assertEqual(consumer.cursors[("rows", 0)], "next")
        self.assertEqual(consumer.metrics()["get_retries"], 1)
        self.assertEqual(
            [call.args[1] for call in self.client.get_messages.call_args_list], ["cursor", "cursor"]
        )

    def test_non_oci_transports_are_rejected(self):
        with self.assertRaisesRegex(ValueError, "Only native OCI"):
            NativeConsumer({"kind": "oci_kafka"}, client_factory=lambda endpoint: self.client)
