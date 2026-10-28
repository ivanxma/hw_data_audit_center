import base64
import hashlib
import json
import struct
import unittest
from unittest.mock import MagicMock, patch

from test_ingestion import FLOW, message

from modules.ingestion import BatchWriter
from modules.message_fragments import frame, identity
from modules.message_policy import CHUNK_BYTES, policy
from modules.schema_history import schema_identity


def framed(topic, key, value):
    key = key.encode() if isinstance(key, str) else key
    value = value.encode() if isinstance(value, str) else value
    packed = struct.pack(">q", -1 if key is None else len(key)) + (key or b"") + value
    checksum = hashlib.sha256(packed).hexdigest()
    identifier = hashlib.sha256((topic + "\0" + checksum).encode()).hexdigest()
    chunks = [packed[i : i + CHUNK_BYTES] for i in range(0, len(packed), CHUNK_BYTES)]
    return [
        json.dumps(
            {
                "_audit_fragment_v1": {
                    "id": identifier,
                    "index": i,
                    "count": len(chunks),
                    "total_bytes": len(packed),
                    "sha256": checksum,
                    "chunk_sha256": hashlib.sha256(c).hexdigest(),
                    "data": base64.b64encode(c).decode(),
                }
            },
            separators=(",", ":"),
        ).encode()
        for i, c in enumerate(chunks)
    ]


class FragmentTests(unittest.TestCase):
    def test_normal_messages_are_not_reframed(self):
        self.assertIsNone(frame(b'{"payload":{"op":"u"}}'))
        self.assertIsNone(frame(None))

    def test_metadata_hashes_and_byte_boundaries_are_validated(self):
        chunks = framed(
            "orders", b'{"id":1}', json.dumps({"value": "界é" * 200000}, ensure_ascii=False)
        )
        self.assertGreater(len(chunks), 1)
        for raw in chunks:
            part = frame(raw)
            self.assertEqual(identity(FLOW, "orders", part), identity(FLOW, "orders", part))
            with self.assertRaises(ValueError):
                identity(FLOW, "another-stream", part)
        bad = json.loads(chunks[0])
        bad["_audit_fragment_v1"]["data"] = "AAAA"
        with self.assertRaisesRegex(ValueError, "checksum|length"):
            frame(json.dumps(bad))
        bad = json.loads(chunks[0])
        bad["_audit_fragment_v1"]["count"] += 1
        with self.assertRaisesRegex(ValueError, "count"):
            frame(json.dumps(bad))
        with self.assertRaisesRegex(ValueError, "logical event"):
            frame(chunks[0], maximum=100)

    def test_invalid_policy_cannot_disable_event_and_custody_limits(self):
        self.assertEqual(policy({})["max_event_bytes"], 16 * 1024 * 1024)
        for value in [0, True, 1024, 65 * 1024 * 1024]:
            with self.assertRaises(ValueError):
                policy({"streaming": {"max_event_bytes": value}})
        with self.assertRaises(ValueError):
            policy({"streaming": {"max_pending_fragment_bytes": 1}})

    def test_incomplete_fragment_custody_precedes_checkpoint_and_ack(self):
        raw = framed("orders", b'{"id":1}', b"x" * 900000)[0]
        writer = BatchWriter(MagicMock(), MagicMock(), FLOW, b"key")
        writer.owned = {("orders", 0)}
        writer.add(message(4, value=raw))
        trace = []
        with (
            patch(
                "modules.message_fragments.accept",
                side_effect=lambda *a: trace.append("durable-part") or None,
            ),
            patch("modules.checkpoints.save", side_effect=lambda *a: trace.append("checkpoint")),
            patch("modules.ingestion.write_batch") as target,
        ):
            writer.consumer.acknowledge.side_effect = lambda *a: trace.append("ack")
            writer.flush()
        self.assertEqual(trace, ["durable-part", "checkpoint", "ack"])
        target.assert_not_called()

    def test_failed_custody_never_advances_checkpoint(self):
        raw = framed("orders", b'{"id":1}', b"x" * 900000)[0]
        writer = BatchWriter(MagicMock(), MagicMock(), FLOW, b"key")
        writer.owned = {("orders", 0)}
        writer.add(message(4, value=raw))
        with (
            patch("modules.message_fragments.accept", side_effect=RuntimeError("target failed")),
            patch("modules.checkpoints.save") as checkpoint,
        ):
            with self.assertRaises(RuntimeError):
                writer.flush()
        checkpoint.assert_not_called()
        writer.consumer.acknowledge.assert_not_called()

    def test_fragment_schema_identity_is_stable_across_republication_offsets(self):
        source = {"snapshot": "true", "file": "binlog.1", "pos": 100}
        a = schema_identity(FLOW, source, "sales.orders", 0, "schema", 0, 10, "a" * 64)
        b = schema_identity(FLOW, source, "sales.orders", 0, "schema", 0, 999, "a" * 64)
        self.assertEqual(a, b)
        self.assertNotEqual(
            a, schema_identity(FLOW, source, "sales.orders", 1, "schema", 0, 10, "a" * 64)
        )
