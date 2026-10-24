import json
import unittest
from datetime import date
from unittest.mock import MagicMock, patch

from confluent_kafka import TopicPartition

from deploy.debezium_bootstrap import properties
from modules.checkpoints import assignment
from modules.events import decode
from modules.fingerprints import row_hash
from modules.ingestion import BatchSettings, BatchWriter, validate_topics
from modules.mappings import validate_mapping
from modules.retention import eligible_partitions, policy_for

FLOW = {
    "id": 7,
    "source_identity": "source-test",
    "lineage_id": "lineage-test",
    "queue_incarnation": "queue-test",
    "source_schema": "sales",
    "source_table": "orders",
    "target_schema": "history",
    "target_table": "orders_history",
    "topic": "orders",
    "schema_topic": "audit_7",
    "offset_commit_interval_ms": 250,
}


def message(offset, *, value=None, partition=0):
    source = {
        "db": "sales",
        "table": "orders",
        "file": "binlog.000001",
        "pos": 100 + offset,
        "row": 0,
        "ts_ms": 1700000000000,
    }
    raw = (
        json.dumps(
            {
                "op": "u",
                "source": source,
                "before": {"id": 1, "state": "pending"},
                "after": {"id": 1, "state": "paid"},
            }
        ).encode()
        if value is None
        else value
    )
    msg = MagicMock()
    msg.topic.return_value = "orders"
    msg.partition.return_value = partition
    msg.offset.return_value = offset
    msg.key.return_value = b'{"id":1}'
    msg.value.return_value = raw
    msg.error.return_value = None
    return msg


class IngestionTests(unittest.TestCase):
    def writer(self):
        consumer = MagicMock()
        consumer.commit.return_value = [TopicPartition("orders", 0, 3)]
        writer = BatchWriter(consumer, MagicMock(), FLOW, b"test-key")
        writer.owned = {("orders", 0), ("orders", 1)}
        return writer

    def test_persistence_precedes_offsets_for_all_lanes(self):
        writer = self.writer()
        writer.add(message(0))
        writer.add(message(2))  # Offsets may contain broker gaps, not all integers must exist.
        writer.add(message(8, partition=1))
        sequence = []
        with (
            patch(
                "modules.ingestion.write_batch",
                side_effect=lambda *args: sequence.append("target") or 3,
            ),
            patch(
                "modules.checkpoints.save", side_effect=lambda *args: sequence.append("checkpoint")
            ),
        ):
            writer.consumer.commit.side_effect = lambda **kwargs: sequence.append("ack") or []
            writer.flush()
        self.assertEqual(sequence, ["target", "checkpoint", "ack"])
        offsets = writer.consumer.commit.call_args.kwargs["offsets"]
        self.assertEqual({p.partition: p.offset for p in offsets}, {0: 3, 1: 9})

    def test_malformed_record_never_leaps_offset(self):
        writer = self.writer()
        writer.add(message(1))
        writer.add(message(2, value=b'{"op":"u"}'))
        with patch("modules.ingestion.write_batch") as write, self.assertRaises(ValueError):
            writer.flush()
        write.assert_not_called()
        writer.consumer.commit.assert_not_called()

    def test_failed_target_never_acknowledges(self):
        writer = self.writer()
        writer.add(message(1))
        with (
            patch("modules.ingestion.write_batch", side_effect=RuntimeError("DB failed")),
            self.assertRaises(RuntimeError),
        ):
            writer.flush()
        writer.consumer.commit.assert_not_called()
        self.assertEqual(len(writer.pending), 1)

    def test_failed_checkpoint_does_not_acknowledge(self):
        writer = self.writer()
        writer.add(message(1))
        with (
            patch("modules.ingestion.write_batch", return_value=1),
            patch("modules.checkpoints.save", side_effect=RuntimeError("checkpoint failed")),
            self.assertRaises(RuntimeError),
        ):
            writer.flush()
        writer.consumer.commit.assert_not_called()

    def test_rebalance_discards_only_unacknowledged_buffer(self):
        writer = self.writer()
        writer.add(message(1))
        writer.revoked(writer.consumer, [TopicPartition("orders", 0)])
        writer.flush()
        writer.consumer.commit.assert_not_called()
        with self.assertRaises(RuntimeError):
            writer.add(message(2))

    def test_offset_result_error_is_not_success(self):
        writer = self.writer()
        writer.add(message(1))
        bad = MagicMock()
        bad.error = "commit failed"
        writer.consumer.commit.return_value = [bad]
        with (
            patch("modules.ingestion.write_batch", return_value=1),
            patch("modules.checkpoints.save"),
            self.assertRaises(RuntimeError),
        ):
            writer.flush()
        self.assertTrue(writer.pending)

    def test_batch_limits_and_schema_partition_validation(self):
        with self.assertRaises(ValueError):
            BatchSettings.from_config({"max_records": True})
        with self.assertRaises(ValueError):
            BatchSettings.from_config({"max_wait_ms": 0})
        consumer = MagicMock()
        topic = MagicMock()
        topic.error = None
        topic.partitions = {0: None, 1: None}
        consumer.list_topics.return_value.topics = {"orders": topic, "audit_7": topic}
        with self.assertRaises(ValueError):
            validate_topics(consumer, FLOW)

    def test_queue_expiry_and_lost_group_recovery(self):
        consumer = MagicMock()
        consumer.committed.return_value = [TopicPartition("orders", 0, -1001)]
        consumer.get_watermark_offsets.return_value = (20, 100)
        with (
            patch("modules.checkpoints.query", return_value={"next_offset": 10}),
            self.assertRaises(ValueError),
        ):
            assignment(consumer, MagicMock(), FLOW, [TopicPartition("orders", 0)])
        consumer.assign.assert_not_called()
        consumer.get_watermark_offsets.return_value = (0, 100)
        with patch("modules.checkpoints.query", return_value={"next_offset": 42}):
            assignment(consumer, MagicMock(), FLOW, [TopicPartition("orders", 0)])
        self.assertEqual(consumer.assign.call_args.args[0][0].offset, 42)

    def test_key_types_and_secret_requirement(self):
        one = row_hash({"id": 1}, FLOW, b"key")
        self.assertNotEqual(one, row_hash({"id": "1"}, FLOW, b"key"))
        self.assertNotEqual(
            one, row_hash({"id": 1}, {**FLOW, "source_identity": "different"}, b"key")
        )
        self.assertEqual(
            row_hash({"b": 2, "a": 1}, FLOW, b"key"), row_hash({"a": 1, "b": 2}, FLOW, b"key")
        )
        with self.assertRaises(ValueError):
            row_hash({"id": 1}, FLOW, None)

    def test_incomplete_source_coordinates_are_rejected(self):
        raw = json.loads(message(0).value())
        raw["source"].pop("row")
        with self.assertRaises(ValueError):
            decode(json.dumps(raw), b'{"id":1}', "orders", 0, 1, FLOW, b"key")

    def test_properties_preserve_spaces_and_non_bmp_unicode(self):
        value = " leading=\\\n\U0001f511"
        encoded = properties(value)
        units = bytes.fromhex(encoded.replace("\\u", ""))
        self.assertEqual(units.decode("utf-16-be"), value)
        self.assertNotIn("\n", encoded)


class RetentionAndMappingTests(unittest.TestCase):
    def test_holds_and_unverified_archive_block_expiry(self):
        policy = {
            "enabled": True,
            "retain_months": 3,
            "legal_hold": False,
            "archive_required": False,
        }
        self.assertEqual(policy_for({"retention": {"7": policy}}, 7), policy)
        for changed in [
            {"legal_hold": True},
            {"archive_required": True},
            {"retain_months": 0},
            {"retain_months": True},
        ]:
            with self.assertRaises(ValueError):
                policy_for({"retention": {"7": {**policy, **changed}}}, 7)
        self.assertIsNone(policy_for({}, 7))

    def test_guards_and_partial_month_are_never_selected(self):
        def part(name, bound):
            return {
                "partition_name": name,
                "upper_bound": bound,
                "estimated_rows": 4,
                "data_bytes": 32,
                "index_bytes": 8,
            }

        parts = [
            part("p_older", "'2026-01-01'"),
            part("p202601", "'2026-02-01'"),
            part("p202602", "'2026-03-01'"),
            part("p_future", "MAXVALUE"),
        ]
        self.assertEqual(
            [p["name"] for p in eligible_partitions(parts, date(2026, 2, 15))], ["p202601"]
        )
        with self.assertRaises(ValueError):
            eligible_partitions([part("p202601", "'2026-03-01'")], date(2026, 4, 1))

    def test_mapping_rejects_alias_duplicates_and_feedback(self):
        source = {"server_identity": "source"}
        target = {"server_identity": "target"}
        flow = {**FLOW, "source": source, "target": target}
        row = {
            "id": 8,
            "source_system": "source",
            "source_schema": "sales",
            "source_table": "orders",
            "target_system": "target",
            "target_schema": "history",
            "target_table": "another",
        }
        with patch("modules.mappings.query", return_value=[row]), self.assertRaises(ValueError):
            validate_mapping(MagicMock(), flow)
        feedback = {**flow, "source": target, "source_schema": "history", "source_table": "another"}
        with patch("modules.mappings.query", return_value=[row]), self.assertRaises(ValueError):
            validate_mapping(MagicMock(), feedback)


class HistoryRangeTests(unittest.TestCase):
    def test_offset_times_are_normalized_to_utc(self):
        from datetime import datetime

        from modules.audit_service import time_bounds

        start, end = time_bounds(
            {"from": "2026-10-23T10:00:00+02:00", "to": "2026-10-23T10:00:00Z"}
        )
        self.assertEqual(start, datetime(2026, 10, 23, 8))
        self.assertEqual(end, datetime(2026, 10, 23, 10))
        with self.assertRaises(ValueError):
            time_bounds({"from": "2026-10-24", "to": "2026-10-23"})

    def test_unkeyed_to_only_range_is_bounded(self):
        from datetime import datetime

        from modules.audit_service import time_bounds

        start, end = time_bounds({"to": "2026-01-02T00:00:00"})
        self.assertEqual(start, datetime(2026, 1, 1))
        self.assertEqual(end, datetime(2026, 1, 2))
        self.assertEqual(time_bounds({"key": '{"id":1}'}), (None, None))
