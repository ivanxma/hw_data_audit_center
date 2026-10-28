"""Single-threaded partition lanes with bounded batches and explicit durable offsets."""

import logging
import json
import time
from dataclasses import dataclass


from .events import decode, write_batch
from .schema_history import write_schema_event

LOG = logging.getLogger(__name__)


@dataclass(frozen=True)
class BatchSettings:
    max_records: int = 250
    max_bytes: int = 4 * 1024 * 1024
    max_wait_ms: int = 500

    @classmethod
    def from_config(cls, settings):
        values = {
            key: settings.get(key, default)
            for key, default in (
                ("max_records", 250),
                ("max_bytes", 4 * 1024 * 1024),
                ("max_wait_ms", 500),
            )
        }
        limits = {
            "max_records": (1, 1000),
            "max_bytes": (1024, 16 * 1024 * 1024),
            "max_wait_ms": (100, 5000),
        }
        for key, (low, high) in limits.items():
            if type(values[key]) is not int or not low <= values[key] <= high:
                raise ValueError("Invalid processor batch setting: " + key)
        return cls(**values)


def validate_topics(consumer, flow):
    if flow["topic"] == flow["schema_topic"]:
        raise ValueError("Data and schema streams must be distinct.")
    for name in (flow["topic"], flow["schema_topic"]):
        stream = consumer.streams.get(name)
        if not stream or stream["partitions"] < 1:
            raise ValueError("Prepare both OCI streams before starting the writer.")
        if name == flow["schema_topic"] and stream["partitions"] != 1:
            raise ValueError("Schema stream must have exactly one partition.")


class BatchWriter:
    def __init__(self, consumer, db, flow, fingerprint_key, settings=None, clock=time.monotonic):
        self.consumer, self.db, self.flow = consumer, db, flow
        self.key = fingerprint_key
        self.settings = settings or BatchSettings()
        self.clock = clock
        self.pending = []
        self.pending_bytes = 0
        self.started = None
        self.owned = set()
        self.stage = "queue_validation"

    def add(self, message):
        if message.error():
            raise RuntimeError("Queue error; batch is not acknowledged.")
        lane = (message.topic(), message.partition())
        if lane not in self.owned:
            raise RuntimeError(
                "Message arrived without partition ownership; no acknowledgement sent."
            )
        size = len(message.value() or b"") + len(message.key() or b"")
        if size > self.settings.max_bytes:
            raise ValueError("Message exceeds the configured writer batch byte limit.")
        if self.pending and (
            self.pending_bytes + size > self.settings.max_bytes
            or len(self.pending) >= self.settings.max_records
        ):
            self.flush()
        if not self.pending:
            self.started = self.clock()
        self.pending.append(message)
        self.pending_bytes += size

    def due(self):
        return bool(self.pending) and (
            len(self.pending) >= self.settings.max_records
            or (self.clock() - self.started) * 1000 >= self.settings.max_wait_ms
        )

    def flush(self):
        if not self.pending:
            return
        messages = self.pending
        events = []
        event_sizes = []
        schemas = []
        offsets = {}
        completed_groups = set()
        from .message_fragments import frame, accept, note_event_time, commit_groups
        from .message_policy import DEFAULT_EVENT_BYTES

        self.stage = "decode"
        for message in messages:
            lane = (message.topic(), message.partition())
            if lane not in self.owned:
                raise RuntimeError("Partition ownership changed; no acknowledgement sent.")
            if message.topic() not in {self.flow["topic"], self.flow["schema_topic"]}:
                raise ValueError("Message is outside the registered streams.")
            value, key, message_id = message.value(), message.key(), None
            part = frame(
                value,
                self.flow.get("message_policy", {}).get("max_event_bytes", DEFAULT_EVENT_BYTES),
            )
            if part is not None:
                self.stage = "fragment_custody"
                assembled = accept(
                    self.db,
                    self.flow,
                    message.topic(),
                    message.partition(),
                    message.offset(),
                    part,
                    completed_groups,
                )
                offsets[lane] = max(offsets.get(lane, 0), message.offset() + 1)
                if assembled is None:
                    continue
                value, key, message_id = (
                    assembled["value"],
                    assembled["key"],
                    assembled["message_id"],
                )
                completed_groups.add(assembled["group_id"])
            self.stage = "decode"
            if message.topic() == self.flow["schema_topic"]:
                schemas.append(
                    (value, message.topic(), message.partition(), message.offset(), message_id)
                )
            else:
                extra = {"message_id": message_id} if message_id else {}
                event = decode(
                    value,
                    key,
                    message.topic(),
                    message.partition(),
                    message.offset(),
                    self.flow,
                    self.key,
                    **extra,
                )
                if event is not None:
                    events.append(event)
                    event_sizes.append(len(value or b"") + len(key or b""))
                    if message_id:
                        note_event_time(
                            self.db, self.flow, assembled["group_id"], event["event_time"]
                        )
            offsets[lane] = max(offsets.get(lane, 0), message.offset() + 1)
        started = self.clock()
        self.stage = "target_write"
        batches = []
        current, size = [], 0
        for event, cost in zip(events, event_sizes):
            if current and size + cost > self.settings.max_bytes:
                batches.append(current)
                current = []
                size = 0
            current.append(event)
            size += cost
        if current:
            batches.append(current)
        written = 0
        for batch in batches:
            if self.flow.get("target_grouping", "flow") == "flow":
                written += write_batch(self.db, self.flow, batch)
            else:
                from .target_routes import write

                written += write(self.db, self.flow, batch)
        # Partition checkpoints can include durable, incomplete fragments. Their
        # custody survives restart; only complete envelopes reach audit payloads.
        self.stage = "schema_write"
        for value, topic, partition, offset, message_id in schemas:
            extra = {"message_id": message_id} if message_id else {}
            write_schema_event(self.db, self.flow, value, topic, partition, offset, **extra)
        from .checkpoints import save

        self.stage = "checkpoint_write"
        save(self.db, self.flow, offsets)
        self.stage = "fragment_receipt"
        commit_groups(self.db, self.flow, completed_groups)
        self.stage = "queue_ack"
        self.consumer.acknowledge(offsets)
        LOG.info(
            "batch_committed records=%d inserted=%d bytes=%d duration_ms=%d",
            len(messages),
            written,
            self.pending_bytes,
            (self.clock() - started) * 1000,
        )
        self.pending = []
        self.pending_bytes = 0
        self.started = None

    def run(self, stopping):
        validate_topics(self.consumer, self.flow)
        self.stage = "checkpoint_recovery"
        partitions = self.consumer.attach(self.db, self.flow)
        self.owned = {(p.topic, p.partition) for p in partitions}
        last_metrics = self.clock()
        while not stopping():
            if self.clock() - last_metrics >= 15:
                LOG.info("AUDIT_STREAM_CONSUMER %s", json.dumps(self.consumer.metrics()))
                last_metrics = self.clock()
            self.stage = "queue_poll"
            message = self.consumer.poll(0.1)
            if message is not None:
                self.add(message)
            if self.due():
                self.flush()
        self.flush()  # Graceful stop persists only work still owned by this consumer.
