"""Single-threaded partition lanes with bounded batches and explicit durable offsets."""

import logging
import time
from dataclasses import dataclass

from confluent_kafka import TopicPartition

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
        raise ValueError("Data and public schema topics must be distinct.")
    for name in (flow["topic"], flow["schema_topic"]):
        metadata = consumer.list_topics(name, timeout=10)
        topic = metadata.topics.get(name)
        if topic is None or topic.error or not topic.partitions:
            raise ValueError("Provision both registered topics before starting the writer.")
        if name == flow["schema_topic"] and len(topic.partitions) != 1:
            raise ValueError(
                "Public schema topic must have exactly one partition to serialize structural history."
            )


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
        self.last_commit = None
        self.stage = "queue_validation"

    def assigned(self, consumer, partitions):
        from .checkpoints import assignment

        self.stage = "checkpoint_recovery"
        assignment(consumer, self.db, self.flow, partitions)
        self.owned = {(p.topic, p.partition) for p in partitions}

    def revoked(self, _consumer, _partitions):
        # Nothing is acknowledged on loss/revocation. Any buffered work replays.
        self.pending.clear()
        self.pending_bytes = 0
        self.started = None
        self.owned.clear()

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
        schemas = []
        offsets = {}
        self.stage = "decode"
        for message in messages:
            lane = (message.topic(), message.partition())
            if lane not in self.owned:
                raise RuntimeError("Partition ownership changed; no acknowledgement sent.")
            if message.topic() == self.flow["schema_topic"]:
                schemas.append(message)
            elif message.topic() == self.flow["topic"]:
                event = decode(
                    message.value(),
                    message.key(),
                    message.topic(),
                    message.partition(),
                    message.offset(),
                    self.flow,
                    self.key,
                )
                if event is not None:
                    events.append(event)
            else:
                raise ValueError("Message is outside the registered topics.")
            offsets[lane] = max(offsets.get(lane, 0), message.offset() + 1)
        started = self.clock()
        self.stage = "target_write"
        written = write_batch(self.db, self.flow, events)
        # One public-schema partition has one owner in the stable writer group.
        # Cross-topic causal ordering is not inferred from arrival or event_time.
        self.stage = "schema_write"
        for message in schemas:
            write_schema_event(
                self.db,
                self.flow,
                message.value(),
                message.topic(),
                message.partition(),
                message.offset(),
            )
        from .checkpoints import save

        self.stage = "checkpoint_write"
        save(self.db, self.flow, offsets)
        # Pace synchronous commits, including full batches, to avoid OCI Streaming's
        # offset-commit rate ceiling. Nothing is acknowledged until persistence succeeds.
        interval = self.flow.get("offset_commit_interval_ms", 500) / 1000
        if self.last_commit is not None:
            remaining = self.last_commit + interval - self.clock()
            if remaining > 0:
                time.sleep(remaining)
        self.stage = "queue_ack"
        committed = self.consumer.commit(
            offsets=[
                TopicPartition(topic, part, offset)
                for (topic, part), offset in sorted(offsets.items())
            ],
            asynchronous=False,
        )
        if any(p.error for p in (committed or [])):
            raise RuntimeError("Queue offset commit failed; stored events will replay.")
        self.last_commit = self.clock()
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
        self.consumer.subscribe(
            [self.flow["topic"], self.flow["schema_topic"]],
            on_assign=self.assigned,
            on_revoke=self.revoked,
            on_lost=self.revoked,
        )
        while not stopping():
            self.stage = "queue_poll"
            message = self.consumer.poll(0.1)
            if message is not None:
                self.add(message)
            if self.due():
                self.flush()
        self.flush()  # Graceful stop persists only work still owned by this consumer.
