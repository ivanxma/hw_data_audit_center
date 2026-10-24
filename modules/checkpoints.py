"""Target-side delivery checkpoints detect expired queue history and lost group offsets."""

from confluent_kafka import TopicPartition

from .repository import query
from .validation import quote


def initialize(db, schema):
    query(
        db,
        f"""CREATE TABLE IF NOT EXISTS {quote(schema)}.audit_delivery_checkpoints (
        flow_id BIGINT NOT NULL, topic VARCHAR(249) NOT NULL, partition_id INT NOT NULL,
        next_offset BIGINT NOT NULL, updated_at TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6)
        ON UPDATE CURRENT_TIMESTAMP(6), PRIMARY KEY(flow_id,topic,partition_id)
    ) ENGINE=InnoDB""",
    )


def assignment(consumer, db, flow, partitions):
    committed = {
        (p.topic, p.partition): p.offset for p in consumer.committed(partitions, timeout=10)
    }
    assigned = []
    for part in partitions:
        checkpoint = query(
            db,
            f"SELECT next_offset FROM {quote(flow['target_schema'])}."
            "audit_delivery_checkpoints WHERE flow_id=%s AND topic=%s AND partition_id=%s",
            (flow["id"], part.topic, part.partition),
            one=True,
        )
        offset = (
            checkpoint["next_offset"]
            if checkpoint
            else committed.get((part.topic, part.partition), -1)
        )
        low, high = consumer.get_watermark_offsets(part, timeout=10, cached=False)
        if offset < 0:
            # A missing group offset may replay from the original retained origin.
            # Existing target identities make that replay idempotent; a trimmed queue
            # must never silently resume from its new earliest record.
            if low != 0:
                raise ValueError(
                    "No recoverable queue checkpoint. Review coverage before resetting this group."
                )
            offset = 0
        if offset < low or offset > high:
            raise ValueError(
                "Queue checkpoint is outside retained history. Capture coverage must be reviewed."
            )
        assigned.append(TopicPartition(part.topic, part.partition, offset))
    consumer.assign(assigned)


def save(db, flow, offsets):
    old_autocommit = db.autocommit
    try:
        db.autocommit = False
        for (topic, part), offset in sorted(offsets.items()):
            query(
                db,
                f"INSERT INTO {quote(flow['target_schema'])}.audit_delivery_checkpoints "
                "(flow_id,topic,partition_id,next_offset) VALUES (%s,%s,%s,%s) "
                "ON DUPLICATE KEY UPDATE next_offset=GREATEST(next_offset,%s)",
                (flow["id"], topic, part, offset, offset),
            )
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        db.autocommit = old_autocommit
