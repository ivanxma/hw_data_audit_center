"""Target-side delivery checkpoints detect expired queue history and lost group offsets."""

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
