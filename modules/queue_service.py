"""Read native OCI Streaming evidence without changing writer checkpoints."""

import json
from datetime import datetime, timedelta, timezone

import oci

from .native_streaming import NativeMessage, data_client


def explore(flow, cloud, limit=25):
    queue = cloud.validate_flow(flow)
    name = flow["topic"]
    stream = queue["streams"][name]
    client = data_client(queue, stream["messages_endpoint"])
    records = []
    try:
        # A bounded recent-time sample; sparse offsets are not message counts.
        created = datetime.fromisoformat(stream["time_created"].replace("Z", "+00:00"))
        start = max(datetime.now(timezone.utc) - timedelta(minutes=5), created)
        for partition in range(min(stream["partitions"], 16)):
            cursor = client.create_cursor(stream["id"], oci.streaming.models.CreateCursorDetails(
                type="AT_TIME", partition=str(partition), time=start,
            )).data.value
            result = client.get_messages(stream["id"], cursor, limit=limit - len(records))
            for raw in result.data:
                message = NativeMessage(name, raw)
                records.append({"topic": name, "partition": message.partition(), "offset": message.offset(),
                                "key": message.key().decode(errors="replace") if message.key() else None,
                                "value": json.loads(message.value()) if message.value() else None})
            if len(records) >= limit:
                break
        return records
    finally:
        client.base_client.session.close()


def validate_registered_topics(flow, cloud):
    cloud.validate_flow(flow)
