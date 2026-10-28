"""Native OCI Streaming cursors with durable target-side partition checkpoints.

One writer owns every partition for its flow under a target MySQL advisory lock.
Cursors are ephemeral; restart always uses the last committed target checkpoint.
"""

import base64
import time
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone

import oci

from .repository import query
from .validation import quote


@dataclass
class StreamPartition:
    topic: str
    partition: int
    offset: int = -1
    error: object = None


class NativeMessage:
    def __init__(self, name, record):
        self.name, self.record = name, record
        self.decoded = {}

    def topic(self):
        return self.name

    def partition(self):
        return int(self.record.partition)

    def offset(self):
        return int(self.record.offset)

    def key(self):
        if "key" not in self.decoded:
            self.decoded["key"] = (
                base64.b64decode(self.record.key, validate=True) if self.record.key else None
            )
        return self.decoded["key"]

    def value(self):
        if "value" not in self.decoded:
            self.decoded["value"] = (
                base64.b64decode(self.record.value, validate=True) if self.record.value else None
            )
        return self.decoded["value"]

    def error(self):
        return None


def data_client(queue, endpoint, *, resource_principal=False):
    signer = (
        oci.auth.signers.get_resource_principals_signer()
        if resource_principal
        else oci.auth.signers.InstancePrincipalsSecurityTokenSigner()
    )
    return oci.streaming.StreamClient(
        {"region": queue["region"]},
        signer=signer,
        service_endpoint=endpoint,
        timeout=(5, 15),
        retry_strategy=oci.retry.NoneRetryStrategy(),
    )


class NativeConsumer:
    def __init__(
        self,
        queue,
        *,
        resource_principal=False,
        client_factory=None,
        clock=time.monotonic,
        poll_policy=None,
    ):
        if queue.get("kind") != "oci_streaming":
            raise ValueError("Only native OCI Streaming is supported.")
        self.queue, self.clock = queue, clock
        factory = client_factory or (
            lambda endpoint: data_client(queue, endpoint, resource_principal=resource_principal)
        )
        self.streams = queue.get("streams", {})
        self.clients = {
            name: factory(stream["messages_endpoint"]) for name, stream in self.streams.items()
        }
        self.cursors, self.last_read = {}, {}
        self.pending, self.lanes = deque(), deque()
        self.acknowledged = {}
        policy = poll_policy or {}
        self.get_limit = policy.get("get_max_records", 1000)
        self.idle_max = policy.get("get_idle_max_ms", 2000) / 1000
        self.byte_rate = policy.get("get_bytes_per_second", 1500000)
        self.read_delay = {}
        self.failures = {}
        self.counters = {
            "get_requests": 0,
            "get_records": 0,
            "get_wire_bytes": 0,
            "empty_gets": 0,
            "get_retries": 0,
            "get_throttled": 0,
        }

    def metrics(self):
        return dict(self.counters)

    def attach(self, db, flow):
        """Called only after the process acquires the flow's target-side writer lock."""
        owned = []
        for name in (flow["topic"], flow["schema_topic"]):
            if name not in self.streams:
                raise ValueError("Prepare this flow's OCI streams before starting the writer.")
            stream = self.streams[name]
            if name == flow["schema_topic"] and stream["partitions"] != 1:
                raise ValueError("Schema stream must have exactly one partition.")
            for partition in range(stream["partitions"]):
                checkpoint = query(
                    db,
                    "SELECT next_offset FROM "
                    + quote(flow["target_schema"])
                    + ".audit_delivery_checkpoints WHERE flow_id=%s AND topic=%s AND partition_id=%s",
                    (flow["id"], name, partition),
                    one=True,
                )
                offset = int(checkpoint["next_offset"]) if checkpoint else 0
                if offset < 0:
                    raise ValueError("The durable stream checkpoint is invalid.")
                if offset == 0:
                    created = datetime.fromisoformat(stream["time_created"].replace("Z", "+00:00"))
                    age = (datetime.now(timezone.utc) - created).total_seconds()
                    if age >= stream["retention_in_hours"] * 3600:
                        raise ValueError(
                            "Stream origin may have expired and no checkpoint exists. Review capture coverage."
                        )
                details = oci.streaming.models.CreateCursorDetails(
                    partition=str(partition),
                    type="AFTER_OFFSET" if offset else "TRIM_HORIZON",
                    offset=offset - 1 if offset else None,
                )
                # AFTER_OFFSET uses the actual last processed offset, so sparse
                # native offsets are never treated as consecutive message counts.
                cursor = self.clients[name].create_cursor(stream["id"], details).data.value
                lane = name, partition
                self.cursors[lane] = cursor
                self.lanes.append(lane)
                self.acknowledged[lane] = offset
                owned.append(StreamPartition(name, partition, offset))
        return owned

    def poll(self, timeout=0.1):
        if self.pending:
            return self.pending.popleft()
        deadline = self.clock() + timeout
        while self.clock() < deadline and self.lanes:
            for _ in range(len(self.lanes)):
                lane = self.lanes[0]
                self.lanes.rotate(-1)
                now = self.clock()
                if now - self.last_read.get(lane, -1000) < self.read_delay.get(lane, 0.25):
                    continue
                name, _partition = lane
                self.counters["get_requests"] += 1
                try:
                    response = self.clients[name].get_messages(
                        self.streams[name]["id"], self.cursors[lane], limit=self.get_limit
                    )
                except oci.exceptions.ServiceError as error:
                    self.last_read[lane] = self.clock()
                    retries = self.failures.get(lane, 0)
                    if error.status not in {429, 500, 502, 503, 504} or retries >= 4:
                        raise
                    self.failures[lane] = retries + 1
                    self.counters["get_retries"] += 1
                    self.counters["get_throttled"] += int(error.status == 429)
                    self.read_delay[lane] = min(10, max(0.5, self.read_delay.get(lane, 0.25) * 2))
                    continue
                self.failures[lane] = 0
                self.last_read[lane] = self.clock()
                count = len(response.data)
                size = sum(
                    len(record.value or "") + len(record.key or "") + 128
                    for record in response.data
                )
                self.counters["get_records"] += count
                self.counters["get_wire_bytes"] += size
                if count:
                    self.read_delay[lane] = max(0.25, size / self.byte_rate)
                else:
                    self.counters["empty_gets"] += 1
                    self.read_delay[lane] = min(self.idle_max, self.read_delay.get(lane, 0.25) * 2)
                following = response.headers.get("opc-next-cursor")
                if not following:
                    raise ValueError(
                        "OCI did not return a continuation cursor; checkpoint was preserved."
                    )
                self.cursors[lane] = following
                for record in response.data:
                    if int(record.partition) != lane[1]:
                        raise ValueError("OCI returned a message for an unowned partition.")
                    self.pending.append(NativeMessage(name, record))
                if self.pending:
                    return self.pending.popleft()
            time.sleep(min(0.02, max(0, deadline - self.clock())))
        return None

    def acknowledge(self, offsets):
        # Individual OCI cursors have no broker commit. This method is called
        # only after MySQL commits the durable delivery checkpoint.
        for lane, offset in offsets.items():
            if lane not in self.acknowledged or offset < self.acknowledged[lane]:
                raise ValueError("Cannot move the durable stream position backward.")
            self.acknowledged[lane] = offset

    def close(self):
        for client in self.clients.values():
            client.base_client.session.close()
