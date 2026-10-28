"""Bounded native message framing; service limits cannot be increased here."""

DEFAULT_EVENT_BYTES = 16 * 1024 * 1024
MAX_EVENT_BYTES = 64 * 1024 * 1024
PUBLISH_BYTES = 900000
CHUNK_BYTES = 400000


def policy(config):
    streaming = config.get("streaming", {})
    result = {
        "max_event_bytes": streaming.get("max_event_bytes", DEFAULT_EVENT_BYTES),
        "max_pending_bytes": streaming.get("max_pending_fragment_bytes", 128 * 1024 * 1024),
    }
    if (
        type(result["max_event_bytes"]) is not int
        or not 1024 * 1024 <= result["max_event_bytes"] <= MAX_EVENT_BYTES
    ):
        raise ValueError("Streaming max_event_bytes must be between 1 MiB and 64 MiB.")
    if (
        type(result["max_pending_bytes"]) is not int
        or not result["max_event_bytes"] <= result["max_pending_bytes"] <= 1024 * 1024 * 1024
    ):
        raise ValueError("Pending fragment bytes must cover one event and be at most 1 GiB.")
    for name, default, minimum, maximum in (
        ("put_max_records", 500, 1, 1000),
        ("put_requests_per_second", 4, 1, 20),
        ("put_bytes_per_second", 850000, 65536, 64000000),
        ("get_max_records", 1000, 1, 1000),
        ("get_idle_max_ms", 2000, 250, 10000),
        ("get_bytes_per_second", 1500000, 65536, 1800000),
    ):
        value = streaming.get(name, default)
        if type(value) is not int or not minimum <= value <= maximum:
            raise ValueError(
                "Invalid Streaming "
                + name
                + ". Use an integer from "
                + str(minimum)
                + " to "
                + str(maximum)
                + "."
            )
        result[name] = value
    return result
