"""Stable typed JSON fingerprints shared by ingestion and history lookup."""

import hashlib
import hmac
import json


def canonical(value):
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
    )


def row_hash(value, flow, fingerprint_key):
    if not isinstance(fingerprint_key, bytes) or not fingerprint_key:
        raise ValueError("A non-empty fingerprint secret is required.")
    namespace = [flow["source_identity"], flow["source_schema"], flow["source_table"], value]
    return hmac.new(fingerprint_key, canonical(namespace).encode(), hashlib.sha256).hexdigest()


def digest(value):
    return hashlib.sha256(canonical(value).encode()).hexdigest()
