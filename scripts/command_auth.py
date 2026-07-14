#!/usr/bin/env python3
"""Shared HMAC signing/verification for the fallback control plane.

Both the outbound side (``meshtastic_fallback_worker.py`` signing queued
commands) and the inbound side (``command_responder.py`` authenticating them)
use this module so the authenticated byte string is defined in exactly one
place.

The canonical message is compact JSON of the record with the ``auth_tag`` field
removed and keys sorted. This matches the existing ACK convention used by
``meshtastic_fallback_worker.ack_message`` so operators only have to reason
about one signing scheme across the whole control plane.

The wire authenticator can be truncated (``tag_len``) to fit the small LoRa
text budget while the durable queue record keeps the full-length tag as
evidence. ``tag_len`` counts hex characters; the default keeps the full
SHA-256 digest.
"""
from __future__ import annotations

import hashlib
import hmac
import json
from typing import Any

AUTH_ALG = "HMAC-SHA256"
AUTH_TAG_FIELD = "auth_tag"
# Fields that participate in the signature. Anything outside this set (transport
# bookkeeping such as send_attempts, status, last_transport_result, ...) is
# intentionally excluded so the tag stays stable across the command lifecycle.
SIGNED_FIELDS = (
    "command_id",
    "nonce",
    "sender",
    "issued_at_utc",
    "ttl_sec",
    "target",
    "command",
)
FULL_TAG_LEN = hashlib.sha256().digest_size * 2  # 64 hex chars


def canonical_bytes(record: dict[str, Any]) -> bytes:
    """Return the canonical authenticated byte string for ``record``.

    Only :data:`SIGNED_FIELDS` that are present are included. The ``auth_tag``
    field is always excluded. Keys are sorted and JSON is compact so the same
    logical record always hashes identically regardless of dict ordering.
    """
    payload = {
        key: record[key]
        for key in SIGNED_FIELDS
        if key in record and record[key] is not None
    }
    return json.dumps(
        payload, sort_keys=True, separators=(",", ":"), allow_nan=False
    ).encode("utf-8")


def sign(key: bytes, record: dict[str, Any], *, tag_len: int = FULL_TAG_LEN) -> str:
    """Compute the lowercase hex HMAC tag for ``record``.

    ``tag_len`` truncates the returned hex string (must be even and no longer
    than the full digest). Truncation trades authenticator strength for wire
    size; 16 hex chars (64 bits) is a common constrained-link choice.
    """
    if key == b"":
        raise ValueError("signing key is empty")
    if tag_len <= 0 or tag_len > FULL_TAG_LEN or tag_len % 2 != 0:
        raise ValueError(f"tag_len must be an even value in 1..{FULL_TAG_LEN}")
    digest = hmac.new(key, canonical_bytes(record), hashlib.sha256).hexdigest()
    return digest[:tag_len]


def verify(key: bytes, record: dict[str, Any], *, tag_len: int = FULL_TAG_LEN) -> bool:
    """Constant-time verification of ``record[auth_tag]`` against ``key``.

    The comparison length is driven by the supplied tag so a caller that signs
    with a truncated tag verifies against the same truncation. A missing or
    non-string ``auth_tag`` fails closed.
    """
    supplied = record.get(AUTH_TAG_FIELD)
    if not isinstance(supplied, str) or supplied == "":
        return False
    effective_len = min(tag_len, len(supplied))
    if effective_len <= 0 or effective_len > FULL_TAG_LEN:
        return False
    try:
        expected = sign(key, record, tag_len=effective_len)
    except ValueError:
        return False
    # compare_digest over equal-length lowercase hex; also guard the supplied
    # length so a short forged tag cannot match a prefix of the real one.
    return len(supplied) == len(expected) and hmac.compare_digest(supplied, expected)
