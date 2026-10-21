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
# Minimum accepted authenticator strength on the wire. 16 hex chars = 64 bits.
# Shorter tags are rejected outright rather than negotiated down.
MIN_TAG_LEN = 16

# Domain separation. The authenticated byte string is prefixed with the message
# direction so an ACK can never verify as a command (and vice versa) even
# though both are signed over the same field set with the same key.
DOMAIN_COMMAND = "command"
DOMAIN_ACK = "ack"
_DOMAINS = (DOMAIN_COMMAND, DOMAIN_ACK)


def canonical_bytes(
    record: dict[str, Any], *, domain: str = DOMAIN_COMMAND
) -> bytes:
    """Return the canonical authenticated byte string for ``record``.

    Only :data:`SIGNED_FIELDS` that are present are included. The ``auth_tag``
    field is always excluded. Keys are sorted and JSON is compact so the same
    logical record always hashes identically regardless of dict ordering.

    ``domain`` is bound into the signed bytes (see :data:`DOMAIN_COMMAND` /
    :data:`DOMAIN_ACK`) so a signature minted for one direction of the control
    plane cannot be replayed as the other.
    """
    if domain not in _DOMAINS:
        raise ValueError(f"domain must be one of {_DOMAINS!r}")
    payload = {
        key: record[key]
        for key in SIGNED_FIELDS
        if key in record and record[key] is not None
    }
    body = json.dumps(
        payload, sort_keys=True, separators=(",", ":"), allow_nan=False
    ).encode("utf-8")
    return domain.encode("ascii") + b"\x00" + body


def sign(
    key: bytes,
    record: dict[str, Any],
    *,
    tag_len: int = FULL_TAG_LEN,
    domain: str = DOMAIN_COMMAND,
) -> str:
    """Compute the lowercase hex HMAC tag for ``record``.

    ``tag_len`` truncates the returned hex string (must be even and no longer
    than the full digest). Truncation trades authenticator strength for wire
    size; 16 hex chars (64 bits) is a common constrained-link choice.
    """
    if key == b"":
        raise ValueError("signing key is empty")
    if tag_len < MIN_TAG_LEN or tag_len > FULL_TAG_LEN or tag_len % 2 != 0:
        raise ValueError(
            f"tag_len must be an even value in {MIN_TAG_LEN}..{FULL_TAG_LEN}"
        )
    digest = hmac.new(
        key, canonical_bytes(record, domain=domain), hashlib.sha256
    ).hexdigest()
    return digest[:tag_len]


def verify(
    key: bytes,
    record: dict[str, Any],
    *,
    tag_len: int = FULL_TAG_LEN,
    domain: str = DOMAIN_COMMAND,
) -> bool:
    """Constant-time verification of ``record[auth_tag]`` against ``key``.

    The comparison length is the *locally configured* ``tag_len``, never a
    length derived from the supplied tag. A tag of any other length is
    rejected without comparison, so a sender cannot downgrade verification
    strength by truncating its own authenticator. ``tag_len`` must itself be at
    least :data:`MIN_TAG_LEN`.

    A missing or non-string ``auth_tag`` fails closed, as does a tag containing
    anything other than lowercase hex.
    """
    supplied = record.get(AUTH_TAG_FIELD)
    if not isinstance(supplied, str) or supplied == "":
        return False
    if tag_len < MIN_TAG_LEN or tag_len > FULL_TAG_LEN or tag_len % 2 != 0:
        return False
    # Length is pinned by local policy: reject before any comparison so a
    # short forged tag can never be matched against a prefix of the real one.
    if len(supplied) != tag_len:
        return False
    if any(c not in "0123456789abcdef" for c in supplied):
        return False
    try:
        expected = sign(key, record, tag_len=tag_len, domain=domain)
    except ValueError:
        return False
    return hmac.compare_digest(supplied, expected)
