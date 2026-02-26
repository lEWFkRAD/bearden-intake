"""Deterministic hashing primitives for Doctrine fingerprinting.

All functions are pure (no I/O) and produce identical output across runs
given identical input — the foundation of tamper-evidence.
"""

import hashlib
import json
from typing import Any


def canonical_json(obj: Any) -> str:
    """Serialize an object to a canonical JSON string.

    Guarantees:
    - Keys sorted recursively
    - Stable separators (no trailing whitespace)
    - Consistent float formatting via default=str
    - Deterministic across Python versions
    """
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), default=str)


def sha256_text(text: str) -> str:
    """SHA-256 hash of a UTF-8 string, returned as lowercase hex."""
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def hash_dict(d: dict) -> str:
    """Hash a dictionary via canonical JSON → SHA-256.

    This is the standard way to fingerprint any structured data
    (rule maps, schema definitions, config objects, etc.).
    """
    return sha256_text(canonical_json(d))
