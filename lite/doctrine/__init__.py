"""Lite Doctrine — deterministic, tamper-evident governance fingerprinting."""

from lite.doctrine.fingerprint import canonical_json, sha256_text, hash_dict
from lite.doctrine.manifest import DoctrineManifest
from lite.doctrine.registry import CURRENT_DOCTRINE_VERSION, get_current_manifest
from lite.doctrine.drift import doctrine_drift_status

__all__ = [
    "canonical_json",
    "sha256_text",
    "hash_dict",
    "DoctrineManifest",
    "CURRENT_DOCTRINE_VERSION",
    "get_current_manifest",
    "doctrine_drift_status",
]
