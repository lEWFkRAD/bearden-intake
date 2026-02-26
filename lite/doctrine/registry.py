"""Doctrine Registry — computes and caches the current Doctrine manifest.

The registry is the single source of truth for "what governance rules
are active right now." It reads from the Ardent ruleset, contracts schemas,
rule map, and Lens defaults to produce a deterministic composite hash.

Pure computation — no I/O aside from importing sibling modules.
"""

from lite.doctrine.fingerprint import canonical_json, sha256_text, hash_dict
from lite.doctrine.manifest import DoctrineManifest

# ── Current Doctrine version (bump on any governance change) ──
CURRENT_DOCTRINE_VERSION = "0.1.0"

# ── Ardent ruleset definition ──
# These will be replaced by real imports when the full engine ships.
ARDENT_RULESET_VERSION = "0.1.0"
ARDENT_RULESET = {
    "version": ARDENT_RULESET_VERSION,
    "rules": {
        "TAX-001": {"name": "W2 wage cross-check", "severity": "error", "doc_types": ["W-2"]},
        "TAX-002": {"name": "1099-R distribution validation", "severity": "warning", "doc_types": ["1099-R"]},
        "TAX-003": {"name": "SSN format validation", "severity": "critical", "doc_types": ["W-2", "1099-INT", "1099-DIV", "1099-R"]},
        "TAX-004": {"name": "Federal withholding bounds", "severity": "warning", "doc_types": ["W-2", "1099-R", "1099-INT"]},
        "TAX-005": {"name": "Duplicate document detection", "severity": "error", "doc_types": ["*"]},
        "TAX-006": {"name": "K-1 income reconciliation", "severity": "warning", "doc_types": ["K-1"]},
        "TAX-007": {"name": "1099-NEC threshold check", "severity": "info", "doc_types": ["1099-NEC"]},
        "TAX-008": {"name": "EIN format validation", "severity": "error", "doc_types": ["W-2", "1099-INT", "1099-DIV", "1099-R", "K-1"]},
    },
}

# ── Contracts schema versions (document_type → version) ──
CONTRACTS_SCHEMA_VERSIONS = {
    "W-2": "1.0.0",
    "1099-INT": "1.0.0",
    "1099-DIV": "1.0.0",
    "1099-R": "1.0.0",
    "K-1": "1.0.0",
    "1099-NEC": "1.0.0",
    "1099-MISC": "1.0.0",
    "SSA-1099": "1.0.0",
}

# ── Rule map (rule_id → applicable document types) ──
RULE_MAP = {
    rule_id: rule_def["doc_types"]
    for rule_id, rule_def in ARDENT_RULESET["rules"].items()
}

# ── Lens defaults ──
LENS_VERSION = "0.1.0"

# ── Cached manifest (computed once per process) ──
_cached_manifest = None


def _compute_doctrine_hash() -> str:
    """Compute the composite Doctrine hash from all governance inputs.

    The hash changes if ANY of these change:
    - Ardent ruleset version or rules
    - Contracts schema versions
    - Rule map structure
    - Lens version
    """
    composite = canonical_json({
        "ardent_ruleset_version": ARDENT_RULESET_VERSION,
        "ardent_ruleset_hash": hash_dict(ARDENT_RULESET),
        "contracts_schema_versions": CONTRACTS_SCHEMA_VERSIONS,
        "rule_map_hash": hash_dict(RULE_MAP),
        "lens_version": LENS_VERSION,
    })
    return sha256_text(composite)


def get_current_manifest() -> DoctrineManifest:
    """Return the current DoctrineManifest, computing and caching on first call.

    The manifest is deterministic: same code always produces the same hash.
    """
    global _cached_manifest
    if _cached_manifest is not None:
        return _cached_manifest

    ardent_ruleset_hash = hash_dict(ARDENT_RULESET)
    rule_map_hash = hash_dict(RULE_MAP)
    doctrine_hash = _compute_doctrine_hash()

    _cached_manifest = DoctrineManifest(
        doctrine_version=CURRENT_DOCTRINE_VERSION,
        doctrine_hash=doctrine_hash,
        ardent_ruleset_version=ARDENT_RULESET_VERSION,
        ardent_ruleset_hash=ardent_ruleset_hash,
        contracts_schema_versions=CONTRACTS_SCHEMA_VERSIONS,
        lens_version=LENS_VERSION,
        rule_map_hash=rule_map_hash,
        notes="Initial Doctrine v0 — governance fingerprint foundation.",
    )
    return _cached_manifest


def invalidate_cache():
    """Force re-computation of the manifest on next call.

    Useful in tests or after dynamic rule map changes.
    """
    global _cached_manifest
    _cached_manifest = None
