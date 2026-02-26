"""DoctrineManifest — the Pydantic model for a Doctrine fingerprint.

A manifest captures the complete governance state at a point in time:
which ruleset version, which contracts schemas, which Lens defaults,
and the composite hash that ties them all together.
"""

from datetime import datetime, timezone
from typing import Dict, Optional

from pydantic import BaseModel, Field


class DoctrineManifest(BaseModel):
    """Immutable record of the governance configuration at evaluation time."""

    schema_version: str = Field(
        default="1.0.0",
        description="Version of the DoctrineManifest schema itself.",
    )

    doctrine_version: str = Field(
        description="SemVer version of the Doctrine (e.g., '0.1.0').",
    )

    doctrine_hash: str = Field(
        description="SHA-256 composite hash of all governance inputs.",
    )

    created_at: str = Field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat(),
        description="ISO-8601 timestamp when this manifest was generated.",
    )

    # ── Ardent ruleset ──
    ardent_ruleset_version: str = Field(
        description="SemVer version of the Ardent ruleset.",
    )
    ardent_ruleset_hash: str = Field(
        description="SHA-256 hash of the serialized Ardent ruleset.",
    )

    # ── Contracts (document-type schemas) ──
    contracts_schema_versions: Dict[str, str] = Field(
        default_factory=dict,
        description="Map of document_type → schema version (e.g., {'W-2': '1.0', '1099-INT': '1.0'}).",
    )

    # ── Lens ──
    lens_version: str = Field(
        default="",
        description="Lens intent set version or hash signature.",
    )

    # ── Rule map ──
    rule_map_hash: str = Field(
        description="SHA-256 hash of the sorted rule_map (rule_id → document_types).",
    )

    # ── Notes ──
    notes: str = Field(
        default="",
        description="Human-readable notes about this Doctrine version.",
    )

    model_config = {"frozen": True}
