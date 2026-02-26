"""Ardent — rule-based evaluation engine for tax document validation.

This module defines the ArdentResult model and the evaluate() entry point.
Doctrine governance fields are wired in at evaluation time.
"""

from datetime import datetime, timezone
from typing import Any, List, Optional

from pydantic import BaseModel, Field


class Finding(BaseModel):
    """A single rule evaluation result."""

    rule_id: str
    rule_name: Optional[str] = None
    severity: str = "info"  # critical | error | warning | info
    message: Optional[str] = None
    passed: bool = True
    evidence: List[dict] = Field(default_factory=list)


class ArdentResult(BaseModel):
    """Complete result of an Ardent evaluation pass.

    Doctrine fields (doctrine_version, doctrine_hash) are optional and
    set by the engine when Doctrine is enabled. Old results without
    these fields remain fully compatible.
    """

    ruleset_id: str = ""
    ruleset_hash: str = ""
    total_rules_evaluated: int = 0
    rules_passed: int = 0
    rules_failed: int = 0
    evaluation_duration_ms: float = 0.0
    evaluated_at: Optional[datetime] = None
    findings: List[Finding] = Field(default_factory=list)

    # ── Doctrine governance fields (optional, non-breaking) ──
    doctrine_version: Optional[str] = Field(
        default=None,
        description="Doctrine SemVer at evaluation time (e.g., '0.1.0').",
    )
    doctrine_hash: Optional[str] = Field(
        default=None,
        description="Doctrine composite hash at evaluation time.",
    )


def evaluate(candidates: Any, context: Any) -> ArdentResult:
    """Evaluate candidates against the Ardent ruleset.

    This is the main entry point called by the extraction pipeline.
    Doctrine fields are set explicitly from the Doctrine registry.

    Args:
        candidates: Extraction candidates from the adapter layer.
        context: Lens bundle with prior-year data and validation context.

    Returns:
        ArdentResult with rule evaluations and doctrine fingerprint.
    """
    start = datetime.now(timezone.utc)

    # ── Import Doctrine registry (pure computation, no I/O) ──
    try:
        from lite.doctrine.registry import get_current_manifest
        manifest = get_current_manifest()
        doctrine_version = manifest.doctrine_version
        doctrine_hash = manifest.doctrine_hash
        ruleset_id = f"ardent-{manifest.ardent_ruleset_version}"
        ruleset_hash = manifest.ardent_ruleset_hash
    except Exception:
        # Doctrine unavailable — proceed without governance fingerprint
        doctrine_version = None
        doctrine_hash = None
        ruleset_id = "ardent-unknown"
        ruleset_hash = ""

    # ── Rule evaluation (stub — real rules will be wired in later) ──
    findings: List[Finding] = []
    # TODO: Iterate over ARDENT_RULESET rules and evaluate each against candidates

    end = datetime.now(timezone.utc)
    duration_ms = (end - start).total_seconds() * 1000

    passed = sum(1 for f in findings if f.passed)
    failed = sum(1 for f in findings if not f.passed)

    return ArdentResult(
        ruleset_id=ruleset_id,
        ruleset_hash=ruleset_hash,
        total_rules_evaluated=len(findings),
        rules_passed=passed,
        rules_failed=failed,
        evaluation_duration_ms=duration_ms,
        evaluated_at=end,
        findings=findings,
        doctrine_version=doctrine_version,
        doctrine_hash=doctrine_hash,
    )
