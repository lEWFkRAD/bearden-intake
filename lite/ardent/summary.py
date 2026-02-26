"""Ardent Summary — builds a UI-facing summary from ArdentResult.

The summary is what gets displayed in the Lite Findings panel.
"""

from typing import Optional

from pydantic import BaseModel, Field


class ArdentSummary(BaseModel):
    """UI-facing summary of an Ardent evaluation."""

    blocked: bool = False
    needs_review: bool = False
    findings: list = Field(default_factory=list)
    critical_count: int = 0
    error_count: int = 0
    warning_count: int = 0
    info_count: int = 0
    verification_requests_count: int = 0
    ruleset_version: Optional[str] = None
    evaluated_at: Optional[str] = None
    deterministic_match_pct: Optional[float] = None
    schema_version: Optional[str] = "1.0.0"

    # ── Doctrine fields (pass-through from ArdentResult) ──
    doctrine_version: Optional[str] = None
    doctrine_hash: Optional[str] = None


def build_ardent_summary(
    ardent_result,
    evaluated_at_iso: str = "",
    deterministic_match_pct: Optional[float] = None,
) -> ArdentSummary:
    """Build a UI-friendly summary from an ArdentResult.

    Args:
        ardent_result: The ArdentResult from evaluate().
        evaluated_at_iso: ISO timestamp string.
        deterministic_match_pct: Match percentage from diff analysis.

    Returns:
        ArdentSummary ready for JSON serialization and UI rendering.
    """
    findings = []
    critical = error = warning = info = 0

    if hasattr(ardent_result, "findings"):
        for f in ardent_result.findings:
            finding_dict = f.model_dump() if hasattr(f, "model_dump") else dict(f)
            findings.append(finding_dict)
            sev = finding_dict.get("severity", "info")
            if sev == "critical":
                critical += 1
            elif sev == "error":
                error += 1
            elif sev == "warning":
                warning += 1
            else:
                info += 1

    blocked = critical > 0
    needs_review = error > 0 or warning > 0

    # Extract doctrine fields from result (if present)
    doctrine_version = getattr(ardent_result, "doctrine_version", None)
    doctrine_hash = getattr(ardent_result, "doctrine_hash", None)

    # Ruleset version from ruleset_id (e.g., "ardent-0.1.0" → "0.1.0")
    ruleset_version = None
    if hasattr(ardent_result, "ruleset_id") and ardent_result.ruleset_id:
        parts = ardent_result.ruleset_id.split("-", 1)
        ruleset_version = parts[1] if len(parts) > 1 else ardent_result.ruleset_id

    return ArdentSummary(
        blocked=blocked,
        needs_review=needs_review,
        findings=findings,
        critical_count=critical,
        error_count=error,
        warning_count=warning,
        info_count=info,
        verification_requests_count=0,
        ruleset_version=ruleset_version,
        evaluated_at=evaluated_at_iso or None,
        deterministic_match_pct=deterministic_match_pct,
        doctrine_version=doctrine_version,
        doctrine_hash=doctrine_hash,
    )
