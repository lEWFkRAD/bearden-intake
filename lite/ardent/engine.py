"""Ardent — rule-based evaluation engine for tax document validation.

This module defines the ArdentResult model and the evaluate() entry point.
Doctrine governance fields are wired in at evaluation time.

Rules (TAX-001 through TAX-008) are deterministic, pure-function checks
that run against CandidateFact objects. Each rule produces a Finding
with pass/fail, severity, message, and evidence.
"""

import re
from collections import defaultdict
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


# ═══════════════════════════════════════════════════════════════════════════════
# Candidate field helpers — candidates may be CandidateFact or plain dicts
# ═══════════════════════════════════════════════════════════════════════════════


def _get_doc_type(c: Any) -> str:
    """Extract document_type from a candidate."""
    if hasattr(c, "document_type"):
        return c.document_type
    if isinstance(c, dict):
        return c.get("document_type", "")
    return ""


def _get_fields(c: Any) -> dict:
    """Extract fields dict from a candidate.

    Returns {field_name: value} — unwraps FieldExtraction objects if needed.
    """
    raw = {}
    if hasattr(c, "fields"):
        raw = c.fields if isinstance(c.fields, dict) else {}
    elif isinstance(c, dict):
        raw = c.get("fields", {})

    result = {}
    for k, v in raw.items():
        if hasattr(v, "value"):
            result[k] = v.value
        elif isinstance(v, dict) and "value" in v:
            result[k] = v["value"]
        else:
            result[k] = v
    return result


def _get_ein(c: Any) -> str:
    """Extract EIN from a candidate."""
    if hasattr(c, "payer_ein"):
        return c.payer_ein or ""
    if isinstance(c, dict):
        return c.get("payer_ein", "") or ""
    return ""


def _get_entity(c: Any) -> str:
    """Extract entity name from a candidate."""
    if hasattr(c, "payer_or_entity"):
        return c.payer_or_entity or ""
    if isinstance(c, dict):
        return c.get("payer_or_entity", "") or ""
    return ""


def _safe_float(val: Any) -> Optional[float]:
    """Coerce a value to float, returning None on failure."""
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


# ═══════════════════════════════════════════════════════════════════════════════
# Rule implementations
# ═══════════════════════════════════════════════════════════════════════════════


def _eval_tax001(candidates: list) -> List[Finding]:
    """TAX-001: W-2 wage cross-check.

    Checks:
    - Federal withholding should not exceed wages
    - SS wages and Medicare wages should be within bounds of wages
    """
    findings = []
    w2s = [c for c in candidates if _get_doc_type(c) == "W-2"]
    if not w2s:
        return findings

    for c in w2s:
        fields = _get_fields(c)
        entity = _get_entity(c) or fields.get("employer_name", "Unknown")
        wages = _safe_float(fields.get("wages"))
        federal_wh = _safe_float(fields.get("federal_wh"))
        ss_wages = _safe_float(fields.get("ss_wages"))
        medicare_wages = _safe_float(fields.get("medicare_wages"))

        evidence = []
        passed = True

        if wages is not None and federal_wh is not None:
            if federal_wh > wages:
                passed = False
                evidence.append({
                    "field": "federal_wh",
                    "issue": f"Federal withholding (${federal_wh:,.2f}) exceeds wages (${wages:,.2f})",
                    "entity": entity,
                })

        if wages is not None and ss_wages is not None:
            # SS wage base cap is ~$168,600 for 2025 — wages above that are normal
            # but ss_wages should not exceed wages
            if ss_wages > wages * 1.01:  # 1% tolerance for rounding
                passed = False
                evidence.append({
                    "field": "ss_wages",
                    "issue": f"SS wages (${ss_wages:,.2f}) exceed total wages (${wages:,.2f})",
                    "entity": entity,
                })

        if wages is not None and medicare_wages is not None:
            if medicare_wages > wages * 1.05:  # 5% tolerance (incl. tips, etc.)
                passed = False
                evidence.append({
                    "field": "medicare_wages",
                    "issue": f"Medicare wages (${medicare_wages:,.2f}) significantly exceed wages (${wages:,.2f})",
                    "entity": entity,
                })

        findings.append(Finding(
            rule_id="TAX-001",
            rule_name="W2 wage cross-check",
            severity="error",
            passed=passed,
            message=f"W-2 cross-check {'passed' if passed else 'FAILED'} for {entity}",
            evidence=evidence,
        ))

    return findings


def _eval_tax002(candidates: list) -> List[Finding]:
    """TAX-002: 1099-R distribution validation.

    Checks:
    - Taxable amount should not exceed gross distribution
    - Distribution code should be a known code
    """
    VALID_CODES = {
        "1", "2", "3", "4", "5", "6", "7", "8", "9", "A", "B", "C", "D",
        "E", "F", "G", "H", "J", "K", "L", "M", "N", "P", "Q", "R", "S",
        "T", "U", "W",
    }
    findings = []
    docs = [c for c in candidates if _get_doc_type(c) == "1099-R"]
    if not docs:
        return findings

    for c in docs:
        fields = _get_fields(c)
        entity = _get_entity(c) or "Unknown payer"
        gross = _safe_float(fields.get("gross_distribution"))
        taxable = _safe_float(fields.get("taxable_amount"))
        code = str(fields.get("distribution_code", "")).strip().upper()

        evidence = []
        passed = True

        if gross is not None and taxable is not None:
            if taxable > gross * 1.001:  # tiny float tolerance
                passed = False
                evidence.append({
                    "field": "taxable_amount",
                    "issue": f"Taxable amount (${taxable:,.2f}) exceeds gross distribution (${gross:,.2f})",
                    "entity": entity,
                })

        if code and code not in VALID_CODES:
            passed = False
            evidence.append({
                "field": "distribution_code",
                "issue": f"Unknown distribution code '{code}'",
                "entity": entity,
            })

        findings.append(Finding(
            rule_id="TAX-002",
            rule_name="1099-R distribution validation",
            severity="warning",
            passed=passed,
            message=f"1099-R validation {'passed' if passed else 'FAILED'} for {entity}",
            evidence=evidence,
        ))

    return findings


_SSN_PATTERN = re.compile(r"^\d{3}-?\d{2}-?\d{4}$")
_SSN_INVALID_PREFIXES = {"000", "666", "9"}


def _eval_tax003(candidates: list) -> List[Finding]:
    """TAX-003: SSN format validation.

    Checks SSN/TIN fields for valid format (XXX-XX-XXXX).
    Flags invalid prefixes (000, 666, 9xx) per IRS rules.
    """
    TARGET_TYPES = {"W-2", "1099-INT", "1099-DIV", "1099-R"}
    findings = []
    docs = [c for c in candidates if _get_doc_type(c) in TARGET_TYPES]
    if not docs:
        return findings

    for c in docs:
        fields = _get_fields(c)
        entity = _get_entity(c) or "Unknown"
        doc_type = _get_doc_type(c)

        # Look for SSN-like fields
        ssn_fields = {k: v for k, v in fields.items()
                      if k in ("ssn", "tin", "recipient_tin", "social_security_number")
                      and v is not None and str(v).strip()}

        if not ssn_fields:
            # No SSN field present — pass (field may not be extracted)
            findings.append(Finding(
                rule_id="TAX-003",
                rule_name="SSN format validation",
                severity="critical",
                passed=True,
                message=f"No SSN field on {doc_type} from {entity} (skipped)",
            ))
            continue

        evidence = []
        passed = True

        for field_name, raw_val in ssn_fields.items():
            val = str(raw_val).strip()
            if not _SSN_PATTERN.match(val):
                passed = False
                evidence.append({
                    "field": field_name,
                    "issue": f"Invalid SSN format: '{val}' (expected XXX-XX-XXXX)",
                    "entity": entity,
                })
            else:
                digits = val.replace("-", "")
                prefix3 = digits[:3]
                if prefix3 in _SSN_INVALID_PREFIXES or digits[0] == "9":
                    passed = False
                    evidence.append({
                        "field": field_name,
                        "issue": f"SSN has invalid area number prefix '{prefix3}'",
                        "entity": entity,
                    })

        findings.append(Finding(
            rule_id="TAX-003",
            rule_name="SSN format validation",
            severity="critical",
            passed=passed,
            message=f"SSN validation {'passed' if passed else 'FAILED'} on {doc_type} from {entity}",
            evidence=evidence,
        ))

    return findings


_MAX_WH_RATE = 0.40  # 40% — generous upper bound for withholding


def _eval_tax004(candidates: list) -> List[Finding]:
    """TAX-004: Federal withholding bounds check.

    Checks that federal_wh is non-negative and within reasonable bounds
    relative to the income amount for W-2, 1099-R, 1099-INT.
    """
    INCOME_FIELD = {
        "W-2": "wages",
        "1099-R": "gross_distribution",
        "1099-INT": "interest_income",
    }
    findings = []
    docs = [c for c in candidates if _get_doc_type(c) in INCOME_FIELD]
    if not docs:
        return findings

    for c in docs:
        fields = _get_fields(c)
        doc_type = _get_doc_type(c)
        entity = _get_entity(c) or "Unknown"
        income_field = INCOME_FIELD[doc_type]
        income = _safe_float(fields.get(income_field))
        wh = _safe_float(fields.get("federal_wh"))

        evidence = []
        passed = True

        if wh is not None:
            if wh < 0:
                passed = False
                evidence.append({
                    "field": "federal_wh",
                    "issue": f"Negative withholding (${wh:,.2f})",
                    "entity": entity,
                })
            elif income is not None and income > 0 and wh > income * _MAX_WH_RATE:
                passed = False
                evidence.append({
                    "field": "federal_wh",
                    "issue": (
                        f"Withholding (${wh:,.2f}) exceeds {_MAX_WH_RATE:.0%} "
                        f"of {income_field} (${income:,.2f})"
                    ),
                    "entity": entity,
                })

        findings.append(Finding(
            rule_id="TAX-004",
            rule_name="Federal withholding bounds",
            severity="warning",
            passed=passed,
            message=f"Withholding bounds {'OK' if passed else 'WARNING'} on {doc_type} from {entity}",
            evidence=evidence,
        ))

    return findings


def _eval_tax005(candidates: list) -> List[Finding]:
    """TAX-005: Duplicate document detection.

    Flags candidates that appear to be duplicates based on matching
    document_type + EIN + key numeric fields.
    """
    findings = []
    if len(candidates) < 2:
        findings.append(Finding(
            rule_id="TAX-005",
            rule_name="Duplicate document detection",
            severity="error",
            passed=True,
            message="Fewer than 2 candidates — no duplicates possible",
        ))
        return findings

    # Group by (doc_type, ein)
    groups = defaultdict(list)
    for i, c in enumerate(candidates):
        doc_type = _get_doc_type(c)
        ein = _get_ein(c)
        if doc_type and ein:
            groups[(doc_type, ein)].append((i, c))

    duplicates_found = False
    evidence = []

    for (doc_type, ein), group in groups.items():
        if len(group) < 2:
            continue

        # Compare key numeric fields to see if values are very similar
        for idx_a in range(len(group)):
            for idx_b in range(idx_a + 1, len(group)):
                i_a, c_a = group[idx_a]
                i_b, c_b = group[idx_b]
                fields_a = _get_fields(c_a)
                fields_b = _get_fields(c_b)

                # Find numeric fields that both have
                shared_nums = []
                for k in set(fields_a.keys()) & set(fields_b.keys()):
                    va = _safe_float(fields_a[k])
                    vb = _safe_float(fields_b[k])
                    if va is not None and vb is not None:
                        shared_nums.append((k, va, vb))

                if not shared_nums:
                    continue

                # If all shared numeric fields match within 1%, flag as duplicate
                all_match = all(
                    abs(va - vb) <= max(abs(va), 1) * 0.01
                    for _, va, vb in shared_nums
                )

                if all_match and len(shared_nums) >= 2:
                    duplicates_found = True
                    entity_a = _get_entity(c_a) or f"candidate {i_a}"
                    entity_b = _get_entity(c_b) or f"candidate {i_b}"
                    evidence.append({
                        "issue": (
                            f"Probable duplicate: {doc_type} EIN {ein} — "
                            f"'{entity_a}' and '{entity_b}' have {len(shared_nums)} "
                            f"matching numeric fields"
                        ),
                        "candidates": [i_a, i_b],
                    })

    findings.append(Finding(
        rule_id="TAX-005",
        rule_name="Duplicate document detection",
        severity="error",
        passed=not duplicates_found,
        message=(
            f"Found {len(evidence)} probable duplicate(s)"
            if duplicates_found else "No duplicate documents detected"
        ),
        evidence=evidence,
    ))

    return findings


def _eval_tax006(candidates: list) -> List[Finding]:
    """TAX-006: K-1 income reconciliation.

    Checks K-1 box sums for internal consistency:
    - Ordinary income (box 1) should be non-zero if other income boxes are set
    - Capital account changes should reconcile (begin + contributions + income
      - distributions = end, approximately)
    """
    findings = []
    k1s = [c for c in candidates if _get_doc_type(c) in ("K-1", "K-1 (1065)", "K-1 (1120-S)")]
    if not k1s:
        return findings

    for c in k1s:
        fields = _get_fields(c)
        entity = _get_entity(c) or fields.get("partnership_name", "Unknown")

        evidence = []
        passed = True

        # Capital account reconciliation
        begin = _safe_float(fields.get("beginning_capital_account"))
        end = _safe_float(fields.get("ending_capital_account"))
        net_income = _safe_float(fields.get("current_year_net_income"))
        contrib = _safe_float(fields.get("capital_contributed"))
        withdrawals = _safe_float(fields.get("withdrawals_distributions"))

        if all(v is not None for v in [begin, end, net_income]):
            expected = begin + (net_income or 0) + (contrib or 0) - (withdrawals or 0)
            tolerance = max(abs(expected) * 0.02, 1.0)  # 2% or $1
            if abs(expected - end) > tolerance:
                passed = False
                evidence.append({
                    "field": "capital_account",
                    "issue": (
                        f"Capital account doesn't reconcile: "
                        f"begin (${begin:,.0f}) + net income (${net_income:,.0f}) "
                        f"+ contributions (${(contrib or 0):,.0f}) "
                        f"- withdrawals (${(withdrawals or 0):,.0f}) "
                        f"= ${expected:,.0f}, but ending is ${end:,.0f}"
                    ),
                    "entity": entity,
                })

        findings.append(Finding(
            rule_id="TAX-006",
            rule_name="K-1 income reconciliation",
            severity="warning",
            passed=passed,
            message=f"K-1 reconciliation {'passed' if passed else 'WARNING'} for {entity}",
            evidence=evidence,
        ))

    return findings


_NEC_REPORTING_THRESHOLD = 600.0


def _eval_tax007(candidates: list) -> List[Finding]:
    """TAX-007: 1099-NEC threshold check.

    Informational — flags NEC amounts at key thresholds:
    - Below $600 (unusual — why was a 1099-NEC filed?)
    - Above $50,000 (high-value — may need Schedule SE attention)
    """
    findings = []
    necs = [c for c in candidates if _get_doc_type(c) == "1099-NEC"]
    if not necs:
        return findings

    for c in necs:
        fields = _get_fields(c)
        entity = _get_entity(c) or "Unknown payer"
        amount = _safe_float(fields.get("nonemployee_compensation"))

        evidence = []
        if amount is not None:
            if amount < _NEC_REPORTING_THRESHOLD:
                evidence.append({
                    "field": "nonemployee_compensation",
                    "note": (
                        f"Amount (${amount:,.2f}) is below the $600 filing threshold — "
                        f"verify this 1099-NEC is correct"
                    ),
                    "entity": entity,
                })
            if amount > 50000:
                evidence.append({
                    "field": "nonemployee_compensation",
                    "note": f"High-value 1099-NEC (${amount:,.2f}) — verify Schedule SE treatment",
                    "entity": entity,
                })

        findings.append(Finding(
            rule_id="TAX-007",
            rule_name="1099-NEC threshold check",
            severity="info",
            passed=True,  # Info-only — never fails
            message=f"1099-NEC threshold review for {entity}: ${amount:,.2f}" if amount else f"1099-NEC from {entity} (no amount)",
            evidence=evidence,
        ))

    return findings


_EIN_PATTERN = re.compile(r"^\d{2}-?\d{7}$")
_EIN_INVALID_PREFIXES = {"00", "07", "08", "09", "17", "18", "19", "28", "29",
                          "49", "69", "70", "78", "79", "89"}


def _eval_tax008(candidates: list) -> List[Finding]:
    """TAX-008: EIN format validation.

    Checks that employer/payer EIN matches XX-XXXXXXX format
    and doesn't use known-invalid prefixes.
    """
    TARGET_TYPES = {"W-2", "1099-INT", "1099-DIV", "1099-R", "K-1", "K-1 (1065)", "K-1 (1120-S)"}
    findings = []
    docs = [c for c in candidates if _get_doc_type(c) in TARGET_TYPES]
    if not docs:
        return findings

    for c in docs:
        fields = _get_fields(c)
        doc_type = _get_doc_type(c)
        entity = _get_entity(c) or "Unknown"
        ein = _get_ein(c) or fields.get("employer_ein", "") or fields.get("partnership_ein", "")

        if not ein or not str(ein).strip():
            # No EIN present — pass (field may not be extracted)
            findings.append(Finding(
                rule_id="TAX-008",
                rule_name="EIN format validation",
                severity="error",
                passed=True,
                message=f"No EIN on {doc_type} from {entity} (skipped)",
            ))
            continue

        ein_str = str(ein).strip()
        evidence = []
        passed = True

        if not _EIN_PATTERN.match(ein_str):
            passed = False
            evidence.append({
                "field": "ein",
                "issue": f"Invalid EIN format: '{ein_str}' (expected XX-XXXXXXX)",
                "entity": entity,
            })
        else:
            prefix = ein_str[:2]
            if prefix in _EIN_INVALID_PREFIXES:
                passed = False
                evidence.append({
                    "field": "ein",
                    "issue": f"EIN prefix '{prefix}' is not assigned by IRS",
                    "entity": entity,
                })

        findings.append(Finding(
            rule_id="TAX-008",
            rule_name="EIN format validation",
            severity="error",
            passed=passed,
            message=f"EIN validation {'passed' if passed else 'FAILED'} on {doc_type} from {entity}",
            evidence=evidence,
        ))

    return findings


# ═══════════════════════════════════════════════════════════════════════════════
# Rule dispatch table
# ═══════════════════════════════════════════════════════════════════════════════

_RULE_EVALUATORS = [
    _eval_tax001,
    _eval_tax002,
    _eval_tax003,
    _eval_tax004,
    _eval_tax005,
    _eval_tax006,
    _eval_tax007,
    _eval_tax008,
]


# ═══════════════════════════════════════════════════════════════════════════════
# Main entry point
# ═══════════════════════════════════════════════════════════════════════════════


def evaluate(candidates: Any, context: Any) -> ArdentResult:
    """Evaluate candidates against the Ardent ruleset.

    This is the main entry point called by the extraction pipeline.
    Doctrine fields are set explicitly from the Doctrine registry.

    Args:
        candidates: Extraction candidates (CandidateFact or dict list).
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

    # ── Normalise candidates to a list ──
    if candidates is None:
        candidates = []
    elif not isinstance(candidates, (list, tuple)):
        candidates = [candidates]

    # ── Evaluate all rules ──
    findings: List[Finding] = []
    for rule_fn in _RULE_EVALUATORS:
        try:
            findings.extend(rule_fn(candidates))
        except Exception:
            # Individual rule failure should not crash the engine
            rule_name = rule_fn.__name__.replace("_eval_", "").upper().replace("TAX", "TAX-")
            findings.append(Finding(
                rule_id=rule_name,
                rule_name=f"(evaluation error)",
                severity="warning",
                passed=True,
                message=f"Rule {rule_name} could not be evaluated (internal error)",
            ))

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
