"""Doctrine Drift Guard — detects governance drift between stored and runtime state.

Drift means the governance rules active NOW are different from those that were
active when a log was created. This is a WARNING system — it never blocks.
"""

from typing import Optional


def doctrine_drift_status(
    log_version: Optional[str],
    log_hash: Optional[str],
    current_version: Optional[str],
    current_hash: Optional[str],
) -> dict:
    """Compare a stored log's doctrine fingerprint against the current runtime.

    Returns a dict with:
        status:          "ok" | "legacy" | "warn_major" | "warn_hash"
        message:         Human-readable explanation
        log_version:     The version from the stored log (or None)
        current_version: The version from the current runtime
        log_hash_short:  First 8 chars of log hash (or None)
        current_hash_short: First 8 chars of current hash
    """
    result = {
        "log_version": log_version,
        "current_version": current_version,
        "log_hash_short": log_hash[:8] if log_hash else None,
        "current_hash_short": current_hash[:8] if current_hash else None,
    }

    # Case 1: Log predates Doctrine — no fingerprint stored
    if not log_version:
        result["status"] = "legacy"
        result["message"] = "Log predates Doctrine — no governance fingerprint stored."
        return result

    # Case 2: Major version differs (first number in SemVer)
    log_major = _major(log_version)
    current_major = _major(current_version)
    if log_major != current_major:
        result["status"] = "warn_major"
        result["message"] = (
            f"Major Doctrine version changed: "
            f"log={log_version} vs current={current_version}."
        )
        return result

    # Case 3: Same version but different hash — rules were modified
    if log_hash and current_hash and log_hash != current_hash:
        result["status"] = "warn_hash"
        result["message"] = (
            f"Doctrine hash mismatch: "
            f"log={log_hash[:8]}... vs current={current_hash[:8]}... "
            f"(same version {log_version})."
        )
        return result

    # Case 4: All good
    result["status"] = "ok"
    result["message"] = f"Doctrine v{current_version} matches."
    return result


def _major(version: Optional[str]) -> Optional[str]:
    """Extract the major version number from a SemVer string."""
    if not version:
        return None
    parts = version.split(".")
    return parts[0] if parts else None
