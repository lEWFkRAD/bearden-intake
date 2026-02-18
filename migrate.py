"""
Bearden Document Intake — JSON-to-SQLite Migration
====================================================
One-time migration from JSON files to SQLite database.

Reads:
  - data/jobs_history.json          → jobs table
  - verifications/*.json            → facts + fact_events + review_state
  - data/vendor_categories.json     → vendor_categories table
  - clients/*/context/index.json    → client_context_docs + client_prior_year_data
  - clients/*/instructions.json     → client_instructions table

Idempotent: checks config table for migration_v1_done flag.
Does NOT delete original JSON files.
"""

import json
from pathlib import Path
from datetime import datetime

import db

BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"
CLIENTS_DIR = BASE_DIR / "clients"
VERIFY_DIR = BASE_DIR / "verifications"


def migrate_json_to_sqlite():
    """Run the full migration. Safe to call multiple times."""
    if db.get_config("migration_v1_done") == "true":
        print("  Migration already complete, skipping.")
        return

    print("  Starting JSON → SQLite migration...")
    migrated = {
        "jobs": _migrate_jobs(),
        "verifications": _migrate_verifications(),
        "vendor_categories": _migrate_vendor_categories(),
        "client_context": _migrate_client_context(),
        "client_instructions": _migrate_client_instructions(),
    }
    db.set_config("migration_v1_done", "true")
    print(f"  Migration complete: {migrated}")


def _migrate_jobs():
    """Migrate data/jobs_history.json → jobs table."""
    jobs_file = DATA_DIR / "jobs_history.json"
    if not jobs_file.exists():
        return 0

    try:
        with open(jobs_file) as f:
            jobs_data = json.load(f)
    except (json.JSONDecodeError, IOError, OSError):
        print("  Warning: Could not read jobs_history.json")
        return 0

    count = 0
    for job_id, job in jobs_data.items():
        # Normalize: ensure job_id is in the dict
        job["id"] = job.get("id", job_id)
        job.setdefault("job_id", job_id)
        # Clear stale running/queued
        if job.get("status") in ("running", "queued"):
            job["status"] = "interrupted"
        try:
            db.save_job(job)
            count += 1
        except Exception as e:
            print(f"  Warning: Could not migrate job {job_id}: {e}")
    return count


def _migrate_verifications():
    """Migrate verifications/*.json → facts + fact_events + review_state."""
    if not VERIFY_DIR.exists():
        return 0

    count = 0
    for vfile in VERIFY_DIR.glob("*.json"):
        job_id = vfile.stem
        try:
            with open(vfile) as f:
                vdata = json.load(f)
        except (json.JSONDecodeError, IOError, OSError):
            continue

        # Look up client_name and year from the job
        job = db.get_job(job_id)
        client_name = job["client_name"] if job else "unknown"
        tax_year = str(job["year"]) if job else ""
        reviewer = vdata.get("reviewer", "")

        fields = vdata.get("fields", {})
        for key, decision in fields.items():
            # Key format: page:extIdx:fieldName
            field_id = f"{job_id}:{key}"
            status = decision.get("status", "")
            if not status or status == "_remove":
                continue

            # Determine the value
            corrected_value = decision.get("corrected_value")
            # For facts, we need the extraction value — but we only have the decision.
            # Store the corrected_value if present, otherwise we'll use None (original stays)
            value = corrected_value if corrected_value is not None else None

            # Create fact entry
            db.set_fact(
                client_id=client_name,
                tax_year=tax_year,
                field_id=field_id,
                value=value,
                set_by=_reviewer_to_user_id(reviewer),
                evidence_id=job_id,
                status="verified" if status in ("confirmed", "corrected") else "flagged",
            )

            # Map reviewer initials to user_id
            actor_id = _reviewer_to_user_id(reviewer)
            action_map = {
                "confirmed": "verify",
                "corrected": "verify",
                "flagged": "verify",
            }
            action = action_map.get(status, "verify")

            db.record_fact_event(
                client_id=client_name,
                tax_year=tax_year,
                field_id=field_id,
                old_value=None,
                new_value=str(corrected_value) if corrected_value is not None else None,
                actor_user_id=actor_id,
                actor_role="preparer",
                action=action,
                reason=decision.get("note", ""),
                evidence_id=job_id,
            )

            # Set review state to 'prepared' (Jeffrey already reviewed these)
            db.set_review_stage(
                client_id=client_name,
                tax_year=tax_year,
                field_id=field_id,
                stage="prepared",
                assigned_to="susan",
                last_action="migrated_from_json",
            )

            count += 1

    return count


def _migrate_vendor_categories():
    """Migrate data/vendor_categories.json → vendor_categories table."""
    vc_file = DATA_DIR / "vendor_categories.json"
    if not vc_file.exists():
        return 0

    try:
        with open(vc_file) as f:
            vc_data = json.load(f)
    except (json.JSONDecodeError, IOError, OSError):
        return 0

    count = 0
    for vendor_norm, info in vc_data.items():
        if isinstance(info, dict):
            db.set_vendor_category(
                vendor_norm=vendor_norm,
                category=info.get("category", ""),
                original=info.get("original", ""),
                count=info.get("count", 1),
            )
        elif isinstance(info, str):
            # Simple format: {vendor: category}
            db.set_vendor_category(vendor_norm=vendor_norm, category=info)
        count += 1

    return count


def _migrate_client_context():
    """Migrate clients/*/context/index.json → client_context_docs + prior_year_data."""
    if not CLIENTS_DIR.exists():
        return 0

    count = 0
    for client_dir in CLIENTS_DIR.iterdir():
        if not client_dir.is_dir():
            continue

        client_name = client_dir.name
        ctx_index = client_dir / "context" / "index.json"
        if not ctx_index.exists():
            continue

        try:
            with open(ctx_index) as f:
                ctx_data = json.load(f)
        except (json.JSONDecodeError, IOError, OSError):
            continue

        # Migrate documents list
        for doc in ctx_data.get("documents", []):
            doc_id = doc.get("id", doc.get("doc_id", ""))
            if not doc_id:
                continue
            db.add_context_doc(
                client_name=client_name,
                doc_id=doc_id,
                label=doc.get("label", ""),
                filename=doc.get("filename", ""),
                file_path=doc.get("file_path", doc.get("path", "")),
                year=doc.get("year", ""),
                payers=doc.get("payers", []),
                raw_text=doc.get("raw_text", ""),
            )
            count += 1

        # Migrate prior-year data
        prior_data = ctx_data.get("prior_year_data", {})
        if prior_data:
            db.set_prior_year_data(client_name, prior_data)

    return count


def _migrate_client_instructions():
    """Migrate clients/*/instructions.json → client_instructions table."""
    if not CLIENTS_DIR.exists():
        return 0

    count = 0
    for client_dir in CLIENTS_DIR.iterdir():
        if not client_dir.is_dir():
            continue

        client_name = client_dir.name
        instr_file = client_dir / "instructions.json"
        if not instr_file.exists():
            continue

        try:
            with open(instr_file) as f:
                instr_data = json.load(f)
        except (json.JSONDecodeError, IOError, OSError):
            continue

        for rule in instr_data.get("rules", []):
            rule_id = rule.get("id", rule.get("rule_id", ""))
            text = rule.get("text", "")
            if not rule_id or not text:
                continue
            db.add_instruction(
                client_name=client_name,
                rule_id=rule_id,
                text=text,
            )
            count += 1

    return count


def _reviewer_to_user_id(reviewer):
    """Map reviewer initials to a user_id. Best effort."""
    if not reviewer:
        return "jeff"
    r = reviewer.upper().strip()
    if r in ("JW", "J", "JEFF", "JEFFREY"):
        return "jeff"
    if r in ("S", "SU", "SUSAN"):
        return "susan"
    if r in ("C", "CH", "CHARLES"):
        return "charles"
    return "jeff"  # Default to preparer


if __name__ == "__main__":
    db.init_db()
    migrate_json_to_sqlite()
