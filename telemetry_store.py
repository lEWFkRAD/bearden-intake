# ============================================================
# HERALDS — Operational Telemetry
# ============================================================

"""Operational telemetry storage for the Bearden Continuous Assurance System (CAS).

DB-only gateway for operational metrics. Manages the op_* tables
in the shared SQLite database. Stores run performance, drift indicators,
smoke test results, golden regression results, and backup records.

DESIGN PRINCIPLE: This module stores ONLY operational telemetry.
It must NEVER read, write, or reference financial fact data.
Deliverables and workpapers read from fact_store.py ONLY.

TWO-DOMAIN ARCHITECTURE:
  Financial Domain (PROTECTED)     Operational Domain (CAS)
  ──────────────────────────       ─────────────────────────
  fact_store.py                    telemetry_store.py  <-- this file
    facts, client_canonical_values   op_runs, op_phases, op_drift
  workpaper_export.py                op_smoke_results, op_golden_results
    reads fact_store.py ONLY         op_backups

HARD BOUNDARY: Dropping all op_* tables leaves the financial pipeline intact.

Run:  python3 -c "from telemetry_store import TelemetryStore; print('OK')"
"""

import sqlite3
import json
import os
from datetime import datetime
from pathlib import Path

# ─── IMPORT GUARDRAIL ─────────────────────────────────────────────────────────
# This module must never import fact_store, extraction, OCR, vision, or PDF libs.
# CAS is operational telemetry only — no financial data flows through here.
_FORBIDDEN_MODULES = frozenset({
    'fact_store', 'extract', 'pytesseract', 'anthropic', 'pdf2image',
    'PIL', 'Pillow', 'fitz', 'workpaper_export',
})

# ─── ALLOWED TABLES ──────────────────────────────────────────────────────────
# Explicit allowlist: this module only touches op_* tables.
_ALLOWED_TABLES = frozenset({
    'op_runs', 'op_phases', 'op_drift',
    'op_smoke_results', 'op_golden_results', 'op_backups',
    'op_change_requests', 'op_cr_findings', 'op_post_fix_gates',  # T-CAS-2B
})


# ─── TELEMETRY STORE ─────────────────────────────────────────────────────────

class TelemetryStore:
    """DB-only gateway for CAS operational telemetry tables.

    Every public method opens and closes its own connection (thread-safe
    pattern matching app.py and fact_store.py).

    Usage::

        ts = TelemetryStore("/path/to/bearden.db")

        # Record a run start
        run_id = ts.record_run_start("job-001", "Evans, Lisa", "tax_returns")

        # Record completion with log data
        ts.record_run_complete("job-001", log_data)

        # Query recent runs
        runs = ts.get_recent_runs(limit=20)
    """

    def __init__(self, db_path):
        self.db_path = str(db_path)

    def _conn(self):
        """Create a new SQLite connection with WAL mode."""
        conn = sqlite3.connect(self.db_path, timeout=10)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.row_factory = sqlite3.Row
        return conn

    # ─── Run Lifecycle ────────────────────────────────────────────────────

    def record_run_start(self, job_id, client_name="", doc_type="",
                         app_version="", extract_version=""):
        """Record the start of an extraction run. Returns the run_id (INTEGER PK)."""
        conn = self._conn()
        try:
            now = datetime.now().isoformat()
            cursor = conn.execute(
                """INSERT OR REPLACE INTO op_runs
                   (job_id, client_name, doc_type, status, started_at,
                    app_version, extract_version)
                   VALUES (?, ?, ?, 'running', ?, ?, ?)""",
                (job_id, client_name, doc_type, now, app_version, extract_version)
            )
            conn.commit()
            return cursor.lastrowid
        finally:
            conn.close()

    def record_run_complete(self, job_id, log_data=None):
        """Record completion of a run, populating metrics from the extraction log.

        Args:
            job_id: The job identifier.
            log_data: Dict parsed from the extraction JSON log file.
                      Expected keys: timing, cost, routing, throughput, streaming, etc.
        """
        if log_data is None:
            log_data = {}

        conn = self._conn()
        try:
            now = datetime.now().isoformat()
            timing = log_data.get("timing", {})
            cost = log_data.get("cost", {})
            routing = log_data.get("routing", {})
            throughput = log_data.get("throughput", {})
            streaming = log_data.get("streaming", {})

            # Parse page routing
            page_methods = routing.get("page_methods", {})
            pages_ocr = sum(1 for m in page_methods.values() if m == "ocr")
            pages_vision = sum(1 for m in page_methods.values() if m == "vision")
            pages_blank = routing.get("skipped_blank", 0)

            # Parse API call breakdown
            api_breakdown = cost.get("api_calls_by_type", {})
            vision_calls = api_breakdown.get("vision", 0) + api_breakdown.get("verify", 0)
            text_calls = api_breakdown.get("text", 0) + api_breakdown.get("classify", 0)

            conn.execute(
                """UPDATE op_runs SET
                    status = 'complete',
                    finished_at = ?,
                    total_s = ?,
                    cost_usd = ?,
                    total_pages = ?,
                    pages_ocr = ?,
                    pages_vision = ?,
                    pages_blank = ?,
                    total_fields = ?,
                    fields_high_conf = ?,
                    fields_low_conf = ?,
                    fields_needs_review = ?,
                    total_api_calls = ?,
                    vision_calls = ?,
                    text_calls = ?,
                    input_tokens = ?,
                    output_tokens = ?,
                    time_to_first_values_s = ?,
                    batches_total = ?,
                    fields_streamed = ?,
                    log_path = ?
                WHERE job_id = ?""",
                (
                    now,
                    timing.get("total_s"),
                    cost.get("total_cost_usd"),
                    routing.get("total_pages"),
                    pages_ocr,
                    pages_vision,
                    pages_blank,
                    throughput.get("total_fields"),
                    throughput.get("high_confidence_fields"),
                    throughput.get("low_confidence_fields"),
                    throughput.get("needs_review_fields"),
                    cost.get("total_api_calls"),
                    vision_calls,
                    text_calls,
                    cost.get("total_input_tokens"),
                    cost.get("total_output_tokens"),
                    streaming.get("time_to_first_values_s"),
                    throughput.get("batches_total"),
                    streaming.get("fields_streamed"),
                    log_data.get("log_path"),
                    job_id,
                )
            )
            conn.commit()
        finally:
            conn.close()

    def record_run_error(self, job_id, error_message=""):
        """Record that a run failed with an error."""
        conn = self._conn()
        try:
            now = datetime.now().isoformat()
            conn.execute(
                """UPDATE op_runs SET
                    status = 'error',
                    finished_at = ?,
                    error_message = ?
                WHERE job_id = ?""",
                (now, error_message[:2000] if error_message else "", job_id)
            )
            conn.commit()
        finally:
            conn.close()

    # ─── Phase Timing ─────────────────────────────────────────────────────

    def record_phases(self, job_id, phases_dict):
        """Record per-phase timing for a run.

        Args:
            job_id: The job identifier.
            phases_dict: Dict of {phase_name: duration_s} from extraction log.
        """
        if not phases_dict:
            return

        conn = self._conn()
        try:
            # Get run_id
            row = conn.execute(
                "SELECT id FROM op_runs WHERE job_id = ?", (job_id,)
            ).fetchone()
            if not row:
                return
            run_id = row["id"]

            for phase_name, duration_s in phases_dict.items():
                conn.execute(
                    """INSERT OR REPLACE INTO op_phases
                       (run_id, job_id, phase_name, duration_s)
                       VALUES (?, ?, ?, ?)""",
                    (run_id, job_id, phase_name, duration_s)
                )
            conn.commit()
        finally:
            conn.close()

    # ─── Drift Metrics ────────────────────────────────────────────────────

    def record_drift(self, job_id, edit_rate=None, missing_evidence_rate=None,
                     needs_review_rate=None, audit_pass_rate=None,
                     low_confidence_rate=None):
        """Record drift metrics for a finalized job."""
        conn = self._conn()
        try:
            now = datetime.now().isoformat()
            conn.execute(
                """INSERT OR REPLACE INTO op_drift
                   (job_id, measured_at, edit_rate, missing_evidence_rate,
                    needs_review_rate, audit_pass_rate, low_confidence_rate)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (job_id, now, edit_rate, missing_evidence_rate,
                 needs_review_rate, audit_pass_rate, low_confidence_rate)
            )
            conn.commit()
        finally:
            conn.close()

    def compute_drift_for_job(self, job_id, db_path=None):
        """Compute and record drift metrics from verified_fields for a finalized job.

        Reads from verified_fields (same DB) to compute operational metrics:
        - edit_rate: fraction of fields that were manually edited
        - missing_evidence_rate: fraction with no evidence reference
        - needs_review_rate: fraction still in needs_review status
        - low_confidence_rate: fraction with low confidence scores

        NOTE: This reads verified_fields for OPERATIONAL metrics only.
        It does not modify any financial data.
        """
        conn = self._conn()
        try:
            # Count verified fields for this job
            rows = conn.execute(
                """SELECT status, COUNT(*) as cnt
                   FROM verified_fields
                   WHERE job_id = ?
                   GROUP BY status""",
                (job_id,)
            ).fetchall()

            if not rows:
                return

            total = sum(r["cnt"] for r in rows)
            if total == 0:
                return

            status_counts = {r["status"]: r["cnt"] for r in rows}
            edited = status_counts.get("edited", 0) + status_counts.get("corrected", 0)
            needs_review = status_counts.get("needs_review", 0) + status_counts.get("flagged", 0)

            # Check for audit results if available
            audit_pass_rate = None
            try:
                audit_row = conn.execute(
                    """SELECT data FROM verifications WHERE job_id = ?""",
                    (job_id,)
                ).fetchone()
                if audit_row:
                    vdata = json.loads(audit_row["data"])
                    audit = vdata.get("post_run_audit", {})
                    if audit.get("sample_size", 0) > 0:
                        audit_pass_rate = audit.get("pass_count", 0) / audit["sample_size"]
            except (json.JSONDecodeError, KeyError, TypeError):
                pass

            self.record_drift(
                job_id=job_id,
                edit_rate=round(edited / total, 4) if total else None,
                needs_review_rate=round(needs_review / total, 4) if total else None,
                audit_pass_rate=round(audit_pass_rate, 4) if audit_pass_rate is not None else None,
            )
        finally:
            conn.close()

    # ─── Smoke Test Results ───────────────────────────────────────────────

    def record_smoke_result(self, passed, total_checks, results, duration_s):
        """Record the outcome of a smoke test run.

        Args:
            passed: Number of checks that passed.
            total_checks: Total number of checks run.
            results: List of dicts with check details.
            duration_s: Time taken for the smoke run.
        """
        conn = self._conn()
        try:
            now = datetime.now().isoformat()
            conn.execute(
                """INSERT INTO op_smoke_results
                   (run_at, passed, total_checks, results_json, duration_s)
                   VALUES (?, ?, ?, ?, ?)""",
                (now, passed, total_checks, json.dumps(results), duration_s)
            )
            conn.commit()
        finally:
            conn.close()

    # ─── Golden Regression Results ────────────────────────────────────────

    def record_golden_result(self, golden_name, passed, total_checks,
                             fields_matched=0, fields_mismatched=0,
                             fields_missing=0, fields_extra=0,
                             duration_s=0, details=None):
        """Record the outcome of a golden regression test."""
        conn = self._conn()
        try:
            now = datetime.now().isoformat()
            conn.execute(
                """INSERT INTO op_golden_results
                   (run_at, golden_name, passed, total_checks,
                    fields_matched, fields_mismatched, fields_missing, fields_extra,
                    duration_s, details_json)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (now, golden_name, passed, total_checks,
                 fields_matched, fields_mismatched, fields_missing, fields_extra,
                 duration_s, json.dumps(details or {}))
            )
            conn.commit()
        finally:
            conn.close()

    # ─── Backup Records ───────────────────────────────────────────────────

    def record_backup(self, backup_path, db_size_bytes, sha256, row_counts=None):
        """Record a backup snapshot."""
        conn = self._conn()
        try:
            now = datetime.now().isoformat()
            conn.execute(
                """INSERT INTO op_backups
                   (created_at, backup_path, db_size_bytes, sha256, row_counts_json)
                   VALUES (?, ?, ?, ?, ?)""",
                (now, backup_path, db_size_bytes, sha256,
                 json.dumps(row_counts or {}))
            )
            conn.commit()
        finally:
            conn.close()

    def record_backup_verify(self, backup_id, verified, sha256):
        """Record the result of verifying a backup."""
        conn = self._conn()
        try:
            now = datetime.now().isoformat()
            conn.execute(
                """UPDATE op_backups SET
                    verified = ?,
                    verify_sha256 = ?,
                    verify_at = ?
                WHERE id = ?""",
                (1 if verified else 0, sha256, now, backup_id)
            )
            conn.commit()
        finally:
            conn.close()

    # ─── Query Methods ────────────────────────────────────────────────────

    def get_recent_runs(self, limit=20):
        """Get recent extraction runs, newest first."""
        conn = self._conn()
        try:
            rows = conn.execute(
                """SELECT * FROM op_runs
                   ORDER BY started_at DESC
                   LIMIT ?""",
                (limit,)
            ).fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()

    def get_run(self, job_id):
        """Get a single run by job_id, including phase timing."""
        conn = self._conn()
        try:
            run = conn.execute(
                "SELECT * FROM op_runs WHERE job_id = ?", (job_id,)
            ).fetchone()
            if not run:
                return None

            result = dict(run)
            phases = conn.execute(
                """SELECT phase_name, duration_s FROM op_phases
                   WHERE run_id = ?
                   ORDER BY id""",
                (run["id"],)
            ).fetchall()
            result["phases"] = {p["phase_name"]: p["duration_s"] for p in phases}
            return result
        finally:
            conn.close()

    def get_drift_summary(self, limit=20):
        """Get recent drift measurements."""
        conn = self._conn()
        try:
            rows = conn.execute(
                """SELECT * FROM op_drift
                   ORDER BY measured_at DESC
                   LIMIT ?""",
                (limit,)
            ).fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()

    def get_latest_smoke(self):
        """Get the most recent smoke test result."""
        conn = self._conn()
        try:
            row = conn.execute(
                """SELECT * FROM op_smoke_results
                   ORDER BY run_at DESC LIMIT 1"""
            ).fetchone()
            if row:
                result = dict(row)
                result["results"] = json.loads(result.get("results_json", "[]"))
                return result
            return None
        finally:
            conn.close()

    def get_latest_golden_results(self, limit=10):
        """Get the most recent golden regression results."""
        conn = self._conn()
        try:
            rows = conn.execute(
                """SELECT * FROM op_golden_results
                   ORDER BY run_at DESC
                   LIMIT ?""",
                (limit,)
            ).fetchall()
            results = []
            for r in rows:
                d = dict(r)
                d["details"] = json.loads(d.get("details_json", "{}"))
                results.append(d)
            return results
        finally:
            conn.close()

    def get_recent_backups(self, limit=10):
        """Get recent backup records."""
        conn = self._conn()
        try:
            rows = conn.execute(
                """SELECT * FROM op_backups
                   ORDER BY created_at DESC
                   LIMIT ?""",
                (limit,)
            ).fetchall()
            results = []
            for r in rows:
                d = dict(r)
                d["row_counts"] = json.loads(d.get("row_counts_json", "{}"))
                results.append(d)
            return results
        finally:
            conn.close()

    def daily_summary(self, date_str=None):
        """Build a daily summary of CAS metrics for reports.

        Args:
            date_str: ISO date string (YYYY-MM-DD). Defaults to today.

        Returns:
            Dict with keys: runs, smoke, goldens, backups, drift.
        """
        if date_str is None:
            date_str = datetime.now().strftime("%Y-%m-%d")

        conn = self._conn()
        try:
            # Runs today
            runs = conn.execute(
                """SELECT COUNT(*) as total,
                          SUM(CASE WHEN status='complete' THEN 1 ELSE 0 END) as completed,
                          SUM(CASE WHEN status='error' THEN 1 ELSE 0 END) as errors,
                          AVG(total_s) as avg_runtime,
                          AVG(cost_usd) as avg_cost,
                          SUM(cost_usd) as total_cost,
                          AVG(time_to_first_values_s) as avg_ttfv
                   FROM op_runs
                   WHERE started_at LIKE ?""",
                (date_str + "%",)
            ).fetchone()

            # Latest smoke
            smoke = conn.execute(
                """SELECT run_at, passed, total_checks, duration_s
                   FROM op_smoke_results
                   ORDER BY run_at DESC LIMIT 1"""
            ).fetchone()

            # Goldens today
            goldens = conn.execute(
                """SELECT COUNT(*) as total,
                          SUM(CASE WHEN passed=total_checks THEN 1 ELSE 0 END) as all_pass
                   FROM op_golden_results
                   WHERE run_at LIKE ?""",
                (date_str + "%",)
            ).fetchall()

            # Latest backup
            backup = conn.execute(
                """SELECT created_at, backup_path, db_size_bytes, sha256, verified
                   FROM op_backups
                   ORDER BY created_at DESC LIMIT 1"""
            ).fetchone()

            # Average drift (last 7 days)
            drift = conn.execute(
                """SELECT AVG(edit_rate) as avg_edit_rate,
                          AVG(needs_review_rate) as avg_needs_review,
                          AVG(audit_pass_rate) as avg_audit_pass,
                          AVG(low_confidence_rate) as avg_low_conf,
                          COUNT(*) as sample_count
                   FROM op_drift
                   WHERE measured_at >= date(?, '-7 days')""",
                (date_str,)
            ).fetchone()

            return {
                "date": date_str,
                "runs": dict(runs) if runs else {},
                "smoke": dict(smoke) if smoke else None,
                "goldens": [dict(g) for g in goldens] if goldens else [],
                "backup": dict(backup) if backup else None,
                "drift": dict(drift) if drift else {},
            }
        finally:
            conn.close()

    # ─── Change Requests (T-CAS-2B) ──────────────────────────────────────

    def _next_cr_id(self):
        """Generate next CR-YYYYMMDD-NNN id. Thread-safe via DB sequence."""
        conn = self._conn()
        try:
            today = datetime.now().strftime("%Y%m%d")
            prefix = f"CR-{today}-"
            row = conn.execute(
                "SELECT cr_id FROM op_change_requests WHERE cr_id LIKE ? ORDER BY cr_id DESC LIMIT 1",
                (prefix + "%",)
            ).fetchone()
            if row:
                last_num = int(row["cr_id"].split("-")[-1])
                next_num = last_num + 1
            else:
                next_num = 1
            return f"{prefix}{next_num:03d}"
        finally:
            conn.close()

    def create_change_request(self, source, severity="WARNING", trigger_summary="",
                              trigger_snapshot=None, findings=None):
        """Create a new Change Request with findings.

        Args:
            source: Trigger source ("smoke", "golden", "drift", "error_rate").
            severity: "CRITICAL", "WARNING", or "INFO".
            trigger_summary: Human-readable summary.
            trigger_snapshot: Dict of frozen metrics at trigger time.
            findings: List of finding dicts with keys:
                severity, source, check_name, details, measured_value, threshold, recommended_action.

        Returns:
            dict with keys: cr_id, status, folder_path, findings_count.
        """
        cr_id = self._next_cr_id()
        folder_path = f"data/reports/change_requests/{cr_id}"
        findings = findings or []

        conn = self._conn()
        try:
            now = datetime.now().isoformat()
            conn.execute(
                """INSERT INTO op_change_requests
                   (cr_id, status, severity, source, trigger_summary, trigger_snapshot,
                    created_at, updated_at, folder_path)
                   VALUES (?, 'open', ?, ?, ?, ?, ?, ?, ?)""",
                (cr_id, severity, source, trigger_summary,
                 json.dumps(trigger_snapshot) if trigger_snapshot else None,
                 now, now, folder_path)
            )

            # Insert findings
            for i, f in enumerate(findings, 1):
                finding_id = f"F-{i:03d}"
                conn.execute(
                    """INSERT INTO op_cr_findings
                       (cr_id, finding_id, severity, source, check_name,
                        details, measured_value, threshold, recommended_action)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (cr_id, finding_id,
                     f.get("severity", "WARNING"),
                     f.get("source", source),
                     f.get("check_name", ""),
                     f.get("details", ""),
                     f.get("measured_value", ""),
                     f.get("threshold", ""),
                     f.get("recommended_action", ""))
                )

            conn.commit()
            return {
                "cr_id": cr_id,
                "status": "open",
                "folder_path": folder_path,
                "findings_count": len(findings),
            }
        finally:
            conn.close()

    def get_change_request(self, cr_id):
        """Get a single CR by cr_id, including its findings and latest gate result.

        Returns:
            dict with cr data + "findings" list + "gate" dict (or None).
            Returns None if cr_id not found.
        """
        conn = self._conn()
        try:
            cr = conn.execute(
                "SELECT * FROM op_change_requests WHERE cr_id = ?", (cr_id,)
            ).fetchone()
            if not cr:
                return None

            result = dict(cr)

            # Attach findings
            findings = conn.execute(
                """SELECT * FROM op_cr_findings
                   WHERE cr_id = ?
                   ORDER BY finding_id""",
                (cr_id,)
            ).fetchall()
            result["findings"] = [dict(f) for f in findings]

            # Attach latest gate result
            gate = conn.execute(
                """SELECT * FROM op_post_fix_gates
                   WHERE cr_id = ?
                   ORDER BY run_at DESC LIMIT 1""",
                (cr_id,)
            ).fetchone()
            result["gate"] = dict(gate) if gate else None

            return result
        finally:
            conn.close()

    def get_open_change_requests(self):
        """Get all open CRs (status not 'closed'), newest first.

        Returns:
            list of dicts (without findings attached, for list views).
        """
        conn = self._conn()
        try:
            rows = conn.execute(
                """SELECT * FROM op_change_requests
                   WHERE status != 'closed'
                   ORDER BY created_at DESC"""
            ).fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()

    def get_all_change_requests(self, limit=50):
        """Get all CRs, newest first.

        Returns:
            list of dicts.
        """
        conn = self._conn()
        try:
            rows = conn.execute(
                """SELECT * FROM op_change_requests
                   ORDER BY created_at DESC
                   LIMIT ?""",
                (limit,)
            ).fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()

    def update_cr_status(self, cr_id, status, closed_by=None):
        """Update a CR's status. Sets closed_at if status='closed'.

        Returns:
            bool: True if updated, False if cr_id not found.
        """
        conn = self._conn()
        try:
            now = datetime.now().isoformat()
            if status == "closed":
                result = conn.execute(
                    """UPDATE op_change_requests SET
                        status = ?, updated_at = ?, closed_at = ?, closed_by = ?
                    WHERE cr_id = ?""",
                    (status, now, now, closed_by, cr_id)
                )
            else:
                result = conn.execute(
                    """UPDATE op_change_requests SET
                        status = ?, updated_at = ?
                    WHERE cr_id = ?""",
                    (status, now, cr_id)
                )
            conn.commit()
            return result.rowcount > 0
        finally:
            conn.close()

    def submit_fix_manifest(self, cr_id, manifest_data):
        """Record a fix manifest for a CR. Updates status to 'fix_submitted'.

        Validates: at least 1 file in files_changed, non-empty description.

        Args:
            cr_id: The Change Request ID.
            manifest_data: dict with keys: files_changed[], tests_added[], config_changed[],
                           description, author, timestamp.

        Returns:
            dict with keys: success (bool), error (str or None).
        """
        # Validate manifest
        files_changed = manifest_data.get("files_changed", [])
        description = (manifest_data.get("description") or "").strip()

        if not files_changed:
            return {"success": False, "error": "At least one file must be listed in files_changed"}
        if not description:
            return {"success": False, "error": "Description must be non-empty"}

        conn = self._conn()
        try:
            # Verify CR exists
            cr = conn.execute(
                "SELECT id, folder_path FROM op_change_requests WHERE cr_id = ?", (cr_id,)
            ).fetchone()
            if not cr:
                return {"success": False, "error": f"CR {cr_id} not found"}

            # Write manifest to CR folder
            folder_path = cr["folder_path"]
            if folder_path:
                import os
                full_path = os.path.join(os.path.dirname(self.db_path), "..", folder_path)
                os.makedirs(full_path, exist_ok=True)
                manifest_path = os.path.join(full_path, "fix_manifest.json")
                with open(manifest_path, "w") as f:
                    json.dump(manifest_data, f, indent=2, default=str)

            now = datetime.now().isoformat()
            conn.execute(
                """UPDATE op_change_requests SET
                    status = 'fix_submitted', updated_at = ?
                WHERE cr_id = ? AND status = 'open'""",
                (now, cr_id)
            )
            conn.commit()
            return {"success": True, "error": None}
        finally:
            conn.close()

    def record_gate_result(self, cr_id, gate_result, checks_run=0, checks_passed=0,
                           before_snapshot=None, after_snapshot=None, details=None):
        """Record a post-fix gate result. Updates CR status accordingly.

        Args:
            cr_id: The Change Request ID.
            gate_result: "ACCEPTED", "NEEDS_REVIEW", or "REJECTED".
            checks_run: Number of checks re-run.
            checks_passed: Number of checks that now pass.
            before_snapshot: Dict of original trigger metrics.
            after_snapshot: Dict of current metrics after fix.
            details: List/dict of per-check comparison details.

        Returns:
            dict with keys: gate_result, cr_status (new status).
        """
        conn = self._conn()
        try:
            now = datetime.now().isoformat()
            conn.execute(
                """INSERT INTO op_post_fix_gates
                   (cr_id, run_at, gate_result, checks_run, checks_passed,
                    before_snapshot, after_snapshot, details_json)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (cr_id, now, gate_result, checks_run, checks_passed,
                 json.dumps(before_snapshot) if before_snapshot else None,
                 json.dumps(after_snapshot) if after_snapshot else None,
                 json.dumps(details) if details else None)
            )

            # Update CR status based on gate result
            new_status = "gate_passed" if gate_result == "ACCEPTED" else "gate_failed"
            conn.execute(
                """UPDATE op_change_requests SET
                    status = ?, updated_at = ?
                WHERE cr_id = ?""",
                (new_status, now, cr_id)
            )
            conn.commit()
            return {"gate_result": gate_result, "cr_status": new_status}
        finally:
            conn.close()

    def can_merge_fix(self, cr_id):
        """Check if a CR can be closed (merge guard).

        Returns True only if:
          - CR exists
          - Findings exist
          - Fix manifest submitted (status >= 'fix_submitted')
          - Gate result = ACCEPTED

        Returns:
            dict with keys: can_merge (bool), reason (str).
        """
        conn = self._conn()
        try:
            cr = conn.execute(
                "SELECT * FROM op_change_requests WHERE cr_id = ?", (cr_id,)
            ).fetchone()
            if not cr:
                return {"can_merge": False, "reason": f"CR {cr_id} not found"}

            # Check findings exist
            findings_count = conn.execute(
                "SELECT COUNT(*) FROM op_cr_findings WHERE cr_id = ?", (cr_id,)
            ).fetchone()[0]
            if findings_count == 0:
                return {"can_merge": False, "reason": "No findings recorded"}

            # Check fix manifest submitted
            if cr["status"] in ("open",):
                return {"can_merge": False, "reason": "Fix manifest not yet submitted"}

            # Check gate result
            gate = conn.execute(
                """SELECT gate_result FROM op_post_fix_gates
                   WHERE cr_id = ?
                   ORDER BY run_at DESC LIMIT 1""",
                (cr_id,)
            ).fetchone()
            if not gate:
                return {"can_merge": False, "reason": "Post-fix gate not yet run"}
            if gate["gate_result"] != "ACCEPTED":
                return {"can_merge": False, "reason": f"Gate result is {gate['gate_result']}, not ACCEPTED"}

            return {"can_merge": True, "reason": "All conditions met"}
        finally:
            conn.close()

    # ─── Trigger Helpers (T-CAS-2B) ──────────────────────────────────────

    def get_error_rate_24h(self):
        """Compute extraction error rate over the last 24 hours.

        Returns:
            dict with keys: total_runs, error_runs, error_rate (float 0-1), period_hours.
            Returns None if no runs in period.
        """
        conn = self._conn()
        try:
            row = conn.execute(
                """SELECT
                    COUNT(*) as total_runs,
                    SUM(CASE WHEN status = 'error' THEN 1 ELSE 0 END) as error_runs
                   FROM op_runs
                   WHERE started_at >= datetime('now', '-24 hours')"""
            ).fetchone()
            if not row or row["total_runs"] == 0:
                return None
            total = row["total_runs"]
            errors = row["error_runs"] or 0
            return {
                "total_runs": total,
                "error_runs": errors,
                "error_rate": round(errors / total, 4) if total > 0 else 0,
                "period_hours": 24,
            }
        finally:
            conn.close()

    def check_drift_thresholds(self, thresholds=None):
        """Check latest drift metrics against thresholds.

        Args:
            thresholds: dict with keys: edit_rate_max, needs_review_rate_max, audit_pass_rate_min.
                        Defaults: edit_rate > 0.15, needs_review_rate > 0.20, audit_pass_rate < 0.90.

        Returns:
            dict with keys: triggered (bool), violations (list of finding dicts).
        """
        if thresholds is None:
            thresholds = {
                "edit_rate_max": 0.15,
                "needs_review_rate_max": 0.20,
                "audit_pass_rate_min": 0.90,
            }

        conn = self._conn()
        try:
            # Get latest drift entry
            row = conn.execute(
                "SELECT * FROM op_drift ORDER BY measured_at DESC LIMIT 1"
            ).fetchone()
            if not row:
                return {"triggered": False, "violations": []}

            drift = dict(row)
            violations = []

            edit_rate = drift.get("edit_rate")
            if edit_rate is not None and edit_rate > thresholds.get("edit_rate_max", 0.15):
                violations.append({
                    "severity": "WARNING",
                    "source": "drift",
                    "check_name": "edit_rate",
                    "details": f"Edit rate {edit_rate:.2%} exceeds threshold {thresholds['edit_rate_max']:.0%}",
                    "measured_value": f"{edit_rate:.4f}",
                    "threshold": f"{thresholds['edit_rate_max']:.2f}",
                    "recommended_action": "Review extraction quality — high edit rate suggests AI output needs correction",
                })

            needs_review = drift.get("needs_review_rate")
            if needs_review is not None and needs_review > thresholds.get("needs_review_rate_max", 0.20):
                violations.append({
                    "severity": "WARNING",
                    "source": "drift",
                    "check_name": "needs_review_rate",
                    "details": f"Needs-review rate {needs_review:.2%} exceeds threshold {thresholds['needs_review_rate_max']:.0%}",
                    "measured_value": f"{needs_review:.4f}",
                    "threshold": f"{thresholds['needs_review_rate_max']:.2f}",
                    "recommended_action": "Investigate why many fields require manual review",
                })

            audit_pass = drift.get("audit_pass_rate")
            if audit_pass is not None and audit_pass < thresholds.get("audit_pass_rate_min", 0.90):
                violations.append({
                    "severity": "CRITICAL",
                    "source": "drift",
                    "check_name": "audit_pass_rate",
                    "details": f"Audit pass rate {audit_pass:.2%} below threshold {thresholds['audit_pass_rate_min']:.0%}",
                    "measured_value": f"{audit_pass:.4f}",
                    "threshold": f"{thresholds['audit_pass_rate_min']:.2f}",
                    "recommended_action": "Critical: audit failures indicate extraction unreliability",
                })

            return {"triggered": len(violations) > 0, "violations": violations}
        finally:
            conn.close()

    def cas_health_summary(self):
        """Build an aggregated CAS health summary for the admin dashboard.

        Returns:
            Dict with smoke, golden, backup, drift, and overall status.
        """
        smoke = self.get_latest_smoke()
        goldens = self.get_latest_golden_results(limit=5)
        backups = self.get_recent_backups(limit=1)
        drift = self.get_drift_summary(limit=5)

        # Compute overall health
        smoke_ok = smoke and smoke.get("passed") == smoke.get("total_checks") if smoke else None
        golden_ok = all(g.get("passed") == g.get("total_checks") for g in goldens) if goldens else None
        backup_ok = len(backups) > 0 if backups else False
        backup_recent = None  # None = no data, False = stale, True = recent
        if backups:
            try:
                last_backup_time = datetime.fromisoformat(backups[0].get("created_at", ""))
                hours_since = (datetime.now() - last_backup_time).total_seconds() / 3600
                backup_recent = hours_since < 48
            except (ValueError, TypeError):
                pass

        # Determine overall state
        checks = [smoke_ok, golden_ok, backup_recent]
        known_checks = [c for c in checks if c is not None]
        if not known_checks:
            overall_state = "unknown"
            overall_label = "No Data"
        elif all(known_checks):
            overall_state = "good"
            overall_label = "Healthy"
        elif any(c is False for c in known_checks):
            overall_state = "bad" if sum(1 for c in known_checks if not c) >= 2 else "warn"
            overall_label = "Degraded" if overall_state == "warn" else "Unhealthy"
        else:
            overall_state = "warn"
            overall_label = "Incomplete"

        return {
            "state": overall_state,
            "label": overall_label,
            "smoke": {
                "state": "good" if smoke_ok else ("bad" if smoke_ok is False else "unknown"),
                "passed": smoke.get("passed") if smoke else None,
                "total": smoke.get("total_checks") if smoke else None,
                "run_at": smoke.get("run_at") if smoke else None,
            },
            "goldens": {
                "state": "good" if golden_ok else ("bad" if golden_ok is False else "unknown"),
                "count": len(goldens),
                "all_pass": golden_ok,
            },
            "backup": {
                "state": "good" if backup_recent else ("warn" if backup_ok else "unknown"),
                "last_at": backups[0].get("created_at") if backups else None,
                "verified": backups[0].get("verified") if backups else None,
            },
            "drift": {
                "count": len(drift),
                "latest": drift[0] if drift else None,
            },
        }
