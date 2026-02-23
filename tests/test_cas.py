#!/usr/bin/env python3
"""Tests for T-CAS-1 — Continuous Assurance System.

Covers: TelemetryStore CRUD, op_* schema, domain isolation,
        smoke tests, backup manager, golden regressions, reports.

Run:  python3 tests/test_cas.py
All test execution is inside run_tests() behind __name__ guard.
"""

import sys, os, json, tempfile, shutil, sqlite3, time, io

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

PASS = 0
FAIL = 0


def check(cond, msg):
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  \u2713 {msg}")
    else:
        FAIL += 1
        print(f"  \u2717 FAIL: {msg}")


# ═══════════════════════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

def _make_test_db():
    """Create a temporary database with CAS tables (same schema as _init_db)."""
    fd, db_path = tempfile.mkstemp(suffix='.db')
    os.close(fd)

    conn = sqlite3.connect(db_path, timeout=10)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")

    # Financial tables (minimal — just enough for isolation tests)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS facts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_id TEXT NOT NULL,
            client_id TEXT NOT NULL,
            tax_year INTEGER,
            fact_key TEXT NOT NULL,
            value_num REAL,
            value_text TEXT,
            status TEXT NOT NULL DEFAULT 'extracted',
            confidence REAL,
            source_method TEXT,
            source_doc TEXT,
            source_page INTEGER,
            evidence_ref TEXT,
            locked INTEGER NOT NULL DEFAULT 0,
            updated_at TEXT NOT NULL,
            UNIQUE(job_id, tax_year, fact_key)
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS verified_fields (
            job_id TEXT NOT NULL,
            field_key TEXT NOT NULL,
            canonical_value TEXT,
            original_value TEXT,
            status TEXT NOT NULL DEFAULT 'confirmed',
            category TEXT DEFAULT '',
            vendor_desc TEXT DEFAULT '',
            note TEXT DEFAULT '',
            reviewer TEXT DEFAULT '',
            verified_at TEXT NOT NULL DEFAULT '',
            review_stage TEXT DEFAULT '',
            reviewer_id INTEGER,
            PRIMARY KEY (job_id, field_key)
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS verifications (
            job_id TEXT PRIMARY KEY,
            data TEXT NOT NULL,
            updated TEXT DEFAULT ''
        )
    """)

    # CAS operational tables (must match app.py _init_db)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS op_runs (
            id INTEGER PRIMARY KEY,
            job_id TEXT UNIQUE,
            client_name TEXT,
            doc_type TEXT,
            status TEXT DEFAULT 'running',
            started_at TEXT,
            finished_at TEXT,
            total_s REAL,
            cost_usd REAL,
            total_pages INTEGER,
            pages_ocr INTEGER,
            pages_vision INTEGER,
            pages_blank INTEGER,
            cache_hit INTEGER DEFAULT 0,
            total_fields INTEGER,
            fields_high_conf INTEGER,
            fields_low_conf INTEGER,
            fields_needs_review INTEGER,
            total_api_calls INTEGER,
            vision_calls INTEGER,
            text_calls INTEGER,
            input_tokens INTEGER,
            output_tokens INTEGER,
            time_to_first_values_s REAL,
            batches_total INTEGER,
            fields_streamed INTEGER,
            app_version TEXT,
            extract_version TEXT,
            log_path TEXT,
            error_message TEXT
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_op_runs_job ON op_runs(job_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_op_runs_started ON op_runs(started_at)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_op_runs_status ON op_runs(status)")

    conn.execute("""
        CREATE TABLE IF NOT EXISTS op_phases (
            id INTEGER PRIMARY KEY,
            run_id INTEGER REFERENCES op_runs(id),
            job_id TEXT,
            phase_name TEXT,
            duration_s REAL,
            UNIQUE(run_id, phase_name)
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS op_drift (
            id INTEGER PRIMARY KEY,
            job_id TEXT UNIQUE,
            measured_at TEXT,
            edit_rate REAL,
            missing_evidence_rate REAL,
            needs_review_rate REAL,
            audit_pass_rate REAL,
            low_confidence_rate REAL
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS op_smoke_results (
            id INTEGER PRIMARY KEY,
            run_at TEXT,
            passed INTEGER,
            total_checks INTEGER,
            results_json TEXT,
            duration_s REAL
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS op_golden_results (
            id INTEGER PRIMARY KEY,
            run_at TEXT,
            golden_name TEXT,
            passed INTEGER,
            total_checks INTEGER,
            fields_matched INTEGER,
            fields_mismatched INTEGER,
            fields_missing INTEGER,
            fields_extra INTEGER,
            duration_s REAL,
            details_json TEXT
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS op_backups (
            id INTEGER PRIMARY KEY,
            created_at TEXT,
            backup_path TEXT,
            db_size_bytes INTEGER,
            sha256 TEXT,
            row_counts_json TEXT,
            verified INTEGER DEFAULT 0,
            verify_sha256 TEXT,
            verify_at TEXT
        )
    """)

    # T-CAS-2B: Change Request tables
    conn.execute("""
        CREATE TABLE IF NOT EXISTS op_change_requests (
            id INTEGER PRIMARY KEY,
            cr_id TEXT UNIQUE NOT NULL,
            status TEXT NOT NULL DEFAULT 'open',
            severity TEXT NOT NULL DEFAULT 'WARNING',
            source TEXT NOT NULL,
            trigger_summary TEXT,
            trigger_snapshot TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT,
            closed_at TEXT,
            closed_by TEXT,
            folder_path TEXT
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_op_cr_id ON op_change_requests(cr_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_op_cr_status ON op_change_requests(status)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_op_cr_created ON op_change_requests(created_at)")

    conn.execute("""
        CREATE TABLE IF NOT EXISTS op_cr_findings (
            id INTEGER PRIMARY KEY,
            cr_id TEXT NOT NULL,
            finding_id TEXT NOT NULL,
            severity TEXT NOT NULL DEFAULT 'WARNING',
            source TEXT NOT NULL,
            check_name TEXT,
            details TEXT,
            measured_value TEXT,
            threshold TEXT,
            recommended_action TEXT,
            UNIQUE(cr_id, finding_id)
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_op_findings_cr ON op_cr_findings(cr_id)")

    conn.execute("""
        CREATE TABLE IF NOT EXISTS op_post_fix_gates (
            id INTEGER PRIMARY KEY,
            cr_id TEXT NOT NULL,
            run_at TEXT NOT NULL,
            gate_result TEXT NOT NULL,
            checks_run INTEGER,
            checks_passed INTEGER,
            before_snapshot TEXT,
            after_snapshot TEXT,
            details_json TEXT
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_op_gates_cr ON op_post_fix_gates(cr_id)")

    conn.commit()
    conn.close()
    return db_path


def _cleanup_db(db_path):
    """Remove temporary database file."""
    try:
        os.unlink(db_path)
        # Also remove WAL/SHM files if present
        for suffix in ("-wal", "-shm"):
            p = db_path + suffix
            if os.path.exists(p):
                os.unlink(p)
    except OSError:
        pass


# ═══════════════════════════════════════════════════════════════════════════════
# TESTS
# ═══════════════════════════════════════════════════════════════════════════════

def run_tests():
    global PASS, FAIL
    PASS = 0
    FAIL = 0

    # ─── 1. Import Tests ─────────────────────────────────────────────────
    print("\n=== TelemetryStore: Import & Module Guard ===")

    from telemetry_store import TelemetryStore, _FORBIDDEN_MODULES, _ALLOWED_TABLES

    check("fact_store" in _FORBIDDEN_MODULES, "fact_store in FORBIDDEN_MODULES")
    check("extract" in _FORBIDDEN_MODULES, "extract in FORBIDDEN_MODULES")
    check("workpaper_export" in _FORBIDDEN_MODULES, "workpaper_export in FORBIDDEN_MODULES")
    check("op_runs" in _ALLOWED_TABLES, "op_runs in ALLOWED_TABLES")
    check("op_phases" in _ALLOWED_TABLES, "op_phases in ALLOWED_TABLES")
    check("op_drift" in _ALLOWED_TABLES, "op_drift in ALLOWED_TABLES")
    check("op_smoke_results" in _ALLOWED_TABLES, "op_smoke_results in ALLOWED_TABLES")
    check("op_golden_results" in _ALLOWED_TABLES, "op_golden_results in ALLOWED_TABLES")
    check("op_backups" in _ALLOWED_TABLES, "op_backups in ALLOWED_TABLES")
    check("op_change_requests" in _ALLOWED_TABLES, "op_change_requests in ALLOWED_TABLES (T-CAS-2B)")
    check("op_cr_findings" in _ALLOWED_TABLES, "op_cr_findings in ALLOWED_TABLES (T-CAS-2B)")
    check("op_post_fix_gates" in _ALLOWED_TABLES, "op_post_fix_gates in ALLOWED_TABLES (T-CAS-2B)")
    check(len(_ALLOWED_TABLES) == 9, f"exactly 9 allowed tables (got {len(_ALLOWED_TABLES)})")

    # ─── 2. Run Lifecycle ─────────────────────────────────────────────────
    print("\n=== TelemetryStore: Run Lifecycle ===")

    db_path = _make_test_db()
    try:
        ts = TelemetryStore(db_path)

        # Record start
        run_id = ts.record_run_start("job-001", "Evans, Lisa", "tax_returns",
                                      app_version="5.2", extract_version="v6")
        check(isinstance(run_id, int) and run_id > 0, f"record_run_start returns run_id={run_id}")

        # Verify running state
        run = ts.get_run("job-001")
        check(run is not None, "get_run returns data for job-001")
        check(run["status"] == "running", f"status is 'running' (got '{run.get('status')}')")
        check(run["client_name"] == "Evans, Lisa", "client_name stored correctly")
        check(run["doc_type"] == "tax_returns", "doc_type stored correctly")
        check(run["app_version"] == "5.2", "app_version stored correctly")

        # Record completion with mock log data
        mock_log = {
            "timing": {"total_s": 42.5, "phases": {"ocr": 5.1, "classify": 3.2, "extract": 25.0, "verify": 8.2}},
            "cost": {"total_cost_usd": 0.035, "total_api_calls": 12, "total_input_tokens": 50000,
                     "total_output_tokens": 5000, "api_calls_by_type": {"vision": 3, "text": 7, "classify": 2}},
            "routing": {"total_pages": 8, "page_methods": {"1": "ocr", "2": "ocr", "3": "vision", "4": "ocr",
                                                            "5": "ocr", "6": "ocr", "7": "vision", "8": "ocr"},
                        "skipped_blank": 1},
            "throughput": {"total_fields": 45, "high_confidence_fields": 38,
                           "low_confidence_fields": 4, "needs_review_fields": 3,
                           "batches_total": 5},
            "streaming": {"time_to_first_values_s": 8.7, "fields_streamed": 45},
            "log_path": "/tmp/test-log.json",
        }

        ts.record_run_complete("job-001", mock_log)

        run = ts.get_run("job-001")
        check(run["status"] == "complete", "status updated to 'complete'")
        check(run["total_s"] == 42.5, f"total_s = 42.5 (got {run.get('total_s')})")
        check(abs(run["cost_usd"] - 0.035) < 0.001, f"cost_usd ~= 0.035 (got {run.get('cost_usd')})")
        check(run["total_pages"] == 8, f"total_pages = 8 (got {run.get('total_pages')})")
        check(run["pages_ocr"] == 6, f"pages_ocr = 6 (got {run.get('pages_ocr')})")
        check(run["pages_vision"] == 2, f"pages_vision = 2 (got {run.get('pages_vision')})")
        check(run["pages_blank"] == 1, f"pages_blank = 1 (got {run.get('pages_blank')})")
        check(run["total_fields"] == 45, f"total_fields = 45 (got {run.get('total_fields')})")
        check(run["fields_high_conf"] == 38, f"fields_high_conf = 38 (got {run.get('fields_high_conf')})")
        check(run["total_api_calls"] == 12, f"total_api_calls = 12 (got {run.get('total_api_calls')})")
        check(run["vision_calls"] == 3, f"vision_calls = 3 (got {run.get('vision_calls')})")
        check(run["text_calls"] == 9, f"text_calls = 9 (got {run.get('text_calls')})")
        check(run["time_to_first_values_s"] == 8.7, f"ttfv = 8.7 (got {run.get('time_to_first_values_s')})")

        # Record error for a different job
        ts.record_run_start("job-002", "Smith, John", "bank_statements")
        ts.record_run_error("job-002", "API rate limit exceeded")
        run2 = ts.get_run("job-002")
        check(run2["status"] == "error", "error status recorded")
        check(run2["error_message"] == "API rate limit exceeded", "error message stored")

    finally:
        _cleanup_db(db_path)

    # ─── 3. Phase Timing ──────────────────────────────────────────────────
    print("\n=== TelemetryStore: Phase Timing ===")

    db_path = _make_test_db()
    try:
        ts = TelemetryStore(db_path)
        ts.record_run_start("job-phases", "Test Client", "tax_returns")

        phases = {"ocr": 5.1, "classify": 3.2, "extract": 25.0, "verify": 8.2, "normalize": 1.0}
        ts.record_phases("job-phases", phases)

        run = ts.get_run("job-phases")
        check("phases" in run, "phases included in get_run result")
        check(len(run["phases"]) == 5, f"5 phases recorded (got {len(run.get('phases', {}))})")
        check(run["phases"].get("extract") == 25.0, f"extract phase = 25.0 (got {run['phases'].get('extract')})")
        check(run["phases"].get("ocr") == 5.1, f"ocr phase = 5.1 (got {run['phases'].get('ocr')})")

        # Record phases for non-existent job (should not crash)
        ts.record_phases("nonexistent-job", {"test": 1.0})
        check(True, "record_phases for nonexistent job does not crash")

    finally:
        _cleanup_db(db_path)

    # ─── 4. Drift Metrics ─────────────────────────────────────────────────
    print("\n=== TelemetryStore: Drift Metrics ===")

    db_path = _make_test_db()
    try:
        ts = TelemetryStore(db_path)

        ts.record_drift("job-drift-1", edit_rate=0.05, needs_review_rate=0.02,
                         audit_pass_rate=1.0, low_confidence_rate=0.08)

        drift = ts.get_drift_summary(limit=5)
        check(len(drift) == 1, f"1 drift record (got {len(drift)})")
        check(drift[0]["job_id"] == "job-drift-1", "correct job_id in drift")
        check(drift[0]["edit_rate"] == 0.05, f"edit_rate = 0.05 (got {drift[0].get('edit_rate')})")
        check(drift[0]["audit_pass_rate"] == 1.0, f"audit_pass_rate = 1.0 (got {drift[0].get('audit_pass_rate')})")

        # Add a second drift record
        ts.record_drift("job-drift-2", edit_rate=0.10, needs_review_rate=0.05)
        drift = ts.get_drift_summary(limit=5)
        check(len(drift) == 2, f"2 drift records (got {len(drift)})")

        # Upsert (same job_id replaces)
        ts.record_drift("job-drift-1", edit_rate=0.07)
        drift = ts.get_drift_summary(limit=5)
        updated = [d for d in drift if d["job_id"] == "job-drift-1"]
        check(len(updated) == 1, "upsert keeps one record per job")
        check(updated[0]["edit_rate"] == 0.07, f"upserted edit_rate = 0.07 (got {updated[0].get('edit_rate')})")

    finally:
        _cleanup_db(db_path)

    # ─── 5. Compute Drift from Verified Fields ───────────────────────────
    print("\n=== TelemetryStore: Compute Drift ===")

    db_path = _make_test_db()
    try:
        ts = TelemetryStore(db_path)

        # Seed verified_fields
        conn = sqlite3.connect(db_path)
        from datetime import datetime
        now = datetime.now().isoformat()
        fields = [
            ("job-vf", "field_1", "100", "100", "confirmed", "", "", "", "Jeff", now, "", None),
            ("job-vf", "field_2", "200", "180", "edited", "", "", "", "Jeff", now, "", None),
            ("job-vf", "field_3", "300", "300", "confirmed", "", "", "", "Jeff", now, "", None),
            ("job-vf", "field_4", "400", "400", "confirmed", "", "", "", "Jeff", now, "", None),
            ("job-vf", "field_5", "", "", "needs_review", "", "", "", "Jeff", now, "", None),
        ]
        conn.executemany(
            """INSERT INTO verified_fields
               (job_id, field_key, canonical_value, original_value, status,
                category, vendor_desc, note, reviewer, verified_at,
                review_stage, reviewer_id)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            fields
        )
        conn.commit()
        conn.close()

        ts.compute_drift_for_job("job-vf")

        drift = ts.get_drift_summary(limit=1)
        check(len(drift) == 1, "drift computed from verified_fields")
        check(drift[0]["job_id"] == "job-vf", "drift job_id matches")
        # 1 edited out of 5 = 0.2
        check(abs(drift[0]["edit_rate"] - 0.2) < 0.01,
              f"edit_rate = 0.2 (got {drift[0].get('edit_rate')})")
        # 1 needs_review out of 5 = 0.2
        check(abs(drift[0]["needs_review_rate"] - 0.2) < 0.01,
              f"needs_review_rate = 0.2 (got {drift[0].get('needs_review_rate')})")

    finally:
        _cleanup_db(db_path)

    # ─── 6. Smoke Test Results ────────────────────────────────────────────
    print("\n=== TelemetryStore: Smoke Results ===")

    db_path = _make_test_db()
    try:
        ts = TelemetryStore(db_path)

        results = [
            {"check": "db_writable", "passed": True},
            {"check": "tesseract_available", "passed": True},
            {"check": "extract_exists", "passed": True},
            {"check": "api_key_set", "passed": False},
        ]
        ts.record_smoke_result(passed=3, total_checks=4, results=results, duration_s=0.5)

        latest = ts.get_latest_smoke()
        check(latest is not None, "smoke result recorded and retrieved")
        check(latest["passed"] == 3, f"3 passed (got {latest.get('passed')})")
        check(latest["total_checks"] == 4, f"4 total (got {latest.get('total_checks')})")
        check(isinstance(latest["results"], list), "results parsed as list")
        check(len(latest["results"]) == 4, f"4 result items (got {len(latest.get('results', []))})")
        check(latest["duration_s"] == 0.5, f"duration = 0.5s (got {latest.get('duration_s')})")

        # Add a newer result
        ts.record_smoke_result(passed=4, total_checks=4, results=[], duration_s=0.3)
        latest2 = ts.get_latest_smoke()
        check(latest2["passed"] == 4, "latest smoke returns newest result")

    finally:
        _cleanup_db(db_path)

    # ─── 7. Golden Regression Results ─────────────────────────────────────
    print("\n=== TelemetryStore: Golden Results ===")

    db_path = _make_test_db()
    try:
        ts = TelemetryStore(db_path)

        ts.record_golden_result("w2-basic", passed=10, total_checks=10,
                                 fields_matched=10, fields_mismatched=0,
                                 fields_missing=0, fields_extra=0,
                                 duration_s=15.2, details={"note": "clean run"})

        ts.record_golden_result("k1-complex", passed=8, total_checks=10,
                                 fields_matched=8, fields_mismatched=1,
                                 fields_missing=1, fields_extra=0,
                                 duration_s=25.5)

        results = ts.get_latest_golden_results(limit=10)
        check(len(results) == 2, f"2 golden results (got {len(results)})")
        # Most recent first
        check(results[0]["golden_name"] == "k1-complex", "most recent golden first")
        check(results[0]["fields_mismatched"] == 1, "mismatched count correct")
        check(results[1]["golden_name"] == "w2-basic", "second golden correct")
        check(results[1]["details"].get("note") == "clean run", "details parsed correctly")

    finally:
        _cleanup_db(db_path)

    # ─── 8. Backup Records ────────────────────────────────────────────────
    print("\n=== TelemetryStore: Backup Records ===")

    db_path = _make_test_db()
    try:
        ts = TelemetryStore(db_path)

        ts.record_backup("/data/backups/2026-02-18.db", db_size_bytes=1024000,
                          sha256="abc123def456", row_counts={"facts": 100, "jobs": 5})

        backups = ts.get_recent_backups(limit=5)
        check(len(backups) == 1, "1 backup recorded")
        check(backups[0]["db_size_bytes"] == 1024000, "db_size_bytes correct")
        check(backups[0]["sha256"] == "abc123def456", "sha256 stored")
        check(backups[0]["row_counts"].get("facts") == 100, "row_counts parsed correctly")
        check(backups[0]["verified"] == 0, "initially unverified")

        # Verify backup
        backup_id = backups[0]["id"]
        ts.record_backup_verify(backup_id, verified=True, sha256="abc123def456")

        backups = ts.get_recent_backups(limit=5)
        check(backups[0]["verified"] == 1, "backup marked verified after verify")
        check(backups[0]["verify_sha256"] == "abc123def456", "verify sha256 stored")
        check(backups[0]["verify_at"] is not None, "verify_at timestamp set")

    finally:
        _cleanup_db(db_path)

    # ─── 9. Recent Runs Query ─────────────────────────────────────────────
    print("\n=== TelemetryStore: Recent Runs Query ===")

    db_path = _make_test_db()
    try:
        ts = TelemetryStore(db_path)

        # Create several runs
        for i in range(5):
            ts.record_run_start(f"job-q-{i:03d}", f"Client {i}", "tax_returns")
            if i % 2 == 0:
                ts.record_run_complete(f"job-q-{i:03d}", {"timing": {"total_s": 10 + i}})
            else:
                ts.record_run_error(f"job-q-{i:03d}", f"Error {i}")

        runs = ts.get_recent_runs(limit=3)
        check(len(runs) == 3, f"limit=3 returns 3 runs (got {len(runs)})")
        # Should be newest first
        check(runs[0]["job_id"] == "job-q-004", f"newest first (got {runs[0].get('job_id')})")

        all_runs = ts.get_recent_runs(limit=100)
        check(len(all_runs) == 5, f"all 5 runs retrievable (got {len(all_runs)})")

        complete_count = sum(1 for r in all_runs if r["status"] == "complete")
        error_count = sum(1 for r in all_runs if r["status"] == "error")
        check(complete_count == 3, f"3 complete (got {complete_count})")
        check(error_count == 2, f"2 errors (got {error_count})")

    finally:
        _cleanup_db(db_path)

    # ─── 10. Daily Summary ────────────────────────────────────────────────
    print("\n=== TelemetryStore: Daily Summary ===")

    db_path = _make_test_db()
    try:
        ts = TelemetryStore(db_path)

        # Populate some data
        ts.record_run_start("job-daily-1", "Client A", "tax_returns")
        ts.record_run_complete("job-daily-1", {"timing": {"total_s": 30}, "cost": {"total_cost_usd": 0.02}})
        ts.record_smoke_result(passed=5, total_checks=5, results=[], duration_s=0.1)

        summary = ts.daily_summary()
        check(isinstance(summary, dict), "daily_summary returns dict")
        check("date" in summary, "summary has date")
        check("runs" in summary, "summary has runs")
        check("smoke" in summary, "summary has smoke")
        check("backup" in summary, "summary has backup")
        check("drift" in summary, "summary has drift")

    finally:
        _cleanup_db(db_path)

    # ─── 11. CAS Health Summary ───────────────────────────────────────────
    print("\n=== TelemetryStore: CAS Health Summary ===")

    db_path = _make_test_db()
    try:
        ts = TelemetryStore(db_path)

        # Empty state
        health = ts.cas_health_summary()
        check(health["state"] == "unknown", f"empty state = 'unknown' (got '{health.get('state')}')")
        check(health["label"] == "No Data", f"empty label = 'No Data' (got '{health.get('label')}')")

        # Add good smoke
        ts.record_smoke_result(passed=5, total_checks=5, results=[], duration_s=0.1)
        health = ts.cas_health_summary()
        check(health["smoke"]["state"] == "good", "smoke state = good after all pass")

        # Add good backup (recent)
        from datetime import datetime
        ts.record_backup("/tmp/test.db", 1000, "sha", {"facts": 10})

        health = ts.cas_health_summary()
        check(health["backup"]["state"] == "good", "backup state = good when recent")

    finally:
        _cleanup_db(db_path)

    # ─── 12. Isolation: op_* Tables Independent of Financial Tables ──────
    print("\n=== Isolation: CAS Tables Independent of Financial Tables ===")

    db_path = _make_test_db()
    try:
        ts = TelemetryStore(db_path)

        # Write some CAS data
        ts.record_run_start("job-iso-1", "Client", "tax_returns")
        ts.record_drift("job-iso-1", edit_rate=0.1)
        ts.record_smoke_result(passed=1, total_checks=1, results=[], duration_s=0.1)

        # Write some financial data
        conn = sqlite3.connect(db_path)
        now = datetime.now().isoformat()
        conn.execute(
            """INSERT INTO facts (job_id, client_id, tax_year, fact_key, value_num, status, updated_at)
               VALUES ('job-iso-1', 'client-1', 2025, 'W-2.wages', 85000, 'extracted', ?)""",
            (now,)
        )
        conn.commit()

        # Drop all op_* tables
        conn.execute("DROP TABLE IF EXISTS op_runs")
        conn.execute("DROP TABLE IF EXISTS op_phases")
        conn.execute("DROP TABLE IF EXISTS op_drift")
        conn.execute("DROP TABLE IF EXISTS op_smoke_results")
        conn.execute("DROP TABLE IF EXISTS op_golden_results")
        conn.execute("DROP TABLE IF EXISTS op_backups")
        conn.execute("DROP TABLE IF EXISTS op_change_requests")
        conn.execute("DROP TABLE IF EXISTS op_cr_findings")
        conn.execute("DROP TABLE IF EXISTS op_post_fix_gates")
        conn.commit()

        # Financial data still intact
        fact = conn.execute("SELECT value_num FROM facts WHERE fact_key = 'W-2.wages'").fetchone()
        check(fact is not None, "facts table intact after dropping op_* tables")
        check(fact[0] == 85000, f"fact value preserved (got {fact[0]})")

        conn.close()

    finally:
        _cleanup_db(db_path)

    # ─── 13. Module Guard: Forbidden Imports ──────────────────────────────
    print("\n=== Isolation: Module Guard ===")

    from telemetry_store import _FORBIDDEN_MODULES

    forbidden_list = list(_FORBIDDEN_MODULES)
    check("fact_store" in forbidden_list, "fact_store forbidden")
    check("extract" in forbidden_list, "extract forbidden")
    check("pytesseract" in forbidden_list, "pytesseract forbidden")
    check("anthropic" in forbidden_list, "anthropic forbidden")
    check("pdf2image" in forbidden_list, "pdf2image forbidden")
    check("workpaper_export" in forbidden_list, "workpaper_export forbidden")

    # Verify telemetry_store does NOT actually import forbidden modules
    import telemetry_store as _ts_module
    import importlib
    for mod_name in ["fact_store", "extract"]:
        check(mod_name not in dir(_ts_module),
              f"{mod_name} not imported by telemetry_store")

    # ─── 14. Run Replace on Duplicate job_id ──────────────────────────────
    print("\n=== TelemetryStore: Idempotent Run Start ===")

    db_path = _make_test_db()
    try:
        ts = TelemetryStore(db_path)

        # Start same job twice (e.g. retry)
        ts.record_run_start("job-dup", "Client A", "tax_returns")
        ts.record_run_start("job-dup", "Client A", "tax_returns")  # re-insert

        runs = ts.get_recent_runs(limit=10)
        dup_runs = [r for r in runs if r["job_id"] == "job-dup"]
        check(len(dup_runs) == 1, f"duplicate job_id results in 1 row (got {len(dup_runs)})")

    finally:
        _cleanup_db(db_path)

    # ─── 15. Error Message Truncation ─────────────────────────────────────
    print("\n=== TelemetryStore: Error Truncation ===")

    db_path = _make_test_db()
    try:
        ts = TelemetryStore(db_path)
        ts.record_run_start("job-err-trunc", "Client", "tax_returns")

        long_error = "X" * 5000
        ts.record_run_error("job-err-trunc", long_error)

        run = ts.get_run("job-err-trunc")
        check(len(run["error_message"]) <= 2000,
              f"error truncated to <=2000 chars (got {len(run.get('error_message', ''))})")

    finally:
        _cleanup_db(db_path)

    # ─── 16. Smoke Tests (assurance_smoke.py) ────────────────────────────
    print("\n=== Smoke Tests: assurance_smoke.py ===")

    from assurance_smoke import run_smoke_tests

    db_path = _make_test_db()
    try:
        # Create minimal required dirs for smoke tests
        base_dir = tempfile.mkdtemp()
        data_dir = os.path.join(base_dir, "data")
        os.makedirs(data_dir, exist_ok=True)
        os.makedirs(os.path.join(data_dir, "uploads"), exist_ok=True)
        os.makedirs(os.path.join(data_dir, "outputs"), exist_ok=True)
        os.makedirs(os.path.join(data_dir, "page_images"), exist_ok=True)
        os.makedirs(os.path.join(data_dir, "backups"), exist_ok=True)

        # Copy test DB into the data dir so smoke can find it
        import shutil as _shutil
        test_db = os.path.join(data_dir, "bearden.db")
        _shutil.copy2(db_path, test_db)

        # Also create a fake extract.py in base_dir
        with open(os.path.join(base_dir, "extract.py"), "w") as f:
            f.write("# fake extract.py for smoke tests\n")

        result = run_smoke_tests(test_db, base_dir)

        check(isinstance(result, dict), "smoke returns dict")
        check("passed" in result, "result has 'passed' key")
        check("total" in result, "result has 'total' key")
        check("results" in result, "result has 'results' list")
        check("duration_s" in result, "result has 'duration_s'")
        check(result["total"] == 8, f"8 smoke checks (got {result.get('total')})")
        check(isinstance(result["results"], list), "results is a list")
        check(len(result["results"]) == 8, f"8 result items (got {len(result.get('results', []))})")

        # Check specific checks passed
        results_by_name = {r["name"]: r for r in result["results"]}
        check(results_by_name.get("db_writable", {}).get("passed") is True, "db_writable passed")
        check(results_by_name.get("op_tables_exist", {}).get("passed") is True, "op_tables_exist passed")
        check(results_by_name.get("extract_exists", {}).get("passed") is True, "extract_exists passed")
        check(results_by_name.get("data_dirs_writable", {}).get("passed") is True, "data_dirs_writable passed")
        check(results_by_name.get("backups_dir_exists", {}).get("passed") is True, "backups_dir_exists passed")

        # Cleanup
        _shutil.rmtree(base_dir, ignore_errors=True)

    finally:
        _cleanup_db(db_path)

    # ─── 17. Backup Manager (assurance_backup.py) ─────────────────────────
    print("\n=== Backup Manager: assurance_backup.py ===")

    from assurance_backup import create_backup, verify_backup, cleanup_old_backups

    db_path = _make_test_db()
    try:
        ts = TelemetryStore(db_path)
        # Seed some data so backup has content
        ts.record_run_start("job-backup-1", "Client", "tax_returns")

        backup_dir = tempfile.mkdtemp()

        # Create backup
        result = create_backup(db_path, backup_dir)
        check(isinstance(result, dict), "create_backup returns dict")
        check("path" in result, "result has path")
        check("sha256" in result, "result has sha256")
        check("size_bytes" in result, "result has size_bytes")
        check("row_counts" in result, "result has row_counts")
        check(result["size_bytes"] > 0, f"backup has content ({result.get('size_bytes')} bytes)")
        check(os.path.exists(result["path"]), "backup file exists on disk")
        check(len(result["sha256"]) == 64, f"sha256 is 64 chars (got {len(result.get('sha256', ''))})")
        check("op_runs" in result["row_counts"], "row_counts includes op_runs")

        # Verify backup
        verify = verify_backup(result["path"], result["sha256"])
        check(verify["verified"] is True, "backup verified successfully")
        check(verify["sha256"] == result["sha256"], "sha256 matches")
        check(verify["tables_ok"] is True, "tables readable in backup")

        # Verify with wrong hash
        verify_bad = verify_backup(result["path"], "wronghash")
        check(verify_bad["verified"] is False, "wrong hash fails verification")

        # Verify nonexistent file
        verify_missing = verify_backup("/tmp/nonexistent_backup.db")
        check(verify_missing["verified"] is False, "missing file fails verification")

        # Cleanup old backups
        # Create a few more backups (microsecond timestamps ensure uniqueness)
        import time as _time
        for i in range(3):
            _time.sleep(0.05)  # Small delay for mtime ordering
            create_backup(db_path, backup_dir)

        # Count files before cleanup
        from pathlib import Path as _Path
        backup_files_before = list(_Path(backup_dir).glob("bearden_*.db"))
        check(len(backup_files_before) == 4,
              f"4 backup files before cleanup (got {len(backup_files_before)})")

        cleanup_result = cleanup_old_backups(backup_dir, keep=2)
        check(isinstance(cleanup_result, dict), "cleanup returns dict")
        check(cleanup_result["kept"] == 2, f"kept 2 backups (got {cleanup_result.get('kept')})")
        check(cleanup_result["removed"] == 2, f"removed 2 backups (got {cleanup_result.get('removed')})")

        # Cleanup
        import shutil as _shutil2
        _shutil2.rmtree(backup_dir, ignore_errors=True)

    finally:
        _cleanup_db(db_path)

    # ─── 18. CAS Reports (cas_reports.py) ────────────────────────────────
    print("\n=== CAS Reports: cas_reports.py ===")

    from cas_reports import CASReportGenerator, _FORBIDDEN_MODULES as report_forbidden

    # Module guard
    check("fact_store" in report_forbidden, "cas_reports forbids fact_store")
    check("extract" in report_forbidden, "cas_reports forbids extract")
    check("workpaper_export" in report_forbidden, "cas_reports forbids workpaper_export")

    db_path = _make_test_db()
    try:
        ts = TelemetryStore(db_path)

        # Seed data for reports
        ts.record_run_start("job-r-1", "Client A", "tax_returns", app_version="5.2")
        ts.record_run_complete("job-r-1", {
            "timing": {"total_s": 30, "phases": {"ocr": 5, "extract": 20, "verify": 5}},
            "cost": {"total_cost_usd": 0.02, "total_api_calls": 8,
                     "total_input_tokens": 30000, "total_output_tokens": 3000,
                     "api_calls_by_type": {"vision": 2, "text": 5, "classify": 1}},
            "routing": {"total_pages": 4, "page_methods": {"1": "ocr", "2": "ocr", "3": "vision", "4": "ocr"},
                        "skipped_blank": 0},
            "throughput": {"total_fields": 20, "high_confidence_fields": 18,
                           "low_confidence_fields": 1, "needs_review_fields": 1,
                           "batches_total": 2},
            "streaming": {"time_to_first_values_s": 5.0, "fields_streamed": 20},
        })
        ts.record_smoke_result(passed=8, total_checks=8,
                                results=[{"name": "db_writable", "passed": True, "message": "OK"}],
                                duration_s=0.5)
        ts.record_drift("job-r-1", edit_rate=0.05, needs_review_rate=0.02, audit_pass_rate=1.0)
        ts.record_backup("/tmp/test.db", 50000, "abc123", {"facts": 50, "jobs": 3})

        gen = CASReportGenerator(ts, app_version="5.2", environment="test")

        # R1: Daily Health
        r1 = gen.render_daily_health()
        check(isinstance(r1, dict), "R1 returns dict")
        check("markdown" in r1, "R1 has markdown")
        check("json" in r1, "R1 has json")
        check(r1["report_type"] == "R1", f"R1 report_type = 'R1' (got '{r1.get('report_type')}')")
        check("Daily Health" in r1["markdown"], "R1 markdown contains title")
        check("Performance Snapshot" in r1["markdown"], "R1 has Performance section")
        check("Drift Metrics" in r1["markdown"], "R1 has Drift section")
        check("Backup" in r1["markdown"], "R1 has Backup section")

        # R2: Runs
        r2 = gen.render_runs(limit=10)
        check(r2["report_type"] == "R2", "R2 report_type correct")
        check("Recent Runs" in r2["markdown"], "R2 has runs table")
        check("Aggregates" in r2["markdown"], "R2 has aggregates")

        # R3: Regressions
        r3 = gen.render_regressions()
        check(r3["report_type"] == "R3", "R3 report_type correct")
        check("Regression" in r3["markdown"], "R3 has regression content")

        # R4: Backups
        r4 = gen.render_backups()
        check(r4["report_type"] == "R4", "R4 report_type correct")
        check("Backup" in r4["markdown"], "R4 has backup content")
        check("abc123" in r4["markdown"][:500], "R4 contains backup SHA")

        # Agent Pack
        zip_bytes = gen.build_agent_pack()
        check(isinstance(zip_bytes, bytes), "Agent Pack is bytes")
        check(len(zip_bytes) > 100, f"Agent Pack has content ({len(zip_bytes)} bytes)")

        # Verify zip contents
        import zipfile as _zipfile
        buf = io.BytesIO(zip_bytes)
        with _zipfile.ZipFile(buf) as zf:
            names = zf.namelist()
            check("README.md" in names, "Agent Pack contains README.md")
            check("R1_daily_health.md" in names, "Agent Pack contains R1 markdown")
            check("R1_daily_health.json" in names, "Agent Pack contains R1 JSON")
            check("R2_runs.md" in names, "Agent Pack contains R2 markdown")
            check("R4_backups.md" in names, "Agent Pack contains R4 markdown")
            check(len(names) >= 9, f"Agent Pack has >= 9 files (got {len(names)})")

            # Verify README content
            readme = zf.read("README.md").decode("utf-8")
            check("Agent Pack" in readme, "README has title")
            check("Suggested Next Tasks" in readme, "README has suggested tasks")

    finally:
        _cleanup_db(db_path)

    # ─── 19. Full Isolation: Drop op_* Tables, Financial Pipeline Intact ──
    print("\n=== Full Isolation: Financial Pipeline Independence ===")

    db_path = _make_test_db()
    try:
        ts = TelemetryStore(db_path)

        # Write CAS + financial data
        ts.record_run_start("job-full-iso", "Client", "tax_returns")
        ts.record_run_complete("job-full-iso", {"timing": {"total_s": 10}})
        ts.record_drift("job-full-iso", edit_rate=0.1)
        ts.record_smoke_result(passed=5, total_checks=5, results=[], duration_s=0.1)
        ts.record_golden_result("test-golden", passed=1, total_checks=1)
        ts.record_backup("/tmp/test.db", 1000, "sha", {})

        conn = sqlite3.connect(db_path)
        from datetime import datetime
        now = datetime.now().isoformat()

        # Insert financial data
        conn.execute(
            """INSERT INTO facts (job_id, client_id, tax_year, fact_key, value_num, status, updated_at)
               VALUES ('job-full-iso', 'client-1', 2025, 'W-2.wages', 85000, 'confirmed', ?)""",
            (now,)
        )
        conn.execute(
            """INSERT INTO verified_fields (job_id, field_key, canonical_value, status, verified_at)
               VALUES ('job-full-iso', 'W-2.wages', '85000', 'confirmed', ?)""",
            (now,)
        )
        conn.commit()

        # Verify both domains have data
        op_run_count = conn.execute("SELECT COUNT(*) FROM op_runs").fetchone()[0]
        fact_count = conn.execute("SELECT COUNT(*) FROM facts").fetchone()[0]
        vf_count = conn.execute("SELECT COUNT(*) FROM verified_fields").fetchone()[0]
        check(op_run_count > 0, f"op_runs has data ({op_run_count} rows)")
        check(fact_count > 0, f"facts has data ({fact_count} rows)")
        check(vf_count > 0, f"verified_fields has data ({vf_count} rows)")

        # Drop ALL op_* tables
        for table in ["op_runs", "op_phases", "op_drift",
                       "op_smoke_results", "op_golden_results", "op_backups",
                       "op_change_requests", "op_cr_findings", "op_post_fix_gates"]:
            conn.execute(f"DROP TABLE IF EXISTS {table}")
        conn.commit()

        # Financial data completely intact
        fact = conn.execute("SELECT value_num, status FROM facts WHERE fact_key = 'W-2.wages'").fetchone()
        check(fact is not None, "facts row survives op_* drop")
        check(fact[0] == 85000, f"fact value = 85000 (got {fact[0]})")
        check(fact[1] == "confirmed", f"fact status = confirmed (got {fact[1]})")

        vf = conn.execute("SELECT canonical_value FROM verified_fields WHERE field_key = 'W-2.wages'").fetchone()
        check(vf is not None, "verified_fields row survives op_* drop")
        check(vf[0] == "85000", f"vf value = '85000' (got {vf[0]})")

        # Op tables are gone
        remaining_op = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'op_%'"
        ).fetchall()
        check(len(remaining_op) == 0, f"all op_* tables dropped (remaining: {len(remaining_op)})")

        conn.close()

    finally:
        _cleanup_db(db_path)

    # ─── 20. Module Independence: No Cross-Domain Imports ─────────────────
    print("\n=== Module Independence: Cross-Domain Guard ===")

    import importlib

    # telemetry_store should not have imported fact_store or extract
    import telemetry_store as _ts
    ts_imported = set(dir(_ts))
    check("FactStore" not in ts_imported, "TelemetryStore does not import FactStore")
    check("fact_store" not in sys.modules or "telemetry_store" not in getattr(sys.modules.get("fact_store", None), "__name__", ""),
          "No circular dependency between telemetry_store and fact_store")

    # cas_reports should not have imported fact_store or extract
    import cas_reports as _cr
    cr_imported = set(dir(_cr))
    check("FactStore" not in cr_imported, "cas_reports does not import FactStore")
    check("extract" not in cr_imported, "cas_reports does not import extract")

    # Verify _FORBIDDEN_MODULES consistency
    from telemetry_store import _FORBIDDEN_MODULES as ts_forbidden
    from cas_reports import _FORBIDDEN_MODULES as cr_forbidden
    check(ts_forbidden == cr_forbidden, "telemetry_store and cas_reports share same forbidden set")

    # ─── 21. T-CAS-2B: CR ID Generation ─────────────────────────────────
    print("\n=== T-CAS-2B: CR ID Generation ===")

    db_path = _make_test_db()
    try:
        from telemetry_store import TelemetryStore
        ts = TelemetryStore(db_path)

        cr_id_1 = ts._next_cr_id()
        check(cr_id_1.startswith("CR-"), f"CR ID starts with CR- (got {cr_id_1})")
        check(cr_id_1.endswith("-001"), f"First CR of day ends with -001 (got {cr_id_1})")

        # Create a CR to advance the counter
        ts.create_change_request(source="smoke", severity="WARNING",
                                 trigger_summary="Test CR 1")
        cr_id_2 = ts._next_cr_id()
        check(cr_id_2.endswith("-002"), f"Second CR of day ends with -002 (got {cr_id_2})")

        # Verify format: CR-YYYYMMDD-NNN
        parts = cr_id_1.split("-")
        check(len(parts) == 3, f"CR ID has 3 parts separated by - (got {len(parts)})")
        check(len(parts[1]) == 8, f"Date part is 8 digits (got {len(parts[1])})")
        check(len(parts[2]) == 3, f"Sequence part is 3 digits (got {len(parts[2])})")

    finally:
        _cleanup_db(db_path)

    # ─── 22. T-CAS-2B: CR Lifecycle ──────────────────────────────────────
    print("\n=== T-CAS-2B: CR Lifecycle ===")

    db_path = _make_test_db()
    try:
        from telemetry_store import TelemetryStore
        ts = TelemetryStore(db_path)

        # Create CR with findings
        findings = [
            {"severity": "CRITICAL", "source": "smoke", "check_name": "db_writable",
             "details": "DB write failed", "measured_value": "FAIL", "threshold": "PASS",
             "recommended_action": "Check disk space"},
            {"severity": "WARNING", "source": "smoke", "check_name": "disk_space",
             "details": "Low disk", "measured_value": "200MB", "threshold": "500MB",
             "recommended_action": "Free disk space"},
        ]
        result = ts.create_change_request(
            source="smoke", severity="CRITICAL",
            trigger_summary="Smoke test failed 2/8",
            trigger_snapshot={"passed": 6, "total": 8},
            findings=findings,
        )
        check(result["cr_id"].startswith("CR-"), f"create_change_request returns cr_id ({result['cr_id']})")
        check(result["status"] == "open", "CR status is 'open'")
        check(result["findings_count"] == 2, f"findings_count == 2 (got {result['findings_count']})")
        check(result["folder_path"].startswith("data/reports/"), f"folder_path set ({result['folder_path']})")

        cr_id = result["cr_id"]

        # Get CR with findings
        cr = ts.get_change_request(cr_id)
        check(cr is not None, "get_change_request returns CR")
        check(cr["severity"] == "CRITICAL", f"severity = CRITICAL (got {cr['severity']})")
        check(cr["source"] == "smoke", f"source = smoke (got {cr['source']})")
        check(len(cr["findings"]) == 2, f"2 findings attached (got {len(cr['findings'])})")
        check(cr["findings"][0]["finding_id"] == "F-001", f"First finding is F-001 (got {cr['findings'][0]['finding_id']})")
        check(cr["findings"][1]["check_name"] == "disk_space", f"Second finding check_name = disk_space")
        check(cr["gate"] is None, "No gate result yet")

        # List open CRs
        open_crs = ts.get_open_change_requests()
        check(len(open_crs) == 1, f"1 open CR (got {len(open_crs)})")
        check(open_crs[0]["cr_id"] == cr_id, "open CR matches created")

        # Update status
        ok = ts.update_cr_status(cr_id, "fix_submitted")
        check(ok, "update_cr_status returns True")
        cr2 = ts.get_change_request(cr_id)
        check(cr2["status"] == "fix_submitted", f"status updated to fix_submitted (got {cr2['status']})")

        # Close CR
        ok = ts.update_cr_status(cr_id, "closed", closed_by="jeffrey")
        check(ok, "close CR returns True")
        cr3 = ts.get_change_request(cr_id)
        check(cr3["status"] == "closed", "status = closed")
        check(cr3["closed_at"] is not None, "closed_at is set")
        check(cr3["closed_by"] == "jeffrey", f"closed_by = jeffrey (got {cr3['closed_by']})")

        # Closed CR excluded from open list
        open_crs2 = ts.get_open_change_requests()
        check(len(open_crs2) == 0, "closed CR not in open list")

        # All CRs still returned by get_all
        all_crs = ts.get_all_change_requests()
        check(len(all_crs) == 1, "get_all_change_requests returns 1")

        # Nonexistent CR
        check(ts.get_change_request("CR-00000000-999") is None, "nonexistent CR returns None")
        check(not ts.update_cr_status("CR-00000000-999", "closed"), "update nonexistent CR returns False")

    finally:
        _cleanup_db(db_path)

    # ─── 23. T-CAS-2B: Fix Manifest ─────────────────────────────────────
    print("\n=== T-CAS-2B: Fix Manifest ===")

    db_path = _make_test_db()
    try:
        from telemetry_store import TelemetryStore
        ts = TelemetryStore(db_path)

        # Create a CR first
        cr = ts.create_change_request(
            source="smoke", severity="WARNING",
            trigger_summary="Smoke failed",
            findings=[{"severity": "WARNING", "source": "smoke", "check_name": "disk_space",
                       "details": "Low disk", "measured_value": "200MB", "threshold": "500MB"}],
        )
        cr_id = cr["cr_id"]

        # Empty files_changed fails
        result = ts.submit_fix_manifest(cr_id, {
            "files_changed": [],
            "description": "Fixed disk issue",
            "author": "jeffrey",
        })
        check(not result["success"], "Empty files_changed rejected")
        check("files_changed" in result["error"], f"Error mentions files_changed (got: {result['error']})")

        # Empty description fails
        result = ts.submit_fix_manifest(cr_id, {
            "files_changed": ["app.py"],
            "description": "",
            "author": "jeffrey",
        })
        check(not result["success"], "Empty description rejected")

        # Valid manifest succeeds
        result = ts.submit_fix_manifest(cr_id, {
            "files_changed": ["app.py", "config.py"],
            "tests_added": ["test_fix.py"],
            "config_changed": [],
            "description": "Fixed disk space check threshold",
            "author": "jeffrey",
            "timestamp": "2026-02-18T12:00:00",
        })
        check(result["success"], "Valid manifest accepted")
        check(result["error"] is None, "No error on valid manifest")

        # CR status updated
        cr2 = ts.get_change_request(cr_id)
        check(cr2["status"] == "fix_submitted", f"Status = fix_submitted (got {cr2['status']})")

        # Manifest for nonexistent CR
        result = ts.submit_fix_manifest("CR-00000000-999", {
            "files_changed": ["x.py"], "description": "test"
        })
        check(not result["success"], "Manifest for nonexistent CR fails")

    finally:
        _cleanup_db(db_path)

    # ─── 24. T-CAS-2B: Post-Fix Gate ─────────────────────────────────────
    print("\n=== T-CAS-2B: Post-Fix Gate ===")

    db_path = _make_test_db()
    try:
        from telemetry_store import TelemetryStore
        ts = TelemetryStore(db_path)

        # Create CR and submit manifest
        cr = ts.create_change_request(
            source="smoke", severity="WARNING",
            trigger_summary="Smoke failed",
            findings=[{"severity": "WARNING", "source": "smoke", "check_name": "disk_space"}],
        )
        cr_id = cr["cr_id"]
        ts.submit_fix_manifest(cr_id, {
            "files_changed": ["app.py"], "description": "Fixed it", "author": "jeffrey"
        })

        # Record ACCEPTED gate
        result = ts.record_gate_result(
            cr_id, "ACCEPTED", checks_run=8, checks_passed=8,
            before_snapshot={"passed": 6}, after_snapshot={"passed": 8},
            details=[{"check": "disk_space", "passed": True}],
        )
        check(result["gate_result"] == "ACCEPTED", f"Gate result = ACCEPTED (got {result['gate_result']})")
        check(result["cr_status"] == "gate_passed", f"CR status = gate_passed (got {result['cr_status']})")

        # Gate included in get_change_request
        cr2 = ts.get_change_request(cr_id)
        check(cr2["gate"] is not None, "Gate result included in CR")
        check(cr2["gate"]["gate_result"] == "ACCEPTED", "Gate result value correct")
        check(cr2["gate"]["checks_run"] == 8, f"checks_run = 8 (got {cr2['gate']['checks_run']})")

        # Test REJECTED gate
        cr3 = ts.create_change_request(
            source="golden", severity="CRITICAL",
            trigger_summary="Golden failed",
            findings=[{"severity": "CRITICAL", "source": "golden", "check_name": "w2-basic"}],
        )
        cr_id3 = cr3["cr_id"]
        ts.submit_fix_manifest(cr_id3, {
            "files_changed": ["extract.py"], "description": "Tried fix", "author": "jeffrey"
        })
        result3 = ts.record_gate_result(
            cr_id3, "REJECTED", checks_run=3, checks_passed=1,
            before_snapshot={"passed": 1}, after_snapshot={"passed": 1},
        )
        check(result3["cr_status"] == "gate_failed", f"REJECTED gate → gate_failed (got {result3['cr_status']})")

        # NEEDS_REVIEW gate
        cr4 = ts.create_change_request(
            source="drift", severity="WARNING",
            trigger_summary="Drift exceeded",
            findings=[{"severity": "WARNING", "source": "drift", "check_name": "edit_rate"}],
        )
        cr_id4 = cr4["cr_id"]
        ts.submit_fix_manifest(cr_id4, {
            "files_changed": ["extract.py"], "description": "Partial fix", "author": "jeffrey"
        })
        result4 = ts.record_gate_result(
            cr_id4, "NEEDS_REVIEW", checks_run=3, checks_passed=2,
        )
        check(result4["cr_status"] == "gate_failed", f"NEEDS_REVIEW gate → gate_failed (got {result4['cr_status']})")

    finally:
        _cleanup_db(db_path)

    # ─── 25. T-CAS-2B: Merge Guard ───────────────────────────────────────
    print("\n=== T-CAS-2B: Merge Guard ===")

    db_path = _make_test_db()
    try:
        from telemetry_store import TelemetryStore
        ts = TelemetryStore(db_path)

        # Create CR with findings
        cr = ts.create_change_request(
            source="smoke", severity="WARNING",
            trigger_summary="Smoke test failed",
            findings=[{"severity": "WARNING", "source": "smoke", "check_name": "disk_space"}],
        )
        cr_id = cr["cr_id"]

        # No manifest → can't merge
        mg1 = ts.can_merge_fix(cr_id)
        check(not mg1["can_merge"], "Can't merge: no manifest")
        check("manifest" in mg1["reason"].lower(), f"Reason mentions manifest (got: {mg1['reason']})")

        # Submit manifest, no gate → can't merge
        ts.submit_fix_manifest(cr_id, {
            "files_changed": ["app.py"], "description": "Fixed", "author": "jeffrey"
        })
        mg2 = ts.can_merge_fix(cr_id)
        check(not mg2["can_merge"], "Can't merge: no gate")
        check("gate" in mg2["reason"].lower(), f"Reason mentions gate (got: {mg2['reason']})")

        # REJECTED gate → can't merge
        ts.record_gate_result(cr_id, "REJECTED", checks_run=8, checks_passed=5)
        mg3 = ts.can_merge_fix(cr_id)
        check(not mg3["can_merge"], "Can't merge: REJECTED gate")
        check("REJECTED" in mg3["reason"], f"Reason mentions REJECTED (got: {mg3['reason']})")

        # ACCEPTED gate → CAN merge
        ts.record_gate_result(cr_id, "ACCEPTED", checks_run=8, checks_passed=8)
        mg4 = ts.can_merge_fix(cr_id)
        check(mg4["can_merge"], "CAN merge after ACCEPTED gate")
        check("met" in mg4["reason"].lower(), f"Reason confirms conditions met (got: {mg4['reason']})")

        # Nonexistent CR
        mg5 = ts.can_merge_fix("CR-00000000-999")
        check(not mg5["can_merge"], "Can't merge nonexistent CR")

    finally:
        _cleanup_db(db_path)

    # ─── 26. T-CAS-2B: Error Rate & Drift Thresholds ────────────────────
    print("\n=== T-CAS-2B: Error Rate & Drift Thresholds ===")

    db_path = _make_test_db()
    try:
        from telemetry_store import TelemetryStore
        ts = TelemetryStore(db_path)

        # No runs → None
        result = ts.get_error_rate_24h()
        check(result is None, "No runs in 24h returns None")

        # Add runs: 5 total, 2 errors → 40% error rate
        from datetime import datetime, timedelta
        now = datetime.now()
        conn = sqlite3.connect(db_path)
        for i in range(5):
            status = "error" if i < 2 else "complete"
            started = (now - timedelta(hours=1, minutes=i)).isoformat()
            conn.execute(
                "INSERT INTO op_runs (job_id, status, started_at) VALUES (?, ?, ?)",
                (f"err-test-{i}", status, started)
            )
        conn.commit()
        conn.close()

        result = ts.get_error_rate_24h()
        check(result is not None, "With runs, returns dict")
        check(result["total_runs"] == 5, f"total_runs = 5 (got {result['total_runs']})")
        check(result["error_runs"] == 2, f"error_runs = 2 (got {result['error_runs']})")
        check(abs(result["error_rate"] - 0.4) < 0.01, f"error_rate ≈ 0.4 (got {result['error_rate']})")

        # Drift thresholds: no drift data → not triggered
        drift_result = ts.check_drift_thresholds()
        check(not drift_result["triggered"], "No drift data → not triggered")
        check(len(drift_result["violations"]) == 0, "No violations with no data")

        # Add drift within bounds
        ts.record_drift("job-ok", edit_rate=0.10, needs_review_rate=0.15, audit_pass_rate=0.95)
        drift_result = ts.check_drift_thresholds()
        check(not drift_result["triggered"], "Drift within bounds → not triggered")

        # Add drift exceeding edit_rate threshold
        ts.record_drift("job-bad", edit_rate=0.25, needs_review_rate=0.10, audit_pass_rate=0.95)
        drift_result = ts.check_drift_thresholds()
        check(drift_result["triggered"], "edit_rate > 0.15 → triggered")
        check(len(drift_result["violations"]) == 1, f"1 violation (got {len(drift_result['violations'])})")
        check(drift_result["violations"][0]["check_name"] == "edit_rate",
              f"Violation is edit_rate (got {drift_result['violations'][0]['check_name']})")

        # Add drift exceeding multiple thresholds
        ts.record_drift("job-worse", edit_rate=0.20, needs_review_rate=0.30, audit_pass_rate=0.80)
        drift_result = ts.check_drift_thresholds()
        check(drift_result["triggered"], "Multiple thresholds exceeded → triggered")
        check(len(drift_result["violations"]) == 3, f"3 violations (got {len(drift_result['violations'])})")

        # Custom thresholds
        drift_result = ts.check_drift_thresholds({"edit_rate_max": 0.50, "needs_review_rate_max": 0.50, "audit_pass_rate_min": 0.50})
        check(not drift_result["triggered"], "Loose thresholds → not triggered")

    finally:
        _cleanup_db(db_path)

    # ─── 27. T-CAS-2B: CR Reports ───────────────────────────────────────
    print("\n=== T-CAS-2B: CR Reports ===")

    db_path = _make_test_db()
    try:
        from telemetry_store import TelemetryStore
        from cas_reports import CASReportGenerator
        ts = TelemetryStore(db_path)
        gen = CASReportGenerator(ts, app_version="test-2b")

        # Create CR with findings
        cr = ts.create_change_request(
            source="smoke", severity="CRITICAL",
            trigger_summary="Smoke test failed 2/8 checks",
            trigger_snapshot={"passed": 6, "total": 8},
            findings=[
                {"severity": "CRITICAL", "source": "smoke", "check_name": "db_writable",
                 "details": "DB write test failed", "measured_value": "FAIL", "threshold": "PASS",
                 "recommended_action": "Check disk permissions"},
                {"severity": "WARNING", "source": "smoke", "check_name": "disk_space",
                 "details": "Low disk space", "measured_value": "200MB", "threshold": "500MB",
                 "recommended_action": "Free disk space"},
            ],
        )
        cr_id = cr["cr_id"]

        # Render CR findings
        report = gen.render_cr_findings(cr_id)
        check("markdown" in report, "render_cr_findings returns markdown")
        check(cr_id in report["markdown"], f"Markdown contains CR ID ({cr_id})")
        check("db_writable" in report["markdown"], "Markdown contains finding check_name")
        check("json" in report, "render_cr_findings returns json")
        check(report["json"]["cr_id"] == cr_id, "JSON contains correct cr_id")
        check(len(report["json"]["findings"]) == 2, f"JSON has 2 findings (got {len(report['json']['findings'])})")

        # Nonexistent CR
        report2 = gen.render_cr_findings("CR-00000000-999")
        check("not found" in report2["markdown"].lower(), "Nonexistent CR gives error markdown")

        # Build CR agent pack
        zip_bytes = gen.build_cr_agent_pack(cr_id)
        check(len(zip_bytes) > 0, "CR agent pack has content")

        import zipfile as _zf
        _buf = io.BytesIO(zip_bytes)
        with _zf.ZipFile(_buf) as zf:
            names = zf.namelist()
            check("README.md" in names, "Agent pack contains README.md")
            check("findings.md" in names, "Agent pack contains findings.md")
            check("findings.json" in names, "Agent pack contains findings.json")
            check("fix_manifest_template.json" in names, "Agent pack contains fix_manifest_template.json")

            # Verify template content
            template_content = json.loads(zf.read("fix_manifest_template.json"))
            check("files_changed" in template_content, "Template has files_changed field")
            check("description" in template_content, "Template has description field")

        # Static fix_manifest_template
        template = CASReportGenerator.fix_manifest_template()
        check(isinstance(template, dict), "fix_manifest_template returns dict")
        check(template["files_changed"] == [], "Template files_changed is empty list")
        check(template["description"] == "", "Template description is empty string")

    finally:
        _cleanup_db(db_path)

    # ═══════════════════════════════════════════════════════════════════════
    # SUMMARY
    # ═══════════════════════════════════════════════════════════════════════

    total = PASS + FAIL
    print(f"\n{'='*60}")
    print(f"CAS Tests: {PASS} passed, {FAIL} failed out of {total}")
    print(f"{'='*60}")

    return FAIL


if __name__ == "__main__":
    failures = run_tests()
    sys.exit(1 if failures > 0 else 0)
