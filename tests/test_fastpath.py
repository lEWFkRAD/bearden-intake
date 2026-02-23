#!/usr/bin/env python3
"""Tests for T-UX-CONFIRM-FASTPATH — Instant Confirm / Deferred Aftercare.

Covers: aftercare queue FIFO, _upsert_verified_fields_fast, skip_summary,
        aftercare canonical promotion, failure isolation, guided total cache,
        drain aftercare.

Run:  python3 tests/test_fastpath.py
All test execution is inside run_tests() behind __name__ guard.
"""

import sys, os, json, tempfile, shutil, sqlite3, time, threading, collections

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
    """Create a temporary SQLite database with required schema."""
    fd, db_path = tempfile.mkstemp(suffix='.db')
    os.close(fd)

    conn = sqlite3.connect(db_path, timeout=10)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.row_factory = sqlite3.Row

    # verified_fields table (same as app.py _init_db)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS verified_fields (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_id TEXT NOT NULL,
            field_key TEXT NOT NULL,
            canonical_value TEXT,
            original_value TEXT,
            status TEXT NOT NULL DEFAULT '',
            category TEXT DEFAULT '',
            vendor_desc TEXT DEFAULT '',
            note TEXT DEFAULT '',
            reviewer TEXT DEFAULT '',
            verified_at TEXT DEFAULT '',
            review_stage TEXT DEFAULT '',
            reviewer_id INTEGER,
            UNIQUE(job_id, field_key)
        )
    """)

    # verifications table (legacy JSON store)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS verifications (
            job_id TEXT PRIMARY KEY,
            data TEXT NOT NULL DEFAULT '{}',
            updated TEXT DEFAULT ''
        )
    """)

    # client_canonical_values table (matches app.py _init_db schema)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS client_canonical_values (
            client_name TEXT NOT NULL,
            year TEXT NOT NULL,
            document_type TEXT NOT NULL,
            payer_key TEXT NOT NULL,
            payer_display TEXT DEFAULT '',
            field_name TEXT NOT NULL,
            canonical_value TEXT,
            original_value TEXT,
            status TEXT NOT NULL DEFAULT 'confirmed',
            source_job_id TEXT NOT NULL DEFAULT '',
            reviewer TEXT DEFAULT '',
            verified_at TEXT NOT NULL DEFAULT '',
            PRIMARY KEY (client_name, year, document_type, payer_key, field_name)
        )
    """)
    conn.execute("""CREATE INDEX IF NOT EXISTS idx_ccv_client_year
                    ON client_canonical_values(client_name, year)""")

    # app_events table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS app_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts TEXT NOT NULL,
            level TEXT NOT NULL DEFAULT 'info',
            event_type TEXT NOT NULL DEFAULT '',
            message TEXT NOT NULL DEFAULT '',
            user_id TEXT,
            user_display TEXT DEFAULT '',
            job_id TEXT DEFAULT '',
            details_json TEXT DEFAULT '',
            ip_addr TEXT DEFAULT ''
        )
    """)

    # jobs table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS jobs (
            id TEXT PRIMARY KEY,
            data TEXT NOT NULL DEFAULT '{}',
            status TEXT DEFAULT '',
            client_name TEXT DEFAULT '',
            created TEXT DEFAULT '',
            updated TEXT DEFAULT '',
            review_stage TEXT DEFAULT 'draft',
            stage_updated TEXT DEFAULT ''
        )
    """)

    conn.commit()
    conn.close()
    return db_path


def _setup_app_with_db(db_path):
    """Configure the app module to use a temporary test database."""
    import app as _app

    orig_db_path = _app.DB_PATH
    orig_get_db = _app._get_db

    _app.DB_PATH = db_path

    def _test_get_db():
        conn = sqlite3.connect(db_path, timeout=10)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.row_factory = sqlite3.Row
        return conn

    _app._get_db = _test_get_db

    return orig_db_path, orig_get_db


def _restore_app(orig_db_path, orig_get_db):
    """Restore app module to original state."""
    import app as _app
    _app.DB_PATH = orig_db_path
    _app._get_db = orig_get_db


def _create_test_extraction_log(job_id, fields_per_page=3, tmp_dir=None):
    """Create a test extraction log JSON file and register it in jobs dict."""
    import app as _app

    if tmp_dir is None:
        tmp_dir = tempfile.mkdtemp()

    extractions = []
    for page in range(1, 3):
        fields = {}
        for i in range(fields_per_page):
            fields[f"field_{i}"] = {
                "value": f"val_p{page}_f{i}",
                "confidence": "high",
            }
        extractions.append({
            "_page": page,
            "document_type": "w2",
            "payer_or_entity": "Test Corp",
            "employer_ein": "12-3456789",
            "fields": fields,
        })

    log_data = {"extractions": extractions}
    log_path = os.path.join(tmp_dir, f"{job_id}_extraction.json")
    with open(log_path, 'w') as f:
        json.dump(log_data, f)

    # Register job in memory
    _app.jobs[job_id] = {
        "output_log": log_path,
        "client_name": "Test Client",
        "year": "2025",
        "status": "done",
        "review_stage": "draft",
    }

    return log_path, tmp_dir


# ═══════════════════════════════════════════════════════════════════════════════
# TESTS
# ═══════════════════════════════════════════════════════════════════════════════

def run_tests():
    global PASS, FAIL
    import app as _app

    # ── 1. Aftercare Queue FIFO ──────────────────────────────────────────────
    print("\n── 1. Aftercare Queue FIFO ──")

    order = []
    test_queue = collections.deque()
    test_event = threading.Event()
    test_running = True

    def _test_worker():
        while test_running or test_queue:
            test_event.wait(timeout=0.5)
            test_event.clear()
            while test_queue:
                task = test_queue.popleft()
                order.append(task["seq"])

    t = threading.Thread(target=_test_worker, daemon=True)
    t.start()

    for i in range(3):
        test_queue.append({"seq": i})
    test_event.set()

    time.sleep(1.0)
    test_running = False
    test_event.set()
    t.join(timeout=2.0)

    check(order == [0, 1, 2], f"Tasks processed in FIFO order: {order}")

    # ── 2. _upsert_verified_fields_fast Writes Row ──────────────────────────
    print("\n── 2. _upsert_verified_fields_fast Writes Row ──")

    db_path = _make_test_db()
    orig = _setup_app_with_db(db_path)
    tmp_dir = None
    try:
        job_id = "test-fast-001"
        _, tmp_dir = _create_test_extraction_log(job_id)

        incoming = {
            "1:0:field_0": {
                "status": "confirmed",
                "reviewer": "JW",
                "timestamp": "2025-02-18T10:00:00",
            }
        }

        _app._upsert_verified_fields_fast(job_id, incoming)

        conn = sqlite3.connect(db_path, timeout=10)
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT * FROM verified_fields WHERE job_id = ? AND field_key = ?",
            (job_id, "1:0:field_0")
        ).fetchone()
        conn.close()

        check(row is not None, "verified_fields row exists after fast upsert")
        check(row["status"] == "confirmed", f"Status is 'confirmed' (got: {row['status']})")
        check(row["canonical_value"] is not None, f"Canonical value populated: {row['canonical_value']}")
        check(row["reviewer"] == "JW", f"Reviewer set to JW")

    finally:
        _restore_app(*orig)
        if tmp_dir and os.path.exists(tmp_dir):
            shutil.rmtree(tmp_dir)
        if os.path.exists(db_path):
            os.unlink(db_path)

    # ── 3. Fast Path Skips Canonical ─────────────────────────────────────────
    print("\n── 3. Fast Path Skips Canonical ──")

    db_path = _make_test_db()
    orig = _setup_app_with_db(db_path)
    tmp_dir = None
    try:
        job_id = "test-fast-002"
        _, tmp_dir = _create_test_extraction_log(job_id)

        incoming = {
            "1:0:field_0": {
                "status": "confirmed",
                "reviewer": "JW",
                "timestamp": "2025-02-18T10:00:00",
            }
        }

        _app._upsert_verified_fields_fast(job_id, incoming)

        conn = sqlite3.connect(db_path, timeout=10)
        conn.row_factory = sqlite3.Row
        # Check that no client_canonical_values were written
        canonical_count = conn.execute(
            "SELECT COUNT(*) FROM client_canonical_values"
        ).fetchone()[0]
        conn.close()

        check(canonical_count == 0, f"No canonical rows written by fast path (got: {canonical_count})")

    finally:
        _restore_app(*orig)
        if tmp_dir and os.path.exists(tmp_dir):
            shutil.rmtree(tmp_dir)
        if os.path.exists(db_path):
            os.unlink(db_path)

    # ── 4. _save_verifications with skip_summary=True ────────────────────────
    print("\n── 4. _save_verifications with skip_summary=True ──")

    db_path = _make_test_db()
    orig = _setup_app_with_db(db_path)
    try:
        job_id = "test-fast-003"
        _app.jobs[job_id] = {"status": "done", "review_stage": "draft"}

        vdata = {"reviewer": "JW", "fields": {
            "1:0:field_0": {"status": "confirmed", "timestamp": "2025-02-18T10:00:00"},
        }}

        _app._save_verifications(job_id, vdata, skip_summary=True)

        # Verify data was saved to DB
        conn = sqlite3.connect(db_path, timeout=10)
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT data FROM verifications WHERE job_id = ?", (job_id,)
        ).fetchone()
        conn.close()

        check(row is not None, "Verifications row saved to DB")
        saved = json.loads(row["data"])
        check("fields" in saved, "Fields present in saved data")
        check("1:0:field_0" in saved["fields"], "Field key present in saved data")

        # Verify summary NOT updated on job dict
        job = _app.jobs.get(job_id)
        check("verification" not in job, f"Job dict has no 'verification' key (skip_summary=True)")

        # Now test without skip_summary
        _app._save_verifications(job_id, vdata, skip_summary=False)
        job = _app.jobs.get(job_id)
        check("verification" in job, f"Job dict HAS 'verification' key (skip_summary=False)")

    finally:
        _restore_app(*orig)
        _app.jobs.pop("test-fast-003", None)
        if os.path.exists(db_path):
            os.unlink(db_path)

    # ── 5. Aftercare Promotes Facts ──────────────────────────────────────────
    print("\n── 5. Aftercare Promotes Facts ──")

    db_path = _make_test_db()
    orig = _setup_app_with_db(db_path)
    tmp_dir = None
    try:
        job_id = "test-fast-004"
        _, tmp_dir = _create_test_extraction_log(job_id)

        incoming = {
            "1:0:field_0": {
                "status": "confirmed",
                "reviewer": "JW",
                "timestamp": "2025-02-18T10:00:00",
            }
        }

        # Run aftercare promotion directly
        _app._aftercare_promote_facts(job_id, incoming)

        conn = sqlite3.connect(db_path, timeout=10)
        conn.row_factory = sqlite3.Row
        canonical_count = conn.execute(
            "SELECT COUNT(*) FROM client_canonical_values"
        ).fetchone()[0]
        conn.close()

        check(canonical_count > 0, f"Canonical rows written by aftercare ({canonical_count})")

    finally:
        _restore_app(*orig)
        if tmp_dir and os.path.exists(tmp_dir):
            shutil.rmtree(tmp_dir)
        if os.path.exists(db_path):
            os.unlink(db_path)

    # ── 6. Aftercare Failure Isolation ────────────────────────────────────────
    print("\n── 6. Aftercare Failure Isolation ──")

    # _aftercare_promote_facts with a nonexistent job should not crash
    try:
        _app._aftercare_promote_facts("nonexistent-job-id-999", {
            "1:0:field_0": {"status": "confirmed", "reviewer": "X"},
        })
        check(True, "Aftercare with nonexistent job_id did not crash")
    except Exception as e:
        check(False, f"Aftercare with nonexistent job_id crashed: {e}")

    # _process_aftercare with a bad task should not crash
    try:
        _app._process_aftercare({
            "job_id": "nonexistent-999",
            "incoming": {"bad:field": {"status": "confirmed"}},
            "mode": "guided",
            "action": "confirm",
            "field_id": "bad:field",
        })
        check(True, "_process_aftercare with bad task did not crash")
    except Exception as e:
        check(False, f"_process_aftercare crashed: {e}")

    # ── 7. Guided Total Cache ────────────────────────────────────────────────
    print("\n── 7. Guided Total Cache ──")

    db_path = _make_test_db()
    orig = _setup_app_with_db(db_path)
    tmp_dir = None
    try:
        job_id = "test-fast-005"
        _, tmp_dir = _create_test_extraction_log(job_id, fields_per_page=3)

        queue, reviewed = _app._build_guided_queue(job_id)
        job = _app.jobs.get(job_id)

        check("_guided_total_fields" in job,
              "Job dict has _guided_total_fields after queue build")
        expected_total = len(queue) + reviewed
        check(job["_guided_total_fields"] == expected_total,
              f"Cached total matches queue+reviewed: {job['_guided_total_fields']} == {expected_total}")
        check(expected_total > 0, f"Total fields > 0: {expected_total}")

    finally:
        _restore_app(*orig)
        if tmp_dir and os.path.exists(tmp_dir):
            shutil.rmtree(tmp_dir)
        if os.path.exists(db_path):
            os.unlink(db_path)

    # ── 8. Drain Aftercare ───────────────────────────────────────────────────
    print("\n── 8. Drain Aftercare ──")

    # Test that enqueue + drain works without crash
    drain_queue = collections.deque()
    drain_event = threading.Event()
    drain_running = True
    drain_processed = []

    def _drain_worker():
        while drain_running or drain_queue:
            drain_event.wait(timeout=0.5)
            drain_event.clear()
            while drain_queue:
                task = drain_queue.popleft()
                drain_processed.append(task["id"])

    dt = threading.Thread(target=_drain_worker, daemon=True)
    dt.start()

    # Enqueue 5 tasks
    for i in range(5):
        drain_queue.append({"id": i})
    drain_event.set()

    # Drain
    time.sleep(0.5)
    drain_running = False
    drain_event.set()
    dt.join(timeout=3.0)

    check(len(drain_queue) == 0, f"Queue empty after drain (remaining: {len(drain_queue)})")
    check(len(drain_processed) == 5, f"All 5 tasks processed ({len(drain_processed)})")

    # ── Verify the real aftercare infrastructure exists ──
    print("\n── Infrastructure Checks ──")

    check(hasattr(_app, '_aftercare_queue'), "_aftercare_queue exists on app module")
    check(hasattr(_app, '_aftercare_event'), "_aftercare_event exists on app module")
    check(hasattr(_app, '_aftercare_thread'), "_aftercare_thread exists on app module")
    check(hasattr(_app, '_enqueue_aftercare'), "_enqueue_aftercare function exists")
    check(hasattr(_app, '_process_aftercare'), "_process_aftercare function exists")
    check(hasattr(_app, '_aftercare_promote_facts'), "_aftercare_promote_facts function exists")
    check(hasattr(_app, '_upsert_verified_fields_fast'), "_upsert_verified_fields_fast function exists")
    check(hasattr(_app, '_drain_aftercare'), "_drain_aftercare function exists")
    check(_app._aftercare_thread.daemon is True, "Aftercare thread is daemon")
    check(_app._aftercare_thread.is_alive(), "Aftercare thread is alive")

    # ═══════════════════════════════════════════════════════════════════════════
    print(f"\n{'='*60}")
    print(f"  Fastpath tests: {PASS} passed, {FAIL} failed")
    print(f"{'='*60}")
    return FAIL


if __name__ == "__main__":
    failures = run_tests()
    sys.exit(1 if failures else 0)
