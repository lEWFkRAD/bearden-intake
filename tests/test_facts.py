#!/usr/bin/env python3
"""Tests for T1.6.2 — Unified Facts Table + DB-First Chain of Custody.

Covers: early candidate persistence, monotonic trust, correction locks,
        no-downgrade rule, API reads from DB, legacy sync.

DESIGN PRINCIPLE: If a number is not in SQLite, it does not exist.
These tests verify that principle is enforced.

Run:  python3 tests/test_facts.py
All test execution is inside run_tests() behind __name__ guard.
"""

import sys, os, json, tempfile, shutil, sqlite3

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

PASS = 0
FAIL = 0


def check(cond, msg):
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  ✓ {msg}")
    else:
        FAIL += 1
        print(f"  ✗ FAIL: {msg}")


def _make_fs():
    """Create a FactStore with a temp database."""
    from fact_store import FactStore
    fd, db_path = tempfile.mkstemp(suffix='.db')
    os.close(fd)
    return FactStore(db_path), db_path


def _cleanup(db_path):
    try:
        os.unlink(db_path)
    except OSError:
        pass


# ═══════════════════════════════════════════════════════════════════════════════
# TEST A — CANDIDATES APPEAR IN DB IMMEDIATELY
# ═══════════════════════════════════════════════════════════════════════════════

def test_candidate_persisted_immediately():
    """Candidate facts written via upsert_candidate_fact appear in DB immediately."""
    fs, db_path = _make_fs()
    try:
        fs.upsert_candidate_fact(
            job_id="job-001", client_id="Evans, Lisa", tax_year=2025,
            fact_key="W-2.ein:12-3456789.wages",
            value_num=85000.00, status="extracted",
            confidence=0.95, source_method="ocr",
            source_doc="evans-w2.pdf", source_page=1
        )

        fact = fs.get_fact("job-001", 2025, "W-2.ein:12-3456789.wages")
        check(fact is not None, "candidate fact exists in DB immediately")
        check(fact["value_num"] == 85000.00, f"value_num preserved (got {fact['value_num']})")
        check(fact["status"] == "extracted", f"status is extracted (got {fact['status']})")
        check(fact["locked"] is False, "not locked initially")
        check(fact["source_method"] == "ocr", "source_method preserved")
        check(fact["source_doc"] == "evans-w2.pdf", "source_doc preserved")
        check(fact["source_page"] == 1, "source_page preserved")
    finally:
        _cleanup(db_path)


def test_candidate_with_text_value():
    """Text-type facts (distribution codes, names) persist correctly."""
    fs, db_path = _make_fs()
    try:
        fs.upsert_candidate_fact(
            job_id="job-001", client_id="Evans, Lisa", tax_year=2025,
            fact_key="1099-R.ein:44-5555555.distribution_code",
            value_text="7", status="extracted"
        )

        fact = fs.get_fact("job-001", 2025, "1099-R.ein:44-5555555.distribution_code")
        check(fact is not None, "text fact exists")
        check(fact["value_text"] == "7", f"value_text preserved (got {fact['value_text']!r})")
        check(fact["value_num"] is None, "value_num is None for text fact")
    finally:
        _cleanup(db_path)


def test_multiple_candidates_same_job():
    """Multiple facts for the same job are all persisted."""
    fs, db_path = _make_fs()
    try:
        fs.upsert_candidate_fact("job-001", "Evans, Lisa", 2025,
                                  "W-2.ein:111.wages", value_num=50000)
        fs.upsert_candidate_fact("job-001", "Evans, Lisa", 2025,
                                  "W-2.ein:111.federal_wh", value_num=7500)
        fs.upsert_candidate_fact("job-001", "Evans, Lisa", 2025,
                                  "1099-INT.ein:222.interest_income", value_num=1200)

        facts = fs.get_facts_for_job("job-001")
        check(len(facts) == 3, f"3 facts persisted (got {len(facts)})")
    finally:
        _cleanup(db_path)


def test_get_facts_for_job_with_year_filter():
    """get_facts_for_job filters by tax_year when provided."""
    fs, db_path = _make_fs()
    try:
        fs.upsert_candidate_fact("job-001", "Evans", 2025, "W-2.ein:111.wages", value_num=50000)
        fs.upsert_candidate_fact("job-001", "Evans", 2024, "W-2.ein:111.wages", value_num=48000)

        all_facts = fs.get_facts_for_job("job-001")
        check(len(all_facts) == 2, f"2 total facts (got {len(all_facts)})")

        facts_2025 = fs.get_facts_for_job("job-001", tax_year=2025)
        check(len(facts_2025) == 1, f"1 fact for 2025 (got {len(facts_2025)})")
        check(facts_2025[0]["value_num"] == 50000, "correct value for 2025")
    finally:
        _cleanup(db_path)


def test_get_facts_for_client():
    """get_facts_for_client retrieves facts across jobs for one client."""
    fs, db_path = _make_fs()
    try:
        fs.upsert_candidate_fact("job-001", "Evans", 2025, "W-2.ein:111.wages", value_num=50000)
        fs.upsert_candidate_fact("job-002", "Evans", 2025, "1099-INT.ein:222.interest", value_num=1200)
        fs.upsert_candidate_fact("job-003", "Smith", 2025, "W-2.ein:333.wages", value_num=60000)

        evans_facts = fs.get_facts_for_client("Evans")
        check(len(evans_facts) == 2, f"Evans has 2 facts (got {len(evans_facts)})")

        smith_facts = fs.get_facts_for_client("Smith")
        check(len(smith_facts) == 1, f"Smith has 1 fact (got {len(smith_facts)})")
    finally:
        _cleanup(db_path)


# ═══════════════════════════════════════════════════════════════════════════════
# TEST B — LOCKED VALUES NEVER OVERWRITTEN
# ═══════════════════════════════════════════════════════════════════════════════

def test_corrected_fact_is_locked():
    """apply_correction sets locked=1 and status='corrected'."""
    fs, db_path = _make_fs()
    try:
        fs.upsert_candidate_fact("job-001", "Evans", 2025,
                                  "W-2.ein:111.wages", value_num=85000, status="extracted")

        result = fs.apply_correction("job-001", 2025, "W-2.ein:111.wages",
                                      value_num=86000)
        check(result is True, "apply_correction returns True")

        fact = fs.get_fact("job-001", 2025, "W-2.ein:111.wages")
        check(fact["value_num"] == 86000, f"corrected value is 86000 (got {fact['value_num']})")
        check(fact["status"] == "corrected", f"status is corrected (got {fact['status']})")
        check(fact["locked"] is True, "fact is locked")
    finally:
        _cleanup(db_path)


def test_locked_fact_not_overwritten_by_candidate():
    """A locked (corrected) fact cannot be overwritten by upsert_candidate_fact."""
    fs, db_path = _make_fs()
    try:
        # Insert and correct
        fs.upsert_candidate_fact("job-001", "Evans", 2025,
                                  "W-2.ein:111.wages", value_num=85000, status="extracted")
        fs.apply_correction("job-001", 2025, "W-2.ein:111.wages", value_num=86000)

        # Attempt to overwrite with a new candidate
        fs.upsert_candidate_fact("job-001", "Evans", 2025,
                                  "W-2.ein:111.wages", value_num=90000, status="auto_verified")

        fact = fs.get_fact("job-001", 2025, "W-2.ein:111.wages")
        check(fact["value_num"] == 86000,
              f"locked value unchanged (got {fact['value_num']}, expected 86000)")
        check(fact["status"] == "corrected",
              f"locked status unchanged (got {fact['status']})")
        check(fact["locked"] is True, "still locked after candidate attempt")
    finally:
        _cleanup(db_path)


def test_locked_fact_not_upgraded():
    """upgrade_fact_status cannot change a locked fact."""
    fs, db_path = _make_fs()
    try:
        fs.upsert_candidate_fact("job-001", "Evans", 2025,
                                  "W-2.ein:111.wages", value_num=85000, status="extracted")
        fs.apply_correction("job-001", 2025, "W-2.ein:111.wages", value_num=86000)

        result = fs.upgrade_fact_status("job-001", 2025, "W-2.ein:111.wages", "confirmed")
        check(result is False, "upgrade_fact_status returns False for locked fact")

        fact = fs.get_fact("job-001", 2025, "W-2.ein:111.wages")
        check(fact["status"] == "corrected", "status still corrected after upgrade attempt")
    finally:
        _cleanup(db_path)


# ═══════════════════════════════════════════════════════════════════════════════
# TEST C — NO DOWNGRADE (MONOTONIC TRUST)
# ═══════════════════════════════════════════════════════════════════════════════

def test_no_downgrade_auto_verified_to_extracted():
    """auto_verified cannot be downgraded to extracted."""
    fs, db_path = _make_fs()
    try:
        fs.upsert_candidate_fact("job-001", "Evans", 2025,
                                  "W-2.ein:111.wages", value_num=85000,
                                  status="auto_verified")

        # Attempt to overwrite with lower status
        fs.upsert_candidate_fact("job-001", "Evans", 2025,
                                  "W-2.ein:111.wages", value_num=84000,
                                  status="extracted")

        fact = fs.get_fact("job-001", 2025, "W-2.ein:111.wages")
        check(fact["status"] == "auto_verified",
              f"status NOT downgraded (got {fact['status']})")
        check(fact["value_num"] == 85000,
              f"value NOT changed by downgrade attempt (got {fact['value_num']})")
    finally:
        _cleanup(db_path)


def test_no_downgrade_confirmed_to_auto_verified():
    """confirmed cannot be downgraded to auto_verified."""
    fs, db_path = _make_fs()
    try:
        fs.upsert_candidate_fact("job-001", "Evans", 2025,
                                  "W-2.ein:111.wages", value_num=85000, status="extracted")
        fs.upgrade_fact_status("job-001", 2025, "W-2.ein:111.wages", "confirmed")

        # Attempt overwrite with auto_verified
        fs.upsert_candidate_fact("job-001", "Evans", 2025,
                                  "W-2.ein:111.wages", value_num=90000,
                                  status="auto_verified")

        fact = fs.get_fact("job-001", 2025, "W-2.ein:111.wages")
        check(fact["status"] == "confirmed",
              f"confirmed NOT downgraded (got {fact['status']})")
        check(fact["value_num"] == 85000,
              f"value NOT changed (got {fact['value_num']})")
    finally:
        _cleanup(db_path)


def test_no_downgrade_via_upgrade_method():
    """upgrade_fact_status refuses to downgrade."""
    fs, db_path = _make_fs()
    try:
        fs.upsert_candidate_fact("job-001", "Evans", 2025,
                                  "W-2.ein:111.wages", value_num=85000,
                                  status="auto_verified")

        result = fs.upgrade_fact_status("job-001", 2025, "W-2.ein:111.wages", "extracted")
        check(result is False, "upgrade_fact_status refuses downgrade")

        result2 = fs.upgrade_fact_status("job-001", 2025, "W-2.ein:111.wages", "needs_review")
        check(result2 is False, "upgrade refuses lower rank (needs_review < auto_verified)")

        fact = fs.get_fact("job-001", 2025, "W-2.ein:111.wages")
        check(fact["status"] == "auto_verified", "status unchanged")
    finally:
        _cleanup(db_path)


def test_upgrade_succeeds_when_higher():
    """upgrade_fact_status succeeds when new status outranks current."""
    fs, db_path = _make_fs()
    try:
        fs.upsert_candidate_fact("job-001", "Evans", 2025,
                                  "W-2.ein:111.wages", value_num=85000,
                                  status="extracted")

        result = fs.upgrade_fact_status("job-001", 2025, "W-2.ein:111.wages", "confirmed")
        check(result is True, "upgrade extracted→confirmed succeeds")

        fact = fs.get_fact("job-001", 2025, "W-2.ein:111.wages")
        check(fact["status"] == "confirmed", f"status is now confirmed (got {fact['status']})")
    finally:
        _cleanup(db_path)


def test_status_rank_hierarchy():
    """Verify the full status rank hierarchy: missing < extracted < needs_review < auto_verified < confirmed < corrected."""
    from fact_store import STATUS_RANK
    check(STATUS_RANK["missing"] < STATUS_RANK["extracted"], "missing < extracted")
    check(STATUS_RANK["extracted"] < STATUS_RANK["needs_review"], "extracted < needs_review")
    check(STATUS_RANK["needs_review"] < STATUS_RANK["auto_verified"], "needs_review < auto_verified")
    check(STATUS_RANK["auto_verified"] < STATUS_RANK["confirmed"], "auto_verified < confirmed")
    check(STATUS_RANK["confirmed"] < STATUS_RANK["corrected"], "confirmed < corrected")


# ═══════════════════════════════════════════════════════════════════════════════
# TEST D — REVIEW QUEUE & COUNT
# ═══════════════════════════════════════════════════════════════════════════════

def test_review_queue_returns_unreviewed():
    """get_review_queue returns only facts needing review."""
    fs, db_path = _make_fs()
    try:
        fs.upsert_candidate_fact("job-001", "Evans", 2025,
                                  "W-2.ein:111.wages", value_num=85000, status="extracted")
        fs.upsert_candidate_fact("job-001", "Evans", 2025,
                                  "W-2.ein:111.federal_wh", value_num=12750, status="auto_verified")
        fs.upsert_candidate_fact("job-001", "Evans", 2025,
                                  "1099-INT.ein:222.interest", value_num=1200, status="needs_review")

        queue = fs.get_review_queue("job-001")
        check(len(queue) == 2, f"review queue has 2 items (got {len(queue)})")

        statuses = {f["status"] for f in queue}
        check("auto_verified" not in statuses, "auto_verified not in review queue")
        check("extracted" in statuses, "extracted in review queue")
        check("needs_review" in statuses, "needs_review in review queue")
    finally:
        _cleanup(db_path)


def test_review_queue_excludes_locked():
    """Locked (corrected) facts do not appear in review queue."""
    fs, db_path = _make_fs()
    try:
        fs.upsert_candidate_fact("job-001", "Evans", 2025,
                                  "W-2.ein:111.wages", value_num=85000, status="extracted")
        fs.apply_correction("job-001", 2025, "W-2.ein:111.wages", value_num=86000)

        queue = fs.get_review_queue("job-001")
        check(len(queue) == 0, "corrected fact not in review queue")
    finally:
        _cleanup(db_path)


def test_count_facts():
    """count_facts returns correct counts by status."""
    fs, db_path = _make_fs()
    try:
        fs.upsert_candidate_fact("job-001", "Evans", 2025,
                                  "W-2.ein:111.wages", value_num=85000, status="extracted")
        fs.upsert_candidate_fact("job-001", "Evans", 2025,
                                  "W-2.ein:111.federal_wh", value_num=12750, status="auto_verified")
        fs.upsert_candidate_fact("job-001", "Evans", 2025,
                                  "1099-INT.ein:222.interest", value_num=1200, status="extracted")

        counts = fs.count_facts("job-001")
        check(counts.get("extracted", 0) == 2, f"2 extracted (got {counts.get('extracted', 0)})")
        check(counts.get("auto_verified", 0) == 1, f"1 auto_verified (got {counts.get('auto_verified', 0)})")
        check(sum(counts.values()) == 3, f"3 total (got {sum(counts.values())})")
    finally:
        _cleanup(db_path)


# ═══════════════════════════════════════════════════════════════════════════════
# TEST E — RUNTIME GUARDRAILS
# ═══════════════════════════════════════════════════════════════════════════════

def test_reject_pdf_path_in_value_text():
    """FactStore rejects PDF file paths as value_text."""
    fs, db_path = _make_fs()
    try:
        try:
            fs.upsert_candidate_fact("job-001", "Evans", 2025,
                                      "W-2.ein:111.wages",
                                      value_text="/path/to/document.pdf")
            check(False, "should reject PDF path")
        except ValueError as e:
            check("PDF" in str(e), "rejects PDF path with descriptive error")
    finally:
        _cleanup(db_path)


def test_reject_large_text_blob():
    """FactStore rejects large text blobs (>5000 chars)."""
    fs, db_path = _make_fs()
    try:
        try:
            fs.upsert_candidate_fact("job-001", "Evans", 2025,
                                      "W-2.ein:111.wages",
                                      value_text="x" * 5001)
            check(False, "should reject large text blob")
        except ValueError as e:
            check("5001" in str(e) or "large" in str(e).lower(),
                  "rejects large text with descriptive error")
    finally:
        _cleanup(db_path)


def test_reject_empty_job_id():
    """FactStore rejects empty job_id."""
    fs, db_path = _make_fs()
    try:
        try:
            fs.upsert_candidate_fact("", "Evans", 2025,
                                      "W-2.ein:111.wages", value_num=85000)
            check(False, "should reject empty job_id")
        except ValueError as e:
            check("required" in str(e).lower(), "rejects empty job_id")
    finally:
        _cleanup(db_path)


def test_reject_empty_fact_key():
    """FactStore rejects empty fact_key."""
    fs, db_path = _make_fs()
    try:
        try:
            fs.upsert_candidate_fact("job-001", "Evans", 2025,
                                      "", value_num=85000)
            check(False, "should reject empty fact_key")
        except ValueError as e:
            check("required" in str(e).lower(), "rejects empty fact_key")
    finally:
        _cleanup(db_path)


# ═══════════════════════════════════════════════════════════════════════════════
# TEST F — IMPORT GUARDRAILS
# ═══════════════════════════════════════════════════════════════════════════════

def test_import_guardrails():
    """fact_store.py has no forbidden imports."""
    from fact_store import _FORBIDDEN_MODULES
    check('extract' in _FORBIDDEN_MODULES, "fact_store forbids 'extract'")
    check('pytesseract' in _FORBIDDEN_MODULES, "fact_store forbids 'pytesseract'")
    check('anthropic' in _FORBIDDEN_MODULES, "fact_store forbids 'anthropic'")

    # Verify source code has no actual forbidden imports
    base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    filepath = os.path.join(base, 'fact_store.py')
    with open(filepath) as f:
        source = f.read()
    lines = source.split('\n')
    violations = []
    for i, line in enumerate(lines, 1):
        stripped = line.strip()
        if stripped.startswith('#') or '_FORBIDDEN_MODULES' in line:
            continue
        if stripped.startswith(('import ', 'from ')):
            for forbidden in ('extract', 'pytesseract', 'anthropic',
                               'pdf2image', 'PIL', 'Pillow', 'fitz'):
                if f'import {forbidden}' in stripped or f'from {forbidden}' in stripped:
                    violations.append(f"  {i}: {stripped}")
    check(len(violations) == 0,
          f"no forbidden imports in source" +
          (f" — found: {violations}" if violations else ""))


# ═══════════════════════════════════════════════════════════════════════════════
# TEST G — LEGACY TABLE SYNC
# ═══════════════════════════════════════════════════════════════════════════════

def test_sync_to_legacy():
    """sync_to_legacy copies facts to client_canonical_values."""
    fs, db_path = _make_fs()
    try:
        fs.upsert_candidate_fact("job-001", "Evans, Lisa", 2025,
                                  "W-2.ein:12-3456789.wages",
                                  value_num=85000, status="extracted",
                                  source_doc="evans-w2.pdf", source_page=1)
        fs.upsert_candidate_fact("job-001", "Evans, Lisa", 2025,
                                  "W-2.ein:12-3456789.federal_wh",
                                  value_num=12750, status="auto_verified",
                                  source_doc="evans-w2.pdf", source_page=1)

        fs.sync_to_legacy("job-001", "Evans, Lisa", "2025")

        legacy_facts = fs.get_legacy_facts("Evans, Lisa", "2025")
        check(len(legacy_facts) == 2,
              f"2 facts synced to legacy table (got {len(legacy_facts)})")

        # Check that values are correct
        wages_fact = next((f for f in legacy_facts if f["field_name"] == "wages"), None)
        check(wages_fact is not None, "wages fact found in legacy table")
        if wages_fact:
            check(wages_fact["canonical_value"] == 85000.0,
                  f"wages value correct (got {wages_fact['canonical_value']})")
    finally:
        _cleanup(db_path)


def test_sync_does_not_overwrite_confirmed_legacy():
    """sync_to_legacy does not overwrite confirmed/corrected legacy facts."""
    fs, db_path = _make_fs()
    try:
        # First, put a confirmed fact in legacy table
        fs.upsert_legacy_fact("Evans, Lisa", "2025", "W-2", "ein:12-3456789",
                               "wages", 86000, status="confirmed")

        # Then insert an extracted fact in unified table
        fs.upsert_candidate_fact("job-001", "Evans, Lisa", 2025,
                                  "W-2.ein:12-3456789.wages",
                                  value_num=85000, status="extracted")

        # Sync — should NOT overwrite the confirmed legacy value
        fs.sync_to_legacy("job-001", "Evans, Lisa", "2025")

        legacy_fact = fs.get_legacy_fact("Evans, Lisa", "2025", "W-2",
                                          "ein:12-3456789", "wages")
        check(legacy_fact is not None, "legacy fact still exists")
        check(legacy_fact["canonical_value"] == 86000,
              f"confirmed legacy value NOT overwritten (got {legacy_fact['canonical_value']})")
        check(legacy_fact["status"] == "confirmed",
              f"confirmed status preserved (got {legacy_fact['status']})")
    finally:
        _cleanup(db_path)


# ═══════════════════════════════════════════════════════════════════════════════
# TEST H — SCHEMA CORRECTNESS
# ═══════════════════════════════════════════════════════════════════════════════

def test_facts_table_exists():
    """The facts table exists with correct columns."""
    fs, db_path = _make_fs()
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.execute("PRAGMA table_info(facts)")
        columns = {row[1] for row in cursor.fetchall()}
        conn.close()

        expected = {"id", "job_id", "client_id", "tax_year", "fact_key",
                    "value_num", "value_text", "status", "confidence",
                    "source_method", "source_doc", "source_page",
                    "evidence_ref", "locked", "updated_at"}
        missing = expected - columns
        check(len(missing) == 0,
              f"facts table has all columns" +
              (f" — missing: {missing}" if missing else ""))
    finally:
        _cleanup(db_path)


def test_facts_table_uniqueness():
    """The facts table has UNIQUE(job_id, tax_year, fact_key)."""
    fs, db_path = _make_fs()
    try:
        conn = sqlite3.connect(db_path)
        # Insert first
        conn.execute(
            """INSERT INTO facts (job_id, client_id, tax_year, fact_key,
               value_num, status, locked, updated_at)
               VALUES ('j1', 'c1', 2025, 'W-2.ein:111.wages', 50000, 'extracted', 0, '2025-01-01')"""
        )
        conn.commit()
        # Insert duplicate — should fail
        try:
            conn.execute(
                """INSERT INTO facts (job_id, client_id, tax_year, fact_key,
                   value_num, status, locked, updated_at)
                   VALUES ('j1', 'c1', 2025, 'W-2.ein:111.wages', 60000, 'extracted', 0, '2025-01-02')"""
            )
            conn.commit()
            check(False, "should reject duplicate (job_id, tax_year, fact_key)")
        except sqlite3.IntegrityError:
            check(True, "UNIQUE constraint enforced on (job_id, tax_year, fact_key)")
        conn.close()
    finally:
        _cleanup(db_path)


def test_facts_indexes_exist():
    """Required indexes exist on the facts table."""
    fs, db_path = _make_fs()
    try:
        conn = sqlite3.connect(db_path)
        indexes = conn.execute("SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='facts'").fetchall()
        index_names = {row[0] for row in indexes}
        conn.close()

        check("idx_facts_job" in index_names, "idx_facts_job exists")
        check("idx_facts_client_year" in index_names, "idx_facts_client_year exists")
        check("idx_facts_fact_key" in index_names, "idx_facts_fact_key exists")
    finally:
        _cleanup(db_path)


# ═══════════════════════════════════════════════════════════════════════════════
# TEST I — EDGE CASES
# ═══════════════════════════════════════════════════════════════════════════════

def test_apply_correction_to_missing_row():
    """apply_correction returns False for nonexistent fact."""
    fs, db_path = _make_fs()
    try:
        result = fs.apply_correction("job-999", 2025, "nonexistent.key", value_num=100)
        check(result is False, "apply_correction returns False for missing fact")
    finally:
        _cleanup(db_path)


def test_upgrade_missing_row():
    """upgrade_fact_status returns False for nonexistent fact."""
    fs, db_path = _make_fs()
    try:
        result = fs.upgrade_fact_status("job-999", 2025, "nonexistent.key", "confirmed")
        check(result is False, "upgrade returns False for missing fact")
    finally:
        _cleanup(db_path)


def test_upgrade_invalid_status():
    """upgrade_fact_status raises ValueError for unknown status."""
    fs, db_path = _make_fs()
    try:
        fs.upsert_candidate_fact("job-001", "Evans", 2025,
                                  "W-2.ein:111.wages", value_num=85000)
        try:
            fs.upgrade_fact_status("job-001", 2025, "W-2.ein:111.wages", "bogus_status")
            check(False, "should raise ValueError for unknown status")
        except ValueError as e:
            check("Unknown status" in str(e), "raises ValueError for unknown status")
    finally:
        _cleanup(db_path)


def test_same_rank_candidate_update_allowed():
    """Re-extraction at same trust level updates value (e.g., re-run OCR)."""
    fs, db_path = _make_fs()
    try:
        fs.upsert_candidate_fact("job-001", "Evans", 2025,
                                  "W-2.ein:111.wages", value_num=85000,
                                  status="extracted")
        # Same status, new value (re-extraction found better number)
        fs.upsert_candidate_fact("job-001", "Evans", 2025,
                                  "W-2.ein:111.wages", value_num=85100,
                                  status="extracted")

        fact = fs.get_fact("job-001", 2025, "W-2.ein:111.wages")
        check(fact["value_num"] == 85100,
              f"same-rank update allowed (got {fact['value_num']})")
    finally:
        _cleanup(db_path)


def test_null_tax_year():
    """Facts with NULL tax_year are stored but must be retrieved via get_facts_for_job.

    SQLite NULL != NULL in WHERE clauses, so get_fact(job, None, key) won't match.
    Use get_facts_for_job() without tax_year filter instead.
    """
    fs, db_path = _make_fs()
    try:
        fs.upsert_candidate_fact("job-001", "Evans", None,
                                  "invoice.name:VENDOR.total", value_num=1500)
        # get_fact won't match NULL — this is expected SQLite behavior
        fact = fs.get_fact("job-001", None, "invoice.name:VENDOR.total")
        # NULL = NULL is false in SQL, so this returns None
        check(fact is None, "get_fact with None tax_year returns None (SQL NULL != NULL)")

        # But the fact is stored and can be retrieved via get_facts_for_job
        all_facts = fs.get_facts_for_job("job-001")
        check(len(all_facts) == 1, f"fact stored (got {len(all_facts)} via get_facts_for_job)")
        check(all_facts[0]["value_num"] == 1500, "value correct via get_facts_for_job")
    finally:
        _cleanup(db_path)


# ═══════════════════════════════════════════════════════════════════════════════
# RUN ALL TESTS
# ═══════════════════════════════════════════════════════════════════════════════

def run_tests():
    global PASS, FAIL

    print("\n═══ TEST A: CANDIDATES APPEAR IN DB IMMEDIATELY ═══")
    test_candidate_persisted_immediately()
    test_candidate_with_text_value()
    test_multiple_candidates_same_job()
    test_get_facts_for_job_with_year_filter()
    test_get_facts_for_client()

    print("\n═══ TEST B: LOCKED VALUES NEVER OVERWRITTEN ═══")
    test_corrected_fact_is_locked()
    test_locked_fact_not_overwritten_by_candidate()
    test_locked_fact_not_upgraded()

    print("\n═══ TEST C: NO DOWNGRADE (MONOTONIC TRUST) ═══")
    test_no_downgrade_auto_verified_to_extracted()
    test_no_downgrade_confirmed_to_auto_verified()
    test_no_downgrade_via_upgrade_method()
    test_upgrade_succeeds_when_higher()
    test_status_rank_hierarchy()

    print("\n═══ TEST D: REVIEW QUEUE & COUNT ═══")
    test_review_queue_returns_unreviewed()
    test_review_queue_excludes_locked()
    test_count_facts()

    print("\n═══ TEST E: RUNTIME GUARDRAILS ═══")
    test_reject_pdf_path_in_value_text()
    test_reject_large_text_blob()
    test_reject_empty_job_id()
    test_reject_empty_fact_key()

    print("\n═══ TEST F: IMPORT GUARDRAILS ═══")
    test_import_guardrails()

    print("\n═══ TEST G: LEGACY TABLE SYNC ═══")
    test_sync_to_legacy()
    test_sync_does_not_overwrite_confirmed_legacy()

    print("\n═══ TEST H: SCHEMA CORRECTNESS ═══")
    test_facts_table_exists()
    test_facts_table_uniqueness()
    test_facts_indexes_exist()

    print("\n═══ TEST I: EDGE CASES ═══")
    test_apply_correction_to_missing_row()
    test_upgrade_missing_row()
    test_upgrade_invalid_status()
    test_same_rank_candidate_update_allowed()
    test_null_tax_year()

    print(f"\n{'='*60}")
    print(f"  PASS: {PASS}  |  FAIL: {FAIL}  |  TOTAL: {PASS + FAIL}")
    print(f"{'='*60}")
    if FAIL > 0:
        sys.exit(1)


if __name__ == "__main__":
    run_tests()
