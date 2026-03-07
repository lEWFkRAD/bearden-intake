#!/usr/bin/env python3
"""Tests for B9-FEATURE — Review Time Tracking.

Covers: review_sessions table schema, field_duration_ms column,
        session start/end endpoints, timing in list_jobs output,
        per-field timing in guidedAction, timing display in UI.

Run:  python3 tests/test_review_time.py
All test execution is inside run_tests() behind __name__ guard.
"""

import sys, os, re

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
# TESTS
# ═══════════════════════════════════════════════════════════════════════════════

def run_tests():
    global PASS, FAIL

    # Read app.py source
    app_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "app.py")
    with open(app_path, "r", encoding="utf-8") as f:
        source = f.read()

    print("\n=== B9: Review Time Tracking ===\n")

    # ── Database Schema ──

    print("Database Schema:")
    check("CREATE TABLE IF NOT EXISTS review_sessions" in source,
          "review_sessions table created in _init_db")
    check("job_id TEXT NOT NULL" in source and "session_start TEXT NOT NULL" in source,
          "review_sessions has required columns (job_id, session_start)")
    check("duration_seconds INTEGER" in source,
          "review_sessions has duration_seconds column")
    check("fields_reviewed INTEGER" in source,
          "review_sessions has fields_reviewed column")
    check('"field_duration_ms"' in source and "ALTER TABLE verified_fields ADD COLUMN" in source,
          "verified_fields migrated with field_duration_ms column")

    # ── Backend Endpoints ──

    print("\nBackend Endpoints:")
    check('"/api/review-session/<job_id>/start"' in source,
          "POST /api/review-session/<job_id>/start endpoint exists")
    check('"/api/review-session/<job_id>/end"' in source,
          "POST /api/review-session/<job_id>/end endpoint exists")
    check('"/api/review-session/<job_id>/timing"' in source,
          "GET /api/review-session/<job_id>/timing endpoint exists")

    # ── Guided Review Action Accepts Timing ──

    print("\nPer-Field Timing:")
    check("field_duration_ms" in source and "payload.get(\"field_duration_ms\")" in source,
          "guided_review_action reads field_duration_ms from payload")
    check("field_duration_ms = excluded.field_duration_ms" in source,
          "field_duration_ms written to verified_fields via upsert")

    # ── list_jobs Returns Review Time ──

    print("\nAPI Output:")
    check("review_time_seconds" in source,
          "list_jobs returns review_time_seconds field")
    check("SUM(duration_seconds)" in source and "review_sessions" in source,
          "review_time_seconds computed from review_sessions aggregate")

    # ── Frontend: Session Tracking ──

    print("\nFrontend Session Tracking:")
    check("_reviewSessionId" in source and "_reviewSessionStart" in source,
          "Review session state variables declared")
    check("_fieldsReviewedCount" in source,
          "Fields reviewed counter declared")
    check("/api/review-session/" in source and "'/start'" in source,
          "Frontend calls session start endpoint")
    check("'/end'" in source and "session_id" in source and "fields_reviewed" in source,
          "Frontend calls session end with session_id and fields_reviewed")

    # ── Frontend: Per-Field Timing ──

    print("\nFrontend Per-Field Timing:")
    check("_fieldLoadTime" in source,
          "Field load timestamp variable declared")
    check("_fieldLoadTime = Date.now()" in source,
          "Field load time recorded when field rendered")
    check("field_duration_ms" in source and "Date.now() - _fieldLoadTime" in source,
          "Field duration computed and sent with action")

    # ── UI Display ──

    print("\nUI Display:")
    check("Review Time" in source,
          "History table header includes 'Review Time' column")
    check("review_time_seconds" in source and "formatDuration" in source,
          "Review time displayed using formatDuration in history table")
    check("Session Time" in source,
          "Session time shown on review complete screen")
    check("avg per field" in source,
          "Average per-field time shown on completion screen")

    # ── Cleanup on Delete ──

    print("\nCleanup:")
    check("DELETE FROM review_sessions WHERE job_id" in source,
          "Review sessions cleaned up on job delete")

    print(f"\n{'=' * 50}")
    print(f"  {PASS} passed, {FAIL} failed")
    if FAIL:
        print(f"  *** {FAIL} FAILURES ***")
    print()
    return FAIL


if __name__ == "__main__":
    failures = run_tests()
    sys.exit(1 if failures else 0)
