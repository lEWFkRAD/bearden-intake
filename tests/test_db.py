"""
Tests for db.py — SQLite database layer.
"""

import sys
import os
import json
import tempfile
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

import db

PASS = 0
FAIL = 0


def check(label, condition):
    global PASS, FAIL
    if condition:
        PASS += 1
        print(f"  PASS  {label}")
    else:
        FAIL += 1
        print(f"  FAIL  {label}")


def setup_test_db():
    """Point db module at a temporary database file."""
    tmp = tempfile.mktemp(suffix=".db")
    db.DB_PATH = Path(tmp)
    db.init_db()
    return tmp


def cleanup_test_db(tmp_path):
    """Remove temporary database."""
    try:
        os.unlink(tmp_path)
    except OSError:
        pass


# ─── Test 1: Schema Initialization ──────────────────────────────────────────

def test_init_db():
    print("\n-- Test 1: Schema Initialization --")
    tmp = setup_test_db()

    conn = db.get_connection()
    tables = [r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    ).fetchall()]
    conn.close()

    check("jobs table exists", "jobs" in tables)
    check("vendor_categories table exists", "vendor_categories" in tables)
    check("client_context_docs table exists", "client_context_docs" in tables)
    check("client_instructions table exists", "client_instructions" in tables)
    check("users table exists", "users" in tables)
    check("facts table exists", "facts" in tables)
    check("fact_events table exists", "fact_events" in tables)
    check("review_state table exists", "review_state" in tables)
    check("audit_events table exists", "audit_events" in tables)
    check("config table exists", "config" in tables)

    # Check seed data
    users = db.list_users()
    check("3 users seeded", len(users) == 3)
    check("jeff exists", any(u["user_id"] == "jeff" for u in users))
    check("susan exists", any(u["user_id"] == "susan" for u in users))
    check("charles exists", any(u["user_id"] == "charles" for u in users))

    # Check config seed
    check("EXPORT_REQUIRES_PARTNER_REVIEW seeded", db.get_config("EXPORT_REQUIRES_PARTNER_REVIEW") == "false")
    check("LOCK_TIMEOUT_MINUTES seeded", db.get_config("LOCK_TIMEOUT_MINUTES") == "30")

    cleanup_test_db(tmp)


# ─── Test 2: Config CRUD ────────────────────────────────────────────────────

def test_config():
    print("\n-- Test 2: Config CRUD --")
    tmp = setup_test_db()

    check("get nonexistent returns None", db.get_config("NOPE") is None)

    db.set_config("MY_KEY", "my_value")
    check("set + get works", db.get_config("MY_KEY") == "my_value")

    db.set_config("MY_KEY", "updated")
    check("update works", db.get_config("MY_KEY") == "updated")

    cleanup_test_db(tmp)


# ─── Test 3: Jobs CRUD ──────────────────────────────────────────────────────

def test_jobs():
    print("\n-- Test 3: Jobs CRUD --")
    tmp = setup_test_db()

    job = {
        "id": "test-001",
        "filename": "test.pdf",
        "client_name": "Acme Corp",
        "doc_type": "tax_returns",
        "output_format": "tax_review",
        "year": "2024",
        "status": "complete",
        "stage": "done",
        "progress": 100,
        "created": "2024-01-15T10:00:00",
        "cost_usd": 0.25,
        "stats": {"documents": 5, "methods": {"vision": 3, "ocr_text": 2}},
        "verification": {"reviewed": 10, "confirmed": 8, "corrected": 2},
    }
    db.save_job(job)

    loaded = db.get_job("test-001")
    check("job saved and loaded", loaded is not None)
    check("filename preserved", loaded["filename"] == "test.pdf")
    check("client_name preserved", loaded["client_name"] == "Acme Corp")
    check("status preserved", loaded["status"] == "complete")
    check("cost preserved", loaded["cost_usd"] == 0.25)
    check("stats is dict", isinstance(loaded["stats"], dict))
    check("stats.documents preserved", loaded["stats"].get("documents") == 5)
    check("verification is dict", isinstance(loaded["verification"], dict))
    check("verification.reviewed preserved", loaded["verification"].get("reviewed") == 10)

    # Update
    db.update_job("test-001", status="error", error="Something failed")
    updated = db.get_job("test-001")
    check("update_job works", updated["status"] == "error")
    check("update preserves other fields", updated["filename"] == "test.pdf")

    # List
    db.save_job({"id": "test-002", "filename": "other.pdf", "client_name": "Beta LLC",
                 "year": "2024", "created": "2024-01-16T10:00:00", "status": "complete"})
    all_jobs = db.list_jobs()
    check("list_jobs returns 2", len(all_jobs) == 2)

    filtered = db.list_jobs(q="Acme")
    check("list_jobs filter by name", len(filtered) == 1)
    check("filter returns correct job", filtered[0]["id"] == "test-001")

    # Delete
    db.delete_job("test-001")
    check("delete_job removes job", db.get_job("test-001") is None)
    check("other job still exists", db.get_job("test-002") is not None)

    # Clear stale
    db.save_job({"id": "test-003", "filename": "stale.pdf", "client_name": "X",
                 "year": "2024", "created": "2024-01-17T10:00:00", "status": "running"})
    db.clear_stale_jobs()
    stale = db.get_job("test-003")
    check("clear_stale_jobs marks interrupted", stale["status"] == "interrupted")

    cleanup_test_db(tmp)


# ─── Test 4: Vendor Categories ──────────────────────────────────────────────

def test_vendor_categories():
    print("\n-- Test 4: Vendor Categories --")
    tmp = setup_test_db()

    db.set_vendor_category("GEORGIA POWER", "Utilities", original="Georgia Power #1234")
    cats = db.get_vendor_categories()
    check("vendor saved", "GEORGIA POWER" in cats)
    check("category correct", cats["GEORGIA POWER"]["category"] == "Utilities")

    # Suggest exact
    check("suggest exact match", db.suggest_category("GEORGIA POWER") == "Utilities")
    # Suggest prefix
    check("suggest prefix match", db.suggest_category("GEORGIA POWER #5678") == "Utilities")
    # No match
    check("suggest no match", db.suggest_category("UNKNOWN VENDOR") == "")

    # Count increments
    db.set_vendor_category("GEORGIA POWER", "Utilities", original="Georgia Power #5678")
    cats2 = db.get_vendor_categories()
    check("count incremented", cats2["GEORGIA POWER"]["count"] == 2)

    cleanup_test_db(tmp)


# ─── Test 5: Client Context ─────────────────────────────────────────────────

def test_client_context():
    print("\n-- Test 5: Client Context --")
    tmp = setup_test_db()

    db.add_context_doc("Smith", "doc-1", label="2023 W-2", filename="w2.pdf",
                       file_path="/clients/Smith/context/w2.pdf", year="2023",
                       payers=[{"name": "Acme", "ein": "12-345"}])
    db.add_context_doc("Smith", "doc-2", label="2023 1099", filename="1099.pdf",
                       file_path="/clients/Smith/context/1099.pdf", year="2023")

    docs = db.list_context_docs("Smith")
    check("2 docs listed", len(docs) == 2)
    check("payers parsed", isinstance(docs[0]["payers"], list))

    doc = db.get_context_doc("doc-1")
    check("get_context_doc works", doc is not None)
    check("payers has ein", doc["payers"][0]["ein"] == "12-345")

    db.delete_context_doc("doc-1")
    check("delete works", db.get_context_doc("doc-1") is None)
    check("other doc still exists", len(db.list_context_docs("Smith")) == 1)

    # Prior-year data
    db.set_prior_year_data("Smith", {"wages": 50000, "interest": 100})
    data = db.get_prior_year_data("Smith")
    check("prior_year_data saved", data.get("wages") == 50000)

    check("nonexistent prior_year_data empty", db.get_prior_year_data("Nobody") == {})

    cleanup_test_db(tmp)


# ─── Test 6: Client Instructions ─────────────────────────────────────────────

def test_instructions():
    print("\n-- Test 6: Client Instructions --")
    tmp = setup_test_db()

    db.add_instruction("Jones", "rule-1", "Always report Schedule E income")
    db.add_instruction("Jones", "rule-2", "Use 4562 for all depreciation")

    instructions = db.list_instructions("Jones")
    check("2 instructions listed", len(instructions) == 2)

    text = db.get_instructions_text("Jones")
    check("instructions text contains both", "Schedule E" in text and "4562" in text)

    db.delete_instruction("rule-1")
    check("delete works", len(db.list_instructions("Jones")) == 1)

    check("empty instructions for unknown client", db.get_instructions_text("Nobody") == "")

    cleanup_test_db(tmp)


# ─── Test 7: Facts CRUD ─────────────────────────────────────────────────────

def test_facts():
    print("\n-- Test 7: Facts CRUD --")
    tmp = setup_test_db()

    db.set_fact("client1", "2024", "job1:1:0:wages", "50000", value_type="number",
                set_by="jeff", evidence_id="job1", status="extracted")

    fact = db.get_fact("client1", "2024", "job1:1:0:wages")
    check("fact saved", fact is not None)
    check("value correct", fact["value"] == "50000")
    check("status correct", fact["status"] == "extracted")

    # Update via set_fact
    db.set_fact("client1", "2024", "job1:1:0:wages", "55000", set_by="susan", status="verified")
    updated = db.get_fact("client1", "2024", "job1:1:0:wages")
    check("fact updated", updated["value"] == "55000")
    check("set_by updated", updated["last_set_by"] == "susan")

    # Bulk set
    db.bulk_set_facts([
        {"client_id": "client1", "tax_year": "2024", "field_id": "job1:1:0:federal_wh", "value": "5000"},
        {"client_id": "client1", "tax_year": "2024", "field_id": "job1:2:0:interest", "value": "100"},
    ])
    job_facts = db.get_facts_for_job("job1")
    check("bulk set + get_facts_for_job", len(job_facts) == 3)

    cleanup_test_db(tmp)


# ─── Test 8: Fact Events ────────────────────────────────────────────────────

def test_fact_events():
    print("\n-- Test 8: Fact Events --")
    tmp = setup_test_db()

    db.record_fact_event("c1", "2024", "j1:1:0:wages", None, "50000",
                         "jeff", "preparer", "verify", evidence_id="j1")
    db.record_fact_event("c1", "2024", "j1:1:0:wages", "50000", "55000",
                         "susan", "reviewer", "override", reason="Corrected from W-2")

    history = db.get_fact_history("c1", "2024", "j1:1:0:wages")
    check("2 events recorded", len(history) == 2)
    check("first event is verify", history[0]["action"] == "verify")
    check("second event is override", history[1]["action"] == "override")
    check("second has reason", history[1]["reason"] == "Corrected from W-2")

    job_events = db.get_fact_events_for_job("j1")
    check("get_fact_events_for_job works", len(job_events) == 2)

    cleanup_test_db(tmp)


# ─── Test 9: Review State ───────────────────────────────────────────────────

def test_review_state():
    print("\n-- Test 9: Review State --")
    tmp = setup_test_db()

    # Need a job for inbox query
    db.save_job({"id": "j1", "filename": "test.pdf", "client_name": "Smith",
                 "year": "2024", "created": "2024-01-15T10:00:00", "status": "complete"})

    db.bulk_init_review_states([
        {"client_id": "Smith", "tax_year": "2024", "field_id": "j1:1:0:wages", "stage": "extracted", "assigned_to": "jeff"},
        {"client_id": "Smith", "tax_year": "2024", "field_id": "j1:1:0:federal_wh", "stage": "extracted", "assigned_to": "jeff"},
        {"client_id": "Smith", "tax_year": "2024", "field_id": "j1:2:0:interest", "stage": "prepared", "assigned_to": "susan"},
    ])

    states = db.get_review_states_for_job("j1")
    check("3 review states created", len(states) == 3)

    rs = db.get_review_state("Smith", "2024", "j1:1:0:wages")
    check("review state loaded", rs is not None)
    check("stage is extracted", rs["stage"] == "extracted")
    check("assigned to jeff", rs["assigned_to"] == "jeff")

    # Advance stage
    db.set_review_stage("Smith", "2024", "j1:1:0:wages", "prepared", assigned_to="susan")
    rs2 = db.get_review_state("Smith", "2024", "j1:1:0:wages")
    check("stage advanced to prepared", rs2["stage"] == "prepared")
    check("assigned to susan", rs2["assigned_to"] == "susan")

    # Summary
    summary = db.get_review_summary_for_job("j1")
    check("summary has stages", "prepared" in summary)
    check("summary counts correct", summary.get("prepared", 0) == 2)

    # Inbox
    inbox = db.get_inbox("jeff")
    check("jeff inbox has 1 item", len(inbox) == 1)
    check("jeff has 1 field", inbox[0]["field_count"] == 1)

    susan_inbox = db.get_inbox("susan")
    check("susan inbox has 1 item", len(susan_inbox) == 1)
    check("susan has 2 fields", susan_inbox[0]["field_count"] == 2)

    cleanup_test_db(tmp)


# ─── Test 10: Locking ───────────────────────────────────────────────────────

def test_locking():
    print("\n-- Test 10: Locking --")
    tmp = setup_test_db()

    # Set up review state
    db.set_review_stage("c1", "2024", "j1:1:0:wages", "prepared", "susan")

    # Jeff tries to lock — should succeed
    check("jeff acquires lock", db.acquire_lock("c1", "2024", "j1:1:0:wages", "jeff"))

    # Susan tries to lock same field — should fail
    check("susan blocked by jeff's lock", not db.acquire_lock("c1", "2024", "j1:1:0:wages", "susan"))

    # Jeff can re-lock (extend)
    check("jeff can re-lock own", db.acquire_lock("c1", "2024", "j1:1:0:wages", "jeff"))

    # Release
    db.release_lock("c1", "2024", "j1:1:0:wages", "jeff")
    check("susan acquires after release", db.acquire_lock("c1", "2024", "j1:1:0:wages", "susan"))

    # Bulk lock
    db.release_lock("c1", "2024", "j1:1:0:wages", "susan")
    db.set_review_stage("c1", "2024", "j1:1:0:federal_wh", "prepared", "susan")

    acquired, blocked = db.bulk_acquire_lock("c1", "2024",
        ["j1:1:0:wages", "j1:1:0:federal_wh"], "jeff")
    check("bulk lock acquired 2", len(acquired) == 2)
    check("bulk lock none blocked", len(blocked) == 0)

    # Someone else tries
    _, blocked2 = db.bulk_acquire_lock("c1", "2024",
        ["j1:1:0:wages", "j1:1:0:federal_wh"], "susan")
    check("susan blocked on bulk", len(blocked2) == 2)

    cleanup_test_db(tmp)


# ─── Test 11: Audit Events ──────────────────────────────────────────────────

def test_audit():
    print("\n-- Test 11: Audit Events --")
    tmp = setup_test_db()

    db.log_audit("FACT_VERIFIED", "c1", "2024", "j1:1:0:wages", "jeff",
                 {"old_value": None, "new_value": "50000"})
    db.log_audit("STAGE_ADVANCED", "c1", "2024", "j1:1:0:wages", "jeff",
                 {"new_stage": "prepared"})
    db.log_audit("REVIEW_APPROVED", "c1", "2024", "j1:1:0:wages", "susan", {})

    trail = db.get_audit_trail(client_id="c1")
    check("3 audit events", len(trail) == 3)
    check("most recent first", trail[0]["event_type"] == "REVIEW_APPROVED")
    check("details parsed", isinstance(trail[2]["details"], dict))

    job_trail = db.get_audit_trail(job_id="j1")
    check("job filter works", len(job_trail) == 3)

    cleanup_test_db(tmp)


# ─── Test 12: Users ─────────────────────────────────────────────────────────

def test_users():
    print("\n-- Test 12: Users --")
    tmp = setup_test_db()

    users = db.list_users()
    check("3 seeded users", len(users) == 3)

    jeff = db.get_user("jeff")
    check("jeff found", jeff is not None)
    check("jeff is preparer", jeff["role"] == "preparer")
    check("jeff display name", jeff["display_name"] == "Jeffrey Watts")

    check("unknown user is None", db.get_user("nobody") is None)

    cleanup_test_db(tmp)


# ─── Test 13: populate_facts_from_extraction ─────────────────────────────────

def test_populate_facts():
    print("\n-- Test 13: Populate Facts from Extraction --")
    tmp = setup_test_db()

    extractions = [
        {
            "_page": 1,
            "_ext_idx": 0,
            "document_type": "W-2",
            "fields": {
                "wages": {"value": 50000, "confidence": "high"},
                "federal_wh": {"value": 5000, "confidence": "high"},
            }
        },
        {
            "_page": 2,
            "_ext_idx": 0,
            "document_type": "1099-INT",
            "fields": {
                "interest_income": {"value": 150.50, "confidence": "medium"},
            }
        }
    ]

    count = db.populate_facts_from_extraction("job-abc", "Smith", "2024", extractions)
    check("3 facts created", count == 3)

    facts = db.get_facts_for_job("job-abc")
    check("3 facts retrieved", len(facts) == 3)

    states = db.get_review_states_for_job("job-abc")
    check("3 review states created", len(states) == 3)
    check("all assigned to jeff", all(s["assigned_to"] == "jeff" for s in states))
    check("all stage extracted", all(s["stage"] == "extracted" for s in states))

    cleanup_test_db(tmp)


# ─── Test 14: Review Chain Workflow ──────────────────────────────────────────

def test_workflow():
    print("\n-- Test 14: Review Chain Workflow --")
    tmp = setup_test_db()

    # Setup: create a fact and review state
    client_id, year, field_id = "Smith", "2024", "j1:1:0:wages"
    db.set_fact(client_id, year, field_id, "50000", set_by="system", status="extracted")
    db.set_review_stage(client_id, year, field_id, "extracted", assigned_to="jeff")

    # Step 1: Jeff verifies
    result = db.process_verify(client_id, year, field_id, "50000", "jeff", "preparer")
    check("verify returns ok", result.get("ok"))
    rs = db.get_review_state(client_id, year, field_id)
    check("stage is prepared after verify", rs["stage"] == "prepared")
    check("assigned to susan after verify", rs["assigned_to"] == "susan")
    fact = db.get_fact(client_id, year, field_id)
    check("fact value set", fact["value"] == "50000")
    check("fact status verified", fact["status"] == "verified")

    # Step 2: Susan approves
    result = db.process_approve(client_id, year, field_id, "susan", "reviewer")
    check("approve returns ok", result.get("ok"))
    rs = db.get_review_state(client_id, year, field_id)
    check("stage is reviewed after approve", rs["stage"] == "reviewed")
    check("assigned to charles after approve", rs["assigned_to"] == "charles")

    # Step 3: Charles approves
    result = db.process_approve(client_id, year, field_id, "charles", "partner")
    check("partner approve returns ok", result.get("ok"))
    rs = db.get_review_state(client_id, year, field_id)
    check("stage is partner_reviewed", rs["stage"] == "partner_reviewed")
    check("assigned to nobody", rs["assigned_to"] == "")

    # Verify event history
    history = db.get_fact_history(client_id, year, field_id)
    check("3 events in history", len(history) == 3)
    check("events: verify, approve, approve",
          [h["action"] for h in history] == ["verify", "approve", "approve"])

    cleanup_test_db(tmp)


# ─── Test 15: Override Flow ─────────────────────────────────────────────────

def test_override():
    print("\n-- Test 15: Override Flow --")
    tmp = setup_test_db()

    client_id, year, field_id = "Jones", "2024", "j2:1:0:interest"
    db.set_fact(client_id, year, field_id, "100", set_by="system", status="extracted")
    db.set_review_stage(client_id, year, field_id, "extracted", assigned_to="jeff")

    # Jeff verifies
    db.process_verify(client_id, year, field_id, "100", "jeff", "preparer")

    # Susan overrides
    result = db.process_override(client_id, year, field_id, "150", "susan", "reviewer",
                                  reason="Checked original 1099, actual amount is $150")
    check("override returns ok", result.get("ok"))
    fact = db.get_fact(client_id, year, field_id)
    check("fact value overridden to 150", fact["value"] == "150")
    rs = db.get_review_state(client_id, year, field_id)
    check("stage is reviewed after override", rs["stage"] == "reviewed")

    # Check event recorded the old/new values
    history = db.get_fact_history(client_id, year, field_id)
    override_event = [h for h in history if h["action"] == "override"][0]
    check("override event has old value", override_event["old_value"] == "100")
    check("override event has new value", override_event["new_value"] == "150")
    check("override has reason", "actual amount" in override_event["reason"])

    cleanup_test_db(tmp)


# ─── Test 16: Send Back Flow ────────────────────────────────────────────────

def test_send_back():
    print("\n-- Test 16: Send Back Flow --")
    tmp = setup_test_db()

    client_id, year, field_id = "Brown", "2024", "j3:1:0:wages"
    db.set_fact(client_id, year, field_id, "40000", set_by="system", status="extracted")
    db.set_review_stage(client_id, year, field_id, "extracted", assigned_to="jeff")

    # Jeff verifies → Susan's turn
    db.process_verify(client_id, year, field_id, "40000", "jeff", "preparer")
    # Susan sends back
    result = db.process_send_back(client_id, year, field_id, "susan", "reviewer",
                                   reason="Amount looks wrong, please re-check page 1")
    check("send_back returns ok", result.get("ok"))
    rs = db.get_review_state(client_id, year, field_id)
    check("stage rolled back to extracted", rs["stage"] == "extracted")
    check("assigned back to jeff", rs["assigned_to"] == "jeff")

    # Jeff re-verifies → Susan approves → Charles sends back
    db.process_verify(client_id, year, field_id, "42000", "jeff", "preparer")
    db.process_approve(client_id, year, field_id, "susan", "reviewer")
    result = db.process_send_back(client_id, year, field_id, "charles", "partner",
                                   reason="Need Susan to double-check this")
    check("partner send_back ok", result.get("ok"))
    rs = db.get_review_state(client_id, year, field_id)
    check("partner sends back to prepared", rs["stage"] == "prepared")
    check("assigned to susan", rs["assigned_to"] == "susan")

    # Partner can also send back to jeff
    db.set_review_stage(client_id, year, field_id, "reviewed", assigned_to="charles")
    result = db.process_send_back(client_id, year, field_id, "charles", "partner",
                                   reason="Start over", send_to="jeff")
    check("partner send_back to jeff ok", result.get("ok"))
    rs = db.get_review_state(client_id, year, field_id)
    check("sent to jeff stage extracted", rs["stage"] == "extracted")
    check("assigned to jeff", rs["assigned_to"] == "jeff")

    cleanup_test_db(tmp)


# ─── Test 17: Stage Guards ──────────────────────────────────────────────────

def test_stage_guards():
    print("\n-- Test 17: Stage Guards --")
    tmp = setup_test_db()

    client_id, year, field_id = "Test", "2024", "j4:1:0:wages"
    db.set_fact(client_id, year, field_id, "30000", set_by="system", status="extracted")
    db.set_review_stage(client_id, year, field_id, "extracted", assigned_to="jeff")

    # Susan can't approve an 'extracted' field (not yet prepared)
    result = db.process_approve(client_id, year, field_id, "susan", "reviewer")
    check("reviewer blocked on extracted stage", "error" in result)

    # Charles can't approve an 'extracted' field
    result = db.process_approve(client_id, year, field_id, "charles", "partner")
    check("partner blocked on extracted stage", "error" in result)

    # Non-preparer can't verify
    result = db.process_verify(client_id, year, field_id, "30000", "susan", "reviewer")
    check("reviewer can't verify", "error" in result)

    cleanup_test_db(tmp)


# ─── Test 18: Review Queue ──────────────────────────────────────────────────

def test_review_queue():
    print("\n-- Test 18: Review Queue --")
    tmp = setup_test_db()

    client_id = "TestClient"
    year = "2025"
    job_id = "job_queue_1"

    # Create facts and review states across 3 pages
    fields = [
        (f"{job_id}:1:0:wages", "50000", "extracted", "jeff"),
        (f"{job_id}:1:0:federal_wh", "8000", "extracted", "jeff"),
        (f"{job_id}:2:0:dividends", "3000", "extracted", "jeff"),
        (f"{job_id}:2:0:interest", "500", "prepared", "susan"),
        (f"{job_id}:3:0:capital_gains", "1200", "reviewed", "charles"),
    ]

    conn = db.get_connection()
    try:
        for fid, val, stage, assigned in fields:
            conn.execute(
                "INSERT OR REPLACE INTO facts (client_id, tax_year, field_id, value, value_type, last_set_by, status) VALUES (?,?,?,?,?,?,?)",
                (client_id, year, fid, val, "number", "system", "extracted")
            )
            conn.execute(
                "INSERT OR REPLACE INTO review_state (client_id, tax_year, field_id, stage, assigned_to) VALUES (?,?,?,?,?)",
                (client_id, year, fid, stage, assigned)
            )
        conn.commit()
    finally:
        conn.close()

    # Preparer queue: should see 3 extracted fields assigned to jeff
    queue = db.get_review_queue(job_id, "jeff", "preparer")
    check("preparer sees 3 extracted fields", len(queue) == 3)
    check("preparer queue sorted by page", queue[0]["page"] == 1 and queue[1]["page"] == 1 and queue[2]["page"] == 2)
    check("preparer queue has correct values", queue[0]["value"] in ("50000", "8000"))

    # Reviewer queue: should see 1 prepared field assigned to susan
    queue = db.get_review_queue(job_id, "susan", "reviewer")
    check("reviewer sees 1 prepared field", len(queue) == 1)
    check("reviewer field is interest", queue[0]["field_name"] == "interest")

    # Partner queue: should see 1 reviewed field assigned to charles
    queue = db.get_review_queue(job_id, "charles", "partner")
    check("partner sees 1 reviewed field", len(queue) == 1)
    check("partner field is capital_gains", queue[0]["field_name"] == "capital_gains")

    # Wrong user: jeff asking for reviewer queue (no prepared assigned to jeff)
    queue = db.get_review_queue(job_id, "jeff", "reviewer")
    check("wrong role/user returns empty", len(queue) == 0)

    # Non-existent job
    queue = db.get_review_queue("nonexistent", "jeff", "preparer")
    check("nonexistent job returns empty", len(queue) == 0)

    cleanup_test_db(tmp)


# ─── Test 19: Text BBox Matching ──────────────────────────────────────────────

def test_find_text_bbox():
    print("\n-- Test 19: Text BBox Matching --")

    # Import the function from app.py
    sys.path.insert(0, str(Path(__file__).parent.parent))
    from app import _find_text_bbox

    words = [
        {"text": "Box", "left": 100, "top": 200, "width": 30, "height": 12, "conf": 95},
        {"text": "1a", "left": 135, "top": 200, "width": 20, "height": 12, "conf": 95},
        {"text": "Ordinary", "left": 160, "top": 200, "width": 60, "height": 12, "conf": 92},
        {"text": "Dividends", "left": 225, "top": 200, "width": 70, "height": 12, "conf": 90},
        {"text": "$4,231.50", "left": 400, "top": 200, "width": 80, "height": 14, "conf": 88},
        {"text": "50,000", "left": 400, "top": 300, "width": 65, "height": 14, "conf": 93},
        {"text": "1,234.56", "left": 400, "top": 350, "width": 70, "height": 14, "conf": 91},
    ]

    # Numeric match
    result = _find_text_bbox(words, "50000")
    check("numeric match 50000 finds 50,000", result is not None and result[0] == 400 and result[1] == 300)

    # Dollar amount match
    result = _find_text_bbox(words, "$4,231.50")
    check("dollar amount match", result is not None and result[0] == 400 and result[1] == 200)

    # Numeric match with different formatting
    result = _find_text_bbox(words, "1234.56")
    check("numeric match 1234.56 finds 1,234.56", result is not None and result[0] == 400 and result[1] == 350)

    # Exact word match
    result = _find_text_bbox(words, "Ordinary")
    check("exact word match", result is not None and result[0] == 160)

    # Multi-word match
    result = _find_text_bbox(words, "Ordinary Dividends")
    check("multi-word match", result is not None and result[0] == 160)

    # No match
    result = _find_text_bbox(words, "99999")
    check("no match returns None", result is None)

    # Empty search
    result = _find_text_bbox(words, "")
    check("empty search returns None", result is None)

    # Empty words list
    result = _find_text_bbox([], "50000")
    check("empty words returns None", result is None)


def test_undo():
    print("\n-- Test 20: Undo Review Actions --")

    db.init_db()
    client, year = "undo_client", "2025"

    # Set up a fact and review state
    fid = "undo_job:1:0:wages"
    db.set_fact(client, year, fid, "50000", set_by="system", status="extracted")
    db.set_review_stage(client, year, fid, "extracted", assigned_to="jeff")

    # Jeff verifies the field (preparer -> prepared)
    result = db.process_verify(client, year, fid, "50000", "jeff", "preparer")
    check("verify succeeds", result.get("ok") is True)

    # Confirm it's now at prepared stage
    rs = db.get_review_state(client, year, fid)
    check("stage is prepared after verify", rs["stage"] == "prepared")

    # Jeff undoes the verify
    result = db.process_undo(client, year, fid, "jeff", "preparer")
    check("undo succeeds", result.get("ok") is True)
    check("undo reports undone action", result.get("undone_action") == "verify")

    # Stage should be back to extracted
    rs = db.get_review_state(client, year, fid)
    check("stage reverted to extracted", rs["stage"] == "extracted")

    # Value should be restored
    fact = db.get_fact(client, year, fid)
    check("value preserved after undo", fact["value"] == "50000")

    # Undo event should be in history
    history = db.get_fact_history(client, year, fid)
    check("undo event recorded in ledger", history[-1]["action"] == "undo")

    # Try to undo again -- should fail (last action is undo)
    result = db.process_undo(client, year, fid, "jeff", "preparer")
    check("cannot undo an undo", result.get("error") is not None)

    # Test undo with value change (edit)
    db.set_review_stage(client, year, fid, "extracted", assigned_to="jeff")
    db.process_verify(client, year, fid, "55000", "jeff", "preparer")

    fact = db.get_fact(client, year, fid)
    check("value changed to 55000", fact["value"] == "55000")

    result = db.process_undo(client, year, fid, "jeff", "preparer")
    check("undo edit succeeds", result.get("ok") is True)

    fact = db.get_fact(client, year, fid)
    check("value restored to 50000 after undo", fact["value"] == "50000")

    # Test undo by different user fails (non-partner)
    db.set_review_stage(client, year, fid, "extracted", assigned_to="jeff")
    db.process_verify(client, year, fid, "50000", "jeff", "preparer")

    result = db.process_undo(client, year, fid, "susan", "reviewer")
    check("reviewer cannot undo preparer action", result.get("error") is not None)

    # But partner can undo anyone's action
    result = db.process_undo(client, year, fid, "charles", "partner")
    check("partner can undo any action", result.get("ok") is True)


# ─── Run All ─────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    test_init_db()
    test_config()
    test_jobs()
    test_vendor_categories()
    test_client_context()
    test_instructions()
    test_facts()
    test_fact_events()
    test_review_state()
    test_locking()
    test_audit()
    test_users()
    test_populate_facts()
    test_workflow()
    test_override()
    test_send_back()
    test_stage_guards()
    test_review_queue()
    test_find_text_bbox()
    test_undo()

    print(f"\n{'='*60}")
    print(f"  Results: {PASS} passed, {FAIL} failed out of {PASS+FAIL}")
    print(f"{'='*60}")

    sys.exit(1 if FAIL > 0 else 0)
