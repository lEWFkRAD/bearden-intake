"""
Bearden Document Intake — Database Layer
=========================================
SQLite persistence for jobs, verifications, vendor categories, client data,
review chain (facts, events, review state), and audit trail.

Database file: data/bearden.db
"""

import sqlite3
import json
import re
from pathlib import Path
from datetime import datetime, timedelta
from contextlib import contextmanager

BASE_DIR = Path(__file__).parent
DB_PATH = BASE_DIR / "data" / "bearden.db"

# Ensure data directory exists
DB_PATH.parent.mkdir(exist_ok=True)

# ─── Connection Management ───────────────────────────────────────────────────

def get_connection():
    """Get a SQLite connection with WAL mode and Row factory."""
    conn = sqlite3.connect(str(DB_PATH), check_same_thread=False, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


@contextmanager
def transaction():
    """Context manager for a database transaction."""
    conn = get_connection()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _row_to_dict(row):
    """Convert a sqlite3.Row to a plain dict."""
    if row is None:
        return None
    return dict(row)


def _rows_to_dicts(rows):
    """Convert a list of sqlite3.Row to a list of dicts."""
    return [dict(r) for r in rows]


# ─── Schema ──────────────────────────────────────────────────────────────────

_SCHEMA_SQL = """
-- ═══ MIGRATED FROM EXISTING JSON FILES ═══

CREATE TABLE IF NOT EXISTS jobs (
    job_id          TEXT PRIMARY KEY,
    filename        TEXT NOT NULL DEFAULT '',
    client_name     TEXT NOT NULL DEFAULT '',
    doc_type        TEXT NOT NULL DEFAULT 'tax_returns',
    output_format   TEXT NOT NULL DEFAULT 'tax_review',
    user_notes      TEXT DEFAULT '',
    ai_instructions TEXT DEFAULT '',
    year            TEXT NOT NULL DEFAULT '',
    status          TEXT NOT NULL DEFAULT 'queued',
    stage           TEXT DEFAULT 'queued',
    progress        INTEGER DEFAULT 0,
    created         TEXT NOT NULL DEFAULT '',
    start_time      TEXT DEFAULT '',
    end_time        TEXT DEFAULT '',
    pdf_path        TEXT DEFAULT '',
    client_folder   TEXT DEFAULT '',
    output_xlsx     TEXT DEFAULT '',
    output_log      TEXT DEFAULT '',
    client_xlsx     TEXT DEFAULT '',
    client_log      TEXT DEFAULT '',
    cost_usd        REAL DEFAULT 0,
    error           TEXT DEFAULT '',
    disable_pii     INTEGER DEFAULT 0,
    no_ocr_first    INTEGER DEFAULT 0,
    total_pages     INTEGER DEFAULT 0,
    retry_count     INTEGER DEFAULT 0,
    last_retry      TEXT DEFAULT '',
    stats_json      TEXT DEFAULT '{}',
    verification_json TEXT DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS vendor_categories (
    vendor_norm     TEXT PRIMARY KEY,
    category        TEXT NOT NULL,
    count           INTEGER DEFAULT 1,
    last_used       TEXT DEFAULT '',
    original        TEXT DEFAULT ''
);

CREATE TABLE IF NOT EXISTS client_context_docs (
    doc_id          TEXT PRIMARY KEY,
    client_name     TEXT NOT NULL,
    label           TEXT DEFAULT '',
    filename        TEXT DEFAULT '',
    file_path       TEXT DEFAULT '',
    year            TEXT DEFAULT '',
    uploaded        TEXT DEFAULT '',
    payers_json     TEXT DEFAULT '[]',
    raw_text        TEXT DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_ctx_client ON client_context_docs(client_name);

CREATE TABLE IF NOT EXISTS client_prior_year_data (
    client_name     TEXT PRIMARY KEY,
    data_json       TEXT DEFAULT '{}',
    updated         TEXT DEFAULT ''
);

CREATE TABLE IF NOT EXISTS client_instructions (
    rule_id         TEXT PRIMARY KEY,
    client_name     TEXT NOT NULL,
    text            TEXT NOT NULL,
    created         TEXT DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_instr_client ON client_instructions(client_name);

-- ═══ REVIEW CHAIN TABLES ═══

CREATE TABLE IF NOT EXISTS users (
    user_id         TEXT PRIMARY KEY,
    display_name    TEXT NOT NULL,
    role            TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS facts (
    client_id       TEXT NOT NULL,
    tax_year        TEXT NOT NULL,
    field_id        TEXT NOT NULL,
    value           TEXT,
    value_type      TEXT DEFAULT 'text',
    last_set_by     TEXT DEFAULT '',
    last_set_at     TEXT DEFAULT '',
    last_evidence_id TEXT DEFAULT '',
    status          TEXT DEFAULT 'extracted',
    PRIMARY KEY (client_id, tax_year, field_id)
);
CREATE INDEX IF NOT EXISTS idx_facts_client_year ON facts(client_id, tax_year);
CREATE INDEX IF NOT EXISTS idx_facts_field_id ON facts(field_id);

CREATE TABLE IF NOT EXISTS fact_events (
    event_id        INTEGER PRIMARY KEY AUTOINCREMENT,
    client_id       TEXT NOT NULL,
    tax_year        TEXT NOT NULL,
    field_id        TEXT NOT NULL,
    old_value       TEXT,
    new_value       TEXT,
    actor_user_id   TEXT NOT NULL,
    actor_role      TEXT NOT NULL,
    action          TEXT NOT NULL,
    reason          TEXT DEFAULT '',
    evidence_id     TEXT DEFAULT '',
    created_at      TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_events_field ON fact_events(client_id, tax_year, field_id);
CREATE INDEX IF NOT EXISTS idx_events_actor ON fact_events(actor_user_id);

CREATE TABLE IF NOT EXISTS review_state (
    client_id       TEXT NOT NULL,
    tax_year        TEXT NOT NULL,
    field_id        TEXT NOT NULL,
    stage           TEXT NOT NULL DEFAULT 'extracted',
    assigned_to     TEXT DEFAULT '',
    locked_by       TEXT DEFAULT '',
    locked_at       TEXT DEFAULT '',
    updated_at      TEXT DEFAULT '',
    last_action     TEXT DEFAULT '',
    PRIMARY KEY (client_id, tax_year, field_id)
);
CREATE INDEX IF NOT EXISTS idx_review_stage ON review_state(stage);
CREATE INDEX IF NOT EXISTS idx_review_assigned ON review_state(assigned_to);

CREATE TABLE IF NOT EXISTS audit_events (
    audit_id        INTEGER PRIMARY KEY AUTOINCREMENT,
    event_type      TEXT NOT NULL,
    client_id       TEXT DEFAULT '',
    tax_year        TEXT DEFAULT '',
    field_id        TEXT DEFAULT '',
    actor_user_id   TEXT DEFAULT '',
    details_json    TEXT DEFAULT '{}',
    created_at      TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_audit_type ON audit_events(event_type);
CREATE INDEX IF NOT EXISTS idx_audit_client ON audit_events(client_id, tax_year);

CREATE TABLE IF NOT EXISTS config (
    key             TEXT PRIMARY KEY,
    value           TEXT NOT NULL
);
"""

_SEED_SQL = """
INSERT OR IGNORE INTO users VALUES ('jeff', 'Jeffrey Watts', 'preparer');
INSERT OR IGNORE INTO users VALUES ('susan', 'Susan', 'reviewer');
INSERT OR IGNORE INTO users VALUES ('charles', 'Charles', 'partner');
INSERT OR IGNORE INTO config VALUES ('EXPORT_REQUIRES_PARTNER_REVIEW', 'false');
INSERT OR IGNORE INTO config VALUES ('LOCK_TIMEOUT_MINUTES', '30');
"""


def init_db():
    """Create all tables and seed data. Safe to call multiple times."""
    with transaction() as conn:
        conn.executescript(_SCHEMA_SQL)
        conn.executescript(_SEED_SQL)


def needs_migration():
    """Check if JSON files exist but migration hasn't been done yet."""
    jobs_file = BASE_DIR / "data" / "jobs_history.json"
    if not jobs_file.exists():
        return False
    val = get_config("migration_v1_done")
    return val != "true"


# ─── Config ──────────────────────────────────────────────────────────────────

def get_config(key):
    """Get a config value. Returns None if not found."""
    conn = get_connection()
    try:
        row = conn.execute("SELECT value FROM config WHERE key = ?", (key,)).fetchone()
        return row["value"] if row else None
    finally:
        conn.close()


def set_config(key, value):
    """Set a config value (upsert)."""
    with transaction() as conn:
        conn.execute(
            "INSERT INTO config (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = ?",
            (key, str(value), str(value))
        )


# ─── Users ───────────────────────────────────────────────────────────────────

def list_users():
    conn = get_connection()
    try:
        return _rows_to_dicts(conn.execute("SELECT * FROM users ORDER BY user_id").fetchall())
    finally:
        conn.close()


def get_user(user_id):
    conn = get_connection()
    try:
        return _row_to_dict(conn.execute("SELECT * FROM users WHERE user_id = ?", (user_id,)).fetchone())
    finally:
        conn.close()


# ─── Jobs CRUD ───────────────────────────────────────────────────────────────

# Columns that map directly from a job dict to the jobs table
_JOB_COLUMNS = [
    "job_id", "filename", "client_name", "doc_type", "output_format",
    "user_notes", "ai_instructions", "year", "status", "stage", "progress",
    "created", "start_time", "end_time", "pdf_path", "client_folder",
    "output_xlsx", "output_log", "client_xlsx", "client_log", "cost_usd",
    "error", "disable_pii", "no_ocr_first", "total_pages", "retry_count",
    "last_retry",
]


def _job_to_row(job_data):
    """Convert a job dict to column values for INSERT/UPDATE."""
    row = {}
    for col in _JOB_COLUMNS:
        val = job_data.get(col, job_data.get("id") if col == "job_id" else "")
        if col == "job_id" and not val:
            val = job_data.get("id", "")
        if isinstance(val, bool):
            val = 1 if val else 0
        row[col] = val
    # JSON blobs
    row["stats_json"] = json.dumps(job_data.get("stats", {}), default=str)
    row["verification_json"] = json.dumps(job_data.get("verification", {}), default=str)
    return row


def _row_to_job(row):
    """Convert a DB row to a job dict matching the legacy format."""
    if row is None:
        return None
    d = dict(row)
    d["id"] = d.pop("job_id", d.get("id", ""))
    d["stats"] = json.loads(d.pop("stats_json", "{}") or "{}")
    d["verification"] = json.loads(d.pop("verification_json", "{}") or "{}")
    d["disable_pii"] = bool(d.get("disable_pii", 0))
    d["no_ocr_first"] = bool(d.get("no_ocr_first", 0))
    return d


def save_job(job_data):
    """Upsert a job record."""
    row = _job_to_row(job_data)
    cols = list(row.keys())
    placeholders = ", ".join(["?"] * len(cols))
    col_names = ", ".join(cols)
    updates = ", ".join(f"{c} = ?" for c in cols if c != "job_id")
    vals = [row[c] for c in cols]
    update_vals = [row[c] for c in cols if c != "job_id"]

    with transaction() as conn:
        conn.execute(
            f"INSERT INTO jobs ({col_names}) VALUES ({placeholders}) "
            f"ON CONFLICT(job_id) DO UPDATE SET {updates}",
            vals + update_vals
        )


def get_job(job_id):
    """Get a single job by ID."""
    conn = get_connection()
    try:
        row = conn.execute("SELECT * FROM jobs WHERE job_id = ?", (job_id,)).fetchone()
        return _row_to_job(row)
    finally:
        conn.close()


def update_job(job_id, **fields):
    """Partial update of a job. Only updates specified fields."""
    if not fields:
        return
    # Handle special JSON fields
    if "stats" in fields:
        fields["stats_json"] = json.dumps(fields.pop("stats"), default=str)
    if "verification" in fields:
        fields["verification_json"] = json.dumps(fields.pop("verification"), default=str)
    if "id" in fields:
        fields.pop("id")  # Don't update PK

    set_clause = ", ".join(f"{k} = ?" for k in fields.keys())
    vals = list(fields.values())
    vals.append(job_id)

    with transaction() as conn:
        conn.execute(f"UPDATE jobs SET {set_clause} WHERE job_id = ?", vals)


def list_jobs(q=None, doc_type=None, status=None):
    """List jobs with optional filters."""
    conn = get_connection()
    try:
        sql = "SELECT * FROM jobs"
        params = []
        conditions = []

        if q:
            conditions.append("(client_name LIKE ? OR filename LIKE ?)")
            params.extend([f"%{q}%", f"%{q}%"])
        if doc_type:
            conditions.append("doc_type = ?")
            params.append(doc_type)
        if status:
            conditions.append("status = ?")
            params.append(status)

        if conditions:
            sql += " WHERE " + " AND ".join(conditions)

        sql += " ORDER BY created DESC"
        rows = conn.execute(sql, params).fetchall()
        return [_row_to_job(r) for r in rows]
    finally:
        conn.close()


def delete_job(job_id):
    """Delete a job and all related data."""
    with transaction() as conn:
        conn.execute("DELETE FROM jobs WHERE job_id = ?", (job_id,))
        # Clean up facts/review_state/events that reference this job
        conn.execute("DELETE FROM facts WHERE field_id LIKE ?", (f"{job_id}:%",))
        conn.execute("DELETE FROM fact_events WHERE field_id LIKE ?", (f"{job_id}:%",))
        conn.execute("DELETE FROM review_state WHERE field_id LIKE ?", (f"{job_id}:%",))
        conn.execute("DELETE FROM audit_events WHERE field_id LIKE ?", (f"{job_id}:%",))


def clear_stale_jobs():
    """Mark running/queued jobs as interrupted (called on startup)."""
    with transaction() as conn:
        conn.execute(
            "UPDATE jobs SET status = 'interrupted' WHERE status IN ('running', 'queued')"
        )


# ─── Vendor Categories ──────────────────────────────────────────────────────

def get_vendor_categories():
    """Return all vendor categories as a dict {vendor_norm: {category, count, last_used, original}}."""
    conn = get_connection()
    try:
        rows = conn.execute("SELECT * FROM vendor_categories").fetchall()
        result = {}
        for r in rows:
            result[r["vendor_norm"]] = {
                "category": r["category"],
                "count": r["count"],
                "last_used": r["last_used"],
                "original": r["original"],
            }
        return result
    finally:
        conn.close()


def set_vendor_category(vendor_norm, category, original="", count=1):
    """Upsert a vendor → category mapping."""
    now = datetime.now().isoformat()
    with transaction() as conn:
        existing = conn.execute(
            "SELECT count FROM vendor_categories WHERE vendor_norm = ?", (vendor_norm,)
        ).fetchone()
        if existing:
            conn.execute(
                "UPDATE vendor_categories SET category = ?, count = count + 1, last_used = ? WHERE vendor_norm = ?",
                (category, now, vendor_norm)
            )
        else:
            conn.execute(
                "INSERT INTO vendor_categories (vendor_norm, category, count, last_used, original) VALUES (?, ?, ?, ?, ?)",
                (vendor_norm, category, count, now, original)
            )


def suggest_category(vendor_desc):
    """Suggest a category for a vendor description using exact or prefix match."""
    norm = _normalize_vendor(vendor_desc)
    if not norm:
        return ""
    conn = get_connection()
    try:
        # Exact match
        row = conn.execute(
            "SELECT category FROM vendor_categories WHERE vendor_norm = ?", (norm,)
        ).fetchone()
        if row:
            return row["category"]
        # Prefix match (vendor norm starts with our query or vice versa)
        rows = conn.execute(
            "SELECT vendor_norm, category, count FROM vendor_categories "
            "WHERE vendor_norm LIKE ? OR ? LIKE vendor_norm || '%' "
            "ORDER BY count DESC LIMIT 1",
            (f"{norm}%", norm)
        ).fetchall()
        if rows:
            return rows[0]["category"]
        return ""
    finally:
        conn.close()


def _normalize_vendor(desc):
    """Normalize a vendor description for matching."""
    if not desc:
        return ""
    s = desc.upper().strip()
    # Strip trailing store numbers (#1234, 0423)
    s = re.sub(r'\s*#?\d{3,}$', '', s)
    # Strip company suffixes
    s = re.sub(r'\s+(LLC|INC|CORP|LTD|CO|LP|LLP|PC|PA|PLLC|NA|FSB)\.?\s*$', '', s, flags=re.IGNORECASE)
    # Strip punctuation
    s = re.sub(r'[^\w\s]', '', s)
    s = re.sub(r'\s+', ' ', s).strip()
    return s


# ─── Client Context ──────────────────────────────────────────────────────────

def list_context_docs(client_name):
    """List context documents for a client."""
    conn = get_connection()
    try:
        rows = conn.execute(
            "SELECT * FROM client_context_docs WHERE client_name = ? ORDER BY uploaded DESC",
            (client_name,)
        ).fetchall()
        result = []
        for r in rows:
            d = dict(r)
            d["payers"] = json.loads(d.pop("payers_json", "[]") or "[]")
            result.append(d)
        return result
    finally:
        conn.close()


def get_context_doc(doc_id):
    conn = get_connection()
    try:
        row = conn.execute("SELECT * FROM client_context_docs WHERE doc_id = ?", (doc_id,)).fetchone()
        if not row:
            return None
        d = dict(row)
        d["payers"] = json.loads(d.pop("payers_json", "[]") or "[]")
        return d
    finally:
        conn.close()


def add_context_doc(client_name, doc_id, label="", filename="", file_path="",
                    year="", payers=None, raw_text=""):
    now = datetime.now().isoformat()
    with transaction() as conn:
        conn.execute(
            "INSERT OR REPLACE INTO client_context_docs "
            "(doc_id, client_name, label, filename, file_path, year, uploaded, payers_json, raw_text) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (doc_id, client_name, label, filename, str(file_path), year, now,
             json.dumps(payers or [], default=str), raw_text)
        )


def delete_context_doc(doc_id):
    with transaction() as conn:
        conn.execute("DELETE FROM client_context_docs WHERE doc_id = ?", (doc_id,))


def get_prior_year_data(client_name):
    conn = get_connection()
    try:
        row = conn.execute(
            "SELECT data_json FROM client_prior_year_data WHERE client_name = ?",
            (client_name,)
        ).fetchone()
        if not row:
            return {}
        return json.loads(row["data_json"] or "{}")
    finally:
        conn.close()


def set_prior_year_data(client_name, data):
    now = datetime.now().isoformat()
    with transaction() as conn:
        conn.execute(
            "INSERT INTO client_prior_year_data (client_name, data_json, updated) "
            "VALUES (?, ?, ?) ON CONFLICT(client_name) DO UPDATE SET data_json = ?, updated = ?",
            (client_name, json.dumps(data, default=str), now,
             json.dumps(data, default=str), now)
        )


# ─── Client Instructions ─────────────────────────────────────────────────────

def list_instructions(client_name):
    conn = get_connection()
    try:
        rows = conn.execute(
            "SELECT * FROM client_instructions WHERE client_name = ? ORDER BY created",
            (client_name,)
        ).fetchall()
        return _rows_to_dicts(rows)
    finally:
        conn.close()


def add_instruction(client_name, rule_id, text):
    now = datetime.now().isoformat()
    with transaction() as conn:
        conn.execute(
            "INSERT OR REPLACE INTO client_instructions (rule_id, client_name, text, created) "
            "VALUES (?, ?, ?, ?)",
            (rule_id, client_name, text, now)
        )


def delete_instruction(rule_id):
    with transaction() as conn:
        conn.execute("DELETE FROM client_instructions WHERE rule_id = ?", (rule_id,))


def get_instructions_text(client_name):
    """Get all instructions as a single text block for prompt injection."""
    instructions = list_instructions(client_name)
    if not instructions:
        return ""
    return "\n".join(i["text"] for i in instructions if i.get("text"))


# ─── Facts (Canonical Values) ────────────────────────────────────────────────

def get_fact(client_id, tax_year, field_id):
    conn = get_connection()
    try:
        row = conn.execute(
            "SELECT * FROM facts WHERE client_id = ? AND tax_year = ? AND field_id = ?",
            (client_id, tax_year, field_id)
        ).fetchone()
        return _row_to_dict(row)
    finally:
        conn.close()


def get_facts_for_job(job_id):
    """Get all facts whose field_id starts with job_id:"""
    conn = get_connection()
    try:
        rows = conn.execute(
            "SELECT * FROM facts WHERE field_id LIKE ?",
            (f"{job_id}:%",)
        ).fetchall()
        return _rows_to_dicts(rows)
    finally:
        conn.close()


def set_fact(client_id, tax_year, field_id, value, value_type="text",
             set_by="", evidence_id="", status="extracted"):
    """Upsert a single canonical fact."""
    now = datetime.now().isoformat()
    with transaction() as conn:
        conn.execute(
            "INSERT INTO facts (client_id, tax_year, field_id, value, value_type, "
            "last_set_by, last_set_at, last_evidence_id, status) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) "
            "ON CONFLICT(client_id, tax_year, field_id) DO UPDATE SET "
            "value = ?, value_type = ?, last_set_by = ?, last_set_at = ?, "
            "last_evidence_id = ?, status = ?",
            (client_id, tax_year, field_id, str(value) if value is not None else None,
             value_type, set_by, now, evidence_id, status,
             str(value) if value is not None else None, value_type, set_by, now, evidence_id, status)
        )


def bulk_set_facts(facts_list):
    """Insert multiple facts in a single transaction.

    facts_list: list of dicts with keys: client_id, tax_year, field_id, value, value_type, set_by, evidence_id, status
    """
    now = datetime.now().isoformat()
    with transaction() as conn:
        for f in facts_list:
            val = f.get("value")
            conn.execute(
                "INSERT OR REPLACE INTO facts "
                "(client_id, tax_year, field_id, value, value_type, last_set_by, last_set_at, last_evidence_id, status) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (f["client_id"], f["tax_year"], f["field_id"],
                 str(val) if val is not None else None,
                 f.get("value_type", "text"), f.get("set_by", "system"),
                 now, f.get("evidence_id", ""), f.get("status", "extracted"))
            )


# ─── Fact Events (Append-Only Ledger) ────────────────────────────────────────

def record_fact_event(client_id, tax_year, field_id, old_value, new_value,
                      actor_user_id, actor_role, action, reason="", evidence_id=""):
    """Append an immutable event record."""
    now = datetime.now().isoformat()
    with transaction() as conn:
        conn.execute(
            "INSERT INTO fact_events "
            "(client_id, tax_year, field_id, old_value, new_value, "
            "actor_user_id, actor_role, action, reason, evidence_id, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (client_id, tax_year, field_id,
             str(old_value) if old_value is not None else None,
             str(new_value) if new_value is not None else None,
             actor_user_id, actor_role, action, reason, evidence_id, now)
        )


def get_fact_history(client_id, tax_year, field_id):
    """Get the full event history for a field."""
    conn = get_connection()
    try:
        rows = conn.execute(
            "SELECT * FROM fact_events WHERE client_id = ? AND tax_year = ? AND field_id = ? "
            "ORDER BY created_at",
            (client_id, tax_year, field_id)
        ).fetchall()
        return _rows_to_dicts(rows)
    finally:
        conn.close()


def get_fact_events_for_job(job_id):
    """Get all fact events for fields belonging to a job."""
    conn = get_connection()
    try:
        rows = conn.execute(
            "SELECT * FROM fact_events WHERE field_id LIKE ? ORDER BY created_at",
            (f"{job_id}:%",)
        ).fetchall()
        return _rows_to_dicts(rows)
    finally:
        conn.close()


# ─── Review State (Workflow) ─────────────────────────────────────────────────

def get_review_state(client_id, tax_year, field_id):
    conn = get_connection()
    try:
        row = conn.execute(
            "SELECT * FROM review_state WHERE client_id = ? AND tax_year = ? AND field_id = ?",
            (client_id, tax_year, field_id)
        ).fetchone()
        return _row_to_dict(row)
    finally:
        conn.close()


def get_review_states_for_job(job_id):
    """Get all review states for fields belonging to a job."""
    conn = get_connection()
    try:
        rows = conn.execute(
            "SELECT * FROM review_state WHERE field_id LIKE ?",
            (f"{job_id}:%",)
        ).fetchall()
        return _rows_to_dicts(rows)
    finally:
        conn.close()


def set_review_stage(client_id, tax_year, field_id, stage, assigned_to="", last_action=""):
    """Update the review stage for a field."""
    now = datetime.now().isoformat()
    with transaction() as conn:
        conn.execute(
            "INSERT INTO review_state (client_id, tax_year, field_id, stage, assigned_to, updated_at, last_action) "
            "VALUES (?, ?, ?, ?, ?, ?, ?) "
            "ON CONFLICT(client_id, tax_year, field_id) DO UPDATE SET "
            "stage = ?, assigned_to = ?, updated_at = ?, last_action = ?, locked_by = '', locked_at = ''",
            (client_id, tax_year, field_id, stage, assigned_to, now, last_action,
             stage, assigned_to, now, last_action)
        )


def bulk_init_review_states(states):
    """Initialize review state for multiple fields.

    states: list of dicts with keys: client_id, tax_year, field_id, stage, assigned_to
    """
    now = datetime.now().isoformat()
    with transaction() as conn:
        for s in states:
            conn.execute(
                "INSERT OR IGNORE INTO review_state "
                "(client_id, tax_year, field_id, stage, assigned_to, updated_at) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (s["client_id"], s["tax_year"], s["field_id"],
                 s.get("stage", "extracted"), s.get("assigned_to", "jeff"), now)
            )


def get_inbox(user_id):
    """Get items assigned to a user, grouped by job.

    Returns a list of dicts: [{job_id, client_name, year, doc_type, field_count, stages}]
    """
    conn = get_connection()
    try:
        # Get all fields assigned to this user
        rows = conn.execute(
            "SELECT rs.field_id, rs.stage, rs.locked_by, "
            "j.job_id, j.client_name, j.year, j.doc_type, j.filename "
            "FROM review_state rs "
            "JOIN jobs j ON rs.field_id LIKE j.job_id || ':%' "
            "WHERE rs.assigned_to = ? "
            "ORDER BY j.created DESC",
            (user_id,)
        ).fetchall()

        # Group by job
        job_groups = {}
        for r in rows:
            jid = r["job_id"]
            if jid not in job_groups:
                job_groups[jid] = {
                    "job_id": jid,
                    "client_name": r["client_name"],
                    "year": r["year"],
                    "doc_type": r["doc_type"],
                    "filename": r["filename"],
                    "field_count": 0,
                    "stages": {},
                }
            job_groups[jid]["field_count"] += 1
            stage = r["stage"]
            job_groups[jid]["stages"][stage] = job_groups[jid]["stages"].get(stage, 0) + 1

        return list(job_groups.values())
    finally:
        conn.close()


def get_review_summary_for_job(job_id):
    """Get a summary of review stages for a job."""
    conn = get_connection()
    try:
        rows = conn.execute(
            "SELECT stage, COUNT(*) as cnt FROM review_state WHERE field_id LIKE ? GROUP BY stage",
            (f"{job_id}:%",)
        ).fetchall()
        return {r["stage"]: r["cnt"] for r in rows}
    finally:
        conn.close()


def get_review_queue(job_id, user_id, user_role):
    """Return an ordered queue of fields for guided review.

    Filters by the stage appropriate for the user's role and assigned_to = user_id.
    Orders by page number, then ext_idx, then field_name (groups same-page fields together).

    Returns: list of dicts with field_id, page, ext_idx, field_name, value, stage, assigned_to
    """
    # Determine which stage(s) this role can act on
    stage_map = {
        "preparer": ("extracted",),
        "reviewer": ("prepared",),
        "partner": ("reviewed",),
    }
    stages = stage_map.get(user_role, ())
    if not stages:
        return []

    conn = get_connection()
    try:
        placeholders = ",".join("?" for _ in stages)
        rows = conn.execute(
            f"""SELECT rs.field_id, rs.stage, rs.assigned_to, rs.locked_by,
                       f.value, f.value_type
                FROM review_state rs
                LEFT JOIN facts f ON rs.client_id = f.client_id
                    AND rs.tax_year = f.tax_year AND rs.field_id = f.field_id
                WHERE rs.field_id LIKE ?
                  AND rs.stage IN ({placeholders})
                  AND rs.assigned_to = ?
                ORDER BY rs.field_id""",
            (f"{job_id}:%", *stages, user_id)
        ).fetchall()

        queue = []
        for r in rows:
            fid = r["field_id"]
            # Parse field_id: job_id:page:ext_idx:field_name
            parts = fid.split(":", 3)
            if len(parts) != 4:
                continue
            _, page_str, ext_idx_str, field_name = parts
            try:
                page = int(page_str)
                ext_idx = int(ext_idx_str)
            except (ValueError, TypeError):
                continue

            queue.append({
                "field_id": fid,
                "page": page,
                "ext_idx": ext_idx,
                "field_name": field_name,
                "value": r["value"],
                "value_type": r["value_type"],
                "stage": r["stage"],
                "assigned_to": r["assigned_to"],
                "locked_by": r["locked_by"] or "",
            })

        # Sort: page ASC, ext_idx ASC, field_name ASC
        queue.sort(key=lambda x: (x["page"], x["ext_idx"], x["field_name"]))
        return queue
    finally:
        conn.close()


# ─── Locking ─────────────────────────────────────────────────────────────────

def acquire_lock(client_id, tax_year, field_id, user_id):
    """Attempt to lock a field for editing. Returns True if acquired or already held."""
    timeout_str = get_config("LOCK_TIMEOUT_MINUTES") or "30"
    timeout = int(timeout_str)
    cutoff = (datetime.utcnow() - timedelta(minutes=timeout)).isoformat()

    with transaction() as conn:
        # Release expired locks
        conn.execute(
            "UPDATE review_state SET locked_by = '', locked_at = '' "
            "WHERE locked_at != '' AND locked_at < ? AND locked_by != ''",
            (cutoff,)
        )

        row = conn.execute(
            "SELECT locked_by FROM review_state "
            "WHERE client_id = ? AND tax_year = ? AND field_id = ?",
            (client_id, tax_year, field_id)
        ).fetchone()

        if not row:
            return False

        if row["locked_by"] and row["locked_by"] != user_id:
            return False  # Locked by someone else

        now = datetime.utcnow().isoformat()
        conn.execute(
            "UPDATE review_state SET locked_by = ?, locked_at = ? "
            "WHERE client_id = ? AND tax_year = ? AND field_id = ?",
            (user_id, now, client_id, tax_year, field_id)
        )
        return True


def release_lock(client_id, tax_year, field_id, user_id):
    """Release a lock held by user_id."""
    with transaction() as conn:
        conn.execute(
            "UPDATE review_state SET locked_by = '', locked_at = '' "
            "WHERE client_id = ? AND tax_year = ? AND field_id = ? AND locked_by = ?",
            (client_id, tax_year, field_id, user_id)
        )


def bulk_acquire_lock(client_id, tax_year, field_ids, user_id):
    """Attempt to lock multiple fields. Returns (acquired_ids, locked_by_others).

    locked_by_others: list of (field_id, locked_by_user_id)
    """
    acquired = []
    locked_by_others = []
    timeout_str = get_config("LOCK_TIMEOUT_MINUTES") or "30"
    timeout = int(timeout_str)
    cutoff = (datetime.utcnow() - timedelta(minutes=timeout)).isoformat()
    now = datetime.utcnow().isoformat()

    with transaction() as conn:
        # Release expired locks first
        conn.execute(
            "UPDATE review_state SET locked_by = '', locked_at = '' "
            "WHERE locked_at != '' AND locked_at < ? AND locked_by != ''",
            (cutoff,)
        )

        for fid in field_ids:
            row = conn.execute(
                "SELECT locked_by FROM review_state "
                "WHERE client_id = ? AND tax_year = ? AND field_id = ?",
                (client_id, tax_year, fid)
            ).fetchone()

            if not row:
                continue

            if row["locked_by"] and row["locked_by"] != user_id:
                locked_by_others.append((fid, row["locked_by"]))
            else:
                conn.execute(
                    "UPDATE review_state SET locked_by = ?, locked_at = ? "
                    "WHERE client_id = ? AND tax_year = ? AND field_id = ?",
                    (user_id, now, client_id, tax_year, fid)
                )
                acquired.append(fid)

    return acquired, locked_by_others


def release_expired_locks():
    """Release all locks that have timed out."""
    timeout_str = get_config("LOCK_TIMEOUT_MINUTES") or "30"
    timeout = int(timeout_str)
    cutoff = (datetime.utcnow() - timedelta(minutes=timeout)).isoformat()

    with transaction() as conn:
        conn.execute(
            "UPDATE review_state SET locked_by = '', locked_at = '' "
            "WHERE locked_at != '' AND locked_at < ? AND locked_by != ''",
            (cutoff,)
        )


def bulk_release_locks(client_id, tax_year, field_ids, user_id):
    """Release locks for multiple fields."""
    with transaction() as conn:
        for fid in field_ids:
            conn.execute(
                "UPDATE review_state SET locked_by = '', locked_at = '' "
                "WHERE client_id = ? AND tax_year = ? AND field_id = ? AND locked_by = ?",
                (client_id, tax_year, fid, user_id)
            )


# ─── Audit Events ────────────────────────────────────────────────────────────

def log_audit(event_type, client_id="", tax_year="", field_id="",
              actor_user_id="", details=None):
    """Append an audit event."""
    now = datetime.now().isoformat()
    with transaction() as conn:
        conn.execute(
            "INSERT INTO audit_events "
            "(event_type, client_id, tax_year, field_id, actor_user_id, details_json, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (event_type, client_id, tax_year, field_id, actor_user_id,
             json.dumps(details or {}, default=str), now)
        )


def get_audit_trail(client_id=None, tax_year=None, job_id=None, limit=500):
    """Get audit events with optional filters."""
    conn = get_connection()
    try:
        sql = "SELECT * FROM audit_events"
        params = []
        conditions = []

        if client_id:
            conditions.append("client_id = ?")
            params.append(client_id)
        if tax_year:
            conditions.append("tax_year = ?")
            params.append(tax_year)
        if job_id:
            conditions.append("field_id LIKE ?")
            params.append(f"{job_id}:%")

        if conditions:
            sql += " WHERE " + " AND ".join(conditions)
        sql += " ORDER BY created_at DESC LIMIT ?"
        params.append(limit)

        rows = conn.execute(sql, params).fetchall()
        result = []
        for r in rows:
            d = dict(r)
            d["details"] = json.loads(d.pop("details_json", "{}") or "{}")
            result.append(d)
        return result
    finally:
        conn.close()


# ─── Review Chain Workflow ────────────────────────────────────────────────────

# Role → next stage mapping
_ROLE_CHAIN = {
    "preparer": {"next_stage": "prepared", "next_assigned": "susan"},
    "reviewer": {"next_stage": "reviewed", "next_assigned": "charles"},
    "partner":  {"next_stage": "partner_reviewed", "next_assigned": ""},
}

# Send-back targets
_SEND_BACK_TARGET = {
    "reviewer": {"stage": "extracted", "assigned_to": "jeff"},
    "partner":  {"stage": "prepared", "assigned_to": "susan"},
}


def process_verify(client_id, tax_year, field_id, value, actor_user_id, actor_role,
                   evidence_id="", reason=""):
    """Preparer verifies or corrects a field value.

    Sets the canonical fact and advances to 'prepared' stage.
    """
    if actor_role != "preparer":
        return {"error": "Only preparer can verify fields"}

    old_fact = get_fact(client_id, tax_year, field_id)
    old_value = old_fact["value"] if old_fact else None

    # Update canonical fact
    set_fact(client_id, tax_year, field_id, value, set_by=actor_user_id,
             evidence_id=evidence_id, status="verified")

    # Record event
    record_fact_event(client_id, tax_year, field_id, old_value, str(value) if value is not None else None,
                      actor_user_id, actor_role, "verify", reason, evidence_id)

    # Advance stage
    chain = _ROLE_CHAIN[actor_role]
    set_review_stage(client_id, tax_year, field_id, chain["next_stage"],
                     assigned_to=chain["next_assigned"], last_action="verified")

    # Audit
    log_audit("FACT_VERIFIED", client_id, tax_year, field_id, actor_user_id,
              {"old_value": old_value, "new_value": value})
    log_audit("STAGE_ADVANCED", client_id, tax_year, field_id, actor_user_id,
              {"new_stage": chain["next_stage"], "assigned_to": chain["next_assigned"]})

    return {"ok": True}


def process_approve(client_id, tax_year, field_id, actor_user_id, actor_role):
    """Reviewer or Partner approves a field (no value change)."""
    if actor_role not in ("reviewer", "partner"):
        return {"error": "Only reviewer or partner can approve fields"}

    # Verify the field is at the right stage
    rs = get_review_state(client_id, tax_year, field_id)
    if not rs:
        return {"error": "No review state found for field"}

    expected_stages = {"reviewer": "prepared", "partner": "reviewed"}
    if rs["stage"] != expected_stages.get(actor_role):
        return {"error": f"Field is at stage '{rs['stage']}', expected '{expected_stages[actor_role]}' for {actor_role}"}

    # Check lock
    if rs["locked_by"] and rs["locked_by"] != actor_user_id:
        return {"error": f"Field locked by {rs['locked_by']}"}

    fact = get_fact(client_id, tax_year, field_id)
    current_value = fact["value"] if fact else None

    # Record approval event (no value change)
    record_fact_event(client_id, tax_year, field_id, current_value, current_value,
                      actor_user_id, actor_role, "approve")

    # Advance stage
    chain = _ROLE_CHAIN[actor_role]
    set_review_stage(client_id, tax_year, field_id, chain["next_stage"],
                     assigned_to=chain["next_assigned"], last_action="approved")

    # Audit
    log_audit("REVIEW_APPROVED", client_id, tax_year, field_id, actor_user_id,
              {"role": actor_role, "stage": chain["next_stage"]})
    log_audit("STAGE_ADVANCED", client_id, tax_year, field_id, actor_user_id,
              {"new_stage": chain["next_stage"], "assigned_to": chain["next_assigned"]})

    return {"ok": True}


def process_override(client_id, tax_year, field_id, new_value, actor_user_id, actor_role,
                     reason="", evidence_id=""):
    """Reviewer or Partner overrides a field value."""
    if actor_role not in ("reviewer", "partner"):
        return {"error": "Only reviewer or partner can override fields"}

    rs = get_review_state(client_id, tax_year, field_id)
    if not rs:
        return {"error": "No review state found for field"}

    expected_stages = {"reviewer": "prepared", "partner": "reviewed"}
    if rs["stage"] != expected_stages.get(actor_role):
        return {"error": f"Field is at stage '{rs['stage']}', expected '{expected_stages[actor_role]}' for {actor_role}"}

    if rs["locked_by"] and rs["locked_by"] != actor_user_id:
        return {"error": f"Field locked by {rs['locked_by']}"}

    old_fact = get_fact(client_id, tax_year, field_id)
    old_value = old_fact["value"] if old_fact else None

    # Update canonical fact
    set_fact(client_id, tax_year, field_id, new_value, set_by=actor_user_id,
             evidence_id=evidence_id, status="verified")

    # Record override event
    record_fact_event(client_id, tax_year, field_id, old_value,
                      str(new_value) if new_value is not None else None,
                      actor_user_id, actor_role, "override", reason, evidence_id)

    # Advance stage
    chain = _ROLE_CHAIN[actor_role]
    set_review_stage(client_id, tax_year, field_id, chain["next_stage"],
                     assigned_to=chain["next_assigned"], last_action="overridden")

    # Audit
    log_audit("FACT_OVERRIDDEN", client_id, tax_year, field_id, actor_user_id,
              {"old_value": old_value, "new_value": new_value, "reason": reason})
    log_audit("STAGE_ADVANCED", client_id, tax_year, field_id, actor_user_id,
              {"new_stage": chain["next_stage"], "assigned_to": chain["next_assigned"]})

    return {"ok": True}


def process_send_back(client_id, tax_year, field_id, actor_user_id, actor_role,
                      reason="", send_to=None):
    """Reviewer or Partner sends a field back for rework."""
    if actor_role not in ("reviewer", "partner"):
        return {"error": "Only reviewer or partner can send back fields"}

    rs = get_review_state(client_id, tax_year, field_id)
    if not rs:
        return {"error": "No review state found for field"}

    if rs["locked_by"] and rs["locked_by"] != actor_user_id:
        return {"error": f"Field locked by {rs['locked_by']}"}

    # Determine target
    if send_to:
        target_user = get_user(send_to)
        if not target_user:
            return {"error": f"Unknown user: {send_to}"}
        # Map user to the stage they work at
        target_stages = {"jeff": "extracted", "susan": "prepared"}
        target_stage = target_stages.get(send_to, "extracted")
    else:
        target = _SEND_BACK_TARGET.get(actor_role, {"stage": "extracted", "assigned_to": "jeff"})
        target_stage = target["stage"]
        send_to = target["assigned_to"]

    fact = get_fact(client_id, tax_year, field_id)
    current_value = fact["value"] if fact else None

    # Record send-back event
    record_fact_event(client_id, tax_year, field_id, current_value, current_value,
                      actor_user_id, actor_role, "send_back", reason)

    # Roll back stage
    set_review_stage(client_id, tax_year, field_id, target_stage,
                     assigned_to=send_to, last_action=f"sent_back_by_{actor_role}")

    # Audit
    log_audit("REVIEW_SENT_BACK", client_id, tax_year, field_id, actor_user_id,
              {"reason": reason, "sent_to": send_to, "new_stage": target_stage})

    return {"ok": True}


# Reverse mapping: given the stage a field is at now, what stage was it before?
_STAGE_REVERSE = {
    "prepared": {"stage": "extracted", "assigned_to": "jeff"},
    "reviewed": {"stage": "prepared", "assigned_to": "susan"},
    "partner_reviewed": {"stage": "reviewed", "assigned_to": "charles"},
}


def process_undo(client_id, tax_year, field_id, actor_user_id, actor_role):
    """Undo the last review action on a field.

    Looks at the most recent fact_event, restores old_value and rolls back stage.
    Records an 'undo' event in the ledger so history is preserved.
    """
    # Get all events for this field, most recent first
    conn = get_connection()
    try:
        rows = conn.execute(
            "SELECT * FROM fact_events WHERE client_id = ? AND tax_year = ? AND field_id = ? "
            "ORDER BY event_id DESC LIMIT 2",
            (client_id, tax_year, field_id)
        ).fetchall()
        events = _rows_to_dicts(rows)
    finally:
        conn.close()

    if not events:
        return {"error": "No actions to undo"}

    last_event = events[0]

    # Don't undo an undo
    if last_event["action"] == "undo":
        return {"error": "Last action was already an undo"}

    # Only the same user (or a partner) can undo their own action
    if last_event["actor_user_id"] != actor_user_id and actor_role != "partner":
        return {"error": "Can only undo your own actions"}

    # Restore old value
    old_value = last_event.get("old_value")
    current_value = last_event.get("new_value")

    # Determine what status to restore for the fact
    prev_status = "extracted"
    if last_event["action"] in ("verify",):
        prev_status = "extracted"
    elif last_event["action"] in ("approve",):
        prev_status = "verified"
    elif last_event["action"] in ("override",):
        prev_status = "verified"
    elif last_event["action"] in ("send_back",):
        # Undoing a send-back means restoring the stage it was at before
        prev_status = "verified"

    # Restore the fact value
    set_fact(client_id, tax_year, field_id, old_value, set_by=actor_user_id,
             status=prev_status)

    # Record the undo event
    record_fact_event(client_id, tax_year, field_id, current_value, old_value,
                      actor_user_id, actor_role, "undo",
                      reason=f"Undo {last_event['action']}")

    # Roll back the review_state stage
    rs = get_review_state(client_id, tax_year, field_id)
    if rs:
        current_stage = rs["stage"]
        if last_event["action"] == "send_back":
            # Undoing a send-back: advance stage back to where it was
            # The send_back event's old stage can be inferred from actor_role
            if last_event["actor_role"] == "reviewer":
                restore_stage = "prepared"
                restore_assigned = "susan"
            elif last_event["actor_role"] == "partner":
                restore_stage = "reviewed"
                restore_assigned = "charles"
            else:
                restore_stage = current_stage
                restore_assigned = rs.get("assigned_to", "")
            set_review_stage(client_id, tax_year, field_id, restore_stage,
                             assigned_to=restore_assigned, last_action="undo")
        else:
            # Normal action undo: roll stage back one step
            prev = _STAGE_REVERSE.get(current_stage)
            if prev:
                set_review_stage(client_id, tax_year, field_id, prev["stage"],
                                 assigned_to=prev["assigned_to"], last_action="undo")
            else:
                # Already at earliest stage, just update last_action
                set_review_stage(client_id, tax_year, field_id, current_stage,
                                 assigned_to=rs.get("assigned_to", ""),
                                 last_action="undo")

    log_audit("REVIEW_UNDONE", client_id, tax_year, field_id, actor_user_id,
              {"undone_action": last_event["action"],
               "restored_value": old_value})

    return {"ok": True, "undone_action": last_event["action"], "restored_value": old_value}


def populate_facts_from_extraction(job_id, client_name, tax_year, extractions):
    """After extraction completes, create facts and review_state entries.

    extractions: list of extraction dicts from the output log JSON.
    """
    facts_list = []
    states_list = []
    client_id = client_name or "unknown"
    year = str(tax_year)

    for ext in extractions:
        page = ext.get("_page", ext.get("page_number", 0))
        # Determine extraction index on this page (for field key)
        ext_idx = ext.get("_ext_idx", 0)

        fields = ext.get("fields", {})
        for field_name, field_data in fields.items():
            # Build field_id matching the verification key format
            field_id = f"{job_id}:{page}:{ext_idx}:{field_name}"

            # Get the value
            if isinstance(field_data, dict):
                value = field_data.get("value")
                value_type = "text"
                if isinstance(value, (int, float)):
                    value_type = "number"
            else:
                value = field_data
                value_type = "text"
                if isinstance(value, (int, float)):
                    value_type = "number"

            facts_list.append({
                "client_id": client_id,
                "tax_year": year,
                "field_id": field_id,
                "value": value,
                "value_type": value_type,
                "set_by": "system",
                "evidence_id": job_id,
                "status": "extracted",
            })

            states_list.append({
                "client_id": client_id,
                "tax_year": year,
                "field_id": field_id,
                "stage": "extracted",
                "assigned_to": "jeff",
            })

    if facts_list:
        bulk_set_facts(facts_list)
    if states_list:
        bulk_init_review_states(states_list)

    return len(facts_list)
