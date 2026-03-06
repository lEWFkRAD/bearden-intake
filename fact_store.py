# ============================================================
# HONOR — The Fact Ledger
# "A number is born once, confirmed once, and used many times."
# ============================================================

"""Canonical fact storage for the Bearden Document Intake Platform.

DB-only gateway for extraction facts. Manages the unified `facts` table
in the shared SQLite database. Also maintains the legacy
`client_canonical_values` table for workpaper compatibility.

DESIGN PRINCIPLE: If a number is not in SQLite, it does not exist.
Temporary files and OCR outputs are helpers — not sources of truth.

CHAIN OF CUSTODY:
  1. OCR / Vision / Text Layer produces a candidate.
  2. That candidate is written immediately into SQLite as a fact row.
  3. The review UI reads only from SQLite.
  4. Human verification updates the same SQLite row.
  5. Workpapers and exports read only from SQLite.
  6. Corrected values are locked and never overwritten.

MONOTONIC TRUST HIERARCHY (never downgrade):
  extracted < needs_review < auto_verified < confirmed < corrected
  Once a fact reaches a higher trust level, no automated process can
  push it back down. Corrected facts are permanently locked.

ARCHITECTURAL RULE: This module must NEVER import extract.py, OCR,
vision, or PDF libraries. If a value is missing in the DB, mark it
as needs_review — do not re-extract.

Run:  python3 -c "from fact_store import FactStore; print('OK')"
"""

import sqlite3
import json
import os
from datetime import datetime
from pathlib import Path

# ─── IMPORT GUARDRAIL ─────────────────────────────────────────────────────────
# This module must never import extraction, OCR, vision, or PDF libraries.
_FORBIDDEN_MODULES = frozenset({
    'extract', 'pytesseract', 'anthropic', 'pdf2image',
    'PIL', 'Pillow', 'fitz', 'pdf2image',
})


# ─── STATUS TRUST HIERARCHY ──────────────────────────────────────────────────
# Higher number = higher trust. Never downgrade.
# "pending" is below "extracted" — used for rollforward placeholders awaiting
# new-year extraction. Any extraction result automatically supersedes pending.
STATUS_RANK = {
    "pending":       -1,
    "missing":        0,
    "extracted":      1,
    "needs_review":   2,
    "auto_verified":  3,
    "confirmed":      4,
    "corrected":      5,
}

# Statuses that are considered "locked" — no automated overwrites
LOCKED_STATUSES = frozenset({"corrected"})

# Statuses that should never be downgraded by automation
PROTECTED_STATUSES = frozenset({"confirmed", "corrected"})


# ─── RUNTIME GUARDRAIL ────────────────────────────────────────────────────────

def _reject_raw_inputs(value):
    """Reject PDF paths, image objects, and OCR text dumps.

    The fact store accepts only clean scalar values (numbers, short strings,
    dates, booleans, None). Anything that looks like raw pipeline data is
    rejected before it reaches the database.
    """
    if isinstance(value, (bytes, bytearray)):
        raise ValueError("FactStore rejects binary data (images, PDFs)")
    if isinstance(value, str):
        stripped = value.strip().lower()
        if stripped.endswith('.pdf'):
            raise ValueError("FactStore rejects PDF file paths")
        if len(value) > 5000:
            raise ValueError(
                f"FactStore rejects large text blobs ({len(value)} chars) — "
                "likely OCR output. Store extracted values, not raw text."
            )


# ─── FACT STORE ────────────────────────────────────────────────────────────────

class FactStore:
    """DB-only gateway for the unified facts table.

    Every public method opens and closes its own connection (thread-safe
    pattern matching app.py). The facts table is the single source of truth.

    Usage::

        fs = FactStore("/path/to/bearden.db")

        # Candidate written immediately during extraction
        fs.upsert_candidate_fact("job-001", "Evans, Lisa", 2025,
            "W-2.ein:12-3456789.wages", value_num=85000.00,
            status="extracted", confidence=0.95,
            source_method="ocr", source_doc="evans-w2.pdf", source_page=1)

        # Reviewer confirms
        fs.upgrade_fact_status("job-001", 2025,
            "W-2.ein:12-3456789.wages", "confirmed")

        # Reviewer corrects
        fs.apply_correction("job-001", 2025,
            "W-2.ein:12-3456789.wages", value_num=86000.00)

        # Read back
        fact = fs.get_fact("job-001", 2025, "W-2.ein:12-3456789.wages")
    """

    def __init__(self, db_path):
        self.db_path = str(db_path)
        self._ensure_schema()

    def _conn(self):
        """Create a new SQLite connection with WAL mode."""
        conn = sqlite3.connect(self.db_path, timeout=10)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        return conn

    # ── Schema ────────────────────────────────────────────────────────────────

    def _ensure_schema(self):
        """Create tables and indexes if they don't exist.

        Creates:
          - facts: unified fact table (T1.6.2)
          - client_canonical_values: legacy table (T1.6 compat)
        """
        conn = self._conn()
        try:
            # ── T1.6.2: Unified facts table ──
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
                CREATE INDEX IF NOT EXISTS idx_facts_job
                    ON facts(job_id)
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_facts_client_year
                    ON facts(client_id, tax_year)
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_facts_fact_key
                    ON facts(fact_key)
            """)

            # ── T1.6: Legacy client_canonical_values table (kept for workpaper compat) ──
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
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_ccv_client_year
                    ON client_canonical_values(client_name, year)
            """)
            # T1.6 extension columns — safe to re-run
            for col, col_type in [("evidence_ref", "TEXT DEFAULT ''"),
                                   ("source_doc", "TEXT DEFAULT ''"),
                                   ("page_number", "INTEGER")]:
                try:
                    conn.execute(
                        f"ALTER TABLE client_canonical_values ADD COLUMN {col} {col_type}"
                    )
                except sqlite3.OperationalError:
                    pass  # Column already exists

            conn.commit()
        finally:
            conn.close()

    # ══════════════════════════════════════════════════════════════════════════
    # UNIFIED FACTS TABLE — WRITE OPERATIONS
    # ══════════════════════════════════════════════════════════════════════════

    def upsert_candidate_fact(self, job_id, client_id, tax_year, fact_key,
                               value_num=None, value_text=None,
                               status='extracted', confidence=None,
                               source_method=None, source_doc=None,
                               source_page=None, evidence_ref=None):
        """Write a candidate fact from extraction. Respects lock rules.

        RULES:
          - If existing row has locked=1, do NOT overwrite value or status.
          - If existing status outranks new status (monotonic trust), keep it.
          - If existing status equals new status, update value (re-extraction
            of same trust level is allowed).

        Args:
            job_id: Job identifier
            client_id: Client name/identifier
            tax_year: Tax year (integer)
            fact_key: Canonical key like "W-2.ein:12-3456789.wages"
            value_num: Numeric value (preferred for money fields)
            value_text: Text value (for non-numeric fields like codes, names)
            status: One of STATUS_RANK keys
            confidence: Extraction confidence score (0.0 - 1.0)
            source_method: How the value was extracted (text_layer, ocr, vision, consensus)
            source_doc: Source document filename
            source_page: Page number in source document
            evidence_ref: Bounding box or evidence pointer
        """
        # Validate inputs
        if value_text is not None:
            _reject_raw_inputs(value_text)
        if not fact_key or not job_id:
            raise ValueError("job_id and fact_key are required")

        conn = self._conn()
        try:
            now = datetime.now().isoformat()

            # Check existing row for lock / trust rules
            existing = conn.execute(
                """SELECT status, locked FROM facts
                   WHERE job_id = ? AND tax_year = ? AND fact_key = ?""",
                (job_id, tax_year, fact_key)
            ).fetchone()

            if existing:
                ex_status, ex_locked = existing
                # Rule 1: locked rows are immutable
                if ex_locked:
                    return  # Silently skip — corrected values never overwritten

                # Rule 2: never downgrade status
                new_rank = STATUS_RANK.get(status, 1)
                old_rank = STATUS_RANK.get(ex_status, 1)
                if new_rank < old_rank:
                    return  # Would be a downgrade — skip

                # Rule 3: protected statuses not overwritten by automation
                if ex_status in PROTECTED_STATUSES:
                    return

            # Insert or update
            conn.execute(
                """INSERT INTO facts
                   (job_id, client_id, tax_year, fact_key,
                    value_num, value_text, status, confidence,
                    source_method, source_doc, source_page,
                    evidence_ref, locked, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?)
                   ON CONFLICT(job_id, tax_year, fact_key)
                   DO UPDATE SET
                       value_num = excluded.value_num,
                       value_text = excluded.value_text,
                       status = excluded.status,
                       confidence = excluded.confidence,
                       source_method = excluded.source_method,
                       source_doc = excluded.source_doc,
                       source_page = excluded.source_page,
                       evidence_ref = excluded.evidence_ref,
                       updated_at = excluded.updated_at""",
                (job_id, client_id, tax_year, fact_key,
                 value_num, value_text, status, confidence,
                 source_method, source_doc, source_page,
                 evidence_ref, now)
            )
            conn.commit()
        finally:
            conn.close()

    def upgrade_fact_status(self, job_id, tax_year, fact_key, new_status):
        """Upgrade the trust level of an existing fact. Never downgrades.

        Args:
            job_id: Job identifier
            tax_year: Tax year
            fact_key: Canonical key
            new_status: New status (must outrank or equal current status)

        Returns:
            True if upgraded, False if skipped (would downgrade or row missing).
        """
        if new_status not in STATUS_RANK:
            raise ValueError(f"Unknown status: {new_status!r}")

        conn = self._conn()
        try:
            now = datetime.now().isoformat()

            existing = conn.execute(
                """SELECT status, locked FROM facts
                   WHERE job_id = ? AND tax_year = ? AND fact_key = ?""",
                (job_id, tax_year, fact_key)
            ).fetchone()

            if not existing:
                return False  # Row doesn't exist

            ex_status, ex_locked = existing
            if ex_locked:
                return False  # Locked rows are immutable

            new_rank = STATUS_RANK.get(new_status, 1)
            old_rank = STATUS_RANK.get(ex_status, 1)
            if new_rank <= old_rank:
                return False  # Would be a downgrade or no change

            lock = 1 if new_status in LOCKED_STATUSES else 0
            conn.execute(
                """UPDATE facts SET status = ?, locked = ?, updated_at = ?
                   WHERE job_id = ? AND tax_year = ? AND fact_key = ?""",
                (new_status, lock, now, job_id, tax_year, fact_key)
            )
            conn.commit()
            return True
        finally:
            conn.close()

    def apply_correction(self, job_id, tax_year, fact_key,
                          value_num=None, value_text=None, reviewer=''):
        """Apply a human correction. Sets status='corrected', locked=1.

        A corrected fact is permanently locked — no automated process
        can ever overwrite it.

        Args:
            job_id: Job identifier
            tax_year: Tax year
            fact_key: Canonical key
            value_num: Corrected numeric value
            value_text: Corrected text value
            reviewer: Name of reviewer who made the correction

        Returns:
            True if corrected, False if row doesn't exist.
        """
        if value_text is not None:
            _reject_raw_inputs(value_text)

        conn = self._conn()
        try:
            now = datetime.now().isoformat()

            existing = conn.execute(
                """SELECT id FROM facts
                   WHERE job_id = ? AND tax_year = ? AND fact_key = ?""",
                (job_id, tax_year, fact_key)
            ).fetchone()

            if not existing:
                return False

            conn.execute(
                """UPDATE facts
                   SET value_num = ?, value_text = ?,
                       status = 'corrected', locked = 1,
                       updated_at = ?, evidence_ref = COALESCE(evidence_ref, '')
                   WHERE job_id = ? AND tax_year = ? AND fact_key = ?""",
                (value_num, value_text, now,
                 job_id, tax_year, fact_key)
            )
            conn.commit()
            return True
        finally:
            conn.close()

    # ══════════════════════════════════════════════════════════════════════════
    # UNIFIED FACTS TABLE — READ OPERATIONS
    # ══════════════════════════════════════════════════════════════════════════

    def get_fact(self, job_id, tax_year, fact_key):
        """Get a single fact by its unique key. Returns dict or None."""
        conn = self._conn()
        try:
            row = conn.execute(
                """SELECT id, job_id, client_id, tax_year, fact_key,
                          value_num, value_text, status, confidence,
                          source_method, source_doc, source_page,
                          evidence_ref, locked, updated_at
                   FROM facts
                   WHERE job_id = ? AND tax_year = ? AND fact_key = ?""",
                (job_id, tax_year, fact_key)
            ).fetchone()
            if not row:
                return None
            return self._fact_row_to_dict(row)
        finally:
            conn.close()

    def get_facts_for_job(self, job_id, tax_year=None):
        """Get all facts for a job, optionally filtered by tax_year.

        Returns list of fact dicts sorted by fact_key.
        """
        conn = self._conn()
        try:
            if tax_year is not None:
                rows = conn.execute(
                    """SELECT id, job_id, client_id, tax_year, fact_key,
                              value_num, value_text, status, confidence,
                              source_method, source_doc, source_page,
                              evidence_ref, locked, updated_at
                       FROM facts
                       WHERE job_id = ? AND tax_year = ?
                       ORDER BY fact_key""",
                    (job_id, tax_year)
                ).fetchall()
            else:
                rows = conn.execute(
                    """SELECT id, job_id, client_id, tax_year, fact_key,
                              value_num, value_text, status, confidence,
                              source_method, source_doc, source_page,
                              evidence_ref, locked, updated_at
                       FROM facts
                       WHERE job_id = ?
                       ORDER BY fact_key""",
                    (job_id,)
                ).fetchall()
            return [self._fact_row_to_dict(r) for r in rows]
        finally:
            conn.close()

    def get_facts_for_client(self, client_id, tax_year=None):
        """Get all facts for a client, optionally filtered by tax_year.

        Returns list of fact dicts sorted by (tax_year, fact_key).
        """
        conn = self._conn()
        try:
            if tax_year is not None:
                rows = conn.execute(
                    """SELECT id, job_id, client_id, tax_year, fact_key,
                              value_num, value_text, status, confidence,
                              source_method, source_doc, source_page,
                              evidence_ref, locked, updated_at
                       FROM facts
                       WHERE client_id = ? AND tax_year = ?
                       ORDER BY tax_year, fact_key""",
                    (client_id, tax_year)
                ).fetchall()
            else:
                rows = conn.execute(
                    """SELECT id, job_id, client_id, tax_year, fact_key,
                              value_num, value_text, status, confidence,
                              source_method, source_doc, source_page,
                              evidence_ref, locked, updated_at
                       FROM facts
                       WHERE client_id = ?
                       ORDER BY tax_year, fact_key""",
                    (client_id,)
                ).fetchall()
            return [self._fact_row_to_dict(r) for r in rows]
        finally:
            conn.close()

    def get_review_queue(self, job_id):
        """Get facts that need human review for a job.

        Returns facts with status in ('extracted', 'needs_review')
        that are NOT locked. Sorted by source_page, fact_key.
        """
        conn = self._conn()
        try:
            rows = conn.execute(
                """SELECT id, job_id, client_id, tax_year, fact_key,
                          value_num, value_text, status, confidence,
                          source_method, source_doc, source_page,
                          evidence_ref, locked, updated_at
                   FROM facts
                   WHERE job_id = ? AND locked = 0
                         AND status IN ('extracted', 'needs_review')
                   ORDER BY source_page, fact_key""",
                (job_id,)
            ).fetchall()
            return [self._fact_row_to_dict(r) for r in rows]
        finally:
            conn.close()

    def count_facts(self, job_id, tax_year=None):
        """Count facts for a job, grouped by status.

        Returns dict: {"extracted": N, "confirmed": M, ...}
        """
        conn = self._conn()
        try:
            sql = "SELECT status, COUNT(*) FROM facts WHERE job_id = ?"
            params = [job_id]
            if tax_year is not None:
                sql += " AND tax_year = ?"
                params.append(tax_year)
            sql += " GROUP BY status"
            rows = conn.execute(sql, params).fetchall()
            return {r[0]: r[1] for r in rows}
        finally:
            conn.close()

    # ══════════════════════════════════════════════════════════════════════════
    # ROLLFORWARD — CLONE PRIOR YEAR FACT STRUCTURE INTO NEW YEAR
    # ══════════════════════════════════════════════════════════════════════════

    def rollforward_facts(self, client_id, from_year, to_year, new_job_id):
        """Clone fact structure from prior year into a new year.

        Creates placeholder facts for every fact_key that existed in
        from_year, but with zeroed values and status='pending'. Text
        labels (payer names, entity info) are preserved; numeric values
        are cleared.

        RULES:
          - Only copies fact_keys, not values (numeric fields get NULL)
          - Text-only facts (no value_num) get their value_text preserved
            (entity names, EINs, codes — these rarely change year to year)
          - Status is set to 'pending' (below 'extracted' in trust)
          - Does NOT overwrite if facts already exist in to_year
          - Returns count of facts created

        Args:
            client_id: Client name/identifier
            from_year: Source tax year (int)
            to_year: Target tax year (int)
            new_job_id: Job ID for the new year's facts

        Returns:
            dict: {"created": N, "skipped": M, "total_source": T,
                   "fact_keys": [...]}
        """
        if from_year == to_year:
            raise ValueError("from_year and to_year must be different")

        conn = self._conn()
        try:
            now = datetime.now().isoformat()

            # Fetch all facts from prior year for this client
            source_facts = conn.execute(
                """SELECT fact_key, value_num, value_text, source_method,
                          source_doc, source_page, confidence
                   FROM facts
                   WHERE client_id = ? AND tax_year = ?
                   ORDER BY fact_key""",
                (client_id, from_year)
            ).fetchall()

            if not source_facts:
                return {
                    "created": 0, "skipped": 0,
                    "total_source": 0, "fact_keys": [],
                }

            created = 0
            skipped = 0
            created_keys = []

            for row in source_facts:
                fk, val_num, val_text, method, doc, page, conf = row

                # Check if fact already exists in target year
                existing = conn.execute(
                    """SELECT 1 FROM facts
                       WHERE job_id = ? AND tax_year = ? AND fact_key = ?""",
                    (new_job_id, to_year, fk)
                ).fetchone()

                if existing:
                    skipped += 1
                    continue

                # For rollforward:
                #   - Numeric values → NULL (to be filled by new extraction)
                #   - Text values for structural fields → preserved
                #     (payer names, EINs, codes that carry forward)
                is_structural = val_num is None and val_text is not None
                carry_text = val_text if is_structural else None

                conn.execute(
                    """INSERT INTO facts
                       (job_id, client_id, tax_year, fact_key,
                        value_num, value_text, status, confidence,
                        source_method, source_doc, source_page,
                        evidence_ref, locked, updated_at)
                       VALUES (?, ?, ?, ?, NULL, ?, 'pending', NULL,
                               'rollforward', ?, NULL, NULL, 0, ?)""",
                    (new_job_id, client_id, to_year, fk,
                     carry_text, doc, now)
                )
                created += 1
                created_keys.append(fk)

            conn.commit()
            return {
                "created": created,
                "skipped": skipped,
                "total_source": len(source_facts),
                "fact_keys": created_keys,
            }
        finally:
            conn.close()

    def get_client_years(self, client_id):
        """Get all tax years that have facts for a client.

        Returns list of years sorted descending (most recent first).
        """
        conn = self._conn()
        try:
            rows = conn.execute(
                """SELECT DISTINCT tax_year FROM facts
                   WHERE client_id = ? AND tax_year IS NOT NULL
                   ORDER BY tax_year DESC""",
                (client_id,)
            ).fetchall()
            return [r[0] for r in rows]
        finally:
            conn.close()

    # ══════════════════════════════════════════════════════════════════════════
    # LEGACY CLIENT_CANONICAL_VALUES — FOR WORKPAPER COMPAT (T1.6)
    # ══════════════════════════════════════════════════════════════════════════

    def get_legacy_fact(self, client_name, year, document_type, payer_key, field_name):
        """Get a single fact from the legacy client_canonical_values table.
        Returns dict or None. Used by workpaper_export.py.
        """
        conn = self._conn()
        try:
            row = conn.execute(
                """SELECT canonical_value, original_value, status, payer_display,
                          source_job_id, reviewer, verified_at,
                          evidence_ref, source_doc, page_number
                   FROM client_canonical_values
                   WHERE client_name = ? AND year = ? AND document_type = ?
                         AND payer_key = ? AND field_name = ?""",
                (client_name, year, document_type, payer_key, field_name)
            ).fetchone()
            if not row:
                return None
            return self._legacy_row_to_dict(document_type, payer_key, field_name, row)
        finally:
            conn.close()

    def get_legacy_facts(self, client_name, year, document_type=None, payer_key=None):
        """Get all facts from legacy table for a client/year. Used by workpaper."""
        conn = self._conn()
        try:
            sql = """SELECT document_type, payer_key, field_name,
                            canonical_value, original_value, status, payer_display,
                            source_job_id, reviewer, verified_at,
                            evidence_ref, source_doc, page_number
                     FROM client_canonical_values
                     WHERE client_name = ? AND year = ?"""
            params = [client_name, year]
            if document_type:
                sql += " AND document_type = ?"
                params.append(document_type)
            if payer_key:
                sql += " AND payer_key = ?"
                params.append(payer_key)
            sql += " ORDER BY document_type, payer_key, field_name"

            rows = conn.execute(sql, params).fetchall()
            return [
                self._legacy_row_to_dict(r[0], r[1], r[2], r[3:])
                for r in rows
            ]
        finally:
            conn.close()

    def list_legacy_facts(self, client_name, year):
        """List all fact keys from legacy table for a client/year."""
        conn = self._conn()
        try:
            rows = conn.execute(
                """SELECT document_type, payer_key, field_name, status
                   FROM client_canonical_values
                   WHERE client_name = ? AND year = ?
                   ORDER BY document_type, payer_key, field_name""",
                (client_name, year)
            ).fetchall()
            return [(r[0], r[1], r[2], r[3]) for r in rows]
        finally:
            conn.close()

    def upsert_legacy_fact(self, client_name, year, document_type, payer_key, field_name,
                            canonical_value, original_value=None, status='extracted',
                            source_job_id='', reviewer='', payer_display='',
                            evidence_ref='', source_doc='', page_number=None):
        """Write or update a single fact in the legacy table.
        Used for workpaper compatibility.
        """
        _reject_raw_inputs(canonical_value)
        if original_value is not None:
            _reject_raw_inputs(original_value)

        conn = self._conn()
        try:
            now = datetime.now().isoformat()
            conn.execute(
                """INSERT INTO client_canonical_values
                   (client_name, year, document_type, payer_key, payer_display,
                    field_name, canonical_value, original_value, status,
                    source_job_id, reviewer, verified_at,
                    evidence_ref, source_doc, page_number)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(client_name, year, document_type, payer_key, field_name)
                   DO UPDATE SET
                       canonical_value = excluded.canonical_value,
                       original_value = excluded.original_value,
                       payer_display = excluded.payer_display,
                       status = excluded.status,
                       source_job_id = excluded.source_job_id,
                       reviewer = excluded.reviewer,
                       verified_at = excluded.verified_at,
                       evidence_ref = excluded.evidence_ref,
                       source_doc = excluded.source_doc,
                       page_number = excluded.page_number""",
                (client_name, year, document_type, payer_key, payer_display,
                 field_name,
                 json.dumps(canonical_value) if canonical_value is not None else None,
                 json.dumps(original_value) if original_value is not None else None,
                 status, source_job_id, reviewer, now,
                 evidence_ref, source_doc, page_number)
            )
            conn.commit()
        finally:
            conn.close()

    # ══════════════════════════════════════════════════════════════════════════
    # SYNC: facts → client_canonical_values
    # ══════════════════════════════════════════════════════════════════════════

    def sync_to_legacy(self, job_id, client_name, year):
        """Promote facts from the unified table to the legacy table.

        Called after extraction completion or verification. Writes the current
        best value for each fact to client_canonical_values so workpapers can
        read them.

        Only facts with status >= extracted are synced. Locked legacy facts
        (status='corrected') are not overwritten.
        """
        conn = self._conn()
        try:
            rows = conn.execute(
                """SELECT fact_key, value_num, value_text, status,
                          source_method, source_doc, source_page,
                          evidence_ref, confidence, job_id
                   FROM facts
                   WHERE job_id = ? AND client_id = ?
                   ORDER BY fact_key""",
                (job_id, client_name)
            ).fetchall()

            now = datetime.now().isoformat()
            for row in rows:
                fact_key, val_num, val_text, status = row[0], row[1], row[2], row[3]
                source_method = row[4] or ''
                source_doc = row[5] or ''
                source_page = row[6]
                evidence_ref = row[7] or ''
                src_job = row[9] or job_id

                # Parse fact_key → document_type, payer_key, field_name
                parts = fact_key.split(".", 2)
                if len(parts) != 3:
                    continue
                doc_type, payer_key, field_name = parts

                # Determine canonical value (prefer numeric)
                canonical = val_num if val_num is not None else val_text

                # Check if legacy row is already corrected — don't overwrite
                existing = conn.execute(
                    """SELECT status FROM client_canonical_values
                       WHERE client_name = ? AND year = ? AND document_type = ?
                             AND payer_key = ? AND field_name = ?""",
                    (client_name, year, doc_type, payer_key, field_name)
                ).fetchone()
                if existing and existing[0] in ("corrected", "confirmed"):
                    continue

                conn.execute(
                    """INSERT INTO client_canonical_values
                       (client_name, year, document_type, payer_key, payer_display,
                        field_name, canonical_value, original_value, status,
                        source_job_id, reviewer, verified_at,
                        evidence_ref, source_doc, page_number)
                       VALUES (?, ?, ?, ?, '', ?, ?, ?, ?, ?, '', ?, ?, ?, ?)
                       ON CONFLICT(client_name, year, document_type, payer_key, field_name)
                       DO UPDATE SET
                           canonical_value = excluded.canonical_value,
                           original_value = excluded.original_value,
                           status = excluded.status,
                           source_job_id = excluded.source_job_id,
                           verified_at = excluded.verified_at,
                           evidence_ref = excluded.evidence_ref,
                           source_doc = excluded.source_doc,
                           page_number = excluded.page_number""",
                    (client_name, year, doc_type, payer_key,
                     field_name,
                     json.dumps(canonical) if canonical is not None else None,
                     json.dumps(canonical) if canonical is not None else None,
                     status, src_job, now,
                     evidence_ref, source_doc, source_page)
                )
            conn.commit()
        finally:
            conn.close()

    # ══════════════════════════════════════════════════════════════════════════
    # HELPERS
    # ══════════════════════════════════════════════════════════════════════════

    @staticmethod
    def fact_key(document_type, payer_key, field_name):
        """Build a canonical fact key string.

        Format: "{document_type}.{payer_key}.{field_name}"
        Example: "W-2.ein:12-3456789.wages"
        """
        return f"{document_type}.{payer_key}.{field_name}"

    def _fact_row_to_dict(self, row):
        """Convert a unified facts table row to a dict."""
        return {
            "id": row[0],
            "job_id": row[1],
            "client_id": row[2],
            "tax_year": row[3],
            "fact_key": row[4],
            "value_num": row[5],
            "value_text": row[6],
            "status": row[7],
            "confidence": row[8],
            "source_method": row[9],
            "source_doc": row[10],
            "source_page": row[11],
            "evidence_ref": row[12],
            "locked": bool(row[13]),
            "updated_at": row[14],
        }

    def _legacy_row_to_dict(self, document_type, payer_key, field_name, row):
        """Convert a legacy client_canonical_values row to a fact dict."""
        canon_json, orig_json = row[0], row[1]
        canonical = None
        if canon_json is not None:
            try:
                canonical = json.loads(canon_json)
            except (json.JSONDecodeError, ValueError):
                canonical = canon_json
        original = None
        if orig_json is not None:
            try:
                original = json.loads(orig_json)
            except (json.JSONDecodeError, ValueError):
                original = orig_json

        return {
            "fact_key": self.fact_key(document_type, payer_key, field_name),
            "document_type": document_type,
            "payer_key": payer_key,
            "field_name": field_name,
            "canonical_value": canonical,
            "original_value": original,
            "status": row[2],
            "payer_display": row[3],
            "source_job_id": row[4],
            "reviewer": row[5],
            "verified_at": row[6],
            "evidence_ref": row[7] if len(row) > 7 else "",
            "source_doc": row[8] if len(row) > 8 else "",
            "page_number": row[9] if len(row) > 9 else None,
        }

# "Honor is dead, but I'll see what I can do."
