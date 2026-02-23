# ============================================================
# HONOR — Transaction Ledger Extension
# ============================================================

"""
Transaction Store — T-TXN-LEDGER-1
===================================
DB-only gateway for the transaction ledger tables.

Every public method opens and closes its own SQLite connection (thread-safe).
Mirrors the FactStore pattern from fact_store.py.

ARCHITECTURAL RULE: This module must NEVER import extract.py, OCR,
vision, or PDF libraries. It reads only from its own SQLite tables.
"""

import hashlib
import json
import re
import sqlite3
from datetime import datetime

# ── Forbidden-module guardrail ───────────────────────────────────────────────
_FORBIDDEN_MODULES = frozenset({
    'extract', 'pytesseract', 'anthropic', 'pdf2image',
    'PIL', 'Pillow', 'fitz',
})

# ── Category Taxonomy ────────────────────────────────────────────────────────

CATEGORY_TAXONOMY = {
    "Income": [
        "Sales/Revenue", "Interest Income", "Dividend Income",
        "Other Income", "Owner Contribution",
    ],
    "COGS": [
        "Inventory Purchases", "Raw Materials", "Freight/Shipping In",
    ],
    "Operating Expenses > Administrative": [
        "Office Supplies", "Software & Subscriptions", "Professional Fees",
        "Bank Fees", "Merchant Fees",
    ],
    "Operating Expenses > Facilities": [
        "Rent/Lease", "Utilities", "Internet/Phone", "Repairs & Maintenance",
    ],
    "Operating Expenses > Personnel": [
        "Payroll", "Payroll Taxes", "Benefits",
    ],
    "Operating Expenses > Sales & Marketing": [
        "Advertising", "Marketing Tools", "Website/Hosting",
    ],
    "Operating Expenses > Travel & Meals": [
        "Travel", "Meals", "Mileage/Fuel",
    ],
    "Financial": [
        "Loan Payments \u2013 Principal", "Loan Payments \u2013 Interest",
        "Credit Card Payments", "Taxes Paid",
    ],
    "Transfers/Equity": [
        "Owner Draw", "Owner Contribution/Investment", "Internal Transfer",
    ],
    "Personal/Misc": [
        "Personal Expense", "Uncategorized",
    ],
}

# Flat list for validation + reverse map
ALL_TXN_CATEGORIES = []
CATEGORY_TO_GROUP = {}
for _group_name, _cats in CATEGORY_TAXONOMY.items():
    for _cat in _cats:
        ALL_TXN_CATEGORIES.append(_cat)
        CATEGORY_TO_GROUP[_cat] = _group_name

# ── Status Hierarchy ─────────────────────────────────────────────────────────

TXN_STATUS_RANK = {
    "staged":     1,   # Auto-ingested from extraction
    "suggested":  2,   # Category suggested by rules engine (not yet confirmed)
    "verified":   3,   # Human confirmed value + category
    "corrected":  4,   # Human corrected value (permanently locked)
    "overridden": 5,   # Human override with notes (permanently locked)
}

LOCKED_TXN_STATUSES = frozenset({"corrected", "overridden"})

# ── Vendor Normalization ─────────────────────────────────────────────────────

def normalize_vendor(desc):
    """Normalize a vendor/payee name for matching.

    'GEORGIA POWER COMPANY #12345' → 'GEORGIA POWER'
    'WAL-MART SUPER CENTER 0423' → 'WAL-MART SUPER CENTER'

    Mirrors app.py _normalize_vendor() logic.
    """
    if not desc:
        return ""
    s = str(desc).upper().strip()
    # Strip trailing reference/store numbers
    s = re.sub(r'[\s#*]+\d{2,}$', '', s)
    # Strip common suffixes
    s = re.sub(
        r'\s+(LLC|INC|CORP|CO|COMPANY|LTD|LP|NA|N\.A\.)\.?\s*$',
        '', s, flags=re.IGNORECASE,
    )
    # Strip trailing punctuation
    s = s.rstrip(' .,;:*#-')
    return s.strip()


# ═════════════════════════════════════════════════════════════════════════════
# TransactionStore
# ═════════════════════════════════════════════════════════════════════════════

class TransactionStore:
    """DB-only gateway for the transaction ledger tables.

    Usage:
        ts = TransactionStore("/path/to/bearden.db")
        ts.ingest_from_extraction(job_id, log_data, "Client Name", 2025)
        uncategorized = ts.get_uncategorized("Client Name", 2025)
    """

    def __init__(self, db_path):
        self.db_path = str(db_path)
        self._ensure_schema()

    def _conn(self):
        """Create a new SQLite connection with WAL mode."""
        conn = sqlite3.connect(self.db_path, timeout=10)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.row_factory = sqlite3.Row
        return conn

    def _ensure_schema(self):
        """Create all txn_* tables if they don't exist."""
        conn = self._conn()
        try:
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS txn_values (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    txn_id TEXT NOT NULL UNIQUE,
                    job_id TEXT NOT NULL,
                    client_name TEXT NOT NULL,
                    year INTEGER NOT NULL,
                    document_type TEXT NOT NULL,
                    payer_key TEXT NOT NULL DEFAULT '',
                    txn_index INTEGER NOT NULL,
                    txn_date TEXT DEFAULT '',
                    description TEXT DEFAULT '',
                    amount REAL,
                    txn_type TEXT DEFAULT '',
                    category TEXT DEFAULT '',
                    category_group TEXT DEFAULT '',
                    vendor_norm TEXT DEFAULT '',
                    status TEXT NOT NULL DEFAULT 'staged',
                    source_page INTEGER,
                    confidence TEXT DEFAULT '',
                    evidence_ref TEXT DEFAULT '',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_txn_values_job
                    ON txn_values(job_id);
                CREATE INDEX IF NOT EXISTS idx_txn_values_client_year
                    ON txn_values(client_name, year);
                CREATE INDEX IF NOT EXISTS idx_txn_values_status
                    ON txn_values(status);
                CREATE INDEX IF NOT EXISTS idx_txn_values_category
                    ON txn_values(category);
                CREATE INDEX IF NOT EXISTS idx_txn_values_vendor
                    ON txn_values(vendor_norm);
                CREATE INDEX IF NOT EXISTS idx_txn_values_date
                    ON txn_values(txn_date);

                CREATE TABLE IF NOT EXISTS txn_events (
                    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    txn_id TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    old_value TEXT DEFAULT '',
                    new_value TEXT DEFAULT '',
                    reviewer TEXT DEFAULT '',
                    event_at TEXT NOT NULL,
                    details_json TEXT DEFAULT ''
                );
                CREATE INDEX IF NOT EXISTS idx_txn_events_txn
                    ON txn_events(txn_id);
                CREATE INDEX IF NOT EXISTS idx_txn_events_type
                    ON txn_events(event_type);

                CREATE TABLE IF NOT EXISTS txn_evidence (
                    evidence_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    txn_id TEXT NOT NULL,
                    job_id TEXT NOT NULL,
                    page_number INTEGER,
                    crop_coords TEXT DEFAULT '',
                    ocr_text TEXT DEFAULT ''
                );
                CREATE INDEX IF NOT EXISTS idx_txn_evidence_txn
                    ON txn_evidence(txn_id);

                CREATE TABLE IF NOT EXISTS vendor_rules (
                    rule_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    vendor_pattern TEXT NOT NULL,
                    match_type TEXT NOT NULL DEFAULT 'exact',
                    category TEXT NOT NULL,
                    category_group TEXT DEFAULT '',
                    source TEXT NOT NULL DEFAULT 'manual',
                    confidence REAL DEFAULT 1.0,
                    usage_count INTEGER DEFAULT 0,
                    created_by TEXT DEFAULT '',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE UNIQUE INDEX IF NOT EXISTS idx_vendor_rules_unique
                    ON vendor_rules(vendor_pattern, match_type);
                CREATE INDEX IF NOT EXISTS idx_vendor_rules_pattern
                    ON vendor_rules(vendor_pattern);
                CREATE INDEX IF NOT EXISTS idx_vendor_rules_category
                    ON vendor_rules(category);

                CREATE TABLE IF NOT EXISTS category_rules (
                    rule_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    keyword TEXT NOT NULL,
                    category TEXT NOT NULL,
                    category_group TEXT DEFAULT '',
                    priority INTEGER DEFAULT 100,
                    source TEXT NOT NULL DEFAULT 'manual',
                    created_by TEXT DEFAULT '',
                    created_at TEXT NOT NULL
                );
                CREATE UNIQUE INDEX IF NOT EXISTS idx_category_rules_unique
                    ON category_rules(keyword, category);
                CREATE INDEX IF NOT EXISTS idx_category_rules_keyword
                    ON category_rules(keyword);
            """)
        finally:
            conn.close()

    # ── Static Helpers ───────────────────────────────────────────────────────

    @staticmethod
    def make_txn_id(job_id, document_type, payer_key, txn_index):
        """Deterministic transaction ID. Survives re-extraction.

        Format: first 16 hex chars of SHA-256 hash.
        """
        raw = f"{job_id}|{document_type}|{payer_key}|{txn_index}"
        return hashlib.sha256(raw.encode()).hexdigest()[:16]

    # ── Write Operations ─────────────────────────────────────────────────────

    def ingest_from_extraction(self, job_id, log_data, client_name, year):
        """Parse txn_N_* fields from extraction log, dedup, insert into txn_values.

        Args:
            job_id: Job identifier
            log_data: Parsed JSON extraction log (dict with "extractions" key)
            client_name: Client name string
            year: Tax year (int)

        Returns:
            dict: {"inserted": N, "skipped_dup": M, "total_parsed": T}
        """
        from transaction_extract import parse_transactions_from_log

        now = datetime.now().isoformat()
        inserted = 0
        skipped_dup = 0
        total_parsed = 0

        conn = self._conn()
        try:
            for txn in parse_transactions_from_log(log_data):
                total_parsed += 1
                payer_key = normalize_vendor(txn.get("payer_entity", ""))
                txn_id = self.make_txn_id(
                    job_id, txn["document_type"], payer_key, txn["txn_index"]
                )

                # Check for existing (dedup)
                existing = conn.execute(
                    "SELECT txn_id FROM txn_values WHERE txn_id = ?",
                    (txn_id,),
                ).fetchone()
                if existing:
                    skipped_dup += 1
                    continue

                vendor_norm = normalize_vendor(txn.get("description", ""))

                conn.execute(
                    """INSERT INTO txn_values
                       (txn_id, job_id, client_name, year, document_type,
                        payer_key, txn_index, txn_date, description, amount,
                        txn_type, category, category_group, vendor_norm,
                        status, source_page, confidence, created_at, updated_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'staged', ?, ?, ?, ?)""",
                    (txn_id, job_id, client_name, int(year),
                     txn["document_type"], payer_key, txn["txn_index"],
                     txn.get("txn_date", ""), txn.get("description", ""),
                     txn.get("amount"), txn.get("txn_type", ""),
                     txn.get("category", ""),
                     CATEGORY_TO_GROUP.get(txn.get("category", ""), ""),
                     vendor_norm, txn.get("source_page"),
                     txn.get("confidence", ""), now, now),
                )

                # Audit event
                conn.execute(
                    """INSERT INTO txn_events
                       (txn_id, event_type, new_value, event_at, details_json)
                       VALUES (?, 'staged', ?, ?, ?)""",
                    (txn_id, json.dumps({"amount": txn.get("amount"),
                                         "description": txn.get("description", "")}),
                     now,
                     json.dumps({"job_id": job_id,
                                 "document_type": txn["document_type"]})),
                )
                inserted += 1

            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

        return {"inserted": inserted, "skipped_dup": skipped_dup,
                "total_parsed": total_parsed}

    def categorize(self, txn_id, category, reviewer=""):
        """Set category on a transaction. Records txn_events(category_set).

        Does NOT change status to verified — that requires explicit verify().
        Sets status to 'suggested' if currently 'staged'.

        Returns:
            True if updated, False if txn not found or locked.
        """
        if category and category not in ALL_TXN_CATEGORIES:
            return False

        group = CATEGORY_TO_GROUP.get(category, "")
        now = datetime.now().isoformat()
        conn = self._conn()
        try:
            row = conn.execute(
                "SELECT txn_id, category, status FROM txn_values WHERE txn_id = ?",
                (txn_id,),
            ).fetchone()
            if not row:
                return False
            if row["status"] in LOCKED_TXN_STATUSES:
                return False

            old_cat = row["category"] or ""

            # Upgrade status: staged → suggested (but not if already verified+)
            new_status = row["status"]
            if row["status"] == "staged":
                new_status = "suggested"

            conn.execute(
                """UPDATE txn_values
                   SET category = ?, category_group = ?, status = ?, updated_at = ?
                   WHERE txn_id = ?""",
                (category, group, new_status, now, txn_id),
            )

            conn.execute(
                """INSERT INTO txn_events
                   (txn_id, event_type, old_value, new_value, reviewer, event_at)
                   VALUES (?, 'category_set', ?, ?, ?, ?)""",
                (txn_id, old_cat, category, reviewer, now),
            )
            conn.commit()
            return True
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def verify(self, txn_id, reviewer=""):
        """Mark transaction as verified. Requires category to be set.

        Returns:
            True if verified, False if txn not found, locked, or uncategorized.
        """
        now = datetime.now().isoformat()
        conn = self._conn()
        try:
            row = conn.execute(
                "SELECT txn_id, category, status FROM txn_values WHERE txn_id = ?",
                (txn_id,),
            ).fetchone()
            if not row:
                return False
            if row["status"] in LOCKED_TXN_STATUSES:
                return False
            if not row["category"]:
                return False  # Must have a category to verify

            conn.execute(
                """UPDATE txn_values SET status = 'verified', updated_at = ?
                   WHERE txn_id = ?""",
                (now, txn_id),
            )
            conn.execute(
                """INSERT INTO txn_events
                   (txn_id, event_type, old_value, new_value, reviewer, event_at)
                   VALUES (?, 'verified', ?, 'verified', ?, ?)""",
                (txn_id, row["status"], reviewer, now),
            )
            conn.commit()
            return True
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def correct(self, txn_id, corrections, reviewer=""):
        """Apply human corrections to transaction fields.

        corrections: dict with any of {amount, txn_date, description, category, txn_type}
        Sets status='corrected', locked.

        Returns:
            True if corrected, False if txn not found.
        """
        if not corrections:
            return False

        now = datetime.now().isoformat()
        conn = self._conn()
        try:
            row = conn.execute(
                "SELECT * FROM txn_values WHERE txn_id = ?",
                (txn_id,),
            ).fetchone()
            if not row:
                return False

            old_values = {}
            new_values = {}
            update_parts = []
            update_vals = []

            for field in ("amount", "txn_date", "description", "category", "txn_type"):
                if field in corrections:
                    old_values[field] = row[field]
                    new_values[field] = corrections[field]
                    update_parts.append(f"{field} = ?")
                    update_vals.append(corrections[field])

            if not update_parts:
                return False

            # If category changed, update group too
            if "category" in corrections:
                cat = corrections["category"]
                group = CATEGORY_TO_GROUP.get(cat, "")
                update_parts.append("category_group = ?")
                update_vals.append(group)

            # If description changed, update vendor_norm
            if "description" in corrections:
                update_parts.append("vendor_norm = ?")
                update_vals.append(normalize_vendor(corrections["description"]))

            update_parts.append("status = ?")
            update_vals.append("corrected")
            update_parts.append("updated_at = ?")
            update_vals.append(now)
            update_vals.append(txn_id)

            conn.execute(
                f"UPDATE txn_values SET {', '.join(update_parts)} WHERE txn_id = ?",
                update_vals,
            )

            conn.execute(
                """INSERT INTO txn_events
                   (txn_id, event_type, old_value, new_value, reviewer, event_at, details_json)
                   VALUES (?, 'corrected', ?, ?, ?, ?, ?)""",
                (txn_id, json.dumps(old_values), json.dumps(new_values),
                 reviewer, now, json.dumps(corrections)),
            )
            conn.commit()
            return True
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def bulk_categorize(self, txn_ids, category, reviewer=""):
        """Apply same category to multiple transactions. Returns count updated."""
        if not txn_ids:
            return 0
        if category and category not in ALL_TXN_CATEGORIES:
            return 0

        group = CATEGORY_TO_GROUP.get(category, "")
        now = datetime.now().isoformat()
        updated = 0
        conn = self._conn()
        try:
            for txn_id in txn_ids:
                row = conn.execute(
                    "SELECT txn_id, category, status FROM txn_values WHERE txn_id = ?",
                    (txn_id,),
                ).fetchone()
                if not row or row["status"] in LOCKED_TXN_STATUSES:
                    continue

                old_cat = row["category"] or ""
                new_status = row["status"]
                if row["status"] == "staged":
                    new_status = "suggested"

                conn.execute(
                    """UPDATE txn_values
                       SET category = ?, category_group = ?, status = ?, updated_at = ?
                       WHERE txn_id = ?""",
                    (category, group, new_status, now, txn_id),
                )
                conn.execute(
                    """INSERT INTO txn_events
                       (txn_id, event_type, old_value, new_value, reviewer, event_at)
                       VALUES (?, 'category_set', ?, ?, ?, ?)""",
                    (txn_id, old_cat, category, reviewer, now),
                )
                updated += 1

            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()
        return updated

    def apply_vendor_rules(self, client_name=None, year=None):
        """Run the rules engine on staged/uncategorized transactions.

        Rule hierarchy:
          1. Exact vendor_rules match (match_type='exact')
          2. Prefix vendor_rules match (match_type='prefix')
          3. Contains vendor_rules match (match_type='contains')
          4. Keyword category_rules match

        Sets category + status='suggested' (never 'verified').
        Skips locked transactions.

        Returns:
            dict: {"matched": N, "already_categorized": M, "unmatched": U}
        """
        now = datetime.now().isoformat()
        conn = self._conn()
        try:
            # Load rules
            exact_rules = {}
            prefix_rules = {}
            contains_rules = {}
            for row in conn.execute(
                "SELECT vendor_pattern, match_type, category, category_group, rule_id "
                "FROM vendor_rules ORDER BY usage_count DESC"
            ).fetchall():
                if row["match_type"] == "exact":
                    exact_rules[row["vendor_pattern"].upper()] = row
                elif row["match_type"] == "prefix":
                    prefix_rules[row["vendor_pattern"].upper()] = row
                elif row["match_type"] == "contains":
                    contains_rules[row["vendor_pattern"].upper()] = row

            keyword_rules = conn.execute(
                "SELECT keyword, category, category_group, rule_id "
                "FROM category_rules ORDER BY priority ASC"
            ).fetchall()

            # Get uncategorized transactions
            query = """SELECT txn_id, vendor_norm, description, status
                       FROM txn_values
                       WHERE status NOT IN ('corrected', 'overridden')
                         AND (category = '' OR category IS NULL)"""
            params = []
            if client_name:
                query += " AND client_name = ?"
                params.append(client_name)
            if year:
                query += " AND year = ?"
                params.append(int(year))

            rows = conn.execute(query, params).fetchall()

            matched = 0
            already_categorized = 0
            unmatched = 0

            for row in rows:
                txn_id = row["txn_id"]
                vendor = (row["vendor_norm"] or "").upper()
                desc = (row["description"] or "").upper()

                # Try exact match
                rule = exact_rules.get(vendor)
                if not rule:
                    # Try prefix match
                    for pattern, r in prefix_rules.items():
                        if vendor.startswith(pattern):
                            rule = r
                            break
                if not rule:
                    # Try contains match
                    for pattern, r in contains_rules.items():
                        if pattern in vendor or pattern in desc:
                            rule = r
                            break
                if not rule:
                    # Try keyword match
                    for kr in keyword_rules:
                        kw = kr["keyword"].upper()
                        if kw in desc or kw in vendor:
                            rule = kr
                            break

                if rule:
                    cat = rule["category"]
                    grp = rule["category_group"] if "category_group" in rule.keys() else CATEGORY_TO_GROUP.get(cat, "")
                    new_status = "suggested" if row["status"] == "staged" else row["status"]

                    conn.execute(
                        """UPDATE txn_values
                           SET category = ?, category_group = ?, status = ?, updated_at = ?
                           WHERE txn_id = ?""",
                        (cat, grp, new_status, now, txn_id),
                    )
                    conn.execute(
                        """INSERT INTO txn_events
                           (txn_id, event_type, old_value, new_value, event_at, details_json)
                           VALUES (?, 'category_set', '', ?, ?, ?)""",
                        (txn_id, cat, now,
                         json.dumps({"source": "rules_engine",
                                     "rule_id": rule["rule_id"] if "rule_id" in rule.keys() else None})),
                    )
                    # Increment usage count for vendor rules
                    if "rule_id" in rule.keys():
                        conn.execute(
                            "UPDATE vendor_rules SET usage_count = usage_count + 1, updated_at = ? WHERE rule_id = ?",
                            (now, rule["rule_id"]),
                        )
                    matched += 1
                else:
                    unmatched += 1

            conn.commit()
            return {"matched": matched, "already_categorized": already_categorized,
                    "unmatched": unmatched}
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def add_vendor_rule(self, vendor_pattern, match_type, category, created_by=""):
        """Create or update a vendor rule. Returns rule_id."""
        if match_type not in ("exact", "prefix", "contains"):
            raise ValueError(f"Invalid match_type: {match_type}")
        if category and category not in ALL_TXN_CATEGORIES:
            raise ValueError(f"Invalid category: {category}")

        group = CATEGORY_TO_GROUP.get(category, "")
        now = datetime.now().isoformat()
        norm_pattern = vendor_pattern.upper().strip()

        conn = self._conn()
        try:
            conn.execute(
                """INSERT INTO vendor_rules
                   (vendor_pattern, match_type, category, category_group, source,
                    created_by, created_at, updated_at)
                   VALUES (?, ?, ?, ?, 'manual', ?, ?, ?)
                   ON CONFLICT(vendor_pattern, match_type) DO UPDATE SET
                       category = excluded.category,
                       category_group = excluded.category_group,
                       updated_at = excluded.updated_at""",
                (norm_pattern, match_type, category, group, created_by, now, now),
            )
            conn.commit()
            row = conn.execute(
                "SELECT rule_id FROM vendor_rules WHERE vendor_pattern = ? AND match_type = ?",
                (norm_pattern, match_type),
            ).fetchone()
            return row["rule_id"] if row else None
        finally:
            conn.close()

    def add_category_rule(self, keyword, category, priority=100, created_by=""):
        """Create or update a category rule. Returns rule_id."""
        if category and category not in ALL_TXN_CATEGORIES:
            raise ValueError(f"Invalid category: {category}")

        group = CATEGORY_TO_GROUP.get(category, "")
        now = datetime.now().isoformat()
        norm_keyword = keyword.upper().strip()

        conn = self._conn()
        try:
            conn.execute(
                """INSERT INTO category_rules
                   (keyword, category, category_group, priority, source,
                    created_by, created_at)
                   VALUES (?, ?, ?, ?, 'manual', ?, ?)
                   ON CONFLICT(keyword, category) DO UPDATE SET
                       priority = excluded.priority,
                       category_group = excluded.category_group""",
                (norm_keyword, category, group, priority, created_by, now),
            )
            conn.commit()
            row = conn.execute(
                "SELECT rule_id FROM category_rules WHERE keyword = ? AND category = ?",
                (norm_keyword, category),
            ).fetchone()
            return row["rule_id"] if row else None
        finally:
            conn.close()

    def delete_vendor_rule(self, rule_id):
        """Delete a vendor rule. Returns True if deleted."""
        conn = self._conn()
        try:
            cursor = conn.execute(
                "DELETE FROM vendor_rules WHERE rule_id = ?", (rule_id,)
            )
            conn.commit()
            return cursor.rowcount > 0
        finally:
            conn.close()

    def delete_category_rule(self, rule_id):
        """Delete a category rule. Returns True if deleted."""
        conn = self._conn()
        try:
            cursor = conn.execute(
                "DELETE FROM category_rules WHERE rule_id = ?", (rule_id,)
            )
            conn.commit()
            return cursor.rowcount > 0
        finally:
            conn.close()

    def learn_vendor_rule(self, vendor_norm, category, source="learned"):
        """Upsert a vendor rule from a categorization action.

        Increments usage_count if rule already exists.
        """
        if not vendor_norm or not category:
            return
        group = CATEGORY_TO_GROUP.get(category, "")
        now = datetime.now().isoformat()
        norm = vendor_norm.upper().strip()

        conn = self._conn()
        try:
            existing = conn.execute(
                "SELECT rule_id, usage_count FROM vendor_rules WHERE vendor_pattern = ? AND match_type = 'exact'",
                (norm,),
            ).fetchone()
            if existing:
                conn.execute(
                    """UPDATE vendor_rules
                       SET category = ?, category_group = ?, usage_count = ?,
                           source = ?, updated_at = ?
                       WHERE rule_id = ?""",
                    (category, group, existing["usage_count"] + 1,
                     source, now, existing["rule_id"]),
                )
            else:
                conn.execute(
                    """INSERT INTO vendor_rules
                       (vendor_pattern, match_type, category, category_group,
                        source, usage_count, created_at, updated_at)
                       VALUES (?, 'exact', ?, ?, ?, 1, ?, ?)""",
                    (norm, category, group, source, now, now),
                )
            conn.commit()
        finally:
            conn.close()

    # ── Read Operations ──────────────────────────────────────────────────────

    def get_transaction(self, txn_id):
        """Get a single transaction. Returns dict or None."""
        conn = self._conn()
        try:
            row = conn.execute(
                "SELECT * FROM txn_values WHERE txn_id = ?", (txn_id,),
            ).fetchone()
            return dict(row) if row else None
        finally:
            conn.close()

    def get_transactions(self, client_name, year, filters=None, page=1, per_page=100):
        """Query transactions with pagination and filters.

        filters: dict with optional keys:
          - status: str or list of str
          - category: str
          - category_group: str
          - vendor_norm: str
          - month: int (1-12)
          - txn_type: str
          - search: str (LIKE match on description)
          - date_from: str (YYYY-MM-DD or MM/DD/YYYY)
          - date_to: str

        Returns:
            {"items": [...], "total": N, "page": P, "per_page": PP, "pages": TP}
        """
        filters = filters or {}
        where = ["client_name = ?", "year = ?"]
        params = [client_name, int(year)]

        if "status" in filters:
            s = filters["status"]
            if isinstance(s, list):
                placeholders = ", ".join("?" * len(s))
                where.append(f"status IN ({placeholders})")
                params.extend(s)
            else:
                where.append("status = ?")
                params.append(s)

        if "category" in filters and filters["category"]:
            where.append("category = ?")
            params.append(filters["category"])

        if "category_group" in filters and filters["category_group"]:
            where.append("category_group = ?")
            params.append(filters["category_group"])

        if "vendor_norm" in filters and filters["vendor_norm"]:
            where.append("vendor_norm = ?")
            params.append(filters["vendor_norm"])

        if "txn_type" in filters and filters["txn_type"]:
            where.append("txn_type = ?")
            params.append(filters["txn_type"])

        if "month" in filters and filters["month"]:
            m = int(filters["month"])
            month_str = f"{m:02d}"
            # Match MM/DD or YYYY-MM
            where.append("(substr(txn_date, 1, 2) = ? OR substr(txn_date, 6, 2) = ?)")
            params.extend([month_str, month_str])

        if "search" in filters and filters["search"]:
            where.append("description LIKE ?")
            params.append(f"%{filters['search']}%")

        if "date_from" in filters and filters["date_from"]:
            where.append("txn_date >= ?")
            params.append(filters["date_from"])

        if "date_to" in filters and filters["date_to"]:
            where.append("txn_date <= ?")
            params.append(filters["date_to"])

        where_clause = " AND ".join(where)

        conn = self._conn()
        try:
            # Count
            count_row = conn.execute(
                f"SELECT COUNT(*) as cnt FROM txn_values WHERE {where_clause}",
                params,
            ).fetchone()
            total = count_row["cnt"]

            # Paginate
            offset = (page - 1) * per_page
            rows = conn.execute(
                f"SELECT * FROM txn_values WHERE {where_clause} ORDER BY txn_date, id LIMIT ? OFFSET ?",
                params + [per_page, offset],
            ).fetchall()

            pages = max(1, (total + per_page - 1) // per_page)
            return {
                "items": [dict(r) for r in rows],
                "total": total,
                "page": page,
                "per_page": per_page,
                "pages": pages,
            }
        finally:
            conn.close()

    def get_uncategorized(self, client_name, year):
        """Get all transactions needing categories (status in staged/suggested, no category).

        Returns list of transaction dicts.
        """
        conn = self._conn()
        try:
            rows = conn.execute(
                """SELECT * FROM txn_values
                   WHERE client_name = ? AND year = ?
                     AND (category = '' OR category IS NULL)
                     AND status NOT IN ('corrected', 'overridden')
                   ORDER BY txn_date, id""",
                (client_name, int(year)),
            ).fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()

    def get_monthly_summary(self, client_name, year):
        """Get category × month pivot data.

        Returns:
            {
              "categories": {"Utilities": {"1": 150.0, "2": 175.0, ...}, ...},
              "monthly_totals": {"1": 5000.0, "2": 4800.0, ...},
              "category_totals": {"Utilities": 1850.0, ...},
              "grand_total": 50000.0
            }
        """
        conn = self._conn()
        try:
            rows = conn.execute(
                """SELECT category, txn_date, amount, txn_type
                   FROM txn_values
                   WHERE client_name = ? AND year = ?
                     AND category != '' AND category IS NOT NULL
                   ORDER BY category, txn_date""",
                (client_name, int(year)),
            ).fetchall()

            categories = {}
            monthly_totals = {}
            category_totals = {}
            grand_total = 0.0

            for row in rows:
                cat = row["category"] or "Uncategorized"
                amount = row["amount"] or 0.0
                txn_date = row["txn_date"] or ""

                # Parse month from date (handles MM/DD/YYYY or YYYY-MM-DD)
                month = self._parse_month(txn_date)
                if not month:
                    continue

                month_key = str(month)

                if cat not in categories:
                    categories[cat] = {}
                categories[cat][month_key] = categories[cat].get(month_key, 0.0) + abs(amount)

                monthly_totals[month_key] = monthly_totals.get(month_key, 0.0) + abs(amount)
                category_totals[cat] = category_totals.get(cat, 0.0) + abs(amount)
                grand_total += abs(amount)

            return {
                "categories": categories,
                "monthly_totals": monthly_totals,
                "category_totals": category_totals,
                "grand_total": grand_total,
            }
        finally:
            conn.close()

    def count_by_status(self, client_name, year):
        """Count transactions grouped by status. Returns dict."""
        conn = self._conn()
        try:
            rows = conn.execute(
                """SELECT status, COUNT(*) as cnt
                   FROM txn_values
                   WHERE client_name = ? AND year = ?
                   GROUP BY status""",
                (client_name, int(year)),
            ).fetchall()
            result = {s: 0 for s in TXN_STATUS_RANK}
            for row in rows:
                result[row["status"]] = row["cnt"]
            # Add computed counts
            result["total"] = sum(result.values())
            result["uncategorized"] = 0
            uncat = conn.execute(
                """SELECT COUNT(*) as cnt FROM txn_values
                   WHERE client_name = ? AND year = ?
                     AND (category = '' OR category IS NULL)""",
                (client_name, int(year)),
            ).fetchone()
            if uncat:
                result["uncategorized"] = uncat["cnt"]
            return result
        finally:
            conn.close()

    def get_clients_with_transactions(self):
        """Get distinct client names that have transactions. Returns list."""
        conn = self._conn()
        try:
            rows = conn.execute(
                "SELECT DISTINCT client_name FROM txn_values ORDER BY client_name"
            ).fetchall()
            return [row["client_name"] for row in rows]
        finally:
            conn.close()

    def get_vendor_rules(self, page=1, per_page=50):
        """Get all vendor rules with pagination."""
        conn = self._conn()
        try:
            count_row = conn.execute(
                "SELECT COUNT(*) as cnt FROM vendor_rules"
            ).fetchone()
            total = count_row["cnt"]
            offset = (page - 1) * per_page
            rows = conn.execute(
                "SELECT * FROM vendor_rules ORDER BY usage_count DESC, vendor_pattern LIMIT ? OFFSET ?",
                (per_page, offset),
            ).fetchall()
            return {
                "rules": [dict(r) for r in rows],
                "total": total,
                "page": page,
                "per_page": per_page,
            }
        finally:
            conn.close()

    def get_category_rules(self):
        """Get all category rules ordered by priority."""
        conn = self._conn()
        try:
            rows = conn.execute(
                "SELECT * FROM category_rules ORDER BY priority ASC, keyword"
            ).fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()

    def get_events(self, txn_id):
        """Get audit trail for a transaction."""
        conn = self._conn()
        try:
            rows = conn.execute(
                "SELECT * FROM txn_events WHERE txn_id = ? ORDER BY event_at",
                (txn_id,),
            ).fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()

    # ── Private Helpers ──────────────────────────────────────────────────────

    @staticmethod
    def _parse_month(date_str):
        """Parse month from a date string. Handles MM/DD/YYYY and YYYY-MM-DD."""
        if not date_str:
            return None
        date_str = str(date_str).strip()
        # Try YYYY-MM-DD
        m = re.match(r'(\d{4})-(\d{2})-', date_str)
        if m:
            return int(m.group(2))
        # Try MM/DD/YYYY or MM-DD-YYYY
        m = re.match(r'(\d{1,2})[/\-]', date_str)
        if m:
            month = int(m.group(1))
            if 1 <= month <= 12:
                return month
        return None
