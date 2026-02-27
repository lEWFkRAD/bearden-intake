#!/usr/bin/env python3
"""
Bearden Document Intake Platform v5.1
==================================
Full-featured local web app wrapping extract.py.

Features:
  - Drag-and-drop PDF upload
  - Live extraction progress with console output
  - Side-by-side review: source PDF page ↔ extracted values
  - Job history with client name search
  - Excel + JSON log download
  - Audit trail generation

Run:
    python3 app.py

Open:
    http://localhost:5000
"""

import os
import sys
import json
import threading
import uuid
import re
import glob
import subprocess
from pathlib import Path
from datetime import datetime
from io import BytesIO

try:
    from flask import Flask, render_template_string, request, jsonify, send_file, abort
except ImportError:
    sys.exit("Install Flask: pip3 install flask")

try:
    from pdf2image import convert_from_path
except ImportError:
    sys.exit("Install pdf2image: pip3 install pdf2image")

try:
    from PIL import Image, ImageDraw
except ImportError:
    sys.exit("Install Pillow: pip3 install Pillow")

import db as appdb
from muse_capture import init_muse_captures_table, register_muse_routes

# ─── Configuration ────────────────────────────────────────────────────────────

BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"
UPLOAD_DIR = DATA_DIR / "uploads"
OUTPUT_DIR = DATA_DIR / "outputs"
CLIENTS_DIR = BASE_DIR / "clients"
PAGES_DIR = DATA_DIR / "page_images"
VERIFY_DIR = BASE_DIR / "verifications"
JOBS_FILE = DATA_DIR / "jobs_history.json"

for d in [DATA_DIR, UPLOAD_DIR, OUTPUT_DIR, CLIENTS_DIR, PAGES_DIR, VERIFY_DIR]:
    d.mkdir(exist_ok=True)

VENDOR_CATEGORIES_FILE = DATA_DIR / "vendor_categories.json"
DB_PATH = DATA_DIR / "bearden.db"

def _secure_file(path):
    """Set restrictive permissions on sensitive files (owner-only read/write)."""
    try:
        os.chmod(str(path), 0o600)
    except OSError:
        pass  # Non-fatal: may fail on some filesystems


def _client_dir(client_name, doc_type, year):
    """Build a per-client output directory:  clients/<Client Name>/<doc_type>/<year>/"""
    safe_client = re.sub(r'[^\w\s\-\.,()]', '', client_name).strip() or "Unknown Client"
    safe_client = safe_client.title()
    type_labels = {
        "tax_returns": "Tax Returns",
        "bank_statements": "Bank Statements",
        "trust_documents": "Trust Documents",
        "bookkeeping": "Bookkeeping",
        "payroll": "Payroll",
        "other": "Other Documents",
    }
    type_folder = type_labels.get(doc_type, "Other")
    client_dir = CLIENTS_DIR / safe_client / type_folder / str(year)
    client_dir.mkdir(parents=True, exist_ok=True)
    return client_dir

app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = 150 * 1024 * 1024  # 150MB

_start_time = datetime.now()
_app_version = "5.2"

VALID_DOC_TYPES = {"tax_returns", "bank_statements", "trust_documents", "bookkeeping", "payroll", "other"}

# In-memory job tracking (persisted to jobs_history.json)
jobs = {}
_jobs_lock = threading.Lock()
_active_procs = {}  # job_id -> subprocess.Popen for cancellation

def load_jobs():
    """Load all jobs from SQLite into the in-memory dict."""
    global jobs
    try:
        all_jobs = appdb.list_jobs()
        jobs = {}
        for j in all_jobs:
            jid = j.get("id", "")
            if jid:
                j["log"] = []
                jobs[jid] = j
    except Exception as e:
        print(f"  Warning: Could not load jobs from database: {e}")
        jobs = {}

def save_jobs():
    """Persist all jobs to SQLite (upsert)."""
    with _jobs_lock:
        for jid, j in jobs.items():
            try:
                appdb.save_job(j)
            except Exception as e:
                print(f"  Warning: Could not save job {jid}: {e}")

def _sync_job_to_db(job_id):
    """Sync a single job to the database."""
    job = jobs.get(job_id)
    if job:
        try:
            appdb.save_job(job)
        except Exception:
            pass

appdb.init_db()
if appdb.needs_migration():
    try:
        from migrate import migrate_json_to_sqlite
        migrate_json_to_sqlite()
    except Exception as e:
        print(f"  Warning: Migration failed: {e}")
appdb.clear_stale_jobs()
load_jobs()

# ─── Muse Capture (brainstorm ingestion) ──────────────────────────────────────
init_muse_captures_table()
register_muse_routes(app)

# ─── Chart of Accounts + Vendor Memory ────────────────────────────────────────

CHART_OF_ACCOUNTS = {
    "Expense": [
        "Advertising & Marketing",
        "Auto & Travel",
        "Bank Service Charges",
        "Computer & Internet",
        "Depreciation",
        "Dues & Subscriptions",
        "Equipment",
        "Insurance",
        "Interest Expense",
        "Legal & Professional",
        "Meals & Entertainment",
        "Office Supplies",
        "Payroll Expenses",
        "Rent",
        "Repairs & Maintenance",
        "Taxes & Licenses",
        "Telephone",
        "Utilities",
        "Miscellaneous Expense",
    ],
    "Income": [
        "Service Revenue",
        "Product Sales",
        "Interest Income",
        "Rental Income",
        "Refund / Rebate",
        "Other Income",
    ],
    "Other": [
        "Owner Draw / Distribution",
        "Owner Contribution / Investment",
        "Loan Proceeds",
        "Loan Payment",
        "Transfer Between Accounts",
    ],
}

# Flat list for validation
ALL_ACCOUNTS = []
for _grp in CHART_OF_ACCOUNTS.values():
    ALL_ACCOUNTS.extend(_grp)

def _normalize_vendor(desc):
    """Normalize a vendor/payee name for matching.
    'GEORGIA POWER COMPANY #12345' → 'GEORGIA POWER'
    'WAL-MART SUPER CENTER 0423' → 'WAL-MART SUPER CENTER'
    """
    if not desc:
        return ""
    s = str(desc).upper().strip()
    # Strip trailing reference/store numbers
    s = re.sub(r'[\s#*]+\d{2,}$', '', s)
    # Strip common suffixes
    s = re.sub(r'\s+(LLC|INC|CORP|CO|COMPANY|LTD|LP|NA|N\.A\.)\.?\s*$', '', s, flags=re.IGNORECASE)
    # Strip trailing punctuation
    s = s.rstrip(' .,;:*#-')
    return s.strip()

def _load_vendor_categories():
    try:
        return appdb.get_vendor_categories()
    except Exception:
        return {}

def _save_vendor_categories(data):
    for vendor, info in data.items():
        cat = info.get("category", "") if isinstance(info, dict) else str(info)
        try:
            appdb.set_vendor_category(vendor, cat, info)
        except Exception as e:
            print(f"  Warning: Could not save vendor category {vendor}: {e}")

def _learn_vendor_category(vendor_desc, category):
    """Record that vendor_desc was categorized as category."""
    if not vendor_desc or not category:
        return
    norm = _normalize_vendor(vendor_desc)
    if not norm or len(norm) < 2:
        return
    data = _load_vendor_categories()
    existing = data.get(norm, {})
    data[norm] = {
        "category": category,
        "count": existing.get("count", 0) + 1,
        "last_used": datetime.now().isoformat(),
        "original": vendor_desc,  # keep one raw example
    }
    _save_vendor_categories(data)

def _suggest_category(vendor_desc):
    """Look up a vendor in the learned map. Returns category or ''."""
    if not vendor_desc:
        return ""
    norm = _normalize_vendor(vendor_desc)
    if not norm:
        return ""
    data = _load_vendor_categories()
    entry = data.get(norm)
    if entry:
        return entry.get("category", "")
    # Try prefix match (e.g., "WALMART SUPERCENTER" matches "WALMART")
    for known, info in data.items():
        if norm.startswith(known) or known.startswith(norm):
            return info.get("category", "")
    return ""

# ─── Prior-Year Context Engine ────────────────────────────────────────────────

def _safe_client_name(name):
    """Sanitize client name for filesystem use."""
    safe = re.sub(r'[^\w\s\-\.,()]', '', name).strip() or "Unknown Client"
    return safe.title()

def _client_info_path(name):
    """Path to client metadata JSON."""
    return CLIENTS_DIR / _safe_client_name(name) / "client_info.json"

def _load_client_info(name):
    """Load client metadata or return None."""
    p = _client_info_path(name)
    if p.exists():
        try:
            with open(p) as f:
                return json.load(f)
        except Exception:
            pass
    return None

def _save_client_info(name, info):
    """Save client metadata JSON."""
    p = _client_info_path(name)
    p.parent.mkdir(parents=True, exist_ok=True)
    info["updated"] = datetime.now().isoformat()
    with open(p, "w") as f:
        json.dump(info, f, indent=2)

def _context_dir(client_name):
    """Get or create the context directory for a client."""
    d = CLIENTS_DIR / _safe_client_name(client_name) / "context"
    d.mkdir(parents=True, exist_ok=True)
    return d

def _context_index_path(client_name):
    return _context_dir(client_name) / "index.json"

def _load_context_index(client_name):
    p = _context_index_path(client_name)
    if p.exists():
        try:
            with open(p) as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            pass
    return {"documents": [], "prior_year_data": {}, "updated": None}

def _save_context_index(client_name, data):
    data["updated"] = datetime.now().isoformat()
    with open(_context_index_path(client_name), "w") as f:
        json.dump(data, f, indent=2, default=str)

def _parse_context_document(file_path, doc_label=""):
    """Parse a context document (PDF, XLSX, TXT) into structured payer/amount data.

    Returns a dict with:
      payers: [{"name": ..., "ein": ..., "form_type": ..., "amounts": {...}}]
      raw_text: str (for instructions/notes)
      year: str or None
    """
    ext = Path(file_path).suffix.lower()
    result = {"payers": [], "raw_text": "", "year": None, "source": doc_label or Path(file_path).name}

    if ext == ".txt":
        try:
            with open(file_path, encoding="utf-8", errors="replace") as f:
                result["raw_text"] = f.read()
        except IOError:
            pass
        return result

    if ext in (".xlsx", ".xls"):
        try:
            import openpyxl
            wb = openpyxl.load_workbook(file_path, data_only=True, read_only=True)
            rows = []
            for ws in wb.worksheets:
                for row in ws.iter_rows(values_only=True):
                    row_data = [str(c).strip() if c is not None else "" for c in row]
                    if any(cell for cell in row_data):
                        rows.append(row_data)
            wb.close()

            # Try to extract payer/amount pairs from tabular data
            text = "\n".join(["\t".join(r) for r in rows])
            result["raw_text"] = text
            result["payers"] = _extract_payers_from_text(text)
        except Exception as e:
            result["raw_text"] = f"(Error reading xlsx: {e})"
        return result

    if ext == ".pdf":
        try:
            # Use Tesseract OCR via the same method as extract.py
            images = convert_from_path(str(file_path), dpi=200, fmt="jpeg")
            import pytesseract
            text_parts = []
            for img in images:
                text_parts.append(pytesseract.image_to_string(img))
            full_text = "\n".join(text_parts)
            result["raw_text"] = full_text
            result["payers"] = _extract_payers_from_text(full_text)
        except Exception as e:
            result["raw_text"] = f"(Error reading PDF: {e})"
        return result

    return result

def _extract_payers_from_text(text):
    """Extract payer names, EINs, form types, and dollar amounts from OCR text.

    Uses pattern matching — no LLM call. Catches common tax form patterns.
    """
    payers = []
    seen_eins = set()

    # EIN pattern: XX-XXXXXXX
    ein_pattern = re.compile(r'\b(\d{2}-\d{7})\b')
    # Dollar amounts: $1,234.56 or 1,234.56
    money_pattern = re.compile(r'\$?([\d,]+\.\d{2})\b')
    # Form type indicators
    form_patterns = {
        "W-2": re.compile(r'\bW[\s-]*2\b', re.I),
        "1099-INT": re.compile(r'\b1099[\s-]*INT\b', re.I),
        "1099-DIV": re.compile(r'\b1099[\s-]*DIV\b', re.I),
        "1099-R": re.compile(r'\b1099[\s-]*R\b', re.I),
        "1099-NEC": re.compile(r'\b1099[\s-]*NEC\b', re.I),
        "1099-MISC": re.compile(r'\b1099[\s-]*MISC\b', re.I),
        "1099-B": re.compile(r'\b1099[\s-]*B\b', re.I),
        "1099-K": re.compile(r'\b1099[\s-]*K\b', re.I),
        "K-1": re.compile(r'\bK[\s-]*1\b', re.I),
        "SSA-1099": re.compile(r'\bSSA[\s-]*1099\b', re.I),
        "1098": re.compile(r'\b1098\b'),
    }

    # Process line by line looking for EINs near entity names
    lines = text.split("\n")
    for i, line in enumerate(lines):
        eins = ein_pattern.findall(line)
        for ein in eins:
            if ein in seen_eins:
                continue
            seen_eins.add(ein)

            # Look for entity name near this EIN (same line or adjacent)
            context = line
            if i > 0:
                context = lines[i-1] + " " + context
            if i < len(lines) - 1:
                context = context + " " + lines[i+1]

            # Detect form type
            form_type = ""
            for ftype, fpat in form_patterns.items():
                if fpat.search(context):
                    form_type = ftype
                    break

            # Extract dollar amounts from context
            amounts = [float(m.replace(",", "")) for m in money_pattern.findall(context)]

            # Try to get entity name (text before or after EIN, not a number)
            parts = re.split(ein_pattern, line)
            name_candidates = [p.strip() for p in parts
                               if p.strip() and p.strip() != ein
                               and not money_pattern.match(p.strip())]
            entity_name = name_candidates[0] if name_candidates else ""
            # Clean up
            entity_name = re.sub(r'^[\s\d\-:]+', '', entity_name).strip()
            entity_name = entity_name[:80]

            payers.append({
                "name": entity_name,
                "ein": ein,
                "form_type": form_type,
                "amounts": amounts[:10],  # cap
            })

    return payers

def _build_completeness_report(client_name, current_extractions, year):
    """Compare current extractions against prior-year context.

    Returns:
      matched: [{payer, form, status: "received", current_amounts, prior_amounts}]
      missing: [{payer, form, status: "expected"}]
      new: [{payer, form, status: "new"}]
      variances: [{payer, form, field, prior, current, pct_change, severity}]
    """
    ctx = _load_context_index(client_name)
    prior_data = ctx.get("prior_year_data", {})
    if not prior_data:
        return {"matched": [], "missing": [], "new": [], "variances": []}

    # Build sets: prior payers by (ein, form_type)
    prior_set = {}
    for doc in prior_data.get("documents", []):
        for payer in doc.get("payers", []):
            key = (payer.get("ein", ""), payer.get("form_type", ""))
            if key[0]:  # only track if we have an EIN
                prior_set[key] = payer

    # Build current set from extractions
    current_set = {}
    for ext in current_extractions:
        dtype = ext.get("document_type", "")
        ein = ""
        fields = ext.get("fields", {})
        for ek in ["payer_ein", "employer_ein", "partnership_ein"]:
            v = fields.get(ek)
            if isinstance(v, dict):
                v = v.get("value", "")
            if v:
                ein = str(v)
                break
        entity = ext.get("payer_or_entity", "")
        key = (ein, dtype)
        current_set[key] = {"name": entity, "ein": ein, "form_type": dtype, "fields": fields}

    matched = []
    missing = []
    new_items = []
    variances = []

    for key, prior in prior_set.items():
        if key in current_set:
            cur = current_set[key]
            matched.append({
                "payer": cur.get("name") or prior.get("name", ""),
                "ein": key[0],
                "form": key[1],
                "status": "received",
            })
            # Check for variances on key amounts
            for pa in prior.get("amounts", []):
                # Find closest matching current amount
                for fname, fdata in cur.get("fields", {}).items():
                    cv = fdata.get("value") if isinstance(fdata, dict) else fdata
                    if isinstance(cv, (int, float)) and cv > 0 and pa > 0:
                        pct = abs(cv - pa) / pa * 100
                        if pct > 25:
                            variances.append({
                                "payer": cur.get("name", ""),
                                "form": key[1],
                                "field": fname,
                                "prior": pa,
                                "current": cv,
                                "pct_change": round(pct, 1),
                                "severity": "red" if pct > 50 else "yellow",
                            })
                        break  # one comparison per prior amount
        else:
            missing.append({
                "payer": prior.get("name", "Unknown"),
                "ein": key[0],
                "form": key[1] or "Unknown form",
                "status": "expected",
            })

    for key, cur in current_set.items():
        if key not in prior_set and key[0]:
            new_items.append({
                "payer": cur.get("name", ""),
                "ein": key[0],
                "form": key[1],
                "status": "new",
            })

    return {"matched": matched, "missing": missing, "new": new_items, "variances": variances}


# ─── Client Instruction Memory ────────────────────────────────────────────────

def _instructions_path(client_name):
    return CLIENTS_DIR / _safe_client_name(client_name) / "instructions.json"

def _load_instructions(client_name):
    p = _instructions_path(client_name)
    if p.exists():
        try:
            with open(p) as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            pass
    return {"rules": [], "updated": None}

def _save_instructions(client_name, data):
    # Ensure parent dir exists
    p = _instructions_path(client_name)
    p.parent.mkdir(parents=True, exist_ok=True)
    data["updated"] = datetime.now().isoformat()
    with open(p, "w") as f:
        json.dump(data, f, indent=2, default=str)

def _instructions_text(client_name):
    """Get all instructions as a single string for prompt injection."""
    data = _load_instructions(client_name)
    rules = data.get("rules", [])
    if not rules:
        return ""
    lines = [f"- {r['text']}" for r in rules if r.get("text")]
    return "CLIENT-SPECIFIC INSTRUCTIONS:\n" + "\n".join(lines)


# ─── Batch Categorization Helpers ─────────────────────────────────────────────

def _gather_uncategorized(job_ids=None, client_name=None):
    """Gather all transactions needing categorization across jobs.

    Returns list of:
      {job_id, page, ext_idx, txn_num, date, desc, amount, type, source,
       current_category, suggested_category, vendor_norm}
    """
    target_jobs = {}
    for jid, j in jobs.items():
        if j.get("status") != "complete":
            continue
        if job_ids and jid not in job_ids:
            continue
        if client_name and j.get("client_name", "").lower() != client_name.lower():
            continue
        target_jobs[jid] = j

    items = []
    for jid, j in target_jobs.items():
        log_path = j.get("output_log")
        if not log_path or not os.path.exists(log_path):
            continue
        try:
            with open(log_path) as f:
                log_data = json.load(f)
        except (json.JSONDecodeError, IOError):
            continue

        vdata = _load_verifications(jid)
        vfields = vdata.get("fields", {})

        for ext in log_data.get("extractions", []):
            dtype = str(ext.get("document_type", ""))
            fields = ext.get("fields", {})
            entity = ext.get("payer_or_entity", "")
            page = ext.get("_page", 0)

            # Find ext index for this page
            page_exts = [e for e in log_data.get("extractions", []) if e.get("_page") == page]
            ext_idx = 0
            for ei, pe in enumerate(page_exts):
                if pe is ext:
                    ext_idx = ei
                    break

            # Bank/CC transactions
            if "bank_statement" in dtype or "credit_card" in dtype:
                bank = ""
                for k in ["bank_name", "card_issuer"]:
                    v = fields.get(k)
                    bank = (v.get("value", "") if isinstance(v, dict) else str(v or "")) if v else ""
                    if bank:
                        break
                source = bank or entity

                txn_nums = sorted(set(
                    int(m.group(1)) for k in fields
                    for m in [re.match(r"txn_(\d+)_", k)] if m
                ))
                for n in txn_nums:
                    amt_key = f"txn_{n}_amount"
                    vk = f"{page}:{ext_idx}:{amt_key}"
                    vstate = vfields.get(vk, {})
                    current_cat = vstate.get("category", "")

                    date_f = fields.get(f"txn_{n}_date")
                    desc_f = fields.get(f"txn_{n}_desc")
                    amt_f = fields.get(amt_key)
                    type_f = fields.get(f"txn_{n}_type")

                    date_v = (date_f.get("value", "") if isinstance(date_f, dict) else str(date_f or "")) if date_f else ""
                    desc_v = (desc_f.get("value", "") if isinstance(desc_f, dict) else str(desc_f or "")) if desc_f else ""
                    amt_v = (amt_f.get("value") if isinstance(amt_f, dict) else amt_f) if amt_f else None
                    type_v = (type_f.get("value", "") if isinstance(type_f, dict) else str(type_f or "")) if type_f else ""

                    if amt_v is None:
                        continue

                    norm = _normalize_vendor(desc_v)
                    suggested = _suggest_category(desc_v) if not current_cat else ""

                    items.append({
                        "job_id": jid, "page": page, "ext_idx": ext_idx,
                        "field_key": vk, "date": date_v, "desc": desc_v,
                        "amount": amt_v, "type": type_v, "source": source,
                        "doc_type": dtype,
                        "current_category": current_cat,
                        "suggested_category": suggested,
                        "vendor_norm": norm,
                        "client_name": j.get("client_name", ""),
                    })

            # Checks
            elif dtype == "check":
                check_amt_f = fields.get("check_amount")
                amt_v = (check_amt_f.get("value") if isinstance(check_amt_f, dict) else check_amt_f) if check_amt_f else None
                if amt_v is None:
                    continue
                vk = f"{page}:{ext_idx}:check_amount"
                vstate = vfields.get(vk, {})
                current_cat = vstate.get("category", "")

                payee_f = fields.get("payee") or fields.get("pay_to")
                payee = (payee_f.get("value", "") if isinstance(payee_f, dict) else str(payee_f or "")) if payee_f else ""
                date_f = fields.get("check_date")
                date_v = (date_f.get("value", "") if isinstance(date_f, dict) else str(date_f or "")) if date_f else ""
                num_f = fields.get("check_number")
                num_v = (num_f.get("value", "") if isinstance(num_f, dict) else str(num_f or "")) if num_f else ""

                norm = _normalize_vendor(payee)
                suggested = _suggest_category(payee) if not current_cat else ""

                items.append({
                    "job_id": jid, "page": page, "ext_idx": ext_idx,
                    "field_key": vk, "date": date_v,
                    "desc": f"Check #{num_v} to {payee}" if num_v else payee,
                    "amount": amt_v, "type": "check", "source": "Check",
                    "doc_type": dtype,
                    "current_category": current_cat,
                    "suggested_category": suggested,
                    "vendor_norm": norm,
                    "client_name": j.get("client_name", ""),
                })

    return items

def auto_rotate_page(img):
    """Detect and fix sideways/landscape pages for the review viewer."""
    w, h = img.size
    if w > h * 1.15:
        # Landscape — use Tesseract OSD to determine correct rotation
        try:
            import pytesseract
            osd = pytesseract.image_to_osd(img, output_type=pytesseract.Output.DICT)
            angle = osd.get("rotate", 0)
            if angle != 0:
                return img.rotate(-angle, expand=True)
            # Tesseract says angle=0 — page is already correct orientation (landscape)
            return img
        except Exception as e:
            print(f"  Auto-rotate: Tesseract OSD failed ({e}), rotating 90 CW as fallback")
        # Fallback only if Tesseract failed entirely
        return img.rotate(-90, expand=True)
    return img

def _generate_word_boxes(pil_image, output_path):
    """Run Tesseract image_to_data to capture word-level bounding boxes. Save as JSON sidecar."""
    try:
        import pytesseract
        data = pytesseract.image_to_data(pil_image, output_type=pytesseract.Output.DICT)
        words = []
        n = len(data['text'])
        for i in range(n):
            txt = data['text'][i].strip()
            if txt:
                words.append({
                    'text': txt,
                    'left': data['left'][i],
                    'top': data['top'][i],
                    'width': data['width'][i],
                    'height': data['height'][i],
                    'conf': data['conf'][i],
                })
        with open(str(output_path), 'w') as f:
            json.dump({
                'words': words,
                'img_width': pil_image.size[0],
                'img_height': pil_image.size[1],
            }, f)
    except Exception as e:
        print(f"  Word box generation skipped: {e}")


def _find_text_bbox(words, search_text):
    """Find bounding box of search_text among OCR word boxes.

    Strategy:
      1. Numeric match -- strip $, commas, compare as float
      2. Exact single-word match (case-insensitive)
      3. Multi-word sliding window (up to 6 consecutive words)

    Returns (x, y, width, height) or None.
    """
    if not search_text or not words:
        return None

    search = str(search_text).strip()
    if not search:
        return None

    # 1. Try numeric matching (for monetary values)
    try:
        search_num = float(re.sub(r'[\$,\s]', '', search))
        for w in words:
            try:
                word_num = float(re.sub(r'[\$,\s]', '', w['text']))
                if abs(word_num - search_num) < 0.015:
                    return (w['left'], w['top'], w['width'], w['height'])
            except (ValueError, TypeError):
                continue
    except (ValueError, TypeError):
        pass

    # 2. Try exact single-word match
    search_upper = search.upper()
    for w in words:
        if w['text'].upper().strip() == search_upper:
            return (w['left'], w['top'], w['width'], w['height'])

    # 3. Try multi-word sliding window
    for i in range(len(words)):
        running = ""
        combined_left = words[i]['left']
        combined_top = words[i]['top']
        combined_right = words[i]['left'] + words[i]['width']
        combined_bottom = words[i]['top'] + words[i]['height']

        for j in range(i, min(i + 6, len(words))):
            running = (running + " " + words[j]['text']).strip()
            combined_right = max(combined_right, words[j]['left'] + words[j]['width'])
            combined_bottom = max(combined_bottom, words[j]['top'] + words[j]['height'])
            combined_top = min(combined_top, words[j]['top'])

            if running.upper() == search_upper:
                return (combined_left, combined_top,
                        combined_right - combined_left,
                        combined_bottom - combined_top)

    return None


def _draw_evidence_highlights(page_img_path, words, value_text, label_text):
    """Draw highlight overlay on page image. Returns PIL Image (RGB)."""
    img = Image.open(str(page_img_path)).convert('RGBA')
    overlay = Image.new('RGBA', img.size, (255, 255, 255, 0))
    draw = ImageDraw.Draw(overlay)

    value_found = False
    label_found = False

    if words:
        value_bbox = _find_text_bbox(words, value_text)
        label_bbox = _find_text_bbox(words, label_text) if label_text else None

        if value_bbox:
            x, y, w, h = value_bbox
            pad = 6
            draw.rectangle([x - pad, y - pad, x + w + pad, y + h + pad],
                           fill=(255, 255, 0, 100),
                           outline=(220, 50, 50, 220),
                           width=3)
            value_found = True

        if label_bbox:
            x, y, w, h = label_bbox
            pad = 3
            draw.rectangle([x - pad, y - pad, x + w + pad, y + h + pad],
                           fill=(66, 133, 244, 50),
                           outline=(66, 133, 244, 140),
                           width=1)
            label_found = True

    if not value_found and not label_found:
        draw.rectangle([0, 0, img.size[0], 32], fill=(255, 243, 205, 230))
    elif not value_found and label_found:
        draw.rectangle([0, 0, img.size[0], 24], fill=(255, 248, 230, 200))

    img = Image.alpha_composite(img, overlay).convert('RGB')
    return img


def generate_page_images(job_id, pdf_path):
    """Convert PDF pages to JPEG images for the side-by-side viewer. Auto-rotates sideways pages."""
    job_pages_dir = PAGES_DIR / job_id
    job_pages_dir.mkdir(exist_ok=True)
    try:
        images = convert_from_path(str(pdf_path), dpi=150)
        for i, img in enumerate(images):
            img = auto_rotate_page(img)
            page_path = job_pages_dir / f"page_{i+1}.jpg"
            img.save(str(page_path), "JPEG", quality=80)
            # Generate word-level bounding boxes for evidence highlighting
            words_path = job_pages_dir / f"page_{i+1}_words.json"
            _generate_word_boxes(img, words_path)
        return len(images)
    except Exception as e:
        print(f"Page image generation error: {e}")
        return 0

# ─── Job Runner ───────────────────────────────────────────────────────────────

def run_extraction(job_id, pdf_path, year, skip_verify, doc_type="tax_returns", output_format="tax_review", user_notes="", ai_instructions="", disable_pii=False, resume=False, use_ocr_first=False):
    """Run extract.py in a background thread, capturing progress line by line."""
    import subprocess
    job = jobs[job_id]
    job["status"] = "running"
    job["log"] = []
    job["stage"] = "starting"
    job["progress"] = 0
    job["start_time"] = datetime.now().isoformat()

    # Generate page images for the side-by-side viewer
    job["stage"] = "rendering"
    job["progress"] = 2
    job["log"].append("Rendering PDF pages for review...")
    num_pages = generate_page_images(job_id, pdf_path)
    job["total_pages"] = num_pages
    job["log"].append(f"  {num_pages} pages rendered")

    # Build command
    output_name = Path(pdf_path).stem + "_intake.xlsx"
    output_path = OUTPUT_DIR / output_name
    log_name = Path(pdf_path).stem + "_intake_log.json"
    log_path = OUTPUT_DIR / log_name

    # Map doc types to extract.py-compatible values
    # (extract.py only accepts: tax_returns, bank_statements, trust_documents, bookkeeping)
    EXTRACT_DOC_TYPE_MAP = {
        "payroll": "bookkeeping",
        "other": "bookkeeping",
    }
    extract_doc_type = EXTRACT_DOC_TYPE_MAP.get(doc_type, doc_type)

    cmd = [sys.executable, str(BASE_DIR / "extract.py"), str(pdf_path),
           "--year", str(year), "--output", str(output_path),
           "--doc-type", extract_doc_type, "--output-format", output_format]
    if skip_verify:
        cmd.append("--skip-verify")
    if disable_pii:
        cmd.append("--no-pii")
    if resume:
        cmd.append("--resume")
    if not use_ocr_first:
        cmd.append("--no-ocr-first")

    # Inject client instructions into ai_instructions
    client_name = job.get("client_name", "")
    instr_text = _instructions_text(client_name) if client_name else ""
    combined_instructions = ai_instructions
    if instr_text:
        combined_instructions = (combined_instructions + "\n\n" + instr_text) if combined_instructions else instr_text

    if user_notes:
        cmd.extend(["--user-notes", user_notes])
    if combined_instructions:
        cmd.extend(["--ai-instructions", combined_instructions])

    # Pass prior-year context if available
    if client_name:
        ctx_idx_path = _context_index_path(client_name)
        if ctx_idx_path.exists():
            cmd.extend(["--context-file", str(ctx_idx_path)])

    try:
        proc = subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, bufsize=1, cwd=str(BASE_DIR)
        )
        _active_procs[job_id] = proc

        for line in proc.stdout:
            line = line.rstrip()
            if not line:
                continue
            job["log"].append(line)

            # Parse progress from extract.py output
            ll = line.lower()
            if "converting pdf" in ll:
                job["stage"] = "scanning"
                job["progress"] = 5
            elif "ocr pass" in ll:
                job["stage"] = "ocr"
                job["progress"] = 8
            elif "ocr success" in ll:
                job["progress"] = 12
            elif "phase 1:" in ll or "classification" in ll or "classify" in ll:
                job["stage"] = "classifying"
                job["progress"] = 15
            elif "document group" in ll:
                job["progress"] = 25
            elif "phase 2:" in ll or "── extraction" in ll:
                job["stage"] = "extracting"
                job["progress"] = 30
            elif "ocr sufficient" in ll or "text extraction" in ll:
                # OCR-first path saved a vision call
                job["progress"] = min(job["progress"] + 3, 72)
            elif ("page" in ll and "extracted" in ll) or ("multi-page extracted" in ll):
                job["progress"] = min(job["progress"] + 3, 72)
            elif "extraction stats" in ll:
                job["progress"] = 75
            elif "phase 3:" in ll or "── verification" in ll:
                job["stage"] = "verifying"
                job["progress"] = 78
            elif "corrected:" in ll:
                job["progress"] = min(job["progress"] + 1, 88)
            elif "phase 4:" in ll or "normalize" in ll:
                job["stage"] = "normalizing"
                job["progress"] = 90
            elif "phase 5:" in ll or "validate" in ll:
                job["progress"] = 93
            elif "phase 6:" in ll or "excel" in ll:
                job["stage"] = "writing"
                job["progress"] = 96
            elif "est. cost:" in ll:
                # Capture cost from final summary line
                import re as _re
                m = _re.search(r'\$(\d+\.\d+)', line)
                if m:
                    job["cost_usd"] = float(m.group(1))
            elif ll.strip().startswith("complete") or ll.strip().endswith("complete"):
                job["progress"] = 100

        proc.wait()
        _active_procs.pop(job_id, None)

        if proc.returncode == 0:
            job["status"] = "complete"
            job["progress"] = 100
            job["stage"] = "done"
            job["end_time"] = datetime.now().isoformat()
            job["output_xlsx"] = str(output_path) if output_path.exists() else None
            job["output_log"] = str(log_path) if log_path.exists() else None
            if job["output_xlsx"]:
                _secure_file(job["output_xlsx"])
            if job["output_log"]:
                _secure_file(job["output_log"])

            # Copy outputs to client directory
            import shutil
            client_folder = job.get("client_folder")
            if client_folder:
                client_dir = Path(client_folder)
                client_dir.mkdir(parents=True, exist_ok=True)
                try:
                    if output_path.exists():
                        dst_xlsx = client_dir / output_path.name
                        shutil.copy2(str(output_path), str(dst_xlsx))
                        _secure_file(dst_xlsx)
                        job["client_xlsx"] = str(dst_xlsx)
                        job["log"].append(f"  Saved to: {dst_xlsx}")
                    if log_path.exists():
                        dst_log = client_dir / log_path.name
                        shutil.copy2(str(log_path), str(dst_log))
                        _secure_file(dst_log)
                        job["client_log"] = str(dst_log)
                    # Copy the original PDF too (use original filename for client folder)
                    src_pdf = Path(pdf_path)
                    if src_pdf.exists():
                        original_name = job.get("filename", src_pdf.name)
                        safe_original = re.sub(r'[^\w\s\-\.,()]', '', original_name).strip() or src_pdf.name
                        dst_pdf = client_dir / safe_original
                        if not dst_pdf.exists():
                            shutil.copy2(str(src_pdf), str(dst_pdf))
                            _secure_file(dst_pdf)
                except Exception as e:
                    job["log"].append(f"  Warning: Could not copy to client folder: {e}")

            # Parse the JSON log for summary stats
            if log_path.exists():
                try:
                    with open(log_path) as f:
                        log_data = json.load(f)
                    exts = log_data.get("extractions", [])
                    methods = {}
                    confs = {}
                    for e in exts:
                        m = e.get("_extraction_method") or "unknown"
                        methods[m] = methods.get(m, 0) + 1
                        for fv in (e.get("fields") or {}).values():
                            if isinstance(fv, dict):
                                c = fv.get("confidence") or "unknown"
                                confs[c] = confs.get(c, 0) + 1
                    job["stats"] = {
                        "documents": len(exts),
                        "methods": methods,
                        "confidences": confs,
                        "warnings": len(log_data.get("warnings", [])),
                        "total_fields": sum(confs.values()),
                    }
                    # Include cost data if present in log
                    cost = log_data.get("cost")
                    if cost:
                        job["stats"]["cost"] = cost
                        job["cost_usd"] = cost.get("estimated_cost_usd", 0)
                    # Populate facts + review_state for the review chain
                    try:
                        page_counts = {}
                        for ext in exts:
                            p = ext.get("_page", 0)
                            ext["_ext_idx"] = page_counts.get(p, 0)
                            page_counts[p] = page_counts.get(p, 0) + 1
                        count = appdb.populate_facts_from_extraction(
                            job_id, job.get("client_name", "unknown"),
                            str(job.get("year", "")), exts)
                        job["log"].append(f"  Review chain: {count} facts created")
                    except Exception as fe:
                        job["log"].append(f"  Warning: Could not populate review chain: {fe}")

                except (json.JSONDecodeError, KeyError, TypeError, IOError) as e:
                    job["log"].append(f"  Warning: Could not parse log stats: {e}")
        else:
            job["status"] = "error"
            job["end_time"] = datetime.now().isoformat()
            job["error"] = f"extract.py exited with code {proc.returncode}"

    except Exception as e:
        job["status"] = "error"
        job["end_time"] = datetime.now().isoformat()
        job["error"] = str(e)
        _active_procs.pop(job_id, None)

    save_jobs()


# ─── Routes ───────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return render_template_string(MAIN_HTML)

@app.route("/api/upload", methods=["POST"])
def upload():
    if "pdf" not in request.files:
        return jsonify({"error": "No file uploaded"}), 400
    f = request.files["pdf"]
    if not f.filename.lower().endswith(".pdf"):
        return jsonify({"error": "File must be a PDF"}), 400

    year = request.form.get("year", str(datetime.now().year))
    # Validate year is a 4-digit number in reasonable range
    try:
        year_int = int(year)
        if year_int < 2000 or year_int > datetime.now().year + 1:
            return jsonify({"error": f"Invalid year: {year}"}), 400
    except ValueError:
        return jsonify({"error": f"Invalid year: {year}"}), 400

    skip_verify = request.form.get("skip_verify") == "true"
    disable_pii = request.form.get("disable_pii") == "true"
    use_ocr_first = request.form.get("use_ocr_first") == "true"
    client_name = request.form.get("client_name", "").strip()
    if not client_name:
        return jsonify({"error": "Please select a client"}), 400
    doc_type = request.form.get("doc_type", "tax_returns")
    if doc_type not in VALID_DOC_TYPES:
        doc_type = "tax_returns"  # Safe default
    output_format = request.form.get("output_format", "tax_review")
    VALID_OUTPUT_FORMATS = {"tax_review", "journal_entries", "account_balances", "trial_balance", "transaction_register"}
    if output_format not in VALID_OUTPUT_FORMATS:
        output_format = "tax_review"
    user_notes = request.form.get("user_notes", "").strip()[:2000]  # Cap at 2000 chars
    ai_instructions = request.form.get("ai_instructions", "").strip()[:2000]

    # Generate job ID first (needed for unique filename)
    job_id = datetime.now().strftime("%m%d") + "-" + str(uuid.uuid4())[:6]

    # Save with unique name to prevent overwrites
    pdf_path = UPLOAD_DIR / (job_id + ".pdf")
    f.save(str(pdf_path))
    _secure_file(pdf_path)

    # Build client folder path
    resolved_client = _safe_client_name(client_name)
    client_dir = _client_dir(resolved_client, doc_type, year)

    jobs[job_id] = {
        "id": job_id,
        "filename": f.filename,
        "client_name": resolved_client,
        "doc_type": doc_type,
        "output_format": output_format,
        "user_notes": user_notes,
        "ai_instructions": ai_instructions,
        "year": year,
        "status": "queued",
        "stage": "queued",
        "progress": 0,
        "log": [],
        "created": datetime.now().isoformat(),
        "pdf_path": str(pdf_path),
        "client_folder": str(client_dir),
        "disable_pii": disable_pii,
        "use_ocr_first": use_ocr_first,
    }
    save_jobs()

    t = threading.Thread(target=run_extraction, args=(job_id, pdf_path, year, skip_verify, doc_type, output_format, user_notes, ai_instructions, disable_pii, False, use_ocr_first))
    t.daemon = True
    t.start()

    return jsonify({"job_id": job_id})

@app.route("/api/status/<job_id>")
def status(job_id):
    job = jobs.get(job_id)
    if not job:
        return jsonify({"error": "Job not found"}), 404
    out = _sanitize_job(job)
    out["recent_log"] = job.get("log", [])[-40:]
    out["log_length"] = len(job.get("log", []))
    return jsonify(out)

@app.route("/api/results/<job_id>")
def results(job_id):
    job = jobs.get(job_id)
    if not job:
        return jsonify({"error": "Job not found"}), 404
    log_path = job.get("output_log")
    if log_path and os.path.exists(log_path):
        with open(log_path) as f:
            data = json.load(f)
        # Attach page mapping: which pages have which extractions
        page_map = {}
        for ext in data.get("extractions", []):
            p = ext.get("_page")
            if p:
                if p not in page_map:
                    page_map[p] = []
                page_map[p].append({
                    "document_type": ext.get("document_type", ""),
                    "entity": ext.get("payer_or_entity", ""),
                    "method": ext.get("_extraction_method", ""),
                    "confidence": ext.get("_overall_confidence", ""),
                    "fields": {k: {
                        "value": v.get("value") if isinstance(v, dict) else v,
                        "confidence": v.get("confidence", "") if isinstance(v, dict) else "",
                        "label": v.get("label_on_form", "") if isinstance(v, dict) else "",
                    } for k, v in (ext.get("fields") or {}).items()
                    if (v.get("value") if isinstance(v, dict) else v) is not None}
                })
        data["page_map"] = page_map
        data["total_pages"] = job.get("total_pages") or max((int(k) for k in page_map.keys()), default=1)
        return jsonify(data)
    return jsonify({"error": "Results not ready"}), 404

@app.route("/api/reextract-page/<job_id>/<int:page_num>", methods=["POST"])
def reextract_page(job_id, page_num):
    """Re-extract a single page with custom AI instructions."""
    job = jobs.get(job_id)
    if not job:
        return jsonify({"error": "Job not found"}), 404

    instructions = request.json.get("instructions", "").strip()
    if not instructions:
        return jsonify({"error": "No instructions provided"}), 400

    # Load the page image
    img_path = PAGES_DIR / job_id / f"page_{page_num}.jpg"
    if not img_path.exists():
        return jsonify({"error": f"Page {page_num} image not found"}), 404

    import base64
    try:
        import anthropic as _anthropic
    except ImportError:
        return jsonify({"error": "Anthropic library not available"}), 500

    with open(img_path, "rb") as f:
        img_b64 = base64.b64encode(f.read()).decode("utf-8")

    # Build extraction prompt with operator instructions
    # Import the vision prompt template from extract.py
    import importlib.util
    spec = importlib.util.spec_from_file_location("extract", str(BASE_DIR / "extract.py"))
    ext_mod = importlib.util.module_from_spec(spec)
    # Only load the constants we need (not the whole module execution)
    try:
        spec.loader.exec_module(ext_mod)
        vision_prompt = ext_mod.VISION_EXTRACTION_PROMPT
        model = ext_mod.MODEL
    except Exception:
        # Fallback if module load fails
        model = "claude-sonnet-4-20250514"
        vision_prompt = "Extract all data from this document page. Return JSON with document_type, payer_or_entity, fields (each with value, label_on_form, confidence)."

    context = f"The operator has provided these specific instructions for this page:\n{instructions}"
    doc_type = job.get("doc_type", "")
    if doc_type:
        context = f"Document type: {doc_type}\n{context}"
    prompt = vision_prompt.replace("{context}", context)

    # Call Claude
    try:
        client = _anthropic.Anthropic()
        msg = client.messages.create(
            model=model, max_tokens=8000,
            messages=[{"role": "user", "content": [
                {"type": "image", "source": {"type": "base64", "media_type": "image/jpeg", "data": img_b64}},
                {"type": "text", "text": prompt}
            ]}]
        )
        raw = msg.content[0].text

        # Parse JSON from response
        import re as _re
        result = None
        # Try full response first
        try:
            result = json.loads(raw)
        except json.JSONDecodeError:
            # Try extracting JSON block
            m = _re.search(r'\{[\s\S]*\}', raw)
            if m:
                try:
                    result = json.loads(m.group(0))
                except json.JSONDecodeError:
                    pass

        if not result or "fields" not in result:
            return jsonify({"error": "AI returned invalid response", "raw": raw[:500]}), 500

    except Exception as e:
        return jsonify({"error": f"AI call failed: {str(e)}"}), 500

    # Update the log file with new extraction
    log_path = job.get("output_log")
    if log_path and os.path.exists(log_path):
        with open(log_path) as f:
            log_data = json.load(f)

        # Find and replace extraction for this page, or add new one
        exts = log_data.get("extractions", [])
        replaced = False
        for i, ext in enumerate(exts):
            if ext.get("_page") == page_num:
                # Preserve metadata, replace content
                result["_page"] = page_num
                result["_extraction_method"] = "vision_reextract"
                result["_overall_confidence"] = ext.get("_overall_confidence")
                result["_reextract_instructions"] = instructions
                exts[i] = result
                replaced = True
                break
        if not replaced:
            result["_page"] = page_num
            result["_extraction_method"] = "vision_reextract"
            result["_reextract_instructions"] = instructions
            exts.append(result)
            exts.sort(key=lambda e: e.get("_page", 0))

        log_data["extractions"] = exts
        with open(log_path, "w") as f:
            json.dump(log_data, f, indent=2, default=str)

        # Also update client folder copy if it exists
        client_log = job.get("client_folder")
        if client_log:
            client_log_path = Path(client_log) / Path(log_path).name
            if client_log_path.exists():
                with open(client_log_path, "w") as f:
                    json.dump(log_data, f, indent=2, default=str)

    # Return the new extraction data in page_map format
    fields_out = {}
    for k, v in (result.get("fields") or {}).items():
        val = v.get("value") if isinstance(v, dict) else v
        if val is not None:
            fields_out[k] = {
                "value": val,
                "confidence": v.get("confidence", "") if isinstance(v, dict) else "",
                "label": v.get("label_on_form", "") if isinstance(v, dict) else "",
            }

    return jsonify({
        "success": True,
        "page": page_num,
        "document_type": result.get("document_type", ""),
        "entity": result.get("payer_or_entity", ""),
        "method": "vision_reextract",
        "fields": fields_out,
    })

@app.route("/api/ai-chat/<job_id>", methods=["POST"])
def ai_chat(job_id):
    """Chat with AI about the current extraction / page."""
    job = jobs.get(job_id)
    if not job:
        return jsonify({"error": "Job not found"}), 404

    message = request.json.get("message", "").strip()
    page_num = request.json.get("page")
    if not message:
        return jsonify({"error": "No message provided"}), 400

    import base64
    try:
        import anthropic as _anthropic
    except ImportError:
        return jsonify({"error": "Anthropic library not available"}), 500

    # Build context from the extraction log
    log_path = job.get("output_log")
    extraction_context = ""
    if log_path and os.path.exists(log_path):
        with open(log_path) as f:
            log_data = json.load(f)
        exts = log_data.get("extractions", [])
        if page_num:
            page_exts = [e for e in exts if e.get("_page") == page_num]
            if page_exts:
                extraction_context = f"Extracted data for page {page_num}:\n{json.dumps(page_exts, indent=2, default=str)[:4000]}"
        if not extraction_context:
            summary = []
            for e in exts:
                p = e.get("_page", "?")
                dt = e.get("document_type", "?")
                ent = e.get("payer_or_entity", "?")
                summary.append(f"Page {p}: {dt} — {ent}")
            extraction_context = "Document summary:\n" + "\n".join(summary)

    # Include page image if available
    content = []
    if page_num:
        img_path = PAGES_DIR / job_id / f"page_{page_num}.jpg"
        if img_path.exists():
            with open(img_path, "rb") as f:
                img_b64 = base64.b64encode(f.read()).decode("utf-8")
            content.append({"type": "image", "source": {"type": "base64", "media_type": "image/jpeg", "data": img_b64}})

    prompt = f"""You are an assistant helping a CPA firm review tax document extractions.
The operator is reviewing extracted data and has a question.

{extraction_context}

Operator's question: {message}

Be concise and helpful. If the operator asks about a specific value, reference the extracted data. If they ask you to look at the page image, describe what you see."""

    content.append({"type": "text", "text": prompt})

    try:
        client = _anthropic.Anthropic()
        msg = client.messages.create(
            model="claude-sonnet-4-20250514", max_tokens=1500,
            messages=[{"role": "user", "content": content}]
        )
        reply = msg.content[0].text
        return jsonify({"reply": reply})
    except Exception as e:
        return jsonify({"error": f"AI call failed: {str(e)}"}), 500


@app.route("/api/page-image/<job_id>/<int:page_num>")
def page_image(job_id, page_num):
    img_path = PAGES_DIR / job_id / f"page_{page_num}.jpg"
    if img_path.exists():
        return send_file(str(img_path), mimetype="image/jpeg")
    abort(404)

@app.route("/api/download/<job_id>")
def download_xlsx(job_id):
    job = jobs.get(job_id)
    if not job or not job.get("output_xlsx"):
        abort(404)
    p = job["output_xlsx"]
    if os.path.exists(p):
        original_stem = Path(job.get("filename", "")).stem or job_id
        friendly = re.sub(r'[^\w\s\-\.,()]', '', original_stem).strip() or job_id
        return send_file(p, as_attachment=True, download_name=friendly + "_intake.xlsx")
    abort(404)

@app.route("/api/download-log/<job_id>")
def download_log(job_id):
    job = jobs.get(job_id)
    if not job or not job.get("output_log"):
        abort(404)
    p = job["output_log"]
    if os.path.exists(p):
        original_stem = Path(job.get("filename", "")).stem or job_id
        friendly = re.sub(r'[^\w\s\-\.,()]', '', original_stem).strip() or job_id
        return send_file(p, as_attachment=True, download_name=friendly + "_intake_log.json")
    abort(404)

def _sanitize_job(j):
    """Make a JSON-safe copy of a job dict (no None keys, no log/pdf_path)."""
    safe = {}
    for k, v in j.items():
        if k in ("log", "pdf_path"):
            continue
        if k == "stats" and isinstance(v, dict):
            v = dict(v)
            for sk in ("methods", "confidences"):
                if sk in v and isinstance(v[sk], dict):
                    v[sk] = {str(dk) if dk is None else dk: dv for dk, dv in v[sk].items()}
        safe[k] = v
    return safe

@app.route("/api/jobs")
def list_jobs():
    q = request.args.get("q", "").strip().lower()
    dtype = request.args.get("doc_type", "").strip()
    out = []
    for j in sorted(jobs.values(), key=lambda x: x.get("created", ""), reverse=True):
        if q and q not in j.get("client_name", "").lower() and q not in j.get("filename", "").lower():
            continue
        if dtype and j.get("doc_type", "") != dtype:
            continue
        out.append(_sanitize_job(j))
    return jsonify(out)

@app.route("/api/delete/<job_id>", methods=["POST"])
def delete_job(job_id):
    if job_id in jobs:
        del jobs[job_id]
        save_jobs()
        # Remove from database
        try:
            appdb.delete_job(job_id)
        except Exception:
            pass
        # Clean up page images
        job_pages = PAGES_DIR / job_id
        if job_pages.exists():
            import shutil
            shutil.rmtree(str(job_pages), ignore_errors=True)
    return jsonify({"ok": True})

# ─── Verification ────────────────────────────────────────────────────────────

def _verify_path(job_id):
    return VERIFY_DIR / f"{job_id}.json"

def _load_verifications(job_id):
    """Load verifications from JSON file (backward compat for _regen_excel, batch-categories)."""
    p = _verify_path(job_id)
    if p.exists():
        try:
            with open(p) as f:
                return json.load(f)
        except (ValueError, OSError):
            pass
    return {"fields": {}, "updated": None, "reviewer": ""}

def _save_verifications(job_id, data):
    """Save verifications to JSON file AND update job summary."""
    data["updated"] = datetime.now().isoformat()
    with open(_verify_path(job_id), "w") as f:
        json.dump(data, f, indent=2, default=str)
    _update_verify_summary(job_id, data)

def _update_verify_summary(job_id, vdata):
    job = jobs.get(job_id)
    if not job:
        return
    fields = vdata.get("fields", {})
    total = len(fields)
    confirmed = sum(1 for f in fields.values() if f.get("status") == "confirmed")
    corrected = sum(1 for f in fields.values() if f.get("status") == "corrected")
    flagged = sum(1 for f in fields.values() if f.get("status") == "flagged")
    job["verification"] = {
        "reviewed": confirmed + corrected + flagged,
        "confirmed": confirmed,
        "corrected": corrected,
        "flagged": flagged,
        "reviewer": vdata.get("reviewer", ""),
        "updated": vdata.get("updated"),
    }
    save_jobs()

@app.route("/api/verify/<job_id>", methods=["GET"])
def get_verifications(job_id):
    return jsonify(_load_verifications(job_id))

@app.route("/api/verify/<job_id>", methods=["POST"])
def save_verification(job_id):
    """Save one or more field verification decisions.

    Body: { "fields": { "page:extIdx:fieldName": { "status": "confirmed"|"corrected"|"flagged",
                                                     "corrected_value": ..., "note": ...,
                                                     "category": "Utilities",
                                                     "vendor_desc": "Georgia Power" } },
            "reviewer": "JW" }

    If a category and vendor_desc are present, also learns the mapping
    for future auto-suggest.
    """
    payload = request.get_json(silent=True) or {}
    data = _load_verifications(job_id)
    reviewer = payload.get("reviewer", data.get("reviewer", ""))
    data["reviewer"] = reviewer

    for key, decision in (payload.get("fields") or {}).items():
        if decision.get("status") == "_remove":
            data["fields"].pop(key, None)
        else:
            decision["timestamp"] = datetime.now().isoformat()
            decision["reviewer"] = reviewer
            data["fields"][key] = decision

            # Learn vendor → category mapping
            cat = decision.get("category", "")
            vendor = decision.get("vendor_desc", "")
            if cat and vendor:
                _learn_vendor_category(vendor, cat)

    _save_verifications(job_id, data)

    # Auto-regenerate Excel with corrections applied
    _regen_excel(job_id)

    return jsonify({"ok": True, "total_reviewed": len(data["fields"])})


# ─── Review Chain Routes ─────────────────────────────────────────────────────

@app.route("/api/users")
def api_list_users():
    """List all users with roles."""
    return jsonify(appdb.list_users())

@app.route("/api/inbox")
def api_inbox():
    """Get inbox items for a user."""
    user_id = request.args.get("user_id", "jeff")
    items = appdb.get_inbox(user_id)
    return jsonify({"user_id": user_id, "items": items, "total": sum(i["field_count"] for i in items)})

@app.route("/api/review/<job_id>/approve", methods=["POST"])
def api_approve_fields(job_id):
    """Bulk approve fields (reviewer or partner)."""
    payload = request.get_json(silent=True) or {}
    user_id = payload.get("user_id", "")
    field_ids = payload.get("field_ids", [])

    if not user_id or not field_ids:
        return jsonify({"error": "user_id and field_ids required"}), 400

    user = appdb.get_user(user_id)
    if not user:
        return jsonify({"error": f"Unknown user: {user_id}"}), 400

    job = jobs.get(job_id)
    client_name = job.get("client_name", "unknown") if job else "unknown"
    tax_year = str(job.get("year", "")) if job else ""

    approved = 0
    errors = []
    for fid in field_ids:
        full_fid = f"{job_id}:{fid}" if not fid.startswith(job_id + ":") else fid
        result = appdb.process_approve(client_name, tax_year, full_fid, user_id, user["role"])
        if result.get("ok"):
            approved += 1
        elif result.get("error"):
            errors.append({"field_id": fid, "error": result["error"]})

    return jsonify({"ok": True, "approved": approved, "errors": errors})

@app.route("/api/review/<job_id>/send-back", methods=["POST"])
def api_send_back_fields(job_id):
    """Send fields back for rework (reviewer or partner)."""
    payload = request.get_json(silent=True) or {}
    user_id = payload.get("user_id", "")
    field_ids = payload.get("field_ids", [])
    reason = payload.get("reason", "")
    send_to = payload.get("send_to")

    if not user_id or not field_ids:
        return jsonify({"error": "user_id and field_ids required"}), 400

    user = appdb.get_user(user_id)
    if not user:
        return jsonify({"error": f"Unknown user: {user_id}"}), 400

    job = jobs.get(job_id)
    client_name = job.get("client_name", "unknown") if job else "unknown"
    tax_year = str(job.get("year", "")) if job else ""

    sent_back = 0
    errors = []
    for fid in field_ids:
        full_fid = f"{job_id}:{fid}" if not fid.startswith(job_id + ":") else fid
        result = appdb.process_send_back(client_name, tax_year, full_fid, user_id, user["role"],
                                          reason=reason, send_to=send_to)
        if result.get("ok"):
            sent_back += 1
        elif result.get("error"):
            errors.append({"field_id": fid, "error": result["error"]})

    return jsonify({"ok": True, "sent_back": sent_back, "errors": errors})

@app.route("/api/review/<job_id>/override", methods=["POST"])
def api_override_field(job_id):
    """Override a field value (reviewer or partner)."""
    payload = request.get_json(silent=True) or {}
    user_id = payload.get("user_id", "")
    field_id = payload.get("field_id", "")
    new_value = payload.get("new_value")
    reason = payload.get("reason", "")

    if not user_id or not field_id:
        return jsonify({"error": "user_id and field_id required"}), 400

    user = appdb.get_user(user_id)
    if not user:
        return jsonify({"error": f"Unknown user: {user_id}"}), 400

    job = jobs.get(job_id)
    client_name = job.get("client_name", "unknown") if job else "unknown"
    tax_year = str(job.get("year", "")) if job else ""

    full_fid = f"{job_id}:{field_id}" if not field_id.startswith(job_id + ":") else field_id
    result = appdb.process_override(client_name, tax_year, full_fid, new_value, user_id, user["role"],
                                     reason=reason)
    if result.get("error"):
        return jsonify(result), 400

    # Also update the verifications JSON for Excel regen
    vdata = _load_verifications(job_id)
    short_key = field_id if not field_id.startswith(job_id + ":") else field_id[len(job_id) + 1:]
    vdata["fields"][short_key] = {
        "status": "corrected",
        "corrected_value": new_value,
        "note": f"Override by {user['display_name']}: {reason}",
        "timestamp": datetime.now().isoformat(),
        "reviewer": user_id,
    }
    _save_verifications(job_id, vdata)
    _regen_excel(job_id)

    return jsonify({"ok": True})

@app.route("/api/lock/<job_id>", methods=["POST"])
def api_lock_fields(job_id):
    """Acquire locks on fields."""
    payload = request.get_json(silent=True) or {}
    user_id = payload.get("user_id", "")
    field_ids = payload.get("field_ids", [])

    if not user_id:
        return jsonify({"error": "user_id required"}), 400

    job = jobs.get(job_id)
    client_name = job.get("client_name", "unknown") if job else "unknown"
    tax_year = str(job.get("year", "")) if job else ""

    full_fids = [f"{job_id}:{fid}" if not fid.startswith(job_id + ":") else fid for fid in field_ids]
    acquired, locked_by_others = appdb.bulk_acquire_lock(client_name, tax_year, full_fids, user_id)

    return jsonify({
        "acquired": len(acquired),
        "locked_by_others": [{"field_id": fid, "locked_by": lb} for fid, lb in locked_by_others],
    })

@app.route("/api/unlock/<job_id>", methods=["POST"])
def api_unlock_fields(job_id):
    """Release locks on fields."""
    payload = request.get_json(silent=True) or {}
    user_id = payload.get("user_id", "")
    field_ids = payload.get("field_ids", [])

    if not user_id:
        return jsonify({"error": "user_id required"}), 400

    job = jobs.get(job_id)
    client_name = job.get("client_name", "unknown") if job else "unknown"
    tax_year = str(job.get("year", "")) if job else ""

    full_fids = [f"{job_id}:{fid}" if not fid.startswith(job_id + ":") else fid for fid in field_ids]
    appdb.bulk_release_locks(client_name, tax_year, full_fids, user_id)

    return jsonify({"ok": True})

@app.route("/api/export-status/<job_id>")
def api_export_status(job_id):
    """Check if a job is ready for export (all fields partner-reviewed)."""
    states = appdb.get_review_states_for_job(job_id)
    total = len(states)
    partner_reviewed = sum(1 for s in states if s.get("stage") == "partner_reviewed")
    requires = appdb.get_config("EXPORT_REQUIRES_PARTNER_REVIEW") == "true"
    all_done = total == 0 or partner_reviewed == total

    return jsonify({
        "can_export": not requires or all_done,
        "total": total,
        "partner_reviewed": partner_reviewed,
        "pct": round(partner_reviewed / total * 100, 1) if total > 0 else 100,
        "draft": requires and not all_done,
        "requires_partner_review": requires,
    })

@app.route("/api/audit/<job_id>")
def api_audit_trail(job_id):
    """Get audit trail for a job."""
    events = appdb.get_audit_trail(job_id=job_id)
    return jsonify(events)


# ─── Guided Review Routes ─────────────────────────────────────────────────────

@app.route("/api/review-queue/<job_id>")
def api_review_queue(job_id):
    """Get ordered review queue for the current user's guided review session."""
    user_id = request.args.get("user_id", "jeff")
    user = appdb.get_user(user_id)
    if not user:
        return jsonify({"error": f"Unknown user: {user_id}"}), 400

    queue = appdb.get_review_queue(job_id, user_id, user["role"])

    # Enrich queue items with extraction metadata from the output log
    job = jobs.get(job_id)
    extraction_map = {}
    if job:
        log_path = job.get("output_log")
        if log_path and os.path.exists(log_path):
            try:
                with open(log_path) as f:
                    log_data = json.load(f)
                for ext in log_data.get("extractions", []):
                    page = ext.get("_page")
                    if page is not None:
                        extraction_map.setdefault(page, {})[0] = ext
            except Exception:
                pass

    enriched = []
    for item in queue:
        page = item["page"]
        ext_idx = item["ext_idx"]
        field_name = item["field_name"]

        ext = extraction_map.get(page, {}).get(ext_idx) or extraction_map.get(page, {}).get(0)
        label_on_form = ""
        confidence = ""
        extraction_method = ""
        doc_type = ""
        entity = ""

        if ext:
            doc_type = ext.get("document_type", "")
            entity = ext.get("payer_or_entity", "")
            extraction_method = ext.get("_extraction_method", "")
            fd = ext.get("fields", {}).get(field_name, {})
            if isinstance(fd, dict):
                label_on_form = fd.get("label_on_form", "")
                confidence = fd.get("confidence", "")

        enriched.append({
            "field_id": item["field_id"],
            "short_key": f"{page}:{ext_idx}:{field_name}",
            "page": page,
            "ext_idx": ext_idx,
            "field_name": field_name,
            "value": item["value"],
            "stage": item["stage"],
            "locked_by": item.get("locked_by", ""),
            "document_type": doc_type,
            "entity": entity,
            "label_on_form": label_on_form,
            "confidence": confidence,
            "extraction_method": extraction_method,
        })

    return jsonify({
        "queue": enriched,
        "total": len(enriched),
        "user_id": user_id,
        "role": user["role"],
    })


@app.route("/api/review-action/<job_id>/<path:field_key>", methods=["POST"])
def api_review_action(job_id, field_key):
    """Process a single guided review action."""
    payload = request.get_json(silent=True) or {}
    action = payload.get("action", "")
    user_id = payload.get("user_id", "")
    new_value = payload.get("new_value")
    reason = payload.get("reason", "")

    if not action or not user_id:
        return jsonify({"error": "action and user_id required"}), 400

    user = appdb.get_user(user_id)
    if not user:
        return jsonify({"error": f"Unknown user: {user_id}"}), 400

    job = jobs.get(job_id)
    client_name = job.get("client_name", "unknown") if job else "unknown"
    tax_year = str(job.get("year", "")) if job else ""
    role = user["role"]
    full_fid = f"{job_id}:{field_key}"

    locked = appdb.acquire_lock(client_name, tax_year, full_fid, user_id)
    if not locked:
        return jsonify({"error": "Field is locked by another user"}), 409

    result = {"ok": False}
    try:
        if action == "skip":
            result = {"ok": True, "action": "skip"}
        elif action == "confirm":
            if role == "preparer":
                fact = appdb.get_fact(client_name, tax_year, full_fid)
                current_val = fact["value"] if fact else None
                result = appdb.process_verify(client_name, tax_year, full_fid,
                                               current_val, user_id, role)
            else:
                result = appdb.process_approve(client_name, tax_year, full_fid,
                                                user_id, role)
        elif action == "edit":
            if not new_value and new_value != 0:
                result = {"error": "new_value required for edit action"}
            elif role == "preparer":
                result = appdb.process_verify(client_name, tax_year, full_fid,
                                               new_value, user_id, role, reason=reason)
            else:
                result = appdb.process_override(client_name, tax_year, full_fid,
                                                 new_value, user_id, role, reason=reason)
        elif action == "not_present":
            if role == "preparer":
                result = appdb.process_verify(client_name, tax_year, full_fid,
                                               None, user_id, role,
                                               reason="Marked not present")
            else:
                result = appdb.process_override(client_name, tax_year, full_fid,
                                                 None, user_id, role,
                                                 reason="Marked not present")
        elif action == "send_back":
            result = appdb.process_send_back(client_name, tax_year, full_fid,
                                              user_id, role, reason=reason)
        else:
            result = {"error": f"Unknown action: {action}"}

        # Sync verification JSON for Excel regen compatibility
        if action in ("confirm", "edit", "not_present") and result.get("ok"):
            vdata = _load_verifications(job_id)
            if action == "edit":
                vdata["fields"][field_key] = {
                    "status": "corrected",
                    "corrected_value": new_value,
                    "timestamp": datetime.now().isoformat(),
                    "reviewer": user_id,
                }
            elif action == "not_present":
                vdata["fields"][field_key] = {
                    "status": "corrected",
                    "corrected_value": None,
                    "note": "Not present",
                    "timestamp": datetime.now().isoformat(),
                    "reviewer": user_id,
                }
            else:
                vdata["fields"][field_key] = {
                    "status": "confirmed",
                    "timestamp": datetime.now().isoformat(),
                    "reviewer": user_id,
                }
            _save_verifications(job_id, vdata)
            _regen_excel(job_id)

    finally:
        appdb.release_lock(client_name, tax_year, full_fid, user_id)

    return jsonify(result)


@app.route("/api/review-undo/<job_id>/<path:field_key>", methods=["POST"])
def api_review_undo(job_id, field_key):
    """Undo the last review action on a field."""
    payload = request.get_json(silent=True) or {}
    user_id = payload.get("user_id", "")

    if not user_id:
        return jsonify({"error": "user_id required"}), 400

    user = appdb.get_user(user_id)
    if not user:
        return jsonify({"error": f"Unknown user: {user_id}"}), 400

    job = jobs.get(job_id)
    client_name = job.get("client_name", "unknown") if job else "unknown"
    tax_year = str(job.get("year", "")) if job else ""
    role = user["role"]
    full_fid = f"{job_id}:{field_key}"

    locked = appdb.acquire_lock(client_name, tax_year, full_fid, user_id)
    if not locked:
        return jsonify({"error": "Field is locked by another user"}), 409

    try:
        result = appdb.process_undo(client_name, tax_year, full_fid, user_id, role)

        if result.get("ok"):
            vdata = _load_verifications(job_id)
            if field_key in vdata.get("fields", {}):
                del vdata["fields"][field_key]
                _save_verifications(job_id, vdata)
                _regen_excel(job_id)
    finally:
        appdb.release_lock(client_name, tax_year, full_fid, user_id)

    return jsonify(result)


@app.route("/api/evidence/<job_id>/<path:field_key>")
def api_evidence(job_id, field_key):
    """Serve a page image with evidence highlighting for a field."""
    parts = field_key.split(":", 2)
    if len(parts) != 3:
        abort(400)
    try:
        page_num = int(parts[0])
        ext_idx = int(parts[1])
    except (ValueError, TypeError):
        abort(400)
    field_name = parts[2]

    cache_name = f"ev_{page_num}_{ext_idx}_{field_name.replace('/', '_')}.jpg"
    cache_path = PAGES_DIR / job_id / cache_name
    if cache_path.exists():
        return send_file(str(cache_path), mimetype="image/jpeg")

    page_img_path = PAGES_DIR / job_id / f"page_{page_num}.jpg"
    if not page_img_path.exists():
        abort(404)

    words_path = PAGES_DIR / job_id / f"page_{page_num}_words.json"
    words = None
    if words_path.exists():
        try:
            with open(str(words_path)) as f:
                words_data = json.load(f)
                words = words_data.get("words", [])
        except Exception:
            pass

    value_text = ""
    label_text = ""
    job = jobs.get(job_id)
    if job:
        log_path = job.get("output_log")
        if log_path and os.path.exists(log_path):
            try:
                with open(log_path) as f:
                    log_data = json.load(f)
                for ext in log_data.get("extractions", []):
                    if ext.get("_page") == page_num:
                        fd = ext.get("fields", {}).get(field_name, {})
                        if isinstance(fd, dict):
                            value_text = str(fd.get("value", "")) if fd.get("value") is not None else ""
                            label_text = fd.get("label_on_form", "")
                        elif fd is not None:
                            value_text = str(fd)
                        break
            except Exception:
                pass

    if not value_text and not label_text:
        return send_file(str(page_img_path), mimetype="image/jpeg")

    img = _draw_evidence_highlights(page_img_path, words, value_text, label_text)

    try:
        img.save(str(cache_path), "JPEG", quality=85)
    except Exception:
        pass

    buf = BytesIO()
    img.save(buf, format="JPEG", quality=85)
    buf.seek(0)
    return send_file(buf, mimetype="image/jpeg")


@app.route("/api/vendor-categories", methods=["GET"])
def get_vendor_categories():
    """Return the learned vendor → category map and the chart of accounts."""
    return jsonify({
        "vendors": _load_vendor_categories(),
        "chart_of_accounts": CHART_OF_ACCOUNTS,
    })


@app.route("/api/suggest-categories", methods=["POST"])
def suggest_categories():
    """Given a list of vendor descriptions, return category suggestions.

    Body: { "descriptions": ["GEORGIA POWER #1234", "WALMART SUPERCENTER", ...] }
    Returns: { "suggestions": {"GEORGIA POWER #1234": "Utilities", ...} }
    """
    payload = request.get_json(silent=True) or {}
    descriptions = payload.get("descriptions", [])
    suggestions = {}
    for desc in descriptions:
        cat = _suggest_category(desc)
        if cat:
            suggestions[desc] = cat
    return jsonify({"suggestions": suggestions})


# ─── Client Management Routes ────────────────────────────────────────────────

@app.route("/api/clients")
def list_clients():
    """List all known clients with basic stats."""
    clients = {}
    # Gather from jobs
    for jid, j in jobs.items():
        cn = j.get("client_name", "")
        if not cn:
            continue
        safe = _safe_client_name(cn)
        if safe not in clients:
            clients[safe] = {"name": safe, "jobs": 0, "latest": "", "years": set(),
                             "has_context": False, "has_instructions": False}
        clients[safe]["jobs"] += 1
        clients[safe]["years"].add(str(j.get("year", "")))
        ts = j.get("created", "")
        if ts > clients[safe]["latest"]:
            clients[safe]["latest"] = ts

    # Check for context and instructions files
    if CLIENTS_DIR.exists():
        for d in CLIENTS_DIR.iterdir():
            if d.is_dir():
                name = d.name
                if name not in clients:
                    clients[name] = {"name": name, "jobs": 0, "latest": "",
                                     "years": set(), "has_context": False, "has_instructions": False}
                ctx_idx = d / "context" / "index.json"
                if ctx_idx.exists():
                    clients[name]["has_context"] = True
                instr = d / "instructions.json"
                if instr.exists():
                    clients[name]["has_instructions"] = True

    result = []
    for c in sorted(clients.values(), key=lambda x: x.get("latest", ""), reverse=True):
        c["years"] = sorted(c["years"])
        # Include client metadata if available
        info = _load_client_info(c["name"])
        if info:
            c["ein_last4"] = info.get("ein_last4", "")
            c["contact"] = info.get("contact", "")
            c["notes"] = info.get("notes", "")
        result.append(c)
    return jsonify(result)


@app.route("/api/clients/create", methods=["POST"])
def create_client():
    """Create a new client with metadata."""
    data = request.get_json(force=True)
    name = (data.get("name") or "").strip()
    if not name:
        return jsonify({"error": "Client name is required"}), 400
    safe = _safe_client_name(name)
    # Create directory
    client_dir = CLIENTS_DIR / safe
    client_dir.mkdir(parents=True, exist_ok=True)
    info = {
        "name": safe,
        "ein_last4": (data.get("ein_last4") or "").strip()[:4],
        "contact": (data.get("contact") or "").strip()[:200],
        "notes": (data.get("notes") or "").strip()[:1000],
        "created": datetime.now().isoformat(),
    }
    _save_client_info(safe, info)
    return jsonify({"ok": True, "name": safe})


@app.route("/api/clients/<path:client_name>/info", methods=["GET"])
def get_client_info(client_name):
    """Get client metadata."""
    info = _load_client_info(client_name)
    if not info:
        return jsonify({"name": _safe_client_name(client_name)})
    return jsonify(info)


@app.route("/api/clients/<path:client_name>/info", methods=["PUT"])
def update_client_info(client_name):
    """Update client metadata."""
    data = request.get_json(force=True)
    safe = _safe_client_name(client_name)
    info = _load_client_info(safe) or {"name": safe, "created": datetime.now().isoformat()}
    if "ein_last4" in data:
        info["ein_last4"] = (data["ein_last4"] or "").strip()[:4]
    if "contact" in data:
        info["contact"] = (data["contact"] or "").strip()[:200]
    if "notes" in data:
        info["notes"] = (data["notes"] or "").strip()[:1000]
    _save_client_info(safe, info)
    return jsonify({"ok": True})


@app.route("/api/clients/<path:client_name>/documents", methods=["GET"])
def get_client_documents(client_name):
    """List all extraction jobs for a client, grouped by document type."""
    safe = _safe_client_name(client_name)
    docs = []
    for jid, j in jobs.items():
        jclient = _safe_client_name(j.get("client_name", ""))
        if jclient != safe:
            continue
        # Check for output files
        has_xlsx = False
        has_log = False
        if j.get("status") == "complete":
            xlsx_path = OUTPUT_DIR / f"{jid}.xlsx"
            log_path = OUTPUT_DIR / f"{jid}_log.json"
            has_xlsx = xlsx_path.exists()
            has_log = log_path.exists()
        docs.append({
            "job_id": jid,
            "filename": j.get("filename", ""),
            "doc_type": j.get("doc_type", ""),
            "year": j.get("year", ""),
            "status": j.get("status", ""),
            "cost_usd": j.get("cost_usd"),
            "created": j.get("created", ""),
            "has_xlsx": has_xlsx,
            "has_log": has_log,
        })
    # Sort by created descending
    docs.sort(key=lambda d: d.get("created", ""), reverse=True)
    # Group by doc_type
    grouped = {}
    for d in docs:
        dt = d["doc_type"] or "other"
        if dt not in grouped:
            grouped[dt] = []
        grouped[dt].append(d)
    return jsonify({"documents": docs, "grouped": grouped})


# ─── Prior-Year Context Routes ────────────────────────────────────────────────

@app.route("/api/context/<path:client_name>", methods=["GET"])
def get_context(client_name):
    """Get the context index for a client."""
    idx = _load_context_index(client_name)
    return jsonify(idx)


@app.route("/api/context/<path:client_name>/upload", methods=["POST"])
def upload_context(client_name):
    """Upload a context document (prior-year return, workbook, notes)."""
    if "file" not in request.files:
        return jsonify({"error": "No file uploaded"}), 400
    f = request.files["file"]
    fname = f.filename or "document"
    ext = Path(fname).suffix.lower()
    if ext not in (".pdf", ".xlsx", ".xls", ".txt", ".csv"):
        return jsonify({"error": "Supported formats: PDF, XLSX, XLS, TXT, CSV"}), 400

    doc_label = request.form.get("label", "").strip() or fname
    doc_year = request.form.get("year", "").strip()

    ctx_dir = _context_dir(client_name)
    safe_fname = re.sub(r'[^\w\s\-\.,()]', '', fname).strip() or "document" + ext
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    saved_name = f"{ts}_{safe_fname}"
    saved_path = ctx_dir / saved_name
    f.save(str(saved_path))

    # Parse the document
    parsed = _parse_context_document(str(saved_path), doc_label)
    if doc_year:
        parsed["year"] = doc_year

    # Update the index
    idx = _load_context_index(client_name)
    doc_entry = {
        "id": ts,
        "filename": saved_name,
        "original_name": fname,
        "label": doc_label,
        "year": doc_year,
        "uploaded": datetime.now().isoformat(),
        "payer_count": len(parsed.get("payers", [])),
        "has_text": bool(parsed.get("raw_text", "").strip()),
    }
    idx["documents"].append(doc_entry)

    # Merge payers into prior_year_data
    if not idx.get("prior_year_data"):
        idx["prior_year_data"] = {"documents": []}
    idx["prior_year_data"]["documents"].append(parsed)

    _save_context_index(client_name, idx)

    # Save parsed data separately for quick access
    parsed_path = ctx_dir / f"{ts}_parsed.json"
    with open(parsed_path, "w") as pf:
        json.dump(parsed, pf, indent=2, default=str)

    return jsonify({"ok": True, "document": doc_entry, "payers_found": len(parsed.get("payers", []))})


@app.route("/api/context/<path:client_name>/<doc_id>", methods=["DELETE"])
def delete_context(client_name, doc_id):
    """Delete a context document."""
    idx = _load_context_index(client_name)
    ctx_dir = _context_dir(client_name)

    new_docs = []
    removed = False
    for doc in idx.get("documents", []):
        if doc.get("id") == doc_id:
            # Delete file
            fpath = ctx_dir / doc.get("filename", "")
            if fpath.exists():
                try:
                    os.remove(str(fpath))
                except OSError:
                    pass
            # Delete parsed JSON
            parsed = ctx_dir / f"{doc_id}_parsed.json"
            if parsed.exists():
                try:
                    os.remove(str(parsed))
                except OSError:
                    pass
            removed = True
        else:
            new_docs.append(doc)

    idx["documents"] = new_docs

    # Rebuild prior_year_data from remaining parsed files
    idx["prior_year_data"] = {"documents": []}
    for doc in new_docs:
        parsed_path = ctx_dir / f"{doc['id']}_parsed.json"
        if parsed_path.exists():
            try:
                with open(parsed_path) as pf:
                    idx["prior_year_data"]["documents"].append(json.load(pf))
            except (json.JSONDecodeError, IOError):
                pass

    _save_context_index(client_name, idx)
    return jsonify({"ok": removed})


@app.route("/api/context/<path:client_name>/completeness", methods=["GET"])
def completeness_report(client_name):
    """Generate a completeness report comparing current extractions to prior year."""
    # Gather current extractions for this client
    current_exts = []
    for jid, j in jobs.items():
        if j.get("status") != "complete":
            continue
        if _safe_client_name(j.get("client_name", "")) != _safe_client_name(client_name):
            continue
        log_path = j.get("output_log")
        if not log_path or not os.path.exists(log_path):
            continue
        try:
            with open(log_path) as f:
                log_data = json.load(f)
            current_exts.extend(log_data.get("extractions", []))
        except (json.JSONDecodeError, IOError):
            pass

    year = str(datetime.now().year)
    report = _build_completeness_report(client_name, current_exts, year)
    return jsonify(report)


# ─── Client Instructions Routes ──────────────────────────────────────────────

@app.route("/api/instructions/<path:client_name>", methods=["GET"])
def get_instructions(client_name):
    """Get client instructions."""
    return jsonify(_load_instructions(client_name))


@app.route("/api/instructions/<path:client_name>", methods=["POST"])
def save_instruction(client_name):
    """Add or update a client instruction.

    Body: { "text": "...", "id": "..." (optional, for update) }
    """
    payload = request.get_json(silent=True) or {}
    text = payload.get("text", "").strip()
    if not text:
        return jsonify({"error": "Instruction text is required"}), 400
    if len(text) > 500:
        return jsonify({"error": "Instruction too long (max 500 chars)"}), 400

    data = _load_instructions(client_name)
    rule_id = payload.get("id", "")

    if rule_id:
        # Update existing
        for rule in data["rules"]:
            if rule.get("id") == rule_id:
                rule["text"] = text
                rule["updated"] = datetime.now().isoformat()
                break
    else:
        # Add new
        rule_id = datetime.now().strftime("%Y%m%d%H%M%S") + str(len(data["rules"]))
        data["rules"].append({
            "id": rule_id,
            "text": text,
            "created": datetime.now().isoformat(),
        })

    _save_instructions(client_name, data)
    return jsonify({"ok": True, "id": rule_id, "total": len(data["rules"])})


@app.route("/api/instructions/<path:client_name>/<rule_id>", methods=["DELETE"])
def delete_instruction(client_name, rule_id):
    """Delete a client instruction."""
    data = _load_instructions(client_name)
    data["rules"] = [r for r in data["rules"] if r.get("id") != rule_id]
    _save_instructions(client_name, data)
    return jsonify({"ok": True, "total": len(data["rules"])})


# ─── Batch Categorization Routes ─────────────────────────────────────────────

@app.route("/api/batch-categories", methods=["GET"])
def batch_categories():
    """Get all uncategorized transactions for batch categorization."""
    client_name = request.args.get("client", "")
    show_all = request.args.get("all", "false") == "true"
    items = _gather_uncategorized(client_name=client_name if client_name else None)
    if not show_all:
        items = [i for i in items if not i.get("current_category")]
    # Group by normalized vendor
    groups = {}
    for item in items:
        key = item.get("vendor_norm") or item.get("desc", "")[:40]
        if key not in groups:
            groups[key] = {
                "vendor": key,
                "display_name": item.get("desc", key),
                "suggested": item.get("suggested_category", ""),
                "current": item.get("current_category", ""),
                "count": 0,
                "total_amount": 0,
                "items": [],
            }
        groups[key]["count"] += 1
        amt = item.get("amount", 0)
        if isinstance(amt, (int, float)):
            groups[key]["total_amount"] += abs(amt)
        groups[key]["items"].append(item)

    sorted_groups = sorted(groups.values(), key=lambda g: g["count"], reverse=True)
    total = len(items)
    categorized = sum(1 for i in items if i.get("current_category"))
    return jsonify({
        "groups": sorted_groups,
        "total": total,
        "categorized": categorized,
        "uncategorized": total - categorized,
        "chart_of_accounts": CHART_OF_ACCOUNTS,
    })


@app.route("/api/batch-categories/apply", methods=["POST"])
def apply_batch_categories():
    """Apply a category to a batch of transactions.

    Body: { "vendor": "VENDOR_NORM", "category": "Utilities",
            "items": [{job_id, field_key, desc}, ...],
            "learn": true }
    """
    payload = request.get_json(silent=True) or {}
    category = payload.get("category", "").strip()
    items = payload.get("items", [])
    learn = payload.get("learn", True)
    vendor = payload.get("vendor", "")

    if not category or not items:
        return jsonify({"error": "category and items required"}), 400

    applied = 0
    for item in items:
        jid = item.get("job_id")
        field_key = item.get("field_key")
        desc = item.get("desc", "")
        if not jid or not field_key:
            continue

        vdata = _load_verifications(jid)
        decision = vdata["fields"].get(field_key, {})
        decision["category"] = category
        decision["vendor_desc"] = desc
        decision["timestamp"] = datetime.now().isoformat()
        decision["reviewer"] = "BATCH"
        if not decision.get("status"):
            decision["status"] = "confirmed"
        vdata["fields"][field_key] = decision
        _save_verifications(jid, vdata)
        applied += 1

    if learn and vendor:
        _learn_vendor_category(vendor, category)

    # Regen Excel for affected jobs
    affected_jobs = set(item.get("job_id") for item in items if item.get("job_id"))
    for jid in affected_jobs:
        _regen_excel(jid)

    return jsonify({"ok": True, "applied": applied})


# ─── Excel Regeneration ──────────────────────────────────────────────────────

def _regen_excel(job_id):
    """Regenerate the Excel file with operator verification corrections applied."""
    job = jobs.get(job_id)
    if not job or job.get("status") != "complete":
        return False

    log_path = job.get("output_log")
    xlsx_path = job.get("output_xlsx")
    if not log_path or not xlsx_path or not os.path.exists(log_path):
        return False

    vdata = _load_verifications(job_id)
    corrections = vdata.get("fields", {})
    if not corrections:
        return True  # Nothing to apply

    try:
        import openpyxl
        from openpyxl.styles import Font, PatternFill
        from openpyxl.comments import Comment

        # Load the extraction log
        with open(log_path) as f:
            log_data = json.load(f)

        extractions = log_data.get("extractions", [])
        if not extractions:
            return False

        # Apply corrections to the extraction data
        corrected_count = 0
        for key, decision in corrections.items():
            if decision.get("status") != "corrected":
                continue
            parts = key.split(":")
            if len(parts) != 3:
                continue
            page_str, ext_idx_str, field_name = parts

            corrected_value = decision.get("corrected_value")
            if corrected_value is None:
                continue

            # Find the matching extraction by page
            page_num = int(page_str) if page_str.isdigit() else None
            ext_idx = int(ext_idx_str) if ext_idx_str.isdigit() else None
            if page_num is None or ext_idx is None:
                continue

            # Match: find extractions for this page
            page_exts = [e for e in extractions if e.get("_page") == page_num]
            if ext_idx < len(page_exts):
                ext = page_exts[ext_idx]
                fields = ext.get("fields", {})
                if field_name in fields:
                    fdata = fields[field_name]
                    old_val = fdata.get("value") if isinstance(fdata, dict) else fdata
                    # Try to convert to number if it looks numeric
                    try:
                        new_val = float(str(corrected_value).replace(",", ""))
                    except (ValueError, TypeError):
                        new_val = corrected_value
                    if isinstance(fdata, dict):
                        fdata["_original_value"] = old_val
                        fdata["value"] = new_val
                        fdata["confidence"] = "operator_corrected"
                    else:
                        fields[field_name] = {
                            "value": new_val,
                            "_original_value": old_val,
                            "confidence": "operator_corrected",
                        }
                    corrected_count += 1

        # Inject operator-assigned categories into extraction fields
        # These flow through to _build_journal_entries to replace "Unclassified"
        for key, decision in corrections.items():
            cat = decision.get("category", "")
            if not cat:
                continue
            parts = key.split(":")
            if len(parts) != 3:
                continue
            page_str, ext_idx_str, field_name = parts
            page_num = int(page_str) if page_str.isdigit() else None
            ext_idx = int(ext_idx_str) if ext_idx_str.isdigit() else None
            if page_num is None or ext_idx is None:
                continue
            page_exts = [e for e in extractions if e.get("_page") == page_num]
            if ext_idx < len(page_exts):
                ext = page_exts[ext_idx]
                fields = ext.get("fields", {})
                # Store category on the field itself so _build_journal_entries can read it
                if field_name in fields:
                    fdata = fields[field_name]
                    if isinstance(fdata, dict):
                        fdata["_operator_category"] = cat
                    else:
                        fields[field_name] = {"value": fdata, "_operator_category": cat}
                # Also store vendor description for the extraction log
                vendor = decision.get("vendor_desc", "")
                if vendor:
                    if isinstance(fields.get(field_name), dict):
                        fields[field_name]["_vendor_desc"] = vendor

        # Now re-run populate_template via extract.py as a subprocess
        # This is the safest way — extract.py has all the Excel formatting logic
        import subprocess
        year = job.get("year", "2024")
        cmd = [
            sys.executable, str(BASE_DIR / "extract.py"),
            "--regen-excel",
            "--log-input", log_path,
            "--output", xlsx_path,
            "--year", str(year),
        ]

        # Write the corrected log to a temp file for extract.py to read
        corrected_log_path = log_path.replace("_log.json", "_corrected_log.json")
        with open(corrected_log_path, "w") as f:
            json.dump(log_data, f, indent=2, default=str)

        cmd[4] = corrected_log_path  # --log-input uses the corrected version

        proc = subprocess.run(cmd, capture_output=True, text=True, cwd=str(BASE_DIR), timeout=30)

        # Clean up temp file
        try:
            os.remove(corrected_log_path)
        except OSError:
            pass

        if proc.returncode != 0:
            print(f"  Regen subprocess failed (rc={proc.returncode}): {proc.stderr[:500] if proc.stderr else 'no stderr'}")
            # Fallback: apply corrections directly to existing Excel
            _apply_corrections_to_excel(xlsx_path, corrections, vdata.get("reviewer", ""))
        elif corrected_count > 0:
            # Verify corrections actually made it into the new Excel
            # (The subprocess rewrote the file using corrected extraction data,
            # so operator_corrected confidence values should appear)
            print(f"  ✓ Regen complete: {corrected_count} corrections applied via extract.py")

        # Always add the audit trail worksheet (primary path doesn't create one)
        _add_audit_trail_worksheet(xlsx_path, corrections, vdata.get("reviewer", ""))

        # Copy updated Excel to client folder
        if job.get("client_folder"):
            import shutil
            client_dir = Path(job["client_folder"])
            if client_dir.exists():
                try:
                    dst = client_dir / Path(xlsx_path).name
                    shutil.copy2(xlsx_path, str(dst))
                except Exception:
                    pass

        return True

    except Exception as e:
        print(f"  Excel regen error: {e}")
        # Fallback: try direct Excel patching
        try:
            _apply_corrections_to_excel(xlsx_path, corrections, vdata.get("reviewer", ""))
        except Exception:
            pass
        return False


def _add_audit_trail_worksheet(xlsx_path, corrections, reviewer=""):
    """Add or replace the Audit Trail worksheet with all verification decisions."""
    if not os.path.exists(xlsx_path) or not corrections:
        return
    try:
        import openpyxl
        from openpyxl.styles import Font, PatternFill, Alignment, Border, Side

        wb = openpyxl.load_workbook(xlsx_path)
        audit_name = "Audit Trail"
        if audit_name in wb.sheetnames:
            del wb[audit_name]
        ws = wb.create_sheet(audit_name)

        # Title
        ws["A1"] = "Operator Verification Audit Trail"
        ws["A1"].font = Font(bold=True, size=14, color="1A252F")
        ws.merge_cells("A1:G1")
        ws["A2"] = f"Generated {datetime.now().strftime('%m/%d/%Y %I:%M %p')}"
        ws["A2"].font = Font(italic=True, color="888888", size=9)
        ws["A3"] = f"Reviewer: {reviewer}" if reviewer else "Reviewer: (not specified)"
        ws["A3"].font = Font(bold=True, size=11)

        # Headers
        row = 5
        header_fill = PatternFill(start_color="2C3E50", end_color="2C3E50", fill_type="solid")
        header_font = Font(bold=True, color="FFFFFF", size=10)
        headers = ["Page:Field", "Status", "Original Value", "Corrected Value", "Reviewer", "Timestamp", "Note"]
        for i, h in enumerate(headers):
            cell = ws.cell(row=row, column=i+1, value=h)
            cell.font = header_font
            cell.fill = header_fill
            cell.alignment = Alignment(horizontal="center" if i == 1 else "left")
        row += 1

        # Data rows with alternating colors
        alt_fill = PatternFill(start_color="F8F9FA", end_color="F8F9FA", fill_type="solid")
        status_fills = {
            "corrected": PatternFill(start_color="FFF9C4", end_color="FFF9C4", fill_type="solid"),
            "confirmed": PatternFill(start_color="C8E6C9", end_color="C8E6C9", fill_type="solid"),
            "flagged": PatternFill(start_color="FFE0B2", end_color="FFE0B2", fill_type="solid"),
        }
        thin_border = Border(bottom=Side(style="thin", color="E0E0E0"))

        for idx, key in enumerate(sorted(corrections.keys())):
            decision = corrections[key]
            parts = key.split(":")
            if len(parts) == 3:
                page_label = f"Pg {parts[0]}"
                field_label = parts[2].replace("_", " ").title()
                field_display = f"{page_label}: {field_label}"
            else:
                field_display = key

            status = decision.get("status", "")
            original = decision.get("original_value", "")
            corrected = decision.get("corrected_value", "")
            rev = decision.get("reviewer", reviewer)
            ts = decision.get("timestamp", "")
            note = decision.get("note", "")

            ws.cell(row=row, column=1, value=field_display)
            status_cell = ws.cell(row=row, column=2, value=status.upper())
            status_cell.alignment = Alignment(horizontal="center")
            ws.cell(row=row, column=3, value=str(original) if original else "")
            ws.cell(row=row, column=4, value=str(corrected) if corrected else "")
            ws.cell(row=row, column=5, value=rev)
            ws.cell(row=row, column=6, value=ts)
            ws.cell(row=row, column=7, value=note)

            # Status color
            if status in status_fills:
                status_cell.fill = status_fills[status]
            # Alternating row background
            if idx % 2 == 1:
                for c in range(1, 8):
                    cell = ws.cell(row=row, column=c)
                    if cell.fill == PatternFill():  # only if not already colored
                        cell.fill = alt_fill
            # Border
            for c in range(1, 8):
                ws.cell(row=row, column=c).border = thin_border
            row += 1

        # Summary
        row += 1
        confirmed = sum(1 for d in corrections.values() if d.get("status") == "confirmed")
        corrected = sum(1 for d in corrections.values() if d.get("status") == "corrected")
        flagged = sum(1 for d in corrections.values() if d.get("status") == "flagged")
        ws.cell(row=row, column=1, value="Summary:").font = Font(bold=True, size=11)
        row += 1
        summary_items = [
            ("Confirmed", confirmed, "C8E6C9"),
            ("Corrected", corrected, "FFF9C4"),
            ("Flagged", flagged, "FFE0B2"),
            ("Total Reviewed", len(corrections), "E0E0E0"),
        ]
        for label, count, color in summary_items:
            ws.cell(row=row, column=1, value=label)
            ct_cell = ws.cell(row=row, column=2, value=count)
            ct_cell.font = Font(bold=True, size=11)
            ct_cell.fill = PatternFill(start_color=color, end_color=color, fill_type="solid")
            ct_cell.alignment = Alignment(horizontal="center")
            row += 1

        # Column widths
        ws.column_dimensions["A"].width = 34
        ws.column_dimensions["B"].width = 14
        ws.column_dimensions["C"].width = 18
        ws.column_dimensions["D"].width = 18
        ws.column_dimensions["E"].width = 10
        ws.column_dimensions["F"].width = 24
        ws.column_dimensions["G"].width = 32

        # Print setup
        ws.sheet_properties.pageSetUpPr = openpyxl.worksheet.properties.PageSetupProperties(fitToPage=True)
        ws.page_setup.fitToWidth = 1
        ws.page_setup.fitToHeight = 0
        ws.page_setup.orientation = "landscape"

        wb.save(xlsx_path)
    except Exception as e:
        print(f"  Audit trail worksheet error: {e}")


def _apply_corrections_to_excel(xlsx_path, corrections, reviewer=""):
    """Direct fallback: patch existing Excel cells with corrected values + add audit sheet."""
    if not os.path.exists(xlsx_path):
        return

    import openpyxl
    from openpyxl.styles import Font, PatternFill
    from openpyxl.comments import Comment

    wb = openpyxl.load_workbook(xlsx_path)
    ws = wb.active

    # Color for operator-corrected cells
    op_fill = PatternFill(start_color="B3E5FC", end_color="B3E5FC", fill_type="solid")

    # Build a list of corrections to apply: (original_value, corrected_value, field_name)
    pending = []
    for key, decision in corrections.items():
        if decision.get("status") != "corrected" or decision.get("corrected_value") is None:
            continue
        parts = key.split(":")
        field_name = parts[2] if len(parts) == 3 else key
        orig = decision.get("original_value")
        corr = decision.get("corrected_value")
        # Normalize to number if possible
        try:
            corr_num = float(str(corr).replace(",", ""))
        except (ValueError, TypeError):
            corr_num = None
        try:
            orig_num = float(str(orig).replace(",", "")) if orig is not None else None
        except (ValueError, TypeError):
            orig_num = None
        pending.append({
            "field": field_name,
            "orig": orig, "orig_num": orig_num,
            "corr": corr, "corr_num": corr_num,
            "reviewer": decision.get("reviewer", reviewer),
            "applied": False,
        })

    # Scan all data cells and match by original value
    # Strategy: for each cell with a value, check if it matches any pending correction's
    # original value. Apply the first match and mark it done. This is best-effort but
    # far better than the previous no-op.
    patched = 0
    for row in ws.iter_rows(min_row=1, max_row=ws.max_row, max_col=ws.max_column):
        for cell in row:
            if cell.value is None:
                continue
            for p in pending:
                if p["applied"]:
                    continue
                matched = False
                # Numeric match (within tolerance for floating point)
                if p["orig_num"] is not None and isinstance(cell.value, (int, float)):
                    if abs(cell.value - p["orig_num"]) < 0.005:
                        matched = True
                # String match
                elif p["orig"] is not None and isinstance(cell.value, str):
                    if str(cell.value).strip() == str(p["orig"]).strip():
                        matched = True
                # String-to-number: cell has number but orig was stored as string
                elif p["orig_num"] is not None and isinstance(cell.value, (int, float)):
                    pass  # already handled above
                elif p["orig"] is not None and isinstance(cell.value, (int, float)):
                    try:
                        if abs(cell.value - float(str(p["orig"]).replace(",", ""))) < 0.005:
                            matched = True
                    except (ValueError, TypeError):
                        pass

                if matched:
                    # Apply correction
                    if p["corr_num"] is not None and isinstance(cell.value, (int, float)):
                        cell.value = p["corr_num"]
                    else:
                        cell.value = p["corr"]
                    cell.fill = op_fill
                    old_display = str(p["orig"]) if p["orig"] is not None else "?"
                    cell.comment = Comment(
                        f"Operator corrected (was {old_display}) — {p['reviewer']}",
                        "Operator"
                    )
                    p["applied"] = True
                    patched += 1
                    break  # move to next cell

    wb.save(xlsx_path)

    # Add audit trail worksheet
    _add_audit_trail_worksheet(xlsx_path, corrections, reviewer)


@app.route("/api/regen-excel/<job_id>", methods=["POST"])
def regen_excel(job_id):
    """Manually trigger Excel regeneration with verification corrections."""
    job = jobs.get(job_id)
    if not job or job.get("status") != "complete":
        return jsonify({"error": "Job not found or not complete"}), 404
    ok = _regen_excel(job_id)
    return jsonify({"ok": ok})


@app.route("/api/clients/<path:client_name>/generate-report", methods=["POST"])
def generate_report(client_name):
    """Generate a combined Excel report from multiple extraction jobs."""
    data = request.get_json(force=True)
    job_ids = data.get("job_ids", [])
    output_format = data.get("output_format", "tax_review")
    year = data.get("year", str(datetime.now().year))

    if not job_ids:
        return jsonify({"error": "No jobs selected"}), 400

    # Gather extractions from all selected jobs
    combined_extractions = []
    for jid in job_ids:
        job = jobs.get(jid)
        if not job or job.get("status") != "complete":
            continue
        log_path = job.get("output_log")
        if not log_path or not os.path.exists(log_path):
            continue
        try:
            with open(log_path) as f:
                log_data = json.load(f)
            combined_extractions.extend(log_data.get("extractions", []))
        except Exception:
            continue

    if not combined_extractions:
        return jsonify({"error": "No extraction data found in selected jobs"}), 400

    # Write combined log
    safe = _safe_client_name(client_name)
    report_id = f"report-{safe}-{datetime.now().strftime('%Y%m%d-%H%M%S')}"
    combined_log = {
        "extractions": combined_extractions,
        "output_format": output_format,
        "year": year,
    }
    combined_path = OUTPUT_DIR / f"{report_id}_log.json"
    with open(combined_path, "w") as f:
        json.dump(combined_log, f)

    # Generate Excel via extract.py
    output_path = OUTPUT_DIR / f"{report_id}.xlsx"
    try:
        cmd = [
            sys.executable, str(BASE_DIR / "extract.py"),
            "--regen-excel",
            "--log-input", str(combined_path),
            "--output", str(output_path),
            "--year", str(year),
            "--output-format", output_format,
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
        if result.returncode != 0:
            return jsonify({"error": f"Report generation failed: {result.stderr[:500]}"}), 500
    except Exception as e:
        return jsonify({"error": str(e)}), 500

    if not output_path.exists():
        return jsonify({"error": "Report file was not created"}), 500

    return jsonify({"ok": True, "filename": report_id, "download_url": f"/api/download-report/{report_id}"})


@app.route("/api/download-report/<report_id>")
def download_report(report_id):
    """Download a generated report."""
    safe_id = re.sub(r'[^\w\-]', '', report_id)
    path = OUTPUT_DIR / f"{safe_id}.xlsx"
    if not path.exists():
        return jsonify({"error": "Report not found"}), 404
    return send_file(str(path), as_attachment=True, download_name=f"{safe_id}.xlsx")


# ─── Retry Failed/Interrupted Jobs ──────────────────────────────────────────

@app.route("/api/retry/<job_id>", methods=["POST"])
def retry_job(job_id):
    """Re-run extraction for a failed or interrupted job using the original PDF."""
    job = jobs.get(job_id)
    if not job:
        return jsonify({"error": "Job not found"}), 404

    if job.get("status") not in ("error", "interrupted", "failed"):
        return jsonify({"error": f"Cannot retry a job with status '{job.get('status')}'. Only failed, error, or interrupted jobs can be retried."}), 400

    pdf_path = job.get("pdf_path", "")
    if not pdf_path or not os.path.exists(pdf_path):
        return jsonify({"error": "Original PDF no longer exists. Please re-upload."}), 410

    # Reset job state for re-run
    job["status"] = "queued"
    job["stage"] = "queued"
    job["progress"] = 0
    job["log"] = []
    job["error"] = ""
    job.pop("end_time", None)
    job["retry_count"] = job.get("retry_count", 0) + 1
    job["last_retry"] = datetime.now().isoformat()
    save_jobs()

    # Rebuild client folder if needed
    client_dir = job.get("client_folder")
    if client_dir:
        Path(client_dir).mkdir(parents=True, exist_ok=True)

    year = job.get("year", "2024")
    skip_verify = False  # Always verify on retry
    doc_type = job.get("doc_type", "tax_returns")
    output_format = job.get("output_format", "tax_review")
    user_notes = job.get("user_notes", "")
    ai_instructions = job.get("ai_instructions", "")
    disable_pii = job.get("disable_pii", False)
    use_ocr_first = job.get("use_ocr_first", False)

    t = threading.Thread(target=run_extraction, kwargs=dict(
        job_id=job_id, pdf_path=pdf_path, year=year, skip_verify=skip_verify,
        doc_type=doc_type, output_format=output_format, user_notes=user_notes,
        ai_instructions=ai_instructions, disable_pii=disable_pii, resume=True,
        use_ocr_first=use_ocr_first,
    ))
    t.daemon = True
    t.start()

    return jsonify({"job_id": job_id, "retry_count": job["retry_count"]})


@app.route("/api/cancel/<job_id>", methods=["POST"])
def cancel_job(job_id):
    """Cancel a running extraction job."""
    job = jobs.get(job_id)
    if not job:
        return jsonify({"error": "Job not found"}), 404

    if job.get("status") not in ("queued", "running"):
        return jsonify({"error": "Job is not running"}), 400

    proc = _active_procs.get(job_id)
    if proc:
        try:
            proc.terminate()
        except Exception:
            pass
        _active_procs.pop(job_id, None)

    job["status"] = "interrupted"
    job["end_time"] = datetime.now().isoformat()
    job["error"] = "Cancelled by user"
    job["log"].append("── Cancelled by user ──")
    save_jobs()

    return jsonify({"success": True})


# ─── Health Check ─────────────────────────────────────────────────────────────

@app.route("/api/health")
def health_check():
    """System health check: version, uptime, job counts, dependency status, disk usage."""
    import shutil as _shutil

    now = datetime.now()
    uptime_seconds = (now - _start_time).total_seconds()

    # Job counts by status
    status_counts = {}
    for j in jobs.values():
        s = j.get("status", "unknown")
        status_counts[s] = status_counts.get(s, 0) + 1

    # Dependency checks
    tesseract_ok = _shutil.which("tesseract") is not None
    extract_ok = (BASE_DIR / "extract.py").exists()
    api_key_set = bool(os.environ.get("ANTHROPIC_API_KEY"))

    # Directory writability
    dirs_ok = {}
    for name, d in [("uploads", UPLOAD_DIR), ("outputs", OUTPUT_DIR), ("clients", CLIENTS_DIR), ("verifications", VERIFY_DIR)]:
        dirs_ok[name] = os.access(str(d), os.W_OK)

    # Disk usage
    try:
        usage = _shutil.disk_usage(str(DATA_DIR))
        disk = {
            "total_gb": round(usage.total / (1024**3), 2),
            "free_gb": round(usage.free / (1024**3), 2),
            "percent_used": round(usage.used / usage.total * 100, 1),
        }
    except Exception:
        disk = None

    # Data directory size
    data_size_mb = 0
    try:
        for dirpath, dirnames, filenames in os.walk(str(DATA_DIR)):
            for fname in filenames:
                try:
                    data_size_mb += os.path.getsize(os.path.join(dirpath, fname))
                except OSError:
                    pass
        data_size_mb = round(data_size_mb / (1024 * 1024), 2)
    except Exception:
        pass

    return jsonify({
        "status": "ok",
        "version": _app_version,
        "uptime_hours": round(uptime_seconds / 3600, 2),
        "started": _start_time.isoformat(),
        "jobs": {"total": len(jobs), "by_status": status_counts},
        "dependencies": {"extract_py": extract_ok, "tesseract": tesseract_ok, "api_key_set": api_key_set},
        "directories": dirs_ok,
        "disk": disk,
        "data_size_mb": data_size_mb,
    })


# ─── HTML ─────────────────────────────────────────────────────────────────────


MAIN_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Bearden Document Intake</title>
<style>
/* ═══ DESIGN SYSTEM ═══ */
:root {
  --bg: #F7F6F3;
  --bg-card: #FFFFFF;
  --bg-sidebar: #1E2A38;
  --bg-sidebar-hover: #2A3A4C;
  --bg-sidebar-active: #344C64;
  --navy: #2C3E50;
  --navy-light: #3D566E;
  --accent: #3498DB;
  --accent-hover: #2980B9;
  --green: #27AE60;
  --green-bg: #E8F8F0;
  --yellow: #F39C12;
  --yellow-bg: #FFF8E8;
  --red: #E74C3C;
  --red-bg: #FDECEC;
  --purple: #8E44AD;
  --purple-bg: #F5EEFA;
  --text: #2C3E50;
  --text-secondary: #7F8C8D;
  --text-light: #95A5A6;
  --border: #E5E5E0;
  --border-light: #F0EFEC;
  --shadow-sm: 0 1px 3px rgba(0,0,0,0.06);
  --shadow-md: 0 4px 12px rgba(0,0,0,0.08);
  --shadow-lg: 0 8px 24px rgba(0,0,0,0.1);
  --radius: 8px;
  --radius-lg: 12px;
  --mono: 'SF Mono', 'Menlo', 'Consolas', monospace;
  --sans: -apple-system, BlinkMacSystemFont, 'Segoe UI', 'Inter', sans-serif;
  --transition: 0.2s ease;
}

* { margin: 0; padding: 0; box-sizing: border-box; }
body { font-family: var(--sans); background: var(--bg); color: var(--text); font-size: 14px; line-height: 1.5; }
::selection { background: var(--accent); color: white; }

/* ═══ LAYOUT ═══ */
.app { display: flex; min-height: 100vh; }
.sidebar { width: 220px; background: var(--bg-sidebar); color: white; display: flex; flex-direction: column; position: fixed; top: 0; left: 0; bottom: 0; z-index: 100; transition: var(--transition); }
.main { margin-left: 220px; flex: 1; min-height: 100vh; padding: 0; }

/* ═══ SIDEBAR ═══ */
.sidebar-brand { padding: 20px 16px 12px; border-bottom: 1px solid rgba(255,255,255,0.08); }
.sidebar-brand h1 { font-size: 16px; font-weight: 700; letter-spacing: 0.02em; }
.sidebar-brand p { font-size: 11px; color: rgba(255,255,255,0.5); margin-top: 2px; }
.sidebar-nav { flex: 1; padding: 8px 0; }
.nav-item { display: flex; align-items: center; gap: 10px; padding: 10px 16px; color: rgba(255,255,255,0.65); cursor: pointer; transition: var(--transition); font-size: 13px; font-weight: 500; border-left: 3px solid transparent; text-decoration: none; }
.nav-item:hover { background: var(--bg-sidebar-hover); color: rgba(255,255,255,0.9); }
.nav-item.active { background: var(--bg-sidebar-active); color: white; border-left-color: var(--accent); }
.nav-item svg { width: 18px; height: 18px; flex-shrink: 0; opacity: 0.7; }
.nav-item.active svg { opacity: 1; }
.nav-badge { background: var(--accent); color: white; font-size: 10px; font-weight: 700; padding: 1px 6px; border-radius: 10px; margin-left: auto; }
.sidebar-footer { padding: 12px 16px; border-top: 1px solid rgba(255,255,255,0.08); font-size: 11px; color: rgba(255,255,255,0.35); }
.sidebar-footer label { display: flex; align-items: center; gap: 6px; font-weight: 600; color: rgba(255,255,255,0.6); }
.sidebar-footer input { background: rgba(255,255,255,0.1); border: 1px solid rgba(255,255,255,0.15); color: white; font-size: 12px; padding: 4px 8px; border-radius: 4px; width: 50px; font-weight: 600; }

/* ═══ PAGE HEADER ═══ */
.page-header { padding: 24px 32px 16px; border-bottom: 1px solid var(--border); background: var(--bg-card); }
.page-header h2 { font-size: 20px; font-weight: 700; color: var(--navy); }
.page-header p { font-size: 13px; color: var(--text-secondary); margin-top: 2px; }
.page-content { padding: 24px 32px; }

/* ═══ SECTIONS (show/hide) ═══ */
.section { display: none; }
.section.active { display: block; }

/* ═══ CARDS ═══ */
.card { background: var(--bg-card); border-radius: var(--radius-lg); box-shadow: var(--shadow-sm); border: 1px solid var(--border-light); }
.card-header { padding: 16px 20px; border-bottom: 1px solid var(--border-light); display: flex; align-items: center; justify-content: space-between; }
.card-header h3 { font-size: 14px; font-weight: 700; color: var(--navy); }
.card-body { padding: 20px; }
.card + .card { margin-top: 16px; }

/* ═══ BUTTONS ═══ */
.btn { display: inline-flex; align-items: center; gap: 6px; padding: 8px 16px; border-radius: 6px; font-size: 13px; font-weight: 600; cursor: pointer; border: none; transition: var(--transition); font-family: var(--sans); }
.btn-primary { background: var(--accent); color: white; }
.btn-primary:hover { background: var(--accent-hover); box-shadow: var(--shadow-sm); }
.btn-secondary { background: var(--bg); color: var(--text); border: 1px solid var(--border); }
.btn-secondary:hover { background: white; border-color: var(--navy-light); }
.btn-success { background: var(--green); color: white; }
.btn-success:hover { opacity: 0.9; }
.btn-danger { background: var(--red); color: white; }
.btn-danger:hover { opacity: 0.9; }
.btn-sm { padding: 5px 10px; font-size: 12px; }
.btn-ghost { background: none; color: var(--text-secondary); padding: 4px 8px; }
.btn-ghost:hover { color: var(--text); background: var(--bg); }
.btn:disabled { opacity: 0.5; cursor: not-allowed; }

/* ═══ FORMS ═══ */
.form-group { margin-bottom: 16px; }
.form-label { display: block; font-size: 12px; font-weight: 600; color: var(--text-secondary); text-transform: uppercase; letter-spacing: 0.04em; margin-bottom: 4px; }
.form-input { width: 100%; padding: 8px 12px; border: 1px solid var(--border); border-radius: 6px; font-size: 13px; font-family: var(--sans); transition: var(--transition); background: white; }
.form-input:focus { outline: none; border-color: var(--accent); box-shadow: 0 0 0 3px rgba(52,152,219,0.12); }
.form-select { appearance: none; background: white url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='12' height='12' viewBox='0 0 12 12'%3E%3Cpath d='M6 8L1 3h10z' fill='%237F8C8D'/%3E%3C/svg%3E") right 10px center no-repeat; padding-right: 28px; }
.form-row { display: grid; grid-template-columns: 1fr 1fr; gap: 16px; }
.form-row-3 { display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 16px; }
textarea.form-input { resize: vertical; min-height: 60px; }
.form-hint { font-size: 11px; color: var(--text-light); margin-top: 2px; }

/* ═══ TABLES ═══ */
.table-wrap { overflow-x: auto; }
table.data-table { width: 100%; border-collapse: collapse; font-size: 13px; }
table.data-table thead th { font-size: 11px; font-weight: 700; text-transform: uppercase; letter-spacing: 0.04em; color: var(--text-light); padding: 8px 12px; text-align: left; border-bottom: 2px solid var(--border); white-space: nowrap; }
table.data-table tbody td { padding: 10px 12px; border-bottom: 1px solid var(--border-light); vertical-align: middle; }
table.data-table tbody tr:hover { background: #FAFAF8; }
table.data-table tbody tr.row-success { background: var(--green-bg); }
table.data-table tbody tr.row-warning { background: var(--yellow-bg); }
table.data-table tbody tr.row-danger { background: var(--red-bg); }
td.mono { font-family: var(--mono); font-size: 12px; }
td.amount { text-align: right; font-family: var(--mono); font-weight: 600; }
td.actions { white-space: nowrap; text-align: right; }

/* ═══ BADGES / PILLS ═══ */
.badge { display: inline-flex; align-items: center; padding: 2px 8px; border-radius: 10px; font-size: 11px; font-weight: 600; letter-spacing: 0.02em; }
.badge-green { background: var(--green-bg); color: var(--green); }
.badge-yellow { background: var(--yellow-bg); color: #B7791F; }
.badge-red { background: var(--red-bg); color: var(--red); }
.badge-blue { background: #EBF5FB; color: var(--accent); }
.badge-purple { background: var(--purple-bg); color: var(--purple); }
.badge-gray { background: #ECF0F1; color: var(--text-secondary); }
.pill { display: inline-flex; align-items: center; gap: 4px; padding: 4px 12px; border-radius: 20px; font-size: 12px; font-weight: 600; cursor: pointer; border: 1px solid var(--border); background: white; transition: var(--transition); }
.pill:hover { border-color: var(--accent); }
.pill.active { background: var(--accent); color: white; border-color: var(--accent); }

/* ═══ TOAST ═══ */
#toast-container { position: fixed; top: 20px; right: 20px; z-index: 9999; display: flex; flex-direction: column; gap: 8px; }
.toast { padding: 10px 16px; border-radius: 8px; font-size: 13px; font-weight: 500; box-shadow: var(--shadow-md); animation: toastIn 0.3s ease; max-width: 360px; display: flex; align-items: center; gap: 8px; }
.toast-success { background: var(--green); color: white; }
.toast-error { background: var(--red); color: white; }
.toast-info { background: var(--navy); color: white; }
@keyframes toastIn { from { opacity: 0; transform: translateY(-10px); } to { opacity: 1; transform: translateY(0); } }

/* ═══ UPLOAD SECTION ═══ */
.upload-area { border: 2px dashed var(--border); border-radius: var(--radius-lg); padding: 48px 24px; text-align: center; cursor: pointer; transition: var(--transition); background: #FAFAF8; }
.upload-area:hover, .upload-area.dragover { border-color: var(--accent); background: #F0F8FF; }
.upload-area svg { width: 48px; height: 48px; color: var(--text-light); margin-bottom: 12px; }
.upload-area h3 { font-size: 16px; color: var(--text); margin-bottom: 4px; }
.upload-area p { font-size: 13px; color: var(--text-secondary); }
.upload-form { display: none; margin-top: 20px; }
.upload-form.visible { display: block; }
.upload-file-name { font-size: 14px; font-weight: 600; color: var(--accent); margin-bottom: 16px; display: flex; align-items: center; gap: 8px; }

/* ═══ DOC TYPE + OUTPUT FORMAT PILLS ═══ */
.pill-group { display: flex; flex-wrap: wrap; gap: 6px; }

/* ═══ PROCESSING ═══ */
.processing-card { max-width: 640px; margin: 0 auto; }
.progress-bar { width: 100%; height: 6px; background: var(--border); border-radius: 3px; overflow: hidden; margin: 12px 0; }
.progress-fill { height: 100%; background: linear-gradient(90deg, var(--accent), #5DADE2); border-radius: 3px; transition: width 0.4s ease; }
.progress-label { display: flex; justify-content: space-between; font-size: 12px; color: var(--text-secondary); }
.console-output { background: #1E2A38; color: #BDC3C7; font-family: var(--mono); font-size: 11px; padding: 12px; border-radius: 6px; max-height: 200px; overflow-y: auto; margin-top: 12px; line-height: 1.6; }
.console-output .line-highlight { color: #5DADE2; }

/* ═══ REVIEW ═══ */
.review-header { display: flex; align-items: center; justify-content: space-between; padding: 12px 20px; background: var(--bg-card); border-bottom: 1px solid var(--border); }
.review-nav { display: flex; align-items: center; gap: 8px; }
.review-nav button { padding: 6px 12px; }
.review-pager { font-size: 13px; font-weight: 600; color: var(--navy); min-width: 80px; text-align: center; }
.review-split { display: grid; grid-template-columns: 1fr 1fr; height: calc(100vh - 120px); }
.review-pdf { background: #3D3D3D; overflow: auto; display: flex; align-items: flex-start; justify-content: center; padding: 16px; }
.review-pdf img { max-width: 100%; height: auto; box-shadow: var(--shadow-lg); border-radius: 4px; }
.review-fields { overflow-y: auto; padding: 16px; background: var(--bg); }
.verify-progress { height: 4px; background: var(--border); border-radius: 2px; overflow: hidden; }
.verify-progress-fill { height: 100%; background: var(--green); transition: width 0.3s ease; }
.verify-stats { display: flex; gap: 16px; font-size: 12px; color: var(--text-secondary); padding: 8px 0; }
.verify-stats span { font-weight: 600; }

/* ─── Field rendering ─── */
.field-group { background: var(--bg-card); border-radius: var(--radius); margin-bottom: 12px; box-shadow: var(--shadow-sm); border: 1px solid var(--border-light); overflow: hidden; }
.field-group-title { font-size: 13px; font-weight: 700; padding: 10px 14px; background: var(--navy); color: white; display: flex; align-items: center; justify-content: space-between; }
.field-entity { font-size: 12px; color: var(--text-secondary); padding: 6px 14px; background: #F8F8F6; border-bottom: 1px solid var(--border-light); display: flex; align-items: center; justify-content: space-between; }
.field-entity .all-done { color: var(--green); font-size: 11px; font-weight: 600; }
.field-row { display: flex; align-items: center; padding: 6px 14px; border-bottom: 1px solid var(--border-light); transition: background 0.1s; min-height: 36px; }
.field-row:hover { background: #FAFAF8; }
.field-row.focused { background: #EBF5FB; }
.field-row.vf-confirmed { background: #F0FBF4; }
.field-row.vf-corrected { background: #FFF8E8; }
.field-row.vf-flagged { background: #FFF0E0; }
.field-name { flex: 0 0 45%; font-size: 12px; color: var(--text-secondary); font-weight: 500; padding-right: 8px; }
.field-val-wrap { flex: 1; display: flex; align-items: center; gap: 6px; }
.field-val { font-size: 13px; font-weight: 600; font-family: var(--mono); color: var(--text); cursor: pointer; }
.field-val:hover { color: var(--accent); }
.field-actions { display: flex; gap: 4px; margin-left: auto; }
.field-edit-input { font-size: 13px; font-family: var(--mono); padding: 2px 6px; border: 1px solid var(--accent); border-radius: 4px; width: 120px; }

/* Confidence dots */
.conf-dot { width: 7px; height: 7px; border-radius: 50%; display: inline-block; flex-shrink: 0; }
.conf-dual { background: #1A8C42; }
.conf-confirmed { background: #5CB85C; }
.conf-corrected { background: #FFCC00; }
.conf-low { background: #FF9800; }
.conf-other { background: #BDC3C7; }

/* Verify buttons */
.vf-btn { width: 26px; height: 26px; border-radius: 5px; border: 1px solid var(--border); background: white; cursor: pointer; font-size: 13px; display: flex; align-items: center; justify-content: center; transition: var(--transition); color: var(--text-light); }
.vf-btn:hover { border-color: var(--navy-light); color: var(--text); }
.vf-btn-confirm.active { background: var(--green); color: white; border-color: var(--green); }
.vf-btn-flag.active { background: var(--yellow); color: white; border-color: var(--yellow); }
.vf-btn-note.has-note { background: #E8F4FD; border-color: #5B9BD5; color: #5B9BD5; }
.vf-note { font-size: 11px; color: var(--text-secondary); padding: 2px 14px 4px 14px; }
.vf-note-input { display:flex; align-items:center; gap:4px; padding:4px 14px; }
.vf-note-input input { font-size:12px; padding:3px 6px; border:1px solid var(--border); border-radius:4px; flex:1; font-family:var(--sans); }
.vf-note-input button { font-size:11px; padding:2px 8px; border-radius:4px; border:1px solid var(--border); background:var(--accent); color:white; cursor:pointer; }
.vf-original { text-decoration: line-through; color: var(--red); }

/* ─── Transaction table ─── */
.txn-section { margin: 8px 0; }
.txn-header { font-size: 11px; font-weight: 700; color: var(--text-light); text-transform: uppercase; letter-spacing: 0.04em; padding: 6px 14px; }
.txn-table { width: 100%; border-collapse: collapse; font-size: 12px; }
.txn-table thead th { font-size: 10px; font-weight: 700; text-transform: uppercase; letter-spacing: 0.03em; color: var(--text-light); padding: 4px 6px; text-align: left; border-bottom: 1px solid var(--border); }
.txn-table tbody tr { border-bottom: 1px solid var(--border-light); transition: background 0.1s; }
.txn-table tbody tr:hover { background: #FAFAF8; }
.txn-table tbody tr.vf-confirmed { background: var(--green-bg); }
.txn-table tbody tr.vf-flagged { background: var(--yellow-bg); }
.txn-table td { padding: 5px 6px; vertical-align: middle; }
.txn-amt { text-align: right; font-family: var(--mono); font-weight: 600; white-space: nowrap; }
.txn-type { font-size: 10px; font-weight: 600; padding: 1px 6px; border-radius: 3px; text-transform: uppercase; }
.txn-type-deposit { background: #D5F5E3; color: #1B7A3D; }
.txn-type-withdrawal { background: #FADBD8; color: #A93226; }
.txn-type-check { background: #FFF3CD; color: #856404; }
.txn-type-fee { background: #F5CBA7; color: #7E5109; }
.txn-type-transfer { background: #D6EAF8; color: #1F618D; }

/* Category dropdown */
.cat-select { font-size: 11px; padding: 2px 4px; border: 1px solid var(--border); border-radius: 4px; background: white; max-width: 150px; cursor: pointer; }
.cat-select:focus { border-color: var(--accent); outline: none; }
.cat-select.cat-set { background: var(--green-bg); border-color: var(--green); font-weight: 600; }
.cat-select.cat-suggested { background: var(--yellow-bg); border-color: #D4B95E; }
.cat-learned-badge { font-size: 9px; padding: 1px 5px; border-radius: 3px; background: var(--purple-bg); color: var(--purple); font-weight: 600; }
.field-cat-row { display: flex; align-items: center; gap: 8px; padding: 2px 14px 4px; font-size: 11px; color: var(--text-light); }
.field-cat-row label { font-weight: 600; text-transform: uppercase; letter-spacing: 0.03em; font-size: 10px; }

/* ─── Info section (collapsible) ─── */
.info-section { margin: 4px 0; border: 1px solid var(--border-light); border-radius: 6px; overflow: hidden; }
.info-toggle { display: flex; align-items: center; gap: 6px; padding: 7px 12px; background: #F8F7F5; cursor: pointer; user-select: none; font-size: 11px; font-weight: 700; color: var(--text-light); text-transform: uppercase; letter-spacing: 0.04em; transition: background 0.1s; }
.info-toggle:hover { background: #F0EFEC; }
.info-toggle-arrow { font-size: 10px; transition: transform 0.2s; }
.info-toggle-arrow.open { transform: rotate(90deg); }
.info-field { display: flex; padding: 4px 12px; font-size: 12px; border-bottom: 1px solid var(--border-light); }
.info-field-name { flex: 0 0 45%; color: var(--text-secondary); }
.info-field-val { flex: 1; font-weight: 500; }

/* ═══ CLIENTS SECTION ═══ */
.client-list { display: grid; grid-template-columns: repeat(auto-fill, minmax(280px, 1fr)); gap: 16px; }
.client-card { background: var(--bg-card); border: 1px solid var(--border-light); border-radius: var(--radius); padding: 16px; cursor: pointer; transition: var(--transition); }
.client-card:hover { border-color: var(--accent); box-shadow: var(--shadow-md); transform: translateY(-1px); }
.client-card h4 { font-size: 15px; font-weight: 700; color: var(--navy); margin-bottom: 4px; }
.client-card .client-meta { font-size: 12px; color: var(--text-secondary); display: flex; gap: 12px; flex-wrap: wrap; }
.client-card .client-badges { margin-top: 8px; display: flex; gap: 6px; flex-wrap: wrap; }
.client-detail { display: none; }
.client-detail.visible { display: block; }
.client-back { font-size: 13px; color: var(--accent); cursor: pointer; display: flex; align-items: center; gap: 4px; margin-bottom: 16px; font-weight: 500; }
.client-back:hover { text-decoration: underline; }

/* Client tabs */
.client-tabs { display: flex; gap: 0; border-bottom: 2px solid var(--border); margin-bottom: 16px; }
.client-tab { padding: 10px 20px; font-size: 13px; font-weight: 600; color: var(--text-secondary); cursor: pointer; border-bottom: 2px solid transparent; margin-bottom: -2px; transition: var(--transition); }
.client-tab:hover { color: var(--text); }
.client-tab.active { color: var(--accent); border-bottom-color: var(--accent); }
.client-tab-content { display: none; }
.client-tab-content.active { display: block; }

/* Context uploads */
.context-doc { display: flex; align-items: center; gap: 12px; padding: 10px 12px; border: 1px solid var(--border-light); border-radius: 6px; margin-bottom: 8px; }
.context-doc-icon { width: 36px; height: 36px; background: #EBF5FB; border-radius: 6px; display: flex; align-items: center; justify-content: center; font-size: 16px; }
.context-doc-info { flex: 1; }
.context-doc-info .name { font-size: 13px; font-weight: 600; }
.context-doc-info .meta { font-size: 11px; color: var(--text-light); }

/* Instructions */
.instruction-item { display: flex; align-items: flex-start; gap: 10px; padding: 10px 12px; border: 1px solid var(--border-light); border-radius: 6px; margin-bottom: 6px; }
.instruction-item .inst-text { flex: 1; font-size: 13px; }
.instruction-item .inst-date { font-size: 11px; color: var(--text-light); white-space: nowrap; }

/* Completeness */
.completeness-item { display: flex; align-items: center; gap: 10px; padding: 8px 12px; border-bottom: 1px solid var(--border-light); font-size: 13px; }
.completeness-icon { width: 24px; text-align: center; font-size: 16px; }
.completeness-info { flex: 1; }
.completeness-info .ci-form { font-weight: 600; }
.completeness-info .ci-payer { color: var(--text-secondary); font-size: 12px; }

/* ═══ BATCH CATEGORIZE ═══ */
.batch-stats { display: grid; grid-template-columns: repeat(3, 1fr); gap: 12px; margin-bottom: 20px; }
.batch-stat { background: var(--bg-card); border: 1px solid var(--border-light); border-radius: var(--radius); padding: 16px; text-align: center; }
.batch-stat .stat-num { font-size: 28px; font-weight: 700; color: var(--navy); }
.batch-stat .stat-label { font-size: 12px; color: var(--text-secondary); margin-top: 2px; }
.vendor-group { border: 1px solid var(--border-light); border-radius: var(--radius); margin-bottom: 8px; overflow: hidden; }
.vendor-group-header { display: flex; align-items: center; gap: 12px; padding: 10px 14px; background: #FAFAF8; cursor: pointer; transition: background 0.1s; }
.vendor-group-header:hover { background: #F0EFEC; }
.vendor-group-header .vg-name { font-weight: 600; flex: 1; }
.vendor-group-header .vg-count { font-size: 12px; color: var(--text-secondary); }
.vendor-group-header .vg-amount { font-family: var(--mono); font-weight: 600; font-size: 13px; }
.vendor-group-items { display: none; padding: 0 14px 8px; }
.vendor-group-items.open { display: block; }

/* ═══ HISTORY ═══ */
.history-filters { display: flex; gap: 8px; margin-bottom: 16px; flex-wrap: wrap; align-items: center; }
.history-filters .form-input { width: 200px; }
.history-filters .form-select { width: 160px; }
.job-status { font-size: 11px; font-weight: 700; text-transform: uppercase; }
.job-status.complete { color: var(--green); }
.job-status.running { color: var(--accent); }
.job-status.failed { color: var(--red); }
.job-status.interrupted { color: var(--yellow); }

/* ═══ EMPTY STATES ═══ */
.empty-state { text-align: center; padding: 48px 24px; color: var(--text-secondary); }
.empty-state svg { width: 48px; height: 48px; color: var(--border); margin-bottom: 12px; }
.empty-state h3 { font-size: 16px; color: var(--text); margin-bottom: 4px; }
.empty-state p { font-size: 13px; }

/* ═══ KEYBOARD HELP ═══ */
.kbd-overlay { display: none; position: fixed; inset: 0; background: rgba(0,0,0,0.5); z-index: 1000; align-items: center; justify-content: center; }
.kbd-overlay.visible { display: flex; }
.kbd-card { background: white; border-radius: var(--radius-lg); padding: 24px; max-width: 400px; box-shadow: var(--shadow-lg); }
.kbd-card h3 { margin-bottom: 12px; }
.kbd-row { display: flex; justify-content: space-between; padding: 4px 0; font-size: 13px; }
kbd { background: var(--bg); border: 1px solid var(--border); border-radius: 4px; padding: 2px 8px; font-size: 12px; font-family: var(--mono); }

/* ═══ RESPONSIVE ═══ */
@media (max-width: 900px) {
  .sidebar { width: 60px; }
  .sidebar-brand h1, .sidebar-brand p, .nav-item span, .sidebar-footer { display: none; }
  .nav-item { justify-content: center; padding: 12px; }
  .main { margin-left: 60px; }
  .review-split { grid-template-columns: 1fr; }
  .form-row { grid-template-columns: 1fr; }
}

/* Modal */
.modal-overlay { position:fixed; inset:0; background:rgba(0,0,0,0.4); z-index:9999; display:none; align-items:center; justify-content:center; }
.modal-overlay.visible { display:flex; }
.modal-content { background:white; border-radius:12px; padding:24px; width:420px; max-width:90vw; box-shadow:0 20px 60px rgba(0,0,0,0.3); }
</style>
</head>
<body>
<div class="app">

<!-- ═══ SIDEBAR ═══ -->
<aside class="sidebar">
  <div class="sidebar-brand">
    <h1>Bearden</h1>
    <p>Document Intake Platform</p>
  </div>
  <nav class="sidebar-nav">
    <a class="nav-item active" onclick="showSection('upload')" data-section="upload">
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M21 15v4a2 2 0 01-2 2H5a2 2 0 01-2-2v-4"/><polyline points="17 8 12 3 7 8"/><line x1="12" y1="3" x2="12" y2="15"/></svg>
      <span>Upload</span>
    </a>
    <a class="nav-item" onclick="showSection('review')" data-section="review" id="navReview" style="display:none">
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M14 2H6a2 2 0 00-2 2v16a2 2 0 002 2h12a2 2 0 002-2V8z"/><polyline points="14 2 14 8 20 8"/><line x1="16" y1="13" x2="8" y2="13"/><line x1="16" y1="17" x2="8" y2="17"/></svg>
      <span>Review</span>
    </a>
    <a class="nav-item" onclick="showSection('clients')" data-section="clients">
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M20 21v-2a4 4 0 00-4-4H8a4 4 0 00-4 4v2"/><circle cx="12" cy="7" r="4"/></svg>
      <span>Clients &amp; PY Docs</span>
    </a>
    <a class="nav-item" onclick="showSection('batch')" data-section="batch" style="display:none">
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="3" y="3" width="7" height="7"/><rect x="14" y="3" width="7" height="7"/><rect x="3" y="14" width="7" height="7"/><rect x="14" y="14" width="7" height="7"/></svg>
      <span>Categorize</span>
    </a>
    <a class="nav-item" onclick="showSection('history')" data-section="history">
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="10"/><polyline points="12 6 12 12 16 14"/></svg>
      <span>History</span>
      <span class="nav-badge" id="historyCount">0</span>
    </a>
  </nav>
  <div class="sidebar-footer">
    <label>Reviewer
      <input type="text" id="reviewerInitials" maxlength="4" placeholder="JW" value="">
    </label>
  </div>
</aside>

<!-- ═══ MAIN CONTENT ═══ -->
<div class="main">
<div id="toast-container"></div>

<!-- ═══ UPLOAD SECTION ═══ -->
<div class="section active" id="sec-upload">
  <div class="page-header"><h2>Upload Document</h2><p>Scan a PDF to extract structured data</p></div>
  <div class="page-content">
    <div class="card">
      <div class="card-body">
        <div class="upload-area" id="dropZone" onclick="document.getElementById('fileInput').click()">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5"><path d="M21 15v4a2 2 0 01-2 2H5a2 2 0 01-2-2v-4"/><polyline points="17 8 12 3 7 8"/><line x1="12" y1="3" x2="12" y2="15"/></svg>
          <h3>Drop PDF here or click to browse</h3>
          <p>Supports scanned tax documents, bank statements, invoices, checks, and more</p>
        </div>
        <input type="file" id="fileInput" accept=".pdf" style="display:none" onchange="handleFile(this)">

        <div class="upload-form" id="uploadForm">
          <div class="upload-file-name" id="fileName">
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" width="16" height="16"><path d="M14 2H6a2 2 0 00-2 2v16a2 2 0 002 2h12a2 2 0 002-2V8z"/><polyline points="14 2 14 8 20 8"/></svg>
            <span id="fileNameText"></span>
            <button class="btn btn-ghost btn-sm" onclick="resetUpload()">Change</button>
          </div>

          <div class="form-row">
            <div class="form-group">
              <label class="form-label">Client</label>
              <div style="display:flex;gap:8px;align-items:center">
                <select id="clientName" class="form-input" style="flex:1">
                  <option value="">— Select client —</option>
                </select>
                <button class="btn btn-secondary btn-sm" onclick="openNewClientModal()" title="Create new client" style="white-space:nowrap">+ New</button>
              </div>
              <a href="#" onclick="event.preventDefault(); const cn=document.getElementById('clientName').value.trim(); if(cn){showSection('clients');setTimeout(()=>openClientDetail(cn),200);} else {showToast('Select a client first','error');}" style="font-size:11px; color:var(--accent); text-decoration:none; margin-top:4px; display:inline-block;">&#x1F4C2; Upload prior-year docs / manage instructions</a>
            </div>
            <div class="form-group">
              <label class="form-label">Tax Year</label>
              <input type="number" id="taxYear" class="form-input" value="2025" min="2000" max="2030">
            </div>
          </div>

          <div class="form-group">
            <label class="form-label">Document Type</label>
            <div class="pill-group" id="docTypePills"></div>
          </div>

          <div class="form-group">
            <label class="form-label">Output Format</label>
            <div class="pill-group" id="outputFormatPills"></div>
          </div>

          <div class="form-group">
            <label class="form-label">AI Instructions</label>
            <textarea id="aiInstructions" class="form-input" rows="3" placeholder="Tell the AI how to handle this document. Example: 'This is a trust return — extract K-1 box 1-14 only' or 'Combine all 1099-DIV pages into one entry per payer'"></textarea>
          </div>

          <details style="margin-bottom:16px">
            <summary style="font-size:12px; font-weight:600; color:var(--text-secondary); cursor:pointer; padding:4px 0;">Advanced Options</summary>
            <div style="padding-top:12px">
              <div class="form-group">
                <label class="form-label">Notes for Extraction</label>
                <textarea id="userNotes" class="form-input" rows="2" placeholder="Optional context about this document..."></textarea>
              </div>
              <div class="form-group">
                <label style="display:flex;align-items:center;gap:6px;font-size:13px;cursor:pointer">
                  <input type="checkbox" id="skipVerify"> Skip AI verification (faster, lower cost)
                </label>
              </div>
              <div class="form-group">
                <label style="display:flex;align-items:center;gap:6px;font-size:13px;cursor:pointer">
                  <input type="checkbox" id="disablePii"> Disable PII tokenization
                </label>
              </div>
              <div class="form-group">
                <label style="display:flex;align-items:center;gap:6px;font-size:13px;cursor:pointer">
                  <input type="checkbox" id="useOcrFirst"> Use OCR-first mode (lower cost, less accurate)
                </label>
              </div>
            </div>
          </details>

          <button class="btn btn-primary" id="startBtn" onclick="startExtraction()" style="width:100%;justify-content:center;padding:12px;">
            Start Extraction
          </button>
        </div>
      </div>
    </div>
  </div>
</div>

<!-- ═══ PROCESSING SECTION ═══ -->
<div class="section" id="sec-processing">
  <div class="page-header"><h2>Processing</h2><p id="processingFile"></p></div>
  <div class="page-content">
    <div class="card processing-card">
      <div class="card-body">
        <div class="progress-label">
          <span id="procStage">Starting...</span>
          <span id="procPct">0%</span>
        </div>
        <div class="progress-bar"><div class="progress-fill" id="procBar" style="width:0%"></div></div>
        <div class="console-output" id="procConsole"></div>
        <div style="margin-top:16px; text-align:center">
          <button class="btn btn-secondary btn-sm" id="procCancelBtn" onclick="cancelJob()" style="display:none">Cancel</button>
        </div>
      </div>
    </div>
  </div>
</div>

<!-- ═══ REVIEW SECTION ═══ -->
<div class="section" id="sec-review">
  <div class="review-header">
    <div class="review-nav">
      <button class="btn btn-secondary btn-sm" onclick="prevPage()">&#9664; Prev</button>
      <span class="review-pager" id="reviewPager">1 / 1</span>
      <button class="btn btn-secondary btn-sm" onclick="nextPage()">Next &#9654;</button>
      <button class="btn btn-secondary btn-sm" onclick="reextractPage()" title="Re-extract this page with AI instructions" style="margin-left:8px">&#x21BB; Re-extract</button>
    </div>
    <div class="verify-stats" id="verifyStats"></div>
    <div style="display:flex;gap:8px;align-items:center">
      <button class="btn btn-success btn-sm" onclick="downloadFile('xlsx')">&#x2B73; Excel</button>
      <button class="btn btn-secondary btn-sm" onclick="downloadFile('log')">&#x2B73; JSON</button>
      <button class="btn btn-secondary btn-sm" onclick="regenExcel()" title="Regenerate Excel with corrections">&#x21BB; Regen Excel</button>
      <button class="btn btn-secondary btn-sm" onclick="toggleAiChat()" title="Ask AI about this page" id="aiChatToggle">&#x1F4AC; Ask AI</button>
      <button class="btn btn-ghost btn-sm" title="Keyboard shortcuts (?)" onclick="toggleKbdHelp()">&#x2328;</button>
    </div>
  </div>
  <div style="padding:0 20px 4px; background:var(--bg-card); border-bottom:1px solid var(--border)">
    <div class="verify-progress"><div class="verify-progress-fill" id="verifyBar" style="width:0%"></div></div>
  </div>
  <!-- Client instructions banner (if any) -->
  <div id="reviewInstructionsBanner" style="display:none; padding:8px 20px; background:#FFF8E8; border-bottom:1px solid #F5E6C8; font-size:12px;"></div>
  <div class="review-split">
    <div class="review-pdf" id="pdfViewer"></div>
    <div class="review-fields" id="fieldsPanel"></div>
  </div>
  <!-- AI Chat Panel -->
  <div id="aiChatPanel" style="display:none; border-top:2px solid var(--accent); background:var(--bg-card);">
    <div style="padding:12px 20px; display:flex; align-items:center; justify-content:space-between; border-bottom:1px solid var(--border);">
      <span style="font-size:13px; font-weight:700; color:var(--navy);">&#x1F4AC; AI Assistant — Page <span id="aiChatPage">1</span></span>
      <button class="btn btn-ghost btn-sm" onclick="toggleAiChat()" title="Close">&#x2716;</button>
    </div>
    <div id="aiChatMessages" style="padding:12px 20px; max-height:200px; overflow-y:auto; font-size:13px;"></div>
    <div style="padding:8px 20px 12px; display:flex; gap:8px;">
      <input type="text" id="aiChatInput" class="form-input" placeholder="Ask about this page... e.g. 'What's in box 14?' or 'Is this K-1 or K-3?'" style="flex:1; font-size:13px;" onkeydown="if(event.key==='Enter')sendAiChat()">
      <button class="btn btn-primary btn-sm" onclick="sendAiChat()">Send</button>
    </div>
  </div>
</div>

<!-- ═══ CLIENTS SECTION ═══ -->
<div class="section" id="sec-clients">
  <div class="page-header"><h2>Client Manager</h2><p>Upload prior-year returns &amp; workpapers, set extraction instructions, track document completeness</p></div>
  <div class="page-content">
    <div id="clientListView">
      <div style="margin-bottom:16px; display:flex; gap:8px; align-items:center">
        <input type="text" class="form-input" id="clientSearch" placeholder="Search clients..." style="max-width:300px" oninput="filterClients()">
      </div>
      <div class="client-list" id="clientGrid"></div>
    </div>
    <div class="client-detail" id="clientDetailView">
      <div class="client-back" onclick="closeClientDetail()">&#9664; Back to all clients</div>
      <h2 id="clientDetailName" style="font-size:20px;font-weight:700;color:var(--navy);margin-bottom:4px"></h2>
      <div id="clientDetailMeta" style="font-size:13px;color:var(--text-secondary);margin-bottom:16px"></div>
      <div class="client-tabs">
        <div class="client-tab active" data-tab="documents" onclick="showClientTab('documents')">Documents</div>
        <div class="client-tab" data-tab="context" onclick="showClientTab('context')">Prior-Year Context</div>
        <div class="client-tab" data-tab="instructions" onclick="showClientTab('instructions')">Instructions</div>
        <div class="client-tab" data-tab="completeness" onclick="showClientTab('completeness')">Completeness</div>
      </div>
      <!-- Documents Tab -->
      <div class="client-tab-content active" id="tab-documents">
        <div id="clientDocGroups">
          <div class="empty-state"><p>No documents yet. Upload a PDF from the Upload section to get started.</p></div>
        </div>
      </div>
      <!-- Context Tab -->
      <div class="client-tab-content" id="tab-context">
        <div class="card" style="margin-bottom:16px">
          <div class="card-header"><h3>Upload Context Document</h3></div>
          <div class="card-body">
            <p style="font-size:13px;color:var(--text-secondary);margin-bottom:12px">Upload a prior-year return, workbook, or notes. The system will extract payer information for completeness tracking and variance checking.</p>
            <div class="form-row">
              <div class="form-group">
                <label class="form-label">File</label>
                <input type="file" id="contextFile" accept=".pdf,.xlsx,.xls,.txt,.csv" class="form-input" style="padding:6px">
              </div>
              <div class="form-group">
                <label class="form-label">Year</label>
                <input type="number" id="contextYear" class="form-input" value="2024" min="2000" max="2030">
              </div>
            </div>
            <div class="form-group">
              <label class="form-label">Label (optional)</label>
              <input type="text" id="contextLabel" class="form-input" placeholder="e.g. 2024 Filed Return">
            </div>
            <button class="btn btn-primary" onclick="uploadContext()">Upload Context</button>
          </div>
        </div>
        <div id="contextDocList"></div>
      </div>
      <!-- Instructions Tab -->
      <div class="client-tab-content" id="tab-instructions">
        <div class="card" style="margin-bottom:16px">
          <div class="card-header"><h3>Add Instruction</h3></div>
          <div class="card-body">
            <p style="font-size:13px;color:var(--text-secondary);margin-bottom:12px">Client-specific rules that apply to every extraction. These are injected into the AI prompts automatically.</p>
            <div class="form-group">
              <textarea id="newInstruction" class="form-input" rows="2" placeholder="e.g. All payments from X Corp are commissions, not regular income."></textarea>
            </div>
            <button class="btn btn-primary" onclick="addInstruction()">Add Instruction</button>
          </div>
        </div>
        <div id="instructionsList"></div>
      </div>
      <!-- Completeness Tab -->
      <div class="client-tab-content" id="tab-completeness">
        <div id="completenessReport">
          <div class="empty-state">
            <p>Upload prior-year context to enable completeness tracking.</p>
          </div>
        </div>
      </div>
    </div>
  </div>
</div>

<!-- ═══ BATCH CATEGORIZE ═══ -->
<div class="section" id="sec-batch" style="display:none">
  <div class="page-header"><h2>Batch Categorize</h2><p>Classify transactions across all documents at once</p></div>
  <div class="page-content">
    <div style="margin-bottom:16px; display:flex; gap:8px; align-items:center; flex-wrap:wrap">
      <input type="text" class="form-input" id="batchClientFilter" placeholder="Filter by client..." style="max-width:220px" oninput="loadBatchData()">
      <label style="font-size:13px; display:flex; align-items:center; gap:4px; cursor:pointer">
        <input type="checkbox" id="batchShowAll" onchange="loadBatchData()"> Show categorized
      </label>
      <input type="text" class="form-input" id="batchSearch" placeholder="Search vendors..." style="max-width:220px" oninput="filterBatchVendors()">
    </div>
    <div class="batch-stats" id="batchStats"></div>
    <div id="batchVendorGroups"></div>
  </div>
</div>

<!-- ═══ HISTORY SECTION ═══ -->
<div class="section" id="sec-history">
  <div class="page-header"><h2>Job History</h2><p>All extractions and their status</p></div>
  <div class="page-content">
    <div class="history-filters">
      <input type="text" class="form-input" id="historySearch" placeholder="Search by client or filename..." oninput="filterHistory()">
      <select class="form-input form-select" id="historyStatusFilter" onchange="filterHistory()" style="width:140px">
        <option value="">All statuses</option>
        <option value="complete">Complete</option>
        <option value="running">Running</option>
        <option value="failed">Failed</option>
        <option value="interrupted">Interrupted</option>
      </select>
    </div>
    <div class="card">
      <div class="table-wrap">
        <table class="data-table" id="historyTable">
          <thead><tr><th>Client</th><th>File</th><th>Type</th><th>Year</th><th>Status</th><th>Cost</th><th>Date</th><th></th></tr></thead>
          <tbody id="historyBody"></tbody>
        </table>
      </div>
    </div>
  </div>
</div>

<!-- ═══ KEYBOARD HELP ═══ -->
<div class="kbd-overlay" id="kbdOverlay" onclick="if(event.target===this)toggleKbdHelp()">
  <div class="kbd-card">
    <h3>Keyboard Shortcuts</h3>
    <div class="kbd-row"><span>Confirm field</span><kbd>Enter</kbd></div>
    <div class="kbd-row"><span>Flag field</span><kbd>F</kbd></div>
    <div class="kbd-row"><span>Edit value</span><kbd>E</kbd></div>
    <div class="kbd-row"><span>Add note</span><kbd>N</kbd></div>
    <div class="kbd-row"><span>Next field</span><kbd>&#x2193; / Tab</kbd></div>
    <div class="kbd-row"><span>Prev field</span><kbd>&#x2191; / Shift+Tab</kbd></div>
    <div class="kbd-row"><span>Next page</span><kbd>&#x2192;</kbd></div>
    <div class="kbd-row"><span>Prev page</span><kbd>&#x2190;</kbd></div>
    <div class="kbd-row"><span>This help</span><kbd>?</kbd></div>
  </div>
</div>

</div><!-- /main -->
</div><!-- /app -->

<!-- ═══════════════════════════════════════════════════════════════════════════ -->
<!-- JAVASCRIPT -->
<!-- ═══════════════════════════════════════════════════════════════════════════ -->
<script>
// ─── State ───
let currentJobId = null;
let pollTimer = null;
let startTime = null;
let elapsedTimer = null;
let reviewData = null;
let currentPage = 1;
let totalPages = 1;
let verifications = {};
let totalFieldCount = 0;
let focusedFieldIdx = -1;
let pageFieldKeys = [];
let selectedDocType = 'tax_returns';

// Field display order by document type (matches extract.py TEMPLATE_SECTIONS)
const FIELD_ORDER = {
  'W-2': ['wages','federal_wh','ss_wages','ss_wh','medicare_wages','medicare_wh','state_wages','state_wh','local_wages','local_wh','nonqualified_plans_12a'],
  '1099-INT': ['interest_income','early_withdrawal_penalty','us_savings_bonds_and_treasury','federal_wh','state_wh'],
  '1099-DIV': ['ordinary_dividends','qualified_dividends','capital_gain_distributions','nondividend_distributions','federal_wh','state_wh','foreign_tax_paid','exempt_interest_dividends'],
  '1099-R': ['gross_distribution','taxable_amount','federal_wh','state_wh','distribution_code','employee_contributions'],
  'K-1': ['ordinary_income','net_rental_income','guaranteed_payments','interest_income','dividends','royalties','net_short_term_capital_gain','net_long_term_capital_gain','net_section_1231_gain','other_income','section_179_deduction','other_deductions','self_employment_earnings'],
  '1099-NEC': ['nonemployee_compensation','federal_wh'],
  '1099-MISC': ['rents','royalties','other_income','federal_wh','fishing_boat_proceeds','medical_payments','nonemployee_compensation'],
  'SSA-1099': ['net_benefits','federal_wh','repaid_benefits'],
};
const FIELD_BOX_LABELS = {
  'W-2': {wages:'Box 1',federal_wh:'Box 2',ss_wages:'Box 3',ss_wh:'Box 4',medicare_wages:'Box 5',medicare_wh:'Box 6',state_wages:'Box 16',state_wh:'Box 17',local_wages:'Box 18',local_wh:'Box 19'},
  '1099-DIV': {ordinary_dividends:'Box 1a',qualified_dividends:'Box 1b',capital_gain_distributions:'Box 2a',nondividend_distributions:'Box 3',federal_wh:'Box 4',foreign_tax_paid:'Box 7',exempt_interest_dividends:'Box 12'},
  '1099-INT': {interest_income:'Box 1',early_withdrawal_penalty:'Box 2',us_savings_bonds_and_treasury:'Box 3',federal_wh:'Box 4'},
  '1099-R': {gross_distribution:'Box 1',taxable_amount:'Box 2a',federal_wh:'Box 4',distribution_code:'Box 7',employee_contributions:'Box 5',state_wh:'Box 12'},
  'K-1': {ordinary_income:'Line 1',net_rental_income:'Line 2',guaranteed_payments:'Line 4c',interest_income:'Line 5',dividends:'Line 6a',royalties:'Line 7',net_short_term_capital_gain:'Line 8',net_long_term_capital_gain:'Line 9a',net_section_1231_gain:'Line 10',other_income:'Line 11',section_179_deduction:'Line 12',other_deductions:'Line 13',self_employment_earnings:'Line 14a'},
  '1099-NEC': {nonemployee_compensation:'Box 1',federal_wh:'Box 4'},
  'SSA-1099': {net_benefits:'Box 5',federal_wh:'Box 6'},
};
let selectedOutputFormat = 'tax_review';
let vendorMap = {};
let chartOfAccounts = {};
let currentClientName = '';
let batchData = null;
let allJobs = [];

const DOC_TYPES = [
  {id:'tax_returns', label:'Tax Returns', icon:'&#x1F4CB;'},
  {id:'bank_statements', label:'Bank Statements', icon:'&#x1F3E6;'},
  {id:'bookkeeping', label:'Bookkeeping', icon:'&#x1F4D2;'},
  {id:'trust_documents', label:'Trust Documents', icon:'&#x1F512;'},
  {id:'payroll', label:'Payroll', icon:'&#x1F4B5;'},
  {id:'other', label:'Other', icon:'&#x1F4C4;'},
];
const OUTPUT_FORMATS = [
  {id:'tax_review', label:'Tax Review'},
  {id:'journal_entries', label:'Journal Entries'},
  {id:'account_balances', label:'Account Balances'},
  {id:'trial_balance', label:'Trial Balance'},
  {id:'transaction_register', label:'Transaction Register'},
];

// ─── Init ───
(function init() {
  buildPills();
  loadJobs();
  loadClientSuggestions();
})();

function buildPills() {
  let dh = '';
  DOC_TYPES.forEach(dt => {
    dh += '<div class="pill' + (dt.id === selectedDocType ? ' active' : '') + '" onclick="selectDocType(\'' + dt.id + '\')">' + dt.icon + ' ' + dt.label + '</div>';
  });
  document.getElementById('docTypePills').innerHTML = dh;
  let oh = '';
  OUTPUT_FORMATS.forEach(of_ => {
    oh += '<div class="pill' + (of_.id === selectedOutputFormat ? ' active' : '') + '" onclick="selectOutputFormat(\'' + of_.id + '\')">' + of_.label + '</div>';
  });
  document.getElementById('outputFormatPills').innerHTML = oh;
}

function selectDocType(id) { selectedDocType = id; buildPills(); }
function selectOutputFormat(id) { selectedOutputFormat = id; buildPills(); }

// ─── Toast ───
function showToast(msg, type) {
  type = type || 'info';
  const el = document.createElement('div');
  el.className = 'toast toast-' + type;
  el.textContent = msg;
  document.getElementById('toast-container').appendChild(el);
  setTimeout(() => { el.style.opacity = '0'; setTimeout(() => el.remove(), 300); }, 3000);
}

// ─── Navigation ───
function showSection(id) {
  document.querySelectorAll('.section').forEach(s => s.classList.remove('active'));
  document.querySelectorAll('.nav-item').forEach(n => n.classList.remove('active'));
  document.getElementById('sec-' + id).classList.add('active');
  const nav = document.querySelector('[data-section="' + id + '"]');
  if (nav) nav.classList.add('active');
  if (id === 'history') loadJobs();
  if (id === 'clients') loadClients();
  if (id === 'batch') loadBatchData();
}

function esc(s) { const d = document.createElement('div'); d.textContent = s; return d.innerHTML; }
function getReviewer() { return (document.getElementById('reviewerInitials').value || '').trim(); }

// ─── Upload ───
const dropZone = document.getElementById('dropZone');
dropZone.addEventListener('dragover', e => { e.preventDefault(); dropZone.classList.add('dragover'); });
dropZone.addEventListener('dragleave', () => dropZone.classList.remove('dragover'));
dropZone.addEventListener('drop', e => { e.preventDefault(); dropZone.classList.remove('dragover'); if (e.dataTransfer.files.length) handleFileObj(e.dataTransfer.files[0]); });

let uploadedFile = null;
function handleFile(input) { if (input.files.length) handleFileObj(input.files[0]); }
function handleFileObj(f) {
  if (!f.name.toLowerCase().endsWith('.pdf')) { showToast('Please upload a PDF file', 'error'); return; }
  uploadedFile = f;
  document.getElementById('fileNameText').textContent = f.name;
  document.getElementById('uploadForm').classList.add('visible');
  dropZone.style.display = 'none';
  // Auto-match client from filename
  if (!document.getElementById('clientName').value) {
    const stem = f.name.replace(/\.pdf$/i, '').replace(/[_-]/g, ' ').toLowerCase();
    const sel = document.getElementById('clientName');
    for (let i = 0; i < sel.options.length; i++) {
      if (sel.options[i].value && stem.includes(sel.options[i].value.toLowerCase())) {
        sel.value = sel.options[i].value;
        break;
      }
    }
  }
}
function resetUpload() {
  uploadedFile = null;
  document.getElementById('fileInput').value = '';
  document.getElementById('uploadForm').classList.remove('visible');
  dropZone.style.display = '';
}

function startExtraction() {
  if (!uploadedFile) return;
  const cn = document.getElementById('clientName').value;
  if (!cn) { showToast('Please select a client', 'error'); return; }
  const fd = new FormData();
  fd.append('pdf', uploadedFile);
  fd.append('year', document.getElementById('taxYear').value);
  fd.append('client_name', cn);
  fd.append('doc_type', selectedDocType);
  fd.append('output_format', selectedOutputFormat);
  fd.append('user_notes', document.getElementById('userNotes').value);
  fd.append('ai_instructions', document.getElementById('aiInstructions').value);
  fd.append('skip_verify', document.getElementById('skipVerify').checked ? 'true' : 'false');
  fd.append('disable_pii', document.getElementById('disablePii').checked ? 'true' : 'false');
  fd.append('use_ocr_first', document.getElementById('useOcrFirst').checked ? 'true' : 'false');

  document.getElementById('startBtn').disabled = true;
  fetch('/api/upload', { method: 'POST', body: fd })
    .then(r => r.json())
    .then(data => {
      if (data.error) { showToast(data.error, 'error'); document.getElementById('startBtn').disabled = false; return; }
      currentJobId = data.job_id;
      document.getElementById('processingFile').textContent = uploadedFile.name;
      showSection('processing');
      document.getElementById('startBtn').disabled = false;
      resetUpload();
      startPolling();
    })
    .catch(e => { showToast('Upload failed: ' + e, 'error'); document.getElementById('startBtn').disabled = false; });
}

// ─── Polling ───
function startPolling() {
  if (pollTimer) clearInterval(pollTimer);
  pollTimer = setInterval(pollStatus, 800);
  pollStatus();
  document.getElementById('procCancelBtn').style.display = '';
}

function cancelJob() {
  if (!currentJobId) return;
  if (!confirm('Cancel this extraction?')) return;
  fetch('/api/cancel/' + currentJobId, { method: 'POST' })
    .then(r => r.json())
    .then(data => {
      if (data.error) { showToast(data.error, 'error'); return; }
      clearInterval(pollTimer); pollTimer = null;
      document.getElementById('procCancelBtn').style.display = 'none';
      showToast('Extraction cancelled', 'info');
      showSection('upload');
    })
    .catch(e => { showToast('Cancel failed: ' + e, 'error'); });
}

function pollStatus() {
  if (!currentJobId) return;
  fetch('/api/status/' + currentJobId).then(r => r.json()).then(data => {
    document.getElementById('procStage').textContent = (data.stage || 'starting').replace(/_/g, ' ');
    const pct = data.progress || 0;
    document.getElementById('procPct').textContent = pct + '%';
    document.getElementById('procBar').style.width = pct + '%';

    // Console output
    const log = data.log || [];
    const console_el = document.getElementById('procConsole');
    console_el.innerHTML = log.slice(-30).map(l => '<div' + (/phase|complete|error|warning/i.test(l) ? ' class="line-highlight"' : '') + '>' + esc(l) + '</div>').join('');
    console_el.scrollTop = console_el.scrollHeight;

    if (data.status === 'complete') {
      clearInterval(pollTimer); pollTimer = null;
      document.getElementById('procCancelBtn').style.display = 'none';
      const costStr = data.cost_usd ? ' ($' + data.cost_usd.toFixed(4) + ')' : '';
      showToast('Extraction complete!' + costStr, 'success');
      document.getElementById('navReview').style.display = '';
      openReview(data);
    } else if (data.status === 'failed' || data.status === 'interrupted') {
      clearInterval(pollTimer); pollTimer = null;
      document.getElementById('procCancelBtn').style.display = 'none';
      showToast(data.status === 'interrupted' ? 'Extraction cancelled' : 'Extraction failed', 'error');
    }
  }).catch(() => {});
}

// ─── Review ───
function openReview(job) {
  showSection('review');
  currentJobId = job.id || job.job_id || currentJobId;

  Promise.all([
    fetch('/api/results/' + currentJobId).then(r => r.json()),
    fetch('/api/verify/' + currentJobId).then(r => r.json()),
    fetch('/api/vendor-categories').then(r => r.json()),
  ]).then(([data, vdata, vcdata]) => {
    reviewData = data;
    verifications = (vdata && vdata.fields) ? vdata.fields : {};
    vendorMap = (vcdata && vcdata.vendors) ? vcdata.vendors : {};
    chartOfAccounts = (vcdata && vcdata.chart_of_accounts) ? vcdata.chart_of_accounts : {};
    if (vdata && vdata.reviewer && !getReviewer()) {
      document.getElementById('reviewerInitials').value = vdata.reviewer;
    }
    // Show client instructions banner
    const clientName = (job.client_name || '');
    if (clientName) {
      fetch('/api/instructions/' + encodeURIComponent(clientName)).then(r => r.json()).then(idata => {
        const rules = (idata.rules || []).filter(r => r.text);
        const banner = document.getElementById('reviewInstructionsBanner');
        if (rules.length) {
          banner.innerHTML = '<strong style="color:#B7791F">&#x26A0; Client Instructions:</strong> ' + rules.map(r => esc(r.text)).join(' &bull; ');
          banner.style.display = '';
        } else {
          banner.style.display = 'none';
        }
      }).catch(() => {});
    }
    countTotalFields();
    updateVerifyBar();
    loadPage(1);
  }).catch(() => { reviewData = null; verifications = {}; loadPage(1); });
}

function countTotalFields() {
  totalFieldCount = 0;
  const skipFields = new Set(['payer_ein','recipient_ssn_last4','tax_year','entity_type','partner_type','state_id','account_number_last4']);
  if (!reviewData || !reviewData.page_map) return;
  for (const pg in reviewData.page_map) {
    reviewData.page_map[pg].forEach((ext, extIdx) => {
      Object.keys(ext.fields || {}).forEach(k => {
        if (skipFields.has(k)) return;
        if (/^txn_\d+_(date|desc|type)$/.test(k)) return;
        const f = ext.fields[k];
        const v = f.value;
        if (typeof v === 'number' || (typeof v === 'string' && /^\-?\$?[\d,]+\.?\d*$/.test(v.trim()))) {
          totalFieldCount++;
        }
      });
    });
  }
}

function updateVerifyBar() {
  const reviewed = Object.keys(verifications).length;
  const pct = totalFieldCount > 0 ? Math.min(100, Math.round(reviewed / totalFieldCount * 100)) : 0;
  document.getElementById('verifyBar').style.width = pct + '%';
  document.getElementById('verifyStats').innerHTML = '<span>' + reviewed + '</span> of <span>' + totalFieldCount + '</span> fields verified (' + pct + '%)';
}

function fieldKey(page, extIdx, fieldName) { return page + ':' + extIdx + ':' + fieldName; }

// ─── Save Verification ───
function saveVerification(key, status, correctedValue, note, category, vendorDesc) {
  const decision = { status: status };
  if (correctedValue !== undefined && correctedValue !== null) decision.corrected_value = correctedValue;
  if (note) decision.note = note;
  if (category) decision.category = category;
  if (vendorDesc) decision.vendor_desc = vendorDesc;
  const existing = verifications[key];
  if (existing && existing.category && !category) {
    decision.category = existing.category;
    if (existing.vendor_desc) decision.vendor_desc = existing.vendor_desc;
  }
  verifications[key] = decision;
  fetch('/api/verify/' + currentJobId, {
    method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ fields: { [key]: decision }, reviewer: getReviewer() })
  }).catch(() => {});
  updateVerifyBar();
}

function confirmField(key) {
  const current = verifications[key];
  if (current && current.status === 'confirmed') {
    // Toggle off — un-confirm
    delete verifications[key];
    fetch('/api/verify/' + currentJobId, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ fields: { [key]: { status: '_remove' } }, reviewer: getReviewer() })
    }).catch(() => {});
    updateVerifyBar();
    loadPage(currentPage, focusedFieldIdx);
    return;
  }
  const nextIdx = focusedFieldIdx + 1;
  saveVerification(key, 'confirmed');
  showToast('\u2713 ' + key.split(':').pop().replace(/_/g,' '), 'success');
  loadPage(currentPage, nextIdx);
}

function flagField(key) {
  const curIdx = focusedFieldIdx;
  const current = verifications[key];
  if (current && current.status === 'flagged') {
    delete verifications[key];
    fetch('/api/verify/' + currentJobId, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ fields: { [key]: { status: '_remove' } }, reviewer: getReviewer() })
    }).catch(() => {});
    updateVerifyBar();
  } else {
    const note = prompt('Flag note (optional):') || '';
    saveVerification(key, 'flagged', null, note);
    showToast('\u26A0 Flagged: ' + key.split(':').pop().replace(/_/g,' '), 'error');
  }
  loadPage(currentPage, curIdx);
}

function startEdit(key, currentVal) {
  const row = document.querySelector('[data-key="' + key.replace(/"/g, '\\"') + '"]');
  if (!row) return;
  const valSpan = row.querySelector('.field-val');
  if (!valSpan) return;
  const input = document.createElement('input');
  input.type = 'text'; input.className = 'field-edit-input';
  input.value = currentVal; input.dataset.key = key; input.dataset.original = currentVal;
  valSpan.innerHTML = ''; valSpan.appendChild(input); input.focus(); input.select();
  function finishEdit() {
    const nv = input.value.trim();
    input.removeEventListener('blur', finishEdit); input.removeEventListener('keydown', onKey);
    if (nv !== '' && nv !== String(currentVal)) {
      saveVerification(key, 'corrected', nv);
    }
    loadPage(currentPage, focusedFieldIdx);
  }
  function onKey(e) { if (e.key === 'Enter') finishEdit(); else if (e.key === 'Escape') { loadPage(currentPage, focusedFieldIdx); } }
  input.addEventListener('blur', finishEdit);
  input.addEventListener('keydown', onKey);
}

function toggleNoteInput(key) {
  const escapedKey = key.replace(/"/g, '\\"');
  const row = document.querySelector('[data-key="' + escapedKey + '"]');
  if (!row) return;
  // Check if note input already exists after this row
  const existing = row.nextElementSibling;
  if (existing && existing.classList.contains('vf-note-input')) {
    existing.remove();
    return;
  }
  const current = verifications[key];
  const currentNote = (current && current.note) || '';
  const div = document.createElement('div');
  div.className = 'vf-note-input';
  div.innerHTML = '<input type="text" placeholder="Add a review note..." value="' + esc(currentNote) + '" onkeydown="if(event.key===\'Enter\')saveFieldNote(\'' + esc(key) + '\',this.value)">'
    + '<button onclick="saveFieldNote(\'' + esc(key) + '\',this.previousElementSibling.value)">Save</button>';
  row.after(div);
  div.querySelector('input').focus();
}

function saveFieldNote(key, note) {
  note = (note || '').trim();
  const current = verifications[key] || {};
  const status = current.status || 'confirmed';
  saveVerification(key, status, current.corrected_value !== undefined ? current.corrected_value : null, note);
  showToast(note ? 'Note saved' : 'Note removed', 'success');
  loadPage(currentPage, focusedFieldIdx);
}

// ─── Category Handling ───
function normalizeVendor(desc) {
  if (!desc) return '';
  var s = String(desc).toUpperCase().trim();
  s = s.replace(/[\s#*]+\d{2,}$/, '');
  s = s.replace(/\s+(LLC|INC|CORP|CO|COMPANY|LTD|LP|NA|N\.A\.)\s*$/i, '');
  s = s.replace(/[\s.,;:*#\-]+$/, '');
  return s.trim();
}
function suggestCategory(desc) {
  if (!desc || !vendorMap) return '';
  var norm = normalizeVendor(desc);
  if (!norm) return '';
  if (vendorMap[norm]) return vendorMap[norm].category || '';
  for (var k in vendorMap) { if (norm.indexOf(k)===0 || k.indexOf(norm)===0) return vendorMap[k].category||''; }
  return '';
}
function buildCategorySelect(vk, vendorDesc, compact) {
  var existing = verifications[vk];
  var currentCat = (existing && existing.category) ? existing.category : '';
  var suggested = !currentCat && vendorDesc ? suggestCategory(vendorDesc) : '';
  var activeCat = currentCat || suggested;
  var cls = 'cat-select' + (currentCat ? ' cat-set' : suggested ? ' cat-suggested' : '');
  var h = '<select class="'+cls+'" onchange="saveFieldCategory(\''+esc(vk)+'\',this,\''+esc(String(vendorDesc||'').replace(/'/g,"\\'"))+'\')" title="'+(activeCat?esc(activeCat):'Assign category')+'">';
  h += '<option value="">'+(compact?'\u2014':'— Category —')+'</option>';
  for (var g in chartOfAccounts) {
    h += '<optgroup label="'+esc(g)+'">';
    (chartOfAccounts[g]||[]).forEach(function(a) { h += '<option value="'+esc(a)+'"'+(a===activeCat?' selected':'')+'>'+esc(a)+'</option>'; });
    h += '</optgroup>';
  }
  h += '</select>';
  if (suggested && !currentCat) h += ' <span class="cat-learned-badge">auto</span>';
  return h;
}
function saveFieldCategory(vk, sel, vendorDesc) {
  var cat = sel.value;
  sel.className = cat ? 'cat-select cat-set' : 'cat-select';
  var badge = sel.parentElement ? sel.parentElement.querySelector('.cat-learned-badge') : null;
  if (badge) badge.remove();
  var ex = verifications[vk] || {};
  saveVerification(vk, ex.status||'confirmed', ex.corrected_value||undefined, ex.note||undefined, cat, vendorDesc);
  if (cat && vendorDesc) { var n = normalizeVendor(vendorDesc); if(n) vendorMap[n] = {category:cat,count:1}; }
  if (cat) showToast('\uD83D\uDCC1 ' + cat, 'success');
}
function needsCategoryPicker(fn, dt) {
  if (!dt) return false;
  if (/^txn_\d+_amount$/.test(fn)) return false;
  if (dt === 'check' && fn === 'check_amount') return true;
  if (/invoice/.test(dt) && fn === 'total_amount') return true;
  if (/receipt/.test(dt) && fn === 'total_amount') return true;
  return false;
}

function toggleInfoSection(id, toggle) {
  const el = document.getElementById(id);
  if (!el) return;
  const showing = el.style.display !== 'none';
  el.style.display = showing ? 'none' : 'block';
  const arrow = toggle.querySelector('.info-toggle-arrow');
  if (arrow) arrow.classList.toggle('open', !showing);
}

// ─── Page Rendering ───
function loadPage(page, focusIdx) {
  if (!reviewData || !reviewData.page_map) return;
  totalPages = reviewData.total_pages || Object.keys(reviewData.page_map).length;
  if (page < 1) page = 1;
  if (page > totalPages) page = totalPages;
  currentPage = page;
  focusedFieldIdx = (focusIdx !== undefined && focusIdx !== null) ? focusIdx : 0;
  pageFieldKeys = [];

  document.getElementById('reviewPager').textContent = page + ' / ' + totalPages;
  document.getElementById('pdfViewer').innerHTML = '<img src="/api/page-image/' + currentJobId + '/' + page + '" alt="Page ' + page + '">';

  const pageExts = reviewData.page_map[currentPage];
  let html = '';

  if (!pageExts || pageExts.length === 0) {
    html = '<div style="padding:40px 20px;text-align:center;color:var(--text-light)">'
      + '<div style="font-size:32px;margin-bottom:12px">&#128196;</div>'
      + '<div style="font-size:14px;font-weight:600;margin-bottom:6px">No extracted data for this page</div>'
      + '<div style="font-size:12px">This page may be a continuation, composite summary, or supplemental info that was processed as part of another document.</div>'
      + '<div style="margin-top:12px"><button class="btn btn-secondary btn-sm" onclick="reextractPage()">&#x21BB; Re-extract this page</button></div>'
      + '</div>';
    document.getElementById('fieldsPanel').innerHTML = html;
    updateVerifyBar();
    return;
  }

  const skipFields = new Set(['payer_ein','recipient_ssn_last4','tax_year','entity_type','partner_type','state_id','account_number_last4']);

  pageExts.forEach((ext, extIdx) => {
    const fields = ext.fields || {};
    // Sort fields by document-type order (box/line number), then alphabetical for unknowns
    const docOrder = FIELD_ORDER[ext.document_type] || [];
    const allKeys = Object.keys(fields).sort((a, b) => {
      const ai = docOrder.indexOf(a), bi = docOrder.indexOf(b);
      if (ai !== -1 && bi !== -1) return ai - bi;
      if (ai !== -1) return -1;
      if (bi !== -1) return 1;
      return a.localeCompare(b);
    });

    html += '<div class="field-group">';
    html += '<div class="field-group-title">' + esc(ext.document_type) + '</div>';
    html += '<div class="field-entity"><span>' + esc(ext.entity) + '</span></div>';

    // Separate txn fields from summary
    const txnRegex = /^txn_(\d+)_(date|desc|amount|type)$/;
    const summaryKeys = allKeys.filter(k => !txnRegex.test(k));
    const txnKeys = allKeys.filter(k => txnRegex.test(k));
    const txnGroups = {};
    txnKeys.forEach(k => { const m = k.match(txnRegex); if(m) { if(!txnGroups[m[1]]) txnGroups[m[1]]={}; txnGroups[m[1]][m[2]]=k; }});
    const txnNums = Object.keys(txnGroups).sort((a,b)=>parseInt(a)-parseInt(b));

    // Split monetary vs info, filter out $0.00 unless significant
    const monetaryKeys = summaryKeys.filter(k => {
      if (skipFields.has(k)) return false;
      const v = fields[k].value;
      const isNumeric = typeof v === 'number' || (typeof v === 'string' && /^\-?\$?[\d,]+\.?\d*$/.test(v.trim()));
      if (!isNumeric) return false;
      // Filter out zero values unless field name suggests significance
      const numVal = typeof v === 'number' ? v : parseFloat(String(v).replace(/[$,]/g, ''));
      if (numVal === 0 && !/(balance|total|net)/i.test(k)) return false;
      return true;
    });
    const infoKeys = summaryKeys.filter(k => {
      if (skipFields.has(k)) return true;
      const v = fields[k].value;
      return !(typeof v === 'number' || (typeof v === 'string' && /^\-?\$?[\d,]+\.?\d*$/.test(v.trim())));
    });

    // Info section (collapsible)
    if (infoKeys.length > 0) {
      const colId = 'info-' + currentPage + '-' + extIdx;
      html += '<div class="info-section">';
      html += '<div class="info-toggle" onclick="toggleInfoSection(\'' + colId + '\',this)">';
      html += '<span class="info-toggle-arrow">\u25B6</span> Document Info (' + infoKeys.length + ')</div>';
      html += '<div class="info-fields" id="' + colId + '" style="display:none">';
      infoKeys.forEach(k => {
        const v = fields[k].value;
        html += '<div class="info-field"><span class="info-field-name">' + esc(k.replace(/_/g,' ').replace(/\b\w/g,c=>c.toUpperCase())) + '</span><span class="info-field-val">' + esc(v==null?'\u2014':String(v)) + '</span></div>';
      });
      html += '</div></div>';
    }

    // Monetary fields
    monetaryKeys.forEach(k => {
      const f = fields[k];
      const vk = fieldKey(currentPage, extIdx, k);
      const vstate = verifications[vk] || null;
      pageFieldKeys.push(vk);
      const idx = pageFieldKeys.length - 1;

      const rawVal = f.value;
      let displayVal = rawVal;
      if (vstate && vstate.corrected_value !== undefined) displayVal = vstate.corrected_value;
      const displayStr = typeof displayVal === 'number' ? displayVal.toLocaleString('en-US',{minimumFractionDigits:2,maximumFractionDigits:2}) : String(displayVal||'');

      let rowClass = 'field-row';
      if (idx === focusedFieldIdx) rowClass += ' focused';
      if (vstate) { if (vstate.status==='confirmed') rowClass+=' vf-confirmed'; else if (vstate.status==='corrected') rowClass+=' vf-corrected'; else if (vstate.status==='flagged') rowClass+=' vf-flagged'; }

      const conf = f.confidence || '';
      let dotClass = 'conf-other';
      if (conf.includes('dual')) dotClass='conf-dual'; else if (conf.includes('confirmed')||conf==='ocr_accepted') dotClass='conf-confirmed'; else if (conf.includes('corrected')) dotClass='conf-corrected'; else if (conf==='low') dotClass='conf-low';

      html += '<div class="' + rowClass + '" data-key="' + esc(vk) + '" onclick="setFocus(' + idx + ')">';
      const boxLabel = (FIELD_BOX_LABELS[ext.document_type] || {})[k];
      const displayName = (boxLabel ? boxLabel + ' \u2014 ' : '') + k.replace(/_/g,' ').replace(/\b\w/g,c=>c.toUpperCase());
      html += '<span class="field-name">' + esc(displayName) + '</span>';
      html += '<span class="field-val-wrap"><span class="conf-dot ' + dotClass + '"></span>';
      html += '<span class="field-val" ondblclick="event.stopPropagation();startEdit(\'' + esc(vk) + '\',' + JSON.stringify(displayStr) + ')">' + esc(displayStr) + '</span>';
      html += '<span class="field-actions">';
      html += '<button class="vf-btn" onclick="event.stopPropagation();startEdit(\'' + esc(vk) + '\',' + JSON.stringify(displayStr) + ')" title="Edit value (E)">&#x270F;</button>';
      html += '<button class="vf-btn vf-btn-confirm' + (vstate&&vstate.status==='confirmed'?' active':'') + '" onclick="event.stopPropagation();confirmField(\'' + esc(vk) + '\')" title="Confirm (Enter)">\u2713</button>';
      html += '<button class="vf-btn vf-btn-flag' + (vstate&&vstate.status==='flagged'?' active':'') + '" onclick="event.stopPropagation();flagField(\'' + esc(vk) + '\')" title="Flag (F)">\u2691</button>';
      html += '<button class="vf-btn vf-btn-note' + (vstate&&vstate.note?' has-note':'') + '" onclick="event.stopPropagation();toggleNoteInput(\'' + esc(vk) + '\')" title="Add note (N)">&#x270E;</button>';
      html += '</span></span></div>';

      if (vstate && vstate.status==='corrected') { html += '<div class="vf-note"><span class="vf-original">' + esc(String(rawVal)) + '</span> \u2192 ' + esc(displayStr) + '</div>'; }
      if (vstate && vstate.note) { html += '<div class="vf-note" id="note-' + esc(vk) + '">' + esc(vstate.note) + '</div>'; }

      if (needsCategoryPicker(k, ext.document_type)) {
        var cv = '';
        if (ext.document_type==='check') { cv = (fields.payee&&fields.payee.value)||(fields.pay_to&&fields.pay_to.value)||ext.entity||''; }
        else { cv = (fields.vendor_name&&fields.vendor_name.value)||ext.entity||''; }
        html += '<div class="field-cat-row"><label>Account:</label>' + buildCategorySelect(vk, String(cv), false) + '</div>';
      }
    });

    // Transaction table
    if (txnNums.length > 0) {
      html += '<div class="txn-section"><div class="txn-header">Transactions (' + txnNums.length + ')</div>';
      html += '<table class="txn-table"><thead><tr><th>Date</th><th>Description</th><th class="txn-amt">Amount</th><th>Type</th><th>Category</th><th></th></tr></thead><tbody>';
      txnNums.forEach(num => {
        const grp = txnGroups[num];
        const pk = grp.amount||grp.desc||grp.date;
        if (!pk) return;
        const vk = fieldKey(currentPage, extIdx, pk);
        const vstate = verifications[vk]||null;
        pageFieldKeys.push(vk);
        let trCls = '';
        if (vstate) { if (vstate.status==='confirmed') trCls=' class="vf-confirmed"'; else if (vstate.status==='flagged') trCls=' class="vf-flagged"'; }
        const dv = grp.date ? (fields[grp.date].value||'') : '';
        const descV = grp.desc ? (fields[grp.desc].value||'') : '';
        const amtF = grp.amount ? fields[grp.amount] : null;
        const amtV = amtF ? (typeof amtF.value==='number'?amtF.value.toLocaleString('en-US',{minimumFractionDigits:2,maximumFractionDigits:2}):(amtF.value||'')) : '';
        const tpV = grp.type ? (fields[grp.type].value||'') : '';
        const conf = amtF ? (amtF.confidence||'') : '';
        let dc = 'conf-other';
        if (conf.includes('dual')) dc='conf-dual'; else if (conf.includes('confirmed')||conf==='ocr_accepted') dc='conf-confirmed'; else if (conf.includes('corrected')) dc='conf-corrected'; else if (conf==='low') dc='conf-low';
        html += '<tr' + trCls + ' data-key="' + esc(vk) + '">';
        html += '<td>'+esc(String(dv))+'</td><td>'+esc(String(descV))+'</td>';
        html += '<td class="txn-amt"><span class="conf-dot '+dc+'"></span>'+esc(String(amtV))+'</td>';
        html += '<td><span class="txn-type txn-type-'+esc(String(tpV).toLowerCase())+'">'+esc(String(tpV))+'</span></td>';
        html += '<td>'+buildCategorySelect(vk,String(descV),true)+'</td>';
        html += '<td><button class="vf-btn vf-btn-confirm'+(vstate&&vstate.status==='confirmed'?' active':'')+'" onclick="confirmField(\''+esc(vk)+'\')">\u2713</button></td>';
        html += '</tr>';
      });
      html += '</tbody></table></div>';
    }

    html += '</div>'; // field-group
  });

  document.getElementById('fieldsPanel').innerHTML = html;
  if (focusedFieldIdx >= pageFieldKeys.length) focusedFieldIdx = Math.max(0, pageFieldKeys.length - 1);
}

function setFocus(idx) {
  // Don't rebuild page if an edit input is active (would destroy it)
  if (document.querySelector('.field-edit-input')) return;
  focusedFieldIdx = idx; loadPage(currentPage, idx);
}
function prevPage() { if (currentPage > 1) loadPage(currentPage - 1); }
function nextPage() { if (currentPage < totalPages) loadPage(currentPage + 1); }

function reextractPage() {
  if (!currentJobId || !currentPage) return;
  const instructions = prompt('Enter instructions for re-extracting this page (e.g., "This is a K-1 Schedule, focus on box 1-3"):');
  if (!instructions) return;
  showToast('Re-extracting page ' + currentPage + '...', 'info');
  fetch('/api/reextract-page/' + currentJobId + '/' + currentPage, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ instructions: instructions })
  })
  .then(r => r.json())
  .then(data => {
    if (data.error) { showToast('Re-extract failed: ' + data.error, 'error'); return; }
    showToast('Page ' + currentPage + ' re-extracted successfully', 'success');
    // Reload the review data to pick up the new extraction
    fetch('/api/results/' + currentJobId).then(r => r.json()).then(rd => {
      if (rd.error) return;
      reviewData = rd;
      verifications = rd.verifications || {};
      totalPages = rd.total_pages || 1;
      loadPage(currentPage);
    });
  })
  .catch(e => { showToast('Re-extract failed: ' + e, 'error'); });
}

// ─── Downloads ───
function downloadFile(type) {
  if (!currentJobId) return;
  window.location = '/api/download' + (type==='log'?'-log':'') + '/' + currentJobId;
}

function regenExcel() {
  if (!currentJobId) return;
  showToast('Regenerating Excel...', 'info');
  fetch('/api/regen-excel/' + currentJobId, { method: 'POST' })
    .then(r => r.json())
    .then(data => {
      if (data.error) { showToast('Regen failed: ' + data.error, 'error'); return; }
      showToast('Excel regenerated successfully', 'success');
    })
    .catch(e => { showToast('Regen failed: ' + e, 'error'); });
}

// ─── AI Chat ───
function toggleAiChat() {
  const panel = document.getElementById('aiChatPanel');
  const isVisible = panel.style.display !== 'none';
  panel.style.display = isVisible ? 'none' : '';
  if (!isVisible) {
    document.getElementById('aiChatPage').textContent = currentPage;
    document.getElementById('aiChatInput').focus();
  }
}

function sendAiChat() {
  const input = document.getElementById('aiChatInput');
  const message = input.value.trim();
  if (!message || !currentJobId) return;
  input.value = '';

  const msgs = document.getElementById('aiChatMessages');
  msgs.innerHTML += '<div style="margin-bottom:8px;"><strong style="color:var(--navy);">You:</strong> ' + esc(message) + '</div>';
  msgs.innerHTML += '<div id="aiTyping" style="margin-bottom:8px;color:var(--text-light);font-style:italic;">AI is thinking...</div>';
  msgs.scrollTop = msgs.scrollHeight;

  fetch('/api/ai-chat/' + currentJobId, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ message: message, page: currentPage })
  })
  .then(r => r.json())
  .then(data => {
    const typing = document.getElementById('aiTyping');
    if (typing) typing.remove();
    if (data.error) {
      msgs.innerHTML += '<div style="margin-bottom:8px;color:var(--red);">Error: ' + esc(data.error) + '</div>';
    } else {
      msgs.innerHTML += '<div style="margin-bottom:8px;"><strong style="color:var(--accent);">AI:</strong> ' + esc(data.reply).replace(/\n/g, '<br>') + '</div>';
    }
    msgs.scrollTop = msgs.scrollHeight;
  })
  .catch(e => {
    const typing = document.getElementById('aiTyping');
    if (typing) typing.remove();
    msgs.innerHTML += '<div style="margin-bottom:8px;color:var(--red);">Error: ' + esc(String(e)) + '</div>';
  });
}

// ─── History ───
function loadJobs() {
  fetch('/api/jobs').then(r=>r.json()).then(data => {
    allJobs = data;
    document.getElementById('historyCount').textContent = data.length;
    renderHistory(data);
  }).catch(()=>{});
}

function renderHistory(data) {
  const body = document.getElementById('historyBody');
  if (!data.length) { body.innerHTML = '<tr><td colspan="8" style="text-align:center;padding:24px;color:var(--text-light)">No jobs yet</td></tr>'; return; }
  body.innerHTML = data.map(j => {
    const dt = j.created ? new Date(j.created).toLocaleDateString('en-US',{month:'short',day:'numeric',hour:'numeric',minute:'2-digit'}) : '';
    const typeLabel = DOC_TYPES.find(d=>d.id===j.doc_type);
    const costStr = j.cost_usd ? '$' + j.cost_usd.toFixed(4) : '—';
    return '<tr>' +
      '<td><strong>' + esc(j.client_name||'—') + '</strong></td>' +
      '<td>' + esc(j.filename||'') + '</td>' +
      '<td><span class="badge badge-blue">' + esc(typeLabel?typeLabel.label:j.doc_type||'') + '</span></td>' +
      '<td>' + esc(j.year||'') + '</td>' +
      '<td><span class="job-status ' + (j.status||'') + '">' + esc(j.status||'') + '</span></td>' +
      '<td style="font-size:12px;font-family:var(--mono);color:var(--text-secondary)">' + costStr + '</td>' +
      '<td style="font-size:12px;color:var(--text-secondary)">' + dt + '</td>' +
      '<td class="actions">' +
        (j.status==='complete'?'<button class="btn btn-sm btn-secondary" onclick=\'openReview('+JSON.stringify({id:j.id,client_name:j.client_name})+')\'>\u{1F50D} Review</button> ':'') +
        (j.status==='failed'||j.status==='interrupted'||j.status==='error'?'<button class="btn btn-sm btn-secondary" onclick="retryJob(\''+j.id+'\')">Retry</button> ':'') +
        '<button class="btn btn-ghost btn-sm" onclick="deleteJob(\''+j.id+'\')" title="Delete">\u2716</button>' +
      '</td></tr>';
  }).join('');
}

function filterHistory() {
  const q = (document.getElementById('historySearch').value||'').toLowerCase();
  const s = document.getElementById('historyStatusFilter').value;
  const filtered = allJobs.filter(j => {
    if (s && j.status !== s) return false;
    if (q && !(j.client_name||'').toLowerCase().includes(q) && !(j.filename||'').toLowerCase().includes(q)) return false;
    return true;
  });
  renderHistory(filtered);
}

function retryJob(id) { fetch('/api/retry/'+id,{method:'POST'}).then(r=>r.json()).then(d=>{if(d.job_id){currentJobId=d.job_id;showSection('processing');startPolling();}}).catch(()=>{}); }
function deleteJob(id) { if(!confirm('Delete this job?')) return; fetch('/api/delete/'+id,{method:'POST'}).then(()=>loadJobs()).catch(()=>{}); }

// ─── Clients ───
function loadClientSuggestions() {
  fetch('/api/clients').then(r=>r.json()).then(data => {
    const sel = document.getElementById('clientName');
    const current = sel.value;
    sel.innerHTML = '<option value="">\u2014 Select client \u2014</option>' +
      data.map(c => {
        const label = c.ein_last4 ? c.name + ' (' + c.ein_last4 + ')' : c.name;
        return '<option value="' + esc(c.name) + '">' + esc(label) + '</option>';
      }).join('');
    if (current) sel.value = current;
  }).catch(()=>{});
}

let allClientsData = [];
function loadClients() {
  fetch('/api/clients').then(r=>r.json()).then(data => {
    allClientsData = data;
    renderClientGrid(data);
  }).catch(()=>{});
}
function filterClients() {
  const q = (document.getElementById('clientSearch').value||'').toLowerCase();
  renderClientGrid(allClientsData.filter(c => c.name.toLowerCase().includes(q)));
}
function renderClientGrid(clients) {
  const g = document.getElementById('clientGrid');
  if (!clients.length) { g.innerHTML = '<div class="empty-state"><h3>No clients yet</h3><p>Upload a document to create a client record.</p></div>'; return; }
  g.innerHTML = clients.map(c => {
    let badges = '';
    if (c.ein_last4) badges += '<span class="badge badge-purple">EIN \u2026'+esc(c.ein_last4)+'</span>';
    if (c.has_context) badges += '<span class="badge badge-purple">Context</span>';
    if (c.has_instructions) badges += '<span class="badge badge-blue">Instructions</span>';
    return '<div class="client-card" onclick="openClientDetail(\''+esc(c.name)+'\')">' +
      '<h4>'+esc(c.name)+'</h4>' +
      '<div class="client-meta"><span>'+c.jobs+' job'+(c.jobs!==1?'s':'')+'</span>' +
      (c.years.length?'<span>'+c.years.join(', ')+'</span>':'') + '</div>' +
      (badges?'<div class="client-badges">'+badges+'</div>':'') +
      '</div>';
  }).join('');
}

function openClientDetail(name) {
  currentClientName = name;
  document.getElementById('clientListView').style.display = 'none';
  document.getElementById('clientDetailView').classList.add('visible');
  document.getElementById('clientDetailName').textContent = name;
  showClientTab('documents');
  loadClientDocuments(name);
  loadClientInfo(name);
  loadContextDocs(name);
  loadInstructions(name);
}
function closeClientDetail() {
  document.getElementById('clientListView').style.display = '';
  document.getElementById('clientDetailView').classList.remove('visible');
  currentClientName = '';
}
function showClientTab(tab) {
  document.querySelectorAll('.client-tab').forEach(t => t.classList.remove('active'));
  document.querySelectorAll('.client-tab-content').forEach(t => t.classList.remove('active'));
  const tabContent = document.querySelector('.client-tab-content#tab-'+tab);
  if (tabContent) tabContent.classList.add('active');
  const tabBtn = document.querySelector('.client-tab[data-tab="'+tab+'"]');
  if (tabBtn) tabBtn.classList.add('active');
  if (tab === 'completeness') loadCompleteness(currentClientName);
  if (tab === 'documents') loadClientDocuments(currentClientName);
}

// Client Info
function loadClientInfo(name) {
  fetch('/api/clients/'+encodeURIComponent(name)+'/info').then(r=>r.json()).then(info => {
    const el = document.getElementById('clientDetailMeta');
    let parts = [];
    if (info.ein_last4) parts.push('<span class="badge badge-purple">EIN \u2026'+esc(info.ein_last4)+'</span>');
    if (info.contact) parts.push('<span>'+esc(info.contact)+'</span>');
    if (info.notes) parts.push('<span style="color:var(--text-light)">'+esc(info.notes)+'</span>');
    el.innerHTML = parts.join(' &middot; ');
  }).catch(()=>{});
}

// Client Documents
let clientCompletedJobs = [];
function loadClientDocuments(name) {
  fetch('/api/clients/'+encodeURIComponent(name)+'/documents').then(r=>r.json()).then(data => {
    const el = document.getElementById('clientDocGroups');
    const docs = data.documents || [];
    clientCompletedJobs = docs.filter(d => d.status === 'complete');
    if (!docs.length) {
      el.innerHTML = '<div class="empty-state"><p>No documents yet. Upload a PDF from the Upload section.</p></div>';
      return;
    }
    const grouped = data.grouped || {};
    const hasComplete = clientCompletedJobs.length > 0;
    let html = '<div style="margin-bottom:16px;display:flex;justify-content:space-between;align-items:center">';
    html += '<span style="font-size:13px;color:var(--text-secondary)">' + docs.length + ' document' + (docs.length!==1?'s':'') + '</span>';
    if (hasComplete) html += '<button class="btn btn-primary btn-sm" onclick="openReportModal()">\u{1F4CA} Generate Report</button>';
    html += '</div>';
    const typeLabels = {tax_returns:'Tax Returns',bank_statements:'Bank Statements',trust_documents:'Trust Documents',bookkeeping:'Bookkeeping',payroll:'Payroll',other:'Other'};
    for (const [dtype, items] of Object.entries(grouped)) {
      const label = typeLabels[dtype] || dtype;
      html += '<div class="card" style="margin-bottom:12px"><div class="card-header"><h3>'+esc(label)+' ('+items.length+')</h3></div>';
      html += '<div class="card-body" style="padding:0"><table style="width:100%;font-size:13px;border-collapse:collapse">';
      html += '<tr style="background:var(--bg);border-bottom:1px solid var(--border)"><th style="padding:8px 12px;text-align:left">File</th><th style="padding:8px 12px;text-align:left">Year</th><th style="padding:8px 12px;text-align:left">Status</th><th style="padding:8px 12px;text-align:left">Cost</th><th style="padding:8px 12px;text-align:right">Actions</th></tr>';
      items.forEach(d => {
        const statusClass = d.status==='complete'?'badge-green':d.status==='running'?'badge-blue':'badge-yellow';
        html += '<tr style="border-bottom:1px solid var(--border)">';
        html += '<td style="padding:8px 12px">'+esc(d.filename)+'</td>';
        html += '<td style="padding:8px 12px">'+esc(d.year)+'</td>';
        html += '<td style="padding:8px 12px"><span class="badge '+statusClass+'">'+esc(d.status)+'</span></td>';
        html += '<td style="padding:8px 12px">'+(d.cost_usd != null && d.status==='complete' ? '$'+Number(d.cost_usd).toFixed(4) : '\u2014')+'</td>';
        html += '<td style="padding:8px 12px;text-align:right">';
        if (d.status === 'complete') {
          html += '<button class="btn btn-secondary btn-sm" onclick="openReview({id:\''+esc(d.job_id)+'\'})">Review</button> ';
          if (d.has_xlsx) html += '<a class="btn btn-ghost btn-sm" href="/api/download/'+esc(d.job_id)+'" title="Download Excel">\u{1F4CA}</a> ';
          if (d.has_log) html += '<a class="btn btn-ghost btn-sm" href="/api/download-log/'+esc(d.job_id)+'" title="Download JSON log">\u{1F4CB}</a> ';
        }
        html += '</td></tr>';
      });
      html += '</table></div></div>';
    }
    el.innerHTML = html;
  }).catch(()=>{});
}

// Context
function loadContextDocs(name) {
  fetch('/api/context/'+encodeURIComponent(name)).then(r=>r.json()).then(data => {
    const docs = data.documents || [];
    const el = document.getElementById('contextDocList');
    if (!docs.length) { el.innerHTML = '<div class="empty-state" style="padding:24px"><p>No context documents uploaded yet.</p></div>'; return; }
    el.innerHTML = docs.map(d => '<div class="context-doc">' +
      '<div class="context-doc-icon">\uD83D\uDCC4</div>' +
      '<div class="context-doc-info"><div class="name">'+esc(d.label||d.original_name)+'</div>' +
      '<div class="meta">'+esc(d.year||'')+' &bull; '+d.payer_count+' payers found &bull; '+esc(d.uploaded||'').split('T')[0]+'</div></div>' +
      '<button class="btn btn-ghost btn-sm" onclick="deleteContext(\''+esc(currentClientName)+'\',\''+esc(d.id)+'\')">&#x2716;</button>' +
    '</div>').join('');
  }).catch(()=>{});
}
function uploadContext() {
  const file = document.getElementById('contextFile').files[0];
  if (!file) { showToast('Select a file', 'error'); return; }
  const fd = new FormData();
  fd.append('file', file);
  fd.append('year', document.getElementById('contextYear').value);
  fd.append('label', document.getElementById('contextLabel').value);
  fetch('/api/context/'+encodeURIComponent(currentClientName)+'/upload', {method:'POST', body:fd})
    .then(r=>r.json()).then(d => {
      if (d.error) { showToast(d.error,'error'); return; }
      showToast('Context uploaded — '+d.payers_found+' payers found', 'success');
      document.getElementById('contextFile').value = '';
      document.getElementById('contextLabel').value = '';
      loadContextDocs(currentClientName);
    }).catch(e => showToast('Upload failed','error'));
}
function deleteContext(client, docId) {
  if (!confirm('Delete this context document?')) return;
  fetch('/api/context/'+encodeURIComponent(client)+'/'+docId, {method:'DELETE'}).then(()=>loadContextDocs(client)).catch(()=>{});
}

// Instructions
function loadInstructions(name) {
  fetch('/api/instructions/'+encodeURIComponent(name)).then(r=>r.json()).then(data => {
    const rules = data.rules || [];
    const el = document.getElementById('instructionsList');
    if (!rules.length) { el.innerHTML = '<div class="empty-state" style="padding:24px"><p>No instructions set.</p></div>'; return; }
    el.innerHTML = rules.map(r => '<div class="instruction-item">' +
      '<div class="inst-text">'+esc(r.text)+'</div>' +
      '<div class="inst-date">'+esc((r.created||'').split('T')[0])+'</div>' +
      '<button class="btn btn-ghost btn-sm" onclick="deleteInstruction(\''+esc(currentClientName)+'\',\''+esc(r.id)+'\')">&#x2716;</button>' +
    '</div>').join('');
  }).catch(()=>{});
}
function addInstruction() {
  const text = document.getElementById('newInstruction').value.trim();
  if (!text) { showToast('Enter an instruction','error'); return; }
  fetch('/api/instructions/'+encodeURIComponent(currentClientName), {
    method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({text:text})
  }).then(r=>r.json()).then(d => {
    if (d.error) { showToast(d.error,'error'); return; }
    showToast('Instruction added','success');
    document.getElementById('newInstruction').value = '';
    loadInstructions(currentClientName);
  }).catch(()=>{});
}
function deleteInstruction(client, ruleId) {
  fetch('/api/instructions/'+encodeURIComponent(client)+'/'+ruleId, {method:'DELETE'}).then(()=>loadInstructions(client)).catch(()=>{});
}

// Completeness
function loadCompleteness(name) {
  fetch('/api/context/'+encodeURIComponent(name)+'/completeness').then(r=>r.json()).then(data => {
    const el = document.getElementById('completenessReport');
    const matched = data.matched || [];
    const missing = data.missing || [];
    const newI = data.new || [];
    if (!matched.length && !missing.length && !newI.length) {
      el.innerHTML = '<div class="empty-state"><p>Upload prior-year context to enable completeness tracking. Then process current-year documents to compare.</p></div>';
      return;
    }
    let h = '';
    if (missing.length) {
      h += '<div class="card" style="margin-bottom:12px"><div class="card-header"><h3 style="color:var(--red)">\u26A0 Missing ('+missing.length+')</h3></div><div class="card-body" style="padding:0">';
      missing.forEach(m => { h += '<div class="completeness-item"><div class="completeness-icon">\u23F3</div><div class="completeness-info"><div class="ci-form">'+esc(m.form)+'</div><div class="ci-payer">'+esc(m.payer)+' (EIN '+esc(m.ein)+')</div></div><span class="badge badge-red">Expected</span></div>'; });
      h += '</div></div>';
    }
    if (matched.length) {
      h += '<div class="card" style="margin-bottom:12px"><div class="card-header"><h3 style="color:var(--green)">\u2705 Received ('+matched.length+')</h3></div><div class="card-body" style="padding:0">';
      matched.forEach(m => { h += '<div class="completeness-item"><div class="completeness-icon">\u2705</div><div class="completeness-info"><div class="ci-form">'+esc(m.form)+'</div><div class="ci-payer">'+esc(m.payer)+'</div></div><span class="badge badge-green">Received</span></div>'; });
      h += '</div></div>';
    }
    if (newI.length) {
      h += '<div class="card"><div class="card-header"><h3 style="color:var(--accent)">\u2728 New This Year ('+newI.length+')</h3></div><div class="card-body" style="padding:0">';
      newI.forEach(m => { h += '<div class="completeness-item"><div class="completeness-icon">\uD83C\uDD95</div><div class="completeness-info"><div class="ci-form">'+esc(m.form)+'</div><div class="ci-payer">'+esc(m.payer)+'</div></div><span class="badge badge-blue">New</span></div>'; });
      h += '</div></div>';
    }
    el.innerHTML = h;
  }).catch(()=>{});
}

// ─── Batch Categorize ───
function loadBatchData() {
  const client = (document.getElementById('batchClientFilter').value||'').trim();
  const showAll = document.getElementById('batchShowAll').checked;
  fetch('/api/batch-categories?client='+encodeURIComponent(client)+'&all='+(showAll?'true':'false'))
    .then(r=>r.json()).then(data => {
      batchData = data;
      chartOfAccounts = data.chart_of_accounts || chartOfAccounts;
      renderBatchStats(data);
      renderBatchGroups(data.groups || []);
    }).catch(()=>{});
}
function renderBatchStats(data) {
  document.getElementById('batchStats').innerHTML =
    '<div class="batch-stat"><div class="stat-num">' + (data.total||0) + '</div><div class="stat-label">Total Transactions</div></div>' +
    '<div class="batch-stat"><div class="stat-num" style="color:var(--green)">' + (data.categorized||0) + '</div><div class="stat-label">Categorized</div></div>' +
    '<div class="batch-stat"><div class="stat-num" style="color:var(--yellow)">' + (data.uncategorized||0) + '</div><div class="stat-label">Uncategorized</div></div>';
}
function renderBatchGroups(groups) {
  const el = document.getElementById('batchVendorGroups');
  if (!groups.length) { el.innerHTML = '<div class="empty-state"><h3>No transactions found</h3><p>Process some bank statements or credit card statements first.</p></div>'; return; }
  el.innerHTML = groups.map((g, gi) => {
    let catSel = '<select class="cat-select'+(g.current?' cat-set':g.suggested?' cat-suggested':'')+'" id="bcat-'+gi+'">';
    catSel += '<option value="">— Category —</option>';
    for (const grp in chartOfAccounts) {
      catSel += '<optgroup label="'+esc(grp)+'">';
      (chartOfAccounts[grp]||[]).forEach(a => { catSel += '<option value="'+esc(a)+'"'+((a===(g.current||g.suggested))?' selected':'')+'>'+esc(a)+'</option>'; });
      catSel += '</optgroup>';
    }
    catSel += '</select>';
    return '<div class="vendor-group">' +
      '<div class="vendor-group-header" onclick="toggleBatchGroup('+gi+')">' +
      '<span class="vg-name">'+esc(g.display_name||g.vendor)+'</span>' +
      '<span class="vg-count">'+g.count+' txn'+(g.count!==1?'s':'')+'</span>' +
      '<span class="vg-amount">$'+Math.abs(g.total_amount).toLocaleString('en-US',{minimumFractionDigits:2})+'</span>' +
      catSel +
      ' <button class="btn btn-sm btn-primary" onclick="event.stopPropagation();applyBatchCategory('+gi+')">Apply</button>' +
      (g.suggested&&!g.current?' <span class="cat-learned-badge">auto</span>':'') +
      '</div>' +
      '<div class="vendor-group-items" id="bg-'+gi+'">' +
      '<table class="data-table" style="font-size:12px"><thead><tr><th>Date</th><th>Description</th><th style="text-align:right">Amount</th><th>Source</th></tr></thead><tbody>' +
      (g.items||[]).map(it => '<tr><td>'+esc(it.date||'')+'</td><td>'+esc(it.desc||'')+'</td><td class="amount">$'+Math.abs(it.amount||0).toLocaleString('en-US',{minimumFractionDigits:2})+'</td><td>'+esc(it.source||'')+'</td></tr>').join('') +
      '</tbody></table></div></div>';
  }).join('');
}
function toggleBatchGroup(i) {
  const el = document.getElementById('bg-'+i);
  if (el) el.classList.toggle('open');
}
function filterBatchVendors() {
  const q = (document.getElementById('batchSearch').value||'').toLowerCase();
  if (!batchData) return;
  const filtered = (batchData.groups||[]).filter(g => (g.vendor||'').toLowerCase().includes(q) || (g.display_name||'').toLowerCase().includes(q));
  renderBatchGroups(filtered);
}
function applyBatchCategory(gi) {
  if (!batchData || !batchData.groups || !batchData.groups[gi]) return;
  const g = batchData.groups[gi];
  const sel = document.getElementById('bcat-'+gi);
  const cat = sel ? sel.value : '';
  if (!cat) { showToast('Select a category first','error'); return; }
  const items = (g.items||[]).map(it => ({job_id:it.job_id, field_key:it.field_key, desc:it.desc}));
  fetch('/api/batch-categories/apply', {
    method:'POST', headers:{'Content-Type':'application/json'},
    body: JSON.stringify({vendor:g.vendor, category:cat, items:items, learn:true})
  }).then(r=>r.json()).then(d => {
    if (d.error) { showToast(d.error,'error'); return; }
    showToast(d.applied + ' transactions \u2192 ' + cat, 'success');
    loadBatchData();
  }).catch(e => showToast('Failed: '+e,'error'));
}

// ─── Keyboard Shortcuts ───
function toggleKbdHelp() { document.getElementById('kbdOverlay').classList.toggle('visible'); }
document.addEventListener('keydown', function(e) {
  if (e.target.tagName === 'INPUT' || e.target.tagName === 'TEXTAREA' || e.target.tagName === 'SELECT') return;
  const sec = document.querySelector('.section.active');
  if (!sec || sec.id !== 'sec-review') {
    if (e.key === '?') toggleKbdHelp();
    return;
  }
  if (e.key === '?') { toggleKbdHelp(); return; }
  if (e.key === 'ArrowRight') { nextPage(); e.preventDefault(); }
  else if (e.key === 'ArrowLeft') { prevPage(); e.preventDefault(); }
  else if (e.key === 'ArrowDown' || e.key === 'Tab' && !e.shiftKey) {
    e.preventDefault();
    if (focusedFieldIdx < pageFieldKeys.length - 1) loadPage(currentPage, focusedFieldIdx + 1);
  }
  else if (e.key === 'ArrowUp' || (e.key === 'Tab' && e.shiftKey)) {
    e.preventDefault();
    if (focusedFieldIdx > 0) loadPage(currentPage, focusedFieldIdx - 1);
  }
  else if (e.key === 'Enter') { if (pageFieldKeys[focusedFieldIdx]) confirmField(pageFieldKeys[focusedFieldIdx]); }
  else if (e.key === 'f' || e.key === 'F') { if (pageFieldKeys[focusedFieldIdx]) flagField(pageFieldKeys[focusedFieldIdx]); }
  else if (e.key === 'n' || e.key === 'N') { if (pageFieldKeys[focusedFieldIdx]) toggleNoteInput(pageFieldKeys[focusedFieldIdx]); }
  else if (e.key === 'e' || e.key === 'E') {
    const vk = pageFieldKeys[focusedFieldIdx];
    if (vk) {
      const row = document.querySelector('[data-key="'+vk.replace(/"/g, '\\\\"')+'"]');
      const valEl = row ? row.querySelector('.field-val') : null;
      if (valEl) startEdit(vk, valEl.textContent);
    }
  }
});

// ─── New Client Modal ───
function openNewClientModal() {
  document.getElementById('newClientOverlay').classList.add('visible');
  document.getElementById('newClientName').value = '';
  document.getElementById('newClientEin').value = '';
  document.getElementById('newClientContact').value = '';
  document.getElementById('newClientNotes').value = '';
  setTimeout(() => document.getElementById('newClientName').focus(), 100);
}
function closeNewClientModal() {
  document.getElementById('newClientOverlay').classList.remove('visible');
}
function createNewClient() {
  const name = document.getElementById('newClientName').value.trim();
  if (!name) { showToast('Client name is required', 'error'); return; }
  const payload = {
    name: name,
    ein_last4: document.getElementById('newClientEin').value.trim(),
    contact: document.getElementById('newClientContact').value.trim(),
    notes: document.getElementById('newClientNotes').value.trim()
  };
  fetch('/api/clients/create', {
    method: 'POST', headers: {'Content-Type':'application/json'},
    body: JSON.stringify(payload)
  }).then(r => r.json()).then(d => {
    if (d.error) { showToast(d.error, 'error'); return; }
    showToast('Client "' + d.name + '" created', 'success');
    closeNewClientModal();
    loadClientSuggestions();
    // Auto-select the new client after dropdown refreshes
    setTimeout(() => { document.getElementById('clientName').value = d.name; }, 300);
    // Refresh clients list if on that section
    if (document.getElementById('sec-clients').classList.contains('active')) loadClients();
  }).catch(e => showToast('Failed: ' + e, 'error'));
}

// ─── Generate Report Modal ───
function openReportModal() {
  const el = document.getElementById('reportJobList');
  if (!clientCompletedJobs.length) { showToast('No completed jobs to report on', 'error'); return; }
  el.innerHTML = clientCompletedJobs.map(d =>
    '<label style="display:flex;align-items:center;gap:8px;padding:6px 0;font-size:13px;cursor:pointer">' +
    '<input type="checkbox" class="report-job-cb" value="'+esc(d.job_id)+'" checked> ' +
    esc(d.filename) + ' <span style="color:var(--text-light)">('+esc(d.year)+')</span></label>'
  ).join('');
  document.getElementById('reportOverlay').classList.add('visible');
}
function closeReportModal() {
  document.getElementById('reportOverlay').classList.remove('visible');
}
function generateReport() {
  const cbs = document.querySelectorAll('.report-job-cb:checked');
  const jobIds = Array.from(cbs).map(cb => cb.value);
  if (!jobIds.length) { showToast('Select at least one job', 'error'); return; }
  const fmt = document.getElementById('reportFormat').value;
  const year = document.getElementById('reportYear').value;
  const btn = document.querySelector('#reportOverlay .btn-primary');
  btn.disabled = true; btn.textContent = 'Generating...';
  fetch('/api/clients/'+encodeURIComponent(currentClientName)+'/generate-report', {
    method:'POST', headers:{'Content-Type':'application/json'},
    body: JSON.stringify({job_ids:jobIds, output_format:fmt, year:year})
  }).then(r=>r.json()).then(d => {
    btn.disabled = false; btn.textContent = 'Generate';
    if (d.error) { showToast(d.error, 'error'); return; }
    showToast('Report generated!', 'success');
    closeReportModal();
    window.open(d.download_url, '_blank');
  }).catch(e => { btn.disabled = false; btn.textContent = 'Generate'; showToast('Failed: '+e, 'error'); });
}
</script>

<!-- New Client Modal -->
<div class="modal-overlay" id="newClientOverlay">
  <div class="modal-content">
    <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:16px">
      <h3 style="margin:0">New Client</h3>
      <button class="btn btn-ghost btn-sm" onclick="closeNewClientModal()">&times;</button>
    </div>
    <div class="form-group" style="margin-bottom:12px">
      <label class="form-label">Client Name <span style="color:var(--danger)">*</span></label>
      <input type="text" id="newClientName" class="form-input" placeholder="e.g. Watts, Stacy">
    </div>
    <div class="form-group" style="margin-bottom:12px">
      <label class="form-label">EIN / SSN (last 4)</label>
      <input type="text" id="newClientEin" class="form-input" placeholder="e.g. 1234" maxlength="4">
    </div>
    <div class="form-group" style="margin-bottom:12px">
      <label class="form-label">Contact</label>
      <input type="text" id="newClientContact" class="form-input" placeholder="e.g. email or phone">
    </div>
    <div class="form-group" style="margin-bottom:16px">
      <label class="form-label">Notes</label>
      <textarea id="newClientNotes" class="form-input" rows="2" placeholder="Optional notes about this client"></textarea>
    </div>
    <div style="display:flex;justify-content:flex-end;gap:8px">
      <button class="btn btn-ghost" onclick="closeNewClientModal()">Cancel</button>
      <button class="btn btn-primary" onclick="createNewClient()">Create Client</button>
    </div>
  </div>
</div>

<!-- Generate Report Modal -->
<div class="modal-overlay" id="reportOverlay">
  <div class="modal-content">
    <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:16px">
      <h3 style="margin:0">Generate Report</h3>
      <button class="btn btn-ghost btn-sm" onclick="closeReportModal()">&times;</button>
    </div>
    <div class="form-group" style="margin-bottom:12px">
      <label class="form-label">Select Jobs</label>
      <div id="reportJobList" style="max-height:200px;overflow-y:auto;border:1px solid var(--border);border-radius:8px;padding:8px 12px"></div>
    </div>
    <div class="form-row" style="margin-bottom:16px">
      <div class="form-group">
        <label class="form-label">Output Format</label>
        <select id="reportFormat" class="form-input">
          <option value="tax_review">Tax Review</option>
          <option value="journal_entries">Journal Entries</option>
          <option value="account_balances">Account Balances</option>
          <option value="trial_balance">Trial Balance</option>
          <option value="transaction_register">Transaction Register</option>
        </select>
      </div>
      <div class="form-group">
        <label class="form-label">Year</label>
        <input type="number" id="reportYear" class="form-input" value="2025" min="2000" max="2030">
      </div>
    </div>
    <div style="display:flex;justify-content:flex-end;gap:8px">
      <button class="btn btn-ghost" onclick="closeReportModal()">Cancel</button>
      <button class="btn btn-primary" onclick="generateReport()">Generate</button>
    </div>
  </div>
</div>

</body>
</html>"""


# ─── Main ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))

    if not os.environ.get("ANTHROPIC_API_KEY"):
        print("\n  ⚠  ANTHROPIC_API_KEY not set!")
        print("  Run: export ANTHROPIC_API_KEY=sk-ant-...")
        print()

    if not (BASE_DIR / "extract.py").exists():
        print("\n  ⚠  extract.py not found in", BASE_DIR)
        print("  Place extract.py in the same folder as app.py\n")

    print("=" * 52)
    print(f"  Bearden Document Intake Platform v{_app_version}")
    print("  ─────────────────────────────────────")
    print(f"  Open in browser:  http://localhost:{port}")
    print(f"  Database:         {DB_PATH}")
    print(f"  Uploads:          {UPLOAD_DIR}")
    print(f"  Outputs:          {OUTPUT_DIR}")
    print(f"  Client folders:   {CLIENTS_DIR}")
    print("=" * 52)
    print()

    app.run(host="127.0.0.1", port=port, debug=False)
