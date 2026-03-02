"""
Clients blueprint — extracted from app.py during RALPH-REFACTOR-001 Phase 2.

12 routes:
  GET    /api/clients
  POST   /api/clients/create
  GET    /api/clients/<client_name>/info
  PUT    /api/clients/<client_name>/info
  GET    /api/clients/<client_name>/documents
  GET    /api/context/<client_name>
  POST   /api/context/<client_name>/upload
  DELETE /api/context/<client_name>/<doc_id>
  GET    /api/context/<client_name>/completeness
  GET    /api/instructions/<client_name>
  POST   /api/instructions/<client_name>
  DELETE /api/instructions/<client_name>/<rule_id>

Also includes client-specific helper functions used only by these routes.
"""

import os
import json
import re
import uuid
from pathlib import Path
from datetime import datetime

from flask import Blueprint, request, jsonify, send_file

from pdf2image import convert_from_path

from helpers import jobs, CLIENTS_DIR, BASE_DIR, PAGES_DIR, OUTPUT_DIR

clients_bp = Blueprint("clients", __name__)


# ─── Client Helper Functions (used only by client routes) ────────────────────

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


# ─── Client Management Routes ────────────────────────────────────────────────

@clients_bp.route("/api/clients")
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


@clients_bp.route("/api/clients/create", methods=["POST"])
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


@clients_bp.route("/api/clients/<path:client_name>/info", methods=["GET"])
def get_client_info(client_name):
    """Get client metadata."""
    info = _load_client_info(client_name)
    if not info:
        return jsonify({"name": _safe_client_name(client_name)})
    return jsonify(info)


@clients_bp.route("/api/clients/<path:client_name>/info", methods=["PUT"])
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


@clients_bp.route("/api/clients/<path:client_name>/documents", methods=["GET"])
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

@clients_bp.route("/api/context/<path:client_name>", methods=["GET"])
def get_context(client_name):
    """Get the context index for a client."""
    idx = _load_context_index(client_name)
    return jsonify(idx)


@clients_bp.route("/api/context/<path:client_name>/upload", methods=["POST"])
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


@clients_bp.route("/api/context/<path:client_name>/<doc_id>", methods=["DELETE"])
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


@clients_bp.route("/api/context/<path:client_name>/completeness", methods=["GET"])
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

@clients_bp.route("/api/instructions/<path:client_name>", methods=["GET"])
def get_instructions(client_name):
    """Get client instructions."""
    return jsonify(_load_instructions(client_name))


@clients_bp.route("/api/instructions/<path:client_name>", methods=["POST"])
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


@clients_bp.route("/api/instructions/<path:client_name>/<rule_id>", methods=["DELETE"])
def delete_instruction(client_name, rule_id):
    """Delete a client instruction."""
    data = _load_instructions(client_name)
    data["rules"] = [r for r in data["rules"] if r.get("id") != rule_id]
    _save_instructions(client_name, data)
    return jsonify({"ok": True, "total": len(data["rules"])})
