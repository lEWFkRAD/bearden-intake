"""Verification and review chain blueprint. Extracted from app.py during RALPH-REFACTOR-001 Phase 2. 13 routes."""

import os
import re
import json
from datetime import datetime
from io import BytesIO

from flask import Blueprint, request, jsonify, send_file, abort
from PIL import Image, ImageDraw

import db as appdb
from helpers import (
    jobs, _load_verifications, _save_verifications, _regen_excel,
    _learn_vendor_category, PAGES_DIR, VERIFY_DIR, OUTPUT_DIR,
)

verification_bp = Blueprint("verification", __name__)


# ─── Helper functions ─────────────────────────────────────────────────────────

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


# ─── Verification Routes ──────────────────────────────────────────────────────

@verification_bp.route("/api/verify/<job_id>", methods=["GET"])
def get_verifications(job_id):
    return jsonify(_load_verifications(job_id))

@verification_bp.route("/api/verify/<job_id>", methods=["POST"])
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


# ─── Review Chain Routes ──────────────────────────────────────────────────────

@verification_bp.route("/api/review/<job_id>/approve", methods=["POST"])
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

@verification_bp.route("/api/review/<job_id>/send-back", methods=["POST"])
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

@verification_bp.route("/api/review/<job_id>/override", methods=["POST"])
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


# ─── Lock Routes ──────────────────────────────────────────────────────────────

@verification_bp.route("/api/lock/<job_id>", methods=["POST"])
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

@verification_bp.route("/api/unlock/<job_id>", methods=["POST"])
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


# ─── Export / Audit Routes ────────────────────────────────────────────────────

@verification_bp.route("/api/export-status/<job_id>")
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

@verification_bp.route("/api/audit/<job_id>")
def api_audit_trail(job_id):
    """Get audit trail for a job."""
    events = appdb.get_audit_trail(job_id=job_id)
    return jsonify(events)


# ─── Guided Review Routes ─────────────────────────────────────────────────────

@verification_bp.route("/api/review-queue/<job_id>")
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


@verification_bp.route("/api/review-action/<job_id>/<path:field_key>", methods=["POST"])
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


@verification_bp.route("/api/review-undo/<job_id>/<path:field_key>", methods=["POST"])
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


# ─── Evidence Route ───────────────────────────────────────────────────────────

@verification_bp.route("/api/evidence/<job_id>/<path:field_key>")
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
