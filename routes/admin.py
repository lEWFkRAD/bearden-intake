"""
Admin blueprint — extracted from app.py during RALPH-REFACTOR-001 Phase 2.

6 routes:
  GET  /api/users
  GET  /api/inbox
  GET  /api/vendor-categories
  POST /api/suggest-categories
  GET  /api/batch-categories
  POST /api/batch-categories/apply
"""

from datetime import datetime
from flask import Blueprint, request, jsonify

import db as appdb
from helpers import (
    jobs,
    _load_vendor_categories,
    _suggest_category,
    CHART_OF_ACCOUNTS,
    _gather_uncategorized,
    _load_verifications,
    _save_verifications,
    _learn_vendor_category,
    _regen_excel,
)

admin_bp = Blueprint("admin", __name__)


@admin_bp.route("/api/users")
def api_list_users():
    """List all users with roles."""
    return jsonify(appdb.list_users())


@admin_bp.route("/api/inbox")
def api_inbox():
    """Get inbox items for a user."""
    user_id = request.args.get("user_id", "jeff")
    items = appdb.get_inbox(user_id)
    return jsonify({"user_id": user_id, "items": items, "total": sum(i["field_count"] for i in items)})


@admin_bp.route("/api/vendor-categories", methods=["GET"])
def get_vendor_categories():
    """Return the learned vendor -> category map and the chart of accounts."""
    return jsonify({
        "vendors": _load_vendor_categories(),
        "chart_of_accounts": CHART_OF_ACCOUNTS,
    })


@admin_bp.route("/api/suggest-categories", methods=["POST"])
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


@admin_bp.route("/api/batch-categories", methods=["GET"])
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


@admin_bp.route("/api/batch-categories/apply", methods=["POST"])
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
