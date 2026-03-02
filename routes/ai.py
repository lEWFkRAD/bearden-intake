"""
AI chat blueprint — extracted from app.py during RALPH-REFACTOR-001 Phase 2.

1 route:
  POST /api/ai-chat/<job_id>
"""

import os
import json
import base64

from flask import Blueprint, request, jsonify

from pii_guard import guard_messages
from helpers import jobs, PAGES_DIR

ai_bp = Blueprint("ai", __name__)


@ai_bp.route("/api/ai-chat/<job_id>", methods=["POST"])
def ai_chat(job_id):
    """Chat with AI about the current extraction / page."""
    job = jobs.get(job_id)
    if not job:
        return jsonify({"error": "Job not found"}), 404

    message = request.json.get("message", "").strip()
    page_num = request.json.get("page")
    if not message:
        return jsonify({"error": "No message provided"}), 400

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
        raw_messages = [{"role": "user", "content": content}]
        safe_messages, pii_tok = guard_messages(raw_messages, job_id=job_id)
        msg = client.messages.create(
            model="claude-sonnet-4-20250514", max_tokens=1500,
            messages=safe_messages
        )
        reply = msg.content[0].text
        if pii_tok:
            reply = pii_tok.detokenize_text(reply)
        return jsonify({"reply": reply})
    except Exception as e:
        return jsonify({"error": f"AI call failed: {str(e)}"}), 500
