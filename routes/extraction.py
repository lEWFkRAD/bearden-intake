"""
Bearden Document Intake — Extraction Blueprint
================================================
Routes for document upload, extraction, review, and download.
Extracted from app.py during refactoring.
"""

import os
import sys
import json
import threading
import uuid
import re
import subprocess
from pathlib import Path
from datetime import datetime
from io import BytesIO

from flask import Blueprint, request, jsonify, send_file, render_template_string, abort

import db as appdb
from pii_guard import guard_messages
from helpers import (
    jobs, _jobs_lock, _active_procs, save_jobs, _sync_job_to_db, load_jobs,
    _sanitize_job, _secure_file, _client_dir, _regen_excel,
    BASE_DIR, DATA_DIR, UPLOAD_DIR, OUTPUT_DIR, CLIENTS_DIR, PAGES_DIR, VERIFY_DIR,
    VALID_DOC_TYPES, _start_time, _app_version,
    _load_verifications,
)

try:
    from pdf2image import convert_from_path
except ImportError:
    sys.exit("Install pdf2image: pip3 install pdf2image")

try:
    from PIL import Image, ImageDraw
except ImportError:
    sys.exit("Install Pillow: pip3 install Pillow")

# Import client helper functions needed by upload and run_extraction
from routes.clients import _safe_client_name, _instructions_text, _context_index_path

extraction_bp = Blueprint("extraction", __name__)

# ─── Constants ────────────────────────────────────────────────────────────────

VALID_OUTPUT_FORMATS = {"tax_review", "journal_entries", "account_balances", "trial_balance", "transaction_register"}


# ─── Helper Functions ─────────────────────────────────────────────────────────

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

@extraction_bp.route("/")
def index():
    return render_template_string(MAIN_HTML)

@extraction_bp.route("/api/upload", methods=["POST"])
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

@extraction_bp.route("/api/status/<job_id>")
def status(job_id):
    job = jobs.get(job_id)
    if not job:
        return jsonify({"error": "Job not found"}), 404
    out = _sanitize_job(job)
    out["recent_log"] = job.get("log", [])[-40:]
    out["log_length"] = len(job.get("log", []))
    return jsonify(out)

@extraction_bp.route("/api/results/<job_id>")
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

@extraction_bp.route("/api/reextract-page/<job_id>/<int:page_num>", methods=["POST"])
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

    # Call Claude (with PII guard)
    try:
        client = _anthropic.Anthropic()
        raw_messages = [{"role": "user", "content": [
            {"type": "image", "source": {"type": "base64", "media_type": "image/jpeg", "data": img_b64}},
            {"type": "text", "text": prompt}
        ]}]
        safe_messages, pii_tok = guard_messages(raw_messages, job_id=job_id)
        msg = client.messages.create(
            model=model, max_tokens=8000,
            messages=safe_messages
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

        # Detokenize PII back into result
        if pii_tok and result:
            result = pii_tok.detokenize_json(result)

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

@extraction_bp.route("/api/page-image/<job_id>/<int:page_num>")
def page_image(job_id, page_num):
    img_path = PAGES_DIR / job_id / f"page_{page_num}.jpg"
    if img_path.exists():
        return send_file(str(img_path), mimetype="image/jpeg")
    abort(404)

@extraction_bp.route("/api/download/<job_id>")
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

@extraction_bp.route("/api/download-log/<job_id>")
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

@extraction_bp.route("/api/jobs")
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

@extraction_bp.route("/api/delete/<job_id>", methods=["POST"])
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

@extraction_bp.route("/api/regen-excel/<job_id>", methods=["POST"])
def regen_excel(job_id):
    """Manually trigger Excel regeneration with verification corrections."""
    job = jobs.get(job_id)
    if not job or job.get("status") != "complete":
        return jsonify({"error": "Job not found or not complete"}), 404
    ok = _regen_excel(job_id)
    return jsonify({"ok": ok})


@extraction_bp.route("/api/clients/<path:client_name>/generate-report", methods=["POST"])
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


@extraction_bp.route("/api/download-report/<report_id>")
def download_report(report_id):
    """Download a generated report."""
    safe_id = re.sub(r'[^\w\-]', '', report_id)
    path = OUTPUT_DIR / f"{safe_id}.xlsx"
    if not path.exists():
        return jsonify({"error": "Report not found"}), 404
    return send_file(str(path), as_attachment=True, download_name=f"{safe_id}.xlsx")


# ─── Retry Failed/Interrupted Jobs ──────────────────────────────────────────

@extraction_bp.route("/api/retry/<job_id>", methods=["POST"])
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


@extraction_bp.route("/api/cancel/<job_id>", methods=["POST"])
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


# ─── HTML Template ─────────────────────────────────────────────────────────────
# MAIN_HTML is the large template for the index page.
# Extracted verbatim from app.py.

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
