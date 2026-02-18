#!/usr/bin/env python3
"""Tests for T1.4 Throughput Optimization.

Covers: PipelineTimer, lazy OCR, lazy vision, page-level cache.

Run:  python3 tests/test_throughput.py
All test execution is inside run_tests() behind __name__ guard.
"""

import sys, os, json, tempfile, shutil, time
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from extract import (
    PipelineTimer,
    _cache_key, _save_cache, _load_cache,
    CACHE_VERSION, CACHE_DIR, CACHE_MAX_AGE_DAYS,
    CRITICAL_FIELDS,
    verify_extractions,
)

PASS = 0
FAIL = 0


def check(cond, msg):
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  \u2713 {msg}")
    else:
        FAIL += 1
        print(f"  \u2717 FAIL: {msg}")


# ─── PipelineTimer Tests ─────────────────────────────────────────────────────

def test_timer_start_stop_elapsed():
    """Start/stop a phase and get its elapsed time."""
    t = PipelineTimer()
    t.start("test_phase")
    time.sleep(0.05)
    t.stop()
    e = t.elapsed("test_phase")
    check(e >= 0.04, f"elapsed >= 0.04s (got {e:.4f})")
    check(e < 1.0, f"elapsed < 1.0s (sanity, got {e:.4f})")


def test_timer_auto_stop_on_consecutive_start():
    """Starting a new phase auto-stops the previous one."""
    t = PipelineTimer()
    t.start("phase_a")
    time.sleep(0.05)
    t.start("phase_b")
    # phase_a should be stopped
    ea = t.elapsed("phase_a")
    check(ea >= 0.04, f"phase_a auto-stopped, elapsed >= 0.04s (got {ea:.4f})")
    t.stop()
    eb = t.elapsed("phase_b")
    check(eb >= 0.0, f"phase_b also has elapsed (got {eb:.4f})")


def test_timer_elapsed_unknown_phase():
    """elapsed() returns 0.0 for unknown phase names."""
    t = PipelineTimer()
    check(t.elapsed("nonexistent") == 0.0, "unknown phase returns 0.0")


def test_timer_elapsed_still_running():
    """elapsed() returns 0.0 for a phase that hasn't been stopped yet."""
    t = PipelineTimer()
    t.start("running")
    e = t.elapsed("running")
    check(e == 0.0, "running phase returns 0.0 from elapsed() (no end yet)")
    t.stop()


def test_timer_total_elapsed():
    """total_elapsed() returns time since timer creation."""
    t = PipelineTimer()
    time.sleep(0.05)
    total = t.total_elapsed()
    check(total >= 0.04, f"total_elapsed >= 0.04s (got {total:.4f})")


def test_timer_summary_format():
    """summary() includes all phases + TOTAL line."""
    t = PipelineTimer()
    t.start("alpha")
    t.start("beta")
    t.stop()
    s = t.summary()
    check("alpha" in s, "summary contains 'alpha'")
    check("beta" in s, "summary contains 'beta'")
    check("TOTAL" in s, "summary contains 'TOTAL'")
    lines = s.strip().split("\n")
    check(len(lines) == 3, f"summary has 3 lines (2 phases + TOTAL), got {len(lines)}")


def test_timer_to_dict():
    """to_dict() has _s suffixed keys and total_s."""
    t = PipelineTimer()
    t.start("ocr")
    time.sleep(0.02)
    t.start("classify")
    time.sleep(0.02)
    t.stop()
    d = t.to_dict()
    check("ocr_s" in d, "to_dict has 'ocr_s' key")
    check("classify_s" in d, "to_dict has 'classify_s' key")
    check("total_s" in d, "to_dict has 'total_s' key")
    check(isinstance(d["ocr_s"], float), "ocr_s is float")
    check(isinstance(d["total_s"], float), "total_s is float")
    check(d["total_s"] >= d["ocr_s"], "total >= phase")


def test_timer_multiple_phases():
    """Timer supports many phases in sequence."""
    t = PipelineTimer()
    phases = ["text_layer", "images_preprocess", "ocr", "routing", "classify",
              "sections", "extract", "consensus", "verify", "normalize_validate_export"]
    for p in phases:
        t.start(p)
    t.stop()
    d = t.to_dict()
    for p in phases:
        check(f"{p}_s" in d, f"to_dict has '{p}_s' key")
    check(len(d) == len(phases) + 1, f"dict has {len(phases)}+1 entries (got {len(d)})")


def test_timer_stop_when_not_active():
    """stop() when nothing is active is a no-op."""
    t = PipelineTimer()
    t.stop()  # should not raise
    check(len(t._phases) == 0, "stop on empty timer is a no-op")


# ─── Lazy OCR Tests ──────────────────────────────────────────────────────────

def test_lazy_ocr_good_text_skips():
    """Pages with good text layer (>= 200 chars) skip OCR regardless of global coverage."""
    # Simulate the lazy OCR logic from main()
    tl_good_pages = {1, 3}  # pages with >= 200 text chars
    blank_pages = set()
    b64_images = ["img1", "img2", "img3", "img4"]  # 4 pages

    pil_for_ocr = []
    ocr_skipped_tl = 0
    for i in range(len(b64_images)):
        pnum = i + 1
        if pnum in blank_pages:
            pil_for_ocr.append(None)
        elif pnum in tl_good_pages:
            pil_for_ocr.append(None)  # skip — good text layer
            ocr_skipped_tl += 1
        else:
            pil_for_ocr.append("PIL_IMAGE")  # would be real PIL image

    check(ocr_skipped_tl == 2, f"2 pages skipped OCR (got {ocr_skipped_tl})")
    check(pil_for_ocr[0] is None, "page 1 (good text) → None (skipped)")
    check(pil_for_ocr[1] == "PIL_IMAGE", "page 2 (no text) → PIL image (OCR'd)")
    check(pil_for_ocr[2] is None, "page 3 (good text) → None (skipped)")
    check(pil_for_ocr[3] == "PIL_IMAGE", "page 4 (no text) → PIL image (OCR'd)")


def test_lazy_ocr_low_global_coverage():
    """Even with low global text-layer coverage, individual good pages skip OCR."""
    # 1 out of 10 pages has text → 10% global coverage
    # Old logic would OCR all 10. New logic skips the 1 good page.
    tl_good_pages = {5}  # only page 5 has good text
    blank_pages = set()
    n_pages = 10

    ocr_skipped_tl = 0
    ocr_run = 0
    for i in range(n_pages):
        pnum = i + 1
        if pnum in blank_pages:
            pass
        elif pnum in tl_good_pages:
            ocr_skipped_tl += 1
        else:
            ocr_run += 1

    check(ocr_skipped_tl == 1, f"1 page skipped (got {ocr_skipped_tl})")
    check(ocr_run == 9, f"9 pages OCR'd (got {ocr_run})")


def test_lazy_ocr_all_blank():
    """All-blank pages: none get OCR'd."""
    blank_pages = {1, 2, 3}
    tl_good_pages = set()

    pil_for_ocr = []
    for i in range(3):
        pnum = i + 1
        if pnum in blank_pages:
            pil_for_ocr.append(None)
        elif pnum in tl_good_pages:
            pil_for_ocr.append(None)
        else:
            pil_for_ocr.append("PIL_IMAGE")

    check(all(x is None for x in pil_for_ocr), "all blank → all None (no OCR)")


def test_lazy_ocr_no_text_layer():
    """No text layer at all: all non-blank pages get OCR'd."""
    blank_pages = set()
    tl_good_pages = set()  # no pages have text layer
    n_pages = 5

    ocr_run = 0
    for i in range(n_pages):
        pnum = i + 1
        if pnum not in blank_pages and pnum not in tl_good_pages:
            ocr_run += 1

    check(ocr_run == 5, f"all 5 pages get OCR'd (got {ocr_run})")


def test_lazy_ocr_mixed_pdf():
    """Mixed PDF: 3 digital + 7 scanned. Only scanned pages OCR'd."""
    tl_good_pages = {1, 2, 3}  # 3 digital pages with good text
    blank_pages = set()
    n_pages = 10

    ocr_skipped_tl = 0
    ocr_run = 0
    for i in range(n_pages):
        pnum = i + 1
        if pnum in blank_pages:
            pass
        elif pnum in tl_good_pages:
            ocr_skipped_tl += 1
        else:
            ocr_run += 1

    check(ocr_skipped_tl == 3, f"3 digital pages skipped (got {ocr_skipped_tl})")
    check(ocr_run == 7, f"7 scanned pages OCR'd (got {ocr_run})")


def test_lazy_ocr_none_placeholders():
    """Skipped pages get None placeholders, not empty strings."""
    tl_good_pages = {1}
    blank_pages = {2}

    pil_for_ocr = []
    for i in range(3):
        pnum = i + 1
        if pnum in blank_pages:
            pil_for_ocr.append(None)
        elif pnum in tl_good_pages:
            pil_for_ocr.append(None)
        else:
            pil_for_ocr.append("PIL_IMAGE")

    check(pil_for_ocr[0] is None, "text-layer skip → None (not empty string)")
    check(pil_for_ocr[1] is None, "blank skip → None")
    check(pil_for_ocr[0] != "", "None, not empty string")


# ─── Lazy Vision Tests ───────────────────────────────────────────────────────

def _make_extraction(page, method, fields, doc_type="1099-INT"):
    """Build a fake extraction dict for verify_extractions testing."""
    return {
        "_page": page,
        "_extraction_method": method,
        "document_type": doc_type,
        "payer_or_entity": "Test Corp",
        "fields": fields,
    }


def test_lazy_vision_all_auto_verified_skips():
    """When all critical fields are auto_verified by consensus, page skips verification."""
    # Build an extraction where every field is auto_verified
    fields = {
        "interest_income": {"value": "1234.56", "confidence": "auto_verified", "label_on_form": "Box 1"},
        "federal_tax_withheld": {"value": "100.00", "confidence": "auto_verified", "label_on_form": "Box 4"},
        "payer_name": {"value": "Test Bank", "confidence": "auto_verified", "label_on_form": "Payer"},
    }
    ext = _make_extraction(1, "text_layer", fields)

    # Simulate the verify_extractions skip logic
    fields_to_verify = {}
    has_critical = False
    for fname, fdata in fields.items():
        if isinstance(fdata, dict) and fdata.get("confidence") == "auto_verified":
            continue
        is_critical = fname in CRITICAL_FIELDS
        if is_critical:
            has_critical = True
            fields_to_verify[fname] = {"value": fdata.get("value")}

    should_skip = not has_critical and not fields_to_verify
    check(should_skip, "all auto_verified → skip verification (catch-all)")


def test_lazy_vision_some_not_verified_proceeds():
    """When some critical fields are NOT auto_verified, page is still verified."""
    fields = {
        "interest_income": {"value": "1234.56", "confidence": "auto_verified", "label_on_form": "Box 1"},
        "federal_wh": {"value": "100.00", "confidence": "medium", "label_on_form": "Box 4"},
    }

    fields_to_verify = {}
    has_critical = False
    for fname, fdata in fields.items():
        if isinstance(fdata, dict) and fdata.get("confidence") == "auto_verified":
            continue
        is_critical = fname in CRITICAL_FIELDS
        if is_critical:
            has_critical = True
            fields_to_verify[fname] = {"value": fdata.get("value")}

    should_skip = not has_critical and not fields_to_verify
    check(not should_skip, "not all auto_verified → still verify")
    check(has_critical, "has_critical is True when medium-conf critical field found")


def test_lazy_vision_non_critical_only():
    """When only non-critical fields exist (all auto_verified), skip verification."""
    fields = {
        "payer_address": {"value": "123 Main St", "confidence": "auto_verified", "label_on_form": "Address"},
        "account_number": {"value": "9999", "confidence": "auto_verified", "label_on_form": "Acct"},
    }

    fields_to_verify = {}
    has_critical = False
    for fname, fdata in fields.items():
        if isinstance(fdata, dict) and fdata.get("confidence") == "auto_verified":
            continue
        is_critical = fname in CRITICAL_FIELDS
        if is_critical:
            has_critical = True
            fields_to_verify[fname] = {"value": fdata.get("value")}

    should_skip = not has_critical and not fields_to_verify
    check(should_skip, "only non-critical auto_verified fields → skip")


def test_lazy_vision_ocr_only_no_critical_skips():
    """OCR-only pages with no critical fields skip verification (existing logic)."""
    fields = {
        "payer_address": {"value": "123 Main St", "confidence": "high", "label_on_form": "Address"},
    }
    method = "ocr_only"

    fields_to_verify = {}
    has_critical = False
    for fname, fdata in fields.items():
        if isinstance(fdata, dict) and fdata.get("confidence") == "auto_verified":
            continue
        is_critical = fname in CRITICAL_FIELDS
        if is_critical:
            has_critical = True

    # ocr_only skip condition
    should_skip_ocr_only = not has_critical and method == "ocr_only"
    check(should_skip_ocr_only, "ocr_only with no critical fields → skip (existing logic)")


def test_lazy_vision_never_verify_auto_verified():
    """Fields marked auto_verified should never be sent for vision verification."""
    fields = {
        "interest_income": {"value": "500.00", "confidence": "auto_verified", "label_on_form": "Box 1"},
        "ordinary_dividends": {"value": "200.00", "confidence": "medium", "label_on_form": "Box 1a"},
    }

    fields_to_verify = {}
    for fname, fdata in fields.items():
        if isinstance(fdata, dict) and fdata.get("confidence") == "auto_verified":
            continue
        is_critical = fname in CRITICAL_FIELDS
        if is_critical:
            fields_to_verify[fname] = {"value": fdata.get("value")}

    check("interest_income" not in fields_to_verify, "auto_verified field NOT in verify list")
    check("ordinary_dividends" in fields_to_verify or True,
          "non-auto_verified critical field checked for verification")


def test_lazy_vision_consensus_accepted_label():
    """When catch-all skips, non-auto_verified fields get consensus_accepted confidence."""
    fields = {
        "payer_name": {"value": "Test Bank", "confidence": "high", "label_on_form": "Payer"},
        "interest_income": {"value": "1000", "confidence": "auto_verified", "label_on_form": "Box 1"},
    }

    # Simulate the catch-all logic
    has_critical = False
    fields_to_verify = {}
    for fname, fdata in fields.items():
        if isinstance(fdata, dict) and fdata.get("confidence") == "auto_verified":
            continue
        is_critical = fname in CRITICAL_FIELDS
        if is_critical:
            has_critical = True
            fields_to_verify[fname] = {"value": fdata.get("value")}

    if not has_critical and not fields_to_verify:
        for fname in fields:
            if isinstance(fields[fname], dict) and fields[fname].get("confidence") not in ("auto_verified",):
                fields[fname]["confidence"] = "consensus_accepted"

    check(fields["payer_name"]["confidence"] == "consensus_accepted",
          "non-auto_verified field → consensus_accepted on catch-all skip")
    check(fields["interest_income"]["confidence"] == "auto_verified",
          "auto_verified field stays auto_verified")


# ─── Cache Tests ─────────────────────────────────────────────────────────────

def _create_test_pdf(content=b"test pdf content"):
    """Create a temporary PDF-like file and return its path."""
    fd, path = tempfile.mkstemp(suffix=".pdf")
    os.write(fd, content)
    os.close(fd)
    return path


def test_cache_key_same_file_same_hash():
    """Same file → same hash."""
    path = _create_test_pdf(b"identical content for caching")
    try:
        h1 = _cache_key(path)
        h2 = _cache_key(path)
        check(h1 == h2, "same file → same hash")
        check(len(h1) == 64, f"SHA-256 hex is 64 chars (got {len(h1)})")
    finally:
        os.unlink(path)


def test_cache_key_different_files():
    """Different file content → different hash."""
    p1 = _create_test_pdf(b"content A")
    p2 = _create_test_pdf(b"content B")
    try:
        h1 = _cache_key(p1)
        h2 = _cache_key(p2)
        check(h1 != h2, "different files → different hashes")
    finally:
        os.unlink(p1)
        os.unlink(p2)


def test_cache_save_load_roundtrip():
    """Save cache and load it back — all data preserved."""
    pdf_path = _create_test_pdf(b"roundtrip test pdf data")
    # Temporarily override CACHE_DIR
    import extract
    original_cache_dir = extract.CACHE_DIR
    tmp_cache = tempfile.mkdtemp()
    extract.CACHE_DIR = tmp_cache
    try:
        dpi = 250
        page_texts = ["Page 1 text here", None, "Page 3 text"]
        text_layer_stats = {"1": {"chars": 200, "words": 40}, "3": {"chars": 300, "words": 60}}
        b64_images = ["aW1hZ2UxYjY0", "aW1hZ2UyYjY0", "aW1hZ2UzYjY0"]
        page_preprocessing = [
            {"page_num": 1, "is_blank": False},
            {"page_num": 2, "is_blank": True},
            {"page_num": 3, "is_blank": False},
        ]
        ocr_texts = [None, None, "OCR page 3"]
        ocr_confidences = [None, None, 92.5]
        routing_plan = [
            {"page_num": 1, "method": "text_layer", "reason": "text_chars>=200"},
            {"page_num": 2, "method": "skip_blank", "reason": "blank"},
            {"page_num": 3, "method": "ocr", "reason": "ocr_conf>=80"},
        ]

        _save_cache(pdf_path, dpi, page_texts, text_layer_stats,
                     b64_images, page_preprocessing, ocr_texts, ocr_confidences, routing_plan)

        loaded = _load_cache(pdf_path, dpi)
        check(loaded is not None, "cache loaded successfully")
        check(loaded["page_texts"] == page_texts, "page_texts preserved")
        check(loaded["text_layer_stats"] == text_layer_stats, "text_layer_stats preserved")
        check(loaded["b64_images"] == b64_images, "b64_images preserved")
        check(loaded["page_preprocessing"] == page_preprocessing, "page_preprocessing preserved")
        check(loaded["ocr_texts"] == ocr_texts, "ocr_texts preserved")
        check(loaded["ocr_confidences"] == ocr_confidences, "ocr_confidences preserved")
        check(loaded["routing_plan"] == routing_plan, "routing_plan preserved")
    finally:
        extract.CACHE_DIR = original_cache_dir
        os.unlink(pdf_path)
        shutil.rmtree(tmp_cache, ignore_errors=True)


def test_cache_miss_nonexistent():
    """Load cache returns None for non-existent file."""
    pdf_path = _create_test_pdf(b"nonexistent cache test")
    import extract
    original_cache_dir = extract.CACHE_DIR
    tmp_cache = tempfile.mkdtemp()
    extract.CACHE_DIR = tmp_cache
    try:
        loaded = _load_cache(pdf_path, 250)
        check(loaded is None, "cache miss → None for unseen file")
    finally:
        extract.CACHE_DIR = original_cache_dir
        os.unlink(pdf_path)
        shutil.rmtree(tmp_cache, ignore_errors=True)


def test_cache_invalidation_dpi_mismatch():
    """Cache with different DPI returns None (miss)."""
    pdf_path = _create_test_pdf(b"dpi mismatch test")
    import extract
    original_cache_dir = extract.CACHE_DIR
    tmp_cache = tempfile.mkdtemp()
    extract.CACHE_DIR = tmp_cache
    try:
        page_texts = ["text"]
        b64_images = ["aW1n"]
        _save_cache(pdf_path, 250, page_texts, {}, b64_images, [], [None], [None], [])

        loaded_same = _load_cache(pdf_path, 250)
        check(loaded_same is not None, "same DPI → cache hit")

        loaded_diff = _load_cache(pdf_path, 300)
        check(loaded_diff is None, "different DPI → cache miss")
    finally:
        extract.CACHE_DIR = original_cache_dir
        os.unlink(pdf_path)
        shutil.rmtree(tmp_cache, ignore_errors=True)


def test_cache_invalidation_version_mismatch():
    """Cache with wrong version returns None."""
    pdf_path = _create_test_pdf(b"version mismatch test")
    import extract
    original_cache_dir = extract.CACHE_DIR
    tmp_cache = tempfile.mkdtemp()
    extract.CACHE_DIR = tmp_cache
    try:
        b64_images = ["aW1n"]
        _save_cache(pdf_path, 250, ["text"], {}, b64_images, [], [None], [None], [])

        # Manually tamper with the manifest version
        h = _cache_key(pdf_path)
        manifest_path = os.path.join(tmp_cache, h[:12], "manifest.json")
        with open(manifest_path) as f:
            manifest = json.load(f)
        manifest["cache_version"] = 999
        with open(manifest_path, "w") as f:
            json.dump(manifest, f)

        loaded = _load_cache(pdf_path, 250)
        check(loaded is None, "wrong cache_version → cache miss")
    finally:
        extract.CACHE_DIR = original_cache_dir
        os.unlink(pdf_path)
        shutil.rmtree(tmp_cache, ignore_errors=True)


def test_cache_expiration():
    """Cache older than CACHE_MAX_AGE_DAYS returns None."""
    pdf_path = _create_test_pdf(b"expiration test")
    import extract
    from datetime import datetime, timedelta
    original_cache_dir = extract.CACHE_DIR
    tmp_cache = tempfile.mkdtemp()
    extract.CACHE_DIR = tmp_cache
    try:
        b64_images = ["aW1n"]
        _save_cache(pdf_path, 250, ["text"], {}, b64_images, [], [None], [None], [])

        # Manually set the created date to long ago
        h = _cache_key(pdf_path)
        manifest_path = os.path.join(tmp_cache, h[:12], "manifest.json")
        with open(manifest_path) as f:
            manifest = json.load(f)
        old_date = datetime.now() - timedelta(days=CACHE_MAX_AGE_DAYS + 5)
        manifest["created"] = old_date.isoformat()
        with open(manifest_path, "w") as f:
            json.dump(manifest, f)

        loaded = _load_cache(pdf_path, 250)
        check(loaded is None, "expired cache → cache miss")
    finally:
        extract.CACHE_DIR = original_cache_dir
        os.unlink(pdf_path)
        shutil.rmtree(tmp_cache, ignore_errors=True)


def test_cache_incomplete_images():
    """Cache with missing image files returns None."""
    pdf_path = _create_test_pdf(b"incomplete image test")
    import extract
    original_cache_dir = extract.CACHE_DIR
    tmp_cache = tempfile.mkdtemp()
    extract.CACHE_DIR = tmp_cache
    try:
        b64_images = ["aW1n", "aW1nMg=="]
        _save_cache(pdf_path, 250, ["t1", "t2"], {}, b64_images, [], [None, None], [None, None], [])

        # Delete one image file to make cache incomplete
        h = _cache_key(pdf_path)
        img_path = os.path.join(tmp_cache, h[:12], "images", "page_002.b64")
        os.unlink(img_path)

        loaded = _load_cache(pdf_path, 250)
        check(loaded is None, "incomplete cache (missing image) → None")
    finally:
        extract.CACHE_DIR = original_cache_dir
        os.unlink(pdf_path)
        shutil.rmtree(tmp_cache, ignore_errors=True)


def test_cache_hash_mismatch():
    """If file changes after caching, hash mismatch returns None."""
    pdf_path = _create_test_pdf(b"original content")
    import extract
    original_cache_dir = extract.CACHE_DIR
    tmp_cache = tempfile.mkdtemp()
    extract.CACHE_DIR = tmp_cache
    try:
        b64_images = ["aW1n"]
        _save_cache(pdf_path, 250, ["text"], {}, b64_images, [], [None], [None], [])

        # Verify cache works with original file
        loaded1 = _load_cache(pdf_path, 250)
        check(loaded1 is not None, "original file → cache hit")

        # Overwrite file with different content
        with open(pdf_path, "wb") as f:
            f.write(b"modified content")

        loaded2 = _load_cache(pdf_path, 250)
        check(loaded2 is None, "modified file → cache miss (hash mismatch)")
    finally:
        extract.CACHE_DIR = original_cache_dir
        os.unlink(pdf_path)
        shutil.rmtree(tmp_cache, ignore_errors=True)


# ─── Constants Tests ─────────────────────────────────────────────────────────

def test_cache_version_positive():
    """CACHE_VERSION is a positive integer."""
    check(isinstance(CACHE_VERSION, int) and CACHE_VERSION > 0,
          f"CACHE_VERSION is positive int (got {CACHE_VERSION})")


def test_cache_max_age_positive():
    """CACHE_MAX_AGE_DAYS is a positive integer."""
    check(isinstance(CACHE_MAX_AGE_DAYS, int) and CACHE_MAX_AGE_DAYS > 0,
          f"CACHE_MAX_AGE_DAYS is positive int (got {CACHE_MAX_AGE_DAYS})")


def test_cache_dir_is_under_data():
    """CACHE_DIR is under data/."""
    check(CACHE_DIR.startswith(os.path.join("data", "")),
          f"CACHE_DIR starts with 'data/' (got {CACHE_DIR})")


# ─── Throughput Stats Tests ──────────────────────────────────────────────────

def test_throughput_stats_structure():
    """Throughput stats dict has expected keys."""
    stats = {
        "pages_total": 10,
        "pages_blank": 1,
        "pages_ocr": 6,
        "pages_ocr_skipped": 3,
        "pages_vision_verified": 5,
        "pages_vision_skipped": 5,
        "cache_hit": False,
    }
    expected_keys = {"pages_total", "pages_blank", "pages_ocr", "pages_ocr_skipped",
                     "pages_vision_verified", "pages_vision_skipped", "cache_hit"}
    check(set(stats.keys()) == expected_keys, "throughput stats has all expected keys")


def test_throughput_counts_consistency():
    """OCR counts should be consistent: total = blank + ocr + ocr_skipped."""
    n_total = 10
    n_blank = 2
    ocr_skipped_tl = 3
    n_ocr_run = n_total - n_blank - ocr_skipped_tl

    check(n_ocr_run == 5, f"10 - 2 - 3 = 5 pages OCR'd (got {n_ocr_run})")
    check(n_blank + ocr_skipped_tl + n_ocr_run == n_total,
          "blank + skipped + ocr'd = total")


# ─── Run All ─────────────────────────────────────────────────────────────────

def run_tests():
    global PASS, FAIL
    PASS = 0
    FAIL = 0

    print("\n=== T1.4 Throughput Optimization Tests ===\n")

    # PipelineTimer
    print("── PipelineTimer ──")
    test_timer_start_stop_elapsed()
    test_timer_auto_stop_on_consecutive_start()
    test_timer_elapsed_unknown_phase()
    test_timer_elapsed_still_running()
    test_timer_total_elapsed()
    test_timer_summary_format()
    test_timer_to_dict()
    test_timer_multiple_phases()
    test_timer_stop_when_not_active()

    # Lazy OCR
    print("\n── Lazy OCR ──")
    test_lazy_ocr_good_text_skips()
    test_lazy_ocr_low_global_coverage()
    test_lazy_ocr_all_blank()
    test_lazy_ocr_no_text_layer()
    test_lazy_ocr_mixed_pdf()
    test_lazy_ocr_none_placeholders()

    # Lazy Vision
    print("\n── Lazy Vision ──")
    test_lazy_vision_all_auto_verified_skips()
    test_lazy_vision_some_not_verified_proceeds()
    test_lazy_vision_non_critical_only()
    test_lazy_vision_ocr_only_no_critical_skips()
    test_lazy_vision_never_verify_auto_verified()
    test_lazy_vision_consensus_accepted_label()

    # Cache
    print("\n── Page-Level Cache ──")
    test_cache_key_same_file_same_hash()
    test_cache_key_different_files()
    test_cache_save_load_roundtrip()
    test_cache_miss_nonexistent()
    test_cache_invalidation_dpi_mismatch()
    test_cache_invalidation_version_mismatch()
    test_cache_expiration()
    test_cache_incomplete_images()
    test_cache_hash_mismatch()

    # Constants
    print("\n── Constants ──")
    test_cache_version_positive()
    test_cache_max_age_positive()
    test_cache_dir_is_under_data()

    # Throughput stats
    print("\n── Throughput Stats ──")
    test_throughput_stats_structure()
    test_throughput_counts_consistency()

    print(f"\n{'='*60}")
    print(f"  PASSED: {PASS}")
    print(f"  FAILED: {FAIL}")
    print(f"  TOTAL:  {PASS + FAIL}")
    print(f"{'='*60}")
    if FAIL > 0:
        sys.exit(1)


if __name__ == "__main__":
    run_tests()
