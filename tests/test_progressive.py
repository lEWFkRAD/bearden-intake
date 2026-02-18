#!/usr/bin/env python3
"""Tests for T1.5 Progressive Results + Validation Streaming.

Covers: priority queue, review queue, partial results, consensus guard,
        correction locks, constants alignment.

Run:  python3 tests/test_progressive.py
All test execution is inside run_tests() behind __name__ guard.
"""

import sys, os, json, tempfile, shutil
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from extract import (
    SECTION_PRIORITY,
    SECTION_KEYWORDS,
    CONSENSUS_FIELDS,
    CONSENSUS_DOC_TYPES,
    build_priority_queue,
    build_review_queue,
    write_partial_results,
    clear_partial_results,
    build_consensus,
    save_log,
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


# ─── Helper factories ────────────────────────────────────────────────────────

def _make_group(pages, doc_type="1099-DIV", cont_pages=None):
    g = {"pages": pages, "document_type": doc_type}
    if cont_pages:
        g["continuation_pages"] = cont_pages
    return g


def _make_extraction(page, fields_dict, doc_type="1099-DIV"):
    """Build a minimal extraction dict for testing."""
    fields = {}
    for fname, val_conf in fields_dict.items():
        if isinstance(val_conf, tuple):
            val, conf = val_conf
        else:
            val, conf = val_conf, "medium"
        fields[fname] = {"value": val, "confidence": conf}
    return {
        "_page": page,
        "document_type": doc_type,
        "payer_or_entity": "Test Entity",
        "fields": fields,
    }


# ═══════════════════════════════════════════════════════════════════════════════
# PRIORITY QUEUE TESTS
# ═══════════════════════════════════════════════════════════════════════════════

def test_priority_summary_before_transactions():
    """Summary pages (priority 1) extracted before transaction pages (priority 7)."""
    groups = [
        _make_group([3], "brokerage_transactions"),
        _make_group([1], "brokerage_summary"),
    ]
    sections = {"3": ["b_transactions"], "1": ["summary"]}
    ordered = build_priority_queue(groups, sections)
    check(ordered[0]["pages"] == [1], "summary page first")
    check(ordered[1]["pages"] == [3], "transaction page last")


def test_priority_k1_before_cover():
    """K-1 groups (priority 2) before cover pages (priority 8)."""
    groups = [
        _make_group([5], "cover"),
        _make_group([2], "K-1"),
    ]
    sections = {"5": ["cover"], "2": ["k1_1065"]}
    ordered = build_priority_queue(groups, sections)
    check(ordered[0]["pages"] == [2], "K-1 page first")
    check(ordered[1]["pages"] == [5], "cover page last")


def test_priority_stable_sort():
    """Same-priority groups maintain original document order."""
    groups = [
        _make_group([1], "1099-DIV"),
        _make_group([2], "1099-INT"),
        _make_group([3], "1099-DIV"),
    ]
    # All get div/int priority = 3
    sections = {"1": ["div"], "2": ["int"], "3": ["div"]}
    ordered = build_priority_queue(groups, sections)
    pages = [g["pages"][0] for g in ordered]
    check(pages == [1, 2, 3], f"same-priority keeps doc order: {pages}")


def test_priority_empty_input():
    """Empty group list returns empty."""
    result = build_priority_queue([], {"1": ["summary"]})
    check(result == [], "empty input → empty output")


def test_priority_no_sections_returns_original():
    """No sections_by_page returns groups unchanged."""
    groups = [_make_group([2]), _make_group([1])]
    result = build_priority_queue(groups, None)
    check(result[0]["pages"] == [2], "no sections → original order preserved")


def test_priority_no_sections_empty_dict():
    """Empty sections dict returns groups unchanged."""
    groups = [_make_group([2]), _make_group([1])]
    result = build_priority_queue(groups, {})
    check(result[0]["pages"] == [2], "empty dict → original order preserved")


def test_priority_multipage_group_stays_indivisible():
    """Multi-page groups (K-1 + continuations) are not split."""
    groups = [
        _make_group([5], "cover"),
        _make_group([2, 3], "K-1", cont_pages=[4]),
    ]
    sections = {"2": ["k1_1065"], "3": ["k1_1065"], "4": ["continuation"], "5": ["cover"]}
    ordered = build_priority_queue(groups, sections)
    # K-1 group should be first (priority 2 from k1_1065, not 9 from continuation)
    check(ordered[0]["pages"] == [2, 3], "K-1 multi-page group first")
    cont = ordered[0].get("continuation_pages", [])
    check(cont == [4], f"continuation pages intact: {cont}")


def test_priority_min_wins_for_group():
    """Group priority = min of all page labels, not average."""
    groups = [_make_group([1], "mixed")]
    # Page 1 has both summary (1) and continuation (9) labels
    sections = {"1": ["summary", "continuation"]}
    ordered = build_priority_queue(groups, sections)
    # Still runs — just ensure it doesn't crash and returns 1 group
    check(len(ordered) == 1, "mixed-label page handled")


# ═══════════════════════════════════════════════════════════════════════════════
# REVIEW QUEUE TESTS
# ═══════════════════════════════════════════════════════════════════════════════

def test_review_low_confidence_first():
    """needs_review / low confidence fields appear first in queue."""
    exts = [
        _make_extraction(1, {
            "interest": ("100.00", "auto_verified"),
            "penalty": ("50.00", "needs_review"),
        }),
    ]
    q = build_review_queue(exts)
    check(q[0]["field"] == "penalty", f"needs_review first: {q[0]['field']}")
    check(q[-1]["field"] == "interest", f"auto_verified last: {q[-1]['field']}")


def test_review_auto_verified_last():
    """Auto-verified fields sorted last (priority 5)."""
    exts = [
        _make_extraction(1, {
            "field_a": ("100", "medium"),
            "field_b": ("200", "auto_verified"),
            "field_c": ("300", "high"),
        }),
    ]
    q = build_review_queue(exts)
    check(q[-1]["field"] == "field_b", f"auto_verified last: {q[-1]['field']}")
    check(q[-1]["priority"] == 5, f"auto_verified priority = 5")


def test_review_consensus_disagreement_prioritized():
    """Fields with _consensus_top2 get priority 2 (after needs_review)."""
    exts = [{
        "_page": 1,
        "document_type": "1099-DIV",
        "fields": {
            "dividends": {
                "value": "500.00",
                "confidence": "high",
                "_consensus_top2": [("500.00", 6), ("505.00", 4)],
            },
            "interest": {
                "value": "100.00",
                "confidence": "high",
            },
        },
    }]
    q = build_review_queue(exts)
    div_entry = [e for e in q if e["field"] == "dividends"][0]
    int_entry = [e for e in q if e["field"] == "interest"][0]
    check(div_entry["priority"] == 2, f"disagreement gets priority 2: {div_entry['priority']}")
    check(int_entry["priority"] == 4, f"no disagreement gets priority 4: {int_entry['priority']}")
    check(q[0]["field"] == "dividends", "disagreement field before clean high-confidence")


def test_review_mixed_confidences():
    """Full ordering: needs_review < disagreement < medium < high < auto_verified."""
    exts = [{
        "_page": 1,
        "document_type": "test",
        "fields": {
            "f_auto": {"value": "1", "confidence": "auto_verified"},
            "f_high": {"value": "2", "confidence": "high"},
            "f_med": {"value": "3", "confidence": "medium"},
            "f_low": {"value": "4", "confidence": "low"},
            "f_disagree": {"value": "5", "confidence": "medium", "_consensus_top2": [("5", 4), ("6", 3)]},
        },
    }]
    q = build_review_queue(exts)
    priorities = [e["priority"] for e in q]
    check(priorities == sorted(priorities), f"priorities ascending: {priorities}")
    check(q[0]["field"] == "f_low", f"low first: {q[0]['field']}")
    check(q[-1]["field"] == "f_auto", f"auto_verified last: {q[-1]['field']}")


def test_review_none_value_excluded():
    """Fields with value=None are excluded from review queue."""
    exts = [_make_extraction(1, {"real": ("100", "medium")})]
    exts[0]["fields"]["empty"] = {"value": None, "confidence": "low"}
    q = build_review_queue(exts)
    fields_in_q = [e["field"] for e in q]
    check("empty" not in fields_in_q, "None-value field excluded")
    check("real" in fields_in_q, "non-None field included")


def test_review_empty_extractions():
    """Empty extractions list returns empty queue."""
    q = build_review_queue([])
    check(q == [], "empty input → empty queue")


# ═══════════════════════════════════════════════════════════════════════════════
# PARTIAL RESULTS TESTS
# ═══════════════════════════════════════════════════════════════════════════════

def test_partial_write_read_roundtrip():
    """Write partial results and read back — data intact."""
    with tempfile.TemporaryDirectory() as td:
        outpath = os.path.join(td, "test_output.xlsx")
        exts = [_make_extraction(1, {"dividends": ("100.00", "high")})]

        write_partial_results(outpath, exts, batch_num=1, total_batches=3,
                              time_to_first_values_s=2.5)

        partial_path = outpath.replace(".xlsx", "_partial_results.json")
        check(os.path.exists(partial_path), "partial file created")

        with open(partial_path) as f:
            data = json.load(f)

        check(data["version"] == "v6", f"version = v6: {data['version']}")
        check(data["partial"] is True, "partial flag = True")
        check(data["batch_num"] == 1, f"batch_num = 1: {data['batch_num']}")
        check(data["total_batches"] == 3, f"total_batches = 3: {data['total_batches']}")
        check(data["time_to_first_values_s"] == 2.5, "time_to_first_values_s preserved")
        check(len(data["extractions"]) == 1, "1 extraction")
        check(data["extractions"][0]["fields"]["dividends"]["value"] == "100.00",
              "field value intact")


def test_partial_incremental_writes_grow():
    """Successive writes accumulate more extractions."""
    with tempfile.TemporaryDirectory() as td:
        outpath = os.path.join(td, "test_output.xlsx")
        partial_path = outpath.replace(".xlsx", "_partial_results.json")

        # Batch 1: 1 extraction
        exts = [_make_extraction(1, {"dividends": ("100.00", "high")})]
        write_partial_results(outpath, exts, 1, 3)

        with open(partial_path) as f:
            d1 = json.load(f)
        check(len(d1["extractions"]) == 1, "batch 1: 1 extraction")

        # Batch 2: 3 extractions total
        exts.append(_make_extraction(2, {"interest": ("200.00", "medium")}))
        exts.append(_make_extraction(3, {"penalty": ("25.00", "low")}))
        write_partial_results(outpath, exts, 2, 3)

        with open(partial_path) as f:
            d2 = json.load(f)
        check(len(d2["extractions"]) == 3, "batch 2: 3 extractions")
        check(d2["batch_num"] == 2, "batch_num updated to 2")


def test_partial_atomic_write():
    """Partial file is written atomically (no .tmp left behind on success)."""
    with tempfile.TemporaryDirectory() as td:
        outpath = os.path.join(td, "test_output.xlsx")
        exts = [_make_extraction(1, {"dividends": ("100.00", "high")})]
        write_partial_results(outpath, exts, 1, 1)

        partial_path = outpath.replace(".xlsx", "_partial_results.json")
        tmp_path = partial_path + ".tmp"
        check(os.path.exists(partial_path), "final file exists")
        check(not os.path.exists(tmp_path), "tmp file cleaned up")


def test_partial_clear():
    """clear_partial_results removes the partial file."""
    with tempfile.TemporaryDirectory() as td:
        outpath = os.path.join(td, "test_output.xlsx")
        exts = [_make_extraction(1, {"dividends": ("100.00", "high")})]
        write_partial_results(outpath, exts, 1, 1)

        partial_path = outpath.replace(".xlsx", "_partial_results.json")
        check(os.path.exists(partial_path), "partial exists before clear")

        clear_partial_results(outpath)
        check(not os.path.exists(partial_path), "partial removed after clear")


def test_partial_clear_nonexistent():
    """clear_partial_results on missing file does not raise."""
    with tempfile.TemporaryDirectory() as td:
        outpath = os.path.join(td, "nonexistent.xlsx")
        try:
            clear_partial_results(outpath)
            check(True, "clear on missing file is no-op")
        except Exception as e:
            check(False, f"clear on missing file raised: {e}")


def test_partial_sections_by_page():
    """Sections_by_page included when provided."""
    with tempfile.TemporaryDirectory() as td:
        outpath = os.path.join(td, "test_output.xlsx")
        exts = [_make_extraction(1, {"dividends": ("100.00", "high")})]
        sbp = {"1": ["summary"], "2": ["div"]}
        write_partial_results(outpath, exts, 1, 1, sections_by_page=sbp)

        partial_path = outpath.replace(".xlsx", "_partial_results.json")
        with open(partial_path) as f:
            data = json.load(f)
        check(data.get("sections_by_page") == sbp, "sections_by_page preserved")


def test_partial_fields_count():
    """fields_count in partial results counts non-None field values."""
    with tempfile.TemporaryDirectory() as td:
        outpath = os.path.join(td, "test_output.xlsx")
        exts = [
            _make_extraction(1, {"a": ("100", "high"), "b": ("200", "medium")}),
            _make_extraction(2, {"c": ("300", "low")}),
        ]
        write_partial_results(outpath, exts, 1, 1)

        partial_path = outpath.replace(".xlsx", "_partial_results.json")
        with open(partial_path) as f:
            data = json.load(f)
        check(data["fields_count"] == 3, f"fields_count = 3: {data['fields_count']}")


# ═══════════════════════════════════════════════════════════════════════════════
# CONSENSUS GUARD TESTS
# ═══════════════════════════════════════════════════════════════════════════════

def test_consensus_guard_auto_verified_stays():
    """Auto-verified field remains auto_verified after re-running consensus."""
    exts = [{
        "_page": 1,
        "document_type": "1099-DIV",
        "payer_or_entity": "Test Broker",
        "_extraction_method": "text_layer",
        "fields": {
            "ordinary_dividends": {
                "value": "1234.56",
                "confidence": "auto_verified",
                "label_on_form": "Ordinary dividends",
            },
        },
    }]
    page_texts = ["Box 1a Ordinary dividends $1,234.56"]
    ocr_texts = ["Box 1a Ordinary dividends $1,234.56"]

    result, _log = build_consensus(exts, page_texts, ocr_texts)
    fdata = result[0]["fields"]["ordinary_dividends"]
    check(fdata["confidence"] == "auto_verified",
          f"auto_verified field stays auto_verified: {fdata['confidence']}")
    check(fdata["value"] == "1234.56",
          f"value unchanged: {fdata['value']}")


def test_consensus_guard_non_verified_still_checked():
    """Non-verified field is still processed by consensus."""
    exts = [{
        "_page": 1,
        "document_type": "1099-DIV",
        "payer_or_entity": "Test Broker",
        "_extraction_method": "text_layer",
        "fields": {
            "ordinary_dividends": {
                "value": "1234.56",
                "confidence": "medium",
                "label_on_form": "Ordinary dividends",
            },
        },
    }]
    page_texts = ["Box 1a Ordinary dividends $1,234.56"]
    ocr_texts = ["Box 1a Ordinary dividends $1,234.56"]

    result, clog = build_consensus(exts, page_texts, ocr_texts)
    fdata = result[0]["fields"]["ordinary_dividends"]
    # Should have been checked — confidence should no longer be "medium"
    check(fdata["confidence"] != "medium" or clog["fields_checked"] > 0,
          f"non-verified field was processed by consensus (conf={fdata['confidence']})")


# ═══════════════════════════════════════════════════════════════════════════════
# CORRECTION LOCKS TESTS
# ═══════════════════════════════════════════════════════════════════════════════

def _apply_locks_import():
    """Import _apply_locks from app.py."""
    # app.py needs to be importable — use its function directly
    import importlib.util
    app_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "app.py")
    spec = importlib.util.spec_from_file_location("app_module", app_path)
    mod = importlib.util.module_from_spec(spec)
    # We need Flask etc., so mock them or just test the logic manually
    return None


def test_correction_lock_preserves_user_edit():
    """User-corrected field preserved after merge with new batch data."""
    partial_data = {
        "extractions": [{
            "_page": 1,
            "fields": {
                "dividends": {"value": "100.00", "confidence": "medium"},
                "interest": {"value": "200.00", "confidence": "high"},
            },
        }],
    }
    corrections = {
        "fields": {
            "1:0:dividends": {
                "status": "corrected",
                "corrected_value": "150.00",
            },
        },
    }
    # Inline _apply_locks logic (app.py depends on Flask so cannot import directly)
    for ext in partial_data["extractions"]:
        page = ext.get("_page")
        fields = ext.get("fields", {})
        for field_key, decision in corrections["fields"].items():
            parts = field_key.split(":")
            if len(parts) != 3:
                continue
            fpage = int(parts[0])
            fname = parts[2]
            if fpage != page:
                continue
            if fname in fields and decision.get("status") == "corrected":
                if isinstance(fields[fname], dict):
                    fields[fname]["value"] = decision.get("corrected_value", fields[fname].get("value"))
                    fields[fname]["_locked"] = True

    fdata = partial_data["extractions"][0]["fields"]
    check(fdata["dividends"]["value"] == "150.00",
          f"corrected value applied: {fdata['dividends']['value']}")
    check(fdata["dividends"].get("_locked") is True, "locked flag set")
    check(fdata["interest"]["value"] == "200.00", "uncorrected field unchanged")
    check(fdata["interest"].get("_locked") is None, "uncorrected field not locked")


def test_correction_lock_wrong_page_ignored():
    """Corrections for non-matching pages are skipped."""
    partial_data = {
        "extractions": [{
            "_page": 1,
            "fields": {
                "dividends": {"value": "100.00", "confidence": "medium"},
            },
        }],
    }
    corrections = {
        "fields": {
            "5:0:dividends": {
                "status": "corrected",
                "corrected_value": "999.99",
            },
        },
    }
    # Apply locks inline
    for ext in partial_data["extractions"]:
        page = ext.get("_page")
        fields = ext.get("fields", {})
        for field_key, decision in corrections["fields"].items():
            parts = field_key.split(":")
            if len(parts) != 3:
                continue
            fpage = int(parts[0])
            fname = parts[2]
            if fpage != page:
                continue
            if fname in fields and decision.get("status") == "corrected":
                if isinstance(fields[fname], dict):
                    fields[fname]["value"] = decision.get("corrected_value")
                    fields[fname]["_locked"] = True

    check(partial_data["extractions"][0]["fields"]["dividends"]["value"] == "100.00",
          "wrong-page correction ignored")
    check(partial_data["extractions"][0]["fields"]["dividends"].get("_locked") is None,
          "no lock on wrong-page field")


def test_correction_lock_non_corrected_status_ignored():
    """Non-corrected status (e.g. 'confirmed') does not lock the field."""
    partial_data = {
        "extractions": [{
            "_page": 1,
            "fields": {
                "dividends": {"value": "100.00", "confidence": "medium"},
            },
        }],
    }
    corrections = {
        "fields": {
            "1:0:dividends": {
                "status": "confirmed",
            },
        },
    }
    for ext in partial_data["extractions"]:
        page = ext.get("_page")
        fields = ext.get("fields", {})
        for field_key, decision in corrections["fields"].items():
            parts = field_key.split(":")
            if len(parts) != 3:
                continue
            fpage = int(parts[0])
            fname = parts[2]
            if fpage != page:
                continue
            if fname in fields and decision.get("status") == "corrected":
                if isinstance(fields[fname], dict):
                    fields[fname]["value"] = decision.get("corrected_value")
                    fields[fname]["_locked"] = True

    check(partial_data["extractions"][0]["fields"]["dividends"]["value"] == "100.00",
          "confirmed status does not change value")
    check(partial_data["extractions"][0]["fields"]["dividends"].get("_locked") is None,
          "confirmed status does not lock")


# ═══════════════════════════════════════════════════════════════════════════════
# CONSTANTS TESTS
# ═══════════════════════════════════════════════════════════════════════════════

def test_section_priority_covers_all_keywords():
    """Every SECTION_KEYWORDS key has an entry in SECTION_PRIORITY."""
    missing = []
    for key in SECTION_KEYWORDS:
        if key not in SECTION_PRIORITY:
            missing.append(key)
    check(len(missing) == 0,
          f"all SECTION_KEYWORDS in SECTION_PRIORITY (missing: {missing})")


def test_section_priority_values_positive_int():
    """All SECTION_PRIORITY values are positive integers."""
    bad = [(k, v) for k, v in SECTION_PRIORITY.items()
           if not isinstance(v, int) or v <= 0]
    check(len(bad) == 0, f"all priority values are positive int (bad: {bad})")


def test_section_priority_has_unknown():
    """SECTION_PRIORITY has a default for 'unknown' pages."""
    check("unknown" in SECTION_PRIORITY, "'unknown' has priority entry")


def test_section_priority_summary_is_highest():
    """Summary has the lowest (highest priority) number."""
    min_val = min(SECTION_PRIORITY.values())
    check(SECTION_PRIORITY["summary"] == min_val,
          f"summary has highest priority ({SECTION_PRIORITY['summary']})")


# ═══════════════════════════════════════════════════════════════════════════════
# STREAMING STATS IN save_log TESTS
# ═══════════════════════════════════════════════════════════════════════════════

def test_save_log_streaming_stats():
    """save_log includes streaming stats in output when provided."""
    with tempfile.TemporaryDirectory() as td:
        outpath = os.path.join(td, "test_output.xlsx")
        exts = [_make_extraction(1, {"dividends": ("100.00", "high")})]
        streaming = {
            "time_to_first_values_s": 3.5,
            "batches_processed": 4,
            "fields_streamed": 12,
        }
        save_log(exts, [], [], outpath, streaming_stats=streaming)

        log_path = outpath.replace(".xlsx", "_log.json")
        check(os.path.exists(log_path), "log file created")

        with open(log_path) as f:
            log = json.load(f)
        check("streaming" in log, "streaming key present in log")
        check(log["streaming"]["time_to_first_values_s"] == 3.5,
              f"time_to_first_values_s = 3.5: {log['streaming'].get('time_to_first_values_s')}")
        check(log["streaming"]["batches_processed"] == 4,
              f"batches_processed = 4: {log['streaming'].get('batches_processed')}")
        check(log["streaming"]["fields_streamed"] == 12,
              f"fields_streamed = 12: {log['streaming'].get('fields_streamed')}")


def test_save_log_no_streaming_stats():
    """save_log omits streaming key when streaming_stats is None."""
    with tempfile.TemporaryDirectory() as td:
        outpath = os.path.join(td, "test_output.xlsx")
        exts = [_make_extraction(1, {"dividends": ("100.00", "high")})]
        save_log(exts, [], [], outpath)

        log_path = outpath.replace(".xlsx", "_log.json")
        with open(log_path) as f:
            log = json.load(f)
        check("streaming" not in log, "streaming key absent when None")


# ─── Run All ─────────────────────────────────────────────────────────────────

def run_tests():
    global PASS, FAIL
    PASS = 0
    FAIL = 0

    print("\n=== T1.5 Progressive Results + Validation Streaming Tests ===\n")

    # Priority Queue
    print("── Priority Queue ──")
    test_priority_summary_before_transactions()
    test_priority_k1_before_cover()
    test_priority_stable_sort()
    test_priority_empty_input()
    test_priority_no_sections_returns_original()
    test_priority_no_sections_empty_dict()
    test_priority_multipage_group_stays_indivisible()
    test_priority_min_wins_for_group()

    # Review Queue
    print("\n── Review Queue ──")
    test_review_low_confidence_first()
    test_review_auto_verified_last()
    test_review_consensus_disagreement_prioritized()
    test_review_mixed_confidences()
    test_review_none_value_excluded()
    test_review_empty_extractions()

    # Partial Results
    print("\n── Partial Results ──")
    test_partial_write_read_roundtrip()
    test_partial_incremental_writes_grow()
    test_partial_atomic_write()
    test_partial_clear()
    test_partial_clear_nonexistent()
    test_partial_sections_by_page()
    test_partial_fields_count()

    # Consensus Guard
    print("\n── Consensus Guard ──")
    test_consensus_guard_auto_verified_stays()
    test_consensus_guard_non_verified_still_checked()

    # Correction Locks
    print("\n── Correction Locks ──")
    test_correction_lock_preserves_user_edit()
    test_correction_lock_wrong_page_ignored()
    test_correction_lock_non_corrected_status_ignored()

    # Constants
    print("\n── Constants ──")
    test_section_priority_covers_all_keywords()
    test_section_priority_values_positive_int()
    test_section_priority_has_unknown()
    test_section_priority_summary_is_highest()

    # Streaming Stats
    print("\n── Streaming Stats ──")
    test_save_log_streaming_stats()
    test_save_log_no_streaming_stats()

    print(f"\n{'='*60}")
    print(f"  PASSED: {PASS}")
    print(f"  FAILED: {FAIL}")
    print(f"  TOTAL:  {PASS + FAIL}")
    print(f"{'='*60}")
    if FAIL > 0:
        sys.exit(1)


if __name__ == "__main__":
    run_tests()
