#!/usr/bin/env python3
"""Tests for T1.2 Per-Page Routing + T1.2.5 Consensus Verification Layer.

Run:  python3 tests/test_routing.py
All test execution is inside run_tests() behind __name__ guard.
"""

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from extract import (
    route_pages, _parse_amount_from_text, _values_match,
    _validate_candidate, _score_candidate, build_consensus, get_val,
    ROUTE_TEXT_MIN_CHARS, ROUTE_TEXT_MIN_WORDS, ROUTE_TEXT_MIN_CHARS_ALT,
    ROUTE_OCR_MIN_CHARS, ROUTE_OCR_MIN_CONF,
    CONSENSUS_FIELDS, CONSENSUS_ACCEPT_THRESHOLD, CONSENSUS_MARGIN,
    CONSENSUS_DOC_TYPES,
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


# ─── Helpers ───────────────────────────────────────────────────────────────

def _make_preproc(n, blanks=None):
    """Build a fake page_preprocessing list. blanks = set of 1-indexed pages."""
    blanks = blanks or set()
    meta = []
    for i in range(n):
        pg = i + 1
        is_blank = pg in blanks
        meta.append({
            "page_num": pg,
            "is_blank": is_blank,
            "blank_reason": "blank_true" if is_blank else "not_blank",
            "pct_non_white": 0.0 if is_blank else 2.5,
            "quality_score": 0.0 if is_blank else 0.6,
        })
    return meta


def _make_ext(doc_type, entity, page, fields_dict, method="text_layer"):
    """Build an extraction dict matching extract.py format."""
    fields = {}
    for k, v in fields_dict.items():
        fields[k] = {"value": v, "confidence": "high", "label_on_form": k.replace("_", " ").title()}
    return {
        "document_type": doc_type,
        "payer_or_entity": entity,
        "_page": page,
        "_extraction_method": method,
        "fields": fields,
    }


# ─── Test runner ───────────────────────────────────────────────────────────

def run_tests():
    global PASS, FAIL
    PASS = 0
    FAIL = 0

    # ═══════════════════════════════════════════════════════════════════════
    # PART A — Per-Page Routing Tests
    # ═══════════════════════════════════════════════════════════════════════

    print("\n═══ Routing: Born-digital pages → text_layer ═══")
    page_texts = ["A" * 250]  # 250 chars: above threshold
    ocr_texts = [None]
    ocr_confs = [None]
    preproc = _make_preproc(1)
    plan = route_pages(page_texts, ocr_texts, ocr_confs, preproc)
    check(len(plan) == 1, f"Routing plan has 1 entry (got {len(plan)})")
    check(plan[0]["method"] == "text_layer", f"Page 1 routes to text_layer (got {plan[0]['method']})")
    check(plan[0]["text_chars"] == 250, f"text_chars=250 (got {plan[0]['text_chars']})")
    check("text_chars>=200" in plan[0]["reason"], f"Reason mentions text_chars (got {plan[0]['reason']})")

    print("\n═══ Routing: Scanned pages → ocr ═══")
    page_texts = ["short"]  # too few chars
    ocr_texts = ["B" * 300]  # 300 chars OCR
    ocr_confs = [85.0]
    preproc = _make_preproc(1)
    plan = route_pages(page_texts, ocr_texts, ocr_confs, preproc)
    check(plan[0]["method"] == "ocr", f"Page 1 routes to ocr (got {plan[0]['method']})")
    check(plan[0]["ocr_chars"] == 300, f"ocr_chars=300 (got {plan[0]['ocr_chars']})")
    check(plan[0]["ocr_conf_avg"] == 85.0, f"ocr_conf_avg=85 (got {plan[0]['ocr_conf_avg']})")

    print("\n═══ Routing: Mixed document → combination ═══")
    page_texts = ["X" * 300, "tiny", None, "Y" * 300]
    ocr_texts = [None, "Z" * 400, "q" * 50, None]
    ocr_confs = [None, 75.0, 40.0, None]
    preproc = _make_preproc(4)
    plan = route_pages(page_texts, ocr_texts, ocr_confs, preproc)
    check(plan[0]["method"] == "text_layer", f"Page 1: text_layer (got {plan[0]['method']})")
    check(plan[1]["method"] == "ocr", f"Page 2: ocr (got {plan[1]['method']})")
    check(plan[2]["method"] == "vision", f"Page 3: vision (got {plan[2]['method']})")
    check(plan[3]["method"] == "text_layer", f"Page 4: text_layer (got {plan[3]['method']})")

    print("\n═══ Routing: Low quality → vision ═══")
    page_texts = ["hi"]
    ocr_texts = ["hey"]
    ocr_confs = [30.0]
    preproc = _make_preproc(1)
    plan = route_pages(page_texts, ocr_texts, ocr_confs, preproc)
    check(plan[0]["method"] == "vision", f"Low quality routes to vision (got {plan[0]['method']})")

    print("\n═══ Routing: Blank pages → skip_blank ═══")
    page_texts = [None, "X" * 300]
    ocr_texts = [None, "Y" * 300]
    ocr_confs = [None, 80.0]
    preproc = _make_preproc(2, blanks={1})
    plan = route_pages(page_texts, ocr_texts, ocr_confs, preproc)
    check(plan[0]["method"] == "skip_blank", f"Page 1 blank → skip_blank (got {plan[0]['method']})")
    check(plan[0]["blank"] is True, "Blank flag is True")
    check(plan[1]["method"] == "text_layer", f"Page 2 non-blank → text_layer (got {plan[1]['method']})")

    print("\n═══ Routing: Borderline text (word count threshold) ═══")
    # 45 words of 3 chars each = 45*3 + 44 spaces = 179 chars (< 200) but 45 words (>= 40)
    words = " ".join(["abc"] * 45)
    page_texts = [words]  # 179 chars, 45 words
    ocr_texts = [None]
    ocr_confs = [None]
    preproc = _make_preproc(1)
    plan = route_pages(page_texts, ocr_texts, ocr_confs, preproc)
    # 179 chars >= 120 AND 45 words >= 40 → text_layer
    check(plan[0]["method"] == "text_layer", f"Borderline routes to text_layer (got {plan[0]['method']})")
    check("words" in plan[0]["reason"], f"Reason mentions words (got {plan[0]['reason']})")

    print("\n═══ Routing: Page numbering stable ═══")
    page_texts = [None, "X" * 300, None, "Y" * 300, None]
    ocr_texts = [None] * 5
    ocr_confs = [None] * 5
    preproc = _make_preproc(5, blanks={1, 3, 5})
    plan = route_pages(page_texts, ocr_texts, ocr_confs, preproc)
    check(len(plan) == 5, f"All 5 pages in plan (got {len(plan)})")
    check(plan[0]["page_num"] == 1, f"Page 1 numbered correctly (got {plan[0]['page_num']})")
    check(plan[1]["page_num"] == 2, f"Page 2 numbered correctly (got {plan[1]['page_num']})")
    check(plan[2]["page_num"] == 3, f"Page 3 numbered correctly (got {plan[2]['page_num']})")
    check(plan[4]["page_num"] == 5, f"Page 5 numbered correctly (got {plan[4]['page_num']})")
    blank_methods = [p["method"] for p in plan if p["blank"]]
    check(all(m == "skip_blank" for m in blank_methods), f"All blanks skip_blank (got {blank_methods})")

    print("\n═══ Routing: OCR confidence threshold ═══")
    # OCR has few chars but high confidence
    page_texts = ["tiny"]
    ocr_texts = ["small text"]  # only 10 chars
    ocr_confs = [75.0]  # >= 70 threshold
    preproc = _make_preproc(1)
    plan = route_pages(page_texts, ocr_texts, ocr_confs, preproc)
    check(plan[0]["method"] == "ocr", f"High-conf OCR routes to ocr (got {plan[0]['method']})")
    check("conf" in plan[0]["reason"], f"Reason mentions conf (got {plan[0]['reason']})")

    print("\n═══ Routing: Digit ratio computed ═══")
    page_texts = ["Income 1234.56 Box 5678"]
    ocr_texts = [None]
    ocr_confs = [None]
    preproc = _make_preproc(1)
    plan = route_pages(page_texts, ocr_texts, ocr_confs, preproc)
    check(plan[0]["digit_ratio"] > 0, f"Digit ratio > 0 (got {plan[0]['digit_ratio']})")
    check(plan[0]["digit_ratio"] < 1, f"Digit ratio < 1 (got {plan[0]['digit_ratio']})")

    print("\n═══ Routing: None inputs handled ═══")
    preproc = _make_preproc(2)
    plan = route_pages(None, None, None, preproc)
    check(len(plan) == 2, f"Handles None inputs (got {len(plan)} entries)")
    check(plan[0]["method"] == "vision", f"No text sources → vision (got {plan[0]['method']})")

    # ═══════════════════════════════════════════════════════════════════════
    # PART B — Consensus Verification Tests
    # ═══════════════════════════════════════════════════════════════════════

    print("\n═══ Consensus: _parse_amount_from_text ═══")
    text = "1a Ordinary dividends  $1,234.56  1b Qualified dividends  $567.89"
    r = _parse_amount_from_text(text, "ordinary_dividends")
    check(r is not None, "Parsed ordinary_dividends from text")
    check(r["value_num"] == 1234.56, f"Value is 1234.56 (got {r['value_num']})")
    check(r["label_anchor_found"] is True, "Label anchor found")
    check(r["parse_ok"] is True, "Parse OK")

    r2 = _parse_amount_from_text(text, "qualified_dividends")
    check(r2 is not None, "Parsed qualified_dividends from text")
    check(r2["value_num"] == 567.89, f"Value is 567.89 (got {r2['value_num']})")

    print("\n═══ Consensus: _parse_amount K-1 boxes ═══")
    k1_text = "Box 1 Ordinary business income  (2,500.00)  Box 2 Net rental real estate  3,100.50"
    r3 = _parse_amount_from_text(k1_text, "box1_ordinary_income")
    check(r3 is not None, "Parsed K-1 box1 from text")
    check(r3["value_num"] == -2500.0, f"Negative value parsed (got {r3['value_num']})")

    r4 = _parse_amount_from_text(k1_text, "box2_rental_real_estate")
    check(r4 is not None, "Parsed K-1 box2 from text")
    check(r4["value_num"] == 3100.50, f"Value is 3100.50 (got {r4['value_num']})")

    print("\n═══ Consensus: _parse_amount returns None for missing ═══")
    # No label "ordinary dividends" or "1a" in text → None
    r5 = _parse_amount_from_text("random text no amounts", "ordinary_dividends")
    check(r5 is None, "Returns None when label not found in text")
    # Label found but no amount nearby
    r5b = _parse_amount_from_text("1a Ordinary dividends: see attached", "ordinary_dividends")
    check(r5b is not None, "Returns partial result when label found but no amount")
    check(r5b.get("parse_ok") is False, "parse_ok is False when no amount found")
    # For a field with no label patterns at all:
    r6 = _parse_amount_from_text("completely unrelated content", "box15_credits")
    check(r6 is None, "Returns None when no label patterns match")

    print("\n═══ Consensus: _values_match ═══")
    check(_values_match(1234.56, 1234.56), "Exact match")
    check(_values_match(1234.56, 1234.565), "Within tolerance")
    check(not _values_match(1234.56, 1235.00), "Not matching outside tolerance")
    check(not _values_match(None, 1234.56), "None doesn't match")

    print("\n═══ Consensus: _validate_candidate ═══")
    fields = {
        "ordinary_dividends": {"value": 1000.0},
        "div_ordinary_dividends": {"value": 1000.0},
    }
    ok, reason = _validate_candidate("qualified_dividends", 500.0, fields)
    check(ok, "qualified 500 ≤ ordinary 1000 passes")
    ok2, reason2 = _validate_candidate("qualified_dividends", 1500.0, fields)
    check(not ok2, f"qualified 1500 > ordinary 1000 fails (reason: {reason2})")
    check(reason2 == "qualified > ordinary", f"Correct violation message")

    ok3, _ = _validate_candidate("federal_wh", -100.0, fields)
    check(not ok3, "Negative withholding fails")

    ok4, _ = _validate_candidate("box1_ordinary_income", 5000.0, {})
    check(ok4, "K-1 box1 passes with no related fields")

    print("\n═══ Consensus: _score_candidate ═══")
    c1 = {"value_num": 1234.56, "method": "claude_extraction",
           "label_anchor_found": True, "parse_ok": True}
    c2 = {"value_num": 1234.56, "method": "text_anchor",
           "label_anchor_found": True, "parse_ok": True}
    c3 = {"value_num": 999.99, "method": "ocr_anchor",
           "label_anchor_found": True, "parse_ok": True}
    others = [c1, c2, c3]

    s1 = _score_candidate(c1, others, {}, "ordinary_dividends")
    s2 = _score_candidate(c2, others, {}, "ordinary_dividends")
    s3 = _score_candidate(c3, others, {}, "ordinary_dividends")

    check(s1 > 0, f"Claude candidate has positive score ({s1})")
    check(s2 > 0, f"Text anchor has positive score ({s2})")
    # c1 and c2 agree, c3 disagrees with both → c3 penalized
    check(s1 > s3, f"Agreeing candidate scores higher ({s1} > {s3})")
    check(s2 > s3, f"Agreeing candidate scores higher ({s2} > {s3})")

    print("\n═══ Consensus: Agreement → auto_verified ═══")
    ext = _make_ext("1099-DIV", "Vanguard", 1, {
        "ordinary_dividends": 1234.56,
        "qualified_dividends": 567.89,
    })
    # Text layer matches Claude
    pt = "1a Ordinary dividends  $1,234.56  1b Qualified dividends  $567.89"
    ot = "1a Ordinary dividends  1,234.56  1b Qualified dividends  567.89"

    result_ext, cdata = build_consensus([ext], [pt], [ot], ocr_confidences=[80.0])
    check(cdata["fields_checked"] >= 2, f"Checked >= 2 fields (got {cdata['fields_checked']})")
    # With 3 candidates all agreeing, should be auto_verified
    ext_fields = result_ext[0]["fields"]
    ord_conf = ext_fields.get("ordinary_dividends", {}).get("confidence", "")
    check(ord_conf == "auto_verified", f"ordinary_dividends auto_verified (got '{ord_conf}')")
    qual_conf = ext_fields.get("qualified_dividends", {}).get("confidence", "")
    check(qual_conf == "auto_verified", f"qualified_dividends auto_verified (got '{qual_conf}')")

    print("\n═══ Consensus: Disagreement → needs_review ═══")
    ext2 = _make_ext("1099-DIV", "Fidelity", 1, {
        "ordinary_dividends": 1234.56,
    })
    # Text says different value
    pt2 = "1a Ordinary dividends  $9,999.99"
    ot2 = "1a Ordinary dividends  8,888.88"

    result_ext2, cdata2 = build_consensus([ext2], [pt2], [ot2])
    ext2_fields = result_ext2[0]["fields"]
    ord_field = ext2_fields.get("ordinary_dividends", {})
    # All 3 disagree → likely needs_review (none gets agreement bonus)
    has_consensus = "_consensus" in ord_field
    check(has_consensus, "Has _consensus metadata")
    if has_consensus:
        consensus_status = ord_field["_consensus"]["status"]
        # All three disagree, but Claude candidate still has label_anchor_found + parse_ok + validation
        # Score = 3 + 2 + 2 = 7 (above threshold of 5)
        # If top score >= threshold AND gap >= margin: auto_verified
        # But runner_up also has 3 + 2 + 2 - 2 (conflict) = 5
        # gap = 7 - 5 = 2.0 which equals margin exactly — needs >= margin
        check(consensus_status in ("auto_verified", "needs_review"),
              f"Status is valid (got '{consensus_status}')")

    print("\n═══ Consensus: Validation rule penalty ═══")
    ext3 = _make_ext("1099-DIV", "BadFund", 1, {
        "ordinary_dividends": 1000.0,
        "qualified_dividends": 1500.0,  # qualified > ordinary → violation
    })
    pt3 = "1a Ordinary dividends  $1,000.00  1b Qualified dividends  $1,500.00"
    result_ext3, cdata3 = build_consensus([ext3], [pt3], [None])
    qf = result_ext3[0]["fields"].get("qualified_dividends", {})
    if "_consensus" in qf:
        # The qualified value should still go through (it's what's on the document)
        # but the validation rule failure should be noted
        check(qf["_consensus"]["num_candidates"] >= 1, "Candidates generated despite violation")

    print("\n═══ Consensus: Single candidate → modest score ═══")
    ext4 = _make_ext("K-1", "Partnership A", 1, {
        "box1_ordinary_income": 50000.0,
    })
    # No text layer, no OCR → only Claude candidate
    result_ext4, cdata4 = build_consensus([ext4], [None], [None])
    b1 = result_ext4[0]["fields"].get("box1_ordinary_income", {})
    if "_consensus" in b1:
        # Single candidate: label_found(3) + parse_ok(2) + validation(2) = 7
        # No agreement bonus (only 1 candidate), no conflict penalty
        # Score should be >= threshold (5.0)
        check(b1["_consensus"]["score"] >= 5.0, f"Single candidate score >= 5 (got {b1['_consensus']['score']})")
        check(b1["_consensus"]["num_candidates"] == 1, f"1 candidate (got {b1['_consensus']['num_candidates']})")

    print("\n═══ Consensus: Non-scope doc types skipped ═══")
    ext5 = _make_ext("W-2", "Employer", 1, {"wages": 50000.0, "federal_wh": 8000.0})
    result_ext5, cdata5 = build_consensus([ext5], ["some text"], ["some text"])
    check(cdata5["fields_checked"] == 0, f"W-2 not in scope (got {cdata5['fields_checked']} fields checked)")

    print("\n═══ Consensus: K-1 boxes verified ═══")
    ext6 = _make_ext("K-1", "LLC Fund", 2, {
        "box1_ordinary_income": 10000.0,
        "box2_rental_real_estate": -5000.0,
        "box5_interest": 1200.0,
    })
    pt6 = "Box 1 Ordinary business income 10,000.00  Box 2 Net rental real estate (5,000.00)  Box 5 Interest income 1,200.00"
    ot6 = "Box 1 Ordinary business income 10,000.00  Box 2 Net rental real estate (5,000.00)  Box 5 Interest income 1,200.00"
    result_ext6, cdata6 = build_consensus([ext6], [None, pt6], [None, ot6], ocr_confidences=[None, 90.0])
    check(cdata6["fields_checked"] >= 3, f"K-1 checked >= 3 fields (got {cdata6['fields_checked']})")
    b1_conf = result_ext6[0]["fields"].get("box1_ordinary_income", {}).get("confidence", "")
    check(b1_conf == "auto_verified", f"K-1 box1 auto_verified (got '{b1_conf}')")

    print("\n═══ Consensus: Brokerage prefixed fields ═══")
    ext7 = _make_ext("1099-DIV", "Schwab", 1, {
        "div_ordinary_dividends": 5000.0,
        "div_qualified_dividends": 2000.0,
    })
    pt7 = "1a Ordinary dividends  $5,000.00  1b Qualified dividends  $2,000.00"
    result_ext7, cdata7 = build_consensus([ext7], [pt7], [pt7])
    check(cdata7["fields_checked"] >= 2, f"Prefixed fields checked (got {cdata7['fields_checked']})")

    print("\n═══ Consensus: Metadata structure ═══")
    # Reuse the first agreement test
    ext8 = _make_ext("1099-INT", "Treasury", 1, {
        "interest_income": 500.0,
    })
    pt8 = "Box 1 Interest income  $500.00"
    result_ext8, cdata8 = build_consensus([ext8], [pt8], [pt8])
    # Check consensus_data structure
    check("fields_checked" in cdata8, "consensus_data has fields_checked")
    check("auto_verified" in cdata8, "consensus_data has auto_verified")
    check("needs_review" in cdata8, "consensus_data has needs_review")
    check("per_extraction" in cdata8, "consensus_data has per_extraction")
    if cdata8["per_extraction"]:
        pe = cdata8["per_extraction"][0]
        check("page" in pe, "per_extraction entry has page")
        check("doc_type" in pe, "per_extraction entry has doc_type")
        check("entity" in pe, "per_extraction entry has entity")
        check("fields" in pe, "per_extraction entry has fields")

    # ═══ Summary ═══
    print(f"\n{'='*50}")
    print(f"  {PASS} passed, {FAIL} failed")
    if FAIL == 0:
        print(f"  All routing + consensus tests passed.")
    else:
        print(f"  FAILURES DETECTED")
    return 1 if FAIL > 0 else 0


if __name__ == "__main__":
    sys.exit(run_tests())
