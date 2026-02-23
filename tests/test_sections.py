#!/usr/bin/env python3
"""Tests for T1.3 Section / Form Detection.

Run:  python3 tests/test_sections.py
All test execution is inside run_tests() behind __name__ guard.
"""

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from extract import (
    detect_sections,
    SECTION_KEYWORDS, SECTION_SCORE_THRESHOLD,
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

def _make_routing(n, blanks=None, methods=None):
    """Build a fake routing_plan list.

    blanks = set of 1-indexed page numbers that are blank.
    methods = dict mapping 1-indexed page to method string (default text_layer).
    """
    blanks = blanks or set()
    methods = methods or {}
    plan = []
    for i in range(n):
        pg = i + 1
        is_blank = pg in blanks
        m = "skip_blank" if is_blank else methods.get(pg, "text_layer")
        plan.append({
            "page_num": pg,
            "blank": is_blank,
            "text_chars": 0 if is_blank else 500,
            "text_words": 0 if is_blank else 80,
            "digit_ratio": 0.0,
            "ocr_chars": 0,
            "ocr_conf_avg": None,
            "method": m,
            "reason": "blank" if is_blank else "text_chars>=200",
        })
    return plan


# ─── Realistic page text builders ─────────────────────────────────────────

def _brokerage_summary_text():
    return """
    TAX INFORMATION STATEMENT
    Summary of Income

    Total Ordinary Dividends    $12,345.67
    Total Qualified Dividends   $8,901.23
    Total Interest Income       $456.78
    Federal Income Tax Withheld $1,234.56

    This information is being furnished to the IRS.
    """

def _div_page_text():
    return """
    FORM 1099-DIV
    Dividends and Distributions

    1a Ordinary Dividends        $5,432.10
    1b Qualified Dividends       $3,210.45
    2a Total Capital Gain Distr  $678.90
    3  Nondividend Distributions  $0.00
    4  Federal Income Tax Withheld $543.21
    5  Section 199A Dividends    $200.00
    6  Foreign Tax Paid          $12.34
    """

def _int_page_text():
    return """
    FORM 1099-INT
    Interest Income

    1  Interest Income            $1,234.56
    3  US Savings Bonds and Treasury  $500.00
    4  Federal Income Tax Withheld    $123.45
    5  Investment Expenses            $10.00
    8  Tax-Exempt Interest            $0.00
    11 Bond Premium                   $25.00
    """

def _oid_page_text():
    return """
    FORM 1099-OID
    Original Issue Discount

    1  Original Issue Discount   $345.67
    2  Other Periodic Interest   $0.00
    4  Federal Income Tax Withheld $34.56
    6  Acquisition Premium       $50.00
    11 OID on US Treasury        $100.00
    """

def _b_summary_text():
    return """
    FORM 1099-B
    Proceeds from Broker and Barter Exchange Transactions

    Short-Term Transactions - Basis Reported to IRS
    Total Proceeds            $25,000.00
    Total Cost Basis          $22,000.00
    Total Gain/Loss           $3,000.00
    Wash Sale Loss Disallowed $150.00

    Long-Term Transactions - Basis Reported to IRS
    Total Proceeds            $50,000.00
    Total Cost Basis          $40,000.00
    Total Gain/Loss           $10,000.00
    """

def _b_transactions_text():
    return """
    Symbol    CUSIP        Date Acquired  Date Sold    Quantity Sold
    AAPL      037833100    01/15/2024     06/30/2024   100
    Proceeds: $15,000.00   Cost Basis: $12,000.00  Gain/Loss: $3,000.00

    MSFT      594918104    03/01/2024     09/15/2024   50
    Proceeds: $10,000.00   Cost Basis: $10,000.00  Gain/Loss: $0.00
    """

def _k1_1065_text():
    return """
    Schedule K-1 (Form 1065)
    Department of the Treasury - Internal Revenue Service

    Partner's Share of Income, Deductions, Credits, etc.
    Partnership name: ABC Investment Partners LP

    Part III Partner's Share of Current Year Income
    1  Ordinary Income     $5,000.00
    2  Rental Real Estate  ($1,200.00)
    5  Interest Income     $300.00

    General Partner
    """

def _k1_1120s_text():
    return """
    Schedule K-1 (Form 1120-S)
    Department of the Treasury - Internal Revenue Service

    Shareholder's Share of Income, Deductions, Credits, etc.
    S Corporation name: XYZ Holdings Inc

    Part III Shareholder's Share of Current Year Income
    1  Ordinary Income     $8,000.00
    5  Interest Income     $150.00
    6a Ordinary Dividends  $400.00
    """

def _k1_1041_text():
    return """
    Schedule K-1 (Form 1041)
    Department of the Treasury - Internal Revenue Service

    Beneficiary's Share of Income, Deductions, Credits, etc.
    Estate or Trust name: Smith Family Trust
    Fiduciary name: John Smith

    1  Interest Income     $2,500.00
    2a Ordinary Dividends  $1,200.00
    5  Capital Gain        $800.00
    """

def _cover_page_text():
    return """
    IMPORTANT TAX INFORMATION

    Dear Client,

    Enclosed you will find your annual tax reporting statement
    for the year ended December 31, 2024.

    This is not a bill. Please retain this document for your records.
    Do not file this page with your tax return.
    """

def _continuation_text():
    return """
    Continued from previous page

    Additional Detail — Supplemental Information

    See Statement for more details on Section 199A dividends.
    """

def _sparse_text():
    return "Page 7 of 12"


def _mixed_div_int_text():
    """A page that has both DIV and INT content (brokerage summary page)."""
    return """
    TAX INFORMATION STATEMENT
    Year-End Summary

    1099-DIV Summary
    Ordinary Dividends   $5,000.00
    Qualified Dividends  $3,000.00

    1099-INT Summary
    Interest Income      $1,200.00
    Tax-Exempt Interest  $0.00

    Federal Income Tax Withheld  $600.00
    """

def _no_match_text():
    """Text that doesn't match any section keywords at threshold."""
    return """
    Lorem ipsum dolor sit amet, consectetur adipiscing elit.
    Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.
    Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris.
    """


# ─── Test functions ───────────────────────────────────────────────────────

def test_div_detection():
    """1099-DIV page correctly tagged as 'div'."""
    print("\n── 1099-DIV Detection ──")
    page_texts = [_div_page_text()]
    routing = _make_routing(1)
    result = detect_sections(page_texts, routing)
    check("1" in result, "page 1 in result")
    check("div" in result.get("1", []), f"'div' in labels: {result.get('1')}")


def test_int_detection():
    """1099-INT page correctly tagged as 'int'."""
    print("\n── 1099-INT Detection ──")
    page_texts = [_int_page_text()]
    routing = _make_routing(1)
    result = detect_sections(page_texts, routing)
    check("int" in result.get("1", []), f"'int' in labels: {result.get('1')}")


def test_oid_detection():
    """1099-OID page correctly tagged as 'oid'."""
    print("\n── 1099-OID Detection ──")
    page_texts = [_oid_page_text()]
    routing = _make_routing(1)
    result = detect_sections(page_texts, routing)
    check("oid" in result.get("1", []), f"'oid' in labels: {result.get('1')}")


def test_b_summary_detection():
    """1099-B summary page correctly tagged as 'b_summary'."""
    print("\n── 1099-B Summary Detection ──")
    page_texts = [_b_summary_text()]
    routing = _make_routing(1)
    result = detect_sections(page_texts, routing)
    check("b_summary" in result.get("1", []), f"'b_summary' in labels: {result.get('1')}")


def test_b_transactions_detection():
    """1099-B transaction detail page correctly tagged."""
    print("\n── 1099-B Transactions Detection ──")
    page_texts = [_b_transactions_text()]
    routing = _make_routing(1)
    result = detect_sections(page_texts, routing)
    check("b_transactions" in result.get("1", []), f"'b_transactions' in labels: {result.get('1')}")


def test_summary_detection():
    """Brokerage summary page correctly tagged as 'summary'."""
    print("\n── Brokerage Summary Detection ──")
    page_texts = [_brokerage_summary_text()]
    routing = _make_routing(1)
    result = detect_sections(page_texts, routing)
    check("summary" in result.get("1", []), f"'summary' in labels: {result.get('1')}")


def test_k1_1065_detection():
    """K-1 Form 1065 (partnership) correctly tagged."""
    print("\n── K-1 1065 Detection ──")
    page_texts = [_k1_1065_text()]
    routing = _make_routing(1)
    result = detect_sections(page_texts, routing)
    check("k1_1065" in result.get("1", []), f"'k1_1065' in labels: {result.get('1')}")
    check("k1_1120s" not in result.get("1", []), "not misidentified as 1120S")
    check("k1_1041" not in result.get("1", []), "not misidentified as 1041")


def test_k1_1120s_detection():
    """K-1 Form 1120S (S-corp) correctly tagged."""
    print("\n── K-1 1120S Detection ──")
    page_texts = [_k1_1120s_text()]
    routing = _make_routing(1)
    result = detect_sections(page_texts, routing)
    check("k1_1120s" in result.get("1", []), f"'k1_1120s' in labels: {result.get('1')}")
    check("k1_1065" not in result.get("1", []), "not misidentified as 1065")


def test_k1_1041_detection():
    """K-1 Form 1041 (trust/estate) correctly tagged."""
    print("\n── K-1 1041 Detection ──")
    page_texts = [_k1_1041_text()]
    routing = _make_routing(1)
    result = detect_sections(page_texts, routing)
    check("k1_1041" in result.get("1", []), f"'k1_1041' in labels: {result.get('1')}")
    check("k1_1065" not in result.get("1", []), "not misidentified as 1065")
    check("k1_1120s" not in result.get("1", []), "not misidentified as 1120S")


def test_cover_detection():
    """Cover page correctly tagged."""
    print("\n── Cover Page Detection ──")
    page_texts = [_cover_page_text()]
    routing = _make_routing(1)
    result = detect_sections(page_texts, routing)
    check("cover" in result.get("1", []), f"'cover' in labels: {result.get('1')}")


def test_continuation_detection():
    """Continuation page correctly tagged."""
    print("\n── Continuation Page Detection ──")
    page_texts = [_continuation_text()]
    routing = _make_routing(1)
    result = detect_sections(page_texts, routing)
    check("continuation" in result.get("1", []), f"'continuation' in labels: {result.get('1')}")


def test_blank_page_skipped():
    """Blank pages are skipped, not labeled."""
    print("\n── Blank Page Skip ──")
    page_texts = ["Some text", None, "More text"]
    routing = _make_routing(3, blanks={2})
    result = detect_sections(page_texts, routing)
    check("2" not in result, f"page 2 (blank) not in result: keys={list(result.keys())}")
    check("1" in result, "page 1 (non-blank) in result")
    check("3" in result, "page 3 (non-blank) in result")


def test_sparse_page_unknown():
    """Sparse text (too few keywords) → 'unknown'."""
    print("\n── Sparse Page → Unknown ──")
    page_texts = [_sparse_text()]
    routing = _make_routing(1)
    result = detect_sections(page_texts, routing)
    check(result.get("1") == ["unknown"], f"sparse page → unknown: {result.get('1')}")


def test_no_text_unknown():
    """Page with no usable text → 'unknown'."""
    print("\n── No Text → Unknown ──")
    page_texts = ["", None, "   "]
    routing = _make_routing(3)
    result = detect_sections(page_texts, routing)
    check(result.get("1") == ["unknown"], f"empty string → unknown: {result.get('1')}")
    check(result.get("2") == ["unknown"], f"None → unknown: {result.get('2')}")
    check(result.get("3") == ["unknown"], f"whitespace → unknown: {result.get('3')}")


def test_no_match_unknown():
    """Text with no keyword matches → 'unknown'."""
    print("\n── No Match → Unknown ──")
    page_texts = [_no_match_text()]
    routing = _make_routing(1)
    result = detect_sections(page_texts, routing)
    check(result.get("1") == ["unknown"], f"no matches → unknown: {result.get('1')}")


def test_multi_label():
    """Page with mixed DIV + INT content gets multiple labels."""
    print("\n── Multi-Label Detection ──")
    page_texts = [_mixed_div_int_text()]
    routing = _make_routing(1)
    result = detect_sections(page_texts, routing)
    labels = result.get("1", [])
    check("summary" in labels, f"'summary' in multi-label: {labels}")
    # The mixed page may also trigger div and/or int
    has_multi = len(labels) > 1
    check(has_multi, f"more than one label assigned: {labels}")


def test_brokerage_packet():
    """Full brokerage packet: cover + summary + div + int + b_summary."""
    print("\n── Brokerage Packet ──")
    page_texts = [
        _cover_page_text(),
        _brokerage_summary_text(),
        _div_page_text(),
        _int_page_text(),
        _b_summary_text(),
        _b_transactions_text(),
    ]
    routing = _make_routing(6)
    result = detect_sections(page_texts, routing)

    check("cover" in result.get("1", []), f"p1 cover: {result.get('1')}")
    check("summary" in result.get("2", []), f"p2 summary: {result.get('2')}")
    check("div" in result.get("3", []), f"p3 div: {result.get('3')}")
    check("int" in result.get("4", []), f"p4 int: {result.get('4')}")
    check("b_summary" in result.get("5", []), f"p5 b_summary: {result.get('5')}")
    check("b_transactions" in result.get("6", []), f"p6 b_transactions: {result.get('6')}")

    # At least one summary page
    has_summary = any("summary" in result.get(str(p), []) for p in range(1, 7))
    check(has_summary, "brokerage packet has at least one summary")


def test_k1_packet():
    """K-1 packet: correct form type tagged."""
    print("\n── K-1 Packet ──")
    page_texts = [
        _k1_1065_text(),
        _continuation_text(),
        _k1_1120s_text(),
    ]
    routing = _make_routing(3)
    result = detect_sections(page_texts, routing)

    check("k1_1065" in result.get("1", []), f"p1 k1_1065: {result.get('1')}")
    check("continuation" in result.get("2", []), f"p2 continuation: {result.get('2')}")
    check("k1_1120s" in result.get("3", []), f"p3 k1_1120s: {result.get('3')}")


def test_mixed_no_mass_mislabel():
    """Mixed document: brokerage pages not mass-mislabeled as K-1."""
    print("\n── Mixed Document — No Mass Mislabel ──")
    page_texts = [
        _brokerage_summary_text(),
        _div_page_text(),
        _int_page_text(),
        _k1_1065_text(),
    ]
    routing = _make_routing(4)
    result = detect_sections(page_texts, routing)

    # Brokerage pages should NOT have k1_* labels
    for p in (1, 2, 3):
        labels = result.get(str(p), [])
        k1_labels = [l for l in labels if l.startswith("k1_")]
        check(len(k1_labels) == 0, f"p{p} not K-1: labels={labels}")

    # K-1 page should have k1_1065
    check("k1_1065" in result.get("4", []), f"p4 K-1: {result.get('4')}")


def test_ocr_text_source():
    """Pages routed to OCR use ocr_texts instead of page_texts."""
    print("\n── OCR Text Source ──")
    # page_texts has no useful content for page 1; ocr_texts has K-1
    page_texts = ["random stuff no keywords", _div_page_text()]
    ocr_texts = [_k1_1065_text(), None]
    routing = _make_routing(2, methods={1: "ocr", 2: "text_layer"})
    result = detect_sections(page_texts, routing, ocr_texts=ocr_texts)

    check("k1_1065" in result.get("1", []), f"p1 used OCR text → k1_1065: {result.get('1')}")
    check("div" in result.get("2", []), f"p2 used text_layer → div: {result.get('2')}")


def test_vision_fallback_text():
    """Vision-routed pages try text_layer then OCR for keyword matching."""
    print("\n── Vision Fallback Text Source ──")
    # Vision page with only OCR text available
    page_texts = [None]
    ocr_texts = [_int_page_text()]
    routing = _make_routing(1, methods={1: "vision"})
    result = detect_sections(page_texts, routing, ocr_texts=ocr_texts)

    check("int" in result.get("1", []), f"vision page fell back to OCR → int: {result.get('1')}")


def test_empty_inputs():
    """Empty inputs return empty dict."""
    print("\n── Empty Inputs ──")
    result = detect_sections([], [], None)
    check(result == {}, f"empty → empty dict: {result}")

    result2 = detect_sections(None, None, None)
    check(result2 == {}, f"None → empty dict: {result2}")


def test_page_numbering():
    """Section labels use correct page numbers (1-indexed strings)."""
    print("\n── Page Numbering ──")
    page_texts = [_div_page_text(), None, _int_page_text()]
    routing = _make_routing(3, blanks={2})
    result = detect_sections(page_texts, routing)

    check("1" in result, "page 1 present")
    check("2" not in result, "page 2 (blank) absent")
    check("3" in result, "page 3 present")
    # Verify correct content
    check("div" in result.get("1", []), "page 1 is div")
    check("int" in result.get("3", []), "page 3 is int")


def test_case_insensitive():
    """Keyword matching is case-insensitive."""
    print("\n── Case Insensitive ──")
    page_texts = ["FORM 1099-DIV\nDIVIDENDS AND DISTRIBUTIONS\n1A ORDINARY DIVIDENDS $500\n1B QUALIFIED DIVIDENDS $300"]
    routing = _make_routing(1)
    result = detect_sections(page_texts, routing)
    check("div" in result.get("1", []), f"uppercase text matched 'div': {result.get('1')}")


def test_threshold_below():
    """Page with low keyword score (below threshold) → unknown."""
    print("\n── Below Threshold → Unknown ──")
    # Only one weak keyword hit — e.g. "trust" (weight=2) < threshold of 3
    page_texts = ["The trust was established in 2020."]
    routing = _make_routing(1)
    result = detect_sections(page_texts, routing)
    check(result.get("1") == ["unknown"], f"below threshold → unknown: {result.get('1')}")


def test_keyword_constants_complete():
    """All expected section labels exist in SECTION_KEYWORDS."""
    print("\n── Keyword Constants Complete ──")
    expected = {"summary", "div", "int", "oid", "b_summary", "b_transactions",
                "k1_1065", "k1_1120s", "k1_1041", "cover", "continuation"}
    actual = set(SECTION_KEYWORDS.keys())
    for label in expected:
        check(label in actual, f"'{label}' in SECTION_KEYWORDS")
    # Each section has at least 2 phrases
    for label, phrases in SECTION_KEYWORDS.items():
        check(len(phrases) >= 2, f"'{label}' has {len(phrases)} phrases (>=2)")


def test_threshold_constant():
    """Score threshold constant is positive and reasonable."""
    print("\n── Threshold Constant ──")
    check(SECTION_SCORE_THRESHOLD > 0, f"threshold > 0: {SECTION_SCORE_THRESHOLD}")
    check(SECTION_SCORE_THRESHOLD <= 10, f"threshold <= 10: {SECTION_SCORE_THRESHOLD}")


def test_result_structure():
    """Result dict has correct structure: str keys, list[str] values."""
    print("\n── Result Structure ──")
    page_texts = [_div_page_text(), _k1_1065_text()]
    routing = _make_routing(2)
    result = detect_sections(page_texts, routing)

    for key, val in result.items():
        check(isinstance(key, str), f"key '{key}' is str")
        check(isinstance(val, list), f"value for page {key} is list")
        for label in val:
            check(isinstance(label, str), f"label '{label}' is str")


def test_oid_not_confused_with_int():
    """OID page is labeled 'oid', not confused with 'int'."""
    print("\n── OID vs INT Distinction ──")
    page_texts = [_oid_page_text()]
    routing = _make_routing(1)
    result = detect_sections(page_texts, routing)
    labels = result.get("1", [])
    check("oid" in labels, f"'oid' in labels: {labels}")
    # OID score should be higher than INT score for an OID page
    # (they share 'early withdrawal penalty' but OID has much stronger signals)
    if "int" in labels and "oid" in labels:
        # Both can be present on OID pages due to overlapping keywords
        # but oid should appear first (sorted by score descending)
        check(labels.index("oid") < labels.index("int"),
              f"'oid' ranks higher than 'int': {labels}")


def test_large_page_count():
    """Works correctly with many pages."""
    print("\n── Large Page Count ──")
    page_texts = [_div_page_text()] * 50
    routing = _make_routing(50)
    result = detect_sections(page_texts, routing)
    check(len(result) == 50, f"50 pages → 50 entries: {len(result)}")
    check(all("div" in result.get(str(i+1), []) for i in range(50)),
          "all 50 pages have 'div' label")


# ─── Runner ───────────────────────────────────────────────────────────────

def run_tests():
    global PASS, FAIL
    PASS = 0
    FAIL = 0

    test_div_detection()
    test_int_detection()
    test_oid_detection()
    test_b_summary_detection()
    test_b_transactions_detection()
    test_summary_detection()
    test_k1_1065_detection()
    test_k1_1120s_detection()
    test_k1_1041_detection()
    test_cover_detection()
    test_continuation_detection()
    test_blank_page_skipped()
    test_sparse_page_unknown()
    test_no_text_unknown()
    test_no_match_unknown()
    test_multi_label()
    test_brokerage_packet()
    test_k1_packet()
    test_mixed_no_mass_mislabel()
    test_ocr_text_source()
    test_vision_fallback_text()
    test_empty_inputs()
    test_page_numbering()
    test_case_insensitive()
    test_threshold_below()
    test_keyword_constants_complete()
    test_threshold_constant()
    test_result_structure()
    test_oid_not_confused_with_int()
    test_large_page_count()

    print(f"\n{'=' * 40}")
    total = PASS + FAIL
    print(f"  Section Detection: {PASS}/{total} passed, {FAIL} failed")
    print(f"{'=' * 40}")
    return FAIL


if __name__ == "__main__":
    failures = run_tests()
    sys.exit(1 if failures else 0)
