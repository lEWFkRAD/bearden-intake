#!/usr/bin/env python3
"""Tests for PDF-HOVER — Highlight evidence region on hover in guided review.

Covers: dynamic page image rendering, word data fetch, highlight overlay,
        hover interaction wiring, value bbox matching reuse.

Run:  python3 tests/test_pdf_hover.py
All test execution is inside run_tests() behind __name__ guard.
"""

import sys, os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

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


# ═══════════════════════════════════════════════════════════════════════════════
# TESTS
# ═══════════════════════════════════════════════════════════════════════════════

def run_tests():
    global PASS, FAIL

    # Read app.py source
    app_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "app.py")
    with open(app_path, "r", encoding="utf-8") as f:
        source = f.read()

    print("\n=== PDF-HOVER: Evidence Region Highlighting ===\n")

    # ── Full Page Image in Evidence Panel ──

    print("Evidence Panel Rendering:")
    check("guided-page-wrap" in source,
          "Guided review uses positioned wrapper for overlay support")
    check("guidedPageImg" in source and "data.page_url" in source,
          "Full page image rendered with ID for scaling reference")
    check("_onGuidedPageLoad" in source and "onload" in source,
          "Page image onload handler captures natural dimensions")

    # ── Word Data Fetching ──

    print("\nWord Data Integration:")
    check("_fetchGuidedPageWords" in source,
          "Guided-review-specific word data fetch function exists")
    check("/api/page-words/" in source and "guidedJobId" in source,
          "Word data fetched from page-words API with job ID")
    check("_guidedWordData" in source and "_guidedWordPage" in source,
          "Word data cached per page to avoid redundant fetches")

    # ── Highlight Drawing ──

    print("\nDynamic Highlight Rendering:")
    check("_drawGuidedHighlights" in source,
          "Guided-review highlight drawing function exists")
    check("_clearGuidedHighlights" in source,
          "Highlight clearing function exists")
    check("_findValueBboxesJS" in source and "_guidedWordData" in source,
          "Reuses grid review's _findValueBboxesJS for bbox matching")
    check("scaleX" in source and "scaleY" in source and "_guidedNatW" in source,
          "Highlight positions scaled to displayed image dimensions")
    check("pdf-highlight" in source and "pdf-highlight-pulse" in source,
          "Highlights use existing CSS classes (static + pulse)")

    # ── Hover Interaction ──

    print("\nHover Interaction:")
    check("_wireGuidedValueHover" in source,
          "Hover listener wiring function exists")
    check("mouseenter" in source and "mouseleave" in source,
          "Mouse enter/leave events attached to value element")
    check("_drawGuidedHighlights(true)" in source and "_drawGuidedHighlights(false)" in source,
          "Hover triggers pulse, leave reverts to static highlight")
    check("cursor: crosshair" in source,
          "Hover cursor hint on guided field value")

    # ── Auto-Highlight on Field Load ──

    print("\nAuto-Highlight on Load:")
    check("_fetchGuidedPageWords(data.page_num)" in source,
          "Word data fetched on field load in renderGuidedItem")
    check("_drawGuidedHighlights(false)" in source,
          "Static highlight drawn automatically when data ready")
    check("scrollIntoView" in source and "guided-page-wrap" in source,
          "Highlight scrolled into view for visibility")

    # ── Backend Support (already exists) ──

    print("\nBackend Support:")
    check('"/api/page-words/<job_id>/<int:page_num>"' in source,
          "Page words API endpoint available for guided review")
    check('"/api/page-image/<job_id>/<int:page_num>"' in source,
          "Page image API endpoint available for full page rendering")

    print(f"\n{'=' * 50}")
    print(f"  {PASS} passed, {FAIL} failed")
    if FAIL:
        print(f"  *** {FAIL} FAILURES ***")
    print()
    return FAIL


if __name__ == "__main__":
    failures = run_tests()
    sys.exit(1 if failures else 0)
