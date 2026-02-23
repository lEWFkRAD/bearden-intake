#!/usr/bin/env python3
"""
Test suite for T1.1 Page Preprocessing Pipeline.
Tests: auto-rotate, deskew, contrast enhancement, blank detection, quality scoring.
Uses 4 synthetic fixtures: rotated, skewed, faint, almost-blank-with-text.

Run: python3 tests/test_preprocessing.py

Safe to import — all test execution is behind if __name__ == "__main__".
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from PIL import Image, ImageDraw, ImageFont, ImageFilter
from io import BytesIO
import base64
import json
import math

from extract import (
    preprocess_page,
    _is_blank_page,
    _compute_quality_score,
    _deskew_page,
    _enhance_contrast,
    auto_rotate,
    BLANK_PAGE_THRESHOLD,
    QUALITY_GOOD,
    QUALITY_POOR,
    CONTRAST_MIN_STD,
)

PASS = 0
FAIL = 0

def check(condition, msg):
    global PASS, FAIL
    if condition:
        PASS += 1
        print(f"  \u2713 {msg}")
    else:
        FAIL += 1
        print(f"  \u2717 FAIL: {msg}")


def _get_font(size=28):
    """Get a readable font. Uses system Helvetica on macOS, falls back to default."""
    font_paths = [
        "/System/Library/Fonts/Helvetica.ttc",
        "/System/Library/Fonts/SFNSMono.ttf",
        "/Library/Fonts/Arial.ttf",
        "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
    ]
    for fp in font_paths:
        if os.path.exists(fp):
            try:
                return ImageFont.truetype(fp, size)
            except Exception:
                continue
    return ImageFont.load_default()


_FONT = _get_font(28)
_FONT_SMALL = _get_font(20)


def _make_text_page(width=2100, height=2800, text_lines=None, fill=(0, 0, 0), bg=(255, 255, 255), font=None):
    """Create a synthetic page image with text lines (simulating 250 DPI letter-size).
    Uses system font for realistic pixel coverage."""
    if font is None:
        font = _FONT
    img = Image.new("RGB", (width, height), bg)
    draw = ImageDraw.Draw(img)
    if text_lines is None:
        text_lines = [
            "SCHEDULE K-1 (Form 1065)              Department of the Treasury",
            "Partner's Share of Income               Internal Revenue Service",
            "",
            "Part III   Partner's Share of Current Year Income,",
            "           Deductions, Credits, and Other Items",
            "",
            "1  Ordinary business income (loss)     $25,432.00",
            "2  Net rental real estate income         $3,100.00",
            "3  Other net rental income (loss)              $0",
            "4a Guaranteed payments for services    $12,000.00",
            "4b Guaranteed payments for capital            $0",
            "5  Interest income                        $847.00",
            "6a Ordinary dividends                   $1,200.00",
            "6b Qualified dividends                    $950.00",
            "7  Royalties                                  $0",
            "8  Net short-term capital gain (loss)          $0",
            "9a Net long-term capital gain (loss)     $4,500.00",
            "",
            "13  Code A \u2014 Contributions                 $2,000",
            "14  Code A \u2014 Net earnings from SE         $25,432",
            "15  Code A \u2014 Foreign taxes paid              $125",
        ]
    y = 100
    for line in text_lines:
        if line:
            draw.text((80, y), line, fill=fill, font=font)
        y += 60
    return img


def run_tests():
    """Run all preprocessing tests."""
    global PASS, FAIL

    # ═════════════════════════════════════════════════════════════════════════
    # FIXTURE 1: Rotated page (90° CW — landscape scan of portrait document)
    # ═════════════════════════════════════════════════════════════════════════
    print("\n\u2550\u2550\u2550 Fixture 1: Rotated page (90\u00b0 CW) \u2550\u2550\u2550")
    base_page = _make_text_page()
    rotated_page = base_page.rotate(90, expand=True)
    check(rotated_page.size[0] > rotated_page.size[1], "Rotated page is landscape")
    fixed = auto_rotate(rotated_page.copy())
    check(fixed.size[1] >= fixed.size[0], "auto_rotate corrects to portrait orientation")
    processed, meta = preprocess_page(rotated_page, page_num=1)
    check(not meta["is_blank"], "Rotated page is NOT blank")
    check(meta["quality_score"] > 0, "Rotated page has quality > 0")
    check("original_size" in meta, "Metadata includes original_size")
    check("processed_size" in meta, "Metadata includes processed_size")
    check("blank_reason" in meta, "Metadata includes blank_reason")
    rotated_page.close()
    base_page.close()
    fixed.close()
    processed.close()

    # ═════════════════════════════════════════════════════════════════════════
    # FIXTURE 2: Skewed page (small angle tilt)
    # ═════════════════════════════════════════════════════════════════════════
    print("\n\u2550\u2550\u2550 Fixture 2: Skewed page (2\u00b0 tilt) \u2550\u2550\u2550")
    base_page2 = _make_text_page()
    skewed_page = base_page2.rotate(2, expand=True, fillcolor=(255, 255, 255))
    check(skewed_page.size[0] > base_page2.size[0] or skewed_page.size[1] > base_page2.size[1],
          "Skewed page expanded due to rotation")
    processed2, meta2 = preprocess_page(skewed_page, page_num=2)
    check(not meta2["is_blank"], "Skewed page is NOT blank")
    check(meta2["quality_score"] > 0, "Skewed page quality > 0")
    check("deskew_angle" in meta2, "Metadata includes deskew_angle")
    check("deskew_conf" in meta2, "Metadata includes deskew_conf")
    print(f"  (deskew detected angle: {meta2['deskew_angle']}\u00b0, conf: {meta2['deskew_conf']})")
    skewed_page.close()
    base_page2.close()
    processed2.close()

    # ═════════════════════════════════════════════════════════════════════════
    # FIXTURE 3: Faint/low-contrast page
    # ═════════════════════════════════════════════════════════════════════════
    print("\n\u2550\u2550\u2550 Fixture 3: Faint/low-contrast page \u2550\u2550\u2550")
    faint_page = _make_text_page(fill=(200, 200, 200), bg=(245, 245, 245))
    enhanced, was_enhanced = _enhance_contrast(faint_page.copy())
    check(was_enhanced, "Faint page triggers contrast enhancement")
    enhanced.close()

    score_faint, details_faint = _compute_quality_score(faint_page)
    good_page = _make_text_page()
    score_good, details_good = _compute_quality_score(good_page)
    check(score_faint < score_good, f"Faint page quality ({score_faint}) < good page ({score_good})")

    processed3, meta3 = preprocess_page(faint_page, page_num=3)
    check(meta3["contrast_enhanced"], "Preprocess enhances contrast on faint page")
    check(not meta3["is_blank"], "Faint page is NOT blank (has text)")
    check(meta3["quality_score"] >= score_faint,
          f"Quality after enhancement ({meta3['quality_score']}) >= raw ({score_faint})")
    faint_page.close()
    good_page.close()
    processed3.close()

    # ═════════════════════════════════════════════════════════════════════════
    # FIXTURE 4: Almost-blank page WITH meaningful text
    # ═════════════════════════════════════════════════════════════════════════
    print("\n\u2550\u2550\u2550 Fixture 4: Almost-blank page with text \u2550\u2550\u2550")
    sparse_lines = [
        "Edward Jones Investments",
        "Tax Year 2024 \u2014 1099 Composite Statement",
        "",
        "",
        "Prepared for: WATTS, JEFFREY S",
        "Account Number: ***-***-1234",
        "",
        "",
        "",
        "This statement contains your:",
        "  Form 1099-DIV   Dividends and Distributions",
        "  Form 1099-INT   Interest Income",
        "  Form 1099-B     Proceeds from Broker Transactions",
    ]
    sparse_page = _make_text_page(text_lines=sparse_lines, font=_FONT_SMALL)

    is_blank_no_text, pct, reason = _is_blank_page(sparse_page)
    print(f"  (pixel-only: is_blank={is_blank_no_text}, pct={pct:.2f}%, reason={reason})")

    sparse_text = "\n".join(sparse_lines)
    is_blank_with_text, pct2, reason2 = _is_blank_page(sparse_page, page_text=sparse_text)
    check(not is_blank_with_text, f"Sparse page NOT blank with text override (reason={reason2})")
    check(reason2 in ("not_blank", "overridden_by_text"),
          f"Blank reason is '{reason2}' (expected not_blank or overridden_by_text)")

    processed4, meta4 = preprocess_page(sparse_page, page_num=4, page_text=sparse_text)
    check(not meta4["is_blank"], "Sparse page preprocess (with text): not blank")
    check(meta4["quality_score"] > 0, f"Sparse page has quality > 0 (got {meta4['quality_score']})")
    sparse_page.close()
    processed4.close()

    # ═════════════════════════════════════════════════════════════════════════
    # BLANK PAGE CLASSIFICATION TESTS
    # ═════════════════════════════════════════════════════════════════════════
    print("\n\u2550\u2550\u2550 Blank page classification \u2550\u2550\u2550")

    blank = Image.new("RGB", (2100, 2800), (255, 255, 255))
    is_b, pct_b, reason_b = _is_blank_page(blank)
    check(is_b, f"Pure white page IS blank (pct={pct_b:.2f}%)")
    check(reason_b == "blank_true", f"Reason is 'blank_true' (got '{reason_b}')")

    smudge = Image.new("RGB", (2100, 2800), (255, 255, 255))
    draw_s = ImageDraw.Draw(smudge)
    draw_s.rectangle([100, 100, 110, 110], fill=(200, 200, 200))
    is_b_s, pct_s, reason_s = _is_blank_page(smudge)
    check(is_b_s, f"Smudge-only page is blank (pct={pct_s:.4f}%)")
    check(reason_s in ("blank_true", "blank_low_value"), f"Reason is '{reason_s}'")
    smudge.close()

    near_blank = Image.new("RGB", (2100, 2800), (255, 255, 255))
    draw_nb = ImageDraw.Draw(near_blank)
    draw_nb.text((100, 100), "x", fill=(0, 0, 0), font=_FONT_SMALL)
    is_b_nb, _, reason_nb = _is_blank_page(near_blank)
    is_b_override, _, reason_override = _is_blank_page(near_blank, page_text="Box 1 Ordinary Income $25,432.00")
    check(is_b_override == False, "Text override prevents blank classification")
    check(reason_override == "overridden_by_text", f"Reason is 'overridden_by_text' (got '{reason_override}')")
    near_blank.close()

    empty_img = Image.new("RGB", (2100, 2800), (255, 255, 255))
    _, _, reason_money = _is_blank_page(empty_img, page_text="Total: $1,234.56")
    check(reason_money == "overridden_by_text", "Money pattern overrides blank")
    _, _, reason_kw = _is_blank_page(empty_img, page_text="Schedule K-1 Form 1065 continuation")
    check(reason_kw == "overridden_by_text", "IRS keyword overrides blank")
    _, _, reason_short = _is_blank_page(empty_img, page_text="hi")
    check(reason_short in ("blank_true", "blank_low_value"), f"Short random text doesn't override (got '{reason_short}')")
    empty_img.close()

    _, meta_blank = preprocess_page(blank, page_num=99)
    check(meta_blank["is_blank"], "Pure white preprocess: is_blank=True")
    check(meta_blank["blank_reason"] in ("blank_true", "blank_low_value"),
          f"blank_reason='{meta_blank['blank_reason']}'")
    check(meta_blank["quality_score"] == 0.0, "Blank page quality_score == 0.0")
    blank.close()

    # ═════════════════════════════════════════════════════════════════════════
    # GUARDRAIL: noisy near-blank pages don't get contrast-enhanced
    # ═════════════════════════════════════════════════════════════════════════
    print("\n\u2550\u2550\u2550 Guardrail: noisy near-blank \u2550\u2550\u2550")
    noisy = Image.new("RGB", (2100, 2800), (252, 252, 252))
    draw_n = ImageDraw.Draw(noisy)
    # Scatter a few tiny marks — noise, not content
    for x in range(0, 2100, 300):
        draw_n.point((x, 500), fill=(200, 200, 200))
    _, was_enh_noisy = _enhance_contrast(noisy)
    check(not was_enh_noisy, "Noisy near-blank page NOT enhanced (guardrail: <0.2% non-white)")
    noisy.close()

    # ═════════════════════════════════════════════════════════════════════════
    # MASKED STD_DEV IN QUALITY SCORE
    # ═════════════════════════════════════════════════════════════════════════
    print("\n\u2550\u2550\u2550 Masked std_dev in quality score \u2550\u2550\u2550")
    good_q = _make_text_page()
    score_q, details_q = _compute_quality_score(good_q)
    check("std_dev_masked" in details_q, "Quality details includes std_dev_masked")
    check("std_dev_whole" in details_q, "Quality details includes std_dev_whole")
    check("content_bbox" in details_q, "Quality details includes content_bbox")
    # Masked std_dev should be higher than whole-page (content area has more contrast)
    if details_q.get("content_bbox"):
        check(details_q["std_dev_masked"] >= details_q["std_dev_whole"],
              f"Masked std_dev ({details_q['std_dev_masked']}) >= whole ({details_q['std_dev_whole']})")
    good_q.close()

    # ═════════════════════════════════════════════════════════════════════════
    # METADATA STRUCTURE TESTS
    # ═════════════════════════════════════════════════════════════════════════
    print("\n\u2550\u2550\u2550 Metadata structure tests \u2550\u2550\u2550")
    test_page = _make_text_page()
    _, meta_full = preprocess_page(test_page, page_num=1)

    required_keys = [
        "page_num", "is_blank", "blank_reason", "pct_non_white",
        "deskew_angle", "deskew_conf",
        "contrast_enhanced", "quality_score", "quality_details",
        "original_size", "processed_size",
    ]
    for key in required_keys:
        check(key in meta_full, f"Metadata contains '{key}'")

    check(isinstance(meta_full["original_size"], list) and len(meta_full["original_size"]) == 2,
          "original_size is [w, h]")
    check(isinstance(meta_full["processed_size"], list) and len(meta_full["processed_size"]) == 2,
          "processed_size is [w, h]")
    check(isinstance(meta_full["quality_score"], float), "quality_score is float")
    check(0.0 <= meta_full["quality_score"] <= 1.0, f"quality_score in [0,1] (got {meta_full['quality_score']})")
    check(isinstance(meta_full["quality_details"], dict), "quality_details is dict")
    check(meta_full["blank_reason"] == "not_blank", f"Non-blank page reason='not_blank' (got '{meta_full['blank_reason']}')")
    test_page.close()

    # ═════════════════════════════════════════════════════════════════════════
    # PAGE NUMBERING STABILITY TEST
    # ═════════════════════════════════════════════════════════════════════════
    print("\n\u2550\u2550\u2550 Page numbering stability \u2550\u2550\u2550")
    pages = []
    for i in range(5):
        if i % 2 == 0:
            lines = [f"Page {i+1} \u2014 Document Content"] + [f"Line {j}: Tax data here $1,234.56" for j in range(15)]
            pages.append(_make_text_page(text_lines=lines))
        else:
            pages.append(Image.new("RGB", (2100, 2800), (255, 255, 255)))

    results = []
    for i, pg in enumerate(pages):
        _, m = preprocess_page(pg, page_num=i + 1)
        results.append(m)
        pg.close()

    check(results[0]["page_num"] == 1, "Page 1 numbered correctly")
    check(results[1]["page_num"] == 2, "Page 2 numbered correctly (blank, not renumbered)")
    check(results[2]["page_num"] == 3, "Page 3 numbered correctly")
    check(results[3]["page_num"] == 4, "Page 4 numbered correctly (blank, not renumbered)")
    check(results[4]["page_num"] == 5, "Page 5 numbered correctly")

    blank_indices = [r["page_num"] for r in results if r["is_blank"]]
    text_indices = [r["page_num"] for r in results if not r["is_blank"]]
    check(blank_indices == [2, 4], f"Blank pages at positions 2,4 (got {blank_indices})")
    check(text_indices == [1, 3, 5], f"Text pages at positions 1,3,5 (got {text_indices})")

    # ═════════════════════════════════════════════════════════════════════════
    # CONTRAST CONDITIONALLY APPLIED TEST
    # ═════════════════════════════════════════════════════════════════════════
    print("\n\u2550\u2550\u2550 Contrast enhancement is conditional \u2550\u2550\u2550")
    good = _make_text_page()
    _, was_enh_good = _enhance_contrast(good)
    check(not was_enh_good, f"High-contrast page NOT enhanced (masked std_dev threshold={CONTRAST_MIN_STD})")
    good.close()

    faint2 = _make_text_page(fill=(210, 210, 210), bg=(240, 240, 240))
    _, was_enh_faint = _enhance_contrast(faint2)
    check(was_enh_faint, "Low-contrast page IS enhanced")
    faint2.close()

    # ═══════════════════════════════
    # SUMMARY
    # ═══════════════════════════════
    print(f"\n{'=' * 50}")
    print(f"  {PASS} passed, {FAIL} failed")
    if FAIL:
        print(f"  *** {FAIL} FAILURES ***")
        return 1
    else:
        print("  All preprocessing tests passed.")
        return 0


if __name__ == "__main__":
    sys.exit(run_tests())
