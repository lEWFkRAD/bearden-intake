#!/usr/bin/env python3
"""Routing effectiveness report + speed benchmark for T1.2.

Creates synthetic born-digital and scanned PDFs, then measures:
1. Routing effectiveness — what % of pages get each method
2. Speed — Phase 0 timing for old approach vs new approach

Run:  python3 tests/benchmark_routing.py
"""

import sys, os, time, io, base64
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import fitz  # PyMuPDF
from PIL import Image, ImageDraw, ImageFont

# ─── PDF Generation ───────────────────────────────────────────────────────

def _get_font():
    """Get a system font for PIL drawing."""
    for path in ["/System/Library/Fonts/Helvetica.ttc",
                 "/System/Library/Fonts/Supplemental/Arial.ttf",
                 "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf"]:
        if os.path.exists(path):
            return ImageFont.truetype(path, 14)
    return ImageFont.load_default()


def create_born_digital_pdf(path, n_pages=10):
    """Create a born-digital PDF with rich text content (PyMuPDF text layer)."""
    doc = fitz.open()
    for i in range(n_pages):
        page = doc.new_page(width=612, height=792)  # Letter size
        # Insert lots of text so text-layer extraction works well
        text_block = (
            f"Page {i+1} of {n_pages} — Consolidated Statement 2024\n\n"
            f"Account: 1234-5678-9012    Statement Period: 01/01/2024 – 12/31/2024\n\n"
            f"1a  Ordinary dividends         $12,345.67\n"
            f"1b  Qualified dividends          $8,901.23\n"
            f"2a  Total capital gain dist      $2,456.78\n"
            f"3   Nontaxable distributions       $500.00\n"
            f"4   Federal income tax withheld     $123.45\n"
            f"5   Section 199A dividends        $1,000.00\n"
            f"6   Investment expenses             $45.67\n"
            f"7   Foreign tax paid                $89.12\n\n"
            f"Interest Income:\n"
            f"Box 1  Interest income            $3,456.78\n"
            f"Box 3  US Savings Bonds/Treasury     $789.01\n"
            f"Box 4  Federal income tax withheld    $56.78\n\n"
            f"Capital Gains Summary:\n"
            f"Short-term gain/loss             ($1,234.56)\n"
            f"Long-term gain/loss               $5,678.90\n"
            f"Total gain/loss                   $4,444.34\n\n"
            f"This is a digitally generated statement with embedded text.\n"
            f"All values are for testing purposes only.\n"
            f"Recipient: John Q. Taxpayer, 123 Main St, Anytown GA 30001\n"
            f"Payer: Vanguard Group, PO Box 2600, Valley Forge PA 19482\n"
            f"Payer TIN: 23-1945930\n"
        )
        rect = fitz.Rect(50, 50, 560, 740)
        page.insert_textbox(rect, text_block, fontsize=10)
    doc.save(path)
    doc.close()
    print(f"  Created born-digital PDF: {path} ({n_pages} pages)")


def create_scanned_pdf(path, n_pages=10):
    """Create a PDF from rendered images (simulates scanned document — no text layer)."""
    font = _get_font()
    doc = fitz.open()
    for i in range(n_pages):
        # Render an image with text
        img = Image.new("L", (1530, 1980), color=255)  # ~6x8 inches at 255 DPI
        draw = ImageDraw.Draw(img)
        lines = [
            f"Page {i+1} of {n_pages} — Scanned Tax Document 2024",
            "",
            f"W-2 Wage and Tax Statement",
            f"EIN: 12-3456789",
            f"Employer: Acme Corporation",
            f"Address: 456 Oak Ave, Rome GA 30161",
            "",
            f"Box 1  Wages, tips, other comp     $52,345.67",
            f"Box 2  Federal income tax w/h        $8,901.23",
            f"Box 3  Social security wages        $52,345.67",
            f"Box 4  Social security tax w/h        $3,245.43",
            f"Box 5  Medicare wages               $52,345.67",
            f"Box 6  Medicare tax withheld            $759.01",
            f"Box 16 State wages                  $52,345.67",
            f"Box 17 State income tax                $2,617.28",
            "",
            f"Employee: Jane A. Doe, 789 Elm St, Rome GA 30161",
            f"SSN: XXX-XX-1234",
        ]
        y = 80
        for line in lines:
            draw.text((60, y), line, fill=0, font=font)
            y += 30

        # Add some noise to simulate scan artifacts
        import random
        random.seed(i)
        for _ in range(50):
            x, yp = random.randint(0, 1529), random.randint(0, 1979)
            draw.point((x, yp), fill=random.randint(180, 220))

        # Convert PIL image to PDF page via PNG
        buf = io.BytesIO()
        img.save(buf, format="PNG")
        buf.seek(0)

        page = doc.new_page(width=612, height=792)
        img_rect = fitz.Rect(0, 0, 612, 792)
        page.insert_image(img_rect, stream=buf.read())

    doc.save(path)
    doc.close()
    print(f"  Created scanned PDF: {path} ({n_pages} pages)")


def create_mixed_pdf(path, n_pages=10):
    """Create a mixed PDF: first half born-digital, second half scanned."""
    font = _get_font()
    doc = fitz.open()
    half = n_pages // 2

    for i in range(n_pages):
        page = doc.new_page(width=612, height=792)

        if i < half:
            # Born-digital page with text layer
            text = (
                f"Page {i+1} of {n_pages} — Digital Section\n\n"
                f"1099-DIV Dividends and Distributions\n"
                f"Payer: Fidelity Investments, 82 Devonshire St, Boston MA 02109\n"
                f"Payer TIN: 04-3523567\n\n"
                f"1a  Ordinary dividends     $6,789.12\n"
                f"1b  Qualified dividends     $4,567.89\n"
                f"4   Federal tax withheld       $67.89\n"
                f"Recipient: Jeffrey Watts\n"
            )
            rect = fitz.Rect(50, 50, 560, 740)
            page.insert_textbox(rect, text, fontsize=10)
        else:
            # Scanned page (image only)
            img = Image.new("L", (1530, 1980), color=255)
            draw = ImageDraw.Draw(img)
            lines = [
                f"Page {i+1} of {n_pages} — Scanned K-1 Section",
                "",
                f"Schedule K-1 (Form 1065)",
                f"Partner's Share of Income",
                f"EIN: 98-7654321",
                f"Box 1 Ordinary income    $15,234.56",
                f"Box 2 Rental real estate  ($3,456.78)",
                f"Box 5 Interest income        $789.01",
            ]
            y = 80
            for line in lines:
                draw.text((60, y), line, fill=0, font=font)
                y += 30

            buf = io.BytesIO()
            img.save(buf, format="PNG")
            buf.seek(0)
            page.insert_image(fitz.Rect(0, 0, 612, 792), stream=buf.read())

    doc.save(path)
    doc.close()
    print(f"  Created mixed PDF: {path} ({half} digital + {n_pages - half} scanned)")


# ─── Routing Effectiveness ─────────────────────────────────────────────────

def measure_routing(pdf_path, label):
    """Run Phase 0a-0d on a PDF and report routing effectiveness."""
    from extract import (extract_text_per_page, has_meaningful_text, pdf_to_images,
                         ocr_all_pages, route_pages, HAS_TESSERACT, HAS_PYMUPDF)

    print(f"\n{'─'*60}")
    print(f"  ROUTING REPORT: {label}")
    print(f"  PDF: {os.path.basename(pdf_path)}")
    print(f"{'─'*60}")

    # Phase 0a: text layer
    page_texts = None
    if HAS_PYMUPDF:
        page_texts = extract_text_per_page(pdf_path)
        if page_texts:
            _, stats = has_meaningful_text(page_texts)
            mp = stats.get("meaningful_pages", 0)
            tp = stats.get("total_pages", 0)
            tc = stats.get("total_chars", 0)
            print(f"  Text layer: {mp}/{tp} meaningful, {tc:,} total chars")
        else:
            print(f"  Text layer: none found")

    # Phase 0b: images + preprocessing
    b64_images, page_preprocessing = pdf_to_images(pdf_path, page_texts=page_texts)
    blank_pages = {m["page_num"] for m in page_preprocessing if m.get("is_blank")}

    # Phase 0c: OCR
    ocr_texts = None
    ocr_confs = None
    if HAS_TESSERACT:
        from PIL import Image as PILImage
        from io import BytesIO as BIO
        pil_for_ocr = []
        for i, b in enumerate(b64_images):
            if (i + 1) in blank_pages:
                pil_for_ocr.append(None)
            else:
                pil_for_ocr.append(PILImage.open(BIO(base64.b64decode(b))))
        ocr_texts, ocr_confs = ocr_all_pages(pil_for_ocr)

    # Phase 0d: Routing
    routing_plan = route_pages(page_texts, ocr_texts, ocr_confs, page_preprocessing)

    # Report
    n = len(routing_plan)
    counts = {}
    for r in routing_plan:
        m = r["method"]
        counts[m] = counts.get(m, 0) + 1

    print(f"\n  EFFECTIVENESS SUMMARY ({n} pages):")
    for method in ("text_layer", "ocr", "vision", "skip_blank"):
        c = counts.get(method, 0)
        pct = c / n * 100 if n > 0 else 0
        bar = "█" * int(pct / 2)
        print(f"    {method:15s}: {c:3d}/{n} ({pct:5.1f}%) {bar}")

    # Per-page detail
    print(f"\n  PER-PAGE DETAIL:")
    for r in routing_plan:
        tl = r.get("text_chars", 0)
        oc = r.get("ocr_chars", 0)
        conf = r.get("ocr_conf_avg")
        conf_str = f" conf={conf:.0f}%" if conf else ""
        print(f"    p.{r['page_num']:2d}: {r['method']:12s} | text={tl:5d} | ocr={oc:5d}{conf_str} | {r['reason']}")

    return routing_plan


# ─── Speed Benchmark ───────────────────────────────────────────────────────

def benchmark_phase0(pdf_path, label):
    """Measure Phase 0 timing: text-layer + images + OCR."""
    from extract import (extract_text_per_page, has_meaningful_text, pdf_to_images,
                         ocr_all_pages, route_pages, HAS_TESSERACT, HAS_PYMUPDF)
    from PIL import Image as PILImage
    from io import BytesIO as BIO

    print(f"\n{'─'*60}")
    print(f"  SPEED BENCHMARK: {label}")
    print(f"  PDF: {os.path.basename(pdf_path)}")
    print(f"{'─'*60}")

    # ─── NEW approach: always text-layer + always OCR + routing ───
    t0 = time.time()

    page_texts = None
    if HAS_PYMUPDF:
        page_texts = extract_text_per_page(pdf_path)

    b64_images, page_preprocessing = pdf_to_images(pdf_path, page_texts=page_texts)
    blank_pages = {m["page_num"] for m in page_preprocessing if m.get("is_blank")}

    t_text_img = time.time() - t0

    ocr_texts = None
    ocr_confs = None
    t_ocr = 0
    if HAS_TESSERACT:
        t_ocr_start = time.time()
        pil_for_ocr = []
        for i, b in enumerate(b64_images):
            if (i + 1) in blank_pages:
                pil_for_ocr.append(None)
            else:
                pil_for_ocr.append(PILImage.open(BIO(base64.b64decode(b))))
        ocr_texts, ocr_confs = ocr_all_pages(pil_for_ocr)
        t_ocr = time.time() - t_ocr_start

    t_route_start = time.time()
    routing_plan = route_pages(page_texts, ocr_texts, ocr_confs, page_preprocessing)
    t_route = time.time() - t_route_start

    t_new_total = time.time() - t0

    # ─── OLD approach simulation: text-layer check → skip OCR if good ───
    t0_old = time.time()

    page_texts_old = None
    text_layer_usable_old = False
    if HAS_PYMUPDF:
        page_texts_old = extract_text_per_page(pdf_path)
        if page_texts_old:
            text_layer_usable_old, _ = has_meaningful_text(page_texts_old)
            if not text_layer_usable_old:
                page_texts_old = None  # discard

    b64_images_old, pp_old = pdf_to_images(pdf_path, page_texts=page_texts_old)
    blank_old = {m["page_num"] for m in pp_old if m.get("is_blank")}

    t_old_text_img = time.time() - t0_old

    ocr_texts_old = None
    t_old_ocr = 0
    if text_layer_usable_old:
        pass  # OCR SKIPPED — this is the old optimization
    elif HAS_TESSERACT:
        t_old_ocr_start = time.time()
        pil_old = []
        for i, b in enumerate(b64_images_old):
            if (i + 1) in blank_old:
                pil_old.append(None)
            else:
                pil_old.append(PILImage.open(BIO(base64.b64decode(b))))
        ocr_texts_old, _ = ocr_all_pages(pil_old)
        t_old_ocr = time.time() - t_old_ocr_start

    t_old_total = time.time() - t0_old

    # Report
    n = len(routing_plan)
    overhead = t_new_total - t_old_total
    overhead_pct = (overhead / t_old_total * 100) if t_old_total > 0 else 0

    tl_count = sum(1 for r in routing_plan if r["method"] == "text_layer")
    ocr_count = sum(1 for r in routing_plan if r["method"] == "ocr")
    vis_count = sum(1 for r in routing_plan if r["method"] == "vision")

    print(f"\n  TIMING ({n} pages):")
    print(f"    OLD approach (text-layer gate + conditional OCR):")
    print(f"      Text+Images:  {t_old_text_img:6.2f}s")
    print(f"      OCR:          {t_old_ocr:6.2f}s {'(SKIPPED — text layer usable)' if text_layer_usable_old else ''}")
    print(f"      Total:        {t_old_total:6.2f}s")
    print()
    print(f"    NEW approach (always text + always OCR + routing):")
    print(f"      Text+Images:  {t_text_img:6.2f}s")
    print(f"      OCR:          {t_ocr:6.2f}s {'(includes confidence scoring)' if t_ocr > 0 else ''}")
    print(f"      Routing:      {t_route:6.2f}s")
    print(f"      Total:        {t_new_total:6.2f}s")
    print()
    print(f"    OVERHEAD:       {overhead:+6.2f}s ({overhead_pct:+.1f}%)")
    print(f"    Routing:        {tl_count} text_layer, {ocr_count} ocr, {vis_count} vision")

    if text_layer_usable_old and t_ocr > 0:
        print(f"\n  ⚠  Born-digital PDF: old approach skipped OCR entirely ({t_old_ocr:.2f}s).")
        print(f"     New approach adds {t_ocr:.2f}s OCR overhead for per-page routing data.")
        print(f"     Consider: skip OCR when ALL pages have text_chars >= 200?")

    return {
        "old_total": t_old_total,
        "new_total": t_new_total,
        "overhead_s": overhead,
        "overhead_pct": overhead_pct,
        "ocr_time": t_ocr,
        "text_layer_usable_old": text_layer_usable_old,
        "routing": {m: sum(1 for r in routing_plan if r["method"] == m)
                    for m in ("text_layer", "ocr", "vision", "skip_blank")},
    }


# ─── Main ──────────────────────────────────────────────────────────────────

def main():
    test_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                             "data", "test_pdfs")
    os.makedirs(test_dir, exist_ok=True)

    bd_pdf = os.path.join(test_dir, "benchmark_born_digital_10p.pdf")
    sc_pdf = os.path.join(test_dir, "benchmark_scanned_10p.pdf")
    mx_pdf = os.path.join(test_dir, "benchmark_mixed_10p.pdf")

    print("=" * 60)
    print("  T1.2 ROUTING EFFECTIVENESS + SPEED BENCHMARK")
    print("=" * 60)

    # Create test PDFs
    print("\n── Creating test PDFs ──")
    create_born_digital_pdf(bd_pdf, 10)
    create_scanned_pdf(sc_pdf, 10)
    create_mixed_pdf(mx_pdf, 10)

    # Part 1: Routing effectiveness
    print("\n" + "=" * 60)
    print("  PART 1: ROUTING EFFECTIVENESS")
    print("=" * 60)

    measure_routing(bd_pdf, "Born-Digital (10 pages)")
    measure_routing(sc_pdf, "Scanned (10 pages)")
    measure_routing(mx_pdf, "Mixed: 5 digital + 5 scanned (10 pages)")

    # Also test real scanned PDFs if available
    real_pdf = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                            "data", "uploads", "BRNB422004A1303_060062.pdf")
    if os.path.exists(real_pdf):
        measure_routing(real_pdf, "Real Scanned (client document)")

    # Part 2: Speed benchmark
    print("\n" + "=" * 60)
    print("  PART 2: SPEED BENCHMARK (Phase 0 only)")
    print("=" * 60)

    bd_bench = benchmark_phase0(bd_pdf, "Born-Digital (10 pages)")
    sc_bench = benchmark_phase0(sc_pdf, "Scanned (10 pages)")

    # Final summary
    print("\n" + "=" * 60)
    print("  FINAL SUMMARY")
    print("=" * 60)
    print(f"\n  Born-digital 10p: old={bd_bench['old_total']:.2f}s, new={bd_bench['new_total']:.2f}s, "
          f"overhead={bd_bench['overhead_s']:+.2f}s ({bd_bench['overhead_pct']:+.1f}%)")
    print(f"  Scanned 10p:      old={sc_bench['old_total']:.2f}s, new={sc_bench['new_total']:.2f}s, "
          f"overhead={sc_bench['overhead_s']:+.2f}s ({sc_bench['overhead_pct']:+.1f}%)")

    if bd_bench["overhead_s"] > 2.0:
        print(f"\n  ⚠  RECOMMENDATION: Add fast-path — if text-layer covers ≥90% of pages,")
        print(f"     skip OCR entirely (same as old behavior) to avoid {bd_bench['ocr_time']:.1f}s overhead.")

    # Cleanup
    for f in [bd_pdf, sc_pdf, mx_pdf]:
        if os.path.exists(f):
            os.remove(f)
    try:
        os.rmdir(test_dir)
    except OSError:
        pass


if __name__ == "__main__":
    main()
