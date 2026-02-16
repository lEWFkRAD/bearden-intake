# Bearden Document Intake Platform

## What This Is
Document intake and extraction platform for Bearden Accounting Firm (Rome, Georgia). Scanned PDFs go in, structured Excel worksheets come out. The operator is Jeffrey Watts — comfortable following technical instructions, not writing code daily. Runs locally on his macOS workstation.

**This is a data entry aid.** It does not prepare returns, calculate anything, or file documents. All output goes through the firm's three-person review chain (Jeffrey → Susan → Charles) before use.

## Versions and File Sizes
- `extract.py` — Document Intake Extractor **v6** (~4,140 lines)
- `app.py` — Bearden Document Intake Platform **v5** (~3,543 lines)
- `test_accounting.py` — Test suite (596 lines, 68 tests)

## Tech Stack
Python 3, Anthropic Claude API (`claude-sonnet-4-20250514`), Tesseract OCR, poppler/pdf2image, openpyxl, Flask, macOS localhost

## Project Structure
```
├── app.py                 # Flask dashboard
├── extract.py             # Core extraction engine (CLI)
├── launch.sh              # macOS launcher (PORT=5050)
├── requirements.txt       # Python dependencies
├── tests/
│   ├── test_accounting.py # 68 tests
│   └── test_pii_tokenizer.py
├── data/                  # Runtime data (gitignored)
│   ├── uploads/           # Uploaded PDFs
│   ├── outputs/           # Excel + JSON output
│   ├── page_images/       # Rendered page JPEGs for review
│   ├── jobs_history.json  # Persisted job state
│   └── vendor_categories.json  # Vendor → category memory (grows over time)
├── clients/               # Client data: context, instructions, outputs (gitignored)
└── verifications/         # Review decisions (gitignored)
```

## Extraction Pipeline (v6)
```
PDF → Images (250 DPI)
  → Phase 0: Parallel OCR (Tesseract, threaded)
  → Phase 1: Classify pages via Claude vision
  → Phase 1.5: Group pages by EIN/entity
  → Phase 2: Extract fields
      Multi-page groups (K-1 + continuations): batched multi-image call
      Single pages: OCR-first (cheap text call) → vision fallback
      [checkpoint saved]
  → Phase 3: Verify critical fields (skips OCR-accepted + multipage-verified)
      [checkpoint saved]
  → Phase 4: Normalize (split brokerage composites, K-1 cross-ref)
  → Phase 5: Validate (arithmetic + cross-doc duplicates + prior-year variance)
  → Phase 6: Excel output + JSON audit log (with cost data)
```

### v6 Improvements (extraction engine)
1. **OCR-first extraction**: Tesseract text → Claude text API (cheap). Vision only when OCR quality is poor. ~90% cost reduction on readable pages.
2. **Multi-page batching**: K-1s with continuations sent as single multi-image API call. Claude resolves "STMT" cross-references in one pass.
3. **Parallel OCR**: Tesseract runs across pages concurrently (ThreadPoolExecutor).
4. **Checkpointing**: Partial results saved after classify/extract/verify. `--resume` flag recovers from crashes.
5. **Cost tracking**: CostTracker records every API call's token usage. Cost logged in JSON and shown in dashboard.
6. **Smart verification skip**: OCR-text pages with all-high confidence skip the vision verification pass. Multi-page extractions skip if no critical issues.
7. **Duplicate document detection**: Flags when same payer + same doc type + same amounts appear twice (scanned copy).
8. **Prior-year variance flagging**: Compares extracted values against prior-year context. Flags >50% changes.
9. **Smart dedup**: Keeps higher-confidence copy when duplicates exist. Merges unique fields from discarded copy.

## Dashboard Sections
1. **Upload** — Drop PDF, select doc type / output format, start extraction
2. **Review** — Side-by-side PDF + fields, confirm/correct/flag, category dropdowns
3. **Clients** — Context (prior-year docs), Instructions (persistent rules), Completeness
4. **Batch Categorize** — Uncategorized transactions grouped by vendor, bulk assign
5. **History** — All jobs with cost display, filter/search, retry, delete

## CLI Reference
```bash
python3 extract.py document.pdf --year 2025 --output out.xlsx \
  --doc-type bank_statements --output-format journal_entries \
  --context-file clients/Name/context/index.json \
  --resume --no-ocr-first
```

## Critical Rules
1. Never change extract.py stdout format without updating app.py progress matching
2. Never remove `_operator_category` from field flow
3. Tax documents never generate journal entries
4. Every journal entry must balance (DR = CR)
5. Vendor memory file grows over time — never reset it
6. Client instructions are injected into AI prompts
7. Context parsing is OCR + pattern matching, not LLM
8. Checkpoints are auto-deleted on successful completion

## Running Tests
```bash
python3 tests/test_accounting.py  # 68 tests, all should pass
```
