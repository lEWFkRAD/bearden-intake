# Bearden Document Intake Platform — Feature Checklist

## Upload Section
- [x] PDF file upload with drag-and-drop
- [x] Client selector dropdown (select existing client or create new)
- [x] "+ New" client button (opens modal with name, EIN, contact, notes)
- [x] Quick link to upload PY docs / manage instructions (below client name)
- [x] Tax year selector
- [x] Document type pills (tax_returns, bank_statements, trust_documents, bookkeeping, payroll, other)
- [x] Output format pills (tax_review, journal_entries, account_balances, trial_balance, transaction_register)
- [x] AI Instructions textarea (visible, not hidden — tell the AI how to handle this document)
- [x] Auto-match filename to existing client in dropdown
- [x] Client selection required before extraction
- [x] Advanced Options accordion:
  - [x] User notes textarea
  - [x] Skip verification checkbox
  - [x] Disable PII tokenization checkbox
  - [x] Use OCR-first mode checkbox (lower cost, less accurate)

## Processing Section
- [x] Progress bar with percentage
- [x] Stage label (scanning, classifying, extracting, verifying, etc.)
- [x] Console output (last 30 lines, auto-scroll)
- [x] Cancel button (terminates extraction subprocess)
- [x] Auto-transition to Review on completion
- [x] Cost display on completion toast

## Review Section
- [x] Side-by-side PDF viewer + field panel
- [x] Page navigation (Prev / Next buttons) — navigates ALL PDF pages, not just extracted ones
- [x] Page indicator (1 / N) — shows actual PDF page count
- [x] Empty page state (no-data pages show message + re-extract button)
- [x] Re-extract page button (sends custom instructions to Claude vision API)
- [x] Download Excel button
- [x] Download JSON log button
- [x] Regenerate Excel button (manual trigger for re-generating with corrections)
- [x] AI Chat panel ("Ask AI" button — chat about current page with Claude vision)
- [x] Keyboard shortcuts overlay (? key)
- [x] Verification progress bar
- [x] Verification stats (confirmed/corrected/flagged counts)
- [x] Client instructions banner (shown if client has extraction rules)
- [x] Reviewer initials input (sidebar)
- [x] Field ordering by box/line number (W-2 wages first, etc.)
- [x] Box/line number labels (e.g. "Box 1 — Wages", "Line 4c — Guaranteed Payments")
- [x] Zero value filtering ($0.00 fields hidden unless balance/total/net)

### Field Verification
- [x] Confirm field (green checkmark button / Enter key)
- [x] Flag field (flag button / F key)
- [x] Edit value (edit button / double-click / E key — inline text input, won't get destroyed by focus events)
- [x] Add note (pencil button / N key — inline text input)
- [x] Confidence dots (green=confirmed, yellow=corrected, orange=low, gray=other)
- [x] Corrected value display (strikethrough original + arrow + new value)
- [x] Notes display (below field when present)
- [x] Category dropdown (for transactions, checks, invoices)
- [x] Category auto-suggestion from vendor memory

### Keyboard Shortcuts
- [x] Enter — Confirm focused field
- [x] F — Flag focused field
- [x] N — Toggle note input
- [x] E — Edit focused field value
- [x] Up/Shift+Tab — Previous field
- [x] Down/Tab — Next field
- [x] Left Arrow — Previous page
- [x] Right Arrow — Next page
- [x] ? — Show/hide keyboard help

### Transaction Table
- [x] Transaction rows with date, description, amount, type
- [x] Per-transaction category dropdown
- [x] Per-transaction confirm button
- [x] Confidence dots on amounts

## Client Manager
### Client List
- [x] Client search/filter
- [x] Client cards with badges (EIN, Context, Instructions)
- [x] Click to open client detail
- [x] New Client modal (name, EIN last 4, contact, notes)

### Client Detail
- [x] Client metadata display (EIN badge, contact, notes)
- [x] Documents tab (default) — shows all extraction jobs grouped by doc type
- [x] Per-document actions: Review, Download Excel, Download JSON
- [x] Generate Report button — combine multiple extractions into one Excel

### Documents Tab
- [x] Document count summary
- [x] Documents grouped by type (Tax Returns, Bank Statements, etc.)
- [x] Status badges (complete, running, failed)
- [x] Cost display per document
- [x] Review button (opens review for that job)
- [x] Excel download link
- [x] JSON log download link

### Generate Report Modal
- [x] Select jobs with checkboxes
- [x] Output format selector (tax_review, journal_entries, etc.)
- [x] Year input
- [x] Generates combined Excel from multiple extraction jobs
- [x] Auto-downloads generated report

### Prior-Year Context Tab
- [x] Upload context documents (PDF, Excel, CSV, TXT)
- [x] Context year and label inputs
- [x] Context document list with delete
- [x] Context data parsed via OCR + pattern matching (not LLM)
- [x] Context injected into extraction prompts as prior-year reference

### Instructions Tab
- [x] Add extraction instruction (textarea + button)
- [x] Instructions list with delete
- [x] Instructions injected into AI prompts during extraction

### Completeness Tab
- [x] Missing payers (expected from prior year, not yet received)
- [x] Matched payers (received this year, matched to prior year)
- [x] New payers (received this year, not in prior year)

## Batch Categorize (hidden — kept for future use)
- [x] Client filter
- [x] Vendor search
- [x] Show categorized toggle
- [x] Stats cards (total, categorized, uncategorized)
- [x] Vendor groups (collapsible, sorted by uncategorized first)
- [x] Category dropdown per vendor group
- [x] Apply button per vendor group (learns vendor → category mapping)
- [x] Transaction detail table per vendor
- [x] Auto-suggest badges

## History
- [x] Job list table with all jobs
- [x] Search by client/filename
- [x] Status filter dropdown
- [x] Status badges (complete, running, failed, interrupted, error)
- [x] Document type badges
- [x] Cost display ($X.XXXX for complete jobs, — otherwise)
- [x] Date/time display
- [x] Review button (complete jobs)
- [x] Retry button (failed, interrupted, error jobs)
- [x] Delete button (all jobs, with confirmation)

## Architecture & Security
- [x] Unique file naming — uploads saved as `<job_id>.pdf` (no filename collisions)
- [x] File permissions — 0o600 (owner-only) on all sensitive files (PDFs, Excel, JSON, DB)
- [x] SQLite database — jobs, verifications, vendor categories persisted to `data/bearden.db`
- [x] Auto-migration from JSON files — `jobs_history.json`, `verifications/*.json`, `vendor_categories.json`
- [x] Legacy files renamed to `.migrated` (not deleted) for safe rollback
- [x] WAL journal mode for concurrent read performance
- [x] Health check endpoint (`GET /api/health`) — version, uptime, dependencies, disk usage
- [x] Friendly download names — Excel/JSON downloads use original filename, not job ID
- [x] Confirm/flag toggle — clicking confirmed/flagged fields un-confirms/un-flags them

## Extraction Engine (extract.py)
- [x] PDF → image conversion (250 DPI via poppler)
- [x] Auto-rotation (Tesseract OSD)
- [x] Parallel OCR (Tesseract, ThreadPoolExecutor)
- [x] Page classification via Claude vision
- [x] EIN/entity grouping (Phase 1.5)
- [x] OCR-first extraction (cheap text API, ~90% cost reduction)
- [x] Vision extraction fallback (when OCR quality is poor)
- [x] Multi-page batching (K-1 + continuations in single API call)
- [x] Checkpointing (save/load/clear after classify/extract/verify)
- [x] --resume flag for crash recovery
- [x] Smart verification skip (OCR-accepted + multipage-verified)
- [x] Duplicate document detection (same payer + type + amounts)
- [x] Smart dedup (keep higher-confidence copy, merge unique fields)
- [x] Prior-year variance flagging (>50% change from context)
- [x] PII tokenization (regex-based SSN/EIN masking)
- [x] Cost tracking (CostTracker records every API call)
- [x] Canonical field naming (wages, federal_wh, etc.)
- [x] Excel output with color-coded confidence
- [x] JSON audit log with cost data
- [x] Brokerage composite splitting (Phase 4)
- [x] K-1 cross-reference resolution
- [x] Arithmetic validation
- [x] Cross-document duplicate validation

## API Coverage
- [x] POST /api/upload — Start extraction (requires client selection)
- [x] GET /api/status/<job_id> — Poll progress
- [x] GET /api/results/<job_id> — Fetch extraction data
- [x] GET /api/page-image/<job_id>/<page> — Serve page image
- [x] POST /api/reextract-page/<job_id>/<page> — Re-extract single page
- [x] GET /api/verify/<job_id> — Fetch verifications
- [x] POST /api/verify/<job_id> — Save field verification
- [x] GET /api/vendor-categories — Fetch vendor memory
- [x] GET /api/download/<job_id> — Download Excel
- [x] GET /api/download-log/<job_id> — Download JSON log
- [x] POST /api/regen-excel/<job_id> — Regenerate Excel
- [x] GET /api/jobs — List all jobs
- [x] POST /api/delete/<job_id> — Delete job
- [x] POST /api/retry/<job_id> — Retry failed job
- [x] POST /api/cancel/<job_id> — Cancel running job
- [x] POST /api/ai-chat/<job_id> — Chat with AI about current page/extraction
- [x] GET /api/clients — List clients (includes EIN, contact, notes)
- [x] POST /api/clients/create — Create new client
- [x] GET /api/clients/<client>/info — Get client metadata
- [x] PUT /api/clients/<client>/info — Update client metadata
- [x] GET /api/clients/<client>/documents — List client's extraction jobs
- [x] POST /api/clients/<client>/generate-report — Generate combined Excel report
- [x] GET /api/download-report/<report_id> — Download generated report
- [x] GET /api/context/<client>/ — Get context index
- [x] POST /api/context/<client>/upload — Upload context doc
- [x] DELETE /api/context/<client>/<doc_id> — Delete context doc
- [x] GET /api/context/<client>/completeness — Completeness report
- [x] GET /api/instructions/<client> — Fetch instructions
- [x] POST /api/instructions/<client> — Add instruction
- [x] DELETE /api/instructions/<client>/<rule_id> — Delete instruction
- [x] GET /api/health — System health check (version, uptime, deps, disk)
- [x] GET /api/batch-categories — Fetch batch data
- [x] POST /api/batch-categories/apply — Apply batch category
