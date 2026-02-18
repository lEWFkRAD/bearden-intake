# Bearden Document Intake — Bugs & Roadmap

> Maintained by Claude + Jeffrey. Updated each session.
> Last updated: 2026-02-18

---

## Active Bugs / Fixes

### P0 — Critical (blocks real use)

| # | Title | Description | Status | Found | Fixed |
|---|-------|-------------|--------|-------|-------|
| B1 | Review chain / Inbox missing | Inbox tab for reviewer chain (Jeff → Susan → Charles) may be gone in latest build. Need login-per-user, assigned queues, stage transitions after Confirm/Edit/Approve. | **done** | 2/18 | 2/18 |
| B2 | Review UI overwhelming | Values shown as big list instead of guided one-at-a-time review. Guided review must be default: one field, big label, highlighted evidence in viewer pane. List view secondary. | **done** | 2/18 | 2/18 |
| B3 | "Number lives once" audit | Verify exports pull from DB facts only — no codepaths reading raw OCR/vision at export time. Verification JSON is cache/UI state; DB is source of truth. Regen Excel must use DB facts, not raw extraction. | open | 2/18 | — |
| B10 | Post-run confidence audit | After Finish Review, sample N meaningful verified fields for lightweight QA check. Zero-inflation aware (exclude $0/null/placeholder fields). Confirm or Flag per sample. Store audit results. Badge on job summary. | **done** | 2/18 | 2/18 |

### P1 — Important (UX / workflow)

| # | Title | Description | Status | Found | Fixed |
|---|-------|-------------|--------|-------|-------|
| B4 | Remove regen Excel buttons from review header | Buttons are redundant/confusing. Replace with "Finish Review" (→ report selection) and "Generate Reports" on client page. | **partial** | 2/18 | 2/18 |
| B5 | Output format selection redesign | Output format should NOT be on upload page. Move to end-of-review (checkboxes) and client page. Only show implemented report types (or mark "coming soon"). | **done** | 2/18 | 2/18 |
| B6 | AI instruction box clutters upload | Move to collapsible "Advanced Options" section, default collapsed. | **done** | 2/18 | 2/18 |
| B7 | Document type selection usefulness | Audit whether doc type actually changes extraction routing/prompts. If not: rename to "Document category (for filing)", make optional, default to Auto-detect. If yes: keep but default auto-detect with override. | open | 2/18 | — |
| B8 | Client document filing + discoverability | Uploaded docs need predictable storage structure and UI actions: copy file path, open folder (macOS), export client folder as zip. Structure: `clients/<id>/source/`, `clients/<id>/context/`, `clients/<id>/outputs/`. | **done** | 2/18 | 2/18 |

### P2 — Nice to have

| # | Title | Description | Status | Found | Fixed |
|---|-------|-------------|--------|-------|-------|
| B9 | Time tracking for Susan | Add time log module (project, task, start/stop timer, notes, CSV export) or at minimum track time-in-review per job/user with admin dashboard view. | open | 2/18 | — |

---

## Completed Fixes

| # | Title | Fix Summary | Date |
|---|-------|-------------|------|
| — | — | — | — |

---

## Acceptance Criteria Reference

### B1 — Review Chain / Inbox
- Jeff verifies a value → appears in Susan's inbox
- Susan approves/overrides → appears in Charles's inbox
- Charles finalizes → item disappears / marks final

### B2 — Guided Review (Default)
- Opening Review lands in Guided mode by default
- Grid/list exists as optional toggle ("List View")
- Back/Previous navigation works
- Note field available in guided mode
- Reviewer identity uses logged-in user (not initials text input)
- Evidence: auto-zoom to bbox if present; OCR word box search fallback; full page + "location uncertain" banner as last resort
- Confirm / Edit / Not Present / Skip advances to next
- "Finish Review" button closes the loop → report selection → post-run audit

### B3 — Number Lives Once
- After manual edit, export reflects edit even if extraction output differs
- No export touches OCR text or vision results directly

### B4 — Remove Regen Excel Buttons
- No excel regen buttons visible on review page
- Reports generated from client page or end-of-review flow

### B5 — Output Format Redesign
- No output format dropdown on upload
- Reports selectable at end-of-review and on client page
- No broken/unimplemented report options exposed

### B6 — AI Instruction Box
- Upload flow is clean by default
- Advanced options still accessible via expand

### B7 — Doc Type Selection
- No forced decisions that don't matter
- Selection either truly changes behavior OR clearly labeled "for filing only"

### B8 — Client Document Filing
- Source docs always findable from within UI
- Can export a client folder as clean zip

### B9 — Time Tracking
- Susan can see hours spent by week/user/job
- Exportable to CSV

### B10 — Post-Run Confidence Audit
- Sampling population: fields where value != 0 OR status in {confirmed, edited} OR evidence_strength != weak
- Exclude placeholders, defaults, auto-filled $0 unless explicitly confirmed/edited
- Sample size: N≤20→3, 21-50→5, 51-120→8, 121-250→10, 250+→12 (cap)
- Target: 60-90 seconds audit time
- UI: same guided one-at-a-time flow, evidence highlight required
- Buttons: Confirm / Flag
- Flag routes item back into review queue as "needs_review (audit)"
- Store: post_run_audit {sample_size, pass_count, fail_count, selected_field_keys, timestamps, reviewer}
- Badge on job/client summary: "Audit Passed: 8/8" or "Audit Flagged: 2 issues"

---

## Living Roadmap

> This is not "someday fantasy." This is ordered by what makes the software
> more trustworthy and deployable fastest.

### Phase 0 — Stabilization (You Are Here)

**Goal:** Make current version dependable and smooth.

#### A. Extraction Quality
| Task | Status | Notes |
|------|--------|-------|
| Improve multi-pass extraction confidence scoring | not started | |
| Tune regex + label anchors | not started | |
| Expand fixture set (1099s, K-1s, mortgage, brokerage, W-2s) | not started | |
| Improve OCR contrast / deskew thresholds | not started | Preprocessing constants exist |

#### B. Verification UX (Build Order — current sprint)
| # | Task | Status | Notes |
|---|------|--------|-------|
| 1 | Guided review default + Back + Notes + logged-in reviewer | **done 2/18** | B2 — /api/me, history stack, note field |
| 2 | Add "Finish Review" step (end state + format selection) | **done 2/18** | B4/B5 — report checkboxes at end of review |
| 3 | Post-run audit sampling (after Finish Review) | **done 2/18** | B10 — zero-inflation aware, Confirm/Flag |
| 4 | Remove upload format selection + stop auto-regen | **done 2/18** | B5/B6 — AI instructions collapsed, regen on finish only |
| 5 | Client docs polish (copy path + zip + file listing API) | **done 2/18** | B8 — /files, /export-zip, copy path button |

#### C. Client Document Management
| Task | Status | Notes |
|------|--------|-------|
| Store uploads under `/clients/<id>/docs/<year>/` | not started | B8 |
| Copy file path button | not started | |
| Export full client folder zip | not started | |
| Clean file naming scheme | not started | |

#### D. Report Generation Cleanup
| Task | Status | Notes |
|------|--------|-------|
| Remove broken report generator dropdowns (B5) | not started | |
| Move output selection to end of workflow | not started | |
| Support multi-select report types | not started | |

### Phase 1 — Throughput & Trust

#### A. Performance
| Task | Status | Notes |
|------|--------|-------|
| Page-level caching | not started | |
| Batch extraction tuning | not started | |
| Parallel OCR limits | not started | Thread pool exists (8 workers) |
| Lazy vision verification | not started | Smart skip partially done |

#### B. Audit & Controls
| Task | Status | Notes |
|------|--------|-------|
| Audit log of audit logs | partial | app_events table exists |
| Partner-only delete permissions | not started | Role system exists |
| Event hashing / tamper-evident flags | not started | |
| Export audit trail PDF | not started | |

#### C. Review Chain
| Task | Status | Notes |
|------|--------|-------|
| Preparer → Reviewer → Partner flow (B1) | **done 2/18** | P0 — 5-stage chain, inbox, auth gates, stage badges |
| Stage rollback + undo (send-back) | **done 2/18** | Reviewer/Partner can send back with reason |
| Partner override authority | **done 2/18** | Admin role can act at any stage |

### Phase 2 — Office-Ready Deployment

#### A. Multi-User Stability
| Task | Status | Notes |
|------|--------|-------|
| Row-level locks | partial | review_locks table exists |
| Job queue prioritization | not started | |
| Conflict resolution UI | not started | |
| Session timeout / PIN rotation | partial | PIN auth + session timeout exist |

#### B. Admin Dashboard Expansion
| Task | Status | Notes |
|------|--------|-------|
| Disk usage alerts | not started | Health endpoint has disk info |
| Job failure analytics | not started | |
| User time tracking (B9) | not started | Susan request |
| Average review time metrics | not started | |
| "Time to first value" metrics | not started | |

#### C. Backups
| Task | Status | Notes |
|------|--------|-------|
| Nightly DB snapshot | not started | SQLite = one file copy |
| One-click restore | not started | |
| Backup verification test | not started | |

### Phase 3 — Deterministic Excel Intelligence

> This is where we become unique.

#### A. Workpaper Engine
| Task | Status | Notes |
|------|--------|-------|
| Pull only from canonical fact table | not started | B3 enforcement |
| Preserve formulas | not started | |
| Input vs calculation separation | not started | |
| Template registry | not started | workpaper_export.py exists |

#### B. Firm-Specific Templates
| Task | Status | Notes |
|------|--------|-------|
| Upload template → map fields → save | not started | |
| Versioned templates | not started | |
| Comparison view | not started | |

### Phase 4 — Learning Appendage (Separate Module)

> Not baked into core. This is an extension brain.

#### A. Historical Intake
| Task | Status | Notes |
|------|--------|-------|
| Feed past years' documents | not started | |
| Compare extracted → actual outputs | not started | |

#### B. Trend & Pattern Learning
| Task | Status | Notes |
|------|--------|-------|
| Output format recognition | not started | |
| Confidence scoring from history | not started | |
| Variance detection | not started | Prior-year variance exists |

#### C. Guardrails
| Task | Status | Notes |
|------|--------|-------|
| Never auto-change facts | enforced | Creed rule |
| Only suggest template improvements | not started | |

### Phase 5 — Optional Enhancements (Not Required for Success)

| Task | Status | Notes |
|------|--------|-------|
| Journal entry automation | not started | Tax docs excluded per rules |
| Bank reconciliation assist | not started | |
| Foreign language extraction tuning | not started | |
| Large enterprise connectors | not started | |

---

## Architecture Evolution (V2 scaling prep — not urgent, do when natural)

These aren't bugs. The codebase works. This is the plan for when files get unwieldy.

### 1. `services/` or `modules/` folder
Extract big logic chunks out of monolithic top-level files when they grow past comfort.
```
services/
  extraction_service.py
  verification_service.py
  review_service.py
```
**Trigger:** when app.py or extract.py changes start causing merge conflicts with themselves.

### 2. `migrations/` for DB schema
When schema changes start happening often, versioned SQL files prevent "did I already run that ALTER?"
```
migrations/
  001_init.sql
  002_add_review_state.sql
  003_add_time_tracking.sql
```
**Trigger:** next time we add a table or column to `_init_db()`.

### 3. `logs/` directory
Dedicated log folder instead of mixing into `data/`. Makes admin dashboards and backup scripts cleaner.
**Trigger:** when we add structured logging or admin log viewer.

### 4. `backups/`
Nightly DB snapshots. SQLite makes this trivial — it's one file copy.
**Trigger:** when the system goes into daily production use.

---

## Foundation Note

> **The most important thing we built: `fact_store.py` + SQLite as single file.**
>
> This gives us: single source of truth, transactional ledger, portable DB,
> easy backup (one file), no external infrastructure, and multi-user capability.
> Everything else — review chain, exports, workpapers — reads from this.

---

## The Bearden Platform Creed

> *Read this when tired or tempted to over-engineer.*

### The Motto

**"Verify once. Store once. Use forever."**

Or the longer form:

> "Put one document in.
> Get one clean, trusted output out.
> With proof."

### A Number Is Born Once. Confirmed Once. Used Many Times.

### Core Principles

**1. Human Judgment Is Supreme**
- The system never overrides a verified fact.
- AI assists; humans decide.

**2. The Database Is Truth**
- Deliverables pull from the ledger.
- OCR and vision are tools, not sources of record.

**3. Transparency Over Cleverness**
- Every number shows its origin.
- Every action is logged.
- Every correction is traceable.

**4. Determinism Over Guessing**
- Outputs are rule-based.
- Calculations are reproducible.
- Excel formulas are preserved, not replaced.

**5. Ease Over Flash**
- One number at a time.
- Highlight the source.
- Reduce cognitive load.

**6. Guardrails Before Features**
- Audit logs before automation.
- Permissions before speed.
- Stability before expansion.

**7. Modular Growth**
- Extraction, Verification, Ledger, Deliverables, Learning.
- No appendage compromises the core.

**8. Serve the Workflow, Not Replace It**
- The goal is not to remove accountants.
- The goal is to remove repetitive typing and uncertainty.

### The Test

> Every time we add something, ask:
>
> **Does this make verification clearer, data more reliable, or outputs more deterministic?**
>
> If yes → it belongs.
> If no → it's noise.
