#!/usr/bin/env python3
"""
Document Intake Extractor v6
=================================
OCR-first architecture: Every page gets free Tesseract OCR first. If the OCR
text is readable (200+ chars, good quality), Claude extracts from text (cheap).
If OCR is poor or Claude flags ambiguous fields, falls back to vision (expensive).
Classification always uses vision (layout matters for form identification).

New in v6:
  - OCR-first extraction (~60-90% cost reduction on readable pages)
  - Cost tracking (token usage + estimated $ per run, in JSON log)
  - Checkpointing (crash recovery — resume from last completed phase)
  - Smart dedup (keeps highest-confidence copy when duplicates exist)
  - --resume flag for crash recovery
  - --no-ocr-first flag to force vision-only mode

Pipeline:
  0. PDF → images (250 DPI)
  0.5. Tesseract OCR every page (free, local)
  1. Claude vision classifies each page
  1.5. Group pages by EIN/entity
  2. Extract fields:
       OCR text good → Claude text call (cheap)
       OCR partial → text call + flag ambiguous fields for verification
       OCR poor → Claude vision call (expensive)
  3. Verify critical fields (vision cross-check)
  4. Normalize (split brokerage composites, cross-ref K-1 continuations)
  5. Validate (arithmetic checks, cross-document reconciliation)
  6. Excel output + JSON audit log (with cost data)

Usage:
    python3 extract.py <pdf_file> [--year 2024] [--output output.xlsx]
    python3 extract.py <pdf_file> --resume   # resume after crash
    python3 extract.py <pdf_file> --no-ocr-first  # force vision-only

Requirements:
    pip install anthropic pdf2image openpyxl Pillow pytesseract
    Also: brew install poppler tesseract (Mac) / sudo apt install poppler-utils tesseract-ocr (Linux)

Setup:
    export ANTHROPIC_API_KEY=your_key_here
"""

import argparse
import base64
import json
import os
import sys
import re
from pathlib import Path
from io import BytesIO
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

try:
    import anthropic
except ImportError:
    sys.exit("ERROR: pip install anthropic")
try:
    from pdf2image import convert_from_path
except ImportError:
    sys.exit("ERROR: pip install pdf2image  (also install poppler)")
try:
    import openpyxl
    from openpyxl.styles import Font, PatternFill, Alignment
    from openpyxl.comments import Comment
except ImportError:
    sys.exit("ERROR: pip install openpyxl")
try:
    import pytesseract
    from PIL import Image, ImageDraw
    HAS_TESSERACT = True
except ImportError:
    HAS_TESSERACT = False
    print("WARNING: pytesseract not installed. Will use vision-only mode (slower, more expensive).")
    print("  Install: pip install pytesseract && brew install tesseract")

# ─── CONFIGURATION ───────────────────────────────────────────────────────────

MODEL = "claude-sonnet-4-20250514"
MAX_TOKENS = 8000
DPI = 250
MAX_CONCURRENT = 4  # Parallel API calls (Anthropic rate limit friendly)

# Minimum OCR character count to consider OCR usable
OCR_MIN_CHARS = 100

# Fields where errors matter most — always verify these against the image
CRITICAL_FIELDS = {
    "wages", "federal_wh", "state_wh",
    "ordinary_dividends", "qualified_dividends",
    "interest_income", "us_savings_bonds_and_treasury",
    "div_ordinary_dividends", "div_qualified_dividends",
    "int_interest_income", "int_us_savings_bonds_and_treasury",
    "b_total_gain_loss", "b_short_term_gain_loss", "b_long_term_gain_loss",
    "short_term_gain_loss", "long_term_gain_loss", "total_gain_loss",
    "box1_ordinary_income", "box2_rental_real_estate",
    "gross_distribution", "taxable_amount", "net_benefits",
    "nonemployee_compensation", "mortgage_interest",
    # Bookkeeping critical fields
    "beginning_balance", "ending_balance", "total_deposits", "total_withdrawals",
    "gross_pay", "net_pay", "total_amount",
}

REQUIRES_HUMAN_REVIEW = [
    "K-1 carryover from prior year (basis/at-risk limitations)",
    "K-1 allowed loss vs. suspended loss allocation",
    "K-1 carryover to next year",
    "Capital loss carryover from prior year",
    "Net operating loss carryover",
    "Section 179 carryover",
    "Passive activity loss carryover",
    "Depreciation schedules (continuing assets)",
    "Estimated tax payments (not on source documents)",
    "Prior year state refund (taxability depends on PY itemizing)",
    "At-risk basis calculations",
    "Qualified business income (QBI) carryover",
    "Charitable contribution carryover (if exceeded AGI limit PY)",
    "Installment sale deferred gain tracking",
]


# ─── COST TRACKER ────────────────────────────────────────────────────────────

class CostTracker:
    """Track API token usage and estimate costs per run.

    Pricing (Sonnet, as of 2025):
      Input:  $3 / MTok
      Output: $15 / MTok
      Image:  ~1,600 tokens per image at 250 DPI
    """
    INPUT_COST_PER_MTOK = 3.0
    OUTPUT_COST_PER_MTOK = 15.0

    def __init__(self):
        self.calls = []  # list of {phase, page, input_tokens, output_tokens, call_type}
        self.text_calls = 0
        self.vision_calls = 0

    def record(self, phase, page, usage, call_type="vision"):
        """Record a single API call's usage."""
        inp = getattr(usage, 'input_tokens', 0) if usage else 0
        out = getattr(usage, 'output_tokens', 0) if usage else 0
        self.calls.append({
            "phase": phase, "page": page,
            "input_tokens": inp, "output_tokens": out,
            "call_type": call_type,
        })
        if call_type == "text":
            self.text_calls += 1
        else:
            self.vision_calls += 1  # vision, vision_multi both count as vision

    def total_input(self):
        return sum(c["input_tokens"] for c in self.calls)

    def total_output(self):
        return sum(c["output_tokens"] for c in self.calls)

    def total_cost(self):
        inp = self.total_input() / 1_000_000
        out = self.total_output() / 1_000_000
        return inp * self.INPUT_COST_PER_MTOK + out * self.OUTPUT_COST_PER_MTOK

    def summary(self):
        if not self.calls:
            return "  No API calls made"
        lines = [
            f"  API calls:    {len(self.calls)} ({self.vision_calls} vision, {self.text_calls} text)",
            f"  Input tokens: {self.total_input():,}",
            f"  Output tokens:{self.total_output():,}",
            f"  Est. cost:    ${self.total_cost():.4f}",
        ]
        # Per-phase breakdown
        phases = {}
        for c in self.calls:
            p = c["phase"]
            if p not in phases:
                phases[p] = {"count": 0, "input": 0, "output": 0}
            phases[p]["count"] += 1
            phases[p]["input"] += c["input_tokens"]
            phases[p]["output"] += c["output_tokens"]
        for p, d in sorted(phases.items()):
            cost = (d["input"] / 1e6 * self.INPUT_COST_PER_MTOK +
                    d["output"] / 1e6 * self.OUTPUT_COST_PER_MTOK)
            lines.append(f"    {p}: {d['count']} calls, ${cost:.4f}")
        return "\n".join(lines)

    def to_dict(self):
        return {
            "total_calls": len(self.calls),
            "vision_calls": self.vision_calls,
            "text_calls": self.text_calls,
            "total_input_tokens": self.total_input(),
            "total_output_tokens": self.total_output(),
            "estimated_cost_usd": round(self.total_cost(), 4),
            "per_phase": {p: sum(1 for c in self.calls if c["phase"] == p)
                          for p in set(c["phase"] for c in self.calls)},
        }

# Global tracker — set per run in main()
_cost_tracker = None

# ─── DOC TYPE → CLASSIFICATION NARROWING ──────────────────────────────────────

DOC_TYPE_CLASSIFICATIONS = {
    "tax_returns": [
        "W-2", "W-2G",
        "1099-INT", "1099-DIV", "1099-B", "1099-R", "1099-NEC", "1099-MISC",
        "1099-SA", "1099-G", "1099-K", "1099-S", "1099-C", "1099-Q", "1099-LTC",
        "SSA-1099", "K-1", "1098", "1098-T", "1098-E",
        "5498", "5498-SA", "1095-A", "1095-B", "1095-C",
        "brokerage_composite", "continuation_statement",
        "property_tax_bill", "charitable_receipt", "estimated_tax_record",
        "farm_income_document", "rental_income_document",
        "schedule_c_summary", "other",
    ],
    "bank_statements": [
        "bank_statement", "bank_statement_deposit_slip", "check", "check_stub",
        "chart_of_accounts", "trial_balance_document", "credit_card_statement", "other",
    ],
    "trust_documents": [
        "K-1", "1099-INT", "1099-DIV", "1099-R", "1099-B",
        "brokerage_composite", "continuation_statement",
        "bank_statement", "trust_accounting_statement", "other",
    ],
    "bookkeeping": [
        "bank_statement", "bank_statement_deposit_slip", "check", "check_stub",
        "credit_card_statement", "invoice", "receipt",
        "profit_loss_statement", "balance_sheet", "aged_receivables", "aged_payables",
        "chart_of_accounts", "trial_balance_document",
        "loan_statement", "mortgage_statement", "other",
    ],
    "payroll": [
        "W-2", "W-3", "check_stub",
        "940", "941", "943", "944", "945",
        "payroll_register", "payroll_summary", "other",
    ],
    "other": [
        "W-2", "1099-INT", "1099-DIV", "1099-B", "1099-R", "1099-NEC", "1099-MISC",
        "1099-K", "1099-S", "1099-C", "1099-G", "1099-SA",
        "K-1", "1098", "bank_statement", "invoice", "receipt", "check",
        "loan_statement", "mortgage_statement", "profit_loss_statement", "balance_sheet",
        "other",
    ],
}

# ─── PII TOKENIZER ───────────────────────────────────────────────────────────

class PIITokenizer:
    """
    Replaces SSNs and personal names with tokens before API calls,
    reverses them after. EINs are NOT tokenized — they're business
    identifiers that Claude needs for classification and grouping.

    Text tokenization: regex detect → replace with [SSN_1], [NAME_1], etc.
    Image tokenization: pytesseract word-level detection → black-box SSN regions.

    The token map lives in memory only and is never written to disk.
    """

    # SSN patterns: 123-45-6789, 123 45 6789, 123456789 (9 digits no dashes)
    # Also partial-masked: XXX-XX-1234, ***-**-1234, xxx-xx-1234
    SSN_PATTERN = re.compile(
        r'\b(\d{3}[-\s]?\d{2}[-\s]?\d{4})\b'
        r'|'
        r'(?<!\w)([Xx*]{3}[-\s]?[Xx*]{2}[-\s]?\d{4})(?!\w)'
    )

    # Last-4 SSN patterns (common on W-2 copies): "SSN: ***-**-1234" already caught above
    # Also catch standalone "last 4: 1234" type references
    SSN_LAST4_PATTERN = re.compile(
        r'(?:SSN|social\s*security)[\s:]*(?:last\s*4[\s:]*)(\d{4})',
        re.IGNORECASE
    )

    def __init__(self):
        self.ssn_map = {}       # token → real value
        self.ssn_reverse = {}   # real value → token
        self._ssn_counter = 0

    def _next_ssn_token(self):
        self._ssn_counter += 1
        return f"[SSN_{self._ssn_counter}]"

    def _register_ssn(self, raw_ssn):
        """Register an SSN and return its token. Reuses token if already seen."""
        # Normalize: strip spaces, keep dashes
        normalized = raw_ssn.strip()
        if normalized in self.ssn_reverse:
            return self.ssn_reverse[normalized]
        token = self._next_ssn_token()
        self.ssn_map[token] = normalized
        self.ssn_reverse[normalized] = token
        return token

    def tokenize_text(self, text):
        """Replace SSNs in OCR text with tokens. Returns (tokenized_text, found_count)."""
        if not text:
            return text, 0

        found = 0

        def replace_ssn(match):
            nonlocal found
            raw = match.group(0)
            found += 1
            return self._register_ssn(raw)

        result = self.SSN_PATTERN.sub(replace_ssn, text)
        return result, found

    def detokenize_text(self, text):
        """Reverse all tokens back to real values in a string."""
        if not text:
            return text
        for token, real in self.ssn_map.items():
            text = text.replace(token, real)
        return text

    def detokenize_json(self, obj):
        """Recursively reverse tokens in a parsed JSON object."""
        if obj is None:
            return None
        if isinstance(obj, str):
            return self.detokenize_text(obj)
        if isinstance(obj, dict):
            return {k: self.detokenize_json(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [self.detokenize_json(item) for item in obj]
        return obj

    def redact_image(self, pil_image):
        """
        Find SSN-pattern text regions in the image and black them out.
        Returns a new PIL Image with SSN regions redacted.

        Uses pytesseract word-level bounding boxes to locate digits,
        then blacks out any sequence matching SSN patterns.
        """
        if not HAS_TESSERACT:
            return pil_image  # Can't redact without tesseract

        img = pil_image.copy()
        draw = ImageDraw.Draw(img)

        try:
            # Get word-level bounding boxes from tesseract
            data = pytesseract.image_to_data(img, output_type=pytesseract.Output.DICT)
        except Exception:
            return img  # If tesseract fails, return unmodified

        n = len(data['text'])
        # Build text with positions for SSN detection
        words = []
        for i in range(n):
            txt = data['text'][i].strip()
            if txt:
                words.append({
                    'text': txt,
                    'left': data['left'][i],
                    'top': data['top'][i],
                    'width': data['width'][i],
                    'height': data['height'][i],
                    'index': i
                })

        # Look for SSN patterns across consecutive words
        # SSNs can appear as: "123-45-6789" (1 word) or "123" "-" "45" "-" "6789" (5 words)
        # or "XXX-XX-2224" (masked, 1 word)
        full_text_positions = []  # (start_char, end_char, word_indices)
        running_text = ""
        for wi, w in enumerate(words):
            start = len(running_text)
            running_text += w['text'] + " "
            full_text_positions.append((start, len(running_text) - 1, wi))

        # Find SSN matches in the concatenated text
        for match in self.SSN_PATTERN.finditer(running_text):
            mstart, mend = match.start(), match.end()
            # Find which words overlap this match
            indices_to_redact = []
            for start, end, wi in full_text_positions:
                if start < mend and end > mstart:
                    indices_to_redact.append(wi)

            # Black out those word regions with padding
            for wi in indices_to_redact:
                w = words[wi]
                pad = 4
                draw.rectangle(
                    [w['left'] - pad, w['top'] - pad,
                     w['left'] + w['width'] + pad, w['top'] + w['height'] + pad],
                    fill='black'
                )

            # Register the matched SSN
            self._register_ssn(match.group(0))

        return img

    def redacted_image_to_b64(self, pil_image):
        """Redact SSNs from image and return base64 JPEG string."""
        redacted = self.redact_image(pil_image)
        buf = BytesIO()
        redacted.save(buf, format="JPEG", quality=85)
        return base64.b64encode(buf.getvalue()).decode("utf-8")

    def get_stats(self):
        """Return summary of tokenization for logging."""
        return {
            "ssns_tokenized": len(self.ssn_map),
            "tokens": list(self.ssn_map.keys()),
        }


# ─── PROMPTS ─────────────────────────────────────────────────────────────────

# Phase 1: Classification (always uses vision — need to see page layout)
CLASSIFICATION_PROMPT = """You are a tax document classification specialist. Examine this scanned page and classify it.

IMPORTANT: A single page from a consolidated brokerage/investment statement may contain data for MULTIPLE tax forms (1099-DIV, 1099-INT, 1099-MISC, 1099-B). If you see multiple form sections on one page, list ALL of them in the sub_types array.

Return JSON only:
{
    "page_number": <int>,
    "document_type": "<primary type - see list>",
    "sub_types": ["<list ALL form types found on this page>"],
    "is_consolidated_brokerage": <bool>,
    "is_duplicate_copy": <bool>,
    "is_supplemental_detail": <bool - true if transaction-level detail, NOT summary>,
    "is_continuation_statement": <bool>,
    "supports_form": "<if supplemental/continuation, which form>",
    "payer_or_entity": "<name>",
    "payer_ein": "<EIN/TIN if visible>",
    "recipient": "<person name>",
    "recipient_ssn_last4": "<last 4 if visible>",
    "tax_year": "<year>",
    "brief_description": "<one line>"
}

Document types:
W-2, W-2G, W-3,
1099-INT, 1099-DIV, 1099-B, 1099-R, 1099-NEC, 1099-MISC, 1099-SA, 1099-G,
1099-K, 1099-S, 1099-C, 1099-Q, 1099-LTC,
SSA-1099, K-1, 1098, 1098-T, 1098-E,
5498, 5498-SA, 1095-A, 1095-B, 1095-C,
brokerage_composite, continuation_statement,
property_tax_bill, charitable_receipt, farm_income_document, rental_income_document,
estimated_tax_record, schedule_c_summary,
bank_statement, bank_statement_deposit_slip, check, check_stub,
credit_card_statement, invoice, receipt,
profit_loss_statement, balance_sheet, aged_receivables, aged_payables,
chart_of_accounts, trial_balance_document,
loan_statement, mortgage_statement,
payroll_register, payroll_summary,
940, 941, 943, 944, 945,
trust_accounting_statement,
other"""

# Phase 2a: OCR-based extraction (cheap, text-only API call)
OCR_EXTRACTION_PROMPT = """You are a tax document extraction specialist. Below is OCR text extracted from a scanned tax document.
The document has been classified as: {doc_type}

Your job:
1. Extract ALL tax-relevant fields from the OCR text
2. Rate the OCR quality for each field
3. Decide if you can confidently extract from this text alone, or if the original image is needed

FIELD NAMING RULES:
For consolidated brokerage statements (multiple forms on one page), use prefixed names:
  1099-DIV: div_ordinary_dividends, div_qualified_dividends, div_capital_gain_distributions, div_nondividend_distributions, div_federal_wh, div_section_199a, div_foreign_tax_paid, div_exempt_interest, div_private_activity_bond, div_state_wh
  1099-INT: int_interest_income, int_early_withdrawal_penalty, int_us_savings_bonds_and_treasury, int_federal_wh, int_investment_expenses, int_foreign_tax_paid, int_tax_exempt_interest, int_market_discount, int_bond_premium, int_bond_premium_treasury, int_state_wh
  1099-B: b_short_term_proceeds, b_short_term_basis, b_short_term_gain_loss, b_long_term_proceeds, b_long_term_basis, b_long_term_gain_loss, b_total_proceeds, b_total_basis, b_total_gain_loss, b_federal_wh, b_market_discount, b_wash_sale_loss
  1099-MISC: misc_royalties, misc_other_income, misc_rents, misc_federal_wh, misc_state_wh

For standalone forms, use unprefixed names:
  W-2: employer_name, employer_ein, wages, federal_wh, ss_wages, ss_wh, medicare_wages, medicare_wh, state_wages, state_wh, local_wages, local_wh, nonqualified_plans_12a, state_id
  1099-R: gross_distribution, taxable_amount, capital_gain, federal_wh, distribution_code, state_wh
  1099-NEC: nonemployee_compensation, federal_wh, state_wh
  SSA-1099: net_benefits, federal_wh
  K-1: partnership_name, partnership_ein, partner_name, entity_type, partner_type,
       profit_share_begin, profit_share_end, loss_share_begin, loss_share_end,
       capital_share_begin, capital_share_end,
       box1_ordinary_income, box2_rental_real_estate, box3_other_rental,
       box4a_guaranteed_services, box5_interest, box6a_ordinary_dividends,
       box6b_qualified_dividends, box7_royalties, box8_short_term_capital_gain,
       box9a_long_term_capital_gain, box10_net_1231_gain, box11_other_income,
       box12_section_179, box13_other_deductions, box14_self_employment,
       box15_credits, box17_alt_min_tax, box18_tax_exempt_income,
       box19_distributions, box20_other_info,
       beginning_capital_account, ending_capital_account,
       current_year_net_income, withdrawals_distributions, capital_contributed
  1098: mortgage_interest, property_tax, mortgage_insurance_premiums
  1098-T: payments_received, scholarships_grants
  1098-E: student_loan_interest
  W-2G: gross_winnings, federal_wh, type_of_wager, state_wh, state_winnings
  1099-K: gross_amount, card_not_present_txns, federal_wh, jan through dec (monthly amounts),
       payment_processor, number_of_transactions
  1099-S: gross_proceeds, buyers_part_of_real_estate_tax, address_of_property, date_of_closing
  1099-C: debt_cancelled, date_cancelled, debt_description, fair_market_value, identifiable_event_code
  1099-Q: gross_distribution, earnings, basis, distribution_type (education/rollover)
  1099-LTC: gross_ltc_benefits, accelerated_death_benefits, per_diem, reimbursed
  5498: ira_contributions, rollover_contributions, roth_conversion, recharacterized,
       fair_market_value, rmd_amount, rmd_date
  5498-SA: hsa_contributions, employer_contributions, total_contributions
  1095-A: monthly_premium, monthly_slcsp, monthly_advance_ptc (for each month)
  Schedule C summary: business_name, gross_income, total_expenses, net_profit

For bookkeeping documents:
  Bank Statement: bank_name, account_number, account_number_last4,
       statement_period_start, statement_period_end,
       beginning_balance, total_deposits, total_withdrawals, ending_balance,
       num_deposits, num_withdrawals, fees_charged, interest_earned
       ALSO extract every individual transaction as numbered fields inside "fields":
       txn_1_date, txn_1_desc, txn_1_amount, txn_1_type (deposit/withdrawal/fee/check/transfer)
       txn_2_date, txn_2_desc, txn_2_amount, txn_2_type
       ...continue numbering for ALL visible transactions (up to 50).
       Include check numbers in desc (e.g. "Check #250 - Final Pmt sewer")
  Credit Card Statement: card_issuer, account_number_last4,
       statement_period_start, statement_period_end,
       previous_balance, payments, credits, purchases, fees_charged, interest_charged,
       new_balance, minimum_payment, payment_due_date
       Extract transactions as txn_N_date, txn_N_desc, txn_N_amount, txn_N_category
  Check Stub (Pay Stub): employer_name, employee_name, pay_period_start, pay_period_end, pay_date,
       gross_pay, federal_wh, state_wh, social_security, medicare, net_pay,
       ytd_gross, ytd_federal_wh, ytd_state_wh, ytd_social_security, ytd_medicare, ytd_net_pay,
       hours_regular, hours_overtime, rate_regular, rate_overtime
  Invoice: vendor_name, invoice_number, invoice_date, due_date, subtotal, tax_amount, total_amount,
       description, payment_terms
       For line items: Return as an array in "line_items" with fields: description, quantity, unit_price, amount
  Receipt: vendor_name, receipt_date, subtotal, tax_amount, total_amount, payment_method,
       category (meals, supplies, travel, equipment, utilities, rent, insurance, professional_services, other)
       For line items: Return as an array in "line_items" with fields: description, amount
  Profit & Loss (P&L): period_start, period_end, total_revenue, total_cogs, gross_profit,
       total_operating_expenses, operating_income, net_income
       Extract line items as: rev_N_desc, rev_N_amount; exp_N_desc, exp_N_amount (numbered)
  Balance Sheet: as_of_date, total_assets, total_liabilities, total_equity
       Extract line items as: asset_N_desc, asset_N_amount; liab_N_desc, liab_N_amount (numbered)
  Loan Statement: lender, account_number, original_amount, current_balance, interest_rate,
       payment_amount, payment_date, principal_paid, interest_paid, maturity_date
  Mortgage Statement: lender, property_address, account_number, original_amount,
       current_balance, interest_rate, escrow_balance,
       payment_amount, principal_paid, interest_paid, escrow_paid, next_due_date

For payroll documents:
  Payroll Register: pay_date, total_gross, total_federal_wh, total_state_wh,
       total_social_security, total_medicare, total_net_pay, num_employees
       Extract per-employee: emp_N_name, emp_N_gross, emp_N_federal_wh, emp_N_net (numbered)
  941/940/943/944/945: quarter, tax_period, total_wages, total_federal_tax,
       total_social_security_tax, total_medicare_tax, total_deposits, balance_due

K-1 CRITICAL NOTES:
  Box 2 = Net rental real estate income (loss). NOT credits.
  Box 15 = Credits. These are DIFFERENT boxes. Do not confuse them.
  Small values like -9, -3 are real. Do not skip them.
  If a box says "STMT" or "* STMT", the value is on a continuation statement.

Return JSON only:
{
    "document_type": "<type>",
    "payer_or_entity": "<name>",
    "payer_ein": "<EIN>",
    "recipient": "<person>",
    "tax_year": "<year>",
    "fields": {
        "<field_name>": {
            "value": <number or string>,
            "label_on_form": "<text context around this value in the OCR>",
            "confidence": "<high|medium|low>",
            "ocr_clear": <true if the digits in the OCR text are unambiguous, false if garbled/unclear>
        }
    },
    "continuation_items": [
        {"line_reference": "<ref>", "description": "<desc>", "amount": <number>, "confidence": "<c>"}
    ],
    "ocr_quality": "<good|partial|poor>",
    "needs_image_review": <true if OCR text was too garbled for confident extraction>,
    "fields_needing_image": ["<field names where OCR text was ambiguous>"],
    "flags": [],
    "notes": []
}"""

# Phase 2b: Vision extraction (expensive, used as fallback or for image-needed fields)
VISION_EXTRACTION_PROMPT = """You are a document extraction specialist for a CPA firm.
The OCR text for this page was insufficient, so you are reading directly from the scanned image.

{context}

CRITICAL ACCURACY RULES:
- Read each number DIGIT BY DIGIT. Do not estimate.
- Pay attention to: commas vs periods, 6 vs 8, 1 vs 7, 0 vs 6, 3 vs 8, 4 vs 9.
- If a value has cents (13,664.79), extract EXACT cents.
- DO NOT add or calculate. Only extract what is printed.
- For dotted-line values, follow the dots carefully to the number.

FIELD NAMING RULES — you MUST use these exact field names:
For consolidated brokerage statements (multiple forms on one page), use prefixed names:
  1099-DIV: div_ordinary_dividends, div_qualified_dividends, div_capital_gain_distributions, div_nondividend_distributions, div_federal_wh, div_section_199a, div_foreign_tax_paid, div_exempt_interest, div_private_activity_bond, div_state_wh
  1099-INT: int_interest_income, int_early_withdrawal_penalty, int_us_savings_bonds_and_treasury, int_federal_wh, int_investment_expenses, int_foreign_tax_paid, int_tax_exempt_interest, int_market_discount, int_bond_premium, int_bond_premium_treasury, int_state_wh
  1099-B: b_short_term_proceeds, b_short_term_basis, b_short_term_gain_loss, b_long_term_proceeds, b_long_term_basis, b_long_term_gain_loss, b_total_proceeds, b_total_basis, b_total_gain_loss, b_federal_wh, b_market_discount, b_wash_sale_loss
  1099-MISC: misc_royalties, misc_other_income, misc_rents, misc_federal_wh, misc_state_wh

For standalone forms, use unprefixed names:
  W-2: employer_name, employer_ein, wages, federal_wh, ss_wages, ss_wh, medicare_wages, medicare_wh, state_wages, state_wh, local_wages, local_wh, nonqualified_plans_12a, state_id
  1099-DIV: ordinary_dividends, qualified_dividends, capital_gain_distributions, nondividend_distributions, federal_wh, section_199a, foreign_tax_paid, exempt_interest, private_activity_bond, state_wh
  1099-INT: interest_income, early_withdrawal_penalty, us_savings_bonds_and_treasury, federal_wh, investment_expenses, foreign_tax_paid, tax_exempt_interest, market_discount, bond_premium, state_wh
  1099-R: gross_distribution, taxable_amount, capital_gain, federal_wh, distribution_code, state_wh
  1099-NEC: nonemployee_compensation, federal_wh, state_wh
  SSA-1099: net_benefits, federal_wh
  K-1: partnership_name, partnership_ein, partner_name, entity_type, partner_type,
       profit_share_begin, profit_share_end, loss_share_begin, loss_share_end,
       capital_share_begin, capital_share_end,
       box1_ordinary_income, box2_rental_real_estate, box3_other_rental,
       box4a_guaranteed_services, box5_interest, box6a_ordinary_dividends,
       box6b_qualified_dividends, box7_royalties, box8_short_term_capital_gain,
       box9a_long_term_capital_gain, box10_net_1231_gain, box11_other_income,
       box12_section_179, box13_other_deductions, box14_self_employment,
       box15_credits, box17_alt_min_tax, box18_tax_exempt_income,
       box19_distributions, box20_other_info,
       beginning_capital_account, ending_capital_account,
       current_year_net_income, withdrawals_distributions, capital_contributed

K-1 CRITICAL: Box 2 = rental real estate income. Box 15 = credits. They are DIFFERENT.
Small values like -9 ARE real values.

BOOKKEEPING DOCUMENTS:
For bank statements: Extract summary fields (beginning_balance, ending_balance, total_deposits,
total_withdrawals, fees_charged, interest_earned, statement_period_start, statement_period_end).
ALSO extract every individual transaction as numbered fields in the fields dict:
  txn_1_date, txn_1_desc, txn_1_amount, txn_1_type (deposit/withdrawal/fee/check/transfer)
  txn_2_date, txn_2_desc, txn_2_amount, txn_2_type
  ...continue numbering for ALL visible transactions (up to 50).
For check stubs: extract gross_pay, federal_wh, state_wh, social_security, medicare, net_pay plus YTD fields.
For invoices and receipts: extract vendor_name, date, subtotal, tax_amount, total_amount, and categorize receipts.

Return JSON:
{
    "document_type": "<type>",
    "payer_or_entity": "<name>",
    "payer_ein": "<EIN>",
    "recipient": "<person>",
    "tax_year": "<year>",
    "fields": {
        "<field_name>": {
            "value": <number or string>,
            "label_on_form": "<exact label text as printed>",
            "confidence": "<high|medium|low>"
        }
    },
    "continuation_items": [
        {"line_reference": "<ref>", "description": "<desc>", "amount": <number>, "confidence": "<c>"}
    ],
    "flags": [],
    "notes": []
}"""

# Phase 3: Verification — cross-check OCR text against image for critical fields
VERIFY_CROSSCHECK_PROMPT = """You are verifying a tax data extraction. I have extracted values using OCR text, and now I need you to check the ORIGINAL IMAGE to verify the critical numeric fields.

Here are the extracted values to verify:
{extracted_json}

INSTRUCTIONS:
1. For EACH field listed, find the corresponding value on the scanned image.
2. Read the number digit by digit from the image.
3. Compare to the extracted value.
4. If they match → status "confirmed"
5. If they differ → status "corrected", provide the value you read from the image
6. If you can't find the field on the image → status "unverifiable"
7. Also look for any fields VISIBLE on the image that are NOT in the extraction.

Return JSON:
{
    "verification_results": {
        "<field_name>": {
            "extracted_value": <from OCR>,
            "image_value": <what you see on image>,
            "final_value": <correct value>,
            "status": "<confirmed|corrected|unverifiable>",
            "notes": "<explanation if corrected>"
        }
    },
    "missing_fields": [
        {"field_name": "<name>", "value": <v>, "label_on_form": "<label>", "notes": "On image but not extracted"}
    ],
    "overall_confidence": "<high|medium|low>"
}"""


# ─── TEMPLATE SECTIONS ───────────────────────────────────────────────────────

TEMPLATE_SECTIONS = [
    {
        "id": "w2",
        "header": "W-2:",
        "match_types": ["W-2"],
        "columns": {"A": "employer_name", "B": "wages", "C": "federal_wh", "D": "state_wh"},
        "col_headers": {"B": "Gross", "C": "Federal WH", "D": "State WH"},
        "sum_cols": ["B", "C", "D"],
    },
    {
        "id": "interest",
        "header": "Interest Income:",
        "match_types": ["1099-INT", "_interest_rollup"],
        "columns": {"A": "_source_name", "B": "interest_income", "C": "us_savings_bonds_and_treasury"},
        "col_headers": {"B": "Interest", "C": "US Bonds"},
        "sum_cols": ["B", "C"],
        "total_formula_col": {"D": "B+C"},
    },
    {
        "id": "dividends",
        "header": "Dividends:",
        "match_types": ["1099-DIV", "_dividend_rollup"],
        "columns": {"A": "_source_name", "B": "ordinary_dividends", "C": "qualified_dividends"},
        "col_headers": {"B": "Total", "C": "Qualified"},
        "sum_cols": ["B", "C"],
    },
    {
        "id": "1099r",
        "header": "1099-R:",
        "match_types": ["1099-R"],
        "columns": {"A": "payer_or_entity", "B": "gross_distribution", "C": "taxable_amount",
                     "D": "federal_wh", "E": "state_wh", "F": "distribution_code"},
        "col_headers": {"B": "Gross", "C": "Taxable", "D": "FWH", "E": "SWH", "F": "Code"},
        "sum_cols": ["B", "C", "D", "E"],
    },
    {
        "id": "ssa",
        "header": "SSA-1099:",
        "match_types": ["SSA-1099"],
        "columns": {"A": "payer_or_entity", "B": "net_benefits", "C": "federal_wh"},
        "col_headers": {"B": "Net Benefits", "C": "Federal WH"},
        "sum_cols": ["B", "C"],
    },
    {
        "id": "1099nec",
        "header": "1099-NEC (Self-Employment):",
        "match_types": ["1099-NEC"],
        "columns": {"A": "payer_or_entity", "B": "nonemployee_compensation", "C": "federal_wh"},
        "col_headers": {"B": "NEC Income", "C": "FWH"},
        "sum_cols": ["B", "C"],
    },
    {
        "id": "1099misc",
        "header": "1099-MISC:",
        "match_types": ["1099-MISC"],
        "columns": {"A": "payer_or_entity", "B": "rents", "C": "royalties", "D": "other_income", "E": "federal_wh"},
        "col_headers": {"B": "Rents", "C": "Royalties", "D": "Other Income", "E": "FWH"},
        "sum_cols": ["B", "C", "D", "E"],
    },
    {
        "id": "1099g",
        "header": "1099-G:",
        "match_types": ["1099-G"],
        "columns": {"A": "payer_or_entity", "B": "unemployment", "C": "state_local_refund", "D": "federal_wh"},
        "col_headers": {"B": "Unemployment", "C": "State Refund", "D": "FWH"},
        "sum_cols": ["B", "C", "D"],
    },
    {
        "id": "schedule_d",
        "header": "Schedule D:",
        "match_types": ["1099-B", "_brokerage_gains"],
        "columns": {"A": "_source_name", "B": "short_term_gain_loss", "C": "long_term_gain_loss", "D": "total_gain_loss"},
        "col_headers": {"B": "Short-Term", "C": "Long-Term", "D": "Total Gain/Loss"},
        "sum_cols": ["B", "C", "D"],
        "flags": ["⚠ Check for capital loss carryover from prior year"],
    },
    {
        "id": "k1",
        "header": "K-1s:",
        "match_types": ["K-1"],
        "columns": {"A": "_display_name", "B": "_carryover_prior", "C": "box1_ordinary_income",
                     "D": "box2_rental_real_estate", "E": "_allowed", "F": "_carryover_next"},
        "col_headers": {"B": "C/O from PY", "C": "Box 1", "D": "Box 2", "E": "Allowed", "F": "C/O to NY"},
        "sum_cols": ["B", "C", "D", "E", "F"],
        "flags": [
            "⚠ Column B (PY carryover): REQUIRES prior year data — enter manually",
            "⚠ Column E (Allowed): REQUIRES basis/at-risk/passive analysis — enter manually",
            "⚠ Column F (NY carryover): REQUIRES basis/at-risk/passive analysis — enter manually",
        ],
    },
    {
        "id": "k1_detail",
        "header": "K-1 Additional Detail:",
        "special": "k1_extras",
    },
    {
        "id": "rental",
        "header": "Rental Income (Schedule E):",
        "match_types": ["rental_income_document"],
        "columns": {"A": "property_address", "B": "gross_rents", "C": "total_expenses", "D": "net_rental_income"},
        "col_headers": {"B": "Gross Rents", "C": "Expenses", "D": "Net Income"},
        "sum_cols": ["B", "C", "D"],
    },
    {
        "id": "farm",
        "header": "Farm Income (Schedule F):",
        "match_types": ["farm_income_document"],
        "columns": {"A": "description", "B": "gross_farm_income", "C": "farm_expenses", "D": "net_farm_income"},
        "col_headers": {"B": "Gross", "C": "Expenses", "D": "Net"},
        "sum_cols": ["B", "C", "D"],
    },
    {
        "id": "1098",
        "header": "Form 1098 (Mortgage Interest):",
        "match_types": ["1098"],
        "columns": {"A": "payer_or_entity", "B": "mortgage_interest", "C": "property_tax", "D": "mortgage_insurance_premiums"},
        "col_headers": {"B": "Mortgage Int", "C": "Property Tax", "D": "PMI"},
        "sum_cols": ["B", "C", "D"],
    },
    {
        "id": "1098t",
        "header": "1098-T (Tuition):",
        "match_types": ["1098-T"],
        "columns": {"A": "institution_name", "B": "payments_received", "C": "scholarships_grants"},
        "col_headers": {"B": "Box 1", "C": "Box 5"},
        "sum_cols": ["B", "C"],
    },
    {
        "id": "property_tax",
        "header": "Property Tax Bills:",
        "match_types": ["property_tax_bill"],
        "columns": {"A": "property_address", "B": "tax_amount"},
        "col_headers": {"B": "Tax Amount"},
        "sum_cols": ["B"],
    },
    {
        "id": "estimated",
        "header": "Estimated Tax Payments:",
        "match_types": ["estimated_tax_record"],
        "columns": {"A": "payment_date", "B": "federal_amount", "C": "state_amount"},
        "col_headers": {"B": "Federal", "C": "State"},
        "sum_cols": ["B", "C"],
        "flags": ["⚠ Estimated payments often NOT on scanned docs — verify with client"],
    },
    {
        "id": "charitable",
        "header": "Charitable Contributions:",
        "match_types": ["charitable_receipt"],
        "columns": {"A": "organization_name", "B": "donation_amount", "C": "donation_type"},
        "col_headers": {"B": "Amount", "C": "Type"},
        "sum_cols": ["B"],
    },
    # ─── Additional Tax Forms ───
    {
        "id": "w2g",
        "header": "W-2G (Gambling Winnings):",
        "match_types": ["W-2G"],
        "columns": {"A": "payer_or_entity", "B": "gross_winnings", "C": "federal_wh",
                     "D": "type_of_wager", "E": "state_wh"},
        "col_headers": {"B": "Winnings", "C": "FWH", "D": "Type", "E": "SWH"},
        "sum_cols": ["B", "C", "E"],
    },
    {
        "id": "1099k",
        "header": "1099-K (Payment Card / Third Party):",
        "match_types": ["1099-K"],
        "columns": {"A": "payer_or_entity", "B": "gross_amount", "C": "number_of_transactions",
                     "D": "federal_wh"},
        "col_headers": {"B": "Gross Amount", "C": "# Txns", "D": "FWH"},
        "sum_cols": ["B", "D"],
    },
    {
        "id": "1099s",
        "header": "1099-S (Real Estate Proceeds):",
        "match_types": ["1099-S"],
        "columns": {"A": "address_of_property", "B": "gross_proceeds", "C": "date_of_closing",
                     "D": "buyers_part_of_real_estate_tax"},
        "col_headers": {"B": "Gross Proceeds", "C": "Closing Date", "D": "RE Tax"},
        "sum_cols": ["B", "D"],
    },
    {
        "id": "1099c",
        "header": "1099-C (Cancellation of Debt):",
        "match_types": ["1099-C"],
        "columns": {"A": "payer_or_entity", "B": "debt_cancelled", "C": "date_cancelled",
                     "D": "fair_market_value", "E": "debt_description"},
        "col_headers": {"B": "Debt Cancelled", "C": "Date", "D": "FMV", "E": "Description"},
        "sum_cols": ["B", "D"],
        "flags": ["⚠ Check insolvency exclusion — may reduce taxable amount"],
    },
    {
        "id": "1098e",
        "header": "1098-E (Student Loan Interest):",
        "match_types": ["1098-E"],
        "columns": {"A": "payer_or_entity", "B": "student_loan_interest"},
        "col_headers": {"B": "Interest Paid"},
        "sum_cols": ["B"],
    },
    {
        "id": "5498",
        "header": "5498 (IRA Contributions):",
        "match_types": ["5498"],
        "columns": {"A": "payer_or_entity", "B": "ira_contributions", "C": "rollover_contributions",
                     "D": "roth_conversion", "E": "fair_market_value", "F": "rmd_amount"},
        "col_headers": {"B": "Contributions", "C": "Rollovers", "D": "Roth Conv", "E": "FMV", "F": "RMD"},
        "sum_cols": ["B", "C", "D"],
    },
    {
        "id": "schedule_c",
        "header": "Schedule C (Business Income):",
        "match_types": ["schedule_c_summary"],
        "columns": {"A": "business_name", "B": "gross_income", "C": "total_expenses", "D": "net_profit"},
        "col_headers": {"B": "Gross Income", "C": "Expenses", "D": "Net Profit"},
        "sum_cols": ["B", "C", "D"],
    },
    # ─── Bookkeeping Sections ───
    {
        "id": "bank_statement",
        "header": "Bank Statements:",
        "match_types": ["bank_statement"],
        "columns": {"A": "bank_name", "B": "beginning_balance", "C": "total_deposits",
                     "D": "total_withdrawals", "E": "fees_charged", "F": "interest_earned", "G": "ending_balance"},
        "col_headers": {"B": "Begin Bal", "C": "Deposits", "D": "Withdrawals", "E": "Fees", "F": "Interest", "G": "End Bal"},
        "sum_cols": ["C", "D", "E", "F"],
    },
    {
        "id": "credit_card",
        "header": "Credit Card Statements:",
        "match_types": ["credit_card_statement"],
        "columns": {"A": "card_issuer", "B": "previous_balance", "C": "purchases",
                     "D": "payments", "E": "interest_charged", "F": "fees_charged", "G": "new_balance"},
        "col_headers": {"B": "Prev Bal", "C": "Purchases", "D": "Payments", "E": "Interest", "F": "Fees", "G": "New Bal"},
        "sum_cols": ["C", "D", "E", "F"],
    },
    {
        "id": "check_stub",
        "header": "Pay Stubs / Check Stubs:",
        "match_types": ["check_stub"],
        "columns": {"A": "employer_name", "B": "gross_pay", "C": "federal_wh",
                     "D": "state_wh", "E": "social_security", "F": "medicare", "G": "net_pay"},
        "col_headers": {"B": "Gross Pay", "C": "FWH", "D": "SWH", "E": "SS", "F": "Medicare", "G": "Net Pay"},
        "sum_cols": ["B", "C", "D", "E", "F", "G"],
    },
    {
        "id": "invoice",
        "header": "Invoices:",
        "match_types": ["invoice"],
        "columns": {"A": "vendor_name", "B": "invoice_number", "C": "invoice_date",
                     "D": "subtotal", "E": "tax_amount", "F": "total_amount"},
        "col_headers": {"B": "Invoice #", "C": "Date", "D": "Subtotal", "E": "Tax", "F": "Total"},
        "sum_cols": ["D", "E", "F"],
    },
    {
        "id": "receipt",
        "header": "Receipts:",
        "match_types": ["receipt"],
        "columns": {"A": "vendor_name", "B": "receipt_date", "C": "category",
                     "D": "subtotal", "E": "tax_amount", "F": "total_amount"},
        "col_headers": {"B": "Date", "C": "Category", "D": "Subtotal", "E": "Tax", "F": "Total"},
        "sum_cols": ["D", "E", "F"],
    },
    # ─── Financial Statements ───
    {
        "id": "profit_loss",
        "header": "Profit & Loss Statements:",
        "match_types": ["profit_loss_statement"],
        "columns": {"A": "payer_or_entity", "B": "total_revenue", "C": "total_cogs",
                     "D": "gross_profit", "E": "total_operating_expenses", "F": "net_income"},
        "col_headers": {"B": "Revenue", "C": "COGS", "D": "Gross Profit", "E": "Op Expenses", "F": "Net Income"},
        "sum_cols": ["B", "C", "D", "E", "F"],
    },
    {
        "id": "balance_sheet",
        "header": "Balance Sheets:",
        "match_types": ["balance_sheet"],
        "columns": {"A": "payer_or_entity", "B": "total_assets", "C": "total_liabilities",
                     "D": "total_equity"},
        "col_headers": {"B": "Total Assets", "C": "Total Liabilities", "D": "Total Equity"},
        "sum_cols": ["B", "C", "D"],
    },
    {
        "id": "loan_statement",
        "header": "Loan Statements:",
        "match_types": ["loan_statement", "mortgage_statement"],
        "columns": {"A": "lender", "B": "current_balance", "C": "interest_rate",
                     "D": "payment_amount", "E": "principal_paid", "F": "interest_paid"},
        "col_headers": {"B": "Balance", "C": "Rate", "D": "Payment", "E": "Principal", "F": "Interest"},
        "sum_cols": ["D", "E", "F"],
    },
    # ─── Payroll Sections ───
    {
        "id": "payroll_register",
        "header": "Payroll Registers:",
        "match_types": ["payroll_register", "payroll_summary"],
        "columns": {"A": "payer_or_entity", "B": "total_gross", "C": "total_federal_wh",
                     "D": "total_state_wh", "E": "total_social_security", "F": "total_medicare", "G": "total_net_pay"},
        "col_headers": {"B": "Gross", "C": "FWH", "D": "SWH", "E": "SS", "F": "Medicare", "G": "Net Pay"},
        "sum_cols": ["B", "C", "D", "E", "F", "G"],
    },
    {
        "id": "payroll_tax",
        "header": "Payroll Tax Forms (940/941/943/944/945):",
        "match_types": ["940", "941", "943", "944", "945"],
        "columns": {"A": "payer_or_entity", "B": "total_wages", "C": "total_federal_tax",
                     "D": "total_social_security_tax", "E": "total_medicare_tax", "F": "balance_due"},
        "col_headers": {"B": "Wages", "C": "Federal Tax", "D": "SS Tax", "E": "Medicare Tax", "F": "Bal Due"},
        "sum_cols": ["B", "C", "D", "E", "F"],
    },
]

ALWAYS_SHOW = ["w2", "interest", "dividends", "schedule_d", "k1"]

# ─── HELPERS ─────────────────────────────────────────────────────────────────

def get_val(fields, key):
    fdata = fields.get(key)
    if fdata is None: return None
    v = fdata.get("value") if isinstance(fdata, dict) else fdata
    try: return float(v) if v is not None else None
    except (ValueError, TypeError): return None

def get_str(fields, key):
    fdata = fields.get(key)
    if fdata is None: return None
    v = fdata.get("value") if isinstance(fdata, dict) else fdata
    return str(v) if v is not None else None

def auto_rotate(img):
    """Detect and fix sideways/landscape pages. Returns corrected PIL image."""
    w, h = img.size
    if w > h * 1.15:
        # Landscape — use Tesseract OSD to determine correct rotation
        if HAS_TESSERACT:
            try:
                osd = pytesseract.image_to_osd(img, output_type=pytesseract.Output.DICT)
                angle = osd.get("rotate", 0)
                if angle != 0:
                    img = img.rotate(-angle, expand=True)
                    return img
                # Tesseract says angle=0 — page is already correct orientation (landscape)
                return img
            except Exception as e:
                print(f"  Auto-rotate: Tesseract OSD failed ({e}), rotating 90 CW as fallback")
        # Fallback only if Tesseract unavailable or failed entirely
        img = img.rotate(-90, expand=True)
    return img

def pdf_to_images(pdf_path, dpi=DPI):
    """Convert PDF to base64 JPEG strings (for API). Auto-rotates sideways pages.
    PIL images are freed after encoding to minimize memory usage."""
    print(f"Converting PDF at {dpi} DPI...")
    raw_images = convert_from_path(pdf_path, dpi=dpi)
    b64_images = []
    rotated_count = 0
    for i, img in enumerate(raw_images):
        orig_size = img.size
        img = auto_rotate(img)
        if img.size != orig_size:
            rotated_count += 1
            print(f"  Page {i+1}: auto-rotated ({orig_size[0]}x{orig_size[1]} -> {img.size[0]}x{img.size[1]})")
        buf = BytesIO()
        img.save(buf, format="JPEG", quality=85)
        b64_images.append(base64.standard_b64encode(buf.getvalue()).decode("utf-8"))
        img.close()  # Free PIL image memory
    del raw_images  # Release reference to all raw images
    print(f"  {len(b64_images)} pages converted" + (f" ({rotated_count} auto-rotated)" if rotated_count else ""))
    return b64_images

def ocr_page(pil_image, page_num=None):
    """Run Tesseract OCR on a PIL image. Returns text or None."""
    if not HAS_TESSERACT:
        return None
    tag = f"[Page {page_num}] " if page_num else ""
    try:
        # Try standard orientation first
        text = pytesseract.image_to_string(pil_image, config='--oem 3 --psm 6')
        if text and len(text.strip()) >= OCR_MIN_CHARS:
            return text
        # Try auto-orientation for sideways pages
        text2 = pytesseract.image_to_string(pil_image, config='--oem 3 --psm 1')
        if text2 and len(text2.strip()) > len(text.strip()):
            text = text2
        if text and len(text.strip()) >= OCR_MIN_CHARS:
            return text
        # Try rotated 90 degrees
        rotated = pil_image.rotate(-90, expand=True)
        text3 = pytesseract.image_to_string(rotated, config='--oem 3 --psm 6')
        if text3 and len(text3.strip()) > len(text.strip()):
            return text3
        if text and len(text.strip()) > 30:  # Accept even short text
            return text
        print(f"  {tag}OCR: too little text ({len(text.strip()) if text else 0} chars)")
        return None
    except Exception as e:
        print(f"  {tag}OCR error: {e}")
        return None

def ocr_all_pages(pil_images):
    """OCR every page upfront using parallel threads. Returns list of text (or None for failed pages)."""
    print("\n── OCR Pass (Tesseract) ──")
    if not HAS_TESSERACT:
        print("  Tesseract not available — all pages will use vision")
        return [None] * len(pil_images)

    results = [None] * len(pil_images)
    good = 0

    def _ocr_one(i, img):
        return i, ocr_page(img, i + 1)

    with ThreadPoolExecutor(max_workers=min(4, len(pil_images))) as pool:
        futures = {pool.submit(_ocr_one, i, img): i for i, img in enumerate(pil_images)}
        for future in as_completed(futures):
            i, text = future.result()
            results[i] = text

    for i, text in enumerate(results):
        if text:
            good += 1
            chars = len(text.strip())
            nums = len(re.findall(r'\d+[,.]?\d*', text))
            print(f"  Page {i+1}: ✓ {chars} chars, {nums} numbers found")
        else:
            print(f"  Page {i+1}: ✗ OCR failed — will use vision")
    print(f"  OCR success: {good}/{len(pil_images)} pages")
    return results

import time as _time

# Retriable HTTP error codes
_RETRIABLE_ERRORS = (429, 500, 502, 503, 529)
_MAX_RETRIES = 3
_RETRY_BASE_DELAY = 2  # seconds, doubles each attempt

def _is_retriable(exc):
    """Check if an API error is transient and worth retrying."""
    exc_str = str(exc).lower()
    # anthropic SDK raises specific error types with status_code attribute
    status = getattr(exc, 'status_code', None)
    if status and status in _RETRIABLE_ERRORS:
        return True
    # Fallback: check error message text
    if any(s in exc_str for s in ['rate limit', 'overloaded', '529', '502', '503', '500', 'timeout', 'connection', 'timed out']):
        return True
    return False

def call_claude_vision(client, image_b64, prompt, page_num=None, tokenizer=None, max_tokens=None, phase="extract"):
    """Send image to Claude with retry logic. Redacts SSNs if tokenizer provided."""
    global _cost_tracker
    tag = f"[Page {page_num}] " if page_num else ""
    tokens = max_tokens or MAX_TOKENS

    # Redact PII from image before sending (do this once, not per-retry)
    send_b64 = image_b64
    if tokenizer and HAS_TESSERACT:
        try:
            img_bytes = base64.b64decode(image_b64)
            pil_img = Image.open(BytesIO(img_bytes))
            send_b64 = tokenizer.redacted_image_to_b64(pil_img)
        except Exception as e:
            print(f"  {tag}PII redaction warning: {e} (sending unredacted)")

    for attempt in range(_MAX_RETRIES):
        try:
            msg = client.messages.create(
                model=MODEL, max_tokens=tokens,
                messages=[{"role": "user", "content": [
                    {"type": "image", "source": {"type": "base64", "media_type": "image/jpeg", "data": send_b64}},
                    {"type": "text", "text": prompt}
                ]}]
            )

            # Track cost
            if _cost_tracker and hasattr(msg, 'usage'):
                _cost_tracker.record(phase, page_num, msg.usage, "vision")

            # Check for truncation (max_tokens hit)
            stop_reason = getattr(msg, 'stop_reason', None)
            if stop_reason == 'max_tokens' and tokens < 16000:
                print(f"  {tag}Response truncated (hit {tokens} tokens) — retrying with higher limit")
                tokens = min(tokens * 2, 16000)
                continue

            result = _parse_json_response(msg.content[0].text, tag)

            # If JSON parse failed, retry once with a nudge
            if result is None and attempt < _MAX_RETRIES - 1:
                print(f"  {tag}JSON parse failed — retrying (attempt {attempt + 2}/{_MAX_RETRIES})")
                _time.sleep(1)
                continue

            # Detokenize any tokens that leaked into the response
            if tokenizer and result:
                result = tokenizer.detokenize_json(result)

            return result

        except Exception as e:
            if _is_retriable(e) and attempt < _MAX_RETRIES - 1:
                delay = _RETRY_BASE_DELAY * (2 ** attempt)
                print(f"  {tag}API error: {e} — retrying in {delay}s (attempt {attempt + 2}/{_MAX_RETRIES})")
                _time.sleep(delay)
                continue
            else:
                print(f"  {tag}VISION ERROR: {e}" + (" (no more retries)" if attempt > 0 else ""))
                return None

    print(f"  {tag}VISION ERROR: all {_MAX_RETRIES} attempts failed")
    return None

def call_claude_text(client, text_content, prompt, page_num=None, tokenizer=None, phase="extract"):
    """Send text to Claude with retry logic. Tokenizes SSNs if tokenizer provided."""
    global _cost_tracker
    tag = f"[Page {page_num}] " if page_num else ""

    # Tokenize PII once before retries
    send_text = text_content
    pii_count = 0
    if tokenizer:
        send_text, pii_count = tokenizer.tokenize_text(text_content)
        if pii_count > 0:
            print(f"  {tag}PII: tokenized {pii_count} SSN(s) before API call")

    for attempt in range(_MAX_RETRIES):
        try:
            msg = client.messages.create(
                model=MODEL, max_tokens=MAX_TOKENS,
                messages=[{"role": "user", "content": f"{prompt}\n\n--- OCR TEXT ---\n{send_text}\n--- END OCR TEXT ---"}]
            )

            # Track cost
            if _cost_tracker and hasattr(msg, 'usage'):
                _cost_tracker.record(phase, page_num, msg.usage, "text")
            result = _parse_json_response(msg.content[0].text, tag)

            if result is None and attempt < _MAX_RETRIES - 1:
                print(f"  {tag}JSON parse failed — retrying (attempt {attempt + 2}/{_MAX_RETRIES})")
                _time.sleep(1)
                continue

            if tokenizer and result:
                result = tokenizer.detokenize_json(result)

            return result

        except Exception as e:
            if _is_retriable(e) and attempt < _MAX_RETRIES - 1:
                delay = _RETRY_BASE_DELAY * (2 ** attempt)
                print(f"  {tag}API error: {e} — retrying in {delay}s (attempt {attempt + 2}/{_MAX_RETRIES})")
                _time.sleep(delay)
                continue
            else:
                print(f"  {tag}TEXT ERROR: {e}" + (" (no more retries)" if attempt > 0 else ""))
                return None

    print(f"  {tag}TEXT ERROR: all {_MAX_RETRIES} attempts failed")
    return None


def call_claude_vision_multipage(client, images_b64, prompt, page_nums=None, tokenizer=None, phase="extract"):
    """Send multiple images in one API call so Claude can cross-reference pages.

    Used for K-1s with continuation statements and multi-page brokerage composites
    where values on one page reference data on another (e.g., "STMT" → continuation).
    """
    global _cost_tracker
    tag = f"[Pages {page_nums}] " if page_nums else ""

    # Build content array with all images + prompt
    content = []
    for i, img_b64 in enumerate(images_b64):
        send_b64 = img_b64
        if tokenizer and HAS_TESSERACT:
            try:
                img_bytes = base64.b64decode(img_b64)
                pil_img = Image.open(BytesIO(img_bytes))
                send_b64 = tokenizer.redacted_image_to_b64(pil_img)
            except Exception:
                pass
        pnum = page_nums[i] if page_nums and i < len(page_nums) else i + 1
        content.append({"type": "text", "text": f"--- PAGE {pnum} ---"})
        content.append({"type": "image", "source": {"type": "base64", "media_type": "image/jpeg", "data": send_b64}})
    content.append({"type": "text", "text": prompt})

    tokens = MAX_TOKENS * min(len(images_b64), 3)  # Scale tokens for multi-page

    for attempt in range(_MAX_RETRIES):
        try:
            msg = client.messages.create(
                model=MODEL, max_tokens=tokens,
                messages=[{"role": "user", "content": content}]
            )

            if _cost_tracker and hasattr(msg, 'usage'):
                _cost_tracker.record(phase, page_nums[0] if page_nums else None, msg.usage, "vision_multi")

            stop_reason = getattr(msg, 'stop_reason', None)
            if stop_reason == 'max_tokens' and tokens < 16000:
                tokens = min(tokens * 2, 16000)
                continue

            result = _parse_json_response(msg.content[0].text, tag)

            if result is None and attempt < _MAX_RETRIES - 1:
                _time.sleep(1)
                continue

            if tokenizer and result:
                result = tokenizer.detokenize_json(result)

            return result

        except Exception as e:
            if _is_retriable(e) and attempt < _MAX_RETRIES - 1:
                delay = _RETRY_BASE_DELAY * (2 ** attempt)
                print(f"  {tag}API error: {e} — retrying in {delay}s")
                _time.sleep(delay)
                continue
            else:
                print(f"  {tag}VISION MULTI ERROR: {e}")
                return None

    return None

def _parse_json_response(text, tag=""):
    """Parse JSON from Claude's response, handling markdown fences and truncation."""
    text = text.strip()
    if text.startswith("```"):
        text = text.split("\n", 1)[1] if "\n" in text else text[3:]
        if text.endswith("```"): text = text[:-3]
        text = text.strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        # Try to extract JSON object from surrounding text
        start = text.find("{")
        end = text.rfind("}") + 1
        if start >= 0 and end > start:
            try: return json.loads(text[start:end])
            except (json.JSONDecodeError, ValueError): pass

        # Attempt to repair truncated JSON (missing closing braces)
        if start >= 0:
            fragment = text[start:]
            # Count unmatched braces/brackets and close them
            opens_b = fragment.count("{") - fragment.count("}")
            opens_a = fragment.count("[") - fragment.count("]")
            if opens_b > 0 or opens_a > 0:
                # Strip trailing comma or partial key
                repaired = fragment.rstrip().rstrip(",").rstrip(":")
                # Remove any incomplete key-value at the end
                last_comma = repaired.rfind(",")
                last_brace = max(repaired.rfind("{"), repaired.rfind("["))
                if last_comma > last_brace:
                    repaired = repaired[:last_comma]
                repaired += "]" * max(0, opens_a) + "}" * max(0, opens_b)
                try:
                    result = json.loads(repaired)
                    print(f"  {tag}WARNING: repaired truncated JSON ({opens_b} braces, {opens_a} brackets)")
                    return result
                except (json.JSONDecodeError, ValueError):
                    pass

        print(f"  {tag}WARNING: JSON parse failed ({len(text)} chars)")
        return None


# ─── PHASE 1: CLASSIFY (always vision — need to see page layout) ─────────

def classify_pages(client, b64_images, tokenizer=None, doc_type=None, user_notes="", ai_instructions=""):
    print(f"\n── Phase 1: Classification ({len(b64_images)} pages, {MAX_CONCURRENT} concurrent) ──")
    results = [None] * len(b64_images)

    # Narrow classification prompt to relevant doc types
    prompt = CLASSIFICATION_PROMPT
    if doc_type and doc_type in DOC_TYPE_CLASSIFICATIONS:
        narrowed = ", ".join(DOC_TYPE_CLASSIFICATIONS[doc_type])
        prompt = prompt.rsplit("Document types:", 1)[0] + "Document types:\n" + narrowed
        print(f"  (narrowed to {doc_type} types: {len(DOC_TYPE_CLASSIFICATIONS[doc_type])} categories)")
    if ai_instructions:
        prompt += f"\n\nSPECIAL INSTRUCTIONS FROM OPERATOR:\n{ai_instructions}"
        print(f"  AI instructions: {ai_instructions[:100]}{'...' if len(ai_instructions) > 100 else ''}")
    if user_notes:
        prompt += f"\n\nADDITIONAL CONTEXT: {user_notes}"

    def _classify_one(i, img):
        r = call_claude_vision(client, img, prompt, i+1, tokenizer=tokenizer, phase="classify")
        if r:
            r["page_number"] = i + 1
        else:
            r = {"page_number": i + 1, "document_type": "unknown"}
        return i, r

    with ThreadPoolExecutor(max_workers=MAX_CONCURRENT) as pool:
        futures = {pool.submit(_classify_one, i, img): i for i, img in enumerate(b64_images)}
        for future in as_completed(futures):
            i, r = future.result()
            results[i] = r
            dtype = r.get("document_type", "?")
            subs = r.get("sub_types", [])
            entity = r.get("payer_or_entity", "?")
            ein = r.get("payer_ein", "")
            flags = []
            if r.get("is_consolidated_brokerage"): flags.append("BROKERAGE")
            if r.get("is_supplemental_detail"): flags.append("detail")
            if r.get("is_continuation_statement"): flags.append("continuation")
            flag_str = f" [{', '.join(flags)}]" if flags else ""
            sub_str = f" → {subs}" if subs and len(subs) > 1 else ""
            print(f"  Page {i+1}: {dtype}{sub_str} — {entity} (EIN: {ein}){flag_str}")

    return results


# ─── PHASE 1.5: GROUP PAGES ──────────────────────────────────────────────────

def group_pages(classifications):
    """Group pages by EIN (primary) or entity name (fallback)."""
    groups = []
    current = None

    for cls in classifications:
        pnum = cls["page_number"]
        dtype = cls.get("document_type", "unknown")
        entity = cls.get("payer_or_entity") or ""
        ein = cls.get("payer_ein") or ""
        is_supp = cls.get("is_supplemental_detail", False)
        is_cont = cls.get("is_continuation_statement", False)
        is_brokerage = cls.get("is_consolidated_brokerage", False)

        if is_supp and not is_cont and current:
            current.setdefault("supplemental_pages", []).append(pnum)
            continue

        if is_cont and current:
            current.setdefault("continuation_pages", []).append(pnum)
            continue

        if current:
            same_ein = (ein and ein == current.get("payer_ein", ""))
            same_name = (entity.upper().strip() == current.get("payer_or_entity", "").upper().strip())
            same_dtype = (dtype == current.get("document_type"))
            same_recip = ((cls.get("recipient") or "").upper() == (current.get("recipient") or "").upper())
            if same_dtype and (same_ein or same_name) and same_recip:
                current["pages"].append(pnum)
                if is_brokerage:
                    current["is_consolidated_brokerage"] = True
                    for st in cls.get("sub_types", []):
                        if st not in current.get("sub_types", []):
                            current.setdefault("sub_types", []).append(st)
                continue

        if current:
            groups.append(current)
        current = {
            "document_type": dtype,
            "payer_or_entity": entity,
            "payer_ein": ein,
            "recipient": cls.get("recipient", ""),
            "tax_year": cls.get("tax_year", ""),
            "pages": [pnum],
            "supplemental_pages": [],
            "continuation_pages": [],
            "is_consolidated_brokerage": is_brokerage,
            "sub_types": cls.get("sub_types", []),
        }

    if current:
        groups.append(current)
    return groups


# ─── PHASE 2: EXTRACT (Vision) ───────────────────────────────────────────────

def extract_data(client, b64_images, groups, tokenizer=None, doc_type=None, user_notes="", ai_instructions="", ocr_texts=None):
    """
    For each page, try OCR-text extraction first (cheap text call).
    If OCR quality is poor or Claude flags needs_image_review, fall back to
    vision extraction. PII redaction (if tokenizer provided) blacks out
    sensitive data in images. Uses concurrent API calls for speed.
    """
    print(f"\n── Phase 2: Extraction ({MAX_CONCURRENT} concurrent) ──")

    # Build context hints based on doc_type
    doc_type_hints = {
        "bookkeeping": "\nThis is a BOOKKEEPING document. Focus on: transaction dates, amounts, accounts, vendor names, categories. Extract individual line items and transactions when present.",
        "bank_statements": "\nThis is a BANK STATEMENT. Focus on: account balances AND every individual transaction. Use numbered fields: txn_1_date, txn_1_desc, txn_1_amount, txn_1_type, txn_2_date, etc. Extract ALL visible transactions — deposits, withdrawals, checks, fees, transfers. Include check numbers in desc field.",
    }
    extra_context = doc_type_hints.get(doc_type, "")
    if ai_instructions:
        extra_context += f"\n\nSPECIAL INSTRUCTIONS FROM OPERATOR — follow these carefully:\n{ai_instructions}"
        print(f"  AI instructions: {ai_instructions[:100]}{'...' if len(ai_instructions) > 100 else ''}")
    if user_notes:
        extra_context += f"\n\nADDITIONAL CONTEXT: {user_notes}"

    # Separate groups into single-page work items vs multi-page batches
    work_items = []       # (group, page_number) for single-page extraction
    multipage_groups = [] # groups with continuations → batched extraction

    for group in groups:
        dtype = group["document_type"]
        entity = group["payer_or_entity"]
        ein = group.get("payer_ein", "")
        is_brokerage = group.get("is_consolidated_brokerage", False)
        cont_pages = group.get("continuation_pages", [])
        all_pages = group["pages"] + cont_pages
        print(f"\n  [{dtype}] {entity} (EIN: {ein}){' [BROKERAGE]' if is_brokerage else ''}")

        # Multi-page batch: K-1 with continuations or multi-page brokerage
        # Send all pages in one call so Claude can cross-reference STMT values
        if cont_pages and ("K-1" in dtype or is_brokerage):
            multipage_groups.append(group)
            print(f"    → multi-page batch ({len(all_pages)} pages: {all_pages})")
        else:
            for pnum in all_pages:
                work_items.append((group, pnum))

    # ─── Process multi-page groups first (K-1 + continuations in one call) ───
    extractions = []
    for group in multipage_groups:
        dtype = group["document_type"]
        ein = group.get("payer_ein", "")
        entity = group["payer_or_entity"]
        is_brokerage = group.get("is_consolidated_brokerage", False)
        all_pages = group["pages"] + group.get("continuation_pages", [])

        # Build multi-page prompt
        context = f"The document is classified as: {dtype}" + extra_context
        multi_prompt = VISION_EXTRACTION_PROMPT.replace("{context}", context)
        multi_prompt += (
            "\n\nMULTI-PAGE DOCUMENT: You are seeing ALL pages of this document together. "
            "If any box shows 'STMT' or '* STMT', look at the continuation statement pages "
            "to find the actual values. Resolve ALL continuation references and return the "
            "final values (not 'STMT'). Return a SINGLE JSON with all fields from all pages combined."
        )

        imgs = [b64_images[p - 1] for p in all_pages]
        r = call_claude_vision_multipage(client, imgs, multi_prompt,
                                          page_nums=all_pages, tokenizer=tokenizer,
                                          phase="extract_multi")
        if r:
            r["_group"] = group
            r["_page"] = all_pages[0]  # Primary page
            r["_pages_batched"] = all_pages
            r["_is_brokerage"] = is_brokerage
            r["_extraction_method"] = "vision_multipage"
            r["payer_ein"] = ein or r.get("payer_ein", "")
            extractions.append(r)
            print(f"    Multi-page extracted: {entity} ({len(all_pages)} pages → 1 call)")
            for fname, fdata in r.get("fields", {}).items():
                val = fdata.get("value") if isinstance(fdata, dict) else fdata
                if val is not None and val != 0 and val != "0" and val != 0.0:
                    conf = fdata.get("confidence", "?") if isinstance(fdata, dict) else "?"
                    print(f"      {fname}: {val} ({conf})")
        else:
            # Fallback: extract pages individually
            print(f"    Multi-page failed — falling back to individual pages")
            for pnum in all_pages:
                work_items.append((group, pnum))

    # ─── Process single-page work items ───

    def _extract_one(group, pnum):
        dtype = group["document_type"]
        ein = group.get("payer_ein", "")
        is_brokerage = group.get("is_consolidated_brokerage", False)
        context = f"The document is classified as: {dtype}" + extra_context

        # ─── OCR-first path: try cheap text call when OCR is good ───
        ocr_text = ocr_texts[pnum - 1] if ocr_texts and pnum <= len(ocr_texts) else None
        method = "vision"  # default

        if ocr_text and len(ocr_text.strip()) >= 200:
            text_prompt = OCR_EXTRACTION_PROMPT.replace("{doc_type}", dtype)
            if extra_context:
                text_prompt += extra_context
            r = call_claude_text(client, ocr_text, text_prompt, pnum, tokenizer=tokenizer, phase="extract_text")
            if r:
                ocr_quality = r.get("ocr_quality", "partial")
                needs_image = r.get("needs_image_review", False)
                fields_needing_image = r.get("fields_needing_image", [])

                if ocr_quality == "good" and not needs_image:
                    method = "ocr_text"
                    print(f"    Page {pnum}: OCR sufficient → text extraction (saved vision call)")
                elif ocr_quality == "partial" and not needs_image and fields_needing_image:
                    method = "ocr_partial"
                    r["_ambiguous_fields"] = fields_needing_image
                    print(f"    Page {pnum}: OCR partial — {len(fields_needing_image)} fields need image check")
                else:
                    r = None
                    print(f"    Page {pnum}: OCR quality '{ocr_quality}' — falling back to vision")

                if r:
                    r["_group"] = group
                    r["_page"] = pnum
                    r["_is_brokerage"] = is_brokerage
                    r["_extraction_method"] = method
                    r["payer_ein"] = ein or r.get("payer_ein", "")
                    return pnum, r

        # ─── Vision path (default or fallback) ───
        prompt_v = VISION_EXTRACTION_PROMPT.replace("{context}", context)
        r = call_claude_vision(client, b64_images[pnum-1], prompt_v, pnum, tokenizer=tokenizer, phase="extract_vision")
        if r:
            r["_group"] = group
            r["_page"] = pnum
            r["_is_brokerage"] = is_brokerage
            r["_extraction_method"] = "vision"
            r["payer_ein"] = ein or r.get("payer_ein", "")
            print(f"    Page {pnum}: vision extracted")
        return pnum, r

    total = len(work_items)
    ocr_saved = 0

    with ThreadPoolExecutor(max_workers=MAX_CONCURRENT) as pool:
        futures = {pool.submit(_extract_one, grp, pn): pn for grp, pn in work_items}
        for future in as_completed(futures):
            pnum, r = future.result()
            if r:
                extractions.append(r)
                m = r.get("_extraction_method", "vision")
                if m in ("ocr_text", "ocr_partial"):
                    ocr_saved += 1
                for fname, fdata in r.get("fields", {}).items():
                    val = fdata.get("value") if isinstance(fdata, dict) else fdata
                    if val is not None and val != 0 and val != "0" and val != 0.0:
                        conf = fdata.get("confidence", "?") if isinstance(fdata, dict) else "?"
                        print(f"      {fname}: {val} ({conf})")
            else:
                print(f"    Page {pnum}: extraction failed")

    # Sort by page number to maintain document order
    extractions.sort(key=lambda e: e.get("_page", 0))

    print(f"\n  Extraction stats: {total} pages processed, {len(extractions)} successful")
    if ocr_saved:
        print(f"  OCR-first: {ocr_saved}/{total} pages used text extraction (vision calls saved)")
    return extractions


# ─── PHASE 3: VERIFY (cross-check OCR vs image for critical fields) ──────────

def verify_extractions(client, b64_images, extractions, tokenizer=None):
    """
    Verification strategy:
      - Vision-extracted pages: send image + extracted values for re-read
      - Focus on CRITICAL_FIELDS to minimize API calls
      - Uses concurrent API calls for speed
    """
    print(f"\n── Phase 3: Verification ({MAX_CONCURRENT} concurrent) ──")
    corrections = 0
    confirmations = 0
    total_fields = 0

    # Separate extractions into those needing API verification and those that can skip
    skip_indices = set()
    verify_work = []  # (index_in_extractions, prompt, page)

    for idx, ext in enumerate(extractions):
        page = ext.get("_page")
        fields = ext.get("fields", {})
        method = ext.get("_extraction_method", "unknown")
        if not fields or page is None:
            skip_indices.add(idx)
            continue

        dtype = ext.get("document_type", "")
        entity = ext.get("payer_or_entity", "")

        # Decide which fields need verification
        fields_to_verify = {}
        ambiguous_fields = ext.get("_ambiguous_fields", [])
        has_critical = False

        for fname, fdata in fields.items():
            val = fdata.get("value") if isinstance(fdata, dict) else fdata
            if val is None: continue
            is_critical = fname in CRITICAL_FIELDS or fname in ambiguous_fields
            is_unclear = isinstance(fdata, dict) and not fdata.get("ocr_clear", True)
            is_low_conf = isinstance(fdata, dict) and fdata.get("confidence") == "low"
            if is_critical or is_unclear or is_low_conf:
                label = fdata.get("label_on_form", "") if isinstance(fdata, dict) else ""
                fields_to_verify[fname] = {"value": val, "label": label}
                has_critical = True

        if not has_critical and method == "ocr_only":
            print(f"  Page {page}: {dtype} — no critical fields, skipping verification")
            for fname in fields:
                if isinstance(fields[fname], dict):
                    fields[fname]["confidence"] = "ocr_accepted"
            skip_indices.add(idx)
            continue

        # OCR-text extractions with all-high/good confidence can skip verification
        # (the OCR was clear enough that Claude rated it "good" quality)
        if method == "ocr_text" and not has_critical:
            print(f"  Page {page}: {dtype} — OCR-text, all high confidence, skipping verification")
            for fname in fields:
                if isinstance(fields[fname], dict):
                    fields[fname]["confidence"] = "ocr_accepted"
            skip_indices.add(idx)
            continue

        # Multi-page extractions already cross-referenced — mark as higher confidence
        if method == "vision_multipage" and not has_critical:
            print(f"  Page {page}: {dtype} — multi-page extraction, no critical issues, skipping verification")
            for fname in fields:
                if isinstance(fields[fname], dict) and fields[fname].get("confidence") in ("high", "medium"):
                    fields[fname]["confidence"] = "multipage_verified"
            skip_indices.add(idx)
            continue

        if fields_to_verify:
            print(f"  Page {page}: {dtype} — {entity} — verifying {len(fields_to_verify)} fields [{method}]")
            prompt = VERIFY_CROSSCHECK_PROMPT.replace("{extracted_json}", json.dumps(fields_to_verify, indent=2))
        else:
            all_fields = {}
            for fname, fdata in fields.items():
                val = fdata.get("value") if isinstance(fdata, dict) else fdata
                if val is not None:
                    label = fdata.get("label_on_form", "") if isinstance(fdata, dict) else ""
                    all_fields[fname] = {"value": val, "label": label}
            print(f"  Page {page}: {dtype} — {entity} — verifying all {len(all_fields)} fields [{method}]")
            prompt = VERIFY_CROSSCHECK_PROMPT.replace("{extracted_json}", json.dumps(all_fields, indent=2))

        verify_work.append((idx, prompt, page))

    # Run all verification API calls concurrently
    def _verify_one(idx, prompt, page):
        vresult = call_claude_vision(client, b64_images[page - 1], prompt, page, tokenizer=tokenizer, phase="verify")
        return idx, vresult

    results_map = {}
    if verify_work:
        with ThreadPoolExecutor(max_workers=MAX_CONCURRENT) as pool:
            futures = {pool.submit(_verify_one, idx, p, pg): idx for idx, p, pg in verify_work}
            for future in as_completed(futures):
                idx, vresult = future.result()
                results_map[idx] = vresult

    # Apply verification results back to extractions (sequential — mutates fields)
    for idx, ext in enumerate(extractions):
        if idx in skip_indices:
            continue
        fields = ext.get("fields", {})
        method = ext.get("_extraction_method", "unknown")
        page = ext.get("_page")
        vresult = results_map.get(idx)

        if vresult and "verification_results" in vresult:
            vr = vresult["verification_results"]
            for fname, vdata in vr.items():
                total_fields += 1
                status = vdata.get("status", "")
                if status == "corrected":
                    final = vdata.get("final_value") or vdata.get("image_value") or vdata.get("verified_value")
                    extracted = vdata.get("extracted_value")
                    if final is not None:
                        corrections += 1
                        if fname in fields and isinstance(fields[fname], dict):
                            fields[fname]["value"] = final
                            fields[fname]["confidence"] = "verified_corrected"
                            fields[fname]["original_value"] = extracted
                            fields[fname]["correction_note"] = vdata.get("notes", "")
                        print(f"    CORRECTED: {fname}: {extracted} → {final} ({vdata.get('notes','')})")
                elif status == "confirmed":
                    confirmations += 1
                    if fname in fields and isinstance(fields[fname], dict):
                        if method.startswith("ocr"):
                            fields[fname]["confidence"] = "dual_confirmed"
                        else:
                            fields[fname]["confidence"] = "verified_confirmed"

            for mf in vresult.get("missing_fields", []):
                fname = mf.get("field_name", "")
                val = mf.get("value")
                if fname and val is not None:
                    fields[fname] = {
                        "value": val,
                        "label_on_form": mf.get("label_on_form", ""),
                        "confidence": "found_in_verification",
                    }
                    print(f"    NEW: {fname}: {val}")

            ext["_verification"] = vresult
            ext["_overall_confidence"] = vresult.get("overall_confidence", "medium")
        else:
            ext["_overall_confidence"] = "unverified"
            print(f"    Page {page}: Verification failed — keeping extraction as-is")

    print(f"\n  Verification: {confirmations} confirmed, {corrections} corrected, {total_fields} fields checked")
    return extractions


# ─── PHASE 4: NORMALIZE ──────────────────────────────────────────────────────

def normalize_brokerage_data(extractions):
    """Split brokerage composites, cross-ref K-1 continuations, roll up K-1 interest."""
    print("\n── Phase 4: Normalize ──")
    normalized = []
    rollups = []
    continuation_data = {}

    # Collect continuation statement data by EIN
    for ext in extractions:
        if "continuation" in str(ext.get("document_type", "")).lower():
            ein = ext.get("payer_ein", "")
            entity = ext.get("payer_or_entity", "")
            key = ein if ein else entity.upper().strip()
            if key not in continuation_data:
                continuation_data[key] = []
            for ci in ext.get("continuation_items", []):
                continuation_data[key].append(ci)
            for fname, fdata in ext.get("fields", {}).items():
                val = fdata.get("value") if isinstance(fdata, dict) else fdata
                if val is not None:
                    continuation_data[key].append({
                        "line_reference": fname,
                        "description": fdata.get("label_on_form", "") if isinstance(fdata, dict) else "",
                        "amount": val,
                    })

    for ext in extractions:
        is_brokerage = ext.get("_is_brokerage", False)
        fields = ext.get("fields", {})
        entity = ext.get("payer_or_entity", "")
        ein = ext.get("payer_ein", "")

        if is_brokerage:
            div_f, int_f, b_f, misc_f = {}, {}, {}, {}

            for fname, fdata in fields.items():
                if fname.startswith("div_"):
                    div_f[fname[4:]] = fdata
                elif fname.startswith("int_"):
                    int_f[fname[4:]] = fdata
                elif fname.startswith("b_"):
                    b_f[fname[2:]] = fdata
                elif fname.startswith("misc_"):
                    misc_f[fname[5:]] = fdata
                else:
                    fl = fname.lower()
                    if any(k in fl for k in ["dividend", "qualified"]): div_f[fname] = fdata
                    elif any(k in fl for k in ["interest_income", "us_savings", "treasury", "bond_premium"]): int_f[fname] = fdata
                    elif any(k in fl for k in ["proceeds", "cost_basis", "gain_loss", "wash_sale"]): b_f[fname] = fdata
                    elif any(k in fl for k in ["royalt", "other_income", "rent"]): misc_f[fname] = fdata

            base = {"payer_ein": ein, "recipient": ext.get("recipient", ""),
                    "_page": ext.get("_page"), "_source_name": entity,
                    "_overall_confidence": ext.get("_overall_confidence", "medium")}

            if div_f:
                normalized.append({**base, "document_type": "1099-DIV", "payer_or_entity": entity, "fields": div_f})
                print(f"  Brokerage → 1099-DIV: {entity}")
            if int_f:
                normalized.append({**base, "document_type": "1099-INT", "payer_or_entity": entity, "fields": int_f})
                print(f"  Brokerage → 1099-INT: {entity}")
            if b_f:
                sched_d = {}
                for k, v in b_f.items():
                    kl = k.lower()
                    if "short_term" in kl and "gain" in kl: sched_d["short_term_gain_loss"] = v
                    elif "long_term" in kl and "gain" in kl: sched_d["long_term_gain_loss"] = v
                    elif "total_gain" in kl or kl == "gain_loss": sched_d["total_gain_loss"] = v
                    else: sched_d[k] = v
                normalized.append({**base, "document_type": "1099-B", "payer_or_entity": entity, "fields": sched_d})
                print(f"  Brokerage → 1099-B: {entity}")
            if misc_f:
                normalized.append({**base, "document_type": "1099-MISC", "payer_or_entity": entity, "fields": misc_f})

            ext["_brokerage_split"] = True
        elif "continuation" in str(ext.get("document_type", "")).lower():
            continue  # Already processed
        else:
            # Strip prefixes from standalone 1099-DIV, 1099-INT, 1099-B, 1099-MISC fields
            # The extraction prompt always uses prefixed names (div_, int_, b_, misc_)
            # but the Excel template expects unprefixed names
            dtype_str = str(ext.get("document_type", "")).upper()
            if any(t in dtype_str for t in ["1099-DIV", "1099-INT", "1099-B", "1099-MISC"]):
                cleaned_fields = {}
                for fname, fdata in fields.items():
                    if fname.startswith("div_"):
                        cleaned_fields[fname[4:]] = fdata
                    elif fname.startswith("int_"):
                        cleaned_fields[fname[4:]] = fdata
                    elif fname.startswith("b_"):
                        cleaned_fields[fname[2:]] = fdata
                    elif fname.startswith("misc_"):
                        cleaned_fields[fname[5:]] = fdata
                    else:
                        cleaned_fields[fname] = fdata
                ext["fields"] = cleaned_fields
            normalized.append(ext)

        # K-1: cross-ref continuations for missing Box 2
        if "K-1" in str(ext.get("document_type", "")):
            k1_name = get_str(fields, "partnership_name") or entity
            k1_ein = ext.get("payer_ein", "")
            box2 = get_val(fields, "box2_rental_real_estate")

            if box2 is None or box2 == 0:
                cont_key = k1_ein if k1_ein else k1_name.upper().strip()
                for ci in continuation_data.get(cont_key, []):
                    ref = str(ci.get("line_reference", "")).lower()
                    desc = str(ci.get("description", "")).lower()
                    amt = ci.get("amount")
                    if amt is not None and amt != 0 and (
                        "8825" in ref or "rental" in ref or "rental" in desc or "box 2" in ref
                    ):
                        print(f"  K-1 cross-ref: {k1_name} Box 2 = {amt} (from continuation)")
                        fields["box2_rental_real_estate"] = {
                            "value": amt, "confidence": "from_continuation",
                            "label_on_form": f"From: {ci.get('line_reference','')}"
                        }
                        break

            # Roll up interest
            box5 = get_val(fields, "box5_interest")
            if box5 and box5 != 0:
                rollups.append({
                    "document_type": "_interest_rollup",
                    "payer_or_entity": k1_name,
                    "fields": {
                        "interest_income": {"value": box5, "confidence": "from_k1_box5"},
                        "us_savings_bonds_and_treasury": {"value": 0, "confidence": "n/a"},
                    },
                    "_source_name": f"{k1_name} (K-1 Box 5)",
                })
                print(f"  K-1 → Interest: {k1_name} Box 5 = {box5}")

    normalized.extend(rollups)
    return normalized


# ─── PHASE 5: VALIDATE ───────────────────────────────────────────────────────

def validate(extractions, prior_year_context=None):
    """Validate extracted data using accounting arithmetic and rules.

    This is deterministic — no LLM calls. Pure math and accounting logic.
    Checks fall into three categories:
      1. Arithmetic: do the numbers on the document add up?
      2. Relational: do related fields follow known constraints?
      3. Cross-document: do related documents agree?
    """
    print("\n── Phase 5: Validate ──")
    warnings = []

    # Per-extraction checks
    for ext in extractions:
        dtype = ext.get("document_type", "")
        entity = ext.get("payer_or_entity", "")
        fields = ext.get("fields", {})
        label = f"{dtype} ({entity})" if entity else dtype

        # ─── Confidence warnings (existing) ───
        for fname, fdata in fields.items():
            if isinstance(fdata, dict):
                conf = fdata.get("confidence", "")
                if conf == "verified_corrected":
                    warnings.append(f"CORRECTED: {label} — {fname}: {fdata.get('original_value')} → {fdata.get('value')}")
                elif conf == "low":
                    warnings.append(f"LOW CONFIDENCE: {label} — {fname} = {fdata.get('value')}")
                elif conf == "found_in_verification":
                    warnings.append(f"FOUND IN VERIFY: {label} — {fname} = {fdata.get('value')}")

        # ─── W-2: wage relationship checks ───
        if dtype == "W-2":
            wages = get_val(fields, "wages")
            ss_wages = get_val(fields, "ss_wages")
            medicare_wages = get_val(fields, "medicare_wages")
            fed_wh = get_val(fields, "federal_wh")
            state_wh = get_val(fields, "state_wh")
            ss_wh = get_val(fields, "ss_wh")
            med_wh = get_val(fields, "medicare_wh")

            if wages and medicare_wages and medicare_wages < wages * 0.8:
                warnings.append(f"CHECK: {label} — Medicare wages ({medicare_wages:,.2f}) << Box 1 wages ({wages:,.2f})")
            # SS wages capped at wage base ($168,600 for 2024, $176,100 for 2025)
            if wages and ss_wages and ss_wages > wages * 1.01:
                warnings.append(f"ARITH: {label} — SS wages ({ss_wages:,.2f}) > Box 1 wages ({wages:,.2f})")
            # SS withholding ≈ 6.2% of SS wages
            if ss_wages and ss_wh:
                expected_ss = ss_wages * 0.062
                if abs(ss_wh - expected_ss) > max(1.0, expected_ss * 0.02):
                    warnings.append(f"CHECK: {label} — SS WH ({ss_wh:,.2f}) ≠ 6.2% of SS wages ({expected_ss:,.2f})")
            # Medicare withholding ≈ 1.45% of Medicare wages (plus 0.9% above $200k)
            if medicare_wages and med_wh:
                expected_med = medicare_wages * 0.0145
                if medicare_wages > 200000:
                    expected_med += (medicare_wages - 200000) * 0.009
                if abs(med_wh - expected_med) > max(1.0, expected_med * 0.05):
                    warnings.append(f"CHECK: {label} — Medicare WH ({med_wh:,.2f}) vs expected ({expected_med:,.2f})")
            # Federal WH sanity: shouldn't exceed wages
            if wages and fed_wh and fed_wh > wages:
                warnings.append(f"ARITH: {label} — Federal WH ({fed_wh:,.2f}) > wages ({wages:,.2f})")

        # ─── 1099-DIV: qualified ≤ ordinary ───
        if "1099-DIV" in dtype:
            ordinary = get_val(fields, "ordinary_dividends") or get_val(fields, "div_ordinary_dividends")
            qualified = get_val(fields, "qualified_dividends") or get_val(fields, "div_qualified_dividends")
            if ordinary and qualified and qualified > ordinary + 0.01:
                warnings.append(f"ARITH: {label} — Qualified ({qualified:,.2f}) > Ordinary ({ordinary:,.2f})")

        # ─── 1099-R: taxable ≤ gross ───
        if "1099-R" in dtype:
            gross = get_val(fields, "gross_distribution")
            taxable = get_val(fields, "taxable_amount")
            fed_wh = get_val(fields, "federal_wh")
            if gross and taxable and taxable > gross + 0.01:
                warnings.append(f"ARITH: {label} — Taxable ({taxable:,.2f}) > Gross distribution ({gross:,.2f})")
            if gross and fed_wh and fed_wh > gross:
                warnings.append(f"ARITH: {label} — Federal WH ({fed_wh:,.2f}) > Gross ({gross:,.2f})")

        # ─── K-1: Box 2 / Box 15 confusion ───
        if "K-1" in dtype:
            box2 = get_val(fields, "box2_rental_real_estate")
            box15 = get_val(fields, "box15_credits")
            box1 = get_val(fields, "box1_ordinary_income")
            if box2 and box15 and box2 == box15 and box2 != 0:
                warnings.append(f"CHECK: {label} — Box 2 ({box2:,.2f}) = Box 15 ({box15:,.2f}), possible misassignment")
            if box2 and box2 > 0 and not box1:
                warnings.append(f"CHECK: {label} — Box 2 positive ({box2:,.2f}) with no Box 1; verify not credits")

        # ─── 1099-K: monthly totals ≈ gross ───
        if "1099-K" in dtype:
            gross = get_val(fields, "gross_amount")
            months = ["jan", "feb", "mar", "apr", "may", "jun",
                      "jul", "aug", "sep", "oct", "nov", "dec"]
            monthly_sum = sum(get_val(fields, m) or 0 for m in months)
            if gross and monthly_sum > 0 and abs(gross - monthly_sum) > 1.0:
                warnings.append(f"ARITH: {label} — Gross ({gross:,.2f}) ≠ sum of monthly ({monthly_sum:,.2f})")

        # ─── Bank statement reconciliation ───
        if "bank_statement" in dtype:
            begin = get_val(fields, "beginning_balance")
            end = get_val(fields, "ending_balance")
            deposits = get_val(fields, "total_deposits") or 0
            withdrawals = get_val(fields, "total_withdrawals") or 0
            fees = get_val(fields, "fees_charged") or 0
            interest = get_val(fields, "interest_earned") or 0

            if begin is not None and end is not None:
                # Fundamental accounting equation for bank statements:
                # ending = beginning + deposits - withdrawals - fees + interest
                expected_end = begin + deposits - withdrawals - fees + interest
                diff = abs(end - expected_end)
                if diff > 1.0:
                    warnings.append(
                        f"ARITH: {label} — Balance doesn't reconcile: "
                        f"begin ({begin:,.2f}) + dep ({deposits:,.2f}) - wdl ({withdrawals:,.2f}) "
                        f"- fees ({fees:,.2f}) + int ({interest:,.2f}) = {expected_end:,.2f}, "
                        f"but ending = {end:,.2f} (diff: {diff:,.2f})")

            # Transaction-level reconciliation
            txn_nums = sorted(set(int(m.group(1)) for k in fields
                                  for m in [re.match(r"txn_(\d+)_", k)] if m))
            if txn_nums and deposits:
                txn_deposits = sum(abs(get_val(fields, f"txn_{n}_amount") or 0)
                                   for n in txn_nums
                                   if (get_str(fields, f"txn_{n}_type") or "").lower() in
                                   ("deposit", "transfer in", "credit"))
                if txn_deposits > 0 and abs(txn_deposits - deposits) > 1.0:
                    warnings.append(
                        f"CHECK: {label} — Sum of deposit txns ({txn_deposits:,.2f}) "
                        f"≠ total deposits ({deposits:,.2f})")

        # ─── Credit card statement reconciliation ───
        if "credit_card" in dtype:
            prev = get_val(fields, "previous_balance") or 0
            payments = get_val(fields, "payments") or 0
            credits = get_val(fields, "credits") or 0
            purchases = get_val(fields, "purchases") or 0
            fees = get_val(fields, "fees_charged") or 0
            interest = get_val(fields, "interest_charged") or 0
            new_bal = get_val(fields, "new_balance")

            if new_bal is not None and prev:
                expected = prev - payments - credits + purchases + fees + interest
                diff = abs(new_bal - expected)
                if diff > 1.0:
                    warnings.append(
                        f"ARITH: {label} — CC balance doesn't reconcile: "
                        f"expected {expected:,.2f}, got {new_bal:,.2f} (diff: {diff:,.2f})")

        # ─── Invoice arithmetic ───
        if "invoice" in dtype:
            subtotal = get_val(fields, "subtotal")
            tax = get_val(fields, "tax_amount") or 0
            total = get_val(fields, "total_amount")
            if subtotal is not None and total is not None:
                expected = subtotal + tax
                if abs(total - expected) > 0.05:
                    warnings.append(
                        f"ARITH: {label} — subtotal ({subtotal:,.2f}) + tax ({tax:,.2f}) "
                        f"= {expected:,.2f}, but total = {total:,.2f}")

        # ─── Receipt arithmetic ───
        if "receipt" in dtype:
            subtotal = get_val(fields, "subtotal")
            tax = get_val(fields, "tax_amount") or 0
            total = get_val(fields, "total_amount")
            if subtotal is not None and total is not None:
                expected = subtotal + tax
                if abs(total - expected) > 0.10:
                    warnings.append(
                        f"ARITH: {label} — subtotal ({subtotal:,.2f}) + tax ({tax:,.2f}) "
                        f"= {expected:,.2f}, but total = {total:,.2f}")

        # ─── Payroll arithmetic ───
        if "check_stub" in dtype:
            gross = get_val(fields, "gross_pay")
            net = get_val(fields, "net_pay")
            fed = get_val(fields, "federal_wh") or 0
            state = get_val(fields, "state_wh") or 0
            ss = get_val(fields, "social_security") or 0
            med = get_val(fields, "medicare") or 0
            if gross and net:
                total_deductions = fed + state + ss + med
                expected_net = gross - total_deductions
                # Allow 10% tolerance because there may be other deductions
                # (401k, insurance, garnishments) we didn't extract
                if expected_net > 0 and abs(net - expected_net) > expected_net * 0.20:
                    warnings.append(
                        f"CHECK: {label} — gross ({gross:,.2f}) - known deductions ({total_deductions:,.2f}) "
                        f"= {expected_net:,.2f}, but net = {net:,.2f} "
                        f"(possible unextracted deductions: {expected_net - net:,.2f})")

        # ─── Loan statement check ───
        if "loan" in dtype or "mortgage" in dtype:
            principal = get_val(fields, "principal_paid") or 0
            interest = get_val(fields, "interest_paid") or 0
            escrow = get_val(fields, "escrow_paid") or 0
            payment = get_val(fields, "payment_amount")
            if payment and (principal + interest + escrow) > 0:
                component_sum = principal + interest + escrow
                if abs(payment - component_sum) > 1.0:
                    warnings.append(
                        f"CHECK: {label} — Payment ({payment:,.2f}) ≠ principal ({principal:,.2f}) "
                        f"+ interest ({interest:,.2f}) + escrow ({escrow:,.2f}) = {component_sum:,.2f}")

    # ─── Cross-document checks ───
    # Bank statements: consecutive months should have ending = next beginning
    bank_stmts = [e for e in extractions if "bank_statement" in str(e.get("document_type", ""))]
    if len(bank_stmts) > 1:
        for i in range(len(bank_stmts) - 1):
            e1 = bank_stmts[i]
            e2 = bank_stmts[i + 1]
            end1 = get_val(e1.get("fields", {}), "ending_balance")
            begin2 = get_val(e2.get("fields", {}), "beginning_balance")
            if end1 is not None and begin2 is not None and abs(end1 - begin2) > 0.01:
                ent1 = e1.get("payer_or_entity", "")
                ent2 = e2.get("payer_or_entity", "")
                if ent1.upper() == ent2.upper():
                    warnings.append(
                        f"CROSS-DOC: {ent1} — ending balance ({end1:,.2f}) ≠ "
                        f"next beginning balance ({begin2:,.2f})")

    # ─── Duplicate document detection ───
    # Flag when same payer + same doc type + same key amounts appear twice
    seen_docs = {}  # key → (entity, page, key_amount)
    for ext in extractions:
        dtype = str(ext.get("document_type", ""))
        entity = ext.get("payer_or_entity", "")
        ein = ext.get("payer_ein", "")
        fields = ext.get("fields", {})
        page = ext.get("_page", "?")

        # Build a fingerprint from key amounts depending on doc type
        key_vals = []
        if "W-2" in dtype:
            key_vals = [get_val(fields, "wages"), get_val(fields, "federal_wh")]
        elif "1099-DIV" in dtype:
            key_vals = [get_val(fields, "ordinary_dividends")]
        elif "1099-INT" in dtype:
            key_vals = [get_val(fields, "interest_income")]
        elif "1099-R" in dtype:
            key_vals = [get_val(fields, "gross_distribution")]
        elif "1099-NEC" in dtype:
            key_vals = [get_val(fields, "nonemployee_compensation")]
        elif "SSA-1099" in dtype:
            key_vals = [get_val(fields, "net_benefits")]
        elif "1098" in dtype:
            key_vals = [get_val(fields, "mortgage_interest")]

        # Only check if we have real values
        key_vals = [v for v in key_vals if v is not None and v != 0]
        if key_vals and (ein or entity):
            ident = ein or entity.upper().strip()
            fingerprint = f"{dtype}|{ident}|{'|'.join(f'{v:.2f}' for v in key_vals)}"
            if fingerprint in seen_docs:
                prev_entity, prev_page, _ = seen_docs[fingerprint]
                warnings.append(
                    f"CROSS-DOC: Possible duplicate — {dtype} from {entity} "
                    f"(page {page}) has same amounts as page {prev_page}. "
                    f"Check if scanned twice.")
            else:
                seen_docs[fingerprint] = (entity, page, key_vals)

    # ─── Prior-year variance detection ───
    if prior_year_context:
        prior_payers = {}
        for doc in prior_year_context.get("prior_year_data", {}).get("documents", []):
            for p in doc.get("payers", []):
                pein = p.get("ein", "")
                pname = p.get("name", "").upper().strip()
                pform = p.get("form_type", "")
                pamounts = p.get("amounts", [])
                key = pein if pein else pname
                if key and pamounts:
                    prior_payers[f"{pform}|{key}"] = {
                        "name": p.get("name", ""),
                        "form": pform,
                        "amount": max(pamounts),  # Use largest amount for comparison
                    }

        for ext in extractions:
            dtype = str(ext.get("document_type", ""))
            entity = ext.get("payer_or_entity", "")
            ein = ext.get("payer_ein", "")
            fields = ext.get("fields", {})

            # Get the primary dollar amount for this extraction
            current_amt = None
            if "W-2" in dtype:
                current_amt = get_val(fields, "wages")
            elif "1099-DIV" in dtype:
                current_amt = get_val(fields, "ordinary_dividends")
            elif "1099-INT" in dtype:
                current_amt = get_val(fields, "interest_income")
            elif "1099-R" in dtype:
                current_amt = get_val(fields, "gross_distribution")
            elif "1099-NEC" in dtype:
                current_amt = get_val(fields, "nonemployee_compensation")
            elif "K-1" in dtype:
                current_amt = get_val(fields, "box1_ordinary_income")

            if current_amt is not None and current_amt != 0:
                ident = ein if ein else entity.upper().strip()
                # Try to match against prior year
                for form_prefix in [dtype, dtype.split("-")[0] if "-" in dtype else dtype]:
                    prior_key = f"{form_prefix}|{ident}"
                    if prior_key in prior_payers:
                        prior = prior_payers[prior_key]
                        prior_amt = prior["amount"]
                        if prior_amt and prior_amt != 0:
                            pct_change = abs(current_amt - prior_amt) / abs(prior_amt) * 100
                            if pct_change > 50:
                                direction = "↑" if current_amt > prior_amt else "↓"
                                warnings.append(
                                    f"VARIANCE: {entity} {dtype} — {direction} "
                                    f"{pct_change:.0f}% vs prior year "
                                    f"(${current_amt:,.2f} vs PY ${prior_amt:,.2f})")
                        break

    if warnings:
        arith = sum(1 for w in warnings if w.startswith("ARITH"))
        checks = sum(1 for w in warnings if w.startswith("CHECK"))
        cross = sum(1 for w in warnings if w.startswith("CROSS"))
        variance = sum(1 for w in warnings if w.startswith("VARIANCE"))
        other = len(warnings) - arith - checks - cross - variance
        print(f"  ⚠ {len(warnings)} items: {arith} arithmetic, {checks} checks, "
              f"{cross} cross-doc, {variance} variance, {other} other")
        for w in warnings: print(f"    {w}")
    else:
        print("  ✓ No warnings")
    return warnings


# ─── PHASE 6: EXCEL OUTPUT ───────────────────────────────────────────────────

BOLD = Font(bold=True)
SECTION_FONT = Font(bold=True, size=11, color="1A252F")
COL_HEADER_FONT = Font(bold=True, size=9, color="FFFFFF")
COL_HEADER_FILL = PatternFill("solid", fgColor="2C3E50")
MONEY_FMT = '#,##0.00'
PCT_FMT = '0.00%'
DATE_FMT = 'MM/DD/YYYY'
SUM_FONT = Font(bold=True, size=10, color="1A252F")
SUM_FILL = PatternFill("solid", fgColor="D5D8DC")
FLAG_FILL = PatternFill("solid", fgColor="FFFDE7")        # Soft yellow — low confidence
CORRECTED_FILL = PatternFill("solid", fgColor="C8E6C9")   # Green — corrected
REVIEW_FILL = PatternFill("solid", fgColor="FFE0B2")      # Orange — needs human
CONFIRMED_FILL = PatternFill("solid", fgColor="E8F5E9")   # Light green — confirmed
DUAL_FILL = PatternFill("solid", fgColor="A5D6A7")        # Darker green — OCR + image agree
FLAG_FONT = Font(italic=True, color="CC0000")
ALT_ROW_FILL = PatternFill("solid", fgColor="F8F9FA")     # Alternating row background
DARK_HEADER_FILL = PatternFill("solid", fgColor="2C3E50")
DARK_HEADER_FONT = Font(bold=True, color="FFFFFF", size=10)
THIN_BORDER = openpyxl.styles.Border(
    bottom=openpyxl.styles.Side(style="thin", color="DEE2E6"),
)
SECTION_BORDER = openpyxl.styles.Border(
    bottom=openpyxl.styles.Side(style="medium", color="2C3E50"),
)
SUM_BORDER = openpyxl.styles.Border(
    top=openpyxl.styles.Side(style="double", color="2C3E50"),
    bottom=openpyxl.styles.Side(style="thin", color="999999"),
)

def populate_template(extractions, template_path, output_path, year, output_format="tax_review"):
    """Router: create workbook, delegate to format-specific function, save."""
    fmt_labels = {
        "tax_review": "Tax Review", "journal_entries": "Journal Entries",
        "account_balances": "Account Balances", "trial_balance": "Trial Balance",
        "transaction_register": "Transaction Register",
    }
    print(f"\n── Phase 6: Excel ({fmt_labels.get(output_format, output_format)}) ──")

    if template_path and os.path.exists(template_path):
        wb = openpyxl.load_workbook(template_path)
    else:
        wb = openpyxl.Workbook()

    sheet_name = str(year)
    if sheet_name in wb.sheetnames:
        ws = wb[sheet_name]
    else:
        ws = wb.create_sheet(sheet_name, 0)

    if output_format == "journal_entries":
        _populate_journal_entries(ws, extractions, year)
    elif output_format == "account_balances":
        _populate_account_balances(ws, extractions, year)
    elif output_format == "trial_balance":
        _populate_trial_balance(ws, extractions, year)
    elif output_format == "transaction_register":
        _populate_transaction_register(ws, extractions, year)
    else:
        _populate_tax_review(ws, extractions, year)

    ws.freeze_panes = "A4"

    # Print setup for all formats
    ws.sheet_properties.pageSetUpPr = openpyxl.worksheet.properties.PageSetupProperties(fitToPage=True)
    ws.page_setup.fitToWidth = 1
    ws.page_setup.fitToHeight = 0
    ws.page_setup.orientation = "landscape"
    ws.page_setup.paperSize = ws.PAPERSIZE_LETTER
    ws.oddHeader.center.text = f"&B{fmt_labels.get(output_format, 'Document Intake')} — {year}"
    ws.oddHeader.center.size = 10
    ws.oddFooter.left.text = "Bearden Accounting — Document Intake v5"
    ws.oddFooter.left.size = 8
    ws.oddFooter.right.text = "Page &P of &N"
    ws.oddFooter.right.size = 8
    ws.print_options.gridLines = True

    # Remove default sheet if it exists and is empty
    if "Sheet" in wb.sheetnames and wb["Sheet"].max_row <= 1:
        del wb["Sheet"]

    wb.save(output_path)
    print(f"  ✓ Saved: {output_path}")


# ─── Shared helpers for section-based rendering ──────────────────────────────

def _match_exts(extractions, match_types):
    matched = []
    for ext in extractions:
        dtype = str(ext.get("document_type", ""))
        for mt in match_types:
            if mt in dtype:
                matched.append(ext)
                break
    return matched

def _dedup_by_ein(exts):
    """Deduplicate extractions by EIN/entity, keeping the BEST version.

    When two copies of the same document exist (common with scanned returns),
    keep the one with more high-confidence fields instead of first-seen.
    Missing fields from the discarded copy are merged into the winner.
    """
    seen = {}       # key → (ext, score)
    order = []      # preserve insertion order of keys

    def _confidence_score(ext):
        """Score an extraction by field confidence. Higher = better."""
        score = 0
        for fdata in ext.get("fields", {}).values():
            if not isinstance(fdata, dict):
                score += 1  # bare value = ok
                continue
            c = fdata.get("confidence", "")
            if c in ("dual_confirmed", "verified_confirmed"):
                score += 3
            elif c in ("verified_corrected", "found_in_verification"):
                score += 2
            elif c in ("high", "ocr_accepted"):
                score += 2
            elif c in ("medium", "from_continuation", "from_k1_box5"):
                score += 1
            elif c in ("low",):
                score += 0
            else:
                score += 1
        return score

    for ext in exts:
        ein = ext.get("payer_ein", "")
        fields = ext.get("fields", {})
        name = get_str(fields, "partnership_name") or get_str(fields, "employer_name") or ext.get("payer_or_entity", "")
        recip = ext.get("recipient", "")
        if ein:
            key = f"EIN:{ein}|{recip}".upper()
        else:
            norm = re.sub(r'\s*\(\d{4}\)\s*', '', name).upper().strip()
            key = f"NAME:{norm}|{recip}".upper()

        score = _confidence_score(ext)

        if key not in seen:
            seen[key] = (ext, score)
            order.append(key)
        else:
            existing_ext, existing_score = seen[key]
            if score > existing_score:
                # New copy is better — swap, merge missing fields from old
                for fname, fdata in existing_ext.get("fields", {}).items():
                    if fname not in ext.get("fields", {}):
                        ext.setdefault("fields", {})[fname] = fdata
                seen[key] = (ext, score)
                print(f"  Dedup: kept better copy of {name or ein} (score {score} > {existing_score})")
            else:
                # Existing is better — merge missing fields from new
                for fname, fdata in ext.get("fields", {}).items():
                    if fname not in existing_ext.get("fields", {}):
                        existing_ext.setdefault("fields", {})[fname] = fdata

    return [seen[k][0] for k in order]

def _write_title(ws, title, year):
    """Write title rows, return next row number."""
    ws["A1"] = f"{title} — {year}"
    ws["A1"].font = Font(bold=True, size=16, color="1A252F")
    ws.merge_cells("A1:G1")
    ws["A2"] = f"Extracted {datetime.now().strftime('%m/%d/%Y %I:%M %p')} — Bearden Tax Intake v5"
    ws["A2"].font = Font(italic=True, color="999999", size=9)
    ws.merge_cells("A2:G2")
    # Thin line under header area
    for col_letter in ["A", "B", "C", "D", "E", "F", "G"]:
        ws[f"{col_letter}3"].border = openpyxl.styles.Border(
            bottom=openpyxl.styles.Side(style="thin", color="DEE2E6"))
    return 4

def _write_cell_value(ws, col, row, fields, field_name, ext, matched):
    """Write a single field cell with formatting and confidence coloring."""
    if field_name == "_display_name":
        name = get_str(fields, "partnership_name") or ext.get("payer_or_entity", "")
        recip = ext.get("recipient", "")
        same = [e for e in matched if (e.get("payer_ein","") == ext.get("payer_ein","") and ext.get("payer_ein",""))
                or (get_str(e.get("fields",{}), "partnership_name") or e.get("payer_or_entity","")).upper() == name.upper()]
        if len(same) > 1 and recip:
            name = f"{name} - {recip}"
        ws[f"{col}{row}"] = name
    elif field_name == "_source_name":
        ws[f"{col}{row}"] = ext.get("_source_name", ext.get("payer_or_entity", ""))
    elif field_name == "_carryover_prior":
        cell = ws[f"{col}{row}"]
        cell.value = 0
        cell.number_format = MONEY_FMT
        cell.alignment = Alignment(horizontal="right")
        cell.fill = REVIEW_FILL
        cell.comment = Comment("REQUIRES PRIOR YEAR DATA", "System")
    elif field_name in ("_allowed", "_carryover_next"):
        ws[f"{col}{row}"] = None
        ws[f"{col}{row}"].fill = REVIEW_FILL
        ws[f"{col}{row}"].comment = Comment("REQUIRES PREPARER JUDGMENT", "System")
    elif field_name == "employer_name":
        name = get_str(fields, "employer_name") or ext.get("payer_or_entity", "")
        name = re.sub(r'\s*\(\d{4}\)\s*$', '', name)
        ws[f"{col}{row}"] = name
    elif field_name in ("payer_or_entity", "institution_name"):
        ws[f"{col}{row}"] = get_str(fields, field_name) or ext.get("payer_or_entity", "")
    else:
        val = get_val(fields, field_name)
        if val is None:
            val = get_str(fields, field_name)
        cell = ws[f"{col}{row}"]
        cell.value = val
        if isinstance(val, (int, float)):
            cell.number_format = MONEY_FMT
            cell.alignment = Alignment(horizontal="right")
        # Confidence coloring
        fdata = fields.get(field_name)
        if isinstance(fdata, dict):
            conf = fdata.get("confidence", "")
            if conf == "dual_confirmed":
                cell.fill = DUAL_FILL
                cell.comment = Comment("DUAL CONFIRMED: OCR + image agree", "System")
            elif conf == "verified_corrected":
                cell.fill = CORRECTED_FILL
                cell.comment = Comment(f"CORRECTED: was {fdata.get('original_value','?')}. {fdata.get('correction_note','')}", "System")
            elif conf in ("verified_confirmed", "ocr_accepted"):
                cell.fill = CONFIRMED_FILL
            elif conf == "low":
                cell.fill = FLAG_FILL
                cell.comment = Comment("LOW CONFIDENCE — check source", "System")
            elif conf in ("found_in_verification", "from_continuation"):
                cell.fill = CORRECTED_FILL
                cell.comment = Comment(f"Source: {conf}", "System")
            elif conf == "operator_corrected":
                cell.fill = PatternFill("solid", fgColor="B3E5FC")
                cell.comment = Comment(f"Operator corrected (was {fdata.get('_original_value','?')})", "Operator")


# ─── TAX REVIEW (original format) ────────────────────────────────────────────

def _populate_tax_review(ws, extractions, year):
    row = _write_title(ws, "Document Intake", year)
    k1_extras = []

    for section in TEMPLATE_SECTIONS:
        sid = section["id"]

        if section.get("special") == "k1_extras":
            if k1_extras:
                ws[f"A{row}"] = section["header"]
                ws[f"A{row}"].font = SECTION_FONT
                ws[f"A{row}"].border = SECTION_BORDER
                for hcol, hlabel in [("B", "Line Ref"), ("C", "Description"), ("D", "Amount")]:
                    cell = ws[f"{hcol}{row}"]
                    cell.value = hlabel
                    cell.font = COL_HEADER_FONT
                    cell.fill = COL_HEADER_FILL
                    cell.border = SECTION_BORDER
                    if hcol == "D":
                        cell.alignment = Alignment(horizontal="right")
                row += 1
                for item in k1_extras:
                    ws[f"A{row}"] = item.get("entity", "")
                    ws[f"B{row}"] = item.get("line_reference", "")
                    ws[f"C{row}"] = item.get("description", "")
                    amt_cell = ws[f"D{row}"]
                    amt_cell.value = item.get("amount")
                    if isinstance(amt_cell.value, (int, float)):
                        amt_cell.number_format = MONEY_FMT
                        amt_cell.alignment = Alignment(horizontal="right")
                    for c in ["A", "B", "C", "D"]:
                        ws[f"{c}{row}"].border = THIN_BORDER
                    row += 1
                row += 1
            continue

        match_types = section.get("match_types", [])
        matched = _match_exts(extractions, match_types) if match_types else []
        col_headers = section.get("col_headers", {})
        columns = section.get("columns", {})
        sum_cols = section.get("sum_cols", [])
        flags = section.get("flags", [])

        if not matched and sid not in ALWAYS_SHOW:
            continue

        ws[f"A{row}"] = section["header"]
        ws[f"A{row}"].font = SECTION_FONT
        ws[f"A{row}"].border = SECTION_BORDER
        for col, label in col_headers.items():
            cell = ws[f"{col}{row}"]
            cell.value = label
            cell.font = COL_HEADER_FONT
            cell.fill = COL_HEADER_FILL
            cell.alignment = Alignment(horizontal="right")
            cell.border = SECTION_BORDER
        row += 1

        if not matched:
            ws[f"A{row}"] = "(no documents found)"
            ws[f"A{row}"].font = Font(italic=True, color="BBBBBB")
            row += 2
            continue

        matched = _dedup_by_ein(matched)
        data_start = row
        all_cols = list(columns.keys())

        for ext_idx, ext in enumerate(matched):
            fields = ext.get("fields", {})
            for col, field_name in columns.items():
                _write_cell_value(ws, col, row, fields, field_name, ext, matched)

            # Alternating row fill (only for cells without confidence coloring)
            if ext_idx % 2 == 1:
                for bcol in all_cols:
                    cell = ws[f"{bcol}{row}"]
                    if cell.fill == PatternFill() or cell.fill is None:
                        cell.fill = ALT_ROW_FILL

            # K-1 extras
            if sid == "k1":
                entity_name = get_str(fields, "partnership_name") or ext.get("payer_or_entity", "")
                extras_map = {
                    "box5_interest": "Box 5 (Interest)",
                    "box6a_ordinary_dividends": "Box 6a (Ordinary Dividends)",
                    "box6b_qualified_dividends": "Box 6b (Qualified Dividends)",
                    "box7_royalties": "Box 7 (Royalties)",
                    "box8_short_term_capital_gain": "Box 8 (ST Cap Gain)",
                    "box9a_long_term_capital_gain": "Box 9a (LT Cap Gain)",
                    "box9c_unrecaptured_1250": "Box 9c (Unrec 1250)",
                    "box10_net_1231_gain": "Box 10 (1231 Gain)",
                    "box11_other_income": "Box 11 (Other Income)",
                    "box12_section_179": "Box 12 (Sec 179)",
                    "box13_other_deductions": "Box 13 (Other Ded)",
                    "box14_self_employment": "Box 14 (SE Earnings)",
                    "box15_credits": "Box 15 (Credits)",
                    "box17_alt_min_tax": "Box 17 (AMT)",
                    "box18_tax_exempt_income": "Box 18 (Tax Exempt)",
                    "box19_distributions": "Box 19 (Distributions)",
                    "box20_other_info": "Box 20 (Other Info)",
                }
                for fkey, label in extras_map.items():
                    val = get_val(fields, fkey)
                    if val and val != 0:
                        k1_extras.append({"entity": entity_name, "line_reference": label, "description": "", "amount": val})
                for ci in ext.get("continuation_items", []):
                    if ci.get("amount") and ci["amount"] != 0:
                        k1_extras.append({"entity": entity_name, "line_reference": ci.get("line_reference", ""), "description": ci.get("description", ""), "amount": ci["amount"]})

            for bcol in list(columns.keys()):
                ws[f"{bcol}{row}"].border = THIN_BORDER
            row += 1

        data_end = row - 1
        if data_end >= data_start and sum_cols:
            for col in sum_cols:
                cell = ws[f"{col}{row}"]
                cell.value = f"=SUM({col}{data_start}:{col}{data_end})"
                cell.font = SUM_FONT
                cell.fill = SUM_FILL
                cell.number_format = MONEY_FMT
                cell.alignment = Alignment(horizontal="right")
                cell.border = SUM_BORDER
            ws[f"A{row}"].font = SUM_FONT
            ws[f"A{row}"].fill = SUM_FILL
            ws[f"A{row}"].value = "TOTAL"
            ws[f"A{row}"].border = SUM_BORDER
            if section.get("total_formula_col"):
                for col, formula in section["total_formula_col"].items():
                    parts = formula.split("+")
                    cell = ws[f"{col}{row}"]
                    cell.value = "=" + "+".join([f"{p}{row}" for p in parts])
                    cell.font = SUM_FONT
                    cell.fill = SUM_FILL
                    cell.number_format = MONEY_FMT
                    cell.alignment = Alignment(horizontal="right")
                    cell.border = SUM_BORDER
                    cell.border = openpyxl.styles.Border(top=openpyxl.styles.Side(style="thin", color="999999"))
            row += 1

        for flag in flags:
            ws[f"A{row}"] = flag
            ws[f"A{row}"].font = FLAG_FONT
            row += 1
        row += 1

    # Schedule A
    ws[f"A{row}"] = "Schedule A:"
    ws[f"A{row}"].font = SECTION_FONT
    ws[f"A{row}"].border = SECTION_BORDER
    note_cell = ws[f"C{row}"]
    note_cell.value = "NOT ENOUGH TO ITEMIZE"
    note_cell.font = Font(italic=True, color="999999", size=9)
    row += 1
    total_state_wh = sum(get_val(e.get("fields",{}), "state_wh") or 0
                         for e in extractions if e.get("document_type") == "W-2")
    for label, val in [("Medical:", None), ("Medical Expenses", 0), ("Total Medical", 0),
                       ("Taxes:", None), ("Income Taxes:", None),
                       ("State Withholding", total_state_wh), ("Total State Tax", total_state_wh),
                       ("Real Estate Taxes:", None), ("", 0), ("Total Real Estate Tax", 0),
                       ("Total Taxes:", total_state_wh),
                       ("Mortgage Interest:", None), ("", 0), ("Total Mortgage Interest", 0),
                       ("Donations:", None), ("Various", 0), ("Total Donations", 0)]:
        ws[f"A{row}"] = label
        if val is not None:
            if "Total" in str(label):
                cell = ws[f"D{row}"]
                cell.value = val
                if isinstance(val, (int, float)):
                    cell.number_format = MONEY_FMT
                    cell.alignment = Alignment(horizontal="right")
                cell.font = SUM_FONT
                cell.fill = SUM_FILL
                ws[f"A{row}"].font = SUM_FONT
                ws[f"A{row}"].fill = SUM_FILL
            else:
                cell = ws[f"C{row}"]
                cell.value = val
                if isinstance(val, (int, float)):
                    cell.number_format = MONEY_FMT
                    cell.alignment = Alignment(horizontal="right")
        elif label.endswith(":"):
            ws[f"A{row}"].font = Font(bold=True, size=10, color="333333")
        row += 1

    row += 1
    ws[f"A{row}"] = "⚠ ITEMS REQUIRING PRIOR YEAR DATA / PREPARER JUDGMENT:"
    ws[f"A{row}"].font = Font(bold=True, color="CC0000", size=11)
    ws.merge_cells(f"A{row}:G{row}")
    row += 1
    for item in REQUIRES_HUMAN_REVIEW:
        ws[f"A{row}"] = f"  • {item}"
        ws[f"A{row}"].font = Font(color="CC0000", size=9)
        row += 1

    row += 2
    ws[f"A{row}"] = "Color Legend:"
    ws[f"A{row}"].font = Font(bold=True, size=10, color="1A252F")
    row += 1
    legend_items = [
        (DUAL_FILL, "Dark green", "OCR + image both agree (highest confidence)"),
        (CONFIRMED_FILL, "Light green", "Verified confirmed"),
        (CORRECTED_FILL, "Bright green", "Corrected / found during verification"),
        (PatternFill("solid", fgColor="B3E5FC"), "Blue", "Operator corrected"),
        (FLAG_FILL, "Yellow", "Low confidence — check source document"),
        (REVIEW_FILL, "Orange", "Requires manual entry / preparer judgment"),
        (ALT_ROW_FILL, "Gray stripe", "Alternating row (no special meaning)"),
    ]
    for fill, label, desc in legend_items:
        ws[f"A{row}"] = label
        ws[f"A{row}"].fill = fill
        ws[f"A{row}"].font = Font(bold=True, size=9)
        ws[f"B{row}"] = desc
        ws[f"B{row}"].font = Font(size=9, color="666666")
        row += 1

    # Column widths
    ws.column_dimensions["A"].width = 42
    for col in ["B", "C", "D", "E", "F", "G"]:
        ws.column_dimensions[col].width = 18


# ─── JOURNAL ENTRIES FORMAT ──────────────────────────────────────────────────

# Fields on tax documents that are informational, not dollar amounts to post.
# These must never become journal entry lines.
_NON_POSTING_FIELDS = {
    # K-1 informational
    "partnership_name", "partnership_ein", "partner_name", "entity_type",
    "partner_type", "profit_share_begin", "profit_share_end",
    "loss_share_begin", "loss_share_end", "capital_share_begin",
    "capital_share_end", "beginning_capital_account", "ending_capital_account",
    "current_year_net_income", "withdrawals_distributions", "capital_contributed",
    # Identifiers on any form
    "employer_ein", "payer_ein", "recipient_ssn_last4", "tax_year",
    "state_id", "account_number", "account_number_last4",
    # Statement period / dates
    "statement_period_start", "statement_period_end",
    "pay_period_start", "pay_period_end",
    # Counts / metadata
    "num_deposits", "num_withdrawals", "number_of_transactions",
    "hours_regular", "hours_overtime", "rate_regular", "rate_overtime",
    "distribution_code", "identifiable_event_code", "type_of_wager",
}


def _build_journal_entries(extractions, year):
    """Transform extractions into balanced double-entry journal entries.

    ACCOUNTING RULES:
    1. Every entry MUST balance: total debits == total credits.
       Entries that fail are flagged "UNBALANCED" and still included
       (so the operator sees them) but marked for review.
    2. Tax information documents (W-2, 1099-*, K-1, SSA-1099, 1098-*,
       5498-*, 1095-*) are NOT source transactions. They report income
       for tax return preparation. They do NOT generate journal entries.
    3. A bank deposit is not revenue until classified. Posts to
       "Unclassified Deposits" pending operator review.
    4. A withdrawal is not a known expense until classified. Posts to
       "Unclassified Expenses" pending operator review.
    5. Credit cards are liabilities (credit-normal). Purchases increase
       the liability (credit CC, debit expense). Payments decrease it.
    6. Payroll: debit Wages Expense for gross, credit each tax payable,
       credit Cash for net. If net is missing, compute it.

    Returns list of:
      {"date", "description", "lines": [{"account", "debit", "credit"}],
       "balanced": bool, "source_type": str}
    """
    journal = []

    def _post(date, description, lines, source_type=""):
        """Validate balance and append entry. Flag if unbalanced."""
        # Round everything to 2 decimals
        for line in lines:
            if line.get("debit"):
                line["debit"] = round(line["debit"], 2)
            else:
                line["debit"] = None
            if line.get("credit"):
                line["credit"] = round(line["credit"], 2)
            else:
                line["credit"] = None

        total_dr = round(sum(l["debit"] or 0 for l in lines), 2)
        total_cr = round(sum(l["credit"] or 0 for l in lines), 2)
        diff = round(total_dr - total_cr, 2)
        balanced = abs(diff) < 0.02

        # Auto-fix small rounding differences (< $1.00)
        if not balanced and 0.02 <= abs(diff) < 1.00 and total_dr > 0 and total_cr > 0:
            if diff > 0:
                lines.append({"account": "Rounding", "debit": None, "credit": abs(diff)})
            else:
                lines.append({"account": "Rounding", "debit": abs(diff), "credit": None})
            balanced = True

        entry = {
            "date": date,
            "description": description,
            "lines": lines,
            "balanced": balanced,
            "source_type": source_type,
        }
        if not balanced:
            entry["description"] = (
                f"\u26a0 UNBALANCED \u2014 {description} "
                f"(DR {total_dr:,.2f} \u2260 CR {total_cr:,.2f})"
            )
        journal.append(entry)

    def _get_cat(fields, field_name, default=""):
        """Read operator-assigned category from a field, if present."""
        fdata = fields.get(field_name)
        if isinstance(fdata, dict):
            return fdata.get("_operator_category", default)
        return default

    for ext in extractions:
        dtype = str(ext.get("document_type", ""))
        fields = ext.get("fields", {})
        entity = ext.get("payer_or_entity", "")

        # ═══════════════════════════════════════════════════════════════
        # TAX INFORMATION DOCUMENTS — DO NOT POST
        # ═══════════════════════════════════════════════════════════════
        # W-2, 1099-*, K-1, SSA-1099, 1098-*, 5498-*, 1095-*, W-2G
        # are information returns. They tell the IRS (and the taxpayer)
        # what happened, but they are NOT the transactions themselves.
        # The actual transactions are on bank statements, checks, etc.
        # These belong in tax_review format, not journal entries.
        if any(tag in dtype for tag in (
            "W-2", "1099", "K-1", "SSA", "1098", "5498", "1095", "W-2G",
        )):
            continue

        # ═══════════════════════════════════════════════════════════════
        # BANK STATEMENTS
        # ═══════════════════════════════════════════════════════════════
        # Cash is an ASSET (debit-normal).
        #   Deposit:    DR Cash / CR Unclassified Deposits
        #   Withdrawal: DR Unclassified Expenses / CR Cash
        #   Fee:        DR Bank Service Charges / CR Cash
        #   Interest:   DR Cash / CR Interest Income
        if "bank_statement" in dtype:
            stmt_date = get_str(fields, "statement_period_end") or f"12/31/{year}"
            bank = get_str(fields, "bank_name") or entity
            acct = get_str(fields, "account_number_last4") or ""
            cash_acct = f"Cash \u2014 {bank}" + (f" (...{acct[-4:]})" if acct else "")

            txn_nums = sorted(set(
                int(m.group(1)) for k in fields
                for m in [re.match(r"txn_(\d+)_", k)] if m
            ))

            if txn_nums:
                for n in txn_nums:
                    tdate = get_str(fields, f"txn_{n}_date") or stmt_date
                    tdesc = get_str(fields, f"txn_{n}_desc") or f"Transaction #{n}"
                    tamt = get_val(fields, f"txn_{n}_amount")
                    ttype = (get_str(fields, f"txn_{n}_type") or "").lower()
                    if not tamt:
                        continue
                    amt = round(abs(tamt), 2)
                    # Operator may have categorized this transaction
                    cat = _get_cat(fields, f"txn_{n}_amount")

                    if ttype in ("deposit", "transfer in", "credit"):
                        cr_acct = cat or "Unclassified Deposits"
                        _post(tdate, tdesc, [
                            {"account": cash_acct, "debit": amt, "credit": None},
                            {"account": cr_acct, "debit": None, "credit": amt},
                        ], "bank_txn")
                    elif ttype == "fee":
                        _post(tdate, f"{bank}: {tdesc}", [
                            {"account": cat or "Bank Service Charges", "debit": amt, "credit": None},
                            {"account": cash_acct, "debit": None, "credit": amt},
                        ], "bank_txn")
                    elif ttype == "interest":
                        _post(tdate, f"{bank}: {tdesc}", [
                            {"account": cash_acct, "debit": amt, "credit": None},
                            {"account": cat or "Interest Income", "debit": None, "credit": amt},
                        ], "bank_txn")
                    else:
                        dr_acct = cat or "Unclassified Expenses"
                        _post(tdate, tdesc, [
                            {"account": dr_acct, "debit": amt, "credit": None},
                            {"account": cash_acct, "debit": None, "credit": amt},
                        ], "bank_txn")
            else:
                # Summary-level entries
                deposits = get_val(fields, "total_deposits")
                if deposits:
                    _post(stmt_date, f"{cash_acct} \u2014 Total deposits", [
                        {"account": cash_acct, "debit": abs(deposits), "credit": None},
                        {"account": "Unclassified Deposits", "debit": None, "credit": abs(deposits)},
                    ], "bank_summary")
                withdrawals = get_val(fields, "total_withdrawals")
                if withdrawals:
                    _post(stmt_date, f"{cash_acct} \u2014 Total withdrawals", [
                        {"account": "Unclassified Expenses", "debit": abs(withdrawals), "credit": None},
                        {"account": cash_acct, "debit": None, "credit": abs(withdrawals)},
                    ], "bank_summary")

            # Fees / interest as separate entries (known categories)
            fees = get_val(fields, "fees_charged")
            if fees and not txn_nums:
                _post(stmt_date, f"{bank} \u2014 Service charges", [
                    {"account": "Bank Service Charges", "debit": abs(fees), "credit": None},
                    {"account": cash_acct, "debit": None, "credit": abs(fees)},
                ], "bank_summary")
            interest = get_val(fields, "interest_earned")
            if interest and not txn_nums:
                _post(stmt_date, f"{bank} \u2014 Interest earned", [
                    {"account": cash_acct, "debit": abs(interest), "credit": None},
                    {"account": "Interest Income", "debit": None, "credit": abs(interest)},
                ], "bank_summary")

        # ═══════════════════════════════════════════════════════════════
        # CREDIT CARD STATEMENTS
        # ═══════════════════════════════════════════════════════════════
        # Credit card is a LIABILITY (credit-normal).
        #   Purchase: DR Unclassified Expenses / CR Credit Card Payable
        #   Payment:  DR Credit Card Payable / CR Cash
        #   Interest: DR Interest Expense / CR Credit Card Payable
        elif "credit_card" in dtype:
            stmt_date = get_str(fields, "statement_period_end") or f"12/31/{year}"
            issuer = get_str(fields, "card_issuer") or entity
            acct_num = get_str(fields, "account_number_last4") or ""
            cc_acct = f"Credit Card Payable \u2014 {issuer}" + (
                f" (...{acct_num[-4:]})" if acct_num else "")

            txn_nums = sorted(set(
                int(m.group(1)) for k in fields
                for m in [re.match(r"txn_(\d+)_", k)] if m
            ))

            if txn_nums:
                for n in txn_nums:
                    tdate = get_str(fields, f"txn_{n}_date") or stmt_date
                    tdesc = get_str(fields, f"txn_{n}_desc") or f"CC Transaction #{n}"
                    tamt = get_val(fields, f"txn_{n}_amount")
                    if not tamt:
                        continue
                    amt = round(abs(tamt), 2)
                    cat = _get_cat(fields, f"txn_{n}_amount")
                    if tamt < 0:
                        # Payment / credit / return
                        cr_acct = cat or "Cash"
                        _post(tdate, f"{issuer}: {tdesc}", [
                            {"account": cc_acct, "debit": amt, "credit": None},
                            {"account": cr_acct, "debit": None, "credit": amt},
                        ], "cc_txn")
                    else:
                        # Purchase / charge
                        dr_acct = cat or "Unclassified Expenses"
                        _post(tdate, tdesc, [
                            {"account": dr_acct, "debit": amt, "credit": None},
                            {"account": cc_acct, "debit": None, "credit": amt},
                        ], "cc_txn")
            else:
                purchases = get_val(fields, "purchases")
                if purchases:
                    _post(stmt_date, f"{cc_acct} \u2014 Purchases", [
                        {"account": "Unclassified Expenses", "debit": abs(purchases), "credit": None},
                        {"account": cc_acct, "debit": None, "credit": abs(purchases)},
                    ], "cc_summary")
                payments = get_val(fields, "payments")
                if payments:
                    _post(stmt_date, f"{cc_acct} \u2014 Payment", [
                        {"account": cc_acct, "debit": abs(payments), "credit": None},
                        {"account": "Cash", "debit": None, "credit": abs(payments)},
                    ], "cc_summary")

            interest_chg = get_val(fields, "interest_charged")
            if interest_chg and not txn_nums:
                _post(stmt_date, f"{issuer} \u2014 Finance charges", [
                    {"account": "Interest Expense", "debit": abs(interest_chg), "credit": None},
                    {"account": cc_acct, "debit": None, "credit": abs(interest_chg)},
                ], "cc_summary")

        # ═══════════════════════════════════════════════════════════════
        # CHECKS
        # ═══════════════════════════════════════════════════════════════
        elif dtype == "check":
            check_date = get_str(fields, "check_date") or f"12/31/{year}"
            check_num = get_str(fields, "check_number") or ""
            payee = get_str(fields, "payee") or get_str(fields, "pay_to") or ""
            check_amt = get_val(fields, "check_amount")
            memo = get_str(fields, "memo_line") or ""
            cat = _get_cat(fields, "check_amount")
            desc = f"Check #{check_num}" if check_num else "Check"
            if payee:
                desc += f" to {payee}"
            if memo:
                desc += f" ({memo})"
            if check_amt:
                dr_acct = cat or "Unclassified Expenses"
                _post(check_date, desc, [
                    {"account": dr_acct, "debit": abs(check_amt), "credit": None},
                    {"account": "Cash", "debit": None, "credit": abs(check_amt)},
                ], "check")

        # ═══════════════════════════════════════════════════════════════
        # INVOICES
        # ═══════════════════════════════════════════════════════════════
        # DR Expense / CR Accounts Payable
        elif "invoice" in dtype:
            inv_date = get_str(fields, "invoice_date") or f"12/31/{year}"
            vendor = get_str(fields, "vendor_name") or entity
            inv_num = get_str(fields, "invoice_number") or ""
            total = get_val(fields, "total_amount")
            cat = _get_cat(fields, "total_amount")
            desc_text = get_str(fields, "description") or ""
            desc = f"{vendor}" + (f" Inv #{inv_num}" if inv_num else "")
            if desc_text:
                desc += f" \u2014 {desc_text}"
            if total:
                dr_acct = cat or "Unclassified Expenses"
                _post(inv_date, desc, [
                    {"account": dr_acct, "debit": abs(total), "credit": None},
                    {"account": "Accounts Payable", "debit": None, "credit": abs(total)},
                ], "invoice")

        # ═══════════════════════════════════════════════════════════════
        # RECEIPTS
        # ═══════════════════════════════════════════════════════════════
        # DR Expense (category if known) / CR Cash or Credit Card
        elif "receipt" in dtype:
            r_date = get_str(fields, "receipt_date") or f"12/31/{year}"
            vendor = get_str(fields, "vendor_name") or entity
            total = get_val(fields, "total_amount")
            cat = _get_cat(fields, "total_amount")
            category = get_str(fields, "category") or ""
            pay_method = (get_str(fields, "payment_method") or "").lower()

            # Priority: operator category > AI-extracted category > unclassified
            if cat:
                expense_acct = cat
            else:
                cat_map = {
                    "meals": "Meals & Entertainment",
                    "supplies": "Office Supplies",
                    "travel": "Auto & Travel",
                    "equipment": "Equipment",
                    "utilities": "Utilities",
                    "rent": "Rent",
                    "insurance": "Insurance",
                    "professional_services": "Legal & Professional",
                }
                expense_acct = cat_map.get(category.lower(), "Unclassified Expenses")

            if "credit" in pay_method or "card" in pay_method:
                contra = "Credit Card Payable"
            elif "check" in pay_method:
                contra = "Cash"
            else:
                contra = "Cash"

            if total:
                _post(r_date, vendor, [
                    {"account": expense_acct, "debit": abs(total), "credit": None},
                    {"account": contra, "debit": None, "credit": abs(total)},
                ], "receipt")

        # ═══════════════════════════════════════════════════════════════
        # PAYROLL (Check Stubs)
        # ═══════════════════════════════════════════════════════════════
        # DR Wages Expense (gross)
        # CR Federal WH Payable, State WH Payable, SS Payable, Medicare Payable
        # CR Cash (net pay)
        # If net is missing: compute as gross - sum(known deductions)
        elif "check_stub" in dtype:
            pay_date = get_str(fields, "pay_date") or f"12/31/{year}"
            employer = get_str(fields, "employer_name") or entity
            employee = get_str(fields, "employee_name") or ""
            label = f"Payroll \u2014 {employer}: {employee}" if employee else f"Payroll \u2014 {employer}"

            gross = get_val(fields, "gross_pay")
            if not gross:
                continue

            lines = [{"account": "Wages Expense", "debit": round(abs(gross), 2), "credit": None}]
            known_deductions = 0.0

            for acct_name, field_name in [
                ("Federal WH Payable", "federal_wh"),
                ("State WH Payable", "state_wh"),
                ("Social Security Payable", "social_security"),
                ("Medicare Payable", "medicare"),
            ]:
                val = get_val(fields, field_name)
                if val:
                    amt = round(abs(val), 2)
                    lines.append({"account": acct_name, "debit": None, "credit": amt})
                    known_deductions += amt

            net = get_val(fields, "net_pay")
            if net:
                lines.append({"account": "Cash", "debit": None, "credit": round(abs(net), 2)})
            else:
                # Compute net to force balance
                computed_net = round(abs(gross) - known_deductions, 2)
                if computed_net > 0:
                    lines.append({"account": "Cash (net \u2014 computed)", "debit": None, "credit": computed_net})

            _post(pay_date, label, lines, "payroll")

        # ═══════════════════════════════════════════════════════════════
        # LOAN / MORTGAGE PAYMENTS
        # ═══════════════════════════════════════════════════════════════
        # DR Loan Payable (principal) + DR Interest Expense + DR Escrow
        # CR Cash (total payment)
        elif "loan_statement" in dtype or "mortgage_statement" in dtype:
            pay_date = (get_str(fields, "payment_date")
                        or get_str(fields, "next_due_date")
                        or f"12/31/{year}")
            lender = get_str(fields, "lender") or entity
            principal = get_val(fields, "principal_paid")
            interest = get_val(fields, "interest_paid")
            escrow = get_val(fields, "escrow_paid")

            if principal or interest:
                lines = []
                if principal:
                    lines.append({"account": f"Loan Payable \u2014 {lender}",
                                  "debit": round(abs(principal), 2), "credit": None})
                if interest:
                    lines.append({"account": "Interest Expense",
                                  "debit": round(abs(interest), 2), "credit": None})
                if escrow:
                    lines.append({"account": "Escrow Deposit",
                                  "debit": round(abs(escrow), 2), "credit": None})
                total = round(sum(l["debit"] or 0 for l in lines), 2)
                lines.append({"account": "Cash", "debit": None, "credit": total})
                _post(pay_date, f"Loan payment \u2014 {lender}", lines, "loan")

        # ═══════════════════════════════════════════════════════════════
        # PROFIT & LOSS STATEMENTS (summary-level)
        # ═══════════════════════════════════════════════════════════════
        elif "profit_loss" in dtype:
            period_end = get_str(fields, "period_end") or f"12/31/{year}"
            revenue = get_val(fields, "total_revenue")
            expenses = (get_val(fields, "total_operating_expenses")
                        or get_val(fields, "total_expenses"))
            if revenue:
                _post(period_end, f"Revenue \u2014 {entity}", [
                    {"account": "Accounts Receivable", "debit": abs(revenue), "credit": None},
                    {"account": "Revenue", "debit": None, "credit": abs(revenue)},
                ], "pnl")
            if expenses:
                _post(period_end, f"Operating expenses \u2014 {entity}", [
                    {"account": "Operating Expenses", "debit": abs(expenses), "credit": None},
                    {"account": "Accounts Payable", "debit": None, "credit": abs(expenses)},
                ], "pnl")

        # All other document types (payroll_register, balance_sheet, etc.)
        # are reporting documents, not source transactions. Skip.

    journal.sort(key=lambda e: e.get("date", ""))
    return journal


def _populate_journal_entries(ws, extractions, year):
    """Write proper double-entry journal entries.

    Layout per entry:
      Row 1: Date | Account (debit) | Description | Debit amount |
      Row 2:      |   Account (credit, indented) |             |  | Credit amount
      ...repeat for multi-line entries
      [blank row separator between entries]

    Columns: A=Date, B=Account, C=Description, D=Debit, E=Credit
    Credits are indented with leading spaces in column B.
    """
    row = _write_title(ws, "Journal Entries", year)

    # Column headers
    for col, label in [("A", "Date"), ("B", "Account"), ("C", "Description"), ("D", "Debit"), ("E", "Credit")]:
        cell = ws[f"{col}{row}"]
        cell.value = label
        cell.font = COL_HEADER_FONT
        cell.fill = COL_HEADER_FILL
        cell.border = SECTION_BORDER
        if col in ("D", "E"):
            cell.alignment = Alignment(horizontal="right")
    row += 1

    journal = _build_journal_entries(extractions, year)

    if not journal:
        ws[f"A{row}"] = "(no journal entries generated)"
        ws[f"A{row}"].font = Font(italic=True, color="BBBBBB")
        ws.column_dimensions["A"].width = 16
        ws.column_dimensions["B"].width = 36
        ws.column_dimensions["C"].width = 40
        for col in ["D", "E"]:
            ws.column_dimensions[col].width = 16
        return

    # Styles for journal entries
    credit_font = Font(size=10, color="444444")
    entry_sep_border = openpyxl.styles.Border(
        bottom=openpyxl.styles.Side(style="thin", color="CCCCCC"))
    entry_num_font = Font(size=8, color="999999")

    data_start = row
    entry_count = 0
    unbalanced_count = 0

    for entry in journal:
        entry_count += 1
        date_str = entry.get("date", "")
        desc = entry.get("description", "")
        lines = entry.get("lines", [])
        balanced = entry.get("balanced", True)
        entry_start_row = row

        if not balanced:
            unbalanced_count += 1

        # Separate debit lines (listed first) and credit lines
        debit_lines = [l for l in lines if l.get("debit")]
        credit_lines = [l for l in lines if l.get("credit")]

        # Write debit lines first
        first_line = True
        for dl in debit_lines:
            if first_line:
                ws[f"A{row}"] = date_str
                ws[f"C{row}"] = desc
                first_line = False
            ws[f"B{row}"] = dl["account"]
            ws[f"B{row}"].font = Font(bold=True, size=10)
            cell_d = ws[f"D{row}"]
            cell_d.value = dl["debit"]
            cell_d.number_format = MONEY_FMT
            cell_d.alignment = Alignment(horizontal="right")
            for c in ["A", "B", "C", "D", "E"]:
                ws[f"{c}{row}"].border = THIN_BORDER
            row += 1

        # Write credit lines (indented)
        for cl in credit_lines:
            if first_line:
                ws[f"A{row}"] = date_str
                ws[f"C{row}"] = desc
                first_line = False
            ws[f"B{row}"] = f"    {cl['account']}"
            ws[f"B{row}"].font = credit_font
            cell_e = ws[f"E{row}"]
            cell_e.value = cl["credit"]
            cell_e.number_format = MONEY_FMT
            cell_e.alignment = Alignment(horizontal="right")
            for c in ["A", "B", "C", "D", "E"]:
                ws[f"{c}{row}"].border = THIN_BORDER
            row += 1

        # Highlight unbalanced entries in orange
        if not balanced:
            for r in range(entry_start_row, row):
                for c in ["A", "B", "C", "D", "E"]:
                    ws[f"{c}{r}"].fill = REVIEW_FILL

        # Blank separator row between entries
        for c in ["A", "B", "C", "D", "E"]:
            ws[f"{c}{row}"].border = entry_sep_border
        row += 1

    # Grand totals
    data_end = row - 2  # skip last separator
    ws[f"B{row}"] = "TOTALS"
    ws[f"B{row}"].font = SUM_FONT
    ws[f"B{row}"].fill = SUM_FILL
    for col in ["D", "E"]:
        cell = ws[f"{col}{row}"]
        cell.value = f"=SUM({col}{data_start}:{col}{data_end})"
        cell.font = SUM_FONT
        cell.fill = SUM_FILL
        cell.number_format = MONEY_FMT
        cell.alignment = Alignment(horizontal="right")
    for c in ["A", "B", "C", "D", "E"]:
        ws[f"{c}{row}"].border = openpyxl.styles.Border(
            top=openpyxl.styles.Side(style="double", color="333333"))
    ws[f"A{row}"].fill = SUM_FILL
    ws[f"C{row}"].fill = SUM_FILL
    row += 1

    # Balance check row
    ws[f"B{row}"] = "BALANCE CHECK (should be zero):"
    ws[f"B{row}"].font = Font(bold=True, size=9, color="666666")
    bal_cell = ws[f"D{row}"]
    bal_cell.value = f"=D{row-1}-E{row-1}"
    bal_cell.number_format = MONEY_FMT
    bal_cell.font = Font(bold=True, size=10, color="CC0000")
    bal_cell.alignment = Alignment(horizontal="right")
    row += 1

    # Entry count
    count_text = f"{entry_count} journal entries"
    if unbalanced_count:
        count_text += f" ({unbalanced_count} UNBALANCED — highlighted orange)"
    ws[f"B{row}"] = count_text
    ws[f"B{row}"].font = Font(italic=True, size=9, color="CC0000" if unbalanced_count else "999999")

    # Column widths
    ws.column_dimensions["A"].width = 14
    ws.column_dimensions["B"].width = 36
    ws.column_dimensions["C"].width = 42
    for col in ["D", "E"]:
        ws.column_dimensions[col].width = 16


# ─── ACCOUNT BALANCES FORMAT ─────────────────────────────────────────────────

ACCT_BAL_TEMPLATE_SECTIONS = [
    {
        "id": "acct_balances",
        "header": "Bank Account Balances:",
        "match_types": ["bank_statement", "bank_statement_deposit_slip"],
        "columns": {
            "A": "bank_name", "B": "account_number_last4",
            "C": "statement_period_start", "D": "statement_period_end",
            "E": "beginning_balance", "F": "total_deposits",
            "G": "total_withdrawals", "H": "fees_charged",
            "I": "interest_earned", "J": "ending_balance",
        },
        "col_headers": {
            "B": "Acct #", "C": "Period Start", "D": "Period End",
            "E": "Begin Bal", "F": "Deposits", "G": "Withdrawals",
            "H": "Fees", "I": "Interest", "J": "End Bal",
        },
        "sum_cols": ["E", "F", "G", "H", "I", "J"],
        "flags": [],
    },
    {
        "id": "credit_card_balances",
        "header": "Credit Card Balances:",
        "match_types": ["credit_card_statement"],
        "columns": {
            "A": "card_issuer", "B": "account_number_last4",
            "C": "statement_period_start", "D": "statement_period_end",
            "E": "previous_balance", "F": "purchases",
            "G": "payments", "H": "interest_charged", "I": "new_balance",
        },
        "col_headers": {
            "B": "Acct #", "C": "Period Start", "D": "Period End",
            "E": "Prev Bal", "F": "Purchases", "G": "Payments",
            "H": "Interest", "I": "New Bal",
        },
        "sum_cols": ["E", "F", "G", "H", "I"],
        "flags": [],
    },
]

ACCT_BAL_ALWAYS_SHOW = ["acct_balances", "credit_card_balances"]

def _populate_account_balances(ws, extractions, year):
    """Write account balance format: one row per bank statement."""
    row = _write_title(ws, "Account Balances", year)

    for section in ACCT_BAL_TEMPLATE_SECTIONS:
        sid = section["id"]
        match_types = section.get("match_types", [])
        matched = _match_exts(extractions, match_types) if match_types else []
        col_headers = section.get("col_headers", {})
        columns = section.get("columns", {})
        sum_cols = section.get("sum_cols", [])

        if not matched and sid not in ACCT_BAL_ALWAYS_SHOW:
            continue

        ws[f"A{row}"] = section["header"]
        ws[f"A{row}"].font = SECTION_FONT
        ws[f"A{row}"].border = SECTION_BORDER
        for col, label in col_headers.items():
            cell = ws[f"{col}{row}"]
            cell.value = label
            cell.font = COL_HEADER_FONT
            cell.fill = COL_HEADER_FILL
            cell.alignment = Alignment(horizontal="right")
            cell.border = SECTION_BORDER
        row += 1

        if not matched:
            ws[f"A{row}"] = "(no bank statements found)"
            ws[f"A{row}"].font = Font(italic=True, color="BBBBBB")
            row += 2
            continue

        matched = _dedup_by_ein(matched)
        data_start = row
        all_cols = list(columns.keys())

        for ext_idx, ext in enumerate(matched):
            fields = ext.get("fields", {})
            for col, field_name in columns.items():
                if field_name in ("bank_name",):
                    ws[f"{col}{row}"] = get_str(fields, field_name) or ext.get("payer_or_entity", "")
                else:
                    val = get_val(fields, field_name)
                    if val is None:
                        val = get_str(fields, field_name)
                    cell = ws[f"{col}{row}"]
                    cell.value = val
                    if isinstance(val, (int, float)):
                        cell.number_format = MONEY_FMT
                        cell.alignment = Alignment(horizontal="right")
            # Alternating row fill
            if ext_idx % 2 == 1:
                for bcol in all_cols:
                    cell = ws[f"{bcol}{row}"]
                    if cell.fill == PatternFill() or cell.fill is None:
                        cell.fill = ALT_ROW_FILL
            for bcol in all_cols:
                ws[f"{bcol}{row}"].border = THIN_BORDER
            row += 1

        data_end = row - 1
        if data_end >= data_start and sum_cols:
            ws[f"A{row}"] = "TOTAL"
            ws[f"A{row}"].font = SUM_FONT
            ws[f"A{row}"].fill = SUM_FILL
            for col in sum_cols:
                cell = ws[f"{col}{row}"]
                cell.value = f"=SUM({col}{data_start}:{col}{data_end})"
                cell.font = SUM_FONT
                cell.fill = SUM_FILL
                cell.number_format = MONEY_FMT
                cell.alignment = Alignment(horizontal="right")
                cell.border = SUM_BORDER
            ws[f"A{row}"].border = SUM_BORDER
            row += 1

    # ─── Transaction Detail Section ───
    row += 1
    all_txns = []
    for ext in extractions:
        if ext.get("document_type") not in ("bank_statement", "bank_statement_deposit_slip", "check", "check_stub", "credit_card_statement"):
            continue
        fields = ext.get("fields", {})
        entity = ext.get("payer_or_entity", "")
        # Gather numbered transaction fields: txn_N_date, txn_N_desc, txn_N_amount, txn_N_type
        txn_nums = set()
        for k in fields:
            m = __import__("re").match(r"txn_(\d+)_", k)
            if m:
                txn_nums.add(int(m.group(1)))
        for n in sorted(txn_nums):
            txn_date = get_str(fields, f"txn_{n}_date") or ""
            txn_desc = get_str(fields, f"txn_{n}_desc") or ""
            txn_amt = get_val(fields, f"txn_{n}_amount")
            txn_type = get_str(fields, f"txn_{n}_type") or ""
            all_txns.append((entity, txn_date, txn_desc, txn_amt, txn_type))

    if all_txns:
        ws[f"A{row}"] = "Transaction Detail:"
        ws[f"A{row}"].font = SECTION_FONT
        ws[f"A{row}"].border = SECTION_BORDER
        for col, label in [("B", "Date"), ("C", "Description"), ("D", "Amount"), ("E", "Type")]:
            cell = ws[f"{col}{row}"]
            cell.value = label
            cell.font = COL_HEADER_FONT
            cell.fill = COL_HEADER_FILL
            cell.alignment = Alignment(horizontal="right") if col == "D" else Alignment(horizontal="left")
            cell.border = SECTION_BORDER
        row += 1
        txn_data_start = row
        for txn_idx, (entity, tdate, tdesc, tamt, ttype) in enumerate(all_txns):
            ws[f"A{row}"] = entity
            ws[f"B{row}"] = tdate
            ws[f"C{row}"] = tdesc
            cell = ws[f"D{row}"]
            cell.value = tamt
            if isinstance(tamt, (int, float)):
                cell.number_format = MONEY_FMT
                cell.alignment = Alignment(horizontal="right")
            ws[f"E{row}"] = ttype
            if txn_idx % 2 == 1:
                for bcol in ["A", "B", "C", "D", "E"]:
                    cell = ws[f"{bcol}{row}"]
                    if cell.fill == PatternFill() or cell.fill is None:
                        cell.fill = ALT_ROW_FILL
            for bcol in ["A", "B", "C", "D", "E"]:
                ws[f"{bcol}{row}"].border = THIN_BORDER
            row += 1
        txn_data_end = row - 1
        # Total row for transaction amounts
        ws[f"A{row}"] = "TOTAL"
        ws[f"A{row}"].font = SUM_FONT
        ws[f"A{row}"].fill = SUM_FILL
        cell = ws[f"D{row}"]
        cell.value = f"=SUM(D{txn_data_start}:D{txn_data_end})"
        cell.font = SUM_FONT
        cell.fill = SUM_FILL
        cell.number_format = MONEY_FMT
        cell.alignment = Alignment(horizontal="right")
        cell.border = openpyxl.styles.Border(top=openpyxl.styles.Side(style="thin", color="999999"))
        row += 1

    ws.column_dimensions["A"].width = 30
    for col in ["B", "C", "D", "E", "F", "G", "H", "I", "J"]:
        ws.column_dimensions[col].width = 16


# ─── TRIAL BALANCE FORMAT ───────────────────────────────────────────────────

def _populate_trial_balance(ws, extractions, year):
    """Write a trial balance: accounts with debit and credit totals.

    Builds journal entries first, then aggregates by account into
    a standard trial balance (Account | Debit | Credit) with totals.
    """
    row = _write_title(ws, "Trial Balance", year)

    # Column headers
    for col, label, align in [("A", "Account", "left"), ("B", "Debit", "right"),
                               ("C", "Credit", "right"), ("D", "Net Balance", "right")]:
        cell = ws[f"{col}{row}"]
        cell.value = label
        cell.font = DARK_HEADER_FONT
        cell.fill = DARK_HEADER_FILL
        cell.border = SECTION_BORDER
        cell.alignment = Alignment(horizontal=align)
    row += 1

    # Build journal entries and aggregate by account
    journal = _build_journal_entries(extractions, year)
    account_totals = {}  # account_name → {"debit": float, "credit": float}

    for entry in journal:
        for line in entry.get("lines", []):
            acct = line.get("account", "Uncategorized")
            dr = line.get("debit") or 0
            cr = line.get("credit") or 0
            if acct not in account_totals:
                account_totals[acct] = {"debit": 0.0, "credit": 0.0}
            account_totals[acct]["debit"] += dr
            account_totals[acct]["credit"] += cr

    if not account_totals:
        ws[f"A{row}"] = "(no account data — upload bank statements, invoices, or receipts)"
        ws[f"A{row}"].font = Font(italic=True, color="BBBBBB")
        _tb_col_widths(ws)
        return

    # Sort accounts: Assets/Expenses (debit-normal) first, then Liabilities/Revenue (credit-normal)
    # Simple heuristic: sort by net balance direction, then alphabetically
    def sort_key(item):
        acct, tots = item
        net = tots["debit"] - tots["credit"]
        # Debit-normal accounts first (positive net = debit heavy)
        return (0 if net >= 0 else 1, acct.lower())

    data_start = row
    for idx, (acct, tots) in enumerate(sorted(account_totals.items(), key=sort_key)):
        ws[f"A{row}"] = acct
        dr_cell = ws[f"B{row}"]
        cr_cell = ws[f"C{row}"]
        net_cell = ws[f"D{row}"]

        if tots["debit"] > 0:
            dr_cell.value = round(tots["debit"], 2)
            dr_cell.number_format = MONEY_FMT
            dr_cell.alignment = Alignment(horizontal="right")
        if tots["credit"] > 0:
            cr_cell.value = round(tots["credit"], 2)
            cr_cell.number_format = MONEY_FMT
            cr_cell.alignment = Alignment(horizontal="right")

        net = round(tots["debit"] - tots["credit"], 2)
        net_cell.value = net
        net_cell.number_format = MONEY_FMT
        net_cell.alignment = Alignment(horizontal="right")
        if net < 0:
            net_cell.font = Font(color="CC0000")

        # Alternating rows
        if idx % 2 == 1:
            for c in ["A", "B", "C", "D"]:
                ws[f"{c}{row}"].fill = ALT_ROW_FILL
        for c in ["A", "B", "C", "D"]:
            ws[f"{c}{row}"].border = THIN_BORDER
        row += 1

    data_end = row - 1

    # Totals row
    for c in ["A", "B", "C", "D"]:
        ws[f"{c}{row}"].fill = SUM_FILL
        ws[f"{c}{row}"].font = SUM_FONT
        ws[f"{c}{row}"].border = openpyxl.styles.Border(
            top=openpyxl.styles.Side(style="double", color="333333"))
    ws[f"A{row}"] = "TOTALS"
    for col in ["B", "C", "D"]:
        cell = ws[f"{col}{row}"]
        cell.value = f"=SUM({col}{data_start}:{col}{data_end})"
        cell.number_format = MONEY_FMT
        cell.alignment = Alignment(horizontal="right")
    row += 1

    # Balance check
    ws[f"A{row}"] = "BALANCE CHECK (Debits − Credits, should be zero):"
    ws[f"A{row}"].font = Font(bold=True, size=9, color="666666")
    bal_cell = ws[f"B{row}"]
    bal_cell.value = f"=B{row-1}-C{row-1}"
    bal_cell.number_format = MONEY_FMT
    bal_cell.font = Font(bold=True, size=10, color="CC0000")
    bal_cell.alignment = Alignment(horizontal="right")
    row += 2

    # Account count
    ws[f"A{row}"] = f"{len(account_totals)} accounts from {len(journal)} journal entries"
    ws[f"A{row}"].font = Font(italic=True, size=9, color="999999")

    _tb_col_widths(ws)


def _tb_col_widths(ws):
    ws.column_dimensions["A"].width = 42
    ws.column_dimensions["B"].width = 18
    ws.column_dimensions["C"].width = 18
    ws.column_dimensions["D"].width = 18


# ─── TRANSACTION REGISTER FORMAT ────────────────────────────────────────────

def _populate_transaction_register(ws, extractions, year):
    """Write a chronological transaction register with running balance.

    Like a checkbook register: Date | Description | Source | Debit | Credit | Balance
    Every transaction from all documents in one flat, time-ordered list.
    """
    row = _write_title(ws, "Transaction Register", year)

    # Column headers
    headers = [("A", "Date", "left"), ("B", "Description", "left"), ("C", "Source", "left"),
               ("D", "Type", "left"), ("E", "Debit (In)", "right"), ("F", "Credit (Out)", "right"),
               ("G", "Balance", "right")]
    for col, label, align in headers:
        cell = ws[f"{col}{row}"]
        cell.value = label
        cell.font = DARK_HEADER_FONT
        cell.fill = DARK_HEADER_FILL
        cell.border = SECTION_BORDER
        cell.alignment = Alignment(horizontal=align)
    row += 1

    # Gather all transactions from all extractions
    txns = []  # list of (date_str, description, source, type, amount_signed)

    for ext in extractions:
        dtype = str(ext.get("document_type", ""))
        fields = ext.get("fields", {})
        entity = ext.get("payer_or_entity", "")

        # Bank statement / credit card individual transactions
        if "bank_statement" in dtype or "credit_card" in dtype:
            bank = get_str(fields, "bank_name") or get_str(fields, "card_issuer") or entity
            acct = get_str(fields, "account_number_last4") or ""
            source = f"{bank}" + (f" (...{acct[-4:]})" if acct else "")

            txn_nums = sorted(set(int(m.group(1)) for k in fields
                                  for m in [re.match(r"txn_(\d+)_", k)] if m))
            for n in txn_nums:
                tdate = get_str(fields, f"txn_{n}_date") or ""
                tdesc = get_str(fields, f"txn_{n}_desc") or ""
                tamt = get_val(fields, f"txn_{n}_amount")
                ttype = get_str(fields, f"txn_{n}_type") or "transaction"
                if tamt is not None:
                    txns.append((tdate, tdesc, source, ttype, tamt))

            # If no individual txns, add summary lines
            if not txn_nums:
                stmt_date = get_str(fields, "statement_period_end") or f"12/31/{year}"
                deposits = get_val(fields, "total_deposits")
                if deposits:
                    txns.append((stmt_date, f"{source} — Total deposits", source, "deposit", abs(deposits)))
                withdrawals = get_val(fields, "total_withdrawals")
                if withdrawals:
                    txns.append((stmt_date, f"{source} — Total withdrawals", source, "withdrawal", -abs(withdrawals)))
                fees = get_val(fields, "fees_charged")
                if fees:
                    txns.append((stmt_date, f"{source} — Fees", source, "fee", -abs(fees)))
                interest = get_val(fields, "interest_earned")
                if interest:
                    txns.append((stmt_date, f"{source} — Interest", source, "interest", abs(interest)))

        elif dtype == "check":
            check_date = get_str(fields, "check_date") or f"12/31/{year}"
            check_num = get_str(fields, "check_number") or ""
            payee = get_str(fields, "payee") or get_str(fields, "pay_to") or ""
            check_amt = get_val(fields, "check_amount")
            desc = f"Check #{check_num}" if check_num else "Check"
            if payee:
                desc += f" to {payee}"
            if check_amt:
                txns.append((check_date, desc, "Check", "check", -abs(check_amt)))

        elif "invoice" in dtype:
            inv_date = get_str(fields, "invoice_date") or f"12/31/{year}"
            vendor = get_str(fields, "vendor_name") or entity
            inv_num = get_str(fields, "invoice_number") or ""
            total = get_val(fields, "total_amount")
            desc = f"{vendor}" + (f" Inv #{inv_num}" if inv_num else "")
            if total:
                txns.append((inv_date, desc, "Invoice", "expense", -abs(total)))

        elif "receipt" in dtype:
            r_date = get_str(fields, "receipt_date") or f"12/31/{year}"
            vendor = get_str(fields, "vendor_name") or entity
            total = get_val(fields, "total_amount")
            category = get_str(fields, "category") or "expense"
            if total:
                txns.append((r_date, vendor, "Receipt", category, -abs(total)))

        elif "check_stub" in dtype:
            pay_date = get_str(fields, "pay_date") or f"12/31/{year}"
            employer = get_str(fields, "employer_name") or entity
            net = get_val(fields, "net_pay")
            if net:
                txns.append((pay_date, f"Payroll — {employer}", "Payroll", "payroll", abs(net)))

    if not txns:
        ws[f"A{row}"] = "(no transactions found — upload bank statements, invoices, or receipts)"
        ws[f"A{row}"].font = Font(italic=True, color="BBBBBB")
        _tr_col_widths(ws)
        return

    # Sort by date
    def date_sort_key(t):
        d = t[0]
        # Try to parse common date formats for proper sorting
        for fmt in ("%m/%d/%Y", "%m/%d/%y", "%Y-%m-%d", "%m-%d-%Y"):
            try:
                return datetime.strptime(d, fmt)
            except (ValueError, TypeError):
                continue
        return datetime(int(year), 12, 31)  # fallback: end of year

    txns.sort(key=date_sort_key)

    # Start running balance from earliest beginning balance (if available)
    # This is fundamental — a register without a starting point is meaningless
    opening_balance = 0.0
    for ext in extractions:
        if "bank_statement" in str(ext.get("document_type", "")):
            begin = get_val(ext.get("fields", {}), "beginning_balance")
            if begin is not None:
                opening_balance = begin
                break  # Use the first one found (earliest in document order)

    data_start = row
    running_balance = opening_balance

    # Write opening balance row if we have one
    if opening_balance != 0.0:
        ws[f"A{row}"] = ""
        ws[f"B{row}"] = "Opening Balance"
        ws[f"B{row}"].font = Font(bold=True, italic=True, size=10)
        ws[f"G{row}"] = round(opening_balance, 2)
        ws[f"G{row}"].number_format = MONEY_FMT
        ws[f"G{row}"].alignment = Alignment(horizontal="right")
        ws[f"G{row}"].font = Font(bold=True)
        for c in ["A", "B", "C", "D", "E", "F", "G"]:
            ws[f"{c}{row}"].fill = SUM_FILL
            ws[f"{c}{row}"].border = THIN_BORDER
        row += 1

    debit_font = Font(color="006600")
    credit_font = Font(color="CC0000")

    for idx, (tdate, tdesc, source, ttype, amount) in enumerate(txns):
        ws[f"A{row}"] = tdate
        ws[f"B{row}"] = tdesc
        ws[f"C{row}"] = source
        ws[f"D{row}"] = ttype.title()

        if amount >= 0:
            # Debit (money in)
            dr_cell = ws[f"E{row}"]
            dr_cell.value = round(amount, 2)
            dr_cell.number_format = MONEY_FMT
            dr_cell.alignment = Alignment(horizontal="right")
            dr_cell.font = debit_font
        else:
            # Credit (money out)
            cr_cell = ws[f"F{row}"]
            cr_cell.value = round(abs(amount), 2)
            cr_cell.number_format = MONEY_FMT
            cr_cell.alignment = Alignment(horizontal="right")
            cr_cell.font = credit_font

        running_balance += amount
        bal_cell = ws[f"G{row}"]
        bal_cell.value = round(running_balance, 2)
        bal_cell.number_format = MONEY_FMT
        bal_cell.alignment = Alignment(horizontal="right")
        if running_balance < 0:
            bal_cell.font = Font(bold=True, color="CC0000")

        # Alternating rows
        if idx % 2 == 1:
            for c in ["A", "B", "C", "D", "E", "F", "G"]:
                cell = ws[f"{c}{row}"]
                if cell.fill == PatternFill():
                    cell.fill = ALT_ROW_FILL
        for c in ["A", "B", "C", "D", "E", "F", "G"]:
            ws[f"{c}{row}"].border = THIN_BORDER
        row += 1

    data_end = row - 1

    # Totals row
    for c in ["A", "B", "C", "D", "E", "F", "G"]:
        ws[f"{c}{row}"].fill = SUM_FILL
        ws[f"{c}{row}"].font = SUM_FONT
        ws[f"{c}{row}"].border = openpyxl.styles.Border(
            top=openpyxl.styles.Side(style="double", color="333333"))
    ws[f"A{row}"] = "TOTALS"
    for col in ["E", "F"]:
        cell = ws[f"{col}{row}"]
        cell.value = f"=SUM({col}{data_start}:{col}{data_end})"
        cell.number_format = MONEY_FMT
        cell.alignment = Alignment(horizontal="right")
    ws[f"G{row}"] = round(running_balance, 2)
    ws[f"G{row}"].number_format = MONEY_FMT
    ws[f"G{row}"].alignment = Alignment(horizontal="right")
    if running_balance < 0:
        ws[f"G{row}"].font = Font(bold=True, color="CC0000")
    row += 2

    # Summary
    ws[f"A{row}"] = f"{len(txns)} transactions"
    ws[f"A{row}"].font = Font(italic=True, size=9, color="999999")

    _tr_col_widths(ws)


def _tr_col_widths(ws):
    ws.column_dimensions["A"].width = 14
    ws.column_dimensions["B"].width = 44
    ws.column_dimensions["C"].width = 24
    ws.column_dimensions["D"].width = 14
    ws.column_dimensions["E"].width = 16
    ws.column_dimensions["F"].width = 16
    ws.column_dimensions["G"].width = 16


# ─── LOG + SUMMARY ───────────────────────────────────────────────────────────

def save_log(extractions, classifications, warnings, output_path, output_format="tax_review", user_notes="", ai_instructions="", cost_data=None):
    global _cost_tracker
    log_path = output_path.replace(".xlsx", "_log.json")
    log = {
        "version": "v6",
        "architecture": "ocr_first_vision_fallback",
        "output_format": output_format,
        "user_notes": user_notes,
        "ai_instructions": ai_instructions,
        "timestamp": datetime.now().isoformat(),
        "model": MODEL,
        "classifications": classifications,
        "extractions": [{k: v for k, v in e.items() if not k.startswith("_")} | {
            "_page": e.get("_page"),
            "_extraction_method": e.get("_extraction_method"),
            "_overall_confidence": e.get("_overall_confidence"),
        } for e in extractions],
        "warnings": warnings,
        "human_review_required": REQUIRES_HUMAN_REVIEW,
    }
    if cost_data:
        log["cost"] = cost_data
    elif _cost_tracker:
        log["cost"] = _cost_tracker.to_dict()
    with open(log_path, "w") as f:
        json.dump(log, f, indent=2, default=str)
    print(f"  Log: {log_path}")


def save_checkpoint(output_path, phase, classifications=None, extractions=None, groups=None):
    """Save partial results after each phase for crash recovery.

    Checkpoint file is {output}_checkpoint.json. If the run completes
    successfully, the checkpoint is deleted. If it crashes, the operator
    can resume from the last completed phase with --resume.
    """
    ckpt_path = output_path.replace(".xlsx", "_checkpoint.json")
    data = {
        "timestamp": datetime.now().isoformat(),
        "completed_phase": phase,
        "model": MODEL,
    }
    if classifications is not None:
        data["classifications"] = classifications
    if groups is not None:
        data["groups"] = [{k: v for k, v in g.items()} for g in groups]
    if extractions is not None:
        data["extractions"] = [{k: v for k, v in e.items() if not k.startswith("_") or k in (
            "_page", "_extraction_method", "_overall_confidence", "_is_brokerage", "_ambiguous_fields"
        )} for e in extractions]
    with open(ckpt_path, "w") as f:
        json.dump(data, f, indent=2, default=str)
    print(f"  Checkpoint saved: {phase}")
    return ckpt_path


def load_checkpoint(output_path):
    """Load checkpoint if it exists. Returns dict or None."""
    ckpt_path = output_path.replace(".xlsx", "_checkpoint.json")
    if os.path.exists(ckpt_path):
        try:
            with open(ckpt_path) as f:
                data = json.load(f)
            print(f"  Found checkpoint: completed through {data.get('completed_phase', '?')}")
            return data
        except (json.JSONDecodeError, IOError) as e:
            print(f"  Checkpoint corrupt: {e} — starting fresh")
    return None


def clear_checkpoint(output_path):
    """Remove checkpoint file after successful completion."""
    ckpt_path = output_path.replace(".xlsx", "_checkpoint.json")
    if os.path.exists(ckpt_path):
        os.remove(ckpt_path)


def print_summary(extractions):
    print("\n── Summary ──")
    methods = {}
    confs = {}
    for ext in extractions:
        m = ext.get("_extraction_method", "unknown")
        methods[m] = methods.get(m, 0) + 1
        for fdata in ext.get("fields", {}).values():
            if isinstance(fdata, dict):
                c = fdata.get("confidence", "unknown")
                confs[c] = confs.get(c, 0) + 1

    total_f = sum(confs.values())
    print(f"  Extraction methods: {dict(methods)}")
    print(f"  Total fields: {total_f}")
    for c, n in sorted(confs.items(), key=lambda x: -x[1]):
        print(f"    {c}: {n} ({n/total_f*100:.0f}%)")


# ─── MAIN ────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Document Intake Extractor v6")
    parser.add_argument("pdf", nargs="?", default=None, help="Scanned PDF path")
    parser.add_argument("--year", type=int, default=2024)
    parser.add_argument("--template", default=None)
    parser.add_argument("--output", default=None)
    parser.add_argument("--dpi", type=int, default=DPI)
    parser.add_argument("--skip-verify", action="store_true", help="Skip verification (faster, cheaper)")
    parser.add_argument("--log-only", action="store_true")
    parser.add_argument("--no-pii", action="store_true", help="Disable PII tokenization (send raw data)")
    parser.add_argument("--regen-excel", action="store_true",
                        help="Regenerate Excel from a log JSON (no extraction). Requires --log-input and --output.")
    parser.add_argument("--log-input", default=None,
                        help="Path to extraction log JSON (used with --regen-excel)")
    parser.add_argument("--doc-type", default="tax_returns",
                        choices=["tax_returns", "bank_statements", "trust_documents", "bookkeeping"],
                        help="Document type category (affects classification and extraction)")
    parser.add_argument("--output-format", default="tax_review",
                        choices=["tax_review", "journal_entries", "account_balances", "trial_balance", "transaction_register"],
                        help="Output Excel format")
    parser.add_argument("--user-notes", default="",
                        help="Operator notes providing additional context for extraction")
    parser.add_argument("--ai-instructions", default="",
                        help="Direct instructions to the AI for classification and extraction")
    parser.add_argument("--context-file", default=None,
                        help="Path to client context index JSON (prior-year data, payer info)")
    parser.add_argument("--resume", action="store_true",
                        help="Resume from checkpoint if available (crash recovery)")
    parser.add_argument("--no-ocr-first", action="store_true",
                        help="Disable OCR-first optimization (force vision-only, more expensive)")
    args = parser.parse_args()

    # ─── Regen-only mode: read log JSON → populate_template → done ───
    if args.regen_excel:
        if not args.log_input or not args.output:
            sys.exit("ERROR: --regen-excel requires --log-input <log.json> and --output <file.xlsx>")
        if not os.path.exists(args.log_input):
            sys.exit(f"ERROR: Log file not found: {args.log_input}")
        with open(args.log_input) as f:
            log_data = json.load(f)
        extractions = log_data.get("extractions", [])
        if not extractions:
            sys.exit("ERROR: No extractions found in log file")
        # Normalize brokerage data (in case corrections changed composite fields)
        extractions = normalize_brokerage_data(extractions)
        fmt = log_data.get("output_format", args.output_format)
        populate_template(extractions, args.template, args.output, args.year, output_format=fmt)
        print(f"  Regenerated: {args.output}")
        return

    # ─── Normal extraction mode ───
    if not args.pdf:
        sys.exit("ERROR: PDF path is required (unless using --regen-excel)")
    if not os.path.exists(args.pdf):
        sys.exit(f"ERROR: File not found: {args.pdf}")
    if not os.environ.get("ANTHROPIC_API_KEY"):
        sys.exit("ERROR: export ANTHROPIC_API_KEY=sk-ant-...")

    output = args.output or args.pdf.replace(".pdf", "_intake.xlsx").replace(".PDF", "_intake.xlsx")

    # Initialize PII tokenizer
    tokenizer = None if args.no_pii else PIITokenizer()

    # Initialize cost tracker
    global _cost_tracker
    _cost_tracker = CostTracker()

    print("=" * 60)
    print("  Document Intake Extractor v6")
    print("  OCR-first | Vision fallback | Checkpointing | Cost tracking")
    print("=" * 60)
    print(f"  PDF:      {args.pdf}")
    print(f"  Year:     {args.year}")
    print(f"  Output:   {output}")
    print(f"  OCR-first:{'YES (cheap text when readable)' if not args.no_ocr_first else 'DISABLED (vision-only)'}")
    print(f"  PII:      {'TOKENIZED (Tesseract for SSN detection)' if tokenizer and HAS_TESSERACT else 'TOKENIZED (no Tesseract — text only)' if tokenizer else 'DISABLED (raw data sent)'}")
    print(f"  Verify:   {'YES' if not args.skip_verify else 'SKIPPED'}")

    client = anthropic.Anthropic()
    b64_images = pdf_to_images(args.pdf, args.dpi)

    # Run OCR on all pages upfront (free, local — enables OCR-first extraction)
    ocr_texts = None
    if not args.no_ocr_first and HAS_TESSERACT:
        ocr_texts = ocr_all_pages([
            Image.open(BytesIO(base64.b64decode(b))) for b in b64_images
        ])
    elif args.no_ocr_first:
        print("\n  OCR-first: DISABLED (--no-ocr-first, all pages use vision)")
    else:
        print("\n  OCR-first: unavailable (Tesseract not installed)")
        ocr_texts = None

    # Load prior-year context if provided
    context_summary = ""
    context_data = None
    if args.context_file and os.path.exists(args.context_file):
        try:
            with open(args.context_file) as cf:
                context_data = json.load(cf)
            prior_docs = context_data.get("prior_year_data", {}).get("documents", [])
            if prior_docs:
                payer_lines = []
                for doc in prior_docs:
                    for p in doc.get("payers", []):
                        name = p.get("name", "Unknown")
                        ein = p.get("ein", "")
                        ftype = p.get("form_type", "")
                        amounts = p.get("amounts", [])
                        amt_str = ", ".join(f"${a:,.2f}" for a in amounts[:3]) if amounts else "amounts unknown"
                        payer_lines.append(f"  {ftype or 'Unknown form'}: {name} (EIN {ein}) — {amt_str}")
                if payer_lines:
                    context_summary = (
                        "PRIOR-YEAR REFERENCE DATA (use for sanity-checking, NOT for values):\n"
                        + "\n".join(payer_lines[:30])  # cap at 30 payers
                    )
                    print(f"  Context: {len(payer_lines)} prior-year payers loaded")
        except (json.JSONDecodeError, IOError) as e:
            print(f"  Warning: Could not load context file: {e}")

    # Merge context into ai_instructions
    effective_instructions = args.ai_instructions
    if context_summary:
        effective_instructions = (effective_instructions + "\n\n" + context_summary
                                  if effective_instructions else context_summary)

    # ─── Check for checkpoint (crash recovery) ───
    checkpoint = load_checkpoint(output) if args.resume else None
    resume_phase = checkpoint.get("completed_phase", "") if checkpoint else ""

    if resume_phase:
        print(f"\n  ⟳ Resuming from checkpoint (completed: {resume_phase})")

    # Phase 1: Classify (vision — need to see layout)
    if resume_phase in ("classify", "group", "extract", "verify"):
        classifications = checkpoint.get("classifications", [])
        print(f"\n── Phase 1: Classification (restored {len(classifications)} from checkpoint) ──")
    else:
        classifications = classify_pages(client, b64_images, tokenizer=tokenizer, doc_type=args.doc_type, user_notes=args.user_notes, ai_instructions=effective_instructions)
        save_checkpoint(output, "classify", classifications=classifications)

    # Phase 1.5: Group
    if resume_phase in ("group", "extract", "verify"):
        groups = checkpoint.get("groups", [])
        print(f"\n  {len(groups)} document groups (from checkpoint)")
    else:
        groups = group_pages(classifications)
        save_checkpoint(output, "group", classifications=classifications, groups=groups)

    print(f"\n  {len(groups)} document groups:")
    for g in groups:
        cont = g.get("continuation_pages", [])
        brok = " [BROKERAGE]" if g.get("is_consolidated_brokerage") else ""
        print(f"    {g['document_type']}: {g['payer_or_entity']} (EIN: {g.get('payer_ein','?')}) pp. {g['pages']}{f' + {cont}' if cont else ''}{brok}")

    # Phase 2: Extract (OCR-first with vision fallback)
    if resume_phase in ("extract", "verify"):
        extractions = checkpoint.get("extractions", [])
        print(f"\n── Phase 2: Extraction (restored {len(extractions)} from checkpoint) ──")
    else:
        extractions = extract_data(client, b64_images, groups, tokenizer=tokenizer,
                                    doc_type=args.doc_type, user_notes=args.user_notes,
                                    ai_instructions=effective_instructions, ocr_texts=ocr_texts)
        save_checkpoint(output, "extract", classifications=classifications, groups=groups, extractions=extractions)

    # Phase 3: Verify (cross-check critical fields against image)
    if resume_phase == "verify":
        print(f"\n── Phase 3: Verification (already done in checkpoint) ──")
    elif not args.skip_verify:
        extractions = verify_extractions(client, b64_images, extractions, tokenizer=tokenizer)
        save_checkpoint(output, "verify", classifications=classifications, groups=groups, extractions=extractions)

    # Phase 4: Normalize
    extractions = normalize_brokerage_data(extractions)

    # Phase 5: Validate
    warnings = validate(extractions, prior_year_context=context_data)

    # Summary
    print_summary(extractions)

    # Cost summary
    print("\n── Cost ──")
    print(_cost_tracker.summary())

    # PII tokenization summary
    if tokenizer:
        stats = tokenizer.get_stats()
        if stats["ssns_tokenized"] > 0:
            print(f"\n  🔒 PII: {stats['ssns_tokenized']} SSN(s) tokenized before API calls")
        else:
            print(f"\n  🔒 PII: tokenizer active, no SSN patterns detected")

    # Save
    save_log(extractions, classifications, warnings, output,
             output_format=args.output_format, user_notes=args.user_notes,
             ai_instructions=effective_instructions, cost_data=_cost_tracker.to_dict())
    if not args.log_only:
        populate_template(extractions, args.template, output, args.year, output_format=args.output_format)

    # Clean up checkpoint on success
    clear_checkpoint(output)

    print("\n" + "=" * 60)
    print("  COMPLETE")
    if warnings:
        print(f"  ⚠ {len(warnings)} items flagged")
    print(f"  ⚠ {len(REQUIRES_HUMAN_REVIEW)} PY/judgment items need manual entry")
    print(f"  💰 Est. cost: ${_cost_tracker.total_cost():.4f}")
    print("=" * 60)

if __name__ == "__main__":
    main()
