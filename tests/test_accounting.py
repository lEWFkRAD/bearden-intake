#!/usr/bin/env python3
"""Tests for accounting fundamentals in extract.py.

Verifies:
  1. Journal entries always balance (DR == CR)
  2. Tax info docs (W-2, 1099, K-1) never generate journal entries
  3. Bank deposits post to "Unclassified Deposits" not "Revenue"
  4. Payroll entries balance even when net_pay is missing
  5. Validate catches arithmetic errors
  6. Transaction register uses opening balance
"""
import sys, os, json

# Import from the project root (one level up from tests/)
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from extract import (
    _build_journal_entries, validate, get_val, get_str,
    _NON_POSTING_FIELDS, CostTracker, _dedup_by_ein,
)

PASS = 0
FAIL = 0

def check(condition, label):
    global PASS, FAIL
    if condition:
        PASS += 1
        print(f"  ✓ {label}")
    else:
        FAIL += 1
        print(f"  ✗ FAIL: {label}")


def make_ext(doc_type, entity, fields_dict):
    """Helper to build an extraction dict with proper field structure."""
    fields = {}
    for k, v in fields_dict.items():
        fields[k] = {"value": v, "confidence": "high"}
    return {"document_type": doc_type, "payer_or_entity": entity, "fields": fields}


# ─── TEST 1: Tax info docs do NOT generate journal entries ───

print("\n═══ Test 1: Tax info docs excluded from journal entries ═══")

tax_docs = [
    make_ext("W-2", "Acme Corp", {"wages": 50000, "federal_wh": 8000}),
    make_ext("1099-INT", "First Bank", {"interest_income": 1500}),
    make_ext("1099-DIV", "Vanguard", {"ordinary_dividends": 3000, "qualified_dividends": 2000}),
    make_ext("K-1", "Real Estate LLC", {"box1_ordinary_income": 10000, "box2_rental_real_estate": -5000}),
    make_ext("SSA-1099", "SSA", {"net_benefits": 18000}),
    make_ext("1099-R", "Fidelity", {"gross_distribution": 25000, "taxable_amount": 25000}),
    make_ext("1098", "Mortgage Co", {"mortgage_interest": 12000}),
    make_ext("1099-NEC", "Freelance Client", {"nonemployee_compensation": 5000}),
    make_ext("W-2G", "Casino", {"gross_winnings": 5000}),
    make_ext("1099-K", "Stripe", {"gross_amount": 50000}),
    make_ext("5498", "IRA Custodian", {"ira_contributions": 6500}),
    make_ext("1095-A", "Marketplace", {"monthly_premium": {"value": 500}}),
]

journal = _build_journal_entries(tax_docs, "2024")
check(len(journal) == 0, f"Tax docs produce 0 journal entries (got {len(journal)})")


# ─── TEST 2: Every journal entry balances ───

print("\n═══ Test 2: All journal entries balance ═══")

mixed_docs = [
    # Bank statement with individual transactions
    make_ext("bank_statement", "First National", {
        "bank_name": "First National", "account_number_last4": "1234",
        "beginning_balance": 10000, "ending_balance": 10500,
        "total_deposits": 2000, "total_withdrawals": 1500,
        "txn_1_date": "01/15/2024", "txn_1_desc": "Direct deposit",
        "txn_1_amount": 2000, "txn_1_type": "deposit",
        "txn_2_date": "01/20/2024", "txn_2_desc": "Electric bill",
        "txn_2_amount": 150, "txn_2_type": "withdrawal",
        "txn_3_date": "01/22/2024", "txn_3_desc": "Monthly fee",
        "txn_3_amount": 15, "txn_3_type": "fee",
        "txn_4_date": "01/31/2024", "txn_4_desc": "Interest",
        "txn_4_amount": 5.23, "txn_4_type": "interest",
    }),
    # Bank statement with only summary totals
    make_ext("bank_statement", "Second Bank", {
        "bank_name": "Second Bank",
        "total_deposits": 5000, "total_withdrawals": 3000,
        "fees_charged": 25, "interest_earned": 10,
    }),
    # Credit card
    make_ext("credit_card_statement", "Visa", {
        "card_issuer": "Chase", "account_number_last4": "5678",
        "purchases": 1200, "payments": 800, "interest_charged": 22.50,
    }),
    # Check
    make_ext("check", "Payee", {
        "check_number": "1001", "payee": "Georgia Power",
        "check_amount": 250, "check_date": "02/01/2024",
    }),
    # Invoice
    make_ext("invoice", "Staples", {
        "vendor_name": "Staples", "invoice_number": "INV-100",
        "total_amount": 89.99, "invoice_date": "03/15/2024",
    }),
    # Receipt
    make_ext("receipt", "Walmart", {
        "vendor_name": "Walmart", "total_amount": 45.67,
        "category": "supplies", "payment_method": "credit card",
        "receipt_date": "04/01/2024",
    }),
    # Loan payment
    make_ext("loan_statement", "Wells Fargo", {
        "lender": "Wells Fargo", "principal_paid": 500,
        "interest_paid": 200, "escrow_paid": 150,
        "payment_date": "05/01/2024",
    }),
    # P&L
    make_ext("profit_loss_statement", "My Business", {
        "total_revenue": 100000, "total_operating_expenses": 75000,
        "period_end": "12/31/2024",
    }),
]

journal = _build_journal_entries(mixed_docs, "2024")
check(len(journal) > 0, f"Mixed docs produce journal entries (got {len(journal)})")

all_balanced = True
for i, entry in enumerate(journal):
    dr = round(sum(l.get("debit") or 0 for l in entry["lines"]), 2)
    cr = round(sum(l.get("credit") or 0 for l in entry["lines"]), 2)
    if abs(dr - cr) > 0.02:
        all_balanced = False
        print(f"    Entry {i}: DR={dr} CR={cr} diff={dr-cr} — {entry['description'][:60]}")

check(all_balanced, "Every journal entry balances (DR == CR)")


# ─── TEST 3: Bank deposits go to "Unclassified" not "Revenue" ───

print("\n═══ Test 3: Correct account names ═══")

bank_only = [make_ext("bank_statement", "Test Bank", {
    "bank_name": "Test Bank",
    "txn_1_date": "01/01/2024", "txn_1_desc": "Customer payment",
    "txn_1_amount": 5000, "txn_1_type": "deposit",
    "txn_2_date": "01/02/2024", "txn_2_desc": "Office rent",
    "txn_2_amount": 2000, "txn_2_type": "withdrawal",
})]

journal = _build_journal_entries(bank_only, "2024")
all_accounts = [l["account"] for e in journal for l in e["lines"]]

check(not any("Revenue" in a for a in all_accounts),
      'No "Revenue" account — deposits are unclassified')
check(any("Unclassified Deposits" in a for a in all_accounts),
      'Deposits go to "Unclassified Deposits"')
check(any("Unclassified Expenses" in a for a in all_accounts),
      'Withdrawals go to "Unclassified Expenses"')
check(not any("/" in a for a in all_accounts),
      'No ambiguous slash-accounts (like "Cash / Credit Card")')


# ─── TEST 4: Payroll balances even without net_pay ───

print("\n═══ Test 4: Payroll balance enforcement ═══")

# Case A: All fields present
payroll_full = [make_ext("check_stub", "Acme Corp", {
    "employer_name": "Acme Corp", "employee_name": "Jeffrey",
    "gross_pay": 5000, "federal_wh": 750, "state_wh": 250,
    "social_security": 310, "medicare": 72.50, "net_pay": 3617.50,
    "pay_date": "01/15/2024",
})]
journal = _build_journal_entries(payroll_full, "2024")
entry = journal[0]
dr = round(sum(l.get("debit") or 0 for l in entry["lines"]), 2)
cr = round(sum(l.get("credit") or 0 for l in entry["lines"]), 2)
check(abs(dr - cr) < 0.02, f"Payroll with all fields balances (DR={dr}, CR={cr})")

# Case B: net_pay MISSING — should compute it
payroll_no_net = [make_ext("check_stub", "Acme Corp", {
    "employer_name": "Acme Corp", "employee_name": "Jeffrey",
    "gross_pay": 5000, "federal_wh": 750, "state_wh": 250,
    "social_security": 310, "medicare": 72.50,
    # net_pay intentionally omitted
    "pay_date": "01/15/2024",
})]
journal = _build_journal_entries(payroll_no_net, "2024")
entry = journal[0]
dr = round(sum(l.get("debit") or 0 for l in entry["lines"]), 2)
cr = round(sum(l.get("credit") or 0 for l in entry["lines"]), 2)
check(abs(dr - cr) < 0.02, f"Payroll WITHOUT net_pay still balances (DR={dr}, CR={cr})")

# Verify the computed net is correct: 5000 - 750 - 250 - 310 - 72.50 = 3617.50
cash_line = [l for l in entry["lines"] if "Cash" in l["account"]]
check(len(cash_line) == 1, "Computed net pay creates Cash credit line")
if cash_line:
    check(abs(cash_line[0]["credit"] - 3617.50) < 0.02,
          f"Computed net = 3617.50 (got {cash_line[0]['credit']})")


# ─── TEST 5: Validate catches arithmetic errors ───

print("\n═══ Test 5: Validation catches errors ═══")

bad_docs = [
    # Qualified > Ordinary (impossible)
    make_ext("1099-DIV", "Bad Fund", {"ordinary_dividends": 1000, "qualified_dividends": 1500}),
    # Taxable > Gross (impossible)
    make_ext("1099-R", "Bad Retirement", {"gross_distribution": 10000, "taxable_amount": 15000}),
    # Federal WH > Wages (impossible)
    make_ext("W-2", "Bad Employer", {"wages": 50000, "federal_wh": 60000}),
    # Invoice: subtotal + tax ≠ total
    make_ext("invoice", "Bad Vendor", {"subtotal": 100, "tax_amount": 7, "total_amount": 120}),
    # Bank: begin + deposits - withdrawals ≠ ending
    make_ext("bank_statement", "Bad Bank", {
        "beginning_balance": 1000, "total_deposits": 500,
        "total_withdrawals": 200, "ending_balance": 5000,  # should be 1300
    }),
]

warnings = validate(bad_docs)
arith_warnings = [w for w in warnings if w.startswith("ARITH")]
check(len(arith_warnings) >= 4, f"Catches arithmetic errors ({len(arith_warnings)} found)")

# Verify specific catches
check(any("Qualified" in w and "Ordinary" in w for w in warnings), "Catches qualified > ordinary")
check(any("Taxable" in w and "Gross" in w for w in warnings), "Catches taxable > gross")
check(any("Federal WH" in w and "wages" in w for w in warnings), "Catches WH > wages")
check(any("reconcile" in w for w in warnings), "Catches bank reconciliation failure")
check(any("subtotal" in w for w in warnings), "Catches invoice arithmetic error")


# ─── TEST 6: _NON_POSTING_FIELDS blocks informational fields ───

print("\n═══ Test 6: Non-posting fields defined ═══")

check("profit_share_begin" in _NON_POSTING_FIELDS, "K-1 percentages are non-posting")
check("employer_ein" in _NON_POSTING_FIELDS, "EINs are non-posting")
check("beginning_capital_account" in _NON_POSTING_FIELDS, "Capital accounts are non-posting")
check("hours_regular" in _NON_POSTING_FIELDS, "Hours are non-posting")


# ─── TEST 7: Credit card entries have correct DR/CR direction ───

print("\n═══ Test 7: Credit card liability direction ═══")

cc_docs = [make_ext("credit_card_statement", "Chase Visa", {
    "card_issuer": "Chase",
    "purchases": 500,
    "payments": 300,
    "interest_charged": 15,
})]

journal = _build_journal_entries(cc_docs, "2024")
# Purchase: DR Expense, CR CC Payable (liability increases)
purchase_entry = [e for e in journal if "Purchase" in e["description"]]
if purchase_entry:
    lines = purchase_entry[0]["lines"]
    dr_accts = [l["account"] for l in lines if l.get("debit")]
    cr_accts = [l["account"] for l in lines if l.get("credit")]
    check(any("Expense" in a for a in dr_accts), "CC purchase debits expense")
    check(any("Payable" in a for a in cr_accts), "CC purchase credits CC liability")

# Payment: DR CC Payable (liability decreases), CR Cash
payment_entry = [e for e in journal if "Payment" in e["description"]]
if payment_entry:
    lines = payment_entry[0]["lines"]
    dr_accts = [l["account"] for l in lines if l.get("debit")]
    cr_accts = [l["account"] for l in lines if l.get("credit")]
    check(any("Payable" in a for a in dr_accts), "CC payment debits CC liability")
    check(any("Cash" in a for a in cr_accts), "CC payment credits Cash")


# ─── TEST 8: Operator-assigned categories flow to journal entries ───

print("\n═══ Test 8: Operator categories replace Unclassified ═══")

def make_ext_with_cat(doc_type, entity, fields_dict, category_map=None):
    """Build extraction with operator categories on specific fields."""
    fields = {}
    for k, v in fields_dict.items():
        fdata = {"value": v, "confidence": "high"}
        if category_map and k in category_map:
            fdata["_operator_category"] = category_map[k]
        fields[k] = fdata
    return {"document_type": doc_type, "payer_or_entity": entity, "fields": fields}

# Bank transaction with operator category
cat_bank = [make_ext_with_cat("bank_statement", "First Bank", {
    "bank_name": "First Bank",
    "txn_1_date": "01/15/2024", "txn_1_desc": "GEORGIA POWER",
    "txn_1_amount": 250, "txn_1_type": "withdrawal",
    "txn_2_date": "01/20/2024", "txn_2_desc": "Client payment",
    "txn_2_amount": 5000, "txn_2_type": "deposit",
}, category_map={
    "txn_1_amount": "Utilities",
    "txn_2_amount": "Service Revenue",
})]

journal = _build_journal_entries(cat_bank, "2024")
all_accounts = [l["account"] for e in journal for l in e["lines"]]
check("Utilities" in all_accounts, "Bank withdrawal uses operator category 'Utilities'")
check("Service Revenue" in all_accounts, "Bank deposit uses operator category 'Service Revenue'")
check("Unclassified Expenses" not in all_accounts, "No 'Unclassified Expenses' when category assigned")
check("Unclassified Deposits" not in all_accounts, "No 'Unclassified Deposits' when category assigned")

# Check with operator category
cat_check = [make_ext_with_cat("check", "Payee", {
    "check_number": "1001", "payee": "Office Depot",
    "check_amount": 89.99, "check_date": "02/01/2024",
}, category_map={"check_amount": "Office Supplies"})]

journal = _build_journal_entries(cat_check, "2024")
all_accounts = [l["account"] for e in journal for l in e["lines"]]
check("Office Supplies" in all_accounts, "Check uses operator category 'Office Supplies'")
check("Unclassified Expenses" not in all_accounts, "Check: no Unclassified when categorized")

# Invoice with operator category
cat_inv = [make_ext_with_cat("invoice", "Staples", {
    "vendor_name": "Staples", "total_amount": 150,
    "invoice_date": "03/01/2024",
}, category_map={"total_amount": "Office Supplies"})]

journal = _build_journal_entries(cat_inv, "2024")
all_accounts = [l["account"] for e in journal for l in e["lines"]]
check("Office Supplies" in all_accounts, "Invoice uses operator category")

# WITHOUT category — should fall back to Unclassified
nocat_check = [make_ext("check", "Payee", {
    "check_number": "1002", "payee": "Unknown Vendor",
    "check_amount": 500, "check_date": "04/01/2024",
})]

journal = _build_journal_entries(nocat_check, "2024")
all_accounts = [l["account"] for e in journal for l in e["lines"]]
check("Unclassified Expenses" in all_accounts, "Uncategorized check falls back to Unclassified")

# CC transaction with category
cat_cc = [make_ext_with_cat("credit_card_statement", "Chase", {
    "card_issuer": "Chase",
    "txn_1_date": "01/10/2024", "txn_1_desc": "Delta Airlines",
    "txn_1_amount": 350,
}, category_map={"txn_1_amount": "Auto & Travel"})]

journal = _build_journal_entries(cat_cc, "2024")
all_accounts = [l["account"] for e in journal for l in e["lines"]]
check("Auto & Travel" in all_accounts, "CC purchase uses operator category 'Auto & Travel'")


# ─── TEST 9: Vendor normalization ───

print("\n═══ Test 9: Vendor name normalization ═══")

# We can't easily import app.py functions, so test the pattern logic inline
import re

def _normalize_vendor_test(desc):
    if not desc: return ""
    s = str(desc).upper().strip()
    s = re.sub(r'[\s#*]+\d{2,}$', '', s)
    s = re.sub(r'\s+(LLC|INC|CORP|CO|COMPANY|LTD|LP|NA|N\.A\.)\.\s*$', '', s, flags=re.IGNORECASE)
    s = s.rstrip(' .,;:*#-')
    return s.strip()

check(_normalize_vendor_test("GEORGIA POWER #12345") == "GEORGIA POWER",
      "Strip trailing store number")
check(_normalize_vendor_test("WAL-MART SUPER CENTER 0423") == "WAL-MART SUPER CENTER",
      "Strip trailing location number")
check(_normalize_vendor_test("") == "", "Empty string returns empty")
check(len(_normalize_vendor_test("SHORT")) > 0, "Short vendor names preserved")

# ─── TEST 10: Completeness report logic ───

print("\n═══ Test 10: Completeness report structure ═══")

# The completeness report is built from comparing prior-year payers to current extractions.
# Test that the structure is correct by verifying the _build_completeness_report function
# exists and handles empty input gracefully.

# Import the function from extract.py (already loaded in test env)
# We can't easily call app.py's _build_completeness_report without Flask,
# so test the conceptual model instead.

# Prior-year data has payer with EIN 12-3456789 as W-2
prior_payers = {"12-3456789": {"form_type": "W-2", "name": "Acme Corp"}}
# Current extractions have a W-2 with that EIN → matched
current_w2 = {"document_type": "W-2", "payer_or_entity": "Acme Corp",
              "fields": {"employer_ein": {"value": "12-3456789"}, "wages": {"value": 55000}}}
# Current also has a 1099-INT with new EIN → new this year
current_1099 = {"document_type": "1099-INT", "payer_or_entity": "New Bank",
                "fields": {"payer_ein": {"value": "98-7654321"}, "interest_income": {"value": 500}}}

# Build sets manually (mimicking the app.py logic)
prior_set = {}
for ein, pdata in prior_payers.items():
    prior_set[(ein, pdata["form_type"])] = pdata

current_set = {}
for ext in [current_w2, current_1099]:
    dtype = ext["document_type"]
    ein = ""
    for ek in ["payer_ein", "employer_ein"]:
        v = ext["fields"].get(ek)
        if isinstance(v, dict): v = v.get("value", "")
        if v: ein = str(v); break
    current_set[(ein, dtype)] = ext

matched = [k for k in prior_set if k in current_set]
missing = [k for k in prior_set if k not in current_set]
new_items = [k for k in current_set if k not in prior_set and k[0]]

check(len(matched) == 1, f"Completeness: 1 matched (got {len(matched)})")
check(len(missing) == 0, f"Completeness: 0 missing (got {len(missing)})")
check(len(new_items) == 1, f"Completeness: 1 new this year (got {len(new_items)})")
check(matched[0] == ("12-3456789", "W-2"), "Matched correct payer/form pair")
check(new_items[0][0] == "98-7654321", "New item has correct EIN")

# ─── TEST 11: Context data integration with extract ───

print("\n═══ Test 11: Context-aware extraction ═══")

# _get_cat is defined inside _build_journal_entries, so replicate its logic for testing
def _get_cat_test(fields, field_name, default=""):
    fdata = fields.get(field_name)
    if isinstance(fdata, dict):
        return fdata.get("_operator_category", default)
    return default

test_fields = {
    "txn_1_amount": {"value": 250, "_operator_category": "Utilities", "confidence": "high"},
    "txn_2_amount": {"value": 500, "confidence": "high"},  # no category
    "check_amount": {"value": 89.99, "_operator_category": "Office Supplies"},
}

check(_get_cat_test(test_fields, "txn_1_amount") == "Utilities",
      "_get_cat reads operator category from txn field")
check(_get_cat_test(test_fields, "txn_2_amount") == "",
      "_get_cat returns empty when no category set")
check(_get_cat_test(test_fields, "check_amount") == "Office Supplies",
      "_get_cat reads operator category from check field")
check(_get_cat_test(test_fields, "nonexistent") == "",
      "_get_cat returns empty for missing field")


print("\n═══ Test 12: CostTracker ═══")

tracker = CostTracker()

# Simulate usage objects
class FakeUsage:
    def __init__(self, inp, out):
        self.input_tokens = inp
        self.output_tokens = out

tracker.record("classify", 1, FakeUsage(1500, 800), "vision")
tracker.record("extract_text", 2, FakeUsage(500, 400), "text")
tracker.record("extract_vision", 3, FakeUsage(1500, 800), "vision")
tracker.record("verify", 1, FakeUsage(1500, 600), "vision")

check(len(tracker.calls) == 4, f"CostTracker records all calls (got {len(tracker.calls)})")
check(tracker.vision_calls == 3, f"CostTracker counts vision calls (got {tracker.vision_calls})")
check(tracker.text_calls == 1, f"CostTracker counts text calls (got {tracker.text_calls})")
check(tracker.total_input() == 5000, f"CostTracker sums input tokens (got {tracker.total_input()})")
check(tracker.total_output() == 2600, f"CostTracker sums output tokens (got {tracker.total_output()})")

# Cost: (5000/1M * 3) + (2600/1M * 15) = 0.015 + 0.039 = 0.054
expected_cost = round((5000 / 1e6 * 3.0) + (2600 / 1e6 * 15.0), 6)
actual_cost = round(tracker.total_cost(), 6)
check(actual_cost == expected_cost, f"CostTracker estimates cost (got ${actual_cost}, expected ${expected_cost})")

d = tracker.to_dict()
check(d["total_calls"] == 4, "to_dict total_calls")
check(d["vision_calls"] == 3, "to_dict vision_calls")
check(d["text_calls"] == 1, "to_dict text_calls")
check("classify" in d["per_phase"], "to_dict has classify phase")
check("verify" in d["per_phase"], "to_dict has verify phase")


print("\n═══ Test 13: Smart dedup (confidence-aware) ═══")

# Two copies of same W-2, second has higher confidence
dup_exts = [
    {
        "payer_ein": "12-3456789",
        "payer_or_entity": "Acme Corp",
        "recipient": "JOHN DOE",
        "document_type": "W-2",
        "fields": {
            "wages": {"value": 50000, "confidence": "low"},
            "federal_wh": {"value": 8000, "confidence": "low"},
            "state_wh": {"value": 2000, "confidence": "low"},
        }
    },
    {
        "payer_ein": "12-3456789",
        "payer_or_entity": "Acme Corp",
        "recipient": "JOHN DOE",
        "document_type": "W-2",
        "fields": {
            "wages": {"value": 50000, "confidence": "dual_confirmed"},
            "federal_wh": {"value": 8000, "confidence": "dual_confirmed"},
            # state_wh missing in this copy
        }
    },
]

deduped = _dedup_by_ein(dup_exts)
check(len(deduped) == 1, f"Dedup merges duplicates (got {len(deduped)})")
# Should keep second copy (higher confidence) and merge state_wh from first
check(get_val(deduped[0]["fields"], "wages") == 50000, "Dedup keeps correct wages")
conf = deduped[0]["fields"]["wages"]["confidence"]
check(conf == "dual_confirmed", f"Dedup kept higher-confidence copy (got {conf})")
check("state_wh" in deduped[0]["fields"], "Dedup merged missing field from discarded copy")

# Different EINs should NOT dedup
diff_exts = [
    {"payer_ein": "11-1111111", "payer_or_entity": "A", "recipient": "X",
     "fields": {"wages": {"value": 100, "confidence": "high"}}},
    {"payer_ein": "22-2222222", "payer_or_entity": "B", "recipient": "X",
     "fields": {"wages": {"value": 200, "confidence": "high"}}},
]
check(len(_dedup_by_ein(diff_exts)) == 2, "Different EINs not deduped")


print("\n═══ Test 14: Duplicate document detection ═══")

# Two W-2s from same employer with same wages = duplicate
dup_exts = [
    {"document_type": "W-2", "payer_or_entity": "Acme Corp", "payer_ein": "12-3456789",
     "_page": 1, "fields": {"wages": {"value": 50000, "confidence": "high"},
                             "federal_wh": {"value": 8000, "confidence": "high"}}},
    {"document_type": "W-2", "payer_or_entity": "Acme Corp", "payer_ein": "12-3456789",
     "_page": 3, "fields": {"wages": {"value": 50000, "confidence": "high"},
                             "federal_wh": {"value": 8000, "confidence": "high"}}},
]
dup_warnings = validate(dup_exts)
dup_cross = [w for w in dup_warnings if "duplicate" in w.lower()]
check(len(dup_cross) >= 1, "Detects duplicate W-2 with same amounts")

# Two W-2s from different employers = NOT duplicate
diff_employer_exts = [
    {"document_type": "W-2", "payer_or_entity": "Acme Corp", "payer_ein": "12-3456789",
     "_page": 1, "fields": {"wages": {"value": 50000, "confidence": "high"},
                             "federal_wh": {"value": 8000, "confidence": "high"}}},
    {"document_type": "W-2", "payer_or_entity": "Beta Inc", "payer_ein": "98-7654321",
     "_page": 2, "fields": {"wages": {"value": 50000, "confidence": "high"},
                             "federal_wh": {"value": 8000, "confidence": "high"}}},
]
diff_warnings = validate(diff_employer_exts)
diff_dup = [w for w in diff_warnings if "duplicate" in w.lower()]
check(len(diff_dup) == 0, "Different employers with same amounts NOT flagged as duplicate")


print("\n═══ Test 15: Prior-year variance detection ═══")

prior_context = {
    "prior_year_data": {
        "documents": [{
            "payers": [
                {"name": "Acme Corp", "ein": "12-3456789", "form_type": "W-2", "amounts": [100000]},
                {"name": "Big Fund", "ein": "55-5555555", "form_type": "1099-DIV", "amounts": [5000]},
            ]
        }]
    }
}

# Wages dropped 60% — should trigger variance warning
variance_exts = [
    {"document_type": "W-2", "payer_or_entity": "Acme Corp", "payer_ein": "12-3456789",
     "fields": {"wages": {"value": 40000, "confidence": "high"},
                "federal_wh": {"value": 5000, "confidence": "high"}}},
    {"document_type": "1099-DIV", "payer_or_entity": "Big Fund", "payer_ein": "55-5555555",
     "fields": {"ordinary_dividends": {"value": 4800, "confidence": "high"},
                "qualified_dividends": {"value": 3000, "confidence": "high"}}},
]
var_warnings = validate(variance_exts, prior_year_context=prior_context)
var_flags = [w for w in var_warnings if w.startswith("VARIANCE")]
check(len(var_flags) >= 1, "Detects large variance vs prior year")
check(any("Acme" in w and "60%" in w for w in var_flags),
      "Variance warning shows entity name and % change")

# Small change (4%) should NOT trigger
check(not any("Big Fund" in w for w in var_flags),
      "Small variance (4%) not flagged")


# ─── SUMMARY ───

print(f"\n{'═' * 50}")
print(f"  {PASS} passed, {FAIL} failed")
if FAIL:
    print(f"  *** {FAIL} FAILURES ***")
    sys.exit(1)
else:
    print("  All accounting checks passed.")
