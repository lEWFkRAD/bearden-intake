"""
Transaction Extract Bridge — T-TXN-LEDGER-1
=============================================
Parses txn_N_* fields from extraction log JSON and yields structured
transaction dicts suitable for TransactionStore.ingest_from_extraction().

ARCHITECTURAL RULE: This module must NEVER import extract.py, OCR,
vision, or PDF libraries. It only parses the JSON output structure.
"""

import re

# ── Forbidden-module guardrail ───────────────────────────────────────────────
_FORBIDDEN_MODULES = frozenset({
    'extract', 'pytesseract', 'anthropic', 'pdf2image',
    'PIL', 'Pillow', 'fitz',
})

# Document types that contain individual transactions
TXN_DOC_TYPES = frozenset({
    "bank_statement",
    "bank_statement_deposit_slip",
    "credit_card_statement",
    "check",
    "check_stub",
})


def parse_transactions_from_log(log_data):
    """Iterate extractions and yield structured transaction dicts.

    Args:
        log_data: Parsed JSON log (dict with "extractions" key)

    Yields:
        dict with keys:
            document_type, payer_entity, txn_index,
            txn_date, description, amount, txn_type, category,
            source_page, confidence, extraction_method
    """
    if not log_data or not isinstance(log_data, dict):
        return

    for ext in log_data.get("extractions", []):
        doc_type = ext.get("document_type", "")
        if doc_type not in TXN_DOC_TYPES:
            continue

        fields = ext.get("fields", {})
        if not fields:
            continue

        payer_entity = ext.get("payer_or_entity", "")
        page = ext.get("_page")
        extraction_method = ext.get("_extraction_method", "")

        # ── Bank / Credit Card: txn_N_date, txn_N_desc, txn_N_amount, txn_N_type ──
        txn_nums = sorted(set(
            int(m.group(1))
            for k in fields
            for m in [re.match(r"txn_(\d+)_", k)]
            if m
        ))

        for n in txn_nums:
            date_f = fields.get(f"txn_{n}_date")
            desc_f = fields.get(f"txn_{n}_desc")
            amt_f = fields.get(f"txn_{n}_amount")
            type_f = fields.get(f"txn_{n}_type")
            cat_f = fields.get(f"txn_{n}_category")  # credit cards may have this

            amount = _extract_numeric(amt_f)
            if amount is None:
                # Skip transactions with no amount
                continue

            yield {
                "document_type": doc_type,
                "payer_entity": payer_entity,
                "txn_index": n,
                "txn_date": _extract_value(date_f),
                "description": _extract_value(desc_f),
                "amount": amount,
                "txn_type": _extract_value(type_f),
                "category": _extract_value(cat_f),
                "source_page": page,
                "confidence": _extract_confidence(amt_f),
                "extraction_method": extraction_method,
            }

        # ── Check documents: single transaction ──
        if doc_type in ("check", "check_stub"):
            # Only yield if we didn't already yield txn_N_* fields
            if txn_nums:
                continue

            amt_f = fields.get("check_amount")
            amt_val = _extract_numeric(amt_f)
            if amt_val is None:
                continue

            payee_f = fields.get("payee") or fields.get("pay_to")
            date_f = fields.get("check_date")
            num_f = fields.get("check_number")

            payee = _extract_value(payee_f)
            num_v = _extract_value(num_f)
            desc = f"Check #{num_v} to {payee}" if num_v and payee else (
                f"Check #{num_v}" if num_v else payee or "Check"
            )

            yield {
                "document_type": doc_type,
                "payer_entity": payer_entity,
                "txn_index": 1,
                "txn_date": _extract_value(date_f),
                "description": desc,
                "amount": amt_val,
                "txn_type": "check",
                "category": "",
                "source_page": page,
                "confidence": _extract_confidence(amt_f),
                "extraction_method": extraction_method,
            }


def _extract_value(field_data):
    """Extract string value from field data (handles dict or scalar).

    Field data can be:
      - {"value": "foo", "confidence": "high", ...}  (standard format)
      - "foo"  (bare scalar)
      - None
    """
    if field_data is None:
        return ""
    if isinstance(field_data, dict):
        v = field_data.get("value", "")
        return str(v) if v is not None else ""
    return str(field_data)


def _extract_numeric(field_data):
    """Extract numeric value from field data. Returns float or None.

    Strips currency symbols, commas, whitespace. Handles negative values
    in parentheses: "(1,234.56)" → -1234.56
    """
    raw = _extract_value(field_data)
    if not raw:
        return None
    raw = raw.strip()

    # Handle parenthesized negatives: (1,234.56)
    negative = False
    if raw.startswith("(") and raw.endswith(")"):
        raw = raw[1:-1]
        negative = True

    raw = raw.replace(",", "").replace("$", "").replace(" ", "")
    if not raw:
        return None

    try:
        val = float(raw)
        return -val if negative else val
    except (ValueError, TypeError):
        return None


def _extract_confidence(field_data):
    """Extract confidence string from field data."""
    if isinstance(field_data, dict):
        return field_data.get("confidence", "")
    return ""
