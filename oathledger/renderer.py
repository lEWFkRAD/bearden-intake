"""
Renderer that writes to the same Tax Review worksheet layout as inkspren._populate_tax_review,
but driven by an explicit payload (see oathledger.rules_engine.build_tax_review_payload).

This allows:
- deterministic payload generation (rules)
- deterministic Excel rendering (presentation)
- swap templates later without rewriting rules
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

import inkspren
from openpyxl.comments import Comment
from openpyxl.styles import Alignment


def _write_cell_from_payload(ws, col: str, row: int, field_name: str, value: Any, meta: Dict[str, Any] | None):
    cell = ws[f"{col}{row}"]
    cell.value = value

    # numeric formatting mirrors inkspren._write_cell_value
    if isinstance(value, (int, float)):
        cell.number_format = inkspren.MONEY_FMT
        cell.alignment = Alignment(horizontal="right")

    if meta and meta.get("comment"):
        cell.comment = Comment(str(meta["comment"]), "System")
        # replicate review-fill behavior for "needs judgement/prior-year"
        if meta.get("requires_preparer_judgment") or meta.get("requires_prior_year_data"):
            cell.fill = inkspren.REVIEW_FILL


def populate_tax_review_from_payload(ws, payload: Dict[str, Any], year: int):
    """
    Writes the Tax Review worksheet using the same formatting constants and header layout
    that inkspren.py already defines.
    """
    client_name = payload.get("client_name", "") or ""
    row = inkspren._write_title(ws, "Document Intake", year, client_name=client_name)

    # k1_extras are rendered later by the special section "k1_detail" if included
    sections: List[Dict[str, Any]] = payload.get("sections", []) or []

    for section in sections:
        sid = section.get("id")

        # Special k1_detail block (mirrors inkspren special section)
        if section.get("special") == "k1_extras":
            rows = section.get("rows", []) or []
            if not rows:
                continue
            ws[f"A{row}"] = section.get("header", "")
            ws[f"A{row}"].font = inkspren.SECTION_FONT
            ws[f"A{row}"].fill = inkspren.SECTION_FILL
            for hcol, hlabel in [("B", "Line Ref"), ("C", "Description"), ("D", "Amount")]:
                cell = ws[f"{hcol}{row}"]
                cell.value = hlabel
                cell.font = inkspren.COL_HEADER_FONT
                cell.fill = inkspren.COL_HEADER_FILL
                cell.alignment = Alignment(horizontal="center")
            for c in ["E", "F"]:
                ws[f"{c}{row}"].fill = inkspren.SECTION_FILL
            row += 1
            for item in rows:
                ws[f"A{row}"] = item.get("entity", "")
                ws[f"B{row}"] = item.get("line_reference", "")
                ws[f"C{row}"] = item.get("description", "")
                amt_cell = ws[f"D{row}"]
                amt_cell.value = item.get("amount")
                if isinstance(amt_cell.value, (int, float)):
                    amt_cell.number_format = inkspren.MONEY_FMT
                    amt_cell.alignment = Alignment(horizontal="right")
                for c in ["A", "B", "C", "D"]:
                    ws[f"{c}{row}"].border = inkspren.THIN_BORDER
                row += 1
            row += 1
            continue

        rows = section.get("rows", []) or []
        if not rows:
            continue

        columns = section.get("columns", {}) or {}
        col_headers = section.get("col_headers", {}) or {}
        sum_cols = section.get("sum_cols", []) or []

        ws[f"A{row}"] = section.get("header", "")
        ws[f"A{row}"].font = inkspren.SECTION_FONT
        ws[f"A{row}"].fill = inkspren.SECTION_FILL

        for col, label in col_headers.items():
            cell = ws[f"{col}{row}"]
            cell.value = label
            cell.font = inkspren.COL_HEADER_FONT
            cell.fill = inkspren.COL_HEADER_FILL
            cell.alignment = Alignment(horizontal="center")

        # Fill remaining header row columns with section fill, same as inkspren (A..F)
        for c in ["A", "B", "C", "D", "E", "F"]:
            if c not in col_headers:
                ws[f"{c}{row}"].fill = inkspren.SECTION_FILL

        row += 1
        data_start = row

        for r in rows:
            f = (r.get("fields", {}) or {})
            fmeta = ((r.get("meta", {}) or {}).get("field_meta", {}) or {})

            for col, field_name in columns.items():
                _write_cell_from_payload(ws, col, row, field_name, f.get(field_name), fmeta.get(field_name))

            # borders on data row
            for bcol in list(columns.keys()):
                ws[f"{bcol}{row}"].border = inkspren.THIN_BORDER
            row += 1

        data_end = row - 1

        # totals (mirrors inkspren)
        if data_end >= data_start and sum_cols:
            for col in sum_cols:
                cell = ws[f"{col}{row}"]
                cell.value = f"=SUM({col}{data_start}:{col}{data_end})"
                cell.font = inkspren.SUM_FONT
                cell.fill = inkspren.SUM_FILL
                cell.number_format = inkspren.MONEY_FMT
                cell.alignment = Alignment(horizontal="right")
                cell.border = inkspren.SUM_BORDER

            ws[f"A{row}"].font = inkspren.SUM_FONT
            ws[f"A{row}"].fill = inkspren.SUM_FILL
            ws[f"A{row}"].value = "TOTAL"
            ws[f"A{row}"].border = inkspren.SUM_BORDER

            if section.get("total_formula_col"):
                for col, formula in (section["total_formula_col"] or {}).items():
                    parts = str(formula).split("+")
                    cell = ws[f"{col}{row}"]
                    cell.value = "=" + "+".join([f"{p}{row}" for p in parts])
                    cell.font = inkspren.SUM_FONT
                    cell.number_format = inkspren.MONEY_FMT
                    cell.alignment = Alignment(horizontal="right")

            row += 1

        # flags
        for flag in (section.get("flags") or []):
            ws[f"A{row}"] = flag
            ws[f"A{row}"].font = inkspren.FLAG_FONT
            row += 1

        row += 1  # blank spacer like inkspren

    # NOTE: inkspren._populate_tax_review continues with Schedule A derived blocks etc.
    # This renderer intentionally stops after TEMPLATE_SECTIONS + k1_detail.
    # Keep the existing Schedule A logic in inkspren for now (lowest risk), or migrate it later
    # into payload-driven rendering once you decide the contract you want for those blocks.
