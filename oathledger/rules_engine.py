"""
OathLedger v2 (wired to inkspren.py)

Goal: produce a deterministic "tax_review payload" that mirrors inkspren.TEMPLATE_SECTIONS
so the Excel renderer can be a pure renderer, and rules can evolve independently.

This module intentionally focuses on *deterministic selection + shaping* of rows/values
to match your existing spreadsheet sections. CPA-grade semantic rules (GAAP/tax edge cases)
can be layered on top later without breaking the renderer contract.

Key properties:
- Stable ordering
- Same filtering/dedup logic as inkspren._populate_tax_review
- "k1_extras" (k1_detail section) is built exactly like inkspren
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple
import hashlib
import json

import inkspren  # relies on your existing helpers + TEMPLATE_SECTIONS


def _stable_json(obj: Any) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _sha256(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def _client_name_from_extractions(extractions: List[Dict[str, Any]]) -> str:
    # Mirrors inkspren._populate_tax_review behavior.
    for ext in extractions:
        fields = ext.get("fields", {}) or {}
        name = (
            inkspren.get_str(fields, "employee_name")
            or inkspren.get_str(fields, "recipient_name")
            or inkspren.get_str(fields, "recipient")
            or inkspren.get_str(fields, "borrower_name")
        )
        if name and len(name) > 2:
            return name
    for ext in extractions:
        name = ext.get("recipient", "")
        if name and len(name) > 2:
            return name
    return ""


def _resolve_field_value(fields: Dict[str, Any], field_name: str, ext: Dict[str, Any], matched: List[Dict[str, Any]]) -> Tuple[Any, Dict[str, Any]]:
    """
    Mirrors inkspren._write_cell_value logic (minus openpyxl styling).
    Returns: (value, meta) where meta can include review flags and comments.
    """
    meta: Dict[str, Any] = {}

    if field_name == "_display_name":
        name = inkspren.get_str(fields, "partnership_name") or ext.get("payer_or_entity", "")
        recip = ext.get("recipient", "")
        same = [
            e for e in matched
            if (e.get("payer_ein","") == ext.get("payer_ein","") and ext.get("payer_ein",""))
            or ((inkspren.get_str(e.get("fields",{}), "partnership_name") or e.get("payer_or_entity","")).upper() == str(name).upper())
        ]
        if len(same) > 1 and recip:
            name = f"{name} - {recip}"
        return name, meta

    if field_name == "_source_name":
        return ext.get("_source_name", ext.get("payer_or_entity", "")), meta

    if field_name == "_carryover_prior":
        # placeholder 0 + review marker
        meta["requires_prior_year_data"] = True
        meta["comment"] = "REQUIRES PRIOR YEAR DATA"
        return 0, meta

    if field_name in ("_allowed", "_carryover_next"):
        meta["requires_preparer_judgment"] = True
        meta["comment"] = "REQUIRES PREPARER JUDGMENT"
        return None, meta

    if field_name == "employer_name":
        name = inkspren.get_str(fields, "employer_name") or ext.get("payer_or_entity", "")
        # remove " (2024)" trailing tags
        name = inkspren.re.sub(r"\s*\(\d{4}\)\s*$", "", name)
        return name, meta

    if field_name in ("payer_or_entity", "institution_name"):
        return inkspren.get_str(fields, field_name) or ext.get("payer_or_entity", ""), meta

    # Normal value: numeric first, then string.
    val = inkspren.get_val(fields, field_name)
    if val is None:
        val = inkspren.get_str(fields, field_name)

    # Attach confidence comments (mirrors inkspren but keeps it as metadata)
    fdata = fields.get(field_name)
    if isinstance(fdata, dict):
        conf = fdata.get("confidence", "")
        if conf == "verified_corrected":
            meta["comment"] = f"Corrected: was {fdata.get('original_value','?')}. {fdata.get('correction_note','')}".strip()
            meta["confidence"] = conf
        elif conf == "low":
            meta["comment"] = "Low confidence — check source"
            meta["confidence"] = conf
        elif conf == "operator_corrected":
            meta["comment"] = f"Operator corrected (was {fdata.get('original_value','?')})"
            meta["confidence"] = conf

    return val, meta


def build_tax_review_payload(extractions: List[Dict[str, Any]], year: int, *, precision: str = "cents") -> Dict[str, Any]:
    """
    Output contract:
    - payload['sections'] is an ordered list matching inkspren.TEMPLATE_SECTIONS (but excluding empty sections),
      plus the special k1_extras section when present.
    - Each section contains rows where keys align to the section columns mapping.
    - Deterministic ordering.
    """
    client_name = _client_name_from_extractions(extractions)

    k1_extras: List[Dict[str, Any]] = []
    sections_out: List[Dict[str, Any]] = []

    for section in inkspren.TEMPLATE_SECTIONS:
        sid = section["id"]

        # Special block handled after K-1 section, same as inkspren.
        if section.get("special") == "k1_extras":
            continue

        match_types = section.get("match_types", []) or []
        matched = inkspren._match_exts(extractions, match_types) if match_types else []
        matched = inkspren._dedup_by_ein(matched) if matched else []

        columns = section.get("columns", {}) or {}
        field_aliases = section.get("field_aliases", {}) or {}

        # Filter out zero-value entries for interest/dividend sections
        if sid in ("interest", "dividends") and matched:
            def has_nonzero(ext: Dict[str, Any]) -> bool:
                fields = ext.get("fields", {}) or {}
                for fn in columns.values():
                    if isinstance(fn, str) and fn.startswith("_"):
                        continue
                    v = inkspren.get_val(fields, fn) or 0
                    if v != 0:
                        return True
                return False
            matched = [e for e in matched if has_nonzero(e)]

        if not matched:
            continue

        # Stable sort: by payer_or_entity/_source_name then doc_id fallback.
        def sort_key(ext: Dict[str, Any]) -> Tuple[str, str]:
            fields = ext.get("fields", {}) or {}
            payer = ext.get("_source_name") or ext.get("payer_or_entity") or inkspren.get_str(fields, "employer_name") or ""
            doc = ext.get("doc_id") or ext.get("document_id") or ""
            return (str(payer).upper(), str(doc))
        matched = sorted(matched, key=sort_key)

        rows: List[Dict[str, Any]] = []

        for ext in matched:
            fields = dict((ext.get("fields", {}) or {}).copy())

            # Apply field aliases deterministically
            for primary, alternates in field_aliases.items():
                if primary not in fields:
                    for alt in alternates:
                        if alt in fields:
                            fields[primary] = fields[alt]
                            break

            row_fields: Dict[str, Any] = {}
            row_meta: Dict[str, Any] = {"source": {}}

            for col, field_name in columns.items():
                val, meta = _resolve_field_value(fields, field_name, ext, matched)
                row_fields[field_name] = val  # key by field_name so payload is schema-driven
                if meta:
                    row_meta.setdefault("field_meta", {})[field_name] = meta

            # K-1 extras collection (mirrors inkspren)
            if sid == "k1":
                entity_name = inkspren.get_str(fields, "partnership_name") or ext.get("payer_or_entity", "")
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
                    v = inkspren.get_val(fields, fkey)
                    if v and v != 0:
                        k1_extras.append({"entity": entity_name, "line_reference": label, "description": "", "amount": v})
                for ci in ext.get("continuation_items", []) or []:
                    if ci.get("amount") and ci["amount"] != 0:
                        k1_extras.append({
                            "entity": entity_name,
                            "line_reference": ci.get("line_reference", ""),
                            "description": ci.get("description", ""),
                            "amount": ci["amount"],
                        })

            # Minimal provenance (expand as needed)
            row_meta["source"] = {
                "document_type": ext.get("document_type"),
                "payer_or_entity": ext.get("payer_or_entity"),
                "payer_ein": ext.get("payer_ein"),
                "doc_id": ext.get("doc_id") or ext.get("document_id"),
                "page": ext.get("page"),
                "method": ext.get("extraction_method") or ext.get("method"),
            }

            rows.append({"fields": row_fields, "meta": row_meta})

        sections_out.append({
            "id": sid,
            "header": section.get("header", ""),
            "match_types": match_types,
            "columns": columns,
            "col_headers": section.get("col_headers", {}),
            "sum_cols": section.get("sum_cols", []),
            "total_formula_col": section.get("total_formula_col"),
            "flags": section.get("flags", []),
            "rows": rows,
        })

    # Append k1_extras section exactly when present (id "k1_detail")
    for section in inkspren.TEMPLATE_SECTIONS:
        if section.get("special") == "k1_extras":
            if k1_extras:
                sections_out.append({
                    "id": section["id"],
                    "header": section.get("header", ""),
                    "special": "k1_extras",
                    "rows": k1_extras,
                })
            break

    payload = {
        "engine": "oathledger",
        "rules_version": "v2-wired-inkspren",
        "tax_year": int(year),
        "precision": precision,
        "schema_hash": "sha256:" + _sha256(_stable_json(inkspren.TEMPLATE_SECTIONS)),
        "client_name": client_name,
        "sections": sections_out,
    }
    payload["payload_hash"] = "sha256:" + _sha256(_stable_json(payload))
    return payload
