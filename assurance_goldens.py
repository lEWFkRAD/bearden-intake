"""CAS Golden Regression Tests — compare extraction against known-good baselines.

Runs extract.py against reference PDFs and compares output to expected.json.
This catches regressions in extraction quality after code changes.

Golden test case structure:
    data/goldens/<case_id>/
        input.pdf        — The reference document
        expected.json    — Expected extraction output
        config.json      — Optional: year, doc_type, flags

This module never modifies financial data. It runs extract.py as a subprocess
and compares its output to the expected baseline.

Run standalone:
    python3 assurance_goldens.py [--goldens-dir data/goldens] [--base-dir .]
"""

import os
import sys
import json
import time
import subprocess
import tempfile
from pathlib import Path


def _compare_values(expected, actual, tolerance_abs=0.01, tolerance_rel=0.001):
    """Compare two values with numeric tolerance.

    Args:
        expected: Expected value (from golden baseline).
        actual: Actual value (from extraction output).
        tolerance_abs: Absolute tolerance for numeric comparison (default 0.01).
        tolerance_rel: Relative tolerance for numeric comparison (default 0.1%).

    Returns:
        tuple (matched: bool, detail: str)
    """
    # Both None or empty
    if expected is None and actual is None:
        return True, "both None"
    if expected == "" and actual == "":
        return True, "both empty"

    # Try numeric comparison
    try:
        exp_num = float(str(expected).replace(",", "").replace("$", "").strip())
        act_num = float(str(actual).replace(",", "").replace("$", "").strip())

        abs_diff = abs(exp_num - act_num)
        if abs_diff <= tolerance_abs:
            return True, f"numeric match (diff={abs_diff:.4f})"

        # Relative check for larger numbers
        if exp_num != 0:
            rel_diff = abs_diff / abs(exp_num)
            if rel_diff <= tolerance_rel:
                return True, f"numeric match (rel_diff={rel_diff:.4%})"

        return False, f"numeric mismatch: expected={exp_num}, actual={act_num}, diff={abs_diff:.4f}"

    except (ValueError, TypeError):
        pass

    # Normalize strings for comparison
    exp_str = str(expected or "").strip().lower()
    act_str = str(actual or "").strip().lower()

    if exp_str == act_str:
        return True, "text match"

    # Fuzzy: remove common formatting differences
    def _normalize(s):
        return s.replace("-", "").replace(" ", "").replace(",", "").replace(".", "")

    if _normalize(exp_str) == _normalize(act_str):
        return True, "text match (after normalization)"

    return False, f"text mismatch: expected='{expected}', actual='{actual}'"


def _compare_extractions(expected_data, actual_data):
    """Compare expected vs actual extraction output.

    Args:
        expected_data: Dict from expected.json (golden baseline).
        actual_data: Dict from extraction JSON log.

    Returns:
        dict with keys:
            matched (int): Fields that match.
            mismatched (int): Fields that differ.
            missing (int): Fields in expected but not actual.
            extra (int): Fields in actual but not expected.
            details (list): List of comparison details.
    """
    expected_fields = {}
    actual_fields = {}

    # Flatten expected extractions into field map
    for ext in expected_data.get("extractions", []):
        fields = ext.get("fields", {})
        prefix = ext.get("_doc_type", "") or ext.get("document_type", "")
        for key, val in fields.items():
            fkey = f"{prefix}.{key}" if prefix else key
            if isinstance(val, dict):
                expected_fields[fkey] = val.get("value")
            else:
                expected_fields[fkey] = val

    # Flatten actual extractions
    for ext in actual_data.get("extractions", []):
        fields = ext.get("fields", {})
        prefix = ext.get("_doc_type", "") or ext.get("document_type", "")
        for key, val in fields.items():
            fkey = f"{prefix}.{key}" if prefix else key
            if isinstance(val, dict):
                actual_fields[fkey] = val.get("value")
            else:
                actual_fields[fkey] = val

    all_keys = set(expected_fields.keys()) | set(actual_fields.keys())
    matched = 0
    mismatched = 0
    missing = 0
    extra = 0
    details = []

    for key in sorted(all_keys):
        if key in expected_fields and key in actual_fields:
            ok, detail = _compare_values(expected_fields[key], actual_fields[key])
            if ok:
                matched += 1
                details.append({"field": key, "status": "match", "detail": detail})
            else:
                mismatched += 1
                details.append({"field": key, "status": "mismatch", "detail": detail,
                                "expected": expected_fields[key], "actual": actual_fields[key]})
        elif key in expected_fields:
            missing += 1
            details.append({"field": key, "status": "missing",
                            "detail": f"expected={expected_fields[key]}, not found in actual"})
        else:
            extra += 1
            details.append({"field": key, "status": "extra",
                            "detail": f"actual={actual_fields[key]}, not in expected"})

    return {
        "matched": matched,
        "mismatched": mismatched,
        "missing": missing,
        "extra": extra,
        "details": details,
    }


def run_single_golden(golden_dir, base_dir=None):
    """Run a single golden regression test.

    Args:
        golden_dir: Path to the golden test case directory.
        base_dir: Project root directory.

    Returns:
        dict with keys:
            golden_name (str): Name of the test case.
            passed (bool): Whether all checks passed.
            total_checks (int): Total field comparisons.
            matched, mismatched, missing, extra (int): Counts.
            duration_s (float): Time taken.
            details (list): Comparison details.
            error (str or None): Error message if extraction failed.
    """
    if base_dir is None:
        base_dir = Path(__file__).parent

    golden_dir = Path(golden_dir)
    base_dir = Path(base_dir)
    start = time.time()

    golden_name = golden_dir.name
    input_pdf = golden_dir / "input.pdf"
    expected_json = golden_dir / "expected.json"
    config_json = golden_dir / "config.json"

    # Validate golden structure
    if not input_pdf.exists():
        return {
            "golden_name": golden_name,
            "passed": False, "total_checks": 0,
            "matched": 0, "mismatched": 0, "missing": 0, "extra": 0,
            "duration_s": round(time.time() - start, 3),
            "details": [],
            "error": f"input.pdf not found in {golden_dir}",
        }

    if not expected_json.exists():
        return {
            "golden_name": golden_name,
            "passed": False, "total_checks": 0,
            "matched": 0, "mismatched": 0, "missing": 0, "extra": 0,
            "duration_s": round(time.time() - start, 3),
            "details": [],
            "error": f"expected.json not found in {golden_dir}",
        }

    # Load expected and config
    with open(expected_json) as f:
        expected_data = json.load(f)

    config = {}
    if config_json.exists():
        with open(config_json) as f:
            config = json.load(f)

    year = config.get("year", 2025)
    doc_type = config.get("doc_type", "tax_returns")
    extra_flags = config.get("flags", [])

    # Run extraction to temp output
    with tempfile.TemporaryDirectory() as tmpdir:
        output_xlsx = Path(tmpdir) / "output.xlsx"
        output_log = Path(tmpdir) / "output_log.json"

        cmd = [
            sys.executable, str(base_dir / "extract.py"),
            str(input_pdf),
            "--year", str(year),
            "--output", str(output_xlsx),
            "--doc-type", doc_type,
        ]
        cmd.extend(extra_flags)

        try:
            result = subprocess.run(
                cmd, capture_output=True, text=True,
                timeout=300, cwd=str(base_dir)
            )
        except subprocess.TimeoutExpired:
            return {
                "golden_name": golden_name,
                "passed": False, "total_checks": 0,
                "matched": 0, "mismatched": 0, "missing": 0, "extra": 0,
                "duration_s": round(time.time() - start, 3),
                "details": [],
                "error": "Extraction timed out (300s)",
            }

        if result.returncode != 0:
            return {
                "golden_name": golden_name,
                "passed": False, "total_checks": 0,
                "matched": 0, "mismatched": 0, "missing": 0, "extra": 0,
                "duration_s": round(time.time() - start, 3),
                "details": [],
                "error": f"Extraction failed (exit {result.returncode}): {result.stderr[:500]}",
            }

        # Find the JSON log
        log_file = None
        stem = input_pdf.stem
        for candidate in [output_log, Path(tmpdir) / f"{stem}_intake_log.json"]:
            if candidate.exists():
                log_file = candidate
                break

        # Also check the output dir
        if not log_file:
            for f in Path(tmpdir).glob("*_log.json"):
                log_file = f
                break

        if not log_file or not log_file.exists():
            return {
                "golden_name": golden_name,
                "passed": False, "total_checks": 0,
                "matched": 0, "mismatched": 0, "missing": 0, "extra": 0,
                "duration_s": round(time.time() - start, 3),
                "details": [],
                "error": "No JSON log produced by extraction",
            }

        with open(log_file) as f:
            actual_data = json.load(f)

    # Compare
    comparison = _compare_extractions(expected_data, actual_data)
    total_checks = comparison["matched"] + comparison["mismatched"] + comparison["missing"]
    passed = comparison["mismatched"] == 0 and comparison["missing"] == 0

    return {
        "golden_name": golden_name,
        "passed": passed,
        "total_checks": total_checks,
        "matched": comparison["matched"],
        "mismatched": comparison["mismatched"],
        "missing": comparison["missing"],
        "extra": comparison["extra"],
        "duration_s": round(time.time() - start, 3),
        "details": comparison["details"],
        "error": None,
    }


def run_all_goldens(goldens_dir=None, base_dir=None):
    """Run all golden regression tests in the goldens directory.

    Args:
        goldens_dir: Path to the goldens root directory. Defaults to data/goldens.
        base_dir: Project root directory.

    Returns:
        list of result dicts from run_single_golden.
    """
    if base_dir is None:
        base_dir = Path(__file__).parent
    base_dir = Path(base_dir)

    if goldens_dir is None:
        goldens_dir = base_dir / "data" / "goldens"
    goldens_dir = Path(goldens_dir)

    if not goldens_dir.exists():
        return []

    results = []
    for case_dir in sorted(goldens_dir.iterdir()):
        if case_dir.is_dir() and (case_dir / "expected.json").exists():
            result = run_single_golden(case_dir, base_dir)
            results.append(result)

    return results


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="CAS Golden Regression Tests")
    parser.add_argument("--goldens-dir", default="data/goldens")
    parser.add_argument("--base-dir", default=".")
    args = parser.parse_args()

    results = run_all_goldens(args.goldens_dir, args.base_dir)

    if not results:
        print("\nNo golden test cases found.")
        print(f"Create cases in {args.goldens_dir}/<case_id>/ with input.pdf + expected.json")
        sys.exit(0)

    print(f"\nCAS Golden Regression Tests: {len(results)} cases")
    for r in results:
        icon = "\u2713" if r["passed"] else "\u2717"
        status = "PASS" if r["passed"] else "FAIL"
        print(f"  {icon} {r['golden_name']}: {status} "
              f"({r['matched']} match, {r['mismatched']} mismatch, "
              f"{r['missing']} missing, {r['extra']} extra) [{r['duration_s']}s]")
        if r.get("error"):
            print(f"    Error: {r['error']}")

    all_pass = all(r["passed"] for r in results)
    sys.exit(0 if all_pass else 1)
