"""CAS Smoke Tests — environment and infrastructure checks.

Validates that the Bearden platform's runtime environment is healthy:
database writable, required tools present, disk space adequate, etc.

This module never modifies financial data. It only reads operational state.

Run standalone:
    python3 assurance_smoke.py [--db-path data/bearden.db] [--base-dir .]
"""

import os
import sys
import time
import shutil
import sqlite3
from pathlib import Path


def run_smoke_tests(db_path, base_dir=None):
    """Run all smoke tests and return structured results.

    Args:
        db_path: Path to the SQLite database.
        base_dir: Project root directory. Defaults to parent of db_path's parent.

    Returns:
        dict with keys:
            passed (int): Number of checks that passed.
            total (int): Total number of checks.
            results (list): List of check dicts with {name, passed, message}.
            duration_s (float): Time taken.
    """
    if base_dir is None:
        base_dir = Path(db_path).parent.parent

    base_dir = Path(base_dir)
    db_path = Path(db_path)
    start = time.time()
    results = []

    def _check(name, fn):
        """Run a single check, catching any exceptions."""
        try:
            passed, message = fn()
            results.append({"name": name, "passed": passed, "message": message})
        except Exception as e:
            results.append({"name": name, "passed": False, "message": f"Exception: {e}"})

    # ─── Check 1: Database writable ──────────────────────────────────────

    def check_db_writable():
        if not db_path.exists():
            return False, f"Database not found: {db_path}"
        try:
            conn = sqlite3.connect(str(db_path), timeout=5)
            conn.execute("PRAGMA journal_mode=WAL")
            # Test write with a temp table
            conn.execute("CREATE TABLE IF NOT EXISTS _smoke_test (id INTEGER)")
            conn.execute("DROP TABLE IF EXISTS _smoke_test")
            conn.close()
            return True, "Database writable"
        except sqlite3.Error as e:
            return False, f"Database not writable: {e}"

    _check("db_writable", check_db_writable)

    # ─── Check 2: All op_* tables exist ──────────────────────────────────

    def check_op_tables():
        required = {"op_runs", "op_phases", "op_drift",
                     "op_smoke_results", "op_golden_results", "op_backups",
                     "op_change_requests", "op_cr_findings", "op_post_fix_gates"}
        try:
            conn = sqlite3.connect(str(db_path), timeout=5)
            tables = {row[0] for row in
                      conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
            conn.close()
            missing = required - tables
            if missing:
                return False, f"Missing tables: {', '.join(sorted(missing))}"
            return True, f"All {len(required)} op_* tables present"
        except sqlite3.Error as e:
            return False, f"Cannot check tables: {e}"

    _check("op_tables_exist", check_op_tables)

    # ─── Check 3: Tesseract available ────────────────────────────────────

    def check_tesseract():
        path = shutil.which("tesseract")
        if path:
            return True, f"Tesseract found: {path}"
        return False, "Tesseract not found in PATH"

    _check("tesseract_available", check_tesseract)

    # ─── Check 4: extract.py exists ──────────────────────────────────────

    def check_extract_exists():
        extract_path = base_dir / "extract.py"
        if extract_path.exists():
            return True, f"extract.py found ({extract_path})"
        return False, f"extract.py not found at {extract_path}"

    _check("extract_exists", check_extract_exists)

    # ─── Check 5: ANTHROPIC_API_KEY set ──────────────────────────────────

    def check_api_key():
        key = os.environ.get("ANTHROPIC_API_KEY", "")
        if key:
            masked = key[:8] + "..." + key[-4:] if len(key) > 12 else "***"
            return True, f"API key set ({masked})"
        return False, "ANTHROPIC_API_KEY not set"

    _check("api_key_set", check_api_key)

    # ─── Check 6: Disk space > 500MB free ────────────────────────────────

    def check_disk_space():
        try:
            usage = shutil.disk_usage(str(base_dir))
            free_mb = usage.free / (1024 * 1024)
            free_gb = round(free_mb / 1024, 1)
            if free_mb >= 500:
                return True, f"{free_gb} GB free"
            return False, f"Only {free_gb} GB free (need >= 0.5 GB)"
        except Exception as e:
            return False, f"Cannot check disk: {e}"

    _check("disk_space_adequate", check_disk_space)

    # ─── Check 7: Data directories writable ──────────────────────────────

    def check_data_dirs():
        required_dirs = [
            base_dir / "data",
            base_dir / "data" / "uploads",
            base_dir / "data" / "outputs",
            base_dir / "data" / "page_images",
        ]
        missing = []
        not_writable = []
        for d in required_dirs:
            if not d.exists():
                missing.append(str(d.relative_to(base_dir)))
            elif not os.access(str(d), os.W_OK):
                not_writable.append(str(d.relative_to(base_dir)))

        if missing:
            return False, f"Missing dirs: {', '.join(missing)}"
        if not_writable:
            return False, f"Not writable: {', '.join(not_writable)}"
        return True, f"All {len(required_dirs)} data dirs writable"

    _check("data_dirs_writable", check_data_dirs)

    # ─── Check 8: Backups directory exists ────────────────────────────────

    def check_backups_dir():
        backups_dir = base_dir / "data" / "backups"
        if backups_dir.exists() and os.access(str(backups_dir), os.W_OK):
            return True, f"Backups dir exists: {backups_dir}"
        if not backups_dir.exists():
            try:
                backups_dir.mkdir(parents=True, exist_ok=True)
                return True, f"Backups dir created: {backups_dir}"
            except Exception as e:
                return False, f"Cannot create backups dir: {e}"
        return False, f"Backups dir not writable: {backups_dir}"

    _check("backups_dir_exists", check_backups_dir)

    # ─── Summary ─────────────────────────────────────────────────────────

    duration_s = round(time.time() - start, 3)
    passed = sum(1 for r in results if r["passed"])

    return {
        "passed": passed,
        "total": len(results),
        "results": results,
        "duration_s": duration_s,
    }


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="CAS Smoke Tests")
    parser.add_argument("--db-path", default="data/bearden.db")
    parser.add_argument("--base-dir", default=".")
    args = parser.parse_args()

    result = run_smoke_tests(args.db_path, args.base_dir)

    print(f"\nCAS Smoke Tests: {result['passed']}/{result['total']} passed ({result['duration_s']}s)")
    for r in result["results"]:
        icon = "\u2713" if r["passed"] else "\u2717"
        print(f"  {icon} {r['name']}: {r['message']}")

    sys.exit(0 if result["passed"] == result["total"] else 1)
