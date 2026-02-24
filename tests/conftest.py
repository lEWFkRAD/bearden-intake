"""Subprocess test collector for OathLedger custom-runner tests.

All 13 test_*.py files in tests/ use a custom check()/PASS/FAIL pattern
with zero assert statements. Letting pytest collect them natively would
silently mark failures as PASSED. Instead, we run each file as a subprocess
and check the exit code (each file does sys.exit(1) on failure).

Usage:
    python3 -m pytest tests/ -v          # 13 OathLedger files
    python3 -m pytest tests/ lite/tests/ # + 119 Lite tests = 132
    python3 tests/test_accounting.py     # standalone still works
"""

import subprocess
import sys
from pathlib import Path

import pytest


_TESTS_DIR = Path(__file__).parent


# ── Suppress native collection ──────────────────────────────────────────────
# Prevents pytest from importing test_*.py files in this directory.
# Without this, test_accounting.py executes at import time (side effects),
# and all other files get silently false-positive results (no asserts).
collect_ignore_glob = ["test_*.py"]


# ── Subprocess collector ────────────────────────────────────────────────────

class SubprocessTestItem(pytest.Item):
    """A single test item that runs a test file as a subprocess."""

    def __init__(self, name, parent, filepath):
        super().__init__(name, parent)
        self._filepath = filepath

    def runtest(self):
        result = subprocess.run(
            [sys.executable, str(self._filepath)],
            capture_output=True,
            text=True,
            timeout=120,
            cwd=str(self._filepath.parent.parent),  # project root
        )
        if result.returncode != 0:
            raise SubprocessTestFailure(
                returncode=result.returncode,
                stdout=result.stdout,
                stderr=result.stderr,
            )

    def repr_failure(self, excinfo, style=None):
        if isinstance(excinfo.value, SubprocessTestFailure):
            err = excinfo.value
            lines = [f"{self._filepath.name} exited with code {err.returncode}"]
            if err.stdout:
                lines.append("")
                lines.append("── stdout ──")
                lines.append(err.stdout.rstrip())
            if err.stderr:
                lines.append("")
                lines.append("── stderr ──")
                lines.append(err.stderr.rstrip())
            return "\n".join(lines)
        return super().repr_failure(excinfo, style)

    def reportinfo(self):
        return str(self._filepath), None, self._filepath.name


class SubprocessTestFailure(Exception):
    """Raised when a subprocess test file exits non-zero."""

    def __init__(self, returncode, stdout, stderr):
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr
        super().__init__(f"Exit code {returncode}")


class SubprocessTestCollector(pytest.Collector):
    """Discovers test_*.py files in tests/ and runs each as a subprocess."""

    def __init__(self, name, parent, tests_dir):
        super().__init__(name, parent)
        self._tests_dir = tests_dir

    def collect(self):
        test_files = sorted(self._tests_dir.glob("test_*.py"))
        for filepath in test_files:
            yield SubprocessTestItem.from_parent(
                self,
                name=filepath.name,
                filepath=filepath,
            )


def pytest_collect_file(parent, file_path):
    """Hook: when pytest sees conftest.py, also emit the subprocess collector.

    We attach to conftest.py itself (which pytest always processes) to
    inject our SubprocessTestCollector. The collect_ignore_glob above
    prevents pytest from natively collecting any test_*.py files.
    """
    if file_path == Path(__file__):
        return SubprocessTestCollector.from_parent(
            parent,
            name="oathledger",
            tests_dir=_TESTS_DIR,
        )
