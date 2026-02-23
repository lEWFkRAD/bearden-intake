# ============================================================
# SIGZIL — Continuous Assurance System
# ============================================================

"""CAS Report Generator — structured reports and Agent Packs.

Generates markdown + JSON reports from CAS telemetry data.
Also produces downloadable Agent Packs (zip files) for partner
and coding agent consumption.

Report Types:
  R1 — Daily Health: overall system status, smoke, backup, drift
  R2 — Runs: recent extraction run performance
  R3 — Regressions: golden test results and failures
  R4 — Backups: backup history and verification status

Agent Pack: zip containing all 4 reports + README with next actions.

This module reads ONLY from telemetry_store.py (op_* tables).
It must NEVER import or read from fact_store, extract, or workpaper modules.

Run:  python3 -c "from cas_reports import CASReportGenerator; print('OK')"
"""

import json
import io
import zipfile
from datetime import datetime

# ─── IMPORT GUARDRAIL ─────────────────────────────────────────────────────────
# This module must never import financial data modules.
_FORBIDDEN_MODULES = frozenset({
    'fact_store', 'extract', 'pytesseract', 'anthropic', 'pdf2image',
    'PIL', 'Pillow', 'fitz', 'workpaper_export',
})


class CASReportGenerator:
    """Generate CAS reports from telemetry data.

    Usage::

        from telemetry_store import TelemetryStore
        ts = TelemetryStore("/path/to/bearden.db")
        gen = CASReportGenerator(ts)

        # Get a single report
        r1 = gen.render_daily_health()
        print(r1["markdown"])

        # Get Agent Pack as zip bytes
        zip_bytes = gen.build_agent_pack()
    """

    def __init__(self, telemetry_store, app_version="", environment="localhost:5050"):
        self.ts = telemetry_store
        self.app_version = app_version
        self.environment = environment

    def _header(self, report_type, report_title):
        """Standard report header."""
        now = datetime.now()
        return f"""# {report_title}

| Field | Value |
|-------|-------|
| Report Type | {report_type} |
| Generated | {now.strftime('%Y-%m-%d %H:%M:%S')} |
| Environment | {self.environment} |
| App Version | {self.app_version} |
| Extract Version | v6 |
"""

    def _executive_summary(self, status, key_notes, interpretation=""):
        """Standard executive summary section."""
        lines = [f"\n## Executive Summary\n", f"**Status:** {status}\n"]
        if key_notes:
            lines.append("**Key Notes:**\n")
            for note in key_notes:
                lines.append(f"- {note}\n")
        if interpretation:
            lines.append(f"\n{interpretation}\n")
        return "".join(lines)

    # ─── R1: Daily Health ─────────────────────────────────────────────────

    def render_daily_health(self, date_str=None):
        """Render the Daily Health report (R1).

        Returns:
            dict with keys: markdown (str), json (dict), report_type (str).
        """
        summary = self.ts.daily_summary(date_str)
        health = self.ts.cas_health_summary()
        smoke = self.ts.get_latest_smoke()
        backups = self.ts.get_recent_backups(limit=1)
        drift = self.ts.get_drift_summary(limit=5)

        md = self._header("R1 — Daily Health", "CAS Daily Health Report")

        # Executive summary
        notes = []
        runs = summary.get("runs", {})
        total_runs = runs.get("total", 0) or 0
        errors = runs.get("errors", 0) or 0
        if total_runs > 0:
            notes.append(f"{total_runs} extraction runs today ({errors} errors)")
        if smoke:
            notes.append(f"Smoke: {smoke.get('passed', 0)}/{smoke.get('total_checks', 0)} checks pass")
        if backups:
            notes.append(f"Last backup: {(backups[0].get('created_at', '') or '')[:16]}")
        if not notes:
            notes.append("No activity recorded today")

        md += self._executive_summary(health.get("label", "Unknown"), notes)

        # System Health Checks
        md += "\n## System Health Checks\n\n"
        if smoke and smoke.get("results"):
            md += "| Check | Status | Message |\n|-------|--------|--------|\n"
            for r in smoke.get("results", []):
                icon = "PASS" if r.get("passed") else "**FAIL**"
                md += f"| {r.get('name', '')} | {icon} | {r.get('message', '')} |\n"
        else:
            md += "*No smoke test data available. Run smoke tests from the admin panel.*\n"

        # Performance Snapshot
        md += "\n## Performance Snapshot\n\n"
        if total_runs > 0:
            avg_rt = runs.get("avg_runtime") or 0
            avg_cost = runs.get("avg_cost") or 0
            total_cost = runs.get("total_cost") or 0
            avg_ttfv = runs.get("avg_ttfv") or 0
            md += "| Metric | Value |\n|--------|-------|\n"
            md += f"| Runs Today | {total_runs} |\n"
            md += f"| Errors | {errors} |\n"
            md += f"| Avg Runtime | {avg_rt:.1f}s |\n"
            md += f"| Avg Cost | ${avg_cost:.4f} |\n"
            md += f"| Total Cost | ${total_cost:.4f} |\n"
            md += f"| Avg Time to First Values | {avg_ttfv:.1f}s |\n"
        else:
            md += "*No runs recorded today.*\n"

        # Drift Metrics
        md += "\n## Drift Metrics\n\n"
        if drift:
            md += "| Job | Edit Rate | Needs Review | Audit Pass | Measured |\n"
            md += "|-----|-----------|-------------|------------|----------|\n"
            for d in drift[:5]:
                er = f"{d['edit_rate']*100:.1f}%" if d.get('edit_rate') is not None else "—"
                nr = f"{d['needs_review_rate']*100:.1f}%" if d.get('needs_review_rate') is not None else "—"
                ap = f"{d['audit_pass_rate']*100:.1f}%" if d.get('audit_pass_rate') is not None else "—"
                md += f"| {(d.get('job_id',''))[:12]} | {er} | {nr} | {ap} | {(d.get('measured_at',''))[:16]} |\n"
        else:
            md += "*No drift data available. Drift is computed when jobs reach final review.*\n"

        # Backup & Recovery
        md += "\n## Backup & Recovery\n\n"
        if backups:
            b = backups[0]
            md += f"- **Last Backup:** {(b.get('created_at', '') or '')[:16]}\n"
            md += f"- **Size:** {(b.get('db_size_bytes', 0) or 0):,} bytes\n"
            md += f"- **Verified:** {'Yes' if b.get('verified') else 'No'}\n"
            md += f"- **SHA-256:** {(b.get('sha256', '') or '')[:16]}...\n"
        else:
            md += "*No backups recorded. Create one from the admin panel.*\n"

        # Actions
        md += "\n## Actions & Recommendations\n\n"
        if health.get("state") == "good":
            md += "- System healthy. No action required.\n"
        else:
            if smoke and smoke.get("passed", 0) < smoke.get("total_checks", 0):
                md += "- **Fix failing smoke checks** (see Health Checks above)\n"
            if not backups:
                md += "- **Create a database backup** (no backups recorded)\n"
            if errors > 0:
                md += f"- **Investigate {errors} extraction errors** today\n"

        json_data = {
            "report_type": "R1",
            "generated": datetime.now().isoformat(),
            "health": health,
            "summary": summary,
            "smoke": smoke,
            "drift": drift,
        }

        return {"markdown": md, "json": json_data, "report_type": "R1"}

    # ─── R2: Runs ─────────────────────────────────────────────────────────

    def render_runs(self, limit=50):
        """Render the Runs report (R2).

        Returns:
            dict with keys: markdown (str), json (dict), report_type (str).
        """
        runs = self.ts.get_recent_runs(limit=limit)

        md = self._header("R2 — Runs", "CAS Extraction Runs Report")

        notes = [f"{len(runs)} recent runs retrieved"]
        complete = sum(1 for r in runs if r.get("status") == "complete")
        errors = sum(1 for r in runs if r.get("status") == "error")
        notes.append(f"{complete} complete, {errors} errors")
        md += self._executive_summary("Operational", notes)

        md += "\n## Recent Runs\n\n"
        if runs:
            md += "| Job ID | Client | Status | Runtime | Cost | Pages | Fields | Started |\n"
            md += "|--------|--------|--------|---------|------|-------|--------|---------|\n"
            for r in runs:
                jid = (r.get("job_id", "") or "")[:12]
                client = r.get("client_name", "") or "-"
                status = r.get("status", "")
                runtime = f"{r.get('total_s', 0) or 0:.1f}s"
                cost = f"${r.get('cost_usd', 0) or 0:.3f}"
                pages = r.get("total_pages", "-") or "-"
                fields = r.get("total_fields", "-") or "-"
                started = (r.get("started_at", "") or "")[:16]
                md += f"| {jid} | {client} | {status} | {runtime} | {cost} | {pages} | {fields} | {started} |\n"

            # Aggregates
            runtimes = [r["total_s"] for r in runs if r.get("total_s")]
            costs = [r["cost_usd"] for r in runs if r.get("cost_usd")]
            md += "\n### Aggregates\n\n"
            md += "| Metric | Value |\n|--------|-------|\n"
            if runtimes:
                md += f"| Avg Runtime | {sum(runtimes)/len(runtimes):.1f}s |\n"
                md += f"| Min Runtime | {min(runtimes):.1f}s |\n"
                md += f"| Max Runtime | {max(runtimes):.1f}s |\n"
            if costs:
                md += f"| Avg Cost | ${sum(costs)/len(costs):.4f} |\n"
                md += f"| Total Cost | ${sum(costs):.4f} |\n"
        else:
            md += "*No runs recorded.*\n"

        json_data = {
            "report_type": "R2",
            "generated": datetime.now().isoformat(),
            "runs": runs,
            "count": len(runs),
        }

        return {"markdown": md, "json": json_data, "report_type": "R2"}

    # ─── R3: Regressions ──────────────────────────────────────────────────

    def render_regressions(self, days=7):
        """Render the Regressions report (R3).

        Returns:
            dict with keys: markdown (str), json (dict), report_type (str).
        """
        goldens = self.ts.get_latest_golden_results(limit=50)

        md = self._header("R3 — Regressions", "CAS Regression Test Report")

        all_pass = all(g.get("passed") == g.get("total_checks") for g in goldens) if goldens else None
        status = "All Passing" if all_pass else ("Failures Detected" if goldens else "No Data")
        notes = [f"{len(goldens)} golden results available"]
        md += self._executive_summary(status, notes)

        md += "\n## Golden Regression Results\n\n"
        if goldens:
            md += "| Case | Result | Matched | Mismatched | Missing | Extra | Duration | Run At |\n"
            md += "|------|--------|---------|------------|---------|-------|----------|--------|\n"
            for g in goldens:
                name = g.get("golden_name", "")
                passed = "PASS" if g.get("passed") == g.get("total_checks") else "**FAIL**"
                md += f"| {name} | {passed} | {g.get('fields_matched', 0)} | {g.get('fields_mismatched', 0)} | {g.get('fields_missing', 0)} | {g.get('fields_extra', 0)} | {g.get('duration_s', 0):.1f}s | {(g.get('run_at', '') or '')[:16]} |\n"

            # Failures detail
            failures = [g for g in goldens if g.get("passed") != g.get("total_checks")]
            if failures:
                md += "\n### Failure Details\n\n"
                for f in failures:
                    md += f"#### {f.get('golden_name', '')}\n\n"
                    details = f.get("details", {})
                    if isinstance(details, list):
                        for d in details[:10]:
                            if d.get("status") in ("mismatch", "missing"):
                                md += f"- **{d.get('field', '')}**: {d.get('detail', '')}\n"
                    elif isinstance(details, dict):
                        md += f"```json\n{json.dumps(details, indent=2)[:500]}\n```\n"
        else:
            md += "*No golden regression data. Run goldens from the admin panel.*\n"

        json_data = {
            "report_type": "R3",
            "generated": datetime.now().isoformat(),
            "goldens": goldens,
        }

        return {"markdown": md, "json": json_data, "report_type": "R3"}

    # ─── R4: Backups ──────────────────────────────────────────────────────

    def render_backups(self, days=30):
        """Render the Backups report (R4).

        Returns:
            dict with keys: markdown (str), json (dict), report_type (str).
        """
        backups = self.ts.get_recent_backups(limit=30)

        md = self._header("R4 — Backups", "CAS Backup Status Report")

        verified_count = sum(1 for b in backups if b.get("verified"))
        notes = [
            f"{len(backups)} backups in history",
            f"{verified_count} verified",
        ]
        if backups:
            notes.append(f"Latest: {(backups[0].get('created_at', '') or '')[:16]}")
        md += self._executive_summary("Operational" if backups else "No Backups", notes)

        md += "\n## Backup History\n\n"
        if backups:
            md += "| Created | Size | SHA-256 | Verified | Path |\n"
            md += "|---------|------|---------|----------|------|\n"
            for b in backups:
                created = (b.get("created_at", "") or "")[:16]
                size = f"{(b.get('db_size_bytes', 0) or 0):,}"
                sha = (b.get("sha256", "") or "")[:12] + "..."
                verified = "Yes" if b.get("verified") else "No"
                path = b.get("backup_path", "")
                md += f"| {created} | {size} B | {sha} | {verified} | {path} |\n"

            # Row counts from latest
            if backups[0].get("row_counts"):
                md += "\n### Latest Backup Row Counts\n\n"
                md += "| Table | Rows |\n|-------|------|\n"
                for table, count in sorted(backups[0]["row_counts"].items()):
                    md += f"| {table} | {count} |\n"
        else:
            md += "*No backups recorded. Create one from the admin panel.*\n"

        md += "\n## Actions & Recommendations\n\n"
        if not backups:
            md += "- **Create an initial backup immediately**\n"
        elif not any(b.get("verified") for b in backups[:3]):
            md += "- **Verify at least one recent backup**\n"

        json_data = {
            "report_type": "R4",
            "generated": datetime.now().isoformat(),
            "backups": backups,
        }

        return {"markdown": md, "json": json_data, "report_type": "R4"}

    # ─── Agent Pack ───────────────────────────────────────────────────────

    def build_agent_pack(self, reports=None):
        """Build an Agent Pack zip file containing all CAS reports.

        Args:
            reports: Optional list of report types to include.
                     Defaults to all: ["daily", "runs", "regressions", "backups"].

        Returns:
            bytes: In-memory zip file content.
        """
        if reports is None:
            reports = ["daily", "runs", "regressions", "backups"]

        render_map = {
            "daily": ("R1_daily_health", self.render_daily_health),
            "runs": ("R2_runs", self.render_runs),
            "regressions": ("R3_regressions", self.render_regressions),
            "backups": ("R4_backups", self.render_backups),
        }

        generated_reports = {}
        for report_key in reports:
            if report_key in render_map:
                name, fn = render_map[report_key]
                try:
                    result = fn()
                    generated_reports[name] = result
                except Exception as e:
                    generated_reports[name] = {
                        "markdown": f"# Error generating {name}\n\n{e}\n",
                        "json": {"error": str(e)},
                        "report_type": name,
                    }

        # Build README
        readme = self._build_agent_pack_readme(generated_reports)

        # Build zip
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
            zf.writestr("README.md", readme)
            for name, report in generated_reports.items():
                zf.writestr(f"{name}.md", report["markdown"])
                zf.writestr(f"{name}.json", json.dumps(report["json"], indent=2, default=str))

        return buf.getvalue()

    def _build_agent_pack_readme(self, reports):
        """Build the Agent Pack README.md."""
        now = datetime.now()
        md = f"""# CAS Agent Pack

**Generated:** {now.strftime('%Y-%m-%d %H:%M:%S')}
**Environment:** {self.environment}
**App Version:** {self.app_version}

## What Changed

This Agent Pack contains the latest CAS telemetry reports.
Review the included reports for system health, performance,
regression status, and backup integrity.

## Current Issues

"""
        # Scan reports for issues
        issues = []
        for name, report in reports.items():
            rj = report.get("json", {})
            health = rj.get("health", {})
            if health.get("state") in ("warn", "bad"):
                issues.append(f"- System health: **{health.get('label', 'Degraded')}** (see {name}.md)")
            goldens = rj.get("goldens", [])
            if goldens and any(g.get("passed") != g.get("total_checks") for g in goldens if isinstance(g, dict)):
                issues.append(f"- Golden regression failures detected (see {name}.md)")

        if issues:
            md += "\n".join(issues) + "\n"
        else:
            md += "No critical issues detected.\n"

        md += """
## Suggested Next Tasks

1. Review the Daily Health report (R1) for overall system status
2. Check Run performance trends in R2
3. Investigate any regression failures in R3
4. Verify backup integrity in R4

## Files Included

| File | Description |
|------|-------------|
| README.md | This file |
"""
        for name in sorted(reports.keys()):
            md += f"| {name}.md | Report (markdown) |\n"
            md += f"| {name}.json | Report (structured data) |\n"

        md += f"""
---
*Generated by Bearden CAS v{self.app_version}*
"""
        return md

    # ─── CR Reports (T-CAS-2B) ────────────────────────────────────────────

    def render_cr_findings(self, cr_id):
        """Render a CR's findings as markdown + JSON.

        Args:
            cr_id: The Change Request ID.

        Returns:
            dict with keys: markdown (str), json (dict).
            Returns error dict if CR not found.
        """
        cr = self.ts.get_change_request(cr_id)
        if not cr:
            return {
                "markdown": f"# Error\n\nChange Request {cr_id} not found.\n",
                "json": {"error": f"CR {cr_id} not found"},
            }

        md = self._header(f"CR Findings — {cr_id}", f"Change Request {cr_id}")

        # CR Summary
        md += f"\n## Change Request Summary\n\n"
        md += f"| Field | Value |\n|-------|-------|\n"
        md += f"| CR ID | {cr_id} |\n"
        md += f"| Status | {cr.get('status', '')} |\n"
        md += f"| Severity | {cr.get('severity', '')} |\n"
        md += f"| Source | {cr.get('source', '')} |\n"
        md += f"| Created | {(cr.get('created_at', '') or '')[:19]} |\n"
        md += f"| Trigger | {cr.get('trigger_summary', '')} |\n"

        # Findings
        findings = cr.get("findings", [])
        md += f"\n## Findings ({len(findings)} total)\n\n"
        if findings:
            md += "| # | Severity | Source | Check | Details | Measured | Threshold | Action |\n"
            md += "|---|----------|--------|-------|---------|----------|-----------|--------|\n"
            for f in findings:
                md += (f"| {f.get('finding_id', '')} "
                       f"| {f.get('severity', '')} "
                       f"| {f.get('source', '')} "
                       f"| {f.get('check_name', '')} "
                       f"| {f.get('details', '')} "
                       f"| {f.get('measured_value', '')} "
                       f"| {f.get('threshold', '')} "
                       f"| {f.get('recommended_action', '')} |\n")
        else:
            md += "*No findings recorded.*\n"

        # Gate result if present
        gate = cr.get("gate")
        if gate:
            md += f"\n## Post-Fix Gate Result\n\n"
            md += f"| Field | Value |\n|-------|-------|\n"
            md += f"| Result | {gate.get('gate_result', '')} |\n"
            md += f"| Checks Run | {gate.get('checks_run', 0)} |\n"
            md += f"| Checks Passed | {gate.get('checks_passed', 0)} |\n"
            md += f"| Run At | {(gate.get('run_at', '') or '')[:19]} |\n"

        # Recommended actions
        md += "\n## Recommended Actions\n\n"
        for f in findings:
            action = f.get("recommended_action", "")
            if action:
                md += f"- **{f.get('check_name', '')}**: {action}\n"

        json_data = {
            "cr_id": cr_id,
            "status": cr.get("status"),
            "severity": cr.get("severity"),
            "source": cr.get("source"),
            "trigger_summary": cr.get("trigger_summary"),
            "created_at": cr.get("created_at"),
            "findings": findings,
            "gate": gate,
        }

        return {"markdown": md, "json": json_data}

    def build_cr_agent_pack(self, cr_id):
        """Build a CR-specific Agent Pack zip.

        Includes: findings.md, findings.json, fix_manifest_template.json, README.

        Args:
            cr_id: The Change Request ID.

        Returns:
            bytes: In-memory zip content.
        """
        cr = self.ts.get_change_request(cr_id)
        if not cr:
            # Return a minimal zip with error
            buf = io.BytesIO()
            with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
                zf.writestr("README.md", f"# Error\n\nChange Request {cr_id} not found.\n")
            return buf.getvalue()

        # Render findings
        findings_report = self.render_cr_findings(cr_id)

        # Build README
        now = datetime.now()
        readme = f"""# CAS Change Request Agent Pack — {cr_id}

**Generated:** {now.strftime('%Y-%m-%d %H:%M:%S')}
**Environment:** {self.environment}
**CR Status:** {cr.get('status', '')}
**Severity:** {cr.get('severity', '')}
**Source:** {cr.get('source', '')}

## Trigger

{cr.get('trigger_summary', 'No summary')}

## What Happened

This Change Request was auto-generated by CAS when an assurance check failed.
Review findings.md for details on what triggered this CR.

## What To Do

1. Read `findings.md` to understand the failures
2. Investigate root cause
3. Make code/config changes to fix
4. Fill out `fix_manifest_template.json` and submit via admin panel or API
5. Run the Post-Fix Gate to verify the fix resolves all triggers
6. Close the CR once the gate passes

## Files Included

| File | Description |
|------|-------------|
| README.md | This file |
| findings.md | Detailed findings report |
| findings.json | Structured findings data |
| fix_manifest_template.json | Template for fix manifest submission |

---
*Generated by Bearden CAS v{self.app_version}*
"""

        # Build zip
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
            zf.writestr("README.md", readme)
            zf.writestr("findings.md", findings_report["markdown"])
            zf.writestr("findings.json", json.dumps(findings_report["json"], indent=2, default=str))
            zf.writestr("fix_manifest_template.json", json.dumps(
                self.fix_manifest_template(), indent=2))

        return buf.getvalue()

    @staticmethod
    def fix_manifest_template():
        """Return an empty fix_manifest.json template.

        Returns:
            dict with template structure for fix manifest submission.
        """
        return {
            "files_changed": [],
            "tests_added": [],
            "config_changed": [],
            "description": "",
            "author": "",
            "timestamp": "",
        }
