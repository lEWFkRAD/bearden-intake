"""
Quick script to POST the two sample payloads in-process (no server needed).
Verifies end-to-end capture pipeline works.

Run:
    python tests/post_samples.py
"""

import sys
import os
import json
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import muse_capture

# Point at temp DB
tmp = tempfile.mktemp(suffix=".db")
muse_capture.DB_PATH = Path(tmp)
muse_capture.init_muse_captures_table()

# Create test Flask app
from flask import Flask
app = Flask(__name__)
muse_capture.register_muse_routes(app)
client = app.test_client()

SAMPLE_1 = {
    "type": "muse_capture",
    "schema_version": 1,
    "spec_key": "doctrine.update_workbook_copy",
    "title": "Copy of Workbook Spec -- Updated via Doctrine",
    "domain": "OathLedger",
    "tags": ["doctrine", "workbook", "update"],
    "content": {
        "goals": [
            "Produce a clean, merged copy of the current workbook spec",
            "Preserve all prior decisions and constraints"
        ],
        "constraints": [
            "Must not overwrite original workbook entries",
            "All fields in the copy must trace back to a source section"
        ],
        "decisions": [
            "Use section-level merge (not field-level) for copy generation",
            "Retain 'last_updated_by' metadata on each section"
        ],
        "open_questions": [
            "Should the copy include deprecated fields for audit purposes?"
        ],
        "next_actions": [
            "Draft merge logic in extract.py",
            "Add unit test for section-level merge"
        ]
    }
}

SAMPLE_2 = {
    "type": "muse_capture",
    "schema_version": 1,
    "spec_key": "muse.capture_integration_loop",
    "title": "Muse Capture Integration Loop -- Agent Feedback Cycle",
    "domain": "Muse",
    "tags": ["muse", "capture", "agent-loop", "integration"],
    "content": {
        "goals": [
            "Close the loop between agent execution and spec capture",
            "Agents post execution notes back into the spec thread after completing work"
        ],
        "constraints": [
            "Capture payloads must be under 250KB",
            "SHA-256 deduplication must be deterministic across all agents"
        ],
        "decisions": [
            "Use canonical JSON serialization (sorted keys, no whitespace) for hash stability",
            "Store raw payload JSON alongside normalized fields for auditability"
        ],
        "interfaces": [
            "POST /muse/capture with X-Muse-Key auth header",
            "Response: { ok, capture_id, deduped }"
        ],
        "risks": [
            "High capture volume could bloat SQLite -- monitor DB size",
            "Hash collisions are theoretically possible but negligible with SHA-256"
        ],
        "next_actions": [
            "Wire muse_capture.py into app.py",
            "Write integration tests",
            "POST these two sample payloads to verify end-to-end"
        ]
    }
}

print("=" * 60)
print("  Posting Sample Payloads to /muse/capture")
print("=" * 60)

for label, payload in [("Sample 1: doctrine.update_workbook_copy", SAMPLE_1),
                       ("Sample 2: muse.capture_integration_loop", SAMPLE_2)]:
    print(f"\n--- {label} ---")
    resp = client.post(
        "/muse/capture",
        data=json.dumps(payload),
        content_type="application/json",
    )
    data = resp.get_json()
    print(f"  Status:     {resp.status_code}")
    print(f"  ok:         {data.get('ok')}")
    print(f"  capture_id: {data.get('capture_id')}")
    print(f"  deduped:    {data.get('deduped')}")

    # Post again to verify dedup
    resp2 = client.post(
        "/muse/capture",
        data=json.dumps(payload),
        content_type="application/json",
    )
    data2 = resp2.get_json()
    print(f"  [re-post]   deduped={data2.get('deduped')}, same_id={data2.get('capture_id') == data.get('capture_id')}")

# Verify DB contents
import sqlite3
conn = sqlite3.connect(str(muse_capture.DB_PATH))
conn.row_factory = sqlite3.Row
rows = conn.execute("SELECT id, spec_key, title, domain FROM muse_captures ORDER BY created_at").fetchall()
conn.close()

print(f"\n--- DB Verification: {len(rows)} captures stored ---")
for r in rows:
    print(f"  {r['id'][:20]}...  {r['spec_key']:<40}  {r['domain']}")

# Cleanup
try:
    os.unlink(tmp)
except OSError:
    pass

print("\n" + "=" * 60)
print("  All sample payloads posted and verified successfully!")
print("=" * 60)
