#!/usr/bin/env python3
"""
muse_post.py — CLI tool to POST muse_capture payloads.

Usage:
    python tools/muse_post.py <json_file>          # POST a JSON file
    python tools/muse_post.py --samples             # POST the two built-in sample payloads

Options:
    --host HOST     Target host (default: http://localhost:5000)
    --key KEY       X-Muse-Key header value (optional; reads MUSE_CAPTURE_KEY env if set)
    --samples       POST the two built-in sample payloads
"""

import argparse
import json
import os
import sys
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError


SAMPLE_1 = {
    "type": "muse_capture",
    "schema_version": 1,
    "spec_key": "doctrine.update_workbook_copy",
    "title": "Copy of Workbook Spec — Updated via Doctrine",
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
    "title": "Muse Capture Integration Loop — Agent Feedback Cycle",
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
            "High capture volume could bloat SQLite — monitor DB size",
            "Hash collisions are theoretically possible but negligible with SHA-256"
        ],
        "next_actions": [
            "Wire muse_capture.py into app.py",
            "Write integration tests",
            "POST these two sample payloads to verify end-to-end"
        ]
    }
}


def post_payload(host, payload, key=None):
    """POST a payload to the muse capture endpoint. Returns (status, response_data)."""
    url = f"{host}/muse/capture"
    data = json.dumps(payload, ensure_ascii=False).encode("utf-8")

    headers = {"Content-Type": "application/json"}
    if key:
        headers["X-Muse-Key"] = key

    req = Request(url, data=data, headers=headers, method="POST")

    try:
        with urlopen(req) as resp:
            body = json.loads(resp.read().decode("utf-8"))
            return resp.status, body
    except HTTPError as e:
        body = json.loads(e.read().decode("utf-8"))
        return e.code, body
    except URLError as e:
        return None, {"error": str(e)}


def main():
    parser = argparse.ArgumentParser(description="POST muse_capture payloads")
    parser.add_argument("json_file", nargs="?", help="JSON file to POST")
    parser.add_argument("--host", default="http://localhost:5000", help="Target host")
    parser.add_argument("--key", default=os.environ.get("MUSE_CAPTURE_KEY", ""), help="X-Muse-Key header")
    parser.add_argument("--samples", action="store_true", help="POST the two built-in sample payloads")
    args = parser.parse_args()

    if not args.samples and not args.json_file:
        parser.print_help()
        sys.exit(1)

    payloads = []
    if args.samples:
        payloads = [("Sample 1: doctrine.update_workbook_copy", SAMPLE_1),
                    ("Sample 2: muse.capture_integration_loop", SAMPLE_2)]
    else:
        with open(args.json_file) as f:
            data = json.load(f)
        payloads = [(args.json_file, data)]

    key = args.key or None

    for label, payload in payloads:
        print(f"\n--- {label} ---")
        status, body = post_payload(args.host, payload, key)
        if status is None:
            print(f"  ERROR: {body.get('error')}")
        else:
            print(f"  Status: {status}")
            print(f"  Response: {json.dumps(body, indent=2)}")


if __name__ == "__main__":
    main()
