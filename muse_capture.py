"""
Muse Capture Module — Brainstorm ingestion endpoint for Lite (Flask @ :5050)

Provides POST /muse/capture to persist structured brainstorm artifacts
into SQLite with deterministic deduplication.

Auth: X-Muse-Key header checked against MUSE_CAPTURE_KEY env var.
"""

import hashlib
import json
import os
import re
import sqlite3
import uuid
from pathlib import Path

# ── Database ──

DB_PATH = Path(__file__).parent / "data" / "bearden.db"

_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS muse_captures (
    id TEXT PRIMARY KEY,
    spec_key TEXT NOT NULL,
    title TEXT NOT NULL,
    domain TEXT NOT NULL,
    tags_json TEXT NOT NULL DEFAULT '[]',
    payload_json TEXT NOT NULL,
    payload_sha256 TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_muse_captures_sha256 ON muse_captures(payload_sha256);
CREATE INDEX IF NOT EXISTS idx_muse_captures_spec_key ON muse_captures(spec_key, created_at);
"""

def init_muse_captures_table():
    """Create the muse_captures table if it doesn't exist."""
    conn = sqlite3.connect(str(DB_PATH), timeout=10)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    try:
        conn.executescript(_CREATE_TABLE_SQL)
        conn.commit()
    finally:
        conn.close()


# ── Hashing ──

def canonical_sha256(payload: dict) -> str:
    """Compute SHA-256 of canonical JSON serialization."""
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


# ── Validation ──

_SPEC_KEY_PATTERN = re.compile(r"^[a-z0-9_.\-]{3,80}$")

MAX_PAYLOAD_BYTES = 250 * 1024  # 250KB

def validate_payload(payload: dict) -> str | None:
    """
    Validate a muse_capture payload. Returns error message string or None if valid.
    """
    if not isinstance(payload, dict):
        return "Payload must be a JSON object"

    if payload.get("type") != "muse_capture":
        return 'Field "type" must be "muse_capture"'

    if payload.get("schema_version") != 1:
        return '"schema_version" must be 1'

    for field in ("spec_key", "title", "domain", "content"):
        if not payload.get(field):
            return f'Missing required field: "{field}"'

    spec_key = payload["spec_key"]
    if not isinstance(spec_key, str) or not _SPEC_KEY_PATTERN.match(spec_key):
        return f'spec_key must match ^[a-z0-9_.\\-]{{3,80}}$ — got: "{spec_key}"'

    if not isinstance(payload["title"], str) or len(payload["title"].strip()) == 0:
        return '"title" must be a non-empty string'

    if not isinstance(payload["domain"], str) or len(payload["domain"].strip()) == 0:
        return '"domain" must be a non-empty string'

    if not isinstance(payload["content"], dict):
        return '"content" must be an object'

    tags = payload.get("tags")
    if tags is not None and not isinstance(tags, list):
        return '"tags" must be an array if provided'

    return None


# ── Storage ──

def insert_or_dedupe(payload: dict) -> tuple[str, bool]:
    """
    Insert a capture or return existing if duplicate.
    Returns (capture_id, deduped).
    """
    sha = canonical_sha256(payload)

    conn = sqlite3.connect(str(DB_PATH), timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    try:
        # Check for existing
        existing = conn.execute(
            "SELECT id FROM muse_captures WHERE payload_sha256 = ?", (sha,)
        ).fetchone()
        if existing:
            return existing["id"], True

        capture_id = f"cap_{uuid.uuid4().hex}"
        tags_json = json.dumps(payload.get("tags", []))
        payload_json = json.dumps(payload, ensure_ascii=False)

        conn.execute(
            """INSERT INTO muse_captures
               (id, spec_key, title, domain, tags_json, payload_json, payload_sha256)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (capture_id, payload["spec_key"], payload["title"],
             payload["domain"], tags_json, payload_json, sha),
        )
        conn.commit()
        return capture_id, False
    finally:
        conn.close()


# ── Auth ──

def check_muse_auth(request) -> str | None:
    """
    Check auth for muse capture. Returns error message or None if authorized.
    Accepts X-Muse-Key header matched against MUSE_CAPTURE_KEY env var.
    If MUSE_CAPTURE_KEY is not set, auth is disabled (development mode).
    """
    env_key = os.environ.get("MUSE_CAPTURE_KEY", "")
    if not env_key:
        # No key configured — allow all (local dev)
        return None

    header_key = request.headers.get("X-Muse-Key", "")
    if not header_key:
        return "Missing X-Muse-Key header"

    if header_key != env_key:
        return "Invalid X-Muse-Key"

    return None


# ── Flask Route Registration ──

def register_muse_routes(app):
    """Register /muse/capture route on a Flask app."""
    from flask import request as flask_request, jsonify as flask_jsonify

    @app.route("/muse/capture", methods=["POST"])
    def muse_capture():
        # Auth
        auth_err = check_muse_auth(flask_request)
        if auth_err:
            return flask_jsonify({"ok": False, "error": auth_err}), 401

        # Size limit
        content_length = flask_request.content_length or 0
        if content_length > MAX_PAYLOAD_BYTES:
            return flask_jsonify({
                "ok": False,
                "error": f"Payload too large ({content_length} bytes). Max {MAX_PAYLOAD_BYTES}."
            }), 413

        # Parse JSON
        payload = flask_request.get_json(silent=True)
        if payload is None:
            return flask_jsonify({"ok": False, "error": "Invalid JSON"}), 400

        # Double-check size after parse (in case content_length was missing)
        raw = json.dumps(payload, ensure_ascii=False)
        if len(raw.encode("utf-8")) > MAX_PAYLOAD_BYTES:
            return flask_jsonify({
                "ok": False,
                "error": f"Payload too large. Max {MAX_PAYLOAD_BYTES} bytes."
            }), 413

        # Validate
        err = validate_payload(payload)
        if err:
            return flask_jsonify({"ok": False, "error": err}), 400

        # Store
        try:
            capture_id, deduped = insert_or_dedupe(payload)
        except Exception as e:
            return flask_jsonify({"ok": False, "error": f"Storage error: {str(e)}"}), 500

        return flask_jsonify({
            "ok": True,
            "capture_id": capture_id,
            "deduped": deduped,
        }), 200
