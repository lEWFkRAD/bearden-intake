#!/usr/bin/env python3
"""
Bearden Document Intake Platform v5.1
==================================
Full-featured local web app wrapping extract.py.

Features:
  - Drag-and-drop PDF upload
  - Live extraction progress with console output
  - Side-by-side review: source PDF page ↔ extracted values
  - Job history with client name search
  - Excel + JSON log download
  - Audit trail generation

Architecture (RALPH-REFACTOR-001 Phase 2):
  app.py          — Bootstrap: create Flask app, register blueprints, run server
  helpers.py      — Shared state, constants, helper functions (single source of truth)
  routes/
    health.py     — 1 route:  GET /api/health
    admin.py      — 6 routes: users, inbox, vendor categories, batch categories
    ai.py         — 1 route:  POST /api/ai-chat
    clients.py    — 12 routes: clients CRUD, context CRUD, instructions CRUD
    verification.py — 13 routes: verify, review chain, lock/unlock, evidence
    extraction.py — 15 routes: upload, status, results, download, retry, cancel

Run:
    python3 app.py

Open:
    http://localhost:5000
"""

import os
import sys

try:
    from flask import Flask
except ImportError:
    sys.exit("Install Flask: pip3 install flask")

import db as appdb
from muse_capture import init_muse_captures_table, register_muse_routes

# ─── Shared state from helpers module (single source of truth) ───────────────
from helpers import (
    BASE_DIR, DB_PATH, UPLOAD_DIR, OUTPUT_DIR, CLIENTS_DIR,
    load_jobs, _app_version,
)

# ─── App creation ────────────────────────────────────────────────────────────

app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = 150 * 1024 * 1024  # 150MB

# ─── Database initialization ─────────────────────────────────────────────────

appdb.init_db()
if appdb.needs_migration():
    try:
        from migrate import migrate_json_to_sqlite
        migrate_json_to_sqlite()
    except Exception as e:
        print(f"  Warning: Migration failed: {e}")
appdb.clear_stale_jobs()
load_jobs()

# ─── Muse Capture (brainstorm ingestion) ─────────────────────────────────────

init_muse_captures_table()
register_muse_routes(app)

# ─── Blueprint registration ──────────────────────────────────────────────────

from routes.health import health_bp
from routes.admin import admin_bp
from routes.ai import ai_bp
from routes.clients import clients_bp
from routes.verification import verification_bp
from routes.extraction import extraction_bp

app.register_blueprint(health_bp)        # 1 route
app.register_blueprint(admin_bp)         # 6 routes
app.register_blueprint(ai_bp)            # 1 route
app.register_blueprint(clients_bp)       # 12 routes
app.register_blueprint(verification_bp)  # 13 routes
app.register_blueprint(extraction_bp)    # 15 routes


# ─── Main ────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))

    if not os.environ.get("ANTHROPIC_API_KEY"):
        print("\n  ⚠  ANTHROPIC_API_KEY not set!")
        print("  Run: export ANTHROPIC_API_KEY=sk-ant-...")
        print()

    if not (BASE_DIR / "extract.py").exists():
        print("\n  ⚠  extract.py not found in", BASE_DIR)
        print("  Place extract.py in the same folder as app.py\n")

    print("=" * 52)
    print(f"  Bearden Document Intake Platform v{_app_version}")
    print("  ─────────────────────────────────────")
    print(f"  Open in browser:  http://localhost:{port}")
    print(f"  Database:         {DB_PATH}")
    print(f"  Uploads:          {UPLOAD_DIR}")
    print(f"  Outputs:          {OUTPUT_DIR}")
    print(f"  Client folders:   {CLIENTS_DIR}")
    print("=" * 52)
    print()

    app.run(host="127.0.0.1", port=port, debug=False)
