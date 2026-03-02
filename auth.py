"""
Minimal Auth Middleware — RALPH-REFACTOR-003
=============================================
Deterministic "office gate" with three modes:
    AUTH_MODE=off           → no auth (dev default)
    AUTH_MODE=shared-secret → requires X-Lite-Key header
    AUTH_MODE=pin           → requires X-Lite-PIN header (SHA-256 hashed compare)

Non-negotiables:
    - No route path changes
    - No request/response schema changes
    - All denials return 401 JSON with stable shape
    - Log denials safely (no secrets)

Usage:
    from auth import register_auth_gate
    register_auth_gate(app)
"""

import os
import hashlib
import hmac
from datetime import datetime, timezone
from flask import request, jsonify

AUTH_MODE = os.environ.get("AUTH_MODE", "off").lower()
LITE_SHARED_SECRET = os.environ.get("LITE_SHARED_SECRET", "")
LITE_PIN_HASH = os.environ.get("LITE_PIN_HASH", "")
LITE_PIN_PLAINTEXT = os.environ.get("LITE_PIN_PLAINTEXT", "")

# Precompute PIN hash from plaintext if LITE_PIN_HASH not provided
_resolved_pin_hash = LITE_PIN_HASH or (
    hashlib.sha256(LITE_PIN_PLAINTEXT.encode()).hexdigest()
    if LITE_PIN_PLAINTEXT else ""
)

# Paths exempt from auth (always allowed)
EXEMPT_PATHS = {
    "/api/health",
}


def _is_exempt(path, method):
    """Check if a request path is exempt from auth."""
    # GET-only health check
    if path == "/api/health" and method == "GET":
        return True
    # Static assets
    if path.startswith("/static/"):
        return True
    return False


def _deny(reason):
    """Build the standard 401 response and log the denial."""
    ts = datetime.now(timezone.utc).isoformat()
    path = request.path
    method = request.method
    print(f"[AUTH] DENIED {method} {path} reason={reason} at={ts}")
    return jsonify({
        "ok": False,
        "error": "unauthorized",
        "reason": reason,
    }), 401


def register_auth_gate(app):
    """Register a before_request hook on the Flask app that enforces auth."""

    @app.before_request
    def auth_gate():
        # Mode: off → pass everything
        if AUTH_MODE == "off":
            return None

        # Exempt paths always pass
        if _is_exempt(request.path, request.method):
            return None

        # Mode: shared-secret
        if AUTH_MODE == "shared-secret":
            key = request.headers.get("X-Lite-Key", "")
            if not key:
                return _deny("missing_key")
            if not LITE_SHARED_SECRET:
                return _deny("auth_disabled")
            if not hmac.compare_digest(key, LITE_SHARED_SECRET):
                return _deny("invalid_key")
            return None

        # Mode: pin
        if AUTH_MODE == "pin":
            pin = request.headers.get("X-Lite-PIN", "")
            if not pin:
                return _deny("missing_pin")
            if not _resolved_pin_hash:
                return _deny("auth_disabled")
            pin_hash = hashlib.sha256(pin.encode()).hexdigest()
            if not hmac.compare_digest(pin_hash, _resolved_pin_hash):
                return _deny("invalid_pin")
            return None

        # Unknown mode → treat as off (safe default)
        return None
