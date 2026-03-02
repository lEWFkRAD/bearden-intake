"""
Health check blueprint.
Extracted from app.py during RALPH-REFACTOR-001 Phase 2.
1 route: GET /api/health
"""

import os
import shutil
from datetime import datetime
from flask import Blueprint, jsonify

from helpers import (
    jobs, _start_time, _app_version,
    BASE_DIR, DATA_DIR, UPLOAD_DIR, OUTPUT_DIR, CLIENTS_DIR, VERIFY_DIR,
)

health_bp = Blueprint("health", __name__)


@health_bp.route("/api/health")
def health_check():
    """System health check: version, uptime, job counts, dependency status, disk usage."""

    now = datetime.now()
    uptime_seconds = (now - _start_time).total_seconds()

    # Job counts by status
    status_counts = {}
    for j in jobs.values():
        s = j.get("status", "unknown")
        status_counts[s] = status_counts.get(s, 0) + 1

    # Dependency checks
    tesseract_ok = shutil.which("tesseract") is not None
    extract_ok = (BASE_DIR / "extract.py").exists()
    api_key_set = bool(os.environ.get("ANTHROPIC_API_KEY"))

    # Directory writability
    dirs_ok = {}
    for name, d in [("uploads", UPLOAD_DIR), ("outputs", OUTPUT_DIR), ("clients", CLIENTS_DIR), ("verifications", VERIFY_DIR)]:
        dirs_ok[name] = os.access(str(d), os.W_OK)

    # Disk usage
    try:
        usage = shutil.disk_usage(str(DATA_DIR))
        disk = {
            "total_gb": round(usage.total / (1024**3), 2),
            "free_gb": round(usage.free / (1024**3), 2),
            "percent_used": round(usage.used / usage.total * 100, 1),
        }
    except Exception:
        disk = None

    # Data directory size
    data_size_mb = 0
    try:
        for dirpath, dirnames, filenames in os.walk(str(DATA_DIR)):
            for fname in filenames:
                try:
                    data_size_mb += os.path.getsize(os.path.join(dirpath, fname))
                except OSError:
                    pass
        data_size_mb = round(data_size_mb / (1024 * 1024), 2)
    except Exception:
        pass

    return jsonify({
        "status": "ok",
        "version": _app_version,
        "uptime_hours": round(uptime_seconds / 3600, 2),
        "started": _start_time.isoformat(),
        "jobs": {"total": len(jobs), "by_status": status_counts},
        "dependencies": {"extract_py": extract_ok, "tesseract": tesseract_ok, "api_key_set": api_key_set},
        "directories": dirs_ok,
        "disk": disk,
        "data_size_mb": data_size_mb,
    })
