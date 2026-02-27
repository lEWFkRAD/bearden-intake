"""
Tests for muse_capture.py — Muse brainstorm capture endpoint.

Run:
    python tests/test_muse_capture.py
"""

import sys
import os
import json
import tempfile
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

import muse_capture

PASS = 0
FAIL = 0


def check(label, condition):
    global PASS, FAIL
    if condition:
        PASS += 1
        print(f"  PASS  {label}")
    else:
        FAIL += 1
        print(f"  FAIL  {label}")


def setup_test_db():
    """Point muse_capture at a temporary database file."""
    tmp = tempfile.mktemp(suffix=".db")
    muse_capture.DB_PATH = Path(tmp)
    muse_capture.init_muse_captures_table()
    return tmp


def cleanup_test_db(tmp_path):
    """Remove temporary database."""
    try:
        os.unlink(tmp_path)
    except OSError:
        pass


def make_valid_payload(**overrides):
    """Return a minimal valid muse_capture payload."""
    base = {
        "type": "muse_capture",
        "schema_version": 1,
        "spec_key": "test.example_spec",
        "title": "Test Capture",
        "domain": "Lite",
        "tags": ["test"],
        "content": {
            "goals": ["Goal 1"],
            "decisions": ["Decision A"],
        },
    }
    base.update(overrides)
    return base


# ─── Test 1: canonical_sha256 determinism ──────────────────────────────────

def test_canonical_sha256():
    print("\n-- Test 1: canonical_sha256 determinism --")

    payload_a = {"z": 1, "a": 2, "m": [3, 4]}
    payload_b = {"a": 2, "m": [3, 4], "z": 1}

    hash_a = muse_capture.canonical_sha256(payload_a)
    hash_b = muse_capture.canonical_sha256(payload_b)

    check("Same payload, different key order -> same hash", hash_a == hash_b)
    check("Hash is 64-char hex string", len(hash_a) == 64 and all(c in "0123456789abcdef" for c in hash_a))

    # Different payload -> different hash
    payload_c = {"a": 2, "m": [3, 5], "z": 1}
    hash_c = muse_capture.canonical_sha256(payload_c)
    check("Different payload -> different hash", hash_a != hash_c)


# ─── Test 2: validate_payload ──────────────────────────────────────────────

def test_validate_payload():
    print("\n-- Test 2: validate_payload --")

    # Valid payload
    p = make_valid_payload()
    check("Valid payload returns None", muse_capture.validate_payload(p) is None)

    # Missing type
    check('Missing type -> error', muse_capture.validate_payload({}) is not None)

    # Wrong type
    check('Wrong type -> error', muse_capture.validate_payload({"type": "wrong"}) is not None)

    # Wrong schema_version
    bad_ver = make_valid_payload(schema_version=2)
    check('schema_version=2 -> error', muse_capture.validate_payload(bad_ver) is not None)

    # Missing required fields
    for field in ("spec_key", "title", "domain", "content"):
        bad = make_valid_payload()
        del bad[field]
        check(f'Missing {field} -> error', muse_capture.validate_payload(bad) is not None)

    # Bad spec_key
    bad_key = make_valid_payload(spec_key="UPPERCASE.BAD")
    check('Invalid spec_key -> error', muse_capture.validate_payload(bad_key) is not None)

    # spec_key too short
    bad_key2 = make_valid_payload(spec_key="ab")
    check('spec_key too short -> error', muse_capture.validate_payload(bad_key2) is not None)

    # content must be dict
    bad_content = make_valid_payload(content="not a dict")
    check('content as string -> error', muse_capture.validate_payload(bad_content) is not None)

    # tags must be list if present
    bad_tags = make_valid_payload(tags="not a list")
    check('tags as string -> error', muse_capture.validate_payload(bad_tags) is not None)

    # tags can be omitted
    no_tags = make_valid_payload()
    del no_tags["tags"]
    check('tags omitted -> valid', muse_capture.validate_payload(no_tags) is None)


# ─── Test 3: insert_or_dedupe — happy path ─────────────────────────────────

def test_insert_happy():
    print("\n-- Test 3: insert_or_dedupe — happy path --")
    tmp = setup_test_db()

    payload = make_valid_payload()
    cap_id, deduped = muse_capture.insert_or_dedupe(payload)

    check("Returns a capture_id", cap_id.startswith("cap_"))
    check("Not deduped on first insert", deduped is False)

    cleanup_test_db(tmp)


# ─── Test 4: insert_or_dedupe — dedupe ─────────────────────────────────────

def test_insert_dedupe():
    print("\n-- Test 4: insert_or_dedupe — deduplication --")
    tmp = setup_test_db()

    payload = make_valid_payload()
    cap_id_1, deduped_1 = muse_capture.insert_or_dedupe(payload)
    cap_id_2, deduped_2 = muse_capture.insert_or_dedupe(payload)

    check("Same payload -> same capture_id", cap_id_1 == cap_id_2)
    check("Second insert -> deduped=True", deduped_2 is True)

    # Different payload -> different id
    payload2 = make_valid_payload(title="Different Title")
    cap_id_3, deduped_3 = muse_capture.insert_or_dedupe(payload2)
    check("Different payload -> new capture_id", cap_id_3 != cap_id_1)
    check("Different payload -> not deduped", deduped_3 is False)

    cleanup_test_db(tmp)


# ─── Test 5: Flask route — happy path ──────────────────────────────────────

def test_flask_route_happy():
    print("\n-- Test 5: Flask route — POST /muse/capture (happy path) --")
    tmp = setup_test_db()

    from flask import Flask
    test_app = Flask(__name__)
    muse_capture.register_muse_routes(test_app)
    client = test_app.test_client()

    payload = make_valid_payload()
    resp = client.post(
        "/muse/capture",
        data=json.dumps(payload),
        content_type="application/json",
    )
    data = resp.get_json()

    check("Status 200", resp.status_code == 200)
    check("ok=True", data.get("ok") is True)
    check("capture_id present", "capture_id" in data)
    check("deduped=False", data.get("deduped") is False)

    # Second POST -> deduped
    resp2 = client.post(
        "/muse/capture",
        data=json.dumps(payload),
        content_type="application/json",
    )
    data2 = resp2.get_json()
    check("Duplicate -> deduped=True", data2.get("deduped") is True)
    check("Duplicate -> same capture_id", data2.get("capture_id") == data.get("capture_id"))

    cleanup_test_db(tmp)


# ─── Test 6: Flask route — invalid payload ─────────────────────────────────

def test_flask_route_invalid():
    print("\n-- Test 6: Flask route — invalid payloads --")
    tmp = setup_test_db()

    from flask import Flask
    test_app = Flask(__name__)
    muse_capture.register_muse_routes(test_app)
    client = test_app.test_client()

    # Not JSON
    resp = client.post("/muse/capture", data="not json", content_type="text/plain")
    check("Non-JSON -> 400", resp.status_code == 400)

    # Missing required field
    bad = {"type": "muse_capture", "schema_version": 1}
    resp = client.post(
        "/muse/capture",
        data=json.dumps(bad),
        content_type="application/json",
    )
    check("Missing fields -> 400", resp.status_code == 400)
    check("Error message present", "error" in resp.get_json())

    cleanup_test_db(tmp)


# ─── Test 7: Flask route — auth required ───────────────────────────────────

def test_flask_route_auth():
    print("\n-- Test 7: Flask route — auth (X-Muse-Key) --")
    tmp = setup_test_db()

    from flask import Flask
    test_app = Flask(__name__)
    muse_capture.register_muse_routes(test_app)
    client = test_app.test_client()

    payload = make_valid_payload()
    payload_json = json.dumps(payload)

    # Set MUSE_CAPTURE_KEY
    old_key = os.environ.get("MUSE_CAPTURE_KEY")
    os.environ["MUSE_CAPTURE_KEY"] = "test-secret-key-123"

    try:
        # No header -> 401
        resp = client.post("/muse/capture", data=payload_json, content_type="application/json")
        check("No auth header -> 401", resp.status_code == 401)

        # Wrong key -> 401
        resp = client.post(
            "/muse/capture",
            data=payload_json,
            content_type="application/json",
            headers={"X-Muse-Key": "wrong-key"},
        )
        check("Wrong key -> 401", resp.status_code == 401)

        # Correct key -> 200
        resp = client.post(
            "/muse/capture",
            data=payload_json,
            content_type="application/json",
            headers={"X-Muse-Key": "test-secret-key-123"},
        )
        check("Correct key -> 200", resp.status_code == 200)
        check("ok=True with correct key", resp.get_json().get("ok") is True)
    finally:
        # Restore env
        if old_key is None:
            del os.environ["MUSE_CAPTURE_KEY"]
        else:
            os.environ["MUSE_CAPTURE_KEY"] = old_key

    cleanup_test_db(tmp)


# ─── Test 8: Flask route — size limit ──────────────────────────────────────

def test_flask_route_size_limit():
    print("\n-- Test 8: Flask route — payload size limit --")
    tmp = setup_test_db()

    from flask import Flask
    test_app = Flask(__name__)
    muse_capture.register_muse_routes(test_app)
    client = test_app.test_client()

    # Create an oversized payload
    big_payload = make_valid_payload(
        content={"data": "x" * (muse_capture.MAX_PAYLOAD_BYTES + 1000)}
    )

    resp = client.post(
        "/muse/capture",
        data=json.dumps(big_payload),
        content_type="application/json",
    )
    check("Oversized payload -> 413", resp.status_code == 413)

    cleanup_test_db(tmp)


# ─── Run All ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 52)
    print("  Muse Capture Tests")
    print("=" * 52)

    test_canonical_sha256()
    test_validate_payload()
    test_insert_happy()
    test_insert_dedupe()
    test_flask_route_happy()
    test_flask_route_invalid()
    test_flask_route_auth()
    test_flask_route_size_limit()

    print("\n" + "=" * 52)
    print(f"  Results: {PASS} passed, {FAIL} failed")
    print("=" * 52)

    sys.exit(0 if FAIL == 0 else 1)
