#!/usr/bin/env python3
"""Tests for Sprint 2 — Auth, Users, Events, Admin Dashboard.

Covers: user DB functions, PIN hashing, auth decorators, event logging,
        admin summary, role enforcement, login lockout, session timeout.

Run:  python3 tests/test_admin.py
All test execution is inside run_tests() behind __name__ guard.
"""

import sys, os, json, tempfile, shutil, sqlite3, time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

PASS = 0
FAIL = 0


def check(cond, msg):
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  \u2713 {msg}")
    else:
        FAIL += 1
        print(f"  \u2717 FAIL: {msg}")


# ═══════════════════════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

def _make_test_app():
    """Create a Flask test app with a temp database, fully initialized."""
    # We need to set up the app module with a temporary database
    # Import from app.py — but we need to override DB_PATH first
    import app as _app
    from werkzeug.security import generate_password_hash

    fd, db_path = tempfile.mkstemp(suffix='.db')
    os.close(fd)

    # Save originals
    orig_db_path = _app.DB_PATH
    orig_get_db = _app._get_db

    # Override DB_PATH
    _app.DB_PATH = db_path

    def _test_get_db():
        conn = sqlite3.connect(db_path, timeout=10)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        return conn

    _app._get_db = _test_get_db

    # Initialize schema
    conn = _test_get_db()
    try:
        # Users table
        conn.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT NOT NULL UNIQUE,
                display_name TEXT NOT NULL,
                role TEXT NOT NULL DEFAULT 'reviewer',
                pin_hash TEXT NOT NULL DEFAULT '',
                is_active INTEGER NOT NULL DEFAULT 1,
                last_login TEXT DEFAULT '',
                created_at TEXT NOT NULL DEFAULT '',
                updated_at TEXT NOT NULL DEFAULT ''
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_users_username ON users(username)")

        # Events table
        conn.execute("""
            CREATE TABLE IF NOT EXISTS app_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TEXT NOT NULL,
                level TEXT NOT NULL DEFAULT 'info',
                event_type TEXT NOT NULL,
                message TEXT NOT NULL DEFAULT '',
                user_id INTEGER,
                user_display TEXT DEFAULT '',
                job_id TEXT DEFAULT '',
                details_json TEXT DEFAULT '',
                ip_addr TEXT DEFAULT ''
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_events_ts ON app_events(ts)")

        # Jobs table (needed for some tests)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS jobs (
                id TEXT PRIMARY KEY,
                data TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT '',
                client_name TEXT DEFAULT '',
                created TEXT DEFAULT '',
                updated TEXT DEFAULT ''
            )
        """)

        conn.commit()
    finally:
        conn.close()

    # Seed a test admin user
    now = _app.datetime.now().isoformat()
    pin_hash = generate_password_hash("123456")
    conn = _test_get_db()
    try:
        conn.execute(
            """INSERT INTO users (username, display_name, role, pin_hash, is_active, created_at, updated_at)
               VALUES (?, ?, ?, ?, 1, ?, ?)""",
            ("testadmin", "Test Admin", "admin", pin_hash, now, now)
        )
        conn.execute(
            """INSERT INTO users (username, display_name, role, pin_hash, is_active, created_at, updated_at)
               VALUES (?, ?, ?, ?, 1, ?, ?)""",
            ("testpartner", "Test Partner", "partner", pin_hash, now, now)
        )
        conn.execute(
            """INSERT INTO users (username, display_name, role, pin_hash, is_active, created_at, updated_at)
               VALUES (?, ?, ?, ?, 1, ?, ?)""",
            ("testreviewer", "Test Reviewer", "reviewer", pin_hash, now, now)
        )
        # Disabled user
        conn.execute(
            """INSERT INTO users (username, display_name, role, pin_hash, is_active, created_at, updated_at)
               VALUES (?, ?, ?, ?, 0, ?, ?)""",
            ("disabled", "Disabled User", "reviewer", pin_hash, now, now)
        )
        conn.commit()
    finally:
        conn.close()

    # Configure test client
    _app.app.config["TESTING"] = True
    _app.app.config["SECRET_KEY"] = "test-secret-key-12345"
    client = _app.app.test_client()

    return client, _app, db_path, orig_db_path, orig_get_db


def _cleanup_test_app(_app, db_path, orig_db_path, orig_get_db):
    """Restore original DB state."""
    _app.DB_PATH = orig_db_path
    _app._get_db = orig_get_db
    try:
        os.unlink(db_path)
    except OSError:
        pass


def _login(client, username="testadmin", pin="123456"):
    """Helper: login and return response."""
    return client.post("/login", data={"username": username, "pin": pin},
                       follow_redirects=False)


# ═══════════════════════════════════════════════════════════════════════════════
# TEST A: USER DB FUNCTIONS
# ═══════════════════════════════════════════════════════════════════════════════

def test_user_db_functions():
    """User CRUD: create, get_by_id, get_by_username, list_all, list_active."""
    print("\n\u2550\u2550\u2550 TEST A: USER DB FUNCTIONS \u2550\u2550\u2550")
    client, _app, db_path, orig_db, orig_get = _make_test_app()
    try:
        # get_user_by_username
        u = _app.get_user_by_username("testadmin")
        check(u is not None, "get_user_by_username returns user")
        check(u["username"] == "testadmin", f"username correct (got {u['username']})")
        check(u["role"] == "admin", f"role correct (got {u['role']})")
        check(u["is_active"] is True, "is_active is True")
        check(u["display_name"] == "Test Admin", f"display_name correct")

        # get_user_by_id
        u2 = _app.get_user_by_id(u["id"])
        check(u2 is not None, "get_user_by_id returns user")
        check(u2["id"] == u["id"], "same user retrieved by ID")

        # Missing user
        missing = _app.get_user_by_username("nonexistent")
        check(missing is None, "missing user returns None")

        # list_all_users
        all_users = _app.list_all_users()
        check(len(all_users) == 4, f"list_all returns 4 users (got {len(all_users)})")

        # list_active_users
        active = _app.list_active_users()
        check(len(active) == 3, f"list_active returns 3 active users (got {len(active)})")

        # create_user
        from werkzeug.security import generate_password_hash
        new_id = _app.create_user("newuser", "New User", "reviewer",
                                   generate_password_hash("654321"))
        check(new_id > 0, f"create_user returns ID (got {new_id})")
        new_user = _app.get_user_by_id(new_id)
        check(new_user["username"] == "newuser", "new user created correctly")

        # Invalid role
        try:
            _app.create_user("bad", "Bad", "superadmin", "hash")
            check(False, "should reject invalid role")
        except ValueError:
            check(True, "rejects invalid role")

        # set_user_active (disable)
        _app.set_user_active(new_id, False)
        disabled = _app.get_user_by_id(new_id)
        check(disabled["is_active"] is False, "user disabled")

        # set_user_active (enable)
        _app.set_user_active(new_id, True)
        enabled = _app.get_user_by_id(new_id)
        check(enabled["is_active"] is True, "user re-enabled")

        # update_last_login
        _app.update_last_login(u["id"])
        u3 = _app.get_user_by_id(u["id"])
        check(u3["last_login"] != "", "last_login updated")

        # set_user_pin_hash
        new_hash = generate_password_hash("999999")
        _app.set_user_pin_hash(u["id"], new_hash)
        u4 = _app.get_user_by_id(u["id"])
        check(u4["pin_hash"] == new_hash, "pin_hash updated")

        # generate_6_digit_pin
        pin = _app.generate_6_digit_pin()
        check(len(pin) == 6, f"PIN is 6 digits (got {len(pin)})")
        check(pin.isdigit(), f"PIN is all digits (got {pin!r})")
        check(int(pin) >= 100000, f"PIN >= 100000 (got {pin})")

    finally:
        _cleanup_test_app(_app, db_path, orig_db, orig_get)


# ═══════════════════════════════════════════════════════════════════════════════
# TEST B: EVENT LOGGING
# ═══════════════════════════════════════════════════════════════════════════════

def test_event_logging():
    """Event log: write + query + filters."""
    print("\n\u2550\u2550\u2550 TEST B: EVENT LOGGING \u2550\u2550\u2550")
    client, _app, db_path, orig_db, orig_get = _make_test_app()
    try:
        # Log events
        _app.log_event("info", "login_success", "Admin logged in",
                       user_id=1, ip_addr="127.0.0.1")
        _app.log_event("warn", "login_failed", "Bad PIN for susan",
                       user_id=2, ip_addr="127.0.0.1")
        _app.log_event("error", "job_failed", "Job crashed",
                       job_id="test-job-001")
        _app.log_event("info", "job_completed", "Job done",
                       job_id="test-job-001",
                       details={"pages": 5, "docs": 3})

        # Query all
        all_events = _app.query_events()
        check(len(all_events) == 4, f"4 events logged (got {len(all_events)})")

        # Most recent first
        check(all_events[0]["event_type"] == "job_completed",
              "most recent event first")

        # Filter by level
        warns = _app.query_events(level="warn")
        check(len(warns) == 1, f"1 warn event (got {len(warns)})")
        check(warns[0]["message"] == "Bad PIN for susan", "correct warn message")

        errors = _app.query_events(level="error")
        check(len(errors) == 1, f"1 error event (got {len(errors)})")

        # Filter by job_id
        job_events = _app.query_events(job_id="test-job-001")
        check(len(job_events) == 2, f"2 events for job (got {len(job_events)})")

        # Filter by user_id
        user1_events = _app.query_events(user_id=1)
        check(len(user1_events) == 1, f"1 event for user 1 (got {len(user1_events)})")

        # Details JSON preserved
        completed = [e for e in all_events if e["event_type"] == "job_completed"][0]
        check(completed["details_json"] != "", "details_json not empty")
        details = json.loads(completed["details_json"])
        check(details["pages"] == 5, "details preserved correctly")

        # Limit
        limited = _app.query_events(limit=2)
        check(len(limited) == 2, f"limit=2 returns 2 (got {len(limited)})")

        # Event structure
        e = all_events[0]
        check("id" in e, "event has id")
        check("ts" in e, "event has ts")
        check("level" in e, "event has level")
        check("event_type" in e, "event has event_type")
        check("message" in e, "event has message")
        check("user_id" in e, "event has user_id")
        check("user_display" in e, "event has user_display")
        check("job_id" in e, "event has job_id")
        check("ip_addr" in e, "event has ip_addr")

    finally:
        _cleanup_test_app(_app, db_path, orig_db, orig_get)


# ═══════════════════════════════════════════════════════════════════════════════
# TEST C: LOGIN FLOW
# ═══════════════════════════════════════════════════════════════════════════════

def test_login_flow():
    """Login: success, fail, disabled user, redirect."""
    print("\n\u2550\u2550\u2550 TEST C: LOGIN FLOW \u2550\u2550\u2550")
    client, _app, db_path, orig_db, orig_get = _make_test_app()
    try:
        # Login page renders
        resp = client.get("/login")
        check(resp.status_code == 200, "GET /login returns 200")
        check(b"Bearden Intake Platform" in resp.data, "login page has title")
        check(b"testadmin" in resp.data, "login page lists users")

        # Successful login
        resp = _login(client, "testadmin", "123456")
        check(resp.status_code == 303, f"successful login redirects (got {resp.status_code})")
        check("/admin" in resp.headers.get("Location", ""),
              f"redirects to /admin (got {resp.headers.get('Location', '')})")

        # Session is set — can access admin
        resp = client.get("/admin")
        check(resp.status_code == 200, "admin accessible after login")
        check(b"Overview" in resp.data, "admin page renders")

        # Logout
        resp = client.post("/logout", follow_redirects=False)
        check(resp.status_code == 303, "logout redirects")

        # After logout, admin requires login again
        resp = client.get("/admin", follow_redirects=False)
        check(resp.status_code == 302, "admin redirects after logout")
        check("/login" in resp.headers.get("Location", ""),
              "redirects to /login")

        # Failed login — wrong PIN
        resp = client.post("/login",
                           data={"username": "testadmin", "pin": "999999"},
                           follow_redirects=True)
        check(resp.status_code == 200, "failed login returns 200")
        check(b"Invalid credentials" in resp.data, "shows error message")

        # Failed login — non-numeric PIN
        resp = client.post("/login",
                           data={"username": "testadmin", "pin": "abcdef"},
                           follow_redirects=True)
        check(b"Invalid credentials" in resp.data, "rejects non-numeric PIN")

        # Failed login — 5-digit PIN
        resp = client.post("/login",
                           data={"username": "testadmin", "pin": "12345"},
                           follow_redirects=True)
        check(b"Invalid credentials" in resp.data, "rejects 5-digit PIN")

        # Disabled user cannot login
        resp = client.post("/login",
                           data={"username": "disabled", "pin": "123456"},
                           follow_redirects=True)
        check(b"Invalid credentials" in resp.data, "disabled user rejected")

        # Nonexistent user
        resp = client.post("/login",
                           data={"username": "nobody", "pin": "123456"},
                           follow_redirects=True)
        check(b"Invalid credentials" in resp.data, "nonexistent user rejected")

        # Login events were logged
        events = _app.query_events(event_type="login_success")
        check(len(events) >= 1, f"login_success event logged (got {len(events)})")
        fail_events = _app.query_events(event_type="login_failed")
        check(len(fail_events) >= 1, f"login_failed events logged (got {len(fail_events)})")

    finally:
        _cleanup_test_app(_app, db_path, orig_db, orig_get)


# ═══════════════════════════════════════════════════════════════════════════════
# TEST D: ROLE ENFORCEMENT
# ═══════════════════════════════════════════════════════════════════════════════

def test_role_enforcement():
    """Role checks: admin-only, partner+admin, reviewer denied."""
    print("\n\u2550\u2550\u2550 TEST D: ROLE ENFORCEMENT \u2550\u2550\u2550")
    client, _app, db_path, orig_db, orig_get = _make_test_app()
    try:
        # Admin can access /admin
        _login(client, "testadmin", "123456")
        resp = client.get("/admin")
        check(resp.status_code == 200, "admin role can access /admin")

        # Admin can access /admin/users
        resp = client.get("/admin/users")
        check(resp.status_code == 200, "admin role can access /admin/users")

        # Admin can access /admin/events
        resp = client.get("/admin/events")
        check(resp.status_code == 200, "admin role can access /admin/events")
        client.post("/logout")

        # Partner can access /admin
        _login(client, "testpartner", "123456")
        resp = client.get("/admin")
        check(resp.status_code == 200, "partner role can access /admin")

        # Partner can access /admin/events
        resp = client.get("/admin/events")
        check(resp.status_code == 200, "partner role can access /admin/events")

        # Partner CANNOT access /admin/users
        resp = client.get("/admin/users")
        check(resp.status_code == 403, f"partner denied /admin/users (got {resp.status_code})")
        client.post("/logout")

        # Reviewer CANNOT access /admin
        _login(client, "testreviewer", "123456")
        resp = client.get("/admin")
        check(resp.status_code == 403, f"reviewer denied /admin (got {resp.status_code})")

        # Reviewer CANNOT access /admin/events
        resp = client.get("/admin/events")
        check(resp.status_code == 403, f"reviewer denied /admin/events (got {resp.status_code})")

        # Reviewer CANNOT access /admin/users
        resp = client.get("/admin/users")
        check(resp.status_code == 403, f"reviewer denied /admin/users (got {resp.status_code})")
        client.post("/logout")

    finally:
        _cleanup_test_app(_app, db_path, orig_db, orig_get)


# ═══════════════════════════════════════════════════════════════════════════════
# TEST E: ADMIN USER MANAGEMENT
# ═══════════════════════════════════════════════════════════════════════════════

def test_admin_user_management():
    """Admin: create user, reset PIN, toggle user."""
    print("\n\u2550\u2550\u2550 TEST E: ADMIN USER MANAGEMENT \u2550\u2550\u2550")
    client, _app, db_path, orig_db, orig_get = _make_test_app()
    try:
        _login(client, "testadmin", "123456")

        # Create user
        resp = client.post("/admin/users/create",
                           data={"username": "newstaff", "display_name": "New Staff",
                                 "role": "reviewer"},
                           follow_redirects=True)
        check(resp.status_code == 200, "create user returns 200")
        # Temp PIN should be displayed
        check(b"Temporary PIN" in resp.data, "temp PIN displayed")

        new_user = _app.get_user_by_username("newstaff")
        check(new_user is not None, "new user created in DB")
        check(new_user["role"] == "reviewer", "new user role is reviewer")
        check(new_user["is_active"] is True, "new user is active")

        # Create event logged
        create_events = _app.query_events(event_type="user_create")
        check(len(create_events) >= 1, "user_create event logged")

        # Reset PIN
        resp = client.post(f"/admin/users/{new_user['id']}/reset_pin",
                           follow_redirects=True)
        check(resp.status_code == 200, "reset PIN returns 200")
        check(b"Temporary PIN" in resp.data, "new temp PIN displayed")

        # PIN reset event logged
        reset_events = _app.query_events(event_type="pin_reset")
        check(len(reset_events) >= 1, "pin_reset event logged")

        # Toggle user (disable)
        resp = client.post(f"/admin/users/{new_user['id']}/toggle",
                           follow_redirects=True)
        check(resp.status_code == 200, "toggle user returns 200")
        toggled = _app.get_user_by_id(new_user["id"])
        check(toggled["is_active"] is False, "user disabled after toggle")

        # Toggle event logged
        toggle_events = _app.query_events(event_type="user_disabled")
        check(len(toggle_events) >= 1, "user_disabled event logged")

        # Toggle back (enable)
        resp = client.post(f"/admin/users/{new_user['id']}/toggle",
                           follow_redirects=True)
        toggled2 = _app.get_user_by_id(new_user["id"])
        check(toggled2["is_active"] is True, "user re-enabled after toggle")

        # Duplicate username
        resp = client.post("/admin/users/create",
                           data={"username": "newstaff", "display_name": "Dup",
                                 "role": "reviewer"},
                           follow_redirects=True)
        check(b"already exists" in resp.data, "duplicate username shows error")

        client.post("/logout")
    finally:
        _cleanup_test_app(_app, db_path, orig_db, orig_get)


# ═══════════════════════════════════════════════════════════════════════════════
# TEST F: LOGIN LOCKOUT
# ═══════════════════════════════════════════════════════════════════════════════

def test_login_lockout():
    """Login lockout after MAX_FAILED_ATTEMPTS."""
    print("\n\u2550\u2550\u2550 TEST F: LOGIN LOCKOUT \u2550\u2550\u2550")
    client, _app, db_path, orig_db, orig_get = _make_test_app()
    try:
        # Clear any prior lockout state
        _app._failed_logins.clear()

        # Fail MAX_FAILED_ATTEMPTS times
        for i in range(_app.MAX_FAILED_ATTEMPTS):
            client.post("/login",
                        data={"username": "testadmin", "pin": "000000"})

        # Next attempt should be locked out
        resp = client.post("/login",
                           data={"username": "testadmin", "pin": "123456"},
                           follow_redirects=True)
        check(b"Too many attempts" in resp.data, "lockout message shown")

        # Lockout event logged
        lockout_events = _app.query_events(event_type="login_lockout")
        check(len(lockout_events) >= 1, "login_lockout event logged")

        client.post("/logout")
    finally:
        _app._failed_logins.clear()
        _cleanup_test_app(_app, db_path, orig_db, orig_get)


# ═══════════════════════════════════════════════════════════════════════════════
# TEST G: SESSION TIMEOUT
# ═══════════════════════════════════════════════════════════════════════════════

def test_session_timeout():
    """Session expires after idle timeout."""
    print("\n\u2550\u2550\u2550 TEST G: SESSION TIMEOUT \u2550\u2550\u2550")
    client, _app, db_path, orig_db, orig_get = _make_test_app()
    try:
        _login(client, "testadmin", "123456")

        # Verify session is active
        resp = client.get("/admin")
        check(resp.status_code == 200, "session active before timeout")

        # Simulate session timeout by manipulating session
        with client.session_transaction() as sess:
            sess["last_seen"] = int(time.time()) - (_app.SESSION_IDLE_SECONDS + 60)

        # Access should redirect to login
        resp = client.get("/admin", follow_redirects=False)
        check(resp.status_code == 302, f"session expired -> redirect (got {resp.status_code})")
        check("/login" in resp.headers.get("Location", ""),
              "expired session redirects to /login")

    finally:
        _cleanup_test_app(_app, db_path, orig_db, orig_get)


# ═══════════════════════════════════════════════════════════════════════════════
# TEST H: ADMIN SUMMARY API
# ═══════════════════════════════════════════════════════════════════════════════

def test_admin_summary_api():
    """API: /api/admin/summary returns correct structure."""
    print("\n\u2550\u2550\u2550 TEST H: ADMIN SUMMARY API \u2550\u2550\u2550")
    client, _app, db_path, orig_db, orig_get = _make_test_app()
    try:
        _login(client, "testadmin", "123456")

        resp = client.get("/api/admin/summary")
        check(resp.status_code == 200, "summary API returns 200")

        data = resp.get_json()
        check("health" in data, "summary has 'health' key")
        check("kpis" in data, "summary has 'kpis' key")
        check("recent_jobs" in data, "summary has 'recent_jobs' key")
        check("recent_events" in data, "summary has 'recent_events' key")

        # Health structure
        h = data["health"]
        check("version" in h, "health has version")
        check("uptime_h" in h, "health has uptime_h")
        check("state" in h, "health has state")
        check("label" in h, "health has label")
        check(h["state"] in ("good", "warn", "bad"), f"health state valid (got {h['state']})")

        # KPI structure
        k = data["kpis"]
        check("jobs_today" in k, "kpis has jobs_today")
        check("failures_today" in k, "kpis has failures_today")
        check("avg_runtime_s" in k, "kpis has avg_runtime_s")
        check("time_to_first_values_s" in k, "kpis has time_to_first_values_s")
        check("disk_free_gb" in k, "kpis has disk_free_gb")
        check("running_jobs" in k, "kpis has running_jobs")

        # Reviewer denied
        client.post("/logout")
        _login(client, "testreviewer", "123456")
        resp = client.get("/api/admin/summary")
        check(resp.status_code == 403, f"reviewer denied summary API (got {resp.status_code})")

        client.post("/logout")
    finally:
        _cleanup_test_app(_app, db_path, orig_db, orig_get)


# ═══════════════════════════════════════════════════════════════════════════════
# TEST I: EVENTS PAGE
# ═══════════════════════════════════════════════════════════════════════════════

def test_events_page():
    """Admin events page renders with filters."""
    print("\n\u2550\u2550\u2550 TEST I: EVENTS PAGE \u2550\u2550\u2550")
    client, _app, db_path, orig_db, orig_get = _make_test_app()
    try:
        # Log some test events
        _app.log_event("info", "test_event", "Test info event")
        _app.log_event("warn", "test_warn", "Test warn event", job_id="job-123")
        _app.log_event("error", "test_error", "Test error event", user_id=1)

        _login(client, "testadmin", "123456")

        # Events page
        resp = client.get("/admin/events")
        check(resp.status_code == 200, "events page returns 200")
        check(b"Events" in resp.data, "events page has title")

        # Filter by level
        resp = client.get("/admin/events?level=warn")
        check(resp.status_code == 200, "level filter works")

        # Filter by job_id
        resp = client.get("/admin/events?job_id=job-123")
        check(resp.status_code == 200, "job_id filter works")

        client.post("/logout")
    finally:
        _cleanup_test_app(_app, db_path, orig_db, orig_get)


# ═══════════════════════════════════════════════════════════════════════════════
# TEST J: SCHEMA CORRECTNESS
# ═══════════════════════════════════════════════════════════════════════════════

def test_schema_correctness():
    """DB schema: users and app_events tables have all required columns."""
    print("\n\u2550\u2550\u2550 TEST J: SCHEMA CORRECTNESS \u2550\u2550\u2550")
    client, _app, db_path, orig_db, orig_get = _make_test_app()
    try:
        conn = sqlite3.connect(db_path)
        try:
            # Users table columns
            cursor = conn.execute("PRAGMA table_info(users)")
            user_cols = {row[1] for row in cursor.fetchall()}
            expected_user_cols = {"id", "username", "display_name", "role",
                                  "pin_hash", "is_active", "last_login",
                                  "created_at", "updated_at"}
            check(expected_user_cols.issubset(user_cols),
                  f"users table has all columns (missing: {expected_user_cols - user_cols})")

            # Events table columns
            cursor = conn.execute("PRAGMA table_info(app_events)")
            event_cols = {row[1] for row in cursor.fetchall()}
            expected_event_cols = {"id", "ts", "level", "event_type", "message",
                                   "user_id", "user_display", "job_id",
                                   "details_json", "ip_addr"}
            check(expected_event_cols.issubset(event_cols),
                  f"app_events table has all columns (missing: {expected_event_cols - event_cols})")

            # Users UNIQUE constraint on username
            try:
                now = _app.datetime.now().isoformat()
                conn.execute(
                    """INSERT INTO users (username, display_name, role, pin_hash, is_active, created_at, updated_at)
                       VALUES (?, ?, ?, ?, 1, ?, ?)""",
                    ("testadmin", "Duplicate", "admin", "hash", now, now)
                )
                check(False, "should enforce unique username")
            except sqlite3.IntegrityError:
                check(True, "UNIQUE constraint on username enforced")

        finally:
            conn.close()
    finally:
        _cleanup_test_app(_app, db_path, orig_db, orig_get)


# ═══════════════════════════════════════════════════════════════════════════════
# TEST K: IMMUTABLE EVENTS (NO DELETE)
# ═══════════════════════════════════════════════════════════════════════════════

def test_immutable_events():
    """Events are immutable: no delete endpoint exists."""
    print("\n\u2550\u2550\u2550 TEST K: IMMUTABLE EVENTS \u2550\u2550\u2550")
    client, _app, db_path, orig_db, orig_get = _make_test_app()
    try:
        _login(client, "testadmin", "123456")

        # No DELETE endpoint for events
        resp = client.delete("/admin/events")
        check(resp.status_code == 405, f"DELETE /admin/events not allowed (got {resp.status_code})")

        # No POST endpoint that could delete events
        resp = client.post("/admin/events", data={})
        check(resp.status_code == 405, f"POST /admin/events not allowed (got {resp.status_code})")

        client.post("/logout")
    finally:
        _cleanup_test_app(_app, db_path, orig_db, orig_get)


# ═══════════════════════════════════════════════════════════════════════════════
# TEST L: PIN HASH SECURITY
# ═══════════════════════════════════════════════════════════════════════════════

def test_pin_hash_security():
    """PINs are stored as hashes, never plaintext."""
    print("\n\u2550\u2550\u2550 TEST L: PIN HASH SECURITY \u2550\u2550\u2550")
    client, _app, db_path, orig_db, orig_get = _make_test_app()
    try:
        u = _app.get_user_by_username("testadmin")
        check("123456" not in u["pin_hash"], "PIN not stored as plaintext")
        check(len(u["pin_hash"]) > 20, f"pin_hash is long hash (len={len(u['pin_hash'])})")

        # Verify hash checking works
        from werkzeug.security import check_password_hash
        check(check_password_hash(u["pin_hash"], "123456"), "correct PIN validates")
        check(not check_password_hash(u["pin_hash"], "000000"), "wrong PIN fails")

    finally:
        _cleanup_test_app(_app, db_path, orig_db, orig_get)


# ═══════════════════════════════════════════════════════════════════════════════
# TEST M: CONSTANTS
# ═══════════════════════════════════════════════════════════════════════════════

def test_constants():
    """Sprint 2 constants are correct."""
    print("\n\u2550\u2550\u2550 TEST M: CONSTANTS \u2550\u2550\u2550")
    import app as _app
    check(_app.SESSION_IDLE_SECONDS == 45 * 60, f"session timeout = 45 min (got {_app.SESSION_IDLE_SECONDS}s)")
    check(_app.LOGIN_LOCKOUT_SECONDS == 120, f"lockout = 2 min (got {_app.LOGIN_LOCKOUT_SECONDS}s)")
    check(_app.MAX_FAILED_ATTEMPTS == 5, f"max failures = 5 (got {_app.MAX_FAILED_ATTEMPTS})")
    check("admin" in _app.VALID_ROLES, "admin in VALID_ROLES")
    check("partner" in _app.VALID_ROLES, "partner in VALID_ROLES")
    check("reviewer" in _app.VALID_ROLES, "reviewer in VALID_ROLES")
    check(len(_app.VALID_ROLES) == 3, f"3 roles (got {len(_app.VALID_ROLES)})")


# ═══════════════════════════════════════════════════════════════════════════════
# RUN ALL
# ═══════════════════════════════════════════════════════════════════════════════

def run_tests():
    global PASS, FAIL

    test_user_db_functions()
    test_event_logging()
    test_login_flow()
    test_role_enforcement()
    test_admin_user_management()
    test_login_lockout()
    test_session_timeout()
    test_admin_summary_api()
    test_events_page()
    test_schema_correctness()
    test_immutable_events()
    test_pin_hash_security()
    test_constants()

    print(f"\n{'='*60}")
    print(f"  PASS: {PASS}  |  FAIL: {FAIL}  |  TOTAL: {PASS + FAIL}")
    print(f"{'='*60}")
    if FAIL > 0:
        sys.exit(1)


if __name__ == "__main__":
    run_tests()
