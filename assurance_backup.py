"""CAS Backup Manager — SQLite database snapshot and verification.

Creates timestamped backups of the Bearden database, verifies integrity
via SHA-256 checksums, and manages retention (cleanup old backups).

SQLite + WAL mode means backups require a WAL checkpoint first to ensure
all committed transactions are in the main database file.

This module never modifies financial data. It only copies the database file.

Run standalone:
    python3 assurance_backup.py [--db-path data/bearden.db] [--backup-dir data/backups]
"""

import os
import sys
import hashlib
import shutil
import sqlite3
import json
from datetime import datetime
from pathlib import Path


def _sha256_file(filepath):
    """Compute SHA-256 hash of a file."""
    h = hashlib.sha256()
    with open(filepath, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def _row_counts(db_path):
    """Get row counts for all tables in the database."""
    conn = sqlite3.connect(str(db_path), timeout=10)
    try:
        tables = [row[0] for row in
                  conn.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name").fetchall()]
        counts = {}
        for table in tables:
            try:
                count = conn.execute(f"SELECT COUNT(*) FROM [{table}]").fetchone()[0]
                counts[table] = count
            except sqlite3.Error:
                counts[table] = -1
        return counts
    finally:
        conn.close()


def create_backup(db_path, backup_dir=None):
    """Create a backup snapshot of the database.

    Args:
        db_path: Path to the SQLite database.
        backup_dir: Directory to store backups. Defaults to <db_dir>/backups/.

    Returns:
        dict with keys:
            path (str): Path to the backup file.
            sha256 (str): SHA-256 hash of the backup.
            size_bytes (int): Size of the backup file.
            row_counts (dict): Row counts for each table.
            created_at (str): ISO timestamp.
    """
    db_path = Path(db_path)
    if not db_path.exists():
        raise FileNotFoundError(f"Database not found: {db_path}")

    if backup_dir is None:
        backup_dir = db_path.parent / "backups"
    backup_dir = Path(backup_dir)
    backup_dir.mkdir(parents=True, exist_ok=True)

    # Checkpoint WAL to ensure all data is in the main file
    try:
        conn = sqlite3.connect(str(db_path), timeout=10)
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        conn.close()
    except sqlite3.Error:
        pass  # Best effort — backup may still work from main file

    # Generate backup filename with timestamp (microseconds for uniqueness)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    backup_name = f"bearden_{timestamp}.db"
    backup_path = backup_dir / backup_name

    # Copy the database file
    shutil.copy2(str(db_path), str(backup_path))

    # Compute checksum and size
    sha256 = _sha256_file(str(backup_path))
    size_bytes = backup_path.stat().st_size

    # Get row counts from the backup (verify it's a valid DB)
    row_counts = _row_counts(str(backup_path))

    return {
        "path": str(backup_path),
        "sha256": sha256,
        "size_bytes": size_bytes,
        "row_counts": row_counts,
        "created_at": datetime.now().isoformat(),
    }


def verify_backup(backup_path, expected_sha256=None):
    """Verify a backup file's integrity.

    Args:
        backup_path: Path to the backup file.
        expected_sha256: Expected SHA-256 hash (optional).

    Returns:
        dict with keys:
            verified (bool): Whether the backup is valid.
            sha256 (str): Computed SHA-256 hash.
            tables_ok (bool): Whether the database is readable and has tables.
            size_bytes (int): File size.
            row_counts (dict): Row counts if readable.
            message (str): Human-readable status.
    """
    backup_path = Path(backup_path)
    if not backup_path.exists():
        return {
            "verified": False,
            "sha256": None,
            "tables_ok": False,
            "size_bytes": 0,
            "row_counts": {},
            "message": f"Backup file not found: {backup_path}",
        }

    # Compute checksum
    sha256 = _sha256_file(str(backup_path))
    size_bytes = backup_path.stat().st_size

    # Check hash match if expected
    hash_ok = True
    if expected_sha256 and sha256 != expected_sha256:
        hash_ok = False

    # Try to read the database
    tables_ok = False
    row_counts = {}
    try:
        row_counts = _row_counts(str(backup_path))
        tables_ok = len(row_counts) > 0
    except Exception:
        pass

    verified = hash_ok and tables_ok

    if not hash_ok:
        message = f"SHA-256 mismatch: expected {expected_sha256[:16]}... got {sha256[:16]}..."
    elif not tables_ok:
        message = "Database file is corrupt or empty"
    else:
        table_count = len(row_counts)
        total_rows = sum(v for v in row_counts.values() if v >= 0)
        message = f"Verified: {table_count} tables, {total_rows} total rows, {size_bytes:,} bytes"

    return {
        "verified": verified,
        "sha256": sha256,
        "tables_ok": tables_ok,
        "size_bytes": size_bytes,
        "row_counts": row_counts,
        "message": message,
    }


def cleanup_old_backups(backup_dir, keep=30):
    """Remove old backup files, keeping the most recent `keep` backups.

    Args:
        backup_dir: Directory containing backup files.
        keep: Number of most recent backups to retain.

    Returns:
        dict with keys:
            removed (int): Number of files removed.
            kept (int): Number of files kept.
            removed_files (list): Paths of removed files.
    """
    backup_dir = Path(backup_dir)
    if not backup_dir.exists():
        return {"removed": 0, "kept": 0, "removed_files": []}

    # Find all backup files
    backups = sorted(backup_dir.glob("bearden_*.db"), key=lambda p: p.stat().st_mtime, reverse=True)

    removed_files = []
    if len(backups) > keep:
        for old_backup in backups[keep:]:
            try:
                old_backup.unlink()
                removed_files.append(str(old_backup))
            except OSError:
                pass

    return {
        "removed": len(removed_files),
        "kept": min(len(backups), keep),
        "removed_files": removed_files,
    }


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="CAS Backup Manager")
    parser.add_argument("action", choices=["create", "verify", "cleanup"],
                        help="Action to perform")
    parser.add_argument("--db-path", default="data/bearden.db",
                        help="Path to SQLite database")
    parser.add_argument("--backup-dir", default="data/backups",
                        help="Backup directory")
    parser.add_argument("--backup-file", help="Specific backup file (for verify)")
    parser.add_argument("--expected-sha256", help="Expected SHA-256 (for verify)")
    parser.add_argument("--keep", type=int, default=30, help="Backups to keep (for cleanup)")
    args = parser.parse_args()

    if args.action == "create":
        result = create_backup(args.db_path, args.backup_dir)
        print(f"\nBackup created:")
        print(f"  Path:     {result['path']}")
        print(f"  SHA-256:  {result['sha256']}")
        print(f"  Size:     {result['size_bytes']:,} bytes")
        print(f"  Tables:   {len(result['row_counts'])}")
        for table, count in sorted(result["row_counts"].items()):
            print(f"    {table}: {count} rows")

    elif args.action == "verify":
        path = args.backup_file or args.backup_dir
        result = verify_backup(path, args.expected_sha256)
        icon = "\u2713" if result["verified"] else "\u2717"
        print(f"\n{icon} {result['message']}")

    elif args.action == "cleanup":
        result = cleanup_old_backups(args.backup_dir, args.keep)
        print(f"\nCleanup: removed {result['removed']}, kept {result['kept']}")
        for f in result["removed_files"]:
            print(f"  Removed: {f}")

    sys.exit(0)
