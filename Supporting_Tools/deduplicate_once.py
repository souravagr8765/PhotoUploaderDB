#!/usr/bin/env python3
"""
One-time Deduplication + Renumber + Sync Script
------------------------------------------------
Finds records in a local SQLite database (clone of Nhost/Neon) with duplicate
file_hash values, removes the extras, renumbers the remaining sl_no
sequentially to fill gaps, logs all changes, and replicates both deletions
and renumbering to the online Nhost and Neon PostgreSQL databases.

Supports:
  - Normal run: full dedup + renumber + sync to cloud
  - --resume:  continue from a saved checkpoint
  - --resync:  binary-search recovery after an interrupted sync (no checkpoint)
               compares local SQLite (source of truth) against each cloud DB
               and replays only what's needed

Usage:
    python Supporting_Tools/deduplicate_once.py                       # live run
    python Supporting_Tools/deduplicate_once.py --dry-run             # preview only
    python Supporting_Tools/deduplicate_once.py --resume              # resume from checkpoint
    python Supporting_Tools/deduplicate_once.py --resync              # binary-search recovery
    python Supporting_Tools/deduplicate_once.py --local-db <path>     # custom path
"""

import os
import sys
import json
import sqlite3
import argparse
import logging
import glob
import socket
import time
from datetime import datetime
from collections import defaultdict
from tqdm import tqdm

# Add project root to sys.path so config_loader works
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

import urllib.parse
import pg8000.dbapi
from infra.config_loader import get_config

# ── Configuration ──────────────────────────────────────────────────────────

DEFAULT_LOCAL_DB = os.path.join(PROJECT_ROOT, "Data", "local_cache.db")
REPORT_DIR = os.path.join(PROJECT_ROOT, "Data", "dedup_reports")
CHECKPOINT_FILE = os.path.join(REPORT_DIR, "dedup_checkpoint.json")

# Set up a simple console logger for this script (independent of infra.logger
# to avoid Loki dependency issues when run stand-alone)
console = logging.StreamHandler()
console.setLevel(logging.INFO)
console.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))

logger = logging.getLogger("dedup_script")
logger.setLevel(logging.INFO)
logger.addHandler(console)
logger.propagate = False


# ── PostgreSQL Helpers ─────────────────────────────────────────────────────

def parse_pg_url(url: str):
    """Parse a PostgreSQL connection URL into kwargs for pg8000."""
    if not url:
        return None
    parsed = urllib.parse.urlparse(url)
    return {
        "user": parsed.username,
        "password": parsed.password,
        "host": parsed.hostname,
        "port": parsed.port or 5432,
        "database": parsed.path.lstrip("/"),
    }


# Default batch size for PG operations — small enough to stay under
# Neon's 5-minute idle timeout between queries.
PG_BATCH_SIZE = 500


def connect_pg(label: str, url: str):
    """Connect to a PostgreSQL database. Returns the connection or None.

    Enables TCP keepalive and stores reconnection parameters so the
    connection can be auto-recovered if the server drops it (e.g. Neon's
    5-minute idle timeout).
    """
    params = parse_pg_url(url)
    if not params:
        logger.warning("  ⚠️  No connection URL provided for %s.", label)
        return None
    try:
        conn = pg8000.dbapi.connect(**params)
        conn.autocommit = True

        # ── Enable TCP keepalive on the underlying socket ──
        try:
            sock = getattr(conn, '_socket', None) or getattr(conn, 'socket', None)
            if sock:
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
                # Send first keepalive probe after 120s idle
                if hasattr(socket, 'TCP_KEEPIDLE'):
                    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, 120)
                if hasattr(socket, 'TCP_KEEPINTVL'):
                    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, 30)
                if hasattr(socket, 'TCP_KEEPCNT'):
                    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, 3)
        except Exception:
            pass  # Best-effort — keepalive is a bonus, not a requirement

        # Store reconnect info on the connection object
        conn._pg_label = label
        conn._pg_params = params

        logger.info("  ✅ Connected to %s", label)
        return conn
    except Exception as e:
        logger.error("  ❌ Failed to connect to %s: %s", label, e)
        return None


def _reconnect_pg(conn):
    """Reconnect a cloud PG connection that was dropped.
    Returns a fresh connection or None.
    """
    label = getattr(conn, '_pg_label', 'unknown')
    params = getattr(conn, '_pg_params', None)
    if not params:
        logger.error("  ❌ Cannot reconnect %s: no stored connection parameters.", label)
        return None
    try:
        new_conn = pg8000.dbapi.connect(**params)
        new_conn.autocommit = True
        new_conn._pg_label = label
        new_conn._pg_params = params
        logger.info("  🔁 Reconnected to %s", label)
        return new_conn
    except Exception as e:
        logger.error("  ❌ Reconnection to %s failed: %s", label, e)
        return None


def _pg_keepalive_ping(conn):
    """Send a lightweight ping to keep the PG connection alive."""
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
    except Exception:
        pass  # Ping failures are handled by the caller's retry logic


def connect_sqlite(db_path: str):
    """Connect to the local SQLite database. Returns the connection or None."""
    if not os.path.exists(db_path):
        logger.error("  ❌ Local database not found at: %s", db_path)
        return None
    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        logger.info("  ✅ Connected to local SQLite: %s", db_path)
        return conn
    except Exception as e:
        logger.error("  ❌ Failed to connect to local database: %s", e)
        return None


# ── Deduplication Logic ────────────────────────────────────────────────────

def find_duplicates(sqlite_conn) -> dict:
    """Find all records with duplicate file_hash values in the local DB.

    Returns {file_hash: [dict records]} — records sorted by sl_no ASC.
    Returns empty dict if no duplicates or if the column doesn't exist.
    """
    cursor = sqlite_conn.cursor()

    # Verify the table and column exist
    cursor.execute("PRAGMA table_info(media_library)")
    cols = [row["name"] for row in cursor.fetchall()]
    if "file_hash" not in cols:
        logger.error("  ❌ media_library table does not have a 'file_hash' column.")
        return {}

    # Find hashes that appear more than once
    cursor.execute("""
        SELECT file_hash, COUNT(*) AS cnt
        FROM media_library
        WHERE file_hash IS NOT NULL AND file_hash != ''
        GROUP BY file_hash
        HAVING COUNT(*) > 1
    """)
    duplicate_hashes = [row["file_hash"] for row in cursor.fetchall()]

    if not duplicate_hashes:
        logger.info("  ✅ No duplicate file_hashes found.")
        return {}

    # Fetch full records for those hashes
    placeholders = ",".join("?" for _ in duplicate_hashes)
    cursor.execute(f"""
        SELECT sl_no, file_hash, filename, file_size_bytes,
               upload_date, account_email, device_source, remote_id, album_name
        FROM media_library
        WHERE file_hash IN ({placeholders})
        ORDER BY file_hash, sl_no ASC
    """, duplicate_hashes)

    col_names = ["sl_no", "file_hash", "filename", "file_size_bytes",
                  "upload_date", "account_email", "device_source",
                  "remote_id", "album_name"]

    duplicates: dict[str, list] = defaultdict(list)
    for row in cursor.fetchall():
        duplicates[row["file_hash"]].append({k: row[k] for k in col_names})

    return dict(duplicates)


def build_deletion_plan(duplicates: dict) -> list[dict]:
    """Given duplicate groups, decide which records to delete.

    Strategy per group of identical file_hash:
      1. If all fields match exactly, keep any one (lowest sl_no).
      2. If account_email differs, delete the one with
         "photouploader.sourav@gmail.com" (from an older sync)
         and keep the other account's record.
      3. Otherwise, fall back to keeping the lowest sl_no.

    Returns a list of dicts describing each record to remove.
    """
    to_delete = []
    BLACKLIST_EMAIL = "photouploader.sourav@gmail.com"

    for file_hash, records in duplicates.items():
        # Records already sorted by sl_no ASC
        # Separate by account_email
        blacklisted = [r for r in records if r["account_email"] == BLACKLIST_EMAIL]
        non_blacklisted = [r for r in records if r["account_email"] != BLACKLIST_EMAIL]

        if blacklisted and non_blacklisted:
            # Different accounts: keep the non-blacklisted record(s),
            # delete all blacklisted ones
            kept = non_blacklisted[0]  # lowest sl_no from other account
            for record in blacklisted + non_blacklisted[1:]:
                to_delete.append(_deletion_item(file_hash, record, kept))
        else:
            # All same account (or only one type): keep lowest sl_no
            kept = records[0]
            for record in records[1:]:
                to_delete.append(_deletion_item(file_hash, record, kept))

    return to_delete


def _deletion_item(file_hash: str, record: dict, kept: dict) -> dict:
    """Build a deletion record entry."""
    return {
        "file_hash":          file_hash,
        "sl_no":              record["sl_no"],
        "filename":           record["filename"],
        "file_size_bytes":    record["file_size_bytes"],
        "upload_date":        record["upload_date"],
        "account_email":      record["account_email"],
        "device_source":      record["device_source"],
        "remote_id":          record["remote_id"],
        "album_name":         record["album_name"],
        "kept_sl_no":         kept["sl_no"],
        "kept_filename":      kept["filename"],
        "kept_account_email": kept["account_email"],
        "reason": (
            "different_account"
            if record["account_email"] != kept["account_email"]
            else "exact_duplicate"
        ),
    }


def preview_deletions(plan: list[dict]):
    """Print a human-readable summary of the deletion plan."""
    logger.info("  Records to delete: %s", len(plan))
    for item in plan[:10]:
        hash_short = item["file_hash"][:16] if item["file_hash"] else "N/A"
        logger.info(
            "    - sl_no=%-6s | hash=%-18s... | filename=%s",
            item["sl_no"], hash_short, item["filename"]
        )
    if len(plan) > 10:
        logger.info("    ... and %s more", len(plan) - 10)


def batch_executemany(conn, label: str, query: str, params_list: list[tuple],
                      batch_size: int = PG_BATCH_SIZE, max_retries: int = 3):
    """Execute executemany in batches with keepalive pings between batches
    and auto-reconnect + retry on failure.

    This prevents Neon's 5-minute idle timeout from killing the connection
    during long-running sync operations.

    Returns (rows_processed, conn) where conn may be a reconnected connection.
    """
    if not params_list:
        return 0, conn

    total = len(params_list)
    remaining = list(params_list)
    processed = 0
    retries = 0

    pbar = tqdm(total=total, desc=f"  {label}", unit="rec", leave=False)

    while remaining and retries <= max_retries:
        batch = remaining[:batch_size]
        try:
            cursor = conn.cursor()
            cursor.executemany(query, batch)
            processed += len(batch)
            remaining = remaining[batch_size:]
            retries = 0  # Reset on success
            pbar.update(len(batch))

            # Keepalive ping between batches
            if remaining:
                _pg_keepalive_ping(conn)

        except Exception as e:
            retries += 1
            pbar.close()
            logger.warning(
                "  ⚠️  Query failed on %s (batch starting at #%s, retry %s/%s): %s",
                label, processed, retries, max_retries, e
            )

            if retries > max_retries:
                logger.error("  ❌ %s: too many retries. %s records remaining.", label, len(remaining))
                raise

            # Reconnect and retry the same batch
            new_conn = _reconnect_pg(conn)
            if new_conn is None:
                raise RuntimeError(f"Cannot reconnect to {label}")
            conn = new_conn
            pbar = tqdm(total=total, desc=f"  {label}", unit="rec", leave=False)
            pbar.update(processed)

    pbar.close()
    return processed, conn


# ── Execution ──────────────────────────────────────────────────────────────

def delete_from_sqlite(sqlite_conn, plan: list[dict], dry_run: bool) -> bool:
    """Delete duplicate records from the local SQLite database."""
    if not plan:
        return True

    cursor = sqlite_conn.cursor()
    sl_nos = [item["sl_no"] for item in plan]

    if dry_run:
        logger.info("  🏜️  [DRY-RUN] Would delete %s record(s) from local SQLite.", len(sl_nos))
        for item in plan[:5]:
            logger.info(
                "         sl_no=%s (hash=%s..., filename=%s)",
                item["sl_no"], item["file_hash"][:12], item["filename"]
            )
        return True

    cursor.executemany("DELETE FROM media_library WHERE sl_no = ?",
                       [(s,) for s in sl_nos])
    sqlite_conn.commit()
    logger.info("  ✅ Local SQLite: %s duplicate record(s) deleted.", len(sl_nos))
    return True


def delete_from_pg(label: str, conn, plan: list[dict], dry_run: bool):
    """Delete duplicate records from a cloud PostgreSQL database by sl_no.
    Uses batched execution with keepalive pings to survive Neon's timeout.

    Returns (success, conn) where conn may be a reconnected connection.
    """
    if not conn or not plan:
        return True, conn

    sl_nos = [item["sl_no"] for item in plan]

    if dry_run:
        logger.info("  🏜️  [DRY-RUN] Would delete %s record(s) from %s.", len(sl_nos), label)
        return True, conn

    processed, conn = batch_executemany(
        conn, label,
        "DELETE FROM media_library WHERE sl_no = %s",
        [(s,) for s in sl_nos]
    )
    logger.info("  ✅ %s: %s duplicate record(s) deleted.", label, processed)
    return True, conn


# ── Renumbering Logic ──────────────────────────────────────────────────────

def _renumber_pg_two_phase(conn, label: str, renumber_map: dict,
                            batch_size: int = PG_BATCH_SIZE):
    """Renumber records in a PG database using a two-phase approach to avoid
    unique constraint violations on the primary key (sl_no).

    Phase 1: Move all affected records to temporary negative values
             (`-old_sl_no`) — guaranteed unique since sl_no is always positive.
    Phase 2: Update from the negative temp values to the correct new sl_nos.

    Returns (processed, conn) where conn may be a reconnected connection.
    """
    if not renumber_map:
        return 0, conn

    old_sl_nos = list(renumber_map.keys())

    # ── Phase 1: move to negative temp values ──
    ph1_params = [(old,) for old in old_sl_nos]
    _, conn = batch_executemany(
        conn, label,
        "UPDATE media_library SET sl_no = -sl_no WHERE sl_no = %s",
        ph1_params, batch_size=batch_size
    )

    # ── Phase 2: move from negative temp to correct new sl_no ──
    ph2_params = [(new, -old) for old, new in renumber_map.items()]
    # WHERE on a negative value needs no special quoting with parameterised query
    processed, conn = batch_executemany(
        conn, label,
        "UPDATE media_library SET sl_no = %s WHERE sl_no = %s",
        ph2_params, batch_size=batch_size
    )

    return processed, conn


def build_renumber_plan(sqlite_conn, deletion_plan=None) -> dict:
    """After deletions, compute the new sequential sl_no for each remaining
    record. Returns {old_sl_no: new_sl_no} mapping.

    If deletion_plan is provided (dry-run mode), simulates the post-deletion
    state by excluding the sl_nos that would be deleted.
    """
    cursor = sqlite_conn.cursor()

    cursor.execute("SELECT sl_no FROM media_library ORDER BY sl_no ASC")
    all_sl_nos = [row["sl_no"] for row in cursor.fetchall()]

    if deletion_plan:
        # Simulate post-deletion state (dry-run: deletions haven't happened)
        deleted_set = set(item["sl_no"] for item in deletion_plan)
        remaining = [s for s in all_sl_nos if s not in deleted_set]
    else:
        # Live state: deletions already executed
        remaining = all_sl_nos

    renumber_map = {}
    for new_sl_no, old_sl_no in enumerate(remaining, start=1):
        if old_sl_no != new_sl_no:
            renumber_map[old_sl_no] = new_sl_no

    return renumber_map


def renumber_sqlite(sqlite_conn, renumber_map: dict, dry_run: bool) -> bool:
    """Reassign sequential sl_no values in the local SQLite database."""
    if not renumber_map:
        return True

    cursor = sqlite_conn.cursor()
    count = len(renumber_map)

    if dry_run:
        logger.info("  🏜️  [DRY-RUN] Would renumber %s sl_no(s) in local SQLite.", count)
        for old, new in list(renumber_map.items())[:5]:
            logger.info("         sl_no %s -> %s", old, new)
        return True

    cursor.executemany(
        "UPDATE media_library SET sl_no = ? WHERE sl_no = ?",
        [(new, old) for old, new in renumber_map.items()]
    )
    sqlite_conn.commit()
    logger.info("  ✅ Local SQLite: %s sl_no value(s) renumbered.", count)
    return True


def renumber_pg(label: str, conn, renumber_map: dict, dry_run: bool):
    """Reassign sequential sl_no values in a PostgreSQL database, then reset
    the auto-increment sequence. Uses batched execution with keepalive pings.

    Returns (success, conn) where conn may be a reconnected connection.
    """
    if not conn or not renumber_map:
        return True, conn

    count = len(renumber_map)

    if dry_run:
        logger.info("  🏜️  [DRY-RUN] Would renumber %s sl_no(s) and reset sequence on %s.", count, label)
        return True, conn

    # Two-phase renumbering to avoid unique constraint violations
    # Phase 1: move to negative temp values
    # Phase 2: move from negative temps to correct sl_nos
    processed, conn = _renumber_pg_two_phase(conn, label, renumber_map)

    # Reset the PG auto-increment sequence (use the possibly-new connection)
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT COALESCE(MAX(sl_no), 1) FROM media_library")
        max_sl_no = cursor.fetchone()[0]
        cursor.execute("SELECT pg_get_serial_sequence('media_library', 'sl_no')")
        seq_row = cursor.fetchone()
        seq_name = seq_row[0] if seq_row and seq_row[0] else 'media_library_sl_no_seq'
        cursor.execute("SELECT setval(%s, %s)", (seq_name, max_sl_no))
        logger.info("  ✅ %s: %s sl_no value(s) renumbered, sequence reset to %s.", label, processed, max_sl_no)
    except Exception as e:
        logger.warning("  ⚠️  %s: sequence reset failed (non-critical): %s", label, e)

    return True, conn


# ── Binary-Search Resync (for interrupted runs without a checkpoint) ──────

def _fetch_slno_hash_list(cursor, table="media_library"):
    """Fetch all (sl_no, file_hash) pairs sorted by sl_no ASC.

    Excludes records with NULL or empty file_hashes — they can't be
    meaningfully compared and would corrupt positional ordering if included
    in the binary search comparison.
    """
    cursor.execute(
        f"SELECT sl_no, file_hash FROM {table} "
        f"WHERE file_hash IS NOT NULL AND file_hash != '' "
        f"ORDER BY sl_no ASC"
    )
    return [{"sl_no": row[0], "file_hash": row[1]} for row in cursor.fetchall()]


def binary_search_sync_boundary(local_records: list, cloud_records: list) -> int:
    """Binary search to find the first index where local and cloud diverge
    (by file_hash at the same positional index).

    Returns the boundary index. Everything from this index onward is
    out-of-sync or missing.
    """
    left, right = 0, min(len(local_records), len(cloud_records))
    while left < right:
        mid = (left + right) // 2
        if local_records[mid]["file_hash"] == cloud_records[mid]["file_hash"]:
            left = mid + 1
        else:
            right = mid
    return left


def compute_resync_plan(local_records: list, cloud_records: list):
    """Compare local (source of truth) vs cloud records using binary search.

    Returns:
        boundary    – first positional index where file_hashes diverge
        to_delete   – list of sl_no values to DELETE from cloud
        to_renumber – dict {old_sl_no: new_sl_no} to UPDATE on cloud
    """
    boundary = binary_search_sync_boundary(local_records, cloud_records)

    # Build local hash → sl_no lookup (local is the source of truth)
    local_hash_map = {}
    for r in local_records:
        if r["file_hash"]:
            local_hash_map[r["file_hash"]] = r["sl_no"]

    to_delete: list[int] = []
    to_renumber: dict[int, int] = {}

    # --- Pre-boundary range (indices match by file_hash) ---
    # sl_no might differ if renumbering wasn't synced to cloud
    for i in range(0, min(boundary, len(local_records), len(cloud_records))):
        lr, cr = local_records[i], cloud_records[i]
        if lr["file_hash"] == cr["file_hash"] and lr["sl_no"] != cr["sl_no"]:
            to_renumber[cr["sl_no"]] = lr["sl_no"]

    # --- Post-boundary range (file_hashes diverged) ---
    # For each cloud record from boundary onward, check if it
    # should be deleted (not in local) or renumbered (sl_no mismatch).
    #
    # IMPORTANT: Track which hashes have already been matched to local
    # records. If cloud has multiple records with the same hash (duplicates
    # that were removed locally), only the FIRST match should be renumbered;
    # subsequent duplicates must be deleted instead.
    # Seed matched_hashes with cloud records matched positionally in the
    # pre-boundary range. This prevents duplicate cloud records (same hash
    # appearing once in pre-boundary and again post-boundary) from being
    # incorrectly renumbered instead of deleted.
    matched_hashes: set[str] = set()
    for i in range(0, min(boundary, len(cloud_records))):
        h = cloud_records[i]["file_hash"]
        if h:
            matched_hashes.add(h)

    pbar = tqdm(total=len(cloud_records) - boundary, desc="  Scanning cloud records",
                unit="rec", leave=False)
    for i in range(boundary, len(cloud_records)):
        cr = cloud_records[i]
        h = cr["file_hash"]
        if h and h in local_hash_map:
            if h not in matched_hashes:
                # First time seeing this hash — queue renumber if sl_no differs
                matched_hashes.add(h)
                local_sl = local_hash_map[h]
                if cr["sl_no"] != local_sl:
                    to_renumber[cr["sl_no"]] = local_sl
            else:
                # Duplicate hash on cloud that doesn't exist in local — delete it
                to_delete.append(cr["sl_no"])
        elif h:
            # Hash exists on cloud but not locally → pending deletion
            to_delete.append(cr["sl_no"])
        # else: hash is null/empty — skip (can't meaningfully compare)
        pbar.update(1)
    pbar.close()

    return boundary, to_delete, to_renumber


def preview_resync(label: str, boundary: int, to_delete: list, to_renumber: dict,
                   local_count: int, cloud_count: int):
    """Print a human-readable summary of the resync plan."""
    logger.info("  Sync status for %s:", label)
    logger.info("    Local records: %s | Cloud records: %s", local_count, cloud_count)
    if boundary == cloud_count == local_count and not to_renumber and not to_delete:
        logger.info("    ✅ Fully in sync — no changes needed.")
        return True

    logger.info("    📍 Binary search boundary: index %s", boundary)
    if boundary > 0:
        logger.info("      First %s record(s) are in sync (by file_hash).", boundary)
    if to_renumber:
        logger.info("    🔢 %s record(s) need sl_no update:", len(to_renumber))
        for old, new in list(to_renumber.items())[:5]:
            logger.info("         sl_no %s → %s", old, new)
        if len(to_renumber) > 5:
            logger.info("         ... and %s more", len(to_renumber) - 5)
    if to_delete:
        logger.info("    🗑️  %s record(s) need deletion (extra on cloud):", len(to_delete))
        for s in to_delete[:5]:
            logger.info("         sl_no=%s", s)
        if len(to_delete) > 5:
            logger.info("         ... and %s more", len(to_delete) - 5)
    if not to_delete and not to_renumber and local_count == cloud_count:
        logger.info("    ✅ Fully in sync.")
        return True
    return False


def apply_resync_pg(label: str, conn, to_delete: list, to_renumber: dict,
                    dry_run: bool):
    """Apply the resync plan to a cloud PostgreSQL database.
    Uses batched execution with keepalive pings.

    Returns (success, conn) where conn may be a reconnected connection.
    """
    if not conn:
        return True, conn

    # 1. Deletions (batched with keepalive)
    if to_delete:
        if dry_run:
            logger.info("  🏜️  [DRY-RUN] Would delete %s record(s) from %s.",
                        len(to_delete), label)
        else:
            _, conn = batch_executemany(
                conn, label,
                "DELETE FROM media_library WHERE sl_no = %s",
                [(s,) for s in to_delete]
            )
            logger.info("  ✅ %s: %s extra record(s) deleted.", label, len(to_delete))

    # 2. Renumbering — two-phase to avoid unique constraint violations
    if to_renumber:
        if dry_run:
            logger.info("  🏜️  [DRY-RUN] Would update %s sl_no(s) on %s.",
                        len(to_renumber), label)
        else:
            processed, conn = _renumber_pg_two_phase(conn, label, to_renumber)
            logger.info("  ✅ %s: %s sl_no value(s) updated.", label, processed)

    # 3. Reset PG sequence (use the possibly-new connection)
    if (to_delete or to_renumber) and not dry_run:
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT COALESCE(MAX(sl_no), 1) FROM media_library")
            max_sl = cursor.fetchone()[0]
            cursor.execute("SELECT pg_get_serial_sequence('media_library', 'sl_no')")
            seq_row = cursor.fetchone()
            seq_name = seq_row[0] if seq_row and seq_row[0] else 'media_library_sl_no_seq'
            cursor.execute("SELECT setval(%s, %s)", (seq_name, max_sl))
            logger.info("  🔄 %s: sequence reset to %s.", label, max_sl)
        except Exception as e:
            logger.warning("  ⚠️  %s: sequence reset failed (non-critical): %s", label, e)

    return True, conn


def run_resync(sqlite_conn, label: str, pg_conn, dry_run: bool):
    """Binary-search compare local SQLite vs a cloud PG database, then sync.

    Returns (changed, conn) where conn may be a reconnected connection.
    """
    logger.info("")
    logger.info("  ── %s ──", label)

    # Fetch records from both databases
    sql_cursor = sqlite_conn.cursor()
    pg_cursor = pg_conn.cursor()

    local_records = _fetch_slno_hash_list(sql_cursor)
    cloud_records = _fetch_slno_hash_list(pg_cursor)

    if not local_records and not cloud_records:
        logger.info("  ✅ Both databases empty — in sync.")
        return False, pg_conn

    # Binary search for sync boundary
    boundary, to_delete, to_renumber = compute_resync_plan(local_records, cloud_records)

    in_sync = preview_resync(
        label, boundary, to_delete, to_renumber,
        len(local_records), len(cloud_records)
    )
    if in_sync:
        return False, pg_conn

    # Apply the sync
    logger.info("  Applying sync...")
    _, pg_conn = apply_resync_pg(label, pg_conn, to_delete, to_renumber, dry_run=dry_run)
    return bool(to_delete or to_renumber), pg_conn


# ── Checkpoint (Resume) Helpers ──────────────────────────────────────────

CHECKPOINT_VERSION = 1


def _checkpoint_path():
    return CHECKPOINT_FILE


def save_checkpoint(deletion_plan: list[dict], renumber_map: dict,
                    status: dict, local_db_path: str, dry_run: bool):
    """Persist a checkpoint so the script can resume if interrupted."""
    os.makedirs(REPORT_DIR, exist_ok=True)
    cp = {
        "version":        CHECKPOINT_VERSION,
        "created_at":     datetime.now().isoformat(),
        "dry_run":        dry_run,
        "local_db_path":  local_db_path,
        "deletion_plan":  deletion_plan,
        "renumber_map":   {str(k): v for k, v in renumber_map.items()},
        "status":         status,
    }
    with open(_checkpoint_path(), "w") as f:
        json.dump(cp, f, indent=2, default=str)
    logger.info("  💾 Checkpoint saved: %s", _checkpoint_path())


def load_checkpoint() -> dict | None:
    """Load a saved checkpoint, or None."""
    path = _checkpoint_path()
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r") as f:
            cp = json.load(f)
        return cp
    except (json.JSONDecodeError, IOError):
        logger.warning("  ⚠️  Corrupt checkpoint file — ignoring.")
        return None


def clear_checkpoint():
    """Remove the checkpoint after successful completion."""
    path = _checkpoint_path()
    if os.path.exists(path):
        os.remove(path)
        logger.info("  🧹 Checkpoint cleared.")


def verify_checkpoint_valid(sqlite_conn, cp: dict, local_db_path: str) -> bool:
    """Quick sanity-check that the checkpoint still matches reality.
    Returns True if it looks valid enough to resume from.
    """
    # Verify same database file
    cp_db_path = cp.get("local_db_path", "")
    if os.path.abspath(cp_db_path) != os.path.abspath(local_db_path):
        logger.warning(
            "  ⚠️  Checkpoint is for a different database:\n"
            "         Checkpoint: %s\n"
            "         Current:    %s",
            cp_db_path, local_db_path
        )
        return False

    plan = cp.get("deletion_plan", [])
    if not plan:
        return False

    # Check that the first planned deletion still exists OR was already deleted
    sample = plan[0]["sl_no"]
    cursor = sqlite_conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM media_library WHERE sl_no = ?", (sample,))
    exists = cursor.fetchone()[0] > 0

    status = cp.get("status", {})
    deletions_done = status.get("deletions_sqlite", False)

    if deletions_done and exists:
        logger.warning("  ⚠️  Checkpoint says deletions done but record still exists.")
        return False
    if not deletions_done and not exists:
        logger.warning("  ⚠️  Checkpoint says deletions NOT done but record is missing.")
        return False

    return True


def make_initial_status() -> dict:
    """Return a fresh status dict with all steps marked incomplete."""
    return {
        "deletions_sqlite": False,
        "deletions_nhost":  False,
        "deletions_neon":   False,
        "renumber_plan_computed": False,
        "renumber_sqlite":  False,
        "renumber_nhost":   False,
        "renumber_neon":    False,
        "report_written":   False,
    }


# ── Reporting ──────────────────────────────────────────────────────────────

def write_report(plan: list[dict], renumber_map: dict, local_db_path: str, dry_run: bool) -> str:
    """Write a JSON report of the deduplication to disk."""
    os.makedirs(REPORT_DIR, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    suffix = "_dryrun" if dry_run else ""
    report_path = os.path.join(REPORT_DIR, f"dedup_report_{timestamp}{suffix}.json")

    report = {
        "timestamp":              datetime.now().isoformat(),
        "local_database":         local_db_path,
        "dry_run":                dry_run,
        "records_removed":        len(plan),
        "deleted_records":        plan,
        "sl_no_renumbered":       len(renumber_map),
        "sl_no_renumber_mapping": {str(k): v for k, v in renumber_map.items()},
    }

    with open(report_path, "w") as f:
        json.dump(report, f, indent=2, default=str)

    logger.info("  📄 Report saved to: %s", report_path)
    return report_path


# ── Main ───────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Deduplicate local SQLite clone by file_hash, sync deletions to Nhost & Neon, and renumber sl_no."
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Preview changes without modifying any database."
    )
    parser.add_argument(
        "--resume", action="store_true",
        help="Resume from a saved checkpoint (if one exists)."
    )
    parser.add_argument(
        "--resync", action="store_true",
        help="Binary-search recovery: compare local SQLite vs cloud DBs and "
             "sync only what diverged (use after an interrupted run without checkpoint)."
    )
    parser.add_argument(
        "--local-db", default=DEFAULT_LOCAL_DB,
        help=f"Path to local SQLite database (default: {DEFAULT_LOCAL_DB})"
    )
    args = parser.parse_args()

    print()
    logger.info("=" * 60)
    logger.info("  🔍 One-Time Dedup, Renumber & Sync")
    logger.info("=" * 60)
    if args.dry_run:
        logger.info("  🏜️  DRY RUN MODE — no changes will be made")
        logger.info("=" * 60)
    print()

    # ── Check for resume ───────────────────────────────────────────────────
    resume_cp = None
    if args.resume:
        cp = load_checkpoint()
        if not cp:
            logger.warning("  ⚠️  No checkpoint found to resume from. Starting fresh.")
        elif cp.get("dry_run", False):
            logger.warning("  ⚠️  Checkpoint is from a --dry-run run. Starting fresh.")
        else:
            resume_cp = cp
            logger.info("  ▶️  Resume mode: checkpoint loaded from %s", _checkpoint_path())

    # ── 1. Connect ────────────────────────────────────────────────────────
    logger.info("[1/7] Connecting to databases...")
    sqlite_conn = connect_sqlite(args.local_db)
    if not sqlite_conn:
        sys.exit(1)

    nhost_url = get_config("database.nhost_url")
    neon_url  = get_config("database.neon_url")

    nhost_conn = connect_pg("Nhost", nhost_url) if nhost_url else None
    neon_conn  = connect_pg("Neon",  neon_url)  if neon_url  else None

    if not nhost_conn and not neon_conn:
        logger.warning("  ⚠️  No cloud databases reachable. Will only deduplicate local SQLite.")
    print()

    # ── RESYNC MODE ────────────────────────────────────────────────────────
    if args.resync:
        if not nhost_conn and not neon_conn:
            logger.error("  ❌ No cloud databases reachable. Cannot resync.")
            sqlite_conn.close()
            sys.exit(1)

        # Warn if local SQLite still has duplicates (means dedup wasn't
        # completed locally before the interruption)
        remaining_dupes = find_duplicates(sqlite_conn)
        if remaining_dupes:
            extra = sum(len(recs) - 1 for recs in remaining_dupes.values())
            logger.warning(
                "  ⚠️  Local SQLite still has %s duplicate record(s) across %s hash(es).\n"
                "     Resync assumes local is the fully-processed source of truth.\n"
                "     Run without --resync first to complete local dedup, or\n"
                "     continue if you're sure local is correct despite duplicates.",
                extra, len(remaining_dupes)
            )
            print()

        logger.info("=" * 60)
        logger.info("  🔄 Binary-Search Resync Mode")
        logger.info("  Comparing local SQLite (source of truth) against cloud DBs")
        logger.info("=" * 60)

        any_changes = False

        # First: Nhost (as the user requested)
        if nhost_conn:
            changed, nhost_conn = run_resync(sqlite_conn, "Nhost (PostgreSQL)", nhost_conn, dry_run=args.dry_run)
            any_changes = any_changes or changed

        # Second: Neon
        if neon_conn:
            changed, neon_conn = run_resync(sqlite_conn, "Neon (PostgreSQL)", neon_conn, dry_run=args.dry_run)
            any_changes = any_changes or changed

        print()
        if args.dry_run:
            logger.info("  🏜️  DRY RUN complete — no changes made.")
        elif any_changes:
            logger.info("  ✅ Resync complete! Cloud databases brought in sync.")
        else:
            logger.info("  ✅ Both cloud databases are already in sync with local.")
        logger.info("=" * 60)

        sqlite_conn.close()
        if nhost_conn: nhost_conn.close()
        if neon_conn:  neon_conn.close()
        return

    # If resuming, verify checkpoint is still valid
    if resume_cp:
        if not verify_checkpoint_valid(sqlite_conn, resume_cp, args.local_db):
            logger.error("  ❌ Checkpoint does not match current database state. Cannot resume safely.")
            logger.info("     Run without --resume to start a fresh dedup pass.")
            sqlite_conn.close()
            if nhost_conn: nhost_conn.close()
            if neon_conn:  neon_conn.close()
            sys.exit(1)
        logger.info("  ✅ Checkpoint verified against current DB state.")
        print()

    # ── 2. Find duplicates ────────────────────────────────────────────────
    logger.info("[2/7] Scanning for duplicate file_hashes...")
    duplicates = find_duplicates(sqlite_conn)

    if not duplicates and not resume_cp:
        logger.info("\n  ✅ Nothing to deduplicate. Exiting.")
        sqlite_conn.close()
        if nhost_conn: nhost_conn.close()
        if neon_conn:  neon_conn.close()
        return

    if duplicates:
        total_groups = len(duplicates)
        total_extra  = sum(len(recs) - 1 for recs in duplicates.values())
        logger.info(
            "  Found %s hash(es) with duplicates — %s record(s) to remove.",
            total_groups, total_extra
        )
    else:
        logger.info("  ✅ No duplicate file_hashes found (likely already deduplicatated).")
    print()

    # ── 3. Build deletion plan ────────────────────────────────────────────
    logger.info("[3/7] Building deletion plan...")

    if resume_cp:
        deletion_plan = resume_cp.get("deletion_plan", [])
        logger.info("  📋 Using deletion plan from checkpoint (%s record(s)).", len(deletion_plan))
    else:
        deletion_plan = build_deletion_plan(duplicates)

    preview_deletions(deletion_plan)
    print()

    # ── 4. Execute deletions ──────────────────────────────────────────────
    logger.info("[4/7] Executing deletions...")

    status = resume_cp.get("status", make_initial_status()) if resume_cp else make_initial_status()
    renumber_map_from_cp = {int(k): v for k, v in resume_cp.get("renumber_map", {}).items()} if resume_cp else {}

    if args.dry_run:
        # Dry-run: skip all writes
        delete_from_sqlite(sqlite_conn, deletion_plan, dry_run=True)
        if nhost_conn:
            _, _ = delete_from_pg("Nhost (PostgreSQL)", nhost_conn, deletion_plan, dry_run=True)
        if neon_conn:
            _, _ = delete_from_pg("Neon (PostgreSQL)", neon_conn, deletion_plan, dry_run=True)
    else:
        if not status.get("deletions_sqlite"):
            delete_from_sqlite(sqlite_conn, deletion_plan, dry_run=False)
            status["deletions_sqlite"] = True
            save_checkpoint(deletion_plan, renumber_map_from_cp, status, args.local_db, dry_run=False)
        else:
            logger.info("  ⏭️  SQLite deletions already done (from checkpoint).")

        if nhost_conn and not status.get("deletions_nhost"):
            _, nhost_conn = delete_from_pg("Nhost (PostgreSQL)", nhost_conn, deletion_plan, dry_run=False)
            status["deletions_nhost"] = True
            save_checkpoint(deletion_plan, renumber_map_from_cp, status, args.local_db, dry_run=False)
        elif nhost_conn:
            logger.info("  ⏭️  Nhost deletions already done (from checkpoint).")

        if neon_conn and not status.get("deletions_neon"):
            _, neon_conn = delete_from_pg("Neon (PostgreSQL)", neon_conn, deletion_plan, dry_run=False)
            status["deletions_neon"] = True
            save_checkpoint(deletion_plan, renumber_map_from_cp, status, args.local_db, dry_run=False)
        elif neon_conn:
            logger.info("  ⏭️  Neon deletions already done (from checkpoint).")
    print()

    # ── 5. Build renumber plan (reads post-deletion state) ────────────────
    logger.info("[5/7] Building renumber plan...")

    if args.dry_run:
        renumber_map = build_renumber_plan(sqlite_conn, deletion_plan)
    elif resume_cp and status.get("renumber_plan_computed"):
        renumber_map = renumber_map_from_cp
        logger.info("  📋 Using renumber map from checkpoint (%s entry(s)).", len(renumber_map))
    else:
        renumber_map = build_renumber_plan(sqlite_conn)
        if not args.dry_run:
            status["renumber_plan_computed"] = True
            save_checkpoint(deletion_plan, renumber_map, status, args.local_db, dry_run=False)

    if renumber_map:
        logger.info("  %s sl_no gap(s) to fill.", len(renumber_map))
        for old, new in list(renumber_map.items())[:5]:
            logger.info("    sl_no %s -> %s", old, new)
        if len(renumber_map) > 5:
            logger.info("    ... and %s more", len(renumber_map) - 5)
    else:
        logger.info("  ✅ sl_no already sequential — no renumbering needed.")
    print()

    # ── 6. Renumber sl_no ─────────────────────────────────────────────────
    logger.info("[6/7] Renumbering sl_no...")

    if args.dry_run:
        renumber_sqlite(sqlite_conn, renumber_map, dry_run=True)
        if nhost_conn:
            _, _ = renumber_pg("Nhost (PostgreSQL)", nhost_conn, renumber_map, dry_run=True)
        if neon_conn:
            _, _ = renumber_pg("Neon (PostgreSQL)", neon_conn, renumber_map, dry_run=True)
    else:
        if not status.get("renumber_sqlite"):
            renumber_sqlite(sqlite_conn, renumber_map, dry_run=False)
            status["renumber_sqlite"] = True
            save_checkpoint(deletion_plan, renumber_map, status, args.local_db, dry_run=False)
        else:
            logger.info("  ⏭️  SQLite renumbering already done (from checkpoint).")

        if nhost_conn and not status.get("renumber_nhost"):
            _, nhost_conn = renumber_pg("Nhost (PostgreSQL)", nhost_conn, renumber_map, dry_run=False)
            status["renumber_nhost"] = True
            save_checkpoint(deletion_plan, renumber_map, status, args.local_db, dry_run=False)
        elif nhost_conn:
            logger.info("  ⏭️  Nhost renumbering already done (from checkpoint).")

        if neon_conn and not status.get("renumber_neon"):
            _, neon_conn = renumber_pg("Neon (PostgreSQL)", neon_conn, renumber_map, dry_run=False)
            status["renumber_neon"] = True
            save_checkpoint(deletion_plan, renumber_map, status, args.local_db, dry_run=False)
        elif neon_conn:
            logger.info("  ⏭️  Neon renumbering already done (from checkpoint).")
    print()

    # ── 7. Write report (after all changes — accurate state) ──────────────
    logger.info("[7/7] Writing change report...")
    if not args.dry_run and status.get("report_written"):
        logger.info("  ⏭️  Report already written in a previous run.")
        # Look for the most recent report
        reports = sorted(glob.glob(os.path.join(REPORT_DIR, "dedup_report_*.json")))
        report_path = reports[-1] if reports else _checkpoint_path()
        logger.info("     📄 Previous report: %s", report_path)
    else:
        report_path = write_report(deletion_plan, renumber_map, args.local_db, dry_run=args.dry_run)
        if not args.dry_run:
            status["report_written"] = True
            save_checkpoint(deletion_plan, renumber_map, status, args.local_db, dry_run=False)
    print()

    # ── Summary ───────────────────────────────────────────────────────────
    if args.dry_run:
        logger.info("  🏜️  DRY RUN complete. No changes made to any database.")
    else:
        logger.info("  ✅ Deduplication complete!")
        logger.info("     - Removed %s duplicate record(s) from local SQLite", len(deletion_plan))
        if renumber_map:
            logger.info("     - Renumbered %s sl_no gap(s)", len(renumber_map))
        if nhost_conn:
            logger.info("     - Replicated deletions & renumbering to Nhost")
        if neon_conn:
            logger.info("     - Replicated deletions & renumbering to Neon")
        logger.info("     - Full report: %s", report_path)
    logger.info("=" * 60)

    # Cleanup checkpoint on successful completion (not dry-run)
    if not args.dry_run and status.get("report_written"):
        clear_checkpoint()

    # Cleanup
    sqlite_conn.close()
    if nhost_conn: nhost_conn.close()
    if neon_conn:  neon_conn.close()


if __name__ == "__main__":
    main()
