#!/bin/bash
#################################################################################
# SQLite Logging Module for VM Backup Script (writer side)
#
# Provides structured SQLite database logging for backup sessions, VM backups,
# replication runs, chain health, and audit events. Runs parallel to CSV
# logging for gradual migration.
#
# Database Location: ${BACKUP_PATH}/_state/vmbackup.db (one per instance)
#
# Tables:
#   sessions          - One row per vmbackup.sh invocation
#   vm_backups        - One row per VM backup attempt
#   replication_runs  - One row per replication endpoint sync
#   replication_vms   - Junction table: VMs included in each replication run
#   chain_health      - Chain health tracking per VM period (state table)
#   chain_events      - Chain lifecycle event log (append-only audit)
#   config_events     - Configuration change event log
#   file_operations   - File operation audit trail
#   period_events     - Period lifecycle event log
#   retention_events  - Retention action audit trail
#   restore_sessions  - One row per vmrestore.sh invocation (UNI-902b)
#
# Schema Version: 2.2
#
# Module structure (Phase 4 — UNI-605):
#   Read-only / utility helpers live in `lib/sqlite_ro.sh` and are sourced
#   at the top of this file. Writer functions in this module call
#   `_sql_escape`, `_sqlite_check_dependency`, `_sqlite_query_formatted`
#   etc. from the read-only module — do NOT inline these or remove the
#   `source` line. Distinct guard variables (`_SQLITE_MODULE_LOADED` here,
#   `_SQLITE_RO_LOADED` in the read-only module) prevent re-source
#   collisions.
#
# Dependencies:
#   - sqlite3: Command-line SQLite tool
#   - STATE_DIR: Directory for database (typically $BACKUP_PATH/_state)
#   - log_info, log_error, log_debug: Logging functions from parent script
#
# Usage:
#   source lib/sqlite_module.sh       # also sources lib/sqlite_ro.sh
#   sqlite_init_database              # Initialize DB and tables (writer)
#   sqlite_session_start              # Create session record
#   sqlite_session_end                # Update session with final stats
#   sqlite_log_vm_backup              # Log VM backup result
#   sqlite_log_replication_run        # Log replication run
#   sqlite_log_replication_vm         # Associate VM with replication run
#   sqlite_log_chain_event            # Log chain lifecycle event
#   sqlite_log_config_event           # Log configuration change
#   sqlite_log_file_operation         # Log file operation
#   sqlite_log_period_event           # Log period lifecycle event
#   sqlite_log_retention_event        # Log retention action
#   sqlite_update_chain_health        # Update chain_health row
#   sqlite_archive_chain              # Mark chain archived
#   sqlite_mark_chain_for_deletion    # Soft-delete (mark for review)
#   sqlite_unmark_chain               # Undo soft-delete
#   sqlite_protect_chain              # Set purge_eligible=0
#   sqlite_unprotect_chain            # Set purge_eligible=1
#
# Read-only API (lives in lib/sqlite_ro.sh, exported when this module is
# sourced):
#   sqlite_init_readonly, sqlite_query_*, sqlite_get_*, sqlite_is_available,
#   sqlite_chain_health_summary — see lib/sqlite_ro.sh header for full list.
#
#################################################################################

# Module identification
readonly SQLITE_MODULE_NAME="sqlite_module"
readonly SQLITE_MODULE_VERSION="2.2"
readonly SQLITE_SCHEMA_VERSION="2.2"

# UNI-321: idempotency guard — re-source is a no-op once sqlite_init_database is defined.
declare -F sqlite_init_database >/dev/null 2>&1 && return 0

# Source the read-only / utility helpers (Phase 4 UNI-605).
# Writer functions below call helpers like _sql_escape and
# _sqlite_check_dependency that live in the read-only module.
# Guard variable in sqlite_ro.sh is distinct (_SQLITE_RO_LOADED).
source "$(dirname "${BASH_SOURCE[0]}")/sqlite_ro.sh"

# Module state
declare -g SQLITE_DB_PATH=""
declare -g SQLITE_CURRENT_SESSION_ID=""
declare -g SQLITE_MODULE_AVAILABLE=0

#=============================================================================
# UTILITY HELPERS
#=============================================================================


# Module-local sqlite3 wrapper (FF-174): prepend a per-connection busy-timeout
# via the `.timeout` dot-command so EVERY sqlite3 invocation in this module waits
# up to SQLITE_BUSY_TIMEOUT_MS (default 5000 ms) for a contended lock instead of
# failing instantly with SQLITE_BUSY. `command` bypasses any function/alias named
# sqlite3; the dot-command runs on the SAME connection before the SQL arg / stdin
# heredoc, so the timeout governs the statement that follows. A numeric `.timeout`
# emits nothing (sqlite3 >= 3.8.7), so command-substitution / 2>&1 captures are
# uncorrupted and the uncontended path is byte-identical.
_sq3() {
    command sqlite3 -cmd ".timeout ${SQLITE_BUSY_TIMEOUT_MS:-5000}" "$@"
}

# Run SQL and get result - helper to consolidate sqlite3 calls
# Usage: result=$(_sql_exec "SELECT ...")
_sql_exec() {
    local result
    result=$(_sq3 "$SQLITE_DB_PATH" "$1" 2>&1)
    local rc=$?
    if [[ $rc -ne 0 ]]; then
        log_debug "$SQLITE_MODULE_NAME" "_sql_exec" "SQL error (rc=$rc): $result"
        return 1
    fi
    printf '%s' "$result"
    return 0
}

#=============================================================================
# DATABASE INITIALIZATION
#=============================================================================


# Initialize database path and create tables if needed
# Sets: SQLITE_DB_PATH, SQLITE_MODULE_AVAILABLE
# Returns: 0 on success, 1 on failure
sqlite_init_database() {
    if ! _sqlite_check_dependency; then
        SQLITE_MODULE_AVAILABLE=0
        return 1
    fi
    
    # Database path: ${STATE_DIR}/vmbackup.db. %/} on the fallback so a slashed
    # BACKUP_PATH (vmbackup's convention) yields a single-slash /_state, not //_state.
    local state_dir="${STATE_DIR:-${BACKUP_PATH%/}/_state}"
    SQLITE_DB_PATH="${state_dir}/vmbackup.db"

    # INT-19 guard: the canonical state dir is "${BACKUP_PATH}/_state" at the
    # configured backup root. Any state_dir that lands inside .archives, inside
    # a period directory (YYYYMMDD, YYYYMM, YYYY-Www), or alongside .archives
    # via the "${dir}_state" no-slash composition pattern indicates a caller has
    # reassigned BACKUP_PATH/STATE_DIR to a sub-tree by mistake. We refuse to
    # mkdir there (would create a split-brain DB invisible to the real catalogue)
    # and surface the offending state so the buggy caller can be found.
    # Patterns:
    #   */.archives/*       — anything inside an .archives subtree
    #   */.archives_state*  — superset of */.archives_state/* (covers both the
    #                         no-slash sibling composition and any subdir);
    #                         we don't list */.archives_state/* separately because
    #                         shellcheck SC2221/SC2222 (rightly) flagged it as
    #                         redundant.
    #   *.archives_state    — catches a relative ending in .archives_state
    case "$state_dir" in
        */.archives/*|*/.archives_state*|*.archives_state)
            log_error "$SQLITE_MODULE_NAME" "sqlite_init_database" \
                "INT-19: refusing to initialise state dir inside .archives: '$state_dir' (BACKUP_PATH='${BACKUP_PATH:-}' STATE_DIR='${STATE_DIR:-}'). Caller must use the configured backup-root BACKUP_PATH."
            SQLITE_MODULE_AVAILABLE=0
            return 1
            ;;
    esac
    case "$state_dir" in
        */[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]/_state|*/[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]_state|\
*/[0-9][0-9][0-9][0-9][0-9][0-9]/_state|*/[0-9][0-9][0-9][0-9][0-9][0-9]_state|\
*/[0-9][0-9][0-9][0-9]-W[0-9][0-9]/_state|*/[0-9][0-9][0-9][0-9]-W[0-9][0-9]_state)
            log_error "$SQLITE_MODULE_NAME" "sqlite_init_database" \
                "INT-19: refusing to initialise state dir inside a period dir: '$state_dir' (BACKUP_PATH='${BACKUP_PATH:-}' STATE_DIR='${STATE_DIR:-}'). Caller must use the configured backup-root BACKUP_PATH."
            SQLITE_MODULE_AVAILABLE=0
            return 1
            ;;
    esac

    # Ensure state directory exists
    if [[ ! -d "$state_dir" ]]; then
        if ! mkdir -p "$state_dir" 2>/dev/null; then
            log_error "$SQLITE_MODULE_NAME" "sqlite_init_database" \
                "Failed to create state directory: $state_dir"
            SQLITE_MODULE_AVAILABLE=0
            return 1
        fi
    fi
    
    # Create tables if database doesn't exist or is empty
    if [[ ! -f "$SQLITE_DB_PATH" ]] || [[ ! -s "$SQLITE_DB_PATH" ]]; then
        log_info "$SQLITE_MODULE_NAME" "sqlite_init_database" \
            "Creating new SQLite database: $SQLITE_DB_PATH"
        if ! _sqlite_create_schema; then
            log_error "$SQLITE_MODULE_NAME" "sqlite_init_database" \
                "Failed to create database schema"
            SQLITE_MODULE_AVAILABLE=0
            return 1
        fi
    fi
    
    # Verify database is accessible
    if ! _sq3 "$SQLITE_DB_PATH" "SELECT 1;" &>/dev/null; then
        log_error "$SQLITE_MODULE_NAME" "sqlite_init_database" \
            "Database not accessible: $SQLITE_DB_PATH"
        SQLITE_MODULE_AVAILABLE=0
        return 1
    fi
    
    # Enable WAL mode for concurrent read/write access (e.g., TUI reading during backup)
    # The busy timeout for this (and every) connection comes from _sq3's `.timeout`
    # -cmd (FF-174). Do NOT re-add an inline PRAGMA busy_timeout here — it would
    # re-set the connection's timeout and override the SQLITE_BUSY_TIMEOUT_MS knob.
    _sq3 "$SQLITE_DB_PATH" "PRAGMA journal_mode=WAL;" &>/dev/null
    
    # Run schema migrations for existing databases. A failed migration must
    # fail closed: declaring a wrong-schema catalogue "available" makes the tool
    # write against a partially-migrated DB instead of falling back to the
    # DB-absent walker/period path. Mirrors the _sqlite_create_schema guard
    # above (F-sql192). (_sqlite_migrate_schema returns 0 on a fully-applied /
    # already-at-current DB, 1 on any failed step — see its explicit `return 0`.)
    if ! _sqlite_migrate_schema; then
        log_error "$SQLITE_MODULE_NAME" "sqlite_init_database" \
            "Schema migration failed; refusing to declare catalogue available"
        SQLITE_MODULE_AVAILABLE=0
        return 1
    fi

    # SEC-01: defensively normalise catalogue perms to 0640 root:backup on every
    # init. The DB's mode is otherwise set purely by the umask in force when the
    # inode was first created — a legacy inode born before the 0.5.6 `umask 027`
    # hardening (under the old umask 022) stays world-readable 644 forever, since
    # sqlite writes in place and never recreates the inode. This self-heals such
    # inodes regardless of creation-time umask, mirroring the explicit-chmod
    # pattern already used for the central log (logging_module.sh) and TPM
    # metadata. Best-effort (|| true): a non-root reader (e.g. vmrestore on a DR
    # recovery host, DOC-01) cannot chmod a root-owned DB and must not fail here.
    # WAL is enabled above, so -wal/-shm sidecars may exist; tighten them too
    # when present (they are transient — SQLite may checkpoint them away).
    chown root:backup "$SQLITE_DB_PATH" 2>/dev/null || true
    chmod 0640 "$SQLITE_DB_PATH" 2>/dev/null || true
    local _sqlite_sidecar
    for _sqlite_sidecar in "${SQLITE_DB_PATH}-wal" "${SQLITE_DB_PATH}-shm"; do
        if [[ -e "$_sqlite_sidecar" ]]; then
            chown root:backup "$_sqlite_sidecar" 2>/dev/null || true
            chmod 0640 "$_sqlite_sidecar" 2>/dev/null || true
        fi
    done

    SQLITE_MODULE_AVAILABLE=1
    log_info "$SQLITE_MODULE_NAME" "sqlite_init_database" \
        "SQLite database initialized (schema v${SQLITE_SCHEMA_VERSION})"
    return 0
}


# Dotted-version "less than" (FF-81): returns 0 iff $1 orders strictly before
# $2 under numeric/dotted-version rules. A bash string compare is lexicographic
# ("2.9" < "2.10" is FALSE because '9' > '1'), which silently mis-sequences the
# migration ladder at the first two-digit minor (2.10+). sort -V (already used
# elsewhere in this tree) orders dotted versions correctly.
_sqlite_version_lt() {
    [[ "${1:-0}" != "${2:-0}" ]] || return 1
    local _lowest
    _lowest=$(printf '%s\n%s\n' "${1:-0}" "${2:-0}" | sort -V | head -n1)
    [[ "$_lowest" == "${1:-0}" ]]
}

# Migrate schema for existing databases
# Adds missing tables/columns from newer schema versions
_sqlite_migrate_schema() {
    local current_version
    current_version=$(_sq3 "$SQLITE_DB_PATH" "SELECT value FROM schema_info WHERE key='version';" 2>/dev/null)
    
    if [[ -z "$current_version" ]]; then
        current_version="1.0"
    fi
    
    # Migration: 1.2 -> 1.3 (add chain_health table)
    if _sqlite_version_lt "$current_version" "1.3"; then
        log_info "$SQLITE_MODULE_NAME" "_sqlite_migrate_schema" \
            "Migrating schema from v$current_version to v1.3"
        
        _sq3 "$SQLITE_DB_PATH" 2>/dev/null << 'MIGRATE_EOF'
CREATE TABLE IF NOT EXISTS chain_health (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    vm_name             TEXT NOT NULL,
    period_id           TEXT NOT NULL,
    chain_location      TEXT NOT NULL,
    chain_status        TEXT NOT NULL DEFAULT 'active',
    total_checkpoints   INTEGER DEFAULT 0,
    restorable_count    INTEGER DEFAULT 0,
    broken_at           INTEGER,
    break_reason        TEXT,
    first_backup        TEXT,
    last_backup         TEXT,
    archived_at         TEXT,
    deleted_at          TEXT,
    created_at          TEXT DEFAULT CURRENT_TIMESTAMP,
    updated_at          TEXT DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(vm_name, period_id)
);
CREATE INDEX IF NOT EXISTS idx_chain_health_vm ON chain_health(vm_name);
CREATE INDEX IF NOT EXISTS idx_chain_health_status ON chain_health(chain_status);
CREATE INDEX IF NOT EXISTS idx_chain_health_restorable ON chain_health(vm_name, chain_status, restorable_count);
UPDATE schema_info SET value = '1.3' WHERE key = 'version';
MIGRATE_EOF
        local _mig_rc=$?
        if [[ $_mig_rc -ne 0 ]]; then
            log_error "$SQLITE_MODULE_NAME" "_sqlite_migrate_schema" \
                "Schema migration v$current_version→v1.3 FAILED (exit=$_mig_rc)"
            return 1
        fi
        
        log_info "$SQLITE_MODULE_NAME" "_sqlite_migrate_schema" \
            "Schema migrated to v1.3 (added chain_health table)"
        current_version="1.3"
    fi
    
    # Migration: 1.3 -> 1.4 (add orphan retention columns to chain_health)
    if _sqlite_version_lt "$current_version" "1.4"; then
        log_info "$SQLITE_MODULE_NAME" "_sqlite_migrate_schema" \
            "Migrating schema from v$current_version to v1.4"
        
        _sq3 "$SQLITE_DB_PATH" 2>/dev/null << 'MIGRATE_1_4_EOF'
BEGIN;

-- Add columns for cross-policy retention (idempotent: check before adding)
-- SQLite has no ADD COLUMN IF NOT EXISTS before 3.35, so we use pragma table_info
INSERT OR IGNORE INTO schema_info(key, value) VALUES('_mig_1_4_guard', '1');

CREATE TABLE IF NOT EXISTS _migration_temp(col_name TEXT);
DELETE FROM _migration_temp;
INSERT INTO _migration_temp SELECT name FROM pragma_table_info('chain_health');

DROP TABLE IF EXISTS _migration_temp;

UPDATE schema_info SET value = '1.4' WHERE key = 'version';
COMMIT;
MIGRATE_1_4_EOF

        # Run ALTER TABLE statements separately (they auto-commit in SQLite)
        # Check each column exists before adding to handle partial prior migrations
        local _ch_cols
        _ch_cols=$(_sq3 "$SQLITE_DB_PATH" "SELECT name FROM pragma_table_info('chain_health');" 2>/dev/null)
        
        if ! echo "$_ch_cols" | grep -qx 'rotation_policy'; then
            _sq3 "$SQLITE_DB_PATH" "ALTER TABLE chain_health ADD COLUMN rotation_policy TEXT;" 2>/dev/null
        fi
        if ! echo "$_ch_cols" | grep -qx 'archive_size_bytes'; then
            _sq3 "$SQLITE_DB_PATH" "ALTER TABLE chain_health ADD COLUMN archive_size_bytes INTEGER DEFAULT 0;" 2>/dev/null
        fi
        if ! echo "$_ch_cols" | grep -qx 'purge_eligible'; then
            _sq3 "$SQLITE_DB_PATH" "ALTER TABLE chain_health ADD COLUMN purge_eligible INTEGER DEFAULT 1;" 2>/dev/null
        fi

        # Create index and backfill (idempotent operations)
        _sq3 "$SQLITE_DB_PATH" 2>/dev/null << 'MIGRATE_1_4_IDX_EOF'
CREATE INDEX IF NOT EXISTS idx_chain_health_policy 
    ON chain_health(vm_name, rotation_policy, chain_status);

UPDATE chain_health SET rotation_policy = 
    CASE 
        WHEN period_id GLOB '[0-9][0-9][0-9][0-9]-W[0-9][0-9]' THEN 'weekly'
        WHEN period_id GLOB '[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]' THEN 'daily'
        WHEN period_id GLOB '[0-9][0-9][0-9][0-9][0-9][0-9]' THEN 'monthly'
        ELSE 'unknown'
    END
WHERE rotation_policy IS NULL;

UPDATE schema_info SET value = '1.4' WHERE key = 'version';
MIGRATE_1_4_IDX_EOF
        if [[ $? -ne 0 ]]; then
            log_error "$SQLITE_MODULE_NAME" "_sqlite_migrate_schema" \
                "Schema migration v$current_version→v1.4 FAILED"
            return 1
        fi
        
        log_info "$SQLITE_MODULE_NAME" "_sqlite_migrate_schema" \
            "Schema migrated to v1.4 (added orphan retention columns)"
    fi
    
    # Migration: 1.4 -> 1.5 (CSV-to-DB migration: new event tables + extended columns)
    if _sqlite_version_lt "$current_version" "1.5"; then
        log_info "$SQLITE_MODULE_NAME" "_sqlite_migrate_schema" \
            "Migrating schema from v$current_version to v1.5 (CSV-to-DB migration)"
        
        # Run ALTER TABLE statements individually with column-existence checks
        # This makes the migration idempotent (safe to re-run after partial failure)
        local _vb_cols _rv_cols _rr_cols
        _vb_cols=$(_sq3 "$SQLITE_DB_PATH" "SELECT name FROM pragma_table_info('vm_backups');" 2>/dev/null)
        _rv_cols=$(_sq3 "$SQLITE_DB_PATH" "SELECT name FROM pragma_table_info('replication_vms');" 2>/dev/null)
        _rr_cols=$(_sq3 "$SQLITE_DB_PATH" "SELECT name FROM pragma_table_info('replication_runs');" 2>/dev/null)

        # vm_backups extensions
        echo "$_vb_cols" | grep -qx 'restore_points_before' || \
            _sq3 "$SQLITE_DB_PATH" "ALTER TABLE vm_backups ADD COLUMN restore_points_before INTEGER DEFAULT 0;" 2>/dev/null
        echo "$_vb_cols" | grep -qx 'retry_attempt' || \
            _sq3 "$SQLITE_DB_PATH" "ALTER TABLE vm_backups ADD COLUMN retry_attempt INTEGER DEFAULT 0;" 2>/dev/null
        echo "$_vb_cols" | grep -qx 'archived_restore_points' || \
            _sq3 "$SQLITE_DB_PATH" "ALTER TABLE vm_backups ADD COLUMN archived_restore_points INTEGER DEFAULT 0;" 2>/dev/null
        echo "$_vb_cols" | grep -qx 'event_type' || \
            _sq3 "$SQLITE_DB_PATH" "ALTER TABLE vm_backups ADD COLUMN event_type TEXT;" 2>/dev/null
        echo "$_vb_cols" | grep -qx 'event_detail' || \
            _sq3 "$SQLITE_DB_PATH" "ALTER TABLE vm_backups ADD COLUMN event_detail TEXT;" 2>/dev/null

        # replication_vms extensions
        echo "$_rv_cols" | grep -qx 'status' || \
            _sq3 "$SQLITE_DB_PATH" "ALTER TABLE replication_vms ADD COLUMN status TEXT;" 2>/dev/null

        # replication_runs extensions
        echo "$_rr_cols" | grep -qx 'retry_attempt' || \
            _sq3 "$SQLITE_DB_PATH" "ALTER TABLE replication_runs ADD COLUMN retry_attempt INTEGER DEFAULT 0;" 2>/dev/null
        echo "$_rr_cols" | grep -qx 'backup_timestamp' || \
            _sq3 "$SQLITE_DB_PATH" "ALTER TABLE replication_runs ADD COLUMN backup_timestamp TEXT;" 2>/dev/null
        echo "$_rr_cols" | grep -qx 'destination_path' || \
            _sq3 "$SQLITE_DB_PATH" "ALTER TABLE replication_runs ADD COLUMN destination_path TEXT;" 2>/dev/null

        # New tables and indexes (all idempotent via IF NOT EXISTS)
        _sq3 "$SQLITE_DB_PATH" 2>/dev/null << 'MIGRATE_1_5_EOF'
CREATE TABLE IF NOT EXISTS chain_events (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id          INTEGER REFERENCES sessions(id),
    timestamp           TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    vm_name             TEXT NOT NULL,
    event_type          TEXT NOT NULL,
    chain_id            TEXT,
    period_id           TEXT,
    backup_dir          TEXT,
    chain_location      TEXT,
    chain_start_time    TEXT,
    chain_end_time      TEXT,
    checkpoint_count    INTEGER DEFAULT 0,
    full_backup_file    TEXT,
    total_chain_bytes   INTEGER DEFAULT 0,
    archive_reason      TEXT,
    archive_trigger     TEXT,
    source_backup_type  TEXT,
    covers_from         TEXT,
    covers_to           TEXT,
    restore_point_ids   TEXT
);
CREATE INDEX IF NOT EXISTS idx_chain_events_vm ON chain_events(vm_name);
CREATE INDEX IF NOT EXISTS idx_chain_events_type ON chain_events(event_type);
CREATE INDEX IF NOT EXISTS idx_chain_events_session ON chain_events(session_id);
CREATE INDEX IF NOT EXISTS idx_chain_events_period ON chain_events(period_id);

-- Configuration change events
CREATE TABLE IF NOT EXISTS config_events (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id      INTEGER REFERENCES sessions(id),
    timestamp       TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    event_type      TEXT NOT NULL,
    config_source   TEXT,
    vm_name         TEXT,
    setting_name    TEXT,
    setting_value   TEXT,
    previous_value  TEXT,
    applied_to      TEXT,
    triggered_by    TEXT,
    detail          TEXT
);
CREATE INDEX IF NOT EXISTS idx_config_events_session ON config_events(session_id);
CREATE INDEX IF NOT EXISTS idx_config_events_vm ON config_events(vm_name);
CREATE INDEX IF NOT EXISTS idx_config_events_setting ON config_events(setting_name);

-- File operation audit trail
CREATE TABLE IF NOT EXISTS file_operations (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id        INTEGER REFERENCES sessions(id),
    timestamp         TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    operation         TEXT NOT NULL,
    vm_name           TEXT,
    source_path       TEXT,
    dest_path         TEXT,
    file_type         TEXT,
    file_size_bytes   INTEGER DEFAULT 0,
    verification_data TEXT,
    reason            TEXT,
    triggered_by      TEXT,
    success           INTEGER DEFAULT 1,
    error_message     TEXT
);
CREATE INDEX IF NOT EXISTS idx_file_operations_session ON file_operations(session_id);
CREATE INDEX IF NOT EXISTS idx_file_operations_vm ON file_operations(vm_name);
CREATE INDEX IF NOT EXISTS idx_file_operations_op ON file_operations(operation);

-- Period lifecycle events
CREATE TABLE IF NOT EXISTS period_events (
    id                    INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id            INTEGER REFERENCES sessions(id),
    timestamp             TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    vm_name               TEXT NOT NULL,
    event_type            TEXT NOT NULL,
    period_id             TEXT,
    rotation_policy       TEXT,
    period_dir            TEXT,
    period_start          TEXT,
    period_end            TEXT,
    chains_count          INTEGER DEFAULT 0,
    total_restore_points  INTEGER DEFAULT 0,
    total_bytes           INTEGER DEFAULT 0,
    previous_period       TEXT,
    archive_location      TEXT,
    retention_remaining   INTEGER
);
CREATE INDEX IF NOT EXISTS idx_period_events_session ON period_events(session_id);
CREATE INDEX IF NOT EXISTS idx_period_events_vm ON period_events(vm_name);
CREATE INDEX IF NOT EXISTS idx_period_events_period ON period_events(period_id);

-- Retention action audit trail
CREATE TABLE IF NOT EXISTS retention_events (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id        INTEGER REFERENCES sessions(id),
    timestamp         TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    vm_name           TEXT NOT NULL,
    action            TEXT NOT NULL,
    target_type       TEXT,
    target_path       TEXT,
    target_period     TEXT,
    rotation_policy   TEXT,
    retention_limit   INTEGER,
    current_count     INTEGER,
    age_days          INTEGER,
    freed_bytes       INTEGER DEFAULT 0,
    preserve_reason   TEXT,
    triggered_by      TEXT,
    success           INTEGER DEFAULT 1,
    action_source     TEXT
);
CREATE INDEX IF NOT EXISTS idx_retention_events_session ON retention_events(session_id);
CREATE INDEX IF NOT EXISTS idx_retention_events_vm ON retention_events(vm_name);
CREATE INDEX IF NOT EXISTS idx_retention_events_action ON retention_events(action);

UPDATE schema_info SET value = '1.5' WHERE key = 'version';
MIGRATE_1_5_EOF
        if [[ $? -ne 0 ]]; then
            log_error "$SQLITE_MODULE_NAME" "_sqlite_migrate_schema" \
                "Schema migration v$current_version→v1.5 FAILED (CSV-to-DB migration incomplete)"
            return 1
        fi
        
        log_info "$SQLITE_MODULE_NAME" "_sqlite_migrate_schema" \
            "Schema migrated to v1.5 (CSV-to-DB migration: 5 new tables, extended columns)"
    fi

    # Migration: 1.6 -> 1.7 (Backup lifecycle & retention management columns)
    if _sqlite_version_lt "$current_version" "1.7"; then
        log_info "$SQLITE_MODULE_NAME" "_sqlite_migrate_schema" \
            "Migrating schema from v$current_version to v1.7 (lifecycle management)"

        local _ch_cols_17
        _ch_cols_17=$(_sq3 "$SQLITE_DB_PATH" "SELECT name FROM pragma_table_info('chain_health');" 2>/dev/null)

        echo "$_ch_cols_17" | grep -qx 'marked_at' || \
            _sq3 "$SQLITE_DB_PATH" "ALTER TABLE chain_health ADD COLUMN marked_at TEXT;" 2>/dev/null
        echo "$_ch_cols_17" | grep -qx 'marked_by' || \
            _sq3 "$SQLITE_DB_PATH" "ALTER TABLE chain_health ADD COLUMN marked_by TEXT;" 2>/dev/null
        echo "$_ch_cols_17" | grep -qx 'purged_at' || \
            _sq3 "$SQLITE_DB_PATH" "ALTER TABLE chain_health ADD COLUMN purged_at TEXT;" 2>/dev/null

        _sq3 "$SQLITE_DB_PATH" "UPDATE schema_info SET value = '1.7' WHERE key = 'version';" 2>/dev/null
        if [[ $? -ne 0 ]]; then
            log_error "$SQLITE_MODULE_NAME" "_sqlite_migrate_schema" \
                "Schema migration v$current_version→v1.7 FAILED"
            return 1
        fi

        log_info "$SQLITE_MODULE_NAME" "_sqlite_migrate_schema" \
            "Schema migrated to v1.7 (added marked_at, marked_by, purged_at to chain_health)"
    fi

    # Migration: 1.7 -> 1.8 (action_source column on retention_events)
    if _sqlite_version_lt "$current_version" "1.8"; then
        log_info "$SQLITE_MODULE_NAME" "_sqlite_migrate_schema" \
            "Migrating schema from v$current_version to v1.8 (retention audit: action_source)"

        local _re_cols_18
        _re_cols_18=$(_sq3 "$SQLITE_DB_PATH" "SELECT name FROM pragma_table_info('retention_events');" 2>/dev/null)

        echo "$_re_cols_18" | grep -qx 'action_source' || \
            _sq3 "$SQLITE_DB_PATH" "ALTER TABLE retention_events ADD COLUMN action_source TEXT;" 2>/dev/null

        _sq3 "$SQLITE_DB_PATH" "UPDATE schema_info SET value = '1.8' WHERE key = 'version';" 2>/dev/null
        if [[ $? -ne 0 ]]; then
            log_error "$SQLITE_MODULE_NAME" "_sqlite_migrate_schema" \
                "Schema migration v$current_version→v1.8 FAILED"
            return 1
        fi

        log_info "$SQLITE_MODULE_NAME" "_sqlite_migrate_schema" \
            "Schema migrated to v1.8 (added action_source to retention_events)"
    fi

    # Migration: 1.8 -> 1.9 (drop unused bytes_transferred from replication_vms)
    if _sqlite_version_lt "$current_version" "1.9"; then
        log_info "$SQLITE_MODULE_NAME" "_sqlite_migrate_schema" \
            "Migrating schema from v$current_version to v1.9 (drop unused column)"

        local _rv_cols_19
        _rv_cols_19=$(_sq3 "$SQLITE_DB_PATH" "SELECT name FROM pragma_table_info('replication_vms');" 2>/dev/null)

        if echo "$_rv_cols_19" | grep -qx 'bytes_transferred'; then
            _sq3 "$SQLITE_DB_PATH" "ALTER TABLE replication_vms DROP COLUMN bytes_transferred;" 2>/dev/null
        fi

        _sq3 "$SQLITE_DB_PATH" "UPDATE schema_info SET value = '1.9' WHERE key = 'version';" 2>/dev/null
        if [[ $? -ne 0 ]]; then
            log_error "$SQLITE_MODULE_NAME" "_sqlite_migrate_schema" \
                "Schema migration v$current_version→v1.9 FAILED"
            return 1
        fi

        log_info "$SQLITE_MODULE_NAME" "_sqlite_migrate_schema" \
            "Schema migrated to v1.9 (dropped bytes_transferred from replication_vms)"
    fi

    # Migration: 1.9 -> 2.0 (add session_type to sessions)
    if _sqlite_version_lt "$current_version" "2.0"; then
        log_info "$SQLITE_MODULE_NAME" "_sqlite_migrate_schema" \
            "Migrating schema from v$current_version to v2.0 (session_type column)"

        local _sess_cols_20
        _sess_cols_20=$(_sq3 "$SQLITE_DB_PATH" "SELECT name FROM pragma_table_info('sessions');" 2>/dev/null)

        echo "$_sess_cols_20" | grep -qx 'session_type' || \
            _sq3 "$SQLITE_DB_PATH" "ALTER TABLE sessions ADD COLUMN session_type TEXT;" 2>/dev/null

        _sq3 "$SQLITE_DB_PATH" "UPDATE schema_info SET value = '2.0' WHERE key = 'version';" 2>/dev/null
        if [[ $? -ne 0 ]]; then
            log_error "$SQLITE_MODULE_NAME" "_sqlite_migrate_schema" \
                "Schema migration v$current_version→v2.0 FAILED"
            return 1
        fi

        log_info "$SQLITE_MODULE_NAME" "_sqlite_migrate_schema" \
            "Schema migrated to v2.0 (added session_type to sessions)"
    fi

    # Migration: 2.0 -> 2.1 (add disk space snapshot columns to sessions)
    if _sqlite_version_lt "$current_version" "2.1"; then
        log_info "$SQLITE_MODULE_NAME" "_sqlite_migrate_schema" \
            "Migrating schema from v$current_version to v2.1 (disk space snapshot columns)"

        local _sess_cols_21
        _sess_cols_21=$(_sq3 "$SQLITE_DB_PATH" "SELECT name FROM pragma_table_info('sessions');" 2>/dev/null)

        echo "$_sess_cols_21" | grep -qx 'disk_free_bytes' || \
            _sq3 "$SQLITE_DB_PATH" "ALTER TABLE sessions ADD COLUMN disk_free_bytes INTEGER DEFAULT 0;" 2>/dev/null
        echo "$_sess_cols_21" | grep -qx 'disk_total_bytes' || \
            _sq3 "$SQLITE_DB_PATH" "ALTER TABLE sessions ADD COLUMN disk_total_bytes INTEGER DEFAULT 0;" 2>/dev/null

        if ! _sq3 "$SQLITE_DB_PATH" "UPDATE schema_info SET value = '2.1' WHERE key = 'version';" 2>/dev/null; then
            log_error "$SQLITE_MODULE_NAME" "_sqlite_migrate_schema" \
                "Schema migration v$current_version→v2.1 FAILED"
            return 1
        fi

        log_info "$SQLITE_MODULE_NAME" "_sqlite_migrate_schema" \
            "Schema migrated to v2.1 (added disk_free_bytes, disk_total_bytes to sessions)"
        current_version="2.1"
    fi

    # Migration: 2.1 -> 2.2 (UNI-902b: restore_sessions table for vmrestore.sh logging)
    if _sqlite_version_lt "$current_version" "2.2"; then
        log_info "$SQLITE_MODULE_NAME" "_sqlite_migrate_schema" \
            "Migrating schema from v$current_version to v2.2 (restore_sessions table)"

        _sq3 "$SQLITE_DB_PATH" 2>/dev/null << 'MIGRATE_2_2_EOF'
CREATE TABLE IF NOT EXISTS restore_sessions (
    id                   INTEGER PRIMARY KEY,
    vm_name              TEXT NOT NULL,
    restore_mode         TEXT,
    dry_run              INTEGER DEFAULT 0,
    target_path          TEXT,
    source_backup_path   TEXT,
    source_period_id     TEXT,
    source_checkpoint    TEXT,
    start_time           TEXT NOT NULL,
    end_time             TEXT,
    duration_sec         INTEGER,
    status               TEXT DEFAULT 'running',
    exit_code            INTEGER,
    error_message        TEXT,
    disks_restored       INTEGER DEFAULT 0,
    bytes_written        INTEGER DEFAULT 0,
    tpm_restored         INTEGER DEFAULT 0,
    identity_redefined   INTEGER DEFAULT 0,
    log_file             TEXT,
    created_at           TEXT DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_restore_sessions_vm ON restore_sessions(vm_name);
CREATE INDEX IF NOT EXISTS idx_restore_sessions_status ON restore_sessions(status);
CREATE INDEX IF NOT EXISTS idx_restore_sessions_start ON restore_sessions(start_time);
UPDATE schema_info SET value = '2.2' WHERE key = 'version';
MIGRATE_2_2_EOF
        if [[ $? -ne 0 ]]; then
            log_error "$SQLITE_MODULE_NAME" "_sqlite_migrate_schema" \
                "Schema migration v$current_version→v2.2 FAILED"
            return 1
        fi

        log_info "$SQLITE_MODULE_NAME" "_sqlite_migrate_schema" \
            "Schema migrated to v2.2 (added restore_sessions table for vmrestore logging)"
    fi

    # Pin success rc explicitly (every failure path above already returns 1, and a
    # DB already at the current schema falls through here): a future appended
    # migration block ending in a nonzero command must not silently fail-close a
    # healthy DB via the sqlite_init_database checked-return guard. Do not remove.
    return 0
}

# Create database schema (tables and indexes)
# Called by sqlite_init_database when DB doesn't exist
_sqlite_create_schema() {
    # Note: Pipe schema to sqlite3 stdin to avoid issues with -- comments
    # being interpreted as command-line options
    _sq3 "$SQLITE_DB_PATH" 2>/dev/null << 'SCHEMA_EOF'
/* Schema version tracking */
CREATE TABLE IF NOT EXISTS schema_info (
    key   TEXT PRIMARY KEY,
    value TEXT
);
INSERT OR REPLACE INTO schema_info (key, value) VALUES ('version', '2.2');
INSERT OR REPLACE INTO schema_info (key, value) VALUES ('created', datetime('now'));

/* A backup session (one vmbackup.sh invocation) */
CREATE TABLE IF NOT EXISTS sessions (
    id              INTEGER PRIMARY KEY,
    instance        TEXT NOT NULL,
    start_time      TEXT NOT NULL,
    end_time        TEXT,
    duration_sec    INTEGER,
    vms_total       INTEGER DEFAULT 0,
    vms_success     INTEGER DEFAULT 0,
    vms_failed      INTEGER DEFAULT 0,
    vms_skipped     INTEGER DEFAULT 0,
    vms_excluded    INTEGER DEFAULT 0,
    bytes_total     INTEGER DEFAULT 0,
    status          TEXT DEFAULT 'running',
    log_file        TEXT,
    session_type    TEXT,
    disk_free_bytes  INTEGER DEFAULT 0,
    disk_total_bytes INTEGER DEFAULT 0
);

/* Individual VM backup within a session */
CREATE TABLE IF NOT EXISTS vm_backups (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id       INTEGER NOT NULL REFERENCES sessions(id),
    vm_name          TEXT NOT NULL,
    vm_status        TEXT,
    os_type          TEXT,
    backup_type      TEXT,
    backup_method    TEXT,
    rotation_policy  TEXT,
    status           TEXT,
    bytes_written    INTEGER DEFAULT 0,
    chain_size_bytes INTEGER DEFAULT 0,
    total_dir_bytes  INTEGER DEFAULT 0,
    restore_points   INTEGER DEFAULT 0,
    restore_points_before INTEGER DEFAULT 0,
    duration_sec     INTEGER,
    backup_path      TEXT,
    log_file         TEXT,
    error_code       TEXT,
    error_message    TEXT,
    event_type       TEXT,
    event_detail     TEXT,
    retry_attempt    INTEGER DEFAULT 0,
    archived_restore_points INTEGER DEFAULT 0,
    qemu_agent       INTEGER DEFAULT 0,
    vm_paused        INTEGER DEFAULT 0,
    chain_archived   INTEGER DEFAULT 0,
    created_at       TEXT DEFAULT CURRENT_TIMESTAMP
);

/* A replication run (one endpoint sync) */
CREATE TABLE IF NOT EXISTS replication_runs (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id        INTEGER NOT NULL REFERENCES sessions(id),
    endpoint_name     TEXT NOT NULL,
    endpoint_type     TEXT NOT NULL,
    transport         TEXT,
    sync_mode         TEXT,
    destination       TEXT,
    start_time        TEXT,
    end_time          TEXT,
    duration_sec      INTEGER,
    bytes_transferred INTEGER DEFAULT 0,
    files_transferred INTEGER DEFAULT 0,
    status            TEXT,
    error_message     TEXT,
    log_file          TEXT,
    dest_avail_bytes  INTEGER DEFAULT 0,
    dest_total_bytes  INTEGER DEFAULT 0,
    dest_space_known  INTEGER DEFAULT 0,
    throttle_count    INTEGER DEFAULT 0,
    bwlimit_final     TEXT,
    retry_attempt     INTEGER DEFAULT 0,
    backup_timestamp  TEXT,
    destination_path  TEXT
);

/* Which VMs were included in a replication run */
CREATE TABLE IF NOT EXISTS replication_vms (
    id       INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id   INTEGER NOT NULL REFERENCES replication_runs(id),
    vm_name  TEXT NOT NULL,
    status   TEXT,
    UNIQUE(run_id, vm_name)
);

/* Chain health tracking - authoritative source for chain integrity (G2/G7) */
CREATE TABLE IF NOT EXISTS chain_health (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    vm_name             TEXT NOT NULL,
    period_id           TEXT NOT NULL,
    chain_location      TEXT NOT NULL,
    chain_status        TEXT NOT NULL DEFAULT 'active',  -- active, archived, broken, deleted
    rotation_policy     TEXT,                            -- daily, weekly, monthly (for cross-policy retention)
    total_checkpoints   INTEGER DEFAULT 0,
    restorable_count    INTEGER DEFAULT 0,               -- checkpoints that can be restored
    broken_at           INTEGER,                         -- checkpoint where chain broke (NULL if healthy)
    break_reason        TEXT,
    first_backup        TEXT,
    last_backup         TEXT,
    archived_at         TEXT,
    deleted_at          TEXT,
    archive_size_bytes  INTEGER DEFAULT 0,               -- size for retention reporting
    purge_eligible      INTEGER DEFAULT 1,               -- 0=protected, 1=can delete
    marked_at           TEXT,                             -- UTC timestamp when marked for deletion
    marked_by           TEXT,                             -- who marked: retention-policy, tui-manual, cli
    purged_at           TEXT,                             -- UTC timestamp when files actually removed
    created_at          TEXT DEFAULT CURRENT_TIMESTAMP,
    updated_at          TEXT DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(vm_name, period_id)
);

/* Indexes for common queries */
CREATE INDEX IF NOT EXISTS idx_sessions_date ON sessions(date(start_time));
CREATE INDEX IF NOT EXISTS idx_sessions_status ON sessions(status);
CREATE INDEX IF NOT EXISTS idx_vm_backups_session ON vm_backups(session_id);
CREATE INDEX IF NOT EXISTS idx_vm_backups_vm ON vm_backups(vm_name);
CREATE INDEX IF NOT EXISTS idx_vm_backups_status ON vm_backups(status);
CREATE INDEX IF NOT EXISTS idx_replication_runs_session ON replication_runs(session_id);
CREATE INDEX IF NOT EXISTS idx_replication_runs_endpoint ON replication_runs(endpoint_name);
CREATE INDEX IF NOT EXISTS idx_replication_runs_status ON replication_runs(status);
CREATE INDEX IF NOT EXISTS idx_replication_vms_run ON replication_vms(run_id);
CREATE INDEX IF NOT EXISTS idx_replication_vms_vm ON replication_vms(vm_name);
CREATE INDEX IF NOT EXISTS idx_chain_health_vm ON chain_health(vm_name);
CREATE INDEX IF NOT EXISTS idx_chain_health_status ON chain_health(chain_status);
CREATE INDEX IF NOT EXISTS idx_chain_health_restorable ON chain_health(vm_name, chain_status, restorable_count);
CREATE INDEX IF NOT EXISTS idx_chain_health_policy ON chain_health(vm_name, rotation_policy, chain_status);

/* Chain lifecycle event log (append-only audit trail) */
CREATE TABLE IF NOT EXISTS chain_events (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id          INTEGER REFERENCES sessions(id),
    timestamp           TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    vm_name             TEXT NOT NULL,
    event_type          TEXT NOT NULL,
    chain_id            TEXT,
    period_id           TEXT,
    backup_dir          TEXT,
    chain_location      TEXT,
    chain_start_time    TEXT,
    chain_end_time      TEXT,
    checkpoint_count    INTEGER DEFAULT 0,
    full_backup_file    TEXT,
    total_chain_bytes   INTEGER DEFAULT 0,
    archive_reason      TEXT,
    archive_trigger     TEXT,
    source_backup_type  TEXT,
    covers_from         TEXT,
    covers_to           TEXT,
    restore_point_ids   TEXT
);
CREATE INDEX IF NOT EXISTS idx_chain_events_vm ON chain_events(vm_name);
CREATE INDEX IF NOT EXISTS idx_chain_events_type ON chain_events(event_type);
CREATE INDEX IF NOT EXISTS idx_chain_events_session ON chain_events(session_id);
CREATE INDEX IF NOT EXISTS idx_chain_events_period ON chain_events(period_id);

/* Configuration change event log */
CREATE TABLE IF NOT EXISTS config_events (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id      INTEGER REFERENCES sessions(id),
    timestamp       TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    event_type      TEXT NOT NULL,
    config_source   TEXT,
    vm_name         TEXT,
    setting_name    TEXT,
    setting_value   TEXT,
    previous_value  TEXT,
    applied_to      TEXT,
    triggered_by    TEXT,
    detail          TEXT
);
CREATE INDEX IF NOT EXISTS idx_config_events_session ON config_events(session_id);
CREATE INDEX IF NOT EXISTS idx_config_events_vm ON config_events(vm_name);
CREATE INDEX IF NOT EXISTS idx_config_events_setting ON config_events(setting_name);

/* File operation audit trail */
CREATE TABLE IF NOT EXISTS file_operations (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id        INTEGER REFERENCES sessions(id),
    timestamp         TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    operation         TEXT NOT NULL,
    vm_name           TEXT,
    source_path       TEXT,
    dest_path         TEXT,
    file_type         TEXT,
    file_size_bytes   INTEGER DEFAULT 0,
    verification_data TEXT,
    reason            TEXT,
    triggered_by      TEXT,
    success           INTEGER DEFAULT 1,
    error_message     TEXT
);
CREATE INDEX IF NOT EXISTS idx_file_operations_session ON file_operations(session_id);
CREATE INDEX IF NOT EXISTS idx_file_operations_vm ON file_operations(vm_name);
CREATE INDEX IF NOT EXISTS idx_file_operations_op ON file_operations(operation);

/* Period lifecycle event log */
CREATE TABLE IF NOT EXISTS period_events (
    id                    INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id            INTEGER REFERENCES sessions(id),
    timestamp             TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    vm_name               TEXT NOT NULL,
    event_type            TEXT NOT NULL,
    period_id             TEXT,
    rotation_policy       TEXT,
    period_dir            TEXT,
    period_start          TEXT,
    period_end            TEXT,
    chains_count          INTEGER DEFAULT 0,
    total_restore_points  INTEGER DEFAULT 0,
    total_bytes           INTEGER DEFAULT 0,
    previous_period       TEXT,
    archive_location      TEXT,
    retention_remaining   INTEGER
);
CREATE INDEX IF NOT EXISTS idx_period_events_session ON period_events(session_id);
CREATE INDEX IF NOT EXISTS idx_period_events_vm ON period_events(vm_name);
CREATE INDEX IF NOT EXISTS idx_period_events_period ON period_events(period_id);

/* Retention action audit trail */
CREATE TABLE IF NOT EXISTS retention_events (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id        INTEGER REFERENCES sessions(id),
    timestamp         TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    vm_name           TEXT NOT NULL,
    action            TEXT NOT NULL,
    target_type       TEXT,
    target_path       TEXT,
    target_period     TEXT,
    rotation_policy   TEXT,
    retention_limit   INTEGER,
    current_count     INTEGER,
    age_days          INTEGER,
    freed_bytes       INTEGER DEFAULT 0,
    preserve_reason   TEXT,
    triggered_by      TEXT,
    success           INTEGER DEFAULT 1,
    action_source     TEXT
);
CREATE INDEX IF NOT EXISTS idx_retention_events_session ON retention_events(session_id);
CREATE INDEX IF NOT EXISTS idx_retention_events_vm ON retention_events(vm_name);
CREATE INDEX IF NOT EXISTS idx_retention_events_action ON retention_events(action);

/* Restore sessions (one row per vmrestore.sh invocation) — UNI-902b */
CREATE TABLE IF NOT EXISTS restore_sessions (
    id                   INTEGER PRIMARY KEY,
    vm_name              TEXT NOT NULL,
    restore_mode         TEXT,
    dry_run              INTEGER DEFAULT 0,
    target_path          TEXT,
    source_backup_path   TEXT,
    source_period_id     TEXT,
    source_checkpoint    TEXT,
    start_time           TEXT NOT NULL,
    end_time             TEXT,
    duration_sec         INTEGER,
    status               TEXT DEFAULT 'running',
    exit_code            INTEGER,
    error_message        TEXT,
    disks_restored       INTEGER DEFAULT 0,
    bytes_written        INTEGER DEFAULT 0,
    tpm_restored         INTEGER DEFAULT 0,
    identity_redefined   INTEGER DEFAULT 0,
    log_file             TEXT,
    created_at           TEXT DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_restore_sessions_vm ON restore_sessions(vm_name);
CREATE INDEX IF NOT EXISTS idx_restore_sessions_status ON restore_sessions(status);
CREATE INDEX IF NOT EXISTS idx_restore_sessions_start ON restore_sessions(start_time);

SCHEMA_EOF
    return $?
}

#=============================================================================
# SESSION MANAGEMENT
#=============================================================================

# Start a new backup session
# Arguments:
#   $1 - instance name (e.g., 'test', 'default')
#   $2 - log file path
# Sets: SQLITE_CURRENT_SESSION_ID
# Returns: 0 on success, 1 on failure
sqlite_session_start() {
    local instance="${1:-default}"
    local log_file="${2:-}"
    local session_type="${3:-standard}"
    
    if [[ "$SQLITE_MODULE_AVAILABLE" -ne 1 ]]; then
        return 1
    fi
    
    local session_id=$(date +%s)
    # Store time in UTC for consistent duration calculations
    local start_time=$(date -u '+%Y-%m-%d %H:%M:%S')
    
    local sql="INSERT INTO sessions (id, instance, start_time, log_file, status, session_type) 
               VALUES ($session_id, '${instance//\'/\'\'}', '$start_time', '${log_file//\'/\'\'}', 'running', '${session_type//\'/\'\'}');"
    
    if ! _sq3 "$SQLITE_DB_PATH" "$sql" 2>/dev/null; then
        log_error "$SQLITE_MODULE_NAME" "sqlite_session_start" \
            "Failed to create session record"
        return 1
    fi
    
    SQLITE_CURRENT_SESSION_ID="$session_id"
    log_debug "$SQLITE_MODULE_NAME" "sqlite_session_start" \
        "Session started: id=$session_id instance=$instance"
    return 0
}

# End the current session with final statistics
# Arguments:
#   $1 - vms_total
#   $2 - vms_success
#   $3 - vms_failed
#   $4 - vms_skipped
#   $5 - vms_excluded
#   $6 - bytes_total
#   $7 - final_status ('success', 'partial', 'failed')
# Returns: 0 on success, 1 on failure
sqlite_session_end() {
    local vms_total="${1:-0}"
    local vms_success="${2:-0}"
    local vms_failed="${3:-0}"
    local vms_skipped="${4:-0}"
    local vms_excluded="${5:-0}"
    local bytes_total="${6:-0}"
    local final_status="${7:-unknown}"
    
    # Guard against double-call (SIGTERM handler → exit → EXIT trap).
    # Return 2 (sentinel) so callers can distinguish "idempotent no-op" from
    # "finalized this call" (rc=0) and "failed" (rc=1) — see INT-13.
    if [[ "${_SQLITE_SESSION_ENDED:-0}" == "1" ]]; then
        log_debug "$SQLITE_MODULE_NAME" "sqlite_session_end" \
            "Session already ended (duplicate call suppressed)"
        return 2
    fi
    
    if [[ "$SQLITE_MODULE_AVAILABLE" -ne 1 ]] || [[ -z "$SQLITE_CURRENT_SESSION_ID" ]]; then
        return 1
    fi
    
    # Store time in UTC for consistency
    local end_time=$(date -u '+%Y-%m-%d %H:%M:%S')
    # Session ID is the epoch timestamp from session start - use it directly
    local start_epoch="$SQLITE_CURRENT_SESSION_ID"
    local end_epoch=$(date +%s)
    local duration_sec=$((end_epoch - start_epoch))
    
    # Calculate actual bytes from vm_backups table (more reliable than parsed human format)
    local actual_bytes
    actual_bytes=$(_sq3 "$SQLITE_DB_PATH" \
        "SELECT COALESCE(SUM(bytes_written), 0) FROM vm_backups WHERE session_id=$SQLITE_CURRENT_SESSION_ID;" 2>/dev/null)
    if [[ -n "$actual_bytes" ]] && [[ "$actual_bytes" =~ ^[0-9]+$ ]] && [[ "$actual_bytes" -gt 0 ]]; then
        bytes_total="$actual_bytes"
    fi

    # Snapshot BACKUP_PATH disk usage (schema v2.1+).
    # Failures default cleanly to 0; the storage report (Change C) filters
    # WHERE disk_total_bytes > 0 so unmounted/missing paths are excluded
    # from growth math rather than treated as zero-size disks.
    local _disk_free_bytes=0 _disk_total_bytes=0
    if [[ -n "${BACKUP_PATH:-}" ]] && [[ -d "$BACKUP_PATH" ]]; then
        local _df_out
        _df_out=$(df -PB1 "$BACKUP_PATH" 2>/dev/null | awk 'NR==2 {print $2 " " $4}')
        if [[ -n "$_df_out" ]]; then
            read -r _disk_total_bytes _disk_free_bytes <<< "$_df_out"
            # Defensive: ensure numeric (df can emit "-" on exotic mounts)
            [[ "$_disk_total_bytes" =~ ^[0-9]+$ ]] || _disk_total_bytes=0
            [[ "$_disk_free_bytes"  =~ ^[0-9]+$ ]] || _disk_free_bytes=0
        fi
    fi

    local sql="UPDATE sessions SET
        end_time = '$end_time',
        duration_sec = $duration_sec,
        vms_total = $vms_total,
        vms_success = $vms_success,
        vms_failed = $vms_failed,
        vms_skipped = $vms_skipped,
        vms_excluded = $vms_excluded,
        bytes_total = $bytes_total,
        status = '$final_status',
        disk_free_bytes = $_disk_free_bytes,
        disk_total_bytes = $_disk_total_bytes
        WHERE id = $SQLITE_CURRENT_SESSION_ID;"
    
    if ! _sq3 "$SQLITE_DB_PATH" "$sql" 2>/dev/null; then
        log_error "$SQLITE_MODULE_NAME" "sqlite_session_end" \
            "Failed to update session record"
        return 1
    fi
    
    log_debug "$SQLITE_MODULE_NAME" "sqlite_session_end" \
        "Session ended: id=$SQLITE_CURRENT_SESSION_ID status=$final_status duration=${duration_sec}s"
    
    # Mark session as ended to prevent double-call via signal handler chain
    _SQLITE_SESSION_ENDED=1
    
    return 0
}

# Get current session ID
# Returns: session_id on stdout, empty if no active session
sqlite_get_session_id() {
    echo "$SQLITE_CURRENT_SESSION_ID"
}

#=============================================================================
# RESTORE SESSION MANAGEMENT (UNI-902b)
#=============================================================================

# Start a new restore session row.
# Arguments (positional):
#   $1 - vm_name
#   $2 - restore_mode (e.g., 'full', 'in-place', 'new-vm')
#   $3 - dry_run (0/1)
#   $4 - target_path
#   $5 - source_backup_path
#   $6 - source_period_id
#   $7 - source_checkpoint
# Stdout: integer rowid of the new row (on success)
# Returns: 0 on success, 1 on failure (no row written, no stdout)
# Side effects: NONE (sets no globals; caller stores rowid into _RESTORE_SESSION_ID)
sqlite_restore_session_start() {
    local vm_name="${1:-}"
    local restore_mode="${2:-}"
    local dry_run="${3:-0}"
    local target_path="${4:-}"
    local source_backup_path="${5:-}"
    local source_period_id="${6:-}"
    local source_checkpoint="${7:-}"

    if [[ "$SQLITE_MODULE_AVAILABLE" -ne 1 ]]; then
        return 1
    fi

    # Defensive: dry_run must be 0 or 1
    [[ "$dry_run" =~ ^[01]$ ]] || dry_run=0

    # Store time in UTC for consistent duration calculations
    local start_time
    start_time=$(date -u '+%Y-%m-%d %H:%M:%S')

    local sql="INSERT INTO restore_sessions (
        vm_name, restore_mode, dry_run, target_path,
        source_backup_path, source_period_id, source_checkpoint,
        start_time, status
    ) VALUES (
        '$(_sql_escape "$vm_name")',
        '$(_sql_escape "$restore_mode")',
        $dry_run,
        '$(_sql_escape "$target_path")',
        '$(_sql_escape "$source_backup_path")',
        '$(_sql_escape "$source_period_id")',
        '$(_sql_escape "$source_checkpoint")',
        '$start_time',
        'running'
    );
    SELECT last_insert_rowid();"

    local rowid
    rowid=$(_sq3 "$SQLITE_DB_PATH" "$sql" 2>/dev/null)
    if [[ -z "$rowid" ]] || ! [[ "$rowid" =~ ^[0-9]+$ ]]; then
        log_error "$SQLITE_MODULE_NAME" "sqlite_restore_session_start" \
            "Failed to create restore_sessions row for vm=$vm_name"
        return 1
    fi

    log_debug "$SQLITE_MODULE_NAME" "sqlite_restore_session_start" \
        "Restore session started: id=$rowid vm=$vm_name mode=$restore_mode dry_run=$dry_run"
    echo "$rowid"
    return 0
}

# End an in-flight restore session row (UPDATE only).
# Idempotent via _SQLITE_RESTORE_SESSION_ENDED flag (V-9): a second invocation
# in the same shell is a no-op so the EXIT trap and an explicit caller cannot
# clobber the first outcome.
# Arguments (positional):
#   $1 - session_id (rowid returned by sqlite_restore_session_start)
#   $2 - final_status ('success' | 'failed' | 'interrupted' | 'running')
#   $3 - exit_code
#   $4 - error_message (optional)
#   $5 - disks_restored (optional, default 0)
#   $6 - bytes_written (optional, default 0)
#   $7 - tpm_restored (optional, 0/1, default 0)
#   $8 - identity_redefined (optional, 0/1, default 0)
# Returns: 0 on success / no-op, 1 on failure
sqlite_restore_session_end() {
    local session_id="${1:-}"
    local final_status="${2:-unknown}"
    local exit_code="${3:-0}"
    local error_message="${4:-}"
    local disks_restored="${5:-0}"
    local bytes_written="${6:-0}"
    local tpm_restored="${7:-0}"
    local identity_redefined="${8:-0}"

    # Idempotency guard (V-9): suppress duplicate calls (EXIT trap chain).
    # Return 2 (sentinel) so callers can distinguish "idempotent no-op" from
    # "finalized this call" (rc=0) and "failed" (rc=1) — see INT-13.
    if [[ "${_SQLITE_RESTORE_SESSION_ENDED:-0}" == "1" ]]; then
        log_debug "$SQLITE_MODULE_NAME" "sqlite_restore_session_end" \
            "Restore session already ended (duplicate call suppressed)"
        return 2
    fi

    if [[ "$SQLITE_MODULE_AVAILABLE" -ne 1 ]]; then
        return 1
    fi

    # Empty session_id → start was skipped (read-only / dry-run / DB unavailable).
    # End is a no-op but still mark the flag so we don't loop on retries.
    if [[ -z "$session_id" ]]; then
        _SQLITE_RESTORE_SESSION_ENDED=1
        return 0
    fi

    # Defensive: numeric only on numeric fields
    [[ "$session_id" =~ ^[0-9]+$ ]] || { log_error "$SQLITE_MODULE_NAME" "sqlite_restore_session_end" "Non-numeric session_id: $session_id"; return 1; }
    [[ "$exit_code"  =~ ^-?[0-9]+$ ]] || exit_code=0
    [[ "$disks_restored"     =~ ^[0-9]+$ ]] || disks_restored=0
    [[ "$bytes_written"      =~ ^[0-9]+$ ]] || bytes_written=0
    [[ "$tpm_restored"       =~ ^[01]$  ]] || tpm_restored=0
    [[ "$identity_redefined" =~ ^[01]$  ]] || identity_redefined=0

    local end_time
    end_time=$(date -u '+%Y-%m-%d %H:%M:%S')

    # duration_sec = end_epoch - start_epoch (start_time stored as UTC text; recompute via SQL strftime)
    local sql="UPDATE restore_sessions SET
        end_time = '$end_time',
        duration_sec = CAST((strftime('%s', '$end_time') - strftime('%s', start_time)) AS INTEGER),
        status = '$(_sql_escape "$final_status")',
        exit_code = $exit_code,
        error_message = '$(_sql_escape "$error_message")',
        disks_restored = $disks_restored,
        bytes_written = $bytes_written,
        tpm_restored = $tpm_restored,
        identity_redefined = $identity_redefined
        WHERE id = $session_id;"

    if ! _sq3 "$SQLITE_DB_PATH" "$sql" 2>/dev/null; then
        log_error "$SQLITE_MODULE_NAME" "sqlite_restore_session_end" \
            "Failed to update restore_sessions row id=$session_id"
        return 1
    fi

    log_debug "$SQLITE_MODULE_NAME" "sqlite_restore_session_end" \
        "Restore session ended: id=$session_id status=$final_status exit_code=$exit_code"

    _SQLITE_RESTORE_SESSION_ENDED=1
    return 0
}

#=============================================================================
# VM BACKUP LOGGING
#=============================================================================

# Log a VM backup result
# Arguments (positional):
#   $1  - vm_name
#   $2  - vm_status (running/shut off/paused)
#   $3  - os_type
#   $4  - backup_type (full/auto/copy/none/n/a)
#   $5  - backup_method (agent/paused/offline/excluded)
#   $6  - rotation_policy (daily/weekly/monthly/accumulate/never)
#   $7  - status (success/failed/skipped/excluded)
#   $8  - bytes_written
#   $9  - chain_size_bytes
#   $10 - total_dir_bytes
#   $11 - restore_points
#   $12 - duration_sec
#   $13 - backup_path
#   $14 - log_file
#   $15 - error_code
#   $16 - error_message
#   $17 - qemu_agent (0/1)
#   $18 - vm_paused (0/1)
#   $19 - chain_archived (0/1)
#   $20 - restore_points_before (optional, default 0)
#   $21 - retry_attempt (optional, default 0)
#   $22 - archived_restore_points (optional, default 0)
#   $23 - event_type (optional)
#   $24 - event_detail (optional)
# Returns: 0 on success, 1 on failure
sqlite_log_vm_backup() {
    local vm_name="$1"
    local vm_status="$2"
    local os_type="$3"
    local backup_type="$4"
    local backup_method="$5"
    local rotation_policy="$6"
    local status="$7"
    local bytes_written="${8:-0}"
    local chain_size_bytes="${9:-0}"
    local total_dir_bytes="${10:-0}"
    local restore_points="${11:-0}"
    local duration_sec="${12:-0}"
    local backup_path="${13:-}"
    local log_file="${14:-}"
    local error_code="${15:-}"
    local error_message="${16:-}"
    local qemu_agent="${17:-0}"
    local vm_paused="${18:-0}"
    local chain_archived="${19:-0}"
    local restore_points_before="${20:-0}"
    local retry_attempt="${21:-0}"
    local archived_restore_points="${22:-0}"
    local event_type="${23:-}"
    local event_detail="${24:-}"
    
    if [[ "$SQLITE_MODULE_AVAILABLE" -ne 1 ]] || [[ -z "$SQLITE_CURRENT_SESSION_ID" ]]; then
        return 1
    fi
    
    # Build SQL with escaped values
    local sql="INSERT INTO vm_backups (
        session_id, vm_name, vm_status, os_type, backup_type, backup_method,
        rotation_policy, status, bytes_written, chain_size_bytes, total_dir_bytes,
        restore_points, restore_points_before, duration_sec, backup_path, log_file,
        error_code, error_message, event_type, event_detail, retry_attempt,
        archived_restore_points, qemu_agent, vm_paused, chain_archived
    ) VALUES (
        $SQLITE_CURRENT_SESSION_ID, 
        '$(_sql_escape "$vm_name")', 
        '$(_sql_escape "$vm_status")', 
        '$(_sql_escape "$os_type")',
        '$(_sql_escape "$backup_type")', 
        '$(_sql_escape "$backup_method")', 
        '$(_sql_escape "$rotation_policy")', 
        '$(_sql_escape "$status")',
        $bytes_written, $chain_size_bytes, $total_dir_bytes, $restore_points,
        $restore_points_before,
        $duration_sec, 
        '$(_sql_escape "$backup_path")', 
        '$(_sql_escape "$log_file")', 
        '$(_sql_escape "$error_code")',
        '$(_sql_escape "$error_message")', 
        '$(_sql_escape "$event_type")',
        '$(_sql_escape "$event_detail")',
        $retry_attempt,
        $archived_restore_points,
        $qemu_agent, $vm_paused, $chain_archived
    );"
    
    if ! _sql_exec "$sql"; then
        log_error "$SQLITE_MODULE_NAME" "sqlite_log_vm_backup" \
            "Failed to insert VM backup record for: $vm_name"
        return 1
    fi
    
    log_debug "$SQLITE_MODULE_NAME" "sqlite_log_vm_backup" \
        "VM backup logged: $vm_name status=$status type=$backup_type"
    return 0
}

#=============================================================================
# REPLICATION LOGGING
#=============================================================================

# Log a replication run
# Arguments:
#   $1  - endpoint_name
#   $2  - endpoint_type (local/cloud)
#   $3  - transport (local/ssh/smb/sharepoint/backblaze)
#   $4  - sync_mode (mirror/accumulate)
#   $5  - destination
#   $6  - start_time (ISO8601)
#   $7  - end_time (ISO8601)
#   $8  - duration_sec
#   $9  - bytes_transferred
#   $10 - files_transferred
#   $11 - status (success/failed/skipped/disabled)
#   $12 - error_message
#   $13 - log_file
#   $14 - dest_avail_bytes (optional)
#   $15 - dest_total_bytes (optional)
#   $16 - dest_space_known (optional, 0/1)
#   $17 - throttle_count (optional, cloud only)
#   $18 - bwlimit_final (optional, cloud only)
# Returns: run_id on stdout, empty on failure
sqlite_log_replication_run() {
    is_dry_run && return 0
    local endpoint_name="$1"
    local endpoint_type="$2"
    local transport="$3"
    local sync_mode="$4"
    local destination="$5"
    local start_time="$6"
    local end_time="$7"
    local duration_sec="${8:-0}"
    local bytes_transferred="${9:-0}"
    local files_transferred="${10:-0}"
    local status="${11:-unknown}"
    local error_message="${12:-}"
    local log_file="${13:-}"
    local dest_avail_bytes="${14:-0}"
    local dest_total_bytes="${15:-0}"
    local dest_space_known="${16:-0}"
    local throttle_count="${17:-0}"
    local bwlimit_final="${18:-}"
    
    if [[ "$SQLITE_MODULE_AVAILABLE" -ne 1 ]] || [[ -z "$SQLITE_CURRENT_SESSION_ID" ]]; then
        return 1
    fi
    
    # Combine INSERT and SELECT last_insert_rowid() in single connection
    # to get the correct row ID
    local run_id
    run_id=$(_sq3 "$SQLITE_DB_PATH" 2>/dev/null << REPL_SQL
INSERT INTO replication_runs (
    session_id, endpoint_name, endpoint_type, transport, sync_mode,
    destination, start_time, end_time, duration_sec, bytes_transferred,
    files_transferred, status, error_message, log_file,
    dest_avail_bytes, dest_total_bytes, dest_space_known,
    throttle_count, bwlimit_final
) VALUES (
    $SQLITE_CURRENT_SESSION_ID, 
    '$(_sql_escape "$endpoint_name")', 
    '$(_sql_escape "$endpoint_type")',
    '$(_sql_escape "$transport")', 
    '$(_sql_escape "$sync_mode")', 
    '$(_sql_escape "$destination")', 
    '$(_sql_escape "$start_time")', 
    '$(_sql_escape "$end_time")',
    $duration_sec, $bytes_transferred, $files_transferred, 
    '$(_sql_escape "$status")',
    '$(_sql_escape "$error_message")', 
    '$(_sql_escape "$log_file")', 
    $dest_avail_bytes, $dest_total_bytes,
    $dest_space_known, $throttle_count, 
    '$(_sql_escape "$bwlimit_final")'
);
SELECT last_insert_rowid();
REPL_SQL
)
    
    if [[ -z "$run_id" ]] || [[ "$run_id" == "0" ]]; then
        log_error "$SQLITE_MODULE_NAME" "sqlite_log_replication_run" \
            "Failed to insert replication run for: $endpoint_name"
        return 1
    fi
    
    log_debug "$SQLITE_MODULE_NAME" "sqlite_log_replication_run" \
        "Replication run logged: $endpoint_name status=$status run_id=$run_id"
    echo "$run_id"
    return 0
}

# Associate a VM with a replication run
# Arguments:
#   $1 - run_id (from sqlite_log_replication_run)
#   $2 - vm_name
# Returns: 0 on success, 1 on failure
sqlite_log_replication_vm() {
    local run_id="$1"
    local vm_name="$2"
    
    if [[ "$SQLITE_MODULE_AVAILABLE" -ne 1 ]] || [[ -z "$run_id" ]]; then
        return 1
    fi
    
    local sql="INSERT OR IGNORE INTO replication_vms (run_id, vm_name) 
               VALUES ($run_id, '$(_sql_escape "$vm_name")');"
    
    if ! _sql_exec "$sql"; then
        log_error "$SQLITE_MODULE_NAME" "sqlite_log_replication_vm" \
            "Failed to associate VM $vm_name with run $run_id"
        return 1
    fi
    
    return 0
}

# Log multiple VMs for a replication run (batch operation)
# Arguments:
#   $1 - run_id
#   $2 - run_status (e.g. success, skipped, failed)
#   $@ - vm names (remaining arguments)
# Returns: 0 on success, 1 on failure
sqlite_log_replication_vms() {
    is_dry_run && return 0
    local run_id="$1"
    shift
    local run_status="$1"
    shift
    local vm_names=("$@")
    
    if [[ "$SQLITE_MODULE_AVAILABLE" -ne 1 ]] || [[ -z "$run_id" ]]; then
        return 1
    fi
    
    local sql="BEGIN TRANSACTION;"
    for vm_name in "${vm_names[@]}"; do
        sql+="INSERT OR IGNORE INTO replication_vms (run_id, vm_name, status) VALUES ($run_id, '$(_sql_escape "$vm_name")', '$(_sql_escape "$run_status")');"
    done
    sql+="COMMIT;"
    
    if ! _sql_exec "$sql"; then
        log_error "$SQLITE_MODULE_NAME" "sqlite_log_replication_vms" \
            "Failed to batch insert VMs for run $run_id"
        return 1
    fi
    
    log_debug "$SQLITE_MODULE_NAME" "sqlite_log_replication_vms" \
        "Logged ${#vm_names[@]} VMs for replication run $run_id"
    return 0
}

#=============================================================================
# QUERY FUNCTIONS
#=============================================================================


# sqlite_query_stats() — REMOVED (dead code, H1 datetime bug)
# Was never called in production. See DATETIME_BUGS.md H1.

#=============================================================================
# STATUS REPORT QUERY FUNCTIONS (ENH-10)
#
# Query functions for the --status reporting command.
# Return pipe-delimited or CSV output depending on output_mode parameter.
#=============================================================================


#=============================================================================
# EMAIL REPORT QUERY FUNCTIONS
#
# Session-scoped queries for the email report module.
# All queries accept a session_id parameter (defaults to current session)
# and return pipe-delimited rows for reliable shell parsing.
#=============================================================================


#=============================================================================
# UTILITY FUNCTIONS
#=============================================================================


#=============================================================================
# CHAIN HEALTH MANAGEMENT (G2/G6/G7)
#=============================================================================

# Update or insert chain health record
# Arguments:
#   $1 - vm_name
#   $2 - period_id
#   $3 - chain_location (backup directory path)
#   $4 - chain_status (active|archived|broken|deleted)
#   $5 - checkpoint_count (total checkpoints in chain)
#   $6 - error_type (optional: backup_failed|interrupted|chain_broken)
#   $7 - error_message (optional)
#   $8 - rotation_policy (optional: daily|weekly|monthly)
# Returns: 0 on success, 1 on failure
sqlite_update_chain_health() {
    is_dry_run && return 0
    local vm_name="$1"
    local period_id="$2"
    local chain_location="$3"
    local chain_status="${4:-active}"
    local checkpoint_count="${5:-0}"
    local error_type="${6:-}"
    local error_message="${7:-}"
    local rotation_policy="${8:-}"
    
    [[ "$SQLITE_MODULE_AVAILABLE" -ne 1 ]] && return 1
    
    local esc_vm=$(_sql_escape "$vm_name")
    local esc_period=$(_sql_escape "$period_id")
    local esc_location=$(_sql_escape "$chain_location")
    local esc_error=$(_sql_escape "$error_message")
    local esc_error_type=$(_sql_escape "$error_type")
    local now=$(date -u '+%Y-%m-%d %H:%M:%S')
    
    # Determine restorable count and broken_at based on status
    local restorable_count="$checkpoint_count"
    local broken_at="NULL"
    local break_reason="NULL"
    
    [[ -n "$error_type" ]] && break_reason="'$esc_error_type: $esc_error'"
    
    if [[ "$chain_status" == "broken" ]]; then
        broken_at="$checkpoint_count"
        restorable_count=$((checkpoint_count > 0 ? checkpoint_count : 0))
    fi
    
    # Handle rotation_policy - if not provided, try to infer from period_id
    local esc_policy="NULL"
    if [[ -n "$rotation_policy" ]]; then
        esc_policy="'$(_sql_escape "$rotation_policy")'"
    elif [[ "$period_id" =~ ^[0-9]{4}-W[0-9]{2}$ ]]; then
        esc_policy="'weekly'"
    elif [[ "$period_id" =~ ^[0-9]{8}$ ]]; then
        esc_policy="'daily'"
    elif [[ "$period_id" =~ ^[0-9]{6}$ ]]; then
        esc_policy="'monthly'"
    fi
    
    _sq3 "$SQLITE_DB_PATH" << SQL_EOF
INSERT INTO chain_health (
    vm_name, period_id, chain_location, chain_status, rotation_policy,
    total_checkpoints, restorable_count, broken_at, 
    break_reason, first_backup, last_backup, updated_at
) VALUES (
    '$esc_vm', '$esc_period', '$esc_location', '$chain_status', $esc_policy,
    $checkpoint_count, $restorable_count, $broken_at,
    $break_reason, '$now', '$now', '$now'
)
ON CONFLICT(vm_name, period_id) DO UPDATE SET
    chain_location = '$esc_location',
    chain_status = '$chain_status',
    rotation_policy = COALESCE($esc_policy, rotation_policy),
    total_checkpoints = $checkpoint_count,
    restorable_count = $restorable_count,
    broken_at = $broken_at,
    break_reason = $break_reason,
    last_backup = '$now',
    updated_at = '$now',
    purged_at = CASE WHEN '$chain_status' IN ('active','broken') THEN NULL ELSE purged_at END;
SQL_EOF
    local rc=$?
    if [[ $rc -ne 0 ]]; then
        log_error "$SQLITE_MODULE_NAME" "sqlite_update_chain_health" "Failed to upsert chain_health: vm=$vm_name period=$period_id status=$chain_status (exit=$rc)"
    else
        log_debug "$SQLITE_MODULE_NAME" "sqlite_update_chain_health" "Upserted: vm=$vm_name period=$period_id status=$chain_status checkpoints=$checkpoint_count"
    fi
    return $rc
}

# Mark chain as archived (G6)
# Arguments:
#   $1 - vm_name
#   $2 - period_id
#   $3 - chain_location (backup directory path)
#   $4 - archive_path (where the archive was stored)
#   $5 - total_size (optional, bytes)
# Returns: 0 on success, 1 on failure
sqlite_archive_chain() {
    is_dry_run && return 0
    local vm_name="$1" period_id="$2" chain_location="$3"
    local archive_path="${4:-}" total_size="${5:-0}"
    
    [[ "$SQLITE_MODULE_AVAILABLE" -ne 1 ]] && return 1
    
    local esc_vm=$(_sql_escape "$vm_name")
    local esc_period=$(_sql_escape "$period_id")
    local esc_location=$(_sql_escape "$chain_location")
    local esc_archive=$(_sql_escape "$archive_path")
    local now=$(date -u '+%Y-%m-%d %H:%M:%S')
    
    # Use archive_path as the new chain_location if provided, else keep original
    local final_location="$esc_location"
    [[ -n "$archive_path" ]] && final_location="$esc_archive"
    
    local _changed
    _changed=$(_sq3 "$SQLITE_DB_PATH" << SQL_EOF
UPDATE chain_health SET
    chain_status = 'archived',
    chain_location = '$final_location',
    archive_size_bytes = $total_size,
    archived_at = '$now',
    updated_at = '$now'
WHERE vm_name = '$esc_vm' 
  AND period_id = '$esc_period';
SELECT changes();
SQL_EOF
)
    local rc=$?
    if [[ $rc -ne 0 ]]; then
        log_error "$SQLITE_MODULE_NAME" "sqlite_archive_chain" "Failed to archive chain: vm=$vm_name period=$period_id (exit=$rc)"
        return $rc
    fi
    # Fail closed: a zero-row UPDATE means no chain_health row exists for this
    # (vm, period), so the archive was NOT recorded — do not report success.
    if [[ "${_changed:-0}" -eq 0 ]]; then
        log_error "$SQLITE_MODULE_NAME" "sqlite_archive_chain" "No chain_health row matched vm=$vm_name period=$period_id; archive not recorded"
        return 1
    fi
    log_debug "$SQLITE_MODULE_NAME" "sqlite_archive_chain" "Archived: vm=$vm_name period=$period_id location=$final_location"
    return 0
}

# Mark chain as broken (interrupted backup)
# Arguments:
#   $1 - vm_name
#   $2 - period_id
#   $3 - chain_location (backup directory path)
#   $4 - broken_at_checkpoint
#   $5 - error_message
# Returns: 0 on success, 1 on failure
sqlite_mark_chain_broken() {
    is_dry_run && return 0
    local vm_name="$1" period_id="$2" chain_location="$3"
    local broken_at="${4:-0}" error_message="${5:-interrupted}"
    
    [[ "$SQLITE_MODULE_AVAILABLE" -ne 1 ]] && return 1
    
    local esc_vm=$(_sql_escape "$vm_name")
    local esc_period=$(_sql_escape "$period_id")
    local esc_location=$(_sql_escape "$chain_location")
    local esc_error=$(_sql_escape "$error_message")
    local now=$(date -u '+%Y-%m-%d %H:%M:%S')
    local restorable=$((broken_at > 0 ? broken_at : 0))
    
    _sq3 "$SQLITE_DB_PATH" << SQL_EOF
INSERT INTO chain_health (
    vm_name, period_id, chain_location, chain_status,
    total_checkpoints, restorable_count, broken_at, break_reason,
    first_backup, updated_at
) VALUES (
    '$esc_vm', '$esc_period', '$esc_location', 'broken',
    $broken_at, $restorable, $broken_at, '$esc_error',
    '$now', '$now'
)
ON CONFLICT(vm_name, period_id) DO UPDATE SET
    chain_location = '$esc_location',
    chain_status = 'broken',
    broken_at = $broken_at,
    restorable_count = $restorable,
    break_reason = '$esc_error',
    updated_at = '$now';
SQL_EOF
    local rc=$?
    if [[ $rc -ne 0 ]]; then
        log_error "$SQLITE_MODULE_NAME" "sqlite_mark_chain_broken" "Failed to mark chain broken: vm=$vm_name period=$period_id (exit=$rc)"
    else
        log_debug "$SQLITE_MODULE_NAME" "sqlite_mark_chain_broken" "Marked broken: vm=$vm_name period=$period_id at_checkpoint=$broken_at"
    fi
    return $rc
}

# Mark chain as deleted (retention cleanup) (G4/G5)
# Arguments:
#   $1 - vm_name
#   $2 - period_id
#   $3 - chain_location (use '.' for legacy/unknown)
#   $4 - reason (retention|prune|space_cleanup)
#   $5 - target_status (deleted|purged, default: deleted)
#        'deleted' = retention removed this chain
#        'purged'  = operator removed via --prune
# Returns: 0 on success, 1 on failure
sqlite_mark_chain_deleted() {
    is_dry_run && return 0
    local vm_name="$1" period_id="$2"
    local chain_location="${3:-.}" reason="${4:-retention}"
    local target_status="${5:-deleted}"
    
    [[ "$SQLITE_MODULE_AVAILABLE" -ne 1 ]] && return 1
    
    # Validate target_status
    case "$target_status" in
        deleted|purged) ;;
        *) target_status="deleted" ;;
    esac
    
    local esc_vm=$(_sql_escape "$vm_name")
    local esc_period=$(_sql_escape "$period_id")
    local esc_location=$(_sql_escape "$chain_location")
    local esc_reason=$(_sql_escape "$reason")
    local esc_status=$(_sql_escape "$target_status")
    local now=$(date -u '+%Y-%m-%d %H:%M:%S')
    
    # Set the appropriate timestamp column based on target status
    local ts_col="deleted_at"
    [[ "$target_status" == "purged" ]] && ts_col="purged_at"
    
    _sq3 "$SQLITE_DB_PATH" << SQL_EOF
INSERT INTO chain_health (vm_name, period_id, chain_location, chain_status, created_at, updated_at)
VALUES ('$esc_vm', '$esc_period', '$esc_location', 'active', '$now', '$now')
ON CONFLICT(vm_name, period_id) DO NOTHING;

UPDATE chain_health SET
    chain_status = '$esc_status', restorable_count = 0,
    break_reason = '$esc_reason', ${ts_col} = '$now',
    marked_by = '$esc_reason', updated_at = '$now'
WHERE vm_name = '$esc_vm' AND period_id = '$esc_period'
  AND chain_status NOT IN ('deleted', 'purged');
SQL_EOF
    local rc=$?
    if [[ $rc -ne 0 ]]; then
        log_error "$SQLITE_MODULE_NAME" "sqlite_mark_chain_deleted" "Failed to mark chain $target_status: vm=$vm_name period=$period_id (exit=$rc)"
    else
        log_debug "$SQLITE_MODULE_NAME" "sqlite_mark_chain_deleted" "Marked $target_status: vm=$vm_name period=$period_id reason=$reason"
    fi
    return $rc
}

# Mark chain as deleted ONLY IF a row already exists (UPDATE-only variant)
# Used for stub-period removal where we must NOT inject a phantom
# 'active'-then-'deleted' row for VMs that never had a real chain.
# Signature parity with sqlite_mark_chain_deleted; chain_location ($3)
# is unused but kept for caller convenience.
# Arguments:
#   $1 - vm_name
#   $2 - period_id
#   $3 - chain_location (unused, kept for signature parity)
#   $4 - reason (retention|prune|space_cleanup|skipped|excluded)
#   $5 - target_status (deleted|purged, default: deleted)
# Returns: 0 on success (including no-row no-op), 1 on failure
sqlite_mark_chain_deleted_if_exists() {
    is_dry_run && return 0
    local vm_name="$1" period_id="$2"
    local _chain_location="${3:-.}"   # accepted for parity, unused
    local reason="${4:-retention}"
    local target_status="${5:-deleted}"

    [[ "$SQLITE_MODULE_AVAILABLE" -ne 1 ]] && return 1

    case "$target_status" in
        deleted|purged) ;;
        *) target_status="deleted" ;;
    esac

    local esc_vm=$(_sql_escape "$vm_name")
    local esc_period=$(_sql_escape "$period_id")
    local esc_reason=$(_sql_escape "$reason")
    local esc_status=$(_sql_escape "$target_status")
    local now=$(date -u '+%Y-%m-%d %H:%M:%S')

    local ts_col="deleted_at"
    [[ "$target_status" == "purged" ]] && ts_col="purged_at"

    _sq3 "$SQLITE_DB_PATH" << SQL_EOF
UPDATE chain_health SET
    chain_status = '$esc_status', restorable_count = 0,
    break_reason = '$esc_reason', ${ts_col} = '$now',
    marked_by = '$esc_reason', updated_at = '$now'
WHERE vm_name = '$esc_vm' AND period_id = '$esc_period'
  AND chain_status NOT IN ('deleted', 'purged');
SQL_EOF
    local rc=$?
    if [[ $rc -ne 0 ]]; then
        log_error "$SQLITE_MODULE_NAME" "sqlite_mark_chain_deleted_if_exists" \
            "Failed UPDATE: vm=$vm_name period=$period_id (exit=$rc)"
    else
        log_debug "$SQLITE_MODULE_NAME" "sqlite_mark_chain_deleted_if_exists" \
            "UPDATE applied (or no-op): vm=$vm_name period=$period_id reason=$reason"
    fi
    return $rc
}

# Phase 4 commit 4b: typed write helper for retention reconcile pass 1.
# Marks an active|archived|broken|marked chain row as deleted with the
# canonical reconcile_phantom break_reason. Distinct from
# sqlite_mark_chain_deleted_if_exists: that variant excludes only
# deleted+purged and may target purged via $5; this variant restricts to
# the four "live" states reconcile is allowed to mutate, hardcodes the
# break_reason / marked_by, and is not parameterised on target_status.
# Arguments:
#   $1 - vm_name
#   $2 - period_id
# Returns: 0 on success, 1 on failure
sqlite_mark_phantom_chain() {
    is_dry_run && return 0
    local vm_name="$1" period_id="$2"

    [[ "$SQLITE_MODULE_AVAILABLE" -ne 1 ]] && return 1
    [[ -z "$vm_name" || -z "$period_id" ]] && return 1

    local esc_vm esc_p now
    esc_vm=$(_sql_escape "$vm_name")
    esc_p=$(_sql_escape "$period_id")
    now=$(date -u '+%Y-%m-%d %H:%M:%S')

    _sq3 "$SQLITE_DB_PATH" << SQL_EOF
UPDATE chain_health SET
    chain_status='deleted', restorable_count=0,
    break_reason='reconcile_phantom', deleted_at='$now',
    marked_by='reconcile', updated_at='$now'
WHERE vm_name='$esc_vm' AND period_id='$esc_p'
  AND chain_status IN ('active', 'archived', 'broken', 'marked');
SQL_EOF
    local rc=$?
    if [[ $rc -ne 0 ]]; then
        log_error "$SQLITE_MODULE_NAME" "sqlite_mark_phantom_chain" \
            "Failed UPDATE: vm=$vm_name period=$period_id (exit=$rc)"
    fi
    return $rc
}

# Phase 4 commit 4b: typed write helper for retention reconcile pass 2.
# Inserts a minimal chain_health row for a filesystem-discovered period
# that the DB does not yet know about. INSERT OR IGNORE means existing
# rows for the (vm, period) pair are left untouched.
# Arguments:
#   $1 - vm_name
#   $2 - period_id
#   $3 - rotation_policy (e.g. weekly, monthly)
# Returns: 0 on success, 1 on failure
sqlite_register_untracked_period() {
    is_dry_run && return 0
    local vm_name="$1" period_id="$2" policy="$3"

    [[ "$SQLITE_MODULE_AVAILABLE" -ne 1 ]] && return 1
    [[ -z "$vm_name" || -z "$period_id" ]] && return 1

    local esc_vm esc_p esc_pol now
    esc_vm=$(_sql_escape "$vm_name")
    esc_p=$(_sql_escape "$period_id")
    esc_pol=$(_sql_escape "$policy")
    now=$(date -u '+%Y-%m-%d %H:%M:%S')

    _sq3 "$SQLITE_DB_PATH" << SQL_EOF
INSERT OR IGNORE INTO chain_health
    (vm_name, period_id, chain_location, chain_status, rotation_policy, created_at, updated_at)
VALUES
    ('$esc_vm', '$esc_p', '.', 'active', '$esc_pol', '$now', '$now');
SQL_EOF
    local rc=$?
    if [[ $rc -ne 0 ]]; then
        log_error "$SQLITE_MODULE_NAME" "sqlite_register_untracked_period" \
            "Failed INSERT: vm=$vm_name period=$period_id policy=$policy (exit=$rc)"
    fi
    return $rc
}

#################################################################################
# Interactive Lifecycle Management (v1.7)
#
# Two-phase delete model: mark → review → purge
# Protection: purge_eligible flag prevents automated & manual deletion
#################################################################################

# Mark a chain for deletion (soft-delete, DB-only, reversible)
# Files remain on disk until purge. Sets chain_status='marked'.
# Arguments:
#   $1 - vm_name
#   $2 - period_id
#   $3 - marked_by (tui-manual|cli|retention-policy)
# Returns: 0 on success, 1 on failure
sqlite_mark_chain_for_deletion() {
    local vm_name="$1" period_id="$2"
    local marked_by="${3:-tui-manual}"

    [[ "$SQLITE_MODULE_AVAILABLE" -ne 1 ]] && return 1

    local esc_vm=$(_sql_escape "$vm_name")
    local esc_period=$(_sql_escape "$period_id")
    local esc_by=$(_sql_escape "$marked_by")
    local now=$(date -u '+%Y-%m-%d %H:%M:%S')

    # Refuse to mark protected chains. Fail CLOSED: a query error (locked DB,
    # missing column) must NOT be read as "not protected" — that would let a
    # protected chain be marked. Distinguish a query failure from a real 0 count.
    local protected _rc_prot
    protected=$(_sq3 "$SQLITE_DB_PATH" \
        "SELECT COUNT(*) FROM chain_health WHERE vm_name='$esc_vm' AND period_id='$esc_period' AND purge_eligible=0;" 2>/dev/null)
    _rc_prot=$?
    if [[ $_rc_prot -ne 0 ]] || ! [[ "$protected" =~ ^[0-9]+$ ]]; then
        log_error "$SQLITE_MODULE_NAME" "sqlite_mark_chain_for_deletion" \
            "Protection check failed (rc=$_rc_prot); refusing to mark: vm=$vm_name period=$period_id"
        return 1
    fi
    if [[ "$protected" -gt 0 ]]; then
        log_warn "$SQLITE_MODULE_NAME" "sqlite_mark_chain_for_deletion" \
            "Cannot mark protected chain: vm=$vm_name period=$period_id"
        return 1
    fi

    # Ensure row exists (INSERT OR IGNORE), then update
    _sq3 "$SQLITE_DB_PATH" << SQL_EOF
INSERT INTO chain_health (vm_name, period_id, chain_location, chain_status, created_at, updated_at)
VALUES ('$esc_vm', '$esc_period', '.', 'active', '$now', '$now')
ON CONFLICT(vm_name, period_id) DO NOTHING;

UPDATE chain_health SET
    chain_status = 'marked',
    marked_at = '$now', marked_by = '$esc_by', updated_at = '$now'
WHERE vm_name = '$esc_vm' AND period_id = '$esc_period'
  AND chain_status IN ('active', 'archived', 'broken')
  AND purge_eligible = 1;
SQL_EOF
    local rc=$?
    if [[ $rc -ne 0 ]]; then
        log_error "$SQLITE_MODULE_NAME" "sqlite_mark_chain_for_deletion" \
            "Failed to mark: vm=$vm_name period=$period_id (exit=$rc)"
    else
        log_debug "$SQLITE_MODULE_NAME" "sqlite_mark_chain_for_deletion" \
            "Marked for deletion: vm=$vm_name period=$period_id by=$marked_by"
    fi
    return $rc
}

# Unmark a chain previously marked for deletion (reverts to 'active')
# Note: Original state (archived/broken) is not preserved — re-run chain
# health checks after unmarking to re-detect non-active states.
# Arguments:
#   $1 - vm_name
#   $2 - period_id
# Returns: 0 on success, 1 on failure
sqlite_unmark_chain() {
    local vm_name="$1" period_id="$2"

    [[ "$SQLITE_MODULE_AVAILABLE" -ne 1 ]] && return 1

    local esc_vm=$(_sql_escape "$vm_name")
    local esc_period=$(_sql_escape "$period_id")
    local now=$(date -u '+%Y-%m-%d %H:%M:%S')

    _sq3 "$SQLITE_DB_PATH" << SQL_EOF
UPDATE chain_health SET
    chain_status = 'active',
    marked_at = NULL, marked_by = NULL, updated_at = '$now'
WHERE vm_name = '$esc_vm' AND period_id = '$esc_period'
  AND chain_status = 'marked';
SQL_EOF
    local rc=$?
    if [[ $rc -ne 0 ]]; then
        log_error "$SQLITE_MODULE_NAME" "sqlite_unmark_chain" \
            "Failed to unmark: vm=$vm_name period=$period_id (exit=$rc)"
    else
        log_debug "$SQLITE_MODULE_NAME" "sqlite_unmark_chain" \
            "Unmarked: vm=$vm_name period=$period_id"
    fi
    return $rc
}

# Protect a chain from automated and manual deletion
# Arguments:
#   $1 - vm_name
#   $2 - period_id
# Returns: 0 on success, 1 on failure
sqlite_protect_chain() {
    local vm_name="$1" period_id="$2"

    [[ "$SQLITE_MODULE_AVAILABLE" -ne 1 ]] && return 1

    local esc_vm=$(_sql_escape "$vm_name")
    local esc_period=$(_sql_escape "$period_id")
    local now=$(date -u '+%Y-%m-%d %H:%M:%S')

    _sq3 "$SQLITE_DB_PATH" << SQL_EOF
INSERT INTO chain_health (vm_name, period_id, chain_location, chain_status, purge_eligible, created_at, updated_at)
VALUES ('$esc_vm', '$esc_period', '.', 'active', 0, '$now', '$now')
ON CONFLICT(vm_name, period_id) DO UPDATE SET purge_eligible=0, updated_at='$now';
SQL_EOF
    local rc=$?
    if [[ $rc -ne 0 ]]; then
        log_error "$SQLITE_MODULE_NAME" "sqlite_protect_chain" \
            "Failed to protect: vm=$vm_name period=$period_id (exit=$rc)"
    else
        log_debug "$SQLITE_MODULE_NAME" "sqlite_protect_chain" \
            "Protected: vm=$vm_name period=$period_id"
    fi
    return $rc
}

# Remove protection from a chain
# Arguments:
#   $1 - vm_name
#   $2 - period_id
# Returns: 0 on success, 1 on failure
sqlite_unprotect_chain() {
    local vm_name="$1" period_id="$2"

    [[ "$SQLITE_MODULE_AVAILABLE" -ne 1 ]] && return 1

    local esc_vm=$(_sql_escape "$vm_name")
    local esc_period=$(_sql_escape "$period_id")
    local now=$(date -u '+%Y-%m-%d %H:%M:%S')

    _sq3 "$SQLITE_DB_PATH" \
        "UPDATE chain_health SET purge_eligible=1, updated_at='$now'
         WHERE vm_name='$esc_vm' AND period_id='$esc_period'
         AND purge_eligible=0;" 2>/dev/null
    local rc=$?
    if [[ $rc -ne 0 ]]; then
        log_error "$SQLITE_MODULE_NAME" "sqlite_unprotect_chain" \
            "Failed to unprotect: vm=$vm_name period=$period_id (exit=$rc)"
    else
        log_debug "$SQLITE_MODULE_NAME" "sqlite_unprotect_chain" \
            "Unprotected: vm=$vm_name period=$period_id"
    fi
    return $rc
}


#=============================================================================
# EVENT LOGGING FUNCTIONS (CSV-to-DB Migration v1.5)
#=============================================================================

# Log a chain lifecycle event (append-only audit trail)
# This complements chain_health (state table) with an event log
# Arguments:
#   $1  - event_type (chain_created|chain_archived|chain_deleted|chain_broken)
#   $2  - vm_name
#   $3  - chain_id
#   $4  - period_id
#   $5  - backup_dir
#   $6  - chain_location
#   $7  - checkpoint_count
#   $8  - total_chain_bytes
#   $9  - archive_reason (optional)
#   $10 - archive_trigger (optional)
#   $11 - source_backup_type (optional)
#   $12 - covers_from (optional)
#   $13 - covers_to (optional)
#   $14 - full_backup_file (optional)
#   $15 - restore_point_ids (optional)
# Returns: 0 on success, 1 on failure
sqlite_log_chain_event() {
    is_dry_run && return 0
    local event_type="$1"
    local vm_name="$2"
    local chain_id="${3:-}"
    local period_id="${4:-}"
    local backup_dir="${5:-}"
    local chain_location="${6:-}"
    local checkpoint_count="${7:-0}"
    local total_chain_bytes="${8:-0}"
    local archive_reason="${9:-}"
    local archive_trigger="${10:-}"
    local source_backup_type="${11:-}"
    local covers_from="${12:-}"
    local covers_to="${13:-}"
    local full_backup_file="${14:-}"
    local restore_point_ids="${15:-}"
    
    [[ "$SQLITE_MODULE_AVAILABLE" -ne 1 ]] && return 1
    
    local timestamp
    timestamp=$(date -u '+%Y-%m-%d %H:%M:%S')
    local session_id="${SQLITE_CURRENT_SESSION_ID:-}"
    local session_val="NULL"
    [[ -n "$session_id" ]] && session_val="$session_id"
    
    local sql="INSERT INTO chain_events (
        session_id, timestamp, vm_name, event_type, chain_id, period_id,
        backup_dir, chain_location, chain_start_time, chain_end_time,
        checkpoint_count, full_backup_file, total_chain_bytes,
        archive_reason, archive_trigger, source_backup_type,
        covers_from, covers_to, restore_point_ids
    ) VALUES (
        $session_val,
        '$timestamp',
        '$(_sql_escape "$vm_name")',
        '$(_sql_escape "$event_type")',
        '$(_sql_escape "$chain_id")',
        '$(_sql_escape "$period_id")',
        '$(_sql_escape "$backup_dir")',
        '$(_sql_escape "$chain_location")',
        '$(_sql_escape "$covers_from")',
        '$(_sql_escape "$covers_to")',
        $checkpoint_count,
        '$(_sql_escape "$full_backup_file")',
        $total_chain_bytes,
        '$(_sql_escape "$archive_reason")',
        '$(_sql_escape "$archive_trigger")',
        '$(_sql_escape "$source_backup_type")',
        '$(_sql_escape "$covers_from")',
        '$(_sql_escape "$covers_to")',
        '$(_sql_escape "$restore_point_ids")'
    );"
    
    if ! _sql_exec "$sql"; then
        log_error "$SQLITE_MODULE_NAME" "sqlite_log_chain_event" \
            "Failed to insert chain event: $event_type vm=$vm_name"
        return 1
    fi
    
    log_debug "$SQLITE_MODULE_NAME" "sqlite_log_chain_event" \
        "Chain event logged: $event_type vm=$vm_name chain=$chain_id"
    return 0
}

# Log a configuration change event
# Arguments:
#   $1 - event_type (config_loaded|policy_applied|policy_override|policy_changed|
#                    config_missing|config_error|script_start|script_end|
#                    audit_start|audit_complete|manifest_rebuilt|state_backup|
#                    csv_recovered|lock_contention)
#   $2 - config_source (path, optional)
#   $3 - vm_name (optional)
#   $4 - setting_name (optional)
#   $5 - setting_value (optional)
#   $6 - previous_value (optional)
#   $7 - applied_to (optional)
#   $8 - triggered_by (optional)
#   $9 - detail (optional)
# Returns: 0 on success, 1 on failure
sqlite_log_config_event() {
    local event_type="$1"
    local config_source="${2:-}"
    local vm_name="${3:-}"
    local setting_name="${4:-}"
    local setting_value="${5:-}"
    local previous_value="${6:-}"
    local applied_to="${7:-}"
    local triggered_by="${8:-}"
    local detail="${9:-}"
    
    [[ "$SQLITE_MODULE_AVAILABLE" -ne 1 ]] && return 1
    
    local timestamp
    timestamp=$(date -u '+%Y-%m-%d %H:%M:%S')
    local session_id="${SQLITE_CURRENT_SESSION_ID:-}"
    local session_val="NULL"
    [[ -n "$session_id" ]] && session_val="$session_id"
    
    local sql="INSERT INTO config_events (
        session_id, timestamp, event_type, config_source, vm_name,
        setting_name, setting_value, previous_value, applied_to,
        triggered_by, detail
    ) VALUES (
        $session_val,
        '$timestamp',
        '$(_sql_escape "$event_type")',
        '$(_sql_escape "$config_source")',
        '$(_sql_escape "$vm_name")',
        '$(_sql_escape "$setting_name")',
        '$(_sql_escape "$setting_value")',
        '$(_sql_escape "$previous_value")',
        '$(_sql_escape "$applied_to")',
        '$(_sql_escape "$triggered_by")',
        '$(_sql_escape "$detail")'
    );"
    
    if ! _sql_exec "$sql"; then
        # Silently fail for config events to avoid infinite loops
        # (config events can be triggered by error handling)
        return 1
    fi
    
    return 0
}


# Log a file operation
# Arguments:
#   $1  - operation (create|move|copy|delete|rename|archive)
#   $2  - vm_name
#   $3  - source_path
#   $4  - dest_path (optional)
#   $5  - file_type
#   $6  - file_size_bytes
#   $7  - verification_data (optional)
#   $8  - reason
#   $9  - triggered_by
#   $10 - success (1/0)
#   $11 - error_message (optional)
# Returns: 0 on success, 1 on failure
sqlite_log_file_operation() {
    local operation="$1"
    local vm_name="$2"
    local source_path="${3:-}"
    local dest_path="${4:-}"
    local file_type="${5:-}"
    local file_size_bytes="${6:-0}"
    local verification_data="${7:-}"
    local reason="${8:-}"
    local triggered_by="${9:-}"
    local success="${10:-1}"
    local error_message="${11:-}"
    
    [[ "$SQLITE_MODULE_AVAILABLE" -ne 1 ]] && return 1
    
    local timestamp
    timestamp=$(date -u '+%Y-%m-%d %H:%M:%S')
    local session_id="${SQLITE_CURRENT_SESSION_ID:-}"
    local session_val="NULL"
    [[ -n "$session_id" ]] && session_val="$session_id"
    
    # Normalize success to integer
    case "$success" in
        true|1|yes) success=1 ;;
        *) success=0 ;;
    esac
    
    local sql="INSERT INTO file_operations (
        session_id, timestamp, operation, vm_name, source_path, dest_path,
        file_type, file_size_bytes, verification_data, reason,
        triggered_by, success, error_message
    ) VALUES (
        $session_val,
        '$timestamp',
        '$(_sql_escape "$operation")',
        '$(_sql_escape "$vm_name")',
        '$(_sql_escape "$source_path")',
        '$(_sql_escape "$dest_path")',
        '$(_sql_escape "$file_type")',
        $file_size_bytes,
        '$(_sql_escape "$verification_data")',
        '$(_sql_escape "$reason")',
        '$(_sql_escape "$triggered_by")',
        $success,
        '$(_sql_escape "$error_message")'
    );"
    
    if ! _sql_exec "$sql"; then
        log_error "$SQLITE_MODULE_NAME" "sqlite_log_file_operation" \
            "Failed to insert file operation: $operation vm=$vm_name"
        return 1
    fi
    
    return 0
}

# Log a period lifecycle event
# Arguments:
#   $1  - event_type (period_created|period_closed|period_archived|period_deleted)
#   $2  - vm_name
#   $3  - period_id
#   $4  - rotation_policy
#   $5  - period_dir (optional)
#   $6  - period_start (optional)
#   $7  - period_end (optional)
#   $8  - chains_count (optional, default 0)
#   $9  - total_restore_points (optional, default 0)
#   $10 - total_bytes (optional, default 0)
#   $11 - previous_period (optional)
#   $12 - archive_location (optional)
#   $13 - retention_remaining (optional)
# Returns: 0 on success, 1 on failure
sqlite_log_period_event() {
    local event_type="$1"
    local vm_name="$2"
    local period_id="${3:-}"
    local rotation_policy="${4:-}"
    local period_dir="${5:-}"
    local period_start="${6:-}"
    local period_end="${7:-}"
    local chains_count="${8:-0}"
    local total_restore_points="${9:-0}"
    local total_bytes="${10:-0}"
    local previous_period="${11:-}"
    local archive_location="${12:-}"
    local retention_remaining="${13:-0}"
    
    [[ "$SQLITE_MODULE_AVAILABLE" -ne 1 ]] && return 1
    
    local timestamp
    timestamp=$(date -u '+%Y-%m-%d %H:%M:%S')
    local session_id="${SQLITE_CURRENT_SESSION_ID:-}"
    local session_val="NULL"
    [[ -n "$session_id" ]] && session_val="$session_id"
    
    local sql="INSERT INTO period_events (
        session_id, timestamp, vm_name, event_type, period_id,
        rotation_policy, period_dir, period_start, period_end,
        chains_count, total_restore_points, total_bytes,
        previous_period, archive_location, retention_remaining
    ) VALUES (
        $session_val,
        '$timestamp',
        '$(_sql_escape "$vm_name")',
        '$(_sql_escape "$event_type")',
        '$(_sql_escape "$period_id")',
        '$(_sql_escape "$rotation_policy")',
        '$(_sql_escape "$period_dir")',
        '$(_sql_escape "$period_start")',
        '$(_sql_escape "$period_end")',
        $chains_count,
        $total_restore_points,
        $total_bytes,
        '$(_sql_escape "$previous_period")',
        '$(_sql_escape "$archive_location")',
        $retention_remaining
    );"
    
    if ! _sql_exec "$sql"; then
        log_error "$SQLITE_MODULE_NAME" "sqlite_log_period_event" \
            "Failed to insert period event: $event_type vm=$vm_name"
        return 1
    fi
    
    log_debug "$SQLITE_MODULE_NAME" "sqlite_log_period_event" \
        "Period event logged: $event_type vm=$vm_name period=$period_id"
    return 0
}

# Log a retention action
# Arguments:
#   $1  - action (delete|archive|keep|skip|error)
#   $2  - vm_name
#   $3  - target_type (period|chain|orphan_period|orphan_file)
#   $4  - target_path
#   $5  - target_period
#   $6  - rotation_policy
#   $7  - retention_limit (optional, default 0)
#   $8  - current_count (optional, default 0)
#   $9  - age_days (optional, default 0)
#   $10 - freed_bytes (optional, default 0)
#   $11 - preserve_reason (optional)
#   $12 - triggered_by (optional)
#   $13 - success (1/0, optional, default 1)
#   $14 - action_source (optional: prune|retention|orphan_retention)
# Returns: 0 on success, 1 on failure
sqlite_log_retention_event() {
    local action="$1"
    local vm_name="$2"
    local target_type="${3:-}"
    local target_path="${4:-}"
    local target_period="${5:-}"
    local rotation_policy="${6:-}"
    local retention_limit="${7:-0}"
    local current_count="${8:-0}"
    local age_days="${9:-0}"
    local freed_bytes="${10:-0}"
    local preserve_reason="${11:-}"
    local triggered_by="${12:-}"
    local success="${13:-1}"
    local action_source="${14:-}"
    
    [[ "$SQLITE_MODULE_AVAILABLE" -ne 1 ]] && return 1
    
    local timestamp
    timestamp=$(date -u '+%Y-%m-%d %H:%M:%S')
    local session_id="${SQLITE_CURRENT_SESSION_ID:-}"
    local session_val="NULL"
    [[ -n "$session_id" ]] && session_val="$session_id"
    
    # Normalize success to integer
    case "$success" in
        true|1|yes) success=1 ;;
        *) success=0 ;;
    esac
    
    # Build action_source SQL value
    local action_source_val="NULL"
    [[ -n "$action_source" ]] && action_source_val="'$(_sql_escape "$action_source")'"
    
    local sql="INSERT INTO retention_events (
        session_id, timestamp, vm_name, action, target_type, target_path,
        target_period, rotation_policy, retention_limit, current_count,
        age_days, freed_bytes, preserve_reason, triggered_by, success,
        action_source
    ) VALUES (
        $session_val,
        '$timestamp',
        '$(_sql_escape "$vm_name")',
        '$(_sql_escape "$action")',
        '$(_sql_escape "$target_type")',
        '$(_sql_escape "$target_path")',
        '$(_sql_escape "$target_period")',
        '$(_sql_escape "$rotation_policy")',
        $retention_limit,
        $current_count,
        $age_days,
        $freed_bytes,
        '$(_sql_escape "$preserve_reason")',
        '$(_sql_escape "$triggered_by")',
        $success,
        $action_source_val
    );"
    
    if ! _sql_exec "$sql"; then
        log_error "$SQLITE_MODULE_NAME" "sqlite_log_retention_event" \
            "Failed to insert retention event: $action vm=$vm_name"
        return 1
    fi
    
    log_debug "$SQLITE_MODULE_NAME" "sqlite_log_retention_event" \
        "Retention event logged: $action vm=$vm_name target=$target_type"
    return 0
}

# Export function names for external use
export -f sqlite_init_database
export -f sqlite_session_start
export -f sqlite_session_end
export -f sqlite_get_session_id
export -f sqlite_log_vm_backup
export -f sqlite_log_replication_run
export -f sqlite_log_replication_vm
export -f sqlite_log_replication_vms
export -f sqlite_log_chain_event
export -f sqlite_log_config_event
export -f sqlite_log_file_operation
export -f sqlite_log_period_event
export -f sqlite_log_retention_event
export -f sqlite_update_chain_health
export -f sqlite_archive_chain
export -f sqlite_mark_chain_broken
export -f sqlite_mark_chain_deleted
export -f sqlite_mark_chain_deleted_if_exists
export -f sqlite_mark_chain_for_deletion
export -f sqlite_unmark_chain
export -f sqlite_protect_chain
export -f sqlite_unprotect_chain
export -f sqlite_mark_phantom_chain
export -f sqlite_register_untracked_period
