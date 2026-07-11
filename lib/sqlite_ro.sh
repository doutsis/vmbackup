#!/usr/bin/env bash
#################################################################################
# SQLite Read-Only Module for VM Backup Tools
#
# Read-only / utility subset carved from lib/sqlite_module.sh (Phase 4,
# UNI-605). Provides the query API and helpers used by:
#   - vmbackup --status (modules/status_module.sh)
#   - future vmrestore --list enrichment
#   - any consumer that needs to read the vmbackup database without the
#     full writer module overhead
#
# Read-only contract:
#   - No INSERT / UPDATE / DELETE / CREATE / ALTER statements.
#   - sqlite_init_readonly opens the DB with PRAGMA query_only=ON.
#   - WAL mode is set by the writer at sqlite_init_database time; readers
#     inherit it (concurrent reader/writer is safe).
#
# Entry point:
#   sqlite_init_readonly        # locate DB, set query_only=ON, set
#                               # SQLITE_DB_PATH and SQLITE_MODULE_AVAILABLE.
#                               # Caller must export BACKUP_PATH (and
#                               # optionally STATE_DIR) before calling.
#
# Sourcing pattern:
#   - Direct consumers:  source lib/sqlite_ro.sh
#   - Via writer module: lib/sqlite_module.sh sources this file at the top
#     so writer functions can call helpers like _sql_escape and
#     _sqlite_check_dependency without duplication.
#
# Guard variable: _SQLITE_RO_LOADED (distinct from _SQLITE_MODULE_LOADED so
# the writer module can source this file without colliding with its own
# load guard).
#
#################################################################################

# UNI-321: idempotency guard — re-source is a no-op once sqlite_init_readonly is defined.
declare -F sqlite_init_readonly >/dev/null 2>&1 && return 0

# Module-state default (FF-87): sqlite_ro.sh may be sourced standalone on a
# DR/restore host where sqlite_module.sh (whose declare -g normally supplies the
# 0-default) is absent. Initialise the availability flag at load so the read
# gates and sqlite_is_available never expand an unset var under the restore
# binary's `set -u` (an rc-127 abort at the start of restore). Preserve any
# value an already-loaded writer module set.
declare -g SQLITE_MODULE_AVAILABLE="${SQLITE_MODULE_AVAILABLE:-0}"

# Escape single quotes for SQL strings.
# Single-quote doubling ('→'') is the ONLY escape needed for SQLite string
# literals. Unlike MySQL/PostgreSQL, SQLite does not interpret backslash
# sequences (\n, \t, etc.) inside quoted strings. NUL bytes cannot appear
# in bash variables (truncated at NUL), so they are not a concern.
# VM names are further sanitised by sanitize_vm_name() ([a-zA-Z0-9._-])
# before reaching any path- or key-derived SQL value.
# Usage: escaped=$(_sql_escape "$value")
_sql_escape() {
    printf '%s' "${1//\'/\'\'}"
}

# Check if sqlite3 is available
_sqlite_check_dependency() {
    if ! command -v sqlite3 &>/dev/null; then
        log_warn "$SQLITE_MODULE_NAME" "_sqlite_check_dependency" \
            "sqlite3 not found - SQLite logging disabled"
        return 1
    fi
    return 0
}

# Initialize database in read-only mode for --status queries
# No mkdir, no WAL, no migrations, no session tracking
# Sets: SQLITE_DB_PATH, SQLITE_MODULE_AVAILABLE
# Returns: 0 on success, 1 on failure
sqlite_init_readonly() {
    if ! command -v sqlite3 &>/dev/null; then
        echo "Error: sqlite3 not found" >&2
        return 1
    fi

    local state_dir="${STATE_DIR:-${BACKUP_PATH%/}/_state}"
    SQLITE_DB_PATH="${state_dir}/vmbackup.db"

    if [[ ! -f "$SQLITE_DB_PATH" ]]; then
        echo "No backup database found at $SQLITE_DB_PATH" >&2
        return 1
    fi

    if ! sqlite3 "$SQLITE_DB_PATH" "PRAGMA query_only=ON; SELECT 1;" &>/dev/null; then
        echo "Database not accessible: $SQLITE_DB_PATH" >&2
        return 1
    fi

    SQLITE_MODULE_AVAILABLE=1
    return 0
}

# Run a sqlite3 query with the appropriate output format
# Arguments:
#   $1 - output_mode: pipe (default) or csv
#   $2 - SQL query string
# Uses: SQLITE_DB_PATH
_sqlite_query_formatted() {
    local output_mode="${1:-pipe}"
    local sql="$2"

    if [[ "$output_mode" == "csv" ]]; then
        sqlite3 -csv -header "$SQLITE_DB_PATH" "$sql"
    else
        sqlite3 -separator '|' -header "$SQLITE_DB_PATH" "$sql"
    fi
}

# Get session summary for the last N days
# Arguments:
#   $1 - days (default 1 = today)
#   $2 - output_mode: pipe (default) or csv
# Reads (env):
#   CONFIG_INSTANCE        - default "default"; restricts results to one instance
#   STATUS_ALL_INSTANCES   - if "true", omits the instance filter (cross-instance view)
# Returns: Session summary rows including 3 trailing row-count columns
#          (vm_rows, repl_rows, retention_rows) for job-type classification
sqlite_query_today_sessions() {
    local days="${1:-1}"
    local output_mode="${2:-pipe}"
    # FF-177: reject a non-integer days (e.g. a '7d' typo) before it splices
    # into datetime('now','-$days days') and yields a silently empty report.
    [[ "$days" =~ ^[1-9][0-9]*$ ]] || days=1

    if [[ "$SQLITE_MODULE_AVAILABLE" -ne 1 ]]; then
        return 1
    fi

    local instance_filter=""
    if [[ "${STATUS_ALL_INSTANCES:-false}" != "true" ]]; then
        local instance="${CONFIG_INSTANCE:-default}"
        local esc_instance="${instance//\'/\'\'}"
        instance_filter="AND instance = '$esc_instance'"
    fi

    _sqlite_query_formatted "$output_mode" \
        "SELECT s.id, COALESCE(s.instance, '<unknown>') as instance,
                COALESCE(s.session_type, '') as session_type, s.start_time,
                COALESCE(s.duration_sec, 0) as duration_sec, s.status,
                s.vms_success, s.vms_failed, COALESCE(s.vms_skipped, 0) as vms_skipped,
                COALESCE(s.bytes_total, 0) as bytes_total,
                (SELECT COUNT(*) FROM vm_backups       vb WHERE vb.session_id = s.id) AS vm_rows,
                (SELECT COUNT(*) FROM replication_runs rr WHERE rr.session_id = s.id) AS repl_rows,
                (SELECT COUNT(*) FROM retention_events re WHERE re.session_id = s.id) AS retention_rows
         FROM sessions s
         WHERE s.start_time >= datetime('now', '-$days days')
           $instance_filter
         ORDER BY s.start_time DESC;"
}

# Get per-VM backup rows for a specific session (for --status VM breakdown)
# Returns sanitized 6-column rows for display: vm_name, state, type, status, bytes, duration_sec
# Arguments:
#   $1 - session_id
# Returns: One row per VM in the session: vm_name, state, type, status, bytes, duration_sec
sqlite_query_session_vm_backups_display() {
    local session_id="$1"
    # FF-177: numeric-only session_id (rejects empty and non-integer) before it
    # splices into "WHERE session_id = $session_id".
    [[ "$session_id" =~ ^[0-9]+$ ]] || return 1

    [[ "$SQLITE_MODULE_AVAILABLE" -ne 1 ]] && return 1
    [[ -z "$session_id" ]] && return 1

    sqlite3 -separator '|' "$SQLITE_DB_PATH" \
        "SELECT vm_name,
                COALESCE(NULLIF(vm_status, ''), '-') as state,
                CASE WHEN LOWER(COALESCE(backup_type,'')) IN
                         ('running','shut off','shut-off','paused','crashed',
                          'pmsuspended','idle','off','auto','none','')
                     THEN '-'
                     ELSE backup_type
                END as type,
                CASE WHEN LOWER(COALESCE(status,'')) IN
                         ('auto','none','unknown','')
                     THEN '-'
                     ELSE COALESCE(status, '-')
                END as status,
                CASE WHEN COALESCE(bytes_written, 0) > 0 THEN bytes_written
                     WHEN COALESCE(total_dir_bytes, 0) > 0 THEN total_dir_bytes
                     WHEN COALESCE(chain_size_bytes, 0) > 0 THEN chain_size_bytes
                     ELSE 0 END as bytes,
                COALESCE(duration_sec, 0) as duration_sec
         FROM vm_backups
         WHERE session_id = $session_id
         ORDER BY vm_name;" 2>/dev/null
}

# Get aggregated retention summary for a single session (for --status prune detail)
# Returns one pipe-delimited row:
#   actions_total | actions_ok | actions_failed | freed_bytes_total | vm_count |
#   action_types | target_types | delete_count | evaluate_count | skip_count
# Arguments:
#   $1 - session_id
sqlite_query_session_retention_summary() {
    local session_id="$1"
    # FF-177: numeric-only session_id (rejects empty and non-integer) before it
    # splices into "WHERE session_id = $session_id".
    [[ "$session_id" =~ ^[0-9]+$ ]] || return 1

    [[ "$SQLITE_MODULE_AVAILABLE" -ne 1 ]] && return 1
    [[ -z "$session_id" ]] && return 1

    sqlite3 -separator '|' "$SQLITE_DB_PATH" \
        "SELECT COALESCE(COUNT(*), 0) AS actions_total,
                COALESCE(SUM(CASE WHEN success = 1 THEN 1 ELSE 0 END), 0) AS actions_ok,
                COALESCE(SUM(CASE WHEN success = 0 THEN 1 ELSE 0 END), 0) AS actions_failed,
                COALESCE(SUM(COALESCE(freed_bytes, 0)), 0) AS freed_bytes_total,
                COALESCE(COUNT(DISTINCT vm_name), 0) AS vm_count,
                COALESCE(GROUP_CONCAT(DISTINCT action), '') AS action_types,
                COALESCE(GROUP_CONCAT(DISTINCT target_type), '') AS target_types,
                COALESCE(SUM(CASE WHEN action = 'delete'   THEN 1 ELSE 0 END), 0) AS delete_count,
                COALESCE(SUM(CASE WHEN action = 'evaluate' THEN 1 ELSE 0 END), 0) AS evaluate_count,
                COALESCE(SUM(CASE WHEN action = 'skip'     THEN 1 ELSE 0 END), 0) AS skip_count
         FROM retention_events
         WHERE session_id = $session_id;" 2>/dev/null
}

# Get per-session replication runs (for --status replicate-only / mixed footer)
# Returns pipe-delimited rows ordered by start_time:
#   endpoint_name | endpoint_type | transport | status |
#   bytes_transferred | files_transferred | duration_sec | error_message
# Arguments:
#   $1 - session_id
sqlite_query_session_replication_summary() {
    local session_id="$1"
    # FF-177: numeric-only session_id (rejects empty and non-integer) before it
    # splices into "WHERE session_id = $session_id".
    [[ "$session_id" =~ ^[0-9]+$ ]] || return 1

    [[ "$SQLITE_MODULE_AVAILABLE" -ne 1 ]] && return 1
    [[ -z "$session_id" ]] && return 1

    sqlite3 -separator '|' "$SQLITE_DB_PATH" \
        "SELECT COALESCE(endpoint_name, '<unknown>') AS endpoint_name,
                COALESCE(endpoint_type, '-')         AS endpoint_type,
                COALESCE(transport, '-')             AS transport,
                COALESCE(status, '-')                AS status,
                COALESCE(bytes_transferred, 0)       AS bytes_transferred,
                COALESCE(files_transferred, 0)       AS files_transferred,
                COALESCE(duration_sec, 0)            AS duration_sec,
                COALESCE(error_message, '')          AS error_message
         FROM replication_runs
         WHERE session_id = $session_id
         ORDER BY start_time;" 2>/dev/null
}

# Get VM backup history
# Arguments:
#   $1 - vm_name
#   $2 - limit (default 10)
#   $3 - output_mode: pipe (default) or csv
# Returns: Recent backup records for the VM
sqlite_query_vm_history() {
    local vm_name="$1"
    local limit="${2:-10}"
    local output_mode="${3:-pipe}"
    # FF-177: reject a non-integer limit before it splices into LIMIT $limit.
    [[ "$limit" =~ ^[1-9][0-9]*$ ]] || limit=10
    
    if [[ "$SQLITE_MODULE_AVAILABLE" -ne 1 ]]; then
        return 1
    fi
    
    local esc_vm_name="${vm_name//\'/\'\'}"
    _sqlite_query_formatted "$output_mode" \
        "SELECT s.start_time, vb.backup_type, vb.status, vb.bytes_written, vb.duration_sec
         FROM vm_backups vb
         JOIN sessions s ON vb.session_id = s.id
         WHERE vb.vm_name = '$esc_vm_name'
         ORDER BY s.start_time DESC
         LIMIT $limit;"
}

# Get last successful backup for a VM
# Arguments:
#   $1 - vm_name
#   $2 - output_mode: pipe (default) or csv
# Returns: Single row with last successful backup info
sqlite_query_last_success() {
    local vm_name="$1"
    local output_mode="${2:-pipe}"
    
    if [[ "$SQLITE_MODULE_AVAILABLE" -ne 1 ]]; then
        return 1
    fi
    
    local esc_vm_name="${vm_name//\'/\'\'}"
    _sqlite_query_formatted "$output_mode" \
        "SELECT s.start_time, vb.backup_type, vb.backup_path, vb.bytes_written
         FROM vm_backups vb
         JOIN sessions s ON vb.session_id = s.id
         WHERE vb.vm_name = '$esc_vm_name' AND vb.status = 'success'
         ORDER BY s.start_time DESC
         LIMIT 1;"
}

# Get failed VMs in the last N days
# Arguments:
#   $1 - days (default 7)
#   $2 - output_mode: pipe (default) or csv
# Returns: List of VMs with failure counts
sqlite_query_recent_failures() {
    local days="${1:-7}"
    local output_mode="${2:-pipe}"
    # FF-177: reject a non-integer days (e.g. a '7d' typo) before it splices
    # into datetime('now','-$days days') and yields a silently empty report.
    [[ "$days" =~ ^[1-9][0-9]*$ ]] || days=7
    
    if [[ "$SQLITE_MODULE_AVAILABLE" -ne 1 ]]; then
        return 1
    fi
    
    _sqlite_query_formatted "$output_mode" \
        "SELECT vm_name, COUNT(*) as failures, MAX(s.start_time) as last_failure
         FROM vm_backups vb
         JOIN sessions s ON vb.session_id = s.id
         WHERE vb.status = 'failed' 
           AND s.start_time >= datetime('now', '-$days days')
         GROUP BY vm_name
         ORDER BY failures DESC;"
}

# Get replication status for the last N days
# Arguments:
#   $1 - days (default 1 = today)
#   $2 - output_mode: pipe (default) or csv
# Returns: Replication runs for recent sessions
sqlite_query_today_replications() {
    local days="${1:-1}"
    local output_mode="${2:-pipe}"
    # FF-177: reject a non-integer days (e.g. a '7d' typo) before it splices
    # into datetime('now','-$days days') and yields a silently empty report.
    [[ "$days" =~ ^[1-9][0-9]*$ ]] || days=1
    
    if [[ "$SQLITE_MODULE_AVAILABLE" -ne 1 ]]; then
        return 1
    fi
    
    _sqlite_query_formatted "$output_mode" \
        "SELECT rr.start_time, rr.endpoint_name, rr.endpoint_type, rr.transport, rr.status, 
                rr.bytes_transferred, rr.duration_sec
         FROM replication_runs rr
         JOIN sessions s ON rr.session_id = s.id
         WHERE s.start_time >= datetime('now', '-$days days')
         ORDER BY rr.start_time DESC;"
}

# Get chain health summary or detail for --status --chains
# Arguments:
#   $1 - vm_name (empty = summary, set = detail for that VM)
#   $2 - output_mode: pipe (default) or csv
# Returns: Chain health rows grouped by VM (summary) or per-chain (detail)
sqlite_query_chain_health() {
    local vm_name="${1:-}"
    local output_mode="${2:-pipe}"

    [[ "$SQLITE_MODULE_AVAILABLE" -ne 1 ]] && return 1

    if [[ -z "$vm_name" ]]; then
        # Summary: one row per VM
        _sqlite_query_formatted "$output_mode" \
            "SELECT vm_name,
                    SUM(CASE WHEN chain_status = 'active' THEN 1 ELSE 0 END) as active,
                    SUM(CASE WHEN chain_status = 'archived' THEN 1 ELSE 0 END) as archived,
                    SUM(CASE WHEN chain_status = 'purged' THEN 1 ELSE 0 END) as purged,
                    SUM(total_checkpoints) as checkpoints,
                    SUM(restorable_count) as restorable,
                    SUM(CASE WHEN broken_at IS NOT NULL THEN 1 ELSE 0 END) as broken,
                    SUM(COALESCE(archive_size_bytes, 0)) as archive_size_bytes,
                    MIN(first_backup) as first_backup,
                    MAX(last_backup) as last_backup
             FROM chain_health
             WHERE chain_status NOT IN ('deleted')
             GROUP BY vm_name
             ORDER BY vm_name;"
    else
        # Detail: one row per chain for a VM
        local esc_vm="${vm_name//\'/\'\'}"
        _sqlite_query_formatted "$output_mode" \
            "SELECT period_id, chain_status as status,
                    COALESCE(rotation_policy, '') as policy,
                    total_checkpoints as checkpoints,
                    restorable_count as restorable,
                    CASE WHEN broken_at IS NOT NULL THEN 1 ELSE 0 END as broken,
                    COALESCE(archive_size_bytes, '') as archive_size_bytes,
                    first_backup, last_backup
             FROM chain_health
             WHERE vm_name = '$esc_vm'
               AND chain_status NOT IN ('deleted')
             ORDER BY last_backup DESC;"
    fi
}

# Get storage growth per VM for --status --storage
# Arguments:
#   $1 - output_mode: pipe (default) or csv
# Returns: Per-VM storage summary (all history)
sqlite_query_storage_growth() {
    local output_mode="${1:-pipe}"

    [[ "$SQLITE_MODULE_AVAILABLE" -ne 1 ]] && return 1

    _sqlite_query_formatted "$output_mode" \
        "SELECT vb.vm_name,
                COALESCE(
                    (SELECT rotation_policy FROM vm_backups
                     WHERE vm_name = vb.vm_name AND status = 'success'
                     ORDER BY id DESC LIMIT 1), '') as policy,
                COUNT(*) as backups,
                COALESCE(AVG(CASE WHEN vb.backup_type = 'full' THEN vb.bytes_written END), 0) as avg_full,
                COALESCE(AVG(CASE WHEN vb.backup_type IN ('auto','inc') THEN vb.bytes_written END), 0) as avg_incr,
                SUM(vb.bytes_written) as total_written,
                COALESCE(
                    (SELECT chain_size_bytes FROM vm_backups
                     WHERE vm_name = vb.vm_name AND status = 'success' AND chain_size_bytes > 0
                     ORDER BY id DESC LIMIT 1), 0) as current_chain
         FROM vm_backups vb
         WHERE vb.status = 'success'
         GROUP BY vb.vm_name
         ORDER BY vb.vm_name;"
}

# Get per-VM storage trend data for --status --storage (104 Change C).
# Returns success-only rows with bytes>0, scoped to a single instance, with
# enough columns for the renderer to compute the trend symbol in shell:
#   vm_name | total_count | avg_full | avg_incr | last_written | last_size | last_5_avg | prev_5_avg
# `last_*` come from the most recent backup row for that VM.
# `last_5_avg` / `prev_5_avg` enable the §5.4 trend indicator with the 10-row
# minimum data guard (renderer prints `—` when total_count < 10).
# Arguments:
#   $1 - instance (default: CONFIG_INSTANCE or 'default')
#   $2 - output_mode: pipe (default) or csv
sqlite_query_storage_trends() {
    local instance="${1:-${CONFIG_INSTANCE:-default}}"
    local output_mode="${2:-pipe}"
    local esc_instance="${instance//\'/\'\'}"

    [[ "$SQLITE_MODULE_AVAILABLE" -ne 1 ]] && return 1

    _sqlite_query_formatted "$output_mode" \
        "WITH ranked AS (
             SELECT vb.vm_name,
                    vb.backup_type,
                    vb.bytes_written,
                    s.start_time,
                    ROW_NUMBER() OVER (PARTITION BY vb.vm_name ORDER BY vb.id DESC) AS rn,
                    COUNT(*)     OVER (PARTITION BY vb.vm_name) AS row_count
             FROM vm_backups vb
             JOIN sessions s ON s.id = vb.session_id
             WHERE vb.status = 'success'
               AND vb.bytes_written > 0
               AND s.instance = '$esc_instance'
         )
         SELECT vm_name,
                MAX(row_count)                                                          AS total_count,
                COALESCE(AVG(CASE WHEN backup_type = 'full'           THEN bytes_written END), 0) AS avg_full,
                COALESCE(AVG(CASE WHEN backup_type IN ('auto','inc')  THEN bytes_written END), 0) AS avg_incr,
                MAX(CASE WHEN rn = 1 THEN start_time    END)                            AS last_written,
                MAX(CASE WHEN rn = 1 THEN bytes_written END)                            AS last_size,
                COALESCE(AVG(CASE WHEN rn <= 5            THEN bytes_written END), 0)   AS last_5_avg,
                COALESCE(AVG(CASE WHEN rn >  5 AND rn <= 10 THEN bytes_written END), 0) AS prev_5_avg
         FROM ranked
         GROUP BY vm_name
         ORDER BY vm_name;"
}

# Get destination-level storage summary for --status --storage footer.
# Returns one row (pipe or CSV), all numeric values raw bytes:
#   sample_count | oldest_time | newest_time | oldest_used | newest_used | newest_free | newest_total | written_7d | written_30d
# `sample_count` is the number of v2.1+ sessions in the projection window.
# Renderer uses (newest_used - oldest_used) / (newest_time - oldest_time) for
# the slope; <7 samples renders "(insufficient history)" per §5.2.
# Arguments:
#   $1 - instance (default: CONFIG_INSTANCE or 'default')
#   $2 - window_size: how many recent v2.1+ sessions to include (default: 30)
#   $3 - output_mode: pipe (default) or csv
sqlite_query_storage_destination_summary() {
    local instance="${1:-${CONFIG_INSTANCE:-default}}"
    local window="${2:-30}"
    local output_mode="${3:-pipe}"
    local esc_instance="${instance//\'/\'\'}"
    # Defensive: window must be a positive integer (default 30 if not)
    [[ "$window" =~ ^[1-9][0-9]*$ ]] || window=30

    [[ "$SQLITE_MODULE_AVAILABLE" -ne 1 ]] && return 1

    _sqlite_query_formatted "$output_mode" \
        "WITH win AS (
             SELECT id, start_time, disk_free_bytes, disk_total_bytes,
                    (disk_total_bytes - disk_free_bytes) AS used_bytes
             FROM sessions
             WHERE instance = '$esc_instance'
               AND status   = 'success'
               AND disk_total_bytes > 0
             ORDER BY id DESC
             LIMIT $window
         )
         SELECT
             (SELECT COUNT(*)   FROM win)                                  AS sample_count,
             (SELECT MIN(start_time) FROM win)                             AS oldest_time,
             (SELECT MAX(start_time) FROM win)                             AS newest_time,
             (SELECT used_bytes FROM win ORDER BY start_time ASC  LIMIT 1) AS oldest_used,
             (SELECT used_bytes FROM win ORDER BY start_time DESC LIMIT 1) AS newest_used,
             (SELECT disk_free_bytes  FROM win ORDER BY start_time DESC LIMIT 1) AS newest_free,
             (SELECT disk_total_bytes FROM win ORDER BY start_time DESC LIMIT 1) AS newest_total,
             COALESCE((SELECT SUM(bytes_total) FROM sessions
                       WHERE instance = '$esc_instance' AND status = 'success'
                         AND start_time > datetime('now','-7 days')),  0) AS written_7d,
             COALESCE((SELECT SUM(bytes_total) FROM sessions
                       WHERE instance = '$esc_instance' AND status = 'success'
                         AND start_time > datetime('now','-30 days')), 0) AS written_30d;"
}

# Get policy summary per VM for --status --policies
# Arguments:
#   $1 - output_mode: pipe (default) or csv
# Returns: Per-VM chain/orphan counts from chain_health
sqlite_query_policy_summary() {
    local output_mode="${1:-pipe}"

    [[ "$SQLITE_MODULE_AVAILABLE" -ne 1 ]] && return 1

    _sqlite_query_formatted "$output_mode" \
        "SELECT ch.vm_name,
                SUM(CASE WHEN ch.chain_status IN ('active','archived') THEN 1 ELSE 0 END) as chains,
                SUM(CASE WHEN ch.chain_status IN ('active','archived')
                          AND ch.rotation_policy != COALESCE(
                              (SELECT rotation_policy FROM vm_backups
                               WHERE vm_name = ch.vm_name AND status = 'success'
                               ORDER BY id DESC LIMIT 1), '')
                     THEN 1 ELSE 0 END) as orphans,
                MIN(CASE WHEN ch.chain_status IN ('active','archived') THEN ch.first_backup END) as oldest_backup
         FROM chain_health ch
         WHERE ch.chain_status NOT IN ('deleted','purged')
         GROUP BY ch.vm_name
         ORDER BY ch.vm_name;"
}

# Phase 8 (UNI-902b): Get recent restore_sessions rows for --status --restores.
# Mirrors sqlite_query_recent_failures shape.
# Arguments:
#   $1 - days (default 7)
#   $2 - vm_name (optional; empty = all VMs)
#   $3 - output_mode: pipe (default) or csv
# Returns: Restore session rows in stable column order matching surface-9
#          baseline: start_time, end_time, vm_name, restore_mode, status,
#          exit_code, dry_run, duration_sec, target_path
sqlite_query_recent_restores() {
    local days="${1:-7}"
    local vm_name="${2:-}"
    local output_mode="${3:-pipe}"
    # FF-177: reject a non-integer days (e.g. a '7d' typo) before it splices
    # into datetime('now','-$days days') and yields a silently empty report.
    [[ "$days" =~ ^[1-9][0-9]*$ ]] || days=7

    [[ "$SQLITE_MODULE_AVAILABLE" -ne 1 ]] && return 1

    local vm_filter=""
    if [[ -n "$vm_name" ]]; then
        local esc_vm="${vm_name//\'/\'\'}"
        vm_filter="AND vm_name = '$esc_vm'"
    fi

    _sqlite_query_formatted "$output_mode" \
        "SELECT start_time,
                COALESCE(end_time, '') AS end_time,
                vm_name,
                COALESCE(restore_mode, '') AS restore_mode,
                status,
                COALESCE(exit_code, '') AS exit_code,
                dry_run,
                COALESCE(duration_sec, 0) AS duration_sec,
                COALESCE(target_path, '') AS target_path
         FROM restore_sessions
         WHERE start_time >= datetime('now', '-$days days')
           $vm_filter
         ORDER BY start_time DESC;"
}

# Get all VM backup records for a session
# Arguments:
#   $1 - session_id (default: current session)
# Returns: Pipe-delimited rows:
#   vm_name|vm_status|os_type|backup_type|backup_method|status|bytes_written|
#   chain_size_bytes|total_dir_bytes|restore_points|restore_points_before|
#   duration_sec|error_code|error_message|event_type|event_detail|
#   qemu_agent|vm_paused|chain_archived|rotation_policy
sqlite_query_session_vm_backups() {
    local session_id="${1:-$SQLITE_CURRENT_SESSION_ID}"
    # FF-177: numeric-only session_id (rejects empty and non-integer) before it
    # splices into "WHERE session_id = $session_id".
    [[ "$session_id" =~ ^[0-9]+$ ]] || return 1

    if [[ "$SQLITE_MODULE_AVAILABLE" -ne 1 ]] || [[ -z "$session_id" ]]; then
        return 1
    fi

    sqlite3 -separator '|' "$SQLITE_DB_PATH" << SQL_EOF
SELECT vm_name, vm_status, os_type, backup_type, backup_method,
       status, bytes_written, chain_size_bytes, total_dir_bytes,
       restore_points, restore_points_before, duration_sec,
       COALESCE(error_code,''), COALESCE(error_message,''),
       COALESCE(event_type,''), COALESCE(event_detail,''),
       qemu_agent, vm_paused, chain_archived,
       COALESCE(rotation_policy,'')
FROM vm_backups
WHERE session_id = $session_id
ORDER BY
  CASE status
    WHEN 'success' THEN 1
    WHEN 'failed'  THEN 2
    WHEN 'skipped' THEN 3
    WHEN 'excluded' THEN 4
    ELSE 5
  END,
  vm_name;
SQL_EOF
}

# Get replication runs for a session
# Arguments:
#   $1 - session_id (default: current session)
# Returns: Pipe-delimited rows:
#   endpoint_name|endpoint_type|transport|status|bytes_transferred|
#   files_transferred|duration_sec|destination|error_message
sqlite_query_session_replication() {
    local session_id="${1:-$SQLITE_CURRENT_SESSION_ID}"
    # FF-177: numeric-only session_id (rejects empty and non-integer) before it
    # splices into "WHERE session_id = $session_id".
    [[ "$session_id" =~ ^[0-9]+$ ]] || return 1

    if [[ "$SQLITE_MODULE_AVAILABLE" -ne 1 ]] || [[ -z "$session_id" ]]; then
        return 1
    fi

    sqlite3 -separator '|' "$SQLITE_DB_PATH" << SQL_EOF
SELECT endpoint_name, endpoint_type, transport,
       status, bytes_transferred, files_transferred,
       duration_sec, COALESCE(destination,''),
       COALESCE(error_message,'')
FROM replication_runs
WHERE session_id = $session_id
ORDER BY endpoint_type, endpoint_name;
SQL_EOF
}

# Get session summary counts (single row)
# Arguments:
#   $1 - session_id (default: current session)
# Returns: Pipe-delimited single row:
#   vms_total|vms_success|vms_failed|vms_skipped|vms_excluded|bytes_total|status
sqlite_query_session_summary() {
    local session_id="${1:-$SQLITE_CURRENT_SESSION_ID}"
    # FF-177: numeric-only session_id (rejects empty and non-integer) before it
    # splices into "WHERE id = $session_id".
    [[ "$session_id" =~ ^[0-9]+$ ]] || return 1

    if [[ "$SQLITE_MODULE_AVAILABLE" -ne 1 ]] || [[ -z "$session_id" ]]; then
        return 1
    fi

    sqlite3 -separator '|' "$SQLITE_DB_PATH" << SQL_EOF
SELECT COALESCE(vms_total,0), COALESCE(vms_success,0),
       COALESCE(vms_failed,0), COALESCE(vms_skipped,0),
       COALESCE(vms_excluded,0), COALESCE(bytes_total,0),
       COALESCE(status,'unknown'),
       COALESCE(session_type,'standard')
FROM sessions
WHERE id = $session_id;
SQL_EOF
}

# Check if database is available and initialized
sqlite_is_available() {
    [[ "$SQLITE_MODULE_AVAILABLE" -eq 1 ]]
}

# Get the last rotation policy used for a VM
# Arguments:
#   $1 - vm_name
# Returns: rotation_policy on stdout (e.g., 'daily', 'monthly'), empty if no history
sqlite_get_last_rotation_policy() {
    local vm_name="$1"
    
    if [[ "$SQLITE_MODULE_AVAILABLE" -ne 1 ]]; then
        return 1
    fi
    
    local esc_vm_name="${vm_name//\'/\'\'}"
    sqlite3 "$SQLITE_DB_PATH" \
        "SELECT rotation_policy FROM vm_backups 
         WHERE vm_name = '$esc_vm_name' 
           AND status = 'success'
           AND rotation_policy IS NOT NULL 
           AND rotation_policy != ''
         ORDER BY id DESC LIMIT 1;" 2>/dev/null
}

# Get database path
sqlite_get_db_path() {
    echo "$SQLITE_DB_PATH"
}

# Get schema version from database
sqlite_get_schema_version() {
    if [[ "$SQLITE_MODULE_AVAILABLE" -ne 1 ]]; then
        return 1
    fi
    
    sqlite3 "$SQLITE_DB_PATH" \
        "SELECT value FROM schema_info WHERE key='version';" 2>/dev/null
}

# Get all chains marked for deletion
# Arguments: none
# Returns: Tab-separated lines: vm_name \t period_id \t marked_at \t marked_by
sqlite_get_marked_chains() {
    [[ "$SQLITE_MODULE_AVAILABLE" -ne 1 ]] && return 1

    sqlite3 -separator $'\t' "$SQLITE_DB_PATH" \
        "SELECT vm_name, period_id, marked_at, marked_by FROM chain_health
         WHERE chain_status = 'marked' ORDER BY marked_at;" 2>/dev/null
}

# Check if any backup session is currently running
# Used as a safety gate for destructive TUI operations (Tier 3 sweeps, bulk deletes)
# Returns: 0 if a session IS active (caller should NOT proceed), 1 if safe
# Output:  prints running session count to stdout
sqlite_is_session_active() {
    # Fail CLOSED: this gates destructive ops (bulk deletes / Tier-3 sweeps).
    # If the catalogue is unavailable or the COUNT query errors we cannot prove
    # the store is idle, so report "at least one session may be active": stdout
    # '1' (a nonzero integer, honoring the documented stdout contract for
    # numeric consumers) and rc 0 (caller must NOT proceed). AMENDED E18.
    if [[ "${SQLITE_MODULE_AVAILABLE:-0}" -ne 1 ]]; then
        echo "1"
        return 0
    fi

    local running_count _rc_q
    running_count=$(sqlite3 "$SQLITE_DB_PATH" \
        "SELECT COUNT(*) FROM sessions WHERE status = 'running';" 2>/dev/null)
    _rc_q=$?

    if [[ $_rc_q -ne 0 ]] || ! [[ "$running_count" =~ ^[0-9]+$ ]]; then
        echo "1"
        return 0
    fi

    if [[ "$running_count" -gt 0 ]]; then
        echo "$running_count"
        return 0
    fi
    echo "0"
    return 1
}

# Arguments:
#   $1 - vm_name
# Returns: JSON array of restorable chains
sqlite_get_restorable_chains() {
    local vm_name="$1"
    
    [[ "$SQLITE_MODULE_AVAILABLE" -ne 1 ]] && { echo "[]"; return 1; }
    
    local esc_vm
    esc_vm=$(_sql_escape "$vm_name")
    
    sqlite3 "$SQLITE_DB_PATH" << SQL_EOF
SELECT json_group_array(json_object(
    'vm_name', vm_name, 'period_id', period_id, 'chain_location', chain_location,
    'chain_status', chain_status, 'restorable_count', restorable_count,
    'total_checkpoints', total_checkpoints, 'broken_at', broken_at,
    'first_backup', first_backup, 'last_backup', last_backup
)) FROM (
    SELECT * FROM chain_health
    WHERE vm_name = '$esc_vm'
      AND chain_status IN ('active', 'archived', 'broken')
      AND restorable_count > 0
    ORDER BY updated_at DESC
);
SQL_EOF
}

# Get chain health summary for all VMs
# Returns: formatted table
sqlite_chain_health_summary() {
    [[ "$SQLITE_MODULE_AVAILABLE" -ne 1 ]] && return 1
    
    sqlite3 -header -column "$SQLITE_DB_PATH" << 'SQL_EOF'
SELECT 
    vm_name,
    COUNT(*) as total_chains,
    SUM(CASE WHEN chain_status = 'active' THEN 1 ELSE 0 END) as active,
    SUM(CASE WHEN chain_status = 'archived' THEN 1 ELSE 0 END) as archived,
    SUM(CASE WHEN chain_status = 'broken' THEN 1 ELSE 0 END) as broken,
    SUM(CASE WHEN chain_status = 'marked' THEN 1 ELSE 0 END) as marked,
    SUM(restorable_count) as total_restore_points
FROM chain_health
WHERE chain_status NOT IN ('deleted', 'purged')
GROUP BY vm_name
ORDER BY vm_name;
SQL_EOF
}

# Query the setting_value for a given setting_name from the previous session
# with the same config instance. Used for config change detection.
#
# Arguments:
#   $1 - setting_name to look up
# Returns: previous value on stdout (empty if no previous session or setting
#          not found); rc 2 on a sqlite3 query failure (FF-90 fail-closed)
sqlite_query_previous_config_value() {
    local setting_name="$1"
    
    [[ "$SQLITE_MODULE_AVAILABLE" -ne 1 ]] && return 1
    [[ -z "$SQLITE_CURRENT_SESSION_ID" ]] && return 1
    
    # Get current instance from the sessions table
    local current_instance
    current_instance=$(sqlite3 "$SQLITE_DB_PATH" 2>/dev/null \
        "SELECT instance FROM sessions WHERE id = $SQLITE_CURRENT_SESSION_ID LIMIT 1;")
    [[ -z "$current_instance" ]] && return 1
    
    # Query the most recent previous session with the same instance
    local prev_value
    prev_value=$(sqlite3 "$SQLITE_DB_PATH" 2>/dev/null << PREV_SQL
SELECT setting_value FROM config_events
WHERE setting_name = '$(_sql_escape "$setting_name")'
  AND session_id = (
    SELECT MAX(id) FROM sessions
    WHERE id < $SQLITE_CURRENT_SESSION_ID
      AND instance = '$(_sql_escape "$current_instance")'
  )
LIMIT 1;
PREV_SQL
)
    
    local _rc_prev=$?
    # FF-90 fail-closed: a query failure (rc 2, per this file's Phase-4
    # read-helper exit-code convention: 0 ok, 1 unavailable, 2 sqlite3 query
    # failure) must be distinguishable from 'setting absent' (rc 0, empty),
    # not silently read as a removed/absent setting.
    [[ $_rc_prev -ne 0 ]] && return 2
    echo "$prev_value"
    return 0
}

# Query all setting_names matching a prefix from the previous session
# with the same config instance. Used for removal detection.
#
# Arguments:
#   $1 - setting_name prefix (e.g. "LOCAL_DEST_" or "CLOUD_DEST_")
# Returns: newline-separated setting_names on stdout
sqlite_query_previous_config_settings() {
    local prefix="$1"
    
    [[ "$SQLITE_MODULE_AVAILABLE" -ne 1 ]] && return 1
    [[ -z "$SQLITE_CURRENT_SESSION_ID" ]] && return 1
    
    local current_instance
    current_instance=$(sqlite3 "$SQLITE_DB_PATH" 2>/dev/null \
        "SELECT instance FROM sessions WHERE id = $SQLITE_CURRENT_SESSION_ID LIMIT 1;")
    [[ -z "$current_instance" ]] && return 1
    
    # Escape the prefix's LIKE metachars, then append a literal '%' wildcard
    # (F-sql862 idiom). '_' in the real prefixes (LOCAL_DEST_ / CLOUD_DEST_) is
    # otherwise LIKE's single-char wildcard; over-match set is empty today,
    # defensive-by-consistency with the two escaped readers below.
    sqlite3 "$SQLITE_DB_PATH" 2>/dev/null << PREV_SETTINGS_SQL
SELECT setting_name FROM config_events
WHERE setting_name LIKE REPLACE(REPLACE('$(_sql_escape "$prefix")','%','\%'),'_','\_') || '%' ESCAPE '\'
  AND event_type = 'config_loaded'
  AND session_id = (
    SELECT MAX(id) FROM sessions
    WHERE id < $SQLITE_CURRENT_SESSION_ID
      AND instance = '$(_sql_escape "$current_instance")'
  );
PREV_SETTINGS_SQL
    
    return 0
}

# =============================================================================
# Phase 4 commit 4a: typed retention/email read helpers
# -----------------------------------------------------------------------------
# These wrap the per-call sqlite3 invocations that previously lived inline in
# modules/retention_module.sh and modules/email_report_module.sh. Each takes
# an explicit db_path so callers that resolve VMBACKUP_DB / BACKUP_PATH per
# call don't have to mutate SQLITE_DB_PATH. Exit codes:
#   0 success, value on stdout
#   1 db_path missing or unreadable
#   2 sqlite3 query failure (stderr suppressed)
# All take pre-validated string arguments and quote via _sql_escape.
# =============================================================================

# Count chain_health rows for VM/period that are protected (purge_eligible=0).
# Args: $1 db_path, $2 vm_name, $3 period_id
sqlite_get_protected_chain_count() {
    local db_path="$1" vm="$2" period="$3"
    [[ -z "$db_path" || ! -f "$db_path" ]] && return 1
    local esc_vm esc_p out
    esc_vm=$(_sql_escape "$vm")
    esc_p=$(_sql_escape "$period")
    out=$(sqlite3 "$db_path" \
        "SELECT COUNT(*) FROM chain_health
         WHERE vm_name='$esc_vm' AND period_id='$esc_p'
         AND purge_eligible = 0;" 2>/dev/null) || return 2
    echo "${out:-0}"
}

# Count successful replication runs (any VM). Used as "is replication
# configured & ever succeeded?" probe.
# Args: $1 db_path
sqlite_get_successful_replication_count() {
    local db_path="$1"
    [[ -z "$db_path" || ! -f "$db_path" ]] && return 1
    local out
    out=$(sqlite3 "$db_path" \
        "SELECT COUNT(*) FROM replication_runs WHERE status='success';" 2>/dev/null) || return 2
    echo "${out:-0}"
}

# Count successful replications of a specific VM+period.
# Args: $1 db_path, $2 vm_name, $3 period_id
sqlite_get_vm_period_replication_count() {
    local db_path="$1" vm="$2" period="$3"
    [[ -z "$db_path" || ! -f "$db_path" ]] && return 1
    local esc_vm esc_p out
    esc_vm=$(_sql_escape "$vm")
    esc_p=$(_sql_escape "$period")
    # Anchor the period as the FINAL path segment. backup_path stores the bare
    # period leaf (…/<vm>/<period>, get_vm_backup_dir), so ends-with ('%/'||<p>)
    # isolates the period's own rows; the old '%/${period}%' substring let a
    # monthly id over-match same-prefix daily dirs (61→3 live). REPLACE escapes
    # LIKE metachars _/% (period ids have none today; defensive). NOT a trailing
    # '/%' — that matches zero bare-leaf rows → fail-open data loss (F-sql862).
    out=$(sqlite3 "$db_path" \
        "SELECT COUNT(*) FROM replication_vms rv
         JOIN replication_runs rr ON rv.run_id = rr.id
         WHERE rv.vm_name = '$esc_vm'
         AND rr.status = 'success'
         AND rr.session_id IN (
             SELECT session_id FROM vm_backups
             WHERE vm_name = '$esc_vm'
             AND backup_path LIKE '%/' || REPLACE(REPLACE('$esc_p','%','\%'),'_','\_') ESCAPE '\'
             AND status = 'success'
         );" 2>/dev/null) || return 2
    echo "${out:-0}"
}

# Last successful backup created_at (UTC bare) for VM+period.
# Args: $1 db_path, $2 vm_name, $3 period_id
sqlite_get_last_successful_backup_at() {
    local db_path="$1" vm="$2" period="$3"
    [[ -z "$db_path" || ! -f "$db_path" ]] && return 1
    local esc_vm esc_p out
    esc_vm=$(_sql_escape "$vm")
    esc_p=$(_sql_escape "$period")
    # Ends-with anchor on the bare period leaf (see sqlite_get_vm_period_replication_count):
    # excludes same-prefix daily dirs so orphan-age reads THIS period's newest
    # backup, not a foreign daily's. NOT trailing '/%' (zero rows → age 9999 →
    # delete-everything). F-sql862.
    out=$(sqlite3 "$db_path" \
        "SELECT MAX(created_at) FROM vm_backups
         WHERE vm_name='$esc_vm'
         AND backup_path LIKE '%/' || REPLACE(REPLACE('$esc_p','%','\%'),'_','\_') ESCAPE '\'
         AND status='success';" 2>/dev/null) || return 2
    echo "$out"
}

# All tracked period_ids for VM in non-deleted/non-purged states (for
# reconcile pass 1).
# Args: $1 db_path, $2 vm_name
sqlite_get_tracked_periods() {
    local db_path="$1" vm="$2"
    [[ -z "$db_path" || ! -f "$db_path" ]] && return 1
    local esc_vm
    esc_vm=$(_sql_escape "$vm")
    sqlite3 "$db_path" \
        "SELECT period_id FROM chain_health
         WHERE vm_name='$esc_vm'
         AND chain_status IN ('active', 'archived', 'broken', 'marked');" 2>/dev/null \
        || return 2
}

# Count chain_health rows for VM+period (any status) — for reconcile pass 2
# "does the DB know about this period at all?" probe.
# Args: $1 db_path, $2 vm_name, $3 period_id
sqlite_chain_period_exists_count() {
    local db_path="$1" vm="$2" period="$3"
    [[ -z "$db_path" || ! -f "$db_path" ]] && return 1
    local esc_vm esc_p out
    esc_vm=$(_sql_escape "$vm")
    esc_p=$(_sql_escape "$period")
    out=$(sqlite3 "$db_path" \
        "SELECT COUNT(*) FROM chain_health
         WHERE vm_name='$esc_vm' AND period_id='$esc_p';" 2>/dev/null) || return 2
    echo "${out:-0}"
}

# Count chain_created events already recorded for a VM's chain (FF-130 dedup).
# post_backup_hook uses this to fire the chain_created lifecycle event exactly
# once per logical chain, keyed on chain_id (the canonical chain identity).
# Read-only SELECT — complies with the sqlite_ro.sh read-only contract.
# Args: $1 db_path, $2 vm_name, $3 chain_id
sqlite_get_chain_created_count() {
    local db_path="$1" vm="$2" chain="$3"
    [[ -z "$db_path" || ! -f "$db_path" ]] && return 1
    local esc_vm esc_chain out
    esc_vm=$(_sql_escape "$vm")
    esc_chain=$(_sql_escape "$chain")
    out=$(sqlite3 "$db_path" \
        "SELECT COUNT(*) FROM chain_events
         WHERE vm_name='$esc_vm' AND chain_id='$esc_chain'
         AND event_type='chain_created';" 2>/dev/null) || return 2
    echo "${out:-0}"
}

# Per-VM chain summary for email report. Pipe-separated rows:
#   vm_name|total_chains|active|archived|broken|restore_points
# Excludes deleted+purged (vs sqlite_query_chain_health which excludes
# only deleted — kept distinct to preserve byte-identical email body).
# Args: (none — uses SQLITE_DB_PATH; SQLITE_MODULE_AVAILABLE check enforced)
sqlite_query_chain_health_email() {
    [[ "${SQLITE_MODULE_AVAILABLE:-0}" -ne 1 ]] && return 1
    [[ -z "${SQLITE_DB_PATH:-}" ]] && return 1
    sqlite3 -separator '|' "$SQLITE_DB_PATH" \
        "SELECT vm_name,
                COUNT(*) as total_chains,
                SUM(CASE WHEN chain_status='active' THEN 1 ELSE 0 END) as active,
                SUM(CASE WHEN chain_status='archived' THEN 1 ELSE 0 END) as archived,
                SUM(CASE WHEN chain_status='broken' THEN 1 ELSE 0 END) as broken,
                SUM(restorable_count) as restore_points
         FROM chain_health
         WHERE chain_status NOT IN ('deleted','purged')
         GROUP BY vm_name
         ORDER BY vm_name;" 2>/dev/null
}

# Export public function names
export -f sqlite_init_readonly
export -f sqlite_query_today_sessions
export -f sqlite_query_session_vm_backups_display
export -f sqlite_query_session_retention_summary
export -f sqlite_query_session_replication_summary
export -f sqlite_query_vm_history
export -f sqlite_query_last_success
export -f sqlite_query_recent_failures
export -f sqlite_query_today_replications
export -f sqlite_query_chain_health
export -f sqlite_query_storage_growth
export -f sqlite_query_storage_trends
export -f sqlite_query_storage_destination_summary
export -f sqlite_query_policy_summary
export -f sqlite_query_session_vm_backups
export -f sqlite_query_session_replication
export -f sqlite_query_session_summary
export -f sqlite_is_available
export -f sqlite_get_last_rotation_policy
export -f sqlite_get_db_path
export -f sqlite_get_schema_version
export -f sqlite_get_marked_chains
export -f sqlite_is_session_active
export -f sqlite_get_restorable_chains
export -f sqlite_chain_health_summary
export -f sqlite_query_previous_config_value
export -f sqlite_query_previous_config_settings
export -f sqlite_get_protected_chain_count
export -f sqlite_get_successful_replication_count
export -f sqlite_get_vm_period_replication_count
export -f sqlite_get_last_successful_backup_at
export -f sqlite_get_tracked_periods
export -f sqlite_chain_period_exists_count
export -f sqlite_get_chain_created_count
export -f sqlite_query_chain_health_email
