#!/bin/bash

#################################################################################
# Logging Module - Structured Event Logging for VM-First Backup Structure
#
# Provides structured logging for backup activities with full chain and
# period tracking. All event data is written to the SQLite database
# (via sqlite_module.sh functions).
#
# Event Types Logged (to SQLite):
#   chain_events      - Chain birth/archive/delete
#   period_events     - Period lifecycle
#   file_operations   - File audit trail
#   retention_events  - Cleanup decisions
#   config_events     - Configuration events
#
# Dependencies:
#   - STATE_DIR: State directory
#   - sqlite_log_*: DB logging functions from lib/sqlite_module.sh
#   - log_info, log_warn, log_error, log_debug: Base logging functions
#
# Usage:
#   source logging_module.sh
#   log_chain_lifecycle "chain_created" "vm-name" "chain-123" ...
#   log_period_lifecycle "period_created" "vm-name" "202602" ...
#
#################################################################################

# Guard against multiple inclusion
[[ -n "${_LOGGING_MODULE_LOADED:-}" ]] && return 0
readonly _LOGGING_MODULE_LOADED=1

# Module version
readonly LOGGING_MODULE_VERSION="3.0"

#################################################################################
# CONFIGURATION
#################################################################################

# File operations checksum mode (false = size:mtime, true = MD5)
FILE_OPS_CHECKSUMS="${FILE_OPS_CHECKSUMS:-false}"

# State backup retention (days to keep state-*.tar.gz archives)
STATE_BACKUP_KEEP_DAYS="${STATE_BACKUP_KEEP_DAYS:-90}"

# Log file retention (days to keep live log files before cleanup)
# Logs are captured in state backups before deletion
LOG_KEEP_DAYS="${LOG_KEEP_DAYS:-30}"

#################################################################################
# CHAIN LIFECYCLE LOGGING
#################################################################################

# Log chain lifecycle event
# Args: $1  - event_type (chain_created|chain_archived|chain_deleted)
#       $2  - vm_name
#       $3  - chain_id
#       $4  - period_id
#       $5  - backup_dir
#       $6  - chain_location (. for active, .chain-xxx/ for archived)
#       $7  - checkpoint_count
#       $8  - total_chain_bytes
#       $9  - archive_reason (optional)
#       $10 - archive_trigger (optional)
#       $11 - source_backup_type (full|copy)
#       $12 - covers_from (ISO8601)
#       $13 - covers_to (ISO8601)
log_chain_lifecycle() {
    [[ "${DRY_RUN:-false}" == true ]] && return 0
    local event_type="$1"
    local vm_name="$2"
    local chain_id="$3"
    local period_id="$4"
    local backup_dir="$5"
    local chain_location="$6"
    local checkpoint_count="${7:-0}"
    local total_chain_bytes="${8:-0}"
    local archive_reason="${9:-}"
    local archive_trigger="${10:-}"
    local source_backup_type="${11:-full}"
    local covers_from="${12:-}"
    local covers_to="${13:-}"
    
    local full_backup_file=""
    local restore_point_ids=""
    
    # Write to SQLite database (primary destination since v1.5)
    if type sqlite_log_chain_event &>/dev/null; then
        sqlite_log_chain_event \
            "$event_type" "$vm_name" "$chain_id" "$period_id" \
            "$backup_dir" "$chain_location" "$checkpoint_count" \
            "$total_chain_bytes" "$archive_reason" "$archive_trigger" \
            "$source_backup_type" "$covers_from" "$covers_to" \
            "$full_backup_file" "$restore_point_ids"
    fi
    
    log_debug "logging_module.sh" "log_chain_lifecycle" \
        "Logged $event_type: chain=$chain_id vm=$vm_name"
}

#################################################################################
# PERIOD LIFECYCLE LOGGING
#################################################################################

# Log period lifecycle event
# Args: $1  - event_type (period_created|period_closed|period_archived|period_deleted)
#       $2  - vm_name
#       $3  - period_id
#       $4  - rotation_policy
#       $5  - period_dir
#       $6  - period_start (ISO8601, optional)
#       $7  - period_end (ISO8601, optional)
#       $8  - chains_count
#       $9  - total_restore_points
#       $10 - total_bytes
#       $11 - previous_period (optional)
#       $12 - archive_location (optional)
#       $13 - retention_remaining
log_period_lifecycle() {
    [[ "${DRY_RUN:-false}" == true ]] && return 0
    local event_type="$1"
    local vm_name="$2"
    local period_id="$3"
    local rotation_policy="$4"
    local period_dir="$5"
    local period_start="${6:-}"
    local period_end="${7:-}"
    local chains_count="${8:-0}"
    local total_restore_points="${9:-0}"
    local total_bytes="${10:-0}"
    local previous_period="${11:-}"
    local archive_location="${12:-}"
    local retention_remaining="${13:-0}"
    
    # Write to SQLite database (primary destination since v1.5)
    if type sqlite_log_period_event &>/dev/null; then
        sqlite_log_period_event \
            "$event_type" "$vm_name" "$period_id" "$rotation_policy" \
            "$period_dir" "$period_start" "$period_end" "$chains_count" \
            "$total_restore_points" "$total_bytes" "$previous_period" \
            "$archive_location" "$retention_remaining"
    fi
    
    log_debug "logging_module.sh" "log_period_lifecycle" \
        "Logged $event_type: period=$period_id vm=$vm_name"
}

#################################################################################
# FILE OPERATIONS LOGGING
#################################################################################

# Log file operation
# Args: $1  - operation (create|move|copy|delete|rename)
#       $2  - vm_name
#       $3  - source_path
#       $4  - dest_path (optional, for move/copy/rename)
#       $5  - file_type (full_backup|inc_backup|copy_backup|checkpoint_xml|config_xml|manifest|directory|host_config)
#       $6  - reason
#       $7  - triggered_by (function name)
#       $8  - success (true|false)
#       $9  - error_message (optional)
log_file_operation() {
    [[ "${DRY_RUN:-false}" == true ]] && return 0
    local operation="$1"
    local vm_name="$2"
    local source_path="$3"
    local dest_path="${4:-}"
    local file_type="$5"
    local reason="$6"
    local triggered_by="${7:-}"
    local success="${8:-true}"
    local error_message="${9:-}"
    
    local file_size_bytes=0
    local verification_data=""
    
    # Get file info if file exists
    if [[ -f "$source_path" ]]; then
        file_size_bytes=$(stat -c %s "$source_path" 2>/dev/null || echo 0)
        
        if [[ "$FILE_OPS_CHECKSUMS" == "true" ]]; then
            verification_data=$(md5sum "$source_path" 2>/dev/null | cut -d' ' -f1)
        else
            local mtime
            mtime=$(stat -c %Y "$source_path" 2>/dev/null || echo 0)
            verification_data="size:${file_size_bytes}:mtime:${mtime}"
        fi
    elif [[ -d "$source_path" ]]; then
        file_size_bytes=$(du -sb "$source_path" 2>/dev/null | cut -f1 || echo 0)
        verification_data="directory"
    fi
    
    log_debug "logging_module.sh" "log_file_operation" "$operation: $vm_name file=$source_path type=$file_type reason=$reason success=$success"
    
    # Write to SQLite database (primary destination since v1.5)
    if type sqlite_log_file_operation &>/dev/null; then
        sqlite_log_file_operation \
            "$operation" "$vm_name" "$source_path" "$dest_path" \
            "$file_type" "$file_size_bytes" "$verification_data" \
            "$reason" "$triggered_by" "$success" "$error_message"
    fi
}

#################################################################################
# RETENTION LOGGING
#################################################################################

# Log retention action
# Args: $1  - action (delete|archive|keep|skip|error)
#       $2  - vm_name
#       $3  - target_type (period|chain|orphan_file)
#       $4  - target_path
#       $5  - target_period
#       $6  - rotation_policy
#       $7  - retention_limit
#       $8  - current_count
#       $9  - age_days
#       $10 - freed_bytes
#       $11 - preserve_reason (for keep/skip)
#       $12 - triggered_by
#       $13 - success (true|false)
log_retention_action() {
    [[ "${DRY_RUN:-false}" == true ]] && return 0
    local action="$1"
    local vm_name="$2"
    local target_type="$3"
    local target_path="$4"
    local target_period="$5"
    local rotation_policy="$6"
    local retention_limit="${7:-0}"
    local current_count="${8:-0}"
    local age_days="${9:-0}"
    local freed_bytes="${10:-0}"
    local preserve_reason="${11:-}"
    local triggered_by="${12:-}"
    local success="${13:-true}"
    
    log_debug "logging_module.sh" "log_retention_action" "$action: $vm_name target=$target_path type=$target_type policy=$rotation_policy success=$success"
    
    # Write to SQLite database (primary destination since v1.5)
    if type sqlite_log_retention_event &>/dev/null; then
        sqlite_log_retention_event \
            "$action" "$vm_name" "$target_type" "$target_path" \
            "$target_period" "$rotation_policy" "$retention_limit" \
            "$current_count" "$age_days" "$freed_bytes" \
            "$preserve_reason" "$triggered_by" "$success"
    fi
}

#################################################################################
# CONFIG EVENTS LOGGING
#################################################################################

# Log configuration event
# Args: $1  - event_type (config_loaded|policy_applied|policy_override|policy_changed|
#                         config_missing|config_error|script_start|script_end|
#                         audit_start|audit_complete|manifest_rebuilt|state_backup|
#                         csv_recovered|lock_contention)
#       $2  - config_source (path)
#       $3  - vm_name (optional, empty for global)
#       $4  - setting_name
#       $5  - setting_value
#       $6  - previous_value (optional)
#       $7  - applied_to (all_vms|vm_name)
#       $8  - triggered_by
#       $9  - detail (optional)
log_config_event() {
    [[ "${DRY_RUN:-false}" == true ]] && return 0
    local event_type="$1"
    local config_source="${2:-}"
    local vm_name="${3:-}"
    local setting_name="${4:-}"
    local setting_value="${5:-}"
    local previous_value="${6:-}"
    local applied_to="${7:-}"
    local triggered_by="${8:-}"
    local detail="${9:-}"
    
    # Write to SQLite database (primary destination since v1.5)
    # Note: uses silent failure to avoid infinite loops
    # since config events can be triggered by error handling
    if type sqlite_log_config_event &>/dev/null; then
        sqlite_log_config_event \
            "$event_type" "$config_source" "$vm_name" "$setting_name" \
            "$setting_value" "$previous_value" "$applied_to" \
            "$triggered_by" "$detail"
    fi
}

#################################################################################
# STATE BACKUP & RECOVERY
#################################################################################

# Cleanup old log files
# Called after state backup to ensure logs are archived before deletion
cleanup_old_logs() {
    local keep_days="${LOG_KEEP_DAYS:-30}"
    local deleted_count=0
    local deleted_bytes=0
    
    # Per-VM backup logs (backup_<vm>_<epoch>.log)
    while IFS= read -r -d '' file; do
        local size
        size=$(stat -c%s "$file" 2>/dev/null || echo 0)
        ((deleted_bytes += size))
        ((deleted_count++))
        rm -f "$file"
    done < <(find "${STATE_DIR}/logs" -name "backup_*.log" -mtime +"$keep_days" -print0 2>/dev/null)
    
    # Replication logs (cloud and local)
    while IFS= read -r -d '' file; do
        local size
        size=$(stat -c%s "$file" 2>/dev/null || echo 0)
        ((deleted_bytes += size))
        ((deleted_count++))
        rm -f "$file"
    done < <(find "${STATE_DIR}/replication_logs" -name "*.log" -mtime +"$keep_days" -print0 2>/dev/null)
    
    # Email debug logs
    while IFS= read -r -d '' file; do
        local size
        size=$(stat -c%s "$file" 2>/dev/null || echo 0)
        ((deleted_bytes += size))
        ((deleted_count++))
        rm -f "$file"
    done < <(find "${STATE_DIR}/email" -name "email-*.txt" -mtime +"$keep_days" -print0 2>/dev/null)
    
    if ((deleted_count > 0)); then
        log_info "logging_module.sh" "cleanup_old_logs" \
            "Cleaned up $deleted_count log files ($(numfmt --to=iec-i --suffix=B $deleted_bytes 2>/dev/null || echo "${deleted_bytes}B")) older than ${keep_days} days"
    fi
}

# Create daily state backup
# Should be called at script start
backup_state_files() {
    local backup_dir="${STATE_DIR}/backups"
    local today
    today=$(date +%Y%m%d)
    local backup_file="${backup_dir}/state-${today}.tar.gz"
    
    mkdir -p "$backup_dir"
    
    # Skip if already backed up today
    if [[ -f "$backup_file" ]]; then
        log_debug "logging_module.sh" "backup_state_files" \
            "State backup exists: $backup_file"
        return 0
    fi
    
    # Check if there's anything to backup
    if [[ ! -d "${STATE_DIR}/manifests" ]]; then
        log_debug "logging_module.sh" "backup_state_files" \
            "No state files to backup yet"
        return 0
    fi
    
    # Create backup
    tar -czf "$backup_file" \
        -C "$STATE_DIR" \
        --exclude='backups' \
        --exclude='*.lock' \
        . 2>/dev/null || {
            log_warn "logging_module.sh" "backup_state_files" \
                "State backup failed (non-fatal)"
            return 0
        }
    
    log_info "logging_module.sh" "backup_state_files" \
        "Created state backup: $backup_file"
    
    # Cleanup old backups
    find "$backup_dir" -name "state-*.tar.gz" -mtime +"${STATE_BACKUP_KEEP_DAYS}" -delete 2>/dev/null
    
    # Cleanup old log files (after they've been captured in tar)
    cleanup_old_logs
    
    log_config_event "state_backup" "$backup_file" "" "state_backup" \
        "$(date -Iseconds)" "" "" "backup_state_files"
    
    return 0
}

#################################################################################
# MODULE INITIALIZATION
#################################################################################

log_debug "logging_module.sh" "init" \
    "Logging module v${LOGGING_MODULE_VERSION} loaded"
