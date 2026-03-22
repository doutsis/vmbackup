#!/bin/bash
#################################################################################
# Cloud Replication Module for vmbackup.sh
#
# Replicates VM backups to cloud storage providers (SharePoint, Backblaze, etc.)
# Called by vmbackup.sh after local replication completes.
#
# Version: 1.1
# Created: 2026-01-26
# Updated: 2026-02-02 (per-instance configuration)
#
# Dependencies:
#   - rclone (installed and configured)
#   - config/<instance>/replication_cloud.conf (per-instance configuration)
#   - cloud_transports/*.sh (provider-specific drivers)
#
# Usage:
#   source replication_cloud_module.sh
#   run_cloud_replication_batch "/path/to/backup"
#
#################################################################################

# Module version
CLOUD_REPLICATION_MODULE_VERSION="1.1"

# Script directory (where this module lives)
CLOUD_REPLICATION_MODULE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Project root directory (one level up from modules/)
_CLOUD_PROJECT_ROOT="${SCRIPT_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"

# Configuration file path - uses instance config directory
# CONFIG_INSTANCE is exported by vmbackup.sh
_cloud_instance="${CONFIG_INSTANCE:-default}"
CLOUD_REPLICATION_CONF="${_CLOUD_PROJECT_ROOT}/config/${_cloud_instance}/replication_cloud.conf"

# Cloud transports directory
CLOUD_TRANSPORTS_DIR="${_CLOUD_PROJECT_ROOT}/cloud_transports"

# State tracking
declare -g CLOUD_REPLICATION_MODULE_LOADED=0
declare -g CLOUD_REPLICATION_INITIALIZED=0
declare -g CLOUD_REPLICATION_TOTAL_BYTES=0
declare -g CLOUD_REPLICATION_TOTAL_FILES=0
declare -g CLOUD_REPLICATION_TOTAL_ERRORS=0
declare -g CLOUD_REPLICATION_START_TIME=""
declare -g CLOUD_REPLICATION_END_TIME=""

# Results arrays for reporting
declare -ga CLOUD_REPLICATION_DEST_STATUS=()
declare -ga CLOUD_REPLICATION_CREDENTIAL_WARNINGS=()
declare -gA CLOUD_REPLICATION_DEST_PROVIDER=()   # Provider: sharepoint/backblaze/etc
declare -gA CLOUD_REPLICATION_DEST_REMOTE=()     # Remote name in rclone config

# Per-destination transport metrics (for DB logging, metrics contract v1.0)
declare -gA CLOUD_REPLICATION_DEST_THROTTLE=()   # Throttle events (0 = none occurred)
declare -gA CLOUD_REPLICATION_DEST_BWLIMIT=()    # Final bwlimit after adjustments
declare -gA CLOUD_REPLICATION_DEST_AVAIL=()      # Free bytes at dest (0 if unknown)
declare -gA CLOUD_REPLICATION_DEST_TOTAL=()      # Total bytes at dest (0 if unknown)
declare -gA CLOUD_REPLICATION_DEST_SPACE_KNOWN=() # 0|1 whether space metrics reliable
#=============================================================================
# LOGGING FUNCTIONS
#
# Log level hierarchy: ERROR > WARN > INFO > DEBUG
# 
# When running within vmbackup.sh context:
#   - Uses main LOG_LEVEL from vmbackup.conf (unified logging)
#   - CLOUD_REPLICATION_LOG_LEVEL can override for cloud-specific verbosity
#
# Standalone mode:
#   - Uses CLOUD_REPLICATION_LOG_LEVEL from replication_cloud.conf
#=============================================================================

# Get effective log level - unifies with main LOG_LEVEL when available
_cloud_get_effective_log_level() {
    # If CLOUD_REPLICATION_LOG_LEVEL is explicitly set, use it (override)
    if [[ -n "${CLOUD_REPLICATION_LOG_LEVEL:-}" ]]; then
        echo "${CLOUD_REPLICATION_LOG_LEVEL,,}"  # lowercase
        return
    fi
    
    # Otherwise inherit from main LOG_LEVEL if available
    if [[ -n "${LOG_LEVEL:-}" ]]; then
        echo "${LOG_LEVEL,,}"  # lowercase
        return
    fi
    
    # Default
    echo "info"
}

cloud_log() {
    local level="$1"
    local message="$2"
    local timestamp
    timestamp=$(date '+%Y-%m-%d %H:%M:%S %Z')
    
    # Map to numeric level for comparison
    local -A levels=([debug]=0 [info]=1 [warn]=2 [error]=3)
    local effective_level
    effective_level=$(_cloud_get_effective_log_level)
    local current_level="${levels[$effective_level]:-1}"
    local msg_level="${levels[$level]:-1}"
    
    # Only log if message level >= configured level
    if [[ $msg_level -ge $current_level ]]; then
        local level_upper
        level_upper=$(echo "$level" | tr '[:lower:]' '[:upper:]')
        local caller_func="${FUNCNAME[2]:-main}"  # Get calling function (skip cloud_log wrapper)
        
        # Use vmbackup's log_msg function if available (logs to file + console)
        if declare -f log_msg >/dev/null 2>&1; then
            log_msg "$level_upper" "replication_cloud_module.sh" "$caller_func" "$message"
        else
            # Standalone mode - log to console
            echo "[$timestamp] [$level_upper] [replication_cloud_module.sh] [$caller_func] $message"
        fi
    fi
}

cloud_log_debug() { cloud_log "debug" "$1"; }
cloud_log_info()  { cloud_log "info" "$1"; }
cloud_log_warn()  { cloud_log "warn" "$1"; }
cloud_log_error() { cloud_log "error" "$1"; }

#=============================================================================
# INITIALIZATION
#=============================================================================

cloud_replication_load_config() {
    # Load configuration file - instance config is REQUIRED (no fallback)
    if [[ ! -f "$CLOUD_REPLICATION_CONF" ]]; then
        local instance="${CONFIG_INSTANCE:-default}"
        cloud_log_error "NO CONFIG FILES FOR $instance - missing $CLOUD_REPLICATION_CONF"
        return 1
    fi
    
    # shellcheck source=/dev/null
    source "$CLOUD_REPLICATION_CONF"
    
    cloud_log_debug "Loaded configuration from $CLOUD_REPLICATION_CONF"
    return 0
}

cloud_replication_load_transports() {
    # Load provider-specific transport drivers
    if [[ ! -d "$CLOUD_TRANSPORTS_DIR" ]]; then
        cloud_log_error "Cloud transports directory not found: $CLOUD_TRANSPORTS_DIR"
        return 1
    fi
    
    local transport_count=0
    for transport_file in "$CLOUD_TRANSPORTS_DIR"/cloud_transport_*.sh; do
        if [[ -f "$transport_file" ]]; then
            # shellcheck source=/dev/null
            source "$transport_file"
            transport_count=$((transport_count + 1))
            cloud_log_debug "Loaded transport: $(basename "$transport_file")"
        fi
    done
    
    if [[ $transport_count -eq 0 ]]; then
        cloud_log_warn "No cloud transport drivers found in $CLOUD_TRANSPORTS_DIR"
    else
        cloud_log_debug "Loaded $transport_count cloud transport driver(s)"
    fi
    
    return 0
}

cloud_replication_check_dependencies() {
    # Check rclone is installed
    if ! command -v rclone &>/dev/null; then
        cloud_log_error "rclone is not installed. Please install rclone first."
        return 1
    fi
    
    local rclone_version
    rclone_version=$(rclone version | head -1)
    cloud_log_debug "Found $rclone_version"
    
    return 0
}

cloud_replication_init() {
    # Initialize the module
    if [[ $CLOUD_REPLICATION_INITIALIZED -eq 1 ]]; then
        cloud_log_debug "Cloud replication module already initialized"
        return 0
    fi
    
    cloud_log_info "Initializing cloud replication module v${CLOUD_REPLICATION_MODULE_VERSION}"
    
    # Load configuration
    if ! cloud_replication_load_config; then
        return 1
    fi
    
    # Check if cloud replication is enabled
    if [[ "${CLOUD_REPLICATION_ENABLED:-no}" != "yes" ]]; then
        cloud_log_info "Cloud replication is disabled in configuration"
        # Log disabled state to database
        _log_cloud_replication_config "no"
        return 0
    fi
    
    # Check dependencies
    if ! cloud_replication_check_dependencies; then
        return 1
    fi
    
    # Load transport drivers
    if ! cloud_replication_load_transports; then
        return 1
    fi
    
    CLOUD_REPLICATION_INITIALIZED=1
    cloud_log_info "Cloud replication module initialized successfully"
    
    # Log config state to database (config_loaded events + change detection)
    _log_cloud_replication_config "yes"
    
    return 0
}

#=============================================================================
# CONFIG STATE LOGGING
#=============================================================================

# Log cloud replication config state to database
# Logs config_loaded events for global enabled state and each destination.
# Detects changes from previous session and logs config_changed events.
#
# Arguments:
#   $1 - enabled state ("yes" or "no")
_log_cloud_replication_config() {
    local enabled_state="$1"
    local config_file="${CLOUD_REPLICATION_CONF:-}"
    
    # Skip DB writes in dry-run mode (matches sqlite session policy)
    [[ "${DRY_RUN:-false}" == true ]] && return 0
    
    # Bail if sqlite not available
    if ! type sqlite_log_config_event &>/dev/null; then
        return 0
    fi
    if ! type sqlite_is_available &>/dev/null || ! sqlite_is_available; then
        return 0
    fi
    
    # Log global enabled state
    sqlite_log_config_event "config_loaded" "$config_file" "" \
        "CLOUD_REPLICATION_ENABLED" "$enabled_state" "" "replication_cloud" "load_config"
    
    # Check for change from previous session
    if type sqlite_query_previous_config_value &>/dev/null; then
        local prev_enabled
        prev_enabled=$(sqlite_query_previous_config_value "CLOUD_REPLICATION_ENABLED")
        if [[ -n "$prev_enabled" ]] && [[ "$prev_enabled" != "$enabled_state" ]]; then
            sqlite_log_config_event "config_changed" "$config_file" "" \
                "CLOUD_REPLICATION_ENABLED" "$enabled_state" "$prev_enabled" "replication_cloud" "load_config"
        fi
    fi
    
    # If disabled, no destination detail to log
    [[ "$enabled_state" != "yes" ]] && return 0
    
    # Log per-destination config
    local n=1
    while [[ $n -le 99 ]]; do
        local enabled_check
        eval "enabled_check=\${CLOUD_DEST_${n}_ENABLED+set}"
        [[ -z "$enabled_check" ]] && break
        
        local dest_name dest_enabled dest_provider dest_remote dest_path dest_sync_mode
        eval "dest_name=\${CLOUD_DEST_${n}_NAME:-cloud_dest_$n}"
        eval "dest_enabled=\${CLOUD_DEST_${n}_ENABLED:-no}"
        eval "dest_provider=\${CLOUD_DEST_${n}_PROVIDER:-unknown}"
        eval "dest_remote=\${CLOUD_DEST_${n}_REMOTE:-}"
        eval "dest_path=\${CLOUD_DEST_${n}_PATH:-}"
        eval "dest_sync_mode=\${CLOUD_DEST_${n}_SYNC_MODE:-$CLOUD_REPLICATION_SYNC_MODE}"
        
        local setting_name="CLOUD_DEST_${dest_name}"
        local setting_value="${dest_provider}:${dest_sync_mode}:${dest_remote}${dest_path}"
        local applied_to="$dest_enabled"
        
        sqlite_log_config_event "config_loaded" "$config_file" "" \
            "$setting_name" "$setting_value" "" "$applied_to" "load_config" \
            "provider=$dest_provider remote=$dest_remote path=$dest_path"
        
        # Check for change from previous session
        if type sqlite_query_previous_config_value &>/dev/null; then
            local prev_value
            prev_value=$(sqlite_query_previous_config_value "$setting_name")
            if [[ -n "$prev_value" ]] && [[ "$prev_value" != "$setting_value" ]]; then
                sqlite_log_config_event "config_changed" "$config_file" "" \
                    "$setting_name" "$setting_value" "$prev_value" "$applied_to" "load_config"
            fi
        fi
        
        n=$((n + 1))
    done
    
    # Detect removed destinations: query previous session for CLOUD_DEST_* settings
    # that no longer exist in the current config
    if type sqlite_query_previous_config_settings &>/dev/null; then
        local prev_settings current_names
        prev_settings=$(sqlite_query_previous_config_settings "CLOUD_DEST_")
        
        # Build list of current destination setting names
        current_names=""
        local cn=1
        while [[ $cn -le 99 ]]; do
            local cn_check
            eval "cn_check=\${CLOUD_DEST_${cn}_ENABLED+set}"
            [[ -z "$cn_check" ]] && break
            local cn_name
            eval "cn_name=\${CLOUD_DEST_${cn}_NAME:-cloud_dest_$cn}"
            current_names="${current_names} CLOUD_DEST_${cn_name}"
            cn=$((cn + 1))
        done
        
        # Check each previous setting against current set
        while IFS= read -r prev_setting; do
            [[ -z "$prev_setting" ]] && continue
            if [[ " $current_names " != *" $prev_setting "* ]]; then
                local prev_value
                prev_value=$(sqlite_query_previous_config_value "$prev_setting")
                sqlite_log_config_event "config_removed" "$config_file" "" \
                    "$prev_setting" "" "$prev_value" "" "load_config" \
                    "destination removed from config"
            fi
        done <<< "$prev_settings"
    fi
    
    return 0
}

#=============================================================================
# LOCK FILE MANAGEMENT
#=============================================================================

cloud_replication_acquire_lock() {
    if [[ "${CLOUD_REPLICATION_USE_LOCKFILE:-yes}" != "yes" ]]; then
        return 0
    fi
    
    local lockfile="${CLOUD_REPLICATION_LOCKFILE:-/var/run/cloud_replication.lock}"
    local timeout="${CLOUD_REPLICATION_LOCK_TIMEOUT:-3600}"
    
    # Check if lockfile exists
    if [[ -f "$lockfile" ]]; then
        local lock_pid
        lock_pid=$(cat "$lockfile" 2>/dev/null)
        
        # Check if process is still running
        if [[ -n "$lock_pid" ]] && kill -0 "$lock_pid" 2>/dev/null; then
            cloud_log_error "Cloud replication already running (PID $lock_pid)"
            return 1
        fi
        
        # Check lock age
        local lock_age
        lock_age=$(( $(date +%s) - $(stat -c %Y "$lockfile" 2>/dev/null || echo 0) ))
        
        if [[ $lock_age -gt $timeout ]]; then
            cloud_log_warn "Breaking stale lock (PID $lock_pid not running, lock age ${lock_age}s > ${timeout}s timeout)"
            rm -f "$lockfile"
        else
            cloud_log_error "Lock file exists but process not found. Lock age: ${lock_age}s"
            return 1
        fi
    fi
    
    # Create lock file
    echo $$ > "$lockfile"
    cloud_log_debug "Acquired lock file: $lockfile (PID $$)"
    
    # Save parent EXIT trap, then set compound trap that does both:
    # release our lock AND run the parent's cleanup (session end, lock removal, etc.)
    _CLOUD_REPL_SAVED_EXIT_TRAP=$(trap -p EXIT | sed "s/^trap -- '//;s/' EXIT$//")
    trap 'cloud_replication_release_lock; eval "$_CLOUD_REPL_SAVED_EXIT_TRAP"' EXIT
    
    return 0
}

cloud_replication_release_lock() {
    if [[ "${CLOUD_REPLICATION_USE_LOCKFILE:-yes}" != "yes" ]]; then
        return 0
    fi
    
    local lockfile="${CLOUD_REPLICATION_LOCKFILE:-/var/run/cloud_replication.lock}"
    
    if [[ -f "$lockfile" ]]; then
        local lock_pid
        lock_pid=$(cat "$lockfile" 2>/dev/null)
        
        # Only remove if we own the lock
        if [[ "$lock_pid" == "$$" ]]; then
            rm -f "$lockfile"
            cloud_log_debug "Released lock file: $lockfile"
        fi
    fi
    
    # Restore parent EXIT trap now that our lock is released
    if [[ -n "${_CLOUD_REPL_SAVED_EXIT_TRAP:-}" ]]; then
        trap "$_CLOUD_REPL_SAVED_EXIT_TRAP" EXIT
        unset _CLOUD_REPL_SAVED_EXIT_TRAP
    fi
}

#=============================================================================
# CREDENTIAL EXPIRY TRACKING
#=============================================================================

cloud_replication_check_credential_expiry() {
    local dest_num="$1"
    local dest_name
    local expiry_date
    
    eval "dest_name=\${CLOUD_DEST_${dest_num}_NAME}"
    eval "expiry_date=\${CLOUD_DEST_${dest_num}_SECRET_EXPIRY}"
    
    if [[ -z "$expiry_date" ]]; then
        cloud_log_debug "No expiry date configured for $dest_name"
        return 0
    fi
    
    # Calculate days until expiry
    local today
    local expiry_epoch
    local today_epoch
    local days_until_expiry
    
    today=$(date '+%Y-%m-%d')
    expiry_epoch=$(date -d "$expiry_date" '+%s' 2>/dev/null)
    today_epoch=$(date -d "$today" '+%s')
    
    if [[ -z "$expiry_epoch" ]]; then
        cloud_log_warn "Invalid expiry date format for $dest_name: $expiry_date"
        return 0
    fi
    
    days_until_expiry=$(( (expiry_epoch - today_epoch) / 86400 ))
    
    local warn_days="${CLOUD_REPLICATION_EXPIRY_WARN_DAYS:-30}"
    local critical_days="${CLOUD_REPLICATION_EXPIRY_CRITICAL_DAYS:-7}"
    
    if [[ $days_until_expiry -lt 0 ]]; then
        cloud_log_error "EXPIRED: $dest_name secret expired on $expiry_date ($(( -days_until_expiry )) days ago)"
        CLOUD_REPLICATION_CREDENTIAL_WARNINGS+=("EXPIRED: $dest_name expired $expiry_date")
        return 1
    elif [[ $days_until_expiry -le $critical_days ]]; then
        cloud_log_error "CRITICAL: $dest_name secret expires in $days_until_expiry days ($expiry_date)"
        CLOUD_REPLICATION_CREDENTIAL_WARNINGS+=("CRITICAL: $dest_name expires in $days_until_expiry days ($expiry_date)")
    elif [[ $days_until_expiry -le $warn_days ]]; then
        cloud_log_warn "Credential warning: $dest_name secret expires in $days_until_expiry days ($expiry_date)"
        CLOUD_REPLICATION_CREDENTIAL_WARNINGS+=("WARNING: $dest_name expires in $days_until_expiry days ($expiry_date)")
    else
        cloud_log_info "Credential status: $dest_name expires in $days_until_expiry days ($expiry_date)"
    fi
    
    return 0
}

#=============================================================================
# DESTINATION PROCESSING
#=============================================================================

cloud_replication_get_dest_count() {
    # Returns count of ENABLED destinations only
    local enabled_count=0
    local n=1
    
    while true; do
        local enabled
        eval "enabled=\${CLOUD_DEST_${n}_ENABLED}"
        
        if [[ -z "$enabled" ]]; then
            break
        fi
        
        if [[ "$enabled" == "yes" ]]; then
            enabled_count=$((enabled_count + 1))
        fi
        
        n=$((n + 1))
    done
    
    echo "$enabled_count"
}

# Get the highest destination number (for iteration)
cloud_replication_get_max_dest_num() {
    local max_n=0
    local n=1
    
    while true; do
        local enabled
        eval "enabled=\${CLOUD_DEST_${n}_ENABLED}"
        
        if [[ -z "$enabled" ]]; then
            break
        fi
        
        max_n=$n
        n=$((n + 1))
    done
    
    echo "$max_n"
}

cloud_replication_process_destination() {
    local dest_num="$1"
    local source_path="$2"
    
    # Get destination settings
    local enabled name provider remote path scope sync_mode bwlimit verify
    
    eval "enabled=\${CLOUD_DEST_${dest_num}_ENABLED}"
    eval "name=\${CLOUD_DEST_${dest_num}_NAME}"
    eval "provider=\${CLOUD_DEST_${dest_num}_PROVIDER}"
    eval "remote=\${CLOUD_DEST_${dest_num}_REMOTE}"
    eval "path=\${CLOUD_DEST_${dest_num}_PATH}"
    eval "scope=\${CLOUD_DEST_${dest_num}_SCOPE:-$CLOUD_REPLICATION_SCOPE}"
    eval "sync_mode=\${CLOUD_DEST_${dest_num}_SYNC_MODE:-$CLOUD_REPLICATION_SYNC_MODE}"
    eval "bwlimit=\${CLOUD_DEST_${dest_num}_BWLIMIT:-$CLOUD_REPLICATION_DEFAULT_BWLIMIT}"
    eval "verify=\${CLOUD_DEST_${dest_num}_VERIFY:-$CLOUD_REPLICATION_POST_VERIFY}"
    
    # Track provider and remote for email reporting (Option B format)
    CLOUD_REPLICATION_DEST_PROVIDER["$name"]="$provider"
    CLOUD_REPLICATION_DEST_REMOTE["$name"]="$remote"
    
    # Check if enabled
    if [[ "$enabled" != "yes" ]]; then
        cloud_log_info "Destination $name: DISABLED"
        CLOUD_REPLICATION_DEST_STATUS+=("$name|disabled|0|0|0|Disabled in config")
        return 0
    fi
    
    cloud_log_info "════════════════════════════════════════════════════════════"
    cloud_log_info "Processing destination: $name ($provider)"
    cloud_log_info "════════════════════════════════════════════════════════════"
    
    # Check credential expiry
    if ! cloud_replication_check_credential_expiry "$dest_num"; then
        cloud_log_error "Skipping $name due to expired credentials"
        CLOUD_REPLICATION_DEST_STATUS+=("$name|failed|0|0|0|Credentials expired")
        return 1
    fi
    
    # Check if dry-run mode
    if [[ "${CLOUD_REPLICATION_DRY_RUN:-no}" == "yes" ]]; then
        cloud_replication_dry_run "$dest_num" "$source_path"
        return $?
    fi
    
    # Call provider-specific upload function
    local upload_func="cloud_transport_${provider}_upload"
    if ! declare -f "$upload_func" >/dev/null 2>&1; then
        cloud_log_error "No upload function found for provider: $provider"
        CLOUD_REPLICATION_DEST_STATUS+=("$name|failed|0|0|0|No transport for $provider")
        return 1
    fi
    
    local start_time
    start_time=$(date '+%s')
    
    # Track bytes/files before upload to calculate delta
    local bytes_before=$CLOUD_REPLICATION_TOTAL_BYTES
    local files_before=$CLOUD_REPLICATION_TOTAL_FILES
    
    # Execute upload
    local result
    "$upload_func" "$dest_num" "$source_path"
    result=$?
    
    # Capture cloud transport metrics (contract v1.0)
    CLOUD_REPLICATION_DEST_THROTTLE["$name"]="${CLOUD_TRANSPORT_THROTTLE_COUNT:-0}"
    CLOUD_REPLICATION_DEST_BWLIMIT["$name"]="${CLOUD_TRANSPORT_BWLIMIT_FINAL:-}"
    CLOUD_REPLICATION_DEST_AVAIL["$name"]="${CLOUD_TRANSPORT_DEST_AVAIL_BYTES:-0}"
    CLOUD_REPLICATION_DEST_TOTAL["$name"]="${CLOUD_TRANSPORT_DEST_TOTAL_BYTES:-0}"
    CLOUD_REPLICATION_DEST_SPACE_KNOWN["$name"]="${CLOUD_TRANSPORT_DEST_SPACE_KNOWN:-0}"
    
    local end_time
    end_time=$(date '+%s')
    local duration=$((end_time - start_time))
    
    # Calculate per-destination stats (delta from before)
    local dest_bytes=$((CLOUD_REPLICATION_TOTAL_BYTES - bytes_before))
    local dest_files=$((CLOUD_REPLICATION_TOTAL_FILES - files_before))
    
    if [[ $result -eq 0 ]]; then
        cloud_log_info "Destination $name: SUCCESS (${duration}s)"
        # NOTE: Transport function (e.g. sharepoint) already adds to CLOUD_REPLICATION_DEST_STATUS
        # Do NOT add again here to avoid duplicate entries

    else
        cloud_log_error "Destination $name: FAILED (${duration}s)"
        CLOUD_REPLICATION_TOTAL_ERRORS=$((CLOUD_REPLICATION_TOTAL_ERRORS + 1))
        # NOTE: Transport function already adds failure status, only add here if transport doesn't
    fi
    
    return $result
}

#=============================================================================
# DRY-RUN MODE
#=============================================================================

cloud_replication_dry_run() {
    local dest_num="$1"
    local source_path="$2"
    
    local name provider remote
    eval "name=\${CLOUD_DEST_${dest_num}_NAME}"
    eval "provider=\${CLOUD_DEST_${dest_num}_PROVIDER}"
    eval "remote=\${CLOUD_DEST_${dest_num}_REMOTE}"
    
    cloud_log_info "[DRY-RUN] Testing destination: $name"
    
    # Test authentication
    cloud_log_info "[DRY-RUN] Testing authentication..."
    if rclone lsd "$remote" --max-depth 0 &>/dev/null; then
        cloud_log_info "[DRY-RUN] $name: Authentication OK"
    else
        cloud_log_error "[DRY-RUN] $name: Authentication FAILED"
        CLOUD_REPLICATION_DEST_STATUS+=("$name|dry-run-failed|0|0|0|Auth failed")
        return 1
    fi
    
    # Test write permissions
    cloud_log_info "[DRY-RUN] Testing write permissions..."
    local test_file="/tmp/cloud_replication_test_$$"
    echo "test" > "$test_file"
    
    local remote_path
    eval "remote_path=\${CLOUD_DEST_${dest_num}_PATH}"
    
    if rclone copy "$test_file" "${remote}${remote_path}/" &>/dev/null; then
        rclone delete "${remote}${remote_path}/$(basename "$test_file")" &>/dev/null
        cloud_log_info "[DRY-RUN] $name: Write permissions OK"
    else
        cloud_log_warn "[DRY-RUN] $name: Write permissions check failed (may be OK)"
    fi
    rm -f "$test_file"
    
    # Check quota if available
    cloud_log_info "[DRY-RUN] Checking quota..."
    local quota_info
    quota_info=$(rclone about "$remote" 2>/dev/null)
    if [[ -n "$quota_info" ]]; then
        cloud_log_info "[DRY-RUN] $name: Quota info:"
        echo "$quota_info" | while read -r line; do
            cloud_log_info "[DRY-RUN]   $line"
        done
    fi
    
    # Check credential expiry
    cloud_replication_check_credential_expiry "$dest_num"
    
    # Count files that would be uploaded
    cloud_log_info "[DRY-RUN] Analyzing source: $source_path"
    local file_count
    local total_size
    file_count=$(find "$source_path" -type f 2>/dev/null | wc -l)
    total_size=$(du -sh "$source_path" 2>/dev/null | cut -f1)
    cloud_log_info "[DRY-RUN] $name: Would upload $file_count files ($total_size)"
    
    CLOUD_REPLICATION_DEST_STATUS+=("$name|dry-run-ok|0|$file_count|0|Dry run passed")
    
    cloud_log_info "[DRY-RUN] All checks passed for $name"
    return 0
}

#=============================================================================
# MAIN ENTRY POINTS
#=============================================================================

run_cloud_replication_batch() {
    local source_path="$1"
    
    # Initialize if not already done
    if [[ $CLOUD_REPLICATION_INITIALIZED -ne 1 ]]; then
        if ! cloud_replication_init; then
            return 1
        fi
    fi
    
    # Check if enabled
    if [[ "${CLOUD_REPLICATION_ENABLED:-no}" != "yes" ]]; then
        cloud_log_info "Cloud replication is disabled"
        return 0
    fi
    
    # Validate source path
    if [[ ! -d "$source_path" ]]; then
        cloud_log_error "Source path does not exist: $source_path"
        return 1
    fi
    
    # Acquire lock
    if ! cloud_replication_acquire_lock; then
        return 1
    fi
    
    CLOUD_REPLICATION_START_TIME=$(date '+%Y-%m-%d %H:%M:%S %Z')
    
    # Check for pre-existing cancellation request
    if type is_replication_cancelled &>/dev/null && is_replication_cancelled; then
        cloud_log_warn "Replication cancellation flag detected before start - skipping all cloud replication"
        CLOUD_REPLICATION_END_TIME=$(date '+%Y-%m-%d %H:%M:%S %Z')
        cloud_replication_release_lock
        return 1
    fi
    
    cloud_log_info "Starting cloud replication batch"
    cloud_log_info "Source path: $source_path"
    
    # Get destination counts
    local enabled_count max_dest_num
    enabled_count=$(cloud_replication_get_dest_count)
    max_dest_num=$(cloud_replication_get_max_dest_num)
    cloud_log_info "Found $enabled_count enabled cloud destination(s) (of $max_dest_num defined)"
    
    if [[ $enabled_count -eq 0 ]]; then
        cloud_log_warn "No enabled cloud destinations found"
        cloud_replication_release_lock
        return 0
    fi
    
    # Process each destination (iterate through all, skip disabled)
    local overall_result=0
    local n=1
    
    while [[ $n -le $max_dest_num ]]; do
        local dest_enabled
        eval "dest_enabled=\${CLOUD_DEST_${n}_ENABLED}"
        
        if [[ "$dest_enabled" != "yes" ]]; then
            cloud_log_debug "Skipping disabled cloud destination $n"
            n=$((n + 1))
            continue
        fi
        
        # Check for cancellation before starting this destination
        if type is_replication_cancelled &>/dev/null && is_replication_cancelled; then
            local cancel_name
            eval "cancel_name=\${CLOUD_DEST_${n}_NAME:-cloud_dest_$n}"
            cloud_log_warn "Replication cancelled - skipping cloud destination: $cancel_name"
            CLOUD_REPLICATION_DEST_STATUS+=("$cancel_name|cancelled|0|0|0|Replication cancelled by operator")
            overall_result=1
            n=$((n + 1))
            continue
        fi
        
        if ! cloud_replication_process_destination "$n" "$source_path"; then
            if [[ "${CLOUD_REPLICATION_ON_FAILURE:-continue}" == "abort" ]]; then
                cloud_log_error "Aborting cloud replication due to failure"
                overall_result=1
                break
            fi
            overall_result=1
        fi
        n=$((n + 1))
    done
    
    CLOUD_REPLICATION_END_TIME=$(date '+%Y-%m-%d %H:%M:%S %Z')
    
    # Calculate duration
    local start_epoch end_epoch duration
    start_epoch=$(date -d "$CLOUD_REPLICATION_START_TIME" '+%s' 2>/dev/null) || start_epoch=0
    end_epoch=$(date -d "$CLOUD_REPLICATION_END_TIME" '+%s' 2>/dev/null) || end_epoch=0
    duration=$((end_epoch - start_epoch))
    
    # Log summary
    cloud_log_info "════════════════════════════════════════════════════════════"
    cloud_log_info "Cloud replication batch complete"
    cloud_log_info "Started: $CLOUD_REPLICATION_START_TIME"
    cloud_log_info "Ended:   $CLOUD_REPLICATION_END_TIME"
    cloud_log_info "Duration: ${duration}s"
    cloud_log_info "Bytes:   $CLOUD_REPLICATION_TOTAL_BYTES"
    cloud_log_info "Files:   $CLOUD_REPLICATION_TOTAL_FILES"
    cloud_log_info "Errors:  $CLOUD_REPLICATION_TOTAL_ERRORS"
    cloud_log_info "════════════════════════════════════════════════════════════"
    
    # Write state to file for parent shell to read (needed when running in subshell)
    # Values MUST be quoted to handle spaces in timestamps
    local state_file="${STATE_DIR}/cloud_replication_state.txt"
    {
        echo "CLOUD_REPLICATION_START_TIME=\"$CLOUD_REPLICATION_START_TIME\""
        echo "CLOUD_REPLICATION_END_TIME=\"$CLOUD_REPLICATION_END_TIME\""
        echo "CLOUD_REPLICATION_TOTAL_BYTES=$CLOUD_REPLICATION_TOTAL_BYTES"
        echo "CLOUD_REPLICATION_TOTAL_FILES=$CLOUD_REPLICATION_TOTAL_FILES"
        echo "CLOUD_REPLICATION_TOTAL_ERRORS=$CLOUD_REPLICATION_TOTAL_ERRORS"
        echo "CLOUD_REPLICATION_ENABLED=\"${CLOUD_REPLICATION_ENABLED:-no}\""
        # Save destination status arrays (pipe-delimited, no spaces so no quotes needed)
        for status_line in "${CLOUD_REPLICATION_DEST_STATUS[@]}"; do
            echo "DEST_STATUS=\"$status_line\""
        done
        for dest_name in "${!CLOUD_REPLICATION_DEST_PROVIDER[@]}"; do
            echo "DEST_PROVIDER=\"${dest_name}|${CLOUD_REPLICATION_DEST_PROVIDER[$dest_name]}\""
        done
        for dest_name in "${!CLOUD_REPLICATION_DEST_REMOTE[@]}"; do
            echo "DEST_REMOTE=\"${dest_name}|${CLOUD_REPLICATION_DEST_REMOTE[$dest_name]}\""
        done
    } > "$state_file"
    cloud_log_debug "Wrote state to $state_file"
    
    # Log to SQLite database (parallel to state file)
    if type sqlite_is_available &>/dev/null && sqlite_is_available; then
        for status_line in "${CLOUD_REPLICATION_DEST_STATUS[@]}"; do
            # Parse pipe-delimited status: "name|status|bytes|files|duration|error_message"
            IFS='|' read -r dest_name status dest_bytes dest_files dest_duration error_msg <<< "$status_line"
            local provider="${CLOUD_REPLICATION_DEST_PROVIDER[$dest_name]:-unknown}"
            local remote="${CLOUD_REPLICATION_DEST_REMOTE[$dest_name]:-}"
            local log_file
            log_file=$(tu_get_replication_log_path "cloud" "$dest_name" 2>/dev/null || echo "")
            # Don't record log_file if it doesn't actually exist on disk
            [[ -n "$log_file" && ! -f "$log_file" ]] && log_file=""
            
            local run_id
            run_id=$(sqlite_log_replication_run \
                "$dest_name" \
                "cloud" \
                "$provider" \
                "accumulate" \
                "$remote" \
                "$CLOUD_REPLICATION_START_TIME" \
                "$CLOUD_REPLICATION_END_TIME" \
                "${dest_duration:-0}" \
                "${dest_bytes:-0}" \
                "${dest_files:-0}" \
                "$status" \
                "$error_msg" \
                "$log_file" \
                "${CLOUD_REPLICATION_DEST_AVAIL[$dest_name]:-0}" \
                "${CLOUD_REPLICATION_DEST_TOTAL[$dest_name]:-0}" \
                "${CLOUD_REPLICATION_DEST_SPACE_KNOWN[$dest_name]:-0}" \
                "${CLOUD_REPLICATION_DEST_THROTTLE[$dest_name]:-0}" \
                "${CLOUD_REPLICATION_DEST_BWLIMIT[$dest_name]:-}")
            
            # Log VMs included in this replication
            if [[ -n "$run_id" ]] && [[ ${#VM_BACKUP_RESULTS[@]} -gt 0 ]]; then
                local vm_names=()
                for result in "${VM_BACKUP_RESULTS[@]}"; do
                    IFS='|' read -r vm vm_status rest <<< "$result"
                    [[ "$vm_status" == "SUCCESS" ]] && vm_names+=("$vm")
                done
                [[ ${#vm_names[@]} -gt 0 ]] && sqlite_log_replication_vms "$run_id" "$status" "${vm_names[@]}"
            fi
        done
        cloud_log_debug "SQLite replication runs logged"
    fi
    
    # Display credential warnings
    if [[ ${#CLOUD_REPLICATION_CREDENTIAL_WARNINGS[@]} -gt 0 ]]; then
        cloud_log_warn "Credential warnings:"
        for warning in "${CLOUD_REPLICATION_CREDENTIAL_WARNINGS[@]}"; do
            cloud_log_warn "  $warning"
        done
    fi
    
    # Release lock
    cloud_replication_release_lock
    
    return $overall_result
}

# Get formatted summary for email reports
# Returns a formatted text block suitable for email body
get_cloud_replication_summary() {
    local state_file="${STATE_DIR}/cloud_replication_state.txt"
    
    # Try to load state from file if variables are empty (subshell isolation fix)
    if [[ -z "$CLOUD_REPLICATION_START_TIME" ]]; then
        if [[ -f "$state_file" ]]; then
            # Source scalar variables
            while IFS='=' read -r key value; do
                # Strip quotes from value
                value="${value#\"}"
                value="${value%\"}"
                case "$key" in
                    CLOUD_REPLICATION_START_TIME) CLOUD_REPLICATION_START_TIME="$value" ;;
                    CLOUD_REPLICATION_END_TIME)   CLOUD_REPLICATION_END_TIME="$value" ;;
                    CLOUD_REPLICATION_TOTAL_BYTES) CLOUD_REPLICATION_TOTAL_BYTES="$value" ;;
                    CLOUD_REPLICATION_TOTAL_FILES) CLOUD_REPLICATION_TOTAL_FILES="$value" ;;
                    CLOUD_REPLICATION_TOTAL_ERRORS) CLOUD_REPLICATION_TOTAL_ERRORS="$value" ;;
                    CLOUD_REPLICATION_ENABLED) CLOUD_REPLICATION_ENABLED="$value" ;;
                    DEST_STATUS) CLOUD_REPLICATION_DEST_STATUS+=("$value") ;;
                esac
            done < "$state_file"
        fi
    fi
    
    # Check if cloud replication ran
    if [[ -z "$CLOUD_REPLICATION_START_TIME" ]]; then
        echo "Cloud Replication: Not Run"
        return 0
    fi
    
    # Check if enabled
    if [[ "${CLOUD_REPLICATION_ENABLED:-no}" != "yes" ]]; then
        echo "Cloud Replication: Disabled"
        return 0
    fi
    
    # Format bytes
    local bytes_fmt
    if [[ $CLOUD_REPLICATION_TOTAL_BYTES -gt 0 ]]; then
        bytes_fmt=$(numfmt --to=iec --suffix=B "$CLOUD_REPLICATION_TOTAL_BYTES" 2>/dev/null || echo "${CLOUD_REPLICATION_TOTAL_BYTES} bytes")
    else
        bytes_fmt="0 B"
    fi
    
    # Calculate duration
    local start_epoch end_epoch duration duration_fmt
    start_epoch=$(date -d "${CLOUD_REPLICATION_START_TIME} UTC" '+%s' 2>/dev/null) || start_epoch=0
    end_epoch=$(date -d "${CLOUD_REPLICATION_END_TIME} UTC" '+%s' 2>/dev/null) || end_epoch=0
    duration=$((end_epoch - start_epoch))
    
    if [[ $duration -ge 3600 ]]; then
        duration_fmt="$((duration / 3600))h $((duration % 3600 / 60))m"
    elif [[ $duration -ge 60 ]]; then
        duration_fmt="$((duration / 60))m $((duration % 60))s"
    else
        duration_fmt="${duration}s"
    fi
    
    # Build status line
    local status_text="SUCCESS"
    [[ $CLOUD_REPLICATION_TOTAL_ERRORS -gt 0 ]] && status_text="ERRORS ($CLOUD_REPLICATION_TOTAL_ERRORS)"
    
    # Output summary
    echo "Status:      $status_text"
    echo "Uploaded:    $bytes_fmt ($CLOUD_REPLICATION_TOTAL_FILES files)"
    echo "Duration:    $duration_fmt"
    echo ""
    
    # Per-destination status
    if [[ ${#CLOUD_REPLICATION_DEST_STATUS[@]} -gt 0 ]]; then
        echo "Destinations:"
        for status in "${CLOUD_REPLICATION_DEST_STATUS[@]}"; do
            # Parse: name|status|bytes|files|duration|message
            local name dest_status dest_message
            name=$(echo "$status" | cut -d'|' -f1)
            dest_status=$(echo "$status" | cut -d'|' -f2)
            dest_message=$(echo "$status" | cut -d'|' -f6)
            if [[ -n "$dest_message" ]]; then
                echo "  • $name: $dest_status - $dest_message"
            else
                echo "  • $name: $dest_status"
            fi
        done
    fi
    
    # Credential warnings
    if [[ ${#CLOUD_REPLICATION_CREDENTIAL_WARNINGS[@]} -gt 0 ]]; then
        echo ""
        echo "⚠ Credential Warnings:"
        for warning in "${CLOUD_REPLICATION_CREDENTIAL_WARNINGS[@]}"; do
            echo "  $warning"
        done
    fi
}

#################################################################################
#################################################################################
# get_cloud_replication_stats - Get one-line summary for email header
#
# Returns a single-line summary like:
#   "✓ 1/1 destinations │ 484.9 MiB (28 files) in 1m 09s"
#
# Used in the SUMMARY section of the email.
#################################################################################
get_cloud_replication_stats() {
    local state_file="${STATE_DIR}/cloud_replication_state.txt"
    
    # Try to load state from file if variables are empty (subshell isolation fix)
    if [[ -z "$CLOUD_REPLICATION_START_TIME" ]] || [[ ${#CLOUD_REPLICATION_DEST_STATUS[@]} -eq 0 ]]; then
        if [[ -f "$state_file" ]]; then
            while IFS='=' read -r key value; do
                # Strip quotes from value
                value="${value#\"}"
                value="${value%\"}"
                case "$key" in
                    CLOUD_REPLICATION_START_TIME) CLOUD_REPLICATION_START_TIME="$value" ;;
                    CLOUD_REPLICATION_ENABLED) CLOUD_REPLICATION_ENABLED="$value" ;;
                    DEST_STATUS) CLOUD_REPLICATION_DEST_STATUS+=("$value") ;;
                    DEST_PROVIDER)
                        local name provider
                        IFS='|' read -r name provider <<< "$value"
                        CLOUD_REPLICATION_DEST_PROVIDER["$name"]="$provider"
                        ;;
                    DEST_REMOTE)
                        local name remote
                        IFS='|' read -r name remote <<< "$value"
                        CLOUD_REPLICATION_DEST_REMOTE["$name"]="$remote"
                        ;;
                esac
            done < "$state_file"
        fi
    fi
    
    # Guard: Return empty if cloud replication didn't run
    if [[ -z "$CLOUD_REPLICATION_START_TIME" ]]; then
        return
    fi
    
    # Guard: Return empty if disabled
    if [[ "${CLOUD_REPLICATION_ENABLED:-no}" != "yes" ]]; then
        return
    fi
    
    # Guard: Return empty if no destinations processed
    if [[ ${#CLOUD_REPLICATION_DEST_STATUS[@]} -eq 0 ]]; then
        return
    fi
    
    # Count destinations by status
    local success_count=0
    local total_count=0
    local total_bytes=0
    local total_files=0
    local total_duration=0
    
    for status_line in "${CLOUD_REPLICATION_DEST_STATUS[@]}"; do
        # Parse: name|status|bytes|files|duration|message
        local dest_status bytes files duration
        dest_status=$(echo "$status_line" | cut -d'|' -f2)
        bytes=$(echo "$status_line" | cut -d'|' -f3)
        files=$(echo "$status_line" | cut -d'|' -f4)
        duration=$(echo "$status_line" | cut -d'|' -f5)
        
        # Only count enabled destinations (not disabled)
        if [[ "$dest_status" != "disabled" ]]; then
            ((total_count++))
            if [[ "$dest_status" == "success" ]]; then
                ((success_count++))
                total_bytes=$((total_bytes + bytes))
                total_files=$((total_files + files))
                total_duration=$((total_duration + duration))
            fi
        fi
    done
    
    # Return empty if nothing to report
    [[ $total_count -eq 0 ]] && return
    
    # Format output
    local status_icon="✓"
    [[ $success_count -lt $total_count ]] && status_icon="⚠"
    [[ $success_count -eq 0 ]] && status_icon="✗"
    
    local bytes_human
    bytes_human=$(numfmt --to=iec-i --suffix=B "$total_bytes" 2>/dev/null || echo "$total_bytes bytes")
    
    local duration_fmt
    if [[ $total_duration -ge 3600 ]]; then
        duration_fmt="$((total_duration / 3600))h $((total_duration % 3600 / 60))m"
    elif [[ $total_duration -ge 60 ]]; then
        duration_fmt="$((total_duration / 60))m $((total_duration % 60))s"
    else
        duration_fmt="${total_duration}s"
    fi
    
    echo "$status_icon $success_count/$total_count destinations │ $bytes_human ($total_files files) in $duration_fmt"
}

#################################################################################
# get_cloud_replication_details - Generate compact per-destination lines
#
# This function produces the "CLOUD REPLICATION" section of the backup email
# in compact format (one line per destination).
#
# OUTPUT FORMAT (compact):
#   ┌─ sharepoint-backup ──────────────────────────────────────────────────────────
#   │ SharePoint → sharepoint-vm: │ ✓ SUCCESS │ 484.9 MiB (28 files) in 1m 09s
#   └──────────────────────────────────────────────────────────────────────────────
#
# Returns:
#   Prints formatted lines to stdout
#################################################################################
get_cloud_replication_details() {
    local state_file="${STATE_DIR}/cloud_replication_state.txt"
    
    # Try to load state from file if variables are empty (subshell isolation fix)
    if [[ -z "$CLOUD_REPLICATION_START_TIME" ]] || [[ ${#CLOUD_REPLICATION_DEST_STATUS[@]} -eq 0 ]]; then
        if [[ -f "$state_file" ]]; then
            while IFS='=' read -r key value; do
                # Strip quotes from value
                value="${value#\"}"
                value="${value%\"}"
                case "$key" in
                    CLOUD_REPLICATION_START_TIME) CLOUD_REPLICATION_START_TIME="$value" ;;
                    CLOUD_REPLICATION_ENABLED) CLOUD_REPLICATION_ENABLED="$value" ;;
                    DEST_STATUS) CLOUD_REPLICATION_DEST_STATUS+=("$value") ;;
                    DEST_PROVIDER)
                        local name provider
                        IFS='|' read -r name provider <<< "$value"
                        CLOUD_REPLICATION_DEST_PROVIDER["$name"]="$provider"
                        ;;
                    DEST_REMOTE)
                        local name remote
                        IFS='|' read -r name remote <<< "$value"
                        CLOUD_REPLICATION_DEST_REMOTE["$name"]="$remote"
                        ;;
                esac
            done < "$state_file"
        fi
    fi
    
    # Guard: Return empty if cloud replication didn't run
    if [[ -z "$CLOUD_REPLICATION_START_TIME" ]]; then
        return
    fi
    
    # Guard: Return empty if disabled
    if [[ "${CLOUD_REPLICATION_ENABLED:-no}" != "yes" ]]; then
        return
    fi
    
    # Guard: Return empty if no destinations processed
    if [[ ${#CLOUD_REPLICATION_DEST_STATUS[@]} -eq 0 ]]; then
        return
    fi
    
    local output=""
    
    for status_line in "${CLOUD_REPLICATION_DEST_STATUS[@]}"; do
        # Parse: name|status|bytes|files|duration|message
        local name dest_status bytes files duration message
        name=$(echo "$status_line" | cut -d'|' -f1)
        dest_status=$(echo "$status_line" | cut -d'|' -f2)
        bytes=$(echo "$status_line" | cut -d'|' -f3)
        files=$(echo "$status_line" | cut -d'|' -f4)
        duration=$(echo "$status_line" | cut -d'|' -f5)
        message=$(echo "$status_line" | cut -d'|' -f6)
        
        # Get provider and remote from tracking vars
        local provider="${CLOUD_REPLICATION_DEST_PROVIDER[$name]:-unknown}"
        local remote="${CLOUD_REPLICATION_DEST_REMOTE[$name]:-}"
        
        # Provider display (capitalized)
        local provider_display="${provider^}"
        
        if [[ "$dest_status" == "success" ]]; then
            # Success: Full block with transfer stats
            local bytes_human duration_fmt
            bytes_human=$(numfmt --to=iec-i --suffix=B "$bytes" 2>/dev/null || echo "$bytes bytes")
            
            if [[ $duration -ge 3600 ]]; then
                duration_fmt="$((duration / 3600))h $((duration % 3600 / 60))m"
            elif [[ $duration -ge 60 ]]; then
                duration_fmt="$((duration / 60))m $((duration % 60))s"
            else
                duration_fmt="${duration}s"
            fi
            
            output+="┌─ ${name} ────────────────────────────────────
│ ${provider_display} → ${remote} │ ✓ SUCCESS
│ ${bytes_human} (${files} files) in ${duration_fmt}
└──────────────────────────────────────────────────
"
        elif [[ "$dest_status" == "failed" || "$dest_status" == "dry-run-failed" ]]; then
            # Failed: Show error
            output+="┌─ ${name} ────────────────────────────────────
│ ${provider_display} → ${remote} │ ✗ FAILED
│ ${message}
└──────────────────────────────────────────────────
"
        else
            # Disabled/Skipped/Other: Simple one-liner
            local status_display=""
            case "$dest_status" in
                disabled)     status_display="⏭️ DISABLED" ;;
                dry-run-ok)   status_display="🔍 DRY RUN OK" ;;
                *)            status_display="? $dest_status" ;;
            esac
            output+="│ ${name}: ${status_display}
"
        fi
    done
    
    # Credential warnings at end
    if [[ ${#CLOUD_REPLICATION_CREDENTIAL_WARNINGS[@]} -gt 0 ]]; then
        output+="
⚠ Credential Warnings:
"
        for warning in "${CLOUD_REPLICATION_CREDENTIAL_WARNINGS[@]}"; do
            output+="  $warning
"
        done
    fi
    
    echo -n "$output"
}

#=============================================================================
# MODULE LOAD
#=============================================================================

# Mark module as loaded
CLOUD_REPLICATION_MODULE_LOADED=1

cloud_log_debug "Cloud replication module v${CLOUD_REPLICATION_MODULE_VERSION} loaded"

#=============================================================================
# STANDALONE CLI MODE
#=============================================================================
# When executed directly (not sourced), provide CLI interface for manual runs

cloud_replication_cli_usage() {
    cat <<EOF
Cloud Replication Module v${CLOUD_REPLICATION_MODULE_VERSION}
Usage: $0 [OPTIONS] [SOURCE_PATH]

Pre-seed or manually trigger cloud replication of VM backups.

OPTIONS:
  -h, --help          Show this help message
  -d, --dry-run       Test connectivity and show what would be uploaded
  -v, --verbose       Enable verbose output
  -q, --quiet         Suppress non-error output
  --status            Show current rclone remote status
  --test              Test SharePoint connection only

SOURCE_PATH:
  Path to backup directory (default: /mnt/pve/vm-backup/)

EXAMPLES:
  # Pre-seed existing backups (initial upload)
  sudo $0 /mnt/pve/vm-backup/

  # Dry-run to test connectivity and see what would upload
  sudo $0 --dry-run /mnt/pve/vm-backup/

  # Test SharePoint connection
  sudo $0 --test

  # Show status of remote
  sudo $0 --status

CONFIGURATION:
  Config file: $CLOUD_REPLICATION_CONF
  Transports:  $CLOUD_TRANSPORTS_DIR/

EOF
}

cloud_replication_cli_status() {
    echo "Cloud Replication Status"
    echo "========================"
    echo ""
    
    # Check rclone
    if command -v rclone &>/dev/null; then
        echo "✓ rclone installed: $(rclone version | head -1)"
    else
        echo "✗ rclone not installed"
        return 1
    fi
    
    # Check config
    if [[ -f "$CLOUD_REPLICATION_CONF" ]]; then
        echo "✓ Config file exists: $CLOUD_REPLICATION_CONF"
        source "$CLOUD_REPLICATION_CONF"
        echo "  - Enabled: ${CLOUD_REPLICATION_ENABLED:-no}"
        echo "  - Scope: ${CLOUD_REPLICATION_SCOPE:-everything}"
        echo "  - Sync mode: ${CLOUD_REPLICATION_SYNC_MODE:-copy}"
    else
        echo "✗ Config file not found: $CLOUD_REPLICATION_CONF"
        return 1
    fi
    
    echo ""
    
    # Check destinations
    local dest_count
    dest_count=$(cloud_replication_get_dest_count 2>/dev/null || echo 0)
    echo "Configured destinations: $dest_count"
    
    local n=1
    while [[ $n -le $dest_count ]]; do
        local name provider remote
        eval "name=\${CLOUD_DEST_${n}_NAME}"
        eval "provider=\${CLOUD_DEST_${n}_PROVIDER}"
        eval "remote=\${CLOUD_DEST_${n}_REMOTE}"
        
        echo ""
        echo "Destination $n: $name"
        echo "  Provider: $provider"
        echo "  Remote:   $remote"
        
        # Test connection
        if rclone lsd "$remote" --max-depth 0 &>/dev/null; then
            echo "  Status:   ✓ Connected"
            
            # Show quota if available
            local quota
            quota=$(rclone about "$remote" 2>/dev/null)
            if [[ -n "$quota" ]]; then
                local used free
                used=$(echo "$quota" | grep "Used:" | awk '{print $2}')
                free=$(echo "$quota" | grep "Free:" | awk '{print $2}')
                echo "  Used:     $used"
                echo "  Free:     $free"
            fi
        else
            echo "  Status:   ✗ Connection failed"
        fi
        
        n=$((n + 1))
    done
}

cloud_replication_cli_test() {
    echo "Testing Cloud Connections..."
    echo ""
    
    if ! cloud_replication_init; then
        echo "✗ Failed to initialize cloud replication module"
        return 1
    fi
    
    local dest_count
    dest_count=$(cloud_replication_get_dest_count)
    
    local n=1
    local all_ok=0
    local tested=0
    while [[ $n -le $dest_count ]]; do
        local name remote enabled
        eval "enabled=\${CLOUD_DEST_${n}_ENABLED}"
        eval "name=\${CLOUD_DEST_${n}_NAME}"
        eval "remote=\${CLOUD_DEST_${n}_REMOTE}"
        
        # Skip disabled destinations
        if [[ "$enabled" != "yes" ]]; then
            echo "Skipping $name (disabled)"
            n=$((n + 1))
            continue
        fi
        
        tested=$((tested + 1))
        echo -n "Testing $name ($remote)... "
        
        if rclone lsd "$remote" --max-depth 0 &>/dev/null; then
            echo "✓ OK"
        else
            echo "✗ FAILED"
            echo "  Run: sudo ${_CLOUD_PROJECT_ROOT}/cloud_transports/sharepoint_auth.sh"
            all_ok=1
        fi
        
        n=$((n + 1))
    done
    
    if [[ $tested -eq 0 ]]; then
        echo "No enabled destinations to test."
    fi
    
    return $all_ok
}

# Main CLI entry point (only when executed directly, not sourced)
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    # Parse CLI arguments
    DRY_RUN=0
    VERBOSE=0
    QUIET=0
    ACTION="upload"
    SOURCE_PATH=""
    
    while [[ $# -gt 0 ]]; do
        case "$1" in
            -h|--help)
                cloud_replication_cli_usage
                exit 0
                ;;
            -d|--dry-run)
                DRY_RUN=1
                shift
                ;;
            -v|--verbose)
                VERBOSE=1
                shift
                ;;
            -q|--quiet)
                QUIET=1
                shift
                ;;
            --status)
                ACTION="status"
                shift
                ;;
            --test)
                ACTION="test"
                shift
                ;;
            -*)
                echo "Unknown option: $1"
                cloud_replication_cli_usage
                exit 1
                ;;
            *)
                SOURCE_PATH="$1"
                shift
                ;;
        esac
    done
    
    # Default source path
    [[ -z "$SOURCE_PATH" ]] && SOURCE_PATH="/mnt/pve/vm-backup/"
    
    # Execute action
    case "$ACTION" in
        status)
            cloud_replication_cli_status
            exit $?
            ;;
        test)
            cloud_replication_cli_test
            exit $?
            ;;
        upload)
            echo "╔════════════════════════════════════════════════════════════╗"
            echo "║  Cloud Replication - Manual/Pre-seed Run                   ║"
            echo "║  $(date '+%Y-%m-%d %H:%M:%S')                                       ║"
            echo "╚════════════════════════════════════════════════════════════╝"
            echo ""
            echo "Source path: $SOURCE_PATH"
            echo "Dry run:     $( [[ $DRY_RUN -eq 1 ]] && echo "YES" || echo "NO" )"
            echo ""
            
            if [[ ! -d "$SOURCE_PATH" ]]; then
                echo "ERROR: Source path does not exist: $SOURCE_PATH"
                exit 1
            fi
            
            # Show backup stats
            echo "Analyzing source directory..."
            file_count=$(find "$SOURCE_PATH" -type f 2>/dev/null | wc -l)
            total_size=$(du -sh "$SOURCE_PATH" 2>/dev/null | cut -f1)
            echo "  Files: $file_count"
            echo "  Size:  $total_size"
            echo ""
            
            if [[ $DRY_RUN -eq 1 ]]; then
                echo "DRY RUN MODE - Testing connectivity only"
                echo ""
                
                if ! cloud_replication_init; then
                    echo "ERROR: Failed to initialize cloud replication"
                    exit 1
                fi
                
                dest_count=$(cloud_replication_get_dest_count)
                n=1
                while [[ $n -le $dest_count ]]; do
                    cloud_replication_dry_run "$n" "$SOURCE_PATH"
                    n=$((n + 1))
                done
                
                echo ""
                echo "Dry run complete. Remove --dry-run to perform actual upload."
                exit 0
            fi
            
            # Confirm before actual upload
            echo "This will upload $total_size to cloud storage."
            echo "For large initial uploads, consider running in screen/tmux."
            echo ""
            read -p "Continue? [y/N] " confirm
            if [[ "$confirm" != "y" && "$confirm" != "Y" ]]; then
                echo "Cancelled."
                exit 0
            fi
            
            echo ""
            echo "Starting cloud replication..."
            echo ""
            
            # Run the batch upload
            if run_cloud_replication_batch "$SOURCE_PATH"; then
                echo ""
                echo "════════════════════════════════════════════════════════════"
                echo "Pre-seed upload completed successfully!"
                echo "════════════════════════════════════════════════════════════"
                get_cloud_replication_summary
                exit 0
            else
                echo ""
                echo "════════════════════════════════════════════════════════════"
                echo "Pre-seed upload completed with errors"
                echo "════════════════════════════════════════════════════════════"
                get_cloud_replication_summary
                exit 1
            fi
            ;;
    esac
fi
