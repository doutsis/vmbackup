#!/bin/bash
#################################################################################
# Local Replication Module for vmbackup.sh
#
# Provides offsite/secondary backup replication functionality.
# Syncs completed backups to configured destinations using rsync
# over various transports (local, SSH, SMB).
#
# Features:
#   - Multi-destination support with per-destination configuration
#   - Batch replication mode (recommended for VM-first structure)
#   - Mirror (--delete) or accumulate sync modes
#   - Space checking before replication
#   - Verification after sync (size or checksum)
#   - Pluggable transport drivers
#   - Per-instance configuration
#
# Directory Structure Compatibility:
#   - VM-first:    /backup/vm_name/period/   (v3.0+)
#   - Month-first: /backup/YYYYMM/vm_name/   (legacy)
#   Both structures are replicated identically in batch mode.
#
# Exported Functions:
#   load_local_replication_module()  - Load config, validate, initialize
#   check_replication_prereqs()      - Verify destinations are accessible
#   replicate_batch()                - Replicate all backups
#   get_replication_summary()        - Get summary for email report
#   get_replication_status()         - Get overall status (0=success, 1=partial, 2=failed)
#
# Dependencies:
#   - config/<instance>/replication_local.conf (per-instance configuration)
#   - transports/*.sh (transport drivers)
#   - rsync, numfmt, df
#   - Logging functions from parent (log_info, log_warn, log_error, log_debug)
#
# Usage:
#   source replication_local_module.sh
#   load_local_replication_module
#   if [[ "$REPLICATION_ENABLED" == "yes" ]]; then
#       replicate_batch "$BACKUP_PATH"
#   fi
#
# Version: 1.3
# Created: 2026-01-23
# Updated: 2026-02-02 (per-instance configuration)
#################################################################################

# Module identification
LOCAL_REPLICATION_MODULE_VERSION="1.3"
LOCAL_REPLICATION_MODULE_LOADED=0

#=============================================================================
# STATE TRACKING VARIABLES
#
# These variables track replication progress across the session for:
#   1. Email report generation (get_replication_summary)
#   2. Exit status determination (get_replication_status)
#   3. Session-level statistics
#
# Data Flow:
#   ┌───────────────────┐     ┌──────────────────────┐     ┌─────────────────┐
#   │  replicate_batch()│────▶│ _replicate_to_dest() │────▶│ CSV: batch row  │
#   └───────────────────┘     └──────────────────────┘     │ State: totals   │
#                                      │                   └─────────────────┘
#                                      ▼
#                             ┌──────────────────┐
#                             │ Email Summary    │
#                             │ (session totals) │
#                             └──────────────────┘
#=============================================================================

# Destination discovery array - populated by _discover_destinations()
# Contains destination numbers (1, 2, 3, ...) found in replication.conf
# NOTE: Must use -g (global) because module is sourced from within a function
declare -ga REPLICATION_DESTINATIONS=()

# Per-destination status tracking (keyed by destination NAME, e.g., "truenas")
# These are used by get_replication_summary() and get_local_replication_details()
# for email report generation
# NOTE: Must use -g (global) because module is sourced from within a function
declare -gA REPLICATION_DEST_STATUS=()       # Status: success/failed/skipped/disabled
declare -gA REPLICATION_DEST_BYTES=()        # Bytes transferred
declare -gA REPLICATION_DEST_DURATION=()     # Duration in seconds
declare -gA REPLICATION_DEST_ERROR=()        # Last error message (for failed status)
declare -gA REPLICATION_DEST_TRANSPORT=()    # Transport type: local/ssh/smb
declare -gA REPLICATION_DEST_PATH=()         # Destination path

# Per-destination transport metrics (for DB logging, metrics contract v1.0)
declare -gA REPLICATION_DEST_AVAIL_BYTES=()  # Free bytes at dest (0 if unknown)
declare -gA REPLICATION_DEST_TOTAL_BYTES=()  # Total bytes at dest (0 if unknown)
declare -gA REPLICATION_DEST_SPACE_KNOWN=()  # 0|1 whether space metrics reliable
declare -gA REPLICATION_DEST_THROTTLE=()     # Throttle events (-1 = not applicable)
declare -gA REPLICATION_DEST_FILES=()         # Files transferred
declare -gA REPLICATION_DEST_BWLIMIT=()      # Final bwlimit after adjustments

# Session-level counters - incremented by replicate_batch()
# Used by get_replication_summary() to show "Total Replicated: X to N destination(s)"
REPLICATION_TOTAL_SUCCESS=0     # Count of successful replication operations
REPLICATION_TOTAL_FAILED=0      # Count of failed replication operations
REPLICATION_TOTAL_SKIPPED=0     # Count of skipped destinations (disabled or mode override)

# Session timing - set by replicate_batch() for duration calculation
REPLICATION_START_TIME=""       # ISO timestamp when replication phase started
REPLICATION_END_TIME=""         # ISO timestamp when replication phase ended

# Dry run mode - can be set externally before calling replication functions
# When true, rsync runs with --dry-run and no actual sync occurs
REPLICATION_DRY_RUN="${REPLICATION_DRY_RUN:-false}"

#################################################################################
# load_local_replication_module - Load configuration and initialize module
#
# Loads replication_local.conf, validates settings, and discovers destinations.
#
# Returns:
#   0 - Module loaded successfully
#   1 - Configuration error or module disabled
#################################################################################
load_local_replication_module() {
    local script_dir="${SCRIPT_DIR:-$(dirname "$(readlink -f "$0")")}"
    local instance="${CONFIG_INSTANCE:-default}"
    local config_file="$script_dir/config/${instance}/replication_local.conf"
    
    log_info "replication_local_module.sh" "load_local_replication_module" "Loading local replication configuration for instance: $instance"
    
    # Check config file exists - instance config is REQUIRED (no fallback)
    if [[ ! -f "$config_file" ]]; then
        log_error "replication_local_module.sh" "load_local_replication_module" "NO CONFIG FILES FOR $instance - missing $config_file"
        REPLICATION_ENABLED="no"
        return 1
    fi
    
    # Source configuration
    if ! source "$config_file" 2>/dev/null; then
        log_error "replication_local_module.sh" "load_local_replication_module" "Failed to load config: $config_file"
        REPLICATION_ENABLED="no"
        return 1
    fi
    
    log_debug "replication_local_module.sh" "load_local_replication_module" "Configuration loaded from: $config_file"
    
    # Check if replication is enabled
    if [[ "${REPLICATION_ENABLED:-no}" != "yes" ]]; then
        log_info "replication_local_module.sh" "load_local_replication_module" "Replication disabled in configuration"
        # Log disabled state to database
        _log_local_replication_config "no" "$config_file"
        return 1
    fi
    
    # Validate global settings - batch mode only (per-vm was removed)
    if [[ "${REPLICATION_MODE:-batch}" != "batch" ]]; then
        log_warn "replication_local_module.sh" "load_local_replication_module" "Invalid REPLICATION_MODE: $REPLICATION_MODE (must be 'batch')"
        REPLICATION_MODE="batch"
    fi
    log_debug "replication_local_module.sh" "load_local_replication_module" "Replication mode: batch"
    
    # Discover and validate destinations
    _discover_destinations
    
    local enabled_count=0
    local disabled_count=0
    local dest_list=""
    
    for dest_name in "${REPLICATION_DESTINATIONS[@]}"; do
        local enabled_var="DEST_${dest_name}_ENABLED"
        if [[ "${!enabled_var}" == "yes" ]]; then
            ((enabled_count++))
            dest_list+="$dest_name (enabled), "
        else
            ((disabled_count++))
            dest_list+="$dest_name (disabled), "
        fi
    done
    
    # Remove trailing comma and space
    dest_list="${dest_list%, }"
    
    if [[ $enabled_count -eq 0 ]]; then
        log_warn "replication_local_module.sh" "load_replication_module" "No enabled destinations found"
        log_warn "replication_local_module.sh" "load_replication_module" "Replication disabled (no destinations)"
        REPLICATION_ENABLED="no"
        return 1
    fi
    
    log_info "replication_local_module.sh" "load_replication_module" "Replication enabled: yes, mode: $REPLICATION_MODE"
    log_info "replication_local_module.sh" "load_replication_module" "Found ${#REPLICATION_DESTINATIONS[@]} destinations: $dest_list"
    
    # Log config state to database (config_loaded events + change detection)
    _log_local_replication_config "yes" "$config_file"
    
    REPLICATION_MODULE_LOADED=1
    return 0
}

#################################################################################
# _log_local_replication_config - Log replication config state to database
#
# Logs config_loaded events for the global enabled state and each destination.
# Detects changes from the previous session and logs config_changed events.
#
# Arguments:
#   $1 - enabled state ("yes" or "no")
#   $2 - config file path
#################################################################################
_log_local_replication_config() {
    local enabled_state="$1"
    local config_file="$2"
    
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
        "LOCAL_REPLICATION_ENABLED" "$enabled_state" "" "replication_local" "load_config"
    
    # Check for change from previous session
    if type sqlite_query_previous_config_value &>/dev/null; then
        local prev_enabled
        prev_enabled=$(sqlite_query_previous_config_value "LOCAL_REPLICATION_ENABLED")
        if [[ -n "$prev_enabled" ]] && [[ "$prev_enabled" != "$enabled_state" ]]; then
            sqlite_log_config_event "config_changed" "$config_file" "" \
                "LOCAL_REPLICATION_ENABLED" "$enabled_state" "$prev_enabled" "replication_local" "load_config"
        fi
    fi
    
    # If disabled, no destination detail to log
    [[ "$enabled_state" != "yes" ]] && return 0
    
    # Log per-destination config
    for dest_num in "${REPLICATION_DESTINATIONS[@]}"; do
        local name_var="DEST_${dest_num}_NAME"
        local enabled_var="DEST_${dest_num}_ENABLED"
        local transport_var="DEST_${dest_num}_TRANSPORT"
        local sync_mode_var="DEST_${dest_num}_SYNC_MODE"
        local path_var="DEST_${dest_num}_PATH"
        
        local dest_name="${!name_var:-dest_$dest_num}"
        local dest_enabled="${!enabled_var:-no}"
        local dest_transport="${!transport_var:-local}"
        local dest_sync_mode="${!sync_mode_var:-mirror}"
        local dest_path="${!path_var:-}"
        
        local setting_name="LOCAL_DEST_${dest_name}"
        local setting_value="${dest_transport}:${dest_sync_mode}:${dest_path}"
        local applied_to="$dest_enabled"
        
        sqlite_log_config_event "config_loaded" "$config_file" "" \
            "$setting_name" "$setting_value" "" "$applied_to" "load_config" \
            "transport=$dest_transport sync_mode=$dest_sync_mode path=$dest_path"
        
        # Check for change from previous session
        if type sqlite_query_previous_config_value &>/dev/null; then
            local prev_value
            prev_value=$(sqlite_query_previous_config_value "$setting_name")
            if [[ -n "$prev_value" ]] && [[ "$prev_value" != "$setting_value" ]]; then
                sqlite_log_config_event "config_changed" "$config_file" "" \
                    "$setting_name" "$setting_value" "$prev_value" "$applied_to" "load_config"
            fi
        fi
    done
    
    # Detect removed destinations: query previous session for LOCAL_DEST_* settings
    # that no longer exist in the current config
    if type sqlite_query_previous_config_settings &>/dev/null; then
        local prev_settings current_names
        prev_settings=$(sqlite_query_previous_config_settings "LOCAL_DEST_")
        
        # Build list of current destination setting names
        current_names=""
        for dest_num in "${REPLICATION_DESTINATIONS[@]}"; do
            local cname_var="DEST_${dest_num}_NAME"
            current_names="${current_names} LOCAL_DEST_${!cname_var:-dest_$dest_num}"
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
#
# Populates REPLICATION_DESTINATIONS array with destination numbers.
# Internal function.
#################################################################################
_discover_destinations() {
    REPLICATION_DESTINATIONS=()
    
    # Look for DEST_N_ENABLED variables (N = 1, 2, 3, ...)
    local n=1
    while [[ $n -le 99 ]]; do
        local enabled_var="DEST_${n}_ENABLED"
        local name_var="DEST_${n}_NAME"
        
        # Check if this destination is defined
        if [[ -n "${!enabled_var+x}" ]]; then
            REPLICATION_DESTINATIONS+=("$n")
            log_debug "replication_local_module.sh" "_discover_destinations" "Found destination $n: ${!name_var:-unnamed}"
        else
            # Stop at first gap (assumes sequential numbering)
            # Continue checking a few more in case of gaps
            local gap_count=0
            while [[ $gap_count -lt 3 ]]; do
                ((n++))
                ((gap_count++))
                enabled_var="DEST_${n}_ENABLED"
                if [[ -n "${!enabled_var+x}" ]]; then
                    REPLICATION_DESTINATIONS+=("$n")
                    log_debug "replication_local_module.sh" "_discover_destinations" "Found destination $n after gap"
                    break
                fi
            done
            [[ $gap_count -eq 3 ]] && break
        fi
        ((n++))
    done
}

#################################################################################
# _load_transport - Load transport driver for a destination
#
# Arguments:
#   $1 - Transport type (local, ssh, smb)
#
# Returns:
#   0 - Transport loaded
#   1 - Transport not found or failed to load
#################################################################################
_load_transport() {
    local transport_type="$1"
    local script_dir="${SCRIPT_DIR:-$(dirname "$(readlink -f "$0")")}"
    local transport_file="$script_dir/transports/transport_${transport_type}.sh"
    
    if [[ ! -f "$transport_file" ]]; then
        log_error "replication_local_module.sh" "_load_transport" "Transport driver not found: $transport_file"
        return 1
    fi
    
    if ! source "$transport_file" 2>/dev/null; then
        log_error "replication_local_module.sh" "_load_transport" "Failed to load transport: $transport_file"
        return 1
    fi
    
    log_debug "replication_local_module.sh" "_load_transport" "Loaded transport: $transport_type (v${TRANSPORT_VERSION:-unknown})"
    return 0
}

#################################################################################
# check_replication_prereqs - Verify all enabled destinations are accessible
#
# Tests connectivity to each enabled destination. Does not transfer data.
#
# Returns:
#   0 - All destinations accessible
#   1 - One or more destinations failed (logged, replication may still proceed)
#################################################################################
check_replication_prereqs() {
    if [[ "$REPLICATION_MODULE_LOADED" -ne 1 ]]; then
        log_warn "replication_local_module.sh" "check_replication_prereqs" "Module not loaded"
        return 1
    fi
    
    log_info "replication_local_module.sh" "check_replication_prereqs" "Checking destination accessibility"
    
    local all_ok=0
    
    for dest_num in "${REPLICATION_DESTINATIONS[@]}"; do
        local enabled_var="DEST_${dest_num}_ENABLED"
        local name_var="DEST_${dest_num}_NAME"
        local transport_var="DEST_${dest_num}_TRANSPORT"
        local path_var="DEST_${dest_num}_PATH"
        
        local dest_name="${!name_var:-dest_$dest_num}"
        local dest_enabled="${!enabled_var:-no}"
        local dest_transport="${!transport_var:-local}"
        local dest_path="${!path_var:-}"
        
        if [[ "$dest_enabled" != "yes" ]]; then
            log_debug "replication_local_module.sh" "check_replication_prereqs" "Skipping disabled destination: $dest_name"
            continue
        fi
        
        log_debug "replication_local_module.sh" "check_replication_prereqs" "Testing: $dest_name ($dest_transport -> $dest_path)"
        
        # Load transport driver
        if ! _load_transport "$dest_transport"; then
            log_error "replication_local_module.sh" "check_replication_prereqs" "Failed to load transport for: $dest_name"
            all_ok=1
            continue
        fi
        
        # Test destination
        if transport_init "$dest_path" "$dest_name"; then
            log_info "replication_local_module.sh" "check_replication_prereqs" "Destination OK: $dest_name"
        else
            log_error "replication_local_module.sh" "check_replication_prereqs" "Destination FAILED: $dest_name"
            all_ok=1
        fi
    done
    
    return $all_ok
}

#################################################################################
# _check_destination_space - Check if destination has enough free space
#
# Arguments:
#   $1 - Destination path
#   $2 - Required space in bytes
#   $3 - Destination name (for logging)
#
# Returns:
#   0 - Sufficient space (or check disabled)
#   1 - Insufficient space
#################################################################################
_check_destination_space() {
    local dest_path="$1"
    local required_bytes="$2"
    local dest_name="${3:-destination}"
    
    # Check if space checking is disabled
    if [[ "${REPLICATION_SPACE_CHECK:-skip}" == "disabled" ]]; then
        log_debug "replication_local_module.sh" "_check_destination_space" "Space check disabled"
        return 0
    fi
    
    # Get free space
    local free_bytes
    free_bytes=$(transport_get_free_space "$dest_path")
    
    if [[ -z "$free_bytes" ]] || [[ "$free_bytes" -eq 0 ]]; then
        log_warn "replication_local_module.sh" "_check_destination_space" "Could not determine free space for: $dest_name"
        if [[ "${REPLICATION_SPACE_CHECK}" == "skip" ]]; then
            return 1
        fi
        return 0  # warn mode: proceed anyway
    fi
    
    # Get total space for percentage calculation
    local total_bytes
    total_bytes=$(df -B1 --output=size "$dest_path" 2>/dev/null | tail -1 | tr -d ' ')
    
    # Calculate what free percentage would be after sync
    local after_sync_free=$((free_bytes - required_bytes))
    [[ $after_sync_free -lt 0 ]] && after_sync_free=0
    
    local min_free_percent="${REPLICATION_MIN_FREE_PERCENT:-10}"
    local free_after_percent=0
    
    if [[ -n "$total_bytes" ]] && [[ "$total_bytes" -gt 0 ]]; then
        free_after_percent=$((after_sync_free * 100 / total_bytes))
    fi
    
    local required_human free_human
    required_human=$(numfmt --to=iec-i --suffix=B "$required_bytes" 2>/dev/null || echo "$required_bytes bytes")
    free_human=$(numfmt --to=iec-i --suffix=B "$free_bytes" 2>/dev/null || echo "$free_bytes bytes")
    
    log_debug "replication_local_module.sh" "_check_destination_space" "Required: $required_human, Available: $free_human, After sync: ${free_after_percent}%"
    
    # Check if we have enough space
    if [[ $free_bytes -lt $required_bytes ]]; then
        log_error "replication_local_module.sh" "_check_destination_space" "Insufficient space on $dest_name"
        log_error "replication_local_module.sh" "_check_destination_space" "Required: $required_human, Available: $free_human"
        
        if [[ "${REPLICATION_SPACE_CHECK}" == "skip" ]]; then
            return 1
        fi
        log_warn "replication_local_module.sh" "_check_destination_space" "Proceeding anyway (space_check=warn)"
        return 0
    fi
    
    # Check minimum free percentage after sync
    if [[ $free_after_percent -lt $min_free_percent ]]; then
        log_warn "replication_local_module.sh" "_check_destination_space" "Low space warning: ${free_after_percent}% free after sync (min: ${min_free_percent}%)"
        
        if [[ "${REPLICATION_SPACE_CHECK}" == "skip" ]]; then
            log_error "replication_local_module.sh" "_check_destination_space" "Skipping replication due to low space"
            return 1
        fi
        log_warn "replication_local_module.sh" "_check_destination_space" "Proceeding anyway (space_check=warn)"
    fi
    
    return 0
}

#################################################################################
# replicate_batch - Replicate all backups (batch mode)
#
# Replicates the entire backup root directory to all enabled destinations.
# This is the only supported replication mode as it:
#   - Works with ANY directory structure (VM-first or month-first)
#   - Is more efficient (single rsync per destination)
#   - Reduces network overhead
#   - Maintains exact source structure at destination
#
# Directory Structure Support:
#   VM-first (v3.0+):   /backup/vm_name/period/    → /dest/vm_name/period/
#   Month-first (legacy): /backup/YYYYMM/vm_name/  → /dest/YYYYMM/vm_name/
#   The structure is preserved exactly as-is during replication.
#
# Arguments:
#   $1 - Backup root directory (e.g., /mnt/backup/vms/)
#
# Returns:
#   0 - All replications successful
#   1 - One or more replications failed
#################################################################################
replicate_batch() {
    local backup_root="$1"
    
    if [[ "$REPLICATION_MODULE_LOADED" -ne 1 ]]; then
        log_debug "replication_local_module.sh" "replicate_batch" "Module not loaded, skipping"
        return 0
    fi
    
    if [[ "${REPLICATION_ENABLED:-no}" != "yes" ]]; then
        log_debug "replication_local_module.sh" "replicate_batch" "Replication disabled"
        return 0
    fi
    
    # Count enabled destinations
    local enabled_count=0
    log_debug "replication_local_module.sh" "replicate_batch" "REPLICATION_DESTINATIONS array: ${#REPLICATION_DESTINATIONS[@]} elements"
    for dest_num in "${REPLICATION_DESTINATIONS[@]}"; do
        local enabled_var="DEST_${dest_num}_ENABLED"
        [[ "${!enabled_var}" == "yes" ]] && ((enabled_count++))
    done
    
    if [[ $enabled_count -eq 0 ]]; then
        log_info "replication_local_module.sh" "replicate_batch" "No enabled destinations, skipping replication"
        return 0
    fi
    
    REPLICATION_START_TIME=$(date -u '+%Y-%m-%d %H:%M:%S')
    
    # Check for pre-existing cancellation request
    if type is_replication_cancelled &>/dev/null && is_replication_cancelled; then
        log_warn "replication_local_module.sh" "replicate_batch" "Replication cancellation flag detected before start - skipping all local replication"
        REPLICATION_END_TIME=$(date -u '+%Y-%m-%d %H:%M:%S')
        return 1
    fi
    
    log_info "replication_local_module.sh" "replicate_batch" "Starting batch replication to $enabled_count enabled destination(s)"
    log_info "replication_local_module.sh" "replicate_batch" "Source: $backup_root"
    
    # Calculate source size for space checking
    log_info "replication_local_module.sh" "replicate_batch" "Calculating source size for space check..."
    local source_size
    source_size=$(du -sb "$backup_root" 2>/dev/null | cut -f1)
    if [[ -n "$source_size" ]] && [[ "$source_size" =~ ^[0-9]+$ ]]; then
        local source_human
        source_human=$(numfmt --to=iec-i --suffix=B "$source_size" 2>/dev/null || echo "$source_size bytes")
        log_info "replication_local_module.sh" "replicate_batch" "Source size: $source_human"
    else
        source_size=0
        log_warn "replication_local_module.sh" "replicate_batch" "Could not determine source size"
    fi
    
    local date_str=$(date '+%Y-%m-%d')
    local dest_index=0
    
    # Reset counters
    REPLICATION_TOTAL_SUCCESS=0
    REPLICATION_TOTAL_FAILED=0
    REPLICATION_TOTAL_SKIPPED=0
    
    for dest_num in "${REPLICATION_DESTINATIONS[@]}"; do
        local enabled_var="DEST_${dest_num}_ENABLED"
        local name_var="DEST_${dest_num}_NAME"
        local transport_var="DEST_${dest_num}_TRANSPORT"
        local path_var="DEST_${dest_num}_PATH"
        local sync_mode_var="DEST_${dest_num}_SYNC_MODE"
        local bwlimit_var="DEST_${dest_num}_BWLIMIT"
        local verify_var="DEST_${dest_num}_VERIFY"
        local mode_override_var="DEST_${dest_num}_MODE_OVERRIDE"
        
        local dest_name="${!name_var:-dest_$dest_num}"
        local dest_enabled="${!enabled_var:-no}"
        local dest_transport="${!transport_var:-local}"
        local dest_path="${!path_var:-}"
        local sync_mode="${!sync_mode_var:-mirror}"
        local bwlimit="${!bwlimit_var:-0}"
        local verify_mode="${!verify_var:-size}"
        local mode_override="${!mode_override_var:-}"
        
        # Track transport and path for email reporting (Option B format)
        REPLICATION_DEST_TRANSPORT["$dest_name"]="$dest_transport"
        REPLICATION_DEST_PATH["$dest_name"]="$dest_path"
        
        # Check if this destination should use per-vm mode instead
        if [[ "$mode_override" == "per-vm" ]]; then
            log_debug "replication_local_module.sh" "replicate_batch" "Destination $dest_name uses per-vm mode override, skipping batch"
            REPLICATION_DEST_STATUS["$dest_name"]="skipped"
            REPLICATION_DEST_ERROR["$dest_name"]="per-vm mode override"
            ((REPLICATION_TOTAL_SKIPPED++))
            continue
        fi
        
        if [[ "$dest_enabled" != "yes" ]]; then
            log_debug "replication_local_module.sh" "replicate_batch" "Destination $dest_name disabled, skipping"
            REPLICATION_DEST_STATUS["$dest_name"]="disabled"
            ((REPLICATION_TOTAL_SKIPPED++))
            continue
        fi
        
        ((dest_index++))
        
        # Check for cancellation before starting this destination
        if type is_replication_cancelled &>/dev/null && is_replication_cancelled; then
            log_warn "replication_local_module.sh" "replicate_batch" "Replication cancelled - skipping destination: $dest_name"
            REPLICATION_DEST_STATUS["$dest_name"]="cancelled"
            REPLICATION_DEST_ERROR["$dest_name"]="Replication cancelled by operator"
            continue
        fi
        
        log_info "replication_local_module.sh" "replicate_batch" "─────────────────────────────────────────────"
        log_info "replication_local_module.sh" "replicate_batch" "Destination $dest_index/$enabled_count: $dest_name"
        log_info "replication_local_module.sh" "replicate_batch" "─────────────────────────────────────────────"
        
        # Load transport
        if ! _load_transport "$dest_transport"; then
            log_error "replication_local_module.sh" "replicate_batch" "Failed to load transport: $dest_transport"
            REPLICATION_DEST_STATUS["$dest_name"]="failed"
            REPLICATION_DEST_ERROR["$dest_name"]="Transport load failed"
            ((REPLICATION_TOTAL_FAILED++))
            
            if [[ "${REPLICATION_ON_FAILURE}" == "abort" ]]; then
                log_error "replication_local_module.sh" "replicate_batch" "Aborting replication (on_failure=abort)"
                break
            fi
            continue
        fi
        
        # Initialize transport (verify destination)
        if ! transport_init "$dest_path" "$dest_name"; then
            log_error "replication_local_module.sh" "replicate_batch" "Destination not accessible: $dest_name"
            REPLICATION_DEST_STATUS["$dest_name"]="failed"
            REPLICATION_DEST_ERROR["$dest_name"]="Destination not accessible"
            ((REPLICATION_TOTAL_FAILED++))
            
            if [[ "${REPLICATION_ON_FAILURE}" == "abort" ]]; then
                log_error "replication_local_module.sh" "replicate_batch" "Aborting replication (on_failure=abort)"
                break
            fi
            continue
        fi
        
        # Space check
        if [[ $source_size -gt 0 ]]; then
            if ! _check_destination_space "$dest_path" "$source_size" "$dest_name"; then
                log_error "replication_local_module.sh" "replicate_batch" "Insufficient space at: $dest_name"
                REPLICATION_DEST_STATUS["$dest_name"]="skipped"
                REPLICATION_DEST_ERROR["$dest_name"]="Insufficient space"
                ((REPLICATION_TOTAL_SKIPPED++))
                continue
            fi
        fi
        
        # Perform sync
        local sync_start
        sync_start=$(date +%s)
        
        if transport_sync "$backup_root" "$dest_path" "$sync_mode" "$bwlimit" "$REPLICATION_DRY_RUN"; then
            local sync_end
            sync_end=$(date +%s)
            local sync_duration=$((sync_end - sync_start))
            
            REPLICATION_DEST_STATUS["$dest_name"]="success"
            REPLICATION_DEST_BYTES["$dest_name"]="${TRANSPORT_BYTES_TRANSFERRED:-0}"
            REPLICATION_DEST_FILES["$dest_name"]="${TRANSPORT_FILES_TRANSFERRED:-0}"
            REPLICATION_DEST_DURATION["$dest_name"]="$sync_duration"
            
            # Capture transport metrics (contract v1.0)
            REPLICATION_DEST_AVAIL_BYTES["$dest_name"]="${TRANSPORT_DEST_AVAIL_BYTES:-0}"
            REPLICATION_DEST_TOTAL_BYTES["$dest_name"]="${TRANSPORT_DEST_TOTAL_BYTES:-0}"
            REPLICATION_DEST_SPACE_KNOWN["$dest_name"]="${TRANSPORT_DEST_SPACE_KNOWN:-0}"
            REPLICATION_DEST_THROTTLE["$dest_name"]="${TRANSPORT_THROTTLE_COUNT:--1}"
            REPLICATION_DEST_BWLIMIT["$dest_name"]="${TRANSPORT_BWLIMIT_FINAL:-}"
            
            # Verify if not dry run
            if [[ "$REPLICATION_DRY_RUN" != "true" ]] && [[ "$verify_mode" != "none" ]]; then
                if transport_verify "$backup_root" "$dest_path" "$verify_mode"; then
                    log_info "replication_local_module.sh" "replicate_batch" "Destination $dest_name: SUCCESS (${sync_duration}s)"
                    ((REPLICATION_TOTAL_SUCCESS++))
                else
                    log_error "replication_local_module.sh" "replicate_batch" "Verification failed for: $dest_name"
                    REPLICATION_DEST_STATUS["$dest_name"]="failed"
                    REPLICATION_DEST_ERROR["$dest_name"]="Verification failed"
                    ((REPLICATION_TOTAL_FAILED++))
                fi
            else
                log_info "replication_local_module.sh" "replicate_batch" "Destination $dest_name: SUCCESS (${sync_duration}s)"
                ((REPLICATION_TOTAL_SUCCESS++))
            fi
        else
            local sync_end
            sync_end=$(date +%s)
            local sync_duration=$((sync_end - sync_start))
            
            # Distinguish cancellation from failure
            if [[ "${TRANSPORT_CANCELLED:-0}" -eq 1 ]]; then
                log_warn "replication_local_module.sh" "replicate_batch" "Destination $dest_name: CANCELLED by operator (${sync_duration}s)"
                REPLICATION_DEST_STATUS["$dest_name"]="cancelled"
                REPLICATION_DEST_DURATION["$dest_name"]="$sync_duration"
                REPLICATION_DEST_ERROR["$dest_name"]="Replication cancelled by operator"
                # Skip remaining destinations — cancellation applies to all
                break
            fi
            
            log_error "replication_local_module.sh" "replicate_batch" "Sync failed for: $dest_name"
            REPLICATION_DEST_STATUS["$dest_name"]="failed"
            REPLICATION_DEST_DURATION["$dest_name"]="$sync_duration"
            REPLICATION_DEST_ERROR["$dest_name"]="Rsync failed"
            
            # Capture transport metrics even on failure (contract v1.0)
            REPLICATION_DEST_AVAIL_BYTES["$dest_name"]="${TRANSPORT_DEST_AVAIL_BYTES:-0}"
            REPLICATION_DEST_TOTAL_BYTES["$dest_name"]="${TRANSPORT_DEST_TOTAL_BYTES:-0}"
            REPLICATION_DEST_SPACE_KNOWN["$dest_name"]="${TRANSPORT_DEST_SPACE_KNOWN:-0}"
            REPLICATION_DEST_THROTTLE["$dest_name"]="${TRANSPORT_THROTTLE_COUNT:--1}"
            REPLICATION_DEST_BWLIMIT["$dest_name"]="${TRANSPORT_BWLIMIT_FINAL:-}"
            ((REPLICATION_TOTAL_FAILED++))
            
            if [[ "${REPLICATION_ON_FAILURE}" == "abort" ]]; then
                log_error "replication_local_module.sh" "replicate_batch" "Aborting replication (on_failure=abort)"
                break
            fi
        fi
        
        # Cleanup
        transport_cleanup "$dest_path"
    done
    
    REPLICATION_END_TIME=$(date -u '+%Y-%m-%d %H:%M:%S')
    
    log_info "replication_local_module.sh" "replicate_batch" "─────────────────────────────────────────────"
    log_info "replication_local_module.sh" "replicate_batch" "Batch replication complete: $REPLICATION_TOTAL_SUCCESS success, $REPLICATION_TOTAL_FAILED failed, $REPLICATION_TOTAL_SKIPPED skipped"
    
    # Calculate total duration
    local start_epoch end_epoch total_duration
    start_epoch=$(date -d "${REPLICATION_START_TIME} UTC" +%s 2>/dev/null || echo "0")
    end_epoch=$(date -d "${REPLICATION_END_TIME} UTC" +%s 2>/dev/null || date +%s)
    total_duration=$((end_epoch - start_epoch))
    
    local duration_min=$((total_duration / 60))
    local duration_sec=$((total_duration % 60))
    log_info "replication_local_module.sh" "replicate_batch" "Total time: ${duration_min}m ${duration_sec}s"
    
    # Write state to file for email reporting (subshell isolation fix)
    # Values MUST be quoted to handle spaces in timestamps
    local state_file="${STATE_DIR}/local_replication_state.txt"
    {
        echo "REPLICATION_MODULE_LOADED=1"
        echo "REPLICATION_ENABLED=\"${REPLICATION_ENABLED:-no}\""
        echo "REPLICATION_START_TIME=\"$REPLICATION_START_TIME\""
        echo "REPLICATION_END_TIME=\"$REPLICATION_END_TIME\""
        echo "REPLICATION_TOTAL_SUCCESS=$REPLICATION_TOTAL_SUCCESS"
        echo "REPLICATION_TOTAL_FAILED=$REPLICATION_TOTAL_FAILED"
        echo "REPLICATION_TOTAL_SKIPPED=$REPLICATION_TOTAL_SKIPPED"
        echo "REPLICATION_TOTAL_BYTES=$REPLICATION_TOTAL_BYTES"
        # Save destination arrays as pipe-delimited lines (quoted for safety)
        for dest_name in "${!REPLICATION_DEST_STATUS[@]}"; do
            local status="${REPLICATION_DEST_STATUS[$dest_name]:-}"
            local bytes="${REPLICATION_DEST_BYTES[$dest_name]:-0}"
            local duration="${REPLICATION_DEST_DURATION[$dest_name]:-0}"
            local transport="${REPLICATION_DEST_TRANSPORT[$dest_name]:-}"
            local path="${REPLICATION_DEST_PATH[$dest_name]:-}"
            local error="${REPLICATION_DEST_ERROR[$dest_name]:-}"
            echo "DEST_ENTRY=\"${dest_name}|${status}|${bytes}|${duration}|${transport}|${path}|${error}\""
        done
    } > "$state_file"
    log_debug "replication_local_module.sh" "replicate_batch" "Wrote state to $state_file"
    
    # Log to SQLite database (parallel to state file)
    if type sqlite_is_available &>/dev/null && sqlite_is_available; then
        for dest_name in "${!REPLICATION_DEST_STATUS[@]}"; do
            local status="${REPLICATION_DEST_STATUS[$dest_name]:-unknown}"
            local bytes="${REPLICATION_DEST_BYTES[$dest_name]:-0}"
            local duration="${REPLICATION_DEST_DURATION[$dest_name]:-0}"
            local transport="${REPLICATION_DEST_TRANSPORT[$dest_name]:-}"
            local dest_path="${REPLICATION_DEST_PATH[$dest_name]:-}"
            local error="${REPLICATION_DEST_ERROR[$dest_name]:-}"
            local log_file
            log_file=$(tu_get_replication_log_path "local" "$dest_name" 2>/dev/null || echo "")
            # Don't record log_file if it doesn't actually exist on disk
            [[ -n "$log_file" && ! -f "$log_file" ]] && log_file=""
            
            # Get sync_mode from destination config
            local sync_mode_var="DEST_${dest_name^^}_SYNC_MODE"
            sync_mode_var="${sync_mode_var//-/_}"
            local sync_mode="${!sync_mode_var:-mirror}"
            
            local run_id
            run_id=$(sqlite_log_replication_run \
                "$dest_name" \
                "local" \
                "$transport" \
                "$sync_mode" \
                "$dest_path" \
                "$REPLICATION_START_TIME" \
                "$REPLICATION_END_TIME" \
                "$duration" \
                "$bytes" \
                "${REPLICATION_DEST_FILES[$dest_name]:-0}" \
                "$status" \
                "$error" \
                "$log_file" \
                "${REPLICATION_DEST_AVAIL_BYTES[$dest_name]:-0}" \
                "${REPLICATION_DEST_TOTAL_BYTES[$dest_name]:-0}" \
                "${REPLICATION_DEST_SPACE_KNOWN[$dest_name]:-0}" \
                "${REPLICATION_DEST_THROTTLE[$dest_name]:--1}" \
                "${REPLICATION_DEST_BWLIMIT[$dest_name]:-}")
            
            # Log VMs included in this replication (all backed up VMs)
            if [[ -n "$run_id" ]] && [[ ${#VM_BACKUP_RESULTS[@]} -gt 0 ]]; then
                local vm_names=()
                for result in "${VM_BACKUP_RESULTS[@]}"; do
                    IFS='|' read -r vm vm_status rest <<< "$result"
                    [[ "$vm_status" == "SUCCESS" ]] && vm_names+=("$vm")
                done
                [[ ${#vm_names[@]} -gt 0 ]] && sqlite_log_replication_vms "$run_id" "$status" "${vm_names[@]}"
            fi
        done
        log_debug "replication_local_module.sh" "replicate_batch" "SQLite replication runs logged"
    fi
    
    if [[ $REPLICATION_TOTAL_FAILED -gt 0 ]]; then
        return 1
    fi
    return 0
}

#################################################################################
# get_replication_summary - Generate summary string for email report
#
# This function produces the "Replication Summary" section of the backup email.
# It reads from the state tracking variables to build a formatted report.
#
# WORKS SEAMLESSLY FOR BOTH MODES:
#   - Batch mode:  State variables contain single sync operation totals
#   - Per-VM mode: State variables contain ACCUMULATED totals across all VMs
#
# OUTPUT FORMAT:
#   truenas: ✅ SUCCESS - 265MiB in 0m 3s (88.3MiB/s)
#   offsite-ssh: ⏭️ DISABLED
#   nas-smb: ⏭️ DISABLED
#
#   Total Replicated: 265MiB to 1 destination(s)
#
# Returns:
#   Prints multi-line summary to stdout (for capture by email report module)
#################################################################################
get_replication_summary() {
    local state_file="${STATE_DIR}/local_replication_state.txt"
    
    #---------------------------------------------------------------------------
    # Load state from file if variables are empty (subshell isolation fix)
    #---------------------------------------------------------------------------
    if [[ ${#REPLICATION_DEST_STATUS[@]} -eq 0 ]]; then
        if [[ -f "$state_file" ]]; then
            while IFS='=' read -r key value; do
                # Strip quotes from value
                value="${value#\"}"
                value="${value%\"}"
                case "$key" in
                    REPLICATION_MODULE_LOADED) REPLICATION_MODULE_LOADED="$value" ;;
                    REPLICATION_ENABLED) REPLICATION_ENABLED="$value" ;;
                    REPLICATION_TOTAL_BYTES) REPLICATION_TOTAL_BYTES="$value" ;;
                    REPLICATION_TOTAL_SUCCESS) REPLICATION_TOTAL_SUCCESS="$value" ;;
                    REPLICATION_TOTAL_FAILED) REPLICATION_TOTAL_FAILED="$value" ;;
                    DEST_ENTRY)
                        # Parse: name|status|bytes|duration|transport|path|error
                        local name status bytes duration transport path error
                        IFS='|' read -r name status bytes duration transport path error <<< "$value"
                        REPLICATION_DEST_STATUS["$name"]="$status"
                        REPLICATION_DEST_BYTES["$name"]="$bytes"
                        REPLICATION_DEST_DURATION["$name"]="$duration"
                        REPLICATION_DEST_TRANSPORT["$name"]="$transport"
                        REPLICATION_DEST_PATH["$name"]="$path"
                        REPLICATION_DEST_ERROR["$name"]="$error"
                        ;;
                esac
            done < "$state_file"
        fi
    fi

    #---------------------------------------------------------------------------
    # Guard: Return early if replication is disabled
    #---------------------------------------------------------------------------
    if [[ "$REPLICATION_MODULE_LOADED" -ne 1 ]] || [[ "${REPLICATION_ENABLED:-no}" != "yes" ]]; then
        echo "Replication: Disabled"
        return
    fi
    
    local summary=""
    local total_bytes=0
    local total_duration=0
    
    #---------------------------------------------------------------------------
    # Build per-destination summary lines
    # Iterate over all destinations that have status recorded
    #---------------------------------------------------------------------------
    for dest_name in "${!REPLICATION_DEST_STATUS[@]}"; do
        local status="${REPLICATION_DEST_STATUS[$dest_name]}"
        local bytes="${REPLICATION_DEST_BYTES[$dest_name]:-0}"      # ACCUMULATED total
        local duration="${REPLICATION_DEST_DURATION[$dest_name]:-0}" # ACCUMULATED total
        local error="${REPLICATION_DEST_ERROR[$dest_name]:-}"
        
        # Map status to emoji icon for visual clarity in email
        local status_icon=""
        case "$status" in
            success)  status_icon="✅ SUCCESS" ;;
            failed)   status_icon="❌ FAILED" ;;
            skipped)  status_icon="⏭️ SKIPPED" ;;
            disabled) status_icon="⏭️ DISABLED" ;;
            *)        status_icon="❓ $status" ;;
        esac
        
        if [[ "$status" == "success" ]]; then
            # Format bytes and duration for human readability
            local bytes_human duration_min duration_sec
            bytes_human=$(numfmt --to=iec-i --suffix=B "$bytes" 2>/dev/null || echo "$bytes bytes")
            duration_min=$((duration / 60))
            duration_sec=$((duration % 60))
            
            # Calculate average transfer speed (if we have meaningful data)
            local speed_human=""
            if [[ $duration -gt 0 ]] && [[ $bytes -gt 0 ]]; then
                local speed=$((bytes / duration))
                speed_human=$(numfmt --to=iec-i --suffix=B/s "$speed" 2>/dev/null || echo "")
                [[ -n "$speed_human" ]] && speed_human=" ($speed_human)"
            fi
            
            summary+="${dest_name}: ${status_icon} - ${bytes_human} in ${duration_min}m ${duration_sec}s${speed_human}\n"
            
            # Accumulate for grand total line
            total_bytes=$((total_bytes + bytes))
            total_duration=$((total_duration + duration))
            
        elif [[ "$status" == "failed" ]]; then
            # Show error message for failed destinations
            summary+="${dest_name}: ${status_icon} - ${error}\n"
        else
            # Skipped/disabled - just show status
            summary+="${dest_name}: ${status_icon}\n"
        fi
    done
    
    #---------------------------------------------------------------------------
    # Add grand total line (only if we had successful replications)
    # Count unique successful destinations from REPLICATION_DEST_STATUS
    # (REPLICATION_TOTAL_SUCCESS counts operations, not unique destinations)
    #---------------------------------------------------------------------------
    if [[ $REPLICATION_TOTAL_SUCCESS -gt 0 ]]; then
        local total_human
        total_human=$(numfmt --to=iec-i --suffix=B "$total_bytes" 2>/dev/null || echo "$total_bytes bytes")
        
        # Count unique successful destinations
        local successful_dest_count=0
        for dest_name in "${!REPLICATION_DEST_STATUS[@]}"; do
            if [[ "${REPLICATION_DEST_STATUS[$dest_name]}" == "success" ]]; then
                ((successful_dest_count++))
            fi
        done
        
        summary+="\nTotal Replicated: ${total_human} to ${successful_dest_count} destination(s)"
    fi
    
    echo -e "$summary"
}

#################################################################################
# get_replication_status - Get overall replication status code
#
# Returns exit code indicating overall replication health for scripting use.
# Used by vmbackup.sh to determine if replication had issues.
#
# Returns:
#   0 - All successful (or replication disabled/not configured)
#   1 - Partial success (some destinations failed, some succeeded)
#   2 - All failed (no successful replications)
#################################################################################
get_replication_status() {
    # Disabled or not loaded = success (no replication to fail)
    if [[ "$REPLICATION_MODULE_LOADED" -ne 1 ]] || [[ "${REPLICATION_ENABLED:-no}" != "yes" ]]; then
        return 0
    fi
    
    # No failures = success
    if [[ $REPLICATION_TOTAL_FAILED -eq 0 ]]; then
        return 0
    # Some success + some failure = partial
    elif [[ $REPLICATION_TOTAL_SUCCESS -gt 0 ]]; then
        return 1
    # All failed = total failure
    else
        return 2
    fi
}

#################################################################################
# get_local_replication_stats - Get one-line summary for email header
#
# Returns a single-line summary like:
#   "✓ 1/1 destinations │ 510.1 MiB in 10s"
#
# Used in the SUMMARY section of the email.
#################################################################################
get_local_replication_stats() {
    local state_file="${STATE_DIR}/local_replication_state.txt"
    
    # Try to load state from file if variables are empty (subshell isolation fix)
    if [[ ${#REPLICATION_DEST_STATUS[@]} -eq 0 ]]; then
        if [[ -f "$state_file" ]]; then
            # Load scalar variables
            while IFS='=' read -r key value; do
                # Strip quotes from value
                value="${value#\"}"
                value="${value%\"}"
                case "$key" in
                    REPLICATION_MODULE_LOADED) REPLICATION_MODULE_LOADED="$value" ;;
                    REPLICATION_ENABLED) REPLICATION_ENABLED="$value" ;;
                    REPLICATION_TOTAL_BYTES) REPLICATION_TOTAL_BYTES="$value" ;;
                    DEST_ENTRY)
                        # Parse: name|status|bytes|duration|transport|path|error
                        local name status bytes duration transport path error
                        IFS='|' read -r name status bytes duration transport path error <<< "$value"
                        REPLICATION_DEST_STATUS["$name"]="$status"
                        REPLICATION_DEST_BYTES["$name"]="$bytes"
                        REPLICATION_DEST_DURATION["$name"]="$duration"
                        REPLICATION_DEST_TRANSPORT["$name"]="$transport"
                        REPLICATION_DEST_PATH["$name"]="$path"
                        REPLICATION_DEST_ERROR["$name"]="$error"
                        ;;
                esac
            done < "$state_file"
        fi
    fi
    
    # Guard: Return empty if replication is disabled
    if [[ "$REPLICATION_MODULE_LOADED" -ne 1 ]] || [[ "${REPLICATION_ENABLED:-no}" != "yes" ]]; then
        return
    fi
    
    # Guard: Return empty if no destinations processed
    if [[ ${#REPLICATION_DEST_STATUS[@]} -eq 0 ]]; then
        return
    fi
    
    # Count destinations by status
    local success_count=0
    local total_count=0
    local total_bytes=0
    local total_duration=0
    
    for dest_name in "${!REPLICATION_DEST_STATUS[@]}"; do
        local status="${REPLICATION_DEST_STATUS[$dest_name]}"
        local bytes="${REPLICATION_DEST_BYTES[$dest_name]:-0}"
        local duration="${REPLICATION_DEST_DURATION[$dest_name]:-0}"
        
        # Only count enabled destinations (not disabled)
        if [[ "$status" != "disabled" ]]; then
            ((total_count++))
            if [[ "$status" == "success" ]]; then
                ((success_count++))
                total_bytes=$((total_bytes + bytes))
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
    if [[ $total_duration -ge 60 ]]; then
        duration_fmt="$((total_duration / 60))m $((total_duration % 60))s"
    else
        duration_fmt="${total_duration}s"
    fi
    
    echo "$status_icon $success_count/$total_count destinations │ $bytes_human in $duration_fmt"
}

#################################################################################
# get_local_replication_details - Generate compact per-destination lines
#
# This function produces the "LOCAL REPLICATION" section of the backup email
# in compact format (one line per destination).
#
# OUTPUT FORMAT (compact):
#   ┌─ truenas ────────────────────────────────────────────────────────────────
#   │ NFS → /mnt/truenas/vms │ ✓ SUCCESS │ 510.1 MiB in 10s (51.0 MiB/s)
#   └──────────────────────────────────────────────────────────────────────────────
#   │ offsite-ssh: ⏭️ DISABLED
#   │ nas-smb: ⏭️ DISABLED
#
# Returns:
#   Prints formatted lines to stdout
#################################################################################
get_local_replication_details() {
    local state_file="${STATE_DIR}/local_replication_state.txt"
    
    # Try to load state from file if variables are empty (subshell isolation fix)
    if [[ ${#REPLICATION_DEST_STATUS[@]} -eq 0 ]]; then
        if [[ -f "$state_file" ]]; then
            # Load scalar variables
            while IFS='=' read -r key value; do
                # Strip quotes from value
                value="${value#\"}"
                value="${value%\"}"
                case "$key" in
                    REPLICATION_MODULE_LOADED) REPLICATION_MODULE_LOADED="$value" ;;
                    REPLICATION_ENABLED) REPLICATION_ENABLED="$value" ;;
                    DEST_ENTRY)
                        # Parse: name|status|bytes|duration|transport|path|error
                        local name status bytes duration transport path error
                        IFS='|' read -r name status bytes duration transport path error <<< "$value"
                        REPLICATION_DEST_STATUS["$name"]="$status"
                        REPLICATION_DEST_BYTES["$name"]="$bytes"
                        REPLICATION_DEST_DURATION["$name"]="$duration"
                        REPLICATION_DEST_TRANSPORT["$name"]="$transport"
                        REPLICATION_DEST_PATH["$name"]="$path"
                        REPLICATION_DEST_ERROR["$name"]="$error"
                        ;;
                esac
            done < "$state_file"
        fi
    fi
    
    # Guard: Return empty if replication is disabled
    if [[ "$REPLICATION_MODULE_LOADED" -ne 1 ]] || [[ "${REPLICATION_ENABLED:-no}" != "yes" ]]; then
        return
    fi
    
    # Guard: Return empty if no destinations processed
    if [[ ${#REPLICATION_DEST_STATUS[@]} -eq 0 ]]; then
        return
    fi
    
    local output=""
    
    for dest_name in "${!REPLICATION_DEST_STATUS[@]}"; do
        local status="${REPLICATION_DEST_STATUS[$dest_name]}"
        local bytes="${REPLICATION_DEST_BYTES[$dest_name]:-0}"
        local duration="${REPLICATION_DEST_DURATION[$dest_name]:-0}"
        local error="${REPLICATION_DEST_ERROR[$dest_name]:-}"
        local transport="${REPLICATION_DEST_TRANSPORT[$dest_name]:-unknown}"
        local path="${REPLICATION_DEST_PATH[$dest_name]:-}"
        
        # Transport display (uppercase)
        local transport_display="${transport^^}"
        
        if [[ "$status" == "success" ]]; then
            # Success: Full block with transfer stats
            local bytes_human duration_fmt speed_human=""
            bytes_human=$(numfmt --to=iec-i --suffix=B "$bytes" 2>/dev/null || echo "$bytes bytes")
            
            if [[ $duration -ge 60 ]]; then
                duration_fmt="$((duration / 60))m $((duration % 60))s"
            else
                duration_fmt="${duration}s"
            fi
            
            if [[ $duration -gt 0 ]] && [[ $bytes -gt 0 ]]; then
                local speed=$((bytes / duration))
                speed_human=" ($(numfmt --to=iec-i --suffix=B/s "$speed" 2>/dev/null || echo "${speed}B/s"))"
            fi
            
            output+="┌─ ${dest_name} ────────────────────────────────
│ ${transport_display} → ${path} │ ✓ SUCCESS
│ ${bytes_human} in ${duration_fmt}${speed_human}
└──────────────────────────────────────────────────
"
        elif [[ "$status" == "failed" ]]; then
            # Failed: Show error
            output+="┌─ ${dest_name} ────────────────────────────────
│ ${transport_display} → ${path} │ ✗ FAILED
│ ${error}
└──────────────────────────────────────────────────
"
        else
            # Disabled/Skipped: Simple one-liner
            local status_display=""
            case "$status" in
                disabled) status_display="⏭️ DISABLED" ;;
                skipped)  status_display="⏭️ SKIPPED" ;;
                *)        status_display="? $status" ;;
            esac
            output+="│ ${dest_name}: ${status_display}
"
        fi
    done
    
    echo -n "$output"
}

#################################################################################
# END OF MODULE
#################################################################################
