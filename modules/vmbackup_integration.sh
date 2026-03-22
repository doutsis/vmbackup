#!/bin/bash

#################################################################################
# VMBackup Integration Module - VM-First Directory Structure
#
# Provides the integration layer for VM-first backup paths:
#   /backup/vm_name/period/  (e.g., /mnt/backup/vms/my-vm/202602/)
#
# Core Functions:
#   - get_backup_dir()     - Returns full path to VM's current backup directory
#   - pre_backup_hook()    - Period boundary detection, chain archiving
#   - post_backup_hook()   - Manifest updates, retention cleanup
#
# Dependencies:
#   - rotation_module.sh      (policies, period IDs, paths)
#   - logging_module.sh       (CSV logging schema v3.0)
#   - chain_manifest_module.sh (JSON manifest management)
#   - retention_module.sh     (per-VM retention enforcement)
#   - config/default/         (configuration files)
#
#################################################################################

# Guard against multiple inclusion
[[ -n "${_VMBACKUP_INTEGRATION_LOADED:-}" ]] && return 0
readonly _VMBACKUP_INTEGRATION_LOADED=1

readonly VMBACKUP_INTEGRATION_VERSION="2.0"

#################################################################################
# CONFIGURATION
#################################################################################

INTEGRATION_SCRIPT_DIR="${INTEGRATION_SCRIPT_DIR:-$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")}"

#################################################################################
# MODULE LOADING
#################################################################################

load_vmbackup_modules() {
    local module module_path
    local modules=(rotation_module.sh logging_module.sh chain_manifest_module.sh retention_module.sh)
    
    for module in "${modules[@]}"; do
        module_path="${INTEGRATION_SCRIPT_DIR}/${module}"
        if [[ -f "$module_path" ]]; then
            # shellcheck source=/dev/null
            source "$module_path" 2>/dev/null || {
                log_error "vmbackup_integration.sh" "load_vmbackup_modules" \
                    "Failed to source: $module (syntax error?)"
                return 1
            }
            log_debug "vmbackup_integration.sh" "load_vmbackup_modules" "Loaded: $module"
        else
            log_warn "vmbackup_integration.sh" "load_vmbackup_modules" "Module not found: $module_path"
        fi
    done
    
    # Load rotation config if available (use CONFIG_INSTANCE from vmbackup.sh)
    local instance="${CONFIG_INSTANCE:-default}"
    declare -f load_rotation_config >/dev/null 2>&1 && load_rotation_config "$instance"
    return 0
}

#################################################################################
# PATH FUNCTIONS
#################################################################################

# Get backup directory for a VM
# Args: $1 - vm_name
# Returns: Full path to VM's current backup directory (VM-first structure)
get_backup_dir() {
    local vm_name="$1"
    get_vm_backup_dir "$vm_name"
}

#################################################################################
# ACCUMULATE LIMIT CHECK (Pre-Backup)
#################################################################################

# Check accumulate chain depth and archive + force full if limit exceeded
# Args: $1 - vm_name
# Returns: 0 always (sets recovery flag if limit hit)
_check_accumulate_limit_pre_backup() {
    local vm_name="$1"
    local safe_name=$(sanitize_vm_name "$vm_name")
    local vm_dir="${BACKUP_PATH}${safe_name}"
    
    local hard_limit=${ACCUMULATE_HARD_LIMIT:-365}
    local warn_depth=${ACCUMULATE_WARN_DEPTH:-100}
    
    # Count checkpoint depth (number of .data files indicates chain length)
    local chain_depth=$(find "$vm_dir" -maxdepth 1 -type f -name "*.data" 2>/dev/null | wc -l)
    
    # Warning threshold - log warning (fires independently of hard limit)
    if [[ "$chain_depth" -ge "$warn_depth" ]]; then
        log_warn "vmbackup_integration.sh" "_check_accumulate_limit_pre_backup" \
            "ACCUMULATE chain depth warning: $vm_name has $chain_depth backups (warn threshold: $warn_depth, limit: $hard_limit)"
    else
        log_debug "vmbackup_integration.sh" "_check_accumulate_limit_pre_backup" \
            "Accumulate chain depth OK: $vm_name has $chain_depth backups (warn: $warn_depth, limit: $hard_limit)"
    fi
    
    # Hard limit reached - archive chain and force full backup
    if [[ "$chain_depth" -ge "$hard_limit" ]]; then
        log_warn "vmbackup_integration.sh" "_check_accumulate_limit_pre_backup" \
            "ACCUMULATE chain depth limit reached: $vm_name ($chain_depth >= $hard_limit)"
        
        # Archive the current chain using the comprehensive archive function
        if declare -f archive_existing_checkpoint_chain >/dev/null 2>&1; then
            log_info "vmbackup_integration.sh" "_check_accumulate_limit_pre_backup" \
                "Archiving accumulate chain for $vm_name before forced full backup"
            archive_existing_checkpoint_chain "$vm_name" "$vm_dir"
        fi
        
        # Delete QEMU checkpoint metadata so virtnbdbackup doesn't try to
        # clean up bitmaps that may no longer exist (causes "bitmap not found" errors)
        local checkpoints
        mapfile -t checkpoints < <(virsh checkpoint-list "$vm_name" --name 2>/dev/null | grep "^virtnbdbackup\.")
        if [[ ${#checkpoints[@]} -gt 0 ]]; then
            log_info "vmbackup_integration.sh" "_check_accumulate_limit_pre_backup" \
                "Deleting ${#checkpoints[@]} QEMU checkpoint(s) for $vm_name"
            for cp in "${checkpoints[@]}"; do
                if virsh checkpoint-delete "$vm_name" "$cp" --metadata 2>/dev/null; then
                    log_debug "vmbackup_integration.sh" "_check_accumulate_limit_pre_backup" \
                        "Deleted QEMU checkpoint: $cp"
                else
                    log_warn "vmbackup_integration.sh" "_check_accumulate_limit_pre_backup" \
                        "Failed to delete QEMU checkpoint: $cp (may not exist)"
                fi
            done
        fi
        
        # Remove stale dirty bitmaps from qcow2 disk images
        if declare -f remove_stale_qemu_bitmaps >/dev/null 2>&1; then
            remove_stale_qemu_bitmaps "$vm_name"
        fi
        
        # Set recovery flag to force full backup in backup_vm()
        mkdir -p "${TEMP_DIR}" 2>/dev/null
        touch "${TEMP_DIR}/vmbackup-recovery-${vm_name}.flag"
        log_info "vmbackup_integration.sh" "_check_accumulate_limit_pre_backup" \
            "Recovery flag set - next backup will be FULL for $vm_name"
        
        return 0
    fi
    
    return 0
}

#################################################################################
# BACKUP LIFECYCLE HOOKS
#################################################################################

# Pre-backup hook - called before starting VM backup
# Handles period boundary detection and chain archiving
# Args: $1 - vm_name
# Returns: 0 to continue, 1 to skip backup
pre_backup_hook() {
    local vm_name="$1"
    local policy=$(get_vm_rotation_policy "$vm_name")
    
    log_debug "vmbackup_integration.sh" "pre_backup_hook" \
        "Entry: vm='$vm_name' policy='$policy'"
    
    # Check for exclusion
    [[ "$policy" == "never" ]] && {
        log_info "vmbackup_integration.sh" "pre_backup_hook" \
            "VM $vm_name excluded by rotation policy (never)"
        return 1
    }
    
    # Create state backup at start of session
    declare -f backup_state_files >/dev/null 2>&1 && backup_state_files
    
    # Initialize chain manifest
    declare -f init_chain_manifest >/dev/null 2>&1 && init_chain_manifest "$vm_name"
    
    # Handle accumulate policy - check chain depth limit
    if [[ "$policy" == "accumulate" ]]; then
        log_debug "vmbackup_integration.sh" "pre_backup_hook" \
            "Accumulate policy for '$vm_name' - checking chain depth limit"
        _check_accumulate_limit_pre_backup "$vm_name"
        return 0
    fi
    
    local current_period=$(get_period_id "$policy")
    local safe_name=$(sanitize_vm_name "$vm_name")
    local vm_dir="${BACKUP_PATH}${safe_name}"
    
    # Use list_vm_periods (sorted newest-first) to find last period
    local last_period=""
    if declare -f list_vm_periods >/dev/null 2>&1; then
        last_period=$(list_vm_periods "$vm_name" | head -1)
    fi
    
    if [[ -n "$last_period" && "$last_period" != "$current_period" ]]; then
        log_info "vmbackup_integration.sh" "pre_backup_hook" \
            "Period boundary detected: $last_period -> $current_period (vm: $vm_name)"
        
        # Archive active chains from previous period
        declare -f archive_active_chains >/dev/null 2>&1 && \
            archive_active_chains "$vm_name" "$last_period" "period_boundary"
        
        # Log period lifecycle
        if declare -f log_period_lifecycle >/dev/null 2>&1; then
            log_period_lifecycle "period_closed" "$vm_name" "$last_period" "$policy" \
                "${vm_dir}/${last_period}" "" "" "0" "0" "0" "" "" ""
            log_period_lifecycle "period_created" "$vm_name" "$current_period" "$policy" \
                "${vm_dir}/${current_period}" "" "" "0" "0" "0" "$last_period" "" ""
        fi
        
        # Validate time progression (clock skew detection)
        if declare -f validate_time_progression >/dev/null 2>&1; then
            validate_time_progression "$vm_name" "$current_period" "$last_period" || \
                log_warn "vmbackup_integration.sh" "pre_backup_hook" \
                    "Clock skew detected for $vm_name - proceeding with caution"
        fi
    fi
    
    return 0
}

# Post-backup hook - called after VM backup completes
# Handles manifest updates, retention, and logging
# Args: $1 - vm_name
#       $2 - backup_status (success|failed|skipped)
#       $3 - backup_type (full|incremental|copy)
#       $4 - backup_size_bytes
#       $5 - backup_duration_seconds
#       $6 - error_message (optional)
post_backup_hook() {
    local vm_name="$1"
    local backup_status="$2"
    local backup_type="$3"
    local backup_size_bytes="${4:-0}"
    local backup_duration_seconds="${5:-0}"
    local error_message="${6:-}"
    
    log_debug "vmbackup_integration.sh" "post_backup_hook" \
        "Entry: vm='$vm_name' status=$backup_status type=$backup_type size=$backup_size_bytes duration=${backup_duration_seconds}s"
    
    local policy
    policy=$(get_vm_rotation_policy "$vm_name")
    local period_id
    period_id=$(get_period_id "$policy")
    local backup_dir
    backup_dir=$(get_vm_backup_dir "$vm_name")
    
    if [[ "$backup_status" == "success" ]]; then
        # Generate restore point ID
        local chain_id
        chain_id=$(get_active_chain "$vm_name")
        
        if [[ -z "$chain_id" ]]; then
            chain_id=$(generate_chain_id)
            log_debug "vmbackup_integration.sh" "post_backup_hook" \
                "Generated new chain_id='$chain_id' for '$vm_name'"
        else
            log_debug "vmbackup_integration.sh" "post_backup_hook" \
                "Using existing chain_id='$chain_id' for '$vm_name'"
        fi
        
        local checkpoint
        checkpoint=$(count_period_restore_points "$vm_name" "$period_id")
        
        local restore_point_id
        restore_point_id=$(generate_restore_point_id "$vm_name" "$period_id" "$chain_id" "$checkpoint")
        
        # Add to manifest
        if declare -f add_restore_point >/dev/null 2>&1; then
            add_restore_point "$vm_name" "$restore_point_id" "$period_id" "$chain_id" \
                "$checkpoint" "$backup_type" "" "$backup_size_bytes" ""
        fi
        
        # Log chain lifecycle for new chains
        if [[ "$checkpoint" -eq 0 ]] && declare -f log_chain_lifecycle >/dev/null 2>&1; then
            log_chain_lifecycle "chain_created" "$vm_name" "$chain_id" "$period_id" \
                "$backup_dir" "." "1" "$backup_size_bytes" "" "" "$backup_type" \
                "$(date -u '+%Y-%m-%d %H:%M:%S')" ""
        fi
        
        # G2/G7: Update chain health in SQLite (success)
        # Use disk-based restore point count (not manifest-based $checkpoint) because
        # rebuilt manifests only track checkpoints created after the rebuild, missing
        # pre-existing ones. get_restore_point_count() counts actual .data files.
        if declare -f sqlite_update_chain_health >/dev/null 2>&1; then
            local disk_restore_points
            disk_restore_points=$(get_restore_point_count "$backup_dir")
            sqlite_update_chain_health "$vm_name" "$period_id" "$backup_dir" \
                "active" "${disk_restore_points:-$((checkpoint + 1))}" "" "" "$policy"
        fi
        
    elif [[ "$backup_status" == "failed" ]]; then
        # G1: Handle failure - log to chain health with error
        local chain_id
        chain_id=$(get_active_chain "$vm_name")
        
        if [[ -n "$chain_id" ]] && declare -f sqlite_update_chain_health >/dev/null 2>&1; then
            local checkpoint
            checkpoint=$(count_period_restore_points "$vm_name" "$period_id")
            sqlite_update_chain_health "$vm_name" "$period_id" "$backup_dir" \
                "active" "$checkpoint" "backup_failed" "$error_message" "$policy"
        fi
        
        log_warn "vmbackup_integration.sh" "post_backup_hook" \
            "Backup failed for $vm_name: $error_message"
    fi
    
    # Run retention cleanup (skip on failure to avoid cascading issues)
    if [[ "$backup_status" == "success" ]] && declare -f run_retention_for_vm >/dev/null 2>&1; then
        # Tier 1: Active policy retention (count-based, current policy format)
        run_retention_for_vm "$vm_name" "false"
        
        # Tier 2: Orphaned policy retention (age-based, previous policy formats)
        if declare -f run_orphan_retention_for_vm >/dev/null 2>&1; then
            run_orphan_retention_for_vm "$vm_name" "false"
        fi
    fi
    
    return 0
}

#################################################################################
# UTILITY FUNCTIONS
#################################################################################

# Get current period (replaces legacy get_current_month)
get_current_month() {
    get_period_id "${DEFAULT_ROTATION_POLICY:-monthly}"
}

# NOTE: get_chain_size(), get_total_dir_size(), get_restore_point_count()
# are defined in vmbackup.sh with fuller implementations that count .data files.
# These wrappers were removed to avoid duplication - vmbackup.sh definitions take precedence.

#################################################################################
# INITIALIZATION
#################################################################################

# Initialize integration on source
_init_vmbackup_integration() {
    # Ensure base logging exists before loading modules
    declare -f log_debug >/dev/null 2>&1 || log_debug() { :; }
    declare -f log_info >/dev/null 2>&1 || log_info() { echo "[INFO] $3"; }
    declare -f log_warn >/dev/null 2>&1 || log_warn() { echo "[WARN] $3" >&2; }
    declare -f log_error >/dev/null 2>&1 || log_error() { echo "[ERROR] $3" >&2; }
    
    # Load modules
    load_vmbackup_modules
    
    log_info "vmbackup_integration.sh" "init" \
        "Integration module v${VMBACKUP_INTEGRATION_VERSION} loaded (VM-first)"
}

# Run initialization
_init_vmbackup_integration
