#!/bin/bash

#################################################################################
# Rotation Module - VM-First Backup Directory Structure
#
# Provides rotation policy management and directory structure for vmbackup.
# Replaces month-first (BACKUP_PATH/YYYYMM/VM/) with VM-first 
# (BACKUP_PATH/VM/period/) layout with configurable rotation policies.
#
# SCHEMA VERSION: 3.0 (2026-02-01)
#
# Rotation Policies:
#   daily     - New period each day (YYYYMMDD)
#   weekly    - New period each Monday (YYYY-Www, ISO 8601)
#   monthly   - New period each month (YYYYMM)
#   accumulate- Single flat directory, no rotation
#   never     - Skip VM entirely
#
# Dependencies:
#   - BACKUP_PATH: Base backup directory (from vmbackup.sh or config)
#   - STATE_DIR: State directory for CSVs and manifests
#   - log_info, log_warn, log_error, log_debug: Logging functions
#
# Usage:
#   source rotation_module.sh
#   load_rotation_config "default"
#   policy=$(get_vm_rotation_policy "test-vm")
#   backup_dir=$(get_vm_backup_dir "test-vm")
#
#################################################################################

# Guard against multiple inclusion
[[ -n "${_ROTATION_MODULE_LOADED:-}" ]] && return 0
readonly _ROTATION_MODULE_LOADED=1

# Module version
readonly ROTATION_MODULE_VERSION="3.0"

#################################################################################
# DEFAULT CONFIGURATION
#################################################################################

# Default rotation policy (daily|weekly|monthly|accumulate|never)
# Note: BACKUP_ROTATION_POLICY is the runtime variable, DEFAULT_ROTATION_POLICY from config
BACKUP_ROTATION_POLICY="${DEFAULT_ROTATION_POLICY:-${BACKUP_ROTATION_POLICY:-monthly}}"

# Retention limits per policy
RETENTION_DAYS="${RETENTION_DAYS:-7}"
RETENTION_WEEKS="${RETENTION_WEEKS:-4}"
RETENTION_MONTHS="${RETENTION_MONTHS:-3}"

# Accumulate safety limits
ACCUMULATE_WARN_DEPTH="${ACCUMULATE_WARN_DEPTH:-100}"
ACCUMULATE_HARD_LIMIT="${ACCUMULATE_HARD_LIMIT:-365}"

# Per-VM policy overrides (associative array)
# Initialize only if not already declared
if ! declare -p VM_POLICY &>/dev/null; then
    declare -gA VM_POLICY=()
fi

# VM exclusion patterns (array of globs)
if ! declare -p EXCLUDE_PATTERNS &>/dev/null; then
    declare -ga EXCLUDE_PATTERNS=()
fi

# Config instance (for multi-instance support)
CONFIG_INSTANCE="${CONFIG_INSTANCE:-default}"

#################################################################################
# CONFIGURATION LOADING
#################################################################################

# Load rotation configuration from config directory
# Args: $1 - instance name (default: "default")
# Returns: 0 on success, 1 on error
load_rotation_config() {
    local instance="${1:-default}"
    # Use SCRIPT_DIR (project root set by vmbackup.sh), not module directory
    local script_dir="${SCRIPT_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
    local config_dir="${script_dir}/config/${instance}"
    
    CONFIG_INSTANCE="$instance"
    
    # Main config
    local main_config="${config_dir}/vmbackup.conf"
    if [[ -f "$main_config" ]]; then
        # shellcheck source=/dev/null
        source "$main_config" || {
            log_error "rotation_module.sh" "load_rotation_config" \
                "Failed to source: $main_config"
            return 1
        }
        # Re-normalize BACKUP_PATH trailing slash (config file may not have it)
        [[ -n "$BACKUP_PATH" && "$BACKUP_PATH" != */ ]] && BACKUP_PATH="${BACKUP_PATH}/"
        log_config_event "config_loaded" "$main_config" "" \
            "BACKUP_ROTATION_POLICY" "$BACKUP_ROTATION_POLICY" "" "all_vms" "load_rotation_config"
    else
        log_debug "rotation_module.sh" "load_rotation_config" \
            "No config file, using defaults: $main_config"
    fi
    
    # Per-VM overrides
    local overrides="${config_dir}/vm_overrides.conf"
    if [[ -f "$overrides" ]]; then
        # shellcheck source=/dev/null
        source "$overrides" || {
            log_warn "rotation_module.sh" "load_rotation_config" \
                "Failed to source overrides: $overrides"
        }
        local override_count="${#VM_POLICY[@]}"
        log_debug "rotation_module.sh" "load_rotation_config" \
            "Loaded VM overrides: ${override_count} entries"
    fi
    
    # Exclusion patterns
    local exclude_file="${config_dir}/exclude_patterns.conf"
    if [[ -f "$exclude_file" ]]; then
        # shellcheck source=/dev/null
        source "$exclude_file" || {
            log_warn "rotation_module.sh" "load_rotation_config" \
                "Failed to source exclusions: $exclude_file"
        }
        local exclude_count=0
        [[ -n "${EXCLUDE_PATTERNS[*]:-}" ]] && exclude_count="${#EXCLUDE_PATTERNS[@]}"
        log_debug "rotation_module.sh" "load_rotation_config" \
            "Loaded exclusion patterns: ${exclude_count} entries"
    fi
    
    return 0
}

#################################################################################
# CORE ROTATION FUNCTIONS
#################################################################################

# Get rotation policy for a VM
# Args: $1 - VM name
# Returns: Policy string (daily|weekly|monthly|accumulate|never)
get_vm_rotation_policy() {
    local vm_name="$1"
    
    # Validate input
    [[ -z "$vm_name" ]] && { echo "monthly"; return 0; }
    
    # Check exclusion patterns first (safely handle empty/unset array)
    local pattern
    for pattern in "${EXCLUDE_PATTERNS[@]:-}"; do
        [[ -z "$pattern" ]] && continue
        # shellcheck disable=SC2053
        if [[ "$vm_name" == $pattern ]]; then
            log_debug "rotation_module.sh" "get_vm_rotation_policy" \
                "VM '$vm_name' excluded by pattern: $pattern"
            echo "never"
            return 0
        fi
    done
    
    # Check per-VM override (safely handle unset associative array key)
    if [[ -v "VM_POLICY[$vm_name]" && -n "${VM_POLICY[$vm_name]}" ]]; then
        echo "${VM_POLICY[$vm_name]}"
        return 0
    fi
    
    # Return default policy
    log_debug "rotation_module.sh" "get_vm_rotation_policy" \
        "VM '$vm_name' using default policy: ${BACKUP_ROTATION_POLICY:-monthly}"
    echo "${BACKUP_ROTATION_POLICY:-monthly}"
}

# Get period ID for a given policy
# NOTE: Period IDs use LOCAL date (intentional — human-readable directory names).
#       DB timestamps (created_at) use UTC. Always match periods via backup_path
#       (which contains the actual directory name), never derive period_id from
#       created_at. See DATETIME_BUGS.md H2.
# Args: $1 - rotation policy
#       $2 - timestamp (optional, defaults to now)
# Returns: Period ID string (format varies by policy)
get_period_id() {
    local policy="$1"
    local timestamp="${2:-now}"
    
    case "$policy" in
        daily)
            date -d "$timestamp" +%Y%m%d
            ;;
        weekly)
            # ISO 8601 week: YYYY-Www (Monday is first day)
            date -d "$timestamp" +%Y-W%V
            ;;
        monthly)
            date -d "$timestamp" +%Y%m
            ;;
        accumulate)
            # Stable period_id for accumulate — used as DB key in chain_health
            # (UNIQUE(vm_name, period_id)) and in restore-point IDs.
            # Filesystem paths are NOT affected — get_vm_backup_dir() short-circuits
            # before calling get_period_id() for accumulate.
            echo "accumulate"
            ;;
        never)
            echo ""
            ;;
        *)
            log_error "rotation_module.sh" "get_period_id" \
                "Unknown rotation policy: $policy"
            echo ""
            return 1
            ;;
    esac
}

# Get full backup directory path for a VM
# Args: $1 - VM name
#       $2 - timestamp (optional, defaults to now)
# Returns: Full path to backup directory
get_vm_backup_dir() {
    local vm_name="$1"
    local timestamp="${2:-now}"
    
    # Validate input
    [[ -z "$vm_name" ]] && {
        log_warn "rotation_module.sh" "get_vm_backup_dir" "Empty vm_name supplied"
        echo ""; return 1
    }
    
    local policy=$(get_vm_rotation_policy "$vm_name")
    
    if [[ "$policy" == "never" ]]; then
        log_debug "rotation_module.sh" "get_vm_backup_dir" \
            "VM '$vm_name' has policy 'never', no backup dir"
        echo ""
        return 1
    fi
    
    local safe_name=$(sanitize_vm_name "$vm_name")
    
    # Accumulate: flat directory under VM (no period)
    if [[ "$policy" == "accumulate" ]]; then
        log_debug "rotation_module.sh" "get_vm_backup_dir" \
            "VM '$vm_name': policy=accumulate path=${BACKUP_PATH}${safe_name}"
        echo "${BACKUP_PATH}${safe_name}"
        return 0
    fi
    
    # Standard: VM/period structure (no trailing slash for consistency with legacy)
    local period_id=$(get_period_id "$policy" "$timestamp")
    log_debug "rotation_module.sh" "get_vm_backup_dir" \
        "VM '$vm_name': policy=$policy period=$period_id path=${BACKUP_PATH}${safe_name}/${period_id}"
    echo "${BACKUP_PATH}${safe_name}/${period_id}"
}

# Sanitize VM name for safe filesystem use
# Args: $1 - VM name
# Returns: Sanitized name (preserves a-z, A-Z, 0-9, dot, underscore, hyphen)
sanitize_vm_name() {
    local vm_name="$1"
    # Use bash parameter expansion - faster than sed for simple substitution
    echo "${vm_name//[^a-zA-Z0-9._-]/_}"
}

#################################################################################
# PERIOD BOUNDARY DETECTION
#################################################################################

# Check if we're at a period boundary (new period started)
# Args: $1 - VM name
#       $2 - stored period ID (from manifest)
#       $3 - current timestamp (optional)
# Returns: 0 if boundary crossed, 1 if same period
is_period_boundary() {
    local vm_name="$1"
    local stored_period="$2"
    local timestamp="${3:-now}"
    
    local policy
    policy=$(get_vm_rotation_policy "$vm_name")
    
    if [[ "$policy" == "accumulate" ]] || [[ "$policy" == "never" ]]; then
        # No boundaries for accumulate/never
        return 1
    fi
    
    local current_period
    current_period=$(get_period_id "$policy" "$timestamp")
    
    if [[ "$current_period" != "$stored_period" ]]; then
        log_info "rotation_module.sh" "is_period_boundary" \
            "Period boundary: $stored_period -> $current_period (VM: $vm_name)"
        return 0
    fi
    
    log_debug "rotation_module.sh" "is_period_boundary" \
        "Same period '$current_period' for VM '$vm_name' - no boundary"
    return 1
}

# Validate time progression (detect clock skew)
# Args: $1 - VM name
#       $2 - current period
#       $3 - stored period
# Returns: 0 if valid, 1 if clock skew detected
validate_time_progression() {
    local vm_name="$1"
    local current_period="$2"
    local stored_period="$3"
    
    # Empty stored period means first backup - always valid
    [[ -z "$stored_period" ]] && return 0
    
    # Lexicographic comparison works for all period formats
    if [[ "$current_period" < "$stored_period" ]]; then
        log_error "rotation_module.sh" "validate_time_progression" \
            "Clock skew detected: current=$current_period < stored=$stored_period"
        return 1
    fi
    
    return 0
}

#################################################################################
# PERIOD DIRECTORY VALIDATION
#################################################################################

# Check if a directory name is a valid period for a policy
# Args: $1 - directory name
#       $2 - policy
# Returns: 0 if valid, 1 if invalid
is_valid_period_dir() {
    local dirname="$1"
    local policy="$2"
    
    case "$policy" in
        daily)
            [[ "$dirname" =~ ^[0-9]{8}$ ]]
            ;;
        weekly)
            [[ "$dirname" =~ ^[0-9]{4}-W[0-9]{2}$ ]]
            ;;
        monthly)
            [[ "$dirname" =~ ^[0-9]{6}$ ]]
            ;;
        *)
            return 1
            ;;
    esac
}

# List all period directories for a VM, sorted newest first
# Args: $1 - VM name
# Returns: Newline-separated list of period IDs
list_vm_periods() {
    local vm_name="$1"
    local safe_name=$(sanitize_vm_name "$vm_name")
    local vm_dir="${BACKUP_PATH}${safe_name}"
    local policy=$(get_vm_rotation_policy "$vm_name")
    
    [[ ! -d "$vm_dir" ]] && return 0
    
    # List directories and filter by policy pattern in one pass
    local dir period_name
    for dir in "${vm_dir}"/*/; do
        [[ -d "$dir" ]] || continue
        period_name=$(basename "$dir")
        is_valid_period_dir "$period_name" "$policy" && echo "$period_name"
    done | sort -r
}

#################################################################################
# CHAIN ID GENERATION
#################################################################################

# Generate a unique chain ID
# Args: $1 - timestamp (optional)
# Returns: Chain ID in format chain-YYYYMMDD-HHMMSS
generate_chain_id() {
    local timestamp="${1:-now}"
    echo "chain-$(date -d "$timestamp" +%Y%m%d-%H%M%S)"
}

# Generate a unique restore point ID
# Args: $1 - VM name
#       $2 - period ID
#       $3 - chain ID
#       $4 - checkpoint number
# Returns: Restore point ID
generate_restore_point_id() {
    local vm_name="$1"
    local period_id="$2"
    local chain_id="$3"
    local checkpoint_num="$4"
    
    echo "${vm_name}:${period_id}:${chain_id}:${checkpoint_num}"
}

#################################################################################
# CHAIN ARCHIVE DIRECTORY
#################################################################################

# Generate chain archive directory name (within period)
# Args: $1 - backup directory
#       $2 - timestamp (optional)
# Returns: Full path to chain archive directory
get_chain_archive_dir() {
    local backup_dir="$1"
    local timestamp="${2:-now}"
    
    local archive_name=".chain-$(date -d "$timestamp" +%Y%m%d-%H%M%S)"
    local archive_path="${backup_dir}${archive_name}"
    
    # Handle sub-second collisions with counter suffix
    local counter=0
    while [[ -d "$archive_path" ]]; do
        ((counter++))
        archive_path="${backup_dir}${archive_name}.${counter}"
    done
    
    echo "$archive_path"
}

#################################################################################
# RETENTION HELPERS
#################################################################################

# Get retention limit for a policy
# Args: $1 - policy
# Returns: Retention limit (number of periods to keep)
get_retention_limit() {
    local policy="$1"
    
    case "$policy" in
        daily)   echo "$RETENTION_DAYS" ;;
        weekly)  echo "$RETENTION_WEEKS" ;;
        monthly) echo "$RETENTION_MONTHS" ;;
        *)       echo "0" ;;
    esac
}

# Calculate age of a period in days
# Args: $1 - period ID
#       $2 - policy
# Returns: Age in days (0 on error)
calculate_period_age() {
    local period="$1"
    local policy="$2"
    local today=$(date -u +%s)
    local period_date
    
    case "$policy" in
        daily)
            # YYYYMMDD format
            period_date=$(date -u -d "${period:0:4}-${period:4:2}-${period:6:2}" +%s 2>/dev/null) || {
                log_debug "rotation_module.sh" "calculate_period_age" "Failed to parse daily period: $period"
                echo "0"; return
            }
            ;;
        weekly)
            # YYYY-Www format - use Thursday of that week (ISO 8601 reference day)
            local year="${period:0:4}"
            local week="${period:6:2}"
            period_date=$(date -u -d "${year}-01-04 +$((week - 1)) weeks" +%s 2>/dev/null) || {
                log_debug "rotation_module.sh" "calculate_period_age" "Failed to parse weekly period: $period"
                echo "0"; return
            }
            ;;
        monthly)
            # YYYYMM format
            period_date=$(date -u -d "${period:0:4}-${period:4:2}-01" +%s 2>/dev/null) || {
                log_debug "rotation_module.sh" "calculate_period_age" "Failed to parse monthly period: $period"
                echo "0"; return
            }
            ;;
        *)
            echo "0"
            return
            ;;
    esac
    
    echo $(( (today - period_date) / 86400 ))
}

#################################################################################
# CONFIG EVENT LOGGING (Stub - implemented in logging_module.sh)
#################################################################################

# Stub for config event logging - will be overridden by logging module
if ! declare -f log_config_event &>/dev/null; then
    log_config_event() {
        # Stub - logging module will override
        :
    }
fi

#################################################################################
# MODULE INITIALIZATION
#################################################################################

log_debug "rotation_module.sh" "init" \
    "Rotation module v${ROTATION_MODULE_VERSION} loaded"
