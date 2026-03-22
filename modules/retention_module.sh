#!/bin/bash

#################################################################################
# Retention Module - Period and Chain Cleanup for VM-First Backup Structure
#
# Handles retention policy enforcement, identifying old periods/chains for
# removal and archiving chains before period rotation.
#
# RETENTION POLICIES:
#   daily    - Keep RETENTION_DAYS periods
#   weekly   - Keep RETENTION_WEEKS periods
#   monthly  - Keep RETENTION_MONTHS periods
#   accumulate - Warn at ACCUMULATE_WARN_DEPTH, archive + force full at ACCUMULATE_HARD_LIMIT
#
# Dependencies:
#   - rotation_module.sh: get_vm_rotation_policy(), get_retention_limit()
#   - logging_module.sh: log_retention_action()
#   - chain_manifest_module.sh: archive_chain_in_manifest()
#
# Usage:
#   source retention_module.sh
#   run_retention_for_vm "vm-name"
#   archive_active_chains "vm-name" "202601"
#################################################################################

# Guard against multiple inclusion
[[ -n "${_RETENTION_MODULE_LOADED:-}" ]] && return 0
readonly _RETENTION_MODULE_LOADED=1

# Module version
readonly RETENTION_MODULE_VERSION="2.1"

#################################################################################
# CONFIGURATION DEFAULTS
#################################################################################

# Orphan retention defaults (can be overridden in config)
: "${RETENTION_ORPHAN_ENABLED:=true}"
: "${RETENTION_ORPHAN_MAX_AGE_DAYS:=90}"
: "${RETENTION_ORPHAN_MIN_AGE_DAYS:=7}"
: "${RETENTION_ORPHAN_DRY_RUN:=false}"

#################################################################################
# PROTECTION CHECKS
#################################################################################

# Check if any chain in a period is protected (purge_eligible=0)
# Args: $1 - vm_name
#       $2 - period_id
# Returns: 0 if protected (should NOT delete), 1 if deletable
_is_period_protected() {
    local vm_name="$1"
    local period_id="$2"
    local db_path="${VMBACKUP_DB:-${BACKUP_PATH}_state/vmbackup.db}"

    # If DB unavailable, allow deletion (backward compatible)
    [[ ! -f "$db_path" ]] && return 1

    local protected_count
    protected_count=$(sqlite3 "$db_path" \
        "SELECT COUNT(*) FROM chain_health
         WHERE vm_name='$(echo "$vm_name" | sed "s/'/''/g")'
         AND period_id='$(echo "$period_id" | sed "s/'/''/g")'
         AND purge_eligible = 0;" 2>/dev/null)

    if [[ "${protected_count:-0}" -gt 0 ]]; then
        log_info "retention_module.sh" "_is_period_protected" \
            "Period $vm_name/$period_id is protected ($protected_count chain(s) with purge_eligible=0)"
        return 0
    fi
    return 1
}

# Check if a period's backup data has been successfully replicated
# Returns: 0 if replicated (or replication not configured), 1 if un-replicated
_is_period_replicated() {
    local vm_name="$1"
    local period_id="$2"
    local db_path="${VMBACKUP_DB:-${BACKUP_PATH}_state/vmbackup.db}"

    # If no DB or no replication configured, consider replicated (don't block)
    [[ ! -f "$db_path" ]] && return 0

    # Check if any replication is configured (presence of successful runs)
    local has_replication
    has_replication=$(sqlite3 "$db_path" \
        "SELECT COUNT(*) FROM replication_runs WHERE status='success';" 2>/dev/null)
    [[ "${has_replication:-0}" -eq 0 ]] && return 0

    # Check if this VM+period has been replicated at least once
    local replicated
    replicated=$(sqlite3 "$db_path" \
        "SELECT COUNT(*) FROM replication_vms rv
         JOIN replication_runs rr ON rv.run_id = rr.id
         WHERE rv.vm_name = '$(echo "$vm_name" | sed "s/'/''/g")'
         AND rr.status = 'success'
         AND rr.session_id IN (
             SELECT session_id FROM vm_backups
             WHERE vm_name = '$(echo "$vm_name" | sed "s/'/''/g")'
             AND backup_path LIKE '%/${period_id}%'
             AND status = 'success'
         );" 2>/dev/null)

    if [[ "${replicated:-0}" -gt 0 ]]; then
        return 0
    fi
    return 1
}

#################################################################################
# PERIOD DISCOVERY
#################################################################################

# Get all period directories for a VM (sorted oldest first)
# Args: $1 - vm_name
# Returns: Newline-separated list of period IDs
get_vm_periods() {
    local vm_name="$1"
    local safe_name=$(sanitize_vm_name "$vm_name")
    local vm_dir="${BACKUP_PATH}${safe_name}"
    local policy=$(get_vm_rotation_policy "$vm_name")
    
    [[ ! -d "$vm_dir" ]] && return 0
    
    case "$policy" in
        daily)
            # YYYYMMDD directories
            find "$vm_dir" -maxdepth 1 -type d -regextype posix-extended \
                -regex '.*/[0-9]{8}$' 2>/dev/null | xargs -r -n1 basename | sort
            ;;
        weekly)
            # YYYY-Www directories
            find "$vm_dir" -maxdepth 1 -type d -regextype posix-extended \
                -regex '.*/[0-9]{4}-W[0-9]{2}$' 2>/dev/null | xargs -r -n1 basename | sort
            ;;
        monthly)
            # YYYYMM directories
            find "$vm_dir" -maxdepth 1 -type d -regextype posix-extended \
                -regex '.*/[0-9]{6}$' 2>/dev/null | xargs -r -n1 basename | sort
            ;;
        accumulate)
            # Flat structure - return "flat" if backups exist
            compgen -G "$vm_dir"/*.qcow2 >/dev/null 2>&1 && echo "flat"
            ;;
    esac
}

# Count periods for a VM
# Args: $1 - vm_name
# Returns: period count
count_vm_periods() {
    get_vm_periods "$1" | grep -c . || true
}

#################################################################################
# CROSS-POLICY PERIOD DISCOVERY
#################################################################################

# Get ALL period directories for a VM regardless of current policy
# Matches daily (YYYYMMDD), weekly (YYYY-Www), and monthly (YYYYMM) formats
# Args: $1 - vm_name
# Returns: Newline-separated list of ALL period IDs (sorted oldest first)
get_all_vm_periods() {
    local vm_name="$1"
    local safe_name=$(sanitize_vm_name "$vm_name")
    local vm_dir="${BACKUP_PATH}${safe_name}"
    
    [[ ! -d "$vm_dir" ]] && return 0
    
    # Match all known period formats with a single find command
    find "$vm_dir" -maxdepth 1 -type d \( \
        -regextype posix-extended \
        -regex '.*/[0-9]{8}$' -o \
        -regex '.*/[0-9]{4}-W[0-9]{2}$' -o \
        -regex '.*/[0-9]{6}$' \
    \) 2>/dev/null | xargs -r -n1 basename | sort
}

# Detect the rotation policy that created a period based on its format
# Args: $1 - period_id (e.g., "20260215", "2026-W07", "202602")
# Returns: policy name (daily|weekly|monthly|unknown)
detect_period_policy() {
    local period_id="$1"
    
    if [[ "$period_id" =~ ^[0-9]{4}-W[0-9]{2}$ ]]; then
        echo "weekly"
    elif [[ "$period_id" =~ ^[0-9]{8}$ ]]; then
        echo "daily"
    elif [[ "$period_id" =~ ^[0-9]{6}$ ]]; then
        echo "monthly"
    else
        echo "unknown"
    fi
}

# Get orphaned periods - directories from a different policy than current
# Args: $1 - vm_name
# Returns: Newline-separated list of orphaned period IDs
get_orphaned_periods() {
    local vm_name="$1"
    local all_periods=$(get_all_vm_periods "$vm_name")
    local current_periods=$(get_vm_periods "$vm_name")
    
    [[ -z "$all_periods" ]] && return 0
    
    # Return periods in all but not in current (orphans)
    # Use comm to find entries only in all_periods
    comm -23 <(echo "$all_periods" | sort) <(echo "$current_periods" | sort) 2>/dev/null
}

# Calculate age of any period format in days (from period start date)
# NOTE: For orphan retention, use calculate_orphan_age() instead - this
#       calculates from period START which is wrong for retention decisions.
# Args: $1 - period_id (any format)
# Returns: age in days (0 if cannot determine)
calculate_any_period_age() {
    local period_id="$1"
    local policy=$(detect_period_policy "$period_id")
    
    [[ "$policy" == "unknown" ]] && { echo "0"; return 1; }
    
    # Use existing calculate_period_age if available
    if declare -f calculate_period_age >/dev/null 2>&1; then
        calculate_period_age "$period_id" "$policy"
        return $?
    fi
    
    # Fallback implementation (DST-safe: use -u for consistent UTC epoch)
    local period_date now_epoch period_epoch
    now_epoch=$(date -u +%s)
    
    case "$policy" in
        daily)
            # YYYYMMDD -> parse directly
            period_date="${period_id:0:4}-${period_id:4:2}-${period_id:6:2}"
            ;;
        weekly)
            # YYYY-Www -> convert to first day of week
            local year="${period_id:0:4}"
            local week="${period_id:6:2}"
            # Get date of Monday in that ISO week
            period_date=$(date -u -d "$year-01-04 +$((week - 1)) weeks - $(date -u -d "$year-01-04" +%u) days + 1 day" +%Y-%m-%d 2>/dev/null)
            [[ -z "$period_date" ]] && { echo "0"; return 1; }
            ;;
        monthly)
            # YYYYMM -> first day of month
            period_date="${period_id:0:4}-${period_id:4:2}-01"
            ;;
    esac
    
    period_epoch=$(date -u -d "$period_date" +%s 2>/dev/null) || { echo "0"; return 1; }
    echo $(( (now_epoch - period_epoch) / 86400 ))
}

# Calculate orphan age based on last SUCCESSFUL backup (from database)
# This is the correct age calculation for orphan retention decisions.
# Args: $1 - vm_name
#       $2 - period_id
# Returns: age in days since last successful backup (9999 if no successful backup found)
calculate_orphan_age() {
    local vm_name="$1"
    local period_id="$2"
    local db_path="${VMBACKUP_DB:-${BACKUP_PATH}_state/vmbackup.db}"
    
    # Check DB exists
    [[ ! -f "$db_path" ]] && {
        log_warn "retention_module.sh" "calculate_orphan_age" \
            "Database not found: $db_path - falling back to period age"
        calculate_any_period_age "$period_id"
        return $?
    }
    
    # Query for last successful backup to this period
    # NOTE: Period IDs use LOCAL date; DB timestamps (created_at) use UTC.
    # Match on backup_path (contains actual directory name), not on created_at.
    # See DATETIME_BUGS.md H2.
    local last_success
    last_success=$(sqlite3 "$db_path" \
        "SELECT MAX(created_at) FROM vm_backups 
         WHERE vm_name='$vm_name' 
         AND backup_path LIKE '%/${period_id}%'
         AND status='success';" 2>/dev/null)
    
    # No successful backup found - return very high age (eligible for deletion)
    if [[ -z "$last_success" || "$last_success" == "" ]]; then
        log_debug "retention_module.sh" "calculate_orphan_age" \
            "No successful backup found for $vm_name/$period_id - marking as very old"
        echo "9999"
        return 0
    fi
    
    # Calculate days since last successful backup
    # NOTE: DB timestamps (created_at) are UTC bare. Must append " UTC" so
    # date -d does not misinterpret as local time (C1 fix — DATETIME_BUGS.md).
    local last_epoch now_epoch
    last_epoch=$(date -d "${last_success} UTC" +%s 2>/dev/null)
    if [[ -z "$last_epoch" ]]; then
        log_warn "retention_module.sh" "calculate_orphan_age" \
            "Could not parse date '$last_success' - falling back to period age"
        calculate_any_period_age "$period_id"
        return $?
    fi
    
    now_epoch=$(date +%s)
    local age_days=$(( (now_epoch - last_epoch) / 86400 ))
    
    log_debug "retention_module.sh" "calculate_orphan_age" \
        "$vm_name/$period_id: last successful backup=$last_success, age=${age_days}d"
    
    echo "$age_days"
}

#################################################################################
# RETENTION POLICY ENFORCEMENT
#################################################################################

# Run retention cleanup for a single VM
# Args: $1 - vm_name
#       $2 - dry_run (optional, true for simulation)
# Returns: 0 on success, 1 on error
run_retention_for_vm() {
    local vm_name="$1"
    local dry_run="${2:-false}"
    local safe_name=$(sanitize_vm_name "$vm_name")
    local vm_dir="${BACKUP_PATH}${safe_name}"
    local policy=$(get_vm_rotation_policy "$vm_name")
    
    # Skip excluded VMs
    [[ "$policy" == "never" ]] && return 0
    
    # Accumulate policy - limit check handled pre-backup (vmbackup_integration.sh)
    # Post-backup just logs current state, no action needed
    [[ "$policy" == "accumulate" ]] && {
        local backup_count=$(find "$vm_dir" -maxdepth 1 -type f -name "*.data" 2>/dev/null | wc -l)
        local hard_limit=${ACCUMULATE_HARD_LIMIT:-365}
        log_debug "retention_module.sh" "run_retention_for_vm" \
            "Accumulate policy: $vm_name chain depth=$backup_count (limit=$hard_limit)"
        return 0
    }
    
    local retention_limit=$(get_retention_limit "$policy")
    local periods=$(get_vm_periods "$vm_name")
    local period_count
    period_count=$(echo "$periods" | grep -c . || true)
    
    log_debug "retention_module.sh" "run_retention_for_vm" \
        "$vm_name: policy=$policy count=$period_count limit=$retention_limit"
    
    # Within limits - nothing to do
    [[ "$period_count" -le "$retention_limit" ]] && return 0
    
    # Calculate and remove excess periods
    local to_remove=$((period_count - retention_limit))
    log_info "retention_module.sh" "run_retention_for_vm" \
        "Retention cleanup: $vm_name - removing $to_remove old period(s)"
    
    local old_periods=$(echo "$periods" | head -n "$to_remove")
    local failed=0 period_id
    
    for period_id in $old_periods; do
        _remove_period "$vm_name" "$period_id" "$dry_run" || ((failed++))
    done
    
    [[ "$failed" -gt 0 ]] && {
        log_error "retention_module.sh" "run_retention_for_vm" \
            "Failed to remove $failed period(s) for $vm_name"
        return 1
    }
    return 0
}

#################################################################################
# ORPHANED POLICY RETENTION (Tier 2)
#
# Age-based cleanup for period directories from previous rotation policies.
# When a VM's policy changes (e.g., weekly → monthly), old format directories
# become orphaned. This function handles their cleanup based on age.
#################################################################################

# Run orphan retention cleanup for a single VM
# Args: $1 - vm_name
#       $2 - dry_run (optional, overrides RETENTION_ORPHAN_DRY_RUN)
# Returns: 0 on success
run_orphan_retention_for_vm() {
    local vm_name="$1"
    local dry_run="${2:-${RETENTION_ORPHAN_DRY_RUN:-false}}"
    
    # Check if orphan retention is enabled
    [[ "${RETENTION_ORPHAN_ENABLED:-true}" != "true" ]] && {
        log_debug "retention_module.sh" "run_orphan_retention_for_vm" \
            "Orphan retention disabled - skipping $vm_name"
        return 0
    }
    
    local policy=$(get_vm_rotation_policy "$vm_name")
    
    # Skip for never/accumulate - no period-based cleanup
    [[ "$policy" == "never" || "$policy" == "accumulate" ]] && return 0
    
    # Get configuration
    local max_age="${RETENTION_ORPHAN_MAX_AGE_DAYS:-90}"
    local min_age="${RETENTION_ORPHAN_MIN_AGE_DAYS:-7}"
    
    # Validate configuration
    if [[ "$min_age" -ge "$max_age" ]]; then
        log_warn "retention_module.sh" "run_orphan_retention_for_vm" \
            "Config error: RETENTION_ORPHAN_MIN_AGE_DAYS ($min_age) >= MAX_AGE_DAYS ($max_age)"
        return 1
    fi
    
    # Find orphaned periods
    local orphans=$(get_orphaned_periods "$vm_name")
    [[ -z "$orphans" ]] && return 0
    
    local orphan_count
    orphan_count=$(echo "$orphans" | grep -c . || true)
    log_debug "retention_module.sh" "run_orphan_retention_for_vm" \
        "$vm_name: found $orphan_count orphaned period(s) from previous policies"
    
    local deleted=0 kept=0 protected=0 failed=0
    local period_id age original_policy
    
    for period_id in $orphans; do
        # Use DB-based age calculation (days since last successful backup)
        age=$(calculate_orphan_age "$vm_name" "$period_id")
        original_policy=$(detect_period_policy "$period_id")
        
        if [[ "$age" -ge "$max_age" ]]; then
            # Past max age - delete
            log_info "retention_module.sh" "run_orphan_retention_for_vm" \
                "Orphan cleanup: $vm_name/$period_id (policy=$original_policy, age=${age}d >= max=${max_age}d)"
            
            if _remove_orphan_period "$vm_name" "$period_id" "$original_policy" "$dry_run"; then
                ((deleted++))
            else
                ((failed++))
            fi
            
        elif [[ "$age" -ge "$min_age" ]]; then
            # Between min and max - aging but not yet deletable
            log_debug "retention_module.sh" "run_orphan_retention_for_vm" \
                "Orphan aging: $vm_name/$period_id (policy=$original_policy, age=${age}d, range=${min_age}-${max_age}d)"
            ((kept++))
            
        else
            # Under min age - protected from cleanup
            log_debug "retention_module.sh" "run_orphan_retention_for_vm" \
                "Orphan protected: $vm_name/$period_id (policy=$original_policy, age=${age}d < min=${min_age}d)"
            ((protected++))
        fi
    done
    
    # Log summary if any action taken
    if [[ "$deleted" -gt 0 || "$kept" -gt 0 || "$protected" -gt 0 ]]; then
        log_info "retention_module.sh" "run_orphan_retention_for_vm" \
            "$vm_name orphan retention: deleted=$deleted aging=$kept protected=$protected failed=$failed"
    fi
    
    [[ "$failed" -gt 0 ]] && return 1
    return 0
}

# Remove an orphaned period directory
# Args: $1 - vm_name
#       $2 - period_id
#       $3 - original_policy (for logging/DB)
#       $4 - dry_run
# Returns: 0 on success, 1 on error
_remove_orphan_period() {
    local vm_name="$1"
    local period_id="$2"
    local original_policy="$3"
    local dry_run="$4"
    local safe_name=$(sanitize_vm_name "$vm_name")
    local period_dir="${BACKUP_PATH}${safe_name}/${period_id}"
    
    # Skip if not exists
    [[ ! -d "$period_dir" ]] && return 0
    
    local age_days=$(calculate_any_period_age "$period_id")
    local freed_bytes=$(du -sb "$period_dir" 2>/dev/null | cut -f1 || echo 0)
    local max_age="${RETENTION_ORPHAN_MAX_AGE_DAYS:-90}"
    
    # Dry run mode
    if [[ "$dry_run" == "true" ]]; then
        log_info "retention_module.sh" "_remove_orphan_period" \
            "[DRY RUN] Would remove orphan: $period_dir (policy=$original_policy, ${freed_bytes} bytes, ${age_days} days old)"
        
        # Log to retention action if available
        if declare -f log_retention_action >/dev/null 2>&1; then
            log_retention_action "delete" "$vm_name" "orphan_period" "$period_dir" "$period_id" \
                "$original_policy" "$max_age" "1" "$age_days" "$freed_bytes" \
                "" "_remove_orphan_period" "dry_run" "orphan_retention"
        fi
        return 0
    fi
    
    # Protection check: refuse to delete if any chain in this period is protected
    if _is_period_protected "$vm_name" "$period_id"; then
        log_info "retention_module.sh" "_remove_orphan_period" \
            "Skipping protected orphan period: $vm_name/$period_id (purge_eligible=0)"
        if declare -f log_retention_action >/dev/null 2>&1; then
            log_retention_action "skip" "$vm_name" "orphan_period" "$period_dir" "$period_id" \
                "$original_policy" "$max_age" "1" "$age_days" "0" \
                "protected" "_remove_orphan_period" "true" "orphan_retention"
        fi
        return 0
    fi
    
    # Keep-last guard: refuse to delete the last period for a VM
    local total_periods_orphan
    total_periods_orphan=$(find "${BACKUP_PATH}${safe_name}" -maxdepth 1 -mindepth 1 -type d 2>/dev/null | wc -l)
    if [[ "${total_periods_orphan:-0}" -le 1 ]]; then
        log_warn "retention_module.sh" "_remove_orphan_period" \
            "Refusing to delete last period for $vm_name: $period_id"
        if declare -f log_retention_action >/dev/null 2>&1; then
            log_retention_action "skip" "$vm_name" "orphan_period" "$period_dir" "$period_id" \
                "$original_policy" "$max_age" "1" "$age_days" "0" \
                "last_period" "_remove_orphan_period" "true" "orphan_retention"
        fi
        return 0
    fi
    
    # Replication-awareness check: warn or block if period has not been replicated
    if ! _is_period_replicated "$vm_name" "$period_id"; then
        local repl_action="${RETENTION_REQUIRE_REPLICATION:-warn}"
        if [[ "$repl_action" == "block" ]]; then
            log_warn "retention_module.sh" "_remove_orphan_period" \
                "Blocking deletion of un-replicated orphan period: $vm_name/$period_id"
            if declare -f log_retention_action >/dev/null 2>&1; then
                log_retention_action "skip" "$vm_name" "orphan_period" "$period_dir" "$period_id" \
                    "$original_policy" "$max_age" "1" "$age_days" "0" \
                    "unreplicated" "_remove_orphan_period" "true" "orphan_retention"
            fi
            return 0
        else
            log_warn "retention_module.sh" "_remove_orphan_period" \
                "Deleting un-replicated orphan period: $vm_name/$period_id (RETENTION_REQUIRE_REPLICATION=${repl_action})"
        fi
    fi
    
    # Safety check (reuse existing function)
    if declare -f _is_safe_to_remove >/dev/null 2>&1; then
        if ! _is_safe_to_remove "$period_dir"; then
            log_error "retention_module.sh" "_remove_orphan_period" \
                "Safety check failed for orphan: $period_dir"
            return 1
        fi
    fi
    
    # Mark chains as deleted in SQLite BEFORE removal
    if declare -f sqlite_mark_chain_deleted >/dev/null 2>&1; then
        sqlite_mark_chain_deleted "$vm_name" "$period_id" "." "retention_orphan"
        log_debug "retention_module.sh" "_remove_orphan_period" \
            "Marked orphan chains as deleted: $vm_name/$period_id (policy=$original_policy)"
    fi
    
    # Actually remove
    log_info "retention_module.sh" "_remove_orphan_period" \
        "Removing orphan period: $period_dir (policy=$original_policy)"
    
    if rm -rf "$period_dir"; then
        # Log success
        if declare -f log_retention_action >/dev/null 2>&1; then
            log_retention_action "delete" "$vm_name" "orphan_period" "$period_dir" "$period_id" \
                "$original_policy" "$max_age" "0" "$age_days" "$freed_bytes" \
                "" "_remove_orphan_period" "true" "orphan_retention"
        fi
        
        if declare -f log_file_operation >/dev/null 2>&1; then
            log_file_operation "delete" "$vm_name" "$period_dir" "" \
                "directory" "Orphan retention (was $original_policy)" "_remove_orphan_period" "true" "" "$freed_bytes"
        fi
        
        return 0
    else
        log_error "retention_module.sh" "_remove_orphan_period" \
            "Failed to remove orphan: $period_dir"
        return 1
    fi
}

# NOTE: Accumulate limit checking moved to pre_backup_hook() in vmbackup_integration.sh
# This ensures chain is archived and full backup forced BEFORE backup runs
# See: _check_accumulate_limit_pre_backup()

#################################################################################
# PERIOD REMOVAL
#################################################################################

# Remove a period directory (with safety checks)
# Args: $1 - vm_name
#       $2 - period_id
#       $3 - dry_run
#       $4 - skip_keep_last (true|false, default: false)
#            Set true only for --prune all (operator wants everything gone)
#       $5 - caller (retention|prune, default: retention)
#            Controls replication behaviour and DB status values
# Returns: 0 on success, 1 on error
_remove_period() {
    local vm_name="$1"
    local period_id="$2"
    local dry_run="$3"
    local skip_keep_last="${4:-false}"
    local caller="${5:-retention}"
    local safe_name=$(sanitize_vm_name "$vm_name")
    local period_dir="${BACKUP_PATH}${safe_name}/${period_id}"
    
    # Skip if not exists
    [[ ! -d "$period_dir" ]] && return 0
    
    local policy=$(get_vm_rotation_policy "$vm_name")
    local retention_limit=$(get_retention_limit "$policy")
    local current_count=$(count_vm_periods "$vm_name")
    local age_days=$(calculate_period_age "$period_id" "$policy")
    local freed_bytes=$(du -sb "$period_dir" 2>/dev/null | cut -f1 || echo 0)
    
    # Protection check: refuse to delete if any chain in this period is protected
    if _is_period_protected "$vm_name" "$period_id"; then
        log_info "retention_module.sh" "_remove_period" \
            "Skipping protected period: $vm_name/$period_id (purge_eligible=0)"
        log_retention_action "skip" "$vm_name" "period" "$period_dir" "$period_id" \
            "$policy" "$retention_limit" "$current_count" "$age_days" "0" \
            "protected" "_remove_period" "true" "$caller"
        return 0
    fi
    
    # Keep-last guard: refuse to delete the last period for a VM
    # Can be overridden by skip_keep_last (--prune all)
    if [[ "$skip_keep_last" != "true" ]]; then
        local total_periods
        total_periods=$(find "${BACKUP_PATH}${safe_name}" -maxdepth 1 -mindepth 1 -type d 2>/dev/null | wc -l)
        if [[ "${total_periods:-0}" -le 1 ]]; then
            log_warn "retention_module.sh" "_remove_period" \
                "Refusing to delete last period for $vm_name: $period_id"
            log_retention_action "skip" "$vm_name" "period" "$period_dir" "$period_id" \
                "$policy" "$retention_limit" "$current_count" "$age_days" "0" \
                "last_period" "_remove_period" "true" "$caller"
            return 0
        fi
    fi
    
    # Replication-awareness check
    # Prune: always warn-only (operator-initiated, never blocks)
    # Retention: respect RETENTION_REQUIRE_REPLICATION setting
    if ! _is_period_replicated "$vm_name" "$period_id"; then
        if [[ "$caller" == "prune" ]]; then
            log_warn "retention_module.sh" "_remove_period" \
                "Pruning un-replicated period: $vm_name/$period_id (operator-initiated)"
        else
            local repl_action="${RETENTION_REQUIRE_REPLICATION:-warn}"
            if [[ "$repl_action" == "block" ]]; then
                log_warn "retention_module.sh" "_remove_period" \
                    "Blocking deletion of un-replicated period: $vm_name/$period_id"
                log_retention_action "skip" "$vm_name" "period" "$period_dir" "$period_id" \
                    "$policy" "$retention_limit" "$current_count" "$age_days" "0" \
                    "unreplicated" "_remove_period" "true" "$caller"
                return 0
            else
                log_warn "retention_module.sh" "_remove_period" \
                    "Deleting un-replicated period: $vm_name/$period_id (RETENTION_REQUIRE_REPLICATION=${repl_action})"
            fi
        fi
    fi
    
    # Safety check
    if ! _is_safe_to_remove "$period_dir"; then
        log_error "retention_module.sh" "_remove_period" "Safety check failed: $period_dir"
        log_retention_action "error" "$vm_name" "period" "$period_dir" "$period_id" \
            "$policy" "$retention_limit" "$current_count" "$age_days" "0" \
            "safety_check_failed" "_remove_period" "false" "$caller"
        return 1
    fi
    
    # Dry run mode (after all checks — report reflects what would actually happen)
    if [[ "$dry_run" == "true" ]]; then
        log_info "retention_module.sh" "_remove_period" \
            "[DRY RUN] Would remove: $period_dir (${freed_bytes} bytes, ${age_days} days old)"
        log_retention_action "delete" "$vm_name" "period" "$period_dir" "$period_id" \
            "$policy" "$retention_limit" "$current_count" "$age_days" "$freed_bytes" \
            "" "_remove_period" "dry_run" "$caller"
        return 0
    fi
    
    # Determine target status based on caller
    local target_status="deleted"
    [[ "$caller" == "prune" ]] && target_status="purged"
    
    # G4: Mark active chain in chain_health BEFORE removal
    if declare -f sqlite_mark_chain_deleted >/dev/null 2>&1; then
        sqlite_mark_chain_deleted "$vm_name" "$period_id" "." "$caller" "$target_status"
        log_debug "retention_module.sh" "_remove_period" \
            "Marked chain as $target_status in chain_health: $vm_name/$period_id"
    fi
    
    # Log chain event for the active chain (Gap 1 fix)
    # Event name matches target_status: chain_deleted (retention) or chain_purged (prune)
    # Use active-only bytes (period total minus archives) to avoid inflating the event
    local active_chain_bytes="$freed_bytes"
    local _archives_dir="${period_dir}/.archives"
    if [[ -d "$_archives_dir" ]]; then
        local _archive_bytes
        _archive_bytes=$(du -sb "$_archives_dir" 2>/dev/null | cut -f1 || echo 0)
        active_chain_bytes=$(( freed_bytes - _archive_bytes ))
        (( active_chain_bytes < 0 )) && active_chain_bytes=0
    fi
    if declare -f sqlite_log_chain_event >/dev/null 2>&1; then
        sqlite_log_chain_event "chain_${target_status}" "$vm_name" "" "$period_id" \
            "$period_dir" "." "" "$active_chain_bytes" "$caller"
    fi
    
    # Log chain events for each archived chain (Gap 2 fix)
    if declare -f sqlite_log_chain_event >/dev/null 2>&1; then
        local archives_dir="${period_dir}/.archives"
        if [[ -d "$archives_dir" ]]; then
            local chain_dir
            for chain_dir in "$archives_dir"/chain-*; do
                [[ -d "$chain_dir" ]] || continue
                local chain_name
                chain_name=$(basename "$chain_dir")
                local chain_bytes
                chain_bytes=$(du -sb "$chain_dir" 2>/dev/null | cut -f1 || echo 0)
                sqlite_log_chain_event "chain_${target_status}" "$vm_name" "$chain_name" "$period_id" \
                    "$chain_dir" ".archives/$chain_name" "" "$chain_bytes" "$caller"
            done
        fi
    fi
    
    # Actually remove
    log_info "retention_module.sh" "_remove_period" "Removing period: $period_dir"
    
    rm -rf "$period_dir" || {
        log_error "retention_module.sh" "_remove_period" "Failed to remove: $period_dir"
        log_retention_action "error" "$vm_name" "period" "$period_dir" "$period_id" \
            "$policy" "$retention_limit" "$current_count" "$age_days" "0" \
            "rm_failed" "_remove_period" "false" "$caller"
        return 1
    }
    
    log_retention_action "delete" "$vm_name" "period" "$period_dir" "$period_id" \
        "$policy" "$retention_limit" "$current_count" "$age_days" "$freed_bytes" \
        "" "_remove_period" "true" "$caller"
    
    log_file_operation "delete" "$vm_name" "$period_dir" "" \
        "directory" "${caller^} cleanup" "_remove_period" "true" "" "$freed_bytes"
    
    # Log period_deleted lifecycle event (Gap 3 fix)
    if declare -f log_period_lifecycle >/dev/null 2>&1; then
        log_period_lifecycle "period_deleted" "$vm_name" "$period_id" "$policy" \
            "$period_dir" "" "" "0" "0" "$freed_bytes" "" "" "0"
    fi
    
    return 0
}

#################################################################################
# ARCHIVE CHAIN REMOVAL (for --prune)
#################################################################################

# Remove a single archived chain directory from .archives/
# Args: $1 - vm_name
#       $2 - period_id
#       $3 - chain_name (e.g. chain-2026-03-09)
#       $4 - dry_run (true|false)
#       $5 - caller (prune|retention, default: prune)
# Returns: 0 on success, 1 on error
_remove_archive_chain() {
    local vm_name="$1"
    local period_id="$2"
    local chain_name="$3"
    local dry_run="${4:-false}"
    local caller="${5:-prune}"
    local safe_name=$(sanitize_vm_name "$vm_name")
    local chain_dir="${BACKUP_PATH}${safe_name}/${period_id}/.archives/${chain_name}"
    
    if [[ ! -d "$chain_dir" ]]; then
        log_error "retention_module.sh" "_remove_archive_chain" \
            "Archive chain not found: $chain_dir"
        return 1
    fi
    
    local freed_bytes
    freed_bytes=$(du -sb "$chain_dir" 2>/dev/null | cut -f1 || echo 0)
    local policy=$(get_vm_rotation_policy "$vm_name")
    
    if [[ "$dry_run" == "true" ]]; then
        log_info "retention_module.sh" "_remove_archive_chain" \
            "[DRY RUN] Would remove archive chain: $chain_dir (${freed_bytes} bytes)"
        echo "$freed_bytes"
        return 0
    fi
    
    # Safety check
    if ! _is_safe_to_remove "$chain_dir"; then
        log_error "retention_module.sh" "_remove_archive_chain" \
            "Safety check failed: $chain_dir"
        return 1
    fi
    
    # Log chain event BEFORE removal — event name derived from caller
    local target_status="purged"
    [[ "$caller" == "retention" ]] && target_status="deleted"
    if declare -f sqlite_log_chain_event >/dev/null 2>&1; then
        sqlite_log_chain_event "chain_${target_status}" "$vm_name" "$chain_name" "$period_id" \
            "$chain_dir" ".archives/$chain_name" "" "$freed_bytes" "$caller"
    fi
    
    log_info "retention_module.sh" "_remove_archive_chain" \
        "Removing archive chain: $chain_dir"
    
    rm -rf "$chain_dir" || {
        log_error "retention_module.sh" "_remove_archive_chain" \
            "Failed to remove: $chain_dir"
        log_retention_action "error" "$vm_name" "archive_chain" "$chain_dir" "$period_id" \
            "$policy" "0" "0" "0" "0" "rm_failed" "_remove_archive_chain" "false" "$caller"
        return 1
    }
    
    log_retention_action "delete" "$vm_name" "archive_chain" "$chain_dir" "$period_id" \
        "$policy" "0" "0" "0" "$freed_bytes" "" "_remove_archive_chain" "true" "$caller"
    
    log_file_operation "delete" "$vm_name" "$chain_dir" "" \
        "directory" "${caller^} archive chain removal" "_remove_archive_chain" "true" "" "$freed_bytes"
    
    echo "$freed_bytes"
    return 0
}

# Remove all archives within a period (keep active chain)
# Args: $1 - vm_name
#       $2 - period_id
#       $3 - dry_run (true|false)
#       $4 - caller (prune|retention, default: prune)
# Returns: 0 on success, 1 on error (partial removal still returns 1)
_remove_archives_in_period() {
    local vm_name="$1"
    local period_id="$2"
    local dry_run="${3:-false}"
    local caller="${4:-prune}"
    local safe_name=$(sanitize_vm_name "$vm_name")
    local archives_dir="${BACKUP_PATH}${safe_name}/${period_id}/.archives"
    
    if [[ ! -d "$archives_dir" ]]; then
        log_debug "retention_module.sh" "_remove_archives_in_period" \
            "No archives directory: $archives_dir"
        return 0
    fi
    
    local total_freed=0
    local chain_count=0
    local fail_count=0
    local chain_dir
    
    for chain_dir in "$archives_dir"/chain-*; do
        [[ -d "$chain_dir" ]] || continue
        local chain_name
        chain_name=$(basename "$chain_dir")
        local result rc
        result=$(_remove_archive_chain "$vm_name" "$period_id" "$chain_name" "$dry_run" "$caller")
        rc=$?
        if [[ $rc -eq 0 ]]; then
            total_freed=$(( total_freed + ${result:-0} ))
            (( chain_count++ ))
        else
            (( fail_count++ ))
        fi
    done
    
    if [[ "$dry_run" != "true" && $chain_count -gt 0 ]]; then
        # Remove the empty .archives directory if all chains removed
        if [[ $fail_count -eq 0 ]]; then
            rmdir "$archives_dir" 2>/dev/null
        fi
        # Rebuild manifest once after all chains removed (not per-chain)
        if declare -f rebuild_chain_manifest >/dev/null 2>&1; then
            rebuild_chain_manifest "$vm_name"
        fi
    fi
    
    log_info "retention_module.sh" "_remove_archives_in_period" \
        "Archives in $vm_name/$period_id: removed=$chain_count failed=$fail_count freed=${total_freed} bytes"
    
    echo "$total_freed"
    [[ $fail_count -gt 0 ]] && return 1
    return 0
}

# Remove all data for a VM (all periods, all archives, VM directory)
# This is the nuclear option — zero backup data remains after this.
# Args: $1 - vm_name
#       $2 - dry_run (true|false)
#       $3 - caller (prune, default: prune)
# Returns: 0 on success, 1 on error (partial removal)
_remove_vm_all() {
    local vm_name="$1"
    local dry_run="${2:-false}"
    local caller="${3:-prune}"
    local safe_name=$(sanitize_vm_name "$vm_name")
    local vm_dir="${BACKUP_PATH}${safe_name}"
    
    if [[ ! -d "$vm_dir" ]]; then
        log_error "retention_module.sh" "_remove_vm_all" \
            "VM directory not found: $vm_dir"
        return 1
    fi
    
    local total_freed=0
    local period_count=0
    local fail_count=0
    
    # Iterate all period directories and remove each via _remove_period
    local period_dir
    for period_dir in "$vm_dir"/*/; do
        [[ -d "$period_dir" ]] || continue
        local period_id
        period_id=$(basename "$period_dir")
        
        # Skip non-period dirs (e.g. _state, .hidden)
        [[ "$period_id" == _* || "$period_id" == .* ]] && continue
        
        local period_bytes
        period_bytes=$(du -sb "$period_dir" 2>/dev/null | cut -f1 || echo 0)
        
        if _remove_period "$vm_name" "$period_id" "$dry_run" "true" "$caller"; then
            # Verify deletion actually happened (protection may skip with rc=0)
            if [[ ! -d "$period_dir" ]] || [[ "$dry_run" == "true" ]]; then
                total_freed=$(( total_freed + ${period_bytes:-0} ))
            fi
            (( period_count++ ))
        else
            (( fail_count++ ))
        fi
    done
    
    # Remove the VM directory itself (should be empty now)
    if [[ "$dry_run" != "true" && $fail_count -eq 0 ]]; then
        if [[ -d "$vm_dir" ]]; then
            # Only remove if empty (safety)
            local remaining
            remaining=$(find "$vm_dir" -mindepth 1 -maxdepth 1 -not -name 'chain-manifest.json' 2>/dev/null | wc -l)
            if [[ "$remaining" -eq 0 ]]; then
                rm -rf "$vm_dir"
                log_info "retention_module.sh" "_remove_vm_all" \
                    "Removed VM directory: $vm_dir"
            else
                log_warn "retention_module.sh" "_remove_vm_all" \
                    "VM directory not empty after period removal ($remaining items remain): $vm_dir"
            fi
        fi
    fi
    
    log_info "retention_module.sh" "_remove_vm_all" \
        "VM $vm_name: periods_removed=$period_count failed=$fail_count freed=${total_freed} bytes"
    
    echo "$total_freed"
    [[ $fail_count -gt 0 ]] && return 1
    return 0
}

# Safety check before removal
# Args: $1 - path to check
# Returns: 0 if safe, 1 if not safe
_is_safe_to_remove() {
    local path="$1"
    
    # Normalize paths - remove double slashes and trailing slashes for comparison
    local norm_path="${path//\/\//\/}"
    norm_path="${norm_path%/}"
    local norm_backup="${BACKUP_PATH//\/\//\/}"
    norm_backup="${norm_backup%/}"
    
    # Must be under BACKUP_PATH
    [[ "$norm_path" != "${norm_backup}"/* ]] && return 1
    
    # Must not be BACKUP_PATH itself
    [[ "$norm_path" == "$norm_backup" ]] && return 1
    
    # Must be a directory
    [[ ! -d "$path" ]] && return 1
    
    # Path depth check (at least VM/period)
    local rel_path="${norm_path#${norm_backup}/}"
    local depth
    depth=$(echo "$rel_path" | tr '/' '\n' | wc -l)
    [[ "$depth" -lt 2 ]] && return 1
    
    return 0
}

#################################################################################
# CHAIN ARCHIVING
#################################################################################

# Archive active chains before period rotation
# Called when transitioning to a new period
# Args: $1 - vm_name
#       $2 - old_period_id
#       $3 - archive_reason (period_boundary|manual|error_recovery)
# Returns: 0 on success, 1 on error
archive_active_chains() {
    local vm_name="$1"
    local old_period_id="$2"
    local archive_reason="${3:-period_boundary}"
    local safe_name
    safe_name=$(sanitize_vm_name "$vm_name")
    local period_dir="${BACKUP_PATH}${safe_name}/${old_period_id}"
    
    if [[ ! -d "$period_dir" ]]; then
        log_debug "retention_module.sh" "archive_active_chains" \
            "Period directory not found: $period_dir"
        return 0
    fi
    
    # Get active chain from manifest
    local active_chain
    active_chain=$(get_active_chain "$vm_name")
    
    if [[ -z "$active_chain" ]]; then
        log_debug "retention_module.sh" "archive_active_chains" \
            "No active chain to archive for: $vm_name"
        return 0
    fi
    
    # Check if there are restore points in the old period
    # NOTE: We check for restore points, not the chain's original period_id,
    # because policy changes can cause a chain to span multiple periods
    # (e.g., chain created under monthly policy now running under daily)
    local restore_point_count
    if declare -f count_period_restore_points >/dev/null 2>&1; then
        restore_point_count=$(count_period_restore_points "$vm_name" "$old_period_id")
    else
        # Fallback: check period directory exists with checkpoint files
        restore_point_count=$(find "$period_dir" -maxdepth 1 -name "*.data" -type f 2>/dev/null | wc -l)
    fi
    
    if [[ "$restore_point_count" -eq 0 ]]; then
        log_debug "retention_module.sh" "archive_active_chains" \
            "No restore points in period $old_period_id for chain $active_chain"
        return 0
    fi
    
    log_debug "retention_module.sh" "archive_active_chains" \
        "Found $restore_point_count restore points in period $old_period_id"
    
    log_info "retention_module.sh" "archive_active_chains" \
        "Archiving chain: $active_chain (period: $old_period_id)"
    
    # Create archive directory (within period directory)
    local archive_dir
    archive_dir=$(get_chain_archive_dir "${period_dir}/")
    
    mkdir -p "$archive_dir" || {
        log_error "retention_module.sh" "archive_active_chains" \
            "Failed to create archive directory: $archive_dir"
        return 1
    }
    
    # Find files to archive
    local archive_pattern="backup-${safe_name}*"
    local files_moved=0
    local total_bytes=0
    
    while IFS= read -r -d '' file; do
        local filename
        filename=$(basename "$file")
        local dest="${archive_dir}/${filename}"
        
        local file_size
        file_size=$(stat -c %s "$file" 2>/dev/null || echo 0)
        
        if mv "$file" "$dest"; then
            ((files_moved++)) || true
            total_bytes=$((total_bytes + file_size))
            
            log_file_operation "move" "$vm_name" "$file" "$dest" \
                "backup" "Chain archive" "archive_active_chains" "true"
        else
            log_error "retention_module.sh" "archive_active_chains" \
                "Failed to move: $file -> $dest"
        fi
    done < <(find "$period_dir" -maxdepth 1 -type f -name "$archive_pattern" -print0 2>/dev/null)
    
    # Also move checkpoint XML files
    while IFS= read -r -d '' file; do
        local filename
        filename=$(basename "$file")
        local dest="${archive_dir}/${filename}"
        
        mv "$file" "$dest" 2>/dev/null
        log_file_operation "move" "$vm_name" "$file" "$dest" \
            "checkpoint_xml" "Chain archive" "archive_active_chains" "true"
    done < <(find "$period_dir" -maxdepth 1 -type f -name "*.checkpoint-*.xml" -print0 2>/dev/null)
    
    # Update manifest
    local archive_subdir
    archive_subdir=$(basename "$archive_dir")
    archive_chain_in_manifest "$vm_name" "$active_chain" "$archive_subdir"
    
    # Update SQLite chain_health to mark as archived
    if declare -f sqlite_archive_chain >/dev/null 2>&1; then
        sqlite_archive_chain "$vm_name" "$old_period_id" "$archive_dir"
        log_debug "retention_module.sh" "archive_active_chains" \
            "Marked chain as archived in SQLite: $vm_name/$old_period_id"
    fi
    
    # Log chain lifecycle
    log_chain_lifecycle "chain_archived" "$vm_name" "$active_chain" "$old_period_id" \
        "${BACKUP_PATH}${safe_name}/${old_period_id}" "$archive_subdir" \
        "$files_moved" "$total_bytes" "$archive_reason" "period_rotation" \
        "incremental" "" ""
    
    log_info "retention_module.sh" "archive_active_chains" \
        "Archived $files_moved files (${total_bytes} bytes) to $archive_dir"
    
    return 0
}

#################################################################################
# FILESYSTEM ↔ DATABASE RECONCILIATION
#################################################################################

# Reconcile filesystem state with chain_health DB records for a single VM.
# Fixes two classes of mismatch:
#   1. DB says active/archived, but period directory is missing → mark 'deleted'
#   2. Filesystem has period, but DB has no row → insert minimal 'active' record
# Args: $1 - vm_name
#       $2 - dry_run (optional, default false)
# Returns: 0 on success (or no mismatches), 1 on error
reconcile_vm_chain_state() {
    local vm_name="$1"
    local dry_run="${2:-false}"
    local safe_name=$(sanitize_vm_name "$vm_name")
    local vm_dir="${BACKUP_PATH}${safe_name}"
    local db_path="${VMBACKUP_DB:-${BACKUP_PATH}_state/vmbackup.db}"

    [[ ! -d "$vm_dir" ]] && return 0
    [[ ! -f "$db_path" ]] && return 0

    local fixed_phantom=0 fixed_orphan=0

    # --- Pass 1: DB rows with no matching filesystem directory → phantom records
    local db_periods
    db_periods=$(sqlite3 "$db_path" \
        "SELECT period_id FROM chain_health
         WHERE vm_name='$(echo "$vm_name" | sed "s/'/''/g")'
         AND chain_status IN ('active', 'archived', 'broken', 'marked');" 2>/dev/null)

    local period_id
    while IFS= read -r period_id; do
        [[ -z "$period_id" ]] && continue
        local period_dir="${vm_dir}/${period_id}"
        if [[ ! -d "$period_dir" ]]; then
            if [[ "$dry_run" == "true" ]]; then
                log_info "retention_module.sh" "reconcile_vm_chain_state" \
                    "[DRY RUN] Would mark phantom record as deleted: $vm_name/$period_id"
            else
                log_warn "retention_module.sh" "reconcile_vm_chain_state" \
                    "Phantom record: $vm_name/$period_id (DB says active, filesystem missing) → marking deleted"
                local now=$(date -u '+%Y-%m-%d %H:%M:%S')
                sqlite3 "$db_path" \
                    "UPDATE chain_health SET
                        chain_status='deleted', restorable_count=0,
                        break_reason='reconcile_phantom', deleted_at='$now',
                        marked_by='reconcile', updated_at='$now'
                     WHERE vm_name='$(echo "$vm_name" | sed "s/'/''/g")'
                     AND period_id='$(echo "$period_id" | sed "s/'/''/g")'
                     AND chain_status IN ('active', 'archived', 'broken', 'marked');" 2>/dev/null
            fi
            ((fixed_phantom++))
        fi
    done <<< "$db_periods"

    # --- Pass 2: Filesystem directories with no DB row → create minimal record
    local fs_periods
    fs_periods=$(get_all_vm_periods "$vm_name")

    while IFS= read -r period_id; do
        [[ -z "$period_id" ]] && continue
        local exists
        exists=$(sqlite3 "$db_path" \
            "SELECT COUNT(*) FROM chain_health
             WHERE vm_name='$(echo "$vm_name" | sed "s/'/''/g")'
             AND period_id='$(echo "$period_id" | sed "s/'/''/g")';" 2>/dev/null)
        if [[ "${exists:-0}" -eq 0 ]]; then
            local detected_policy=$(detect_period_policy "$period_id")
            if [[ "$dry_run" == "true" ]]; then
                log_info "retention_module.sh" "reconcile_vm_chain_state" \
                    "[DRY RUN] Would create DB record for untracked period: $vm_name/$period_id (policy=$detected_policy)"
            else
                log_info "retention_module.sh" "reconcile_vm_chain_state" \
                    "Untracked period: $vm_name/$period_id → creating DB record (policy=$detected_policy)"
                local now=$(date -u '+%Y-%m-%d %H:%M:%S')
                sqlite3 "$db_path" \
                    "INSERT OR IGNORE INTO chain_health
                        (vm_name, period_id, chain_location, chain_status, rotation_policy, created_at, updated_at)
                     VALUES
                        ('$(echo "$vm_name" | sed "s/'/''/g")', '$(echo "$period_id" | sed "s/'/''/g")',
                         '.', 'active', '$detected_policy', '$now', '$now');" 2>/dev/null
            fi
            ((fixed_orphan++))
        fi
    done <<< "$fs_periods"

    if [[ $((fixed_phantom + fixed_orphan)) -gt 0 ]]; then
        log_info "retention_module.sh" "reconcile_vm_chain_state" \
            "$vm_name: reconciled $fixed_phantom phantom(s), $fixed_orphan untracked period(s) (dry_run=$dry_run)"
    fi

    return 0
}

# Reconcile all VMs in backup path
# Args: $1 - dry_run (optional, default false)
reconcile_all_chain_state() {
    local dry_run="${1:-false}"
    local vm_dir

    log_info "retention_module.sh" "reconcile_all_chain_state" \
        "Starting filesystem ↔ DB reconciliation (dry_run=$dry_run)"

    while IFS= read -r vm_dir; do
        [[ -z "$vm_dir" ]] && continue
        # Skip non-VM directories (_state, _archive, etc.)
        local dir_name=$(basename "$vm_dir")
        [[ "$dir_name" == _* ]] && continue
        [[ "$dir_name" == .* ]] && continue

        reconcile_vm_chain_state "$dir_name" "$dry_run"
    done < <(find "$BACKUP_PATH" -maxdepth 1 -mindepth 1 -type d 2>/dev/null | sort)

    log_info "retention_module.sh" "reconcile_all_chain_state" \
        "Reconciliation complete"
}

#################################################################################
# MODULE INITIALIZATION
#################################################################################

log_debug "retention_module.sh" "init" \
    "Retention module v${RETENTION_MODULE_VERSION} loaded"
