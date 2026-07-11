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
    local db_path="${VMBACKUP_DB:-${BACKUP_PATH%/}/_state/vmbackup.db}"

    # If DB unavailable, allow deletion (backward compatible)
    [[ ! -f "$db_path" ]] && return 1

    # Phase 4 commit 4a: typed read via lib/sqlite_ro.sh.
    local protected_count
    protected_count=$(sqlite_get_protected_chain_count "$db_path" "$vm_name" "$period_id")

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
    local db_path="${VMBACKUP_DB:-${BACKUP_PATH%/}/_state/vmbackup.db}"

    # F-571 (R2): configured-guard FIRST — if replication is not configured on
    # this instance there is nothing to wait for, so allow deletion (this keeps
    # every disabled-replication install pruning normally). MUST precede the
    # DB-absent return below, else block would stall all retention on DB-less /
    # never-replicated installs.
    [[ "${REPLICATION_ENABLED:-no}" != "yes" && "${CLOUD_REPLICATION_ENABLED:-no}" != "yes" ]] && return 0

    # F-571 (R2): replication IS configured but the catalogue is absent → we
    # cannot prove this period was replicated → fail-CLOSED (keep it). Deliberate
    # inversion of _is_period_protected's return-1-means-allow convention; safe
    # because the configured-guard above already released never-replicated
    # instances before this line is reached.
    [[ ! -f "$db_path" ]] && return 1

    # Phase 4 commit 4a: typed read via lib/sqlite_ro.sh.
    # F-571 (R2): reaching here means replication IS configured; the old global
    # "zero successful replication_runs → treat as replicated" short-circuit is
    # removed — a global zero proves NOTHING is replicated yet, so fall through to
    # the per-VM+period check, which returns 1 (not proven) for a never-shipped
    # period.
    # Check if this VM+period has been replicated at least once
    local replicated
    replicated=$(sqlite_get_vm_period_replication_count "$db_path" "$vm_name" "$period_id")

    if [[ "${replicated:-0}" -gt 0 ]]; then
        return 0
    fi
    return 1
}

#################################################################################
# PERIOD DISCOVERY
#################################################################################
#
# UNI-007 (Phase 3): get_vm_periods, get_all_vm_periods, detect_period_policy,
# and calculate_any_period_age were lifted from this module to lib/period.sh
# in commit 1 of Phase 3 (hard cutover, no shim wrappers — D3). All callers
# in this file were migrated to the new (vm_dir, ...)/( vm_dir, policy)
# signatures. The lib is sourced at startup from both vmbackup.sh and (in
# commit 2) vmrestore.sh.
#
# - get_vm_periods(vm_dir)              — cross-format FS enumeration
# - get_vm_periods_for_policy(vm_dir,p) — policy-driven filter
# - detect_period_policy(period_id)     — pure string parse
# - calculate_any_period_age(period_id) — UTC age in days; lifted from
#                                          rotation_module.sh::calculate_period_age
#                                          (the production path) verbatim. The
#                                          old shim-with-fallback in this file
#                                          was removed; equivalence proven by
#                                          tests/109-phase3-period-vectors.sh.
#

# Count periods for a VM
# Args: $1 - vm_name
# Returns: period count
# UNI-007 (Phase 3): now consumes lib/period.sh::get_vm_periods_for_policy.
# Caller still passes vm_name; we resolve vm_dir + policy here. count_vm_periods
# retains its (vm_name) signature but gains a get_vm_rotation_policy dependency
# (vmbackup-only, defined in modules/rotation_module.sh — same load order, safe).
count_vm_periods() {
    local vm_name="$1"
    local safe_name vm_dir policy
    safe_name=$(vm_fs_name "$vm_name") || return $?
    vm_dir="${BACKUP_PATH}${safe_name}"
    policy=$(get_vm_rotation_policy "$vm_name")
    get_vm_periods_for_policy "$vm_dir" "$policy" | grep -c . || true
}

# Get orphaned periods - directories from a different policy than current
# Args: $1 - vm_name
# Returns: Newline-separated list of orphaned period IDs
# UNI-007 (Phase 3): consumes lib/period.sh helpers. Resolves vm_dir + policy
# locally and passes them in (per Phase 3 spec §2.2 caller responsibilities).
get_orphaned_periods() {
    local vm_name="$1"
    local safe_name vm_dir policy
    safe_name=$(vm_fs_name "$vm_name") || return $?
    vm_dir="${BACKUP_PATH}${safe_name}"
    policy=$(get_vm_rotation_policy "$vm_name")
    local all_periods=$(get_vm_periods "$vm_dir")
    local current_periods=$(get_vm_periods_for_policy "$vm_dir" "$policy")
    
    [[ -z "$all_periods" ]] && return 0
    
    # Return periods in all but not in current (orphans)
    # Use comm to find entries only in all_periods
    comm -23 <(echo "$all_periods" | sort) <(echo "$current_periods" | sort) 2>/dev/null
}

# Calculate orphan age based on last SUCCESSFUL backup (from database)
# This is the correct age calculation for orphan retention decisions.
# Args: $1 - vm_name
#       $2 - period_id
# Returns: age in days since last successful backup (9999 if no successful backup found)
calculate_orphan_age() {
    local vm_name="$1"
    local period_id="$2"
    local db_path="${VMBACKUP_DB:-${BACKUP_PATH%/}/_state/vmbackup.db}"
    
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
    # Phase 4 commit 4a: typed read via lib/sqlite_ro.sh.
    # FF-116 fail-closed: capture the helper's rc. It returns non-zero (rc 2,
    # empty stdout) on a sqlite3 QUERY FAILURE — indistinguishable from the
    # legitimate rc-0 empty result for a genuinely-unbacked period. On failure,
    # fall back to period age (KEEP), exactly as the DB-absent branch above,
    # instead of echoing 9999 (delete-eligible).
    local last_success
    if ! last_success=$(sqlite_get_last_successful_backup_at "$db_path" "$vm_name" "$period_id"); then
        log_warn "retention_module.sh" "calculate_orphan_age" \
            "Query failed for $vm_name/$period_id - falling back to period age (fail-closed)"
        calculate_any_period_age "$period_id"
        return $?
    fi
    
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
    local trigger="${3:-post_backup}"
    local safe_name
    safe_name=$(vm_fs_name "$vm_name") || return $?
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
    # FF-5: retention_limit drives the deletion arithmetic below. A blank,
    # whitespace, negative, or non-numeric value (e.g. an operator writing
    # RETENTION_WEEKS= as an "unlimited" guess) resolves to 0 in the -le test
    # and in $((period_count - retention_limit)), making to_remove ==
    # period_count so `head -n` selects EVERY period and all but the
    # keep-last-guarded newest are deleted silently, rc 0, every run. Fail
    # closed: reject non-numeric limits and skip retention for this VM. "0"
    # and positive integers are valid and behave exactly as before.
    if [[ ! "$retention_limit" =~ ^[0-9]+$ ]]; then
        log_error "retention_module.sh" "run_retention_for_vm" \
            "Invalid retention limit '$retention_limit' for policy '$policy' (check RETENTION_DAYS/RETENTION_WEEKS/RETENTION_MONTHS in vmbackup.conf) - SKIPPING retention for $vm_name (fail-closed, no periods removed)"
        log_retention_action "error" "$vm_name" "vm_retention" \
            "$vm_dir" "" "$policy" "0" "0" \
            "" "0" "invalid_limit" "$trigger" "false" "retention_module"
        return 1
    fi
    # UNI-007 (Phase 3): consumes lib/period.sh::get_vm_periods_for_policy.
    # vm_dir + policy already resolved at function top.
    # Sort chronologically (period IDs sort lexically == chronologically for
    # daily YYYYMMDD, weekly YYYY-Www, monthly YYYYMM) so the subsequent
    # `head -n "$to_remove"` selects the OLDEST periods. The lib function
    # honours an "UNSORTED — caller sorts" contract (R2); without this
    # sort, `find` returns entries in ext4 htree hash order and `head`
    # picks a non-deterministic period — including, in production, the
    # current week — which deletes the active chain and forces a FULL
    # backup every night until count drops back within retention_limit.
    local periods=$(get_vm_periods_for_policy "$vm_dir" "$policy" | sort)
    local period_count
    period_count=$(echo "$periods" | grep -c . || true)
    
    log_debug "retention_module.sh" "run_retention_for_vm" \
        "$vm_name: policy=$policy count=$period_count limit=$retention_limit"
    
    # Within limits - nothing to do
    if [[ "$period_count" -le "$retention_limit" ]]; then
        log_retention_action "evaluate" "$vm_name" "vm_retention" \
            "$vm_dir" "" "$policy" "$retention_limit" "$period_count" \
            "" "0" "within_limit" "$trigger" "true" "retention_module"
        return 0
    fi
    
    # Calculate and remove excess periods
    local to_remove=$((period_count - retention_limit))
    log_info "retention_module.sh" "run_retention_for_vm" \
        "Retention cleanup: $vm_name - removing $to_remove old period(s)"
    
    local old_periods=$(echo "$periods" | head -n "$to_remove")
    local failed=0 period_id
    
    for period_id in $old_periods; do
        _remove_period "$vm_name" "$period_id" "$dry_run" "false" "retention" "$trigger" || ((failed++))
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
    local trigger="${3:-orphan_retention}"
    
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
    if [[ -z "$orphans" ]]; then
        local safe_name
        safe_name=$(vm_fs_name "$vm_name") || return $?
        log_retention_action "evaluate" "$vm_name" "orphan_retention" \
            "${BACKUP_PATH}${safe_name}" "" "$policy" "" "0" \
            "" "0" "no_orphans" "$trigger" "true" "retention_module"
        return 0
    fi
    
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
            
            if _remove_orphan_period "$vm_name" "$period_id" "$original_policy" "$dry_run" "$trigger"; then
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
    local trigger="${5:-orphan_retention}"
    local safe_name
    safe_name=$(vm_fs_name "$vm_name") || return $?
    local period_dir="${BACKUP_PATH}${safe_name}/${period_id}"
    
    # Skip if not exists
    [[ ! -d "$period_dir" ]] && return 0
    
    local age_days=$(calculate_any_period_age "$period_id")
    local freed_bytes=$(du -sb "$period_dir" 2>/dev/null | cut -f1 || echo 0)
    local max_age="${RETENTION_ORPHAN_MAX_AGE_DAYS:-90}"
    
    # Protection check: refuse to delete if any chain in this period is protected
    if _is_period_protected "$vm_name" "$period_id"; then
        log_info "retention_module.sh" "_remove_orphan_period" \
            "Skipping protected orphan period: $vm_name/$period_id (purge_eligible=0)"
        if declare -f log_retention_action >/dev/null 2>&1; then
            log_retention_action "skip" "$vm_name" "orphan_period" "$period_dir" "$period_id" \
                "$original_policy" "$max_age" "1" "$age_days" "0" \
                "protected" "$trigger" "true" "orphan_retention"
        fi
        return 0
    fi
    
    # Keep-last guard: refuse to delete the last period for a VM
    # FF-117 fail-closed: count VALID period dirs via get_vm_periods (see
    # _remove_period), not every subdir, so a stray non-period dir cannot mask the
    # last real period. get_vm_periods failure -> 0 -> refuse (fail-closed).
    local total_periods_orphan
    total_periods_orphan=$(get_vm_periods "${BACKUP_PATH}${safe_name}" | grep -c . || true)
    if [[ "${total_periods_orphan:-0}" -le 1 ]]; then
        log_warn "retention_module.sh" "_remove_orphan_period" \
            "Refusing to delete last period for $vm_name: $period_id"
        if declare -f log_retention_action >/dev/null 2>&1; then
            log_retention_action "skip" "$vm_name" "orphan_period" "$period_dir" "$period_id" \
                "$original_policy" "$max_age" "1" "$age_days" "0" \
                "last_period" "$trigger" "true" "orphan_retention"
        fi
        return 0
    fi
    
    # Replication-awareness check: warn or block if period has not been replicated
    if ! _is_period_replicated "$vm_name" "$period_id"; then
        local repl_action="${RETENTION_REQUIRE_REPLICATION:-block}"
        if [[ "$repl_action" == "block" ]]; then
            log_warn "retention_module.sh" "_remove_orphan_period" \
                "Blocking deletion of un-replicated orphan period: $vm_name/$period_id"
            if declare -f log_retention_action >/dev/null 2>&1; then
                log_retention_action "skip" "$vm_name" "orphan_period" "$period_dir" "$period_id" \
                    "$original_policy" "$max_age" "1" "$age_days" "0" \
                    "unreplicated" "$trigger" "true" "orphan_retention"
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
    
    # FF-185: dry-run mode — AFTER all guards, so the preview reflects what a real
    # run would actually delete (a guarded orphan never reaches here).
    if [[ "$dry_run" == "true" ]]; then  # [DRY-RUN-KEEPER: local-param polarity preserved, see 109-phase7-spec.md §1.3.3]
        log_info "retention_module.sh" "_remove_orphan_period" \
            "[DRY RUN] Would remove orphan: $period_dir (policy=$original_policy, ${freed_bytes} bytes, ${age_days} days old)"
        if declare -f log_retention_action >/dev/null 2>&1; then
            log_retention_action "delete" "$vm_name" "orphan_period" "$period_dir" "$period_id" \
                "$original_policy" "$max_age" "1" "$age_days" "$freed_bytes" \
                "" "$trigger" "dry_run" "orphan_retention"
        fi
        return 0
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
                "" "$trigger" "true" "orphan_retention"
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
    local trigger="${6:-post_backup}"
    local safe_name
    safe_name=$(vm_fs_name "$vm_name") || return $?
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
            "protected" "$trigger" "true" "$caller"
        return 0
    fi
    
    # Keep-last guard: refuse to delete the last period for a VM
    # Can be overridden by skip_keep_last (--prune all)
    if [[ "$skip_keep_last" != "true" ]]; then
        # FF-117 fail-closed: count VALID period dirs (strict format) via
        # get_vm_periods, not every subdir — a stray non-period dir (lost+found,
        # hidden artefact) must not inflate the count past 1 and let the last REAL
        # period be deleted. get_vm_periods failure -> empty -> 0 -> <=1 -> refuse.
        local total_periods
        total_periods=$(get_vm_periods "${BACKUP_PATH}${safe_name}" | grep -c . || true)
        if [[ "${total_periods:-0}" -le 1 ]]; then
            log_warn "retention_module.sh" "_remove_period" \
                "Refusing to delete last period for $vm_name: $period_id"
            log_retention_action "skip" "$vm_name" "period" "$period_dir" "$period_id" \
                "$policy" "$retention_limit" "$current_count" "$age_days" "0" \
                "last_period" "$trigger" "true" "$caller"
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
            local repl_action="${RETENTION_REQUIRE_REPLICATION:-block}"
            if [[ "$repl_action" == "block" ]]; then
                log_warn "retention_module.sh" "_remove_period" \
                    "Blocking deletion of un-replicated period: $vm_name/$period_id"
                log_retention_action "skip" "$vm_name" "period" "$period_dir" "$period_id" \
                    "$policy" "$retention_limit" "$current_count" "$age_days" "0" \
                    "unreplicated" "$trigger" "true" "$caller"
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
            "safety_check_failed" "$trigger" "false" "$caller"
        return 1
    fi
    
    # Dry run mode (after all checks — report reflects what would actually happen)
    if [[ "$dry_run" == "true" ]]; then  # [DRY-RUN-KEEPER: local-param polarity preserved, see 109-phase7-spec.md §1.3.3]
        log_info "retention_module.sh" "_remove_period" \
            "[DRY RUN] Would remove: $period_dir (${freed_bytes} bytes, ${age_days} days old)"
        log_retention_action "delete" "$vm_name" "period" "$period_dir" "$period_id" \
            "$policy" "$retention_limit" "$current_count" "$age_days" "$freed_bytes" \
            "" "$trigger" "dry_run" "$caller"
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
            "rm_failed" "$trigger" "false" "$caller"
        return 1
    }
    
    log_retention_action "delete" "$vm_name" "period" "$period_dir" "$period_id" \
        "$policy" "$retention_limit" "$current_count" "$age_days" "$freed_bytes" \
        "" "$trigger" "true" "$caller"
    
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
# STUB-PERIOD REMOVAL (Bug 1 fix; called from skip/exclude/post-backup paths)
#
# Removes period directories that contain zero top-level *.data files AND no
# .archives/ subdirectory. Bypasses _remove_period because its keep-last,
# replication-awareness, and protected-period guards are all wrong for stubs.
#
# DB writes (audit pattern):
#   1. log_retention_action "remove_stub" -> retention_events
#   2. sqlite_mark_chain_deleted_if_exists -> chain_health (UPDATE-only;
#                                            no phantom rows for pure stubs)
#   3. log_period_lifecycle "period_deleted" -> period_events
#                                              (closes the period_created
#                                               emitted by pre_backup_hook on
#                                               period boundary)
#   4. log_file_operation "delete" -> file_operations (size_override path,
#                                                     source already gone)
#
# Note: chain_events is intentionally NOT emitted -- stubs have no chain
# content. chain_history audit stream stays clean.
#
# Args: $1 - vm_name
#       $2 - trigger (skipped|excluded|orphan_dir|post_backup)
# Returns: 0 always
_remove_empty_period_dirs() {
    local vm_name="$1"
    local trigger="$2"
    local safe_name vm_dir
    safe_name=$(vm_fs_name "$vm_name") || return $?
    vm_dir="${BACKUP_PATH:?BACKUP_PATH must be set}${safe_name}"
    [[ -d "$vm_dir" ]] || return 0

    local periods
    # UNI-007 (Phase 3): consumes lib/period.sh::get_vm_periods. vm_dir already
    # resolved at function top.
    periods=$(get_vm_periods "$vm_dir")
    [[ -z "$periods" ]] && return 0

    local period_id period_dir data_count freed_bytes policy
    while IFS= read -r period_id; do
        [[ -z "$period_id" ]] && continue
        period_dir="${vm_dir}/${period_id}"
        [[ -d "$period_dir" ]] || continue

        # Defence in depth -- refuse to operate outside BACKUP_PATH.
        [[ "$period_dir" == "${BACKUP_PATH}"* ]] || continue
        [[ "$period_id" =~ ^[0-9]{4}-W[0-9]{2}$|^[0-9]{6}$|^[0-9]{8}$ ]] || continue

        # B4/CLEAN-01 self-heal: reap empty .chain-* markers minted by the old
        # broken archiver. They sit INSIDE populated periods (the period still
        # holds .copy.data), so the stub check below never reaches them -- hook
        # here, before it. Content-test: reap a .chain-* dir only if it holds no
        # *.data and no *.xml anywhere beneath; never touch .archives/ or a
        # non-empty marker. Honour dry-run.
        local _b4_marker
        while IFS= read -r _b4_marker; do
            [[ -z "$_b4_marker" ]] && continue
            # FF-118 fail-closed: keep the marker on a find ERROR too — a failed
            # find must never be read as "nothing beneath" and reap a populated
            # marker on storage error. (`if !` tests the rc directly.)
            local _mk_out
            if ! _mk_out=$(find "$_b4_marker" -type f \( -name '*.data' -o -name '*.xml' \) -print -quit 2>/dev/null); then
                continue
            fi
            [[ -n "$_mk_out" ]] && continue
            if is_dry_run; then
                log_info "retention_module.sh" "_remove_empty_period_dirs" \
                    "[DRY-RUN] Would reap empty chain marker: $_b4_marker (trigger=$trigger)"
            else
                log_info "retention_module.sh" "_remove_empty_period_dirs" \
                    "Reaping empty chain marker: $_b4_marker (trigger=$trigger)"
                rm -rf "$_b4_marker" 2>/dev/null || true
            fi
        done < <(find "$period_dir" -maxdepth 1 -type d -name '.chain-*' 2>/dev/null)

        # Stub criterion: zero top-level *.data files AND no .archives/ subdir
        # (chain history under .archives/ must be preserved).
        # FF-118 fail-closed: a find ERROR (I/O error / permission loss on a
        # degrading mount) must NOT read as "empty" and rm -rf a populated period.
        local _data_out
        if ! _data_out=$(find "$period_dir" -maxdepth 1 -type f -name '*.data' 2>/dev/null); then
            log_warn "retention_module.sh" "_remove_empty_period_dirs" \
                "find failed on $period_dir — keeping period (fail-closed)"
            continue
        fi
        data_count=$(printf '%s\n' "$_data_out" | grep -c . || true)
        [[ "${data_count:-0}" -gt 0 ]] && continue
        [[ -d "${period_dir}/.archives" ]] && continue

        freed_bytes=$(du -sb "$period_dir" 2>/dev/null | cut -f1)
        freed_bytes="${freed_bytes:-0}"

        policy=$(detect_period_policy "$period_id" 2>/dev/null || echo "")

        if is_dry_run; then
            log_info "retention_module.sh" "_remove_empty_period_dirs" \
                "[DRY-RUN] Would remove stub: $period_dir (${freed_bytes}B, trigger=$trigger)"
            log_retention_action "remove_stub" "$vm_name" "period" \
                "$period_dir" "$period_id" "$policy" "" "" "" "$freed_bytes" \
                "" "$trigger" "dry_run" "_remove_empty_period_dirs"
            continue
        fi

        log_info "retention_module.sh" "_remove_empty_period_dirs" \
            "Removing stub period: $period_dir (${freed_bytes}B, trigger=$trigger)"
        if rm -rf "$period_dir"; then
            log_retention_action "remove_stub" "$vm_name" "period" \
                "$period_dir" "$period_id" "$policy" "" "" "" "$freed_bytes" \
                "" "$trigger" "true" "_remove_empty_period_dirs"
            declare -f sqlite_mark_chain_deleted_if_exists >/dev/null 2>&1 && \
                sqlite_mark_chain_deleted_if_exists "$vm_name" \
                    "$period_id" "." "$trigger" "deleted"
            declare -f log_period_lifecycle >/dev/null 2>&1 && \
                log_period_lifecycle "period_deleted" "$vm_name" \
                    "$period_id" "$policy" "$period_dir" "" "" \
                    "0" "0" "$freed_bytes" "" "" "0"
            declare -f log_file_operation >/dev/null 2>&1 && \
                log_file_operation "delete" "$vm_name" "$period_dir" "" \
                    "directory" "stub remediation ($trigger)" \
                    "_remove_empty_period_dirs" "true" "" "$freed_bytes"
        else
            log_error "retention_module.sh" "_remove_empty_period_dirs" \
                "Failed to remove stub: $period_dir"
            log_retention_action "error" "$vm_name" "period" \
                "$period_dir" "$period_id" "$policy" "" "" "" "0" \
                "rm_failed" "$trigger" "false" "_remove_empty_period_dirs"
        fi
    done <<< "$periods"
    return 0
}

#################################################################################
# UNBACKED-VM RETENTION WRAPPER (Bug 2 fix)
#
# Runs retention for a VM that was not backed up this session
# (skipped, excluded by policy=never, or VM-no-longer-in-libvirt).
#
# CRITICAL ORDERING:
# _remove_empty_period_dirs MUST run BEFORE run_retention_for_vm. Otherwise
# the just-mkdir'd empty W## stub at vmbackup.sh inflates the period count
# by 1 and causes run_retention_for_vm to delete one too many populated
# periods (e.g. RETENTION_WEEKS=4 at exact limit + skip crossing boundary
# -> 3 populated periods after, not 4).
#
# Honours DRY_RUN by passing through to inner functions which already log
# "[DRY-RUN] would..." lines.
#
# Args: $1 - vm_name
#       $2 - trigger (skipped|excluded|orphan_dir)
# Returns: 0 always
run_retention_for_unbacked_vm() {
    local vm_name="$1"
    local trigger="$2"
    local dry_run="${DRY_RUN:-false}"

    # 1) Stub cleanup FIRST -- so retention math sees only populated periods.
    declare -f _remove_empty_period_dirs >/dev/null 2>&1 && \
        _remove_empty_period_dirs "$vm_name" "$trigger"

    # 2) Active retention (early-returns on policy=never/accumulate).
    declare -f run_retention_for_vm >/dev/null 2>&1 && \
        run_retention_for_vm "$vm_name" "$dry_run" "$trigger"

    # 3) Orphan retention (early-returns on policy=never/accumulate).
    declare -f run_orphan_retention_for_vm >/dev/null 2>&1 && \
        run_orphan_retention_for_vm "$vm_name" "$dry_run" "$trigger"

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
    local trigger="${6:-prune}"
    local safe_name
    safe_name=$(vm_fs_name "$vm_name") || return $?
    local chain_dir="${BACKUP_PATH}${safe_name}/${period_id}/.archives/${chain_name}"
    
    if [[ ! -d "$chain_dir" ]]; then
        log_error "retention_module.sh" "_remove_archive_chain" \
            "Archive chain not found: $chain_dir"
        return 1
    fi
    
    local freed_bytes
    freed_bytes=$(du -sb "$chain_dir" 2>/dev/null | cut -f1 || echo 0)
    local policy=$(get_vm_rotation_policy "$vm_name")
    
    # Safety check
    if ! _is_safe_to_remove "$chain_dir"; then
        log_error "retention_module.sh" "_remove_archive_chain" \
            "Safety check failed: $chain_dir"
        return 1
    fi
    
    # FF-185: dry-run report AFTER the safety check (mirrors _remove_period) so a
    # preview never overstates a chain the real run would refuse.
    if [[ "$dry_run" == "true" ]]; then  # [DRY-RUN-KEEPER: local-param polarity preserved, see 109-phase7-spec.md §1.3.3]
        log_info "retention_module.sh" "_remove_archive_chain" \
            "[DRY RUN] Would remove archive chain: $chain_dir (${freed_bytes} bytes)"
        echo "$freed_bytes"
        return 0
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
            "$policy" "0" "0" "0" "0" "rm_failed" "$trigger" "false" "$caller"
        return 1
    }
    
    log_retention_action "delete" "$vm_name" "archive_chain" "$chain_dir" "$period_id" \
        "$policy" "0" "0" "0" "$freed_bytes" "" "$trigger" "true" "$caller"
    
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
    local trigger="${5:-prune}"
    local safe_name
    safe_name=$(vm_fs_name "$vm_name") || return $?
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
        result=$(_remove_archive_chain "$vm_name" "$period_id" "$chain_name" "$dry_run" "$caller" "$trigger")
        rc=$?
        if [[ $rc -eq 0 ]]; then
            total_freed=$(( total_freed + ${result:-0} ))
            (( chain_count++ ))
        else
            (( fail_count++ ))
        fi
    done
    
    if [[ "$dry_run" != "true" && $chain_count -gt 0 ]]; then  # [DRY-RUN-KEEPER: local-param inverted polarity preserved, see 109-phase7-spec.md §1.3.3]
        # Remove the empty .archives directory if all chains removed
        if [[ $fail_count -eq 0 ]]; then
            rmdir "$archives_dir" 2>/dev/null
        fi
        # DUP-10: rebuild_chain_manifest call removed (writer to dead file).
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
    local trigger="${4:-prune}"
    local safe_name
    safe_name=$(vm_fs_name "$vm_name") || return $?
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
        
        if _remove_period "$vm_name" "$period_id" "$dry_run" "true" "$caller" "$trigger"; then
            # Verify deletion actually happened (protection may skip with rc=0)
            if [[ ! -d "$period_dir" ]] || [[ "$dry_run" == "true" ]]; then  # [DRY-RUN-KEEPER: local-param polarity preserved, see 109-phase7-spec.md §1.3.3]
                total_freed=$(( total_freed + ${period_bytes:-0} ))
            fi
            (( period_count++ ))
        else
            (( fail_count++ ))
        fi
    done
    
    # Remove the VM directory itself (should be empty now)
    if [[ "$dry_run" != "true" && $fail_count -eq 0 ]]; then  # [DRY-RUN-KEEPER: local-param inverted polarity preserved, see 109-phase7-spec.md §1.3.3]
        if [[ -d "$vm_dir" ]]; then
            # Only remove if empty (safety)
            local remaining
            remaining=$(find "$vm_dir" -mindepth 1 -maxdepth 1 2>/dev/null | wc -l)
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
    safe_name=$(vm_fs_name "$vm_name") || return $?
    local period_dir="${BACKUP_PATH}${safe_name}/${old_period_id}"
    
    if [[ ! -d "$period_dir" ]]; then
        log_debug "retention_module.sh" "archive_active_chains" \
            "Period directory not found: $period_dir"
        return 0
    fi
    
    # DUP-10: derive active chain id from disk (mtime of earliest .full.data
    # in the OLD period — not current period — because this function is invoked
    # at period boundary against the closing period).
    local active_chain
    local earliest_mtime
    earliest_mtime=$(find "$period_dir" -maxdepth 1 -type f \
      \( -name "*.full.data" -o -name "*.copy.data" -o -name "*.inc.virtnbdbackup.*.data" \) \
      -printf '%T@\n' 2>/dev/null | sort -n | head -1)
    if [[ -n "$earliest_mtime" ]]; then
      active_chain="chain-$(date -d "@${earliest_mtime%.*}" +%Y-%m-%d)"
    fi

    if [[ -z "$active_chain" ]]; then
        log_debug "retention_module.sh" "archive_active_chains" \
            "No active chain to archive for: $vm_name"
        return 0
    fi

    # Count restore points directly from disk (DUP-10: was count_period_restore_points)
    local restore_point_count
    restore_point_count=$(find "$period_dir" -maxdepth 1 -type f \
      \( -name "*.full.data" -o -name "*.copy.data" -o -name "*.inc.virtnbdbackup.*.data" \) \
      2>/dev/null | wc -l)
    
    if [[ "$restore_point_count" -eq 0 ]]; then
        log_debug "retention_module.sh" "archive_active_chains" \
            "No restore points in period $old_period_id for chain $active_chain"
        return 0
    fi
    
    # B4/CLEAN-01: archive via the shared canonical mover. The previous body
    # wrote to <period>/.chain-<ts> (invisible to vmrestore; later reaped as a
    # stub) and moved nothing (dead backup-<name>* pattern). Delegate to
    # _archive_chain_files so the period-boundary path produces the SAME canonical
    # .archives/chain-<date> layout (incl. NVRAM/TPM) as the in-backup archiver,
    # and honour dry-run (previously absent here).
    if is_dry_run; then
        log_info "retention_module.sh" "archive_active_chains" \
            "[DRY-RUN] Would archive chain $active_chain (period $old_period_id, $restore_point_count restore points) into .archives/"
        return 0
    fi

    if ! declare -f _archive_chain_files >/dev/null 2>&1; then
        log_error "retention_module.sh" "archive_active_chains" \
            "Shared archiver _archive_chain_files unavailable - refusing to archive (avoids mis-filing the chain)"
        return 1
    fi

    log_info "retention_module.sh" "archive_active_chains" \
        "Archiving chain: $active_chain (period: $old_period_id)"

    _archive_chain_files "$vm_name" "$period_dir"
    local _acf_rc=$?
    local archive_dir="$_ARCHIVE_LAST_PATH"
    if (( _acf_rc == 2 )); then
        log_debug "retention_module.sh" "archive_active_chains" \
            "Nothing to archive for $vm_name in period $old_period_id"
        return 0
    elif (( _acf_rc != 0 )); then
        log_error "retention_module.sh" "archive_active_chains" \
            "Failed to archive chain for $vm_name (shared mover rc=$_acf_rc)"
        return 1
    fi

    local files_moved=${_ARCHIVE_LAST_TOTAL:-0}
    local total_bytes
    total_bytes=$(du -sb "$archive_dir" 2>/dev/null | cut -f1 || echo 0)

    # Update SQLite chain_health to mark as archived (INT-20: archive_path="" keeps
    # chain_location semantics; total_bytes persists archive_size_bytes).
    if declare -f sqlite_archive_chain >/dev/null 2>&1; then
        sqlite_archive_chain "$vm_name" "$old_period_id" "$archive_dir" "" "$total_bytes"
        log_debug "retention_module.sh" "archive_active_chains" \
            "Marked chain as archived in SQLite: $vm_name/$old_period_id (size=${total_bytes}B)"
    fi

    # Log chain lifecycle. archive_subdir = basename of the canonical dest (the old
    # dangling $archive_subdir from the DUP-10 removal is gone).
    if declare -f log_chain_lifecycle >/dev/null 2>&1; then
        log_chain_lifecycle "chain_archived" "$vm_name" "$active_chain" "$old_period_id" \
            "$archive_dir" "$(basename "$archive_dir")" \
            "$files_moved" "$total_bytes" "$archive_reason" "period_rotation" \
            "incremental" "" ""
    fi

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
    local safe_name
    safe_name=$(vm_fs_name "$vm_name") || return $?
    local vm_dir="${BACKUP_PATH}${safe_name}"
    local db_path="${VMBACKUP_DB:-${BACKUP_PATH%/}/_state/vmbackup.db}"

    [[ ! -d "$vm_dir" ]] && return 0
    [[ ! -f "$db_path" ]] && return 0

    local fixed_phantom=0 fixed_orphan=0

    # --- Pass 1: DB rows with no matching filesystem directory → phantom records
    # Phase 4 commit 4a: typed read via lib/sqlite_ro.sh.
    local db_periods
    db_periods=$(sqlite_get_tracked_periods "$db_path" "$vm_name")

    local period_id
    while IFS= read -r period_id; do
        [[ -z "$period_id" ]] && continue
        local period_dir="${vm_dir}/${period_id}"
        if [[ ! -d "$period_dir" ]]; then
            if [[ "$dry_run" == "true" ]]; then  # [DRY-RUN-KEEPER: local-param polarity preserved, see 109-phase7-spec.md §1.3.3]
                log_info "retention_module.sh" "reconcile_vm_chain_state" \
                    "[DRY RUN] Would mark phantom record as deleted: $vm_name/$period_id"
            else
                log_warn "retention_module.sh" "reconcile_vm_chain_state" \
                    "Phantom record: $vm_name/$period_id (DB says active, filesystem missing) → marking deleted"
                # Phase 4 commit 4b: typed write via lib/sqlite_module.sh.
                sqlite_mark_phantom_chain "$vm_name" "$period_id" 2>/dev/null
            fi
            ((fixed_phantom++))
        fi
    done <<< "$db_periods"

    # --- Pass 2: Filesystem directories with no DB row → create minimal record
    local fs_periods
    # UNI-007 (Phase 3): consumes lib/period.sh::get_vm_periods. vm_dir already
    # resolved at function top.
    fs_periods=$(get_vm_periods "$vm_dir")

    while IFS= read -r period_id; do
        [[ -z "$period_id" ]] && continue
        # Phase 4 commit 4a: typed read via lib/sqlite_ro.sh.
        local exists
        exists=$(sqlite_chain_period_exists_count "$db_path" "$vm_name" "$period_id")
        if [[ "${exists:-0}" -eq 0 ]]; then
            local detected_policy=$(detect_period_policy "$period_id")
            if [[ "$dry_run" == "true" ]]; then  # [DRY-RUN-KEEPER: local-param polarity preserved, see 109-phase7-spec.md §1.3.3]
                log_info "retention_module.sh" "reconcile_vm_chain_state" \
                    "[DRY RUN] Would create DB record for untracked period: $vm_name/$period_id (policy=$detected_policy)"
            else
                log_info "retention_module.sh" "reconcile_vm_chain_state" \
                    "Untracked period: $vm_name/$period_id → creating DB record (policy=$detected_policy)"
                # Phase 4 commit 4b: typed write via lib/sqlite_module.sh.
                sqlite_register_untracked_period "$vm_name" "$period_id" "$detected_policy" 2>/dev/null
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

        # FF-186 (RECON-01): the on-disk basename is the vm_fs_name TOKEN
        # (slug-<sha256>) for spaced/special VM names, but the catalogue is keyed
        # on the REAL libvirt name. Resolve token -> real (the same mapper PRUNE-01
        # uses in run_prune_mode) so pass-1 phantom detection and pass-2
        # registration hit the correct key. Falls back to the basename when the
        # mapper is absent (name already safe / VM gone from libvirt).
        local real_name="$dir_name"
        declare -f _prune_real_name >/dev/null 2>&1 && real_name=$(_prune_real_name "$dir_name")

        reconcile_vm_chain_state "$real_name" "$dry_run"
    done < <(find "$BACKUP_PATH" -maxdepth 1 -mindepth 1 -type d 2>/dev/null | sort)

    log_info "retention_module.sh" "reconcile_all_chain_state" \
        "Reconciliation complete"
}

#################################################################################
# MODULE INITIALIZATION
#################################################################################

log_debug "retention_module.sh" "init" \
    "Retention module v${RETENTION_MODULE_VERSION} loaded"
