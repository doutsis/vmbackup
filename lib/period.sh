#!/usr/bin/env bash
#
# lib/period.sh — Period directory enumeration and age calculation for
#                  vmbackup/vmrestore.
#
# UNI-007: lifts four FS-only helpers out of modules/retention_module.sh
# so vmrestore (which has no concept of rotation policy) can consume the
# same period-discovery logic without dragging vmbackup's policy/global
# state into the lib.
#
# Periodic-only by design (Phase 3 D1): no `accumulate` branch lives
# here. Accumulate is a vmbackup-orchestration concept; lifting it would
# re-introduce implicit policy knowledge the G3 decision was meant to
# eliminate. Callers that need accumulate detection use the existing
# binary-local helpers (vmrestore's `is_accumulate` / `has_backup_data`,
# vmbackup's inline check at the call site).
#
# UNI-321: idempotency guard — re-source is a no-op once get_vm_periods is defined.
declare -F get_vm_periods >/dev/null 2>&1 && return 0

# API (Phase 3 spec §2.1):
#   get_vm_periods <vm_dir>
#       Cross-format FS enumeration of periodic dirs only. Matches
#       YYYYMMDD / YYYY-Www / YYYYMM. No policy knowledge, no
#       BACKUP_PATH knowledge, no sanitize_vm_name call. Returns every
#       periodic dir present regardless of rotation policy. Returns
#       empty (rc 0) for accumulate VMs or non-existent vm_dir.
#       Does NOT sort — callers sort if they want order (R2).
#
#   get_vm_periods_for_policy <vm_dir> <policy>
#       Policy-driven filter. Policy MUST be passed explicitly — no
#       environment lookup, no implicit defaults, no `accumulate`
#       branch (caller handles accumulate separately).
#
#   detect_period_policy <period_id>
#       Inspects a single period ID string; returns daily / weekly /
#       monthly / unknown. Pure string parse, no FS, no policy state.
#
#   calculate_any_period_age <period_id>
#       Returns age in days from period start to current UTC.
#       Equivalence proven against modules/rotation_module.sh
#       ::calculate_period_age (the production path) by
#       tests/109-phase3-period-vectors.sh — see Phase 3 spec §4
#       commit 1 / U2.
#
# Caller responsibilities (Phase 3 spec §2.2):
# Callers pass `vm_dir` (path), not `vm_name` (string). Each caller
# must (1) call sanitize_vm_name "$vm_name", (2) assemble
# vm_dir="${BACKUP_PATH}${safe_name}" (or use the already-resolved
# vm_dir), (3) pass vm_dir into the lib function.
#

# Cross-format FS enumeration of periodic dirs only.
# Args: $1 - vm_dir (absolute path)
# Returns: newline-separated list of period IDs (UNSORTED — caller sorts).
get_vm_periods() {
    local vm_dir="$1"

    [[ ! -d "$vm_dir" ]] && return 0

    # Match all known periodic formats with a single find command.
    # Note: no `| sort` — R2: callers sort if they want order.
    # shellcheck disable=SC2038  # period IDs are strict [0-9W-]+; no whitespace possible by construction
    find "$vm_dir" -maxdepth 1 -type d \( \
        -regextype posix-extended \
        -regex '.*/[0-9]{8}$' -o \
        -regex '.*/[0-9]{4}-W[0-9]{2}$' -o \
        -regex '.*/[0-9]{6}$' \
    \) 2>/dev/null | xargs -r -n1 basename
}

# Policy-driven filter on top of get_vm_periods.
# Args: $1 - vm_dir (absolute path)
#       $2 - policy (daily|weekly|monthly)
# Returns: newline-separated list of period IDs matching the policy
#          (UNSORTED — caller sorts).
get_vm_periods_for_policy() {
    local vm_dir="$1"
    local policy="$2"

    [[ ! -d "$vm_dir" ]] && return 0

    # shellcheck disable=SC2038  # period IDs are strict [0-9W-]+; no whitespace possible by construction
    case "$policy" in
        daily)
            find "$vm_dir" -maxdepth 1 -type d -regextype posix-extended \
                -regex '.*/[0-9]{8}$' 2>/dev/null | xargs -r -n1 basename
            ;;
        weekly)
            find "$vm_dir" -maxdepth 1 -type d -regextype posix-extended \
                -regex '.*/[0-9]{4}-W[0-9]{2}$' 2>/dev/null | xargs -r -n1 basename
            ;;
        monthly)
            find "$vm_dir" -maxdepth 1 -type d -regextype posix-extended \
                -regex '.*/[0-9]{6}$' 2>/dev/null | xargs -r -n1 basename
            ;;
        *)
            # accumulate / unknown: not this lib's concern (D1).
            return 0
            ;;
    esac
}

# Detect the rotation policy that created a period based on its format.
# Args: $1 - period_id (e.g. "20260215", "2026-W07", "202602")
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

# Calculate age of any period format in days (from period start date).
# NOTE: For orphan retention, use calculate_orphan_age() (lives in
#       retention_module.sh — SQL-backed) instead. This function
#       calculates from period START which is wrong for retention
#       decisions on orphans.
#
# Implementation lifted from modules/rotation_module.sh::calculate_period_age
# verbatim (the production path — U2). The shim that lived in
# retention_module.sh L223–226 is removed in this phase; equivalence
# proven by tests/109-phase3-period-vectors.sh.
#
# Args: $1 - period_id (any format)
# Returns: age in days (echoes "0" with rc 1 on parse failure)
calculate_any_period_age() {
    local period_id="$1"
    local policy
    policy=$(detect_period_policy "$period_id")

    [[ "$policy" == "unknown" ]] && { echo "0"; return 1; }

    local today period_date
    today=$(date -u +%s)

    case "$policy" in
        daily)
            # YYYYMMDD format
            period_date=$(date -u -d "${period_id:0:4}-${period_id:4:2}-${period_id:6:2}" +%s 2>/dev/null) || {
                echo "0"; return 1
            }
            ;;
        weekly)
            # YYYY-Www format. Match rotation_module.sh::calculate_period_age
            # exactly (the production path): use Jan-4 + (N-1) weeks as the
            # ISO 8601 reference. Do NOT adjust to Monday — the dead-code
            # fallback in retention_module.sh did, but the live code path
            # never executed that branch (the shim always delegated to
            # rotation_module.sh). U2 / spec §6 risk row 1.
            local year="${period_id:0:4}"
            local week="${period_id:6:2}"
            period_date=$(date -u -d "${year}-01-04 +$((week - 1)) weeks" +%s 2>/dev/null) || {
                echo "0"; return 1
            }
            ;;
        monthly)
            # YYYYMM format
            period_date=$(date -u -d "${period_id:0:4}-${period_id:4:2}-01" +%s 2>/dev/null) || {
                echo "0"; return 1
            }
            ;;
        *)
            echo "0"
            return 1
            ;;
    esac

    echo $(( (today - period_date) / 86400 ))
}
