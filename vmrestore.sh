#!/bin/bash

#################################################################################
# vmrestore — Automated Restore for libvirt/KVM Virtual Machines
# Vibe coded by James Doutsis — james@doutsis.com
#
# Wraps virtnbdrestore to provide single-command disaster recovery, clone
# restores and point-in-time recovery — with full identity management,
# TPM/BitLocker support and pre-flight safety checks.
#
# Features:
#   - Disaster recovery: rebuilds VM with original UUID, MACs and name
#   - Clone mode: independent copy with new identity (one flag: --name)
#   - Disk restore: in-place single-disk replacement or extract to staging
#   - Point-in-time: restore to any restore point, period or archived chain
#   - TPM/BitLocker: state restored automatically in both DR and clone mode
#   - UEFI/NVRAM isolation: clone gets its own firmware state file
#   - Pre-flight safety: disk collision, free space, running VM detection
#   - Auto-detection: backup type, period, chain layout, storage pool
#   - Dry-run mode: preview every action without writing anything
#
# Prerequisites:
#   virtnbdbackup >= 2.28    virtnbdrestore (disk restore engine)
#   libvirt-daemon-system    virsh domain management
#   qemu-utils               qemu-img for post-restore disk checks
#   bash >= 5.0              required for associative arrays
#
# Usage:
#   vmrestore --help
#
# Repository:
#   https://github.com/doutsis/vmrestore
#
# Relationship to vmbackup:
#   vmrestore is a standalone script — no shared code, no sourced modules,
#   no runtime coupling to vmbackup.sh. But it exclusively restores backups
#   created by vmbackup. The two scripts are complementary halves of one
#   system: vmbackup backs up, vmrestore restores.
#
#################################################################################
#
# SCRIPT ARCHITECTURE
# ===================
# Single self-contained script. No modules, no database,
# no runtime dependency on vmbackup.
#
# Section layout (search with "# ── Section Name"):
#
#   Logging                  Log file init, structured log_info/warn/error
#   Configuration            Backup path resolution from vmbackup config
#   Pre-flight Free Space    Destination capacity check before restore
#   Backup Detection         Full vs incremental, checkpoint enumeration
#   Path Resolution          Period/chain/archive path discovery
#   Listing                  --list and --list-restore-points output
#   Storage Pool Refresh     Longest-prefix pool match + virsh pool-refresh
#   TPM Restore              swtpm state dir recreation at correct UUID
#   New-Identity Define      Clone mode: strip UUID/MACs, rename, define
#   Disk Enumeration         enumerate_disks() for --disk validation/display
#   Disk Collision Protection  Predict output files, check for conflicts
#   Core Restore             Main restore_vm() orchestration function
#   Verify / Dump            --verify checksum validation, --dump output
#   Usage                    --help output
#   CLI Parsing              Argument parsing and validation
#   Main                     Entry point, mode dispatch
#
# Restore flow (inside restore_vm):
#
#   1. Resolve backup path, detect layout, find latest period/chain
#   2. Pre-flight: disk collision, free space, running VM checks
#   3. Disk-restore mode: if --disk set, branch to in-place replacement
#      or staging extract (no VM define, no TPM)
#   4. Run virtnbdrestore (DR: -c -D, clone: -c with staging dir)
#   5. DR: re-inject original UUID and MACs
#      Clone: strip UUID/MACs, rename disks, define with new identity
#   6. Restore TPM state, isolate NVRAM for clones
#   7. qemu-img check, storage pool refresh, completion summary
#
#################################################################################
#
# DISCLAIMER
# ==========
# 100% vibe coded. Could be 100% wrong.
# Appropriate testing in any and all environments is required.
# Build your own confidence that the backups work.
# Backups are only as good as your restores.
#
#################################################################################

set -uo pipefail

# UNI-003 + UNI-008 + UNI-321: Source bootstrap.sh (lib loader) + version
# + exit codes from lib/. This is the ONLY remaining inline source block —
# bootstrap.sh provides source_lib_or_die for every subsequent lib load.
# Done very early because the rest of the script depends on these constants.
# Variable name is _VMBACKUP_LIB_DIR (package-scoped) so both binaries
# share the same canonical name.
_VMBACKUP_LIB_DIR="$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")/lib" && pwd 2>/dev/null)" || _VMBACKUP_LIB_DIR=""
for _lib in bootstrap.sh version.sh exit_codes.sh; do
  if [[ -r "$_VMBACKUP_LIB_DIR/$_lib" ]]; then
    # shellcheck source=/dev/null
    source "$_VMBACKUP_LIB_DIR/$_lib"
  elif [[ -r "/opt/vmbackup/lib/$_lib" ]]; then
    # shellcheck source=/dev/null
    source "/opt/vmbackup/lib/$_lib"
  else
    echo "Error: lib/$_lib not found" >&2; exit 8
  fi
done
unset _VMBACKUP_LIB_DIR _lib

# ── Logging ──────────────────────────────────────────────────────────────────

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# UNI-001: Source unified logging library. Provides log_msg/info/warn/
# error/debug with 3-arg signature (process, fn, msg). Replaces previous
# inline 2-arg log_* definitions; vmrestore now calls log_X with 3 args.
# LOG_FILE is checked at call time (not source time); fine to source early.
source_lib_or_die logging.sh

# UNI-323 (Phase 7 commit 2): Source canonical path-canonicalisation API
# (pu_normalise_path, pu_safe_realpath, pu_strip_trailing_slash,
# pu_ensure_trailing_slash, pu_join_paths). Strict-alphabetical position:
# logging < path_utils < period. No load-order dependency (pure functions).
source_lib_or_die path_utils.sh

# UNI-011: Source VM-name sanitiser. Reject-not-substitute contract; relies
# on EXIT_USAGE being defined (it is, just above).
source_lib_or_die vm_name_utils.sh

# UNI-007: Source period-helper library. Provides get_vm_periods,
# get_vm_periods_for_policy, detect_period_policy, calculate_any_period_age.
# vmrestore consumes get_vm_periods (in list_periods); the other three are
# unused here but loaded for symmetry with vmbackup. See Phase 3 spec §4
# commit 2.
source_lib_or_die period.sh

# UNI-309: Source backup-tree walker (skeletal, D2). Provides walk_backup_tree()
# used by list_vms() for VM-level skip-list convergence (B). Period-level skip
# convergence in vmrestore is handled by lib/period.sh (get_vm_periods'
# strict period-shape regex), which list_periods() delegates to.
source_lib_or_die backup_walker.sh

# UNI-006 (Phase 6 commit 2): Source shared TPM read-side helpers
# (get_vm_uuid, has_tpm_device, validate_tpm_backup, …) so restore_tpm()
# in this binary does not depend on the optional, lazy-loaded
# modules/tpm_backup_module.sh.
source_lib_or_die tpm_io.sh

# UNI-014 M1 (Phase 6 commit 3): Source virtnbd arg-builder helpers.
# Used by the per-disk loop, full-restore array, and run_virtnbd_action.
# Retry path (post-failure -c -U qemu:///system) stays inline (out of M1).
source_lib_or_die virtnbd.sh

# UNI-013 (Phase 6 commit 4): Source libvirt domain/XML/pool helpers.
# Replaces 26 inline virsh callsites with lv_* wrappers (see
# 109-phase6-spec.md §2.4 and tests/baselines/phase6/libvirt-fn-table.md).
source_lib_or_die libvirt.sh

# UNI-012: Source config-instance path resolver. Provides
# resolve_config_instance() and get_config_file(); used by resolve_backup_path().
source_lib_or_die config_instance.sh

# UNI-322 (Phase 7 commit 1): Source canonical dry-run predicate API
# (is_dry_run, if_dry_run, if_not_dry_run + dry_run_normalise_state).
# Strict-alphabetical position: config_instance < dry_run < exit_codes.
# CLI parser (--dry-run handler) re-invokes dry_run_normalise_state
# after parsing completes — see end of CLI parsing section.
source_lib_or_die dry_run.sh

# UNI-002: Source per-VM locking library. Same lock-file naming convention
# as vmbackup ("vmbackup-<vm>.lock") so the two tools mutually exclude.
# Requires \$LOCK_DIR set before any create_lock/remove_lock call (set in
# main() once \$OPT_BACKUP_PATH is known; see UNI-002 wrapper around
# restore_vm()).
source_lib_or_die vm_lock.sh

# UNI-010: Source shared signal-trap registration helpers.
source_lib_or_die signal_handlers.sh

# Phase 4 (UNI-605 / USE-03): Source the SQLite read-only library so --list
# can enrich each VM's output with chain-health summary line. Optional —
# vmrestore must remain usable on hosts that do not have a vmbackup ledger
# DB present (and on hosts without the sqlite3 binary). The library is
# sourced eagerly here for symbol availability; sqlite_init_readonly() is
# only called on demand from list_vms (memoised via
# _VMRESTORE_SQLITE_RO_INITED). All call sites guard on
# SQLITE_MODULE_AVAILABLE before issuing queries, so missing DB / missing
# sqlite3 / permission error all degrade silently to the pre-Phase-4
# walker-only output.
# UNI-321: kept optional (try/no-fail) because vmrestore must work on
# hosts without sqlite_ro.sh installed.
if [[ -r "$SCRIPT_DIR/lib/sqlite_ro.sh" ]]; then
    # shellcheck source=lib/sqlite_ro.sh
    source "$SCRIPT_DIR/lib/sqlite_ro.sh"
elif [[ -r /opt/vmbackup/lib/sqlite_ro.sh ]]; then
    source /opt/vmbackup/lib/sqlite_ro.sh
fi
# Note: missing sqlite_ro.sh is NOT fatal — vmrestore stays standalone-capable.
# Process-scoped memo: sqlite_init_readonly is called at most once per run.
_VMRESTORE_SQLITE_RO_INITED=0
_VMRESTORE_SQLITE_RO_OK=0

# Phase 8 (UNI-902b): Source the SQLite write-side library so vmrestore can
# log restore_sessions rows. Same warn-and-continue contract as sqlite_ro.sh
# (missing module is NOT fatal — DR invariant: vmrestore never blocks on the
# catalogue being unreachable). All call sites guard on sqlite_is_available
# before invoking writers.
if [[ -r "$SCRIPT_DIR/lib/sqlite_module.sh" ]]; then
    # shellcheck source=lib/sqlite_module.sh
    source "$SCRIPT_DIR/lib/sqlite_module.sh"
elif [[ -r /opt/vmbackup/lib/sqlite_module.sh ]]; then
    # shellcheck source=/dev/null  # installed-tree fallback, may not be readable at lint time
    source /opt/vmbackup/lib/sqlite_module.sh
fi
# _RESTORE_SESSION_ID: rowid of the in-flight restore_sessions row, set by
# sqlite_restore_session_start() and consumed by _vmrestore_cleanup().
# Empty → start was skipped (DB unavailable / dry-run / read-only mode) or
# never reached (init failure) → end-call is a no-op.
_RESTORE_SESSION_ID=""

# UNI-010: Cleanup state. _VMRESTORE_LOCK_VM is set after create_lock; the
# EXIT trap releases the lock if still held. _VMRESTORE_STAGING_DIRS tracks
# any PIT staging dirs and clone-mode staging dirs created so a SIGINT/SIGTERM
# mid-restore removes them (success paths still call cleanup_pit_staging /
# rm -rf the clone staging directly; the trap is the safety net for
# interrupted runs).
_VMRESTORE_LOCK_VM=""
declare -a _VMRESTORE_STAGING_DIRS=()
# F-vmr2514: a vmconfig.virtnbdbackup.0.xml provisioned INTO the backup/archive
# dir for archived chains (restore_vm) must not survive a die/signal before the
# success-path rm — the backup tree is read-only. Trap-tracked so any exit
# restores it. Empty unless restore_vm provisioned one this run.
_VMRESTORE_PROVISIONED_VMCONFIG=""

_vmrestore_cleanup() {
    local rc=$?
    # Remove tracked staging dirs (idempotent; cleanup_pit_staging is rm -rf)
    if (( ${#_VMRESTORE_STAGING_DIRS[@]} > 0 )); then
        local d
        for d in "${_VMRESTORE_STAGING_DIRS[@]}"; do
            [[ -n "$d" && -d "$d" ]] || continue
            log_warn "vmrestore.sh" "_vmrestore_cleanup" "Removing staging dir on exit (rc=$rc): $d"
            rm -rf "$d" 2>/dev/null || true
        done
        _VMRESTORE_STAGING_DIRS=()
    fi
    # F-vmr2514: remove a provisioned vmconfig left in the backup/archive dir by
    # an interrupted/failed restore_vm (the backup tree is read-only). The happy
    # path already rm's it at the end of the else-block and clears this global,
    # so on success this is a no-op. The -f guard + the "this-run-only" creation
    # invariant (provisioning runs only when no vmconfig.virtnbdbackup.*.xml
    # pre-existed) ensure we never delete a genuine backup artefact.
    if [[ -n "${_VMRESTORE_PROVISIONED_VMCONFIG:-}" && -f "$_VMRESTORE_PROVISIONED_VMCONFIG" ]]; then
        log_warn "vmrestore.sh" "_vmrestore_cleanup" "Removing provisioned vmconfig on exit (rc=$rc): $_VMRESTORE_PROVISIONED_VMCONFIG"
        rm -f "$_VMRESTORE_PROVISIONED_VMCONFIG" 2>/dev/null || true
        _VMRESTORE_PROVISIONED_VMCONFIG=""
    fi
    # Release lock if still held
    if [[ -n "$_VMRESTORE_LOCK_VM" && -n "${LOCK_DIR:-}" ]]; then
        log_warn "vmrestore.sh" "_vmrestore_cleanup" "Releasing lock on exit (rc=$rc): VM '$_VMRESTORE_LOCK_VM'"
        remove_lock "$_VMRESTORE_LOCK_VM" 2>/dev/null || true
        _VMRESTORE_LOCK_VM=""
    fi
    # Phase 8 (UNI-902b): finalise restore_sessions row if one is in flight.
    # Trap fires unconditionally (success / die / SIGINT / SIGTERM); this is
    # the only place that sees the final rc. sqlite_restore_session_end is
    # idempotent via _SQLITE_RESTORE_SESSION_ENDED so a re-entrant trap is
    # harmless. Empty session-id → start was skipped → end is a no-op.
    if [[ -n "${_RESTORE_SESSION_ID:-}" ]] && declare -F sqlite_is_available >/dev/null 2>&1 && sqlite_is_available; then
        local final_status
        case "$rc" in
            0)         final_status=success ;;
            130|143)   final_status=interrupted ;;  # SIGINT / SIGTERM
            *)         final_status=failed ;;
        esac
        local _rse_rc=0
        sqlite_restore_session_end "$_RESTORE_SESSION_ID" "$final_status" "$rc" || _rse_rc=$?
        # rc=2 = idempotent no-op (writer was already called); rc=1 = real failure.
        # Only warn on the latter — see INT-13.
        if (( _rse_rc != 0 && _rse_rc != 2 )); then
            log_warn "vmrestore.sh" "_vmrestore_cleanup" "restore session end-row not recorded"
        fi
    fi
    return $rc
}

_vmrestore_handle_sigint() {
    log_error "vmrestore.sh" "main" "Restore interrupted by SIGINT (Ctrl+C)"
    log_error "vmrestore.sh" "main" "Recovery: re-run vmrestore; lock and staging dir have been removed"
    # _vmrestore_cleanup will fire via EXIT trap with rc=130
    exit 130
}

_vmrestore_handle_sigterm() {
    log_error "vmrestore.sh" "main" "Restore terminated by SIGTERM (timeout or systemd stop)"
    exit 143
}

if [[ -d /var/log/vmrestore ]]; then
    LOG_DIR="${LOG_DIR:-/var/log/vmrestore}"
else
    LOG_DIR="${LOG_DIR:-$SCRIPT_DIR/logs}"
fi
LOG_FILE=""
START_EPOCH=""
ORIG_ARGS=""

init_logging() {
    START_EPOCH=$(date +%s)
    mkdir -p "$LOG_DIR" 2>/dev/null || LOG_DIR="/tmp"
    # Temporary log until we know the VM name (finalize_log renames it)
    LOG_FILE="$LOG_DIR/vmrestore-$(date +%Y%m%d-%H%M%S).log"
    touch "$LOG_FILE" 2>/dev/null || LOG_FILE="/tmp/vmrestore-$(date +%s).log"
    chmod 600 "$LOG_FILE" 2>/dev/null || true
}

finalize_log_name() {
    # Rename log to include VM name: {vmname}-{timestamp}.log
    local vm_label="${OPT_VM_NAME:-unknown}"
    # Strip path components if --vm was given a full path
    vm_label=$(basename "$vm_label")
    local new_log="$LOG_DIR/${vm_label}-$(date +%Y%m%d-%H%M%S).log"
    if [[ "$LOG_FILE" != "$new_log" ]]; then
        mv "$LOG_FILE" "$new_log" 2>/dev/null && LOG_FILE="$new_log"
    fi
}

log_invocation_summary() {
    local sep="════════════════════════════════════════════════════════════"
    {
        echo "$sep"
        echo "vmrestore v$VMBACKUP_VERSION — $(date '+%Y-%m-%d %H:%M:%S')"
        echo "$sep"
        echo "Invocation:   vmrestore.sh $ORIG_ARGS"
        echo "Mode:         ${OPT_MODE:-unset}"
        echo "VM:           ${OPT_VM_NAME:-unset}"
        echo "Period:       ${OPT_PERIOD:-auto}"
        echo "Restore Point: ${OPT_RESTORE_POINT:-latest}"
        echo "Restore Path: ${OPT_RESTORE_PATH:-unset}"
        echo "Clone Name:   ${OPT_NAME:-none (disaster recovery)}"
        echo "Backup Path:  ${OPT_BACKUP_PATH:-unset}"
        echo "Disk Filter:  ${OPT_DISK:-all}"
        echo "No Pre-Restore: $OPT_NO_PRE_RESTORE"
        echo "Skip Config:  $OPT_SKIP_CONFIG"
        echo "Skip TPM:     $OPT_SKIP_TPM"
        echo "Force:        $OPT_FORCE"
        echo "Dry Run:      $OPT_DRY_RUN"  # [DRY-RUN-KEEPER: display string, not a guard — see 109-phase7-spec.md §4 commit 1 acceptance + R3]
        echo "Log File:     $LOG_FILE"
        echo "$sep"
    } >> "$LOG_FILE"
}

log_completion_summary() {
    local rc="$1"
    local end_epoch
    end_epoch=$(date +%s)
    local elapsed=$(( end_epoch - START_EPOCH ))
    local mins=$(( elapsed / 60 ))
    local secs=$(( elapsed % 60 ))
    local sep="════════════════════════════════════════════════════════════"
    {
        echo "$sep"
        printf "Duration:     %dm %ds\n" "$mins" "$secs"
        echo "Exit Status:  $rc"
        echo "$sep"
    } >> "$LOG_FILE"
    # Also show to terminal
    log_info "vmrestore.sh" "main" "Duration: ${mins}m ${secs}s — exit $rc — log: $LOG_FILE"
}

_log() {
    # UNI-001: Removed in favour of lib/logging.sh log_msg(). Kept as a
    # thin shim ONLY for any caller that still uses the old 3-arg form
    # (level, fn, msg); transparently forwards to log_msg with the
    # vmrestore.sh process tag. New code should call log_info / log_warn
    # / log_error / log_debug directly with 3 args (process, fn, msg).
    log_msg "$1" "vmrestore.sh" "$2" "$3"
}
# UNI-001: die() retains its (msg, fn, exitcode) signature; updated to
# pass the vmrestore.sh process tag through to lib/logging.sh log_error.
die() {
    log_error "vmrestore.sh" "${2:-main}" "$1"
    exit "${3:-$EXIT_ERROR}"
}

# Run a command, teeing all output (stdout+stderr) into the log file
# while still displaying on the terminal. Returns the command's exit code.
run_logged() {
    "$@" 2>&1 | tee -a "$LOG_FILE"
    return "${PIPESTATUS[0]}"
}

# INT-09 (2026-05-23): Human-readable size formatting — pure bash, no
# external dep. Mirrors vmbackup.sh::_format_size verbatim so output is
# byte-identical across the two scripts. Replaces 12 prior
# `numfmt --to=iec-i --suffix=B` sites (numfmt is provided by coreutils
# in Debian/Ubuntu but is missing in minimal containers and was an
# undeclared dependency).
_format_size() {
    local bytes="${1:-0}"
    if (( bytes >= 1073741824 )); then
        local whole=$(( bytes / 1073741824 ))
        local frac=$(( (bytes % 1073741824) * 10 / 1073741824 ))
        printf "%d.%d GiB" "$whole" "$frac"
    elif (( bytes >= 1048576 )); then
        local whole=$(( bytes / 1048576 ))
        local frac=$(( (bytes % 1048576) * 10 / 1048576 ))
        printf "%d.%d MiB" "$whole" "$frac"
    elif (( bytes >= 1024 )); then
        local whole=$(( bytes / 1024 ))
        local frac=$(( (bytes % 1024) * 10 / 1024 ))
        printf "%d.%d KiB" "$whole" "$frac"
    else
        printf "%d B" "$bytes"
    fi
}

# ── Configuration ────────────────────────────────────────────────────────────

resolve_backup_path() {
    # Cascade: --backup-path CLI > vmbackup.conf (instance-aware)
    if [[ -n "${BACKUP_PATH_CLI:-}" ]]; then
        echo "$BACKUP_PATH_CLI"
        return
    fi
    # UNI-012: Determine config instance and config file via lib helpers.
    local instance
    instance=$(resolve_config_instance "${OPT_CONFIG_INSTANCE:-}" "${VMBACKUP_INSTANCE:-}")
    local conf
    # R1: get_config_file validates the instance name; a non-zero return means
    # an invalid/unsafe --config-instance and must be a hard failure (it would
    # otherwise feed an attacker-influenced path to the resolver below).
    if ! conf=$(get_config_file "$instance"); then
        die "Invalid config instance '$instance' (allowed: letters, digits, . _ - ; no path separators)" "resolve_backup_path" "$EXIT_CONFIG"
    fi
    if [[ ! -f "$conf" && "$instance" != "default" ]]; then
        die "Config instance '$instance' not found: $conf" "resolve_backup_path" "$EXIT_CONFIG"
    fi
    if [[ -f "$conf" ]]; then
        # VMR3/X1: resolve BACKUP_PATH with the same shell semantics vmbackup
        # uses (it sources the conf), not a regex scrape, so variable refs and
        # spaces agree between backup and restore. $conf is the validated,
        # in-tree conf (see get_config_file above).
        local val
        val=$(resolve_backup_path_shell "$conf" 2>/dev/null || true)
        if [[ -n "$val" ]]; then
            echo "$val"
            return
        fi
    fi
    die "Cannot resolve backup path. Use --backup-path or install vmbackup with a configured BACKUP_PATH in /opt/vmbackup/config/${instance}/vmbackup.conf" "resolve_backup_path" "$EXIT_CONFIG"
}

# ── Pre-flight Free Space Check ─────────────────────────────────────────────
# Sum the backup data files that will be restored and compare against
# available space on the destination filesystem. This prevents
# virtnbdrestore from silently producing truncated output on ENOSPC
# (upstream bug: virtnbdrestore exits 0 even when writes fail).
#
# Args: data_dir restore_path backup_type [until_checkpoint]
# Returns: 0 if OK, dies if insufficient space

preflight_free_space() {
    local data_dir="$1" restore_path="$2" btype="$3" until_cp="${4:-}"

    # Sum source data files (bytes)
    local total_bytes=0
    local file_count=0
    local f

    case "$btype" in
        incremental)
            # Include full + incrementals up to --until checkpoint
            while IFS= read -r -d '' f; do
                local basename
                basename=$(basename "$f")
                # If --until is set, skip files beyond that checkpoint
                if [[ -n "$until_cp" ]]; then
                    local cp_name
                    # Extract checkpoint name: e.g. sda.inc.virtnbdbackup.3.data → virtnbdbackup.3
                    cp_name=$(echo "$basename" | grep -oP 'virtnbdbackup\.\d+' || true)
                    local until_num cp_num
                    until_num=${until_cp##*.}
                    cp_num=${cp_name##*.}
                    if [[ -n "$cp_num" && -n "$until_num" ]] && (( cp_num > until_num )); then
                        continue
                    fi
                fi
                local fsize
                fsize=$(stat -c '%s' "$f" 2>/dev/null || echo 0)
                total_bytes=$(( total_bytes + fsize ))
                ((file_count++))
            done < <(find "$data_dir" -maxdepth 1 -type f \( \
                -name "*.full.data" -o -name "*.inc.virtnbdbackup.*.data" \
            \) -print0 2>/dev/null)
            ;;
        full)
            while IFS= read -r -d '' f; do
                local fsize
                fsize=$(stat -c '%s' "$f" 2>/dev/null || echo 0)
                total_bytes=$(( total_bytes + fsize ))
                ((file_count++))
            done < <(find "$data_dir" -maxdepth 1 -type f -name "*.full.data" -print0 2>/dev/null)
            ;;
        copy)
            while IFS= read -r -d '' f; do
                local fsize
                fsize=$(stat -c '%s' "$f" 2>/dev/null || echo 0)
                total_bytes=$(( total_bytes + fsize ))
                ((file_count++))
            done < <(find "$data_dir" -maxdepth 1 -type f -name "*.copy.data" -print0 2>/dev/null)
            ;;
    esac

    if (( file_count == 0 )); then
        log_warn "vmrestore.sh" "preflight_free_space" "No data files found to estimate size — skipping space check"
        return 0
    fi

    # Get available space on the destination filesystem
    # Use the parent dir if restore_path doesn't exist yet
    local check_path="$restore_path"
    while [[ ! -d "$check_path" && "$check_path" != "/" ]]; do
        check_path=$(dirname "$check_path")
    done
    local avail_bytes
    avail_bytes=$(df --output=avail -B1 "$check_path" 2>/dev/null | tail -1 | tr -d ' ')

    if [[ -z "$avail_bytes" || "$avail_bytes" == "0" ]]; then
        log_warn "vmrestore.sh" "preflight_free_space" "Cannot determine free space on $check_path — skipping check"
        return 0
    fi

    # Human-readable sizes
    local total_hr avail_hr
    total_hr=$(_format_size "$total_bytes")
    avail_hr=$(_format_size "$avail_bytes")

    log_info "vmrestore.sh" "preflight_free_space" "Backup data: $total_hr ($file_count files) — Destination free: $avail_hr ($check_path)"

    if (( total_bytes > avail_bytes )); then
        die "Insufficient space: restore needs $total_hr but only $avail_hr available on $check_path" "preflight_free_space" "$EXIT_STORAGE"
    fi

    # Warn if less than 10% headroom (restored qcow2 can be larger than raw data)
    local headroom=$(( avail_bytes - total_bytes ))
    local ten_pct=$(( total_bytes / 10 ))
    if (( headroom < ten_pct )); then
        log_warn "vmrestore.sh" "preflight_free_space" "Tight on space: only $(_format_size "$headroom") headroom after restore"
    fi
}

# ── Backup Detection ────────────────────────────────────────────────────────

has_backup_data() {
    # Avoid `find ... | grep -q .`: with `set -o pipefail`, grep closes the
    # pipe after the first match and find dies with SIGPIPE (141), making
    # the pipeline fail for any directory large enough that find has not
    # yet finished writing — symptom observed on W22 with 138 .data files
    # ("No backup data files in: <dir>" despite files being present).
    # `-print -quit` makes find exit cleanly after the first match.
    local _hit
    _hit=$(find "$1" -maxdepth 1 -type f \( \
        -name "*.full.data" -o \
        -name "*.inc.virtnbdbackup.*.data" -o \
        -name "*.copy.data" \
    \) -print -quit 2>/dev/null)
    [[ -n "$_hit" ]]
}

detect_backup_type() {
    local dir="$1"
    [[ -d "$dir" ]] || { echo "unknown"; return; }

    local has_inc=false has_full=false has_copy=false
    local f
    while IFS= read -r -d '' f; do
        case "$f" in
            *.inc.virtnbdbackup.*.data) has_inc=true ;;
            *.full.data) has_full=true ;;
            *.copy.data) has_copy=true ;;
        esac
    done < <(find "$dir" -maxdepth 1 -type f -name '*.data' -print0 2>/dev/null)

    if $has_inc;   then echo "incremental"
    elif $has_full; then echo "full"
    elif $has_copy; then echo "copy"
    else                 echo "unknown"
    fi
}

is_accumulate() { has_backup_data "$1"; }

# ── Path Resolution ─────────────────────────────────────────────────────────

# List period subdirectories, newest first, sorted by actual *.data recency (see body).
# Delegates enumeration to lib/period.sh. Periodic-only by design — accumulate VMs are
# handled upstream by is_accumulate() / has_backup_data() and never reach this function.
list_periods() {
    local vm_dir="$1"
    # "Newest first" by ACTUAL RECENCY (newest *.data mtime within each period),
    # NOT a lexical `sort -rV` on the period-id string. When rotation formats
    # coexist — daily YYYYMMDD / monthly YYYYMM alongside weekly YYYY-Www, e.g.
    # after a rotation-policy change or a normal rollover — version-sort ranks a
    # same-year numeric id ABOVE the weekly id (a digit run outranks the "-W"),
    # so `sort -rV` would elect a STALE numeric period over a newer weekly one and
    # restore-latest would silently resolve to a wrong (older) restore point.
    # Recency is format-agnostic and correct. Out-of-policy periods are left for
    # retention / orphan-retention to age out — the restore path must NOT depend
    # on their absence. Periods with no *.data get mtime 0 (sorted last); callers
    # that require data still skip them via has_backup_data().
    local _p _mt
    while IFS= read -r _p; do
        [[ -n "$_p" ]] || continue
        _mt=$(find "$vm_dir/$_p" -maxdepth 1 -type f -name '*.data' -printf '%T@\n' 2>/dev/null \
                | sort -rn | head -1)
        printf '%s %s\n' "${_mt:-0}" "$_p"
    done < <(get_vm_periods "$vm_dir") \
        | sort -k1,1nr -k2,2Vr \
        | awk '{print $2}'
}

# Resolve the directory containing .data files for a VM
# Accumulate: VM root. Period-based: specified or latest period.
# 118-spaces: resolve a VM's backup folder from its REAL libvirt name. Prefer the
# slug+hash token (vm_fs_name); fall back to the pre-118 legacy slug
# (vm_fs_name_legacy) for a folder that MIG-01 has not migrated yet (the rc3
# transition window). Echoes the existing dir (rc 0), or nothing (rc 1). -u-clean.
resolve_vm_backup_dir() {
    local real="${1:-}" base="${2:-}"
    local d tok
    if tok=$(vm_fs_name "$real" 2>/dev/null); then
        d="$base/$tok"
        [[ -d "$d" ]] && { printf '%s' "$d"; return 0; }
    fi
    d="$base/$(vm_fs_name_legacy "$real")"
    [[ -d "$d" ]] && { printf '%s' "$d"; return 0; }
    return 1
}

resolve_data_dir() {
    local vm_dir="$1" period="${2:-}"

    # Explicit period always wins (even for accumulate VMs with period subdirs)
    if [[ -n "$period" ]]; then
        local target="$vm_dir/$period"
        if [[ -d "$target" ]]; then
            echo "$target"
        else
            log_error "vmrestore.sh" "resolve_data_dir" "Period not found: $target"
            return 1
        fi
        return
    fi

    # No period specified.
    # FF-192: a VM switched from accumulate to periodic keeps its VM-root *.data
    # (rotation writes accumulate data at the root; migrate_layout renames folders
    # only — nothing migrates root data), so testing is_accumulate FIRST let the
    # stale root chain win forever and silently ignored newer period chains.
    # Resolve by DATA RECENCY instead (consistent with list_periods): find the
    # newest period that has data, then pick whichever of {VM root, that period}
    # carries the newest *.data. Warn when both coexist so --period can override.
    local _root_acc=false
    is_accumulate "$vm_dir" && _root_acc=true

    # Compute the recency-ordered period list ONCE (list_periods forks a find(1)
    # per period) and reuse it for the no-data fallback below.
    local -a _periods
    mapfile -t _periods < <(list_periods "$vm_dir")
    local _p _newest_period=""
    for _p in "${_periods[@]}"; do
        [[ -n "$_p" ]] || continue
        if has_backup_data "$vm_dir/$_p"; then
            _newest_period="$vm_dir/$_p"
            break
        fi
    done

    if [[ "$_root_acc" == true && -n "$_newest_period" ]]; then
        # Mixed layout — pick newest by *.data mtime, warn either way.
        local _root_mt _per_mt
        _root_mt=$(find "$vm_dir" -maxdepth 1 -type f -name '*.data' -printf '%T@\n' 2>/dev/null | sort -rn | head -1)
        _per_mt=$(find "$_newest_period" -maxdepth 1 -type f -name '*.data' -printf '%T@\n' 2>/dev/null | sort -rn | head -1)
        _root_mt="${_root_mt%%.*}"; _root_mt="${_root_mt:-0}"
        _per_mt="${_per_mt%%.*}"; _per_mt="${_per_mt:-0}"
        if (( _per_mt > _root_mt )); then
            log_warn "vmrestore.sh" "resolve_data_dir" "Mixed layout for '$(basename "$vm_dir")': VM-root accumulate data AND newer period data both present; restoring newest period '$(basename "$_newest_period")' (use --period to override)"
            echo "$_newest_period"
        else
            log_warn "vmrestore.sh" "resolve_data_dir" "Mixed layout for '$(basename "$vm_dir")': VM-root accumulate data AND period subdirectories both present; restoring newest VM-root data (use --period to select a period)"
            echo "$vm_dir"
        fi
        return
    fi

    # Pure accumulate — VM root.
    if [[ "$_root_acc" == true ]]; then
        echo "$vm_dir"
        return
    fi

    # Pure periodic — newest period with data.
    if [[ -n "$_newest_period" ]]; then
        echo "$_newest_period"
        return
    fi

    # Fallback: no period has data — return the newest dir anyway so the caller
    # gets a meaningful error path.
    if [[ -n "${_periods[0]:-}" ]]; then
        echo "$vm_dir/${_periods[0]}"
    else
        log_error "vmrestore.sh" "resolve_data_dir" "No period directories in: $vm_dir"
        return 1
    fi
}

# ── Listing ──────────────────────────────────────────────────────────────────

list_vms() {
    local backup_path="$1"

    echo ""
    echo "Available VMs in: $backup_path"
    echo "══════════════════════════════════════════════════════════════"

    # Presenter-scope counter (U1: reduce inside callback).
    _LIST_VMS_FOUND=0

    # Phase 4 (UNI-605 / USE-03): try once to open the vmbackup SQLite DB
    # so _list_vms_vm_cb can enrich each VM block with a chain-health
    # summary line. Memoised via _VMRESTORE_SQLITE_RO_INITED so a re-entrant
    # list_vms call does not re-init. Failure is silent — chain-health line
    # simply does not appear; the rest of --list output is unchanged from
    # the pre-Phase-4 walker contract (USE-03: nice-to-have, not required).
    if (( _VMRESTORE_SQLITE_RO_INITED == 0 )) && declare -f sqlite_init_readonly >/dev/null 2>&1; then
        _VMRESTORE_SQLITE_RO_INITED=1
        # Export BACKUP_PATH so sqlite_init_readonly can locate _state/.
        BACKUP_PATH="$backup_path"
        if sqlite_init_readonly 2>/dev/null; then
            # Schema-version sanity check (Q-5 disposition). The DB may
            # have been written by a newer vmbackup; if so, log a warning
            # and continue without enrichment rather than risk garbage
            # output from a missing/renamed column.
            local _db_schema
            _db_schema=$(sqlite_get_schema_version 2>/dev/null)
            local _expected_major="2"
            if [[ -n "$_db_schema" && "${_db_schema%%.*}" == "$_expected_major" ]]; then
                _VMRESTORE_SQLITE_RO_OK=1
                # Pre-fetch the per-VM chain-health summary once so the
                # row callback is O(1) per VM (no per-VM sqlite3 fork).
                declare -gA _VMRESTORE_CH_ACTIVE=()
                declare -gA _VMRESTORE_CH_BROKEN=()
                declare -gA _VMRESTORE_CH_LAST=()
                local _ch_line _ch_vm _ch_active _ch_archived _ch_purged _ch_chk _ch_rest _ch_brk _ch_sz _ch_first _ch_last
                while IFS='|' read -r _ch_vm _ch_active _ch_archived _ch_purged _ch_chk _ch_rest _ch_brk _ch_sz _ch_first _ch_last; do
                    [[ -z "$_ch_vm" || "$_ch_vm" == "vm_name" ]] && continue
                    # FF-165: the walker keys the row callback by the on-disk folder
                    # TOKEN (vm_fs_name slug+hash for any spaced/special-char name),
                    # but chain_health.vm_name is the REAL libvirt name. Key the map
                    # by vm_fs_name(real) so the $vm lookup at the "Chains:" line hits
                    # for spaced VMs (vm_fs_name is identity for fs-safe names).
                    local _ch_key
                    _ch_key=$(vm_fs_name "$_ch_vm" 2>/dev/null) || _ch_key="$_ch_vm"
                    [[ -n "$_ch_key" ]] || _ch_key="$_ch_vm"
                    _VMRESTORE_CH_ACTIVE["$_ch_key"]="${_ch_active:-0}"
                    _VMRESTORE_CH_BROKEN["$_ch_key"]="${_ch_brk:-0}"
                    _VMRESTORE_CH_LAST["$_ch_key"]="${_ch_last:-(never)}"
                done < <(sqlite_query_chain_health "" pipe 2>/dev/null)
            else
                log_warn "vmrestore.sh" "list_vms" \
                    "DB schema $_db_schema unsupported by this vmrestore (expected major $_expected_major); chain-health enrichment disabled"
                SQLITE_MODULE_AVAILABLE=0
            fi
        fi
    fi

    # UNI-309: walk the backup tree via lib/backup_walker.sh. The walker
    # enforces VM-level skip-list (replaces the inline `[[ "$vm" == _state ]] &&
    # continue` rule and adds `_*|.*` convergence with vmbackup). Period-level
    # skip convergence in vmrestore is handled inside _list_vms_vm_cb via
    # list_periods() → lib/period.sh, so we always return non-zero from
    # vm_cb to suppress walker period iteration (the gather-then-print
    # presenter shape has no clean streaming finalize hook in the skeletal
    # walker, D2).
    walk_backup_tree "$backup_path" _list_vms_vm_cb _list_vms_period_cb_unused

    echo ""
    echo "══════════════════════════════════════════════════════════════"
    (( _LIST_VMS_FOUND == 0 )) && { log_warn "vmrestore.sh" "list_vms" "No backups found in $backup_path"; return 1; }
    return 0
}

# Walker callback: present a single VM's restore-inventory block. Always
# returns non-zero so walk_backup_tree skips period iteration (handled
# internally via list_periods).
_list_vms_vm_cb() {
    local vm="$1"
    local vm_dir="$2"

    local data_dir=""
    local periods=()
    local is_acc=false

    # FF-192 (twin of resolve_data_dir): enumerate periods even when the VM root
    # carries accumulate *.data, so a mixed layout (an accumulate→periodic switch
    # leaves stale VM-root data behind) still surfaces the newer periods here
    # instead of hiding them, and the displayed source tracks resolve_data_dir's
    # data-recency choice.
    local _root_acc=false
    is_accumulate "$vm_dir" && _root_acc=true
    mapfile -t periods < <(list_periods "$vm_dir")

    # FF-164: pick the FIRST period that has data from the recency-sorted
    # list_periods output — NOT a `stat -c %Y` max over period DIRECTORY mtimes,
    # which any later write in an OLDER period (retention prune / archive collapse
    # / checkpoint cleanup) skews, making --list render from the wrong period.
    local _newest_period="" _p
    for _p in "${periods[@]}"; do
        [[ -n "$_p" ]] || continue
        if has_backup_data "$vm_dir/$_p"; then
            _newest_period="$vm_dir/$_p"
            break
        fi
    done

    if [[ "$_root_acc" == true && -n "$_newest_period" ]]; then
        # Mixed layout: choose newest by *.data mtime (matches resolve_data_dir),
        # keep periods populated so the "Periods:" line still lists them.
        local _root_mt _per_mt
        _root_mt=$(find "$vm_dir" -maxdepth 1 -type f -name '*.data' -printf '%T@\n' 2>/dev/null | sort -rn | head -1)
        _per_mt=$(find "$_newest_period" -maxdepth 1 -type f -name '*.data' -printf '%T@\n' 2>/dev/null | sort -rn | head -1)
        _root_mt="${_root_mt%%.*}"; _root_mt="${_root_mt:-0}"
        _per_mt="${_per_mt%%.*}"; _per_mt="${_per_mt:-0}"
        if (( _per_mt > _root_mt )); then
            data_dir="$_newest_period"
        else
            data_dir="$vm_dir"
            is_acc=true
        fi
    elif [[ "$_root_acc" == true ]]; then
        # Pure accumulate — VM root; no data-bearing periods to list.
        data_dir="$vm_dir"
        is_acc=true
        periods=()
    elif [[ -n "$_newest_period" ]]; then
        # Pure periodic — newest period with data.
        data_dir="$_newest_period"
    elif [[ ${#periods[@]} -gt 0 ]]; then
        # Periods exist but none has data yet (rotation before first backup);
        # show the newest so the block still renders (unchanged from prior).
        data_dir="$vm_dir/${periods[0]:-}"
    else
        return 1
    fi

    local btype size tpm_tag="" disk_tag=""
    btype=$(detect_backup_type "$data_dir")
    size=$(du -sh "$vm_dir" 2>/dev/null | awk '{print $1}')
    [[ -f "$data_dir/.tpm-backup-marker" ]] && tpm_tag=" [TPM]"

    # Show disk tags only for multi-disk VMs (latest CP's disks, not union)
    local disks _latest_cp
    _latest_cp=$(find "$data_dir/checkpoints" -name "virtnbdbackup.*.xml" 2>/dev/null \
        | sed -n 's/.*virtnbdbackup\.\([0-9]*\)\.xml/\1/p' \
        | sort -n | tail -1)
    if [[ -n "$_latest_cp" ]]; then
        # TODO(phase6/UNI-013): swap to lib/libvirt.sh
        disks=$(enumerate_disks_at_checkpoint "$data_dir" "$_latest_cp")
    else
        # TODO(phase6/UNI-013): swap to lib/libvirt.sh
        disks=$(enumerate_disks "$data_dir")
    fi
    [[ "$disks" == *,* ]] && disk_tag=" [$disks]"

    # Count archives across all period dirs
    local archive_count=0
    local -A _seen_adirs=()
    local -a _archive_search=("$vm_dir/.archives")
    if [[ ${#periods[@]} -gt 0 ]]; then
        local _p
        for _p in "${periods[@]}"; do
            _archive_search+=("$vm_dir/$_p/.archives")
        done
    else
        _archive_search+=("$data_dir/.archives")
    fi
    local adir
    for adir in "${_archive_search[@]}"; do
        [[ -d "$adir" ]] || continue
        local _real
        _real=$(pu_normalise_path "$adir")
        [[ -n "${_seen_adirs[$_real]:-}" ]] && continue
        _seen_adirs[$_real]=1
        archive_count=$(( archive_count + $(find "$adir" -maxdepth 1 -type d -name "chain-*" 2>/dev/null | wc -l) ))
    done

    # Count restore points across ALL active periods
    local rpoints=0
    if $is_acc; then
        if [[ -d "$data_dir/checkpoints" ]]; then
            rpoints=$(find "$data_dir/checkpoints" -name "virtnbdbackup.*.xml" 2>/dev/null | wc -l)
        fi
        [[ "$btype" =~ ^(full|copy)$ && $rpoints -eq 0 ]] && rpoints=1
    else
        local _p
        for _p in "${periods[@]}"; do
            local _pdir="$vm_dir/$_p"
            if [[ -d "$_pdir/checkpoints" ]]; then
                rpoints=$(( rpoints + $(find "$_pdir/checkpoints" -name "virtnbdbackup.*.xml" 2>/dev/null | wc -l) ))
            else
                local _ptype
                _ptype=$(detect_backup_type "$_pdir")
                [[ "$_ptype" =~ ^(full|copy)$ ]] && ((rpoints++))
            fi
        done
    fi

    # Build detail line with proper pluralisation
    local p_word="points"; (( rpoints == 1 )) && p_word="point"
    local detail="$btype · $rpoints $p_word"
    if (( archive_count > 0 )); then
        local a_word="archives"; (( archive_count == 1 )) && a_word="archive"
        detail+=" · $archive_count $a_word"
    fi
    detail+="$disk_tag$tpm_tag"

    printf "\n  %-53s %6s\n" "$vm" "$size"
    printf "    %s\n" "$detail"

    if [[ ${#periods[@]} -gt 0 ]]; then
        printf "    Periods: %s\n" "${periods[*]}"
    fi

    # Phase 4 (UNI-605 / USE-03): chain-health enrichment line when DB
    # opened successfully in list_vms(). Format (Q-2 disposition):
    #   "    Chains: <N> active, <N> broken, last backup <ISO timestamp>"
    # Silent skip when DB unavailable or this VM has no DB rows —
    # preserves the v0.5.4 standalone contract (vmrestore must work on
    # any host with just the backup tree mounted).
    if (( _VMRESTORE_SQLITE_RO_OK == 1 )) && [[ -n "${_VMRESTORE_CH_LAST[$vm]:-}" ]]; then
        printf "    Chains: %d active, %d broken, last backup %s\n" \
            "${_VMRESTORE_CH_ACTIVE[$vm]:-0}" \
            "${_VMRESTORE_CH_BROKEN[$vm]:-0}" \
            "${_VMRESTORE_CH_LAST[$vm]:-(never)}"
    fi

    (( _LIST_VMS_FOUND++ )) || true
    # Suppress walker period iteration; periods are handled internally above.
    return 1
}

# Walker period-callback placeholder. Never invoked because
# _list_vms_vm_cb always returns non-zero (skip period iteration). Defined
# only so walk_backup_tree's invocation contract is satisfied.
_list_vms_period_cb_unused() {
    return 0
}

# ── Disk Enumeration ─────────────────────────────────────────────────────────
# Scan a backup directory for unique device target names from .data files.
# Returns a sorted, comma-separated list (e.g., "sda, vda, vdb").
# Used by: --list-restore-points display, --disk validation, disk-restore mode.

enumerate_disks() {
    local data_dir="$1"
    local -A seen=();
    local f fname dev
    while IFS= read -r -d '' f; do
        fname=$(basename "$f")
        dev=""
        case "$fname" in
            *.full.data)                dev="${fname%.full.data}" ;;
            *.inc.virtnbdbackup.*.data) dev="${fname%%.*}" ;;
            *.copy.data)                dev="${fname%.copy.data}" ;;
        esac
        [[ -n "$dev" ]] && seen[$dev]=1
    done < <(find "$data_dir" -maxdepth 1 -type f -name '*.data' -print0 2>/dev/null)
    # Return sorted, comma-separated
    printf '%s\n' "${!seen[@]}" | sort | paste -sd, | sed 's/,/, /g'
}

# Return sorted, comma-separated list of disks at a specific checkpoint number.
# Parses .data filenames: {dev}.full.data (CP 0), {dev}.inc.virtnbdbackup.{N}.data,
# {dev}.copy.data (CP 0). Used by: --list-restore-points, PIT staging trigger, --disk validation.
enumerate_disks_at_checkpoint() {
    local data_dir="$1" cp_num="$2"
    local -A seen=()
    local f fname dev
    while IFS= read -r -d '' f; do
        fname=$(basename "$f")
        dev=""
        case "$fname" in
            *.full.data)
                [[ "$cp_num" == "0" ]] && dev="${fname%.full.data}" ;;
            *.inc.virtnbdbackup."${cp_num}".data)
                dev="${fname%%.*}" ;;
            *.copy.data)
                [[ "$cp_num" == "0" ]] && dev="${fname%.copy.data}" ;;
        esac
        [[ -n "$dev" ]] && seen[$dev]=1
    done < <(find "$data_dir" -maxdepth 1 -type f -name '*.data' -print0 2>/dev/null)
    printf '%s\n' "${!seen[@]}" | sort | paste -sd, | sed 's/,/, /g'
}

# ── Chain Completeness Check (INT-16) ────────────────────────────────────────
# Validates that a chain directory contains all expected data files for every
# disk declared in its checkpoint metadata, and that each *.data has a sibling
# *.qcow.json. A chain produced by an interrupted/overlapping backup may have
# (e.g.) only vdb.copy.data while the checkpoint metadata declares both vda
# and vdb — restoring such a chain produces a partial/inconsistent VM.
#
# Sets global _CHAIN_INCOMPLETE_REASON on incomplete result (caller may log it).
# Args:    $1 = chain dir
# Returns: 0 = complete, 1 = incomplete (reason in _CHAIN_INCOMPLETE_REASON),
#          2 = empty/undeterminable (no data and no metadata)
_CHAIN_INCOMPLETE_REASON=""
chain_check_complete() {
    local chain_dir="$1"
    _CHAIN_INCOMPLETE_REASON=""
    [[ -d "$chain_dir" ]] || { _CHAIN_INCOMPLETE_REASON="missing dir"; return 1; }

    # 1) Every *.data must have a sibling qcow.json. Active-chain layout uses
    #    <dev>.virtnbdbackup.<N>.qcow.json (0 = full, N = inc), archived-chain
    #    layout uses <dev>.copy.qcow.json. Older layouts may use a bare
    #    <dev>.qcow.json; accept that as a last-ditch fallback.
    local f base dev kind cpnum sidecar1 sidecar2 sidecar3
    local -a missing_json=()
    while IFS= read -r -d '' f; do
        base=$(basename "$f")
        sidecar1=""; sidecar2=""; sidecar3=""
        case "$base" in
            *.full.data)
                dev="${base%.full.data}"; kind="full"
                sidecar1="$chain_dir/${dev}.virtnbdbackup.0.qcow.json"
                sidecar2="$chain_dir/${dev}.qcow.json" ;;
            *.copy.data)
                # Legacy "offline copy" chains (older code paths) shipped only
                # <dev>.copy.data with no qcow.json sidecar; the file is still
                # restorable via plain copy. Newer copy chains do include the
                # sidecar. Tolerate either: don't enforce a sidecar for copy.
                continue ;;
            *.inc.virtnbdbackup.*.data)
                dev="${base%%.*}"
                cpnum="${base%.data}"; cpnum="${cpnum##*.virtnbdbackup.}"
                kind="inc${cpnum}"
                sidecar1="$chain_dir/${dev}.virtnbdbackup.${cpnum}.qcow.json"
                sidecar2="$chain_dir/${dev}.qcow.json" ;;
            *) continue ;;
        esac
        if [[ ! -f "$sidecar1" ]] && [[ -z "$sidecar2" || ! -f "$sidecar2" ]] \
               && [[ -z "$sidecar3" || ! -f "$sidecar3" ]]; then
            missing_json+=("${dev}.${kind}")
        fi
    done < <(find "$chain_dir" -maxdepth 1 -type f -name '*.data' -print0 2>/dev/null)

    # 2) Each checkpoint metadata must have a data file per declared disk.
    #
    # INT-16 phantom-disk filter: libvirt domain checkpoint XML enumerates
    # every block device on the domain — including CD-ROMs and other devices
    # virtnbdbackup deliberately excludes from backup. Such disks appear
    # under <disk name='...'> in the checkpoint XML but never receive a
    # .data or .qcow.json sidecar. Without this filter every VM with a
    # CD-ROM (i.e. essentially every Windows VM) is permanently reported
    # INCOMPLETE, training operators to reflexively reach for
    # --include-incomplete and defeating the guard.
    #
    # The checkpoint XML itself does not carry the device='disk|cdrom'
    # attribute (only target name + bitmap). The per-checkpoint
    # vmconfig.virtnbdbackup.N.xml snapshot does — it's the full libvirt
    # domain XML at backup time. Parse that to build the per-checkpoint
    # set of non-disk devices (cdrom, floppy, …) to exclude from the
    # "should have a .data file" check.

    local cpdir="$chain_dir/checkpoints"
    local -a missing_data=()
    local have_meta=0
    if [[ -d "$cpdir" ]]; then
        local cpfile cpname declared d vmcfg
        local -A _phantoms
        while IFS= read -r -d '' cpfile; do
            have_meta=1
            cpname=$(basename "$cpfile" .xml)              # virtnbdbackup.N
            cpnum="${cpname##*.}"
            declared=$(grep -oE "<disk name='[^']+'" "$cpfile" 2>/dev/null \
                | sed "s/<disk name='//;s/'\$//" | sort -u)

            # Build phantom set for THIS checkpoint from its vmconfig snapshot.
            # awk extracts target dev= of every <disk> whose device= attr is
            # not 'disk' (so cdrom/floppy/lun all get excluded). Tolerates
            # single or double quotes and attributes in any order on the
            # <disk> tag. Missing vmconfig → empty phantom set (no filter).
            _phantoms=()
            vmcfg="$chain_dir/vmconfig.virtnbdbackup.${cpnum}.xml"
            if [[ -f "$vmcfg" ]]; then
                local _ph
                while IFS= read -r _ph; do
                    [[ -n "$_ph" ]] && _phantoms["$_ph"]=1
                done < <(awk '
                    /<disk[[:space:]]/ {
                        dev=""; tgt=""
                        # device attribute
                        if (match($0, /device=['\''"][^'\''"]+['\''"]/)) {
                            dev = substr($0, RSTART+8, RLENGTH-9)
                            # strip leading/trailing quote
                            gsub(/^['\''"]|['\''"]$/, "", dev)
                        }
                        in_disk = 1
                        if (dev != "" && dev != "disk") want_target = 1
                        else want_target = 0
                        next
                    }
                    in_disk && want_target && /<target[[:space:]]/ {
                        if (match($0, /dev=['\''"][^'\''"]+['\''"]/)) {
                            tgt = substr($0, RSTART+5, RLENGTH-6)
                            gsub(/^['\''"]|['\''"]$/, "", tgt)
                            print tgt
                        }
                        want_target = 0
                    }
                    /<\/disk>/ { in_disk = 0; want_target = 0 }
                ' "$vmcfg" 2>/dev/null)
            fi

            for d in $declared; do
                # Skip non-disk devices (CD-ROM, floppy, lun, …) as recorded
                # by THIS checkpoint's vmconfig snapshot.
                [[ -n "${_phantoms[$d]+x}" ]] && continue
                if [[ "$cpnum" == "0" ]]; then
                    # CP0 may be satisfied by full.data, copy.data, or — for an
                    # archived chain where the original full was collapsed —
                    # by the chain's single copy.data.
                    if [[ ! -f "$chain_dir/${d}.full.data" ]] \
                           && [[ ! -f "$chain_dir/${d}.copy.data" ]]; then
                        missing_data+=("CP${cpnum}:${d}")
                    fi
                else
                    # For an archived/collapsed chain, the original inc segments
                    # are gone but a *.copy.data covers the whole chain.
                    if [[ ! -f "$chain_dir/${d}.inc.virtnbdbackup.${cpnum}.data" ]] \
                           && [[ ! -f "$chain_dir/${d}.copy.data" ]]; then
                        missing_data+=("CP${cpnum}:${d}")
                    fi
                fi
            done
        done < <(find "$cpdir" -maxdepth 1 -type f -name 'virtnbdbackup.*.xml' -print0 2>/dev/null)
    fi

    local data_count
    data_count=$(find "$chain_dir" -maxdepth 1 -type f -name '*.data' 2>/dev/null | wc -l)
    if [[ "$have_meta" -eq 0 && "$data_count" -eq 0 ]]; then
        _CHAIN_INCOMPLETE_REASON="empty (no data, no metadata)"
        return 2
    fi

    local -a problems=()
    if [[ ${#missing_data[@]} -gt 0 ]]; then
        problems+=("missing data: ${missing_data[*]}")
    fi
    if [[ ${#missing_json[@]} -gt 0 ]]; then
        problems+=("missing qcow.json: ${missing_json[*]}")
    fi

    if [[ ${#problems[@]} -gt 0 ]]; then
        _CHAIN_INCOMPLETE_REASON="$(IFS='; '; echo "${problems[*]}")"
        return 1
    fi
    return 0
}

# ── PIT Staging Directory ────────────────────────────────────────────────────
# When a point-in-time restore targets a checkpoint whose disk set differs from
# the latest checkpoint, virtnbdrestore picks the wrong vmconfig (always latest).
# create_pit_staging() builds a temp directory with:
#   - Symlinks to all .data files and checkpoints/ dir from the backup
#   - A copy of the target checkpoint's vmconfig (the ONLY vmconfig present)
# virtnbdrestore's lib.getLatest() then finds only the correct config.
#
# Returns: the staging directory path via stdout.
# Caller must call cleanup_pit_staging() when done.

create_pit_staging() {
    local data_dir="$1" target_cp="$2"
    local staging=""

    # Prefer TMPDIR, fall back to a subdir of the backup parent.
    # FF-166: rc-check mktemp -d. On failure staging='' and the ln/cp below would
    # symlink backup .data files and copy vmconfig into '/' as root, then return 0
    # (defeating the caller's `|| die`). Fail closed instead.
    if [[ -d "${TMPDIR:-/tmp}" && -w "${TMPDIR:-/tmp}" ]]; then
        staging=$(mktemp -d "${TMPDIR:-/tmp}/vmrestore-pit-XXXXXX") || staging=""
    else
        staging=$(mktemp -d "$(dirname "$data_dir")/.vmrestore-pit-XXXXXX") || staging=""
    fi
    if [[ -z "$staging" || ! -d "$staging" ]]; then
        log_error "vmrestore.sh" "create_pit_staging" "Could not create PIT staging directory (mktemp failed) — refusing to stage into '/'"
        return 1
    fi

    # UNI-010 / FF-193: the EXIT/SIGINT trap consumes _VMRESTORE_STAGING_DIRS from
    # the PARENT shell, but create_pit_staging runs inside the caller's $() command
    # substitution — an append here lands in the subshell and never reaches the
    # trap. The two call sites append $pit_input_dir after capture (mirroring the
    # clone-staging append), so staging tracking is done there, not here.

    # Symlink .data files
    local f
    while IFS= read -r -d '' f; do
        ln -s "$f" "$staging/$(basename "$f")"
    done < <(find "$data_dir" -maxdepth 1 -type f -name '*.data' -print0 2>/dev/null)

    # Symlink checkpoints directory
    [[ -d "$data_dir/checkpoints" ]] && ln -s "$data_dir/checkpoints" "$staging/checkpoints"

    # Copy the target checkpoint's vmconfig — the ONLY vmconfig in staging
    local target_vmconfig="$data_dir/vmconfig.virtnbdbackup.${target_cp}.xml"
    if [[ -f "$target_vmconfig" ]]; then
        cp "$target_vmconfig" "$staging/"
    else
        # FF-194: the per-checkpoint vmconfig.virtnbdbackup.<cp>.xml is the only
        # domain XML that reliably describes THIS checkpoint's disk topology. The
        # former fallback picked a substitute from config/*.xml by checkpoint
        # ORDINAL (`sort | sed -n "$((cp+1))p"`), assuming config files are one
        # per checkpoint, oldest-first. That contract is false: vmbackup's
        # backup_vm_config writes change-triggered ${vm}_config_<ts>.xml plus a
        # ${vm}_config_<YYYYMM>_FIRST marker ('_FIRST' sorts AFTER same-month
        # timestamps), so the ordinal selects an arbitrary, wrong-vintage XML.
        # PIT staging is entered precisely BECAUSE this checkpoint's disk set
        # differs from latest, so a wrong-topology config would hand
        # virtnbdrestore a mismatched disk set — a silent wrong/partial restore.
        # No sound checkpoint->config map exists here, so fail closed.
        log_error "vmrestore.sh" "create_pit_staging" "No per-checkpoint vmconfig (vmconfig.virtnbdbackup.${target_cp}.xml) for checkpoint ${target_cp}; refusing to guess a substitute domain XML from config/ (topology may differ)"
        rm -rf "$staging"
        return 1
    fi

    echo "$staging"
}

cleanup_pit_staging() {
    local staging_dir="${1:-}"
    if [[ -n "$staging_dir" && -d "$staging_dir" && "$staging_dir" == *vmrestore-pit-* ]]; then
        rm -rf "$staging_dir"
    fi
}

# P1: scan a chain dir's *.data files ONCE and group disks by checkpoint number,
# so show_restore_points() can look a checkpoint's disk set up from memory
# instead of calling enumerate_disks_at_checkpoint() (a fresh find over the whole
# dir) once PER checkpoint. That per-checkpoint rescan is the O(checkpoints^2)
# cost behind a slow `--list-restore-points` on busy VMs (~102s measured for a
# 110-checkpoint VM). Populates the global _RP_DISKS_BY_CP: cp_num -> the same
# sorted, comma-separated disk string enumerate_disks_at_checkpoint() returns,
# so the listing output is byte-identical.
#
# This is a PRIVATE helper for show_restore_points() only. The shared
# enumerate_disks_at_checkpoint() (11 call sites incl. PIT staging, --disk
# validation, latest-cp detection) is deliberately left untouched.
#
# Reset at the top of every call: show_restore_points() runs once per period AND
# once per archived chain, so a stale map must never leak between chains.
declare -gA _RP_DISKS_BY_CP=()
_rp_index_disks_by_cp() {
    local data_dir="$1"
    _RP_DISKS_BY_CP=()
    local f fname dev cp
    local -A _seen=()
    while IFS= read -r -d '' f; do
        fname=$(basename "$f")
        dev=""; cp=""
        case "$fname" in
            # CP0 is satisfied by either the full base or an offline copy
            # (mirrors enumerate_disks_at_checkpoint's cp_num==0 handling).
            *.full.data)                dev="${fname%.full.data}"; cp="0" ;;
            *.copy.data)                dev="${fname%.copy.data}"; cp="0" ;;
            *.inc.virtnbdbackup.*.data) dev="${fname%%.*}"; cp="${fname%.data}"; cp="${cp##*.virtnbdbackup.}" ;;
        esac
        [[ -z "$dev" || -z "$cp" ]] && continue
        [[ -n "${_seen[$cp:$dev]+x}" ]] && continue
        _seen[$cp:$dev]=1
        if [[ -n "${_RP_DISKS_BY_CP[$cp]+x}" ]]; then
            _RP_DISKS_BY_CP[$cp]+=$'\n'"$dev"
        else
            _RP_DISKS_BY_CP[$cp]="$dev"
        fi
    done < <(find "$data_dir" -maxdepth 1 -type f -name '*.data' -print0 2>/dev/null)
    # Normalise each checkpoint's disk list to the exact "sorted, comma+space"
    # form enumerate_disks_at_checkpoint() emits.
    local k
    for k in "${!_RP_DISKS_BY_CP[@]}"; do
        _RP_DISKS_BY_CP[$k]=$(printf '%s\n' "${_RP_DISKS_BY_CP[$k]}" | sort | paste -sd, | sed 's/,/, /g')
    done
}

show_restore_points() {
    local data_dir="$1"
    local btype
    btype=$(detect_backup_type "$data_dir")

    echo "  Restore Point   Date                 Type            Disk(s)"
    echo "  ──────────────────────────────────────────────────────────────────────"

    local count=0
    case "$btype" in
        incremental)
            if [[ -d "$data_dir/checkpoints" ]]; then
                # P1: one scan up front, then O(1) lookups per checkpoint
                # instead of a fresh find per checkpoint.
                _rp_index_disks_by_cp "$data_dir"
                while IFS= read -r -d '' f; do
                    [[ -f "$f" ]] || continue
                    local name num ftime ptype disks
                    name=$(basename "$f" .xml)
                    num="${name##*.}"
                    ftime=$(stat -c '%y' "$f" 2>/dev/null | cut -d. -f1)
                    ptype="Incremental"
                    [[ "$num" == "0" ]] && ptype="FULL (base)"
                    disks="${_RP_DISKS_BY_CP[$num]:-}"
                    printf "  %-15s   %-19s  %-15s %s\n" "$num" "$ftime" "$ptype" "$disks"
                    ((count++))
                done < <(find "$data_dir/checkpoints" -maxdepth 1 -name "virtnbdbackup.*.xml" -print0 2>/dev/null | sort -zV)
            fi
            ;;
        full)
            local ff disks
            # Sort for deterministic pick across multi-disk VMs
            # (display-only; 109-bugs audit item 6a).
            ff=$(find "$data_dir" -maxdepth 1 -name "*.full.data" 2>/dev/null | sort | head -1)
            if [[ -n "$ff" ]]; then
                disks=$(enumerate_disks_at_checkpoint "$data_dir" "0")
                printf "  %-15s   %-19s  %-15s %s\n" "0" \
                    "$(stat -c '%y' "$ff" 2>/dev/null | cut -d. -f1)" "FULL (only)" "$disks"
                count=1
            fi
            ;;
        copy)
            local cf disks
            # Sort for deterministic pick across multi-disk VMs
            # (display-only; 109-bugs audit item 6a).
            cf=$(find "$data_dir" -maxdepth 1 -name "*.copy.data" 2>/dev/null | sort | head -1)
            if [[ -n "$cf" ]]; then
                disks=$(enumerate_disks_at_checkpoint "$data_dir" "0")
                printf "  %-15s   %-19s  %-15s %s\n" "0" \
                    "$(stat -c '%y' "$cf" 2>/dev/null | cut -d. -f1)" "COPY (offline)" "$disks"
                count=1
            fi
            ;;
    esac

    echo "  ──────────────────────────────────────────────────────────────────────"
    echo "  Total: $count"
    echo ""
}

# ── Storage Pool Refresh ─────────────────────────────────────────────────────
# Detect which libvirt storage pool (if any) contains the given directory
# and refresh it so newly-created volumes are visible to virt-manager.

refresh_storage_pool() {
    local target_dir="$1"
    local pool_name=""
    local best_len=0
    local pool
    while IFS= read -r pool; do
        [[ -z "$pool" ]] && continue
        local pool_path
        pool_path=$(virsh pool-dumpxml "$pool" 2>/dev/null | grep -oP '<path>\K[^<]+' || true)  # [LIBVIRT-KEEPER: filtered against backup-target prefix in caller — lv_pool_paths emits all]
        if [[ -n "$pool_path" && "$target_dir" == "$pool_path"* ]]; then
            # Prefer the most specific (longest) matching pool path
            if (( ${#pool_path} > best_len )); then
                best_len=${#pool_path}
                pool_name="$pool"
            fi
        fi
    done < <(virsh pool-list --name 2>/dev/null)  # [LIBVIRT-KEEPER: paired with KEEPER pool-dumpxml above for caller-side path matching]

    if [[ -n "$pool_name" ]]; then
        if lv_pool_refresh "$pool_name"; then
            log_info "vmrestore.sh" "restore_vm" "Refreshed storage pool '$pool_name'"
        else
            log_warn "vmrestore.sh" "restore_vm" "Failed to refresh storage pool '$pool_name'"
        fi
    fi
}

# ── Post-Restore Integrity Check (race-tolerant) ─────────────────────────────
# Classifies a single `qemu-img check` of a restored qcow2 into one of three
# verdicts so callers never collapse "structurally corrupt" and "could not be
# checked" into the same failure. The restore engine (virtnbdrestore) has just
# written and closed this qcow2, and its qcow2 "write" lock is not released
# instantaneously — a helper fd lingers ~1-2s. A `qemu-img check` fired in that
# same second loses the race and fails rc=1 with `Failed to get shared "write"
# lock / Is another process using the image?` — a verdict on the LOCK, not the
# image (reproduced 5/5 at product level; the image checks clean ~1-2s later).
# A bare `qemu-img check ... && ok || corrupt` would flag that perfectly good
# image as corrupt, so rc=1 with lock text is RETRIED (bounded) rather than
# trusted; only rc=2 is CORRUPT.
#
# Fail-closed contract: anything we cannot positively read as clean — internal
# errors, unexpected rcs, an exhausted lock-settle budget, or an empty path — is
# UNVERIFIABLE (return 1), never a silent PASS. The one deliberate exception is
# rc=63 (format does not support checking): "not checkable" is not evidence of
# corruption, so it PASSes with a logged warning rather than failing closed.
# rc=63 is unreachable for qcow2 restores (always checkable); the arm exists
# only to keep the contract honest for a non-qcow2 format.
#
# Sets global _QIMG_VERDICT ∈ {PASS,CORRUPT,UNVERIFIABLE} for the caller.
# Args:    $1 = qcow2 image path
# Returns: 0 = PASS (structurally sound), 1 = CORRUPT or UNVERIFIABLE
_qcow_check_classified() {
    local _f="${1:-}"
    local _out _rc _flat _last
    local _attempt=0
    local _max_attempts=8
    local _settle_secs=1
    local _cap=2000
    local _tailcap=200
    _QIMG_VERDICT=""

    # Fail closed on an empty path BEFORE probing: an argless future caller must
    # fail this check gracefully, not abort the whole restore binary (rc 127) on
    # a bare `local _f="$1"` under `set -uo pipefail`. Today's three callers all
    # pass proven-non-empty paths; this guards the contract, not those callers.
    if [[ -z "$_f" ]]; then
        log_error "vmrestore.sh" "_qcow_check_classified" "integrity check called with empty image path; integrity COULD NOT BE VERIFIED"
        _QIMG_VERDICT="UNVERIFIABLE"
        return 1
    fi

    while (( _attempt < _max_attempts )); do
        _attempt=$(( _attempt + 1 ))
        _out=$(qemu-img check "$_f" 2>&1)
        _rc=$?
        # Log-safe diagnostic: one bounded line. When truncating keep the head
        # (where the ERROR body starts) AND the final line, both capped, so
        # total stays bounded.
        _flat="${_out//$'\n'/ | }"
        if (( ${#_flat} > _cap )); then
            _last="${_out##*$'\n'}"
            (( ${#_last} > _tailcap )) && _last="...${_last: -_tailcap}"
            _flat="${_flat:0:$_cap} [output truncated: $(( ${#_flat} - _cap )) more chars] | tail: ${_last}"
        fi
        case "$_rc" in
            0)
                _QIMG_VERDICT="PASS"
                return 0
                ;;
            3)
                log_info "vmrestore.sh" "_qcow_check_classified" "Integrity OK with leaked clusters (non-fatal): $_f"
                _QIMG_VERDICT="PASS"
                return 0
                ;;
            63)
                log_warn "vmrestore.sh" "_qcow_check_classified" "Integrity check unsupported for this image format; structural verification skipped: $_f"
                _QIMG_VERDICT="PASS"
                return 0
                ;;
            2)
                log_error "vmrestore.sh" "_qcow_check_classified" "qemu-img check rc=2 CORRUPT: $_f :: $_flat"
                _QIMG_VERDICT="CORRUPT"
                return 1
                ;;
            1)
                if [[ "$_out" == *lock* || "$_out" == *"Is another process using the image"* ]]; then
                    log_info "vmrestore.sh" "_qcow_check_classified" "qemu-img write-lock contention (attempt $_attempt/$_max_attempts), settling: $_f :: $_flat"
                    (( _attempt < _max_attempts )) && sleep "$_settle_secs"
                    continue
                fi
                log_error "vmrestore.sh" "_qcow_check_classified" "qemu-img check rc=1 internal error, integrity UNVERIFIABLE: $_f :: $_flat"
                _QIMG_VERDICT="UNVERIFIABLE"
                return 1
                ;;
            *)
                log_error "vmrestore.sh" "_qcow_check_classified" "qemu-img check unexpected rc=$_rc, integrity UNVERIFIABLE: $_f :: $_flat"
                _QIMG_VERDICT="UNVERIFIABLE"
                return 1
                ;;
        esac
    done

    # Lock never released within the settle window (max_attempts-1 sleeps ×
    # settle_secs — the final attempt skips its sleep).
    log_error "vmrestore.sh" "_qcow_check_classified" "qemu-img write-lock did not release after ~$(( (_max_attempts - 1) * _settle_secs ))s (rc=1, lock); integrity COULD NOT BE VERIFIED: $_f :: $_flat"
    _QIMG_VERDICT="UNVERIFIABLE"
    return 1
}

# ── TPM Restore ──────────────────────────────────────────────────────────────

restore_tpm() {
    local vm_name="$1" data_dir="$2" dry_run="$3" uuid_override="${4:-}"
    local tpm_dir="$data_dir/tpm-state"

    # Archived chains may lack .tpm-backup-marker but still have tpm-state/
    if [[ ! -f "$data_dir/.tpm-backup-marker" && ! -d "$tpm_dir" ]]; then
        return 0
    fi
    if [[ ! -d "$tpm_dir" ]]; then
        log_warn "vmrestore.sh" "restore_tpm" "Marker present but no tpm-state/ in $data_dir"
        return 0
    fi

    # UUID: override (new-identity) > BACKUP_METADATA.txt > virsh
    local vm_uuid="$uuid_override"
    if [[ -z "$vm_uuid" ]]; then
        local metadata="$tpm_dir/BACKUP_METADATA.txt"
        if [[ -f "$metadata" ]]; then
            vm_uuid=$(grep -oP '^\s*VM UUID:\s+\K\S+' "$metadata" 2>/dev/null || true)
        fi
    fi
    # Fallback: virsh (only works if VM is already defined).
    # UNI-006: delegate to lib/tpm_io.sh::get_vm_uuid (functionally equivalent).
    if [[ -z "$vm_uuid" ]]; then
        vm_uuid=$(get_vm_uuid "$vm_name" || true)
    fi
    [[ -n "$vm_uuid" ]] || { log_error "vmrestore.sh" "restore_tpm" "Cannot determine UUID for TPM restore"; return 1; }

    local target="/var/lib/libvirt/swtpm/$vm_uuid"

    if [[ "$dry_run" == true ]]; then  # [DRY-RUN-KEEPER: local-param polarity preserved, see 109-phase7-spec.md §1.3.3]
        log_info "vmrestore.sh" "restore_tpm" "[DRY RUN] Would restore TPM: $tpm_dir → $target"
        return 0
    fi

    # FF-10: validate the SOURCE before mutating the target. A legacy flat
    # layout (tpm-state/ with no tpm2/ subdir) cannot be restored by the copy
    # block below; the pre-fix code moved the working state aside, created an
    # empty target and still logged success. Fail closed so the caller renders
    # ACTION-REQUIRED + "TPM ✗" and existing state is left untouched.
    if [[ ! -d "$tpm_dir/tpm2" ]]; then
        log_error "vmrestore.sh" "restore_tpm" \
            "TPM backup at $tpm_dir has no tpm2/ subdirectory (unsupported/legacy layout) — TPM NOT restored; existing state untouched"
        return 1
    fi

    # UNI-006: validate the source state before copying — guards against a
    # half-written backup (no tpm2* files, or empty tpm2* files) silently
    # restoring to a broken state. Runs BEFORE any target mutation (FF-10).
    if ! validate_tpm_backup "$tpm_dir"; then
        log_error "vmrestore.sh" "restore_tpm" \
            "TPM backup at $tpm_dir failed validation (missing or empty tpm2* files)"
        return 1
    fi

    log_info "vmrestore.sh" "restore_tpm" "Restoring TPM for $vm_name (UUID: $vm_uuid)"

    # Preserve existing state. bak stays empty unless we actually move state
    # aside, so the error paths below can name it only when it exists (set -u).
    local bak=""
    if [[ -d "$target" && -n "$(ls -A "$target" 2>/dev/null)" ]]; then
        bak="${target}.pre-restore-$(date +%s)"
        log_warn "vmrestore.sh" "restore_tpm" "Backing up existing TPM to $bak"
        if ! mv "$target" "$bak"; then
            log_error "vmrestore.sh" "restore_tpm" \
                "Failed to move existing TPM state $target aside — existing state is unmodified; TPM NOT restored"
            return 1
        fi
    fi

    # UUID dir: root:root 711 (matches system layout)
    if ! { mkdir -p "$target" && chown root:root "$target" && chmod 711 "$target"; }; then
        if [[ -n "$bak" ]]; then
            log_error "vmrestore.sh" "restore_tpm" \
                "Failed to create/prepare $target — previous TPM state preserved at $bak (restore it manually); TPM NOT restored"
        else
            log_error "vmrestore.sh" "restore_tpm" \
                "Failed to create/prepare $target — TPM NOT restored"
        fi
        return 1
    fi

    # tpm2/ subdir: tss:tss 700 (matches system layout).
    if ! { cp -a "$tpm_dir/tpm2" "$target/" && chown -R tss:tss "$target/tpm2" && chmod 700 "$target/tpm2"; }; then
        if [[ -n "$bak" ]]; then
            log_error "vmrestore.sh" "restore_tpm" \
                "Failed to copy TPM state into $target — previous TPM state preserved at $bak (restore it manually); TPM NOT restored"
        else
            log_error "vmrestore.sh" "restore_tpm" \
                "Failed to copy TPM state into $target — TPM NOT restored"
        fi
        return 1
    fi

    log_info "vmrestore.sh" "restore_tpm" "TPM state restored: $target"
    return 0
}

# ── Chain-Endpoint NVRAM Helpers ──────────────────────────────────────────────
# Backups store per-checkpoint NVRAM as <vm>_VARS*.fd.virtnbdbackup.<N> inside
# the period directory. The chain-endpoint NVRAM is paired with the disk state
# at that checkpoint — restoring without it leaves UEFI variables (SecureBoot,
# BootOrder, MOK) drifted relative to the disk, producing "BdsDxe: No mapping".

# Derive the chain-endpoint checkpoint number for a restore.
# Args: data_dir, opt_restore_point (may be empty/"latest"/"full"/numeric)
# Stdout: endpoint checkpoint number (empty if undeterminable).
chain_endpoint_cp() {
    local data_dir="$1" rp="${2:-}"
    case "$rp" in
        full)   printf '0\n'; return 0 ;;
        [0-9]*) printf '%s\n' "$rp"; return 0 ;;
    esac
    local max_n="" f n
    for f in "$data_dir"/vmconfig.virtnbdbackup.*.xml; do
        [[ -f "$f" ]] || continue
        n="${f##*virtnbdbackup.}"; n="${n%.xml}"
        [[ "$n" =~ ^[0-9]+$ ]] || continue
        if [[ -z "$max_n" ]] || (( n > max_n )); then max_n="$n"; fi
    done
    [[ -n "$max_n" ]] && printf '%s\n' "$max_n"
}

# Locate the chain-NVRAM backup file for a VM at a given checkpoint.
# Tolerant of OVMF naming variants (_VARS.fd, _VARS.ms.fd, etc).
# Args: data_dir, vm_name, endpoint_cp
# Stdout: single matching path; exit 1 if none, exit 2 if ambiguous.
find_chain_nvram() {
    local data_dir="$1" vm_name="$2" endpoint_cp="$3"
    [[ -z "$endpoint_cp" ]] && return 1
    local -a matches=()
    shopt -s nullglob
    matches=( "$data_dir"/"${vm_name}"_VARS*.fd.virtnbdbackup."${endpoint_cp}" )
    shopt -u nullglob
    if [[ ${#matches[@]} -eq 0 ]]; then
        return 1
    elif [[ ${#matches[@]} -gt 1 ]]; then
        log_error "vmrestore.sh" "find_chain_nvram" \
            "Ambiguous chain NVRAM files for ${vm_name} at cp ${endpoint_cp}: ${matches[*]}"
        return 2
    fi
    printf '%s\n' "${matches[0]}"
}

# ── New-Identity Define ───────────────────────────────────────────────────────
# When --name is used: strip UUID + MACs so libvirt generates new ones,
# rename, and define. Returns the new UUID on stdout.
# $4 (optional): serialised disk rename map "old1|new1\nold2|new2"
# $5 (optional): chain-endpoint NVRAM backup path (preferred over live host
#                NVRAM so the clone's UEFI variables match the restored disk).

define_new_identity() {
    local src_xml="$1" new_name="$2" dry_run="$3"
    local disk_rename_map="${4:-}"
    local chain_nvram="${5:-}"

    if [[ "$dry_run" == true ]]; then  # [DRY-RUN-KEEPER: local-param polarity preserved, see 109-phase7-spec.md §1.3.3]
        log_info "vmrestore.sh" "define_new_identity" "[DRY RUN] Would define '$new_name' with new UUID/MACs from: $src_xml"
        echo "dry-run-uuid"
        return 0
    fi

    local tmp_xml
    tmp_xml=$(mktemp)
    local safe_name
    safe_name=$(printf '%s' "$new_name" | sed 's/[&/\\]/\\&/g')

    # Copy NVRAM to a new file named after the new VM (avoid sharing with original).
    # Prefer the chain-endpoint NVRAM backup (paired with the disk state being
    # restored) over the live host NVRAM. Falls back to live host NVRAM only
    # when the backup chain has no NVRAM file (older chains pre-NVRAM-backup
    # feature); a clear warning is emitted because boot may fail if SecureBoot
    # / UEFI variable state has drifted since the backup was taken.
    local orig_nvram new_nvram="" nvram_src=""
    orig_nvram=$(grep -oP '<nvram[^>]*>\K[^<]+' "$src_xml" || true)
    if [[ -n "$chain_nvram" && -f "$chain_nvram" ]]; then
        nvram_src="$chain_nvram"
        log_info "vmrestore.sh" "define_new_identity" "NVRAM source: chain-endpoint backup ($chain_nvram)"
    elif [[ -n "$orig_nvram" && -f "$orig_nvram" ]]; then
        nvram_src="$orig_nvram"
        log_warn "vmrestore.sh" "define_new_identity" \
            "No chain NVRAM in backup — falling back to live host NVRAM ($orig_nvram). Clone may fail to boot if SecureBoot/identity state has drifted since the backup."
    fi
    if [[ -n "$nvram_src" && -n "$orig_nvram" ]]; then
        local nvram_dir nvram_ext
        nvram_dir=$(dirname "$orig_nvram")
        nvram_ext="${orig_nvram##*_}"   # e.g. VARS.fd, VARS.ms.fd
        new_nvram="${nvram_dir}/${new_name}_${nvram_ext}"
        cp "$nvram_src" "$new_nvram"
        chown libvirt-qemu:libvirt-qemu "$new_nvram" 2>/dev/null || true
        chmod 600 "$new_nvram" 2>/dev/null || true
        log_info "vmrestore.sh" "define_new_identity" "NVRAM copied: $nvram_src → $new_nvram"
    fi

    # Build sed expressions: rename, strip UUID + MACs, update NVRAM path
    local -a sed_args=(
        -e 's|<name>[^<]*</name>|<name>'"${safe_name}"'</name>|'
        -e 's|<title>[^<]*</title>|<title>'"${safe_name}"'</title>|'
        -e '/<uuid>/d'
        -e '/<mac address=/d'
    )
    if [[ -n "$new_nvram" ]]; then
        local esc_orig esc_new
        esc_orig=$(printf '%s' "$orig_nvram" | sed 's|[&/\\]|\\&|g')
        esc_new=$(printf '%s' "$new_nvram" | sed 's|[&/\\]|\\&|g')
        sed_args+=(-e "s|${esc_orig}|${esc_new}|g")
    fi

    # Apply disk rename map: update <source file="..."> paths in the XML
    if [[ -n "$disk_rename_map" ]]; then
        while IFS='|' read -r old_path new_path; do
            [[ -z "$old_path" ]] && continue
            local esc_old esc_new_d
            esc_old=$(printf '%s' "$old_path" | sed 's|[&/\\]|\\&|g')
            esc_new_d=$(printf '%s' "$new_path" | sed 's|[&/\\]|\\&|g')
            sed_args+=(-e "s|${esc_old}|${esc_new_d}|g")
        done <<< "$disk_rename_map"
    fi

    sed "${sed_args[@]}" "$src_xml" > "$tmp_xml"

    local define_out
    if define_out=$(lv_define_xml "$tmp_xml"); then
        rm -f "$tmp_xml"
        # Extract new UUID from libvirt
        local new_uuid
        new_uuid=$(lv_domain_uuid "$new_name" || true)
        if [[ -n "$new_uuid" ]]; then
            log_info "vmrestore.sh" "define_new_identity" "Defined '$new_name' with new UUID: $new_uuid"
            # Log assigned MAC addresses so the user can verify network identity
            local _new_macs
            _new_macs=$(lv_list_iface_macs "$new_name" || true)
            if [[ -n "$_new_macs" ]]; then
                while IFS= read -r _m; do
                    log_info "vmrestore.sh" "define_new_identity" "New MAC: $_m"
                done <<< "$_new_macs"
            fi
            echo "$new_uuid"
        else
            log_warn "vmrestore.sh" "define_new_identity" "Defined but cannot read new UUID"
            echo ""
        fi
    else
        rm -f "$tmp_xml"
        # Clean up copied NVRAM on failure
        [[ -n "$new_nvram" && -f "$new_nvram" ]] && rm -f "$new_nvram"
        log_error "vmrestore.sh" "define_new_identity" "virsh define failed: $define_out"
        return 1
    fi
}

# ── Disk Collision Protection ─────────────────────────────────────────────────
# Predict what files virtnbdrestore will write to --restore-path, check for
# collisions with existing files and live VM disks, and rename clone disks
# after restore so filenames reflect the clone name.

# Predict output filenames virtnbdrestore will write.
# virtnbdrestore ALWAYS reads a vmconfig and names each output by that disk's
# <source file> basename (restore/files.py:target; disk.restore runs for every
# vmrestore invocation -- vmrestore never passes --sequence). -c only controls
# whether the WRITTEN config is rewritten, NOT the output filenames. So BOTH
# arms predict the source basename from the config the engine selects:
# With -c:  source basename for every config disk (e.g. my-server.qcow2)
# Without -c (--skip-config): same, restricted to config disks that have a .data
#   file present (a config disk with none is skipped by the engine)
#
# Populates global arrays:
#   _PREDICTED_BASENAMES  — what virtnbdrestore writes (e.g. my-server.qcow2)
#   _PREDICTED_DEVICE_MAP — "orig_source_path|device_target" per entry
#   _PREDICTED_FILES      — FINAL destination paths after rename
#                           (clone: restore-path/clone-name.qcow2)
#                           (non-clone: restore-path/original-basename.qcow2)
predict_output_files() {
    local data_dir="$1" restore_path="$2" use_c="$3" disk_filter="${4:-}" clone_name="${5:-}" cfg_override="${6:-}"
    _PREDICTED_FILES=()
    _PREDICTED_BASENAMES=()
    _PREDICTED_DEVICE_MAP=()

    # ── Collect raw basenames + device map ──
    if [[ "$use_c" == true ]]; then
        # With -c: output = basename of original <source file=...> from config XML
        local cfg_xml=""
        if [[ -n "$cfg_override" && -f "$cfg_override" ]]; then
            # Fix 5: Use the target checkpoint's vmconfig for PIT restores
            cfg_xml="$cfg_override"
        else
            for search_dir in "$data_dir/config" "$(dirname "$data_dir")/config"; do
                [[ -d "$search_dir" ]] || continue
                cfg_xml=$(ls -1t "$search_dir"/*.xml 2>/dev/null | head -1 || true)
                [[ -n "$cfg_xml" ]] && break
            done
        fi
        if [[ -z "$cfg_xml" || ! -f "$cfg_xml" ]]; then
            log_warn "vmrestore.sh" "predict_output_files" "No config XML found — cannot predict output filenames"
            return 1
        fi

        # Parse disk elements: extract device target and source file path
        # Only include type='file' device='disk' (skip cdrom, raw, etc.)
        local in_disk=false disk_device="" disk_target="" disk_source=""
        while IFS= read -r line; do
            if [[ "$line" =~ \<disk\ .*type=\'file\' ]]; then
                in_disk=true
                disk_device="" disk_target="" disk_source=""
                if [[ "$line" =~ device=\'([^\']+)\' ]]; then
                    disk_device="${BASH_REMATCH[1]}"
                fi
            fi
            if [[ "$in_disk" == true ]]; then
                if [[ "$line" =~ \<target\ dev=\'([^\']+)\' ]]; then
                    disk_target="${BASH_REMATCH[1]}"
                fi
                if [[ "$line" =~ \<source\ file=\'([^\']+)\' ]]; then
                    disk_source="${BASH_REMATCH[1]}"
                fi
                if [[ "$line" =~ \</disk\> ]]; then
                    if [[ "$disk_device" == "disk" && -n "$disk_source" && -n "$disk_target" ]]; then
                        if [[ -z "$disk_filter" || "$disk_target" == "$disk_filter" ]]; then
                            _PREDICTED_BASENAMES+=("$(basename "$disk_source")")
                            _PREDICTED_DEVICE_MAP+=("${disk_source}|${disk_target}")
                        fi
                    fi
                    in_disk=false
                fi
            fi
        done < "$cfg_xml"
    else
        # virtnbdrestore names every output by basename(<source file>) even
        # without -c (restore/files.py:target; disk.restore runs for every
        # vmrestore invocation -- never --sequence). So predict source basenames
        # from the SAME config the engine will select, restricted to disks that
        # actually have a .data file (a config disk with none is skipped:
        # restore/disk.py:68-77).

        # -- Step 1: device targets that have a .data file (engine writes only these) --
        local -A data_devices=()
        local f fname dev
        while IFS= read -r -d '' f; do
            fname=$(basename "$f")
            dev=""
            case "$fname" in
                *.full.data)               dev="${fname%.full.data}" ;;
                *.inc.virtnbdbackup.*.data) dev="${fname%%.*}" ;;
                *.copy.data)               dev="${fname%.copy.data}" ;;
            esac
            [[ -n "$dev" ]] && data_devices[$dev]=1
        done < <(find "$data_dir" -maxdepth 1 -type f -name '*.data' -print0 2>/dev/null)

        # -- Step 2: pick the config EXACTLY as virtnbdrestore will (mutate nothing) --
        # Mirror the engine pipeline: restore_vm's provisioning block (the
        # "Provision vmconfig XML for archived chains" gate + plain-cp with a
        # fresh mtime, below the predict call site) -> getLatest("vmconfig*.xml",
        # -1) = newest-by-mtime over the WIDE glob (virtnbdrestore:259;
        # common.getLatest L174-192). This block is prediction only -- it reads,
        # never writes; the real provisioning in restore_vm is mirrored, not edited.
        local cfg_xml=""
        if [[ -n "$cfg_override" && -f "$cfg_override" ]]; then
            # (1) Caller/PIT override -- the engine runs against this exact config.
            cfg_xml="$cfg_override"
        elif ls "$data_dir"/vmconfig.virtnbdbackup.*.xml &>/dev/null; then
            # (2) Narrow inline config present -> restore_vm's provisioning gate does NOT
            # fire; the dir is unchanged, so getLatest picks the newest file over
            # the WIDE glob -- a newer vmconfig.copy.xml would win over the narrow
            # one, so predict from the WIDE-glob newest, not the narrow file.
            cfg_xml=$(ls -1t "$data_dir"/vmconfig*.xml 2>/dev/null | head -1 || true)
        else
            # No narrow inline config -> provisioning WILL run. Reproduce its search.
            local search_dir
            for search_dir in "$data_dir/config" "$(dirname "$data_dir")/config"; do
                [[ -d "$search_dir" ]] || continue
                cfg_xml=$(ls -1t "$search_dir"/*.xml 2>/dev/null | head -1 || true)
                [[ -n "$cfg_xml" ]] && break
            done
            if [[ -n "$cfg_xml" && -f "$cfg_xml" ]]; then
                # (3) Provisioning copies this config/ file to
                # vmconfig.virtnbdbackup.0.xml with a FRESH mtime (plain cp,
                # no -p) -> it becomes the newest vmconfig*.xml, so getLatest
                # selects it. cp preserves basenames, so predicting from this
                # config/ source is byte-equivalent to what the engine reads.
                :
            else
                # (4) No config/ found -> provisioning attempts and fails (its
                # own log_warn) but does NOT abort; the dir is unchanged and getLatest
                # still finds any inline WIDE-glob file (e.g. a lone
                # vmconfig.copy.xml) and the engine proceeds against it.
                cfg_xml=$(ls -1t "$data_dir"/vmconfig*.xml 2>/dev/null | head -1 || true)
            fi
        fi
        if [[ -z "$cfg_xml" || ! -f "$cfg_xml" ]]; then
            # (5) No config anywhere -> virtnbdrestore dies ("No domain config file
            # found": virtnbdrestore:260-262). Predict nothing; fail closed.
            log_warn "vmrestore.sh" "predict_output_files" "No config XML found — cannot predict output filenames"
            return 1
        fi

        # -- Step 3: predict PRIMARY <source file> basename per disk --
        # xpath('source')[0] semantics -- the FIRST <source file> wins; a
        # backingStore adds later <source> lines the engine ignores for naming
        # (client.py:331 disk.xpath('source')[0]). Only type='file' device='disk';
        # honor -d (disk_filter); and only disks whose target has a .data file
        # (Step 1) -- the engine skips the rest.
        local in_disk=false disk_device="" disk_target="" disk_source=""
        while IFS= read -r line; do
            if [[ "$line" =~ \<disk\ .*type=\'file\' ]]; then
                in_disk=true
                disk_device="" disk_target="" disk_source=""
                if [[ "$line" =~ device=\'([^\']+)\' ]]; then
                    disk_device="${BASH_REMATCH[1]}"
                fi
            fi
            if [[ "$in_disk" == true ]]; then
                if [[ "$line" =~ \<target\ dev=\'([^\']+)\' ]]; then
                    disk_target="${BASH_REMATCH[1]}"
                fi
                if [[ -z "$disk_source" && "$line" =~ \<source\ file=\'([^\']+)\' ]]; then
                    disk_source="${BASH_REMATCH[1]}"
                fi
                if [[ "$line" =~ \</disk\> ]]; then
                    if [[ "$disk_device" == "disk" && -n "$disk_source" && -n "$disk_target" && -n "${data_devices[$disk_target]:-}" ]]; then
                        if [[ -z "$disk_filter" || "$disk_target" == "$disk_filter" ]]; then
                            _PREDICTED_BASENAMES+=("$(basename "$disk_source")")
                            _PREDICTED_DEVICE_MAP+=("${disk_source}|${disk_target}")
                        fi
                    fi
                    in_disk=false
                fi
            fi
        done < "$cfg_xml"
    fi

    if [[ ${#_PREDICTED_BASENAMES[@]} -eq 0 ]]; then
        log_warn "vmrestore.sh" "predict_output_files" "Could not predict any output files"
        return 1
    fi

    # ── Compute final destination paths ──
    local multi_disk=false
    [[ ${#_PREDICTED_BASENAMES[@]} -gt 1 ]] && multi_disk=true

    local idx=0
    for raw_base in "${_PREDICTED_BASENAMES[@]}"; do
        local final_base="$raw_base"
        if [[ -n "$clone_name" ]]; then
            local ext="${raw_base##*.}"
            local device_target="${_PREDICTED_DEVICE_MAP[$idx]##*|}"
            if [[ "$multi_disk" == true ]]; then
                final_base="${clone_name}-${device_target}.${ext}"
            else
                final_base="${clone_name}.${ext}"
            fi
        fi
        _PREDICTED_FILES+=("$restore_path/$final_base")
        log_info "vmrestore.sh" "predict_output_files" "Predicted final: $restore_path/$final_base (virtnbdrestore writes: $raw_base)"
        ((idx++))
    done
    return 0
}

# Check predicted output files for collisions with existing files and live VM
# disks. Aborts on unsafe conditions, warns on force-overridable ones.
preflight_disk_safety() {
    local vm_name="$1" dry_run="$2" force="$3"
    local dry_tag=""
    [[ "$dry_run" == true ]] && dry_tag="[DRY RUN] "  # [DRY-RUN-KEEPER: local-param polarity preserved, see 109-phase7-spec.md §1.3.3]

    if [[ ${#_PREDICTED_FILES[@]} -eq 0 ]]; then
        log_info "vmrestore.sh" "preflight_disk_safety" "No predicted files — skipping safety checks"
        return 0
    fi

    # Build set of all defined VMs' disk paths → associative array path→vm_name
    local -A live_disk_map=()
    local vm_entry
    while IFS= read -r vm_entry; do
        [[ -z "$vm_entry" ]] && continue
        # DISK-02 (118-spaces): build the map from lv_list_disk_paths, NOT
        # `domblklist | awk '{print $2}'`. The old parse truncated any spaced disk
        # path at its first space, so a running VM with a spaced disk was never
        # entered in the map — and this overwrite-protection gate then FAILED OPEN
        # (a restore could clobber the live disk of a running VM). This loop only
        # consumes the Source, so the (now space-safe) lv_list_disk_paths is a
        # drop-in; it already skips the header and media-less ("-") entries.
        local blk_src
        while IFS= read -r blk_src; do
            [[ -z "$blk_src" ]] && continue
            local real_blk
            real_blk=$(pu_safe_realpath "$blk_src")
            live_disk_map["$real_blk"]="$vm_entry"
        done < <(lv_list_disk_paths "$vm_entry")
    done < <(lv_list_all_domains)

    local abort=false
    for pred_file in "${_PREDICTED_FILES[@]}"; do
        local real_pred
        real_pred=$(pu_safe_realpath "$pred_file")

        # Check: is this a live VM's disk?
        local owner_vm="${live_disk_map[$real_pred]:-}"
        if [[ -n "$owner_vm" ]]; then
            if [[ "$owner_vm" == "$vm_name" ]]; then
                # Same VM — disaster recovery scenario
                local vm_state
                vm_state=$(lv_domain_state "$owner_vm"); [[ -z $vm_state ]] && vm_state="unknown"
                if [[ "$vm_state" =~ running|paused ]]; then
                    log_error "vmrestore.sh" "preflight_disk_safety" "${dry_tag}BLOCKED: $pred_file is the live disk of running VM '$owner_vm' — shut it off first"
                    abort=true
                elif [[ "$force" == true ]]; then
                    log_warn "vmrestore.sh" "preflight_disk_safety" "${dry_tag}Will overwrite disk of shut-off VM '$owner_vm': $pred_file (disaster recovery with --force)"
                else
                    log_error "vmrestore.sh" "preflight_disk_safety" "${dry_tag}BLOCKED: $pred_file is the disk of VM '$owner_vm' — use --force for disaster recovery"
                    abort=true
                fi
            else
                # Different VM — NEVER allow
                log_error "vmrestore.sh" "preflight_disk_safety" "${dry_tag}BLOCKED: $pred_file is the live disk of VM '$owner_vm' — choose a different --restore-path"
                abort=true
            fi
        elif [[ -f "$pred_file" ]]; then
            # File exists but not a live VM disk
            if [[ "$force" == true ]]; then
                log_warn "vmrestore.sh" "preflight_disk_safety" "${dry_tag}File exists and will be overwritten: $pred_file"
            else
                log_error "vmrestore.sh" "preflight_disk_safety" "${dry_tag}BLOCKED: File already exists: $pred_file — use --force to overwrite"
                abort=true
            fi
        fi
    done

    if [[ "$abort" == true ]]; then
        if [[ "$dry_run" == true ]]; then  # [DRY-RUN-KEEPER: local-param polarity preserved, see 109-phase7-spec.md §1.3.3]
            log_warn "vmrestore.sh" "preflight_disk_safety" "[DRY RUN] Would abort due to safety checks above"
            return 0
        fi
        die "Pre-flight disk safety check failed — see errors above" "preflight_disk_safety" "$EXIT_STORAGE"
    fi

    log_info "vmrestore.sh" "preflight_disk_safety" "${dry_tag}All safety checks passed"
    return 0
}

# Move restored disk files from staging dir to final location with clone name.
# Only applies when --name is used (new-identity clone).
# Populates global _DISK_RENAME_MAP ("original_source_path|new_path" per line)
# for passing to define_new_identity().
# Args: clone_name staging_dir restore_path dry_run
stage_and_rename_clone_disks() {
    local clone_name="$1" staging_dir="$2" restore_path="$3" dry_run="$4"
    _DISK_RENAME_MAP=""
    # FF-11: count per-disk promotion (mv) failures so an incomplete promotion
    # returns non-zero and the caller can fail closed instead of defining a
    # half-populated clone.
    local _promote_failed=0

    if [[ ${#_PREDICTED_BASENAMES[@]} -eq 0 ]]; then
        log_warn "vmrestore.sh" "stage_and_rename" "No predicted files — nothing to rename"
        return 0
    fi

    local multi_disk=false
    [[ ${#_PREDICTED_BASENAMES[@]} -gt 1 ]] && multi_disk=true

    local i=0
    for raw_base in "${_PREDICTED_BASENAMES[@]}"; do
        local mapping="${_PREDICTED_DEVICE_MAP[$i]}"
        local orig_source_path="${mapping%%|*}"
        local device_target="${mapping##*|}"
        local extension="${raw_base##*.}"  # qcow2

        local new_basename
        if [[ "$multi_disk" == true ]]; then
            new_basename="${clone_name}-${device_target}.${extension}"
        else
            new_basename="${clone_name}.${extension}"
        fi

        local staged_file="$staging_dir/$raw_base"
        local final_file="$restore_path/$new_basename"

        local _do_append=false
        if [[ "$dry_run" == true ]]; then  # [DRY-RUN-KEEPER: local-param polarity preserved, see 109-phase7-spec.md §1.3.3]
            log_info "vmrestore.sh" "stage_and_rename" "[DRY RUN] Would move: staging/$raw_base → $new_basename"
            # Dry-run parity only: the dry-run caller discards this map. Kept so
            # preview and real runs build the same structure.
            _do_append=true
        else
            if [[ -f "$staged_file" ]]; then
                if mv "$staged_file" "$final_file"; then
                    log_info "vmrestore.sh" "stage_and_rename" "Moved: staging/$raw_base → $new_basename"
                    _do_append=true
                else
                    log_error "vmrestore.sh" "stage_and_rename" "Failed to promote staged disk to final location: $staged_file → $final_file"
                    ((_promote_failed++))
                fi
            else
                log_error "vmrestore.sh" "stage_and_rename" "Expected file not found in staging: $staged_file"
                ((_promote_failed++))
            fi
        fi

        # Build rename map for define_new_identity(): staged_path|new_absolute_path
        # Use staged_file (not orig_source_path) because virtnbdrestore rewrites
        # vmconfig.xml <source file="..."> to point at the output directory (staging).
        # FF-11: only map disks that were actually promoted (or dry-run parity); a
        # failed mv must never seed a define that points at a missing/half path.
        if [[ "$_do_append" == true ]]; then
            if [[ -n "$_DISK_RENAME_MAP" ]]; then
                _DISK_RENAME_MAP+=$'\n'
            fi
            _DISK_RENAME_MAP+="${staged_file}|${final_file}"
        fi
        ((i++))
    done

    (( _promote_failed > 0 )) && return 1
    return 0
}

# ── Core Restore ─────────────────────────────────────────────────────────────

# FF-11: drop a staging dir from the EXIT-trap cleanup list (_VMRESTORE_STAGING_DIRS)
# so a subsequent fail-closed die PRESERVES it — it holds the only copy of the
# un-promoted clone disks, or a restore-written vmconfig.xml that could not be
# moved. Rebuilds the array WITHOUT the named dir; deliberately never uses the
# append form other gates anchor on.
_vmrestore_unregister_staging() {
    local target="$1" _keep=() _d
    if (( ${#_VMRESTORE_STAGING_DIRS[@]} > 0 )); then
        for _d in "${_VMRESTORE_STAGING_DIRS[@]}"; do
            [[ "$_d" == "$target" ]] && continue
            _keep+=("$_d")
        done
    fi
    _VMRESTORE_STAGING_DIRS=("${_keep[@]}")
}

restore_vm() {
    local vm_name="$1"
    log_info "vmrestore.sh" "restore_vm" "Starting restore: $vm_name"

    # Phase 8 (UNI-902b): open a restore_sessions row at the "Starting restore"
    # boundary. Dry-run short-circuits per §2.5 — predictions do not pollute
    # the audit table. Warn-and-continue per §2.2 — failure to record never
    # blocks the restore. Returned rowid lives in _RESTORE_SESSION_ID for
    # _vmrestore_cleanup to finalise. Source path uses $OPT_BACKUP_PATH as-is
    # (the operator-supplied source); period_id and checkpoint default to
    # empty here — they are not always known at this entry point and are
    # informational columns only.
    if [[ "${OPT_DRY_RUN:-false}" == false ]] \
       && declare -F sqlite_is_available >/dev/null 2>&1 \
       && sqlite_is_available; then
        local _rs_id
        _rs_id=$(sqlite_restore_session_start \
            "$vm_name" \
            "${OPT_MODE:-restore}" \
            0 \
            "${OPT_RESTORE_PATH:-}" \
            "${OPT_BACKUP_PATH:-}" \
            "${OPT_PERIOD:-}" \
            "${OPT_RESTORE_POINT:-}" 2>/dev/null) || \
            log_warn "vmrestore.sh" "restore_vm" "restore session not recorded in catalogue (continuing)"
        if [[ -n "$_rs_id" ]] && [[ "$_rs_id" =~ ^[0-9]+$ ]]; then
            _RESTORE_SESSION_ID="$_rs_id"
            log_info "vmrestore.sh" "restore_vm" "Catalogue: restore session id=$_RESTORE_SESSION_ID"
        fi
    fi

    # Resolve backup data directory
    local data_dir=""
    if has_backup_data "$OPT_BACKUP_PATH"; then
        # Direct path to period dir or archive chain
        data_dir="$OPT_BACKUP_PATH"
        log_info "vmrestore.sh" "restore_vm" "Using direct backup path: $data_dir"
    else
        local vm_dir
        vm_dir=$(resolve_vm_backup_dir "$vm_name" "$OPT_BACKUP_PATH") \
            || die "VM directory not found for '$vm_name' (looked for the token and legacy folders under $OPT_BACKUP_PATH)" "restore_vm" "$EXIT_VM"
        data_dir=$(resolve_data_dir "$vm_dir" "${OPT_PERIOD:-}") || \
            die "Cannot resolve data directory for $vm_name" "restore_vm" "$EXIT_VM"
    fi

    has_backup_data "$data_dir" || die "No backup data files in: $data_dir" "restore_vm" "$EXIT_VM"

    # INT-16: refuse to restore from an incomplete chain unless the operator
    # has explicitly opted in with --include-incomplete. An incomplete chain
    # (e.g. produced by overlapping backups per INT-15) has missing per-disk
    # data files or qcow.json sidecars relative to the declared disk set, and
    # restoring it yields a partial / inconsistent VM.
    chain_check_complete "$data_dir"
    local _chain_rc=$?
    if [[ "$_chain_rc" -eq 1 ]]; then
        if [[ "$OPT_INCLUDE_INCOMPLETE" == true ]]; then
            log_warn "vmrestore.sh" "restore_vm" \
                "⚠ INCOMPLETE chain accepted via --include-incomplete: $_CHAIN_INCOMPLETE_REASON"
        else
            die "Refusing to restore from incomplete chain at $data_dir (INT-16: $_CHAIN_INCOMPLETE_REASON). Use --include-incomplete to override (forensic only)." "restore_vm" "$EXIT_VM"
        fi
    fi

    # If --vm pointed to an archive chain or period directory, vm_name will be
    # the directory basename (e.g. "chain-2026-03-12").  Extract the real VM
    # name from the backup's vmconfig XML so -D/-N use the correct identity.
    local _cfg_xml_for_name
    _cfg_xml_for_name=$(ls -1t "$data_dir"/vmconfig.virtnbdbackup.*.xml 2>/dev/null | head -1 || true)
    if [[ -z "$_cfg_xml_for_name" ]]; then
        # Archived chains may lack vmconfig — check config/ dir
        for _cdir in "$data_dir/config" "$(dirname "$data_dir")/config"; do
            [[ -d "$_cdir" ]] || continue
            _cfg_xml_for_name=$(ls -1t "$_cdir"/*.xml 2>/dev/null | head -1 || true)
            [[ -n "$_cfg_xml_for_name" ]] && break
        done
    fi
    if [[ -n "$_cfg_xml_for_name" ]]; then
        local _xml_vm_name
        _xml_vm_name=$(grep -oP '<name>\K[^<]+' "$_cfg_xml_for_name" 2>/dev/null | head -1 || true)
        if [[ -n "$_xml_vm_name" && "$_xml_vm_name" != "$vm_name" ]]; then
            log_info "vmrestore.sh" "restore_vm" "Resolved VM name from backup config: $vm_name → $_xml_vm_name"
            vm_name="$_xml_vm_name"
        fi
    fi

    local btype
    btype=$(detect_backup_type "$data_dir")
    log_info "vmrestore.sh" "restore_vm" "Data dir: $data_dir (type: $btype)"

    # ── Disk-Restore Mode ────────────────────────────────────────────────────
    # When --disk is specified, this is a disk-level file replacement — not a
    # VM restore. No VM definition changes, no TPM, no UUID/MAC changes. The
    # VM already exists. Works for single-disk and multi-disk VMs alike.
    # Supports: --disk vda | --disk vda,vdb | --disk all
    if [[ -n "${OPT_DISK:-}" ]]; then
        local available_disks
        available_disks=$(enumerate_disks "$data_dir")

        if [[ -z "$available_disks" ]]; then
            die "No disks found in backup: $data_dir" "restore_vm" "$EXIT_VM"
        fi

        # Parse --disk value into array: single name, comma-separated, or "all"
        local -a disk_list=()
        if [[ "$OPT_DISK" == "all" ]]; then
            local IFS=', '
            for _d in $available_disks; do
                disk_list+=("$_d")
            done
            unset IFS
        else
            local IFS=','
            for _d in $OPT_DISK; do
                _d=$(echo "$_d" | tr -d '[:space:]')
                [[ -n "$_d" ]] && disk_list+=("$_d")
            done
            unset IFS
        fi

        if [[ ${#disk_list[@]} -eq 0 ]]; then
            die "No disk names specified" "restore_vm" "$EXIT_USAGE"
        fi

        # Validate all disk names against available disks (union across all CPs)
        for _d in "${disk_list[@]}"; do
            local _found=false
            local IFS=', '
            for _avail in $available_disks; do
                [[ "$_avail" == "$_d" ]] && _found=true
            done
            unset IFS
            if [[ "$_found" == false ]]; then
                die "Disk '$_d' not found in backup. Available disks: $available_disks" "restore_vm" "$EXIT_VM"
            fi
        done

        # Point-in-time disk availability: validate each disk exists at the target checkpoint
        if [[ "$btype" == "incremental" ]]; then
            local _target_cp=""
            if [[ "$OPT_RESTORE_POINT" != "latest" ]]; then
                case "$OPT_RESTORE_POINT" in
                    full)   _target_cp="0" ;;
                    [0-9]*) _target_cp="$OPT_RESTORE_POINT" ;;
                esac
            else
                # Latest = highest checkpoint number
                _target_cp=$(find "$data_dir/checkpoints" -maxdepth 1 -name "virtnbdbackup.*.xml" -printf '%f\n' 2>/dev/null \
                    | sed 's/virtnbdbackup\.\([0-9]*\)\.xml/\1/' | sort -n | tail -1)
            fi
            if [[ -n "$_target_cp" ]]; then
                local _cp_disks
                _cp_disks=$(enumerate_disks_at_checkpoint "$data_dir" "$_target_cp")
                for _d in "${disk_list[@]}"; do
                    local _cp_found=false
                    local IFS=', '
                    for _cp_avail in $_cp_disks; do
                        [[ "$_cp_avail" == "$_d" ]] && _cp_found=true
                    done
                    unset IFS
                    if [[ "$_cp_found" == false ]]; then
                        # Find the last checkpoint where this disk existed
                        local _last_seen=""
                        local _cp_n
                        for _cp_n in $(find "$data_dir/checkpoints" -maxdepth 1 -name "virtnbdbackup.*.xml" -printf '%f\n' 2>/dev/null \
                            | sed 's/virtnbdbackup\.\([0-9]*\)\.xml/\1/' | sort -rn); do
                            local _check_disks
                            _check_disks=$(enumerate_disks_at_checkpoint "$data_dir" "$_cp_n")
                            if [[ ", $_check_disks, " == *", $_d, "* ]]; then
                                _last_seen="$_cp_n"
                                break
                            fi
                        done
                        if [[ -n "$_last_seen" ]]; then
                            die "Disk '$_d' is not available at checkpoint $_target_cp (disks: $_cp_disks). It was last backed up at checkpoint $_last_seen. Use --restore-point $_last_seen to restore this disk." "restore_vm" "$EXIT_VM"
                        else
                            die "Disk '$_d' is not available at checkpoint $_target_cp (disks: $_cp_disks)." "restore_vm" "$EXIT_VM"
                        fi
                    fi
                done
            fi
        fi

        local disk_list_display
        disk_list_display=$(printf '%s, ' "${disk_list[@]}")
        disk_list_display="${disk_list_display%, }"
        log_info "vmrestore.sh" "restore_vm" "Disk restore mode: replacing '$disk_list_display' (available: $available_disks)"

        # ── Common pre-checks (once) ─────────────────────────────────
        local _inplace=false
        if [[ -z "$OPT_RESTORE_PATH" ]]; then
            _inplace=true
            if ! lv_domain_exists "$vm_name"; then
                die "VM '$vm_name' is not defined in libvirt — cannot determine original disk paths. Use --restore-path to extract disks to a specific location instead." "restore_vm" "$EXIT_VM"
            fi
            local vm_state
            vm_state=$(lv_domain_state "$vm_name")
            if [[ "$vm_state" != "shut off" ]]; then
                die "VM '$vm_name' is $vm_state — shut it down first (virsh shutdown $vm_name). Replacing disks under a running VM will cause corruption." "restore_vm" "$EXIT_VM"
            fi
            log_info "vmrestore.sh" "restore_vm" "VM '$vm_name' is shut off ✓"
        else
            log_info "vmrestore.sh" "restore_vm" "Extract mode: writing to $OPT_RESTORE_PATH"
        fi

        # ── Per-disk resolution ──────────────────────────────────────
        # Build associative arrays: disk → original path, target dir, target file
        local -A _dk_path=()   # disk → original file path (in-place only)
        local -A _dk_dir=()    # disk → restore target directory
        local -A _dk_file=()   # disk → original filename (in-place only)
        local -A _dk_need=()   # FF-168: disk → bytes needed on its target fs
        local total_data_bytes=0
        local total_prerestore_bytes=0

        local _vm_xml=""
        if [[ "$_inplace" == true ]]; then
            _vm_xml=$(lv_dump_xml "$vm_name" --inactive)
        fi

        for _d in "${disk_list[@]}"; do
            if [[ "$_inplace" == true ]]; then
                local _opath
                _opath=$(echo "$_vm_xml" | grep -B5 "target dev='$_d'" | \
                    grep -oP "source file='\K[^']+" | head -1 || true)
                if [[ -z "$_opath" ]]; then
                    die "Disk '$_d' not found in VM '$vm_name' configuration. Available disks: $(lv_list_disk_targets "$vm_name" | paste -sd', ')" "restore_vm" "$EXIT_VM"
                fi
                if [[ ! -f "$_opath" ]]; then
                    die "Original disk path does not exist: $_opath — Use --restore-path to extract disks to a specific location instead." "restore_vm" "$EXIT_STORAGE"
                fi
                # Check for .pre-restore overwrite
                if [[ -f "${_opath}.pre-restore" && "$OPT_NO_PRE_RESTORE" == false ]]; then
                    die "Pre-restore file already exists: ${_opath}.pre-restore — delete it first or use --no-pre-restore" "restore_vm" "$EXIT_STORAGE"
                fi
                _dk_path[$_d]="$_opath"
                _dk_dir[$_d]=$(dirname "$_opath")
                _dk_file[$_d]=$(basename "$_opath")
                log_info "vmrestore.sh" "restore_vm" "  $_d → $_opath"
                # Accumulate .pre-restore space
                if [[ "$OPT_NO_PRE_RESTORE" == false ]]; then
                    local _prb
                    _prb=$(stat -c%s "$_opath" 2>/dev/null || echo 0)
                    total_prerestore_bytes=$(( total_prerestore_bytes + _prb ))
                    _dk_need[$_d]=$(( ${_dk_need[$_d]:-0} + _prb ))   # FF-168: per-fs need
                fi
            else
                _dk_dir[$_d]="$OPT_RESTORE_PATH"
            fi

            # Accumulate backup data size for this disk
            while IFS= read -r -d '' _dfile; do
                local _dfname _ddev=""
                _dfname=$(basename "$_dfile")
                case "$_dfname" in
                    *.full.data)                _ddev="${_dfname%.full.data}" ;;
                    *.inc.virtnbdbackup.*.data) _ddev="${_dfname%%.*}" ;;
                    *.copy.data)                _ddev="${_dfname%.copy.data}" ;;
                esac
                if [[ "$_ddev" == "$_d" ]]; then
                    local _dbb
                    _dbb=$(stat -c%s "$_dfile" 2>/dev/null || echo 0)
                    total_data_bytes=$(( total_data_bytes + _dbb ))
                    _dk_need[$_d]=$(( ${_dk_need[$_d]:-0} + _dbb ))   # FF-168: per-fs need
                fi
            done < <(find "$data_dir" -maxdepth 1 -type f -name '*.data' -print0 2>/dev/null)
        done

        # ── Space check, grouped per target filesystem (FF-168) ──────
        # A multi-disk in-place restore can span multiple mounts (OS-on-SSD,
        # data-on-HDD). The former check summed every disk's need and probed only
        # disk_list[0]'s filesystem, so it both false-passed (a second fs fills
        # mid-restore, caught only afterwards by qemu-img) and false-blocked (the
        # summed need charged to one fs). Group each disk's need by the filesystem
        # its target dir resolves to, and check every filesystem.
        local -A _fs_need=()    # fs mountpoint → summed bytes needed there
        local -A _fs_probe=()   # fs mountpoint → an existing dir on it to df
        local _sd _cdir _mnt
        for _sd in "${disk_list[@]}"; do
            _cdir="${_dk_dir[$_sd]:-$OPT_RESTORE_PATH}"
            while [[ -n "$_cdir" && ! -d "$_cdir" ]]; do
                _cdir=$(dirname "$_cdir")
            done
            [[ -n "$_cdir" ]] || _cdir="/"
            _mnt=$(df --output=target "$_cdir" 2>/dev/null | tail -1)
            [[ -n "$_mnt" ]] || _mnt="$_cdir"
            _fs_need[$_mnt]=$(( ${_fs_need[$_mnt]:-0} + ${_dk_need[$_sd]:-0} ))
            _fs_probe[$_mnt]="$_cdir"
        done
        local avail_bytes="" _mkey _need_fs _fs_avail
        for _mkey in "${!_fs_need[@]}"; do
            _need_fs="${_fs_need[$_mkey]}"
            _fs_avail=$(df --output=avail -B1 "${_fs_probe[$_mkey]}" 2>/dev/null | tail -1 | tr -d '[:space:]')
            _fs_avail="${_fs_avail:-0}"
            if [[ -z "$avail_bytes" ]] || (( _fs_avail < avail_bytes )); then avail_bytes="$_fs_avail"; fi
            if (( _need_fs > _fs_avail )); then
                local need_hr avail_hr
                need_hr=$(_format_size "$_need_fs")
                avail_hr=$(_format_size "$_fs_avail")
                die "Insufficient space on ${_mkey}: need $need_hr (restore + .pre-restore) but only $avail_hr available" "restore_vm" "$EXIT_STORAGE"
            fi
        done
        avail_bytes="${avail_bytes:-0}"
        local data_hr
        data_hr=$(_format_size "$total_data_bytes")
        if [[ "$_inplace" == true && "$OPT_NO_PRE_RESTORE" == false && "$total_prerestore_bytes" -gt 0 ]]; then
            local prerestore_hr
            prerestore_hr=$(_format_size "$total_prerestore_bytes")
            log_info "vmrestore.sh" "restore_vm" "Space check: ${data_hr} restore + ${prerestore_hr} .pre-restore — $(_format_size "$avail_bytes") available ✓"
        else
            log_info "vmrestore.sh" "restore_vm" "Space check: ${data_hr} restore — $(_format_size "$avail_bytes") available ✓"
        fi

        # ── PIT staging (disk-restore mode) ──────────────────────────
        # When point-in-time targets a checkpoint with a different disk set,
        # create a staging input directory so virtnbdrestore reads the correct vmconfig.
        local pit_input_dir=""
        if [[ "$OPT_RESTORE_POINT" != "latest" && "$btype" == "incremental" ]]; then
            local _pit_target_cp=""
            case "$OPT_RESTORE_POINT" in
                full)   _pit_target_cp="0" ;;
                [0-9]*) _pit_target_cp="$OPT_RESTORE_POINT" ;;
            esac
            if [[ -n "$_pit_target_cp" ]]; then
                local _pit_latest_cp
                _pit_latest_cp=$(find "$data_dir/checkpoints" -maxdepth 1 -name "virtnbdbackup.*.xml" -printf '%f\n' 2>/dev/null \
                    | sed 's/virtnbdbackup\.\([0-9]*\)\.xml/\1/' | sort -n | tail -1)
                local _pit_target_disks _pit_latest_disks
                _pit_target_disks=$(enumerate_disks_at_checkpoint "$data_dir" "$_pit_target_cp")
                _pit_latest_disks=$(enumerate_disks_at_checkpoint "$data_dir" "$_pit_latest_cp")
                if [[ "$_pit_target_disks" != "$_pit_latest_disks" ]]; then
                    log_warn "vmrestore.sh" "restore_vm" "Disk configuration changed between checkpoint $_pit_target_cp and latest ($_pit_latest_cp)."
                    log_warn "vmrestore.sh" "restore_vm" "  Checkpoint $_pit_target_cp: $_pit_target_disks"
                    log_warn "vmrestore.sh" "restore_vm" "  Latest (CP $_pit_latest_cp): $_pit_latest_disks"
                    log_warn "vmrestore.sh" "restore_vm" "  Restoring with checkpoint $_pit_target_cp disk configuration."
                    if ! is_dry_run; then
                        pit_input_dir=$(create_pit_staging "$data_dir" "$_pit_target_cp") || \
                            die "Failed to create PIT staging directory" "restore_vm" "$EXIT_STORAGE"
                        # FF-193: track in the PARENT shell (the append inside
                        # create_pit_staging ran dead in this $() subshell) so the
                        # EXIT/SIGINT trap removes the staging symlink farm.
                        _VMRESTORE_STAGING_DIRS+=("$pit_input_dir")
                        log_info "vmrestore.sh" "restore_vm" "PIT staging directory: $pit_input_dir"
                    fi
                fi
            fi
        fi
        local _virtnbd_data_dir="${pit_input_dir:-$data_dir}"

        # ── Dry run ──────────────────────────────────────────────────
        if is_dry_run; then
            for _d in "${disk_list[@]}"; do
                log_info "vmrestore.sh" "restore_vm" "[DRY RUN] Disk restore: $_d"
                if [[ -n "${_dk_path[$_d]:-}" ]]; then
                    log_info "vmrestore.sh" "restore_vm" "[DRY RUN] Would rename: ${_dk_path[$_d]} → ${_dk_path[$_d]}.pre-restore"
                    log_info "vmrestore.sh" "restore_vm" "[DRY RUN] Would restore to: ${_dk_path[$_d]}"
                else
                    log_info "vmrestore.sh" "restore_vm" "[DRY RUN] Would restore to: $OPT_RESTORE_PATH/"
                fi
                log_info "vmrestore.sh" "restore_vm" "[DRY RUN] virtnbdrestore -i $_virtnbd_data_dir -o ${_dk_dir[$_d]} -d $_d"
            done
            if [[ "$OPT_RESTORE_POINT" != "latest" && "$btype" == "incremental" ]]; then
                local until_cp_dr
                case "$OPT_RESTORE_POINT" in
                    full)    until_cp_dr="virtnbdbackup.0" ;;
                    [0-9]*)  until_cp_dr="virtnbdbackup.$OPT_RESTORE_POINT" ;;
                esac
                log_info "vmrestore.sh" "restore_vm" "[DRY RUN] Point-in-time: --until $until_cp_dr"
            fi
            if [[ -n "$pit_input_dir" ]]; then
                log_info "vmrestore.sh" "restore_vm" "[DRY RUN] PIT staging: would create staging dir with checkpoint $OPT_RESTORE_POINT vmconfig"
            fi
            log_info "vmrestore.sh" "restore_vm" "Disk restore complete: $vm_name/$disk_list_display [DRY RUN — no changes made]"
            return 0
        fi

        # ── Restore loop ─────────────────────────────────────────────
        mkdir -p "${OPT_RESTORE_PATH:-${_dk_dir[${disk_list[0]}]}}"

        local -a _restored_disks=()
        local -a _prerestore_files=()
        local _failed_disk=""
        local _disk_idx=0
        local _disk_total=${#disk_list[@]}

        for _d in "${disk_list[@]}"; do
            ((_disk_idx++))
            log_info "vmrestore.sh" "restore_vm" "── Restoring $_d [$_disk_idx/$_disk_total] ──"

            local _tgt_dir="${_dk_dir[$_d]}"
            local _orig="${_dk_path[$_d]:-}"

            # Create .pre-restore backup
            if [[ -n "$_orig" && -f "$_orig" && "$OPT_NO_PRE_RESTORE" == false ]]; then
                mv "$_orig" "${_orig}.pre-restore"
                _prerestore_files+=("${_orig}.pre-restore")
                log_info "vmrestore.sh" "restore_vm" "Backed up existing: ${_orig}.pre-restore"
            elif [[ -n "$_orig" && "$OPT_NO_PRE_RESTORE" == true ]]; then
                rm -f "$_orig"
                log_warn "vmrestore.sh" "restore_vm" "Removed existing disk (--no-pre-restore): $_orig"
            fi

            # UNI-014 M1: build virtnbdrestore argv via lib helper.
            build_virtnbdrestore_args "$_virtnbd_data_dir" "$_tgt_dir" "restore" -d "$_d"
            local -a disk_cmd=("${_VIRTNBD_ARGS[@]}")

            # Point-in-time
            if [[ "$OPT_RESTORE_POINT" != "latest" && "$btype" == "incremental" ]]; then
                local until_cp=""
                case "$OPT_RESTORE_POINT" in
                    full)    until_cp="virtnbdbackup.0" ;;
                    [0-9]*)  until_cp="virtnbdbackup.$OPT_RESTORE_POINT" ;;
                    *)       die "Invalid restore point: $OPT_RESTORE_POINT (use latest, full, or number)" "restore_vm" "$EXIT_USAGE" ;;
                esac
                disk_cmd+=(--until "$until_cp")
                [[ $_disk_idx -eq 1 ]] && log_info "vmrestore.sh" "restore_vm" "Point-in-time: $until_cp"
            elif [[ "$OPT_RESTORE_POINT" != "latest" && "$btype" != "incremental" ]]; then
                [[ $_disk_idx -eq 1 ]] && log_warn "vmrestore.sh" "restore_vm" "Point-in-time ignored (backup type: $btype)"
            fi

            log_info "vmrestore.sh" "restore_vm" "Executing: ${disk_cmd[*]}"
            if ! run_logged "${disk_cmd[@]}"; then
                # Restore failed — rollback this disk's .pre-restore
                if [[ -n "$_orig" && -f "${_orig}.pre-restore" ]]; then
                    mv "${_orig}.pre-restore" "$_orig"
                    # Remove from prerestore_files list
                    local -a _tmp_pr=()
                    for _pf in "${_prerestore_files[@]}"; do
                        [[ "$_pf" != "${_orig}.pre-restore" ]] && _tmp_pr+=("$_pf")
                    done
                    _prerestore_files=("${_tmp_pr[@]}")
                    log_warn "vmrestore.sh" "restore_vm" "Restored original from .pre-restore after failure"
                fi
                _failed_disk="$_d"
                break
            fi

            # Clean up vmconfig.xml dropped by virtnbdrestore
            rm -f "$_tgt_dir/vmconfig.xml"

            # In-place post-processing
            if [[ -n "$_orig" ]]; then
                # Find restored file — virtnbdrestore uses original filename
                if [[ -f "$_orig" ]]; then
                    true  # already at correct path
                else
                    local _restored_file="$_tgt_dir/${_d}.qcow2"
                    if [[ -f "$_restored_file" ]]; then
                        mv "$_restored_file" "$_orig"
                        log_info "vmrestore.sh" "restore_vm" "Renamed: ${_d}.qcow2 → ${_dk_file[$_d]}"
                    else
                        log_warn "vmrestore.sh" "restore_vm" "Expected restored file not found at $_orig or $_restored_file"
                    fi
                fi

                # Ownership and permissions
                chown libvirt-qemu:libvirt-qemu "$_orig" 2>/dev/null || \
                    log_warn "vmrestore.sh" "restore_vm" "Failed to set ownership on $_orig"
                chmod 600 "$_orig" 2>/dev/null || true

                # Integrity check
                if _qcow_check_classified "$_orig"; then
                    log_info "vmrestore.sh" "restore_vm" "  $_d: ownership ✓ integrity ✓"
                elif [[ "${_QIMG_VERDICT:-}" == "UNVERIFIABLE" ]]; then
                    log_error "vmrestore.sh" "restore_vm" "INTEGRITY COULD NOT BE VERIFIED: $_orig"
                    if [[ -f "${_orig}.pre-restore" ]]; then
                        log_error "vmrestore.sh" "restore_vm" "Roll back: mv ${_orig}.pre-restore $_orig"
                    fi
                    _failed_disk="$_d"
                    break
                else
                    log_error "vmrestore.sh" "restore_vm" "INTEGRITY CHECK FAILED: $_orig"
                    if [[ -f "${_orig}.pre-restore" ]]; then
                        log_error "vmrestore.sh" "restore_vm" "Roll back: mv ${_orig}.pre-restore $_orig"
                    fi
                    _failed_disk="$_d"
                    break
                fi
            else
                # Staging: integrity check on extracted file.
                # 109-bugs audit item 6b — sort makes the pick deterministic,
                # but for multi-disk VMs the integrity check may still verify a
                # different disk's staged qcow2 than the loop's current $_d. A
                # follow-up fix should scope the glob to the disk identifier
                # (e.g. -name "*${_d}*.qcow2").
                local _stage_file
                _stage_file=$(find "$_tgt_dir" -maxdepth 1 -name "*.qcow2" -newer "$data_dir" 2>/dev/null | sort | head -1)
                if [[ -n "$_stage_file" ]]; then
                    ls -lh "$_stage_file" 2>/dev/null | while IFS= read -r line; do
                        log_info "vmrestore.sh" "restore_vm" "  $line"
                    done
                    if _qcow_check_classified "$_stage_file"; then
                        log_info "vmrestore.sh" "restore_vm" "  $_d: integrity ✓"
                    elif [[ "${_QIMG_VERDICT:-}" == "UNVERIFIABLE" ]]; then
                        log_error "vmrestore.sh" "restore_vm" "INTEGRITY COULD NOT BE VERIFIED: $_stage_file"
                    else
                        log_error "vmrestore.sh" "restore_vm" "INTEGRITY CHECK FAILED: $_stage_file"
                    fi
                fi
            fi
            _restored_disks+=("$_d")
        done

        # ── Post-loop: summary, warnings, cleanup notice ─────────────

        # Storage pool refresh (once)
        if [[ "$_inplace" == true ]]; then
            refresh_storage_pool "${_dk_dir[${disk_list[0]}]}"
        fi

        # Handle failure with partial completion
        if [[ -n "$_failed_disk" ]]; then
            if [[ ${#_restored_disks[@]} -gt 0 ]]; then
                local _restored_display
                _restored_display=$(printf '%s, ' "${_restored_disks[@]}")
                _restored_display="${_restored_display%, }"
                log_error "vmrestore.sh" "restore_vm" "Partial restore: $_restored_display succeeded, $_failed_disk FAILED"
                # List remaining skipped disks
                local _in_skipped=false
                for _d in "${disk_list[@]}"; do
                    [[ "$_d" == "$_failed_disk" ]] && _in_skipped=true && continue
                    [[ "$_in_skipped" == true ]] && log_error "vmrestore.sh" "restore_vm" "Skipped: $_d"
                done
            fi
            cleanup_pit_staging "$pit_input_dir"
            die "Disk restore failed for $_failed_disk" "restore_vm" "$EXIT_TOOL"
        fi

        # Checkpoint invalidation warning (once)
        if [[ "$_inplace" == true ]]; then
            echo ""
            log_warn "vmrestore.sh" "restore_vm" "═══════════════════════════════════════════════════════════"
            log_warn "vmrestore.sh" "restore_vm" "CHECKPOINT CHAIN INVALIDATED for '$vm_name'"
            log_warn "vmrestore.sh" "restore_vm" "═══════════════════════════════════════════════════════════"
            log_warn "vmrestore.sh" "restore_vm" "Replaced disk(s) no longer match existing QEMU checkpoint bitmaps."
            log_warn "vmrestore.sh" "restore_vm" ""
            log_warn "vmrestore.sh" "restore_vm" "If vmbackup ENABLE_AUTO_RECOVERY_ON_CHECKPOINT_CORRUPTION=\"yes\":"
            log_warn "vmrestore.sh" "restore_vm" "  → Next backup will auto-archive old chain and start a fresh FULL."
            log_warn "vmrestore.sh" "restore_vm" ""
            log_warn "vmrestore.sh" "restore_vm" "If \"warn\" (default):"
            log_warn "vmrestore.sh" "restore_vm" "  → Next backup will FAIL until checkpoints are manually cleaned."
            log_warn "vmrestore.sh" "restore_vm" "  → To clean: for cp in \$(virsh checkpoint-list $vm_name --name 2>/dev/null); do"
            log_warn "vmrestore.sh" "restore_vm" "       virsh checkpoint-delete $vm_name \$cp --metadata; done"
            log_warn "vmrestore.sh" "restore_vm" "═══════════════════════════════════════════════════════════"
        fi

        # .pre-restore cleanup notice (once, all files)
        if [[ ${#_prerestore_files[@]} -gt 0 ]]; then
            echo ""
            log_warn "vmrestore.sh" "restore_vm" "═══════════════════════════════════════════════════════════"
            log_warn "vmrestore.sh" "restore_vm" "ACTION REQUIRED: Remove .pre-restore file(s) once VM is confirmed working"
            local _total_pr_size=0
            for _pf in "${_prerestore_files[@]}"; do
                local _pf_size _pf_size_hr
                _pf_size=$(stat -c%s "$_pf" 2>/dev/null || echo 0)
                _pf_size_hr=$(du -sh "$_pf" 2>/dev/null | awk '{print $1}')
                log_warn "vmrestore.sh" "restore_vm" "  rm $_pf  ($_pf_size_hr)"
                _total_pr_size=$(( _total_pr_size + _pf_size ))
            done
            if [[ ${#_prerestore_files[@]} -gt 1 ]]; then
                local _total_pr_hr
                _total_pr_hr=$(_format_size "$_total_pr_size")
                log_warn "vmrestore.sh" "restore_vm" "  Total: $_total_pr_hr"
            fi
            log_warn "vmrestore.sh" "restore_vm" "═══════════════════════════════════════════════════════════"
        fi

        # Disk size comparisons
        if [[ "$_inplace" == true ]]; then
            for _d in "${_restored_disks[@]}"; do
                local _orig="${_dk_path[$_d]:-}"
                if [[ -n "$_orig" && -f "${_orig}.pre-restore" ]]; then
                    local old_vsize new_vsize
                    old_vsize=$(python3 -c "import json,sys; print(json.load(sys.stdin).get('virtual-size',0))" < <(qemu-img info --output=json "${_orig}.pre-restore" 2>/dev/null) 2>/dev/null || echo 0)
                    new_vsize=$(python3 -c "import json,sys; print(json.load(sys.stdin).get('virtual-size',0))" < <(qemu-img info --output=json "$_orig" 2>/dev/null) 2>/dev/null || echo 0)
                    if [[ "$old_vsize" -gt 0 && "$new_vsize" -gt 0 && "$old_vsize" != "$new_vsize" ]]; then
                        local old_hr new_hr
                        old_hr=$(_format_size "$old_vsize")
                        new_hr=$(_format_size "$new_vsize")
                        log_warn "vmrestore.sh" "restore_vm" "Disk $_d capacity changed: $old_hr → $new_hr (disk was resized since backup)"
                    fi
                fi
            done
        fi

        # Final summary
        local _restored_display
        _restored_display=$(printf '%s, ' "${_restored_disks[@]}")
        _restored_display="${_restored_display%, }"
        if [[ "$_inplace" == true ]]; then
            log_info "vmrestore.sh" "restore_vm" "Disk restore complete: $vm_name/$_restored_display — all ✓"
        else
            log_info "vmrestore.sh" "restore_vm" "Disk extract complete: $vm_name/$_restored_display → $OPT_RESTORE_PATH"
        fi

        cleanup_pit_staging "$pit_input_dir"
        return 0
    fi
    # ── End Disk-Restore Mode ────────────────────────────────────────────────

    # Build virtnbdrestore command via lib helper (UNI-014 M1).
    # Mode-dependent identity flags are collected here, then the helper
    # assembles the final argv with -i/-o once virtnbd_output_path is resolved.
    # PIT staging may later mutate cmd[2] (the -i input dir) — the helper
    # preserves the (virtnbdrestore, -i, <input>) prefix layout this assumes.
    local staging_dir=""
    local virtnbd_output_path="$OPT_RESTORE_PATH"
    local -a cmd=()
    local -a _identity_extras=()

    # VM definition strategy
    local new_identity=false
    if [[ "$OPT_SKIP_CONFIG" == false ]]; then
        local check_name="${OPT_NAME:-$vm_name}"
        if lv_domain_exists "$check_name"; then
            if is_dry_run; then
                log_warn "vmrestore.sh" "restore_vm" "[DRY RUN] VM '$check_name' exists (would need --force)"
            elif [[ "$OPT_FORCE" == true ]]; then
                log_warn "vmrestore.sh" "restore_vm" "Undefining existing VM: $check_name"
                lv_undefine_domain "$check_name" || \
                    log_warn "vmrestore.sh" "restore_vm" "Failed to undefine '$check_name' — continuing anyway"
            else
                die "VM '$check_name' already defined (use --force to override)" "restore_vm" "$EXIT_VM"
            fi
        fi

        if [[ -n "${OPT_NAME:-}" ]]; then
            # New identity: -c only (adjust paths), we define manually with new UUID/MACs
            new_identity=true
            _identity_extras+=(-c)
            # Set up staging directory under restore-path (same filesystem = atomic mv)
            staging_dir="$OPT_RESTORE_PATH/.vmrestore-staging-$$"
            virtnbd_output_path="$staging_dir"
            log_info "vmrestore.sh" "restore_vm" "Restore mode: clone (new identity — new UUID, new MACs)"
            log_info "vmrestore.sh" "restore_vm" "Clone name: $OPT_NAME — staging dir: $staging_dir"
        else
            # Disaster recovery: -c -D preserves original UUID/MACs
            # -N is mandatory: without it virtnbdrestore prefixes "restore_" to the name
            _identity_extras+=(-c -D -U "qemu:///system" -N "$vm_name")
            log_info "vmrestore.sh" "restore_vm" "Restore mode: disaster recovery (original identity preserved)"
        fi
    fi

    # Assemble argv: helper produces (virtnbdrestore, -i, <data_dir>, -o, <output>, <identity_extras...>).
    build_virtnbdrestore_args "$data_dir" "$virtnbd_output_path" "restore" "${_identity_extras[@]}"
    cmd=("${_VIRTNBD_ARGS[@]}")

    # Single disk filter
    [[ -n "${OPT_DISK:-}" ]] && cmd+=(-d "$OPT_DISK")

    # Point-in-time restore (incremental backups only)
    local until_cp=""
    if [[ "$OPT_RESTORE_POINT" != "latest" && "$btype" == "incremental" ]]; then
        case "$OPT_RESTORE_POINT" in
            full)    until_cp="virtnbdbackup.0" ;;
            [0-9]*)  until_cp="virtnbdbackup.$OPT_RESTORE_POINT" ;;
            *)       die "Invalid restore point: $OPT_RESTORE_POINT (use latest, full, or number)" "restore_vm" "$EXIT_USAGE" ;;
        esac
        cmd+=(--until "$until_cp")
        log_info "vmrestore.sh" "restore_vm" "Point-in-time: $until_cp"
    elif [[ "$OPT_RESTORE_POINT" != "latest" && "$btype" != "incremental" ]]; then
        log_warn "vmrestore.sh" "restore_vm" "Point-in-time ignored (backup type: $btype)"
    fi

    # ── PIT staging (DR/clone mode) ──────────────────────────────────────
    # When point-in-time targets a checkpoint with a different disk set,
    # create a staging input directory so virtnbdrestore reads the correct vmconfig.
    local pit_input_dir=""
    if [[ -n "$until_cp" && "$btype" == "incremental" ]]; then
        local _pit_target_cp=""
        case "$OPT_RESTORE_POINT" in
            full)   _pit_target_cp="0" ;;
            [0-9]*) _pit_target_cp="$OPT_RESTORE_POINT" ;;
        esac
        if [[ -n "$_pit_target_cp" ]]; then
            local _pit_latest_cp
            _pit_latest_cp=$(find "$data_dir/checkpoints" -maxdepth 1 -name "virtnbdbackup.*.xml" -printf '%f\n' 2>/dev/null \
                | sed 's/virtnbdbackup\.\([0-9]*\)\.xml/\1/' | sort -n | tail -1)
            local _pit_target_disks _pit_latest_disks
            _pit_target_disks=$(enumerate_disks_at_checkpoint "$data_dir" "$_pit_target_cp")
            _pit_latest_disks=$(enumerate_disks_at_checkpoint "$data_dir" "$_pit_latest_cp")
            if [[ "$_pit_target_disks" != "$_pit_latest_disks" ]]; then
                log_warn "vmrestore.sh" "restore_vm" "Disk configuration changed between checkpoint $_pit_target_cp and latest ($_pit_latest_cp)."
                log_warn "vmrestore.sh" "restore_vm" "  Checkpoint $_pit_target_cp: $_pit_target_disks"
                log_warn "vmrestore.sh" "restore_vm" "  Latest (CP $_pit_latest_cp): $_pit_latest_disks"
                log_warn "vmrestore.sh" "restore_vm" "  Restoring with checkpoint $_pit_target_cp disk configuration."
                if ! is_dry_run; then
                    pit_input_dir=$(create_pit_staging "$data_dir" "$_pit_target_cp") || \
                        die "Failed to create PIT staging directory" "restore_vm" "$EXIT_STORAGE"
                    # FF-193: track in the PARENT shell (the append inside
                    # create_pit_staging ran dead in this $() subshell) so the
                    # EXIT/SIGINT trap removes the staging symlink farm.
                    _VMRESTORE_STAGING_DIRS+=("$pit_input_dir")
                    log_info "vmrestore.sh" "restore_vm" "PIT staging directory: $pit_input_dir"
                    # Replace -i in cmd array: element 0=virtnbdrestore, 1=-i, 2=data_dir
                    cmd[2]="$pit_input_dir"
                fi
            fi
        fi
    fi

    # ── Pre-flight free space check ──
    preflight_free_space "$data_dir" "$OPT_RESTORE_PATH" "$btype" "$until_cp"

    # ── Pre-flight disk safety checks ──
    # predict_output_files computes FINAL paths (after rename for clones)
    local use_c_flag=false
    [[ "$OPT_SKIP_CONFIG" == false ]] && use_c_flag=true
    local _predicted_ok=true
    # Fix 5: When PIT staging detected a disk config change, use the target CP's vmconfig
    local _predict_cfg_override=""
    if [[ -n "$pit_input_dir" ]]; then
        # Real run: use vmconfig from the PIT staging dir
        _predict_cfg_override=$(ls -1 "$pit_input_dir"/vmconfig.virtnbdbackup.*.xml 2>/dev/null | head -1)
    elif [[ -n "${_pit_target_cp:-}" && "${_pit_target_disks:-}" != "${_pit_latest_disks:-}" ]]; then
        # Dry run: predict from the target checkpoint's per-checkpoint vmconfig.
        _predict_cfg_override="$data_dir/vmconfig.virtnbdbackup.${_pit_target_cp}.xml"
        if [[ ! -f "$_predict_cfg_override" ]]; then
            # FF-195: the former fallback picked config/*.xml by checkpoint ORDINAL
            # (`sort | sed -n "$((cp+1))p"`) — the same unsound mapping fixed in
            # create_pit_staging (FF-194). vmbackup's backup_vm_config writes
            # change-triggered timestamped configs plus a _FIRST marker, not one
            # per checkpoint, so the ordinal selects a wrong-vintage XML and
            # predict_output_files then mis-predicts output topology / the clone
            # rename map. Do NOT guess: without the target checkpoint's own
            # vmconfig the target topology cannot be reconstructed, and a real PIT
            # restore now fails closed there (create_pit_staging). Leave the
            # override empty (best-effort preview from the latest config) and warn.
            # Dry-run only: log + assignment, no mutation.
            _predict_cfg_override=""
            log_warn "vmrestore.sh" "restore_vm" "[DRY RUN] No per-checkpoint vmconfig (vmconfig.virtnbdbackup.${_pit_target_cp}.xml) for checkpoint ${_pit_target_cp}; a real PIT restore would fail closed here — output-topology preview may reflect the latest checkpoint, not ${_pit_target_cp}"
        fi
    fi
    if predict_output_files "$data_dir" "$OPT_RESTORE_PATH" "$use_c_flag" "${OPT_DISK:-}" "${OPT_NAME:-}" "$_predict_cfg_override"; then
        preflight_disk_safety "$vm_name" "$OPT_DRY_RUN" "$OPT_FORCE"
    else
        # VMR1: a clone whose output files cannot be predicted has no rename map
        # (which raw file -> which clone-<dev>.qcow2). Promotion and the post-
        # restore qemu-img check are both gated on a successful prediction, so a
        # clone would otherwise restore into the staging dir, skip promotion, and
        # the EXIT trap would then delete the only copy just made -- all while the
        # summary printed "defined ✓". Fail closed BEFORE the destructive engine
        # runs rather than restore-then-delete. DR / in-place overwrite predicted
        # paths and don't depend on staging promotion, so they may still proceed.
        if [[ "$new_identity" == true ]] && ! is_dry_run; then
            die "Cannot predict clone output files for '$OPT_NAME' (no usable config/disks in backup) — refusing a clone that cannot be safely promoted or validated" "restore_vm" "$EXIT_VM"
        fi
        log_warn "vmrestore.sh" "restore_vm" "Could not predict output files — skipping disk safety checks"
        _predicted_ok=false
    fi

    # Track new UUID for TPM (only set in new-identity mode)
    local new_uuid=""

    # DR + --force: remove existing disk files that virtnbdrestore would refuse to overwrite
    # Only in non-clone mode (clone uses staging dir, no collision with output path)
    if [[ "$new_identity" == false && "$OPT_FORCE" == true && "$_predicted_ok" == true ]] && ! is_dry_run; then
        for pred_file in "${_PREDICTED_FILES[@]}"; do
            if [[ -f "$pred_file" ]]; then
                rm -f "$pred_file"
                log_info "vmrestore.sh" "restore_vm" "Removed existing file for DR overwrite: $pred_file"
            fi
        done
    fi

    if [[ "$OPT_SKIP_CONFIG" == true ]]; then
        log_info "vmrestore.sh" "restore_vm" "Restore mode: data-only (--skip-config — disk restore without VM definition)"
    fi

    if is_dry_run; then
        log_info "vmrestore.sh" "restore_vm" "[DRY RUN] ${cmd[*]}"
        if [[ -n "$pit_input_dir" || ( -n "$until_cp" && "$_pit_target_disks" != "${_pit_latest_disks:-}" ) ]]; then
            log_info "vmrestore.sh" "restore_vm" "[DRY RUN] PIT staging: would create staging dir with checkpoint $OPT_RESTORE_POINT vmconfig"
        fi
        if [[ "$new_identity" == true ]]; then
            log_info "vmrestore.sh" "restore_vm" "[DRY RUN] Would define '$OPT_NAME' with new UUID and MACs"
            log_info "vmrestore.sh" "restore_vm" "[DRY RUN] Staging dir: $staging_dir"
            if [[ "$_predicted_ok" == true ]]; then
                stage_and_rename_clone_disks "$OPT_NAME" "$staging_dir" "$OPT_RESTORE_PATH" true
            fi
        fi
    else
        mkdir -p "$OPT_RESTORE_PATH"
        # Create staging dir for clone mode
        if [[ -n "$staging_dir" ]]; then
            mkdir -p "$staging_dir"
            # UNI-010: Track for SIGINT/SIGTERM/EXIT-trap cleanup so a
            # mid-restore interrupt removes the partial staging tree
            # (success path explicitly removes it after stage_and_rename).
            _VMRESTORE_STAGING_DIRS+=("$staging_dir")
            log_info "vmrestore.sh" "restore_vm" "Created staging directory: $staging_dir"
        fi

        # Provision vmconfig XML for archived chains that lack vmconfig.virtnbdbackup.*.xml
        # virtnbdrestore requires this file even without -c/-D flags
        # Skip when PIT staging is active — staging dir already has the correct vmconfig
        local _provisioned_vmconfig=""
        if [[ -z "$pit_input_dir" ]] && ! ls "$data_dir"/vmconfig.virtnbdbackup.*.xml &>/dev/null; then
            local _cfg_xml=""
            for _search_dir in "$data_dir/config" "$(dirname "$data_dir")/config"; do
                [[ -d "$_search_dir" ]] || continue
                _cfg_xml=$(ls -1t "$_search_dir"/*.xml 2>/dev/null | head -1 || true)
                [[ -n "$_cfg_xml" ]] && break
            done
            if [[ -n "$_cfg_xml" && -f "$_cfg_xml" ]]; then
                cp "$_cfg_xml" "$data_dir/vmconfig.virtnbdbackup.0.xml"
                _provisioned_vmconfig="$data_dir/vmconfig.virtnbdbackup.0.xml"
                # F-vmr2514: mirror to the trap global so any die/signal before
                # the success-path rm restores the read-only backup/archive tree.
                _VMRESTORE_PROVISIONED_VMCONFIG="$_provisioned_vmconfig"
                log_info "vmrestore.sh" "restore_vm" "Provisioned vmconfig from: $_cfg_xml"
            else
                log_warn "vmrestore.sh" "restore_vm" "No vmconfig XML found — virtnbdrestore may fail"
            fi
        fi

        log_info "vmrestore.sh" "restore_vm" "Executing: ${cmd[*]}"

        local restore_ok=false
        if run_logged "${cmd[@]}"; then
            log_info "vmrestore.sh" "restore_vm" "Disk restored to $virtnbd_output_path"
            restore_ok=true
        elif [[ "$OPT_SKIP_CONFIG" == false ]]; then
            # Primary failed — retry without -D (disk restore only)
            log_warn "vmrestore.sh" "restore_vm" "virtnbdrestore failed, retrying disk-only..."
            local retry=(virtnbdrestore -i "${pit_input_dir:-$data_dir}" -o "$virtnbd_output_path" -c -U "qemu:///system")
            [[ -n "${OPT_DISK:-}" ]] && retry+=(-d "$OPT_DISK")
            [[ -n "$until_cp" ]] && retry+=(--until "$until_cp")
            if run_logged "${retry[@]}"; then
                restore_ok=true
            else
                # Clean up staging dirs on failure before dying
                [[ -n "$staging_dir" && -d "$staging_dir" ]] && rm -rf "$staging_dir"
                cleanup_pit_staging "$pit_input_dir"
                die "Disk restore failed" "restore_vm" "$EXIT_TOOL"
            fi
        else
            [[ -n "$staging_dir" && -d "$staging_dir" ]] && rm -rf "$staging_dir"
            cleanup_pit_staging "$pit_input_dir"
            die "Disk restore failed" "restore_vm" "$EXIT_TOOL"
        fi

        # Move + rename clone disks from staging to final location
        local disk_rename_map=""
        if [[ "$new_identity" == true && "$_predicted_ok" == true && "$restore_ok" == true ]]; then
            # FF-11: promotion is the point of no return for a clone. If any disk
            # mv fails, PRESERVE staging (it holds the only copy of the un-promoted
            # disks) and fail closed rather than define a clone over missing paths.
            # Un-register staging first so the die's EXIT trap does not rm -rf the
            # very disks the operator is told to recover.
            if ! stage_and_rename_clone_disks "$OPT_NAME" "$staging_dir" "$OPT_RESTORE_PATH" false; then
                _vmrestore_unregister_staging "$staging_dir"
                die "Clone disk promotion FAILED: one or more disks could not be moved from staging to '$OPT_RESTORE_PATH'. The staging dir '$staging_dir' is PRESERVED and holds the ONLY copy of any un-promoted disks (do NOT delete it); already-promoted disks are in '$OPT_RESTORE_PATH'. NO clone was defined. Finish manually: move the remaining staged disks into '$OPT_RESTORE_PATH' with the clone-named filenames, define from the config XML after adjusting <source file> paths, then remove the staging dir." "restore_vm" "$EXIT_TOOL"
            fi
            disk_rename_map="$_DISK_RENAME_MAP"
            # All disks promoted. Move the restore-written vmconfig.xml (whose
            # <source file> paths were rewritten to the STAGING paths that key
            # _DISK_RENAME_MAP) to the restore path so define_new_identity() reads
            # it. FF-11: if this move fails, do NOT fall through to define — the
            # fallback config XML is the BACKUP config whose <source file> still
            # points at the SOURCE VM's live disks, and the staging-keyed map
            # would rewrite nothing, defining the clone onto the source disks.
            if [[ -f "$staging_dir/vmconfig.xml" ]]; then
                if ! mv "$staging_dir/vmconfig.xml" "$OPT_RESTORE_PATH/vmconfig.xml"; then
                    _vmrestore_unregister_staging "$staging_dir"
                    die "All clone disks were promoted to '$OPT_RESTORE_PATH', but the restore-written vmconfig.xml could not be moved and is PRESERVED in '$staging_dir'. NO clone was defined. Define manually from the config XML (the staging copy at '$staging_dir/vmconfig.xml', or $data_dir/config/*.xml) after adjusting <source file> paths to the final disk locations; do NOT start any clone whose XML still points at the source VM's disk paths." "restore_vm" "$EXIT_TOOL"
                fi
            fi
            rm -rf "$staging_dir"
            log_info "vmrestore.sh" "restore_vm" "Staging directory cleaned up"
        fi

        # Clean up PIT staging directory (input symlinks + vmconfig copy)
        cleanup_pit_staging "$pit_input_dir"

        # Define VM from config (when virtnbdrestore didn't -D, or new-identity)
        if [[ "$OPT_SKIP_CONFIG" == false && "$new_identity" == true ]]; then
            # New identity: find the output XML, strip UUID + MACs, rename, define
            local out_xml="$OPT_RESTORE_PATH/vmconfig.xml"
            if [[ ! -f "$out_xml" ]]; then
                # Fallback: vmbackup config/ directory
                out_xml=$(ls -1t "$data_dir/config"/*.xml 2>/dev/null | head -1 || true)
            fi
            if [[ -n "$out_xml" && -f "$out_xml" ]]; then
                # Pair the clone's NVRAM with the chain endpoint so UEFI vars
                # match the restored disk state (prevents "BdsDxe: No mapping"
                # for SecureBoot guests when the source VM has run since the
                # backup was taken).
                local _ni_endpoint _ni_chain_nvram="" _ni_fcn_rc=0
                _ni_endpoint=$(chain_endpoint_cp "$data_dir" "${OPT_RESTORE_POINT:-}")
                if [[ -n "$_ni_endpoint" ]]; then
                    # VMR4: tell ambiguous (rc 2) apart from not-found (rc 1). An
                    # ambiguous NVRAM match must not silently collapse to "no
                    # NVRAM": guessing the wrong UEFI varstore can break Secure
                    # Boot / BitLocker on the clone. rc 1 keeps the not-found path
                    # (define without a paired NVRAM, exactly as before).
                    _ni_chain_nvram=$(find_chain_nvram "$data_dir" "$vm_name" "$_ni_endpoint") || _ni_fcn_rc=$?
                    if (( _ni_fcn_rc == 2 )); then
                        die "Ambiguous chain NVRAM for '$vm_name' at endpoint cp ${_ni_endpoint}: more than one *_VARS*.fd backup matches (candidates logged above). Refusing to guess which UEFI/NVRAM state to pair with the restored disks — a wrong choice can break Secure Boot or BitLocker unlock on the clone. Resolve the duplicates in '$data_dir' and retry." "restore_vm" "$EXIT_VM"
                    fi
                fi
                new_uuid=$(define_new_identity "$out_xml" "$OPT_NAME" false "$disk_rename_map" "$_ni_chain_nvram") || \
                    log_warn "vmrestore.sh" "restore_vm" "VM define failed (restore disks OK — define manually)"
            else
                log_warn "vmrestore.sh" "restore_vm" "No config XML found — define VM manually"
            fi
        elif [[ "$OPT_SKIP_CONFIG" == false && "$new_identity" == false ]]; then
            # In-place NVRAM restore: pair live NVRAM with the chain endpoint.
            # Without this, an in-place restore of an older period over a still-
            # running source VM leaves UEFI variables drifted from the restored
            # disk, causing "BdsDxe: No mapping" / boot failure for SecureBoot
            # guests. The previous live NVRAM is preserved as a timestamped
            # backup so manual rollback is possible.
            local _ip_endpoint _ip_chain_nvram="" _ip_fcn_rc=0
            _ip_endpoint=$(chain_endpoint_cp "$data_dir" "${OPT_RESTORE_POINT:-}")
            if [[ -n "$_ip_endpoint" ]]; then
                # VMR4: ambiguous (rc 2) => fail closed; rc 1 => unchanged not-found.
                _ip_chain_nvram=$(find_chain_nvram "$data_dir" "$vm_name" "$_ip_endpoint") || _ip_fcn_rc=$?
                if (( _ip_fcn_rc == 2 )); then
                    die "Ambiguous chain NVRAM for '$vm_name' at endpoint cp ${_ip_endpoint}: more than one *_VARS*.fd backup matches (candidates logged above). Refusing to guess which UEFI/NVRAM state to restore in place — a wrong choice can break Secure Boot or BitLocker unlock. Resolve the duplicates in '$data_dir' and retry." "restore_vm" "$EXIT_VM"
                fi
            fi
            if [[ -n "$_ip_chain_nvram" && -f "$_ip_chain_nvram" ]]; then
                local _ip_cfg_xml="" _ip_live_nvram=""
                _ip_cfg_xml="$data_dir/vmconfig.virtnbdbackup.${_ip_endpoint}.xml"
                [[ -f "$_ip_cfg_xml" ]] || \
                    _ip_cfg_xml=$(ls -1t "$data_dir"/vmconfig.virtnbdbackup.*.xml 2>/dev/null | head -1 || true)
                if [[ -n "$_ip_cfg_xml" && -f "$_ip_cfg_xml" ]]; then
                    _ip_live_nvram=$(grep -oP '<nvram[^>]*>\K[^<]+' "$_ip_cfg_xml" 2>/dev/null | head -1 || true)
                fi
                if [[ -n "$_ip_live_nvram" ]]; then
                    if [[ -f "$_ip_live_nvram" ]]; then
                        local _ip_ts _ip_bak
                        _ip_ts=$(date +%Y%m%dT%H%M%S)
                        _ip_bak="${_ip_live_nvram}.before-restore.${_ip_ts}"
                        if cp -a "$_ip_live_nvram" "$_ip_bak"; then
                            log_info "vmrestore.sh" "restore_vm" "Backed up live NVRAM: $_ip_bak"
                        else
                            log_warn "vmrestore.sh" "restore_vm" "Failed to back up live NVRAM ($_ip_live_nvram) — proceeding with overwrite"
                        fi
                    fi
                    if cp "$_ip_chain_nvram" "$_ip_live_nvram"; then
                        chown libvirt-qemu:libvirt-qemu "$_ip_live_nvram" 2>/dev/null || true
                        chmod 600 "$_ip_live_nvram" 2>/dev/null || true
                        log_info "vmrestore.sh" "restore_vm" "NVRAM restored from chain endpoint cp ${_ip_endpoint}: $_ip_chain_nvram → $_ip_live_nvram"
                    else
                        # VMR5: an in-place DR restore is identity-bearing. We have
                        # restored disks AND a chain NVRAM to pair, but the copy
                        # failed — leaving UEFI/NVRAM vars drifted from the restored
                        # disk is a failed identity restore, not a warning.
                        die "In-place restore of '$vm_name': failed to copy chain NVRAM into place ($_ip_chain_nvram → $_ip_live_nvram). Refusing to leave UEFI/NVRAM vars drifted from the restored disks — the VM may fail Secure Boot or BitLocker unlock. The previous live NVRAM was saved alongside as *.before-restore.* for manual recovery." "restore_vm" "$EXIT_VM"
                    fi
                else
                    # VMR5: chain NVRAM exists in the backup but we cannot resolve
                    # where the live NVRAM lives — we cannot pair them. For an
                    # in-place DR restore that is a failed identity restore, not a
                    # warning: fail closed rather than imply success with drift.
                    die "In-place restore of '$vm_name': the backup has chain NVRAM to restore but the live NVRAM path could not be resolved from the backup vmconfig. Refusing to leave UEFI/NVRAM vars drifted from the restored disks (Secure Boot / BitLocker may break). Restore the NVRAM manually from '$_ip_chain_nvram'." "restore_vm" "$EXIT_VM"
                fi
            else
                log_warn "vmrestore.sh" "restore_vm" \
                    "No chain NVRAM in backup at endpoint cp ${_ip_endpoint:-?} — live host NVRAM left untouched. VM may fail to boot if SecureBoot state has drifted since the backup."
            fi
            # Disaster recovery: if virtnbdrestore -D didn't define, try fallback
            if ! lv_domain_exists "$vm_name"; then
                local fb_xml
                fb_xml=$(ls -1t "$data_dir/config"/*.xml 2>/dev/null | head -1 || true)
                if [[ -n "$fb_xml" ]]; then
                    log_info "vmrestore.sh" "restore_vm" "Defining VM from backup config: $fb_xml"
                    lv_define_xml "$fb_xml" >/dev/null || log_warn "vmrestore.sh" "restore_vm" "virsh define failed"
                fi
            fi
            # VMR2: a DR restore (--skip-config is false here) must end with a
            # defined VM. If neither virtnbdrestore -D nor the fallback define
            # produced one, fail loudly now rather than print a false "defined ✓".
            if ! lv_domain_exists "$vm_name"; then
                die "DR restore of '$vm_name' could not define the VM (virtnbdrestore -D and the fallback define both failed) — restore the domain XML manually from $data_dir/config" "restore_vm" "$EXIT_VM"
            fi

            # virtnbdrestore -D always strips UUID — re-inject original so TPM/identity is preserved
            if lv_domain_exists "$vm_name"; then
                local orig_uuid=""
                local _src_xml
                _src_xml=$(ls -1t "$data_dir"/vmconfig.virtnbdbackup.*.xml 2>/dev/null | head -1 || true)
                # Archived chains may lack vmconfig — fall back to config/ dir
                if [[ -z "$_src_xml" ]]; then
                    for _uuid_cdir in "$data_dir/config" "$(dirname "$data_dir")/config"; do
                        [[ -d "$_uuid_cdir" ]] || continue
                        _src_xml=$(ls -1t "$_uuid_cdir"/*.xml 2>/dev/null | head -1 || true)
                        [[ -n "$_src_xml" ]] && break
                    done
                fi
                if [[ -n "$_src_xml" ]]; then
                    orig_uuid=$(grep -oP '<uuid>\K[^<]+' "$_src_xml" 2>/dev/null || true)
                fi
                if [[ -n "$orig_uuid" ]]; then
                    local current_uuid
                    current_uuid=$(lv_domain_uuid "$vm_name" || true)
                    if [[ "$current_uuid" != "$orig_uuid" ]]; then
                        log_info "vmrestore.sh" "restore_vm" "Re-injecting original UUID: $orig_uuid (virtnbdrestore assigned: $current_uuid)"
                        local _fixxml _origxml
                        _fixxml=$(mktemp /tmp/vmrestore-fixuuid-XXXXXX.xml)
                        # VMR2: keep a pre-sed, known-good copy (the domain exactly
                        # as virtnbdrestore defined it) so a failed re-inject define
                        # can fall back to a working definition instead of leaving
                        # the VM undefined.
                        _origxml=$(mktemp /tmp/vmrestore-origxml-XXXXXX.xml)
                        lv_dump_xml "$vm_name" --inactive > "$_fixxml"
                        cp "$_fixxml" "$_origxml"
                        sed -i "s|<uuid>[^<]*</uuid>|<uuid>$orig_uuid</uuid>|" "$_fixxml"
                        # Must undefine first — virsh refuses UUID change on existing domain
                        # Backup NVRAM before undefine --nvram (which deletes it)
                        local _nvram_path _nvram_bak=""
                        _nvram_path=$(grep -oP '<nvram[^>]*>\K[^<]+' "$_fixxml" 2>/dev/null || true)
                        if [[ -n "$_nvram_path" && -f "$_nvram_path" ]]; then
                            _nvram_bak=$(mktemp /tmp/vmrestore-nvram-XXXXXX.fd)
                            cp "$_nvram_path" "$_nvram_bak"
                        fi
                        lv_undefine_domain "$vm_name" || true
                        # Restore NVRAM before redefine
                        if [[ -n "$_nvram_bak" && -f "$_nvram_bak" ]]; then
                            cp "$_nvram_bak" "$_nvram_path"
                            rm -f "$_nvram_bak"
                        fi
                        if lv_define_xml "$_fixxml" >/dev/null 2>&1; then
                            log_info "vmrestore.sh" "restore_vm" "UUID restored to $orig_uuid"
                        elif lv_define_xml "$_origxml" >/dev/null 2>&1; then
                            # Re-inject failed but the VM is back with its prior
                            # (virtnbdrestore-assigned) identity — disks are safe and
                            # the domain exists; only the original UUID is missing.
                            log_warn "vmrestore.sh" "restore_vm" "UUID re-inject failed; VM re-defined with assigned UUID $current_uuid (original $orig_uuid NOT restored — TPM may need manual unlock)"
                        else
                            # Both defines failed: the VM is now undefined. That is a
                            # VM-level failure, not a soft warning — surface it and
                            # name the XML the operator can recover from by hand.
                            die "DR UUID re-inject left '$vm_name' undefined and re-define failed — recover manually: virsh define $_origxml" "restore_vm" "$EXIT_VM"
                        fi
                        rm -f "$_fixxml" "$_origxml"
                    fi
                fi
                # Log preserved MAC addresses for DR verification
                local _dr_macs
                _dr_macs=$(lv_list_iface_macs "$vm_name" || true)
                if [[ -n "$_dr_macs" ]]; then
                    while IFS= read -r _m; do
                        log_info "vmrestore.sh" "restore_vm" "MAC preserved: $_m"
                    done <<< "$_dr_macs"
                fi
            fi
        fi

        # Clean up provisioned vmconfig if we created one (happy path); clear
        # the trap global so _vmrestore_cleanup no-ops on a clean exit (F-vmr2514).
        [[ -n "${_provisioned_vmconfig:-}" && -f "$_provisioned_vmconfig" ]] && rm -f "$_provisioned_vmconfig"
        _VMRESTORE_PROVISIONED_VMCONFIG=""

        # Post-restore validation: only check files we actually restored
        if [[ "$restore_ok" == true && "$_predicted_ok" == true && ${#_PREDICTED_FILES[@]} -gt 0 ]]; then
            local _any_corrupt=false
            local _any_unverified=false
            for _qcow in "${_PREDICTED_FILES[@]}"; do
                # FF-169: _PREDICTED_FILES is the definitive expected output set
                # (it gated the collision preflight). virtnbdrestore can exit 0
                # having written nothing for a disk, so a MISSING predicted file
                # after a "successful" engine run means the restore is INCOMPLETE
                # relative to the VM's own domain configuration — fail, not skip.
                if [[ ! -f "$_qcow" ]]; then
                    log_error "vmrestore.sh" "restore_vm" "Restore INCOMPLETE: predicted output missing (engine reported success but did not write it): $_qcow"
                    _any_corrupt=true
                    continue
                fi
                if _qcow_check_classified "$_qcow"; then
                    log_info "vmrestore.sh" "restore_vm" "Disk integrity OK: $(basename "$_qcow")"
                elif [[ "${_QIMG_VERDICT:-}" == "UNVERIFIABLE" ]]; then
                    log_error "vmrestore.sh" "restore_vm" "Restored image COULD NOT BE VERIFIED: $_qcow"
                    _any_unverified=true
                else
                    log_error "vmrestore.sh" "restore_vm" "Restored image FAILED integrity check: $_qcow"
                    _any_corrupt=true
                fi
            done
            if [[ "$_any_corrupt" == true ]]; then
                log_error "vmrestore.sh" "restore_vm" "One or more restored images are corrupt or INCOMPLETE (never written, ENOSPC, or I/O error)"
                die "Restore produced corrupt or incomplete disk images" "restore_vm" "$EXIT_STORAGE"
            elif [[ "$_any_unverified" == true ]]; then
                log_error "vmrestore.sh" "restore_vm" "One or more restored images COULD NOT BE VERIFIED (integrity check did not complete)"
                die "Restore verification incomplete — could not verify restored disk images" "restore_vm" "$EXIT_STORAGE"
            fi
        fi

        # Show restored files
        if [[ "$_predicted_ok" == true && ${#_PREDICTED_FILES[@]} -gt 0 ]]; then
            for _rfile in "${_PREDICTED_FILES[@]}"; do
                [[ -f "$_rfile" ]] || continue
                ls -lh "$_rfile" 2>/dev/null | while IFS= read -r line; do
                    log_info "vmrestore.sh" "restore_vm" "  $line"
                done
            done
        fi

        # Refresh libvirt storage pool so new volumes are discovered by virt-manager
        refresh_storage_pool "$OPT_RESTORE_PATH"
    fi

    # TPM state (pass new UUID for new-identity mode)
    # Skip TPM restore when --skip-config is set (data-only restore should not
    # touch the live VM's TPM state)
    #
    # INT-22: capture restore_tpm rc so the summary reflects reality. rc=0 means
    # either "restored OK" or "no TPM in this backup" — distinguish using the
    # backup-side marker/tpm-state dir presence. rc=1 means attempted-and-failed
    # (validation failed, UUID unresolved, etc.) — disk restore is unaffected
    # but the user will need their BitLocker recovery key / LUKS passphrase to
    # boot the cloned VM. NOT fatal: the disk is intact and recoverable.
    local _tpm_applicable=false
    local _tpm_rc=0
    if [[ "$OPT_SKIP_TPM" == false && "$OPT_SKIP_CONFIG" == false ]]; then
        local tpm_name="${OPT_NAME:-$vm_name}"
        if [[ -f "$data_dir/.tpm-backup-marker" || -d "$data_dir/tpm-state" ]]; then
            _tpm_applicable=true
        fi
        # FF-12: clone-mode TPM guard. In clone mode the clone's OWN UUID is the
        # only safe restore_tpm target. When it is unresolvable (new_uuid empty —
        # define_new_identity defined the domain but could not read its UUID
        # back), restore_tpm's UUID ladder would fall back to BACKUP_METADATA.txt,
        # which carries the ORIGINAL source VM's UUID (written capture-side at
        # modules/tpm_backup_module.sh). restore_tpm would then move the RUNNING
        # source VM's swtpm state aside and copy the backup over it: silent
        # cross-VM TPM/BitLocker corruption. Gate on the tpm-state/ dir — the exact
        # precondition restore_tpm mutates under: a marker-only backup with no
        # tpm-state/ returns 0 at restore_tpm's 1500-1503 arm without ever
        # computing a target, so it must fall through to the HEAD-identical quiet
        # path, NOT this skip. When tpm-state/ IS present (incl. archived chains
        # with no marker), skip entirely, never touch the source, and warn loudly
        # so the operator brings the clone's recovery credential to first boot.
        local _tpm_clone_skip=false
        if [[ "$new_identity" == true && -d "$data_dir/tpm-state" && -z "${new_uuid:-}" ]]; then
            _tpm_clone_skip=true
            if [[ "$OPT_DRY_RUN" == true ]]; then
                log_info "vmrestore.sh" "restore_vm" "[DRY RUN] Clone '$tpm_name' TPM: would restore to the clone's NEW UUID (assigned at define time — unknown in preview), or SKIP with a loud warning if that UUID cannot be read back; the source VM's swtpm is never touched either way"
            else
                _tpm_rc=1  # skipped, NOT restored: summary must render TPM ✗, never ✓
                log_warn "vmrestore.sh" "restore_vm" "TPM restore SKIPPED for clone '$tpm_name' (non-fatal — disk is intact)"
                log_warn "vmrestore.sh" "restore_vm" "============================================================"
                log_warn "vmrestore.sh" "restore_vm" "  ACTION REQUIRED: TPM state was NOT restored for clone '$tpm_name'."
                log_warn "vmrestore.sh" "restore_vm" "  Its UUID could not be resolved, so restoring the TPM could"
                log_warn "vmrestore.sh" "restore_vm" "  have misdirected onto the SOURCE VM's swtpm state — refused, to keep"
                log_warn "vmrestore.sh" "restore_vm" "  the source VM untouched. The clone's disk restore succeeded"
                log_warn "vmrestore.sh" "restore_vm" "  and it will boot, but its TPM is empty until you supply the"
                log_warn "vmrestore.sh" "restore_vm" "  recovery credential at first boot:"
                log_warn "vmrestore.sh" "restore_vm" "    - Windows + BitLocker: 48-digit BitLocker recovery key"
                log_warn "vmrestore.sh" "restore_vm" "    - Linux LUKS-via-TPM: fall back to LUKS passphrase"
                log_warn "vmrestore.sh" "restore_vm" "============================================================"
            fi
        else
            restore_tpm "$tpm_name" "$data_dir" "$OPT_DRY_RUN" "$new_uuid" && _tpm_rc=0 || _tpm_rc=$?
        fi
        if [[ "$_tpm_applicable" == true && "$_tpm_rc" -ne 0 && "$OPT_DRY_RUN" != true && "$_tpm_clone_skip" == false ]]; then
            log_warn "vmrestore.sh" "restore_vm" "TPM restore failed (non-fatal — disk is intact)"
            log_warn "vmrestore.sh" "restore_vm" "============================================================"
            log_warn "vmrestore.sh" "restore_vm" "  ACTION REQUIRED: TPM state was NOT restored for this VM."
            log_warn "vmrestore.sh" "restore_vm" "  The disk restore succeeded and the VM will boot, but"
            log_warn "vmrestore.sh" "restore_vm" "  any TPM-bound secrets are inaccessible until you supply"
            log_warn "vmrestore.sh" "restore_vm" "  the recovery credential at first boot:"
            log_warn "vmrestore.sh" "restore_vm" "    - Windows + BitLocker: 48-digit BitLocker recovery key"
            log_warn "vmrestore.sh" "restore_vm" "    - Windows 11 (no BitLocker): a fresh TPM will be created;"
            log_warn "vmrestore.sh" "restore_vm" "      Windows Hello PIN and similar will need re-enrollment"
            log_warn "vmrestore.sh" "restore_vm" "    - Linux LUKS-via-TPM: fall back to LUKS passphrase"
            log_warn "vmrestore.sh" "restore_vm" "  Check the [ERROR] line above for the underlying cause."
            log_warn "vmrestore.sh" "restore_vm" "============================================================"
        fi
    elif [[ "$OPT_SKIP_CONFIG" == true && "$OPT_SKIP_TPM" == false ]]; then
        log_info "vmrestore.sh" "restore_vm" "Skipping VM definition (--skip-config — data-only restore)"
        log_info "vmrestore.sh" "restore_vm" "Skipping TPM restore (--skip-config implies data-only)"
    fi

    # Completion summary — single line confirming what was done
    local _summary="Restore complete: $vm_name"
    if is_dry_run; then
        _summary="$_summary [DRY RUN — no changes made]"
    else
        local _parts=()
        # X2: every ✓ below is a verified post-condition (mirroring the INT-22
        # TPM token), not an intent — a partial/failed restore can no longer
        # render an all-green summary.
        local _tgt="${OPT_NAME:-$vm_name}" _defined=0
        lv_domain_exists "$_tgt" && _defined=1
        # disk: the restore engine (or its disk-only retry) reported success; in
        # clone mode VMR1 guarantees promotion ran before we reach this point.
        if [[ "${restore_ok:-false}" == true ]]; then
            _parts+=("disk ✓")
        else
            _parts+=("disk ✗")
        fi
        if [[ "$OPT_SKIP_CONFIG" == false ]]; then
            if [[ "$_defined" -eq 1 ]]; then _parts+=("defined ✓"); else _parts+=("defined ✗"); fi
            if [[ "$new_identity" == true ]]; then
                # new identity ✓ only if define_new_identity read a new UUID back.
                if [[ -n "$new_uuid" ]]; then _parts+=("new identity ✓"); else _parts+=("new identity ✗"); fi
            else
                # UUID ✓ only if the live domain UUID matches the re-injected original.
                if [[ "$_defined" -eq 1 && -n "${orig_uuid:-}" && "$(lv_domain_uuid "$_tgt" 2>/dev/null)" == "${orig_uuid:-}" ]]; then
                    _parts+=("UUID ✓")
                else
                    _parts+=("UUID ✗")
                fi
                # MACs ride with the domain definition: present iff the VM is defined.
                if [[ "$_defined" -eq 1 ]]; then _parts+=("MACs ✓"); else _parts+=("MACs ✗"); fi
            fi
        else
            _parts+=("data-only")
        fi
        # INT-22: TPM token reflects actual outcome. Only emit when TPM was
        # applicable (marker or tpm-state/ present in the backup); otherwise
        # the VM had no TPM to restore and the token would be noise.
        if [[ "$OPT_SKIP_TPM" == false && "$OPT_SKIP_CONFIG" == false && "$_tpm_applicable" == true ]]; then
            if [[ "$_tpm_rc" -eq 0 ]]; then
                _parts+=("TPM ✓")
            else
                _parts+=("TPM ✗ (manual unlock required)")
            fi
        fi
        _summary="$_summary — ${_parts[*]}"
    fi
    log_info "vmrestore.sh" "restore_vm" "$_summary"
}

# ── Verify / Dump ────────────────────────────────────────────────────────────

run_virtnbd_action() {
    local action="$1" vm_name="$2"

    local data_dir=""
    if has_backup_data "$OPT_BACKUP_PATH"; then
        data_dir="$OPT_BACKUP_PATH"
    else
        local vm_dir
        vm_dir=$(resolve_vm_backup_dir "$vm_name" "$OPT_BACKUP_PATH") \
            || die "VM directory not found for '$vm_name'" "run_virtnbd_action" "$EXIT_VM"
        data_dir=$(resolve_data_dir "$vm_dir" "${OPT_PERIOD:-}") || \
            die "Cannot resolve data directory" "run_virtnbd_action" "$EXIT_VM"
    fi

    log_info "vmrestore.sh" "run_virtnbd_action" "Running $action on: $data_dir"
    # UNI-014 M1: build via lib helper. For dump/verify, action is the -o keyword.
    build_virtnbdrestore_args "$data_dir" "" "$action"
    run_logged "${_VIRTNBD_ARGS[@]}"
}

# ── Usage ────────────────────────────────────────────────────────────────────

usage() {
    echo "vmrestore.sh v${VMBACKUP_VERSION} — Automated VM restoration wrapping virtnbdrestore"
    cat << 'EOF'

USAGE:
  vmrestore.sh --vm <name|path> --restore-path <path> [options]
  vmrestore.sh --vm <name|path> --disk <dev[,dev,...]|all> [--restore-path <path>]
  vmrestore.sh --list [--backup-path <path>]
  vmrestore.sh --list-restore-points <name|path> [--period <id>]
  vmrestore.sh --verify <name> [--period <id>]
  vmrestore.sh --dump <name> [--period <id>]

RESTORE:
  --vm <name|path>       VM to restore (name or full path to backup dir)
  --restore-path <path>  Output directory for restored VM (required for DR/clone)
  --backup-path <path>   Backup root (overrides vmbackup.conf)
                         Not needed when --vm is given a full path
  --period <id>          Specific period (2026-W09, 20260303, 202602)
  --restore-point <p>    latest (default) | full | restore point number
  --name <name>          Clone with new identity (new UUID, new MACs, isolated NVRAM)
  --disk <dev[,dev,...]>  Restore specific disk(s) from a multi-disk backup.
                         Comma-separated: --disk vda,vdb  or  --disk all
                         Replaces existing qcow2 file(s) in-place.
                         For single-disk VMs this is ignored (full restore).

CONTROL:
  --skip-config          Restore disk data only, don't define VM
  --skip-tpm             Skip TPM state restoration
  --force                Undefine existing VM before restoring
  --dry-run              Show commands without executing
  --no-pre-restore       Skip creating .pre-restore backup of existing disk
                         (disk restore only — saves space at risk of no rollback)
  --include-incomplete   Allow restore from a chain flagged as incomplete and
                         show the same flag in --list-restore-points output
                         (INT-16 — forensic / recovery use only)

INSPECTION:
  --list                 List all VMs with backup info
  --list-restore-points  Show available restore points
  --verify <name>        Checksum validation (virtnbdrestore -o verify)
  --dump <name>          Backup metadata (virtnbdrestore -o dump)
  --config-instance <n>  Use named vmbackup config instance (default: default)
                         Also reads VMBACKUP_INSTANCE env var as fallback

EXAMPLES:
  # Disaster recovery — rebuild VM with original identity
  vmrestore.sh --vm my-server --restore-path /var/lib/libvirt/images/my-server

  # Clone — independent copy with new UUID and MACs
  vmrestore.sh --vm my-server --name test-clone --restore-path /var/lib/libvirt/images/test-clone

  # Point-in-time — restore to restore point 3
  vmrestore.sh --vm my-server --restore-point 3 --restore-path /tmp/restore

  # Disk restore — replace data disk in-place (VM must be shut off)
  vmrestore.sh --vm my-server --disk vdb

  # Disk restore — replace multiple disks at once
  vmrestore.sh --vm my-server --disk vda,vdb,sda

  # Disk restore — replace all disks
  vmrestore.sh --vm my-server --disk all

  # Disk restore — extract disk to staging path
  vmrestore.sh --vm my-server --disk vdb --restore-path /tmp/restore

  # Disk restore — point-in-time, roll back vdb to restore point 1
  vmrestore.sh --vm my-server --disk vdb --restore-point 1

  # Restore from specific period
  vmrestore.sh --vm my-workstation --period 20260302 --restore-path /tmp/restore

  # Restore from archived chain
  vmrestore.sh --vm /mnt/backups/vm/my-server/2026-W09/.archives/chain-2026-02-28.1 \
    --restore-path /tmp/restore/archived

  # Inspect
  vmrestore.sh --list-restore-points my-server
  vmrestore.sh --verify my-workstation --period 20260303
  vmrestore.sh --list
EOF
    exit 0
}

# ── CLI Parsing ──────────────────────────────────────────────────────────────

OPT_MODE=""
OPT_VM_NAME=""
OPT_BACKUP_PATH=""
BACKUP_PATH_CLI=""
OPT_CONFIG_INSTANCE=""
OPT_RESTORE_PATH=""
OPT_PERIOD=""
OPT_RESTORE_POINT="latest"
OPT_NAME=""
OPT_DISK=""
OPT_SKIP_CONFIG=false
OPT_SKIP_TPM=false
OPT_FORCE=false
OPT_DRY_RUN=false
OPT_NO_PRE_RESTORE=false
OPT_INCLUDE_INCOMPLETE=false   # INT-16: include incomplete chains in --list-restore-points and allow restore from them

# ── INT-18: --restore-path overlap guard ─────────────────────────────────────
# Refuse to write virtnbdrestore output into any directory that equals or sits
# inside a configured vmbackup BACKUP_PATH. Probes every instance config under
# /opt/vmbackup/config/*/vmbackup.conf and the active OPT_BACKUP_PATH. Pure
# canonicalised string check — no race with the filesystem.
restore_path_overlap_check() {
    [[ -z "${OPT_RESTORE_PATH:-}" ]] && return 0

    local rp_canon
    rp_canon=$(realpath -m -- "$OPT_RESTORE_PATH" 2>/dev/null) || rp_canon="$OPT_RESTORE_PATH"
    rp_canon="${rp_canon%/}"

    # Build the list of backup roots to probe: active OPT_BACKUP_PATH plus
    # every BACKUP_PATH declared by an installed instance config.
    local -a roots=()
    [[ -n "${OPT_BACKUP_PATH:-}" ]] && roots+=("$OPT_BACKUP_PATH")

    local cfg bp
    local cfg_dir="/opt/vmbackup/config"
    if [[ -d "$cfg_dir" ]]; then
        while IFS= read -r -d '' cfg; do
            # Source in an isolated subshell so the parent env is untouched
            # and arbitrary code in the conf cannot leak into vmrestore.
            # SC2030: subshell-local BACKUP_PATH mutation is INTENTIONAL — that
            #         is the isolation; we extract the value via printf, not
            #         via the variable.
            # SC1090: $cfg is a runtime-discovered conf file, no shellcheck
            #         path resolution is possible or desired.
            # shellcheck disable=SC2030,SC1090
            bp=$(BACKUP_PATH=""; set +u; . "$cfg" >/dev/null 2>&1; printf '%s' "${BACKUP_PATH:-}")
            [[ -n "$bp" ]] && roots+=("$bp")
        done < <(find "$cfg_dir" -maxdepth 2 -type f -name 'vmbackup.conf' -print0 2>/dev/null)
    fi

    local root rcanon
    for root in "${roots[@]}"; do
        # Skip placeholder paths from template configs.
        [[ "$root" == "/path/to/backups"* ]] && continue
        rcanon=$(realpath -m -- "$root" 2>/dev/null) || rcanon="$root"
        rcanon="${rcanon%/}"
        [[ -z "$rcanon" || "$rcanon" == "/" ]] && continue

        if [[ "$rp_canon" == "$rcanon" || "$rp_canon" == "$rcanon"/* ]]; then
            die "Refusing --restore-path '$OPT_RESTORE_PATH': resolves inside vmbackup BACKUP_PATH '$root' (INT-18). Restoring into an active backup tree would corrupt chains and catalogue state. Choose a path outside every BACKUP_PATH." "parse_args" "$EXIT_USAGE"
        fi
    done
    return 0
}

parse_args() {
    [[ $# -eq 0 ]] && usage

    while [[ $# -gt 0 ]]; do
        case "$1" in
            --vm)
                OPT_MODE="restore"
                local vm_arg="${2:?'--vm requires a VM name or path'}"
                if [[ "$vm_arg" == */* ]]; then
                    # Path given: derive VM name and backup path
                    OPT_VM_NAME=$(basename "$vm_arg")
                    BACKUP_PATH_CLI=$(dirname "$vm_arg")
                else
                    OPT_VM_NAME="$vm_arg"
                fi
                shift 2 ;;
            --list)
                OPT_MODE="list"
                shift ;;
            --list-restore-points)
                OPT_MODE="list-rp"
                if [[ -n "${2:-}" && ! "${2:-}" =~ ^-- ]]; then
                    local rp_arg="$2"
                    if [[ "$rp_arg" == */* ]]; then
                        OPT_VM_NAME=$(basename "$rp_arg")
                        BACKUP_PATH_CLI=$(dirname "$rp_arg")
                    else
                        OPT_VM_NAME="$rp_arg"
                    fi
                    shift
                fi
                shift ;;
            --verify)
                OPT_MODE="verify"
                OPT_VM_NAME="${2:?'--verify requires a VM name'}"
                shift 2 ;;
            --dump)
                OPT_MODE="dump"
                OPT_VM_NAME="${2:?'--dump requires a VM name'}"
                shift 2 ;;
            --backup-path)
                BACKUP_PATH_CLI="${2:?'--backup-path requires a path'}"
                shift 2 ;;
            --config-instance)
                OPT_CONFIG_INSTANCE="${2:?'--config-instance requires an instance name'}"
                shift 2 ;;
            --restore-path)
                OPT_RESTORE_PATH="${2:?'--restore-path requires a path'}"
                shift 2 ;;
            --period)
                OPT_PERIOD="${2:?'--period requires a period ID'}"
                shift 2 ;;
            --restore-point)
                OPT_RESTORE_POINT="${2:?'--restore-point requires a value'}"
                shift 2 ;;
            --name)
                OPT_NAME="${2:?'--name requires a name'}"
                shift 2 ;;
            --disk)
                OPT_DISK="${2:?'--disk requires a device name (e.g. vdb, vda,vdb, or all)'}"
                shift 2 ;;
            --skip-config)    OPT_SKIP_CONFIG=true; shift ;;
            --skip-tpm)       OPT_SKIP_TPM=true; shift ;;
            --force)          OPT_FORCE=true; shift ;;
            --dry-run)        OPT_DRY_RUN=true; shift ;;
            --no-pre-restore) OPT_NO_PRE_RESTORE=true; shift ;;
            --include-incomplete) OPT_INCLUDE_INCOMPLETE=true; shift ;;
            --help|-h)        usage ;;
            --version|-V)     echo "vmrestore $VMBACKUP_VERSION"; echo "Vibe coded by James Doutsis | https://www.github.com/doutsis/"; exit 0 ;;
            *)                die "Unknown option: $1" "parse_args" "$EXIT_USAGE" ;;
        esac
    done

    OPT_BACKUP_PATH=$(resolve_backup_path) || exit $?

    # Validate incompatible flag combinations
    if [[ -n "${OPT_DISK:-}" && -n "${OPT_NAME:-}" ]]; then
        die "--disk and --name cannot be combined (disk restore replaces disk files, it does not create a VM)" "parse_args" "$EXIT_USAGE"
    fi
    if [[ "${OPT_SKIP_CONFIG:-false}" == true && -n "${OPT_NAME:-}" ]]; then
        die "--skip-config and --name cannot be combined (--name requests a clone, which requires defining the restored VM; --skip-config skips VM definition)" "parse_args" "$EXIT_USAGE"
    fi

    # Normalise paths: strip trailing slashes to avoid ugly double-slash //
    OPT_RESTORE_PATH=$(pu_strip_trailing_slash "$OPT_RESTORE_PATH")
    OPT_BACKUP_PATH=$(pu_strip_trailing_slash "$OPT_BACKUP_PATH")
    BACKUP_PATH_CLI=$(pu_strip_trailing_slash "$BACKUP_PATH_CLI")

    # INT-18: refuse a --restore-path that equals or lives inside any vmbackup
    # BACKUP_PATH. Writing virtnbdrestore output into an active backup tree
    # would corrupt sessions, mutate chains, and surface as catalogue drift.
    restore_path_overlap_check

    # UNI-322 (Phase 7 commit 1): re-normalise dry-run state now that
    # OPT_DRY_RUN may have been set by --dry-run. is_dry_run / if_dry_run /
    # if_not_dry_run will report the correct value to all subsequent callers.
    dry_run_normalise_state
}

# ── Main ─────────────────────────────────────────────────────────────────────

main() {
    ORIG_ARGS="$*"
    parse_args "$@"

    # Read-only modes: no log file, no banner
    case "$OPT_MODE" in
        list|list-rp)
            [[ -d "$OPT_BACKUP_PATH" ]] || { echo "Backup path not found: $OPT_BACKUP_PATH" >&2; return 1; }
            case "$OPT_MODE" in
                list)    list_vms "$OPT_BACKUP_PATH" ;;
                list-rp)
                    [[ -n "$OPT_VM_NAME" ]] || { echo "VM name required for --list-restore-points" >&2; return 1; }
                    local vm_dir
                    vm_dir=$(resolve_vm_backup_dir "$OPT_VM_NAME" "$OPT_BACKUP_PATH") \
                        || { echo "VM not found: $OPT_VM_NAME" >&2; return 1; }

                    echo ""
                    echo "Restore Points: $OPT_VM_NAME"

                    # Build list of period dirs to show
                    local -a period_dirs=()
                    if [[ -n "${OPT_PERIOD:-}" ]]; then
                        local target="$vm_dir/$OPT_PERIOD"
                        [[ -d "$target" ]] || { echo "Period not found: $target" >&2; return 1; }
                        period_dirs+=("$target")
                    elif is_accumulate "$vm_dir"; then
                        period_dirs+=("$vm_dir")
                    else
                        local -a periods=()
                        mapfile -t periods < <(list_periods "$vm_dir")
                        if [[ ${#periods[@]} -eq 0 ]]; then
                            echo "No period directories in: $vm_dir" >&2; return 1
                        fi
                        for p in "${periods[@]}"; do
                            period_dirs+=("$vm_dir/$p")
                        done
                    fi

                    # Show each period
                    local -A _seen_archives=()
                    for data_dir in "${period_dirs[@]}"; do
                        local _period_label
                        _period_label=$(basename "$data_dir")
                        if [[ "$data_dir" == "$vm_dir" ]]; then
                            # Detect if this is an archived chain (parent is .archives/)
                            local _parent_dir
                            _parent_dir=$(basename "$(dirname "$data_dir")")
                            if [[ "$_parent_dir" == ".archives" ]]; then
                                _period_label="(archive)"
                            else
                                _period_label="(accumulate)"
                            fi
                        fi

                        echo ""
                        echo "  ── $_period_label ──"
                        echo "  Directory: $data_dir"
                        echo "  Type: $(detect_backup_type "$data_dir")"
                        local _disks
                        _disks=$(enumerate_disks "$data_dir")
                        [[ -n "$_disks" ]] && echo "  Disks: $_disks"
                        # INT-16: mark incomplete chains so operators don't pick them.
                        chain_check_complete "$data_dir"
                        case $? in
                            1) echo "  ⚠ INCOMPLETE: $_CHAIN_INCOMPLETE_REASON" ;;
                        esac
                        echo ""
                        show_restore_points "$data_dir"

                        # Show archived chains for this period
                        for adir in "$vm_dir/.archives" "$data_dir/.archives"; do
                            [[ -d "$adir" ]] || continue
                            local real_adir
                            real_adir=$(pu_normalise_path "$adir")
                            [[ -n "${_seen_archives[$real_adir]:-}" ]] && continue
                            _seen_archives[$real_adir]=1
                            echo "  Archived Chains:"
                            for chain in "$adir"/chain-*; do
                                [[ -d "$chain" ]] || continue
                                local cname csize ctype
                                cname=$(basename "$chain")
                                csize=$(du -sh "$chain" 2>/dev/null | awk '{print $1}')
                                ctype=$(detect_backup_type "$chain")
                                local cdisks
                                cdisks=$(enumerate_disks "$chain")
                                # INT-16: completeness probe per archived chain.
                                chain_check_complete "$chain"
                                local _crc=$?
                                local _cmark=""
                                [[ "$_crc" -eq 1 ]] && _cmark=" ⚠ INCOMPLETE"
                                if [[ "$_crc" -eq 1 && "$OPT_INCLUDE_INCOMPLETE" != true ]]; then
                                    # By default still show the row but flag it; show reason
                                    # on a continuation line for operator visibility.
                                    if [[ -n "$cdisks" ]]; then
                                        printf "    %-30s %6s  %s  [%s]%s\n" "$cname" "$csize" "$ctype" "$cdisks" "$_cmark"
                                    else
                                        printf "    %-30s %6s  %s%s\n" "$cname" "$csize" "$ctype" "$_cmark"
                                    fi
                                    printf "      reason: %s\n" "$_CHAIN_INCOMPLETE_REASON"
                                else
                                    if [[ -n "$cdisks" ]]; then
                                        printf "    %-30s %6s  %s  [%s]%s\n" "$cname" "$csize" "$ctype" "$cdisks" "$_cmark"
                                    else
                                        printf "    %-30s %6s  %s%s\n" "$cname" "$csize" "$ctype" "$_cmark"
                                    fi
                                fi
                                show_restore_points "$chain"
                            done
                        done
                    done
                    ;;
            esac
            return 0
            ;;
    esac

    # Write modes: full logging
    init_logging
    finalize_log_name
    log_invocation_summary
    log_info "vmrestore.sh" "main" "====== vmrestore v$VMBACKUP_VERSION ======"
    [[ -d "$OPT_BACKUP_PATH" ]] || die "Backup path not found: $OPT_BACKUP_PATH" "main" "$EXIT_STORAGE"

    # INT-22: derive STATE_DIR once here (hoisted above the lock line, and above
    # the sqlite-init block that also assigns it) so the lock dir and the
    # catalogue dir can never drift. vmrestore normalizes OPT_BACKUP_PATH to NO
    # trailing slash at parse-time (pu_strip_trailing_slash), so we re-add the
    # "/_state" segment explicitly — exactly as vmbackup.sh does off its slashed
    # BACKUP_PATH. Guarded ${STATE_DIR:-...} keeps -u happy (first read self-guards)
    # and is idempotent (the later sqlite-init assignment becomes a no-op).
    STATE_DIR="${STATE_DIR:-${OPT_BACKUP_PATH%/}/_state}"

    # UNI-002: per-VM lock dir for create_lock/remove_lock, layout identical to
    # vmbackup ("${STATE_DIR}/locks" = "<root>/_state/locks"), so vmbackup and
    # vmrestore on the same VM mutually exclude via the shared "vmbackup-<vm>.lock".
    # Previously this glued the "_state" segment onto the no-slash-normalized root,
    # producing a "<root>_state/locks" SIBLING that defeated the cross-tool lock (INT-22).
    LOCK_DIR="${STATE_DIR}/locks"

    # UNI-010: Register signal handlers now that LOG_FILE and LOCK_DIR are
    # known. EXIT trap is the safety net for both signal-induced and normal
    # exits (cleanup is idempotent and rc-aware).
    setup_exit_cleanup    '_vmrestore_cleanup'
    setup_sigint_handler  '_vmrestore_handle_sigint'
    setup_sigterm_handler '_vmrestore_handle_sigterm'

    # Phase 8 (UNI-902b): initialise SQLite catalogue for restore session
    # logging. Warn-and-continue per §2.2 — DR invariant prohibits hard-fail
    # on catalogue unavailability. Init failure sets SQLITE_MODULE_AVAILABLE=0
    # which the writer-side gates (sqlite_is_available) detect; vmrestore
    # then proceeds without writing rows.
    # sqlite_init_database expects BACKUP_PATH (vmbackup-side convention) to
    # derive the default ${STATE_DIR}; vmrestore's equivalent is
    # OPT_BACKUP_PATH so we export it here for the lib's view.
    if declare -F sqlite_init_database >/dev/null 2>&1; then
        # SC2031: shellcheck sees the BACKUP_PATH mutation in the earlier
        #         restore_path_overlap_check() subshell and warns that this
        #         read may see a stale value. False positive — that subshell
        #         is deliberately isolated and never propagated; the value we
        #         read here is the parent's (untouched) BACKUP_PATH.
        # shellcheck disable=SC2031
        BACKUP_PATH="${BACKUP_PATH:-$OPT_BACKUP_PATH}"
        STATE_DIR="${STATE_DIR:-${OPT_BACKUP_PATH%/}/_state}"
        sqlite_init_database || \
            log_info "vmrestore.sh" "main" "catalogue init skipped; restore will not be recorded"
    fi

    local rc=0
    case "$OPT_MODE" in
        restore)
            # --disk without --restore-path = in-place disk replacement (no --restore-path needed)
            if [[ -z "$OPT_RESTORE_PATH" && -z "${OPT_DISK:-}" ]]; then
                die "--restore-path is required for restore (unless --disk is used for in-place replacement)" "main" "$EXIT_USAGE"
            fi
            # UNI-002: Sanitise + lock + restore + unlock. Validation here
            # rather than inside restore_vm so the lock-acquisition error
            # message mentions the same name the user typed.
            local _safe_lock_vm
            _safe_lock_vm=$(vm_fs_name "$OPT_VM_NAME") || exit $?
            if ! create_lock "$_safe_lock_vm"; then
                die "Another vmbackup or vmrestore is already running on VM '$_safe_lock_vm' (lock held)" \
                    "main" "$EXIT_LOCK"
            fi
            _VMRESTORE_LOCK_VM="$_safe_lock_vm"  # UNI-010: track for cleanup trap
            restore_vm "$OPT_VM_NAME"
            rc=$?
            remove_lock "$_safe_lock_vm"
            _VMRESTORE_LOCK_VM=""  # UNI-010: untrack on clean release
            ;;

        verify)
            if run_virtnbd_action "verify" "$OPT_VM_NAME"; then
                log_info "vmrestore.sh" "main" "Verification passed: backup checksums are valid"
            else
                rc=$?
                log_error "vmrestore.sh" "main" "Verification FAILED: backup checksums do not match (exit $rc)"
            fi
            ;;

        dump)
            run_virtnbd_action "dump" "$OPT_VM_NAME" || rc=$?
            ;;

        *)
            die "No mode specified (try --help)" "main" "$EXIT_USAGE"
            ;;
    esac

    log_info "vmrestore.sh" "main" "====== vmrestore completed ======"
    log_completion_summary $rc
    return $rc
}

main "$@"
exit $?
