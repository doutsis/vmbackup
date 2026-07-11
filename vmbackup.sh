#!/bin/bash

#################################################################################
# vmbackup — Automated Backup for libvirt/KVM Virtual Machines
# Vibe coded by James Doutsis — james@doutsis.com
#
# Repository:  https://github.com/doutsis/vmbackup
# Restore:     https://github.com/doutsis/vmrestore
# 
# Features:
#   - Monthly YYYYMM directory structure with weekly checkpoint consolidation
#   - Full + incremental backups with automatic weekly cleanup
#   - QEMU guest agent detection with pause/resume fallback
#   - Comprehensive health checks and stale state recovery
#   - Per-VM locking to prevent concurrent backups
#   - Checkpoint depth monitoring and VM protection
#   - Comprehensive timestamped logging with process/function context
#   - Configurable retention, compression, process priority
#   - Smart offline VM change detection using mtime to avoid redundant copy backups
#     (detects disk modifications while VM offline, skips unchanged backups)
#
#################################################################################
#
# MODULE ARCHITECTURE
# ===================
# This script uses a cascaded module loading pattern. Not all modules are
# loaded directly by vmbackup.sh - some are loaded through vmbackup_integration.sh.
#
# Loading chain (in main() starting ~line 5600):
#
#   vmbackup.sh (this file)
#   ├── lib/sqlite_module.sh       [direct]  SQLite database logging
#   ├── vmbackup_integration.sh    [direct]  → cascades to load:
#   │   ├── rotation_module.sh              Rotation policies, period IDs
#   │   ├── logging_module.sh               Structured event logging (SQLite-backed)
#   │   └── retention_module.sh             Per-VM retention enforcement
#   ├── replication_local_module.sh [direct] NFS/SSH/SMB sync (via init_local_replication_module)
#   ├── replication_cloud_module.sh [direct] SharePoint/Backblaze (via init_cloud_replication_module)
#   ├── fstrim_optimization_module.sh [conditional] FSTRIM if ENABLE_FSTRIM=true
#   ├── tpm_backup_module.sh       [lazy]   TPM key backup (loaded per-VM if needed)
#   └── email_report_module.sh     [lazy]   Email reports (loaded at session end)
#
# To search for a module's integration:
#   1. Check vmbackup.sh directly: grep -n "module_name" vmbackup.sh
#   2. Check vmbackup_integration.sh: grep -n "module_name" vmbackup_integration.sh
#   3. Check if loaded via init_* function: grep -n "init_.*module" vmbackup.sh
#
# Signal handlers (line ~5547):
#   - EXIT  → cleanup_on_exit()         Final cleanup, lock release
#   - SIGINT → inline handler           Ctrl+C during backup
#   - SIGTERM → handle_sigterm()        Graceful termination
#   - SIGTSTP → handle_sigtstp()        Suspend request
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

set -o pipefail

# Security: restrict file creation permissions
# umask 027 → Files: 640 (rw-r-----), Dirs: 750 (rwxr-x---)
# SGID on backup dirs → mode 2750 (rwxr-s---), new files inherit backup group
# Group = backup (set by postinst), so backup group members can read backups.
# virtnbdbackup inherits this umask from vmbackup.sh.
# Belt-and-suspenders with UMask=0027 in vmbackup.service.
umask 027

# UNI-003 + UNI-008 + UNI-321: Source bootstrap.sh (lib loader) + version
# + exit codes from lib/. This is the ONLY remaining inline source block —
# bootstrap.sh provides source_lib_or_die for every subsequent lib load.
# Done very early because the rest of the script depends on these
# constants (CLI parsing, config load, etc.). Derive script dir locally
# because SCRIPT_DIR proper is established later (L447ish).
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

# Dry-run mode: show what would happen without executing destructive operations
DRY_RUN=false

# UNI-322 / F-372 (R1): source the dry-run predicate API + dry_run_normalise_state
# HERE — right after DRY_RUN is initialised and while source_lib_or_die (bootstrap.sh)
# is available — so the predicate is DEFINED before its FIRST use in the pre-config
# CLI-validation window (the --cancel-replication/--dry-run conflict check).
# dry_run.sh's only deps are DRY_RUN (above) + exit_codes.sh (bootstrap loop); it does
# NOT depend on config_instance.sh, so this earlier position breaks no load order.
source_lib_or_die dry_run.sh

# Guard against duplicate email reports (BUG-02: double email on SIGTERM)
_EMAIL_SENT=false

#################################################################################
# ARGUMENT PARSING (must happen before config load)
#################################################################################

# Config instance (default, test, etc.)
CONFIG_INSTANCE="default"

# Prune mode flags
_PRUNE_MODE=false
_PRUNE_TARGET=""
_TARGET_VM=""
_PRUNE_CONFIRM_SKIP=false

# Replicate-only mode (empty = normal backup, local|cloud|both = replicate-only)
_REPLICATE_ONLY_MODE=""

# Status report mode (--status)
_STATUS_MODE=false
_STATUS_SUB=""
_STATUS_DAYS=""
_STATUS_CSV=false

# Backup mode (requires explicit --run)
_BACKUP_MODE=false

# Config maintenance: --config-prune-removed (cleanup helper for ghost vars)
_CONFIG_PRUNE_REMOVED=false

# Parse arguments early to get --config-instance before config load
for arg in "$@"; do
    case "$arg" in
        --config-instance=*)
            CONFIG_INSTANCE="${arg#*=}"
            ;;
    esac
done
# Also handle --config-instance VALUE format
while [[ $# -gt 0 ]]; do
    case "$1" in
        --config-instance=*)
            # FF-137: the pre-scan loop above accepts the equals form, but the
            # main parser lacked this arm, so --config-instance=NAME fell through
            # to the '*)' Unknown-option default. CONFIG_INSTANCE is already set
            # by the pre-scan; consume the token here so parsing continues.
            CONFIG_INSTANCE="${1#*=}"
            shift
            ;;
        --config-instance)
            [[ -n "${2:-}" ]] && CONFIG_INSTANCE="$2"
            shift 2 || break
            ;;
        --dry-run)
            DRY_RUN=true
            shift
            ;;
        --cancel-replication)
            _CANCEL_REPLICATION_REQUESTED=true
            shift
            ;;
        --replicate-only)
            _REPLICATE_ONLY_MODE="both"
            if [[ -n "${2:-}" && "${2:0:1}" != "-" ]]; then
                case "$2" in
                    local|cloud|both) _REPLICATE_ONLY_MODE="$2"; shift ;;
                    *) echo "Error: --replicate-only accepts: local, cloud, both (got '$2')" >&2; exit "$EXIT_USAGE" ;;
                esac
            fi
            shift
            ;;
        --prune)
            _PRUNE_MODE=true
            [[ -n "${2:-}" && "${2:0:1}" != "-" ]] && { _PRUNE_TARGET="$2"; shift; }
            shift
            ;;
        --vm)
            if [[ -n "${2:-}" && "${2:0:1}" != "-" ]]; then
                _TARGET_VM="$2"
                shift 2
            else
                echo "Error: --vm requires a VM name" >&2
                exit "$EXIT_USAGE"
            fi
            ;;
        --cleanup-stale-manifests)
            # DUP-10: Phase C postinst-driven reaping of stale chain-manifest.json.
            _CLEANUP_STALE_MANIFESTS=true
            shift
            ;;
        --status)
            _STATUS_MODE=true
            shift
            ;;
        --config-prune-removed)
            _CONFIG_PRUNE_REMOVED=true
            shift
            ;;
        --failures)
            _STATUS_SUB="failures"
            shift
            ;;
        --replication)
            _STATUS_SUB="replication"
            shift
            ;;
        --chains)
            _STATUS_SUB="chains"
            shift
            ;;
        --storage)
            _STATUS_SUB="storage"
            shift
            ;;
        --policies)
            _STATUS_SUB="policies"
            shift
            ;;
        --restores)
            _STATUS_SUB="restores"
            shift
            ;;
        --days)
            if [[ -n "${2:-}" && "${2:0:1}" != "-" ]]; then
                _STATUS_DAYS="$2"
                shift 2
            else
                echo "Error: --days requires a number" >&2
                exit "$EXIT_USAGE"
            fi
            ;;
        --csv)
            _STATUS_CSV=true
            shift
            ;;
        --all-instances)
            _STATUS_ALL_INSTANCES=true
            shift
            ;;
        --yes|-y)
            _PRUNE_CONFIRM_SKIP=true
            shift
            ;;
        --run)
            _BACKUP_MODE=true
            shift
            ;;
        --help|-h)
            cat << HELP_EOF
vmbackup ${VMBACKUP_VERSION} — KVM/QEMU virtual machine backup utility
Vibe coded by James Doutsis | https://www.github.com/doutsis/

Usage: vmbackup.sh <MODE> [OPTIONS]

  sudo vmbackup.sh --run                     Start full backup (all VMs)
  sudo vmbackup.sh --run --vm web,db         Backup specific VMs
  sudo vmbackup.sh --prune list              Show backup inventory
  sudo vmbackup.sh --replicate-only          Run replication only
  sudo vmbackup.sh --help                    Full help and examples

GENERAL:
    --help, -h              Show this help message
    --version               Show version and exit

BACKUP:
    --run                   Start a backup session (required)
    --vm NAME               Target specific VM(s), comma-separated: --vm web,db
                            Replication is skipped in targeted backup mode.
    --dry-run               Preview without executing changes (read-only mode)
    --config-instance NAME  Use config from config/NAME/ (default: "default")

PRUNE (standalone cleanup — no backup session):
    --prune TARGET          Remove backup data. Targets:
        list                  Show backup inventory with sizes
        archives              Remove .archives/ dirs (all VMs, or --vm for one)
        archives:<period>     Remove .archives/ in a specific period (requires --vm)
        chain:<name>          Remove one archived chain (requires --vm)
        period:<period_id>    Remove entire period directory (requires --vm)
        all                   Remove everything for a VM (requires --vm)
    --vm NAME               Scope prune to a single VM (required for most targets)
    --yes, -y               Skip confirmation prompt (for scripted use)

REPLICATE-ONLY (run replication without backups):
    --replicate-only [SCOPE]  SCOPE: local, cloud, both (default: both)

STATUS REPORTS (read-only — no locks, no session):
    --status                Today's session summary
    --status --vm NAME      Backup history for a VM
    --status --failures     Recent failures
    --status --replication  Replication status
    --status --chains       Chain health overview
    --status --storage      Storage per VM (all history)
    --status --policies     Rotation policy summary
    --status --restores     Recent vmrestore sessions (UNI-902b)

    --days N                Time window (default: 1 = today)
    --csv                   CSV output with raw + formatted columns
    --all-instances         Show sessions from all config instances
                            (default: scoped to --config-instance)

SIGNAL:
    --cancel-replication    Signal running session to cancel replication phase.
                            Creates flag file in STATE_DIR; replication checks
                            this file and terminates gracefully.

CONFIG MAINTENANCE:
    --config-prune-removed  Comment out config variables removed in the running
                            release. Idempotent and reversible (lines are
                            commented, not deleted). Operates on default/ and
                            all custom instances; skips template/.
                            Use with --dry-run to preview.

CONFIG INSTANCES:
    Each instance (config/<name>/) contains vmbackup.conf, vm_overrides.conf,
    and exclude_patterns.conf. Copy config/template/ to create a new instance.

EXAMPLES:
    sudo vmbackup.sh --run                                    # Full backup
    sudo vmbackup.sh --run --vm web                           # Single VM
    sudo vmbackup.sh --run --vm web,db,mail                   # Multiple VMs
    sudo vmbackup.sh --run --dry-run                          # Preview full backup
    sudo vmbackup.sh --run --config-instance test --dry-run   # Test config preview
    sudo vmbackup.sh --prune list                             # Backup inventory
    sudo vmbackup.sh --prune list --vm myvm                   # Single VM inventory
    sudo vmbackup.sh --prune archives --vm myvm               # Remove VM archives
    sudo vmbackup.sh --prune archives --dry-run               # Preview archive removal
    sudo vmbackup.sh --prune all --vm myvm --yes              # ⚠ DESTRUCTIVE: removes all backups for myvm
    sudo vmbackup.sh --replicate-only                         # Both local + cloud
    sudo vmbackup.sh --replicate-only local --dry-run         # Preview local only
    sudo vmbackup.sh --cancel-replication                     # Cancel replication
    sudo vmbackup.sh --status                                 # Today's backup sessions
    sudo vmbackup.sh --status --days 7                        # Last 7 days
    sudo vmbackup.sh --status --vm web                        # VM backup history
    sudo vmbackup.sh --status --failures --days 30            # Failures last 30 days
    sudo vmbackup.sh --status --chains                        # Chain health overview
    sudo vmbackup.sh --status --policies                      # Rotation policy summary
    sudo vmbackup.sh --status --failures --csv                # CSV export

    See full documentation: https://github.com/doutsis/vmbackup
HELP_EOF
            exit 0
            ;;
        --version)
            echo "vmbackup ${VMBACKUP_VERSION}"
            echo "Vibe coded by James Doutsis | https://www.github.com/doutsis/"
            exit 0
            ;;
        *)
            echo "Error: Unknown option: $1" >&2
            echo "Run vmbackup.sh --help for usage." >&2
            exit "$EXIT_USAGE"
            ;;
    esac
done

# Mutual exclusivity guards
if [[ "${_PRUNE_MODE}" == "true" && -n "${_REPLICATE_ONLY_MODE}" ]]; then
    echo "Error: --prune and --replicate-only cannot be used together" >&2
    exit "$EXIT_USAGE"
fi
if [[ -n "${_REPLICATE_ONLY_MODE}" && -n "${_TARGET_VM}" ]]; then
    echo "Error: --vm cannot be used with --replicate-only (replication operates on the entire backup path)" >&2
    exit "$EXIT_USAGE"
fi
if [[ "${_PRUNE_MODE}" == "true" && "${_TARGET_VM}" == *","* ]]; then
    echo "Error: --prune requires a single VM name (comma-separated list not supported)" >&2
    exit "$EXIT_USAGE"
fi
if [[ "${_CANCEL_REPLICATION_REQUESTED:-false}" == "true" ]]; then
    if [[ "${_PRUNE_MODE}" == "true" ]]; then
        echo "Error: --cancel-replication cannot be combined with --prune" >&2
        exit "$EXIT_USAGE"
    fi
    if [[ -n "${_REPLICATE_ONLY_MODE}" ]]; then
        echo "Error: --cancel-replication cannot be combined with --replicate-only" >&2
        exit "$EXIT_USAGE"
    fi
    if [[ -n "${_TARGET_VM}" ]]; then
        echo "Error: --cancel-replication cannot be combined with --vm" >&2
        exit "$EXIT_USAGE"
    fi
    if is_dry_run; then
        echo "Error: --cancel-replication cannot be combined with --dry-run" >&2
        exit "$EXIT_USAGE"
    fi
fi
if [[ "${_BACKUP_MODE}" == "true" && "${_PRUNE_MODE}" == "true" ]]; then
    echo "Error: --run and --prune cannot be used together" >&2
    exit "$EXIT_USAGE"
fi
if [[ "${_BACKUP_MODE}" == "true" && -n "${_REPLICATE_ONLY_MODE}" ]]; then
    echo "Error: --run and --replicate-only cannot be used together" >&2
    exit "$EXIT_USAGE"
fi
if [[ "${_BACKUP_MODE}" == "true" && "${_CANCEL_REPLICATION_REQUESTED:-false}" == "true" ]]; then
    echo "Error: --run and --cancel-replication cannot be used together" >&2
    exit "$EXIT_USAGE"
fi
if [[ "${_STATUS_MODE}" == "true" ]]; then
    if [[ "${_BACKUP_MODE}" == "true" ]]; then
        echo "Error: --status cannot be combined with --run" >&2
        exit "$EXIT_USAGE"
    fi
    if [[ "${_PRUNE_MODE}" == "true" ]]; then
        echo "Error: --status cannot be combined with --prune" >&2
        exit "$EXIT_USAGE"
    fi
    if [[ -n "${_REPLICATE_ONLY_MODE}" ]]; then
        echo "Error: --status cannot be combined with --replicate-only" >&2
        exit "$EXIT_USAGE"
    fi
fi
if [[ -n "${_TARGET_VM}" && "${_BACKUP_MODE}" == "false" && "${_PRUNE_MODE}" == "false" && "${_STATUS_MODE}" == "false" ]]; then
    echo "Error: --vm requires --run (for backup), --prune (for cleanup), or --status (for reports)" >&2
    echo "Example: sudo vmbackup.sh --run --vm ${_TARGET_VM}" >&2
    exit "$EXIT_USAGE"
fi

# If no mode flag given, show usage and exit
if [[ "${_BACKUP_MODE}" == "false" \
   && "${_PRUNE_MODE}" == "false" \
   && "${_STATUS_MODE}" == "false" \
   && "${_CONFIG_PRUNE_REMOVED}" == "false" \
   && -z "${_REPLICATE_ONLY_MODE}" \
   && "${_CANCEL_REPLICATION_REQUESTED:-false}" != "true" \
   && "${_CLEANUP_STALE_MANIFESTS:-false}" != "true" ]]; then
    cat << USAGE_EOF
vmbackup ${VMBACKUP_VERSION} — KVM/QEMU virtual machine backup utility
Vibe coded by James Doutsis | https://www.github.com/doutsis/

Usage:
  sudo vmbackup.sh --run                           Start full backup (all VMs)
  sudo vmbackup.sh --run --vm web,db               Backup specific VMs
  sudo vmbackup.sh --prune list                    Show backup inventory
  sudo vmbackup.sh --replicate-only                Run replication only
  sudo vmbackup.sh --cancel-replication            Cancel running replication

Options:
  --dry-run              Preview without making changes
  --config-instance NAME Use alternate config (default: "default")
  --help                 Full help and examples
  --version              Show version
USAGE_EOF
    exit 0
fi

# ── Root privilege check ─────────────────────────────────────────────────────
# vmbackup needs root for virsh, QEMU agent, backup paths, lock files, and DB.
# --help and --version exit above before reaching this point.
if [[ $EUID -ne 0 ]]; then
    echo "Error: vmbackup must be run as root (e.g. sudo $0)" >&2
    exit "$EXIT_CONFIG"
fi

export CONFIG_INSTANCE

#################################################################################
# EARLY CONFIG LOAD
# Source config file, but preserve any environment variables passed in
# (env vars take precedence over config file)
# NOTE: Use script directory, not $HOME (which changes to /root under sudo)
# NOTE: readlink -f resolves symlinks (e.g., /usr/local/bin/vmbackup → /opt/vmbackup/vmbackup.sh)
#################################################################################
_VMBACKUP_SCRIPT_DIR="$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")" && pwd)"
SCRIPT_DIR="$_VMBACKUP_SCRIPT_DIR"  # Export for modules that expect SCRIPT_DIR
_VMBACKUP_ENV_BACKUP_PATH="${BACKUP_PATH:-}"

# UNI-012: Source config-instance path resolver and use it instead of
# inline path concatenation. CONFIG_INSTANCE was already resolved by the
# pre-config-load arg parser above (L110/137/145).
# UNI-321: kept here (pre-config-load) because get_config_file is needed
# before _VMBACKUP_CONFIG can be resolved.
source_lib_or_die config_instance.sh

# F-372 (R1): dry_run.sh is sourced early (near DRY_RUN init), not here. The CLI parser
# re-invokes dry_run_normalise_state after parsing (below) to reflect a --dry-run flag.

# R1: get_config_file validates the instance name; a non-zero return means an
# invalid/unsafe --config-instance. Fail loudly before building or sourcing
# any path from it.
if ! _VMBACKUP_CONFIG=$(get_config_file "$CONFIG_INSTANCE"); then
    echo "Error: invalid config instance '${CONFIG_INSTANCE}' (allowed: letters, digits, . _ - ; no path separators)" >&2
    exit "$EXIT_CONFIG"
fi
if [[ ! -f "$_VMBACKUP_CONFIG" ]]; then
    echo "Error: Config instance '${CONFIG_INSTANCE}' not found: $_VMBACKUP_CONFIG" >&2
    exit "$EXIT_CONFIG"
fi
# FF-138: fail closed on a config SYNTAX error. With pipefail-only (no set -e)
# a syntax error inside the .conf aborts the source mid-file and execution
# would otherwise continue on a silent partial config (mis-filed backups /
# lost retention+replication settings). bash -n checks syntax WITHOUT executing,
# so a valid config whose last statement happens to return non-zero is NOT
# affected (narrower than an rc check on source itself).
if ! bash -n "$_VMBACKUP_CONFIG" 2>/dev/null; then
    echo "Error: syntax error in config instance '${CONFIG_INSTANCE}' — check $_VMBACKUP_CONFIG" >&2
    exit "$EXIT_CONFIG"
fi
source "$_VMBACKUP_CONFIG"
# Restore env var if it was set (env takes precedence over config)
[[ -n "$_VMBACKUP_ENV_BACKUP_PATH" ]] && BACKUP_PATH="$_VMBACKUP_ENV_BACKUP_PATH"

# UNI-322 (Phase 7 commit 1): re-normalise dry-run state now that DRY_RUN
# may have been set by --dry-run earlier in CLI parsing. The lib's load-
# time call already saw the global, but re-call defensively in case any
# downstream code path expects the predicates to reflect post-config state.
dry_run_normalise_state

#################################################################################
# CONFIGURATION SECTION
# 
# Priority: Environment variable > Config file > Default
# Edit config/default/vmbackup.conf to change defaults permanently.
#################################################################################

# Backup destination path (ensure trailing slash)
# [PATH-KEEPER: pre-lib-load default initialiser, runs before path_utils.sh
#  is sourced (~L860). Inline parameter expansion only.] (UNI-323)
BACKUP_PATH="${BACKUP_PATH:-/mnt/backup/vms/}"
[[ "$BACKUP_PATH" != */ ]] && BACKUP_PATH="${BACKUP_PATH}/"

# === DUP-10 ===
# Phase C: reap stale chain-manifest.json files left over from pre-decommission
# backups. Idempotent. Safe to re-run. Returns 0 even on partial failure so
# postinst does not break upgrades. Defined here (before dispatch) because
# bash registers functions at execution time, not parse time — placing the
# definition near other late helpers caused the dispatch to fail with
# "command not found" (exit 127). See follow-up to commit beaae9f.
_cleanup_stale_manifests() {
  local removed=0 examined=0
  # Logger libs are not sourced yet at this dispatch point — use echo only.
  if [[ ! -d "$BACKUP_PATH" ]]; then
    echo "vmbackup: --cleanup-stale-manifests: BACKUP_PATH not found: $BACKUP_PATH" >&2
    return 0
  fi
  local mf
  while IFS= read -r -d '' mf; do
    ((examined++))
    # FF-139: --dry-run is read-only. Preview the reap but delete nothing (and
    # do not count it, so "removed=N" stays truthful) — matches the sibling
    # _config_prune_removed which already honours is_dry_run.
    if is_dry_run; then
      echo "vmbackup: [DRY-RUN] would remove stale manifest: $mf (skipping — read-only)"
      continue
    fi
    if rm -f "$mf"; then
      ((removed++))
      echo "vmbackup: removed stale manifest: $mf"
    else
      echo "vmbackup: failed to remove: $mf" >&2
    fi
  done < <(find "$BACKUP_PATH" -maxdepth 2 -type f \
              -name 'chain-manifest.json' -print0 2>/dev/null)
  echo "vmbackup: --cleanup-stale-manifests: examined=$examined removed=$removed"
  return 0
}
# === /DUP-10 ===

# ── DUP-10: stale chain-manifest reaping (postinst-callable, idempotent) ──
if [[ "${_CLEANUP_STALE_MANIFESTS:-false}" == "true" ]]; then
    _cleanup_stale_manifests
    exit $?
fi

# ── STATUS MODE: dispatch early (read-only, no session lifecycle) ──────────
# Sources lib/sqlite_ro.sh — read-only / utility subset only (Phase 4
# UNI-605). No writer init, no schema migrations, no session tracking.
# Lightest possible code path. PRAGMA query_only=ON is set by
# sqlite_init_readonly() inside status_module.sh::run_status_report.
# UNI-321: kept conditional inside status-mode dispatch (carve-out) so
# normal backup runs don't pay the sqlite_ro load cost.
if [[ "${_STATUS_MODE}" == "true" ]]; then
    source_lib_or_die sqlite_ro.sh
    source "$SCRIPT_DIR/modules/status_module.sh" 2>/dev/null || {
        echo "Error: Status module not found" >&2; exit "$EXIT_DEPENDENCY"
    }
    # Determine sub-mode
    local_sub="${_STATUS_SUB:-default}"
    [[ -n "${_TARGET_VM}" && "$local_sub" == "default" ]] && local_sub="vm"
    # Export instance-scoping flag for sqlite_query_today_sessions()
    export STATUS_ALL_INSTANCES="${_STATUS_ALL_INSTANCES:-false}"
    run_status_report "$local_sub" "$_TARGET_VM" "${_STATUS_DAYS:-1}" "${_STATUS_CSV}"
    exit $?
fi

#################################################################################
# CONFIG PRUNE REMOVED HELPER
#
# Comments out configuration variables that have been removed from the
# codebase. Per-name allowlist with version annotation. Idempotent.
# Honours global DRY_RUN flag.
#
# To extend in future releases: add entries to _CONFIG_REMOVED_IN below.
#################################################################################

# Map: variable_name -> version_in_which_it_was_removed
declare -A _CONFIG_REMOVED_IN=(
    [OFFLINE_CHANGE_DETECTION_THRESHOLD]="0.5.5"
    [EMAIL_INCLUDE_REPLICATION]="0.5.5"
    [EMAIL_INCLUDE_DISK_SPACE]="0.5.5"
)

_config_prune_removed() {
    local config_root="${SCRIPT_DIR}/config"
    local files_scanned=0 lines_changed=0 files_changed=0
    local dry_label=""
    is_dry_run && dry_label=" (dry-run)"

    echo "vmbackup --config-prune-removed${dry_label}"
    echo "Scanning ${config_root} (excluding template/)..."
    echo ""

    # Collect allowlist keys.
    local var_names=()
    local key
    for key in "${!_CONFIG_REMOVED_IN[@]}"; do
        var_names+=("$key")
    done

    # Find all *.conf files outside template/.
    local conf_file
    while IFS= read -r -d '' conf_file; do
        ((files_scanned++))
        local file_changes=0
        local var
        for var in "${var_names[@]}"; do
            local removed_in="${_CONFIG_REMOVED_IN[$var]}"
            # Match lines that start with optional whitespace + VAR= (live, not commented).
            local match_pattern="^[[:space:]]*${var}="
            if grep -qE "$match_pattern" "$conf_file" 2>/dev/null; then
                local matches
                matches=$(grep -cE "$match_pattern" "$conf_file" 2>/dev/null)
                file_changes=$((file_changes + matches))
                if is_dry_run; then
                    echo "  [${conf_file}] would comment ${matches} line(s) for ${var}"
                else
                    # Comment out matching lines, prefixing with marker. Uses awk for
                    # safety vs sed-with-special-chars in arbitrary captured values.
                    local tmp_file
                    tmp_file=$(mktemp) || { echo "Error: mktemp failed" >&2; return 1; }
                    awk -v var="$var" -v ver="$removed_in" '
                        $0 ~ "^[[:space:]]*"var"=" {
                            print "# REMOVED in v"ver" — was: "$0
                            next
                        }
                        { print }
                    ' "$conf_file" > "$tmp_file" || { rm -f "$tmp_file"; echo "Error: awk failed on $conf_file" >&2; return 1; }
                    # Preserve original ownership and permissions.
                    chmod --reference="$conf_file" "$tmp_file"
                    chown --reference="$conf_file" "$tmp_file"
                    mv "$tmp_file" "$conf_file"
                    echo "  [${conf_file}] commented ${matches} line(s) for ${var}"
                fi
            fi
        done
        if (( file_changes > 0 )); then
            ((files_changed++))
            lines_changed=$((lines_changed + file_changes))
        fi
    # FF-140: -mindepth 2 suppresses ALL tests/actions (including -prune) at
    # depth 1, so the depth-1 template/ dir was never pruned and its depth-2
    # *.conf files were scanned. Drop -mindepth so -prune fires on template/.
    done < <(find "$config_root" -path "${config_root}/template" -prune -o -type f -name '*.conf' -print0 2>/dev/null)

    echo ""
    echo "Scanned ${files_scanned} file(s); ${files_changed} file(s) with matches; ${lines_changed} line(s)${dry_label}."
    if is_dry_run && (( lines_changed > 0 )); then
        echo "Re-run without --dry-run to apply."
    fi
    return 0
}

# ── CONFIG PRUNE REMOVED MODE: dispatch early ─────────────────────────────────
# Cleans up configuration variables removed in this release. Comments lines
# rather than deleting them; idempotent; honours --dry-run.
if [[ "${_CONFIG_PRUNE_REMOVED}" == "true" ]]; then
    _config_prune_removed
    exit $?
fi


#################################################################################
# VM-FIRST DIRECTORY STRUCTURE (v3.0)
# 
# Backup paths: /backup/vm_name/period/
# Features:
#   - Per-VM rotation policies (daily/weekly/monthly/accumulate/never)
#   - Chain manifest tracking (JSON)
#   - SQLite logging
#   - Automatic retention management
#################################################################################

# Log level (can be set in config or here)
# Values: ERROR, WARN, INFO, DEBUG
# Default: INFO (from config)
LOG_LEVEL="${LOG_LEVEL:-INFO}"

# Adaptive backup timeout - monitors actual progress instead of wall-clock limit
# Prevents killing healthy large backups while still detecting true hangs
BACKUP_STARTUP_GRACE="${BACKUP_STARTUP_GRACE:-300}"      # Grace period (5 min) for NBD setup, VM pause, checkpoint creation
BACKUP_STALL_THRESHOLD="${BACKUP_STALL_THRESHOLD:-180}"    # Declare backup hung if no I/O for this many seconds (3 min)
BACKUP_CHECK_INTERVAL="${BACKUP_CHECK_INTERVAL:-30}"      # Check backup progress every N seconds
# R2: max consecutive filesystem-freeze stall-kills before perform_backup()
# stops retrying and fails closed with LAST_ERROR_CODE=FSFREEZE_STALL. A single
# transient stall that recovers on a later attempt must NOT trip this; only
# repeated stalls reaching this count do. virtnbdbackup always freezes via the
# guest agent (the engine has no freeze opt-out; -F only narrows mountpoints),
# so a recurring freeze-stall means the guest agent is unhealthy and burning
# more attempts just hangs again.
BACKUP_FSFREEZE_STALL_LIMIT="${BACKUP_FSFREEZE_STALL_LIMIT:-2}"

#################################################################################
# VIRTNBDBACKUP COMMAND OPTIONS
#################################################################################

# Native virtnbdbackup compression level (LZ4 fast / LZ4 HC)
#   0     = BROKEN in virtnbdbackup <= 2.28 (auto-corrected to 1)
#   1-2   = LZ4 fast mode (500+ MiB/s, near-identical ratio to HC)
#   3-16  = LZ4 HC (high compression) — 10-30x slower, <1% better ratio
# Default: 4 (minimum HC — good ratio without excessive CPU)
VIRTNBD_COMPRESS_LEVEL="${VIRTNBD_COMPRESS_LEVEL:-4}"

# Guard: --compress=0 crashes virtnbdbackup <= 2.28 (Python truthiness bug
# in backup/disk.py line 188: 'if args.compress:' treats 0 as False,
# hitting 'assert size == save.length' because compression WAS applied).
if [[ "$VIRTNBD_COMPRESS_LEVEL" -eq 0 ]] 2>/dev/null; then
  VIRTNBD_COMPRESS_LEVEL=1
  echo "[WARN] VIRTNBD_COMPRESS_LEVEL=0 is broken in virtnbdbackup <= 2.28 — using level 1 (LZ4 fast) instead" >&2
fi

# Worker threads for parallel disk backup (1=sequential, auto=detect)
VIRTNBD_WORKERS="${VIRTNBD_WORKERS:-auto}"

# Exclude disks by device name (comma-separated, e.g., "sdb,sdc")
VIRTNBD_EXCLUDE_DISKS="${VIRTNBD_EXCLUDE_DISKS:-}"

# Include only specific disks (overrides exclude)
VIRTNBD_INCLUDE_DISKS="${VIRTNBD_INCLUDE_DISKS:-}"

# Filesystem freeze (true=use QEMU agent to quiesce filesystems)
VIRTNBD_FSFREEZE="${VIRTNBD_FSFREEZE:-true}"

# Filesystems to freeze (leave empty for all, or specify: "/mnt,/var")
VIRTNBD_FSFREEZE_PATHS="${VIRTNBD_FSFREEZE_PATHS:-}"

# Backup threshold in bytes (only backup if delta >= threshold)
VIRTNBD_THRESHOLD="${VIRTNBD_THRESHOLD:-}"

# Backup output format: stream (thin-prov default) or raw (full-prov)
VIRTNBD_OUTPUT_FORMAT="${VIRTNBD_OUTPUT_FORMAT:-stream}"

# Use sparse detection (skip trimmed blocks, default=true)
VIRTNBD_SPARSE_DETECTION="${VIRTNBD_SPARSE_DETECTION:-true}"

# Scratch directory for fleece operations (default /var/tmp)
VIRTNBD_SCRATCH_DIR="${VIRTNBD_SCRATCH_DIR:-/var/tmp}"

#################################################################################
# PROCESS AND OPERATIONAL SETTINGS
#################################################################################

# Process priority settings for backup operations
# These control how "polite" the backup process is to other system tasks
#
# CPU Priority (nice): -20=highest, 0=normal, 19=lowest
# Lower values = more CPU time = faster backups but more VM impact
PROCESS_PRIORITY="${PROCESS_PRIORITY:-10}"

# I/O Priority Class (ionice -c):
#   1 = Realtime (use with caution, can starve other I/O)
#   2 = Best-effort (normal, respects nice level)
#   3 = Idle (only when disk is idle - very slow)
IO_PRIORITY_CLASS="${IO_PRIORITY_CLASS:-2}"

# I/O Priority Level (ionice -n): 0-7 (only for class 2)
#   0 = highest priority within class
#   4 = normal
#   7 = lowest priority within class
IO_PRIORITY_LEVEL="${IO_PRIORITY_LEVEL:-5}"

#################################################################################
# CHECKPOINT MANAGEMENT STRATEGY (Monthly Rotation with Health Checks)
#################################################################################

# Intelligent monthly backup strategy:
# - Day 1 of month: Always FULL backup (resets all checkpoints)
# - Days 2-28: AUTO mode (incremental if checkpoint healthy)
CHECKPOINT_FORCE_FULL_ON_DAY="${CHECKPOINT_FORCE_FULL_ON_DAY:-1}"        # Day of month for full backup
CHECKPOINT_HEALTH_CHECK="${CHECKPOINT_HEALTH_CHECK:-yes}"            # Validate checkpoint before AUTO mode
CHECKPOINT_MAX_DEPTH_WARN="${CHECKPOINT_MAX_DEPTH_WARN:-10}"          # Warn if chain exceeds this
CHECKPOINT_RETRY_AUTO_TO_FULL="${CHECKPOINT_RETRY_AUTO_TO_FULL:-yes}"     # Convert to FULL if AUTO fails
CHECKPOINT_MAX_RETRIES_AUTO="${CHECKPOINT_MAX_RETRIES_AUTO:-1}"         # Max AUTO retries before FULL

#################################################################################
# OPERATIONAL SETTINGS - LOADED FROM CONFIG
#
# These settings are now loaded from config/<instance>/vmbackup.conf
# If missing from config, safe defaults are applied with EXPLICIT warnings.
# See validate_operational_settings() for default values and logging.
#################################################################################

# Retry configuration
MAX_RETRIES="${MAX_RETRIES:-3}"
RETRY_DELAY="${RETRY_DELAY:-30}"  # seconds

# State directory (locks, logs, recovery flags)
STATE_DIR="${BACKUP_PATH%/}/_state"
LOCK_DIR="${STATE_DIR}/locks"
TEMP_DIR="${STATE_DIR}/temp"           # Temporary files and recovery flags
LOG_DIR="${STATE_DIR}/logs"            # Per-backup log files (virtnbdbackup output)

# Log file
LOG_FILE="${LOG_DIR}/vmbackup.log"

#################################################################################
# CANCEL-REPLICATION HANDLER
#
# When --cancel-replication is passed, touch the flag file and exit immediately.
# A running vmbackup session detects this file and gracefully terminates
# replication (rsync/rclone), logging "cancelled" status to the database.
# Backup operations are unaffected.
#################################################################################
CANCEL_REPLICATION_FLAG="${STATE_DIR}/cancel-replication"

if [[ "${_CANCEL_REPLICATION_REQUESTED:-false}" == "true" ]]; then
    # FF-142: fail closed on a control op. If STATE_DIR can't be created or the
    # flag write fails (unmounted / read-only backup volume), do NOT report
    # success — a running session would never see a flag that was never written.
    if ! mkdir -p "$STATE_DIR" 2>/dev/null; then
        echo "Error: cannot create state directory: $STATE_DIR" >&2
        exit "$EXIT_STORAGE"
    fi
    if ! echo "$(date '+%Y-%m-%d %H:%M:%S') PID=$$ user=$(whoami)" > "$CANCEL_REPLICATION_FLAG"; then
        echo "Error: failed to write cancel-replication flag: $CANCEL_REPLICATION_FLAG" >&2
        exit "$EXIT_STORAGE"
    fi
    echo "Replication cancellation requested."
    echo "Flag file created: $CANCEL_REPLICATION_FLAG"
    echo "Active replication jobs will terminate gracefully within ~30 seconds."
    exit 0
fi

#################################################################################
# PRUNE MODE VALIDATION
#
# When --prune is passed, validate flags and redirect logging to vmprune.log.
# Prune is NOT a backup session — it uses separate logging.
#################################################################################
if [[ "${_PRUNE_MODE:-false}" == "true" ]]; then
    # Validate: --prune requires a target
    if [[ -z "$_PRUNE_TARGET" ]]; then
        echo "Error: --prune requires a target (e.g. --prune list, --prune archives, --prune all)"
        echo "Run vmbackup.sh --help for usage."
        exit "$EXIT_USAGE"
    fi
    
    # Validate: --prune incompatible with --cancel-replication
    if [[ "${_CANCEL_REPLICATION_REQUESTED:-false}" == "true" ]]; then
        echo "Error: --prune cannot be combined with --cancel-replication"
        exit "$EXIT_USAGE"
    fi
    
    # Validate target syntax
    case "$_PRUNE_TARGET" in
        list|archives|all)
            ;;
        archives:*|chain:*|period:*)
            ;;
        *)
            echo "Error: Unknown prune target: $_PRUNE_TARGET"
            echo "Valid targets: list, archives, archives:<period>, chain:<name>, period:<id>, all"
            echo "Run vmbackup.sh --help for usage."
            exit "$EXIT_USAGE"
            ;;
    esac
    
    # Validate: --vm required for certain targets
    case "$_PRUNE_TARGET" in
        all|period:*|archives:*|chain:*)
            if [[ -z "$_TARGET_VM" ]]; then
                echo "Error: --vm is required for --prune $_PRUNE_TARGET"
                echo "Example: sudo ./vmbackup.sh --prune $_PRUNE_TARGET --vm <vm-name>"
                exit "$EXIT_USAGE"
            fi
            ;;
    esac
    
    # Redirect log file for prune operations
    LOG_FILE="${LOG_DIR}/vmprune.log"
fi

#################################################################################
# GLOBAL ERROR TRACKING
# These variables capture detailed error information for reporting
#################################################################################

# Error tracking globals (set by various functions, read by backup_vm for reporting)
LAST_ERROR_CODE=""          # Specific error code (e.g., CHECKPOINT_CORRUPTION, VIRTNBD_EXIT_1)
LAST_ERROR_DETAIL=""        # Human-readable error description
LAST_ERROR_CONTEXT=""       # Additional context (e.g., virtnbdbackup log tail)

# Reset error tracking for a new VM backup
reset_error_tracking() {
  LAST_ERROR_CODE=""
  LAST_ERROR_DETAIL=""
  LAST_ERROR_CONTEXT=""
}

# Set error with code and detail
set_backup_error() {
  local code="$1"
  local detail="$2"
  local context="${3:-}"
  
  LAST_ERROR_CODE="$code"
  LAST_ERROR_DETAIL="$detail"
  LAST_ERROR_CONTEXT="$context"
  
  log_debug "vmbackup.sh" "set_backup_error" "Error set: code=$code, detail=$detail"
}

#################################################################################
# LOGGING FUNCTIONS
#################################################################################

# Initialize logging
init_logging() {
  mkdir -p "$(dirname "$LOG_FILE")"
  mkdir -p "$TEMP_DIR"
  
  # Security: ensure state/log directories are owned by backup group
  set_backup_permissions "${STATE_DIR}"
  set_backup_permissions "$(dirname "$LOG_FILE")"
  set_backup_permissions "$TEMP_DIR"
  
  # Write header
  {
    echo ""
    echo "================================================================================"
    echo "VM Backup Session Started: $(date '+%Y-%m-%d %H:%M:%S')"
    echo "vmbackup v${VMBACKUP_VERSION}"
    echo "Vibe coded by James Doutsis | https://www.github.com/doutsis/"
    echo "================================================================================"
  } >> "$LOG_FILE"
}

# UNI-321: Source shared logging library (was bare `source`; now hard-fail
# on missing lib via source_lib_or_die — matches vmrestore behaviour).
source_lib_or_die logging.sh

# UNI-323 (Phase 7 commit 2): Source canonical path-canonicalisation API
# (pu_normalise_path, pu_safe_realpath, pu_strip_trailing_slash,
# pu_ensure_trailing_slash, pu_join_paths). Strict-alphabetical position:
# logging < path_utils < period. No load-order dependency (pure functions).
source_lib_or_die path_utils.sh

# UNI-011: Source VM-name sanitiser early so all --prune call sites have a
# defined sanitize_vm_name() (was previously undefined; only loaded
# transitively via modules/rotation_module.sh inside _status_policies()).
source_lib_or_die vm_name_utils.sh

# UNI-007 (Phase 3): Source period helpers at startup (G-J). The four
# functions (get_vm_periods, get_vm_periods_for_policy, detect_period_policy,
# calculate_any_period_age) are consumed by modules/retention_module.sh from
# multiple call sites; eager sourcing avoids the lazy-load gate footgun.
source_lib_or_die period.sh

# UNI-309 (Phase 3): Source backup-tree walker (skeletal, D2). Provides
# walk_backup_tree() with VM- and period-level skip-list enforcement. Used
# by _prune_list() (this file) and list_vms() (vmrestore.sh) as the single
# source of truth for backup-tree iteration / skip-list convergence (B).
source_lib_or_die backup_walker.sh

# UNI-006 (Phase 6 commit 2): Source shared TPM read-side helpers so inline
# TPM/UUID code in this binary (and in vmrestore.sh) does not depend on the
# lazy-loaded modules/tpm_backup_module.sh. The module itself sources this
# lib as the one allowed module-level source_lib_or_die exception.
source_lib_or_die tpm_io.sh

# UNI-014 M1 (Phase 6 commit 3): Source virtnbd arg-builder helpers.
# perform_backup() builds its virtnbdbackup argv via build_virtnbdbackup_args
# (string→array refactor — see 109-phase6-spec.md §2.3).
source_lib_or_die virtnbd.sh

# UNI-013 (Phase 6 commit 4): Source libvirt domain/XML/pool helpers.
# Replaces 39 inline virsh callsites with lv_* wrappers (see
# 109-phase6-spec.md §2.4 and tests/baselines/phase6/libvirt-fn-table.md).
source_lib_or_die libvirt.sh

# ----- die helper -----
# Log an error and exit with the given code. Default code is EXIT_ERROR (1).
# Must be defined after lib/logging.sh is sourced (uses log_error).
die() {
    local msg="$1"
    local ctx="${2:-main}"
    local code="${3:-$EXIT_ERROR}"
    log_error "vmbackup.sh" "$ctx" "$msg"
    exit "$code"
}

#################################################################################
# SECURITY: BACKUP PERMISSIONS HELPER
#################################################################################

# Ensure BACKUP_PATH has correct ownership and SGID before anything else.
#
# On a fresh install the user only needs: mkdir -p /path/to/backups
# This function detects a missing backup group or SGID bit and fixes it,
# so init_logging() and everything after inherits the correct group.
#
# Called once at the very start of main(), before init_logging().
ensure_backup_path_sgid() {
  [[ ! -d "$BACKUP_PATH" ]] && return 0          # check_backup_destination will catch this later
  getent group backup >/dev/null 2>&1 || return 0  # no backup group — nothing to do

  local current_group
  current_group=$(stat -c '%G' "$BACKUP_PATH" 2>/dev/null)
  local has_sgid
  has_sgid=$(stat -c '%a' "$BACKUP_PATH" 2>/dev/null)

  if [[ "$current_group" != "backup" || "${has_sgid:0:1}" != "2" ]]; then
    # FF-159: --dry-run is read-only end-to-end. This runs as main()'s first
    # statement (before the dry-run banner); preview the SGID bootstrap but
    # mutate nothing. (dry-run-must-be-readonly class.)
    if is_dry_run; then
      echo "[vmbackup] [DRY-RUN] Would set $BACKUP_PATH to root:backup 2750 (skipping — read-only)" >&2
      return 0
    fi
    chown root:backup "$BACKUP_PATH" 2>/dev/null ||
      echo "[vmbackup] WARN: Failed to set ownership root:backup on $BACKUP_PATH" >&2
    chmod 2750 "$BACKUP_PATH" 2>/dev/null ||
      echo "[vmbackup] WARN: Failed to set permissions 2750 on $BACKUP_PATH" >&2
    # Log to stderr since logging isn't initialised yet
    echo "[vmbackup] SGID bootstrap: set $BACKUP_PATH to root:backup 2750" >&2
  fi
}

# Apply backup ownership and SGID to a directory tree.
#
# SGID (setgid) on directories causes new files and subdirectories to
# automatically inherit the directory's group (backup). This means files
# are born root:backup — no post-hoc chown needed on individual files.
#
# Usage:
#   set_backup_permissions "/path"               — single path
#   set_backup_permissions "/path" --recursive   — full tree
#
# Called from:
#   init_logging()            — state/log/temp dirs (before recursive sweep)
#   check_backup_destination()— recursive sweep at session start
#   perform_backup()          — safety net after virtnbdbackup (external tool)
#
# Excludes tpm-state/ (TPM private keys — must stay root:root 600).
set_backup_permissions() {
  local target_path="$1"
  local recursive="${2:-}"
  [[ -z "$target_path" || ! -e "$target_path" ]] && return 0

  # Only apply if the backup group exists
  if getent group backup >/dev/null 2>&1; then
    if [[ "$recursive" == "--recursive" ]]; then
      # Exclude tpm-state/ from ownership change (TPM private keys — root:root 600)
      find "$target_path" \
        -not -path '*/tpm-state/*' -not -path '*/tpm-state' \
        -exec chown root:backup {} + 2>/dev/null ||
        log_warn "vmbackup.sh" "set_backup_permissions" "Failed to set ownership on $target_path (recursive)"
      # Set SGID on directories for automatic group inheritance
      find "$target_path" -type d \
        -not -path '*/tpm-state/*' -not -path '*/tpm-state' \
        -exec chmod g+s {} + 2>/dev/null ||
        log_warn "vmbackup.sh" "set_backup_permissions" "Failed to set SGID on directories in $target_path"
    else
      chown root:backup "$target_path" 2>/dev/null ||
        log_warn "vmbackup.sh" "set_backup_permissions" "Failed to set ownership on $target_path"
      # Set SGID on directories for automatic group inheritance
      if [[ -d "$target_path" ]]; then
        chmod g+s "$target_path" 2>/dev/null ||
          log_warn "vmbackup.sh" "set_backup_permissions" "Failed to set SGID on $target_path"
      fi
    fi
  fi
}

#################################################################################
# TPM BACKUP MODULE SOURCING
#################################################################################

# Source TPM backup module if available
load_tpm_backup_module() {
  # Get the directory of the MAIN script (vmbackup.sh), not the sourced script
  local script_dir="${SCRIPT_DIR:-$(dirname "$(readlink -f "$0")")}"
  local tpm_module="$script_dir/modules/tpm_backup_module.sh"
  
  log_info "vmbackup.sh" "load_tpm_backup_module" "Attempting to load TPM backup module (looking in: $script_dir)"
  
  if [[ -f "$tpm_module" ]]; then
    if source "$tpm_module" 2>/dev/null; then
      log_info "vmbackup.sh" "load_tpm_backup_module" "TPM backup module loaded successfully"
      TPM_BACKUP_MODULE_LOADED=1
      return 0
    else
      log_warn "vmbackup.sh" "load_tpm_backup_module" "Failed to source TPM module: $tpm_module (syntax error?)"
      return 1
    fi
  else
    log_warn "vmbackup.sh" "load_tpm_backup_module" "TPM backup module not found at: $tpm_module (TPM backup will be skipped)"
    return 1
  fi
}

#################################################################################
# OPERATIONAL SETTINGS VALIDATION
#
# Validates settings from config/<instance>/vmbackup.conf
# Applies safe defaults with EXPLICIT logging if settings are missing.
#################################################################################

validate_operational_settings() {
  local instance="${CONFIG_INSTANCE:-default}"
  local missing_count=0
  
  log_info "vmbackup.sh" "validate_operational_settings" "Validating operational settings for instance: $instance"
  
  #-----------------------------------------------------------------------------
  # FSTRIM Settings
  #-----------------------------------------------------------------------------
  if [[ -z "${ENABLE_FSTRIM+x}" ]]; then
    ENABLE_FSTRIM="true"
    log_warn "vmbackup.sh" "validate_operational_settings" "MISSING: ENABLE_FSTRIM not set in config/$instance/vmbackup.conf"
    log_warn "vmbackup.sh" "validate_operational_settings" "USING DEFAULT: ENABLE_FSTRIM=true (fstrim enabled — set to false to disable)"
    ((missing_count++))
  else
    log_debug "vmbackup.sh" "validate_operational_settings" "ENABLE_FSTRIM=$ENABLE_FSTRIM (from config)"
  fi
  
  if [[ -z "${FSTRIM_MINIMUM+x}" ]]; then
    FSTRIM_MINIMUM=1048576
    log_warn "vmbackup.sh" "validate_operational_settings" "MISSING: FSTRIM_MINIMUM not set in config/$instance/vmbackup.conf"
    log_warn "vmbackup.sh" "validate_operational_settings" "USING DEFAULT: FSTRIM_MINIMUM=1048576 (1 MB — Linux only, Windows ignores this)"
    ((missing_count++))
  else
    log_debug "vmbackup.sh" "validate_operational_settings" "FSTRIM_MINIMUM=$FSTRIM_MINIMUM (from config)"
  fi
  
  if [[ -z "${FSTRIM_TIMEOUT+x}" ]]; then
    FSTRIM_TIMEOUT=300
    log_warn "vmbackup.sh" "validate_operational_settings" "MISSING: FSTRIM_TIMEOUT not set in config/$instance/vmbackup.conf"
    log_warn "vmbackup.sh" "validate_operational_settings" "USING DEFAULT: FSTRIM_TIMEOUT=300 (5 minutes, Linux guests)"
    ((missing_count++))
  else
    log_debug "vmbackup.sh" "validate_operational_settings" "FSTRIM_TIMEOUT=$FSTRIM_TIMEOUT (from config)"
  fi
  
  if [[ -z "${FSTRIM_WINDOWS_TIMEOUT+x}" ]]; then
    FSTRIM_WINDOWS_TIMEOUT=600
    log_warn "vmbackup.sh" "validate_operational_settings" "MISSING: FSTRIM_WINDOWS_TIMEOUT not set in config/$instance/vmbackup.conf"
    log_warn "vmbackup.sh" "validate_operational_settings" "USING DEFAULT: FSTRIM_WINDOWS_TIMEOUT=600 (10 minutes, Windows guests — apply discard_granularity XML fix for <2s)"
    ((missing_count++))
  else
    log_debug "vmbackup.sh" "validate_operational_settings" "FSTRIM_WINDOWS_TIMEOUT=$FSTRIM_WINDOWS_TIMEOUT (from config)"
  fi
  
  if [[ -z "${FSTRIM_EXCLUDE_FILE+x}" ]]; then
    FSTRIM_EXCLUDE_FILE="fstrim_exclude.conf"
    log_debug "vmbackup.sh" "validate_operational_settings" "FSTRIM_EXCLUDE_FILE not set, using default: fstrim_exclude.conf"
  else
    log_debug "vmbackup.sh" "validate_operational_settings" "FSTRIM_EXCLUDE_FILE=$FSTRIM_EXCLUDE_FILE (from config)"
  fi
  
  #-----------------------------------------------------------------------------
  # Offline VM Optimization Settings
  #-----------------------------------------------------------------------------
  if [[ -z "${SKIP_OFFLINE_UNCHANGED_BACKUPS+x}" ]]; then
    SKIP_OFFLINE_UNCHANGED_BACKUPS="true"
    log_warn "vmbackup.sh" "validate_operational_settings" "MISSING: SKIP_OFFLINE_UNCHANGED_BACKUPS not set in config/$instance/vmbackup.conf"
    log_warn "vmbackup.sh" "validate_operational_settings" "USING DEFAULT: SKIP_OFFLINE_UNCHANGED_BACKUPS=true (skip unchanged offline VMs)"
    ((missing_count++))
  else
    log_debug "vmbackup.sh" "validate_operational_settings" "SKIP_OFFLINE_UNCHANGED_BACKUPS=$SKIP_OFFLINE_UNCHANGED_BACKUPS (from config)"
  fi
  
  #-----------------------------------------------------------------------------
  # Checkpoint Recovery Settings
  #-----------------------------------------------------------------------------
  if [[ -z "${ENABLE_AUTO_RECOVERY_ON_CHECKPOINT_CORRUPTION+x}" ]]; then
    ENABLE_AUTO_RECOVERY_ON_CHECKPOINT_CORRUPTION="yes"
    log_warn "vmbackup.sh" "validate_operational_settings" "MISSING: ENABLE_AUTO_RECOVERY_ON_CHECKPOINT_CORRUPTION not set in config/$instance/vmbackup.conf"
    log_warn "vmbackup.sh" "validate_operational_settings" "USING DEFAULT: ENABLE_AUTO_RECOVERY_ON_CHECKPOINT_CORRUPTION=yes (self-healing enabled)"
    ((missing_count++))
  else
    log_debug "vmbackup.sh" "validate_operational_settings" "ENABLE_AUTO_RECOVERY_ON_CHECKPOINT_CORRUPTION=$ENABLE_AUTO_RECOVERY_ON_CHECKPOINT_CORRUPTION (from config)"
  fi
  
  # Validate checkpoint recovery value
  case "$ENABLE_AUTO_RECOVERY_ON_CHECKPOINT_CORRUPTION" in
    yes|warn|no)
      ;;
    *)
      log_warn "vmbackup.sh" "validate_operational_settings" "INVALID: ENABLE_AUTO_RECOVERY_ON_CHECKPOINT_CORRUPTION='$ENABLE_AUTO_RECOVERY_ON_CHECKPOINT_CORRUPTION' (must be yes/warn/no)"
      log_warn "vmbackup.sh" "validate_operational_settings" "USING DEFAULT: ENABLE_AUTO_RECOVERY_ON_CHECKPOINT_CORRUPTION=yes"
      ENABLE_AUTO_RECOVERY_ON_CHECKPOINT_CORRUPTION="yes"
      ;;
  esac

  #-----------------------------------------------------------------------------
  # I/O / Process Priority Settings
  #-----------------------------------------------------------------------------
  # SEC-02 Vector-2: these values word-split UNQUOTED into the ionice/nice priority
  # wrapper in perform_backup (split into argv by design), so a malformed root-owned
  # config value would garble the wrapped command. Reject-and-default to a valid
  # integer here — the word-split of a VALIDATED integer is safe and is the design.
  # (10# forces base-10 so a zero-padded value like "08"/"09" isn't read as octal.)
  if [[ ! "$IO_PRIORITY_CLASS" =~ ^[0-9]+$ ]] || (( 10#$IO_PRIORITY_CLASS < 1 || 10#$IO_PRIORITY_CLASS > 3 )); then
    log_warn "vmbackup.sh" "validate_operational_settings" "INVALID: IO_PRIORITY_CLASS='$IO_PRIORITY_CLASS' (must be integer 1-3: 1=realtime, 2=best-effort, 3=idle)"
    log_warn "vmbackup.sh" "validate_operational_settings" "USING DEFAULT: IO_PRIORITY_CLASS=2 (best-effort)"
    IO_PRIORITY_CLASS=2
  else
    log_debug "vmbackup.sh" "validate_operational_settings" "IO_PRIORITY_CLASS=$IO_PRIORITY_CLASS (from config)"
  fi

  if [[ ! "$IO_PRIORITY_LEVEL" =~ ^[0-9]+$ ]] || (( 10#$IO_PRIORITY_LEVEL < 0 || 10#$IO_PRIORITY_LEVEL > 7 )); then
    log_warn "vmbackup.sh" "validate_operational_settings" "INVALID: IO_PRIORITY_LEVEL='$IO_PRIORITY_LEVEL' (must be integer 0-7)"
    log_warn "vmbackup.sh" "validate_operational_settings" "USING DEFAULT: IO_PRIORITY_LEVEL=5 (normal)"
    IO_PRIORITY_LEVEL=5
  else
    log_debug "vmbackup.sh" "validate_operational_settings" "IO_PRIORITY_LEVEL=$IO_PRIORITY_LEVEL (from config)"
  fi

  # PROCESS_PRIORITY (nice) is the third SEC-02 Vector-2 lever: it word-splits
  # UNQUOTED into the same priority_wrapper (perform_backup) AND is arithmetic-
  # evaluated in `(( PROCESS_PRIORITY != 0 ))`. Signed integer -20..19; reject a
  # leading-zero form too so the arithmetic context is octal-safe.
  if [[ ! "$PROCESS_PRIORITY" =~ ^-?(0|[1-9][0-9]*)$ ]]; then
    log_warn "vmbackup.sh" "validate_operational_settings" "INVALID: PROCESS_PRIORITY='$PROCESS_PRIORITY' (must be integer -20..19)"
    log_warn "vmbackup.sh" "validate_operational_settings" "USING DEFAULT: PROCESS_PRIORITY=10 (low)"
    PROCESS_PRIORITY=10
  else
    log_debug "vmbackup.sh" "validate_operational_settings" "PROCESS_PRIORITY=$PROCESS_PRIORITY (from config)"
  fi

  #-----------------------------------------------------------------------------
  # Summary
  #-----------------------------------------------------------------------------
  if [[ $missing_count -gt 0 ]]; then
    log_warn "vmbackup.sh" "validate_operational_settings" "=========================================="
    log_warn "vmbackup.sh" "validate_operational_settings" "$missing_count operational setting(s) missing from config/$instance/vmbackup.conf"
    log_warn "vmbackup.sh" "validate_operational_settings" "Safe defaults applied - add settings to config to suppress these warnings"
    log_warn "vmbackup.sh" "validate_operational_settings" "See config/template/vmbackup.conf for documentation"
    log_warn "vmbackup.sh" "validate_operational_settings" "=========================================="
  else
    log_info "vmbackup.sh" "validate_operational_settings" "All operational settings loaded from config"
  fi
  
  # Log final effective settings
  log_info "vmbackup.sh" "validate_operational_settings" "Effective settings: FSTRIM=$ENABLE_FSTRIM, SKIP_OFFLINE=$SKIP_OFFLINE_UNCHANGED_BACKUPS, RECOVERY=$ENABLE_AUTO_RECOVERY_ON_CHECKPOINT_CORRUPTION"
  
  return 0
}

#################################################################################
# LOCAL REPLICATION MODULE SOURCING
#################################################################################

# Load local replication module if available
# Provides offsite/secondary backup replication functionality
init_local_replication_module() {
  local script_dir="${SCRIPT_DIR:-$(dirname "$(readlink -f "$0")")}"
  local repl_module="$script_dir/modules/replication_local_module.sh"
  
  log_info "vmbackup.sh" "init_local_replication_module" "Checking for local replication module"
  
  if [[ -f "$repl_module" ]]; then
    if source "$repl_module" 2>/dev/null; then
      log_debug "vmbackup.sh" "init_local_replication_module" "Local replication module sourced successfully"
      # Initialize the module (loads config, validates destinations)
      # Note: load_local_replication_module is the function from replication_local_module.sh
      if load_local_replication_module 2>/dev/null; then
        log_info "vmbackup.sh" "init_local_replication_module" "Local replication module initialized"
        LOCAL_REPLICATION_MODULE_AVAILABLE=1
        return 0
      else
        log_info "vmbackup.sh" "init_local_replication_module" "Local replication disabled or no valid destinations"
        LOCAL_REPLICATION_MODULE_AVAILABLE=0
        return 1
      fi
    else
      log_warn "vmbackup.sh" "init_local_replication_module" "Failed to source local replication module: $repl_module (syntax error?)"
      LOCAL_REPLICATION_MODULE_AVAILABLE=0
      return 1
    fi
  else
    log_debug "vmbackup.sh" "init_local_replication_module" "Local replication module not found at: $repl_module (replication disabled)"
    LOCAL_REPLICATION_MODULE_AVAILABLE=0
    return 1
  fi
}

#################################################################################
# REPLICATION CANCELLATION SUPPORT
#
# Flag-file based cancellation for graceful replication shutdown.
# Operator creates the flag file (via --cancel-replication or manual touch),
# and replication loops detect it at multiple check points:
#   1. Before each destination in replicate_batch / run_cloud_replication_batch
#   2. During rsync monitoring loop (transport_local.sh) — kills rsync
#   3. During rclone monitoring loop (cloud_transport_sharepoint.sh) — kills rclone
#
# Flag file: $STATE_DIR/cancel-replication
# Status logged to DB: "cancelled" with error_message "Replication cancelled by operator"
#################################################################################

# Check if replication cancellation has been requested via flag file
# Returns: 0 if cancelled (true), 1 if not (false)
is_replication_cancelled() {
    [[ -f "${CANCEL_REPLICATION_FLAG:-${STATE_DIR}/cancel-replication}" ]]
}

# Remove the cancellation flag file after processing
clear_replication_cancel_flag() {
    # FF-162: --dry-run is read-only. The cancel flag is an operator-armed
    # control file (pre-created via --cancel-replication to suppress the NEXT
    # real session); consuming it during a preview silently loses the pending
    # cancellation. Preview and keep it. (dry-run-must-be-readonly class.)
    if is_dry_run; then
        [[ -f "${CANCEL_REPLICATION_FLAG:-${STATE_DIR}/cancel-replication}" ]] && \
          log_info "vmbackup.sh" "clear_replication_cancel_flag" \
            "[DRY-RUN] Would clear replication cancel flag - skipping (read-only)"
        return 0
    fi
    if [[ -f "${CANCEL_REPLICATION_FLAG:-${STATE_DIR}/cancel-replication}" ]]; then
        local flag_contents
        flag_contents=$(cat "$CANCEL_REPLICATION_FLAG" 2>/dev/null)
        rm -f "$CANCEL_REPLICATION_FLAG"
        log_info "vmbackup.sh" "clear_replication_cancel_flag" \
            "Replication cancel flag removed (was: $flag_contents)"
    fi
}

#################################################################################
# LOCAL REPLICATION WRAPPER FUNCTIONS
#
# These functions provide a clean interface between vmbackup.sh and the
# replication_local_module.sh. They handle module availability checks and provide
# consistent logging.
#
# Batch replication runs once after ALL VMs are backed up.
#################################################################################

#-------------------------------------------------------------------------------
# run_local_replication_batch - Wrapper for batch mode local replication
#
# Called at session end (after all VMs backed up)
# Replicates the entire backup directory tree to all enabled destinations.
#
# Arguments:
#   $1 - backup_path: Root backup directory (default: $BACKUP_PATH)
#
# Returns:
#   0 - Replication successful or module not available
#   1 - Replication completed with errors
#-------------------------------------------------------------------------------
run_local_replication_batch() {
  # Guard: Skip if local replication module didn't load
  if [[ "${LOCAL_REPLICATION_MODULE_AVAILABLE:-0}" -ne 1 ]]; then
    log_debug "vmbackup.sh" "run_local_replication_batch" "Local replication module not available, skipping"
    return 0
  fi
  
  local backup_path="${1:-$BACKUP_PATH}"
  
  log_info "vmbackup.sh" "run_local_replication_batch" "Starting batch local replication"
  if replicate_batch "$backup_path"; then
    if [[ "${REPLICATION_TOTAL_SUCCESS:-0}" -gt 0 ]]; then
      log_info "vmbackup.sh" "run_local_replication_batch" "Batch local replication completed successfully"
    else
      log_info "vmbackup.sh" "run_local_replication_batch" "Batch local replication: all destinations skipped (none replicated)"
    fi
    return 0
  else
    log_warn "vmbackup.sh" "run_local_replication_batch" "Batch local replication completed with errors (backup data preserved)"
    return 1
  fi
}

#################################################################################
# CLOUD REPLICATION MODULE
#
# Handles replication to cloud storage providers (SharePoint, Backblaze, etc.)
# Called AFTER local replication completes.
#################################################################################

# Global flag for cloud replication availability
declare -g CLOUD_REPLICATION_MODULE_AVAILABLE=0

#-------------------------------------------------------------------------------
# init_cloud_replication_module - Load and initialize cloud replication
#
# Sources replication_cloud_module.sh and initializes it.
# Sets CLOUD_REPLICATION_MODULE_AVAILABLE=1 if successful.
#
# Returns:
#   0 - Module loaded and initialized
#   1 - Module not available or disabled
#-------------------------------------------------------------------------------
init_cloud_replication_module() {
  local script_dir="${SCRIPT_DIR:-$(dirname "$(readlink -f "$0")")}"
  local cloud_module="$script_dir/modules/replication_cloud_module.sh"
  
  log_info "vmbackup.sh" "init_cloud_replication_module" "Checking for cloud replication module"
  
  if [[ -f "$cloud_module" ]]; then
    if source "$cloud_module" 2>/dev/null; then
      log_debug "vmbackup.sh" "init_cloud_replication_module" "Cloud replication module sourced successfully"
      # Initialize the module (loads config, validates destinations)
      if cloud_replication_init 2>/dev/null; then
        if [[ "${CLOUD_REPLICATION_ENABLED:-no}" == "yes" ]]; then
          log_info "vmbackup.sh" "init_cloud_replication_module" "Cloud replication module initialized and enabled"
          CLOUD_REPLICATION_MODULE_AVAILABLE=1
          return 0
        else
          log_info "vmbackup.sh" "init_cloud_replication_module" "Cloud replication disabled in config"
          CLOUD_REPLICATION_MODULE_AVAILABLE=0
          return 1
        fi
      else
        log_info "vmbackup.sh" "init_cloud_replication_module" "Cloud replication init failed (check config)"
        CLOUD_REPLICATION_MODULE_AVAILABLE=0
        return 1
      fi
    else
      log_warn "vmbackup.sh" "init_cloud_replication_module" "Failed to source cloud replication module (syntax error?)"
      CLOUD_REPLICATION_MODULE_AVAILABLE=0
      return 1
    fi
  else
    log_debug "vmbackup.sh" "init_cloud_replication_module" "Cloud replication module not found (disabled)"
    CLOUD_REPLICATION_MODULE_AVAILABLE=0
    return 1
  fi
}

#-------------------------------------------------------------------------------
# invoke_cloud_replication - Wrapper for cloud replication
#
# Called after local replication completes. Uploads backups to configured
# cloud destinations (SharePoint, Backblaze, etc.)
#
# Arguments:
#   $1 - backup_path: Root backup directory (default: $BACKUP_PATH)
#
# Returns:
#   0 - Cloud replication successful or module not available
#   1 - Cloud replication completed with errors
#-------------------------------------------------------------------------------
invoke_cloud_replication() {
  # Guard: Skip if cloud replication module didn't load
  if [[ "${CLOUD_REPLICATION_MODULE_AVAILABLE:-0}" -ne 1 ]]; then
    log_debug "vmbackup.sh" "invoke_cloud_replication" "Cloud replication module not available, skipping"
    return 0
  fi
  
  local backup_path="${1:-$BACKUP_PATH}"
  
  log_info "vmbackup.sh" "invoke_cloud_replication" "Starting cloud replication"
  
  # Call the cloud replication module's entry point
  # run_cloud_replication_batch is defined in cloud_replication_module.sh
  if run_cloud_replication_batch "$backup_path"; then
    log_info "vmbackup.sh" "invoke_cloud_replication" "Cloud replication completed successfully"
    return 0
  else
    log_warn "vmbackup.sh" "invoke_cloud_replication" "Cloud replication completed with errors"
    return 1
  fi
}

#-------------------------------------------------------------------------------
# _invalidate_replication_state_files - Remove stale state files at session start
#
# State files persist on disk between runs. If a module was disabled or didn't
# run this session, the reader functions would fall back to reading stale data
# from a prior run — producing incorrect summaries and emails.
#
# Called once at session startup, AFTER module init but BEFORE any backups.
# Each module re-creates its state file during its actual run.
#
# Applies uniformly to all replication types (local and cloud).
#-------------------------------------------------------------------------------
_invalidate_replication_state_files() {
  # FF-161: --dry-run is read-only. These files are consumed by email/status
  # reporting and are re-created only by a REAL replication run; under dry-run
  # replication is skipped, so deleting them would wipe the last real session's
  # reporting state. Preview and skip. (dry-run-must-be-readonly class.)
  if is_dry_run; then
    log_debug "vmbackup.sh" "_invalidate_replication_state_files" \
      "[DRY-RUN] Would invalidate stale replication state files - skipping (read-only)"
    return 0
  fi
  local state_dir="${STATE_DIR:-${BACKUP_PATH%/}/_state}"

  local -a state_files=(
    "${state_dir}/local_replication_state.txt"
    "${state_dir}/cloud_replication_state.txt"
  )

  for state_file in "${state_files[@]}"; do
    if [[ -f "$state_file" ]]; then
      rm -f "$state_file"
      log_debug "vmbackup.sh" "_invalidate_replication_state_files" \
        "Removed stale state file: $(basename "$state_file")"
    fi
  done
}

#################################################################################
# DEPENDENCY CHECK
#################################################################################

# Check if all required dependencies are installed
check_dependencies() {
  local missing_deps=()
  local missing_optional=()
  
  # Required binaries
  # INT-07 (2026-05-23): xmllint removed — no callers remained in the
  # codebase; was a phantom dependency. debian/control already clean.
  # DEP-01 (118-spaces): sha256sum is load-bearing — vm_fs_name() derives the
  # per-VM backup folder token from it. If absent, the encoder fails CLOSED
  # (refuses to mis-file), so gate it here to fail loudly up front instead.
  local required_tools=("virsh" "virtnbdbackup" "bash" "grep" "awk" "sed" "cut" "tr" "wc" "find" "date" "stat" "mkdir" "rm" "touch" "chmod" "tar" "sha256sum")
  
  log_info "vmbackup.sh" "check_dependencies" "Verifying required dependencies"
  
  for tool in "${required_tools[@]}"; do
    if ! command -v "$tool" &>/dev/null; then
      missing_deps+=("$tool")
      log_error "vmbackup.sh" "check_dependencies" "REQUIRED: $tool not found in PATH"
    fi
  done
  
  # Optional compression tools (for month-end archival)
  local optional_tools=("gzip" "bzip2" "xz")
  
  for tool in "${optional_tools[@]}"; do
    if ! command -v "$tool" &>/dev/null; then
      missing_optional+=("$tool")
      log_warn "vmbackup.sh" "check_dependencies" "OPTIONAL: $tool not found (archival compression will use fallback)"
    fi
  done

  # R5: jq is used only by the stale-bitmap self-healer
  # (remove_stale_qemu_bitmaps). Packaged installs already depend on jq via
  # debian/control; source/manual installs may lack it. Report it honestly as
  # optional with an accurate message (it is not a compression tool) so a
  # missing jq is visible rather than silently degrading the recovery path.
  if ! command -v jq &>/dev/null; then
    missing_optional+=("jq")
    log_warn "vmbackup.sh" "check_dependencies" "OPTIONAL: jq not found (stale-bitmap cleanup/self-heal will be skipped with a warning)"
  fi
  
  # Check libvirt daemon is running (virsh availability already checked in the loop above)
  if command -v virsh &>/dev/null && ! lv_daemon_alive; then
    missing_deps+=("libvirt-daemon")
    log_error "vmbackup.sh" "check_dependencies" "REQUIRED: libvirt daemon not running or not accessible"
  fi
  
  # Check virtnbdbackup specific requirements
  if command -v virtnbdbackup &>/dev/null; then
    # Verify virtnbdbackup is executable and has correct version
    local virtnbd_version=$(virtnbdbackup --version 2>/dev/null || echo "unknown")
    log_info "vmbackup.sh" "check_dependencies" "virtnbdbackup version: $virtnbd_version"
  fi
  
  # If any required dependencies are missing, report and exit
  if [[ ${#missing_deps[@]} -gt 0 ]]; then
    log_error "vmbackup.sh" "check_dependencies" "Missing required dependencies: ${missing_deps[*]}"
    log_error "vmbackup.sh" "check_dependencies" ""
    log_error "vmbackup.sh" "check_dependencies" "Install missing packages with:"
    log_error "vmbackup.sh" "check_dependencies" "  Ubuntu/Debian: sudo apt-get install virtnbdbackup libvirt-clients"
    log_error "vmbackup.sh" "check_dependencies" "  RHEL/CentOS: sudo yum install virtnbdbackup libvirt-client"
    log_error "vmbackup.sh" "check_dependencies" ""
    return 1
  fi
  
  # Report optional dependencies status
  if [[ ${#missing_optional[@]} -gt 0 ]]; then
    log_warn "vmbackup.sh" "check_dependencies" "Missing optional compression tools: ${missing_optional[*]}"
    log_warn "vmbackup.sh" "check_dependencies" "Script will continue but some compression formats unavailable"
  fi
  
  # Summary
  log_info "vmbackup.sh" "check_dependencies" "Dependency check PASSED - all required tools available"
  
  # Check AppArmor for virtnbdbackup socket access (Debian/Ubuntu with AppArmor)
  if command -v aa-status &>/dev/null && aa-status --enabled 2>/dev/null; then
    local scratch_dir="${VIRTNBD_SCRATCH_DIR:-/var/tmp}"
    local aa_local_file="/etc/apparmor.d/local/abstractions/libvirt-qemu"
    local aa_abstraction="/etc/apparmor.d/abstractions/libvirt-qemu"
    
    # FF-143: check the CONFIGURED scratch dir's rule, not the bare literal
    # 'virtnbdbackup'. With a custom VIRTNBD_SCRATCH_DIR the packaged /var/tmp
    # rule still contains 'virtnbdbackup', so the old grep passed while the
    # backup would fail at runtime for exactly the reason this check exists to
    # catch. Grep (fixed-string) for '<scratch_dir>/virtnbdbackup'.
    local scratch_rule="${scratch_dir}/virtnbdbackup"
    local needs_fix=false
    
    if [[ -f "$aa_abstraction" ]]; then
      # Check if virtnbdbackup socket access is allowed for THIS scratch dir
      if ! grep -qF "$scratch_rule" "$aa_abstraction" 2>/dev/null && \
         ! grep -qF "$scratch_rule" "$aa_local_file" 2>/dev/null; then
        needs_fix=true
      fi
    fi
    
    if [[ "$needs_fix" == true ]]; then
      log_error "vmbackup.sh" "check_dependencies" "AppArmor: libvirt-qemu profile does not allow virtnbdbackup sockets in ${scratch_dir}"
      log_error "vmbackup.sh" "check_dependencies" "AppArmor: Backups will fail until this is fixed. Run:"
      log_error "vmbackup.sh" "check_dependencies" "  echo '${scratch_dir}/virtnbdbackup.* rwk,' | sudo tee -a ${aa_local_file}"
      log_error "vmbackup.sh" "check_dependencies" "  sudo apparmor_parser -r /etc/apparmor.d/libvirt/libvirt-*"
      log_error "vmbackup.sh" "check_dependencies" "Or reinstall the vmbackup package: sudo dpkg -i vmbackup_*.deb"
    else
      log_info "vmbackup.sh" "check_dependencies" "AppArmor: virtnbdbackup socket access OK"
    fi
  fi
  
  return 0
}

#################################################################################
# UTILITY FUNCTIONS
#################################################################################

# get_current_month() — defined in vmbackup_integration.sh (policy-aware via get_period_id).
# Legacy monthly-only version removed (M5 — DATETIME_BUGS.md).

# Get VM status (running, shut off, paused, etc)
get_vm_status() {
  local vm_name=$1
  local status
  status=$(lv_domain_state "$vm_name")
  [[ -z "$status" ]] && status="unknown"
  log_debug "vmbackup.sh" "get_vm_status" "VM '$vm_name' state: '$status'"
  echo "$status"
}

# Check if QEMU guest agent is responsive
check_qemu_agent() {
  local vm_name=$1
  
  # Try to ping the agent
  virsh qemu-agent-command "$vm_name" '{"execute":"guest-ping"}' &>/dev/null  # [LIBVIRT-KEEPER: guest-agent I/O — orthogonal to domain metadata]
  local rc=$?
  log_debug "vmbackup.sh" "check_qemu_agent" "Agent ping for '$vm_name': $([ $rc -eq 0 ] && echo 'responsive' || echo "not responding (rc=$rc)")"
  return $rc
}

# Detect guest OS via QEMU agent. Outputs: "windows", "linux", or "unknown".
# Requires agent to be responsive (caller should check first).
detect_guest_os() {
  local vm_name=${1:?}
  local osinfo
  # [LIBVIRT-KEEPER: guest-agent I/O — orthogonal to domain metadata]
  osinfo=$(virsh qemu-agent-command --timeout 10 "$vm_name" \
    '{"execute":"guest-get-osinfo"}' 2>/dev/null) || {
    echo "unknown"
    return 1
  }
  if echo "$osinfo" | grep -qi '"id".*"mswindows"'; then
    echo "windows"
  else
    echo "linux"
  fi
  return 0
}

#################################################################################
# PER-VM BACKUP SUMMARY LOGGING
#################################################################################
# Logs a detailed summary at the end of each VM backup for easy identification
# Global arrays to track all VM results for final session summary
# Format: "vm_name|status|backup_type|duration|checkpoints|size|error|policy"
# Status: SUCCESS, FAILED, SKIPPED (offline), EXCLUDED (policy=never/pattern)
declare -ga VM_BACKUP_RESULTS=()

# Return codes for backup_vm():
#   2 = Excluded (policy=never or pattern exclusion - don't count as success)
readonly BACKUP_RC_EXCLUDED=2

# Log VM backup result summary
# Args: vm_name, start_time, start_epoch, status, backup_type, checkpoint_before,
#       checkpoint_after, error_msg, backup_size, policy, backup_dir,
#       event_type, event_detail, retry_attempt, archived_restore_points
_log_vm_backup_summary() {
  local vm_name=$1
  local start_time=$2
  local start_epoch=$3
  local status=$4           # SUCCESS, FAILED, SKIPPED, EXCLUDED
  local backup_type=$5      # full, auto, copy, excluded, n/a
  local checkpoint_before=$6
  local checkpoint_after=$7
  local error_msg=$8
  local backup_size=$9
  local policy=${10:-""}    # Rotation policy (daily/weekly/monthly/accumulate/never)
  local backup_dir=${11:-""}  # Backup directory path for this VM
  local event_type=${12:-""}
  local event_detail=${13:-""}
  local retry_attempt=${14:-0}
  local archived_restore_points=${15:-0}
  
  local end_time=$(date '+%Y-%m-%d %H:%M:%S')
  local end_epoch=$(date +%s)
  local duration_seconds=$((end_epoch - start_epoch))
  local duration_human=$(printf '%02d:%02d:%02d' $((duration_seconds/3600)) $((duration_seconds%3600/60)) $((duration_seconds%60)))
  
  # Restore points count (uses checkpoint_after which counts actual restorable data files)
  # Note: Copy-mode backups (.copy.data) count as 1 restore point each
  local restore_points="$checkpoint_after"
  
  # Format size
  local size_human="N/A"
  if [[ -n "$backup_size" && "$backup_size" != "0" ]]; then
    size_human=$(numfmt --to=iec-i --suffix=B "$backup_size" 2>/dev/null || echo "${backup_size}B")
  fi
  
  # Store result for session summary (now includes policy)
  VM_BACKUP_RESULTS+=("$vm_name|$status|$backup_type|$duration_human|$restore_points|$size_human|$error_msg|$policy")
  
  # Log to SQLite database
  if sqlite_is_available 2>/dev/null && ! is_dry_run; then
    local sqlite_status=$(echo "$status" | tr '[:upper:]' '[:lower:]')
    local backup_method="unknown"
    if [[ "$status" == "EXCLUDED" ]]; then
      backup_method="excluded"
    else
      [[ "${VM_STATE:-}" == "shut off" ]] && backup_method="offline"
      [[ -n "$QEMU_AGENT_AVAILABLE" && "$QEMU_AGENT_AVAILABLE" -eq 1 ]] && backup_method="agent"
      [[ -n "$VM_WAS_PAUSED" && "$VM_WAS_PAUSED" -eq 1 ]] && backup_method="paused"
    fi
    
    # Determine chain_archived flag (1 if we archived a chain during this backup)
    local chain_archived_flag=0
    if [[ "${_ARCHIVE_CHAIN_ARCHIVED:-false}" == "true" ]]; then
      chain_archived_flag=1
    fi
    
    # Enhance error info if policy change was detected
    local final_error_code="${LAST_ERROR_CODE:-}"
    local final_error_msg="$error_msg"
    if [[ "$_POLICY_CHANGE_DETECTED" == "true" ]]; then
      if [[ -n "$final_error_code" ]]; then
        final_error_code="POLICY_CHANGE|${final_error_code}"
      else
        final_error_code="POLICY_CHANGE"
      fi
      local policy_change_detail="policy_changed:${_POLICY_CHANGE_PREVIOUS:-unknown}->${_POLICY_CHANGE_CURRENT:-unknown}"
      if [[ -n "$final_error_msg" ]]; then
        final_error_msg="${policy_change_detail}|${final_error_msg}"
      else
        final_error_msg="$policy_change_detail"
      fi
    fi
    
    sqlite_log_vm_backup \
      "$vm_name" \
      "${VM_STATE:-unknown}" \
      "" \
      "$backup_type" \
      "$backup_method" \
      "$policy" \
      "$sqlite_status" \
      "${backup_size:-0}" \
      "$(get_chain_size "${backup_dir:-}" 2>/dev/null || echo 0)" \
      "$(get_total_dir_size "${backup_dir:-}" 2>/dev/null || echo 0)" \
      "${checkpoint_after:-0}" \
      "$duration_seconds" \
      "${backup_dir:-}" \
      "" \
      "$final_error_code" \
      "$final_error_msg" \
      "${QEMU_AGENT_AVAILABLE:-0}" \
      "${VM_WAS_PAUSED:-0}" \
      "$chain_archived_flag" \
      "${checkpoint_before:-0}" \
      "${retry_attempt:-0}" \
      "${archived_restore_points:-0}" \
      "$event_type" \
      "$event_detail"
    
    # Update chain_health for successful/failed backups (not excluded)
    # Skip if integration module already handled this via post_backup_hook
    # NOTE: This fallback path only fires when the integration module is NOT loaded.
    # For accumulate-policy VMs the basename of backup_dir is the VM name (flat path),
    # NOT the correct period_id ("accumulate"). If the integration module is ever made
    # optional, this derivation must be replaced with a get_period_id() call.
    if [[ "$status" != "EXCLUDED" ]] && [[ -n "${backup_dir:-}" ]] && ! declare -f post_backup_hook >/dev/null 2>&1; then
      local period_id
      period_id=$(basename "$backup_dir" 2>/dev/null || echo "unknown")
      local chain_status="active"
      local error_type="" error_msg_chain=""
      if [[ "$status" == "FAILED" ]]; then
        chain_status="broken"
        error_type="backup_failed"
        error_msg_chain="$final_error_msg"
      fi
      # Use actual restore point count (data files), not virsh checkpoint count
      local actual_restore_points
      actual_restore_points=$(get_restore_point_count "$backup_dir" 2>/dev/null || echo 0)
      sqlite_update_chain_health "$vm_name" "$period_id" "$backup_dir" "$chain_status" \
        "$actual_restore_points" "$error_type" "$error_msg_chain" 2>/dev/null || true
    fi
  fi
  
  # Log the summary (skip detailed box for excluded VMs - just log inline)
  if [[ "$status" == "EXCLUDED" ]]; then
    log_info "vmbackup.sh" "backup_vm" "VM $vm_name: EXCLUDED (policy=$policy)"
    return 0
  fi
  
  log_info "vmbackup.sh" "backup_vm" ""
  log_info "vmbackup.sh" "backup_vm" "╔══════════════════════════════════════════════════════════════════════════════╗"
  log_info "vmbackup.sh" "backup_vm" "║  BACKUP END: $vm_name"
  log_info "vmbackup.sh" "backup_vm" "╠══════════════════════════════════════════════════════════════════════════════╣"
  log_info "vmbackup.sh" "backup_vm" "║  Status:              $status"
  log_info "vmbackup.sh" "backup_vm" "║  Backup Type:         $backup_type"
  log_info "vmbackup.sh" "backup_vm" "║  Policy:              $policy"
  log_info "vmbackup.sh" "backup_vm" "║  Duration:            $duration_human"
  log_info "vmbackup.sh" "backup_vm" "║  Restore Points:      $restore_points"
  log_info "vmbackup.sh" "backup_vm" "║  Backup Size:         $size_human"
  if [[ -n "$error_msg" ]]; then
    log_info "vmbackup.sh" "backup_vm" "║  Error:               $error_msg"
  fi
  log_info "vmbackup.sh" "backup_vm" "║  Start:               $start_time"
  log_info "vmbackup.sh" "backup_vm" "║  End:                 $end_time"
  log_info "vmbackup.sh" "backup_vm" "╚══════════════════════════════════════════════════════════════════════════════╝"
  log_info "vmbackup.sh" "backup_vm" ""
}

# Log final session summary with all VMs
# Now properly categorizes: Backed Up, Excluded, Skipped (offline), Failed
_log_session_summary() {
  local backed_up_count=$1
  local excluded_count=$2
  local skipped_count=$3  
  local fail_count=$4
  local total_count=$((backed_up_count + excluded_count + skipped_count + fail_count))
  
  # Count by policy type
  local daily_count=0 weekly_count=0 monthly_count=0 accumulate_count=0
  for result in "${VM_BACKUP_RESULTS[@]}"; do
    IFS='|' read -r vm status btype duration ckpt size err policy <<< "$result"
    if [[ "$status" == "SUCCESS" ]]; then
      case "$policy" in
        daily)      ((daily_count++)) ;;
        weekly)     ((weekly_count++)) ;;
        monthly)    ((monthly_count++)) ;;
        accumulate) ((accumulate_count++)) ;;
      esac
    fi
  done
  
  log_info "vmbackup.sh" "main" ""
  log_info "vmbackup.sh" "main" "╔══════════════════════════════════════════════════════════════════════════════════════════════════════════╗"
  log_info "vmbackup.sh" "main" "║                                  VM BACKUP SESSION SUMMARY                                              ║"
  log_info "vmbackup.sh" "main" "╠══════════════════════════════════════════════════════════════════════════════════════════════════════════╣"
  log_info "vmbackup.sh" "main" "║  Total VMs: $total_count"
  log_info "vmbackup.sh" "main" "║"
  log_info "vmbackup.sh" "main" "║  ✓ Backed Up: $backed_up_count  (daily: $daily_count, weekly: $weekly_count, monthly: $monthly_count, accumulate: $accumulate_count)"
  log_info "vmbackup.sh" "main" "║  ○ Excluded:  $excluded_count  (policy=never or pattern match)"
  log_info "vmbackup.sh" "main" "║  ◇ Skipped:   $skipped_count  (offline/unchanged)"
  log_info "vmbackup.sh" "main" "║  ✗ Failed:    $fail_count"
  log_info "vmbackup.sh" "main" "╠══════════════════════════════════════════════════════════════════════════════════════════════════════════╣"
  log_info "vmbackup.sh" "main" "║  VM NAME               │ STATUS   │ TYPE  │ POLICY    │ DURATION │ CHKPTS │ SIZE        │ ERROR"
  log_info "vmbackup.sh" "main" "╠══════════════════════════════════════════════════════════════════════════════════════════════════════════╣"
  
  for result in "${VM_BACKUP_RESULTS[@]}"; do
    IFS='|' read -r vm status btype duration ckpt size err policy <<< "$result"
    # Format each column with padding for alignment
    local vm_padded=$(printf '%-20s' "$vm")
    local status_padded=$(printf '%-8s' "$status")
    local btype_padded=$(printf '%-5s' "$btype")
    local policy_padded=$(printf '%-9s' "$policy")
    local duration_padded=$(printf '%-8s' "$duration")
    local ckpt_padded=$(printf '%-6s' "$ckpt")
    local size_padded=$(printf '%-11s' "$size")
    log_info "vmbackup.sh" "main" "║  $vm_padded │ $status_padded │ $btype_padded │ $policy_padded │ $duration_padded │ $ckpt_padded │ $size_padded │ $err"
  done
  
  log_info "vmbackup.sh" "main" "╠══════════════════════════════════════════════════════════════════════════════════════════════════════════╣"
  
  # Local replication summary (require module available + function defined)
  if [[ "${LOCAL_REPLICATION_MODULE_AVAILABLE:-0}" -eq 1 ]] && declare -f get_replication_summary >/dev/null 2>&1; then
    local local_repl_summary
    local_repl_summary=$(get_replication_summary 2>/dev/null)
    if [[ -n "$local_repl_summary" ]]; then
      log_info "vmbackup.sh" "main" "║  LOCAL REPLICATION"
      while IFS= read -r line; do
        [[ -n "$line" ]] && log_info "vmbackup.sh" "main" "║    $line"
      done <<< "$local_repl_summary"
    fi
  else
    log_info "vmbackup.sh" "main" "║  LOCAL REPLICATION: Not configured"
  fi
  
  log_info "vmbackup.sh" "main" "║"
  
  # Cloud replication summary (require module available + function defined)
  if [[ "${CLOUD_REPLICATION_MODULE_AVAILABLE:-0}" -eq 1 ]] && declare -f get_cloud_replication_summary >/dev/null 2>&1; then
    local cloud_repl_summary
    cloud_repl_summary=$(get_cloud_replication_summary 2>/dev/null)
    if [[ -n "$cloud_repl_summary" ]]; then
      log_info "vmbackup.sh" "main" "║  CLOUD REPLICATION"
      while IFS= read -r line; do
        [[ -n "$line" ]] && log_info "vmbackup.sh" "main" "║    $line"
      done <<< "$cloud_repl_summary"
    fi
  else
    log_info "vmbackup.sh" "main" "║  CLOUD REPLICATION: Not configured"
  fi
  
  log_info "vmbackup.sh" "main" "╚══════════════════════════════════════════════════════════════════════════════════════════════════════════╝"
  log_info "vmbackup.sh" "main" ""
}

# Get file descriptor usage (pure bash, no external processes)
get_fd_count() {
  local fds=(/proc/$$/fd/*)
  echo ${#fds[@]}
}

# Get file descriptor limit
get_fd_limit() {
  ulimit -n
}

# Parse disk space (in MB)
get_available_space_mb() {
  local path="$1"
  # df default output is 1K-blocks; divide by 1024 to get MB
  df -k "$path" 2>/dev/null | awk 'NR==2 {printf "%d", $4/1024}'
}

# UNI-321: Source shared locking library
source_lib_or_die vm_lock.sh

#################################################################################
#################################################################################
# BACKUP PROGRESS MONITORING (ADAPTIVE FSFREEZE TIMEOUT)
#################################################################################
# PURPOSE: Detect and kill hung backups caused by FSFREEZE timeouts (GitHub #102)
# 
# BACKGROUND: On some guest OSes (CloudLinux, cPanel, NetBSD), the QEMU guest agent
# fails to respond to FSFREEZE requests (-F flag), causing virtnbdbackup to hang
# indefinitely waiting for filesystem quiescence. This replaces a naive 1-hour
# wall-clock timeout which was too aggressive for legitimate large backups.
#
# APPROACH: Adaptive stall detection instead of fixed timeout
# - Monitors actual backup file size progress (*.data.partial)
# - If file size doesn't increase for BACKUP_STALL_THRESHOLD seconds (180s), assume hung
# - Allows large backups to complete naturally while catching true hangs
#
# RETURN VALUES:
#   0 = Backup completed normally (with or without progress)
#   1 = Backup stalled and was killed (likely FSFREEZE hang)
#
# FREEZE-STALL SIGNALLING (R2):
#   - On a stall-kill this function also touches the sentinel file passed as $4
#     (when provided). perform_backup() reads that sentinel to tell a freeze
#     stall apart from an ordinary virtnbdbackup failure, because the failure
#     path's `wait $monitor_pid || true` swallows this function's return code
#     and the success path never waits on the monitor at all.
#   - perform_backup() then classifies the failure as FSFREEZE_STALL and, after
#     BACKUP_FSFREEZE_STALL_LIMIT consecutive freeze stalls, stops retrying and
#     fails closed. Disabling the freeze on retry is NOT an option here:
#     virtnbdbackup 2.x always freezes via the guest agent (-F only narrows the
#     mountpoints, it cannot turn freezing off), so a retry re-freezes the same
#     way. Containing the hang and reporting it honestly is the fix.

monitor_backup_progress() {
  local backup_pid=$1
  local backup_dir=$2
  local vm_name=$3
  local _stall_sentinel="${4:-}"   # R2: touched on a freeze-stall kill (optional)
  
  # PHASE 0: Wait for virtnbdbackup to create *.data.partial file
  # File creation can be delayed on slow systems (NBD setup, VM pause, checkpoint creation)
  # Retry for up to 10 minutes before giving up on monitoring
  local data_file=""
  local max_retries=120
  local retry_count=0
  
  while [[ -z "$data_file" && $retry_count -lt $max_retries ]]; do
    sleep 5
    # Sort for deterministic pick if more than one .partial exists
    # (109-bugs audit item 3).
    data_file=$(find "$backup_dir" -maxdepth 1 -name "*.data.partial" 2>/dev/null | sort | head -1)
    ((retry_count++))
    
    # Early exit if backup process dies before creating .partial file (very fast completion)
    if [[ -z "$data_file" ]] && ! kill -0 $backup_pid 2>/dev/null; then
      log_warn "vmbackup.sh" "monitor_backup_progress" "Backup process ended before .partial file was created for VM: $vm_name (fast completion)"
      return 0
    fi
  done
  
  # If .partial file never appears, we can't monitor - don't kill a working backup
  if [[ -z "$data_file" ]]; then
    log_warn "vmbackup.sh" "monitor_backup_progress" "No .partial file found for VM: $vm_name after 10 minutes - cannot monitor progress (will rely on normal exit detection)"
    return 0
  fi
  
  log_info "vmbackup.sh" "monitor_backup_progress" "Starting progress monitor for VM: $vm_name | File: $data_file"
  log_info "vmbackup.sh" "monitor_backup_progress" "Grace: ${BACKUP_STARTUP_GRACE}s | Stall threshold: ${BACKUP_STALL_THRESHOLD}s | Check interval: ${BACKUP_CHECK_INTERVAL}s"
  
  # PHASE 1: Startup grace period
  # Allow time for NBD setup, VM pause, checkpoint creation before monitoring for stalls
  sleep $BACKUP_STARTUP_GRACE
  
  # PHASE 2: Monitor for file size progress
  local last_size=0
  local stall_count=0
  
  while kill -0 $backup_pid 2>/dev/null; do
    # Wait for file to exist (should already exist, but be defensive)
    if [[ ! -f "$data_file" ]]; then
      sleep $BACKUP_CHECK_INTERVAL
      continue
    fi
    
    local current_size=$(stat -c %s "$data_file" 2>/dev/null || echo 0)
    local human_size=$(numfmt --to=iec-i --suffix=B $current_size 2>/dev/null || echo "$current_size bytes")
    
    if [[ $current_size -gt $last_size ]]; then
      # File size increased - backup is making progress
      stall_count=0
      last_size=$current_size
      log_info "vmbackup.sh" "monitor_backup_progress" "VM: $vm_name | Progress: $human_size"
    else
      # File size unchanged - potential stall
      stall_count=$((stall_count + 1))
      local stall_time=$((stall_count * BACKUP_CHECK_INTERVAL))
      log_warn "vmbackup.sh" "monitor_backup_progress" "VM: $vm_name | No progress: ${stall_time}s elapsed | Size: $human_size"
      
      # If stalled too long, assume FSFREEZE hang and kill backup
      if (( stall_time >= BACKUP_STALL_THRESHOLD )); then
        log_error "vmbackup.sh" "monitor_backup_progress" "FSFREEZE TIMEOUT DETECTED: VM $vm_name stalled for ${BACKUP_STALL_THRESHOLD}s - killing backup"
        log_error "vmbackup.sh" "monitor_backup_progress" "  (Guest agent likely failed to freeze filesystems; perform_backup will classify this as FSFREEZE_STALL and contain it)"
        # R2: signal the parent that THIS kill was a freeze stall. The failure
        # path's `wait $monitor_pid || true` discards our return code, so a
        # sentinel file is the reliable channel.
        [[ -n "$_stall_sentinel" ]] && : > "$_stall_sentinel" 2>/dev/null
        kill $backup_pid 2>/dev/null
        return 1  # Signal that this was a stall kill, not normal exit
      fi
    fi
    
    sleep $BACKUP_CHECK_INTERVAL
  done
  
  # Backup process exited normally (killed by backup process exit, not by us)
  log_info "vmbackup.sh" "monitor_backup_progress" "Backup monitor ending (process exited normally)"
  return 0
}

# Check if lock exists
has_lock() {
  local vm_name=${1:?Error: vm_name required}
  local lock_file; lock_file=$(vm_lock_file "$vm_name" 2>/dev/null) || lock_file=""  # LOCK-01: token-keyed
  
  # Check if lock file exists
  [[ -f "$lock_file" ]] || return 1
  
  # Read PID from lock file
  local lock_pid=$(cat "$lock_file" 2>/dev/null)
  [[ -z "$lock_pid" ]] && return 1
  
  # Verify a live, legitimate holder. The canonical allowlist
  # (vmbackup/vmrestore/virtnbdbackup) lives in lib/vm_lock.sh so this hot-path
  # site can't drift and reap a live vmrestore/virtnbdbackup holder (FF-6).
  if vm_lock_holder_live "$lock_pid"; then
    log_debug "vmbackup.sh" "has_lock" "Active lock for '$vm_name': PID $lock_pid is a live vmbackup/vmrestore/virtnbdbackup holder"
    return 0
  fi
  
  # Dead PID, or PID reused by an unrelated process - remove stale lock.
  # SP-DR: --dry-run is read-only end-to-end. Skip ONLY the file deletion; the
  # semantic answer is unchanged (return 1 = "no live lock"). A surviving stale
  # file is inert for the preview (backup_vm skips create_lock under dry-run) and
  # is reaped by create_lock on the next REAL run.
  if is_dry_run; then
    log_debug "vmbackup.sh" "has_lock" "[DRY-RUN] Would remove stale lock: $lock_file (PID $lock_pid not a live holder) - skipping (read-only)"
    return 1
  fi
  log_debug "vmbackup.sh" "has_lock" "Stale lock for '$vm_name': PID $lock_pid is not a live vmbackup/vmrestore/virtnbdbackup holder, removing"
  rm -f "$lock_file"
  return 1
}

#################################################################################
# EMERGENCY INTERRUPT RECOVERY FUNCTIONS
#################################################################################

# Clean up lock, active checkpoint, and partial backup files for interrupted VM
# Called when CTRL+Z or other interrupt is detected mid-backup
# Purpose: Recover from interrupted backup and allow retry in same session
emergency_cleanup_current_vm() {
  local vm_name=${1:?Error: vm_name required}

  if is_dry_run; then
    log_info "vmbackup.sh" "emergency_cleanup_current_vm" "[DRY-RUN] Would clean up lock/bitmaps/partial files for VM: $vm_name - skipping (read-only)"
    return 0
  fi
  
  log_warn "vmbackup.sh" "emergency_cleanup_current_vm" "Starting emergency cleanup for VM: $vm_name"
  
  # Remove lock file
  local lock_file; lock_file=$(vm_lock_file "$vm_name" 2>/dev/null) || lock_file=""  # LOCK-01: token-keyed
  if [[ -f "$lock_file" ]]; then
    log_debug "vmbackup.sh" "emergency_cleanup_current_vm" "Deleting lock file: $lock_file"
    rm -f "$lock_file"
    log_info "vmbackup.sh" "emergency_cleanup_current_vm" "Removed stale lock file: $lock_file"
  fi
  
  # CLEANUP-01 (118-spaces): BACKUP_BASE_DIR was never defined (only BACKUP_PATH
  # exists), so "$BACKUP_BASE_DIR/$vm_name" resolved to "/$vm_name" — the
  # filesystem root, not the VM's backup tree. Use BACKUP_PATH + the vm_fs_name
  # token (also correct for spaced names).
  local _ec_tok; _ec_tok=$(vm_fs_name "$vm_name" 2>/dev/null) || _ec_tok=""
  local backup_dir=""; [[ -n "$_ec_tok" ]] && backup_dir="${BACKUP_PATH}${_ec_tok}"

  # Remove orphaned bitmaps (Issue #223: metadata deleted but QEMU bitmap persists)
  # This is critical because orphaned bitmaps cause "bitmap not found in backing chain" errors
  [[ -n "$backup_dir" ]] && remove_orphaned_vm_bitmaps "$vm_name" "$backup_dir" || true
  if [[ -d "$backup_dir" ]]; then
    # Find incomplete backup directories (marked with .incomplete suffix)
    log_debug "vmbackup.sh" "emergency_cleanup_current_vm" "Searching for .incomplete directories in: $backup_dir"
    find "$backup_dir" -maxdepth 1 -type d -name "*.incomplete" -printf "Removing incomplete backup: %p\n" -exec rm -rf {} + 2>/dev/null
    
    # Find recent failed backup files (created <5 minutes ago with obvious incomplete markers)
    log_debug "vmbackup.sh" "emergency_cleanup_current_vm" "Deleting partial/tmp files (<5 min old) in: $backup_dir"
    find "$backup_dir" -maxdepth 2 -type f \( -name "*.partial" -o -name "*.tmp" \) -mmin -5 -exec rm -f {} + 2>/dev/null
  fi
  
  log_info "vmbackup.sh" "emergency_cleanup_current_vm" "Emergency cleanup completed for VM: $vm_name"
  return 0
}

# Detect if backup was interrupted recently (within current session)
# Returns 0 if interrupted backup detected, 1 otherwise
detect_interrupted_backup() {
  local vm_name=${1:?Error: vm_name required}
  
  # Check for fresh stale lock (<5 minutes old indicates recent interrupt)
  local lock_file; lock_file=$(vm_lock_file "$vm_name" 2>/dev/null) || lock_file=""  # LOCK-01: token-keyed
  if [[ -f "$lock_file" ]]; then
    local lock_age=$(($(date +%s) - $(stat -c %Y "$lock_file" 2>/dev/null)))
    if [[ $lock_age -lt 300 ]]; then  # < 5 minutes
      log_warn "vmbackup.sh" "detect_interrupted_backup" "Fresh stale lock detected for $vm_name (age: ${lock_age}s)"
      return 0  # Interrupted backup detected
    fi
  fi
  
  # Check for active checkpoint (indicates incomplete backup operation)
  local checkpoints
  checkpoints=$(lv_checkpoint_count_virtnbd "$vm_name")
  if [[ $checkpoints -gt 0 ]]; then
    log_warn "vmbackup.sh" "detect_interrupted_backup" "Active checkpoint found for $vm_name - possible interrupted backup"
    return 0  # Incomplete operation detected
  fi
  
  log_debug "vmbackup.sh" "detect_interrupted_backup" "No interruption evidence for '$vm_name' (lock_age=${lock_age:-n/a}s, checkpoints=${checkpoints:-0})"
  return 1  # No evidence of interruption
}

#################################################################################
# BITMAP CLEANUP FUNCTIONS
#################################################################################

# Remove all bitmaps from VM's disk images
# Purpose: Clean QEMU bitmaps that persist even when virsh metadata is deleted
# Fix for Issue #223: Replaces virsh checkpoint-delete --metadata with full cleanup
remove_orphaned_vm_bitmaps() {
  local vm_name=${1:?Error: vm_name required}
  local backup_dir=${2:?Error: backup_dir required}
  
  if ! command -v qemu-img &>/dev/null; then
    log_warn "vmbackup.sh" "remove_orphaned_vm_bitmaps" "qemu-img not available - cannot remove bitmaps"
    return 1
  fi
  
  # FF-145: qemu-img info --output=json emits pretty-printed, multi-line JSON,
  # which the old single-line grep '"bitmaps":\[.*\]' could NEVER match — this
  # function was a guaranteed silent no-op. Parse with jq. jq is a hard package
  # dependency (debian/control), so packaged installs never hit this arm; on a
  # source install warn + return 1 (matching the qemu-img arm above) rather
  # than silently degrade back to a no-op.
  if ! command -v jq &>/dev/null; then
    log_warn "vmbackup.sh" "remove_orphaned_vm_bitmaps" "jq not found - cannot enumerate qemu-img bitmaps; skipping orphaned-bitmap cleanup for VM $vm_name"
    return 1
  fi
  
  local removed_count=0
  local failed_count=0
  
  log_info "vmbackup.sh" "remove_orphaned_vm_bitmaps" "Starting bitmap removal for VM: $vm_name"
  
  # Get all disk paths for VM
  while IFS= read -r disk_path; do
    [[ -z "$disk_path" ]] && continue
    [[ ! -e "$disk_path" ]] && continue
    
    # Enumerate this disk's persistent bitmaps from qemu-img's JSON via jq
    # (FF-145: pretty-printed multi-line JSON; the old single-line grep matched
    # nothing). Recurse for any "bitmaps" array and emit each bitmap name.
    while read -r bitmap_name; do
      [[ -z "$bitmap_name" ]] && continue
      log_info "vmbackup.sh" "remove_orphaned_vm_bitmaps" "Removing bitmap '$bitmap_name' from disk: $disk_path"
      
      if qemu-img bitmap --remove "$disk_path" "$bitmap_name" 2>/dev/null; then
        log_info "vmbackup.sh" "remove_orphaned_vm_bitmaps" "✓ Successfully removed bitmap: $bitmap_name"
        removed_count=$((removed_count + 1))
      else
        log_error "vmbackup.sh" "remove_orphaned_vm_bitmaps" "✗ Failed to remove bitmap: $bitmap_name from $disk_path"
        failed_count=$((failed_count + 1))
      fi
    done < <(qemu-img info --output=json "$disk_path" 2>/dev/null | jq -r '.. | .bitmaps? // empty | .[]? | .name? // empty' 2>/dev/null)
  done < <(lv_list_disk_paths "$vm_name")
  
  if [[ $removed_count -gt 0 || $failed_count -gt 0 ]]; then
    log_info "vmbackup.sh" "remove_orphaned_vm_bitmaps" "Bitmap removal summary: Removed=$removed_count Failed=$failed_count"
    [[ $failed_count -eq 0 ]] && return 0 || return 1
  else
    log_debug "vmbackup.sh" "remove_orphaned_vm_bitmaps" "No bitmaps found to remove for VM: $vm_name"
    return 0
  fi
}

#################################################################################
# CONFIGURATION BACKUP FUNCTIONS
#################################################################################

# Backup VM XML configuration
# Strategy: Export current domain XML to backup directory
# Keep: First backup of each month + any subsequent backups if config changed
backup_vm_config() {
  local vm_name=$1
  local backup_dir=$2  # Directory for this VM's backups
  local current_month=$(get_current_month)
  local config_dir="$backup_dir/config"
  
  mkdir -p "$config_dir"
  
  # Generate config filename with timestamp
  local backup_date=$(date '+%Y%m%d_%H%M%S')
  local config_file="$config_dir/${vm_name}_config_${backup_date}.xml"
  
  # Export domain XML
  if ! lv_dump_xml_to_file "$vm_name" "$config_file"; then
    log_error "vmbackup.sh" "backup_vm_config" "Failed to export XML for VM: $vm_name"
    return 1
  fi
  
  # Check if this is the first of the month
  local first_of_month_file="$config_dir/${vm_name}_config_${current_month}_FIRST.xml"
  
  if [[ ! -f "$first_of_month_file" ]]; then
    # First backup of month - keep it and mark as first
    cp "$config_file" "$first_of_month_file"
    log_info "vmbackup.sh" "backup_vm_config" "VM config backup: $vm_name (FIRST of month)"
    if declare -f log_file_operation >/dev/null 2>&1; then
      log_file_operation "create" "$vm_name" "$config_file" "" \
        "config_xml" "VM config export" "backup_vm_config" "true"
      log_file_operation "copy" "$vm_name" "$config_file" "$first_of_month_file" \
        "config_xml" "First-of-month config" "backup_vm_config" "true"
    fi
    return 0
  fi
  
  # Compare with first-of-month backup
  if ! diff -q "$first_of_month_file" "$config_file" >/dev/null 2>&1; then
    # Config changed - keep this backup too
    log_info "vmbackup.sh" "backup_vm_config" "VM config backup: $vm_name (CHANGED, retained)"
    if declare -f log_file_operation >/dev/null 2>&1; then
      log_file_operation "create" "$vm_name" "$config_file" "" \
        "config_xml" "VM config changed - retained" "backup_vm_config" "true"
    fi
    return 0
  else
    # Config unchanged - delete the temporary backup
    if rm -f "$config_file"; then
      log_debug "vmbackup.sh" "backup_vm_config" "Deleted unchanged config backup: $(basename "$config_file")"
      if declare -f log_file_operation >/dev/null 2>&1; then
        log_file_operation "delete" "$vm_name" "$config_file" "" \
          "config_xml" "Config unchanged - not retained" "backup_vm_config" "true"
      fi
    fi
    log_info "vmbackup.sh" "backup_vm_config" "VM config backup: $vm_name (unchanged, not retained)"
    return 0
  fi
}
#################################################################################
# HEALTH CHECK FUNCTIONS
#################################################################################

# Check file descriptors
check_file_descriptors() {
  local current=$(get_fd_count)
  local limit=$(get_fd_limit)
  
  log_info "vmbackup.sh" "check_file_descriptors" "Current FDs: $current / $limit"
  
  # FF-146: 'ulimit -n' can be the literal 'unlimited' (systemd LimitNOFILE=
  # infinity). In the arithmetic below the unset word 'unlimited' evaluates to 0,
  # so the low-FD branch always fires and `ulimit -n $((unlimited+512))` = 512,
  # LOWERING the session limit. An unlimited budget can never be low — skip.
  if [[ "$limit" == "unlimited" ]]; then
    return 0
  fi
  
  if (( current > limit - 100 )); then
    log_warn "vmbackup.sh" "check_file_descriptors" "Low file descriptors: $current / $limit"
    
    # Try to increase
    ulimit -n $((limit + 512)) 2>/dev/null || true
    log_info "vmbackup.sh" "check_file_descriptors" "Attempted to increase FD limit"
  fi
}

# Check backup destination writable
check_backup_destination() {
  log_info "vmbackup.sh" "check_backup_destination" "Testing write access to $BACKUP_PATH"
  
  if [[ ! -d "$BACKUP_PATH" ]]; then
    log_error "vmbackup.sh" "check_backup_destination" "Backup path does not exist: $BACKUP_PATH"
    return 1
  fi
  
  # Dry-run must not mutate the store: skip the probe-file write + rm and the
  # recursive permission sweep. Still fail-closed — validate the destination
  # with read-only checks (existence checked above; writability via [[ -w ]]).
  if is_dry_run; then
    if [[ ! -w "$BACKUP_PATH" ]]; then
      log_error "vmbackup.sh" "check_backup_destination" "Cannot write to backup path: $BACKUP_PATH"
      return 1
    fi
    log_info "vmbackup.sh" "check_backup_destination" "[DRY-RUN] Would write probe file and recursively set backup permissions on $BACKUP_PATH (validated via read-only checks)"
    return 0
  fi
  
  # Test write access
  local test_file="$BACKUP_PATH/.vmbackup-test-$$"
  log_debug "vmbackup.sh" "check_backup_destination" "Creating test file to verify writability: $test_file"
  if ! touch "$test_file" 2>/dev/null; then
    log_error "vmbackup.sh" "check_backup_destination" "Cannot write to backup path: $BACKUP_PATH"
    return 1
  fi
  
  log_debug "vmbackup.sh" "check_backup_destination" "Deleting writability test file: $test_file"
  rm -f "$test_file"
  
  # Security: ensure BACKUP_PATH tree is owned by backup group
  set_backup_permissions "$BACKUP_PATH" --recursive
  
  log_info "vmbackup.sh" "check_backup_destination" "Write access verified"
  return 0
}

# Check scratch path consistency
check_scratch_path() {
  log_info "vmbackup.sh" "check_scratch_path" "Checking scratch directory: $VIRTNBD_SCRATCH_DIR"
  
  if [[ ! -d "$VIRTNBD_SCRATCH_DIR" ]]; then
    log_error "vmbackup.sh" "check_scratch_path" "Scratch directory does not exist: $VIRTNBD_SCRATCH_DIR"
    return 1
  fi
  
  if [[ ! -w "$VIRTNBD_SCRATCH_DIR" ]]; then
    log_error "vmbackup.sh" "check_scratch_path" "Scratch directory not writable: $VIRTNBD_SCRATCH_DIR"
    return 1
  fi
  
  log_info "vmbackup.sh" "check_scratch_path" "Scratch path verified"
  return 0
}

# Check disk space
# NOTE: GitHub issue virtnbdbackup#226 - Bitmap corruption occurs when backup destination fills up mid-backup
# See: https://github.com/abbbi/virtnbdbackup/discussions/226
# Thresholds are configurable per-instance via vmbackup.conf:
#   DISK_ABORT_PCT (default 20)  — abort if < this % free
#   DISK_WARN_PCT  (default 30)  — log warn if < this % free
#   DISK_ABORT_GB  (default 10)  — abort if < this GB free (0 = disabled)
#   DISK_WARN_GB   (default 50)  — log warn if < this GB free (0 = disabled)
# Fallback defaults match the historic hardcoded behaviour for backward compat.
check_disk_space() {
  local abort_pct="${DISK_ABORT_PCT:-20}"
  local warn_pct="${DISK_WARN_PCT:-30}"
  local abort_gb="${DISK_ABORT_GB:-10}"
  local warn_gb="${DISK_WARN_GB:-50}"
  local abort_mb=$(( abort_gb * 1024 ))
  local warn_mb=$(( warn_gb * 1024 ))

  local available_mb=$(get_available_space_mb "$BACKUP_PATH")
  local available_gb=$(( available_mb / 1024 ))
  # df -k to ensure consistent 1K-block output, then convert to MB
  local total_kb=$(df -k "$BACKUP_PATH" 2>/dev/null | tail -1 | awk '{print $2}')
  local total_mb=$(( ${total_kb:-0} / 1024 ))
  local total_gb=$(( total_mb / 1024 ))

  # Guard against division by zero (df failure, empty/unmounted path)
  if (( total_mb == 0 )); then
    log_error "vmbackup.sh" "check_disk_space" "Cannot determine disk space for $BACKUP_PATH (total_mb=0)"
    return 1
  fi

  local percentage_free=$(( available_mb * 100 / total_mb ))

  log_info "vmbackup.sh" "check_disk_space" \
    "Available space: ${available_gb}GB / ${total_gb}GB (${percentage_free}% free) — thresholds: abort<${abort_pct}%/${abort_gb}GB, warn<${warn_pct}%/${warn_gb}GB"

  # CRITICAL: percentage abort
  if (( percentage_free < abort_pct )); then
    log_error "vmbackup.sh" "check_disk_space" "CRITICAL: Destination only has ${percentage_free}% free space (${available_gb}GB), threshold: ${abort_pct}%"
    log_error "vmbackup.sh" "check_disk_space" "Risk: Backup may fail mid-operation causing bitmap corruption (GitHub issue #226)"
    log_error "vmbackup.sh" "check_disk_space" "Action: Free space, lower DISK_ABORT_PCT in vmbackup.conf, or this backup will be skipped to prevent corruption"
    return 1
  fi

  # Warn: below warn percentage OR below absolute warn GB
  if { (( warn_mb > 0 )) && (( available_mb < warn_mb )); } || (( percentage_free < warn_pct )); then
    log_warn "vmbackup.sh" "check_disk_space" "Low disk space: ${available_gb}GB / ${total_gb}GB (${percentage_free}% free, threshold: ${warn_gb}GB or ${warn_pct}%)"
  fi

  # Absolute abort (0 disables)
  if (( abort_mb > 0 )) && (( available_mb < abort_mb )); then
    log_error "vmbackup.sh" "check_disk_space" "Critical: Insufficient absolute space: ${available_gb}GB free (minimum: ${abort_gb}GB)"
    return 1
  fi

  return 0
}

# Check libvirt version compatibility
check_libvirt_version() {
  log_info "vmbackup.sh" "check_libvirt_version" "Checking libvirt version >= 7.2 (required for backup API)"
  
  local version
  version=$(lv_libvirt_version)
  
  if [[ -z "$version" ]]; then
    log_warn "vmbackup.sh" "check_libvirt_version" "Could not determine libvirt version"
    return 0
  fi
  
  local version_major_minor=$(echo "$version" | cut -d. -f1-2)
  local vmaj vmin
  IFS='.' read -r vmaj vmin <<< "$version_major_minor"
  
  # Minimum version 7.2 is required for backup API
  if [[ $vmaj -lt 7 || ($vmaj -eq 7 && $vmin -lt 2) ]]; then
    log_error "vmbackup.sh" "check_libvirt_version" "libvirt version $version detected - FAILED (requires >= 7.2)"
    return 1
  else
    log_info "vmbackup.sh" "check_libvirt_version" "libvirt version $version detected - OK"
  fi
}

#################################################################################
# STALE STATE RECOVERY FUNCTIONS
#################################################################################

# Unified cleanup function for system checkpoints and locks
# DESIGN: Consolidates logic from 7 separate cleanup functions into one coherent function
# Purpose: Remove orphaned checkpoints, stale locks, and corrupted checkpoint metadata
# Parameters: $1 = cleanup mode ("orphaned", "stale_locks", or "all")
cleanup_system_checkpoints_and_locks() {
  local mode="${1:-all}"
  
  # NOTE: Checkpoint cleanup REMOVED from session-level (2026-01-21)
  # REASON: The previous logic deleted valid checkpoints because it checked for
  #         $BACKUP_PATH/$MONTH/$VM/ directories that don't exist yet at session start.
  #         This caused all backups to become FULL instead of incremental.
  # FIX: Checkpoint validation and remediation is now handled ONLY at per-VM level
  #      by report_checkpoint_health() in backup_vm(), which runs AFTER the backup
  #      directory exists and can properly assess checkpoint chain integrity.
  # See: backup_vm() → report_checkpoint_health() for per-VM checkpoint handling
  
  if [[ "$mode" == "orphaned" ]] || [[ "$mode" == "all" ]]; then
    # Checkpoint cleanup is now a no-op at session level
    # Per-VM checkpoint handling occurs in backup_vm() via report_checkpoint_health()
    log_debug "vmbackup.sh" "cleanup_system_checkpoints_and_locks" \
      "Checkpoint cleanup skipped at session level (handled per-VM in backup_vm)"
  fi
  
  if [[ "$mode" == "stale_locks" ]] || [[ "$mode" == "all" ]]; then
    log_info "vmbackup.sh" "cleanup_system_checkpoints_and_locks" "Scanning for stale lock files (>12 hours old)"
    
    if [[ ! -d "$LOCK_DIR" ]]; then
      return 0
    fi
    
    local stale_count=0
    local lock_files=()
    
    # Collect all stale lock files first (avoid subshell issues)
    while IFS= read -r lock_file; do
      lock_files+=("$lock_file")
    done < <(find "$LOCK_DIR" -name "vmbackup-*.lock" -type f -mtime +0.5 2>/dev/null)
    
    # Process each lock file
    for lock_file in "${lock_files[@]}"; do
      [[ -z "$lock_file" ]] && continue
      
      # Extract VM name from lock file: vmbackup-{vm_name}.lock
      local vm_name=$(basename "$lock_file" .lock | sed 's/vmbackup-//')
      local locked_pid=$(cat "$lock_file" 2>/dev/null)
      
      log_warn "vmbackup.sh" "cleanup_system_checkpoints_and_locks" "Found stale lock file: $lock_file (PID: $locked_pid, VM: $vm_name)"
      
      # Check the lock file's PID is a live, legitimate holder. The allowlist
      # (vmbackup/vmrestore/virtnbdbackup) lives in lib/vm_lock.sh so this reaper
      # can't drift and reap a live restore's lock (FF-24).
      if vm_lock_holder_live "$locked_pid"; then
        log_warn "vmbackup.sh" "cleanup_system_checkpoints_and_locks" "Lock file is old BUT a backup/restore process IS running (PID: $locked_pid) - keeping lock"
        continue
      fi
      
      # No running backup process found - safe to delete the stale lock
      log_warn "vmbackup.sh" "cleanup_system_checkpoints_and_locks" "Stale lock detected for VM: $vm_name (no active backup process) - removing and will retry"
      # SP-DR: --dry-run is read-only. Show what WOULD be reaped but delete
      # nothing and do NOT count it, so "Files removed: N" stays truthful.
      if is_dry_run; then
        log_info "vmbackup.sh" "cleanup_system_checkpoints_and_locks" "[DRY-RUN] Would delete stale lock: $lock_file - skipping (read-only)"
        continue
      fi
      rm -f "$lock_file"
      log_info "vmbackup.sh" "cleanup_system_checkpoints_and_locks" "Deleted stale lock: $lock_file"
      ((stale_count++))
    done
    
    log_info "vmbackup.sh" "cleanup_system_checkpoints_and_locks" "Stale lock cleanup complete. Files removed: $stale_count"
  fi
  
  return 0
}

#################################################################################
# CHECKPOINT MANAGEMENT FUNCTIONS
#################################################################################

# Get QEMU checkpoint chain depth for a VM (virsh metadata)
# NOTE: This counts virsh checkpoint metadata, NOT actual backup data files.
# Use get_restore_point_count() for actual restorable backup count.
# Parameters: $1 = vm_name
get_checkpoint_depth() {
  local vm_name=${1:?Error: vm_name required}
  lv_checkpoint_count_virtnbd "$vm_name"
}

# Get actual restore point count from data files on disk (current chain only)
# This counts logical restore points (backup operations), not individual disk files.
# A multi-disk VM backed up once = 1 restore point (all disks together).
# - Full/copy backup present = 1 restore point (regardless of disk count)
# - Each distinct incremental checkpoint level = 1 additional restore point
# Parameters: $1 = backup_dir (VM's backup directory)
# Returns: Number of restorable backup points in the current chain
get_restore_point_count() {
  local backup_dir="${1:?Error: backup_dir required}"
  
  if [[ ! -d "$backup_dir" ]]; then
    echo "0"
    return
  fi
  
  # Count logical restore points in the root directory (current chain)
  # Excludes .archives/ which contains archived chains
  local count=0
  
  # A full/copy backup = 1 restore point (regardless of how many disks)
  if find "$backup_dir" -maxdepth 1 -type f \( -name "*.full.data" -o -name "*.copy.data" \) -print -quit 2>/dev/null | grep -q .; then
    count=1
  fi
  
  # Count distinct incremental checkpoint levels
  # Files: *.inc.virtnbdbackup.N.data — extract unique N values
  local inc_levels
  inc_levels=$(find "$backup_dir" -maxdepth 1 -type f -name "*.inc.virtnbdbackup.*.data" 2>/dev/null \
    | sed -n 's/.*\.inc\.virtnbdbackup\.\([0-9]*\)\.data$/\1/p' \
    | sort -un | wc -l)
  count=$((count + inc_levels))
  
  log_debug "vmbackup.sh" "get_restore_point_count" "Dir='$backup_dir' has_full=$((count > 0 && inc_levels == 0 ? 1 : (count > inc_levels ? 1 : 0))) inc_levels=$inc_levels total=$count"
  echo "$count"
}

# === DUP-10 ===
# Derive active chain id from disk: chain-YYYY-MM-DD based on the earliest
# full/copy/inc backup file at the period root. Returns empty string if no
# active chain exists. Post-DUP-10 replacement for
# chain_manifest_module.sh::get_active_chain. The shape matches
# archive_existing_checkpoint_chain() so chain identity is stable across
# the active → archived transition.
#
# Parameters: $1 = vm_name
# Stdout:     chain id string ("chain-YYYY-MM-DD") or empty
# Exit code:  always 0 (empty stdout = no active chain, callers handle it)
get_active_chain_id_from_disk() {
  local vm_name="${1:?Error: vm_name required}"
  local backup_dir
  backup_dir=$(get_vm_backup_dir "$vm_name") || return 0
  [[ -d "$backup_dir" ]] || return 0

  # Earliest full/copy/inc file mtime at the period root = chain creation moment.
  local earliest_mtime
  earliest_mtime=$(find "$backup_dir" -maxdepth 1 -type f \
    \( -name "*.full.data" -o -name "*.copy.data" -o -name "*.inc.virtnbdbackup.*.data" \) \
    -printf '%T@\n' 2>/dev/null | sort -n | head -1)

  [[ -z "$earliest_mtime" ]] && return 0

  # %T@ is float seconds; strip fractional part before date -d @<int>.
  local epoch="${earliest_mtime%.*}"
  local datestr
  if ! datestr=$(date -d "@${epoch}" +%Y-%m-%d 2>/dev/null) || [[ -z "$datestr" ]]; then
    log_warn "vmbackup.sh" "get_active_chain_id_from_disk" \
      "date -d failed for vm='$vm_name' epoch='$epoch' — falling back to today"
    datestr=$(date +%Y-%m-%d)
  fi
  echo "chain-${datestr}"
}
# === /DUP-10 ===

#################################################################################
# BACKUP METRIC HELPER FUNCTIONS
# These functions calculate metrics for backup session logging
#################################################################################

# Calculate size of THIS backup only (not total directory)
# For incremental: size of newest .data files created in this run
# For full: size of .full.data files
# Parameters: $1 = backup_dir, $2 = backup_start_epoch (timestamp when backup started)
get_this_backup_size() {
  local backup_dir="$1"
  local backup_start_epoch="$2"
  local total_size=0
  
  # Find all .data files modified after backup started
  # This captures: *.full.data, *.copy.data, *.inc.virtnbdbackup.*.data
  while IFS= read -r -d '' file; do
    local file_mtime=$(stat -c %Y "$file" 2>/dev/null || echo "0")
    if [[ $file_mtime -ge $backup_start_epoch ]]; then
      local file_size=$(stat -c %s "$file" 2>/dev/null || echo "0")
      total_size=$((total_size + file_size))
    fi
  done < <(find "$backup_dir" -maxdepth 1 -type f -name "*.data" -print0 2>/dev/null)
  
  echo "$total_size"
}

# Calculate size of active backup chain (full + all incrementals, excluding archives)
# Parameters: $1 = backup_dir
get_chain_size() {
  local backup_dir="$1"
  
  # Calculate size excluding .archives directory
  # du -sb with --exclude doesn't work reliably, so we subtract archives
  local total_size=$(du -sb "$backup_dir" 2>/dev/null | awk '{print $1}' || echo "0")
  local archives_size=0
  
  if [[ -d "$backup_dir/.archives" ]]; then
    archives_size=$(du -sb "$backup_dir/.archives" 2>/dev/null | awk '{print $1}' || echo "0")
  fi
  
  echo $((total_size - archives_size))
}

# Calculate total directory size (including all archives)
# Parameters: $1 = backup_dir
get_total_dir_size() {
  local backup_dir="$1"
  du -sb "$backup_dir" 2>/dev/null | awk '{print $1}' || echo "0"
}

# Build dynamic event_detail message with context
# Parameters: $1 = backup_status, $2 = backup_type, $3 = this_backup_bytes, 
#            $4 = restore_points_after, $5 = backup_method, $6 = chain_archived,
#            $7 = error_msg (optional)
build_event_detail() {
  local backup_status="$1"
  local backup_type="$2"
  local this_backup_bytes="$3"
  local restore_points_after="$4"
  local backup_method="$5"
  local chain_archived="$6"
  local error_msg="${7:-}"
  
  local size_human=$(numfmt --to=iec-i --suffix=B "$this_backup_bytes" 2>/dev/null || echo "${this_backup_bytes}B")
  
  case "$backup_status" in
    "success")
      local detail="${backup_type} +${size_human} to checkpoint ${restore_points_after}"
      if [[ "$backup_method" == "agent" ]]; then
        detail+=", agent-assisted"
      elif [[ "$backup_method" == "paused" ]]; then
        detail+=", VM paused (no agent)"
      elif [[ "$backup_method" == "offline" ]]; then
        detail+=", offline copy"
      fi
      if [[ "$chain_archived" == "true" ]]; then
        detail+=", archived previous chain"
      fi
      echo "$detail"
      ;;
    "skipped")
      echo "skipped: disks unchanged since last backup"
      ;;
    "error"|"failed")
      echo "failed: ${error_msg:-unknown error}"
      ;;
    *)
      echo "status: $backup_status"
      ;;
  esac
}

#################################################################################
# VM STATE MANAGEMENT FUNCTIONS
#################################################################################

# Pause VM for backup consistency
pause_vm() {
  local vm_name=$1
  
  log_info "vmbackup.sh" "pause_vm" "Pausing VM: $vm_name"
  
  if ! lv_suspend "$vm_name"; then
    log_error "vmbackup.sh" "pause_vm" "Failed to pause VM: $vm_name"
    return 1
  fi
  
  sleep 2  # Wait for pause to complete
  log_info "vmbackup.sh" "pause_vm" "VM paused: $vm_name"
  return 0
}

# Resume VM after backup
resume_vm() {
  local vm_name=$1
  
  log_info "vmbackup.sh" "resume_vm" "Resuming VM: $vm_name"
  
  if ! lv_resume "$vm_name"; then
    log_error "vmbackup.sh" "resume_vm" "Failed to resume VM: $vm_name"
    return 1
  fi
  
  sleep 1
  log_info "vmbackup.sh" "resume_vm" "VM resumed: $vm_name"
  return 0
}

#################################################################################
# BACKUP FUNCTIONS
#################################################################################

# ═══════════════════════════════════════════════════════════════════════════════
# UNIFIED BACKUP STATE VALIDATION
# ═══════════════════════════════════════════════════════════════════════════════
# Single source of truth for checkpoint/backup state validation.
# Consolidates: report_checkpoint_health, validate_checkpoint_health, validate_backup_preconditions
# Benefits:
#   - ONE virsh checkpoint-list call per VM (was 3)
#   - Cached results for downstream functions
#   - Detailed validation output for troubleshooting
#   - Clear state classification for backup decisions
#
# Global cache variables (set by validate_backup_state):
#   CACHED_VM_NAME          - VM name this cache is for
#   CACHED_CHECKPOINT_COUNT - Number of QEMU checkpoints
#   CACHED_CHECKPOINT_FIRST - First checkpoint name (virtnbdbackup.0)
#   CACHED_CHECKPOINT_LAST  - Last checkpoint name (virtnbdbackup.N)
#   CACHED_CHECKPOINT_CHAIN - Full chain string (0-1-2-3...)
#   CACHED_CHAIN_HEALTHY    - "true" if chain is continuous, "false" if gaps
#   CACHED_VM_STATE         - VM state (running, shut off, paused)
#   CACHED_DIR_STATE        - Directory state (clean, stale_metadata, broken_chain, etc)
#   CACHED_HAS_BACKUP_DATA  - "true" if .data files exist
#   CACHED_VALIDATION_STATE - Overall state for prepare_backup_directory
# ═══════════════════════════════════════════════════════════════════════════════

# Reset the validation cache (call before validating a new VM)
reset_validation_cache() {
  log_debug "vmbackup.sh" "reset_validation_cache" "Clearing validation cache"
  CACHED_VM_NAME=""
  CACHED_CHECKPOINT_COUNT=0
  CACHED_CHECKPOINT_FIRST=""
  CACHED_CHECKPOINT_LAST=""
  CACHED_CHECKPOINT_CHAIN=""
  CACHED_CHAIN_HEALTHY="false"
  CACHED_VM_STATE=""
  CACHED_DIR_STATE="unknown"
  CACHED_HAS_BACKUP_DATA="false"
  CACHED_VALIDATION_STATE="unknown"
}

# Unified backup state validation - single virsh call, comprehensive output
# Usage: validate_backup_state <vm_name> <backup_dir>
# Sets: All CACHED_* global variables
# Returns: 0 if validation passed (backup can proceed), 1 if critical failure
# Outputs: Detailed validation box to log
validate_backup_state() {
  local vm_name="${1:?Error: vm_name required}"
  local backup_dir="${2:?Error: backup_dir required}"
  
  log_info "vmbackup.sh" "validate_backup_state" "Starting unified validation for VM: $vm_name"
  log_debug "vmbackup.sh" "validate_backup_state" "Backup directory: $backup_dir"
  
  # Reset cache for this VM
  reset_validation_cache
  CACHED_VM_NAME="$vm_name"
  
  # ─────────────────────────────────────────────────────────────────────────────
  # PHASE 1: VM State Check
  # ─────────────────────────────────────────────────────────────────────────────
  log_debug "vmbackup.sh" "validate_backup_state" "[Phase 1/5] Querying VM state via virsh domstate"
  CACHED_VM_STATE=$(lv_domain_state "$vm_name")
  if [[ -z "$CACHED_VM_STATE" ]]; then
    log_error "vmbackup.sh" "validate_backup_state" "[Phase 1/5] VM $vm_name not found or not accessible"
    return 1
  fi
  log_debug "vmbackup.sh" "validate_backup_state" "[Phase 1/5] VM state: $CACHED_VM_STATE"
  
  # ─────────────────────────────────────────────────────────────────────────────
  # PHASE 2: QEMU Checkpoint Query (SINGLE virsh call for entire backup)
  # ─────────────────────────────────────────────────────────────────────────────
  log_debug "vmbackup.sh" "validate_backup_state" "[Phase 2/5] Querying QEMU checkpoints via virsh checkpoint-list"
  local checkpoints_raw
  checkpoints_raw=$(lv_checkpoint_list_virtnbd "$vm_name" | sort -V)
  
  local -a checkpoints=()
  if [[ -n "$checkpoints_raw" ]]; then
    mapfile -t checkpoints <<< "$checkpoints_raw"
  fi
  
  CACHED_CHECKPOINT_COUNT=${#checkpoints[@]}
  log_debug "vmbackup.sh" "validate_backup_state" "[Phase 2/5] Found $CACHED_CHECKPOINT_COUNT QEMU checkpoint(s)"
  
  if [[ $CACHED_CHECKPOINT_COUNT -gt 0 ]]; then
    CACHED_CHECKPOINT_FIRST="${checkpoints[0]}"
    CACHED_CHECKPOINT_LAST="${checkpoints[-1]}"
    log_debug "vmbackup.sh" "validate_backup_state" "[Phase 2/5] Checkpoint range: $CACHED_CHECKPOINT_FIRST → $CACHED_CHECKPOINT_LAST"
    
    # Build chain string and check continuity
    local expected_idx=0
    CACHED_CHAIN_HEALTHY="true"
    local chain_parts=()
    local gap_detected=""
    
    for cp in "${checkpoints[@]}"; do
      local idx=$(echo "$cp" | sed 's/virtnbdbackup\.//')
      chain_parts+=("$idx")
      if [[ "$idx" -ne "$expected_idx" ]]; then
        CACHED_CHAIN_HEALTHY="false"
        gap_detected="expected virtnbdbackup.$expected_idx, found $cp"
        log_warn "vmbackup.sh" "validate_backup_state" "[Phase 2/5] Chain gap detected: $gap_detected"
      fi
      ((expected_idx++))
    done
    
    CACHED_CHECKPOINT_CHAIN=$(IFS='-'; echo "${chain_parts[*]}")
    log_debug "vmbackup.sh" "validate_backup_state" "[Phase 2/5] Chain sequence: $CACHED_CHECKPOINT_CHAIN"
    log_debug "vmbackup.sh" "validate_backup_state" "[Phase 2/5] Chain healthy: $CACHED_CHAIN_HEALTHY"
  else
    log_debug "vmbackup.sh" "validate_backup_state" "[Phase 2/5] No QEMU checkpoints found (first backup or reset)"
  fi
  
  # ─────────────────────────────────────────────────────────────────────────────
  # PHASE 3: Directory State Analysis
  # ─────────────────────────────────────────────────────────────────────────────
  log_debug "vmbackup.sh" "validate_backup_state" "[Phase 3/5] Analyzing backup directory state"
  local dir_has_checkpoint_metadata=false
  local has_checkpoint_markers=false
  local has_backup_data=false
  local has_incomplete_data=false
  local cpt_file_valid=false
  
  # Check for checkpoints/ directory with content
  if [[ -d "$backup_dir/checkpoints" ]] && [[ -n "$(find "$backup_dir/checkpoints" -type f 2>/dev/null | head -1)" ]]; then
    dir_has_checkpoint_metadata=true
    local checkpoint_file_count=$(find "$backup_dir/checkpoints" -type f 2>/dev/null | wc -l)
    log_debug "vmbackup.sh" "validate_backup_state" "[Phase 3/5] checkpoints/ directory: $checkpoint_file_count files"
  else
    log_debug "vmbackup.sh" "validate_backup_state" "[Phase 3/5] checkpoints/ directory: empty or missing"
  fi
  
  # Check for .cpt marker files
  local cpt_files=$(find "$backup_dir" -maxdepth 1 -name "*.cpt" 2>/dev/null)
  if [[ -n "$cpt_files" ]]; then
    has_checkpoint_markers=true
    log_debug "vmbackup.sh" "validate_backup_state" "[Phase 3/5] .cpt marker files found"
    # Validate .cpt file content
    while IFS= read -r cpt_file; do
      if [[ -f "$cpt_file" ]] && [[ -s "$cpt_file" ]]; then
        if grep -q "virtnbdbackup\|checkpoint" "$cpt_file" 2>/dev/null; then
          cpt_file_valid=true
          log_debug "vmbackup.sh" "validate_backup_state" "[Phase 3/5] .cpt file valid: $(basename "$cpt_file")"
          break
        else
          log_debug "vmbackup.sh" "validate_backup_state" "[Phase 3/5] .cpt file invalid content: $(basename "$cpt_file")"
        fi
      else
        log_debug "vmbackup.sh" "validate_backup_state" "[Phase 3/5] .cpt file empty: $(basename "$cpt_file")"
      fi
    done <<< "$cpt_files"
  else
    log_debug "vmbackup.sh" "validate_backup_state" "[Phase 3/5] No .cpt marker files found"
  fi
  
  # Check for other checkpoint markers
  if ! $has_checkpoint_markers; then
    if find "$backup_dir" -maxdepth 1 \( -name "*virtnbdbackup*.qcow.json" -o -name "vmconfig.virtnbdbackup*.xml" \) -print -quit 2>/dev/null | grep -q .; then
      has_checkpoint_markers=true
      log_debug "vmbackup.sh" "validate_backup_state" "[Phase 3/5] Other checkpoint markers found (qcow.json/xml)"
    fi
  fi
  
  # Check for backup data files
  local full_data_files=$(find "$backup_dir" -maxdepth 1 \( -name "*.full.data" -o -name "vd[a-z].full.data" -o -name "sd[a-z].full.data" \) 2>/dev/null | wc -l)
  local inc_data_files=$(find "$backup_dir" -maxdepth 1 -name "*.inc.virtnbdbackup.*.data" 2>/dev/null | wc -l)
  local copy_data_files=$(find "$backup_dir" -maxdepth 1 -name "*.copy.data" 2>/dev/null | wc -l)
  
  if [[ $full_data_files -gt 0 ]] || [[ $inc_data_files -gt 0 ]] || [[ $copy_data_files -gt 0 ]]; then
    has_backup_data=true
  fi
  log_debug "vmbackup.sh" "validate_backup_state" "[Phase 3/5] Backup data files: $full_data_files full, $inc_data_files incremental, $copy_data_files copy"
  CACHED_HAS_BACKUP_DATA="$has_backup_data"
  
  # Track copy backup data separately (copy backups are COMPLETE, not incomplete)
  local has_copy_backup=false
  if [[ $copy_data_files -gt 0 ]]; then
    has_copy_backup=true
    log_debug "vmbackup.sh" "validate_backup_state" "[Phase 3/5] Copy backup data found: $copy_data_files files (made while VM was offline)"
  fi
  
  # Check for incomplete/partial files (NOT including copy backups)
  local partial_files=$(find "$backup_dir" -maxdepth 1 -name "*.partial" 2>/dev/null | wc -l)
  if [[ $partial_files -gt 0 ]]; then
    has_incomplete_data=true
    log_debug "vmbackup.sh" "validate_backup_state" "[Phase 3/5] Incomplete data detected: $partial_files partial files"
  fi
  
  # Check for orphaned copy metadata
  if ! $has_incomplete_data; then
    if find "$backup_dir" -maxdepth 1 -name "*.copy.qcow.json" -print -quit 2>/dev/null | grep -q . && \
       ! find "$backup_dir" -maxdepth 1 -name "*.copy.data" -print -quit 2>/dev/null | grep -q .; then
      has_incomplete_data=true
      log_debug "vmbackup.sh" "validate_backup_state" "[Phase 3/5] Orphaned copy metadata detected (*.copy.qcow.json without *.copy.data)"
    fi
  fi
  
  # Check for empty checkpoints directory
  local has_empty_checkpoints_dir=false
  if [[ -d "$backup_dir/checkpoints" ]] && [[ -z "$(find "$backup_dir/checkpoints" -type f 2>/dev/null | head -1)" ]]; then
    has_empty_checkpoints_dir=true
    log_debug "vmbackup.sh" "validate_backup_state" "[Phase 3/5] Empty checkpoints/ directory detected (incomplete backup indicator)"
  fi
  
  log_debug "vmbackup.sh" "validate_backup_state" "[Phase 3/5] Summary: dir_metadata=$dir_has_checkpoint_metadata markers=$has_checkpoint_markers data=$has_backup_data incomplete=$has_incomplete_data cpt_valid=$cpt_file_valid"
  
  # ─────────────────────────────────────────────────────────────────────────────
  # PHASE 4: State Classification
  # ─────────────────────────────────────────────────────────────────────────────
  log_debug "vmbackup.sh" "validate_backup_state" "[Phase 4/5] Classifying backup state"
  local qemu_has_checkpoints=false
  [[ $CACHED_CHECKPOINT_COUNT -gt 0 ]] && qemu_has_checkpoints=true
  
  # Classify directory state with detailed logging
  if $has_incomplete_data; then
    CACHED_DIR_STATE="incomplete_backup"
    log_debug "vmbackup.sh" "validate_backup_state" "[Phase 4/5] State: incomplete_backup (partial data files present)"
  elif $has_copy_backup; then
    # Copy backups are COMPLETE valid backups made while VM was offline
    # They should be ARCHIVED (not deleted) when VM comes online
    CACHED_DIR_STATE="copy_backup"
    log_debug "vmbackup.sh" "validate_backup_state" "[Phase 4/5] State: copy_backup (valid offline backup present)"
  elif $has_empty_checkpoints_dir && ($has_checkpoint_markers || ! $has_backup_data); then
    CACHED_DIR_STATE="incomplete_backup"
    log_debug "vmbackup.sh" "validate_backup_state" "[Phase 4/5] State: incomplete_backup (empty checkpoints/ dir with markers or no data)"
  elif ($dir_has_checkpoint_metadata || $has_checkpoint_markers) && ! $has_backup_data; then
    CACHED_DIR_STATE="missing_backup_data"
    log_debug "vmbackup.sh" "validate_backup_state" "[Phase 4/5] State: missing_backup_data (metadata exists but no .data files)"
  elif ! $qemu_has_checkpoints && ! $dir_has_checkpoint_metadata && ! $has_checkpoint_markers; then
    CACHED_DIR_STATE="clean"
    log_debug "vmbackup.sh" "validate_backup_state" "[Phase 4/5] State: clean (no QEMU checkpoints, no metadata, no markers)"
  elif $qemu_has_checkpoints && ! $dir_has_checkpoint_metadata && ! $has_checkpoint_markers; then
    CACHED_DIR_STATE="broken_chain"
    log_debug "vmbackup.sh" "validate_backup_state" "[Phase 4/5] State: broken_chain (QEMU checkpoints exist but no backup metadata)"
  elif ! $qemu_has_checkpoints && ($dir_has_checkpoint_metadata || $has_checkpoint_markers); then
    CACHED_DIR_STATE="stale_metadata"
    log_debug "vmbackup.sh" "validate_backup_state" "[Phase 4/5] State: stale_metadata (backup metadata exists but QEMU checkpoints missing)"
  elif $has_checkpoint_markers && ! $cpt_file_valid; then
    CACHED_DIR_STATE="stale_metadata"
    log_debug "vmbackup.sh" "validate_backup_state" "[Phase 4/5] State: stale_metadata (.cpt file exists but contains invalid data)"
  else
    CACHED_DIR_STATE="clean"
    log_debug "vmbackup.sh" "validate_backup_state" "[Phase 4/5] State: clean (QEMU and directory state consistent)"
  fi
  
  # Check for QEMU checkpoint chain corruption (overrides above classification)
  if $qemu_has_checkpoints && [[ "$CACHED_CHAIN_HEALTHY" == "false" ]]; then
    local prev_state="$CACHED_DIR_STATE"
    CACHED_DIR_STATE="broken_chain"
    log_debug "vmbackup.sh" "validate_backup_state" "[Phase 4/5] State override: $prev_state → broken_chain (QEMU checkpoint chain has gaps)"
  fi
  
  CACHED_VALIDATION_STATE="$CACHED_DIR_STATE"
  log_info "vmbackup.sh" "validate_backup_state" "[Phase 4/5] Final state classification: $CACHED_VALIDATION_STATE"
  
  # ─────────────────────────────────────────────────────────────────────────────
  # PHASE 5: Output Validation Box
  # ─────────────────────────────────────────────────────────────────────────────
  log_debug "vmbackup.sh" "validate_backup_state" "[Phase 5/5] Generating validation summary"
  local chain_status="OK (continuous)"
  [[ "$CACHED_CHAIN_HEALTHY" == "false" ]] && chain_status="BROKEN (gaps detected)"
  [[ $CACHED_CHECKPOINT_COUNT -eq 0 ]] && chain_status="N/A (no checkpoints)"
  
  local vm_state_note=""
  case "$CACHED_VM_STATE" in
    running)    vm_state_note="can do incremental" ;;
    paused)     vm_state_note="can do incremental (will unpause after)" ;;
    "shut off") vm_state_note="copy mode only" ;;
    *)          vm_state_note="unknown state" ;;
  esac
  
  local result_status="READY"
  local result_type="incremental"
  if [[ "$CACHED_DIR_STATE" == "copy_backup" ]]; then
    result_status="ARCHIVE"
    result_type="FULL (archiving copy backup first)"
  elif [[ "$CACHED_DIR_STATE" != "clean" ]]; then
    result_status="RECOVERY"
    result_type="FULL (cleanup required)"
  elif [[ $CACHED_CHECKPOINT_COUNT -eq 0 ]]; then
    result_type="FULL (first backup)"
  elif [[ "$CACHED_VM_STATE" == "shut off" ]]; then
    result_type="copy (VM offline)"
  fi
  
  # Reuse data file counts from Phase 3
  local data_summary="none"
  if [[ $full_data_files -gt 0 ]] || [[ $inc_data_files -gt 0 ]]; then
    data_summary="${full_data_files} full + ${inc_data_files} incremental"
  fi
  
  # Get .cpt marker name. Sort for deterministic pick if multiple
  # .cpt files exist (anomaly path — 109-bugs audit item 4).
  local cpt_marker="none"
  if [[ -n "$cpt_files" ]]; then
    cpt_marker=$(basename "$(echo "$cpt_files" | sort | head -1)" 2>/dev/null)
  fi
  
  # Output validation box
  log_info "vmbackup.sh" "validate_backup_state" "══════════════════════════════════════════════════════════"
  log_info "vmbackup.sh" "validate_backup_state" "VM: $vm_name"
  if [[ $CACHED_CHECKPOINT_COUNT -gt 0 ]]; then
    log_info "vmbackup.sh" "validate_backup_state" "QEMU Checkpoints: $CACHED_CHECKPOINT_COUNT ($CACHED_CHECKPOINT_FIRST → $CACHED_CHECKPOINT_LAST)"
  else
    log_info "vmbackup.sh" "validate_backup_state" "QEMU Checkpoints: 0 (none)"
  fi
  log_info "vmbackup.sh" "validate_backup_state" "Chain Integrity:  $chain_status"
  log_info "vmbackup.sh" "validate_backup_state" "VM State:         $CACHED_VM_STATE ($vm_state_note)"
  log_info "vmbackup.sh" "validate_backup_state" "Directory State:  $CACHED_DIR_STATE"
  log_info "vmbackup.sh" "validate_backup_state" "  ├─ .cpt marker: $cpt_marker"
  log_info "vmbackup.sh" "validate_backup_state" "  ├─ Backup data: $data_summary"
  log_info "vmbackup.sh" "validate_backup_state" "  └─ Incomplete:  $(if $has_incomplete_data; then echo 'YES (cleanup needed)'; else echo 'none'; fi)"
  log_info "vmbackup.sh" "validate_backup_state" "Result: $result_status for $result_type backup"
  log_info "vmbackup.sh" "validate_backup_state" "══════════════════════════════════════════════════════════"
  
  # Return based on critical failures only
  # Non-clean states are recoverable, so return 0
  return 0
}

# Check if validation cache is current for given VM
# Usage: is_cache_valid <vm_name>
is_cache_valid() {
  local vm_name="$1"
  [[ "$CACHED_VM_NAME" == "$vm_name" ]] && [[ -n "$CACHED_VALIDATION_STATE" ]]
}

# Get cached checkpoint count (returns 0 if cache invalid)
get_cached_checkpoint_count() {
  echo "${CACHED_CHECKPOINT_COUNT:-0}"
}

# Get cached validation state (returns "unknown" if cache invalid)
get_cached_validation_state() {
  echo "${CACHED_VALIDATION_STATE:-unknown}"
}

# Get cached chain health (returns "false" if cache invalid)
get_cached_chain_healthy() {
  echo "${CACHED_CHAIN_HEALTHY:-false}"
}

# Get cached backup data presence (returns "false" if cache invalid)
get_cached_has_backup_data() {
  echo "${CACHED_HAS_BACKUP_DATA:-false}"
}

# Determine backup level (full vs auto/incremental) based on day-of-month strategy
# Now uses cached validation data instead of making separate virsh calls
determine_backup_level() {
  local vm_name="$1"
  local requested_level="${2:-auto}"  # "full" or "auto"
  
  local day_of_month=$(date +%d)
  
  # RULE 1: First day of month = FULL if no valid full backup exists yet in current period
  # This ensures we get a monthly baseline, but allows incremental if already backed up today
  # FF-148: force base-10. printf '%02d' treats a zero-padded value ('08'/'09')
  # as invalid octal and substitutes 0 -> '00', which never equals date's '08'/
  # '09', so the monthly forced FULL was silently skipped. 10# forces decimal.
  if [[ "$day_of_month" == "$(printf '%02d' "$((10#$CHECKPOINT_FORCE_FULL_ON_DAY))")" ]]; then
    # Check if a valid full backup already exists for this period (using cached validation)
    if is_cache_valid "$vm_name"; then
      local cached_state=$(get_cached_validation_state)
      local cached_chain=$(get_cached_chain_healthy)
      local cached_has_data=$(get_cached_has_backup_data 2>/dev/null || echo "false")
      
      # If clean state with valid backup data, allow incremental (full already done today)
      if [[ "$cached_state" == "clean" ]] && [[ "$cached_has_data" == "true" || "$cached_chain" == "true" ]]; then
        log_info "vmbackup.sh" "determine_backup_level" \
          "Day $day_of_month (month start): Valid full backup exists in current period, allowing AUTO (incremental)"
        # Fall through to RULE 2 logic for auto handling
      else
        log_info "vmbackup.sh" "determine_backup_level" \
          "Day $day_of_month (month start): No valid full backup exists (state=$cached_state), forcing FULL for monthly reset"
        echo "full"
        return 0
      fi
    else
      # No cache available, be conservative and force FULL
      log_info "vmbackup.sh" "determine_backup_level" \
        "Day $day_of_month (month start): No cached validation, forcing FULL backup for monthly checkpoint reset"
      echo "full"
      return 0
    fi
  fi
  
  # RULE 2: Try AUTO if checkpoint healthy (using cached data)
  if [[ "$requested_level" == "auto" ]]; then
    # Use cached validation data if available
    if is_cache_valid "$vm_name"; then
      local cached_state=$(get_cached_validation_state)
      local cached_chain=$(get_cached_chain_healthy)
      
      if [[ "$cached_state" == "clean" ]] && [[ "$cached_chain" == "true" || $(get_cached_checkpoint_count) -eq 0 ]]; then
        log_info "vmbackup.sh" "determine_backup_level" \
          "Day $day_of_month: checkpoint healthy (cached), using AUTO (incremental) mode"
        echo "auto"
        return 0
      else
        log_warn "vmbackup.sh" "determine_backup_level" \
          "Day $day_of_month: checkpoint state=$cached_state chain_healthy=$cached_chain, forcing FULL backup"
        echo "full"
        return 0
      fi
    fi
    
    # Fallback to legacy check if cache not populated (shouldn't happen in normal flow)
    log_warn "vmbackup.sh" "determine_backup_level" \
      "Cache not populated for $vm_name, falling back to legacy validation"
    if validate_checkpoint_health "$vm_name"; then
      log_info "vmbackup.sh" "determine_backup_level" \
        "Day $day_of_month: checkpoint healthy, using AUTO (incremental) mode"
      echo "auto"
      return 0
    else
      log_warn "vmbackup.sh" "determine_backup_level" \
        "Day $day_of_month: checkpoint corrupted/missing, forcing FULL backup"
      echo "full"
      return 1
    fi
  fi
  
  # Explicit request (not auto)
  echo "$requested_level"
  return 0
}

# Validate checkpoint health before incremental backup
# ═══════════════════════════════════════════════════════════════════════════════
# DEPRECATED: Use validate_backup_state() instead (consolidated validation)
# This function is kept for backward compatibility but now uses cached data
# when available. Will be removed in future version.
# ═══════════════════════════════════════════════════════════════════════════════
validate_checkpoint_health() {
  local vm_name="$1"
  
  if [[ "$CHECKPOINT_HEALTH_CHECK" != "yes" ]]; then
    return 0  # Health check disabled
  fi
  
  # Use cached data if available (from validate_backup_state)
  if is_cache_valid "$vm_name"; then
    local cached_state=$(get_cached_validation_state)
    local cached_chain=$(get_cached_chain_healthy)
    local cached_count=$(get_cached_checkpoint_count)
    
    log_debug "vmbackup.sh" "validate_checkpoint_health" \
      "[DEPRECATED] Using cached validation: state=$cached_state chain=$cached_chain count=$cached_count"
    
    # Return based on cached state
    if [[ "$cached_state" == "clean" ]] && [[ "$cached_chain" == "true" || $cached_count -eq 0 ]]; then
      return 0
    else
      return 1
    fi
  fi
  
  # Legacy path: No cache available (shouldn't happen in normal flow)
  log_warn "vmbackup.sh" "validate_checkpoint_health" \
    "[DEPRECATED] Cache not available, using legacy validation for VM: $vm_name"
  
  log_info "vmbackup.sh" "validate_checkpoint_health" \
    "Monitoring QEMU checkpoint chain for VM: $vm_name"
  
  # CHECK 1: VM is running or paused (paused is OK - we pause for backup if no guest agent)
  local vm_state
  vm_state=$(lv_domain_state "$vm_name")
  if ! echo "$vm_state" | grep -q "running\|paused"; then
    log_warn "vmbackup.sh" "validate_checkpoint_health" \
      "VM state is $vm_state - backup will be in COPY mode (not incremental)"
    return 1
  fi
  
  # CHECK 2: QEMU Checkpoints exist (informational)
  local checkpoints=()
  mapfile -t checkpoints < <(lv_checkpoint_list_virtnbd "$vm_name")
  local depth=${#checkpoints[@]}
  
  if [[ $depth -eq 0 ]]; then
    log_info "vmbackup.sh" "validate_checkpoint_health" \
      "No QEMU checkpoints found - next backup will be FULL"
    return 0
  fi
  
  # CHECK 3: Monitor checkpoint depth (warning only, do NOT delete)
  # virtnbdbackup manages depth through monthly rotation:
  # - Each full backup (-l full) resets the chain to virtnbdbackup.0
  # - Incremental backups stack: virtnbdbackup.1, .2, .3, etc.
  # - Monthly rotation to new directory = automatic fresh chain
  if [[ $depth -gt $CHECKPOINT_MAX_DEPTH_WARN ]]; then
    log_warn "vmbackup.sh" "validate_checkpoint_health" \
      "QEMU checkpoint depth ($depth) exceeds recommendation (warn at $CHECKPOINT_MAX_DEPTH_WARN)"
    log_warn "vmbackup.sh" "validate_checkpoint_health" \
      "Consider forcing FULL backup or rotating to new backup directory"
  fi
  
  # Show first and last checkpoint for visibility
  log_info "vmbackup.sh" "validate_checkpoint_health" \
    "QEMU checkpoint chain healthy - VM has $depth checkpoint(s): ${checkpoints[0]} ... ${checkpoints[-1]}"
  return 0
}

# Monitor incremental backup size and detect anomalies
# PURPOSE: Detect sparseness issues and other anomalies during incremental backups
# See: https://github.com/abbbi/virtnbdbackup/issues/244 (sparseness anomalies)
monitor_incremental_size() {
  local backup_dir="${1:?Error: backup_dir required}"
  local vm_name="${2:?Error: vm_name required}"
  
  log_info "vmbackup.sh" "monitor_incremental_size" \
    "Monitoring incremental backup size for sparseness issues (VM: $vm_name)"
  
  # Get the size of all incremental backup files
  local total_size=0
  while IFS= read -r file; do
    [[ -z "$file" ]] && continue
    local size=$(stat -c%s "$file" 2>/dev/null || echo "0")
    total_size=$((total_size + size))
  done < <(find "$backup_dir" -maxdepth 1 -type f -name "*.inc.virtnbdbackup.*.data" 2>/dev/null)
  
  if [[ $total_size -gt 0 ]]; then
    local total_size_mb=$((total_size / 1024 / 1024))
    log_info "vmbackup.sh" "monitor_incremental_size" \
      "Incremental backup total size: ${total_size_mb}MB for VM $vm_name"
    
    # ANOMALY DETECTION: Warn if incremental backup is unusually large
    # Rule of thumb: incremental should be < 5% of full backup (assuming daily delta)
    # If full backup is 30GB, daily incremental should be < 1.5GB
    # This is a heuristic - actual size depends on workload change rate
    if [[ $total_size_mb -gt 2048 ]]; then
      log_warn "vmbackup.sh" "monitor_incremental_size" \
        "Large incremental backup detected (${total_size_mb}MB) - may indicate sparseness issues (GitHub issue #244)"
      log_warn "vmbackup.sh" "monitor_incremental_size" \
        "Monitor disk change rate - if consistently large, consider forcing FULL backup or monthly rotation"
    fi
  else
    log_info "vmbackup.sh" "monitor_incremental_size" \
      "No incremental backup files found for VM $vm_name (expected if this was first backup)"
  fi
  
  return 0
}

# Remove stale QEMU dirty bitmaps from a VM's disk images
# These bitmaps persist inside qcow2 even after checkpoint metadata is deleted,
# causing virtnbdbackup to fail with "Bitmap already exists" errors.
# Only operates on running VMs (offline VMs don't have active bitmaps).
# Args: $1=vm_name
# Returns: 0 always (best-effort cleanup, non-fatal)
remove_stale_qemu_bitmaps() {
  local vm_name="$1"

  # Only running VMs have in-memory dirty bitmaps to remove
  local vm_state
  vm_state=$(lv_domain_state "$vm_name")
  if [[ "$vm_state" != "running" && "$vm_state" != "paused" ]]; then
    log_debug "vmbackup.sh" "remove_stale_qemu_bitmaps" "VM $vm_name is $vm_state - skipping bitmap cleanup (only needed for running/paused VMs)"
    return 0
  fi

  # R5: jq parses the QEMU monitor JSON below. Without it we cannot tell
  # "no stale bitmaps" from "couldn't check", so guard explicitly and warn
  # here rather than letting an empty parse fall through to the reassuring
  # "No stale virtnbdbackup bitmaps found" log line further down. Packaged
  # installs always have jq (debian/control); this protects source installs.
  if ! command -v jq &>/dev/null; then
    log_warn "vmbackup.sh" "remove_stale_qemu_bitmaps" "bitmap cleanup unavailable: jq not found - cannot check for stale virtnbdbackup bitmaps on VM $vm_name"
    return 0
  fi

  # Query all block devices and extract virtnbdbackup dirty bitmaps
  local query_output
  # [LIBVIRT-KEEPER: QEMU-monitor bitmap surgery — not generic libvirt]
  query_output=$(virsh qemu-monitor-command "$vm_name" '{"execute":"query-block"}' 2>/dev/null) || {
    log_warn "vmbackup.sh" "remove_stale_qemu_bitmaps" "Failed to query block devices for VM $vm_name - bitmap cleanup skipped"
    return 0
  }

  # Parse JSON: extract node-name + bitmap name pairs for virtnbdbackup.* bitmaps
  local bitmap_entries
  bitmap_entries=$(echo "$query_output" | jq -r '
    .return[]
    | .inserted // empty
    | . as $dev
    | (.["dirty-bitmaps"] // [])[]
    | select(.name | startswith("virtnbdbackup."))
    | "\($dev["node-name"])|\(.name)"
  ' 2>/dev/null)

  if [[ -z "$bitmap_entries" ]]; then
    log_debug "vmbackup.sh" "remove_stale_qemu_bitmaps" "No stale virtnbdbackup bitmaps found for VM $vm_name"
    return 0
  fi

  local removed=0 failed=0
  while IFS='|' read -r node_name bitmap_name; do
    [[ -z "$node_name" || -z "$bitmap_name" ]] && continue
    local remove_result
    # [LIBVIRT-KEEPER: QEMU-monitor bitmap surgery — not generic libvirt]
    remove_result=$(virsh qemu-monitor-command "$vm_name" \
      "{\"execute\":\"block-dirty-bitmap-remove\",\"arguments\":{\"node\":\"${node_name}\",\"name\":\"${bitmap_name}\"}}" 2>&1)
    if echo "$remove_result" | jq -e '.return == {}' >/dev/null 2>&1; then
      log_info "vmbackup.sh" "remove_stale_qemu_bitmaps" "Removed stale bitmap: node=$node_name name=$bitmap_name"
      ((removed++))
    else
      log_warn "vmbackup.sh" "remove_stale_qemu_bitmaps" "Failed to remove bitmap: node=$node_name name=$bitmap_name result=$remove_result"
      ((failed++))
    fi
  done <<< "$bitmap_entries"

  log_info "vmbackup.sh" "remove_stale_qemu_bitmaps" "Bitmap cleanup for $vm_name: $removed removed, $failed failed"
  return 0
}

# _fail_chain_archive: FF-14 fail-closed handler for chain/copy archival failure.
# WHY: a failure from archive_existing_checkpoint_chain() means the existing
#   restore point was NOT safely archived. The callers used to log_warn and fall
#   through into destructive cleanup, silently discarding the only good backup.
#   We now refuse the cleanup before any delete so nothing is lost (fail-closed).
# PRESERVES: backup_dir is left exactly as-is (no checkpoints deleted, no data
#   removed); re-running retries archival. Sets CHAIN_ARCHIVE_FAILED for the
#   caller's fail-closed report. Always returns 1.
# Args: <vm_name> <backup_dir> <what>   (<what> = short noun, e.g. "checkpoint chain")
_fail_chain_archive() {
  local vm_name="$1"
  local backup_dir="$2"
  local what="$3"
  log_error "vmbackup.sh" "prepare_backup_directory" "[Cleanup 2/2] Archival of $what for VM $vm_name FAILED - preserved IN PLACE at $backup_dir (nothing deleted)"
  log_error "vmbackup.sh" "prepare_backup_directory" "[Cleanup 2/2] Cleanup REFUSED fail-closed - no files deleted, existing restore point retained"
  log_error "vmbackup.sh" "prepare_backup_directory" "[Cleanup 2/2] REMEDIATION: check free space / permissions / read-only state under <period>/.archives/chain-<date>"
  log_error "vmbackup.sh" "prepare_backup_directory" "[Cleanup 2/2] Re-run the backup to retry archival once the underlying condition is resolved"
  set_backup_error "CHAIN_ARCHIVE_FAILED" "Archival of $what failed - cleanup refused fail-closed, $backup_dir preserved in place" "Check free space/permissions under <period>/.archives/chain-<date>, then re-run"
  return 1
}

# Pre-backup cleanup: prepare directory based on validation state
# Returns: 0 if successful, 1 if cleanup failed
prepare_backup_directory() {
  local vm_name=$1
  local backup_dir=$2
  local requested_backup_type=$3
  local validation_state=$4
  
  log_info "vmbackup.sh" "prepare_backup_directory" \
    "VM: $vm_name | State: $validation_state | Requested: $requested_backup_type"
  
  case "$validation_state" in
    copy_backup)
      # Copy backup is a VALID complete backup made while VM was offline
      # When VM comes online, archive it (preserve restore point) before starting fresh chain
      log_info "vmbackup.sh" "prepare_backup_directory" "[Cleanup 2/2] COPY BACKUP DETECTED - archiving valid offline backup before new chain"
      
      if archive_existing_checkpoint_chain "$vm_name" "$backup_dir"; then
        log_info "vmbackup.sh" "prepare_backup_directory" "[Cleanup 2/2] Copy backup archived successfully - proceeding with new backup chain"
      else
        _fail_chain_archive "$vm_name" "$backup_dir" "copy backup"
        return 1
      fi

      if is_dry_run; then
        log_info "vmbackup.sh" "prepare_backup_directory" "[DRY-RUN] would clear orphaned QEMU checkpoints + checkpoints/ dir + copy metadata for new chain (VM: $vm_name)"
        return 0
      fi
      
      local cleanup_count=0
      
      # Delete orphaned QEMU checkpoints (from pre-copy-backup chain, now stale)
      # These exist in qcow2 but don't match the copy backup - must be cleared for new chain
      local checkpoints=()
      mapfile -t checkpoints < <(lv_checkpoint_list_virtnbd "$vm_name")
      if [[ ${#checkpoints[@]} -gt 0 ]]; then
        log_info "vmbackup.sh" "prepare_backup_directory" "[Cleanup 2/2] Found ${#checkpoints[@]} orphaned QEMU checkpoints - clearing for new chain"
        for cp in "${checkpoints[@]}"; do
          if lv_checkpoint_delete_metadata "$vm_name" "$cp"; then
            log_debug "vmbackup.sh" "prepare_backup_directory" "[Cleanup 2/2] Deleted orphaned checkpoint: $cp"
            ((cleanup_count++))
          fi
        done
      fi
      
      # Clean checkpoints/ directory if present
      if [[ -d "$backup_dir/checkpoints" ]]; then
        rm -rf "$backup_dir/checkpoints"
        log_debug "vmbackup.sh" "prepare_backup_directory" "[Cleanup 2/2] Deleted: checkpoints/"
        ((cleanup_count++))
      fi
      
      # Clean any remaining copy metadata (json files without data already moved)
      for pattern in "vmconfig.copy.xml" "*.copy.qcow.json" "*.copy.data.chksum" "*.cpt"; do
        for file in "$backup_dir"/$pattern; do
          if [[ -f "$file" ]]; then
            log_debug "vmbackup.sh" "prepare_backup_directory" "[Cleanup 2/2] Deleted remaining: $(basename "$file")"
            rm -f "$file"
            ((cleanup_count++))
          fi
        done
      done
      
      if [[ $cleanup_count -gt 0 ]]; then
        log_info "vmbackup.sh" "prepare_backup_directory" "[Cleanup 2/2] Cleanup complete: $cleanup_count items removed"
        if declare -f log_file_operation >/dev/null 2>&1; then
          log_file_operation "delete" "$vm_name" "$backup_dir" "" \
            "directory" "Copy backup upgrade cleanup: $cleanup_count items removed" \
            "prepare_backup_directory" "true"
        fi
      fi
      return 0
      ;;
      
    clean)
      # State is clean - minimal cleanup needed
      log_info "vmbackup.sh" "prepare_backup_directory" "[Cleanup 2/2] Directory validated: clean state, proceeding with backup"

      if is_dry_run; then
        log_info "vmbackup.sh" "prepare_backup_directory" "[DRY-RUN] would remove stale backup.*.log files for VM: $vm_name"
        return 0
      fi
      
      local cleanup_count=0
      for file in "$backup_dir"/backup.*.log; do
        if [[ -f "$file" ]]; then
          log_debug "vmbackup.sh" "prepare_backup_directory" "[Cleanup 2/2] Deleted: $(basename "$file")"
          rm -f "$file"
          ((cleanup_count++))
        fi
      done
      
      if [[ $cleanup_count -gt 0 ]]; then
        log_info "vmbackup.sh" "prepare_backup_directory" "[Cleanup 2/2] Complete: $cleanup_count old log files removed"
      fi
      return 0
      ;;
      
    stale_metadata)
      # CRITICAL: Stale metadata blocks virtnbdbackup from starting FULL backups
      # Respect ENABLE_AUTO_RECOVERY_ON_CHECKPOINT_CORRUPTION setting
      log_error "vmbackup.sh" "prepare_backup_directory" "[Cleanup 2/2] STALE CHECKPOINT DETECTED (invalid .cpt content or missing QEMU checkpoints)"
      
      if [[ "$ENABLE_AUTO_RECOVERY_ON_CHECKPOINT_CORRUPTION" == "no" ]]; then
        # NO MODE: Fail immediately without remediation info
        log_error "vmbackup.sh" "prepare_backup_directory" "Auto-recovery is DISABLED (ENABLE_AUTO_RECOVERY_ON_CHECKPOINT_CORRUPTION=no)"
        log_error "vmbackup.sh" "prepare_backup_directory" "Backup aborted - manual intervention required"
        set_backup_error "CHECKPOINT_STALE" "Stale checkpoint metadata - auto-recovery disabled" "Recovery mode: no"
        return 1
      elif [[ "$ENABLE_AUTO_RECOVERY_ON_CHECKPOINT_CORRUPTION" == "warn" ]]; then
        # WARN MODE: Fail with clear remediation steps
        log_error "vmbackup.sh" "prepare_backup_directory" "CHECKPOINT CORRUPTION RECOVERY REQUIRED (user decision needed)"
        log_error "vmbackup.sh" "prepare_backup_directory" ""
        log_error "vmbackup.sh" "prepare_backup_directory" "ROOT CAUSE: .cpt file contains invalid/corrupted checkpoint data"
        log_error "vmbackup.sh" "prepare_backup_directory" "IMPACT: Cannot continue incremental backups - recovery required"
        log_error "vmbackup.sh" "prepare_backup_directory" ""
        log_error "vmbackup.sh" "prepare_backup_directory" "REMEDIATION OPTIONS:"
        log_error "vmbackup.sh" "prepare_backup_directory" "  Option 1 (RECOMMENDED - Full reset):"
        log_error "vmbackup.sh" "prepare_backup_directory" "    sudo rm -rf $backup_dir/checkpoints"
        log_error "vmbackup.sh" "prepare_backup_directory" "    sudo rm -f $backup_dir/*.cpt"
        log_error "vmbackup.sh" "prepare_backup_directory" "    sudo rm -f $backup_dir/*.data"
        log_error "vmbackup.sh" "prepare_backup_directory" "    Then re-run backup (will do FULL backup)"
        log_error "vmbackup.sh" "prepare_backup_directory" ""
        log_error "vmbackup.sh" "prepare_backup_directory" "  Option 2 (AUTO - Enable auto-recovery):"
        log_error "vmbackup.sh" "prepare_backup_directory" "    Set ENABLE_AUTO_RECOVERY_ON_CHECKPOINT_CORRUPTION=\"yes\" in config"
        log_error "vmbackup.sh" "prepare_backup_directory" "    Then re-run backup (will auto-cleanup and do FULL)"
        log_error "vmbackup.sh" "prepare_backup_directory" ""
        set_backup_error "CHECKPOINT_STALE" "Stale checkpoint - manual recovery required (warn mode)" "Run: sudo rm -rf $backup_dir/checkpoints $backup_dir/*.cpt"
        return 1
      fi
      
      # YES MODE: Auto-cleanup and proceed
      log_warn "vmbackup.sh" "prepare_backup_directory" "AUTO-RECOVERY ENABLED: Cleaning stale checkpoint data"
      if is_dry_run; then
        log_info "vmbackup.sh" "prepare_backup_directory" "[DRY-RUN] would clean stale checkpoint data (archive chain, delete checkpoints + bitmaps, clean directory) and force FULL for VM: $vm_name"
        return 0
      fi
      
      # Archive existing valid backup chain before cleanup destroys it
      local has_valid_chain=false
      if find "$backup_dir" -maxdepth 1 \( -name "*.full.data" -o -name "*.copy.data" \) -print -quit 2>/dev/null | grep -q .; then
        has_valid_chain=true
      fi
      
      if [[ "$has_valid_chain" == "true" ]]; then
        log_info "vmbackup.sh" "prepare_backup_directory" "[Cleanup 2/2] Valid backup chain detected - archiving before cleanup"
        if archive_existing_checkpoint_chain "$vm_name" "$backup_dir"; then
          log_info "vmbackup.sh" "prepare_backup_directory" "[Cleanup 2/2] Previous chain archived successfully"
        else
          _fail_chain_archive "$vm_name" "$backup_dir" "checkpoint chain"
          return 1
        fi
      else
        log_debug "vmbackup.sh" "prepare_backup_directory" "[Cleanup 2/2] No valid backup chain to archive"
      fi
      
      local cleanup_count=0
      
      # Delete QEMU checkpoints
      local checkpoints
      mapfile -t checkpoints < <(lv_checkpoint_list_virtnbd "$vm_name")
      for cp in "${checkpoints[@]}"; do
        if lv_checkpoint_delete_metadata "$vm_name" "$cp"; then
          log_info "vmbackup.sh" "prepare_backup_directory" "[Cleanup 2/2] Deleted QEMU checkpoint: $cp"
          ((cleanup_count++))
        fi
      done
      
      # Remove stale dirty bitmaps from qcow2 disk images (P4-1 fix)
      remove_stale_qemu_bitmaps "$vm_name"
      
      # Clean backup directory completely (with quoted paths for space handling)
      if [[ -d "$backup_dir/checkpoints" ]]; then
        log_debug "vmbackup.sh" "prepare_backup_directory" "[Cleanup 2/2] Deleted: checkpoints/"
        rm -rf "$backup_dir/checkpoints"
        ((cleanup_count++))
      fi
      
      for pattern in "*.cpt" "*.virtnbdbackup.*.qcow.json" "vmconfig.virtnbdbackup.*.xml" "*.data" "*.data.chksum" "backup.*.log"; do
        for file in "$backup_dir"/$pattern; do
          if [[ -f "$file" ]]; then
            log_debug "vmbackup.sh" "prepare_backup_directory" "[Cleanup 2/2] Deleted: $(basename "$file")"
            rm -f "$file"
            ((cleanup_count++))
          fi
        done
      done
      
      log_warn "vmbackup.sh" "prepare_backup_directory" \
        "[Cleanup 2/2] Stale checkpoint cleanup complete: $cleanup_count items removed - forcing FULL backup"
      if declare -f log_file_operation >/dev/null 2>&1; then
        log_file_operation "delete" "$vm_name" "$backup_dir" "" \
          "directory" "Stale metadata auto-recovery: $cleanup_count items removed" \
          "prepare_backup_directory" "true"
      fi
      return 0
      ;;
      
    broken_chain)
      # Checkpoint chain is inconsistent - respect ENABLE_AUTO_RECOVERY_ON_CHECKPOINT_CORRUPTION setting
      log_error "vmbackup.sh" "prepare_backup_directory" "[Cleanup 2/2] BROKEN CHECKPOINT CHAIN (gaps in sequence detected)"
      
      if [[ "$ENABLE_AUTO_RECOVERY_ON_CHECKPOINT_CORRUPTION" == "no" ]]; then
        # NO MODE: Fail immediately without remediation info
        log_error "vmbackup.sh" "prepare_backup_directory" "Auto-recovery is DISABLED (ENABLE_AUTO_RECOVERY_ON_CHECKPOINT_CORRUPTION=no)"
        log_error "vmbackup.sh" "prepare_backup_directory" "Backup aborted - manual intervention required"
        set_backup_error "CHECKPOINT_BROKEN" "Broken checkpoint chain - auto-recovery disabled" "Recovery mode: no"
        return 1
      elif [[ "$ENABLE_AUTO_RECOVERY_ON_CHECKPOINT_CORRUPTION" == "warn" ]]; then
        # WARN MODE: Fail with clear remediation steps
        log_error "vmbackup.sh" "prepare_backup_directory" "CHECKPOINT CORRUPTION RECOVERY REQUIRED (user decision needed)"
        log_error "vmbackup.sh" "prepare_backup_directory" ""
        log_error "vmbackup.sh" "prepare_backup_directory" "ROOT CAUSE: Checkpoint chain has gaps (missing checkpoints in sequence)"
        log_error "vmbackup.sh" "prepare_backup_directory" "IMPACT: Cannot continue incremental backups - recovery required"
        log_error "vmbackup.sh" "prepare_backup_directory" ""
        log_error "vmbackup.sh" "prepare_backup_directory" "REMEDIATION OPTIONS:"
        log_error "vmbackup.sh" "prepare_backup_directory" "  Option 1 (RECOMMENDED - Full reset):"
        log_error "vmbackup.sh" "prepare_backup_directory" "    sudo rm -rf $backup_dir/checkpoints"
        log_error "vmbackup.sh" "prepare_backup_directory" "    sudo rm -f $backup_dir/*.cpt"
        log_error "vmbackup.sh" "prepare_backup_directory" "    sudo rm -f $backup_dir/*.data"
        log_error "vmbackup.sh" "prepare_backup_directory" "    Then re-run backup (will do FULL backup)"
        log_error "vmbackup.sh" "prepare_backup_directory" ""
        log_error "vmbackup.sh" "prepare_backup_directory" "  Option 2 (AUTO - Enable auto-recovery):"
        log_error "vmbackup.sh" "prepare_backup_directory" "    Set ENABLE_AUTO_RECOVERY_ON_CHECKPOINT_CORRUPTION=\"yes\" in config"
        log_error "vmbackup.sh" "prepare_backup_directory" "    Then re-run backup (will auto-cleanup and do FULL)"
        log_error "vmbackup.sh" "prepare_backup_directory" ""
        log_error "vmbackup.sh" "prepare_backup_directory" "KNOWN ISSUE: https://github.com/abbbi/virtnbdbackup/discussions/267"
        set_backup_error "CHECKPOINT_BROKEN" "Broken checkpoint chain - manual recovery required (warn mode)" "Run: sudo rm -rf $backup_dir/checkpoints $backup_dir/*.cpt"
        return 1
      fi
      
      # YES MODE: Auto-cleanup and proceed
      log_warn "vmbackup.sh" "prepare_backup_directory" "AUTO-RECOVERY ENABLED: Resetting broken checkpoint chain"
      if is_dry_run; then
        log_info "vmbackup.sh" "prepare_backup_directory" "[DRY-RUN] would reset broken checkpoint chain (archive chain, delete checkpoints + bitmaps, clean directory) and force FULL for VM: $vm_name"
        return 0
      fi
      
      # Archive existing valid backup chain before cleanup destroys it
      local has_valid_chain=false
      if find "$backup_dir" -maxdepth 1 \( -name "*.full.data" -o -name "*.copy.data" \) -print -quit 2>/dev/null | grep -q .; then
        has_valid_chain=true
      fi
      
      if [[ "$has_valid_chain" == "true" ]]; then
        log_info "vmbackup.sh" "prepare_backup_directory" "[Cleanup 2/2] Valid backup chain detected - archiving before cleanup"
        if archive_existing_checkpoint_chain "$vm_name" "$backup_dir"; then
          log_info "vmbackup.sh" "prepare_backup_directory" "[Cleanup 2/2] Previous chain archived successfully"
        else
          _fail_chain_archive "$vm_name" "$backup_dir" "checkpoint chain"
          return 1
        fi
      else
        log_debug "vmbackup.sh" "prepare_backup_directory" "[Cleanup 2/2] No valid backup chain to archive"
      fi
      
      local cleanup_count=0
      
      # Delete ALL QEMU checkpoints for this VM
      local checkpoints
      mapfile -t checkpoints < <(lv_checkpoint_list_virtnbd "$vm_name")
      if [[ ${#checkpoints[@]} -gt 0 ]]; then
        for cp in "${checkpoints[@]}"; do
          if lv_checkpoint_delete_metadata "$vm_name" "$cp"; then
            log_info "vmbackup.sh" "prepare_backup_directory" "[Cleanup 2/2] Deleted orphaned QEMU checkpoint: $cp"
            ((cleanup_count++))
          fi
        done
      fi
      
      # Remove stale dirty bitmaps from qcow2 disk images (P4-1 fix)
      remove_stale_qemu_bitmaps "$vm_name"
      
      # Clean backup directory completely to start fresh
      if [[ -d "$backup_dir/checkpoints" ]]; then
        log_debug "vmbackup.sh" "prepare_backup_directory" "[Cleanup 2/2] Deleted: checkpoints/"
        rm -rf "$backup_dir/checkpoints"
        ((cleanup_count++))
      fi
      
      for pattern in "*.cpt" "*.virtnbdbackup.*.qcow.json" "vmconfig.virtnbdbackup.*.xml" "*.data" "*.data.chksum" "backup.*.log"; do
        for file in "$backup_dir"/$pattern; do
          if [[ -f "$file" ]]; then
            log_debug "vmbackup.sh" "prepare_backup_directory" "[Cleanup 2/2] Deleted: $(basename "$file")"
            rm -f "$file"
            ((cleanup_count++))
          fi
        done
      done
      
      log_warn "vmbackup.sh" "prepare_backup_directory" \
        "[Cleanup 2/2] Broken checkpoint chain reset: $cleanup_count items removed - forcing FULL backup"
      if declare -f log_file_operation >/dev/null 2>&1; then
        log_file_operation "delete" "$vm_name" "$backup_dir" "" \
          "directory" "Broken chain auto-recovery: $cleanup_count items removed" \
          "prepare_backup_directory" "true"
      fi
      return 0
      ;;
      
    missing_backup_data)
      # CRITICAL: Checkpoint metadata exists but actual backup data files are missing
      # This indicates failed/incomplete previous backups that left metadata orphaned
      log_error "vmbackup.sh" "prepare_backup_directory" \
        "[Cleanup 2/2] ORPHANED CHECKPOINT METADATA DETECTED - checkpoint files exist but backup data is missing"
      log_error "vmbackup.sh" "prepare_backup_directory" \
        "[Cleanup 2/2] This indicates incomplete/failed backups. Performing aggressive cleanup and forcing FULL backup"
      if is_dry_run; then
        log_info "vmbackup.sh" "prepare_backup_directory" "[DRY-RUN] would aggressively clean orphaned checkpoint metadata + incomplete backup data and force FULL for VM: $vm_name"
        return 0
      fi
      
      local cleanup_count=0
      
      # Delete ALL QEMU checkpoints for this VM
      local checkpoints
      mapfile -t checkpoints < <(lv_checkpoint_list_virtnbd "$vm_name")
      if [[ ${#checkpoints[@]} -gt 0 ]]; then
        for cp in "${checkpoints[@]}"; do
          if lv_checkpoint_delete_metadata "$vm_name" "$cp"; then
            log_info "vmbackup.sh" "prepare_backup_directory" "[Cleanup 2/2] Deleted QEMU checkpoint: $cp (orphaned)"
            ((cleanup_count++))
          fi
        done
      fi
      
      # Remove stale dirty bitmaps from qcow2 disk images (P4-1 fix)
      remove_stale_qemu_bitmaps "$vm_name"
      
      # Aggressively clean ENTIRE backup directory - corrupted state requires fresh start
      log_warn "vmbackup.sh" "prepare_backup_directory" "[Cleanup 2/2] Removing all checkpoint metadata and incomplete backup data"
      
      if [[ -d "$backup_dir/checkpoints" ]]; then
        log_debug "vmbackup.sh" "prepare_backup_directory" "[Cleanup 2/2] Deleted: checkpoints/"
        rm -rf "$backup_dir/checkpoints"
        ((cleanup_count++))
      fi
      
      for pattern in "*.cpt" "*.virtnbdbackup.*.qcow.json" "*.virtnbdbackup.*" "vmconfig.virtnbdbackup.*.xml" "*.data" "*.data.chksum" "*.inc.virtnbdbackup*.data" "*.tar.gzip" "backup.*.log"; do
        for file in "$backup_dir"/$pattern; do
          if [[ -f "$file" ]]; then
            log_debug "vmbackup.sh" "prepare_backup_directory" "[Cleanup 2/2] Deleted: $(basename "$file")"
            rm -f "$file"
            ((cleanup_count++))
          fi
        done
      done
      
      log_warn "vmbackup.sh" "prepare_backup_directory" \
        "[Cleanup 2/2] Orphaned metadata cleanup complete: $cleanup_count items removed - forcing FULL backup"
      if declare -f log_file_operation >/dev/null 2>&1; then
        log_file_operation "delete" "$vm_name" "$backup_dir" "" \
          "directory" "Missing backup data cleanup: $cleanup_count items removed" \
          "prepare_backup_directory" "true"
      fi
      return 0
      ;;
      
    incomplete_backup)
      # Incomplete backup detected - interrupted/failed backup with partial files and/or empty checkpoints
      log_error "vmbackup.sh" "prepare_backup_directory" \
        "[Cleanup 2/2] INCOMPLETE BACKUP DETECTED - partial/interrupted backup files or empty checkpoint directory found"
      log_error "vmbackup.sh" "prepare_backup_directory" \
        "[Cleanup 2/2] Partial backup files and all checkpoint metadata will be removed to allow fresh backup attempt"

      if is_dry_run; then
        log_info "vmbackup.sh" "prepare_backup_directory" "[DRY-RUN] would clean incomplete backup data (archive valid chain, delete checkpoints + bitmaps, remove partial/data files) and force FULL for VM: $vm_name"
        return 0
      fi
      
      # Archive existing valid backup chain before cleanup destroys it
      # An interrupted incremental may still have a valid FULL that's restorable
      local has_valid_chain=false
      if find "$backup_dir" -maxdepth 1 \( -name "*.full.data" -o -name "*.copy.data" \) -print -quit 2>/dev/null | grep -q .; then
        has_valid_chain=true
      fi
      
      if [[ "$has_valid_chain" == "true" ]]; then
        log_info "vmbackup.sh" "prepare_backup_directory" "[Cleanup 2/2] Valid backup data detected - archiving before cleanup"
        if archive_existing_checkpoint_chain "$vm_name" "$backup_dir"; then
          log_info "vmbackup.sh" "prepare_backup_directory" "[Cleanup 2/2] Previous chain archived successfully"
        else
          _fail_chain_archive "$vm_name" "$backup_dir" "checkpoint chain"
          return 1
        fi
      else
        log_debug "vmbackup.sh" "prepare_backup_directory" "[Cleanup 2/2] No valid backup chain to archive"
      fi
      
      local cleanup_count=0
      
      # Delete ALL QEMU checkpoints for this VM
      local checkpoints
      mapfile -t checkpoints < <(lv_checkpoint_list_virtnbd "$vm_name")
      if [[ ${#checkpoints[@]} -gt 0 ]]; then
        for cp in "${checkpoints[@]}"; do
          if lv_checkpoint_delete_metadata "$vm_name" "$cp"; then
            log_info "vmbackup.sh" "prepare_backup_directory" "[Cleanup 2/2] Deleted QEMU checkpoint: $cp (from incomplete backup)"
            ((cleanup_count++))
          fi
        done
      fi
      
      # Remove stale dirty bitmaps from qcow2 disk images (P4-1 fix)
      remove_stale_qemu_bitmaps "$vm_name"
      
      # Remove incomplete backup files and all checkpoint metadata
      log_warn "vmbackup.sh" "prepare_backup_directory" "[Cleanup 2/2] Removing incomplete backup files and checkpoint data"
      
      if [[ -d "$backup_dir/checkpoints" ]]; then
        log_debug "vmbackup.sh" "prepare_backup_directory" "[Cleanup 2/2] Deleted: checkpoints/"
        rm -rf "$backup_dir/checkpoints"
        ((cleanup_count++))
      fi
      
      for pattern in "*.cpt" "*.partial" "*.virtnbdbackup.*.qcow.json" "*.virtnbdbackup.*" "vmconfig.virtnbdbackup.*.xml" "vmconfig.copy.xml" "*.copy.data" "*.copy.qcow.json" "*.copy.data.chksum" "*.data" "*.data.chksum" "*.qcow.json" "*.inc.virtnbdbackup*.data" "*.full.*" "*.tar.gzip" "backup.*.log"; do
        for file in "$backup_dir"/$pattern; do
          if [[ -f "$file" ]]; then
            log_debug "vmbackup.sh" "prepare_backup_directory" "[Cleanup 2/2] Deleted: $(basename "$file")"
            rm -f "$file"
            ((cleanup_count++))
          fi
        done
      done
      
      log_warn "vmbackup.sh" "prepare_backup_directory" \
        "[Cleanup 2/2] Incomplete backup cleanup complete: $cleanup_count items removed - forcing FULL backup"
      if declare -f log_file_operation >/dev/null 2>&1; then
        log_file_operation "delete" "$vm_name" "$backup_dir" "" \
          "directory" "Incomplete backup cleanup: $cleanup_count items removed" \
          "prepare_backup_directory" "true"
      fi
      return 0
      ;;
      
    *)
      log_error "vmbackup.sh" "prepare_backup_directory" \
        "CRITICAL: Unknown/invalid checkpoint validation state: '$validation_state' (expected: copy_backup, clean, stale_metadata, broken_chain, missing_backup_data, incomplete_backup)"
      return 1
      ;;
  esac
}

# Perform backup with retry logic
# Now uses cached validation data from validate_backup_state() called in backup_vm()
perform_backup() {
  local vm_name=$1
  local requested_backup_type=${2:-auto}  # "full", "incremental", or "auto"
  local backup_dir=$3
  local attempt=1
  local backup_type
  # R2: freeze-stall detection state. The progress monitor touches
  # $_stall_sentinel when it kills an attempt for a filesystem-freeze stall;
  # we count those across attempts and fail closed at BACKUP_FSFREEZE_STALL_LIMIT.
  # vm_name is already charset-validated upstream, so it is safe in the path.
  local _stall_sentinel="${TEMP_DIR}/.fsfreeze-stall.$$.${vm_name}"
  local _freeze_stall_count=0
  local _freeze_stall_seen=false
  rm -f "$_stall_sentinel" 2>/dev/null || true
  # FF-150: build the reap-pattern for THIS VM's virtnbdbackup only. pgrep/pkill
  # -f compile the pattern with glibc regcomp(REG_EXTENDED), where GNU regex
  # operators (\< \> \` \') stay active — backslash-escaping a NON-metacharacter
  # would forge an operator, not a literal — so we escape ONLY the true ERE
  # metacharacters ( . [ ] ( ) { } * + ? | ^ $ \ ) and pass every other byte
  # through untouched. Anchoring to the "-d <name> -l" argv boundary that
  # lib/virtnbd.sh always emits means a substring or special-character name can
  # never match a DIFFERENT VM's in-flight backup; a name embedding the literal
  # " -l " token can still prefix-match, but that is unreachable in-session
  # (single-session pidfile guard + sequential per-VM loop) and accepted.
  # Pure-bash character loop (no sed/awk).
  local _vnb_c _vnb_esc="" _vnb_i
  for (( _vnb_i=0; _vnb_i<${#vm_name}; _vnb_i++ )); do
    _vnb_c="${vm_name:_vnb_i:1}"
    case "$_vnb_c" in
      '.'|'['|']'|'('|')'|'{'|'}'|'*'|'+'|'?'|'|'|'^'|'$'|\\) _vnb_esc+="\\$_vnb_c" ;;
      *) _vnb_esc+="$_vnb_c" ;;
    esac
  done
  local _vnb_reap_pat="virtnbdbackup -d ${_vnb_esc} -l"
  
  # PRE-BACKUP VALIDATION: Use cached validation state from backup_vm()
  # validate_backup_state() was already called in backup_vm() before this function
  local validation_state
  if is_cache_valid "$vm_name"; then
    validation_state=$(get_cached_validation_state)
    log_info "vmbackup.sh" "perform_backup" "Using cached validation state: $validation_state"
  else
    # Fallback: Run validation if cache is missing (shouldn't happen in normal flow)
    log_warn "vmbackup.sh" "perform_backup" "Cache not populated, running validation for VM: $vm_name"
    validate_backup_state "$vm_name" "$backup_dir"
    validation_state=$(get_cached_validation_state)
  fi
  
  # Prepare directory based on validation result
  if ! prepare_backup_directory "$vm_name" "$backup_dir" "$requested_backup_type" "$validation_state"; then
    log_error "vmbackup.sh" "perform_backup" "Failed to prepare backup directory for VM: $vm_name"
    # Error code already set by prepare_backup_directory if it was a checkpoint issue
    # Only set generic error if not already set
    if [[ -z "$LAST_ERROR_CODE" ]]; then
      set_backup_error "DIRECTORY_PREP_FAILED" "Failed to prepare backup directory" "validation_state=$validation_state"
    fi
    return 1
  fi
  
  # Determine backup level based on day-of-month strategy AND validation state
  backup_type=$(determine_backup_level "$vm_name" "$requested_backup_type")
  
  # CRITICAL: If checkpoint chain is broken, force FULL backup regardless of strategy
  if [[ "$validation_state" == "broken_chain" ]] || [[ "$validation_state" == "stale_metadata" ]] || [[ "$validation_state" == "missing_backup_data" ]] || [[ "$validation_state" == "incomplete_backup" ]]; then
    log_warn "vmbackup.sh" "perform_backup" \
      "Checkpoint state compromised ($validation_state) - forcing FULL backup for VM: $vm_name"
    backup_type="full"
  fi
  
  log_info "vmbackup.sh" "perform_backup" "Starting $backup_type backup for VM: $vm_name (attempt 1/$((MAX_RETRIES + 1)))"
  
  while (( attempt <= MAX_RETRIES + 1 )); do
    # R2: clear any freeze-stall sentinel from a previous attempt so detection
    # only ever reflects THIS attempt's monitor.
    rm -f "$_stall_sentinel" 2>/dev/null || true
    # UNI-014 M1: build virtnbdbackup argv via lib/virtnbd.sh helper.
    # Strategy: Leverage virtnbdbackup's native capabilities for full/incremental backups,
    # compression, and changed block tracking (CBT) via QEMU dirty bitmaps/checkpoints.
    # All option-driven branches now live in build_virtnbdbackup_args (109-phase6-spec.md §2.3).
    build_virtnbdbackup_args "$backup_type" "$vm_name" "$backup_dir" "$VIRTNBD_SCRATCH_DIR"
    local virtnbd_cmd_display="${_VIRTNBD_ARGS[*]}"
    
    log_info "vmbackup.sh" "perform_backup" "Command: $virtnbd_cmd_display"
    log_info "vmbackup.sh" "perform_backup" "Backup level: $backup_type | Output: $backup_dir | Compression: $VIRTNBD_COMPRESS_LEVEL"
    log_info "vmbackup.sh" "perform_backup" "Backup starting at $(date '+%Y-%m-%d %H:%M:%S')"
    
    # DRY-RUN: Skip actual backup execution
    if is_dry_run; then
      log_info "vmbackup.sh" "perform_backup" "[DRY-RUN] Would execute: $virtnbd_cmd_display"
      log_info "vmbackup.sh" "perform_backup" "[DRY-RUN] Skipping actual backup for VM: $vm_name ($backup_type)"
      return 0
    fi
    
    # Execute backup without wall-clock timeout - uses adaptive progress monitoring instead
    # This allows large backups to complete naturally while detecting true hangs
    local backup_log="${LOG_DIR}/backup_${vm_name}_$(date +%s).log"
    
    # Run backup process (no timeout wrapper)
    # Build priority wrapper command based on settings
    local priority_wrapper=""
    if [[ -n "$IO_PRIORITY_CLASS" ]] && command -v ionice &>/dev/null; then
      if [[ "$IO_PRIORITY_CLASS" == "3" ]]; then
        # Idle class doesn't use -n level
        priority_wrapper="ionice -c $IO_PRIORITY_CLASS"
      else
        priority_wrapper="ionice -c ${IO_PRIORITY_CLASS:-2} -n ${IO_PRIORITY_LEVEL:-4}"
      fi
    fi
    if (( PROCESS_PRIORITY != 0 )); then
      priority_wrapper="$priority_wrapper nice -n $PROCESS_PRIORITY"
    fi
    
    # UNI-014 M1: exec via array (no `bash -c "$virtnbd_cmd"`). Priority
    # wrapper is unquoted on purpose so its words split into argv.
    if [[ -n "$priority_wrapper" ]]; then
      log_debug "vmbackup.sh" "perform_backup" "Running with priority: $priority_wrapper"
      _BACKUP_IN_PROGRESS="true"
      $priority_wrapper "${_VIRTNBD_ARGS[@]}" > >(tee -a "$backup_log") 2>&1 &
    else
      _BACKUP_IN_PROGRESS="true"
      "${_VIRTNBD_ARGS[@]}" > >(tee -a "$backup_log") 2>&1 &
    fi
    
    local backup_pid=$!
    
    # Start progress monitor in background - watches for stalls, not elapsed time
    monitor_backup_progress $backup_pid "$backup_dir" "$vm_name" "$_stall_sentinel" &
    local monitor_pid=$!
    
    # Wait for backup to complete
    if wait $backup_pid 2>/dev/null; then
      _BACKUP_IN_PROGRESS="false"
      local backup_end_time=$(date '+%Y-%m-%d %H:%M:%S')
      
      # Kill monitor and its children (e.g. sleep) if still running
      pkill -P $monitor_pid 2>/dev/null || true
      kill $monitor_pid 2>/dev/null || true
      
      # virtnbdbackup sometimes exits 0 despite logging ERROR lines (e.g.,
      # "target directory already contains full or copy backup", bitmap
      # conflicts, extent read failures). Scan the captured log for any
      # ERROR line and treat it as a backup failure.
      #
      # INT-23: anchor the match to virtnbdbackup's own log-line format
      # ("[YYYY-MM-DD HH:MM:SS] ERROR ") and strip ANSI colour codes first,
      # so that benign substrings containing the word "error" elsewhere
      # (guest-agent warnings, fstrim INFO lines, bitlocker "skipping"
      # WARNINGs, ANSI-coloured non-ERROR severities) do not mis-flag a
      # successful run as VIRTNBD_FALSE_SUCCESS. Originally reported in
      # doutsis/vmbackup#1 and proposed in PR#2 by @hostarts.
      if [[ -f "$backup_log" ]] && \
         sed -E 's/\x1b\[[0-9;]*m//g' "$backup_log" 2>/dev/null | \
         grep -qE '^\[[0-9-]{10} [0-9:]{8}\] ERROR ' 2>/dev/null; then
          local error_lines
          error_lines=$(sed -E 's/\x1b\[[0-9;]*m//g' "$backup_log" 2>/dev/null | \
              grep -E '^\[[0-9-]{10} [0-9:]{8}\] ERROR ' | tail -3 | tr '\n' ' ' | cut -c1-200)
          log_error "vmbackup.sh" "perform_backup" \
              "virtnbdbackup exited 0 but logged ERROR(s) for VM $vm_name: $error_lines"
          set_backup_error "VIRTNBD_FALSE_SUCCESS" \
              "virtnbdbackup exited 0 but logged errors" "$error_lines"
          _BACKUP_IN_PROGRESS="false"
          return 1
      fi
      
      log_info "vmbackup.sh" "perform_backup" "$backup_type backup successful for VM: $vm_name at $backup_end_time"
      
      # Verify backup directory contents
      local file_count=$(find "$backup_dir" -maxdepth 1 -type f 2>/dev/null | wc -l)
      local total_size=$(du -sh "$backup_dir" 2>/dev/null | awk '{print $1}')
      log_info "vmbackup.sh" "perform_backup" "Backup verification: $file_count files, $total_size total size"
      
      # CRITICAL: PRESERVE checkpoint marker files (.cpt) after successful backup
      # These marker files are REQUIRED by virtnbdbackup to locate parent checkpoints
      # for incremental backups. Deleting them causes next incremental to fail with
      # "No existing checkpoints found" error.
      # 
      # The validate_backup_preconditions() function already detects and handles
      # orphaned metadata correctly via checkpoint state detection, so the .cpt files
      # are safe to preserve and necessary for proper incremental backup chaining.
      log_debug "vmbackup.sh" "perform_backup" "Preserving checkpoint marker files for VM: $vm_name (required for incremental backups)"
      
      # Safety net: ensure backup files created by virtnbdbackup (external tool)
      # are owned by backup group, in case it overrides SGID behaviour
      set_backup_permissions "$backup_dir" --recursive
      
      # R2: if earlier attempts freeze-stalled but this one succeeded, the copy
      # IS quiesced (virtnbdbackup always freezes; a clean run means the freeze
      # worked this time). Surface the flaky guest agent without downgrading the
      # backup or its reported consistency.
      if [[ "$_freeze_stall_seen" == true ]]; then
        log_warn "vmbackup.sh" "perform_backup" \
          "Backup of $vm_name SUCCEEDED but survived $_freeze_stall_count earlier filesystem-freeze stall(s). The copy IS quiesced (the freeze succeeded on the final attempt); investigate qemu-guest-agent in the guest, which is intermittently slow to freeze."
      fi
      
      return 0
    else
      local exit_code=$?
      _BACKUP_IN_PROGRESS="false"
      local backup_end_time=$(date '+%Y-%m-%d %H:%M:%S')
      log_error "vmbackup.sh" "perform_backup" "$backup_type backup failed for VM: $vm_name (exit code: $exit_code) at $backup_end_time"
      log_error "vmbackup.sh" "perform_backup" "Backup directory state: $(ls -lh "$backup_dir" 2>/dev/null | head -10 || echo 'Directory not accessible')"
      
      # Kill monitor and its children (must happen on failure path too)
      pkill -P $monitor_pid 2>/dev/null || true
      kill $monitor_pid 2>/dev/null || true
      wait $monitor_pid 2>/dev/null || true
      
      # R2: did the monitor kill THIS attempt for a filesystem-freeze stall?
      # (The wait above discarded the monitor's return code, so read the
      # sentinel it leaves behind.)
      local _was_freeze_stall=false
      if [[ -f "$_stall_sentinel" ]]; then
        _was_freeze_stall=true
        _freeze_stall_seen=true
        ((_freeze_stall_count++))
        rm -f "$_stall_sentinel" 2>/dev/null || true
        log_error "vmbackup.sh" "perform_backup" \
          "Backup of $vm_name killed after a filesystem-freeze stall (no I/O for ${BACKUP_STALL_THRESHOLD}s) — freeze stall #${_freeze_stall_count} of max ${BACKUP_FSFREEZE_STALL_LIMIT}"
      fi
      
      # Capture virtnbdbackup log tail for error reporting
      local log_tail=""
      if [[ -f "$backup_log" ]]; then
        log_tail=$(tail -5 "$backup_log" 2>/dev/null | tr '\n' ' ' | cut -c1-200)
      fi
      
      # Set specific error code. R2: a freeze stall gets its own honest code so
      # the summary explains the cause instead of a generic engine exit status.
      if [[ "$_was_freeze_stall" == true ]]; then
        set_backup_error "FSFREEZE_STALL" \
          "Guest agent did not respond to filesystem freeze within ${BACKUP_STALL_THRESHOLD}s; backup was killed to avoid an indefinite hang" \
          "$log_tail"
      else
        set_backup_error "VIRTNBD_EXIT_${exit_code}" "virtnbdbackup failed with exit code $exit_code" "$log_tail"
      fi
      
      # ═══════════════════════════════════════════════════════════════════════════
      # RETRY SELF-HEALING: Archive valid chain, re-validate, and cleanup
      # Before retrying, preserve any valid backup data and get fresh state
      # ═══════════════════════════════════════════════════════════════════════════
      
      # R2: fail closed once freeze stalls keep recurring. A retry always
      # re-freezes (the engine has no opt-out), so repeated freeze stalls mean
      # the guest agent is unhealthy and more attempts just hang again. The
      # single-stall-then-recover case is preserved: we only break AT the limit.
      if [[ "$_was_freeze_stall" == true ]] && (( _freeze_stall_count >= BACKUP_FSFREEZE_STALL_LIMIT )); then
        log_error "vmbackup.sh" "perform_backup" \
          "Filesystem-freeze stalls reached the limit (${_freeze_stall_count}/${BACKUP_FSFREEZE_STALL_LIMIT}) for VM $vm_name — refusing to retry. The guest agent is not responding to freeze; investigate qemu-guest-agent inside the guest."
        # Reap any lingering virtnbdbackup and abort the libvirt job before bailing.
        if pgrep -f "$_vnb_reap_pat" &>/dev/null; then           # FF-150
            pkill -f "$_vnb_reap_pat" 2>/dev/null || true
            sleep 2
            pgrep -f "$_vnb_reap_pat" &>/dev/null && \
                pkill -9 -f "$_vnb_reap_pat" 2>/dev/null || true
        fi
        lv_domjobabort "$vm_name"
        break
      fi
      
      # SMART RETRY STRATEGY: If AUTO failed, try FULL instead of retrying AUTO
      if [[ "$backup_type" == "auto" && "$CHECKPOINT_RETRY_AUTO_TO_FULL" == "yes" && $attempt -le $CHECKPOINT_MAX_RETRIES_AUTO ]]; then
        ((attempt++))
        log_warn "vmbackup.sh" "perform_backup" \
          "AUTO backup failed, converting to FULL backup (attempt $attempt of $((MAX_RETRIES + 1)))"
        
        # Ensure virtnbdbackup and children are fully terminated
        if pgrep -f "$_vnb_reap_pat" &>/dev/null; then           # FF-150
            log_warn "vmbackup.sh" "perform_backup" "virtnbdbackup still running for $vm_name — killing"
            pkill -f "$_vnb_reap_pat" 2>/dev/null || true
            sleep 2
            # Force kill if still alive
            pgrep -f "$_vnb_reap_pat" &>/dev/null && \
                pkill -9 -f "$_vnb_reap_pat" 2>/dev/null || true
        fi
        # Abort any lingering libvirt backup job
        lv_domjobabort "$vm_name"
        
        # STEP 1: Archive existing valid backup chain before cleanup destroys it
        # Check if there's restorable backup data (*.full.data or *.copy.data present)
        local has_valid_chain=false
        if find "$backup_dir" -maxdepth 1 \( -name "*.full.data" -o -name "*.copy.data" \) -print -quit 2>/dev/null | grep -q .; then
          has_valid_chain=true
        fi
        
        if [[ "$has_valid_chain" == "true" ]]; then
          log_info "vmbackup.sh" "perform_backup" "[Retry Self-Healing] Valid backup chain detected - archiving before cleanup"
          if archive_existing_checkpoint_chain "$vm_name" "$backup_dir"; then
            log_info "vmbackup.sh" "perform_backup" "[Retry Self-Healing] Previous chain archived successfully"
          else
            log_warn "vmbackup.sh" "perform_backup" "[Retry Self-Healing] Chain archival failed - proceeding with cleanup anyway"
          fi
        else
          log_debug "vmbackup.sh" "perform_backup" "[Retry Self-Healing] No valid backup chain to archive"
        fi
        
        # STEP 2: Re-validate state (may have changed during failed backup)
        log_info "vmbackup.sh" "perform_backup" "[Retry Self-Healing] Re-validating backup state after failed attempt"
        validate_backup_state "$vm_name" "$backup_dir"
        validation_state=$(get_cached_validation_state)
        log_info "vmbackup.sh" "perform_backup" "[Retry Self-Healing] Fresh validation state: $validation_state"
        
        # STEP 3: Re-run cleanup based on new state
        if ! prepare_backup_directory "$vm_name" "$backup_dir" "full" "$validation_state"; then
          log_error "vmbackup.sh" "perform_backup" "[Retry Self-Healing] Failed to prepare directory for retry"
          # Don't return - attempt the backup anyway
        fi
        
        backup_type="full"
        sleep "$RETRY_DELAY"
        continue
      fi
      
      # Normal retry logic - increment attempt and check if we should retry
      ((attempt++))
      if (( attempt <= MAX_RETRIES + 1 )); then
        log_warn "vmbackup.sh" "perform_backup" \
          "Retrying $backup_type backup in $RETRY_DELAY seconds (attempt $attempt of $((MAX_RETRIES + 1)))"
        
        # Ensure virtnbdbackup and children are fully terminated
        if pgrep -f "$_vnb_reap_pat" &>/dev/null; then           # FF-150
            log_warn "vmbackup.sh" "perform_backup" "virtnbdbackup still running for $vm_name — killing"
            pkill -f "$_vnb_reap_pat" 2>/dev/null || true
            sleep 2
            # Force kill if still alive
            pgrep -f "$_vnb_reap_pat" &>/dev/null && \
                pkill -9 -f "$_vnb_reap_pat" 2>/dev/null || true
        fi
        # Abort any lingering libvirt backup job
        lv_domjobabort "$vm_name"
        
        # STEP 1: Archive existing valid backup chain before cleanup destroys it
        local has_valid_chain=false
        if find "$backup_dir" -maxdepth 1 \( -name "*.full.data" -o -name "*.copy.data" \) -print -quit 2>/dev/null | grep -q .; then
          has_valid_chain=true
        fi
        
        if [[ "$has_valid_chain" == "true" ]]; then
          log_info "vmbackup.sh" "perform_backup" "[Retry Self-Healing] Valid backup chain detected - archiving before cleanup"
          if archive_existing_checkpoint_chain "$vm_name" "$backup_dir"; then
            log_info "vmbackup.sh" "perform_backup" "[Retry Self-Healing] Previous chain archived successfully"
          else
            log_warn "vmbackup.sh" "perform_backup" "[Retry Self-Healing] Chain archival failed - proceeding with cleanup anyway"
          fi
        else
          log_debug "vmbackup.sh" "perform_backup" "[Retry Self-Healing] No valid backup chain to archive"
        fi
        
        # STEP 2: Re-validate state (may have changed during failed backup)
        log_info "vmbackup.sh" "perform_backup" "[Retry Self-Healing] Re-validating backup state after failed attempt"
        validate_backup_state "$vm_name" "$backup_dir"
        validation_state=$(get_cached_validation_state)
        log_info "vmbackup.sh" "perform_backup" "[Retry Self-Healing] Fresh validation state: $validation_state"
        
        # STEP 3: Re-run cleanup based on new state
        if ! prepare_backup_directory "$vm_name" "$backup_dir" "$backup_type" "$validation_state"; then
          log_error "vmbackup.sh" "perform_backup" "[Retry Self-Healing] Failed to prepare directory for retry"
          # Don't return - attempt the backup anyway
        fi
        
        # STEP 4: Re-determine backup type based on fresh state
        local new_backup_type=$(determine_backup_level "$vm_name" "$requested_backup_type")
        
        # Force FULL if state is compromised
        if [[ "$validation_state" != "clean" ]]; then
          log_warn "vmbackup.sh" "perform_backup" "[Retry Self-Healing] State compromised ($validation_state) - forcing FULL backup"
          new_backup_type="full"
        fi
        
        if [[ "$new_backup_type" != "$backup_type" ]]; then
          log_info "vmbackup.sh" "perform_backup" "[Retry Self-Healing] Backup type changed: $backup_type → $new_backup_type"
          backup_type="$new_backup_type"
        fi
        
        sleep "$RETRY_DELAY"
      else
        # Out of retries, break out of loop
        break
      fi
    fi
  done
  
  log_error "vmbackup.sh" "perform_backup" "All backup attempts failed for VM: $vm_name"
  return 1
}

# Verify backup files
verify_backup() {
  local vm_name=$1
  local backup_dir=$2
  
  log_info "vmbackup.sh" "verify_backup" "Verifying backup for VM: $vm_name in directory: $backup_dir"
  
  if [[ ! -d "$backup_dir" ]]; then
    log_error "vmbackup.sh" "verify_backup" "Backup directory not found: $backup_dir"
    return 1
  fi
  
  # Count all backup files (active only, exclude .archives subdirectory for archived chains)
  local file_count=$(find "$backup_dir" -maxdepth 1 -type f ! -name ".full-backup-done" ! -name "*.sha256" 2>/dev/null | wc -l)
  local total_size=$(du -sh "$backup_dir" --exclude=.archives 2>/dev/null | awk '{print $1}')
  
  # FF-151: the fail-closed "empty backup" backstop must key on real restorable
  # payload (*.data), not the raw file count. virtnbdbackup can exit 0 having
  # written nothing, leaving only non-payload files (.full-backup-month marker,
  # .agent-status, backup.*.log). The old count excluded '.full-backup-done' — a
  # marker this tool never writes — so those files kept file_count > 0 and the
  # backstop passed vacuously. *.data is the canonical payload extension used
  # everywhere else in this file.
  local data_count
  data_count=$(find "$backup_dir" -maxdepth 1 -type f -name "*.data" 2>/dev/null | wc -l)
  if (( data_count == 0 )); then
    log_error "vmbackup.sh" "verify_backup" "No backup data files (*.data) found in $backup_dir"
    log_error "vmbackup.sh" "verify_backup" "Directory contents: $(ls -la "$backup_dir" 2>/dev/null | tail -20)"
    return 1
  fi
  
  log_info "vmbackup.sh" "verify_backup" "Backup files verified: $file_count files, total size: $total_size"
  
  # List file types found - nicely formatted for readability (active only)
  local file_types=$(find "$backup_dir" -maxdepth 1 -type f ! -name ".full-backup-done" 2>/dev/null | xargs -I {} basename {} | cut -d. -f2- | sort | uniq -c)
  if [[ -n "$file_types" ]]; then
    log_info "vmbackup.sh" "verify_backup" "File breakdown:"
    while IFS= read -r line; do
      log_info "vmbackup.sh" "verify_backup" "  $line"
    done <<< "$file_types"
  fi
  
  # Verify no missing segments (check for gaps in numbered files if present).
  # Sort for deterministic pick if multiple manifest-named files exist
  # (109-bugs audit item 5; future audit may tighten the glob to an exact
  # match — current behaviour preserved, only determinism hardened).
  local manifest_file=$(find "$backup_dir" -type f -name "*manifest*" 2>/dev/null | sort | head -1)
  if [[ -n "$manifest_file" ]]; then
    log_info "vmbackup.sh" "verify_backup" "Manifest found: $manifest_file"
  fi
  
  return 0
}

# Archive compress consolidated backup (at month-end only)

# Checksum backup
# TODO: ENHANCEMENT - Per-file checksums for backup integrity
#   Current: Single checksum file covers all files at one point in time
#   Limitation: Incremental backups added after initial full backup are not checksummed
#   Proposed: Create individual checksum for each backup file:
#     - sda.full.data → sda.full.data.sha256
#     - sda.inc.virtnbdbackup.0.data → sda.inc.virtnbdbackup.0.data.sha256
#     - sda.inc.virtnbdbackup.1.data → sda.inc.virtnbdbackup.1.data.sha256
#   Benefits:
#     1. Each file independently verified (full + all incrementals)
#     2. Can restore individual files and verify integrity
#     3. Checksums remain valid across multiple incremental backups
#     4. Detect corruption in any specific backup file
#   Implementation notes:
#     - Create checksum immediately after each backup file generated
#     - Update verify_backup_images() to handle per-file checksums
#     - Ensure restore process validates each file against its checksum
#     - Maintain backward compatibility with single-file checksums
#     - Handle cleanup: delete .sha256 files when deleting .data files
#     - Document: restore process must verify all checksums before combining backups

# Create individual checksums for each backup data file (per-file format)

#################################################################################
# FSTRIM OPTIMIZATION FUNCTIONS
#################################################################################

# Cache FSTRIM module availability after loading
cache_fstrim_availability() {
  if declare -f execute_fstrim_in_guest >/dev/null 2>&1; then
    FSTRIM_IMPL_AVAILABLE=1
    log_debug "vmbackup.sh" "cache_fstrim_availability" "FSTRIM module implementation available"
  else
    FSTRIM_IMPL_AVAILABLE=0
    log_debug "vmbackup.sh" "cache_fstrim_availability" "FSTRIM module implementation not available"
  fi
}

# Check VirtIO discard_granularity for Windows VMs.
# Parses virsh dumpxml to find VirtIO disks with discard='unmap' that are
# missing the qemu:override discard_granularity property. Emits a clear
# warning with the exact XML fix needed for each missing disk.
#
# Args: $1=vm_name  $2=os_type
# Returns: 0 always (advisory only — does not block backup)
# Only runs once per VM per session (cached in associative array).
declare -A _DISCARD_GRANULARITY_CHECKED 2>/dev/null || true

check_discard_granularity() {
  local vm_name=$1
  local os_type=${2:-unknown}

  # Only relevant for Windows VMs
  [[ "$os_type" != "windows" ]] && return 0

  # Only check once per VM per session
  [[ -n "${_DISCARD_GRANULARITY_CHECKED[$vm_name]+x}" ]] && return 0
  _DISCARD_GRANULARITY_CHECKED[$vm_name]=1

  local xml_dump
  xml_dump=$(lv_dump_xml "$vm_name") || {
    log_debug "vmbackup.sh" "check_discard_granularity" \
      "Could not dump XML for $vm_name — skipping discard_granularity check"
    return 0
  }

  # Find all VirtIO disk aliases with discard='unmap'
  # Strategy: read disk blocks, track discard+virtio+alias across lines
  # Uses POSIX-compatible awk (no gawk match() with capture groups)
  local virtio_aliases
  virtio_aliases=$(echo "$xml_dump" | awk '
    /<disk / { in_disk=1; has_discard=0; is_virtio=0; alias="" }
    in_disk && /discard=.unmap/ { has_discard=1 }
    in_disk && /bus=.virtio/ { is_virtio=1 }
    in_disk && /alias name=/ {
      s=$0; sub(/.*alias name=./, "", s); sub(/[^a-zA-Z0-9_-].*/, "", s)
      alias=s
    }
    /<\/disk>/ {
      if (has_discard && is_virtio && alias != "") print alias
      in_disk=0
    }
  ')

  [[ -z "$virtio_aliases" ]] && return 0

  # Find which aliases have the qemu:override discard_granularity set
  local override_aliases
  override_aliases=$(echo "$xml_dump" | awk '
    /qemu:device alias=/ {
      s=$0; sub(/.*alias=./, "", s); sub(/[^a-zA-Z0-9_-].*/, "", s)
      current_alias=s
    }
    /discard_granularity/ && current_alias != "" {
      print current_alias
      current_alias=""
    }
  ')

  # Compare: find VirtIO disks missing the override
  local missing_aliases=()
  local alias
  while IFS= read -r alias; do
    [[ -z "$alias" ]] && continue
    if ! echo "$override_aliases" | grep -qxF "$alias"; then
      missing_aliases+=("$alias")
    fi
  done <<< "$virtio_aliases"

  if [[ ${#missing_aliases[@]} -gt 0 ]]; then
    log_warn "vmbackup.sh" "check_discard_granularity" \
      "WARNING: Windows VM '$vm_name' has VirtIO disks without discard_granularity"
    log_warn "vmbackup.sh" "check_discard_granularity" \
      "FSTRIM will be extremely slow (~3-15 minutes) on unfixed disks."
    log_warn "vmbackup.sh" "check_discard_granularity" \
      "With the fix applied, FSTRIM completes in ~1-3 seconds."

    for alias in "${missing_aliases[@]}"; do
      log_warn "vmbackup.sh" "check_discard_granularity" \
        "MISSING discard_granularity on: $alias"
      log_warn "vmbackup.sh" "check_discard_granularity" \
        "  Fix: Add to VM XML inside <qemu:override>:"
      log_warn "vmbackup.sh" "check_discard_granularity" \
        "    <qemu:device alias='"'"'$alias'"'"'>"
      log_warn "vmbackup.sh" "check_discard_granularity" \
        "      <qemu:frontend>"
      log_warn "vmbackup.sh" "check_discard_granularity" \
        "        <qemu:property name='"'"'discard_granularity'"'"' type='"'"'unsigned'"'"' value='"'"'33554432'"'"'/>"
      log_warn "vmbackup.sh" "check_discard_granularity" \
        "      </qemu:frontend>"
      log_warn "vmbackup.sh" "check_discard_granularity" \
        "    </qemu:device>"
    done

    log_warn "vmbackup.sh" "check_discard_granularity" \
      "See vmbackup.md: VirtIO discard_granularity & Windows TRIM Performance"
  else
    log_debug "vmbackup.sh" "check_discard_granularity" \
      "All VirtIO disks for $vm_name have discard_granularity set"
  fi

  return 0
}



#################################################################################
# OFFLINE VM OPTIMIZATION FUNCTIONS
#################################################################################

# Get timestamp of last successful backup for a VM
# Searches the current period dir first, then falls back to scanning ALL period
# dirs under the VM's backup path. This handles daily/weekly rotation where the
# current period dir is brand new and empty (offline VM skip detection).
# Returns: Unix timestamp of last backup, or 0 if no backup exists
get_last_backup_timestamp() {
  local vm_name=$1
  # R3: route through the single strict validator (was an inline sed slug).
  local safe_name
  safe_name=$(vm_fs_name "$vm_name") || return $?
  local vm_dir="${BACKUP_PATH}${safe_name}"
  
  # Use VM-first structure via integration module
  local backup_dir
  if declare -f get_backup_dir >/dev/null 2>&1; then
    backup_dir=$(get_backup_dir "$vm_name")
  else
    # Fallback if integration module not loaded yet
    local current_month=$(get_current_month)
    backup_dir="$vm_dir/$current_month"
  fi
  
  # Try current period dir first (fast path)
  # NOTE: -maxdepth 3 is required so this can see archived chains at
  #   <period>/.archives/chain-YYYY-MM-DD[.N]/*.{full,inc,copy}.data
  # Post-archival the live period dir contains no data files directly — the
  # most-recent backup lives inside .archives/. With -maxdepth 1 we'd return
  # ts=0 here, fall through to a forced full. Do NOT narrow without also
  # changing the archive layout.
  if [[ -d "$backup_dir" ]]; then
    local newest_backup_file
    newest_backup_file=$(find "$backup_dir" -maxdepth 3 -type f \( -name "*.full.data" -o -name "*.inc.virtnbdbackup.*.data" -o -name "*.copy.data" \) -printf '%T@ %p\n' 2>/dev/null | sort -rn | head -1 | cut -d' ' -f2-)
    
    if [[ -n "$newest_backup_file" && -f "$newest_backup_file" ]]; then
      local ts
      ts=$(stat -c %Y "$newest_backup_file" 2>/dev/null || echo "0")
      log_debug "vmbackup.sh" "get_last_backup_timestamp" "VM '$vm_name': found in current period dir, newest='$(basename "$newest_backup_file")' timestamp=$ts"
      echo "$ts"
      return
    fi
    log_debug "vmbackup.sh" "get_last_backup_timestamp" "VM '$vm_name': current period dir '$backup_dir' has no data files, scanning all periods"
  fi
  
  # Fallback: scan ALL period dirs under the VM's backup path
  # This covers daily/weekly rotation where the current period is a new empty dir
  # NOTE: -maxdepth 4 mirrors the depth-3 reasoning above, with one extra level
  #   for the period-dir hop:  <vm>/<period>/.archives/chain-*/<file>.data
  if [[ -d "$vm_dir" ]]; then
    local newest_backup_file
    newest_backup_file=$(find "$vm_dir" -maxdepth 4 -type f \( -name "*.full.data" -o -name "*.inc.virtnbdbackup.*.data" -o -name "*.copy.data" \) -printf '%T@ %p\n' 2>/dev/null | sort -rn | head -1 | cut -d' ' -f2-)
    
    if [[ -n "$newest_backup_file" && -f "$newest_backup_file" ]]; then
      local ts
      ts=$(stat -c %Y "$newest_backup_file" 2>/dev/null || echo "0")
      log_debug "vmbackup.sh" "get_last_backup_timestamp" "VM '$vm_name': found in previous period, newest='$(basename "$newest_backup_file")' timestamp=$ts"
      echo "$ts"
      return
    fi
  fi
  
  log_debug "vmbackup.sh" "get_last_backup_timestamp" "VM '$vm_name': no backup data found anywhere → timestamp=0"
  echo "0"
}

# Check if offline VM disks have changed since last backup
has_offline_vm_changed() {
  local vm_name=$1
  local last_backup_time=$(get_last_backup_timestamp "$vm_name")
  
  log_info "vmbackup.sh" "has_offline_vm_changed" "Checking for disk changes on offline VM: $vm_name"
  
  # If no backup exists yet, consider it as changed (needs full backup)
  if [[ "$last_backup_time" == "0" || -z "$last_backup_time" ]]; then
    log_info "vmbackup.sh" "has_offline_vm_changed" "No previous backup found for $vm_name - will require full backup"
    return 0  # Changed (needs backup)
  fi
  
  # Get current timestamp
  local current_time=$(date +%s)
  local time_since_backup=$((current_time - last_backup_time))
  
  log_info "vmbackup.sh" "has_offline_vm_changed" "Last backup for $vm_name was $time_since_backup seconds ago"
  
  # Check if any VM disk files have been modified since last backup
  # Use virsh to get VM disk paths
  local disk_changed=0
  local disks_examined=0   # FF-152: count disks actually stat-compared
  
  while IFS= read -r disk_path; do
    [[ -z "$disk_path" ]] && continue
    
    if [[ ! -e "$disk_path" ]]; then
      continue
    fi
    
    # Get disk modification time
    local disk_mtime=$(stat -c %Y "$disk_path" 2>/dev/null)
    
    if [[ -z "$disk_mtime" ]]; then
      log_warn "vmbackup.sh" "has_offline_vm_changed" "Could not stat disk: $disk_path"
      continue
    fi
    ((disks_examined++))   # FF-152: a disk was actually verified
    
    # Compare disk modification time to last backup time
    if (( disk_mtime > last_backup_time )); then
      log_info "vmbackup.sh" "has_offline_vm_changed" "Disk changed detected: $disk_path (mtime: $disk_mtime > backup: $last_backup_time)"
      disk_changed=1
      break
    else
      log_info "vmbackup.sh" "has_offline_vm_changed" "Disk unchanged: $disk_path (mtime: $disk_mtime <= backup: $last_backup_time)"
    fi
  done < <(lv_list_disk_paths "$vm_name")
  
  if (( disk_changed == 1 )); then
    log_info "vmbackup.sh" "has_offline_vm_changed" "Offline VM $vm_name has changed - backup required"
    return 0  # Changed
  elif (( disks_examined == 0 )); then
    # FF-152: fail closed. If lv_list_disk_paths yielded nothing (transient
    # virsh empty-enumeration on this contended host) or every listed disk was
    # missing/unstat-able, NOTHING was verified — treat as changed so the
    # offline backup is not silently skipped on an unverified VM.
    log_warn "vmbackup.sh" "has_offline_vm_changed" "Offline VM $vm_name: no disks could be examined (enumeration/stat failure) - treating as changed (fail-closed)"
    return 0  # Changed (fail-closed)
  else
    log_info "vmbackup.sh" "has_offline_vm_changed" "Offline VM $vm_name disks unchanged - backup can be skipped"
    return 1  # Not changed
  fi
}

#################################################################################
# SQLITE LOGGING MODULE LOADING
#################################################################################

# Load SQLite logging module (provides structured database logging)
load_sqlite_logging_module() {
  local script_dir="${SCRIPT_DIR:-$(dirname "$(readlink -f "$0")")}"
  local sqlite_module="$script_dir/lib/sqlite_module.sh"
  
  if [[ ! -f "$sqlite_module" ]]; then
    log_debug "vmbackup.sh" "load_sqlite_logging_module" "SQLite module not found at: $sqlite_module"
    return 1
  fi
  
  if ! source "$sqlite_module" 2>/dev/null; then
    log_warn "vmbackup.sh" "load_sqlite_logging_module" "Failed to load SQLite module (syntax error?)"
    return 1
  fi
  
  # Initialize database
  if ! sqlite_init_database; then
    log_warn "vmbackup.sh" "load_sqlite_logging_module" "SQLite database initialization failed"
    return 1
  fi
  
  # Export VMBACKUP_DB for modules that reference it (retention_module, email_report_module)
  # SQLITE_DB_PATH is set by sqlite_init_database in lib/sqlite_module.sh
  VMBACKUP_DB="$SQLITE_DB_PATH"
  
  log_info "vmbackup.sh" "load_sqlite_logging_module" "SQLite logging module loaded successfully"
  return 0
}

# Load chain validation module (provides chain integrity checking)
load_chain_validation_module() {
  local script_dir="${SCRIPT_DIR:-$(dirname "$(readlink -f "$0")")}"
  local validation_module="$script_dir/lib/chain_validation.sh"
  
  if [[ ! -f "$validation_module" ]]; then
    log_debug "vmbackup.sh" "load_chain_validation_module" "Chain validation module not found at: $validation_module"
    return 1
  fi
  
  if ! source "$validation_module" 2>/dev/null; then
    log_warn "vmbackup.sh" "load_chain_validation_module" "Failed to load chain validation module"
    return 1
  fi
  
  log_debug "vmbackup.sh" "load_chain_validation_module" "Chain validation module loaded"
  return 0
}

#################################################################################
# MAIN BACKUP PROCESS
#################################################################################
#
# KNOWN ISSUES MITIGATION:
# This section implements workarounds for known virtnbdbackup issues:
#
# 1. CHECKPOINT BITMAP CORRUPTION (GitHub #267, #226)
#    - Occurs randomly (5-10% of backups) when:
#      a) VM powered off without qemu bitmap flush
#      b) VM migrated between hosts
#      c) Third-party tools modify disk images
#      d) Destination fills up mid-backup
#    - MITIGATION: Enhanced disk space checks (20% threshold), monthly rotation
#
# 2. CHECKPOINT DELETION FAILURES (GitHub #223)
#    - Error: "bitmap not found in backing chain"
#    - MITIGATION: Disabled auto-checkpoint deletion (too risky)
#
# 3. FSFREEZE TIMEOUT (GitHub #102)
#    - Backup hangs on some guest OSes (CloudLinux, cPanel, NetBSD)
#    - MITIGATION: Added timeout guard in perform_backup()
#
# 4. INCREMENTAL SIZE ANOMALIES (GitHub #244, #139)
#    - Full backup created instead of incremental after sparseness loss
#    - MITIGATION: Monthly directory rotation provides natural full backup reset
#
# See VIRTNBDBACKUP_KNOWN_ISSUES.md for full documentation
#

#################################################################################
# OFFLINE VM BACKUP ARCHIVAL FUNCTIONS
# PURPOSE: Preserve multiple full backups for offline VMs without data loss
# SCENARIO: Offline VM with disk changes creates new full backup, archiving previous
# STRATEGY: Archive complete checkpoint chain to .archives/chain-DATE/, create fresh baseline
#################################################################################

# Archive existing checkpoint chain before new full backup
# Global tracking variables for chain archival (set by archive_existing_checkpoint_chain)
# These are read by backup_vm() for session logging after archiving occurs in any code path
_ARCHIVE_CHAIN_ARCHIVED="false"      # Was a chain archived this backup run?
_ARCHIVE_RESTORE_POINTS=0            # How many restore points were in the archived chain?
_ARCHIVE_PATH=""                     # Path to the archived chain

# INT-15 guard: per-(session, vm) idempotency for chain archival.
# Multiple code paths in prepare_backup_directory/perform_backup/backup_vm may all detect
# "valid chain present" and call archive_existing_checkpoint_chain. After the FIRST archive,
# subsequent calls would detect the FRESH post-archive backup data as a "chain to archive"
# and move the just-written backup into a chain-DATE.1 sibling, destroying the live chain.
# This map records VMs already archived in the current process (= one vmbackup session).
declare -gA _ARCHIVED_VMS_THIS_SESSION=()

# Backup operation tracking (set by perform_backup, read by _log_interrupted_chain)
# Prevents marking chains as broken when interrupted during pre-backup phases (fstrim, validation, etc.)
_BACKUP_IN_PROGRESS="false"          # Is perform_backup actively running virtnbdbackup?

# Policy change tracking (set by detect_policy_change, read by logging functions)
_POLICY_CHANGE_DETECTED="false"      # Was a policy change detected?
_POLICY_CHANGE_PREVIOUS=""           # Previous rotation policy
_POLICY_CHANGE_CURRENT=""            # Current rotation policy
_POLICY_CHANGE_ARCHIVE_PATH=""       # Where the old chain was archived

# PURPOSE: Preserve complete checkpoint chain (full + incrementals) to dated subdirectory
# DESIGN: Moves virtnbdbackup.*.xml and *.data files to .archives/chain-YYYY-MM-DD/
# PARAMETERS: $1 = vm_name, $2 = backup_dir
# SIDE EFFECTS: Sets global _ARCHIVE_CHAIN_ARCHIVED and _ARCHIVE_RESTORE_POINTS

# Reset policy change tracking (call at start of each VM backup)
reset_policy_change_tracking() {
  _POLICY_CHANGE_DETECTED="false"
  _POLICY_CHANGE_PREVIOUS=""
  _POLICY_CHANGE_CURRENT=""
  _POLICY_CHANGE_ARCHIVE_PATH=""
}

# Detect if rotation policy changed since last successful backup
# Uses SQLite to query previous policy
# Sets: _POLICY_CHANGE_DETECTED, _POLICY_CHANGE_PREVIOUS, _POLICY_CHANGE_CURRENT
# Parameters: $1 = vm_name, $2 = current_policy
# Returns: 0 if policy changed, 1 if no change or unable to detect
detect_policy_change() {
  local vm_name="$1"
  local current_policy="$2"
  
  # Query previous policy from SQLite
  local previous_policy=""
  if sqlite_is_available 2>/dev/null; then
    previous_policy=$(sqlite_get_last_rotation_policy "$vm_name" 2>/dev/null)
  fi
  
  # No history = no change detection possible
  if [[ -z "$previous_policy" ]]; then
    log_debug "vmbackup.sh" "detect_policy_change" \
      "No policy history for $vm_name - cannot detect change"
    return 1
  fi
  
  # Compare policies
  if [[ "$previous_policy" != "$current_policy" ]]; then
    _POLICY_CHANGE_DETECTED="true"
    _POLICY_CHANGE_PREVIOUS="$previous_policy"
    _POLICY_CHANGE_CURRENT="$current_policy"
    log_info "vmbackup.sh" "detect_policy_change" \
      "Policy change detected for $vm_name: $previous_policy → $current_policy"
    
    # Log config event to SQLite
    if declare -f log_config_event &>/dev/null; then
      log_config_event "policy_change" "" "$vm_name" "rotation_policy" \
        "$current_policy" "$previous_policy" "$vm_name" "detect_policy_change" \
        "Chain will be archived and new FULL backup started"
    fi
    return 0
  fi
  
  log_debug "vmbackup.sh" "detect_policy_change" \
    "No policy change for $vm_name (policy=$current_policy)"
  return 1
}

# B4/CLEAN-01: output channels for the shared chain-archival mover, read by the
# caller immediately after _archive_chain_files returns.
declare -gA _ARCHIVE_LAST_COUNTS=()
declare -g  _ARCHIVE_LAST_TOTAL=0
declare -g  _ARCHIVE_LAST_PATH=""

# _archive_chain_files <vm_name> <period_dir>
# Shared canonical chain-archival mover. Relocates a period's chain artefacts into
# <period_dir>/.archives/chain-<YYYY-MM-DD>[.N]/ -- the ONLY archived-chain layout
# vmrestore and orphan-retention recognise. Side-effect-free beyond the moves and
# the two output channels above: NO session globals, NO SQLite, NO INT-15 (each
# caller owns its own bookkeeping). Used by BOTH archive_existing_checkpoint_chain
# (in-backup path) and archive_active_chains (retention period-boundary path) so the
# two can never drift apart again (B4/CLEAN-01 Q1).
# Output: stdout = chain-archive path (empty if nothing moved); _ARCHIVE_LAST_TOTAL
#         = total artefacts moved/copied; _ARCHIVE_LAST_COUNTS = per-class counts.
# Returns: 0 = >=1 artefact archived; 1 = mkdir failure; 2 = nothing to move (no
#          directory is left behind -- the mkdir-gate, CLEAN-01 Fault 2/3).
_archive_chain_files() {
  local vm_name="$1" period_dir="$2"
  _ARCHIVE_LAST_COUNTS=()
  _ARCHIVE_LAST_TOTAL=0

  local archive_base="${period_dir}/.archives"
  local archive_date; archive_date=$(date +%Y-%m-%d)
  local chain_archive="${archive_base}/chain-${archive_date}"
  local _n=0
  while [[ -d "$chain_archive" ]]; do
    ((_n++))
    chain_archive="${archive_base}/chain-${archive_date}.${_n}"
  done

  mkdir -p "$chain_archive" || {
    log_error "vmbackup.sh" "_archive_chain_files" "Failed to create archive directory: $chain_archive"
    return 1
  }

  local _moved=0

  # Move one find-matched file class into the archive; record count under <key>.
  _acf_move() {
    local key="$1"; shift
    local n=0 file
    while IFS= read -r file; do
      [[ -z "$file" ]] && continue
      if mv "$file" "$chain_archive/"; then ((n++)); ((_moved++)); fi
    done < <(find "$period_dir" -maxdepth 1 "$@" 2>/dev/null)
    _ARCHIVE_LAST_COUNTS[$key]=$n
  }
  _acf_move metadata  -type f -name "virtnbdbackup.*.xml"
  _acf_move vmconfig  -type f -name "vmconfig.virtnbdbackup.*.xml"
  _acf_move full      -type f \( -name "*.full.data" -o -name "*.copy.data" \)
  _acf_move inc       -type f -name "*.inc.virtnbdbackup.*.data"
  _acf_move cpt       -type f -name "*.cpt"
  _acf_move chksum    -type f -name "*.data.chksum"
  _acf_move qcow_json -type f -name "*.qcow.json"
  _acf_move nvram     -type f \( -name "*_VARS.fd" -o -name "*_VARS.fd.virtnbdbackup.*" -o -name "OVMF_CODE*.fd" -o -name "OVMF_CODE*.fd.virtnbdbackup.*" \)
  unset -f _acf_move

  # Directory classes moved whole.
  local _d
  for _d in checkpoints tpm-state config; do
    if [[ -d "$period_dir/$_d" ]]; then
      if mv "$period_dir/$_d" "$chain_archive/"; then _ARCHIVE_LAST_COUNTS[$_d]=1; ((_moved++)); else _ARCHIVE_LAST_COUNTS[$_d]=0; fi
    fi
  done

  # .tpm-backup-marker is COPIED, not moved: the marker must remain in the period
  # so the next chain's first backup detects TPM capability.
  if [[ -f "$period_dir/.tpm-backup-marker" ]]; then
    if cp "$period_dir/.tpm-backup-marker" "$chain_archive/"; then _ARCHIVE_LAST_COUNTS[tpm_marker]=1; ((_moved++)); fi
  fi

  _ARCHIVE_LAST_TOTAL=$_moved
  if (( _moved == 0 )); then
    # mkdir-gate: leave NOTHING on a no-op boundary -- remove the just-created
    # chain dir AND the .archives parent if we left it empty (an empty .archives/
    # would otherwise make the stub-reaper preserve the period forever).
    rmdir "$chain_archive" 2>/dev/null || true
    rmdir "$archive_base" 2>/dev/null || true
    _ARCHIVE_LAST_PATH=""
    return 2
  fi
  # Outputs travel via globals (NOT stdout): callers read _ARCHIVE_LAST_PATH /
  # _ARCHIVE_LAST_TOTAL directly, because capturing via $(...) would fork a
  # subshell and lose every global this function set.
  _ARCHIVE_LAST_PATH="$chain_archive"
  return 0
}

archive_existing_checkpoint_chain() {
  local vm_name=$1
  local backup_dir=$2
  
  if is_dry_run; then
    log_info "vmbackup.sh" "archive_existing_checkpoint_chain" "[DRY-RUN] Would archive existing chain for $vm_name in $backup_dir - skipping"
    return 0
  fi
  
  if [[ -z "$vm_name" ]] || [[ -z "$backup_dir" ]]; then
    log_error "vmbackup.sh" "archive_existing_checkpoint_chain" "Missing parameters: vm_name='$vm_name', backup_dir='$backup_dir'"
    return 1
  fi

  # INT-15 guard: skip if this VM's chain has already been archived this session.
  # Subsequent in-session calls would otherwise misidentify fresh post-archive backup data
  # as an existing chain and move it into chain-DATE.1, corrupting the live chain.
  if [[ -n "${_ARCHIVED_VMS_THIS_SESSION[$vm_name]:-}" ]]; then
    log_info "vmbackup.sh" "archive_existing_checkpoint_chain" \
      "VM $vm_name already archived this session at ${_ARCHIVED_VMS_THIS_SESSION[$vm_name]} - skipping duplicate archive (INT-15 guard)"
    return 0
  fi
  
  # Count restore points BEFORE archiving (for session logging)
  local restore_point_count
  restore_point_count=$(find "$backup_dir" -maxdepth 1 -name "virtnbdbackup.*.xml" -type f 2>/dev/null | wc -l)
  
  # B4/CLEAN-01: relocate the chain's artefacts into the canonical
  # .archives/chain-<date>[.N]/ layout via the shared mover (one routine shared
  # with retention's archive_active_chains so the two can never drift again).
  _archive_chain_files "$vm_name" "$backup_dir"
  local _acf_rc=$?
  local chain_archive="$_ARCHIVE_LAST_PATH"
  if (( _acf_rc == 2 )); then
    log_info "vmbackup.sh" "archive_existing_checkpoint_chain" \
      "No chain artefacts to archive for $vm_name in $backup_dir - nothing moved"
    return 0
  elif (( _acf_rc != 0 )); then
    log_error "vmbackup.sh" "archive_existing_checkpoint_chain" \
      "Failed to archive chain for $vm_name (shared mover rc=$_acf_rc)"
    return 1
  fi
  local total_archived=${_ARCHIVE_LAST_TOTAL:-0}
  log_info "vmbackup.sh" "archive_existing_checkpoint_chain" \
    "Archived chain for VM $vm_name: $total_archived files to $chain_archive"

  # Log chain archive as a file operation (summary-level, not per-file)
  if declare -f log_file_operation >/dev/null 2>&1; then
    local archive_total_bytes
    archive_total_bytes=$(_du_bytes "$chain_archive")
    log_file_operation "move" "$vm_name" "$backup_dir" "$chain_archive" \
      "directory" "Chain archived: ${total_archived} files (${_ARCHIVE_LAST_COUNTS[metadata]:-0} metadata, ${_ARCHIVE_LAST_COUNTS[vmconfig]:-0} vmconfig, ${_ARCHIVE_LAST_COUNTS[full]:-0} full, ${_ARCHIVE_LAST_COUNTS[inc]:-0} inc, ${_ARCHIVE_LAST_COUNTS[cpt]:-0} cpt, ${_ARCHIVE_LAST_COUNTS[chksum]:-0} chksum, ${_ARCHIVE_LAST_COUNTS[qcow_json]:-0} qcow.json, ${_ARCHIVE_LAST_COUNTS[nvram]:-0} nvram, ${_ARCHIVE_LAST_COUNTS[tpm_marker]:-0} tpm-marker)" \
      "archive_existing_checkpoint_chain" "true"
  fi

  # Set global tracking variables for session logging
  # These are read by backup_vm() regardless of which code path triggered the archival
  _ARCHIVE_CHAIN_ARCHIVED="true"
  _ARCHIVE_RESTORE_POINTS=$restore_point_count
  _ARCHIVE_PATH="$chain_archive"

  # INT-15 guard: mark this VM as already archived in the current session so any
  # subsequent in-session call short-circuits (see entry guard above).
  _ARCHIVED_VMS_THIS_SESSION[$vm_name]="$chain_archive"
  
  # If policy change was detected, record the archive path
  if [[ "$_POLICY_CHANGE_DETECTED" == "true" ]]; then
    _POLICY_CHANGE_ARCHIVE_PATH="$chain_archive"
  fi
  
  log_debug "vmbackup.sh" "archive_existing_checkpoint_chain" \
    "Set archive tracking: chain_archived=true, restore_points=$restore_point_count, path=$chain_archive"
  
  # G6/G7: Log archive to SQLite chain_health + chain_events audit trail
  if declare -f sqlite_archive_chain >/dev/null 2>&1; then
    # Derive period_id via get_period_id when available (handles accumulate correctly).
    # Fallback to the period-directory NAME for non-integration deployments.
    # FF-153: chain_archive is <period_dir>/.archives/chain-<date>, so ONE
    # dirname yields '.archives' — two dirname hops are needed to reach the
    # period directory whose basename is the period id.
    local archive_period_id
    if declare -f get_vm_rotation_policy >/dev/null 2>&1 && declare -f get_period_id >/dev/null 2>&1; then
      archive_period_id=$(get_period_id "$(get_vm_rotation_policy "$vm_name")" 2>/dev/null)
    fi
    archive_period_id="${archive_period_id:-$(basename "$(dirname "$(dirname "$chain_archive")")")}"
    local archive_chain_id=$(basename "$chain_archive")
    local archive_size
    archive_size=$(_du_bytes "$chain_archive")
    
    sqlite_archive_chain "$vm_name" "$archive_period_id" "$archive_chain_id" \
      "$chain_archive" "$archive_size"
    log_debug "vmbackup.sh" "archive_existing_checkpoint_chain" \
      "Logged archive to SQLite: $vm_name/$archive_period_id/$archive_chain_id"
    
    # Write chain_events audit trail entry
    if declare -f sqlite_log_chain_event >/dev/null 2>&1; then
      local archive_trigger="archive_existing_checkpoint_chain"
      [[ "${_POLICY_CHANGE_DETECTED:-false}" == "true" ]] && archive_trigger="policy_change"
      sqlite_log_chain_event "chain_archived" "$vm_name" "$archive_chain_id" \
        "$archive_period_id" "$backup_dir" "$chain_archive" \
        "$restore_point_count" "$archive_size" \
        "${_POLICY_CHANGE_DETECTED:+policy_change}" "$archive_trigger" \
        "" "" "" "" "" 2>/dev/null || true
    fi
  fi
  
  return 0
}

backup_vm() {
  local vm_name=$1
  local recovery_attempted=false
  local max_retries=1  # Only retry once after emergency cleanup
  local retry_count=0
  
  # Reset error tracking for this VM
  reset_error_tracking
  
  # Track backup metrics for summary
  local backup_start_time=$(date '+%Y-%m-%d %H:%M:%S')
  local backup_start_epoch=$(date +%s)
  local checkpoint_before=0
  local final_backup_type="unknown"
  local final_status="unknown"
  local final_error=""
  local final_size="0"
  
  # Get VM's rotation policy (needed for logging even if excluded)
  local vm_policy=""
  if declare -f get_vm_rotation_policy >/dev/null 2>&1; then
    vm_policy=$(get_vm_rotation_policy "$vm_name")
  fi
  
  #############################################################################
  # VM-First Integration: Pre-backup hook
  # Handles: period boundary detection, chain archiving, exclusion check
  # In dry-run mode: only check exclusion, skip chain archiving
  #############################################################################
  if is_dry_run; then
    # In dry-run, only evaluate exclusion policy — don't archive chains
    local _dr_policy
    _dr_policy=$(get_vm_rotation_policy "$vm_name" 2>/dev/null || echo "")
    if [[ "$_dr_policy" == "never" ]]; then
      log_info "vmbackup.sh" "backup_vm" "[DRY-RUN] VM $vm_name would be EXCLUDED (policy=never)"
      VM_BACKUP_RESULTS+=("$vm_name|EXCLUDED|n/a|00:00:00|0|N/A||$_dr_policy")
      # Bug 2 fix: run retention + stub cleanup for excluded VMs (DRY_RUN-aware via wrapper)
      declare -f run_retention_for_unbacked_vm >/dev/null 2>&1 && \
        run_retention_for_unbacked_vm "$vm_name" "excluded"
      return $BACKUP_RC_EXCLUDED
    fi
    log_info "vmbackup.sh" "backup_vm" "[DRY-RUN] pre_backup_hook: policy=$_dr_policy (chain archiving skipped)"
  else
    # NAME-02 (118-spaces): distinguish an intentional policy exclusion from a
    # pre-backup ERROR. pre_backup_hook returns BACKUP_RC_EXCLUDED for policy=never
    # and any other non-zero for a fault (e.g. a vm_fs_name hash failure). The old
    # `! pre_backup_hook` collapsed EVERY non-zero to EXCLUDED, laundering errors
    # into a benign-looking "success / 0 failed" (the exact thing that hid NAME-01).
    pre_backup_hook "$vm_name"
    local _ph_rc=$?
    if [[ "$_ph_rc" -eq "$BACKUP_RC_EXCLUDED" ]]; then
      # VM excluded by policy - log to session summary
      _log_vm_backup_summary "$vm_name" "$backup_start_time" "$backup_start_epoch" \
        "EXCLUDED" "n/a" "0" "0" "" "0" "$vm_policy" "" \
        "vm_excluded" "excluded by rotation policy (policy=$vm_policy)" "0" "0"
      # Bug 2 fix: run retention + stub cleanup for excluded VMs
      declare -f run_retention_for_unbacked_vm >/dev/null 2>&1 && \
        run_retention_for_unbacked_vm "$vm_name" "excluded"
      return $BACKUP_RC_EXCLUDED  # Return 2 = excluded (don't count as success)
    elif [[ "$_ph_rc" -ne 0 ]]; then
      # A non-policy pre-backup failure -> FAILED, never silent.
      log_error "vmbackup.sh" "backup_vm" "pre-backup hook failed for VM '$vm_name' (rc=$_ph_rc)"
      _log_vm_backup_summary "$vm_name" "$backup_start_time" "$backup_start_epoch" \
        "FAILED" "n/a" "0" "0" "" "0" "$vm_policy" "" \
        "pre_backup_error" "pre-backup hook failed (rc=$_ph_rc)" "0" "0"
      return 1  # FAILED (counted as a failure by the session loop)
    fi
  fi
  
  #############################################################################
  # State Tracking: Reset archive tracking and initialize backup run variables
  #############################################################################
  _ARCHIVE_CHAIN_ARCHIVED="false"
  _ARCHIVE_RESTORE_POINTS=0
  VM_STATE="unknown"
  QEMU_AGENT_AVAILABLE=0
  VM_WAS_PAUSED=0
  local backup_method="unknown"              # Backup method: agent/paused/offline
  local restore_points_before=0              # Restore points before backup
  local restore_points_after=0               # Restore points after backup
  
  log_info "vmbackup.sh" "backup_vm" ""
  log_info "vmbackup.sh" "backup_vm" "╔══════════════════════════════════════════════════════════════════════════════╗"
  log_info "vmbackup.sh" "backup_vm" "║  BACKUP START: $vm_name"
  log_info "vmbackup.sh" "backup_vm" "║  Time: $backup_start_time"
  log_info "vmbackup.sh" "backup_vm" "╚══════════════════════════════════════════════════════════════════════════════╝"
  
  # Loop to allow retry after emergency recovery
  while true; do
    # Check lock
    if has_lock "$vm_name"; then
      # FF-48: post-FF-6, has_lock returns 0 ONLY when the lock's PID is a LIVE
      # vmbackup/vmrestore/virtnbdbackup owner (anything else is reaped) — so a lock
      # seen here is ALWAYS a legitimate live process, never a stale artefact.
      # Emergency recovery must therefore fire ONLY when WE are that owner: a
      # same-session Ctrl+Z / interrupt that re-enters backup_vm with our own $$
      # still in the lock file. A live FOREIGN holder (e.g. a vmrestore mid-restore
      # of this same VM) must fall through to the skip arm and fail closed — never
      # be emergency-cleaned, or vmbackup would steal the lock and back up a VM
      # mid-restore (reopening FF-6). pid==$$ is the discriminator because
      # create_lock only ever writes the main process's own $$.
      local _lk_file; _lk_file=$(vm_lock_file "$vm_name" 2>/dev/null) || _lk_file=""  # LOCK-01: token-keyed
      local _lk_pid; _lk_pid=$(cat "$_lk_file" 2>/dev/null)
      # Recover only OUR OWN interrupted lock (pid==$$); never a live foreign holder.
      if [[ "$_lk_pid" == "$$" ]] && [[ "$recovery_attempted" == false ]] && detect_interrupted_backup "$vm_name"; then
        log_warn "vmbackup.sh" "backup_vm" "Detected interrupted backup for $vm_name - performing emergency recovery"
        emergency_cleanup_current_vm "$vm_name"
        recovery_attempted=true
        retry_count=$((retry_count + 1))
        
        # Loop back to retry after emergency cleanup
        log_info "vmbackup.sh" "backup_vm" "Retrying backup for $vm_name after emergency recovery"
        continue
      else
        log_warn "vmbackup.sh" "backup_vm" "Backup already in progress for VM: $vm_name (lock held by live PID ${_lk_pid:-unknown}) - skipping"
        _log_vm_backup_summary "$vm_name" "$backup_start_time" "$backup_start_epoch" \
          "SKIPPED" "n/a" "0" "0" "already in progress" "0" "$vm_policy" "" \
          "backup_skipped" "already in progress (lock exists)" "0" "0"
        # FF-48b: return 0 (was 1), matching the offline-unchanged skip arm's
        # SKIPPED+return-0 contract. rc1 DOUBLE-COUNTED this VM: the session
        # loop's fail_count++ AND the post-loop SKIPPED sweep both fired, then
        # backed_up_count -= skipped_count decremented a VM never added to it,
        # so backed_up_count could go NEGATIVE and a benign concurrent-restore
        # skip raised a FALSE nightly failure. rc0 => backed_up_count++ then -=
        # its own SKIPPED row => net 0 (skip-only, not failed).
        return 0
      fi
    fi
    
    # Create lock (skip in dry-run - no filesystem writes)
    if is_dry_run; then
      log_info "vmbackup.sh" "backup_vm" "[DRY-RUN] Skipping lock creation for VM: $vm_name"
    else
      if ! create_lock "$vm_name"; then
        log_error "vmbackup.sh" "backup_vm" "Failed to create lock for VM: $vm_name"
        _log_vm_backup_summary "$vm_name" "$backup_start_time" "$backup_start_epoch" \
          "FAILED" "n/a" "0" "0" "lock creation failed" "0" "$vm_policy" "" \
          "backup_failed" "lock creation failed" "0" "0"
        return 1
      fi
      
      trap "remove_lock '$vm_name'" RETURN
    fi
    
    # Lock created successfully - break out of retry loop
    break
  done
  
  # HIGH FIX: Cache VM status at start to avoid redundant virsh calls (eliminates 200-500ms overhead)
  local vm_status=$(get_vm_status "$vm_name")
  log_info "vmbackup.sh" "backup_vm" "VM status: $vm_status"
  
  # Set globals for _log_vm_backup_summary -> sqlite_log_vm_backup
  VM_STATE="$vm_status"
  
  # Cache agent availability check (eliminates redundant virsh qemu-agent-command calls)
  local has_qemu_agent=false
  local guest_os="unknown"
  if [[ "$vm_status" == "running" ]] && check_qemu_agent "$vm_name"; then
    has_qemu_agent=true
    QEMU_AGENT_AVAILABLE=1
    # Detect OS once — used by FSTRIM timeout selection and future OS-specific logic
    guest_os=$(detect_guest_os "$vm_name")
    log_debug "vmbackup.sh" "backup_vm" "Guest OS detected: $guest_os"
  else
    QEMU_AGENT_AVAILABLE=0
  fi
  
  # Determine initial backup method based on VM state
  if [[ "$vm_status" == "shut off" ]]; then
    backup_method="offline"
  elif [[ "$has_qemu_agent" == "true" ]]; then
    backup_method="agent"
  else
    backup_method="paused"  # Will be confirmed when actually paused
  fi
  log_debug "vmbackup.sh" "backup_vm" "Method decision: vm_status=$vm_status has_agent=$has_qemu_agent → method=$backup_method"
  
  # Store current agent status persistently for future reference
  # When VM is running, record whether agent is present for when it goes offline
  # This allows us to make intelligent decisions about directory preservation
  local backup_dir
  backup_dir=$(get_backup_dir "$vm_name")
  local agent_status_file="$backup_dir/.agent-status"
  # 118/dry-run: backup_dir is the slug+hash TOKEN folder. Creating it (and writing
  # .agent-status into it) during a preview pre-makes the folder, which then blocks
  # MIG-01's no-clobber migration on the next real run. Skip all of it under --dry-run.
  if is_dry_run; then
    log_info "vmbackup.sh" "backup_vm" "[DRY-RUN] would create backup dir + record agent status for $vm_name — skipping"
  else
    mkdir -p "$backup_dir" 2>/dev/null
    if [[ "$vm_status" == "running" ]]; then
      if [[ "$has_qemu_agent" == "true" ]]; then
        echo "yes" > "$agent_status_file"
        log_debug "vmbackup.sh" "backup_vm" "Agent status recorded: VM $vm_name HAS agent (persisted to .agent-status)"
      else
        echo "no" > "$agent_status_file"
        log_debug "vmbackup.sh" "backup_vm" "Agent status recorded: VM $vm_name NO agent (persisted to .agent-status)"
      fi
    fi
  fi
  
  # OFFLINE VM BACKUP STRATEGY
  # =========================
  # When VM is offline: Check if disks have changed
  # - No changes: SKIP backup (preserve checkpoint chain if it exists)
  # - Changes detected: ARCHIVE existing chain (if present) + create fresh FULL backup
  # 
  # This handles both agent=yes and agent=no cases uniformly:
  # - agent=yes: Archived chain available if VM comes back online (fresh baseline for new incrementals)
  # - agent=no: Archived chains preserve historical restore points
  
  if [[ "$vm_status" == "shut off" ]]; then
    log_info "vmbackup.sh" "backup_vm" "Offline VM detected: $vm_name - checking if disk changes present"
    
    # Check if offline VM disks have changed (honour SKIP_OFFLINE_UNCHANGED_BACKUPS).
    # When SKIP_OFFLINE_UNCHANGED_BACKUPS=false, force the "changed" path so the
    # offline VM is backed up unconditionally.
    local _offline_unchanged=false
    if [[ "$SKIP_OFFLINE_UNCHANGED_BACKUPS" == "true" ]]; then
      if ! has_offline_vm_changed "$vm_name"; then
        _offline_unchanged=true
      fi
    else
      log_info "vmbackup.sh" "backup_vm" "SKIP_OFFLINE_UNCHANGED_BACKUPS=false — backing up offline VM $vm_name unconditionally"
    fi
    
    if [[ "$_offline_unchanged" == "true" ]]; then
      # No changes detected: SKIP backup entirely
      log_info "vmbackup.sh" "backup_vm" "SKIPPING backup for offline VM: $vm_name (disks unchanged since last backup)"
      
      # Calculate existing restore points for skip log entry (count actual data files, not virsh checkpoints)
      local existing_restore_points=$(get_restore_point_count "$backup_dir")
      local skip_event_detail="skipped: disks unchanged since last backup"
      
      log_info "vmbackup.sh" "backup_vm" "SKIP REASON: VM '$vm_name' is offline (shut off) AND disk files have not been modified since last backup"
      log_info "vmbackup.sh" "backup_vm" "SKIP ACTION: Preserving existing backup chain - no new backup created"
      log_info "vmbackup.sh" "backup_vm" "========== Backup SKIPPED for VM: $vm_name (offline, no changes) =========="
      
      # Record in session summary (CRITICAL: without this, VM won't appear in summary table)
      # For skipped VMs: checkpoint_before = checkpoint_after since nothing changed
      _log_vm_backup_summary "$vm_name" "$backup_start_time" "$backup_start_epoch" \
        "SKIPPED" "none" "$existing_restore_points" "$existing_restore_points" \
        "offline unchanged" "0" "$vm_policy" "$backup_dir" \
        "backup_skipped" "$skip_event_detail" "0" "0"
      
      # Bug 2 fix: run retention + stub cleanup for skipped VMs
      declare -f run_retention_for_unbacked_vm >/dev/null 2>&1 && \
        run_retention_for_unbacked_vm "$vm_name" "skipped"

      return 0  # Success - skip backup
    else
      # DISK CHANGES DETECTED on offline VM
      log_warn "vmbackup.sh" "backup_vm" "CHANGE DETECTED: Offline VM $vm_name disks modified since last backup"
      log_info "vmbackup.sh" "backup_vm" "ACTION PLAN: 1) Archive existing chain (if any), 2) Clear virsh checkpoints, 3) Create fresh full (copy) backup"
      
      # Check if there's an existing checkpoint chain to archive
      if find "$backup_dir" -maxdepth 1 \( -name "virtnbdbackup.*.xml" -o -name "*.full.data" -o -name "*.inc.virtnbdbackup.*.data" -o -name "*.copy.data" -o -name "vmconfig.copy.xml" \) -print -quit 2>/dev/null | grep -q .; then
        log_info "vmbackup.sh" "backup_vm" "Existing checkpoint chain found for $vm_name - archiving before fresh full backup"
        
        # Archive the existing chain (full + any incrementals)
        # Note: archive_existing_checkpoint_chain sets _ARCHIVE_CHAIN_ARCHIVED and _ARCHIVE_RESTORE_POINTS
        if is_dry_run; then
          log_info "vmbackup.sh" "backup_vm" "[DRY-RUN] would archive existing checkpoint chain for $vm_name before fresh full — skipping"
        elif archive_existing_checkpoint_chain "$vm_name" "$backup_dir"; then
          log_info "vmbackup.sh" "backup_vm" "Successfully archived checkpoint chain for VM: $vm_name ($_ARCHIVE_RESTORE_POINTS restore points)"
          
          # NOTE: virsh checkpoints persist in qcow2 and cannot be deleted while VM is offline.
          # These orphan checkpoints will be automatically detected and cleaned up when the VM
          # comes back online via the orphan handling in backup_vm() (lines 4269-4298).
          # The copy-mode backup proceeds without needing checkpoint deletion.
          log_info "vmbackup.sh" "backup_vm" "Note: virsh checkpoints persist in qcow2 (VM offline) - will be cleaned when VM restarts"
        else
          log_error "vmbackup.sh" "backup_vm" "Failed to archive checkpoint chain for VM: $vm_name"
          _log_vm_backup_summary "$vm_name" "$backup_start_time" "$backup_start_epoch" \
            "FAILED" "archive" "0" "0" "offline archive failed" "0" "$vm_policy" "$backup_dir" \
            "backup_failed" "offline archive failed" "0" "0"
          return 1
        fi
      else
        log_info "vmbackup.sh" "backup_vm" "No existing checkpoint chain found for $vm_name - proceeding with fresh full backup"
      fi
      
      log_info "vmbackup.sh" "backup_vm" "Offline VM $vm_name proceeding with FULL backup to capture disk changes"
    fi
  fi
  
  # CRITICAL: Clean backup directory state files before backup
  # This must happen BEFORE checkpoint health check, which depends on clean state
  # virtnbdbackup requires empty directory - remove only old log/state files, preserve checkpoints
  
  # Only clean if directory exists (backup was done before)
  if [[ -d "$backup_dir" ]]; then
    # CRITICAL: Different cleanup strategy for offline vs online VMs
    # Offline VMs: Clean everything for fresh full backup (old chain was already archived)
    # Online VMs: Clean only old logs - PRESERVE backup data and checkpoints for incremental backups
    if is_dry_run; then
      log_info "vmbackup.sh" "backup_vm" "[DRY-RUN] would clean backup directory before backup (status=$vm_status) — skipping"
    elif [[ "$vm_status" == "shut off" ]]; then
      # OFFLINE VM: Clean everything for fresh full backup
      # Note: If checkpoint chain existed, it was already archived above before we get here
      log_info "vmbackup.sh" "backup_vm" "Offline VM - cleaning backup directory for fresh full backup"
      
      # Validate backup_dir before using rm -rf to prevent filesystem destruction
      if [[ -n "$backup_dir" && "$backup_dir" == "$BACKUP_PATH"* && -d "$backup_dir" ]]; then
        rm -rf "$backup_dir"/*
        if declare -f log_file_operation >/dev/null 2>&1; then
          log_file_operation "delete" "$vm_name" "$backup_dir" "" \
            "directory" "Offline VM cleanup for fresh full backup" "backup_vm" "true"
        fi
      else
        log_error "vmbackup.sh" "backup_vm" "SECURITY: Refusing to delete - backup_dir validation failed: '$backup_dir'"
      fi
      
      log_info "vmbackup.sh" "backup_vm" "Offline VM backup directory cleaned for fresh full (copy-mode) backup"
    else
      # ONLINE VM: Clean only old logs and failed backups, PRESERVE backup data and checkpoints
      log_info "vmbackup.sh" "backup_vm" "[Cleanup 1/2] Removing stale artifacts (logs, partial files) - backup data preserved"
      
      local cleanup_count=0
      
      # Remove old backup log files (backup.auto.*.log, backup.full.*.log)
      while IFS= read -r deleted; do
        log_debug "vmbackup.sh" "backup_vm" "[Cleanup 1/2] Deleted: $(basename "$deleted")"
        ((cleanup_count++))
      done < <(find "$backup_dir" -maxdepth 1 -name "backup.*.log" -type f -print -delete 2>/dev/null)
      
      # Remove only stale backup data files (incomplete/failed backups marked with .partial suffix)
      while IFS= read -r deleted; do
        log_debug "vmbackup.sh" "backup_vm" "[Cleanup 1/2] Deleted: $(basename "$deleted")"
        ((cleanup_count++))
      done < <(find "$backup_dir" -maxdepth 1 -name "*.partial" -type f -print -delete 2>/dev/null)
      
      # Remove old format backup files (pre-virtnbdbackup format) - but NOT virtnbdbackup .data files
      for pattern in "*.qcow2" "*.img" "*.raw" "*.backup" "*.tar.gzip"; do
        for file in "$backup_dir"/$pattern; do
          if [[ -f "$file" ]]; then
            log_debug "vmbackup.sh" "backup_vm" "[Cleanup 1/2] Deleted: $(basename "$file")"
            rm -f "$file"
            ((cleanup_count++))
          fi
        done
      done
      
      log_info "vmbackup.sh" "backup_vm" "[Cleanup 1/2] Complete: $cleanup_count files removed"
    fi
  fi
  
  # ═══════════════════════════════════════════════════════════════════════════════
  # UNIFIED VALIDATION: Single checkpoint/state validation for entire backup
  # Replaces: report_checkpoint_health() + validate_checkpoint_health() + validate_backup_preconditions()
  # Benefits: ONE virsh call, cached results, detailed validation output
  # ═══════════════════════════════════════════════════════════════════════════════
  
  # Run unified validation (populates CACHED_* globals)
  validate_backup_state "$vm_name" "$backup_dir"
  
  # ═══════════════════════════════════════════════════════════════════════════════
  # POLICY CHANGE DETECTION: Detect rotation policy changes that invalidate chains
  # If policy changed (e.g., daily→monthly), existing checkpoint chain cannot be
  # continued because QEMU only maintains one checkpoint chain per VM. The chain
  # metadata (virtnbdbackup.X in QEMU) won't match the folder's expected checkpoints.
  # Solution: Archive existing chain and start fresh FULL backup in new policy folder.
  # ═══════════════════════════════════════════════════════════════════════════════
  
  # Reset policy change tracking for this VM
  reset_policy_change_tracking
  
  # Detect policy change by comparing current policy with SQLite history
  if detect_policy_change "$vm_name" "$vm_policy"; then
    log_warn "vmbackup.sh" "backup_vm" "POLICY CHANGE DETECTED: ${_POLICY_CHANGE_PREVIOUS} → ${_POLICY_CHANGE_CURRENT}"
    log_warn "vmbackup.sh" "backup_vm" "Existing checkpoint chain is incompatible with new policy"
    
    # Check if there's an existing chain to archive
    local has_existing_chain=false
    if [[ $CACHED_CHECKPOINT_COUNT -gt 0 ]]; then
      has_existing_chain=true
    elif find "$backup_dir" -maxdepth 1 \( -name "*.full.data" -o -name "*.inc.virtnbdbackup.*.data" -o -name "*.copy.data" \) -print -quit 2>/dev/null | grep -q .; then
      has_existing_chain=true
    fi
    
    if [[ "$has_existing_chain" == "true" ]]; then
      if is_dry_run; then
        # DRY-RUN: preview only — mutate nothing. List checkpoints read-only to
        # report the count that WOULD be cleared; leave CACHED_* at validated
        # values (no fake fresh-start). The only downstream mutator of that
        # stale state is the checkpoint-corruption arm below, dry-run-guarded
        # by item R2-C.
        local dr_checkpoints=()
        mapfile -t dr_checkpoints < <(lv_checkpoint_list_virtnbd "$vm_name")
        log_info "vmbackup.sh" "backup_vm" "[DRY-RUN] Would archive existing chain and clear ${#dr_checkpoints[@]} QEMU checkpoint(s) for policy change ${_POLICY_CHANGE_PREVIOUS} → ${_POLICY_CHANGE_CURRENT} (VM: $vm_name)"
      else
        log_info "vmbackup.sh" "backup_vm" "Archiving existing chain before starting new policy baseline"
        
        # Archive the existing chain
        if archive_existing_checkpoint_chain "$vm_name" "$backup_dir"; then
          log_info "vmbackup.sh" "backup_vm" "Chain archived successfully to: $_ARCHIVE_PATH"
          
          # Delete QEMU checkpoints to allow fresh start
          local checkpoints=()
          mapfile -t checkpoints < <(lv_checkpoint_list_virtnbd "$vm_name")
          if [[ ${#checkpoints[@]} -gt 0 ]]; then
            log_info "vmbackup.sh" "backup_vm" "Clearing ${#checkpoints[@]} QEMU checkpoints for new chain"
            for cp in "${checkpoints[@]}"; do
              lv_checkpoint_delete_metadata "$vm_name" "$cp" || true
            done
          fi
          
          # Update cache to reflect fresh start
          CACHED_CHECKPOINT_COUNT=0
          CACHED_CHAIN_HEALTHY="true"
          CACHED_VALIDATION_STATE="clean"
          
          log_info "vmbackup.sh" "backup_vm" "Policy change handling complete - proceeding with FULL backup"
        else
          log_error "vmbackup.sh" "backup_vm" "Failed to archive chain during policy change - aborting"
          set_backup_error "POLICY_CHANGE_ARCHIVE_FAILED" "Could not archive chain during policy change" ""
          _log_vm_backup_summary "$vm_name" "$backup_start_time" "$backup_start_epoch" \
            "FAILED" "archive" "0" "0" "policy change archive failed" "0" "$vm_policy" "$backup_dir" \
            "backup_failed" "policy change archive failed" "0" "0"
          return 1
        fi
      fi
    else
      log_info "vmbackup.sh" "backup_vm" "No existing chain to archive - proceeding with FULL backup for new policy"
      CACHED_VALIDATION_STATE="clean"
    fi
  fi
  
  # Check for checkpoint chain corruption and handle auto-recovery
  if [[ "$CACHED_CHAIN_HEALTHY" == "false" ]] && [[ $CACHED_CHECKPOINT_COUNT -gt 0 ]]; then
    # Checkpoint corruption detected (broken chain - gaps in sequence)
    log_error "vmbackup.sh" "backup_vm" "CHECKPOINT CORRUPTION DETECTED: VM $vm_name has broken checkpoint chain"
    log_error "vmbackup.sh" "backup_vm" "See: https://github.com/abbbi/virtnbdbackup/discussions/267"
    
    # Handle based on configuration
    if [[ "$ENABLE_AUTO_RECOVERY_ON_CHECKPOINT_CORRUPTION" == "yes" ]]; then
      # AUTO-RECOVERY: Delete corrupted checkpoints
      log_warn "vmbackup.sh" "backup_vm" "AUTO-RECOVERY ENABLED: Deleting corrupted checkpoints for $vm_name"
      log_warn "vmbackup.sh" "backup_vm" "WARNING: This will reset point-in-time recovery baseline and force a FULL backup"
      
      # DRY-RUN: Skip — auto-recovery deletes real backup-store checkpoint metadata
      # (rm -rf checkpoints/ + *.cpt) and rewrites the CACHED_* chain state; preview only.
      if is_dry_run; then
        log_info "vmbackup.sh" "backup_vm" "[DRY-RUN] Would delete corrupted checkpoint metadata ($backup_dir/checkpoints and $backup_dir/*.cpt) and reset checkpoint chain to force a FULL backup for VM: $vm_name"
      else
        # Log state BEFORE deletion for forensics
        local cp_dir_exists="no"
        local cpt_file_count=0
        [[ -d "$backup_dir/checkpoints" ]] && cp_dir_exists="yes"
        cpt_file_count=$(ls -1 "$backup_dir"/*.cpt 2>/dev/null | wc -l)
        log_warn "vmbackup.sh" "backup_vm" "[DELETE-BEFORE] checkpoints dir exists: $cp_dir_exists, .cpt files: $cpt_file_count"
        log_warn "vmbackup.sh" "backup_vm" "[DELETE] Target: $backup_dir/checkpoints and $backup_dir/*.cpt"

        rm -rf "$backup_dir/checkpoints" "$backup_dir"/*.cpt 2>/dev/null
        local rm_result=$?

        # Log state AFTER deletion
        local cp_dir_after="no"
        local cpt_after=0
        [[ -d "$backup_dir/checkpoints" ]] && cp_dir_after="yes"
        cpt_after=$(ls -1 "$backup_dir"/*.cpt 2>/dev/null | wc -l)
        log_info "vmbackup.sh" "backup_vm" "[DELETE-AFTER] checkpoints dir exists: $cp_dir_after, .cpt files: $cpt_after, rm exit code: $rm_result"

        if [[ $rm_result -eq 0 ]]; then
          log_info "vmbackup.sh" "backup_vm" "Checkpoint metadata deleted successfully - next backup will be FULL (recovery mode)"
          log_info "vmbackup.sh" "backup_vm" "After recovery FULL backup, incremental backups will resume normally"
          # Update cache to reflect cleanup
          CACHED_CHECKPOINT_COUNT=0
          CACHED_CHAIN_HEALTHY="true"
          CACHED_VALIDATION_STATE="clean"
        else
          log_error "vmbackup.sh" "backup_vm" "Failed to delete corrupted checkpoint metadata"
          _log_vm_backup_summary "$vm_name" "$backup_start_time" "$backup_start_epoch" \
            "FAILED" "recovery" "0" "0" "checkpoint delete failed" "0" "$vm_policy" "$backup_dir" \
            "backup_failed" "checkpoint delete failed during recovery" "${retry_count:-0}" "0"
          return 1
        fi
      fi
    elif [[ "$ENABLE_AUTO_RECOVERY_ON_CHECKPOINT_CORRUPTION" == "warn" ]]; then
      # WARN MODE: Fail with clear remediation steps
      log_error "vmbackup.sh" "backup_vm" "CHECKPOINT CORRUPTION RECOVERY REQUIRED (user decision needed)"
      log_error "vmbackup.sh" "backup_vm" ""
      log_error "vmbackup.sh" "backup_vm" "ROOT CAUSE: Checkpoint chain has gaps (missing checkpoints in sequence)"
      log_error "vmbackup.sh" "backup_vm" "IMPACT: Cannot continue incremental backups - recovery required"
      log_error "vmbackup.sh" "backup_vm" ""
      log_error "vmbackup.sh" "backup_vm" "REMEDIATION OPTIONS:"
      log_error "vmbackup.sh" "backup_vm" "  Option 1 (RECOMMENDED - Full reset):"
      log_error "vmbackup.sh" "backup_vm" "    sudo rm -rf $backup_dir/checkpoints"
      log_error "vmbackup.sh" "backup_vm" "    sudo rm -f $backup_dir/*.cpt"
      log_error "vmbackup.sh" "backup_vm" "    Then re-run backup (will do FULL backup, ~5-10GB)"
      log_error "vmbackup.sh" "backup_vm" "    After FULL completes, incremental backups resume"
      log_error "vmbackup.sh" "backup_vm" "    NOTE: Loses point-in-time recovery for current month"
      log_error "vmbackup.sh" "backup_vm" ""
      log_error "vmbackup.sh" "backup_vm" "  Option 2 (WAIT - Monthly reset):"
      log_error "vmbackup.sh" "backup_vm" "    Wait for next month boundary (auto resets all checkpoints)"
      log_error "vmbackup.sh" "backup_vm" "    Next backup in new month will succeed automatically"
      log_error "vmbackup.sh" "backup_vm" ""
      log_error "vmbackup.sh" "backup_vm" "KNOWN ISSUE: https://github.com/abbbi/virtnbdbackup/discussions/267"
      log_error "vmbackup.sh" "backup_vm" "STATUS: Reported as random corruption after failed backup chains"
      log_error "vmbackup.sh" "backup_vm" ""
      _log_vm_backup_summary "$vm_name" "$backup_start_time" "$backup_start_epoch" \
        "FAILED" "n/a" "0" "0" "checkpoint corruption (warn mode)" "0" "$vm_policy" "$backup_dir" \
        "backup_failed" "checkpoint corruption (warn mode, recovery required)" "0" "0"
      return 1
    else
      # NO MODE: Fail immediately
      log_error "vmbackup.sh" "backup_vm" "Checkpoint health check failed for VM: $vm_name"
      log_error "vmbackup.sh" "backup_vm" "Auto-recovery is DISABLED (ENABLE_AUTO_RECOVERY_ON_CHECKPOINT_CORRUPTION=no)"
      log_error "vmbackup.sh" "backup_vm" "See logs above for remediation steps"
      _log_vm_backup_summary "$vm_name" "$backup_start_time" "$backup_start_epoch" \
        "FAILED" "n/a" "0" "0" "checkpoint corruption (recovery disabled)" "0" "$vm_policy" "$backup_dir" \
        "backup_failed" "checkpoint corruption (auto-recovery disabled)" "0" "0"
      return 1
    fi
  fi
  
  local current_month
  current_month=$(get_current_month)
  
  # Create backup directory if needed
  if is_dry_run; then
    [[ ! -d "$backup_dir" ]] && log_info "vmbackup.sh" "backup_vm" "[DRY-RUN] Would create backup directory: $backup_dir"
  else
    mkdir -p "$backup_dir"
  fi
  
  # Execute fstrim if enabled and agent available (use cached result)
  if [[ "$ENABLE_FSTRIM" == "true" ]] && [[ "$has_qemu_agent" == "true" ]]; then
    # Pre-flight: check discard_granularity on Windows VirtIO disks (advisory warning)
    check_discard_granularity "$vm_name" "$guest_os"
    if is_dry_run; then
      log_info "vmbackup.sh" "backup_vm" "[DRY-RUN] Would execute FSTRIM on VM: $vm_name"
    else
      [[ ${FSTRIM_IMPL_AVAILABLE:-0} -eq 1 ]] && execute_fstrim_in_guest "$vm_name" "$guest_os"
    fi
  fi
  
  # ═══════════════════════════════════════════════════════════════════════════
  # BACKUP TYPE DECISION LOGIC
  # ═══════════════════════════════════════════════════════════════════════════
  # 
  # This determines whether to request a FULL or AUTO (incremental) backup.
  # virtnbdbackup is passed -l full or -l auto accordingly.
  #
  # FULL backup is forced when:
  #   1. MONTH BOUNDARY - First backup of a new month (monthly consolidation)
  #   2. NEW VM - VM has no prior backups (no .full-backup-month marker)
  #   3. RECOVERY FLAG - Checkpoint corruption recovery in progress
  #   4. OFFLINE ARCHIVAL - VM was offline, chain was archived, needs fresh base
  #
  # AUTO mode (incremental) when:
  #   - Within same month as last full backup
  #   - virtnbdbackup decides full vs inc based on checkpoint state
  #
  # LOGGING NOTE:
  #   The backup_type logged reflects what we REQUEST here, not what
  #   virtnbdbackup ultimately decides. For "auto" mode, check restore_points
  #   to determine actual outcome: restore_points=1 means full, >1 means inc.
  #
  # NEW VM BEHAVIOR:
  #   A VM introduced mid-month will get a forced FULL backup on first run
  #   (no marker file exists), then AUTO (incremental) for remaining days
  #   of that month. Next month boundary triggers another FULL.
  # ═══════════════════════════════════════════════════════════════════════════
  
  local backup_type="auto"
  local last_full_month=""
  local full_backup_marker="$backup_dir/.full-backup-month"
  
  # OPTIMIZATION: Read file once (eliminates redundant cat subprocess call, ~1ms savings)
  [[ -f "$full_backup_marker" ]] && read last_full_month < "$full_backup_marker"
  
  # SELF-HEAL: Marker file exists but is empty (legacy code wrote empty markers).
  # The file's existence proves a full backup was already done in this directory.
  # Treat as current month to avoid destructive forced-FULL on existing data,
  # then write the correct value so it's fixed permanently.
  if [[ -f "$full_backup_marker" ]] && [[ -z "$last_full_month" ]]; then
    log_warn "vmbackup.sh" "backup_vm" "SELF-HEAL: Marker file exists but is empty: $full_backup_marker"
    log_warn "vmbackup.sh" "backup_vm" "  Writing correct value '$current_month' — future runs will read it normally"
    last_full_month="$current_month"
    if is_dry_run; then
      log_info "vmbackup.sh" "backup_vm" "[DRY-RUN] Would self-heal empty marker: write '$current_month' → $full_backup_marker (skipping write)"
    else
      echo "$current_month" > "$full_backup_marker"
    fi
  fi
  
  # Force full backup if:
  # 1. Month boundary (different month since last full backup)
  # 2. Recovery flag set (checkpoint recovery in progress)
  # 3. New VM (no marker file, so last_full_month is empty)
  if [[ "$last_full_month" != "$current_month" ]] || [[ -f "${TEMP_DIR}/vmbackup-recovery-${vm_name}.flag" ]]; then
    backup_type="full"
    log_info "vmbackup.sh" "backup_vm" "FULL BACKUP DECISION: Month boundary detected or recovery flag present"
    log_info "vmbackup.sh" "backup_vm" "  - Last full backup month: ${last_full_month:-'(none/new VM)'}"
    log_info "vmbackup.sh" "backup_vm" "  - Current month: $current_month"
    log_info "vmbackup.sh" "backup_vm" "  - Recovery flag: $([ -f "${TEMP_DIR}/vmbackup-recovery-${vm_name}.flag" ] && echo 'YES' || echo 'NO')"
    
    if [[ -f "${TEMP_DIR}/vmbackup-recovery-${vm_name}.flag" ]]; then
      log_warn "vmbackup.sh" "backup_vm" "Recovery action: Forcing FULL backup for $vm_name to reset checkpoints/bitmaps"
      if is_dry_run; then
        log_info "vmbackup.sh" "backup_vm" "[DRY-RUN] Would consume recovery flag: ${TEMP_DIR}/vmbackup-recovery-${vm_name}.flag (leaving it in place for the real backup)"
      else
        log_debug "vmbackup.sh" "backup_vm" "Deleting recovery flag: ${TEMP_DIR}/vmbackup-recovery-${vm_name}.flag"
        rm -f "${TEMP_DIR}/vmbackup-recovery-${vm_name}.flag"
      fi
    fi
    
    # Update full backup month marker
    if is_dry_run; then
      log_info "vmbackup.sh" "backup_vm" "[DRY-RUN] Would update full-backup-month marker: $full_backup_marker → $current_month (skipping write)"
    else
      echo "$current_month" > "$full_backup_marker"
      log_debug "vmbackup.sh" "backup_vm" "Updated full-backup-month marker: $full_backup_marker → $current_month"
    fi
  elif [[ "$vm_status" == "shut off" ]] && [[ "${_ARCHIVE_CHAIN_ARCHIVED:-false}" == "true" ]]; then
    # OFFLINE VM ARCHIVAL TRIGGER: If checkpoint chain was just archived, force FULL backup
    # This creates a fresh baseline after archiving the previous chain
    backup_type="full"
    log_info "vmbackup.sh" "backup_vm" "FULL backup forced: Offline VM $vm_name had checkpoint chain archived, creating fresh baseline"
    if is_dry_run; then
      log_info "vmbackup.sh" "backup_vm" "[DRY-RUN] Would update full-backup-month marker: $full_backup_marker → $current_month (skipping write)"
    else
      echo "$current_month" > "$full_backup_marker"
    fi
  else
    log_info "vmbackup.sh" "backup_vm" "INCREMENTAL backup (auto mode): continuing within month $current_month (last full: $last_full_month)"
  fi
  
  log_info "vmbackup.sh" "backup_vm" "Backup type determined: $backup_type (Full=month boundary reset, Auto=daily incremental)"
  
  # Archive orphaned offline backup data before FULL backup overwrites it
  # This handles the case where VM was offline (backup created without virsh checkpoints),
  # then comes back online and triggers a FULL backup that would overwrite the offline data
  # 
  # Orphan detection criteria:
  #   1. backup_type is "full" (about to overwrite existing data)
  #   2. VM is currently "running" (online VMs create virsh checkpoints, offline don't)
  #   3. Backup data exists (*.full.data or *.copy.data present)
  #   4. No virsh checkpoints exist (data is orphaned from previous offline backup)
  #
  # This preserves offline backup data that would otherwise be lost when VM comes back online
  if [[ "$backup_type" == "full" ]] && [[ "$vm_status" == "running" ]]; then
    log_debug "vmbackup.sh" "backup_vm" "Orphan check: FULL backup on running VM - checking for orphaned offline backup data"
    # Check if backup data exists but no virsh checkpoints (orphaned offline backup)
    local has_backup_data=false
    local backup_data_files
    backup_data_files=$(find "$backup_dir" -maxdepth 1 \( -name "*.full.data" -o -name "*.copy.data" \) 2>/dev/null)
    if [[ -n "$backup_data_files" ]]; then
      has_backup_data=true
      log_debug "vmbackup.sh" "backup_vm" "Orphan check: Found existing backup data files in $backup_dir"
    else
      log_debug "vmbackup.sh" "backup_vm" "Orphan check: No existing backup data files - fresh backup directory"
    fi
    
    if [[ "$has_backup_data" == "true" ]]; then
      local orphan_checkpoint_count
      orphan_checkpoint_count=$(lv_checkpoint_count_all "$vm_name")
      log_debug "vmbackup.sh" "backup_vm" "Orphan check: virsh checkpoint count for $vm_name = $orphan_checkpoint_count"
      
      if [[ "$orphan_checkpoint_count" -eq 0 ]]; then
        log_warn "vmbackup.sh" "backup_vm" "ORPHAN DETECTED: Backup data exists but no virsh checkpoints - this is orphaned offline backup data"
        log_info "vmbackup.sh" "backup_vm" "Archiving orphaned offline backup before FULL overwrites it (preserving previous offline backup)"
        if archive_existing_checkpoint_chain "$vm_name" "$backup_dir"; then
          log_info "vmbackup.sh" "backup_vm" "Successfully archived orphaned offline backup for VM: $vm_name"
        else
          # FF-155: fail closed, matching the two sibling archive-before-destroy
          # paths. Continuing into the FULL would overwrite the orphaned offline
          # restore point on a single archive fault (e.g. .archives mkdir ENOSPC).
          log_error "vmbackup.sh" "backup_vm" "Failed to archive orphaned offline backup for VM: $vm_name - aborting FULL to avoid overwriting the previous offline restore point"
          set_backup_error "ORPHAN_ARCHIVE_FAILED" "Could not archive orphaned offline backup before FULL" ""
          _log_vm_backup_summary "$vm_name" "$backup_start_time" "$backup_start_epoch" \
            "FAILED" "archive" "0" "0" "orphan archive failed" "0" "$vm_policy" "$backup_dir" \
            "backup_failed" "orphan archive failed" "0" "0"
          return 1
        fi
      else
        log_debug "vmbackup.sh" "backup_vm" "Orphan check: $orphan_checkpoint_count virsh checkpoints exist - not orphaned (normal full backup with existing chain)"
      fi
    fi
  else
    if [[ "$backup_type" == "full" ]]; then
      log_debug "vmbackup.sh" "backup_vm" "Orphan check: Skipped - VM is offline (offline archival handled separately)"
    fi
  fi
  
  # Capture checkpoint count BEFORE backup (for summary and QEMU management)
  checkpoint_before=$(get_checkpoint_depth "$vm_name")
  # Restore points = logical backup operations on disk, not virsh checkpoint count
  restore_points_before=$(get_restore_point_count "$backup_dir")
  final_backup_type="$backup_type"
  
  # Handle VM pause/resume if needed (use cached agent check result)
  local paused=false
  if [[ "$vm_status" == "running" ]]; then
    if ! [[ "$has_qemu_agent" == "true" ]]; then
      # DRY-RUN: preview only — a preview must never suspend a running guest.
      # Emit the would-pause preview in place of the warn+pause; paused stays
      # false (so both resume blocks below no-op) and VM_WAS_PAUSED stays 0.
      if is_dry_run; then
        log_info "vmbackup.sh" "backup_vm" "[DRY-RUN] Would pause VM: $vm_name for backup (no QEMU guest agent available) - skipping (read-only)"
      else
      log_warn "vmbackup.sh" "backup_vm" "QEMU guest agent not available for VM: $vm_name - pausing for backup"
      
      if pause_vm "$vm_name"; then
        paused=true
        backup_method="paused"  # Confirm backup method
        VM_WAS_PAUSED=1
        log_info "vmbackup.sh" "backup_vm" "VM paused successfully for backup"
      else
        log_error "vmbackup.sh" "backup_vm" "Failed to pause VM: $vm_name"
        final_status="FAILED"
        final_error="Failed to pause VM"
        set_backup_error "PAUSE_FAILED" "Failed to pause VM for backup (no QEMU agent)" "VM may not support pause or is in invalid state"
        _log_vm_backup_summary "$vm_name" "$backup_start_time" "$backup_start_epoch" "$final_status" "$final_backup_type" "$checkpoint_before" "0" "$final_error" "0" "$vm_policy" "$backup_dir" \
          "backup_failed" "Failed to pause VM for backup (no QEMU agent)" "${retry_count:-0}" "0"
        return 1
      fi
      fi
    else
      log_info "vmbackup.sh" "backup_vm" "QEMU guest agent available for VM: $vm_name - using agent-assisted backup"
    fi
  else
    log_info "vmbackup.sh" "backup_vm" "VM is offline, using crash-consistent backup"
  fi
  
  # Perform backup
  log_info "vmbackup.sh" "backup_vm" "Starting $backup_type backup operation for VM: $vm_name with compression level $VIRTNBD_COMPRESS_LEVEL"
  if ! perform_backup "$vm_name" "$backup_type" "$backup_dir"; then
    log_error "vmbackup.sh" "backup_vm" "Backup failed for VM: $vm_name"
    final_status="FAILED"
    
    # Use tracked error information if available, otherwise use generic message
    local error_code="${LAST_ERROR_CODE:-BACKUP_FAILED}"
    local error_detail="${LAST_ERROR_DETAIL:-virtnbdbackup failed}"
    final_error="${error_detail}"
    
    # Build event detail for session summary logging
    local fail_event_detail=$(build_event_detail "error" "$backup_type" "0" "$restore_points_before" "$backup_method" "$_ARCHIVE_CHAIN_ARCHIVED" "$error_detail")
    
    if [[ "$paused" == "true" ]]; then
      log_warn "vmbackup.sh" "backup_vm" "Resuming paused VM: $vm_name after failed backup"
      if ! resume_vm "$vm_name"; then
        log_error "vmbackup.sh" "backup_vm" "CRITICAL: Failed to resume VM: $vm_name after failed backup - VM may still be paused!"
        log_error "vmbackup.sh" "backup_vm" "MANUAL ACTION REQUIRED: Run 'virsh resume $vm_name' to restore VM operation"
      fi
    fi
    
    # G1 Fix: Call post_backup_hook on failure path for chain state tracking
    # DRY-RUN: Skip — post_backup_hook updates SQLite
    if ! is_dry_run; then
    local fail_duration=$(($(date +%s) - backup_start_epoch))
    post_backup_hook "$vm_name" "failed" "$backup_type" "0" "$fail_duration" "$final_error"
    fi
    
    _log_vm_backup_summary "$vm_name" "$backup_start_time" "$backup_start_epoch" "$final_status" "$final_backup_type" "$checkpoint_before" "0" "$final_error" "0" "$vm_policy" "$backup_dir" \
      "backup_failed" "$fail_event_detail" "${retry_count:-0}" "${_ARCHIVE_RESTORE_POINTS:-0}"
    return 1
  fi
  
  # Resume if paused
  if [[ "$paused" == "true" ]]; then
    log_info "vmbackup.sh" "backup_vm" "Resuming previously paused VM: $vm_name (was paused for backup due to missing QEMU agent)"
    if ! resume_vm "$vm_name"; then
      log_error "vmbackup.sh" "backup_vm" "CRITICAL: Failed to resume VM: $vm_name - VM may still be paused!"
      log_error "vmbackup.sh" "backup_vm" "MANUAL ACTION REQUIRED: Run 'virsh resume $vm_name' to restore VM operation"
    else
      local post_resume_state
      post_resume_state=$(lv_domain_state "$vm_name")
      log_info "vmbackup.sh" "backup_vm" "VM resumed successfully - current state: $post_resume_state"
    fi
  fi
  
  # Post-backup operations
  log_info "vmbackup.sh" "backup_vm" "Performing post-backup operations for VM: $vm_name"
  
  # Verify backup (skip during dry-run — no backup files to verify)
  if is_dry_run; then
    log_info "vmbackup.sh" "backup_vm" "[DRY-RUN] Skipping backup verification (no files written)"
  else
  log_info "vmbackup.sh" "backup_vm" "Verifying backup files written to disk for VM: $vm_name"
  if ! verify_backup "$vm_name" "$backup_dir"; then
    log_error "vmbackup.sh" "backup_vm" "Backup verification failed for VM: $vm_name - files may not have been written"
    _log_vm_backup_summary "$vm_name" "$backup_start_time" "$backup_start_epoch" \
      "FAILED" "$final_backup_type" "$checkpoint_before" "0" "verification failed" "0" "$vm_policy" "$backup_dir" \
      "backup_failed" "post-backup verification failed" "${retry_count:-0}" "${_ARCHIVE_RESTORE_POINTS:-0}"
    return 1
  fi
  log_info "vmbackup.sh" "backup_vm" "Backup verification passed for VM: $vm_name"
  fi
  
  # TPM Backup (non-fatal if TPM module not available or VM has no TPM)
  # DRY-RUN: Skip entirely — TPM backup copies files and runs guest-agent commands
  if is_dry_run; then
    if declare -f backup_vm_tpm &>/dev/null && declare -f has_tpm_device &>/dev/null && has_tpm_device "$vm_name"; then
      log_info "vmbackup.sh" "backup_vm" "[DRY-RUN] Would backup TPM state and extract BitLocker keys for VM: $vm_name"
    fi
  else
  log_info "vmbackup.sh" "backup_vm" "Starting TPM state backup for VM: $vm_name (if available)"
  local tpm_start_time=$(date +%s%N)  # Nanosecond precision
  if declare -f backup_vm_tpm &>/dev/null; then
    log_info "vmbackup.sh" "backup_vm" "TPM backup function available - proceeding with TPM backup"
    if backup_vm_tpm "$vm_name" "$backup_dir"; then
      local tpm_end_time=$(date +%s%N)
      local tpm_duration_ms=$(( (tpm_end_time - tpm_start_time) / 1000000 ))
      log_info "vmbackup.sh" "backup_vm" "TPM backup completed successfully for VM: $vm_name (duration: ${tpm_duration_ms}ms)"
      
      # Verify TPM backup directory exists and has content
      if [[ -d "$backup_dir/tpm-state" ]]; then
        local tpm_size=$(du -sh "$backup_dir/tpm-state" | cut -f1)
        local tpm_files=$(find "$backup_dir/tpm-state" -type f | wc -l)
        log_info "vmbackup.sh" "backup_vm" "TPM backup verified: total_size=$tpm_size, file_count=$tpm_files"
        log_info "vmbackup.sh" "backup_vm" "TPM backup location: $backup_dir/tpm-state"
        # Create TPM marker file for restore script to verify TPM was backed up
        echo "$(date '+%Y-%m-%d %H:%M:%S')" > "$backup_dir/.tpm-backup-marker"
        log_info "vmbackup.sh" "backup_vm" "TPM backup marker created for restore identification"
      else
        log_info "vmbackup.sh" "backup_vm" "TPM backup result: No TPM device on VM or backup was skipped (non-fatal)"
      fi
    else
      log_warn "vmbackup.sh" "backup_vm" "TPM backup failed or skipped for VM: $vm_name (continuing with disk backup only)"
    fi
  else
    log_info "vmbackup.sh" "backup_vm" "TPM backup skipped - no TPM device detected for this VM"
  fi
  fi  # end DRY_RUN guard
  
  # Native compression already applied by virtnbdbackup --compress flag
  log_info "vmbackup.sh" "backup_vm" "Native virtnbdbackup compression applied (level: $VIRTNBD_COMPRESS_LEVEL) for VM: $vm_name"
  
  # Monitor incremental size
  if [[ "$backup_type" == "auto" ]]; then
    log_info "vmbackup.sh" "backup_vm" "Monitoring auto-mode incremental backup size for sparseness issues"
    monitor_incremental_size "$backup_dir" "$vm_name"
  fi
  
  # Post-backup checkpoint coordination
  log_info "vmbackup.sh" "backup_vm" "Post-backup checkpoint coordination (backup_type=$backup_type)"
  if [[ "$backup_type" == "full" ]]; then
    log_info "vmbackup.sh" "backup_vm" "Full backup completed: checkpoint chain reset, ready for incremental backups"
  else
    log_info "vmbackup.sh" "backup_vm" "Incremental backup: checkpoint chain extended for next backup"
  fi
  
  # Backup VM configuration (XML definition)
  # DRY-RUN: Skip — backup_vm_config writes XML files to disk via virsh dumpxml
  if is_dry_run; then
    log_info "vmbackup.sh" "backup_vm" "[DRY-RUN] Would backup VM configuration (XML definition) to $backup_dir/config/"
  else
  log_info "vmbackup.sh" "backup_vm" "Backing up VM configuration (XML definition)"
  if ! backup_vm_config "$vm_name" "$backup_dir"; then
    log_warn "vmbackup.sh" "backup_vm" "Failed to backup VM configuration, but disk backup completed successfully"
    # Don't fail the entire backup if config backup fails - disk backup is primary
  fi
  fi
  
  local final_depth=$(get_checkpoint_depth "$vm_name")
  # Restore points = logical backup operations, not individual disk files
  restore_points_after=$(get_restore_point_count "$backup_dir")
  log_info "vmbackup.sh" "backup_vm" "Final QEMU checkpoint depth: $final_depth, Restore points on disk: $restore_points_after"
  
  # Calculate size metrics for session summary
  local this_backup_bytes=$(get_this_backup_size "$backup_dir" "$backup_start_epoch")
  local total_dir_bytes=$(get_total_dir_size "$backup_dir")
  
  # Build dynamic event detail
  local event_detail=$(build_event_detail "success" "$backup_type" "$this_backup_bytes" "$restore_points_after" "$backup_method" "$_ARCHIVE_CHAIN_ARCHIVED")
  
  # Success - set final status and log summary
  final_status="SUCCESS"
  final_size="$total_dir_bytes"
  
  #############################################################################
  # VM-First Integration: Post-backup hook
  # Handles: retention cleanup, chain lifecycle logging
  # DRY-RUN: Skip — post_backup_hook updates SQLite
  #############################################################################
  if is_dry_run; then
    log_info "vmbackup.sh" "backup_vm" "[DRY-RUN] Skipping post_backup_hook (no manifest/chain updates)"
  else
  local backup_duration=$(($(date +%s) - backup_start_epoch))
  post_backup_hook "$vm_name" "success" "$final_backup_type" "$final_size" "$backup_duration"
  fi
  
  # Pass restore_points_after (logical backup count), not checkpoint depth (virsh count)
  # NOTE: Pass this_backup_bytes (actual bytes written this run), NOT final_size (total dir size).
  # total_dir_bytes is separately computed inside sqlite_log_vm_backup via get_total_dir_size().
  _log_vm_backup_summary "$vm_name" "$backup_start_time" "$backup_start_epoch" "$final_status" "$final_backup_type" "$restore_points_before" "$restore_points_after" "" "$this_backup_bytes" "$vm_policy" "$backup_dir" \
    "backup_completed" "$event_detail" "${retry_count:-0}" "${_ARCHIVE_RESTORE_POINTS:-0}"
  return 0
}

#################################################################################
# MAIN EXECUTION
#################################################################################

# G3: Log interrupted chain to SQLite when signal received
# Detects current VM from lock file and marks chain as broken
_log_interrupted_chain() {
  local signal_name="${1:-UNKNOWN}"
  
  # FF-48b: select ONLY a lock OWNED by THIS process ($$). The newest lock in
  # LOCK_DIR can belong to a live FOREIGN holder — marking THAT VM's chain broken
  # writes a phantom SQLite row (the FF-48 mis-selection). Glob candidates, keep
  # only files whose content == $$, pick the newest OWN lock by mtime; if none,
  # current_vm stays empty and the existing empty-guard below writes nothing.
  local current_vm="" _own_lock="" _own_mtime=0 _cand _cpid _cmt
  for _cand in "$LOCK_DIR"/vmbackup-*.lock; do
    [[ -f "$_cand" ]] || continue
    _cpid=$(cat "$_cand" 2>/dev/null)
    [[ "$_cpid" == "$$" ]] || continue
    _cmt=$(stat -c %Y "$_cand" 2>/dev/null) || _cmt=0
    [[ "$_cmt" -ge "$_own_mtime" ]] && { _own_mtime=$_cmt; _own_lock=$_cand; }
  done
  # FF-156: derive the vm_fs_name TOKEN from the OWNED lock filename via
  # parameter expansion. The old greedy sed 's|.*vmbackup-\(.*\)\.lock|\1|' let
  # '.*' consume through the LAST 'vmbackup-', truncating tokens for VMs named
  # 'vmbackup-*'. ## strips up to the last '/vmbackup-' (always the lock-file
  # prefix), so BACKUP_PATH may itself contain 'vmbackup-'.
  [[ -n "$_own_lock" ]] && { current_vm="${_own_lock##*/vmbackup-}"; current_vm="${current_vm%.lock}"; }

  if [[ -z "$current_vm" ]]; then
    log_debug "vmbackup.sh" "_log_interrupted_chain" "No active backup detected"
    return 0
  fi

  # M2 (118-spaces): the lock filename is the vm_fs_name TOKEN (LOCK-01). Map it
  # back to the REAL libvirt name so the chain-broken SQLite row + policy lookup
  # below are keyed by the real name (a token key would write a phantom row).
  declare -f _prune_real_name >/dev/null 2>&1 && current_vm=$(_prune_real_name "$current_vm")

  log_warn "vmbackup.sh" "_log_interrupted_chain" \
    "Signal $signal_name received during backup of: $current_vm"
  
  # Only mark chain as broken if virtnbdbackup is actually running.
  # Interrupts during pre-backup phases (fstrim, validation, VSS pause) should NOT
  # mark a healthy chain as broken — the chain data is untouched at that point.
  if [[ "$_BACKUP_IN_PROGRESS" != "true" ]]; then
    log_info "vmbackup.sh" "_log_interrupted_chain" \
      "Interrupted during pre/post-backup phase (not during virtnbdbackup) - chain is intact, not marking broken"
    return 0
  fi
  
  # Get chain context
  local policy
  policy=$(get_vm_rotation_policy "$current_vm" 2>/dev/null || echo "monthly")
  local period_id
  period_id=$(get_period_id "$policy" 2>/dev/null || date +%Y%m)
  # DUP-10: disk-derived chain id from in-flight backup dir.
  # Signal-handler context — must remain fast and tolerate the dir not
  # existing yet (early-trap case → empty chain_id → block short-circuits).
  local chain_id
  chain_id=$(get_active_chain_id_from_disk "$current_vm" 2>/dev/null || echo "")

  if [[ -n "$chain_id" ]]; then
    local backup_dir
    backup_dir=$(get_vm_backup_dir "$current_vm" 2>/dev/null || echo "")
    local checkpoint=0
    [[ -n "$backup_dir" && -d "$backup_dir" ]] && \
      checkpoint=$(get_restore_point_count "$backup_dir" 2>/dev/null || echo "0")

    if declare -f sqlite_mark_chain_broken >/dev/null 2>&1; then
      sqlite_mark_chain_broken "$current_vm" "$period_id" "$chain_id" \
        "$checkpoint" "interrupted by $signal_name"
      log_warn "vmbackup.sh" "_log_interrupted_chain" \
        "Marked chain $chain_id as broken at checkpoint $checkpoint"
    fi
  fi
}

# INT-24: notifier return-code dispatcher.
#
# send_backup_report() in modules/email_report_module.sh returns three values:
#   0 → report delivered to MTA
#   1 → transport failure (SMTP error, sendmail missing, etc.)
#   2 → intentionally skipped (EMAIL_ON_SUCCESS=no, EMAIL_ON_FAILURE=no,
#       module disabled). NOT a failure — this is operator-configured.
#
# The previous `if send_backup_report ...; then INFO; else WARN; fi` pattern
# collapsed rc=2 into the failure branch, emitting a misleading WARN on every
# successful run when the operator only wanted failure mail. It also left
# _EMAIL_SENT=false on rc=2, which could cause downstream cleanup paths to
# re-attempt delivery.
#
# This helper centralises the three-way mapping and the _EMAIL_SENT side-effect.
# All four send_backup_report call sites (main, cleanup_on_exit, handle_sigterm,
# replicate-only) use it.
#
# Args: $1=rc from send_backup_report, $2=call-site name (for log context).
# Originally proposed in doutsis/vmbackup#4 by @hostarts (as _handle_notifier_rc).
# Ported to 0.6.0 as INT-24 (email-only scope; Slack PR was not adopted).
_handle_notifier_rc() {
    local rc="${1:-0}"
    local site="${2:-unknown}"
    case "$rc" in
        0)
            log_info "vmbackup.sh" "$site" "Email report sent successfully"
            _EMAIL_SENT=true
            ;;
        2)
            log_debug "vmbackup.sh" "$site" \
                "Email report intentionally skipped (per EMAIL_ON_SUCCESS / EMAIL_ON_FAILURE config or module disabled)"
            # Treat as "handled" so downstream paths don't retry — the operator
            # explicitly opted out of this notification.
            _EMAIL_SENT=true
            ;;
        *)
            log_warn "vmbackup.sh" "$site" \
                "Failed to send email report (rc=$rc; backup data preserved)"
            # Do NOT set _EMAIL_SENT — cleanup_on_exit may retry on a fresh path.
            ;;
    esac
}

# Slack-side parallel of _handle_notifier_rc. Sets _SLACK_SENT so the four
# fire-once sites don't double-post. Same 3-way rc: 0=sent, 2=skipped, *=failed.
_handle_slack_rc() {
    local rc="${1:-0}"
    local site="${2:-unknown}"
    case "$rc" in
        0)
            log_info "vmbackup.sh" "$site" "Slack notification sent successfully"
            _SLACK_SENT=true
            ;;
        2)
            log_debug "vmbackup.sh" "$site" \
                "Slack notification intentionally skipped (per SLACK_ON_SUCCESS / SLACK_ON_FAILURE config or module disabled)"
            _SLACK_SENT=true
            ;;
        *)
            log_warn "vmbackup.sh" "$site" \
                "Failed to send Slack notification (rc=$rc; backup data preserved)"
            ;;
    esac
}

# MEDIUM FIX #3: Cleanup handler for signal exits to remove temporary files
cleanup_on_exit() {
  local exit_code=$?
  
  # Log interruption/timeout signals clearly
  if [[ $exit_code -eq 130 ]]; then
    log_error "vmbackup.sh" "cleanup_on_exit" "=== BACKUP SESSION INTERRUPTED BY USER (SIGINT) ==="
  elif [[ $exit_code -eq 143 ]]; then
    log_error "vmbackup.sh" "cleanup_on_exit" "=== BACKUP SESSION KILLED BY TIMEOUT/SIGTERM ==="
    log_error "vmbackup.sh" "cleanup_on_exit" "TROUBLESHOOTING:"
    log_error "vmbackup.sh" "cleanup_on_exit" "  1. Check systemd timeout: systemctl show vmbackup.service | grep TimeoutStartUSec"
    log_error "vmbackup.sh" "cleanup_on_exit" "  2. Check backup size and speed: Consider increasing timeout if backups are large"
    log_error "vmbackup.sh" "cleanup_on_exit" "  3. Next run will auto-cleanup stale locks and orphaned checkpoints"
  fi
  
  # Finalize SQLite session if not already ended
  # Normal exits finalize in main()/prune/replicate-only, but early errors,
  # unhandled exits, or edge cases may skip those. This catch-all is safe
  # because sqlite_session_end() has an idempotency guard (_SQLITE_SESSION_ENDED).
  if sqlite_is_available 2>/dev/null && [[ -n "${SQLITE_CURRENT_SESSION_ID:-}" ]] && ! is_dry_run; then
    local _se_rc=0
    if [[ $exit_code -eq 130 ]] || [[ $exit_code -eq 143 ]]; then
      # Signal exit — count results from what we processed so far
      local int_total=0 int_success=0 int_failed=0 int_skipped=0 int_excluded=0
      for result in "${VM_BACKUP_RESULTS[@]}"; do
        IFS='|' read -r vm status rest <<< "$result"
        ((int_total++))
        case "$status" in
          SUCCESS) ((int_success++)) ;;
          FAILED) ((int_failed++)) ;;
          SKIPPED) ((int_skipped++)) ;;
          EXCLUDED) ((int_excluded++)) ;;
        esac
      done
      local int_status="interrupted"
      [[ $exit_code -eq 143 ]] && int_status="killed"
      sqlite_session_end "$int_total" "$int_success" "$int_failed" "$int_skipped" "$int_excluded" "0" "$int_status" || _se_rc=$?
      # rc=2 means main() already finalized the row — trap is a duplicate call (no-op). Stay quiet.
      if (( _se_rc != 2 )); then
        log_info "vmbackup.sh" "cleanup_on_exit" "SQLite session finalized as '$int_status'"
      fi
    else
      # Non-signal exit that didn't finalize normally (early error, unexpected path)
      sqlite_session_end "0" "0" "0" "0" "0" "0" "incomplete" || _se_rc=$?
      # rc=2 means main() already finalized the row cleanly — suppress the misleading
      # 'incomplete' WARN per INT-13. Only warn when the trap actually wrote 'incomplete'.
      if (( _se_rc != 2 )); then
        log_warn "vmbackup.sh" "cleanup_on_exit" "SQLite session finalized as 'incomplete' (exit code $exit_code)"
      fi
    fi
  fi

  # Email on pre-flight / non-signal failure (104 Change B).
  # Only fires when:
  #   - exit code is non-zero (something went wrong),
  #   - a SQLite session was registered (excludes failures earlier than
  #     sqlite_session_start: check_dependencies, lock contention, missing
  #     config, and all read-only modes such as --status / --prune list),
  #   - no email was already sent by main()'s normal path or by handle_sigterm,
  #   - DRY_RUN is off,
  #   - the email module file is present.
  # Catches all three pre-flight aborts (check_backup_destination,
  # check_scratch_path, check_disk_space) which previously failed silently.
  if [[ $exit_code -ne 0 ]] && \
     [[ -n "${SQLITE_CURRENT_SESSION_ID:-}" ]] && \
     [[ "${_EMAIL_SENT:-false}" != "true" ]] && \
     ! is_dry_run && \
     [[ -f "${SCRIPT_DIR}/modules/email_report_module.sh" ]]; then
    log_info "vmbackup.sh" "cleanup_on_exit" "Sending email report after non-zero exit (code $exit_code)"
    # shellcheck source=/dev/null
    source "${SCRIPT_DIR}/modules/email_report_module.sh"
    if load_email_config; then
      local _session_end_time _rc=0
      _session_end_time=$(date '+%Y-%m-%d %H:%M:%S %Z')
      send_backup_report "${session_start_time:-unknown}" "$_session_end_time" "failed" || _rc=$?
      _handle_notifier_rc "$_rc" cleanup_on_exit
    else
      log_debug "vmbackup.sh" "cleanup_on_exit" "Email disabled or not configured for this instance"
    fi
  fi

  # Slack notification on non-zero exit — parallel to the email block above.
  # Fires from the same gate; independent of email.
  if [[ $exit_code -ne 0 ]] && \
     [[ -n "${SQLITE_CURRENT_SESSION_ID:-}" ]] && \
     [[ "${_SLACK_SENT:-false}" != "true" ]] && \
     ! is_dry_run && \
     [[ -f "${SCRIPT_DIR}/modules/slack_notification_module.sh" ]]; then
    # shellcheck source=/dev/null
    source "${SCRIPT_DIR}/modules/slack_notification_module.sh"
    if load_slack_config; then
      local _slack_end_time _src=0
      _slack_end_time=$(date '+%Y-%m-%d %H:%M:%S %Z')
      send_slack_notification "${session_start_time:-unknown}" "$_slack_end_time" "failed" || _src=$?
      _handle_slack_rc "$_src" cleanup_on_exit
    fi
  fi

  log_info "vmbackup.sh" "cleanup_on_exit" "Cleaning up temporary files before exit (exit code: $exit_code)"
  
  # Remove stale lock files — only those whose owning process is no longer running.
  # Uses vmbackup-*.lock glob to avoid touching locks from other tools.
  if [[ -d "$LOCK_DIR" ]]; then
    local stale_found=false
    while IFS= read -r lock_file; do
      [[ -z "$lock_file" ]] && continue
      local lock_pid
      lock_pid=$(cat "$lock_file" 2>/dev/null)
      # Only delete if the owning process is gone (or PID is empty/unreadable)
      if [[ -z "$lock_pid" ]] || ! kill -0 "$lock_pid" 2>/dev/null; then
        if is_dry_run; then
          log_debug "vmbackup.sh" "cleanup_on_exit" "[DRY-RUN] Would remove stale lock: $(basename "$lock_file") (PID ${lock_pid:-unknown} not running) - skipping (read-only)"
          stale_found=true
        elif rm -f "$lock_file"; then
          log_debug "vmbackup.sh" "cleanup_on_exit" "Deleted stale lock file: $(basename "$lock_file") (PID ${lock_pid:-unknown} not running)"
          stale_found=true
        fi
      fi
    done < <(find "$LOCK_DIR" -name "vmbackup-*.lock" -type f -mmin +60 2>/dev/null)
    if [[ "$stale_found" != true ]]; then
      log_debug "vmbackup.sh" "cleanup_on_exit" "No stale lock files found"
    fi
  fi
  
  # Remove temporary scratch files
  if [[ -n "$VIRTNBD_SCRATCH_DIR" && -d "$VIRTNBD_SCRATCH_DIR" ]]; then
    # Clean only our backup-related temp files, not other applications' files
    local virtnbd_temps=$(find "$VIRTNBD_SCRATCH_DIR" -maxdepth 1 -name "*virtnbdbackup*" -type f 2>/dev/null)
    if [[ -n "$virtnbd_temps" ]]; then
      while IFS= read -r temp_file; do
        if rm -f "$temp_file"; then
          log_debug "vmbackup.sh" "cleanup_on_exit" "Deleted virtnbdbackup temp: $(basename "$temp_file")"
        fi
      done <<< "$virtnbd_temps"
    fi
    
    local vmbackup_temps=$(find "$VIRTNBD_SCRATCH_DIR" -maxdepth 1 -name "*vmbackup*" -type f 2>/dev/null)
    if [[ -n "$vmbackup_temps" ]]; then
      while IFS= read -r temp_file; do
        if rm -f "$temp_file"; then
          log_debug "vmbackup.sh" "cleanup_on_exit" "Deleted vmbackup temp: $(basename "$temp_file")"
        fi
      done <<< "$vmbackup_temps"
    fi
  fi
  
  # Clean recovery flags for any stale operations
  if [[ -d "$TEMP_DIR" ]]; then
    local recovery_flags=$(find "$TEMP_DIR" -name "vmbackup-recovery-*.flag" -type f 2>/dev/null)
    if [[ -n "$recovery_flags" ]]; then
      if is_dry_run; then
        log_info "vmbackup.sh" "cleanup_on_exit" "[DRY-RUN] Would clear stale recovery flag(s) under $TEMP_DIR — leaving in place (dry-run mutates nothing)"
      else
        while IFS= read -r flag_file; do
          if rm -f "$flag_file"; then
            log_debug "vmbackup.sh" "cleanup_on_exit" "Deleted recovery flag: $(basename "$flag_file")"
          fi
        done <<< "$recovery_flags"
      fi
    fi
  fi
  
  # Remove global session PID file (only our own)
  if [[ -f "$STATE_DIR/vmbackup.pid" ]]; then
    local pid_content
    pid_content=$(cat "$STATE_DIR/vmbackup.pid" 2>/dev/null)
    if [[ "$pid_content" == "$$" ]]; then
      rm -f "$STATE_DIR/vmbackup.pid"
      log_debug "vmbackup.sh" "cleanup_on_exit" "Removed session PID file"
    fi
  fi
  
  log_info "vmbackup.sh" "cleanup_on_exit" "Temporary file cleanup complete"
  
  return $exit_code
}

# Emergency handler for CTRL+Z (SIGTSTP) - suspend signal
# When user presses CTRL+Z, cleanup current VM before suspending
handle_sigtstp() {
  # FF-48b: select ONLY a lock OWNED by THIS process ($$). The newest lock in
  # LOCK_DIR can belong to a live FOREIGN holder (e.g. an in-flight vmrestore of
  # this same VM); emergency_cleanup rm's that lock + the VM's *.incomplete/
  # *.partial files — bulldozing a live foreign holder is the FF-48 class. Glob
  # the candidates (no ls-parsing/word-split; the -f check absorbs the no-match
  # literal, so no nullglob needed), keep only files whose content == $$, pick the
  # newest OWN lock by mtime; if none, do NO cleanup (suspend cleanly below).
  local current_vm="" _own_lock="" _own_mtime=0 _cand _cpid _cmt
  for _cand in "$LOCK_DIR"/vmbackup-*.lock; do
    [[ -f "$_cand" ]] || continue
    _cpid=$(cat "$_cand" 2>/dev/null)
    [[ "$_cpid" == "$$" ]] || continue
    _cmt=$(stat -c %Y "$_cand" 2>/dev/null) || _cmt=0
    [[ "$_cmt" -ge "$_own_mtime" ]] && { _own_mtime=$_cmt; _own_lock=$_cand; }
  done
  # FF-156: derive the vm_fs_name TOKEN via parameter expansion (see twin site
  # in _log_interrupted_chain). The old greedy sed truncated tokens for VMs
  # literally named 'vmbackup-*'. ## strips up to the last '/vmbackup-'.
  [[ -n "$_own_lock" ]] && { current_vm="${_own_lock##*/vmbackup-}"; current_vm="${current_vm%.lock}"; }
  # M2 (118-spaces): map the lock-filename token back to the real libvirt name.
  [[ -n "$current_vm" ]] && declare -f _prune_real_name >/dev/null 2>&1 && current_vm=$(_prune_real_name "$current_vm")

  if [[ -n "$current_vm" ]]; then
    log_warn "vmbackup.sh" "handle_sigtstp" "CTRL+Z detected during backup of $current_vm - performing emergency cleanup"
    
    # G3: Log interrupted chain to SQLite before cleanup
    _log_interrupted_chain "SIGTSTP"
    
    emergency_cleanup_current_vm "$current_vm"
  fi
  
  # Resume normal signal handling and suspend
  trap - SIGTSTP
  kill -SIGTSTP $$
}

# Handler for SIGTERM - sent by systemd timeout or manual stop
# Ensures email report is sent before exit
handle_sigterm() {
  log_error "vmbackup.sh" "handle_sigterm" "Script interrupted by SIGTERM (timeout or systemd stop)"
  log_error "vmbackup.sh" "handle_sigterm" "Recovery: Run vmbackup.sh again - stale locks and incomplete checkpoints will be cleaned up"
  
  # G3: Log interrupted chain to SQLite
  _log_interrupted_chain "SIGTERM"
  
  # Attempt to send email report before exit
  local session_end_time=$(date '+%Y-%m-%d %H:%M:%S %Z')
  if is_dry_run; then
    log_info "vmbackup.sh" "handle_sigterm" "[DRY-RUN] Skipping email report"
  elif [[ "${_EMAIL_SENT:-false}" == "true" ]]; then
    log_info "vmbackup.sh" "handle_sigterm" "Email already sent — skipping duplicate report"
  elif [[ -f "${SCRIPT_DIR}/modules/email_report_module.sh" ]]; then
    log_info "vmbackup.sh" "handle_sigterm" "Loading email report module..."
    source "${SCRIPT_DIR}/modules/email_report_module.sh"
    if load_email_config; then
      log_info "vmbackup.sh" "handle_sigterm" "Sending email report before SIGTERM exit..."
      local _rc=0
      send_backup_report "${session_start_time:-unknown}" "$session_end_time" "failed" || _rc=$?
      _handle_notifier_rc "$_rc" handle_sigterm
    else
      log_debug "vmbackup.sh" "handle_sigterm" "Email disabled or not configured for this instance"
    fi
  fi

  # Slack (parallel to the email block above)
  if ! is_dry_run && \
     [[ "${_SLACK_SENT:-false}" != "true" ]] && \
     [[ -f "${SCRIPT_DIR}/modules/slack_notification_module.sh" ]]; then
    source "${SCRIPT_DIR}/modules/slack_notification_module.sh"
    if load_slack_config; then
      local _src=0
      send_slack_notification "${session_start_time:-unknown}" "$session_end_time" "failed" || _src=$?
      _handle_slack_rc "$_src" handle_sigterm
    fi
  fi
  
  exit 143
}

#################################################################################
# PRUNE MODE — Standalone Backup Cleanup
#
# Targeted on-demand cleanup of backup data — archives, periods, or entire VMs.
# Runs outside of a backup session (no session_id, no email report).
# Logs to ${BACKUP_PATH}_state/logs/vmprune.log
#################################################################################

# Human-readable size formatting (pure bash, no bc dependency)
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

# FF-158: safe byte count for a path. `du -sb | cut -f1 || echo 0` under
# pipefail appends a 2nd line ("0") when du fails mid-scan but still prints a
# total, yielding a two-line value that breaks the caller's arithmetic. Capture
# then validate a single integer (0 on any failure/non-numeric).
_du_bytes() {
    local _n
    _n=$(du -sb "$1" 2>/dev/null | cut -f1)
    [[ "$_n" =~ ^[0-9]+$ ]] || _n=0
    printf '%s' "$_n"
}

# Discovery listing — filesystem scan, size reporting, prune command hints
# Args: $1 - vm_filter (optional, specific VM name)
# Uses: BACKUP_PATH
_prune_list() {
    local vm_filter="${1:-}"

    # Presenter-scope state for walker callbacks (U1: reduce inside cb's).
    _PRUNE_LIST_TOTAL_BYTES=0
    _PRUNE_LIST_TOTAL_ARCHIVE_BYTES=0
    _PRUNE_LIST_VM_COUNT=0
    _PRUNE_LIST_VM_FILTER="$vm_filter"
    # FF-157: the walker passes the on-disk FOLDER token (vm_fs_name slug) to the
    # callback, but --vm carries the real libvirt name. For spaced/special names
    # the raw name never equals the token, so precompute the current + pre-118
    # legacy token forms; the callback matches any of the three.
    _PRUNE_LIST_VM_FILTER_TOKEN=""
    _PRUNE_LIST_VM_FILTER_TOKEN_LEGACY=""
    if [[ -n "$vm_filter" ]]; then
        _PRUNE_LIST_VM_FILTER_TOKEN=$(vm_fs_name "$vm_filter" 2>/dev/null) || _PRUNE_LIST_VM_FILTER_TOKEN=""
        declare -f vm_fs_name_legacy >/dev/null 2>&1 && \
            _PRUNE_LIST_VM_FILTER_TOKEN_LEGACY=$(vm_fs_name_legacy "$vm_filter" 2>/dev/null)
    fi
    if [[ -n "$vm_filter" ]]; then
        _PRUNE_LIST_SHOW_CHAIN_CMDS=true
    else
        _PRUNE_LIST_SHOW_CHAIN_CMDS=false
    fi

    echo ""
    echo "vmbackup prune — backup inventory"
    echo ""

    # UNI-309: walk the backup tree via lib/backup_walker.sh. VM- and
    # period-level skip-list enforcement lives in the walker (B).
    walk_backup_tree "$BACKUP_PATH" _prune_list_vm_cb _prune_list_period_cb

    if [[ $_PRUNE_LIST_VM_COUNT -eq 0 ]]; then
        if [[ -n "$vm_filter" ]]; then
            echo "No backup data found for VM: $vm_filter"
            echo "Check --prune list (without --vm) to see all VMs."
        else
            echo "No backup data found in: $BACKUP_PATH"
        fi
        return 0
    fi

    # Footer
    local total_size archives_size
    total_size=$(_format_size "$_PRUNE_LIST_TOTAL_BYTES")
    archives_size=$(_format_size "$_PRUNE_LIST_TOTAL_ARCHIVE_BYTES")

    if [[ -z "$vm_filter" ]]; then
        local total_line
        total_line=$(printf "Total: %d VMs, %s (archives: %s)" \
            "$_PRUNE_LIST_VM_COUNT" "$total_size" "$archives_size")
        if [[ $_PRUNE_LIST_TOTAL_ARCHIVE_BYTES -gt 0 ]]; then
            local pad=$(( 44 - ${#total_line} ))
            (( pad < 4 )) && pad=4
            printf "%s%*s%s\n" "$total_line" "$pad" "" "# --prune archives"
        else
            printf "%s\n" "$total_line"
        fi
    else
        printf "Total: %s (archives: %s)\n" "$total_size" "$archives_size"
    fi
    echo ""
}

# Walker callback: VM-level header for `_prune_list`. Returns 0 to allow the
# walker to iterate this VM's periods, 1 to skip (when --vm filter excludes).
_prune_list_vm_cb() {
    local vm_name="$1"
    local vm_dir="$2"

    # Honour --vm filter (walker passes the on-disk folder token as $vm_name).
    # FF-157: match the raw name (token supplied verbatim), the current
    # vm_fs_name token, or the pre-118 legacy token, so spaced/special names
    # resolve to their folder.
    if [[ -n "$_PRUNE_LIST_VM_FILTER" \
          && "$vm_name" != "$_PRUNE_LIST_VM_FILTER" \
          && "$vm_name" != "$_PRUNE_LIST_VM_FILTER_TOKEN" \
          && "$vm_name" != "$_PRUNE_LIST_VM_FILTER_TOKEN_LEGACY" ]]; then
        return 1
    fi

    local vm_bytes vm_size
    vm_bytes=$(_du_bytes "$vm_dir")
    vm_size=$(_format_size "$vm_bytes")
    _PRUNE_LIST_TOTAL_BYTES=$(( _PRUNE_LIST_TOTAL_BYTES + vm_bytes ))
    (( _PRUNE_LIST_VM_COUNT++ )) || true

    printf "%-30s %10s    %s\n" "$vm_name" "$vm_size" "# --prune all --vm ${vm_name}"
    return 0
}

# Walker callback: per-period block for `_prune_list`. Computes active-chain
# size, archive count, and per-chain breakdown (single-VM mode includes the
# `# --prune chain:` hint).
_prune_list_period_cb() {
    local vm_name="$1"
    # vm_dir unused here (passed by walker as $2); period_id and period_dir follow.
    local period_id="$3"
    local period_dir="$4"

    local period_bytes period_size
    period_bytes=$(_du_bytes "$period_dir")
    period_size=$(_format_size "$period_bytes")

    printf "  %-28s %10s    %s\n" "$period_id" "$period_size" \
        "# --prune period:${period_id} --vm ${vm_name}"

    # Active chain stats (everything except .archives)
    local active_bytes=0
    local data_file_count=0
    data_file_count=$(find "$period_dir" -maxdepth 1 -name "*.data" -type f 2>/dev/null | wc -l)
    local archives_dir="${period_dir}.archives"
    local archives_total_bytes=0
    if [[ -d "$archives_dir" ]]; then
        archives_total_bytes=$(_du_bytes "$archives_dir")
    fi
    active_bytes=$(( period_bytes - archives_total_bytes ))
    local active_size
    active_size=$(_format_size "$active_bytes")

    printf "    %-26s %10s    (%d data files)\n" "active chain" "$active_size" "$data_file_count"

    # Archives
    if [[ -d "$archives_dir" ]]; then
        local chain_count=0
        local chain_dir
        for chain_dir in "$archives_dir"/chain-*; do
            [[ -d "$chain_dir" ]] && (( chain_count++ ))
        done

        if [[ $chain_count -gt 0 ]]; then
            local archives_size
            archives_size=$(_format_size "$archives_total_bytes")
            _PRUNE_LIST_TOTAL_ARCHIVE_BYTES=$(( _PRUNE_LIST_TOTAL_ARCHIVE_BYTES + archives_total_bytes ))

            local chain_word="chains"
            [[ $chain_count -eq 1 ]] && chain_word="chain"
            printf "    %-26s %10s    %-14s %s\n" "archives" "$archives_size" \
                "(${chain_count} ${chain_word})" "# --prune archives:${period_id} --vm ${vm_name}"

            for chain_dir in "$archives_dir"/chain-*; do
                [[ -d "$chain_dir" ]] || continue
                local chain_name chain_bytes chain_size
                chain_name=$(basename "$chain_dir")
                chain_bytes=$(_du_bytes "$chain_dir")
                chain_size=$(_format_size "$chain_bytes")

                if [[ "$_PRUNE_LIST_SHOW_CHAIN_CMDS" == "true" ]]; then
                    printf "      %-24s %10s    %s\n" "$chain_name" "$chain_size" \
                        "# --prune chain:${chain_name} --vm ${vm_name}"
                else
                    printf "      %-24s %10s\n" "$chain_name" "$chain_size"
                fi
            done
        else
            printf "    %-26s %10s\n" "archives" "—"
        fi
    else
        printf "    %-26s %10s\n" "archives" "—"
    fi

    echo ""
}

#################################################################################
# REPLICATE-ONLY MODE
#################################################################################

# Log a replication-only session summary (no VM table, replication results only)
_log_replicate_only_summary() {
  local mode="$1"

  log_info "vmbackup.sh" "main" ""
  log_info "vmbackup.sh" "main" "╔══════════════════════════════════════════════════════════════════════════════════════════════════════════╗"
  log_info "vmbackup.sh" "main" "║                              REPLICATION-ONLY SESSION SUMMARY                                          ║"
  log_info "vmbackup.sh" "main" "╠══════════════════════════════════════════════════════════════════════════════════════════════════════════╣"
  log_info "vmbackup.sh" "main" "║  Mode: $mode  (no backups, retention, or FSTRIM)"
  log_info "vmbackup.sh" "main" "╠══════════════════════════════════════════════════════════════════════════════════════════════════════════╣"

  # Local replication summary
  if [[ "$mode" == "local" || "$mode" == "both" ]]; then
    if [[ "${LOCAL_REPLICATION_MODULE_AVAILABLE:-0}" -eq 1 ]] && declare -f get_replication_summary >/dev/null 2>&1; then
      local local_repl_summary
      local_repl_summary=$(get_replication_summary 2>/dev/null)
      if [[ -n "$local_repl_summary" ]]; then
        log_info "vmbackup.sh" "main" "║  LOCAL REPLICATION"
        while IFS= read -r line; do
          [[ -n "$line" ]] && log_info "vmbackup.sh" "main" "║    $line"
        done <<< "$local_repl_summary"
      fi
    else
      log_info "vmbackup.sh" "main" "║  LOCAL REPLICATION: Not configured"
    fi
    log_info "vmbackup.sh" "main" "║"
  fi

  # Cloud replication summary
  if [[ "$mode" == "cloud" || "$mode" == "both" ]]; then
    if [[ "${CLOUD_REPLICATION_MODULE_AVAILABLE:-0}" -eq 1 ]] && declare -f get_cloud_replication_summary >/dev/null 2>&1; then
      local cloud_repl_summary
      cloud_repl_summary=$(get_cloud_replication_summary 2>/dev/null)
      if [[ -n "$cloud_repl_summary" ]]; then
        log_info "vmbackup.sh" "main" "║  CLOUD REPLICATION"
        while IFS= read -r line; do
          [[ -n "$line" ]] && log_info "vmbackup.sh" "main" "║    $line"
        done <<< "$cloud_repl_summary"
      fi
    else
      log_info "vmbackup.sh" "main" "║  CLOUD REPLICATION: Not configured"
    fi
  fi

  log_info "vmbackup.sh" "main" "╚══════════════════════════════════════════════════════════════════════════════════════════════════════════╝"
  log_info "vmbackup.sh" "main" ""
}

# Run replication without performing backups
# Arguments:
#   $1 - mode: "local", "cloud", or "both"
#   $2 - session_start_time (for email report)
# Returns: 0 if all requested replication succeeded (or nothing to do), 1 if any failed
_run_replicate_only() {
  local mode="$1"
  local session_start_time="$2"

  log_info "vmbackup.sh" "main" "===== REPLICATE-ONLY MODE START (scope=$mode) ====="

  # Verify BACKUP_PATH exists (replication reads from it)
  if [[ ! -d "$BACKUP_PATH" ]]; then
    log_error "vmbackup.sh" "main" "BACKUP_PATH does not exist: $BACKUP_PATH"
    log_error "vmbackup.sh" "main" "Nothing to replicate — run a backup first"
    _sqlite_end_replicate_only "failed"
    return 1
  fi

  # Determine what modules are available for the requested scope
  local local_repl_needed=0
  local cloud_repl_needed=0

  if [[ "$mode" == "local" || "$mode" == "both" ]]; then
    if [[ "${LOCAL_REPLICATION_MODULE_AVAILABLE:-0}" -eq 1 ]]; then
      local_repl_needed=1
    else
      log_warn "vmbackup.sh" "main" "Local replication requested but not configured/available — skipping"
    fi
  fi

  if [[ "$mode" == "cloud" || "$mode" == "both" ]]; then
    if [[ "${CLOUD_REPLICATION_MODULE_AVAILABLE:-0}" -eq 1 ]]; then
      cloud_repl_needed=1
    else
      log_warn "vmbackup.sh" "main" "Cloud replication requested but not configured/available — skipping"
    fi
  fi

  # Nothing to do — exit cleanly
  if [[ $local_repl_needed -eq 0 && $cloud_repl_needed -eq 0 ]]; then
    log_info "vmbackup.sh" "main" "No replication modules available for scope=$mode — nothing to do"
    _log_replicate_only_summary "$mode"
    _sqlite_end_replicate_only "success"
    log_info "vmbackup.sh" "main" "===== REPLICATE-ONLY MODE END (exit=0) ====="
    return 0
  fi

  # DRY-RUN: report what would run, skip execution
  if is_dry_run; then
    log_info "vmbackup.sh" "main" "[DRY-RUN] Would run replication (scope=$mode, local=$local_repl_needed, cloud=$cloud_repl_needed)"
    _log_replicate_only_summary "$mode"
    _sqlite_end_replicate_only "success"
    log_info "vmbackup.sh" "main" "===== REPLICATE-ONLY MODE END (dry-run, exit=0) ====="
    return 0
  fi

  # Check cancellation flag before starting
  if is_replication_cancelled; then
    log_warn "vmbackup.sh" "main" "Replication cancellation flag detected — skipping replication"
    clear_replication_cancel_flag
    _log_replicate_only_summary "$mode"
    _sqlite_end_replicate_only "success"
    log_info "vmbackup.sh" "main" "===== REPLICATE-ONLY MODE END (cancelled, exit=0) ====="
    return 0
  fi

  # Execute replication — honour REPLICATION_ORDER setting
  local replication_mode="${REPLICATION_ORDER:-simultaneous}"
  local local_repl_pid=""
  local cloud_repl_pid=""
  local local_repl_result=0
  local cloud_repl_result=0
  local local_repl_config="${SCRIPT_DIR}/config/${CONFIG_INSTANCE:-default}/replication_local.conf"

  log_info "vmbackup.sh" "main" "Replication order: $replication_mode"

  if [[ "$replication_mode" == "simultaneous" ]]; then
    # Start local in background
    if [[ $local_repl_needed -eq 1 ]]; then
      log_info "vmbackup.sh" "main" "Starting local replication (background)"
      (
        if run_local_replication_batch "$BACKUP_PATH"; then exit 0; else exit 1; fi
      ) &
      local_repl_pid=$!
    fi
    # Start cloud in background
    if [[ $cloud_repl_needed -eq 1 ]]; then
      log_info "vmbackup.sh" "main" "Starting cloud replication (background)"
      (
        if invoke_cloud_replication "$BACKUP_PATH"; then exit 0; else exit 1; fi
      ) &
      cloud_repl_pid=$!
    fi
    # Wait for both
    if [[ -n "$local_repl_pid" ]]; then
      wait $local_repl_pid; local_repl_result=$?
      log_info "vmbackup.sh" "main" "Local replication finished (rc=$local_repl_result)"
    fi
    if [[ -n "$cloud_repl_pid" ]]; then
      wait $cloud_repl_pid; cloud_repl_result=$?
      log_info "vmbackup.sh" "main" "Cloud replication finished (rc=$cloud_repl_result)"
    fi

  elif [[ "$replication_mode" == "local_first" ]]; then
    if [[ $local_repl_needed -eq 1 ]]; then
      log_info "vmbackup.sh" "main" "Starting local replication"
      [[ -f "$local_repl_config" ]] && source "$local_repl_config"
      run_local_replication_batch "$BACKUP_PATH" && local_repl_result=0 || local_repl_result=1
      log_info "vmbackup.sh" "main" "Local replication finished (rc=$local_repl_result)"
    fi
    if [[ $cloud_repl_needed -eq 1 ]]; then
      log_info "vmbackup.sh" "main" "Starting cloud replication"
      invoke_cloud_replication "$BACKUP_PATH" && cloud_repl_result=0 || cloud_repl_result=1
      log_info "vmbackup.sh" "main" "Cloud replication finished (rc=$cloud_repl_result)"
    fi

  elif [[ "$replication_mode" == "cloud_first" ]]; then
    if [[ $cloud_repl_needed -eq 1 ]]; then
      log_info "vmbackup.sh" "main" "Starting cloud replication"
      invoke_cloud_replication "$BACKUP_PATH" && cloud_repl_result=0 || cloud_repl_result=1
      log_info "vmbackup.sh" "main" "Cloud replication finished (rc=$cloud_repl_result)"
    fi
    if [[ $local_repl_needed -eq 1 ]]; then
      log_info "vmbackup.sh" "main" "Starting local replication"
      [[ -f "$local_repl_config" ]] && source "$local_repl_config"
      run_local_replication_batch "$BACKUP_PATH" && local_repl_result=0 || local_repl_result=1
      log_info "vmbackup.sh" "main" "Local replication finished (rc=$local_repl_result)"
    fi

  else
    log_warn "vmbackup.sh" "main" "Unknown REPLICATION_ORDER: $replication_mode — running simultaneous"
    if [[ $local_repl_needed -eq 1 ]]; then
      ( run_local_replication_batch "$BACKUP_PATH" && exit 0 || exit 1 ) &
      local_repl_pid=$!
    fi
    if [[ $cloud_repl_needed -eq 1 ]]; then
      ( invoke_cloud_replication "$BACKUP_PATH" && exit 0 || exit 1 ) &
      cloud_repl_pid=$!
    fi
    [[ -n "$local_repl_pid" ]] && { wait $local_repl_pid; local_repl_result=$?; }
    [[ -n "$cloud_repl_pid" ]] && { wait $cloud_repl_pid; cloud_repl_result=$?; }
  fi

  # Clean up cancel flag if set during replication
  is_replication_cancelled && clear_replication_cancel_flag

  # Determine final status
  local any_failed=0
  (( local_repl_result != 0 )) && any_failed=1
  (( cloud_repl_result != 0 )) && any_failed=1

  local final_status="success"
  (( any_failed )) && final_status="failed"

  # Session summary
  _log_replicate_only_summary "$mode"

  log_info "vmbackup.sh" "main" "FINAL: local_rc=$local_repl_result cloud_rc=$cloud_repl_result status=$final_status"
  log_info "vmbackup.sh" "main" "Backup location: $BACKUP_PATH"
  log_info "vmbackup.sh" "main" "Full log: $LOG_FILE"

  # End SQLite session
  _sqlite_end_replicate_only "$final_status"

  # Send email report
  local session_end_time=$(date '+%Y-%m-%d %H:%M:%S %Z')
  if [[ -f "${SCRIPT_DIR}/modules/email_report_module.sh" ]]; then
    source "${SCRIPT_DIR}/modules/email_report_module.sh"
    if load_email_config; then
      log_info "vmbackup.sh" "main" "Sending email report to $EMAIL_RECIPIENT"
      local _rc=0
      send_backup_report "$session_start_time" "$session_end_time" "$final_status" || _rc=$?
      _handle_notifier_rc "$_rc" main
    else
      log_debug "vmbackup.sh" "main" "Email disabled or not configured"
    fi
  fi

  # Slack (parallel to the email block above)
  if [[ "${_SLACK_SENT:-false}" != "true" ]] && \
     [[ -f "${SCRIPT_DIR}/modules/slack_notification_module.sh" ]]; then
    source "${SCRIPT_DIR}/modules/slack_notification_module.sh"
    if load_slack_config; then
      local _src=0
      send_slack_notification "$session_start_time" "$session_end_time" "$final_status" || _src=$?
      _handle_slack_rc "$_src" main
    fi
  fi

  log_info "vmbackup.sh" "main" "===== REPLICATE-ONLY MODE END (exit=$any_failed) ====="
  return $any_failed
}

# Helper: end SQLite session for replicate-only mode
_sqlite_end_replicate_only() {
  local status="$1"
  if sqlite_is_available 2>/dev/null && ! is_dry_run; then
    sqlite_session_end "0" "0" "0" "0" "0" "0" "$status"
    log_debug "vmbackup.sh" "main" "SQLite session ended: status=$status"
  fi
}

#################################################################################
# PRUNE MODE
#################################################################################

# PRUNE-01 (118-spaces): the all-VMs prune walker derives the VM identity from the
# backup folder name = the vm_fs_name TOKEN (or the pre-118 legacy slug). But
# retention keys rotation policy + SQLite by the REAL libvirt name. Map the folder
# name back to the real name (for any LIVE domain) so a policy=never spaced VM is
# honoured rather than pruned; an orphan folder (no live domain) keeps its own
# name (pre-existing default-policy behaviour). Map built once, both token forms.
_PRUNE_TOKEN2REAL_BUILT=0
declare -gA _PRUNE_TOKEN2REAL=()
_prune_real_name() {
    local key=$1
    if [[ "$_PRUNE_TOKEN2REAL_BUILT" -eq 0 ]]; then
        local d t
        while IFS= read -r d; do
            [[ -z "$d" ]] && continue
            t=$(vm_fs_name "$d" 2>/dev/null)        && _PRUNE_TOKEN2REAL["$t"]="$d"
            t=$(vm_fs_name_legacy "$d" 2>/dev/null) && _PRUNE_TOKEN2REAL["$t"]="$d"
        done < <(lv_list_all_domains 2>/dev/null)
        _PRUNE_TOKEN2REAL_BUILT=1
    fi
    printf '%s' "${_PRUNE_TOKEN2REAL[$key]:-$key}"
}

# Main prune dispatch — target parsing, validation, confirmation, execution
# Uses globals: _PRUNE_TARGET, _TARGET_VM, _PRUNE_CONFIRM_SKIP, DRY_RUN, BACKUP_PATH
run_prune_mode() {
    local target="$_PRUNE_TARGET"
    local vm_name="$_TARGET_VM"
    local dry_run="$DRY_RUN"  # [DRY-RUN-KEEPER: local snapshot, see 109-phase7-spec.md §1.3.5]
    
    log_info "vmbackup.sh" "run_prune_mode" "===== PRUNE MODE START ====="
    
    # Validate target is specified
    if [[ -z "$target" ]]; then
        echo "Error: --prune requires a target (list, archives, chain:NAME, period:ID, all)"
        echo "Run: sudo ./vmbackup.sh --help"
        log_error "vmbackup.sh" "run_prune_mode" "No prune target specified"
        return 1
    fi
    
    # Validate target type is recognized
    local target_type_check="${target%%:*}"
    case "$target_type_check" in
        list|archives|chain|period|all) ;;
        *)
            echo "Error: Unknown prune target: $target"
            echo "Valid targets: list, archives, chain:NAME, period:ID, all"
            log_error "vmbackup.sh" "run_prune_mode" "Unknown prune target: $target"
            return 1
            ;;
    esac
    
    # Validate --vm is provided for targets that require it
    if [[ -z "$vm_name" ]]; then
        case "$target_type_check" in
            period|chain|all)
                echo "Error: --prune $target_type_check requires --vm NAME"
                echo "Run: sudo ./vmbackup.sh --prune list"
                log_error "vmbackup.sh" "run_prune_mode" "--prune $target_type_check requires --vm"
                return 1
                ;;
            archives)
                local _param_check="${target#*:}"
                [[ "$target_type_check" == "$_param_check" ]] && _param_check=""
                if [[ -n "$_param_check" ]]; then
                    echo "Error: --prune archives:<period> requires --vm NAME"
                    echo "Run: sudo ./vmbackup.sh --prune list"
                    log_error "vmbackup.sh" "run_prune_mode" "--prune archives:<period> requires --vm"
                    return 1
                fi
                ;;
        esac
    fi
    
    log_info "vmbackup.sh" "run_prune_mode" "Target: $target VM: ${vm_name:-all} Dry-run: $dry_run"
    
    # Handle 'list' target (no destructive action)
    if [[ "$target" == "list" ]]; then
        _prune_list "$vm_name"
        return 0
    fi
    
    # Validate VM exists on disk (if specified)
    if [[ -n "$vm_name" ]]; then
        local safe_name
        safe_name=$(vm_fs_name "$vm_name") || exit $?
        local vm_dir="${BACKUP_PATH}${safe_name}"
        if [[ ! -d "$vm_dir" ]]; then
            echo "Error: VM not found: $vm_name"
            echo "No backup directory at: $vm_dir"
            echo "Run: sudo ./vmbackup.sh --prune list"
            log_error "vmbackup.sh" "run_prune_mode" "VM not found: $vm_name (path=$vm_dir)"
            return 1
        fi
    fi
    
    # Parse target-specific parameters
    local target_type="${target%%:*}"
    local target_param="${target#*:}"
    [[ "$target_type" == "$target_param" ]] && target_param=""

    # FF-9 selector validation (defense-in-depth, runs before any disk lookup).
    # chain/period reject an EMPTY selector whether or not a ':' was given: a
    # bare `chain`/`period` (no colon) leaves target_param="" (cleared on the
    # line above). The chain-search loop below would then test ".archives/"
    # (an empty path component), MATCH the period's .archives directory, and
    # _remove_archive_chain (modules/retention_module.sh) would rm -rf the whole
    # .archives tree — wholesale archive loss. The public CLI already requires
    # the colon for chain/period; this in-function guard aligns the two layers.
    case "$target_type" in
        chain|period)
            if [[ -z "$target_param" ]]; then
                echo "Error: --prune $target_type requires a non-empty selector (e.g. ${target_type}:NAME)"
                echo "Run: sudo ./vmbackup.sh --help"
                log_error "vmbackup.sh" "run_prune_mode" "Empty selector for --prune $target_type: '$target'"
                return 1
            fi
            ;;
    esac
    # A ':' parameter is only meaningful for archives/chain/period. When a colon
    # is present, reject list/all (no parameter allowed) and reject empty or
    # path-traversing selectors (e.g. `archives:`, `period:2025/..`, `chain:a/b`)
    # fail-closed. Bare `archives` (no colon, target_param="") stays mass-scoped.
    if [[ "$target" == *:* ]]; then
        case "$target_type" in
            list|all)
                echo "Error: --prune $target_type does not take a ':' parameter"
                echo "Run: sudo ./vmbackup.sh --help"
                log_error "vmbackup.sh" "run_prune_mode" "--prune $target_type takes no parameter: '$target'"
                return 1
                ;;
            *)
                if [[ -z "$target_param" || "$target_param" == */* || "$target_param" == "." || "$target_param" == ".." ]]; then
                    echo "Error: Invalid --prune $target_type selector: '$target_param'"
                    echo "Run: sudo ./vmbackup.sh --help"
                    log_error "vmbackup.sh" "run_prune_mode" "Invalid selector for --prune $target_type: '$target'"
                    return 1
                fi
                ;;
        esac
    fi
    
    # Validate specific targets exist on disk
    case "$target_type" in
        period)
            local safe_name
            safe_name=$(vm_fs_name "$vm_name") || exit $?
            local period_dir="${BACKUP_PATH}${safe_name}/${target_param}"
            if [[ ! -d "$period_dir" ]]; then
                echo "Error: Period not found: $vm_name/$target_param"
                echo "Run: sudo ./vmbackup.sh --prune list --vm $vm_name"
                log_error "vmbackup.sh" "run_prune_mode" "Period not found: $period_dir"
                return 1
            fi
            ;;
        chain)
            # Chain target needs to find the chain in any period's .archives
            local safe_name
            safe_name=$(vm_fs_name "$vm_name") || exit $?
            local found_chain=""
            local found_period=""
            local period_dir
            for period_dir in "${BACKUP_PATH}${safe_name}"/*/; do
                [[ -d "$period_dir" ]] || continue
                if [[ -d "${period_dir}.archives/${target_param}" ]]; then
                    found_chain="${period_dir}.archives/${target_param}"
                    found_period=$(basename "$period_dir")
                    break
                fi
            done
            if [[ -z "$found_chain" ]]; then
                echo "Error: Archive chain not found: $target_param (VM: $vm_name)"
                echo "Run: sudo ./vmbackup.sh --prune list --vm $vm_name"
                log_error "vmbackup.sh" "run_prune_mode" "Chain not found: $target_param for VM $vm_name"
                return 1
            fi
            ;;
        archives)
            if [[ -n "$target_param" ]]; then
                # archives:<period> — verify period exists
                local safe_name
                safe_name=$(vm_fs_name "$vm_name") || exit $?
                local period_dir="${BACKUP_PATH}${safe_name}/${target_param}"
                if [[ ! -d "$period_dir" ]]; then
                    echo "Error: Period not found: $vm_name/$target_param"
                    echo "Run: sudo ./vmbackup.sh --prune list --vm $vm_name"
                    log_error "vmbackup.sh" "run_prune_mode" "Period not found: $period_dir"
                    return 1
                fi
            fi
            ;;
    esac
    
    # Calculate preview — what will be affected
    local preview_bytes=0
    local preview_desc=""
    
    case "$target_type" in
        archives)
            if [[ -n "$target_param" ]]; then
                # archives:<period> — one period's archives
                local safe_name
                safe_name=$(vm_fs_name "$vm_name") || exit $?
                local archives_dir="${BACKUP_PATH}${safe_name}/${target_param}/.archives"
                if [[ -d "$archives_dir" ]]; then
                    preview_bytes=$(_du_bytes "$archives_dir")
                fi
                preview_desc="archives in $vm_name/$target_param"
            elif [[ -n "$vm_name" ]]; then
                # archives for one VM (all periods)
                local safe_name
                safe_name=$(vm_fs_name "$vm_name") || exit $?
                local period_dir
                for period_dir in "${BACKUP_PATH}${safe_name}"/*/; do
                    [[ -d "${period_dir}.archives" ]] || continue
                    local ab
                    ab=$(_du_bytes "${period_dir}.archives")
                    preview_bytes=$(( preview_bytes + ab ))
                done
                preview_desc="all archives for $vm_name"
            else
                # archives for all VMs
                local vd
                for vd in "$BACKUP_PATH"*/; do
                    [[ -d "$vd" ]] || continue
                    local vn=$(basename "$vd")
                    [[ "$vn" == _* || "$vn" == .* ]] && continue
                    local pd
                    for pd in "$vd"*/; do
                        [[ -d "${pd}.archives" ]] || continue
                        local ab
                        ab=$(_du_bytes "${pd}.archives")
                        preview_bytes=$(( preview_bytes + ab ))
                    done
                done
                preview_desc="all archives for all VMs"
            fi
            ;;
        chain)
            preview_bytes=$(_du_bytes "$found_chain")
            preview_desc="chain $target_param in $vm_name/$found_period"
            ;;
        period)
            local safe_name
            safe_name=$(vm_fs_name "$vm_name") || exit $?
            preview_bytes=$(_du_bytes "${BACKUP_PATH}${safe_name}/${target_param}")
            preview_desc="period $target_param for $vm_name"
            ;;
        all)
            local safe_name
            safe_name=$(vm_fs_name "$vm_name") || exit $?
            preview_bytes=$(_du_bytes "${BACKUP_PATH}${safe_name}")
            preview_desc="ALL data for $vm_name"
            ;;
    esac
    
    local preview_size
    preview_size=$(_format_size "$preview_bytes")
    
    # Show preview / dry-run output
    echo ""
    echo "vmbackup prune — ${preview_desc}"
    echo "  Space to free: ${preview_size}"
    
    if [[ "$dry_run" == "true" ]]; then  # [DRY-RUN-KEEPER: reads L6012 local snapshot, see 109-phase7-spec.md §1.3.5]
        echo "  [DRY RUN] No data will be deleted."
        log_info "vmbackup.sh" "run_prune_mode" "[DRY RUN] Would free ${preview_size} (${preview_bytes} bytes) — ${preview_desc}"
        echo ""
        return 0
    fi
    
    # Confirmation prompt (unless --yes)
    if [[ "$_PRUNE_CONFIRM_SKIP" != "true" ]]; then
        echo ""
        printf "  Continue? [y/N] "
        local confirm
        read -r confirm
        case "$confirm" in
            [yY]|[yY][eE][sS])
                ;;
            *)
                echo "  Cancelled."
                log_info "vmbackup.sh" "run_prune_mode" "User declined confirmation"
                return 0
                ;;
        esac
    fi
    
    echo ""
    
    # FF-52 (R5-C): acquire the per-VM lock before any single-VM deletion so a
    # prune cannot race a concurrent backup/restore of the same VM. create_lock
    # is the sole, non-destructive acquirer: a live vmbackup/vmrestore/
    # virtnbdbackup holder -> return 1 WITHOUT touching the file; a dead/reused
    # PID -> reap + acquire. No has_lock pre-probe (that probe is destructive and
    # its regex misses vmrestore). The all-VMs sweep locks per VM in its own loop.
    if [[ -n "$vm_name" ]]; then
        if ! create_lock "$vm_name"; then
            local _lf
            _lf=$(vm_lock_file "$vm_name" 2>/dev/null)
            local _hp=""
            [[ -n "$_lf" && -f "$_lf" ]] && _hp=$(cat "$_lf" 2>/dev/null)
            echo "Error: Cannot prune $vm_name - a backup or restore is in progress (PID ${_hp:-unknown})"
            echo "Retry after the backup/restore completes."
            log_error "vmbackup.sh" "run_prune_mode" "Refusing prune of $vm_name - live lock held by PID ${_hp:-unknown}"
            return 1
        fi
    fi

    # Execute deletion
    local success_count=0
    local fail_count=0
    local total_freed=0
    # FF-52 (R5-C): rc-neutral count of VMs the all-VMs sweep skipped because a
    # live lock was held; surfaced in the summary so a skip is never silent.
    local skipped_locked=0
    
    case "$target_type" in
        archives)
            if [[ -n "$target_param" ]]; then
                # archives:<period> — one period
                local result
                result=$(_remove_archives_in_period "$vm_name" "$target_param" "$dry_run" "prune")
                if [[ $? -eq 0 ]]; then
                    total_freed=$(( total_freed + ${result:-0} ))
                    (( success_count++ ))
                else
                    total_freed=$(( total_freed + ${result:-0} ))
                    (( fail_count++ ))
                fi
            elif [[ -n "$vm_name" ]]; then
                # archives for one VM (all periods)
                local safe_name
                safe_name=$(vm_fs_name "$vm_name") || { local _rc=$?; remove_lock "$vm_name"; exit $_rc; }
                local period_dir
                for period_dir in "${BACKUP_PATH}${safe_name}"/*/; do
                    [[ -d "$period_dir" ]] || continue
                    local pid=$(basename "$period_dir")
                    [[ "$pid" == _* || "$pid" == .* ]] && continue
                    [[ -d "${period_dir}.archives" ]] || continue
                    local result
                    result=$(_remove_archives_in_period "$vm_name" "$pid" "$dry_run" "prune")
                    if [[ $? -eq 0 ]]; then
                        total_freed=$(( total_freed + ${result:-0} ))
                        (( success_count++ ))
                    else
                        total_freed=$(( total_freed + ${result:-0} ))
                        (( fail_count++ ))
                    fi
                done
            else
                # archives for all VMs
                local vd
                for vd in "$BACKUP_PATH"*/; do
                    [[ -d "$vd" ]] || continue
                    local vn=$(basename "$vd")
                    [[ "$vn" == _* || "$vn" == .* ]] && continue
                    vn=$(_prune_real_name "$vn")   # PRUNE-01: token/legacy folder -> real name for policy + SQLite
                    # FF-52 (R5-C): lock this VM before sweeping its archives.
                    # create_lock-first (no destructive pre-probe); a live
                    # vmbackup/vmrestore/virtnbdbackup holder -> skip this VM with
                    # a WARN naming the holder (fail-closed, never queue-behind).
                    if ! create_lock "$vn"; then
                        local _hp=""
                        local _lf
                        _lf=$(vm_lock_file "$vn" 2>/dev/null)
                        [[ -n "$_lf" && -f "$_lf" ]] && _hp=$(cat "$_lf" 2>/dev/null)
                        log_warn "vmbackup.sh" "run_prune_mode" "Skipping archives prune for $vn: live lock held by PID ${_hp:-unknown}"
                        (( skipped_locked++ ))
                        continue
                    fi
                    local period_dir
                    for period_dir in "$vd"*/; do
                        [[ -d "$period_dir" ]] || continue
                        local pid=$(basename "$period_dir")
                        [[ "$pid" == _* || "$pid" == .* ]] && continue
                        [[ -d "${period_dir}.archives" ]] || continue
                        local result
                        result=$(_remove_archives_in_period "$vn" "$pid" "$dry_run" "prune")
                        if [[ $? -eq 0 ]]; then
                            total_freed=$(( total_freed + ${result:-0} ))
                            (( success_count++ ))
                        else
                            total_freed=$(( total_freed + ${result:-0} ))
                            (( fail_count++ ))
                        fi
                    done
                    remove_lock "$vn"
                done
            fi
            ;;
        chain)
            local result
            result=$(_remove_archive_chain "$vm_name" "$found_period" "$target_param" "$dry_run" "prune" "prune")
            if [[ $? -eq 0 ]]; then
                total_freed=$(( total_freed + ${result:-0} ))
                (( success_count++ ))
            else
                (( fail_count++ ))
            fi
            # DUP-10: rebuild_chain_manifest call removed (writer to dead file).
            ;;
        period)
            _remove_period "$vm_name" "$target_param" "$dry_run" "false" "prune" "prune"
            local rc=$?
            if [[ $rc -eq 0 ]]; then
                # Verify deletion actually happened (protection/keep-last may skip)
                local safe_name
                safe_name=$(vm_fs_name "$vm_name") || { local _rc=$?; remove_lock "$vm_name"; exit $_rc; }
                if [[ ! -d "${BACKUP_PATH}${safe_name}/${target_param}" ]]; then
                    total_freed=$preview_bytes
                fi
                (( success_count++ ))
            else
                (( fail_count++ ))
            fi
            # DUP-10: rebuild_chain_manifest call removed (writer to dead file).
            ;;
        all)
            local result
            result=$(_remove_vm_all "$vm_name" "$dry_run" "prune" "prune")
            if [[ $? -eq 0 ]]; then
                total_freed=$(( total_freed + ${result:-0} ))
                (( success_count++ ))
            else
                total_freed=$(( total_freed + ${result:-0} ))
                (( fail_count++ ))
            fi
            ;;
    esac
    
    # FF-52 (R5-C): release the single-VM lock acquired above before the summary
    # returns. Explicit (not a RETURN trap): the many pre-acquisition validation
    # returns would misfire a function-scoped trap, and the in-case vm_fs_name
    # '|| exit' paths release-then-exit themselves. rm -f is idempotent, so a
    # double release never faults. Guarded on vm_name; the all-VMs sweep
    # locks/releases per VM inline and does not reach here holding a lock.
    [[ -n "$vm_name" ]] && remove_lock "$vm_name"

    # Summary
    local freed_size
    freed_size=$(_format_size "$total_freed")
    
    echo "vmbackup prune — complete"
    echo "  Freed: ${freed_size}"
    # FF-52 (R5-C): surface VMs the all-VMs sweep skipped because a live lock was
    # held. rc-neutral — a skip is not a failure, but it must be visible so an
    # operator never reads "skipped, backup in progress" as "nothing to prune".
    if [[ ${skipped_locked:-0} -gt 0 ]]; then
        echo "  Skipped (locked): ${skipped_locked} (a backup/restore was in progress; retry later)"
        log_warn "vmbackup.sh" "run_prune_mode" "Prune skipped ${skipped_locked} VM(s) holding a live lock"
    fi
    if [[ $fail_count -gt 0 ]]; then
        echo "  Errors: ${fail_count} (check ${LOG_FILE})"
        log_error "vmbackup.sh" "run_prune_mode" \
            "Prune completed with errors: freed=${total_freed} bytes, failures=${fail_count}"
        return 1
    else
        log_info "vmbackup.sh" "run_prune_mode" \
            "Prune completed: freed=${freed_size} (${total_freed} bytes)"
        return 0
    fi
}

# UNI-010: Source shared signal-trap registration helpers.
# UNI-321: kept late in source order (after main has set up state) because
# the trap callbacks reference _BACKUP_IN_PROGRESS, _CURRENT_VM_LOCK etc.
source_lib_or_die signal_handlers.sh

# Define the SIGINT callback as a real function (was previously a quoted
# multi-statement trap body; cleaner under the lib's interface).
_handle_sigint() {
  _log_interrupted_chain "SIGINT"
  log_error "vmbackup.sh" "main" "Script interrupted by SIGINT (Ctrl+C)"
  log_error "vmbackup.sh" "main" "Recovery: Run vmbackup.sh again - incomplete backups will be cleaned up automatically"
  exit 130
}

# Set up signal handlers via lib/signal_handlers.sh
setup_exit_cleanup    'cleanup_on_exit'
setup_sigint_handler  '_handle_sigint'
setup_sigterm_handler 'handle_sigterm'
setup_sigtstp_handler 'handle_sigtstp'

main() {
  ensure_backup_path_sgid
  init_logging
  
  log_info "vmbackup.sh" "main" "===== VM BACKUP SESSION START ====="
  log_info "vmbackup.sh" "main" "100% vibe coded. Could be 100% wrong."
  log_info "vmbackup.sh" "main" "Config instance: ${CONFIG_INSTANCE:-default}"
  
  if is_dry_run; then
    log_info "vmbackup.sh" "main" "╔══════════════════════════════════════════════════════════════╗"
    log_info "vmbackup.sh" "main" "║  DRY-RUN MODE: No backups, retention, replication, or FSTRIM ║"
    log_info "vmbackup.sh" "main" "║  will be executed. Read-only — showing what would happen.    ║"
    log_info "vmbackup.sh" "main" "╚══════════════════════════════════════════════════════════════╝"
  fi
  
  # Validate operational settings from config (with explicit defaults if missing)
  validate_operational_settings
  
  log_info "vmbackup.sh" "main" "Configuration: COMPRESS_LEVEL=$VIRTNBD_COMPRESS_LEVEL, HEALTH_CHECK=$CHECKPOINT_HEALTH_CHECK"
  
  # Global session lock — prevent concurrent vmbackup invocations (atomic)
  local session_pidfile="$STATE_DIR/vmbackup.pid"
  if ! ( set -o noclobber; echo "$$" > "$session_pidfile" ) 2>/dev/null; then
    # Lock file exists — check if holder is still alive
    local existing_pid
    existing_pid=$(cat "$session_pidfile" 2>/dev/null)
    # FF-160: kill -0 alone can't distinguish a live vmbackup session from an
    # unrelated process that recycled the PID after an unclean shutdown (the
    # pidfile lives on the persistent backup volume and survives reboots).
    # Mirror the per-VM lock's /proc/<pid>/cmdline check (lib/vm_lock.sh) so a
    # reused PID whose process is NOT vmbackup is treated as a stale file.
    local existing_cmdline=""
    [[ -n "$existing_pid" ]] && existing_cmdline=$(tr '\0' ' ' < "/proc/$existing_pid/cmdline" 2>/dev/null)
    if [[ -n "$existing_pid" ]] && kill -0 "$existing_pid" 2>/dev/null \
         && [[ "$existing_cmdline" == *"vmbackup"* ]]; then
      die "Another vmbackup session is already running (PID $existing_pid) — aborting" "main" "$EXIT_LOCK"
    fi
    # Stale PID file — remove and retry once
    rm -f "$session_pidfile"
    log_debug "vmbackup.sh" "main" "Removed stale session PID file (PID ${existing_pid:-unknown} no longer running)"
    if ! ( set -o noclobber; echo "$$" > "$session_pidfile" ) 2>/dev/null; then
      die "Failed to acquire session lock after stale removal — aborting" "main" "$EXIT_LOCK"
    fi
  fi
  log_debug "vmbackup.sh" "main" "Session PID file created: $session_pidfile (PID $$)"
  
  # Track session start time for email report (include %Z for unambiguous
  # epoch conversion in email_report_module — avoids DST fall-back mismatch)
  local session_start_time=$(date '+%Y-%m-%d %H:%M:%S %Z')
  
  # Dependency check — skip in replicate-only mode (virsh/virtnbdbackup not needed)
  if [[ -n "${_REPLICATE_ONLY_MODE:-}" ]]; then
    log_info "vmbackup.sh" "main" "Replicate-only mode: skipping dependency check (backup tools not required)"
  else
    log_info "vmbackup.sh" "main" "Checking dependencies"
    if ! check_dependencies; then
      die "Dependency check failed - aborting session" "main" "$EXIT_DEPENDENCY"
    fi
  fi
  
  # CRITICAL: Clean up any stale qemu-nbd processes from previous interrupted runs
  # These hold write locks on qcow2 files and prevent VMs from starting
  # Skip in replicate-only mode — no backup processes to clean up
  if [[ -n "${_REPLICATE_ONLY_MODE:-}" ]]; then
    log_debug "vmbackup.sh" "main" "Replicate-only mode: skipping stale qemu-nbd cleanup"
  elif is_dry_run; then
    local stale_qemu_nbd=$(pgrep -f "qemu-nbd.*virtnbdbackup" 2>/dev/null)
    if [[ -n "$stale_qemu_nbd" ]]; then
      log_info "vmbackup.sh" "main" "[DRY-RUN] Would clean up stale qemu-nbd processes (found $(echo "$stale_qemu_nbd" | wc -l))"
    fi
  else
  local stale_qemu_nbd=$(pgrep -f "qemu-nbd.*virtnbdbackup" 2>/dev/null)
  if [[ -n "$stale_qemu_nbd" ]]; then
    log_warn "vmbackup.sh" "main" "Found stale qemu-nbd processes from previous run - cleaning up"
    while IFS= read -r pid; do
      if [[ -n "$pid" ]]; then
        local cmdline=$(ps -p "$pid" -o args= 2>/dev/null | head -c 100)
        log_warn "vmbackup.sh" "main" "Killing stale qemu-nbd PID $pid: $cmdline"
        kill "$pid" 2>/dev/null
      fi
    done <<< "$stale_qemu_nbd"
    sleep 2
    # Force kill any remaining
    stale_qemu_nbd=$(pgrep -f "qemu-nbd.*virtnbdbackup" 2>/dev/null)
    if [[ -n "$stale_qemu_nbd" ]]; then
      log_warn "vmbackup.sh" "main" "Force killing remaining stale qemu-nbd processes"
      while IFS= read -r pid; do
        [[ -n "$pid" ]] && kill -9 "$pid" 2>/dev/null
      done <<< "$stale_qemu_nbd"
    fi
    log_info "vmbackup.sh" "main" "Stale qemu-nbd cleanup complete"
  else
    log_debug "vmbackup.sh" "main" "No stale qemu-nbd processes found"
  fi
  fi  # end DRY_RUN/replicate-only else block
  
  # OPT #4: Lazy load TPM backup module only when first VM is processed
  # This defers module initialization until we know if any VMs have TPM
  local tpm_module_loaded=false
  
  # Load SQLite logging module (provides structured database logging)
  if load_sqlite_logging_module; then
    log_info "vmbackup.sh" "main" "SQLite logging module loaded successfully"
    # Determine session type for SQLite tracking
    local _session_type="standard"
    [[ -n "$_TARGET_VM" && "$_PRUNE_MODE" != "true" ]] && _session_type="targeted"
    [[ -n "$_REPLICATE_ONLY_MODE" ]] && _session_type="replicate_only"
    [[ "$_PRUNE_MODE" == "true" ]] && _session_type="prune"
    # --prune list is read-only (like --status) — skip session tracking
    [[ "$_PRUNE_MODE" == "true" && "$_PRUNE_TARGET" == "list" ]] && _session_type=""
    # Start SQLite session tracking (skip in dry-run and read-only modes)
    if is_dry_run; then
      log_info "vmbackup.sh" "main" "[DRY-RUN] SQLite session tracking disabled - no DB writes"
    elif [[ -z "$_session_type" ]]; then
      log_debug "vmbackup.sh" "main" "Read-only mode — SQLite session tracking skipped"
    elif sqlite_session_start "${CONFIG_INSTANCE:-default}" "$LOG_FILE" "$_session_type"; then
      log_debug "vmbackup.sh" "main" "SQLite session started: $(sqlite_get_session_id) type=$_session_type"
    fi
  else
    log_debug "vmbackup.sh" "main" "SQLite logging module unavailable - database logging disabled"
  fi
  
  # Load chain validation module (provides chain integrity checking)
  if load_chain_validation_module; then
    log_debug "vmbackup.sh" "main" "Chain validation module loaded"
  else
    log_debug "vmbackup.sh" "main" "Chain validation module unavailable"
  fi
  
  # Load VM-first integration module (required for backup operation)
  # Provides: get_backup_dir, pre_backup_hook, post_backup_hook
  local script_dir="${SCRIPT_DIR:-$(dirname "$(readlink -f "$0")")}"
  local integration_module="$script_dir/modules/vmbackup_integration.sh"
  if [[ -f "$integration_module" ]]; then
    export DIRECTORY_STRUCTURE_MODE="vm_first"
    if source "$integration_module" 2>/dev/null; then
      log_info "vmbackup.sh" "main" "VM-first integration module loaded (v${VMBACKUP_INTEGRATION_VERSION:-unknown})"
    else
      die "FATAL: Failed to load vmbackup_integration.sh (syntax error?)" "main" "$EXIT_DEPENDENCY"
    fi
  else
    die "FATAL: vmbackup_integration.sh not found at: $integration_module" "main" "$EXIT_DEPENDENCY"
  fi

  # MIG-01 (118-spaces): on-disk layout migrator. Optional — only renames the few
  # pre-118 unsafe-named folders to the slug+hash token; absence is non-fatal.
  local migrate_module="$script_dir/modules/migrate_layout.sh"
  [[ -f "$migrate_module" ]] && source "$migrate_module" 2>/dev/null \
    && log_info "vmbackup.sh" "main" "MIG-01 layout migrator loaded"

  # PRUNE MODE: dispatch now that config, SQLite, and integration modules are loaded
  # Prune skips replication modules, FSTRIM, pre-flight checks, and session tracking
  if [[ "${_PRUNE_MODE:-false}" == "true" ]]; then
    log_info "vmbackup.sh" "main" "Entering prune mode (skipping backup pipeline)"
    run_prune_mode
    local rc=$?
    log_info "vmbackup.sh" "main" "===== PRUNE MODE END (exit=$rc) ====="
    # End SQLite session (skipped for --prune list which has no session)
    if [[ "$_PRUNE_TARGET" != "list" ]] && declare -f sqlite_session_end >/dev/null 2>&1; then
      local prune_status="success"
      [[ $rc -ne 0 ]] && prune_status="failed"
      # Count VMs evaluated: 1 if --vm specified, else count VM dirs in BACKUP_PATH
      local _prune_vms_total=0
      if [[ -n "$_TARGET_VM" ]]; then
        _prune_vms_total=1
      else
        local _pvd
        for _pvd in "$BACKUP_PATH"*/; do
          [[ -d "$_pvd" ]] || continue
          local _pvn
          _pvn=$(basename "$_pvd")
          [[ "$_pvn" == _* || "$_pvn" == .* ]] && continue
          (( _prune_vms_total++ ))
        done
      fi
      # Args: vms_total vms_success vms_failed vms_skipped vms_excluded bytes_total final_status
      sqlite_session_end "$_prune_vms_total" "0" "0" "0" "0" "0" "$prune_status"
    fi
    exit $rc
  fi

  # Load local replication module at startup (provides offsite backup via local/SSH/SMB)
  # Module self-configures based on config/<instance>/replication_local.conf
  if init_local_replication_module; then
    log_info "vmbackup.sh" "main" "Local replication module loaded and enabled"
  else
    log_debug "vmbackup.sh" "main" "Local replication module not available or disabled"
  fi
  
  # Load cloud replication module (provides SharePoint, Backblaze, etc.)
  # Module self-configures based on config/<instance>/replication_cloud.conf
  if init_cloud_replication_module; then
    log_info "vmbackup.sh" "main" "Cloud replication module loaded and enabled"
  else
    log_debug "vmbackup.sh" "main" "Cloud replication module not available or disabled"
  fi

  # Invalidate stale replication state files from prior sessions.
  # Each module re-creates its state file when it actually runs.
  # This prevents disabled/skipped modules from showing data from a prior run.
  _invalidate_replication_state_files

  # REPLICATE-ONLY dispatch — runs replication only, then exits
  # Placed after replication module init + state invalidation, before FSTRIM/pre-flight/VM-listing
  if [[ -n "${_REPLICATE_ONLY_MODE:-}" ]]; then
    _run_replicate_only "$_REPLICATE_ONLY_MODE" "$session_start_time"
    exit $?
  fi
  
  # Load FSTRIM module if enabled (can be loaded once at startup for caching)
  if [[ "$ENABLE_FSTRIM" == "true" ]]; then
    local fstrim_module="$script_dir/modules/fstrim_optimization_module.sh"
    if [[ -f "$fstrim_module" ]]; then
      if source "$fstrim_module" 2>/dev/null; then
        log_info "vmbackup.sh" "main" "FSTRIM optimization module loaded successfully"
        cache_fstrim_availability
      else
        log_warn "vmbackup.sh" "main" "Failed to load FSTRIM module (syntax error?)"
      fi
    else
      log_debug "vmbackup.sh" "main" "FSTRIM module not found at: $fstrim_module"
    fi
  fi
  
  # Pre-flight checks
  log_info "vmbackup.sh" "main" "Running pre-flight checks"
  
  check_file_descriptors
  check_libvirt_version
  
  if ! check_backup_destination; then
    die "Backup destination check failed - aborting session" "main" "$EXIT_STORAGE"
  fi
  
  if ! check_scratch_path; then
    die "Scratch path check failed - aborting session" "main" "$EXIT_STORAGE"
  fi
  
  if ! check_disk_space; then
    die "Insufficient disk space - aborting session" "main" "$EXIT_STORAGE"
  fi
  
  # Stale state recovery (locks only - checkpoint cleanup handled per-VM in backup_vm())
  log_info "vmbackup.sh" "main" "Running stale lock cleanup"
  cleanup_system_checkpoints_and_locks "stale_locks"
  
  # Get VM list
  log_info "vmbackup.sh" "main" "Retrieving VM list from libvirt"
  
  # Read VM names line by line to handle names with spaces
  local vm_list=()
  while IFS= read -r vm; do
    [[ -n "$vm" ]] && vm_list+=("$vm")
  done < <(lv_list_all_domains)
  
  if [[ ${#vm_list[@]} -eq 0 ]]; then
    log_warn "vmbackup.sh" "main" "No VMs found to backup"
  else
    log_info "vmbackup.sh" "main" "Found ${#vm_list[@]} VMs to process: ${vm_list[*]}"
  fi
  
  # Targeted backup: filter VM list to requested VM(s)
  if [[ -n "$_TARGET_VM" && "$_PRUNE_MODE" != "true" ]]; then
    local _target_vm_list=()
    IFS=',' read -ra _target_vm_list <<< "$_TARGET_VM"
    # Validate each VM exists in libvirt
    for _tvm in "${_target_vm_list[@]}"; do
      if ! lv_domain_exists "$_tvm"; then
        die "Targeted backup: VM not found in libvirt: $_tvm" "main" "$EXIT_VM"
      fi
    done
    vm_list=("${_target_vm_list[@]}")
    log_info "vmbackup.sh" "main" "Targeted backup: processing ${#vm_list[@]} VM(s): ${vm_list[*]}"
  fi
  
  # Backup each VM (sequential processing)
  # Track separate counts for accurate reporting:
  #   backed_up  = VMs that were actually backed up (success)
  #   excluded   = VMs with policy=never or pattern exclusion (return code 2)
  #   skipped    = VMs that were offline/unchanged (still success, tracked in VM_BACKUP_RESULTS)
  #   failed     = VMs with backup errors (return code 1)
  local backed_up_count=0
  local excluded_count=0
  local skipped_count=0
  local fail_count=0

  # MIG-01 (118-spaces): migrate any pre-118 unsafe-named folder to its slug+hash
  # token BEFORE the backup loop derives a path — so a fresh chain never lands in
  # the legacy folder. Runs over every libvirt-enumerated VM; safe names are
  # no-ops, so the 99% of VMs are untouched. Idempotent + honours dry-run.
  if declare -f migrate_all_layouts >/dev/null 2>&1; then
    migrate_all_layouts
  fi

  for vm_name in "${vm_list[@]}"; do
    [[ -z "$vm_name" ]] && continue
    
    # OPT #4b: Lazy load TPM module on first VM that has TPM
    if [[ "$tpm_module_loaded" == "false" ]]; then
      # Check if this VM has TPM before loading module
      if lv_xml_has_tpm "$vm_name"; then
        log_info "vmbackup.sh" "main" "VM $vm_name has TPM - loading TPM backup module"
        if load_tpm_backup_module; then
          tpm_module_loaded=true
          log_info "vmbackup.sh" "main" "TPM backup module loaded - will backup TPM state"
        else
          log_warn "vmbackup.sh" "main" "TPM module unavailable - skipping TPM backup"
        fi
      fi
    fi
    
    # Sequential backup processing with proper return code handling
    backup_vm "$vm_name"
    local rc=$?
    # Clear the RETURN trap set inside backup_vm() to prevent it leaking.
    # The trap fires correctly when backup_vm returns (releasing the VM lock),
    # but without clearing it, the last VM's remove_lock fires again when main() returns.
    trap - RETURN
    
    case $rc in
      0)  # Success or skipped (offline/unchanged)
          ((backed_up_count++))
          log_info "vmbackup.sh" "main" "VM $vm_name: backup completed (backed_up=$backed_up_count, excluded=$excluded_count, failed=$fail_count)"
          ;;
      1)  # Failed
          ((fail_count++))
          log_warn "vmbackup.sh" "main" "VM $vm_name: backup FAILED (backed_up=$backed_up_count, excluded=$excluded_count, failed=$fail_count)"
          ;;
      2)  # Excluded by policy
          ((excluded_count++))
          log_info "vmbackup.sh" "main" "VM $vm_name: excluded by policy (backed_up=$backed_up_count, excluded=$excluded_count, failed=$fail_count)"
          ;;
      *)  # Unknown return code - treat as failure
          ((fail_count++))
          log_warn "vmbackup.sh" "main" "VM $vm_name: unknown result rc=$rc (treated as failure)"
          ;;
    esac
  done
  
  # Count skipped VMs (offline/unchanged) from VM_BACKUP_RESULTS
  for result in "${VM_BACKUP_RESULTS[@]}"; do
    IFS='|' read -r vm status rest <<< "$result"
    [[ "$status" == "SKIPPED" ]] && ((skipped_count++))
  done
  # Adjust backed_up_count: it currently includes skipped
  backed_up_count=$((backed_up_count - skipped_count))
  
  log_info "vmbackup.sh" "main" "Backup phase complete: $backed_up_count backed up, $excluded_count excluded, $skipped_count skipped, $fail_count failed"
  
  #=============================================================================
  # REPLICATION PHASE (Local + Cloud)
  # Controlled by REPLICATION_ORDER in vmbackup.conf:
  #   "simultaneous"  - Run local and cloud in parallel (default, fastest)
  #   "local_first"   - Complete local before starting cloud
  #   "cloud_first"   - Complete cloud before starting local
  #
  # Cancellation: Operator can request graceful cancellation by:
  #   sudo vmbackup.sh --cancel-replication [--config-instance NAME]
  # This creates $STATE_DIR/cancel-replication, which is checked:
  #   - Before starting replication
  #   - Before each destination
  #   - During rsync/rclone transfer (kills process gracefully)
  # Cancelled destinations are logged as status="cancelled" in the database.
  #=============================================================================
  local replication_mode="${REPLICATION_ORDER:-simultaneous}"
  local local_repl_needed=0
  local cloud_repl_needed=0
  local local_repl_pid=""
  local cloud_repl_pid=""
  local local_repl_result=0
  local cloud_repl_result=0
  
  # Determine what replication is needed
  if [[ "${LOCAL_REPLICATION_MODULE_AVAILABLE:-0}" -eq 1 ]]; then
    local_repl_needed=1
  fi
  if [[ "${CLOUD_REPLICATION_MODULE_AVAILABLE:-0}" -eq 1 ]]; then
    cloud_repl_needed=1
  fi
  
  # TARGETED BACKUP: Skip replication (operator runs --replicate-only separately if needed)
  if [[ -n "$_TARGET_VM" ]]; then
    log_info "vmbackup.sh" "main" "Targeted backup mode: skipping replication (use --replicate-only separately)"
    local_repl_needed=0
    cloud_repl_needed=0
  fi
  
  # DRY-RUN: Skip entire replication phase (AFTER determination so we can report what would run)
  if is_dry_run; then
    log_info "vmbackup.sh" "main" "[DRY-RUN] Would run replication (mode=$replication_mode, local=$local_repl_needed, cloud=$cloud_repl_needed) - skipping"
    local_repl_needed=0
    cloud_repl_needed=0
  fi
  
  # CANCELLATION: Skip replication if cancel flag already exists before we start
  if is_replication_cancelled; then
    log_warn "vmbackup.sh" "main" "Replication cancellation flag detected - skipping entire replication phase"
    local_repl_needed=0
    cloud_repl_needed=0
    clear_replication_cancel_flag
  fi
  
  # Local replication config file path (needed for subshell re-sourcing)
  local local_repl_config="${SCRIPT_DIR}/config/${CONFIG_INSTANCE:-default}/replication_local.conf"
  
  # Execute based on mode
  if [[ "$replication_mode" == "simultaneous" ]]; then
    #---------------------------------------------------------------------------
    # SIMULTANEOUS MODE: Run local and cloud replication in parallel
    #---------------------------------------------------------------------------
    log_info "vmbackup.sh" "main" "Replication mode: simultaneous (local and cloud run in parallel)"
    
    # Start local replication in background
    # NOTE: Subshell inherits variables including REPLICATION_DESTINATIONS array
    if [[ $local_repl_needed -eq 1 ]]; then
      log_info "vmbackup.sh" "main" "Starting local replication (background)"
      log_debug "vmbackup.sh" "main" "Before subshell: REPLICATION_DESTINATIONS has ${#REPLICATION_DESTINATIONS[@]} elements"
      (
        # Log inherited state
        log_debug "vmbackup.sh" "main" "In subshell: REPLICATION_DESTINATIONS has ${#REPLICATION_DESTINATIONS[@]} elements: ${REPLICATION_DESTINATIONS[*]}"
        log_debug "vmbackup.sh" "main" "In subshell: REPLICATION_MODULE_LOADED=$REPLICATION_MODULE_LOADED"
        log_debug "vmbackup.sh" "main" "In subshell: REPLICATION_ENABLED=$REPLICATION_ENABLED"
        
        if run_local_replication_batch "$BACKUP_PATH"; then
          exit 0
        else
          exit 1
        fi
      ) &
      local_repl_pid=$!
    fi
    
    # Start cloud replication in background
    if [[ $cloud_repl_needed -eq 1 ]]; then
      log_info "vmbackup.sh" "main" "Starting cloud replication (background)"
      (
        if invoke_cloud_replication "$BACKUP_PATH"; then
          exit 0
        else
          exit 1
        fi
      ) &
      cloud_repl_pid=$!
    fi
    
    # Wait for both to complete
    if [[ -n "$local_repl_pid" ]]; then
      wait $local_repl_pid
      local_repl_result=$?
      if [[ $local_repl_result -eq 0 ]]; then
        log_info "vmbackup.sh" "main" "Local replication completed successfully"
      else
        log_warn "vmbackup.sh" "main" "Local replication completed with errors (see log for details)"
      fi
    fi
    
    if [[ -n "$cloud_repl_pid" ]]; then
      wait $cloud_repl_pid
      cloud_repl_result=$?
      if [[ $cloud_repl_result -eq 0 ]]; then
        log_info "vmbackup.sh" "main" "Cloud replication completed successfully"
      else
        log_warn "vmbackup.sh" "main" "Cloud replication completed with errors (see log for details)"
      fi
    fi
    
  elif [[ "$replication_mode" == "local_first" ]]; then
    #---------------------------------------------------------------------------
    # LOCAL_FIRST MODE: Complete local replication before starting cloud
    #---------------------------------------------------------------------------
    log_info "vmbackup.sh" "main" "Replication mode: local_first (local completes before cloud starts)"
    
    # Run local replication first
    if [[ $local_repl_needed -eq 1 ]]; then
      log_info "vmbackup.sh" "main" "Starting local replication phase"
      # Re-source config for DEST_* variables
      if [[ -f "$local_repl_config" ]]; then
        source "$local_repl_config"
      fi
      if run_local_replication_batch "$BACKUP_PATH"; then
        log_info "vmbackup.sh" "main" "Local replication completed successfully"
        local_repl_result=0
      else
        log_warn "vmbackup.sh" "main" "Local replication completed with errors (see log for details)"
        local_repl_result=1
      fi
    fi
    
    # Then run cloud replication
    if [[ $cloud_repl_needed -eq 1 ]]; then
      log_info "vmbackup.sh" "main" "Starting cloud replication phase"
      if invoke_cloud_replication "$BACKUP_PATH"; then
        log_info "vmbackup.sh" "main" "Cloud replication completed successfully"
      else
        log_warn "vmbackup.sh" "main" "Cloud replication completed with errors (see log for details)"
      fi
    fi
    
  elif [[ "$replication_mode" == "cloud_first" ]]; then
    #---------------------------------------------------------------------------
    # CLOUD_FIRST MODE: Complete cloud replication before starting local
    #---------------------------------------------------------------------------
    log_info "vmbackup.sh" "main" "Replication mode: cloud_first (cloud completes before local starts)"
    
    # Run cloud replication first
    if [[ $cloud_repl_needed -eq 1 ]]; then
      log_info "vmbackup.sh" "main" "Starting cloud replication phase"
      if invoke_cloud_replication "$BACKUP_PATH"; then
        log_info "vmbackup.sh" "main" "Cloud replication completed successfully"
        cloud_repl_result=0
      else
        log_warn "vmbackup.sh" "main" "Cloud replication completed with errors (see log for details)"
        cloud_repl_result=1
      fi
    fi
    
    # Then run local replication
    if [[ $local_repl_needed -eq 1 ]]; then
      log_info "vmbackup.sh" "main" "Starting local replication phase"
      # Re-source config for DEST_* variables
      if [[ -f "$local_repl_config" ]]; then
        source "$local_repl_config"
      fi
      if run_local_replication_batch "$BACKUP_PATH"; then
        log_info "vmbackup.sh" "main" "Local replication completed successfully"
      else
        log_warn "vmbackup.sh" "main" "Local replication completed with errors (see log for details)"
      fi
    fi
    
  else
    log_warn "vmbackup.sh" "main" "Unknown REPLICATION_ORDER: $replication_mode - skipping replication"
  fi
  
  # Clean up cancellation flag if it was set during replication
  # (flag may have been created while replication was in progress)
  is_replication_cancelled && clear_replication_cancel_flag
  
  # Note: Replication failures do NOT affect backup success status
  
  # Session Summary (detailed table of all VMs with accurate categorization)
  _log_session_summary "$backed_up_count" "$excluded_count" "$skipped_count" "$fail_count"
  
  # Final summary (clear natural language breakdown)
  log_info "vmbackup.sh" "main" "===== VM BACKUP SESSION COMPLETE ====="
  log_info "vmbackup.sh" "main" "FINAL RESULTS: $backed_up_count backed up, $excluded_count excluded, $skipped_count skipped, $fail_count failed"
  log_info "vmbackup.sh" "main" "Backup location: $BACKUP_PATH"
  log_info "vmbackup.sh" "main" "Full log: $LOG_FILE"
  
  # Calculate total bytes from VM_BACKUP_RESULTS
  local total_bytes=0
  for result in "${VM_BACKUP_RESULTS[@]}"; do
    IFS='|' read -r vm status btype duration ckpt size err policy <<< "$result"
    if [[ "$size" != "N/A" ]]; then
      local bytes_val
      # FF-163: sizes are formatted with `numfmt --to=iec-i` (e.g. '1.5GiB'),
      # and ${size%B} leaves an 'i' suffix ('1.5Gi') that --from=iec REJECTS,
      # so every >=1KiB VM contributed 0. Parse with the matching --from=iec-i.
      bytes_val=$(numfmt --from=iec-i "${size%B}" 2>/dev/null || echo 0)
      total_bytes=$((total_bytes + bytes_val))
    fi
  done
  
  # End SQLite session with final stats
  local final_status="success"
  if (( fail_count > 0 && backed_up_count == 0 )); then
    final_status="failed"
  elif (( fail_count > 0 )); then
    final_status="partial"
  fi
  
  if sqlite_is_available 2>/dev/null && ! is_dry_run; then
    local total_vms=$((backed_up_count + excluded_count + skipped_count + fail_count))
    sqlite_session_end "$total_vms" "$backed_up_count" "$fail_count" "$skipped_count" "$excluded_count" "$total_bytes" "$final_status"
    log_debug "vmbackup.sh" "main" "SQLite session ended: status=$final_status"
  fi
  
  {
    echo "================================================================================"
    echo "VM Backup Session Ended: $(date '+%Y-%m-%d %H:%M:%S')"
    echo "FINAL RESULTS: $backed_up_count backed up, $excluded_count excluded, $skipped_count skipped, $fail_count failed"
    echo "Auto-recovery actions performed (if checkpoint issues detected)"
    echo "================================================================================"
    echo ""
  } >> "$LOG_FILE"
  
  # Send email report
  local session_end_time=$(date '+%Y-%m-%d %H:%M:%S %Z')
  local overall_status="success"
  if (( fail_count > 0 && backed_up_count == 0 )); then
    overall_status="failed"
  elif (( fail_count > 0 )); then
    overall_status="partial"
  fi
  
  if is_dry_run; then
    log_info "vmbackup.sh" "main" "[DRY-RUN] Skipping email report"
  elif [[ -f "${SCRIPT_DIR}/modules/email_report_module.sh" ]]; then
    log_info "vmbackup.sh" "main" "Loading email report module"
    source "${SCRIPT_DIR}/modules/email_report_module.sh"
    
    if load_email_config; then
      log_info "vmbackup.sh" "main" "Sending email report to $EMAIL_RECIPIENT"
      local _rc=0
      send_backup_report "$session_start_time" "$session_end_time" "$overall_status" || _rc=$?
      _handle_notifier_rc "$_rc" main
    else
      log_debug "vmbackup.sh" "main" "Email disabled or not configured for this instance"
    fi
  else
    log_debug "vmbackup.sh" "main" "Email report module not found - skipping email notification"
  fi

  # Slack (parallel to the email block above)
  if ! is_dry_run && \
     [[ "${_SLACK_SENT:-false}" != "true" ]] && \
     [[ -f "${SCRIPT_DIR}/modules/slack_notification_module.sh" ]]; then
    source "${SCRIPT_DIR}/modules/slack_notification_module.sh"
    if load_slack_config; then
      local _src=0
      send_slack_notification "$session_start_time" "$session_end_time" "$overall_status" || _src=$?
      _handle_slack_rc "$_src" main
    fi
  fi
  
  if (( fail_count > 0 )); then
    log_error "vmbackup.sh" "main" "Session ended with failures - exit code 1"
    exit 1
  fi
  
  log_info "vmbackup.sh" "main" "Session ended successfully - exit code 0"
  exit 0
}

# Run main
main "$@"
