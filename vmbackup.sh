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
#   │   ├── chain_manifest_module.sh        JSON manifest management
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

# Version - used by Debian packaging and --version flag
VMBACKUP_VERSION="0.5.0"

# Dry-run mode: show what would happen without executing destructive operations
DRY_RUN=false

#################################################################################
# ARGUMENT PARSING (must happen before config load)
#################################################################################

# Config instance (default, test, etc.)
CONFIG_INSTANCE="default"

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
        --help|-h)
            cat << 'HELP_EOF'
Usage: vmbackup.sh [OPTIONS]

OPTIONS:
    --config-instance NAME  Use config from config/NAME/ (default: "default")
                            Examples: --config-instance test
    --dry-run               Show what would happen without executing backups,
                            retention, replication, or FSTRIM. Read-only mode.
    --cancel-replication    Signal a running vmbackup session to cancel its
                            replication phase. Backups continue unaffected.
                            Creates flag file in STATE_DIR; replication checks
                            this file and terminates gracefully.
    --version               Show version and exit
    --help                  Show this help message

CONFIG INSTANCES:
    default   Production config (config/default/)
    test      Test config - excludes production VMs (config/test/)

Each instance has its own:
    - vmbackup.conf        Main settings (BACKUP_PATH, retention, etc.)
    - vm_overrides.conf    Per-VM policy overrides (never = skip)
    - exclude_patterns.conf Glob patterns to exclude

EXAMPLES:
    sudo ./vmbackup.sh                          # Production (all VMs)
    sudo ./vmbackup.sh --config-instance test   # Test VMs only
    sudo ./vmbackup.sh --config-instance test --dry-run  # Preview without changes
    sudo ./vmbackup.sh --cancel-replication     # Cancel running replication
HELP_EOF
            exit 0
            ;;
        --version)
            echo "vmbackup ${VMBACKUP_VERSION}"
            exit 0
            ;;
        *)
            shift
            ;;
    esac
done

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
_VMBACKUP_CONFIG="${_VMBACKUP_SCRIPT_DIR}/config/${CONFIG_INSTANCE}/vmbackup.conf"
[[ -f "$_VMBACKUP_CONFIG" ]] && source "$_VMBACKUP_CONFIG"
# Restore env var if it was set (env takes precedence over config)
[[ -n "$_VMBACKUP_ENV_BACKUP_PATH" ]] && BACKUP_PATH="$_VMBACKUP_ENV_BACKUP_PATH"

#################################################################################
# CONFIGURATION SECTION
# 
# Priority: Environment variable > Config file > Default
# Edit config/default/vmbackup.conf to change defaults permanently.
#################################################################################

# Backup destination path (ensure trailing slash)
BACKUP_PATH="${BACKUP_PATH:-/mnt/backup/vms/}"
[[ "$BACKUP_PATH" != */ ]] && BACKUP_PATH="${BACKUP_PATH}/"


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
BACKUP_STARTUP_GRACE=300      # Grace period (5 min) for NBD setup, VM pause, checkpoint creation
BACKUP_STALL_THRESHOLD=180    # Declare backup hung if no I/O for this many seconds (3 min)
BACKUP_CHECK_INTERVAL=30      # Check backup progress every N seconds

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
VIRTNBD_THRESHOLD=""

# Backup output format: stream (thin-prov default) or raw (full-prov)
VIRTNBD_OUTPUT_FORMAT="stream"

# Use sparse detection (skip trimmed blocks, default=true)
VIRTNBD_SPARSE_DETECTION=true

# Scratch directory for fleece operations (default /var/tmp)
VIRTNBD_SCRATCH_DIR="/var/tmp"

#################################################################################
# PROCESS AND OPERATIONAL SETTINGS
#################################################################################

# Process priority settings for backup operations
# These control how "polite" the backup process is to other system tasks
#
# CPU Priority (nice): -20=highest, 0=normal, 19=lowest
# Lower values = more CPU time = faster backups but more VM impact
PROCESS_PRIORITY=10

# I/O Priority Class (ionice -c):
#   1 = Realtime (use with caution, can starve other I/O)
#   2 = Best-effort (normal, respects nice level)
#   3 = Idle (only when disk is idle - very slow)
IO_PRIORITY_CLASS=2

# I/O Priority Level (ionice -n): 0-7 (only for class 2)
#   0 = highest priority within class
#   4 = normal
#   7 = lowest priority within class
IO_PRIORITY_LEVEL=5

#################################################################################
# CHECKPOINT MANAGEMENT STRATEGY (Monthly Rotation with Health Checks)
#################################################################################

# Intelligent monthly backup strategy:
# - Day 1 of month: Always FULL backup (resets all checkpoints)
# - Days 2-28: AUTO mode (incremental if checkpoint healthy)
CHECKPOINT_FORCE_FULL_ON_DAY=1        # Day of month for full backup
CHECKPOINT_HEALTH_CHECK=yes            # Validate checkpoint before AUTO mode
CHECKPOINT_MAX_DEPTH_WARN=10          # Warn if chain exceeds this
CHECKPOINT_RETRY_AUTO_TO_FULL=yes     # Convert to FULL if AUTO fails
CHECKPOINT_MAX_RETRIES_AUTO=1         # Max AUTO retries before FULL

#################################################################################
# OPERATIONAL SETTINGS - LOADED FROM CONFIG
#
# These settings are now loaded from config/<instance>/vmbackup.conf
# If missing from config, safe defaults are applied with EXPLICIT warnings.
# See validate_operational_settings() for default values and logging.
#################################################################################

# Retry configuration
MAX_RETRIES=2
RETRY_DELAY=30  # seconds

# State directory (locks, logs, recovery flags)
STATE_DIR="${BACKUP_PATH}_state"
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
    mkdir -p "$STATE_DIR" 2>/dev/null
    echo "$(date '+%Y-%m-%d %H:%M:%S') PID=$$ user=$(whoami)" > "$CANCEL_REPLICATION_FLAG"
    echo "Replication cancellation requested."
    echo "Flag file created: $CANCEL_REPLICATION_FLAG"
    echo "Active replication jobs will terminate gracefully within ~30 seconds."
    exit 0
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
    echo "================================================================================"
  } >> "$LOG_FILE"
}

# Source shared logging library
source "${SCRIPT_DIR}/lib/logging.sh"

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
    chown root:backup "$BACKUP_PATH" 2>/dev/null || true
    chmod 2750 "$BACKUP_PATH" 2>/dev/null || true
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
        -exec chown root:backup {} + 2>/dev/null || true
      # Set SGID on directories for automatic group inheritance
      find "$target_path" -type d \
        -not -path '*/tpm-state/*' -not -path '*/tpm-state' \
        -exec chmod g+s {} + 2>/dev/null || true
    else
      chown root:backup "$target_path" 2>/dev/null || true
      # Set SGID on directories for automatic group inheritance
      if [[ -d "$target_path" ]]; then
        chmod g+s "$target_path" 2>/dev/null || true
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
    ENABLE_FSTRIM="false"
    log_warn "vmbackup.sh" "validate_operational_settings" "MISSING: ENABLE_FSTRIM not set in config/$instance/vmbackup.conf"
    log_warn "vmbackup.sh" "validate_operational_settings" "USING DEFAULT: ENABLE_FSTRIM=false (fstrim disabled for safety)"
    ((missing_count++))
  else
    log_debug "vmbackup.sh" "validate_operational_settings" "ENABLE_FSTRIM=$ENABLE_FSTRIM (from config)"
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
  
  #-----------------------------------------------------------------------------
  # Offline VM Optimization Settings
  #-----------------------------------------------------------------------------
  if [[ -z "${SKIP_OFFLINE_UNCHANGED_BACKUPS+x}" ]]; then
    SKIP_OFFLINE_UNCHANGED_BACKUPS="false"
    log_warn "vmbackup.sh" "validate_operational_settings" "MISSING: SKIP_OFFLINE_UNCHANGED_BACKUPS not set in config/$instance/vmbackup.conf"
    log_warn "vmbackup.sh" "validate_operational_settings" "USING DEFAULT: SKIP_OFFLINE_UNCHANGED_BACKUPS=false (always backup offline VMs)"
    ((missing_count++))
  else
    log_debug "vmbackup.sh" "validate_operational_settings" "SKIP_OFFLINE_UNCHANGED_BACKUPS=$SKIP_OFFLINE_UNCHANGED_BACKUPS (from config)"
  fi
  
  if [[ -z "${OFFLINE_CHANGE_DETECTION_THRESHOLD+x}" ]]; then
    OFFLINE_CHANGE_DETECTION_THRESHOLD=60
    log_warn "vmbackup.sh" "validate_operational_settings" "MISSING: OFFLINE_CHANGE_DETECTION_THRESHOLD not set in config/$instance/vmbackup.conf"
    log_warn "vmbackup.sh" "validate_operational_settings" "USING DEFAULT: OFFLINE_CHANGE_DETECTION_THRESHOLD=60 (60 second change window)"
    ((missing_count++))
  else
    log_debug "vmbackup.sh" "validate_operational_settings" "OFFLINE_CHANGE_DETECTION_THRESHOLD=$OFFLINE_CHANGE_DETECTION_THRESHOLD (from config)"
  fi
  
  #-----------------------------------------------------------------------------
  # Checkpoint Recovery Settings
  #-----------------------------------------------------------------------------
  if [[ -z "${ENABLE_AUTO_RECOVERY_ON_CHECKPOINT_CORRUPTION+x}" ]]; then
    ENABLE_AUTO_RECOVERY_ON_CHECKPOINT_CORRUPTION="warn"
    log_warn "vmbackup.sh" "validate_operational_settings" "MISSING: ENABLE_AUTO_RECOVERY_ON_CHECKPOINT_CORRUPTION not set in config/$instance/vmbackup.conf"
    log_warn "vmbackup.sh" "validate_operational_settings" "USING DEFAULT: ENABLE_AUTO_RECOVERY_ON_CHECKPOINT_CORRUPTION=warn (will fail on corruption, manual fix required)"
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
      log_warn "vmbackup.sh" "validate_operational_settings" "USING DEFAULT: ENABLE_AUTO_RECOVERY_ON_CHECKPOINT_CORRUPTION=warn"
      ENABLE_AUTO_RECOVERY_ON_CHECKPOINT_CORRUPTION="warn"
      ;;
  esac
  
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
  local state_dir="${STATE_DIR:-${BACKUP_PATH}_state}"

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
  local required_tools=("virsh" "virtnbdbackup" "bash" "grep" "awk" "sed" "cut" "tr" "wc" "find" "date" "stat" "mkdir" "rm" "touch" "chmod" "tar" "xmllint")
  
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
  
  # Check libvirt daemon is running (virsh availability already checked in the loop above)
  if command -v virsh &>/dev/null && ! virsh list --all >/dev/null 2>&1; then
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
    
    # Check if the scratch dir is allowed in libvirt-qemu AppArmor profile
    local scratch_pattern="${scratch_dir}/virtnbdbackup.*"
    local needs_fix=false
    
    if [[ -f "$aa_abstraction" ]]; then
      # Check if virtnbdbackup socket access is already allowed
      if ! grep -q "virtnbdbackup" "$aa_abstraction" 2>/dev/null && \
         ! grep -q "virtnbdbackup" "$aa_local_file" 2>/dev/null; then
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
  status=$(virsh domstate "$vm_name" 2>/dev/null | tr -d '\n') || status="unknown"
  [[ -z "$status" ]] && status="unknown"
  log_debug "vmbackup.sh" "get_vm_status" "VM '$vm_name' state: '$status'"
  echo "$status"
}

# Check if QEMU guest agent is responsive
check_qemu_agent() {
  local vm_name=$1
  
  # Try to ping the agent
  virsh qemu-agent-command "$vm_name" '{"execute":"guest-ping"}' &>/dev/null
  local rc=$?
  log_debug "vmbackup.sh" "check_qemu_agent" "Agent ping for '$vm_name': $([ $rc -eq 0 ] && echo 'responsive' || echo "not responding (rc=$rc)")"
  return $rc
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
  
  # Log to SQLite database (parallel to CSV logging)
  if sqlite_is_available 2>/dev/null && [[ "$DRY_RUN" != true ]]; then
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

# Source shared locking library
source "${SCRIPT_DIR}/lib/vm_lock.sh"

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
# CRITICAL EDGE CASE (Not Currently Handled):
#   - perform_backup() does NOT distinguish between return value 0 vs 1
#   - Retry logic retries with FSFREEZE still enabled (-F flag)
#   - If stall was caused by FSFREEZE, retry will hang the same way
#   - Should disable FSFREEZE on retry: VIRTNBD_FSFREEZE=false before retry
#   - See GitHub #102 mitigation strategy for future enhancement

monitor_backup_progress() {
  local backup_pid=$1
  local backup_dir=$2
  local vm_name=$3
  
  # PHASE 0: Wait for virtnbdbackup to create *.data.partial file
  # File creation can be delayed on slow systems (NBD setup, VM pause, checkpoint creation)
  # Retry for up to 10 minutes before giving up on monitoring
  local data_file=""
  local max_retries=120
  local retry_count=0
  
  while [[ -z "$data_file" && $retry_count -lt $max_retries ]]; do
    sleep 5
    data_file=$(find "$backup_dir" -maxdepth 1 -name "*.data.partial" | head -1)
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
        log_error "vmbackup.sh" "monitor_backup_progress" "  (This usually indicates guest agent FSFREEZE failure - retry should disable FSFREEZE)"
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
  local lock_file="$LOCK_DIR/vmbackup-${vm_name}.lock"
  
  # Check if lock file exists
  [[ -f "$lock_file" ]] || return 1
  
  # Read PID from lock file
  local lock_pid=$(cat "$lock_file" 2>/dev/null)
  [[ -z "$lock_pid" ]] && return 1
  
  # Check if process is still running
  if ! kill -0 "$lock_pid" 2>/dev/null; then
    # Process not running - remove stale lock and return false
    log_debug "vmbackup.sh" "has_lock" "Stale lock for '$vm_name': PID $lock_pid not running, removing"
    rm -f "$lock_file"
    return 1
  fi
  
  # Verify it's actually a backup process (not PID reuse)
  local cmdline=$(cat "/proc/$lock_pid/cmdline" 2>/dev/null | tr '\0' ' ')
  if [[ ! "$cmdline" =~ vmbackup ]]; then
    # Different process using same PID - remove stale lock
    log_debug "vmbackup.sh" "has_lock" "Stale lock for '$vm_name': PID $lock_pid reused by non-backup process, removing"
    rm -f "$lock_file"
    return 1
  fi
  
  # Lock is valid and from active backup process
  log_debug "vmbackup.sh" "has_lock" "Active lock for '$vm_name': PID $lock_pid is running vmbackup"
  return 0
}

#################################################################################
# EMERGENCY INTERRUPT RECOVERY FUNCTIONS
#################################################################################

# Clean up lock, active checkpoint, and partial backup files for interrupted VM
# Called when CTRL+Z or other interrupt is detected mid-backup
# Purpose: Recover from interrupted backup and allow retry in same session
emergency_cleanup_current_vm() {
  local vm_name=${1:?Error: vm_name required}
  
  log_warn "vmbackup.sh" "emergency_cleanup_current_vm" "Starting emergency cleanup for VM: $vm_name"
  
  # Remove lock file
  local lock_file="$LOCK_DIR/vmbackup-${vm_name}.lock"
  if [[ -f "$lock_file" ]]; then
    log_debug "vmbackup.sh" "emergency_cleanup_current_vm" "Deleting lock file: $lock_file"
    rm -f "$lock_file"
    log_info "vmbackup.sh" "emergency_cleanup_current_vm" "Removed stale lock file: $lock_file"
  fi
  
  # Remove orphaned bitmaps (Issue #223: metadata deleted but QEMU bitmap persists)
  # This is critical because orphaned bitmaps cause "bitmap not found in backing chain" errors
  remove_orphaned_vm_bitmaps "$vm_name" "$BACKUP_BASE_DIR/$vm_name" || true
  
  # Remove partial/incomplete backup files from this session
  local backup_dir="$BACKUP_BASE_DIR/$vm_name"
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
  local lock_file="$LOCK_DIR/vmbackup-${vm_name}.lock"
  if [[ -f "$lock_file" ]]; then
    local lock_age=$(($(date +%s) - $(stat -c %Y "$lock_file" 2>/dev/null)))
    if [[ $lock_age -lt 300 ]]; then  # < 5 minutes
      log_warn "vmbackup.sh" "detect_interrupted_backup" "Fresh stale lock detected for $vm_name (age: ${lock_age}s)"
      return 0  # Interrupted backup detected
    fi
  fi
  
  # Check for active checkpoint (indicates incomplete backup operation)
  local checkpoints
  checkpoints=$(virsh checkpoint-list "$vm_name" --name 2>/dev/null | grep -c "^virtnbdbackup\." || true)
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
  
  local removed_count=0
  local failed_count=0
  
  log_info "vmbackup.sh" "remove_orphaned_vm_bitmaps" "Starting bitmap removal for VM: $vm_name"
  
  # Get all disk paths for VM
  while IFS= read -r disk_path; do
    [[ -z "$disk_path" ]] && continue
    [[ ! -e "$disk_path" ]] && continue
    
    # Use qemu-img info to find bitmaps
    local bitmap_json=$(qemu-img info --output=json "$disk_path" 2>/dev/null | grep -o '"bitmaps":\[.*\]' | head -1)
    
    if [[ -n "$bitmap_json" ]]; then
      # Extract and remove each bitmap
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
      done < <(echo "$bitmap_json" | grep -o '"name":"[^"]*"' | cut -d'"' -f4)
    fi
  done < <(virsh domblklist "$vm_name" 2>/dev/null | grep -E '^\s+' | awk '{print $2}' | grep -v '^-$')
  
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
  if ! virsh dumpxml "$vm_name" > "$config_file" 2>/dev/null; then
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

# Backup host-level libvirt, QEMU, and network configuration
# Strategy: Tar and compress /etc/libvirt, QEMU, and network bridge configs
# Keep: First backup of each month + any subsequent backups if config changed
# Location: $BACKUP_PATH/__HOST_CONFIG__/YYYYMM/ (centralized, not per-VM)
#
# Network paths captured (both may exist — only the active manager matters):
#   /etc/network/              — ifupdown (interfaces, interfaces.d/)
#   /etc/NetworkManager/system-connections/ — NetworkManager connection profiles
backup_host_config() {
  local current_month=$(get_current_month)
  local host_config_dir="$BACKUP_PATH/__HOST_CONFIG__/$current_month"
  local backup_date=$(date '+%Y%m%d_%H%M%S')
  
  mkdir -p "$host_config_dir"
  
  # Generate host config filename
  local config_archive="$host_config_dir/libvirt_qemu_config_${backup_date}.tar.gz"
  
  # Create temporary tar for comparison
  local temp_tar="/tmp/libvirt_config_temp_$$.tar.gz"
  
  # Tar libvirt, QEMU, and network configs (requires root access, use sudo if needed)
  # Missing directories are silently ignored by tar (stderr suppressed)
  local -a tar_paths=(
    /etc/libvirt
    /var/lib/libvirt/qemu/
    /var/lib/libvirt/network/
    /var/lib/libvirt/storage/
    /var/lib/libvirt/secrets/
    /var/lib/libvirt/dnsmasq/
    /etc/network/
    /etc/NetworkManager/system-connections/
  )
  
  # Check if we have read permission to /etc/libvirt
  if [[ -r /etc/libvirt/qemu.conf ]]; then
    # We have permission, tar directly (ignore errors for missing directories)
    log_debug "vmbackup.sh" "backup_host_config" "Using direct tar (have read permission to /etc/libvirt)"
    tar czf "$temp_tar" "${tar_paths[@]}" 2>/dev/null
    if [[ ! -s "$temp_tar" ]]; then
      rm -f "$temp_tar"
      log_error "vmbackup.sh" "backup_host_config" "Failed to create host configuration archive"
      return 1
    fi
  else
    # Don't have read permission, try with sudo
    log_debug "vmbackup.sh" "backup_host_config" "Using sudo tar (no read permission to /etc/libvirt)"
    sudo tar czf "$temp_tar" "${tar_paths[@]}" 2>/dev/null
    if [[ ! -s "$temp_tar" ]]; then
      rm -f "$temp_tar"
      log_error "vmbackup.sh" "backup_host_config" "Failed to create host configuration archive (need root/sudo for /etc/libvirt access)"
      return 1
    fi
    # Ensure we own the temp file
    sudo chown $(id -u):$(id -g) "$temp_tar" 2>/dev/null || true
  fi
  
  # Check if this is the first of the month
  local first_of_month_file="$host_config_dir/libvirt_qemu_config_${current_month}_FIRST.tar.gz"
  
  if [[ ! -f "$first_of_month_file" ]]; then
    # First backup of month - keep it and mark as first
    mv "$temp_tar" "$first_of_month_file"
    chmod 600 "$first_of_month_file"
    # mv preserves original ownership from /tmp/ — fix to match SGID parent
    chown root:backup "$first_of_month_file" 2>/dev/null || true
    log_info "vmbackup.sh" "backup_host_config" "Host config backup: FIRST of month"
    if declare -f log_file_operation >/dev/null 2>&1; then
      log_file_operation "create" "__HOST__" "$first_of_month_file" "" \
        "host_config" "First-of-month host config archive" "backup_host_config" "true"
    fi
    return 0
  fi
  
  # Compare with first-of-month backup
  if ! cmp -s "$first_of_month_file" "$temp_tar"; then
    # Config changed - keep this backup too
    mv "$temp_tar" "$config_archive"
    chmod 600 "$config_archive"
    # mv preserves original ownership from /tmp/ — fix to match SGID parent
    chown root:backup "$config_archive" 2>/dev/null || true
    log_info "vmbackup.sh" "backup_host_config" "Host config backup: CHANGED, retained"
    if declare -f log_file_operation >/dev/null 2>&1; then
      log_file_operation "create" "__HOST__" "$config_archive" "" \
        "host_config" "Host config changed - retained" "backup_host_config" "true"
    fi
    return 0
  else
    # Config unchanged - delete the temporary backup
    rm -f "$temp_tar"
    log_info "vmbackup.sh" "backup_host_config" "Host config backup: unchanged, not retained"
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
# MITIGATION: Enhanced threshold checking (20% free = critical threshold)
check_disk_space() {
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
  
  log_info "vmbackup.sh" "check_disk_space" "Available space: ${available_gb}GB / ${total_gb}GB (${percentage_free}% free)"
  
  # CRITICAL: If destination < 20% free, bitmap corruption risk is HIGH
  # This matches virtnbdbackup issue: full destination causes incomplete backups
  if (( percentage_free < 20 )); then
    log_error "vmbackup.sh" "check_disk_space" "CRITICAL: Destination only has ${percentage_free}% free space (${available_gb}GB)"
    log_error "vmbackup.sh" "check_disk_space" "Risk: Backup may fail mid-operation causing bitmap corruption (GitHub issue #226)"
    log_error "vmbackup.sh" "check_disk_space" "Action: Free up space or this backup will be skipped to prevent corruption"
    return 1
  fi
  
  # Warn if < 50GB or < 30%
  if (( available_mb < 51200 )) || (( percentage_free < 30 )); then
    log_warn "vmbackup.sh" "check_disk_space" "Low disk space: ${available_gb}GB / ${total_gb}GB (${percentage_free}% free, threshold: 50GB or 30%)"
  fi
  
  # Error if < 10GB absolute (catches small disks where 20% is still too little)
  if (( available_mb < 10240 )); then
    log_error "vmbackup.sh" "check_disk_space" "Critical: Insufficient space for backup: ${available_gb}GB (${percentage_free}% free)"
    return 1
  fi
  
  return 0
}

# Check libvirt version compatibility
check_libvirt_version() {
  log_info "vmbackup.sh" "check_libvirt_version" "Checking libvirt version >= 7.2 (required for backup API)"
  
  local version=$(virsh version 2>/dev/null | grep "Using library:" | awk '{print $4}')
  
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
      
      # Check if the process in the lock file is still running
      if [[ -n "$locked_pid" ]] && kill -0 "$locked_pid" 2>/dev/null; then
        # Process is running - verify it's actually a backup process for this VM
        local proc_cmdline=$(cat "/proc/$locked_pid/cmdline" 2>/dev/null | tr '\0' ' ')
        if [[ "$proc_cmdline" == *"vmbackup"* ]] || [[ "$proc_cmdline" == *"virtnbdbackup"* ]]; then
          log_warn "vmbackup.sh" "cleanup_system_checkpoints_and_locks" "Lock file is old BUT backup process IS running (PID: $locked_pid) - keeping lock"
          continue
        fi
      fi
      
      # No running backup process found - safe to delete the stale lock
      log_warn "vmbackup.sh" "cleanup_system_checkpoints_and_locks" "Stale lock detected for VM: $vm_name (no active backup process) - removing and will retry"
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
  # grep -c outputs "0" AND exits 1 when no matches — || echo "0" would
  # produce a second "0", resulting in "0 0" in command substitution.
  # Use || true to suppress the non-zero exit without extra output.
  virsh checkpoint-list "$vm_name" --name 2>/dev/null | grep -c "^virtnbdbackup\." || true
}

# Get actual restore point count from data files on disk (current chain only)
# This counts real backup data files that can be used for restore:
# - *.full.data (full backup)
# - *.copy.data (copy-mode full backup)  
# - *.inc.virtnbdbackup.*.data (incremental backups)
# Parameters: $1 = backup_dir (VM's backup directory)
# Returns: Number of restorable backup points in the current chain
get_restore_point_count() {
  local backup_dir="${1:?Error: backup_dir required}"
  
  if [[ ! -d "$backup_dir" ]]; then
    echo "0"
    return
  fi
  
  # Count data files in the root directory (current chain)
  # Excludes .archives/ which contains archived chains
  local count=0
  
  # Count full/copy backups (base of chain)
  local full_count=$(find "$backup_dir" -maxdepth 1 -type f \( -name "*.full.data" -o -name "*.copy.data" \) 2>/dev/null | wc -l)
  
  # Count incremental backups
  local inc_count=$(find "$backup_dir" -maxdepth 1 -type f -name "*.inc.virtnbdbackup.*.data" 2>/dev/null | wc -l)
  
  # Total = full + incrementals
  # Note: A chain with 1 full + 3 incrementals = 4 restore points
  count=$((full_count + inc_count))
  
  log_debug "vmbackup.sh" "get_restore_point_count" "Dir='$backup_dir' full=$full_count inc=$inc_count total=$count"
  echo "$count"
}

#################################################################################
# CSV LOGGING HELPER FUNCTIONS
# These functions calculate metrics for the enhanced CSV schema (v2.0)
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
  
  if ! virsh suspend "$vm_name" 2>/dev/null; then
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
  
  if ! virsh resume "$vm_name" 2>/dev/null; then
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
  CACHED_VM_STATE=$(virsh domstate "$vm_name" 2>/dev/null | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')
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
  checkpoints_raw=$(virsh checkpoint-list "$vm_name" --name 2>/dev/null | grep "^virtnbdbackup\." | sort -V)
  
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
    if find "$backup_dir" -maxdepth 1 \( -name "*virtnbdbackup*.qcow.json" -o -name "vmconfig.virtnbdbackup*.xml" \) 2>/dev/null | grep -q .; then
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
    if find "$backup_dir" -maxdepth 1 -name "*.copy.qcow.json" 2>/dev/null | grep -q . && \
       ! find "$backup_dir" -maxdepth 1 -name "*.copy.data" 2>/dev/null | grep -q .; then
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
    running) vm_state_note="can do incremental" ;;
    paused)  vm_state_note="can do incremental (will unpause after)" ;;
    shutoff|"shut off") vm_state_note="copy mode only" ;;
    *)       vm_state_note="unknown state" ;;
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
  elif [[ "$CACHED_VM_STATE" == "shutoff" ]] || [[ "$CACHED_VM_STATE" == "shut off" ]]; then
    result_type="copy (VM offline)"
  fi
  
  # Reuse data file counts from Phase 3
  local data_summary="none"
  if [[ $full_data_files -gt 0 ]] || [[ $inc_data_files -gt 0 ]]; then
    data_summary="${full_data_files} full + ${inc_data_files} incremental"
  fi
  
  # Get .cpt marker name
  local cpt_marker="none"
  if [[ -n "$cpt_files" ]]; then
    cpt_marker=$(basename "$(echo "$cpt_files" | head -1)" 2>/dev/null)
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
  if [[ "$day_of_month" == "$(printf '%02d' $CHECKPOINT_FORCE_FULL_ON_DAY)" ]]; then
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
  local vm_state=$(virsh domstate "$vm_name" 2>/dev/null)
  if ! echo "$vm_state" | grep -q "running\|paused"; then
    log_warn "vmbackup.sh" "validate_checkpoint_health" \
      "VM state is $vm_state - backup will be in COPY mode (not incremental)"
    return 1
  fi
  
  # CHECK 2: QEMU Checkpoints exist (informational)
  local checkpoints=($(virsh checkpoint-list "$vm_name" --name 2>/dev/null | grep "^virtnbdbackup\."))
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
  vm_state=$(virsh domstate "$vm_name" 2>/dev/null)
  if [[ "$vm_state" != "running" && "$vm_state" != "paused" ]]; then
    log_debug "vmbackup.sh" "remove_stale_qemu_bitmaps" "VM $vm_name is $vm_state - skipping bitmap cleanup (only needed for running/paused VMs)"
    return 0
  fi

  # Query all block devices and extract virtnbdbackup dirty bitmaps
  local query_output
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
        log_warn "vmbackup.sh" "prepare_backup_directory" "[Cleanup 2/2] Archive failed but continuing - copy backup files may remain"
      fi
      
      local cleanup_count=0
      
      # Delete orphaned QEMU checkpoints (from pre-copy-backup chain, now stale)
      # These exist in qcow2 but don't match the copy backup - must be cleared for new chain
      local checkpoints=($(virsh checkpoint-list "$vm_name" --name 2>/dev/null | grep "^virtnbdbackup\." || true))
      if [[ ${#checkpoints[@]} -gt 0 ]]; then
        log_info "vmbackup.sh" "prepare_backup_directory" "[Cleanup 2/2] Found ${#checkpoints[@]} orphaned QEMU checkpoints - clearing for new chain"
        for cp in "${checkpoints[@]}"; do
          if virsh checkpoint-delete "$vm_name" "$cp" --metadata 2>/dev/null; then
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
      
      # Archive existing valid backup chain before cleanup destroys it
      local has_valid_chain=false
      if find "$backup_dir" -maxdepth 1 \( -name "*.full.data" -o -name "*.copy.data" \) 2>/dev/null | grep -q .; then
        has_valid_chain=true
      fi
      
      if [[ "$has_valid_chain" == "true" ]]; then
        log_info "vmbackup.sh" "prepare_backup_directory" "[Cleanup 2/2] Valid backup chain detected - archiving before cleanup"
        if archive_existing_checkpoint_chain "$vm_name" "$backup_dir"; then
          log_info "vmbackup.sh" "prepare_backup_directory" "[Cleanup 2/2] Previous chain archived successfully"
        else
          log_warn "vmbackup.sh" "prepare_backup_directory" "[Cleanup 2/2] Chain archival failed - proceeding with cleanup anyway"
        fi
      else
        log_debug "vmbackup.sh" "prepare_backup_directory" "[Cleanup 2/2] No valid backup chain to archive"
      fi
      
      local cleanup_count=0
      
      # Delete QEMU checkpoints
      local checkpoints
      mapfile -t checkpoints < <(virsh checkpoint-list "$vm_name" --name 2>/dev/null | grep "^virtnbdbackup\.")
      for cp in "${checkpoints[@]}"; do
        if virsh checkpoint-delete "$vm_name" "$cp" --metadata 2>/dev/null; then
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
      
      # Archive existing valid backup chain before cleanup destroys it
      local has_valid_chain=false
      if find "$backup_dir" -maxdepth 1 \( -name "*.full.data" -o -name "*.copy.data" \) 2>/dev/null | grep -q .; then
        has_valid_chain=true
      fi
      
      if [[ "$has_valid_chain" == "true" ]]; then
        log_info "vmbackup.sh" "prepare_backup_directory" "[Cleanup 2/2] Valid backup chain detected - archiving before cleanup"
        if archive_existing_checkpoint_chain "$vm_name" "$backup_dir"; then
          log_info "vmbackup.sh" "prepare_backup_directory" "[Cleanup 2/2] Previous chain archived successfully"
        else
          log_warn "vmbackup.sh" "prepare_backup_directory" "[Cleanup 2/2] Chain archival failed - proceeding with cleanup anyway"
        fi
      else
        log_debug "vmbackup.sh" "prepare_backup_directory" "[Cleanup 2/2] No valid backup chain to archive"
      fi
      
      local cleanup_count=0
      
      # Delete ALL QEMU checkpoints for this VM
      local checkpoints
      mapfile -t checkpoints < <(virsh checkpoint-list "$vm_name" --name 2>/dev/null | grep "^virtnbdbackup\.")
      if [[ ${#checkpoints[@]} -gt 0 ]]; then
        for cp in "${checkpoints[@]}"; do
          if virsh checkpoint-delete "$vm_name" "$cp" --metadata 2>/dev/null; then
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
      
      local cleanup_count=0
      
      # Delete ALL QEMU checkpoints for this VM
      local checkpoints
      mapfile -t checkpoints < <(virsh checkpoint-list "$vm_name" --name 2>/dev/null | grep "^virtnbdbackup\.")
      if [[ ${#checkpoints[@]} -gt 0 ]]; then
        for cp in "${checkpoints[@]}"; do
          if virsh checkpoint-delete "$vm_name" "$cp" --metadata 2>/dev/null; then
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
      
      # Archive existing valid backup chain before cleanup destroys it
      # An interrupted incremental may still have a valid FULL that's restorable
      local has_valid_chain=false
      if find "$backup_dir" -maxdepth 1 \( -name "*.full.data" -o -name "*.copy.data" \) 2>/dev/null | grep -q .; then
        has_valid_chain=true
      fi
      
      if [[ "$has_valid_chain" == "true" ]]; then
        log_info "vmbackup.sh" "prepare_backup_directory" "[Cleanup 2/2] Valid backup data detected - archiving before cleanup"
        if archive_existing_checkpoint_chain "$vm_name" "$backup_dir"; then
          log_info "vmbackup.sh" "prepare_backup_directory" "[Cleanup 2/2] Previous chain archived successfully"
        else
          log_warn "vmbackup.sh" "prepare_backup_directory" "[Cleanup 2/2] Chain archival failed - proceeding with cleanup anyway"
        fi
      else
        log_debug "vmbackup.sh" "prepare_backup_directory" "[Cleanup 2/2] No valid backup chain to archive"
      fi
      
      local cleanup_count=0
      
      # Delete ALL QEMU checkpoints for this VM
      local checkpoints
      mapfile -t checkpoints < <(virsh checkpoint-list "$vm_name" --name 2>/dev/null | grep "^virtnbdbackup\.")
      if [[ ${#checkpoints[@]} -gt 0 ]]; then
        for cp in "${checkpoints[@]}"; do
          if virsh checkpoint-delete "$vm_name" "$cp" --metadata 2>/dev/null; then
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
    # Build virtnbdbackup command with all options
    # Strategy: Leverage virtnbdbackup's native capabilities for full/incremental backups,
    # compression, and changed block tracking (CBT) via QEMU dirty bitmaps/checkpoints
    local virtnbd_cmd="virtnbdbackup"
    
    # Core: domain, backup level, compression, output format, output dir
    virtnbd_cmd="$virtnbd_cmd -d \"$vm_name\" -l \"$backup_type\""
    virtnbd_cmd="$virtnbd_cmd --compress=$VIRTNBD_COMPRESS_LEVEL"
    if [[ "$VIRTNBD_OUTPUT_FORMAT" != "stream" ]]; then
      virtnbd_cmd="$virtnbd_cmd -t $VIRTNBD_OUTPUT_FORMAT"
    fi
    virtnbd_cmd="$virtnbd_cmd -o \"$backup_dir\""
    
    # Parallel workers (auto=CPU count)
    if [[ -n "$VIRTNBD_WORKERS" && "$VIRTNBD_WORKERS" != "auto" ]]; then
      virtnbd_cmd="$virtnbd_cmd --worker $VIRTNBD_WORKERS"
    fi
    
    # Selective disk backup (exclude/include)
    if [[ -n "$VIRTNBD_EXCLUDE_DISKS" ]]; then
      virtnbd_cmd="$virtnbd_cmd -x \"$VIRTNBD_EXCLUDE_DISKS\""
    fi
    
    if [[ -n "$VIRTNBD_INCLUDE_DISKS" ]]; then
      virtnbd_cmd="$virtnbd_cmd -i \"$VIRTNBD_INCLUDE_DISKS\""
    fi
    
    # Filesystem consistency via QEMU guest agent
    if [[ "$VIRTNBD_FSFREEZE" == "true" ]]; then
      if [[ -n "$VIRTNBD_FSFREEZE_PATHS" ]]; then
        virtnbd_cmd="$virtnbd_cmd -F \"$VIRTNBD_FSFREEZE_PATHS\""
      fi
    fi
    
    # Skip backup if changed data < threshold
    if [[ -n "$VIRTNBD_THRESHOLD" ]]; then
      virtnbd_cmd="$virtnbd_cmd --threshold $VIRTNBD_THRESHOLD"
    fi
    
    # Disable sparse block detection if configured
    if [[ "$VIRTNBD_SPARSE_DETECTION" == "false" ]]; then
      virtnbd_cmd="$virtnbd_cmd --no-sparse-detection"
    fi
    
    # Scratch directory for NBD socket and fleece operations
    virtnbd_cmd="$virtnbd_cmd --scratchdir \"$VIRTNBD_SCRATCH_DIR\""
    
    log_info "vmbackup.sh" "perform_backup" "Command: $virtnbd_cmd"
    log_info "vmbackup.sh" "perform_backup" "Backup level: $backup_type | Output: $backup_dir | Compression: $VIRTNBD_COMPRESS_LEVEL"
    log_info "vmbackup.sh" "perform_backup" "Backup starting at $(date '+%Y-%m-%d %H:%M:%S')"
    
    # DRY-RUN: Skip actual backup execution
    if [[ "$DRY_RUN" == true ]]; then
      log_info "vmbackup.sh" "perform_backup" "[DRY-RUN] Would execute: $virtnbd_cmd"
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
    
    if [[ -n "$priority_wrapper" ]]; then
      log_debug "vmbackup.sh" "perform_backup" "Running with priority: $priority_wrapper"
      _BACKUP_IN_PROGRESS="true"
      $priority_wrapper bash -c "$virtnbd_cmd" > >(tee -a "$backup_log") 2>&1 &
    else
      _BACKUP_IN_PROGRESS="true"
      bash -c "$virtnbd_cmd" > >(tee -a "$backup_log") 2>&1 &
    fi
    
    local backup_pid=$!
    
    # Start progress monitor in background - watches for stalls, not elapsed time
    monitor_backup_progress $backup_pid "$backup_dir" "$vm_name" &
    local monitor_pid=$!
    
    # Wait for backup to complete
    if wait $backup_pid 2>/dev/null; then
      _BACKUP_IN_PROGRESS="false"
      local backup_end_time=$(date '+%Y-%m-%d %H:%M:%S')
      log_info "vmbackup.sh" "perform_backup" "$backup_type backup successful for VM: $vm_name at $backup_end_time"
      
      # Kill monitor if still running
      kill $monitor_pid 2>/dev/null || true
      
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
      
      return 0
    else
      local exit_code=$?
      _BACKUP_IN_PROGRESS="false"
      local backup_end_time=$(date '+%Y-%m-%d %H:%M:%S')
      log_error "vmbackup.sh" "perform_backup" "$backup_type backup failed for VM: $vm_name (exit code: $exit_code) at $backup_end_time"
      log_error "vmbackup.sh" "perform_backup" "Backup directory state: $(ls -lh "$backup_dir" 2>/dev/null | head -10 || echo 'Directory not accessible')"
      
      # Kill monitor if still running (must happen on failure path too)
      kill $monitor_pid 2>/dev/null || true
      wait $monitor_pid 2>/dev/null || true
      
      # Capture virtnbdbackup log tail for error reporting
      local log_tail=""
      if [[ -f "$backup_log" ]]; then
        log_tail=$(tail -5 "$backup_log" 2>/dev/null | tr '\n' ' ' | cut -c1-200)
      fi
      
      # Set specific error code with virtnbdbackup exit code
      set_backup_error "VIRTNBD_EXIT_${exit_code}" "virtnbdbackup failed with exit code $exit_code" "$log_tail"
      
      # ═══════════════════════════════════════════════════════════════════════════
      # RETRY SELF-HEALING: Archive valid chain, re-validate, and cleanup
      # Before retrying, preserve any valid backup data and get fresh state
      # ═══════════════════════════════════════════════════════════════════════════
      
      # SMART RETRY STRATEGY: If AUTO failed, try FULL instead of retrying AUTO
      if [[ "$backup_type" == "auto" && "$CHECKPOINT_RETRY_AUTO_TO_FULL" == "yes" && $attempt -le $CHECKPOINT_MAX_RETRIES_AUTO ]]; then
        ((attempt++))
        log_warn "vmbackup.sh" "perform_backup" \
          "AUTO backup failed, converting to FULL backup (attempt $attempt of $((MAX_RETRIES + 1)))"
        
        # STEP 1: Archive existing valid backup chain before cleanup destroys it
        # Check if there's restorable backup data (*.full.data or *.copy.data present)
        local has_valid_chain=false
        if find "$backup_dir" -maxdepth 1 \( -name "*.full.data" -o -name "*.copy.data" \) 2>/dev/null | grep -q .; then
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
        sleep $RETRY_DELAY
        continue
      fi
      
      # Normal retry logic - increment attempt and check if we should retry
      ((attempt++))
      if (( attempt <= MAX_RETRIES + 1 )); then
        log_warn "vmbackup.sh" "perform_backup" \
          "Retrying $backup_type backup in $RETRY_DELAY seconds (attempt $attempt of $((MAX_RETRIES + 1)))"
        
        # STEP 1: Archive existing valid backup chain before cleanup destroys it
        local has_valid_chain=false
        if find "$backup_dir" -maxdepth 1 \( -name "*.full.data" -o -name "*.copy.data" \) 2>/dev/null | grep -q .; then
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
        
        sleep $RETRY_DELAY
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
  
  if (( file_count == 0 )); then
    log_error "vmbackup.sh" "verify_backup" "No backup files found in $backup_dir"
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
  
  # Verify no missing segments (check for gaps in numbered files if present)
  local manifest_file=$(find "$backup_dir" -type f -name "*manifest*" 2>/dev/null | head -1)
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
  if declare -f apply_fstrim_optimization >/dev/null 2>&1; then
    FSTRIM_IMPL_AVAILABLE=1
    log_debug "vmbackup.sh" "cache_fstrim_availability" "FSTRIM module implementation available"
  else
    FSTRIM_IMPL_AVAILABLE=0
    log_debug "vmbackup.sh" "cache_fstrim_availability" "FSTRIM module implementation not available"
  fi
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
  local safe_name=$(printf '%s' "$vm_name" | sed 's/[^A-Za-z0-9._-]/_/g')
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
  if [[ -d "$backup_dir" ]]; then
    local newest_backup_file
    newest_backup_file=$(find "$backup_dir" -maxdepth 1 -type f \( -name "*.full.data" -o -name "*.inc.data" -o -name "*.copy.data" \) -printf '%T@ %p\n' 2>/dev/null | sort -rn | head -1 | cut -d' ' -f2-)
    
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
  if [[ -d "$vm_dir" ]]; then
    local newest_backup_file
    newest_backup_file=$(find "$vm_dir" -maxdepth 2 -type f \( -name "*.full.data" -o -name "*.inc.data" -o -name "*.copy.data" \) -printf '%T@ %p\n' 2>/dev/null | sort -rn | head -1 | cut -d' ' -f2-)
    
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
    
    # Compare disk modification time to last backup time
    if (( disk_mtime > last_backup_time )); then
      log_info "vmbackup.sh" "has_offline_vm_changed" "Disk changed detected: $disk_path (mtime: $disk_mtime > backup: $last_backup_time)"
      disk_changed=1
      break
    else
      log_info "vmbackup.sh" "has_offline_vm_changed" "Disk unchanged: $disk_path (mtime: $disk_mtime <= backup: $last_backup_time)"
    fi
  done < <(virsh domblklist "$vm_name" 2>/dev/null | grep -E '^\s+' | awk '{print $2}' | grep -v '^-$')
  
  if (( disk_changed == 1 )); then
    log_info "vmbackup.sh" "has_offline_vm_changed" "Offline VM $vm_name has changed - backup required"
    return 0  # Changed
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
# These are read by backup_vm() for CSV logging after archiving occurs in any code path
_ARCHIVE_CHAIN_ARCHIVED="false"      # Was a chain archived this backup run?
_ARCHIVE_RESTORE_POINTS=0            # How many restore points were in the archived chain?
_ARCHIVE_PATH=""                     # Path to the archived chain

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
    
    # Log to config-events CSV
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

archive_existing_checkpoint_chain() {
  local vm_name=$1
  local backup_dir=$2
  
  if [[ "$DRY_RUN" == true ]]; then
    log_info "vmbackup.sh" "archive_existing_checkpoint_chain" "[DRY-RUN] Would archive existing chain for $vm_name in $backup_dir - skipping"
    return 0
  fi
  
  if [[ -z "$vm_name" ]] || [[ -z "$backup_dir" ]]; then
    log_error "vmbackup.sh" "archive_existing_checkpoint_chain" "Missing parameters: vm_name='$vm_name', backup_dir='$backup_dir'"
    return 1
  fi
  
  # Count restore points BEFORE archiving (for CSV tracking)
  local restore_point_count
  restore_point_count=$(find "$backup_dir" -maxdepth 1 -name "virtnbdbackup.*.xml" -type f 2>/dev/null | wc -l)
  
  # Create archives directory structure
  local archive_base="$backup_dir/.archives"
  local archive_date=$(date +%Y-%m-%d)
  local chain_archive="$archive_base/chain-${archive_date}"
  
  # Handle multiple chains on same day: chain-2026-02-10, chain-2026-02-10.1, chain-2026-02-10.2, etc
  local archive_counter=0
  while [[ -d "$chain_archive" ]]; do
    ((archive_counter++))
    chain_archive="$archive_base/chain-${archive_date}.${archive_counter}"
  done
  
  mkdir -p "$chain_archive" || {
    log_error "vmbackup.sh" "archive_existing_checkpoint_chain" "Failed to create archive directory: $chain_archive"
    return 1
  }
  
  log_info "vmbackup.sh" "archive_existing_checkpoint_chain" \
    "Archiving checkpoint chain for VM $vm_name to: $chain_archive"
  
  # Archive all checkpoint metadata files (virtnbdbackup.*.xml)
  local metadata_count=0
  while IFS= read -r metadata_file; do
    if mv "$metadata_file" "$chain_archive/"; then
      ((metadata_count++))
      log_debug "vmbackup.sh" "archive_existing_checkpoint_chain" \
        "Archived metadata: $(basename "$metadata_file")"
    else
      log_warn "vmbackup.sh" "archive_existing_checkpoint_chain" \
        "Failed to move metadata file: $metadata_file"
    fi
  done < <(find "$backup_dir" -maxdepth 1 -name "virtnbdbackup.*.xml" -type f 2>/dev/null)
  
  # Archive full backup data file (*.full.data)
  local full_count=0
  while IFS= read -r full_file; do
    if mv "$full_file" "$chain_archive/"; then
      ((full_count++))
      log_debug "vmbackup.sh" "archive_existing_checkpoint_chain" \
        "Archived full backup: $(basename "$full_file")"
    else
      log_warn "vmbackup.sh" "archive_existing_checkpoint_chain" \
        "Failed to move full backup file: $full_file"
    fi
  done < <(find "$backup_dir" -maxdepth 1 \( -name "*.full.data" -o -name "*.copy.data" \) -type f 2>/dev/null)
  
  # Archive incremental backup data files (*.inc.virtnbdbackup.*.data)
  local inc_count=0
  while IFS= read -r inc_file; do
    if mv "$inc_file" "$chain_archive/"; then
      ((inc_count++))
      log_debug "vmbackup.sh" "archive_existing_checkpoint_chain" \
        "Archived incremental: $(basename "$inc_file")"
    else
      log_warn "vmbackup.sh" "archive_existing_checkpoint_chain" \
        "Failed to move incremental file: $inc_file"
    fi
  done < <(find "$backup_dir" -maxdepth 1 -name "*.inc.virtnbdbackup.*.data" -type f 2>/dev/null)
  
  # Archive checkpoint marker files (*.cpt)
  local cpt_count=0
  while IFS= read -r cpt_file; do
    if mv "$cpt_file" "$chain_archive/"; then
      ((cpt_count++))
      log_debug "vmbackup.sh" "archive_existing_checkpoint_chain" \
        "Archived checkpoint marker: $(basename "$cpt_file")"
    else
      log_warn "vmbackup.sh" "archive_existing_checkpoint_chain" \
        "Failed to move checkpoint marker: $cpt_file"
    fi
  done < <(find "$backup_dir" -maxdepth 1 -name "*.cpt" -type f 2>/dev/null)
  
  # Archive checksum files (*.data.chksum) — virtnbdbackup's targetIsEmpty() uses *.data* glob,
  # so leftover .chksum files cause "already contains full or copy backup" errors on retry
  local chksum_count=0
  while IFS= read -r chksum_file; do
    if mv "$chksum_file" "$chain_archive/"; then
      ((chksum_count++))
      log_debug "vmbackup.sh" "archive_existing_checkpoint_chain" \
        "Archived checksum: $(basename "$chksum_file")"
    else
      log_warn "vmbackup.sh" "archive_existing_checkpoint_chain" \
        "Failed to move checksum file: $chksum_file"
    fi
  done < <(find "$backup_dir" -maxdepth 1 -name "*.data.chksum" -type f 2>/dev/null)
  
  # Archive any checkpoint subdirectory if present
  if [[ -d "$backup_dir/checkpoints" ]]; then
    if mv "$backup_dir/checkpoints" "$chain_archive/"; then
      log_debug "vmbackup.sh" "archive_existing_checkpoint_chain" \
        "Archived checkpoints subdirectory"
    else
      log_warn "vmbackup.sh" "archive_existing_checkpoint_chain" \
        "Failed to move checkpoints subdirectory"
    fi
  fi
  
  # Archive qcow metadata files (*.qcow.json) — per-checkpoint disk geometry/format info
  local qcow_json_count=0
  while IFS= read -r qcow_file; do
    if mv "$qcow_file" "$chain_archive/"; then
      ((qcow_json_count++))
      log_debug "vmbackup.sh" "archive_existing_checkpoint_chain" \
        "Archived qcow metadata: $(basename "$qcow_file")"
    else
      log_warn "vmbackup.sh" "archive_existing_checkpoint_chain" \
        "Failed to move qcow metadata: $qcow_file"
    fi
  done < <(find "$backup_dir" -maxdepth 1 -name "*.qcow.json" -type f 2>/dev/null)
  
  # Archive NVRAM/OVMF firmware files (UEFI VM state — filenames may contain spaces)
  local nvram_count=0
  while IFS= read -r nvram_file; do
    if mv "$nvram_file" "$chain_archive/"; then
      ((nvram_count++))
      log_debug "vmbackup.sh" "archive_existing_checkpoint_chain" \
        "Archived NVRAM/OVMF: $(basename "$nvram_file")"
    else
      log_warn "vmbackup.sh" "archive_existing_checkpoint_chain" \
        "Failed to move NVRAM/OVMF file: $nvram_file"
    fi
  done < <(find "$backup_dir" -maxdepth 1 \( -name "*_VARS.fd" -o -name "*_VARS.fd.virtnbdbackup.*" -o -name "OVMF_CODE*.fd" -o -name "OVMF_CODE*.fd.virtnbdbackup.*" \) -type f 2>/dev/null)
  
  # Archive TPM state directory if present
  local tpm_archived=0
  if [[ -d "$backup_dir/tpm-state" ]]; then
    if mv "$backup_dir/tpm-state" "$chain_archive/"; then
      tpm_archived=1
      log_debug "vmbackup.sh" "archive_existing_checkpoint_chain" \
        "Archived tpm-state subdirectory"
    else
      log_warn "vmbackup.sh" "archive_existing_checkpoint_chain" \
        "Failed to move tpm-state subdirectory"
    fi
  fi
  
  # Archive config subdirectory if present (per-chain VM XML snapshots)
  local config_archived=0
  if [[ -d "$backup_dir/config" ]]; then
    if mv "$backup_dir/config" "$chain_archive/"; then
      config_archived=1
      log_debug "vmbackup.sh" "archive_existing_checkpoint_chain" \
        "Archived config subdirectory"
    else
      log_warn "vmbackup.sh" "archive_existing_checkpoint_chain" \
        "Failed to move config subdirectory"
    fi
  fi
  
  local total_archived=$((metadata_count + full_count + inc_count + cpt_count + chksum_count + qcow_json_count + nvram_count + tpm_archived + config_archived))
  log_info "vmbackup.sh" "archive_existing_checkpoint_chain" \
    "Archived chain for VM $vm_name: $total_archived files to $chain_archive"
  
  # Log chain archive as a file operation (summary-level, not per-file)
  if declare -f log_file_operation >/dev/null 2>&1; then
    local archive_total_bytes
    archive_total_bytes=$(du -sb "$chain_archive" 2>/dev/null | cut -f1 || echo 0)
    log_file_operation "move" "$vm_name" "$backup_dir" "$chain_archive" \
      "directory" "Chain archived: ${total_archived} files (${metadata_count} metadata, ${full_count} full, ${inc_count} inc, ${cpt_count} cpt, ${chksum_count} chksum, ${qcow_json_count} qcow.json, ${nvram_count} nvram)" \
      "archive_existing_checkpoint_chain" "true"
  fi
  
  # Set global tracking variables for CSV logging
  # These are read by backup_vm() regardless of which code path triggered the archival
  _ARCHIVE_CHAIN_ARCHIVED="true"
  _ARCHIVE_RESTORE_POINTS=$restore_point_count
  _ARCHIVE_PATH="$chain_archive"
  
  # If policy change was detected, record the archive path
  if [[ "$_POLICY_CHANGE_DETECTED" == "true" ]]; then
    _POLICY_CHANGE_ARCHIVE_PATH="$chain_archive"
  fi
  
  log_debug "vmbackup.sh" "archive_existing_checkpoint_chain" \
    "Set archive tracking: chain_archived=true, restore_points=$restore_point_count, path=$chain_archive"
  
  # G6/G7: Log archive to SQLite chain_health + chain_events audit trail
  if declare -f sqlite_archive_chain >/dev/null 2>&1; then
    # Extract period_id and chain_id from archive path
    local archive_period_id=$(basename "$(dirname "$chain_archive")")
    local archive_chain_id=$(basename "$chain_archive")
    local archive_size=$(du -sb "$chain_archive" 2>/dev/null | cut -f1 || echo 0)
    
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
  if [[ "$DRY_RUN" == true ]]; then
    # In dry-run, only evaluate exclusion policy — don't archive chains
    local _dr_policy
    _dr_policy=$(get_vm_rotation_policy "$vm_name" 2>/dev/null || echo "")
    if [[ "$_dr_policy" == "never" ]]; then
      log_info "vmbackup.sh" "backup_vm" "[DRY-RUN] VM $vm_name would be EXCLUDED (policy=never)"
      VM_BACKUP_RESULTS+=("$vm_name|EXCLUDED|n/a|00:00:00|0|N/A||$_dr_policy")
      return $BACKUP_RC_EXCLUDED
    fi
    log_info "vmbackup.sh" "backup_vm" "[DRY-RUN] pre_backup_hook: policy=$_dr_policy (chain archiving skipped)"
  elif ! pre_backup_hook "$vm_name"; then
    # VM excluded by policy - log to session summary
    _log_vm_backup_summary "$vm_name" "$backup_start_time" "$backup_start_epoch" \
      "EXCLUDED" "n/a" "0" "0" "" "0" "$vm_policy" "" \
      "vm_excluded" "excluded by rotation policy (policy=$vm_policy)" "0" "0"
    return $BACKUP_RC_EXCLUDED  # Return 2 = excluded (don't count as success)
  fi
  
  #############################################################################
  # State Tracking: Reset archive tracking and initialize backup run variables
  #############################################################################
  _ARCHIVE_CHAIN_ARCHIVED="false"
  _ARCHIVE_RESTORE_POINTS=0
  VM_STATE="unknown"
  QEMU_AGENT_AVAILABLE=0
  VM_WAS_PAUSED=0
  local csv_backup_method="unknown"          # Backup method: agent/paused/offline
  local csv_restore_points_before=0          # Restore points before backup
  local csv_restore_points_after=0           # Restore points after backup
  
  log_info "vmbackup.sh" "backup_vm" ""
  log_info "vmbackup.sh" "backup_vm" "╔══════════════════════════════════════════════════════════════════════════════╗"
  log_info "vmbackup.sh" "backup_vm" "║  BACKUP START: $vm_name"
  log_info "vmbackup.sh" "backup_vm" "║  Time: $backup_start_time"
  log_info "vmbackup.sh" "backup_vm" "╚══════════════════════════════════════════════════════════════════════════════╝"
  
  # Loop to allow retry after emergency recovery
  while true; do
    # Check lock
    if has_lock "$vm_name"; then
      # Lock exists - check if it's from recent interruption
      if [[ "$recovery_attempted" == false ]] && detect_interrupted_backup "$vm_name"; then
        log_warn "vmbackup.sh" "backup_vm" "Detected interrupted backup for $vm_name - performing emergency recovery"
        emergency_cleanup_current_vm "$vm_name"
        recovery_attempted=true
        retry_count=$((retry_count + 1))
        
        # Loop back to retry after emergency cleanup
        log_info "vmbackup.sh" "backup_vm" "Retrying backup for $vm_name after emergency recovery"
        continue
      else
        log_warn "vmbackup.sh" "backup_vm" "Backup already in progress for VM: $vm_name (skipping)"
        _log_vm_backup_summary "$vm_name" "$backup_start_time" "$backup_start_epoch" \
          "SKIPPED" "n/a" "0" "0" "already in progress" "0" "$vm_policy" "" \
          "backup_skipped" "already in progress (lock exists)" "0" "0"
        return 1
      fi
    fi
    
    # Create lock (skip in dry-run - no filesystem writes)
    if [[ "$DRY_RUN" == true ]]; then
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
  if [[ "$vm_status" == "running" ]] && check_qemu_agent "$vm_name"; then
    has_qemu_agent=true
    QEMU_AGENT_AVAILABLE=1
  else
    QEMU_AGENT_AVAILABLE=0
  fi
  
  # Determine initial backup method based on VM state
  if [[ "$vm_status" == "shut off" ]]; then
    csv_backup_method="offline"
  elif [[ "$has_qemu_agent" == "true" ]]; then
    csv_backup_method="agent"
  else
    csv_backup_method="paused"  # Will be confirmed when actually paused
  fi
  log_debug "vmbackup.sh" "backup_vm" "Method decision: vm_status=$vm_status has_agent=$has_qemu_agent → method=$csv_backup_method"
  
  # Store current agent status persistently for future reference
  # When VM is running, record whether agent is present for when it goes offline
  # This allows us to make intelligent decisions about directory preservation
  local backup_dir
  backup_dir=$(get_backup_dir "$vm_name")
  local agent_status_file="$backup_dir/.agent-status"
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
    
    # Check if offline VM disks have changed
    if ! has_offline_vm_changed "$vm_name"; then
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
      
      return 0  # Success - skip backup
    else
      # DISK CHANGES DETECTED on offline VM
      log_warn "vmbackup.sh" "backup_vm" "CHANGE DETECTED: Offline VM $vm_name disks modified since last backup"
      log_info "vmbackup.sh" "backup_vm" "ACTION PLAN: 1) Archive existing chain (if any), 2) Clear virsh checkpoints, 3) Create fresh full (copy) backup"
      
      # Check if there's an existing checkpoint chain to archive
      if find "$backup_dir" -maxdepth 1 \( -name "virtnbdbackup.*.xml" -o -name "*.full.data" -o -name "*.inc.virtnbdbackup.*.data" \) 2>/dev/null | grep -q .; then
        log_info "vmbackup.sh" "backup_vm" "Existing checkpoint chain found for $vm_name - archiving before fresh full backup"
        
        # Archive the existing chain (full + any incrementals)
        # Note: archive_existing_checkpoint_chain sets _ARCHIVE_CHAIN_ARCHIVED and _ARCHIVE_RESTORE_POINTS
        if archive_existing_checkpoint_chain "$vm_name" "$backup_dir"; then
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
    if [[ "$vm_status" == "shut off" ]]; then
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
    elif find "$backup_dir" -maxdepth 1 \( -name "*.full.data" -o -name "*.inc.virtnbdbackup.*.data" \) 2>/dev/null | grep -q .; then
      has_existing_chain=true
    fi
    
    if [[ "$has_existing_chain" == "true" ]]; then
      log_info "vmbackup.sh" "backup_vm" "Archiving existing chain before starting new policy baseline"
      
      # Archive the existing chain
      if archive_existing_checkpoint_chain "$vm_name" "$backup_dir"; then
        log_info "vmbackup.sh" "backup_vm" "Chain archived successfully to: $_ARCHIVE_PATH"
        
        # Delete QEMU checkpoints to allow fresh start
        local checkpoints=($(virsh checkpoint-list "$vm_name" --name 2>/dev/null | grep "^virtnbdbackup\." || true))
        if [[ ${#checkpoints[@]} -gt 0 ]]; then
          log_info "vmbackup.sh" "backup_vm" "Clearing ${#checkpoints[@]} QEMU checkpoints for new chain"
          for cp in "${checkpoints[@]}"; do
            virsh checkpoint-delete "$vm_name" "$cp" --metadata 2>/dev/null || true
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
  if [[ "$DRY_RUN" == true ]]; then
    [[ ! -d "$backup_dir" ]] && log_info "vmbackup.sh" "backup_vm" "[DRY-RUN] Would create backup directory: $backup_dir"
  else
    mkdir -p "$backup_dir"
  fi
  
  # Execute fstrim if enabled and agent available (use cached result)
  if [[ "$ENABLE_FSTRIM" == "true" ]] && [[ "$has_qemu_agent" == "true" ]]; then
    if [[ "$DRY_RUN" == true ]]; then
      log_info "vmbackup.sh" "backup_vm" "[DRY-RUN] Would execute FSTRIM on VM: $vm_name"
    else
      [[ ${FSTRIM_IMPL_AVAILABLE:-0} -eq 1 ]] && execute_fstrim_in_guest "$vm_name"
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
  # CSV LOGGING NOTE:
  #   The backup_type logged to CSV reflects what we REQUEST here, not what
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
    echo "$current_month" > "$full_backup_marker"
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
      log_debug "vmbackup.sh" "backup_vm" "Deleting recovery flag: ${TEMP_DIR}/vmbackup-recovery-${vm_name}.flag"
      rm -f "${TEMP_DIR}/vmbackup-recovery-${vm_name}.flag"
    fi
    
    # Update full backup month marker
    echo "$current_month" > "$full_backup_marker"
    log_debug "vmbackup.sh" "backup_vm" "Updated full-backup-month marker: $full_backup_marker → $current_month"
  elif [[ "$vm_status" == "shut off" ]] && [[ "${_ARCHIVE_CHAIN_ARCHIVED:-false}" == "true" ]]; then
    # OFFLINE VM ARCHIVAL TRIGGER: If checkpoint chain was just archived, force FULL backup
    # This creates a fresh baseline after archiving the previous chain
    backup_type="full"
    log_info "vmbackup.sh" "backup_vm" "FULL backup forced: Offline VM $vm_name had checkpoint chain archived, creating fresh baseline"
    echo "$current_month" > "$full_backup_marker"
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
      orphan_checkpoint_count=$(virsh checkpoint-list "$vm_name" --name 2>/dev/null | grep -c . || true)
      log_debug "vmbackup.sh" "backup_vm" "Orphan check: virsh checkpoint count for $vm_name = $orphan_checkpoint_count"
      
      if [[ "$orphan_checkpoint_count" -eq 0 ]]; then
        log_warn "vmbackup.sh" "backup_vm" "ORPHAN DETECTED: Backup data exists but no virsh checkpoints - this is orphaned offline backup data"
        log_info "vmbackup.sh" "backup_vm" "Archiving orphaned offline backup before FULL overwrites it (preserving previous offline backup)"
        if archive_existing_checkpoint_chain "$vm_name" "$backup_dir"; then
          log_info "vmbackup.sh" "backup_vm" "Successfully archived orphaned offline backup for VM: $vm_name"
        else
          log_error "vmbackup.sh" "backup_vm" "Failed to archive orphaned offline backup for VM: $vm_name - continuing with FULL backup (data may be lost)"
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
  # CSV restore_points tracks actual data files on disk, not virsh checkpoints
  csv_restore_points_before=$(get_restore_point_count "$backup_dir")
  final_backup_type="$backup_type"
  
  # Handle VM pause/resume if needed (use cached agent check result)
  local paused=false
  if [[ "$vm_status" == "running" ]]; then
    if ! [[ "$has_qemu_agent" == "true" ]]; then
      log_warn "vmbackup.sh" "backup_vm" "QEMU guest agent not available for VM: $vm_name - pausing for backup"
      
      if pause_vm "$vm_name"; then
        paused=true
        csv_backup_method="paused"  # Confirm backup method
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
    local fail_event_detail=$(build_event_detail "error" "$backup_type" "0" "$csv_restore_points_before" "$csv_backup_method" "$_ARCHIVE_CHAIN_ARCHIVED" "$error_detail")
    
    if [[ "$paused" == "true" ]]; then
      log_warn "vmbackup.sh" "backup_vm" "Resuming paused VM: $vm_name after failed backup"
      if ! resume_vm "$vm_name"; then
        log_error "vmbackup.sh" "backup_vm" "CRITICAL: Failed to resume VM: $vm_name after failed backup - VM may still be paused!"
        log_error "vmbackup.sh" "backup_vm" "MANUAL ACTION REQUIRED: Run 'virsh resume $vm_name' to restore VM operation"
      fi
    fi
    
    # G1 Fix: Call post_backup_hook on failure path for chain state tracking
    # DRY-RUN: Skip — post_backup_hook writes chain-manifest.json and updates SQLite
    if [[ "$DRY_RUN" != true ]]; then
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
      local post_resume_state=$(virsh domstate "$vm_name" 2>/dev/null)
      log_info "vmbackup.sh" "backup_vm" "VM resumed successfully - current state: $post_resume_state"
    fi
  fi
  
  # Post-backup operations
  log_info "vmbackup.sh" "backup_vm" "Performing post-backup operations for VM: $vm_name"
  
  # Verify backup (skip during dry-run — no backup files to verify)
  if [[ "$DRY_RUN" == true ]]; then
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
  if [[ "$DRY_RUN" == true ]]; then
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
  if [[ "$DRY_RUN" == true ]]; then
    log_info "vmbackup.sh" "backup_vm" "[DRY-RUN] Would backup VM configuration (XML definition) to $backup_dir/config/"
  else
  log_info "vmbackup.sh" "backup_vm" "Backing up VM configuration (XML definition)"
  if ! backup_vm_config "$vm_name" "$backup_dir"; then
    log_warn "vmbackup.sh" "backup_vm" "Failed to backup VM configuration, but disk backup completed successfully"
    # Don't fail the entire backup if config backup fails - disk backup is primary
  fi
  fi
  
  local final_depth=$(get_checkpoint_depth "$vm_name")
  # Restore_points tracks actual data files on disk, not virsh checkpoints
  csv_restore_points_after=$(get_restore_point_count "$backup_dir")
  log_info "vmbackup.sh" "backup_vm" "Final QEMU checkpoint depth: $final_depth, Restore points on disk: $csv_restore_points_after"
  
  # Calculate size metrics for session summary
  local csv_this_backup_bytes=$(get_this_backup_size "$backup_dir" "$backup_start_epoch")
  local csv_total_dir_bytes=$(get_total_dir_size "$backup_dir")
  
  # Build dynamic event detail
  local csv_event_detail=$(build_event_detail "success" "$backup_type" "$csv_this_backup_bytes" "$csv_restore_points_after" "$csv_backup_method" "$_ARCHIVE_CHAIN_ARCHIVED")
  
  # Success - set final status and log summary
  final_status="SUCCESS"
  final_size="$csv_total_dir_bytes"
  
  #############################################################################
  # VM-First Integration: Post-backup hook
  # Handles: manifest updates, retention cleanup, chain lifecycle logging
  # DRY-RUN: Skip — post_backup_hook writes chain-manifest.json and updates SQLite
  #############################################################################
  if [[ "$DRY_RUN" == true ]]; then
    log_info "vmbackup.sh" "backup_vm" "[DRY-RUN] Skipping post_backup_hook (no manifest/chain updates)"
  else
  local backup_duration=$(($(date +%s) - backup_start_epoch))
  post_backup_hook "$vm_name" "success" "$final_backup_type" "$final_size" "$backup_duration"
  fi
  
  # Use csv_restore_points_after (actual data files) for restore_points, not checkpoint_after (virsh count)
  # NOTE: Pass csv_this_backup_bytes (actual bytes written this run), NOT final_size (total dir size).
  # total_dir_bytes is separately computed inside sqlite_log_vm_backup via get_total_dir_size().
  _log_vm_backup_summary "$vm_name" "$backup_start_time" "$backup_start_epoch" "$final_status" "$final_backup_type" "$csv_restore_points_before" "$csv_restore_points_after" "" "$csv_this_backup_bytes" "$vm_policy" "$backup_dir" \
    "backup_completed" "$csv_event_detail" "${retry_count:-0}" "${_ARCHIVE_RESTORE_POINTS:-0}"
  return 0
}

#################################################################################
# MAIN EXECUTION
#################################################################################

# G3: Log interrupted chain to SQLite when signal received
# Detects current VM from lock file and marks chain as broken
_log_interrupted_chain() {
  local signal_name="${1:-UNKNOWN}"
  
  # Find current VM from lock file
  local current_vm=$(ls -t "$LOCK_DIR"/vmbackup-*.lock 2>/dev/null | head -1 | sed 's|.*vmbackup-\(.*\)\.lock|\1|')
  
  if [[ -z "$current_vm" ]]; then
    log_debug "vmbackup.sh" "_log_interrupted_chain" "No active backup detected"
    return 0
  fi
  
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
  local chain_id
  chain_id=$(get_active_chain "$current_vm" 2>/dev/null || echo "")
  
  if [[ -n "$chain_id" ]]; then
    # Get current checkpoint count
    local checkpoint
    checkpoint=$(count_period_restore_points "$current_vm" "$period_id" 2>/dev/null || echo "0")
    
    # Log to SQLite as broken chain
    if declare -f sqlite_mark_chain_broken >/dev/null 2>&1; then
      sqlite_mark_chain_broken "$current_vm" "$period_id" "$chain_id" \
        "$checkpoint" "interrupted by $signal_name"
      log_warn "vmbackup.sh" "_log_interrupted_chain" \
        "Marked chain $chain_id as broken at checkpoint $checkpoint"
    fi
  fi
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
  
  # Finalize SQLite session if interrupted (exit codes 130=SIGINT, 143=SIGTERM)
  if [[ $exit_code -eq 130 ]] || [[ $exit_code -eq 143 ]]; then
    if sqlite_is_available 2>/dev/null && [[ -n "$SQLITE_CURRENT_SESSION_ID" ]] && [[ "$DRY_RUN" != true ]]; then
      # Count current results from VM_BACKUP_RESULTS array
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
      sqlite_session_end "$int_total" "$int_success" "$int_failed" "$int_skipped" "$int_excluded" "0" "$int_status"
      log_info "vmbackup.sh" "cleanup_on_exit" "SQLite session finalized as '$int_status'"
    fi
  fi
  
  log_info "vmbackup.sh" "cleanup_on_exit" "Cleaning up temporary files before exit (exit code: $exit_code)"
  
  # Remove temporary lock files if script exits unexpectedly
  if [[ -d "$LOCK_DIR" ]]; then
    # Clean only lock files older than 1 hour (stale locks)
    local stale_locks=$(find "$LOCK_DIR" -name "*.lock" -type f -mmin +60 2>/dev/null)
    if [[ -n "$stale_locks" ]]; then
      while IFS= read -r lock_file; do
        if rm -f "$lock_file"; then
          log_debug "vmbackup.sh" "cleanup_on_exit" "Deleted stale lock file: $(basename "$lock_file")"
        fi
      done <<< "$stale_locks"
    else
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
      while IFS= read -r flag_file; do
        if rm -f "$flag_file"; then
          log_debug "vmbackup.sh" "cleanup_on_exit" "Deleted recovery flag: $(basename "$flag_file")"
        fi
      done <<< "$recovery_flags"
    fi
  fi
  
  # Clean CSV lock files (created by flock for atomic CSV writes)
  local csv_dir="${STATE_DIR}/csv"
  if [[ -d "$csv_dir" ]]; then
    local csv_locks=$(find "$csv_dir" -name "*.lock" -type f 2>/dev/null)
    if [[ -n "$csv_locks" ]]; then
      while IFS= read -r lock_file; do
        if rm -f "$lock_file"; then
          log_debug "vmbackup.sh" "cleanup_on_exit" "Deleted CSV lock file: $(basename "$lock_file")"
        fi
      done <<< "$csv_locks"
    fi
  fi
  
  log_info "vmbackup.sh" "cleanup_on_exit" "Temporary file cleanup complete"
  
  return $exit_code
}

# Emergency handler for CTRL+Z (SIGTSTP) - suspend signal
# When user presses CTRL+Z, cleanup current VM before suspending
handle_sigtstp() {
  # Find which VM is currently being backed up by checking recent lock files
  local current_vm=$(ls -t "$LOCK_DIR"/vmbackup-*.lock 2>/dev/null | head -1 | sed 's|.*vmbackup-\(.*\)\.lock|\1|')
  
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
  local session_end_time=$(date '+%Y-%m-%d %H:%M:%S')
  if [[ "$DRY_RUN" == true ]]; then
    log_info "vmbackup.sh" "handle_sigterm" "[DRY-RUN] Skipping email report"
  elif [[ -f "${SCRIPT_DIR}/modules/email_report_module.sh" ]]; then
    log_info "vmbackup.sh" "handle_sigterm" "Loading email report module..."
    source "${SCRIPT_DIR}/modules/email_report_module.sh"
    if load_email_config; then
      log_info "vmbackup.sh" "handle_sigterm" "Sending email report before SIGTERM exit..."
      if send_backup_report "${session_start_time:-unknown}" "$session_end_time" "failed"; then
        log_info "vmbackup.sh" "handle_sigterm" "Email report sent successfully"
      else
        log_warn "vmbackup.sh" "handle_sigterm" "Failed to send email report"
      fi
    else
      log_debug "vmbackup.sh" "handle_sigterm" "Email disabled or not configured for this instance"
    fi
  fi
  
  exit 143
}

# Set up signal handlers
trap cleanup_on_exit EXIT
trap '_log_interrupted_chain "SIGINT"; log_error "vmbackup.sh" "main" "Script interrupted by SIGINT (Ctrl+C)"; log_error "vmbackup.sh" "main" "Recovery: Run vmbackup.sh again - incomplete backups will be cleaned up automatically"; exit 130' SIGINT
trap 'handle_sigterm' SIGTERM
trap handle_sigtstp SIGTSTP

main() {
  ensure_backup_path_sgid
  init_logging
  
  log_info "vmbackup.sh" "main" "===== VM BACKUP SESSION START ====="
  log_info "vmbackup.sh" "main" "100% vibe coded. Could be 100% wrong."
  log_info "vmbackup.sh" "main" "Config instance: ${CONFIG_INSTANCE:-default}"
  
  if [[ "$DRY_RUN" == true ]]; then
    log_info "vmbackup.sh" "main" "╔══════════════════════════════════════════════════════════════╗"
    log_info "vmbackup.sh" "main" "║  DRY-RUN MODE: No backups, retention, replication, or FSTRIM ║"
    log_info "vmbackup.sh" "main" "║  will be executed. Read-only — showing what would happen.    ║"
    log_info "vmbackup.sh" "main" "╚══════════════════════════════════════════════════════════════╝"
  fi
  
  # Validate operational settings from config (with explicit defaults if missing)
  validate_operational_settings
  
  log_info "vmbackup.sh" "main" "Configuration: COMPRESS_LEVEL=$VIRTNBD_COMPRESS_LEVEL, HEALTH_CHECK=$CHECKPOINT_HEALTH_CHECK"
  
  # Track session start time for email report
  local session_start_time=$(date '+%Y-%m-%d %H:%M:%S')
  
  # Dependency check (MUST be first, before any tool usage)
  log_info "vmbackup.sh" "main" "Checking dependencies"
  if ! check_dependencies; then
    log_error "vmbackup.sh" "main" "Dependency check failed - aborting session"
    exit 1
  fi
  
  # CRITICAL: Clean up any stale qemu-nbd processes from previous interrupted runs
  # These hold write locks on qcow2 files and prevent VMs from starting
  if [[ "$DRY_RUN" == true ]]; then
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
  fi  # end DRY_RUN else block
  
  # OPT #4: Lazy load TPM backup module only when first VM is processed
  # This defers module initialization until we know if any VMs have TPM
  local tpm_module_loaded=false
  
  # Load SQLite logging module (provides structured database logging)
  if load_sqlite_logging_module; then
    log_info "vmbackup.sh" "main" "SQLite logging module loaded successfully"
    # Start SQLite session tracking (skip in dry-run to avoid polluting DB)
    if [[ "$DRY_RUN" == true ]]; then
      log_info "vmbackup.sh" "main" "[DRY-RUN] SQLite session tracking disabled - no DB writes"
    elif sqlite_session_start "${CONFIG_INSTANCE:-default}" "$LOG_FILE"; then
      log_debug "vmbackup.sh" "main" "SQLite session started: $(sqlite_get_session_id)"
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
      log_error "vmbackup.sh" "main" "FATAL: Failed to load vmbackup_integration.sh (syntax error?)"
      exit 1
    fi
  else
    log_error "vmbackup.sh" "main" "FATAL: vmbackup_integration.sh not found at: $integration_module"
    exit 1
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
    log_error "vmbackup.sh" "main" "Backup destination check failed - aborting session"
    exit 1
  fi
  
  if ! check_scratch_path; then
    log_error "vmbackup.sh" "main" "Scratch path check failed - aborting session"
    exit 1
  fi
  
  if ! check_disk_space; then
    log_error "vmbackup.sh" "main" "Insufficient disk space - aborting session"
    exit 1
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
  done < <(virsh list --all --name)
  
  if [[ ${#vm_list[@]} -eq 0 ]]; then
    log_warn "vmbackup.sh" "main" "No VMs found to backup"
  else
    log_info "vmbackup.sh" "main" "Found ${#vm_list[@]} VMs to process: ${vm_list[*]}"
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
  
  for vm_name in "${vm_list[@]}"; do
    [[ -z "$vm_name" ]] && continue
    
    # OPT #4b: Lazy load TPM module on first VM that has TPM
    if [[ "$tpm_module_loaded" == "false" ]]; then
      # Check if this VM has TPM before loading module
      if virsh dumpxml "$vm_name" 2>/dev/null | grep -q "<tpm"; then
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
  
  # Backup host-level configuration (libvirt/QEMU)
  if [[ "$DRY_RUN" == true ]]; then
    log_info "vmbackup.sh" "main" "[DRY-RUN] Would backup host-level libvirt/QEMU configuration - skipping"
  else
    log_info "vmbackup.sh" "main" "Backing up host-level libvirt/QEMU configuration"
    if ! backup_host_config; then
      log_warn "vmbackup.sh" "main" "Failed to backup host configuration, but VM backups completed"
      # Don't fail the entire session if host config backup fails - VMs are primary
    fi
  fi
  
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
  
  # DRY-RUN: Skip entire replication phase (AFTER determination so we can report what would run)
  if [[ "$DRY_RUN" == true ]]; then
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
      bytes_val=$(numfmt --from=iec "${size%B}" 2>/dev/null || echo 0)
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
  
  if sqlite_is_available 2>/dev/null && [[ "$DRY_RUN" != true ]]; then
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
  local session_end_time=$(date '+%Y-%m-%d %H:%M:%S')
  local overall_status="success"
  if (( fail_count > 0 && backed_up_count == 0 )); then
    overall_status="failed"
  elif (( fail_count > 0 )); then
    overall_status="partial"
  fi
  
  if [[ "$DRY_RUN" == true ]]; then
    log_info "vmbackup.sh" "main" "[DRY-RUN] Skipping email report"
  elif [[ -f "${SCRIPT_DIR}/modules/email_report_module.sh" ]]; then
    log_info "vmbackup.sh" "main" "Loading email report module"
    source "${SCRIPT_DIR}/modules/email_report_module.sh"
    
    if load_email_config; then
      log_info "vmbackup.sh" "main" "Sending email report to $EMAIL_RECIPIENT"
      if send_backup_report "$session_start_time" "$session_end_time" "$overall_status"; then
        log_info "vmbackup.sh" "main" "Email report sent successfully"
      else
        log_warn "vmbackup.sh" "main" "Failed to send email report (backup data preserved)"
      fi
    else
      log_debug "vmbackup.sh" "main" "Email disabled or not configured for this instance"
    fi
  else
    log_debug "vmbackup.sh" "main" "Email report module not found - skipping email notification"
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
