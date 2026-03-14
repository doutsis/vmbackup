#!/bin/bash
#
# TPM Backup Module for vmbackup.sh
# 
# This module provides functions to backup TPM (Trusted Platform Module) state
# alongside disk backups from virtnbdbackup. TPM state contains encryption keys,
# secure boot configuration, and other critical VM security state.
#
# Usage: Source this file in vmbackup.sh before calling backup_vm()
#   source tpm_backup_module.sh
#   backup_vm_tpm <vm_name> <backup_directory>
#
# Author: VM Backup System
# Version: 1.0
#

# NOTE: set -o pipefail removed - this module is sourced into the parent shell
# and setting pipefail here would affect all subsequent parent pipelines.
# Individual pipelines that need pipefail should use subshells: (set -o pipefail; cmd | cmd)

# Configuration - can be overridden by caller
: "${TPM_BACKUP_ENABLED:=yes}"
: "${TPM_BACKUP_METHOD:=full}"      # Options: full, incremental, consistent
: "${SWTPM_STATE_DIR:=/var/lib/libvirt/swtpm}"
: "${TPM_BACKUP_TIMEOUT:=300}"      # Timeout for TPM operations (seconds)
: "${BITLOCKER_KEY_EXTRACTION:=yes}" # Extract BitLocker recovery keys from Windows guests
: "${BITLOCKER_EXEC_TIMEOUT:=30}"    # Timeout for guest-exec commands (seconds)

##############################################################################
# Utility Functions
##############################################################################

# Get VM UUID from libvirt
get_vm_uuid() {
    local vm_name="$1"
    
    virsh dominfo "$vm_name" 2>/dev/null | grep "^UUID" | awk '{print $2}'
}

# Check if VM has TPM device
has_tpm_device() {
    local vm_name="$1"
    
    virsh dumpxml "$vm_name" 2>/dev/null | grep -q '<tpm' && return 0
    return 1
}

# Get TPM model and backend type
get_tpm_info() {
    local vm_name="$1"
    
    virsh dumpxml "$vm_name" 2>/dev/null | grep -A 5 '<tpm'
}

# Log message (compatible with vmbackup.sh logging)
# Bridges to main log_* functions when available, falls back to stderr
log_tpm() {
    local level="$1"
    local message="$2"
    local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    
    # Bridge to main logging system if available
    case "$level" in
        DEBUG) declare -f log_debug >/dev/null 2>&1 && log_debug "tpm_backup_module.sh" "${FUNCNAME[1]:-tpm}" "$message" && return ;;
        INFO)  declare -f log_info  >/dev/null 2>&1 && log_info  "tpm_backup_module.sh" "${FUNCNAME[1]:-tpm}" "$message" && return ;;
        WARN)  declare -f log_warn  >/dev/null 2>&1 && log_warn  "tpm_backup_module.sh" "${FUNCNAME[1]:-tpm}" "$message" && return ;;
        ERROR) declare -f log_error >/dev/null 2>&1 && log_error "tpm_backup_module.sh" "${FUNCNAME[1]:-tpm}" "$message" && return ;;
    esac
    
    # Fallback: stderr-only (standalone mode)
    echo "[$timestamp] [$level] TPM: $message" >&2
}

##############################################################################
# Full TPM Backup Method
##############################################################################

backup_vm_tpm_full() {
    local vm_name="$1"
    local backup_dir="$2"
    
    [[ -z "$vm_name" || -z "$backup_dir" ]] && return 1
    
    # Get VM UUID
    local vm_uuid
    vm_uuid=$(get_vm_uuid "$vm_name") || {
        log_tpm "WARN" "$vm_name: Could not retrieve UUID"
        return 0  # Non-fatal
    }
    
    local swtpm_dir="$SWTPM_STATE_DIR/$vm_uuid"
    
    # Check if TPM exists
    if [[ ! -d "$swtpm_dir" ]]; then
        log_tpm "DEBUG" "$vm_name: No TPM state directory found"
        return 0  # Non-fatal
    fi
    
    # Verify TPM state files exist
    if ! ls "$swtpm_dir"/tpm2* >/dev/null 2>&1; then
        log_tpm "DEBUG" "$vm_name: No TPM state files found"
        return 0  # Non-fatal
    fi
    
    local tpm_backup_dir="$backup_dir/tpm-state"
    
    # Create TPM backup directory
    mkdir -p "$tpm_backup_dir" || {
        log_tpm "WARN" "$vm_name: Failed to create TPM backup directory: $tpm_backup_dir"
        return 0  # Non-fatal, continue with disk backup
    }
    # Remove SGID inherited from parent — tpm-state must stay root:root
    chmod g-s "$tpm_backup_dir" 2>/dev/null || true
    
    log_tpm "INFO" "$vm_name: Starting TPM state backup from $swtpm_dir"
    
    # Backup TPM state with timeout protection (always uncompressed)
    if timeout "$TPM_BACKUP_TIMEOUT" sudo cp -r "$swtpm_dir"/* "$tpm_backup_dir/" 2>/dev/null; then
        # Fix ownership for non-root user
        sudo chown -R "$(id -u):$(id -g)" "$tpm_backup_dir" 2>/dev/null || \
            log_tpm "WARN" "$vm_name: chown failed on $tpm_backup_dir (may require manual fix)"
        
        # Record backup metadata
        _record_tpm_backup_metadata "$vm_name" "$vm_uuid" "$tpm_backup_dir"
        
        # Log TPM backup as file operation
        if declare -f log_file_operation >/dev/null 2>&1; then
            log_file_operation "copy" "$vm_name" "$swtpm_dir" "$tpm_backup_dir" \
                "tpm_state" "TPM state backup (full copy)" "backup_vm_tpm_full" "true"
        fi
        
        log_tpm "INFO" "$vm_name: TPM state backup completed successfully"
        return 0
    else
        log_tpm "WARN" "$vm_name: TPM state backup failed or timed out (continuing disk backup)"
        if declare -f log_file_operation >/dev/null 2>&1; then
            log_file_operation "copy" "$vm_name" "$swtpm_dir" "$tpm_backup_dir" \
                "tpm_state" "TPM state backup failed/timed out" "backup_vm_tpm_full" "false"
        fi
        return 0  # Non-fatal
    fi
}

##############################################################################
# Incremental TPM Backup Method (rsync-based)
##############################################################################

backup_vm_tpm_incremental() {
    local vm_name="$1"
    local backup_dir="$2"
    
    [[ -z "$vm_name" || -z "$backup_dir" ]] && return 1
    
    local vm_uuid
    vm_uuid=$(get_vm_uuid "$vm_name") || return 0
    
    local swtpm_dir="$SWTPM_STATE_DIR/$vm_uuid"
    
    [[ ! -d "$swtpm_dir" ]] && return 0
    
    local tpm_backup_dir="$backup_dir/tpm-state"
    mkdir -p "$tpm_backup_dir" || return 0
    # Remove SGID inherited from parent — tpm-state must stay root:root
    chmod g-s "$tpm_backup_dir" 2>/dev/null || true
    
    log_tpm "INFO" "$vm_name: Starting incremental TPM backup"
    
    # Use rsync for incremental backup (only changed files)
    if timeout "$TPM_BACKUP_TIMEOUT" sudo rsync -av --delete "$swtpm_dir/" "$tpm_backup_dir/" 2>/dev/null; then
        sudo chown -R "$(id -u):$(id -g)" "$tpm_backup_dir" 2>/dev/null || true
        log_tpm "INFO" "$vm_name: Incremental TPM backup completed"
        _record_tpm_backup_metadata "$vm_name" "$vm_uuid" "$tpm_backup_dir"
        return 0
    else
        log_tpm "WARN" "$vm_name: Incremental TPM backup failed"
        return 0  # Non-fatal
    fi
}

##############################################################################
# Consistent TPM Backup Method (tar-based with atomic snapshot)
##############################################################################

backup_vm_tpm_consistent() {
    local vm_name="$1"
    local backup_dir="$2"
    
    [[ -z "$vm_name" || -z "$backup_dir" ]] && return 1
    
    local vm_uuid
    vm_uuid=$(get_vm_uuid "$vm_name") || return 0
    
    local swtpm_dir="$SWTPM_STATE_DIR/$vm_uuid"
    
    [[ ! -d "$swtpm_dir" ]] && return 0
    
    local tpm_backup_dir="$backup_dir/tpm-state"
    mkdir -p "$tpm_backup_dir" || return 0
    # Remove SGID inherited from parent — tpm-state must stay root:root
    chmod g-s "$tpm_backup_dir" 2>/dev/null || true
    
    log_tpm "INFO" "$vm_name: Starting consistent TPM backup (atomic snapshot)"
    
    # Create atomic tar snapshot to ensure consistency
    local tar_file="$tpm_backup_dir/tpm-state-$(date +%s).tar.gz"
    
    if timeout "$TPM_BACKUP_TIMEOUT" sudo tar --ignore-failed-read \
        -czf "$tar_file" -C "$(dirname "$swtpm_dir")" "$(basename "$swtpm_dir")" 2>/dev/null; then
        
        # Extract for consistency with other backup methods
        if sudo tar -xzf "$tar_file" -C "$tpm_backup_dir/" 2>/dev/null; then
            sudo rm -f "$tar_file"
            sudo chown -R "$(id -u):$(id -g)" "$tpm_backup_dir" 2>/dev/null || true
            log_tpm "INFO" "$vm_name: Consistent TPM backup completed"
            _record_tpm_backup_metadata "$vm_name" "$vm_uuid" "$tpm_backup_dir"
            return 0
        fi
    fi
    
    log_tpm "WARN" "$vm_name: Consistent TPM backup failed"
    return 0  # Non-fatal
}

##############################################################################
# Metadata Recording
##############################################################################

_record_tpm_backup_metadata() {
    local vm_name="$1"
    local vm_uuid="$2"
    local tpm_backup_dir="$3"
    
    local metadata_file="$tpm_backup_dir/BACKUP_METADATA.txt"
    
    cat > "$metadata_file" <<EOF
TPM State Backup Metadata
=========================
VM Name:            $vm_name
VM UUID:            $vm_uuid
Backup Timestamp:   $(date -u '+%Y-%m-%d %H:%M:%S UTC')
Backup Method:      $TPM_BACKUP_METHOD
Source Directory:   $SWTPM_STATE_DIR/$vm_uuid
Backup Directory:   $tpm_backup_dir

TPM Details:
$(get_tpm_info "$vm_name" 2>/dev/null | sed 's/^/  /')

File Inventory:
$(ls -lh "$tpm_backup_dir"/ 2>/dev/null | tail -n +2 | sed 's/^/  /')

Recovery Instructions:
1. Backup Current TPM: sudo cp -r /var/lib/libvirt/swtpm/$vm_uuid /root/tpm-backup-original
2. Clear TPM: sudo rm -rf /var/lib/libvirt/swtpm/$vm_uuid/*
3. Restore From Backup: sudo cp -r $tpm_backup_dir/* /var/lib/libvirt/swtpm/$vm_uuid/
4. Fix Permissions: sudo chown -R tss:tss /var/lib/libvirt/swtpm/$vm_uuid
5. Start VM: virsh start $vm_name

Important Notes:
- TPM state includes encryption keys and secure boot configuration
- If TPM backup is corrupt, VM may fail to boot
- Always verify backup before deleting original
- For Windows VMs: BitLocker keys are stored in TPM
- For Linux VMs: Trusted Boot measurements are affected

EOF

    chmod 640 "$metadata_file"
}

##############################################################################
# BitLocker Recovery Key Extraction (Windows guests only)
##############################################################################
# Extracts BitLocker recovery keys from running Windows guests via the QEMU
# guest agent. Keys are saved alongside TPM state for disaster recovery.
#
# Security:
#   - Output file is root:root 600 (only root can read)
#   - Keys are extracted via guest-exec running as SYSTEM inside the VM
#   - The file lives inside $backup_dir/tpm-state/ and inherits the same
#     lifecycle as TPM state: archived, replicated, and retained automatically
#
# When these keys are needed:
#   - New-identity restore (--name): VM gets a new UUID, TPM state is bound
#     to the old UUID. Even though we restore TPM to the new path, Windows
#     may detect the hardware change and require the recovery key.
#   - TPM state corruption or loss
#   - Secure Boot policy changes
#   - Motherboard/firmware replacement (real hardware)
#
# Extraction approach:
#   1. guest-get-osinfo → confirm Windows guest
#   2. guest-exec manage-bde -status → discover all protected volumes
#   3. guest-exec manage-bde -protectors -get <vol>: → capture full output
#   4. Write complete raw output to bitlocker-recovery-keys.txt (root:root 600)
#
# Non-fatal: All failures are logged and return 0. BitLocker key extraction
# never blocks the backup.

# Run a command inside the guest via QEMU agent guest-exec.
# Returns decoded stdout on success, empty string on failure.
# Args: vm_name, executable_path, arg1, arg2, ...
_guest_exec_capture() {
    local vm_name="$1" exe_path="$2"
    shift 2
    local -a args=("$@")

    # JSON-escape backslashes in path and args
    local json_exe="${exe_path//\\/\\\\}"
    local json_args=""
    for arg in "${args[@]}"; do
        [[ -n "$json_args" ]] && json_args+=","
        json_args+="\"${arg//\\/\\\\}\""
    done

    # Launch command
    local exec_json="{\"execute\":\"guest-exec\",\"arguments\":{\"path\":\"$json_exe\",\"arg\":[$json_args],\"capture-output\":true}}"
    local exec_result
    exec_result=$(virsh qemu-agent-command --timeout "$BITLOCKER_EXEC_TIMEOUT" \
        "$vm_name" "$exec_json" 2>/dev/null) || return 1

    local pid
    pid=$(echo "$exec_result" | grep -oP '"pid":\K[0-9]+') || return 1

    # Poll for completion (up to BITLOCKER_EXEC_TIMEOUT seconds)
    local elapsed=0
    local status_result=""
    while (( elapsed < BITLOCKER_EXEC_TIMEOUT )); do
        sleep 1
        ((elapsed++))
        status_result=$(virsh qemu-agent-command --timeout "$BITLOCKER_EXEC_TIMEOUT" \
            "$vm_name" "{\"execute\":\"guest-exec-status\",\"arguments\":{\"pid\":$pid}}" 2>/dev/null) || continue
        if echo "$status_result" | grep -q '"exited":true'; then
            break
        fi
    done

    # Check exit code
    local exitcode
    exitcode=$(echo "$status_result" | grep -oP '"exitcode":\K[0-9]+' || echo "")
    if [[ "$exitcode" != "0" ]]; then
        return 1
    fi

    # Decode base64 stdout
    local b64_out
    b64_out=$(echo "$status_result" | grep -oP '"out-data":"\K[^"]+' || echo "")
    if [[ -n "$b64_out" ]]; then
        echo "$b64_out" | base64 -d 2>/dev/null
    fi
}

# Extract BitLocker recovery keys from a Windows guest.
# Called from backup_vm_tpm() after TPM state has been copied.
# Args: vm_name, tpm_backup_dir
extract_bitlocker_keys() {
    local vm_name="$1" tpm_backup_dir="$2"

    [[ "$BITLOCKER_KEY_EXTRACTION" != "yes" ]] && return 0

    # Require running VM with guest agent
    if ! virsh domstate "$vm_name" 2>/dev/null | grep -q "running"; then
        log_tpm "DEBUG" "$vm_name: VM not running — skipping BitLocker key extraction"
        return 0
    fi

    # Confirm Windows guest via agent
    local os_id
    os_id=$(virsh qemu-agent-command --timeout 10 "$vm_name" \
        '{"execute":"guest-get-osinfo"}' 2>/dev/null \
        | grep -oP '"id"\s*:\s*"\K[^"]+' || echo "")
    if [[ "$os_id" != "mswindows" ]]; then
        log_tpm "DEBUG" "$vm_name: Not a Windows guest (os_id=$os_id) — skipping BitLocker"
        return 0
    fi

    log_tpm "INFO" "$vm_name: Windows guest detected — extracting BitLocker recovery keys"

    local output_file="$tpm_backup_dir/bitlocker-recovery-keys.txt"
    local exe="C:\\Windows\\System32\\manage-bde.exe"
    local timestamp
    timestamp=$(date '+%Y-%m-%d %H:%M:%S')

    # Step 1: manage-bde -status (discover volumes and protection state)
    local status_output
    status_output=$(_guest_exec_capture "$vm_name" "$exe" "-status") || {
        log_tpm "WARN" "$vm_name: manage-bde -status failed — skipping BitLocker key extraction"
        return 0
    }

    # Parse protected volumes: lines matching "Protection Status:    Protection On"
    # Volume lines look like: "Volume C: [label]"
    local -a protected_volumes=()
    local current_vol=""
    while IFS= read -r line; do
        if [[ "$line" =~ ^Volume\ ([A-Z]): ]]; then
            current_vol="${BASH_REMATCH[1]}:"
        elif [[ "$line" =~ "Protection Status:".*"Protection On" && -n "$current_vol" ]]; then
            protected_volumes+=("$current_vol")
            current_vol=""
        elif [[ "$line" =~ ^Volume\ [A-Z]: ]]; then
            current_vol=""
        fi
    done <<< "$status_output"

    # Step 2: If no protected volumes, log and return without writing a file
    if (( ${#protected_volumes[@]} == 0 )); then
        log_tpm "INFO" "$vm_name: No BitLocker-protected volumes found"
        return 0
    fi

    # Write header + status overview
    {
        echo "BitLocker Recovery Keys"
        echo "VM: $vm_name"
        echo "Extracted: $timestamp"
        echo "Protected Volumes: ${#protected_volumes[@]}"
        echo "========================================"
        echo ""
        echo "=== manage-bde -status ==="
        echo "$status_output"
    } > "$output_file"

    for vol in "${protected_volumes[@]}"; do
        log_tpm "INFO" "$vm_name: Extracting BitLocker protectors for volume $vol"
        local protector_output
        protector_output=$(_guest_exec_capture "$vm_name" "$exe" "-protectors" "-get" "$vol") || {
            {
                echo ""
                echo "=== manage-bde -protectors -get $vol ==="
                echo "ERROR: Failed to retrieve protectors for $vol"
            } >> "$output_file"
            log_tpm "WARN" "$vm_name: Failed to extract protectors for volume $vol"
            continue
        }

        {
            echo ""
            echo "=== manage-bde -protectors -get $vol ==="
            echo "$protector_output"
        } >> "$output_file"
    done

    # Secure the file: root:root 600
    chmod 600 "$output_file"
    chown root:root "$output_file" 2>/dev/null || true

    local vol_count=${#protected_volumes[@]}
    log_tpm "INFO" "$vm_name: BitLocker recovery keys saved to $output_file ($vol_count protected volume(s))"
    return 0
}

##############################################################################
# Validation Functions
##############################################################################

validate_tpm_backup() {
    local tpm_backup_dir="$1"
    
    [[ ! -d "$tpm_backup_dir" ]] && return 1
    
    # Check for TPM state files
    if ls "$tpm_backup_dir"/tpm2* >/dev/null 2>&1; then
        # Check that files are non-empty
        local empty_count=0
        for file in "$tpm_backup_dir"/tpm2*; do
            [[ ! -s "$file" ]] && ((empty_count++))
        done
        
        [[ $empty_count -eq 0 ]] && return 0
    fi
    
    return 1
}

get_tpm_backup_size() {
    local tpm_backup_dir="$1"
    
    [[ ! -d "$tpm_backup_dir" ]] && echo "0" && return 1
    
    du -sh "$tpm_backup_dir" 2>/dev/null | awk '{print $1}'
}

##############################################################################
# Main Backup Function
##############################################################################

backup_vm_tpm() {
    local vm_name="$1"
    local backup_dir="$2"
    
    # Validate inputs
    [[ -z "$vm_name" ]] && log_tpm "ERROR" "VM name not provided" && return 1
    [[ -z "$backup_dir" ]] && log_tpm "ERROR" "Backup directory not provided" && return 1
    [[ ! -d "$backup_dir" ]] && log_tpm "ERROR" "Backup directory does not exist: $backup_dir" && return 1
    
    # Check if TPM backup is enabled
    [[ "$TPM_BACKUP_ENABLED" != "yes" ]] && {
        log_tpm "DEBUG" "$vm_name: TPM backup disabled (TPM_BACKUP_ENABLED=$TPM_BACKUP_ENABLED)"
        return 0
    }
    
    # Check if VM has TPM device
    if ! has_tpm_device "$vm_name" 2>/dev/null; then
        log_tpm "DEBUG" "$vm_name: No TPM device found in VM definition"
        return 0  # Non-fatal
    fi
    
    # Dispatch to appropriate backup method
    log_tpm "DEBUG" "$vm_name: Dispatching TPM backup method='$TPM_BACKUP_METHOD'"
    case "$TPM_BACKUP_METHOD" in
        incremental)
            backup_vm_tpm_incremental "$vm_name" "$backup_dir"
            ;;
        consistent)
            backup_vm_tpm_consistent "$vm_name" "$backup_dir"
            ;;
        full|*)
            backup_vm_tpm_full "$vm_name" "$backup_dir"
            ;;
    esac
    
    # Validate backup result
    local tpm_backup_dir="$backup_dir/tpm-state"
    if [[ -d "$tpm_backup_dir" ]]; then
        if validate_tpm_backup "$tpm_backup_dir"; then
            local size=$(get_tpm_backup_size "$tpm_backup_dir")
            log_tpm "INFO" "$vm_name: TPM backup validation passed (size: $size)"
        else
            log_tpm "WARN" "$vm_name: TPM backup validation failed - backup may be incomplete"
        fi

        # BitLocker recovery key extraction (Windows guests only, non-fatal)
        extract_bitlocker_keys "$vm_name" "$tpm_backup_dir" || \
            log_tpm "WARN" "$vm_name: BitLocker key extraction encountered an error (non-fatal)"
    fi
    
    return 0
}

##############################################################################
# Restoration Functions (for vmrestore.sh integration)
##############################################################################

restore_vm_tpm() {
    local vm_name="$1"
    local tpm_backup_dir="$2"
    
    [[ -z "$vm_name" || -z "$tpm_backup_dir" ]] && return 1
    [[ ! -d "$tpm_backup_dir" ]] && {
        log_tpm "WARN" "$vm_name: No TPM backup directory provided"
        return 0  # Non-fatal
    }
    
    # Get current VM UUID
    local vm_uuid
    vm_uuid=$(get_vm_uuid "$vm_name") || {
        log_tpm "ERROR" "$vm_name: Could not retrieve VM UUID"
        return 1
    }
    
    # CRITICAL: validate UUID is non-empty to prevent rm -rf on wrong path
    if [[ -z "$vm_uuid" ]]; then
        log_tpm "ERROR" "$vm_name: VM UUID is empty - aborting TPM restore to prevent data loss"
        return 1
    fi
    
    local swtpm_dir="$SWTPM_STATE_DIR/$vm_uuid"
    
    # Backup current TPM state before restoring
    local backup_dir="$swtpm_dir.backup-before-restore"
    if [[ -d "$swtpm_dir" ]]; then
        log_tpm "INFO" "$vm_name: Backing up current TPM state to $backup_dir"
        sudo mkdir -p "$backup_dir"
        sudo cp -r "$swtpm_dir"/* "$backup_dir/" 2>/dev/null || true
    fi
    
    log_tpm "INFO" "$vm_name: Clearing current TPM state"
    sudo rm -rf "$swtpm_dir"/* || {
        log_tpm "ERROR" "$vm_name: Failed to clear TPM state directory"
        return 1
    }
    
    log_tpm "INFO" "$vm_name: Restoring TPM state from backup"
    if sudo cp -r "$tpm_backup_dir"/* "$swtpm_dir/" 2>/dev/null; then
        # Fix ownership — detect correct TPM user (tss on Debian, swtpm on Arch)
        local tpm_user="tss"
        if ! getent passwd tss >/dev/null 2>&1; then
            if getent passwd swtpm >/dev/null 2>&1; then
                tpm_user="swtpm"
            else
                log_tpm "WARN" "$vm_name: Neither tss nor swtpm user found — skipping chown"
                tpm_user=""
            fi
        fi
        if [[ -n "$tpm_user" ]]; then
            sudo chown -R "${tpm_user}:${tpm_user}" "$swtpm_dir" 2>/dev/null || {
                log_tpm "WARN" "$vm_name: Could not set TPM ownership to ${tpm_user}:${tpm_user}"
            }
        fi
        
        log_tpm "INFO" "$vm_name: TPM state restored successfully"
        return 0
    else
        log_tpm "ERROR" "$vm_name: Failed to restore TPM state from backup"
        log_tpm "INFO" "$vm_name: Attempting to restore from pre-restore backup at $backup_dir"
        sudo cp -r "$backup_dir"/* "$swtpm_dir/" 2>/dev/null || true
        return 1
    fi
}

##############################################################################
# Reporting Functions
##############################################################################

report_tpm_backup_status() {
    local backup_dir="$1"
    local vm_name="$2"
    
    [[ -z "$backup_dir" ]] && return 1
    
    local tpm_backup_dir="$backup_dir/tpm-state"
    
    if [[ ! -d "$tpm_backup_dir" ]]; then
        echo "TPM Backup Status: NOT FOUND"
        return 1
    fi
    
    echo "TPM Backup Status: EXISTS"
    echo "  Size: $(du -sh "$tpm_backup_dir" 2>/dev/null | awk '{print $1}')"
    echo "  Files: $(find "$tpm_backup_dir" -type f 2>/dev/null | wc -l)"
    echo "  Metadata: $([ -f "$tpm_backup_dir/BACKUP_METADATA.txt" ] && echo "Yes" || echo "No")"
    
    if validate_tpm_backup "$tpm_backup_dir"; then
        echo "  Validation: PASS"
        return 0
    else
        echo "  Validation: WARN"
        return 1
    fi
}

# Export functions for use by parent scripts
export -f backup_vm_tpm
export -f restore_vm_tpm
export -f validate_tpm_backup
export -f report_tpm_backup_status
export -f extract_bitlocker_keys
export -f _guest_exec_capture
export -f log_tpm
export -f get_vm_uuid
export -f has_tpm_device

# End of TPM Backup Module
