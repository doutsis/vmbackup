#!/bin/bash
#################################################################################
# Local Filesystem Transport Driver for Replication Module
#
# Handles rsync-based replication to local or pre-mounted filesystem destinations.
# Works with any locally accessible path — local disks, NFS mounts, virtiofs, CIFS, etc.
#
# Exported Functions:
#   transport_init()    - Verify destination is accessible
#   transport_sync()    - Perform rsync to destination
#   transport_verify()  - Verify sync completed correctly
#   transport_cleanup() - Cleanup (no-op for local)
#
# Dependencies:
#   - rsync
#   - Destination path accessible and writable
#   - lib/transfer_utils.sh
#   - Logging functions from parent (log_info, log_warn, log_error, log_debug)
#
# Version: 2.0
# Created: 2026-01-23
# Updated: 2026-02-02 - Refactored to use transfer_utils.sh
#################################################################################

# Transport identification
TRANSPORT_NAME="local"
TRANSPORT_VERSION="2.1"

#=============================================================================
# TRANSPORT METRICS CONTRACT (v1.0)
#
# All local transports (transport_*.sh) MUST set these globals after sync:
#
#   TRANSPORT_BYTES_TRANSFERRED  - integer: bytes transferred (0 if none)
#   TRANSPORT_SYNC_DURATION      - integer: seconds elapsed
#   TRANSPORT_DEST_AVAIL_BYTES   - integer: free bytes at dest (0 if unknown)
#   TRANSPORT_DEST_TOTAL_BYTES   - integer: total bytes at dest (0 if unknown)
#   TRANSPORT_DEST_SPACE_KNOWN   - 0|1: whether space metrics are reliable
#   TRANSPORT_THROTTLE_COUNT     - integer: throttle events (-1 = not applicable)
#   TRANSPORT_BWLIMIT_FINAL      - string: final bwlimit after adjustments ("" = none)
#
# Sentinel values:
#   -1 = metric structurally not applicable to this transport type
#    0 = applicable but none occurred / not available
#   "" = no value (string fields)
#
# For local transport:
#   TRANSPORT_THROTTLE_COUNT = -1 (rsync does not throttle)
#   TRANSPORT_DEST_SPACE_KNOWN = 1 (local fs always knowable)
#=============================================================================

# Metrics globals
TRANSPORT_BYTES_TRANSFERRED=0
TRANSPORT_FILES_TRANSFERRED=0
TRANSPORT_SYNC_DURATION=0
TRANSPORT_CANCELLED=0
TRANSPORT_MOUNT_DEVICE=""
TRANSPORT_MOUNT_FREE=0
TRANSPORT_DEST_AVAIL_BYTES=0
TRANSPORT_DEST_TOTAL_BYTES=0
TRANSPORT_DEST_SPACE_KNOWN=0
TRANSPORT_THROTTLE_COUNT=-1
TRANSPORT_BWLIMIT_FINAL=""

# Load transfer utilities
_TRANSPORT_LOCAL_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
_TRANSPORT_LIB_DIR="$(dirname "$_TRANSPORT_LOCAL_DIR")/lib"
if [[ -f "$_TRANSPORT_LIB_DIR/transfer_utils.sh" ]]; then
    # shellcheck source=../lib/transfer_utils.sh
    source "$_TRANSPORT_LIB_DIR/transfer_utils.sh"
fi

#=============================================================================
# INTERNAL HELPERS
#=============================================================================

# Log wrapper for consistent format
_local_log_info()  { log_info  "transport_local" "$1" "$2"; }
_local_log_warn()  { log_warn  "transport_local" "$1" "$2"; }
_local_log_error() { log_error "transport_local" "$1" "$2"; }
_local_log_debug() { log_debug "transport_local" "$1" "$2"; }

# Progress callback for rsync monitoring
# Uses destination size as progress indicator
_local_get_progress_bytes() {
    local dest_path="$1"
    tu_get_dir_size "$dest_path"
}

# Parse rsync statistics output
_local_parse_rsync_bytes() {
    local output="$1"
    local bytes_line
    bytes_line=$(echo "$output" | grep -E "^Total transferred file size:" | head -1)
    
    if [[ -z "$bytes_line" ]]; then
        echo 0
        return
    fi
    
    # Extract value: "265.30M" or "1,234,567"
    local size_str
    size_str=$(echo "$bytes_line" | sed -E 's/.*: ([0-9.,]+[KMGTP]?) bytes.*/\1/' | tr -d ',')
    
    if [[ "$size_str" =~ ^[0-9]+$ ]]; then
        echo "$size_str"
    elif [[ "$size_str" =~ ^([0-9.]+)([KMGTP])$ ]]; then
        local num="${BASH_REMATCH[1]}"
        local suffix="${BASH_REMATCH[2]}"
        local multiplier=1
        case "$suffix" in
            K) multiplier=1024 ;;
            M) multiplier=$((1024 * 1024)) ;;
            G) multiplier=$((1024 * 1024 * 1024)) ;;
            T) multiplier=$((1024 * 1024 * 1024 * 1024)) ;;
            P) multiplier=$((1024 * 1024 * 1024 * 1024 * 1024)) ;;
        esac
        echo "$num * $multiplier" | bc 2>/dev/null | cut -d'.' -f1
    else
        echo 0
    fi
}

# Parse rsync file count from stats output
# Matches: "Number of regular files transferred: 127"
_local_parse_rsync_files() {
    local output="$1"
    local files_line
    files_line=$(echo "$output" | grep -E "^Number of regular files transferred:" | head -1)
    
    if [[ -z "$files_line" ]]; then
        echo 0
        return
    fi
    
    local count
    count=$(echo "$files_line" | sed -E 's/.*: ([0-9,]+).*/\1/' | tr -d ',')
    echo "${count:-0}"
}

#################################################################################
# transport_init - Verify destination is accessible and writable
#################################################################################
transport_init() {
    local dest_path="$1"
    local dest_name="${2:-local}"
    
    _local_log_info "init" "Checking destination: $dest_path"
    
    # Check if path exists
    if [[ ! -d "$dest_path" ]]; then
        _local_log_error "init" "Destination path does not exist: $dest_path"
        return 1
    fi
    
    # Check if it's a mount point (informational, not required)
    if mountpoint -q "$dest_path" 2>/dev/null; then
        local mount_info
        mount_info=$(findmnt -n -o SOURCE "$dest_path" 2>/dev/null)
        TRANSPORT_MOUNT_DEVICE="${mount_info:-unknown}"
        local fs_type
        fs_type=$(findmnt -n -o FSTYPE "$dest_path" 2>/dev/null)
        _local_log_info "init" "Mount point verified: $TRANSPORT_MOUNT_DEVICE (${fs_type:-unknown})"
    else
        TRANSPORT_MOUNT_DEVICE="local"
        _local_log_info "init" "Local path (not a mount point): $dest_path"
    fi
    
    # Check write access
    local test_file="$dest_path/.replication_write_test_$$"
    if ! touch "$test_file" 2>/dev/null; then
        _local_log_error "init" "Cannot write to destination: $dest_path"
        _local_log_error "init" "Check filesystem permissions"
        return 1
    fi
    rm -f "$test_file" 2>/dev/null
    _local_log_debug "init" "Write access verified"
    
    # Get free space
    local free_bytes
    free_bytes=$(df -B1 --output=avail "$dest_path" 2>/dev/null | tail -1 | tr -d ' ')
    if [[ -n "$free_bytes" ]] && [[ "$free_bytes" =~ ^[0-9]+$ ]]; then
        TRANSPORT_MOUNT_FREE="$free_bytes"
        TRANSPORT_DEST_AVAIL_BYTES="$free_bytes"
        TRANSPORT_DEST_SPACE_KNOWN=1
        local free_human
        free_human=$(tu_format_bytes "$free_bytes")
        
        local total_bytes
        total_bytes=$(df -B1 --output=size "$dest_path" 2>/dev/null | tail -1 | tr -d ' ')
        if [[ -n "$total_bytes" ]] && [[ "$total_bytes" =~ ^[0-9]+$ ]] && [[ "$total_bytes" -gt 0 ]]; then
            TRANSPORT_DEST_TOTAL_BYTES="$total_bytes"
            local free_percent=$((free_bytes * 100 / total_bytes))
            _local_log_info "init" "Destination free space: $free_human ($free_percent%)"
        else
            _local_log_info "init" "Destination free space: $free_human"
        fi
    else
        TRANSPORT_MOUNT_FREE=0
        TRANSPORT_DEST_AVAIL_BYTES=0
        TRANSPORT_DEST_TOTAL_BYTES=0
        TRANSPORT_DEST_SPACE_KNOWN=0
        _local_log_warn "init" "Could not determine free space"
    fi
    
    return 0
}

#################################################################################
# transport_sync - Perform rsync to local destination
#################################################################################
transport_sync() {
    local source_path="$1"
    local dest_path="$2"
    local sync_mode="${3:-mirror}"
    local bwlimit="${4:-0}"
    local dry_run="${5:-false}"
    local dest_name="${6:-nfs}"
    
    # Set bwlimit_final for metrics contract
    TRANSPORT_BWLIMIT_FINAL="$bwlimit"
    
    # Ensure source path has trailing slash for rsync directory sync
    [[ "${source_path}" != */ ]] && source_path="${source_path}/"
    
    _local_log_info "sync" "Starting rsync ($sync_mode mode${bwlimit:+, bwlimit=${bwlimit}KB/s}${dry_run:+, DRY RUN})"
    _local_log_info "sync" "Source: $source_path"
    _local_log_info "sync" "Destination: $dest_path"
    
    # Build rsync options
    local rsync_opts=()
    rsync_opts+=("--archive" "--human-readable" "--itemize-changes" "--stats")
    rsync_opts+=("--info=progress2")  # Show overall progress: bytes, %, speed, ETA
    
    if [[ "$sync_mode" == "mirror" ]]; then
        rsync_opts+=("--delete" "--delete-during")
        _local_log_debug "sync" "Mirror mode: --delete enabled"
    else
        _local_log_debug "sync" "Accumulate mode: no deletion"
    fi
    
    if [[ "$bwlimit" -gt 0 ]]; then
        rsync_opts+=("--bwlimit=$bwlimit")
        _local_log_debug "sync" "Bandwidth limited to ${bwlimit} KB/s"
    fi
    
    if [[ "$dry_run" == "true" ]]; then
        rsync_opts+=("--dry-run")
        _local_log_info "sync" "DRY RUN - no changes will be made"
    fi
    
    _local_log_debug "sync" "[RSYNC-CMD] rsync ${rsync_opts[*]} $source_path $dest_path"
    
    # Get source size
    local start_time source_size source_human
    start_time=$(date +%s)
    source_size=$(tu_get_dir_size "$source_path")
    source_human=$(tu_format_bytes "$source_size")
    _local_log_info "sync" "Source size: $source_human"
    
    # Setup log file
    local rsync_log
    rsync_log=$(tu_get_replication_log_path "local" "$dest_name")
    _local_log_info "sync" "Detailed rsync log: $rsync_log"
    
    # Run rsync in background
    rsync "${rsync_opts[@]}" "$source_path" "$dest_path" > "$rsync_log" 2>&1 &
    local rsync_pid=$!
    _local_log_info "sync" "Rsync started (PID: $rsync_pid)"
    
    # Reset cancellation state
    TRANSPORT_CANCELLED=0
    
    # Monitor progress
    local progress_interval=30
    local last_bytes=0
    local last_time=$start_time
    
    while kill -0 "$rsync_pid" 2>/dev/null; do
        sleep 5
        
        if ! kill -0 "$rsync_pid" 2>/dev/null; then
            break
        fi
        
        # Check for replication cancellation
        if type is_replication_cancelled &>/dev/null && is_replication_cancelled; then
            _local_log_warn "sync" "Replication cancellation detected - terminating rsync (PID: $rsync_pid)"
            kill "$rsync_pid" 2>/dev/null
            # Give rsync a moment to clean up gracefully
            sleep 2
            kill -0 "$rsync_pid" 2>/dev/null && kill -9 "$rsync_pid" 2>/dev/null
            wait "$rsync_pid" 2>/dev/null
            TRANSPORT_CANCELLED=1
            _local_log_warn "sync" "Rsync terminated by cancellation request"
            return 1
        fi
        
        local now=$(($(date +%s)))
        local elapsed=$((now - start_time))
        
        # Only log at intervals
        if [[ $((elapsed - (elapsed % progress_interval))) -gt $((last_time - start_time)) ]] || \
           [[ $((now - last_time)) -ge $progress_interval ]]; then
            
            local current_bytes
            current_bytes=$(_local_get_progress_bytes "$dest_path")
            
            if [[ "$current_bytes" -gt 0 ]]; then
                local progress_line
                progress_line=$(tu_format_progress "$current_bytes" "$source_size" "$elapsed" "$last_bytes" "$((last_time - start_time))")
                _local_log_info "sync" "$progress_line"
                
                last_bytes=$current_bytes
                last_time=$now
            fi
        fi
    done
    
    # Wait for completion
    wait "$rsync_pid"
    local rsync_exit=$?
    
    local end_time
    end_time=$(date +%s)
    TRANSPORT_SYNC_DURATION=$((end_time - start_time))
    
    # Read and parse output
    local rsync_output
    rsync_output=$(cat "$rsync_log")
    
    # Log exit status
    local exit_msg
    exit_msg=$(tu_rsync_exit_message "$rsync_exit")
    if [[ $rsync_exit -eq 0 ]]; then
        _local_log_debug "sync" "[RSYNC-EXIT] code=$rsync_exit ($exit_msg)"
    else
        _local_log_warn "sync" "[RSYNC-EXIT] code=$rsync_exit ($exit_msg)"
    fi
    
    # Parse transfer statistics
    TRANSPORT_BYTES_TRANSFERRED=$(_local_parse_rsync_bytes "$rsync_output")
    TRANSPORT_FILES_TRANSFERRED=$(_local_parse_rsync_files "$rsync_output")
    
    # Log rsync statistics (debug level)
    local stats_section=false
    while IFS= read -r line; do
        if [[ "$line" =~ ^Number\ of\ files: ]]; then
            stats_section=true
        fi
        if [[ "$stats_section" == true ]]; then
            _local_log_debug "sync" "rsync: $line"
        fi
    done <<< "$rsync_output"
    
    # Final status
    if [[ $rsync_exit -eq 0 ]]; then
        local bytes_human
        bytes_human=$(tu_format_bytes "$TRANSPORT_BYTES_TRANSFERRED")
        _local_log_info "sync" "Rsync completed: $bytes_human transferred in ${TRANSPORT_SYNC_DURATION}s"
        # Security: ensure rsync log file is accessible to backup group
        command -v set_backup_permissions &>/dev/null && set_backup_permissions "$rsync_log"
        return 0
    else
        _local_log_error "sync" "Rsync failed with exit code: $rsync_exit ($exit_msg)"
        _local_log_error "sync" "Last 10 lines of output:"
        echo "$rsync_output" | tail -10 | while IFS= read -r line; do
            _local_log_error "sync" "  $line"
        done
        # Security: ensure rsync log file is accessible to backup group
        command -v set_backup_permissions &>/dev/null && set_backup_permissions "$rsync_log"
        return 1
    fi
}

#################################################################################
# transport_verify - Verify sync completed correctly
#################################################################################
transport_verify() {
    local source_path="$1"
    local dest_path="$2"
    local verify_mode="${3:-size}"
    
    _local_log_info "verify" "Starting verification (mode: $verify_mode)"
    
    case "$verify_mode" in
        none)
            _local_log_info "verify" "Verification skipped (mode=none)"
            return 0
            ;;
        size)
            return $(_local_verify_size "$source_path" "$dest_path")
            ;;
        checksum)
            return $(_local_verify_checksum "$source_path" "$dest_path")
            ;;
        *)
            _local_log_warn "verify" "Unknown verify mode: $verify_mode, defaulting to size"
            return $(_local_verify_size "$source_path" "$dest_path")
            ;;
    esac
}

_local_verify_size() {
    local source_path="$1"
    local dest_path="$2"
    
    local source_size dest_size
    source_size=$(tu_get_dir_size "$source_path")
    dest_size=$(tu_get_dir_size "$dest_path")
    
    local source_human dest_human
    source_human=$(tu_format_bytes "$source_size")
    dest_human=$(tu_format_bytes "$dest_size")
    
    _local_log_info "verify" "Source size: $source_human, Destination size: $dest_human"
    
    # Allow 1% tolerance for filesystem differences
    local tolerance=$((source_size / 100))
    local diff=$((source_size - dest_size))
    [[ $diff -lt 0 ]] && diff=$((-diff))
    
    if [[ $diff -le $tolerance ]]; then
        _local_log_info "verify" "Size verification passed"
        return 0
    else
        _local_log_error "verify" "Size mismatch: difference of $(tu_format_bytes $diff)"
        return 1
    fi
}

_local_verify_checksum() {
    local source_path="$1"
    local dest_path="$2"
    
    _local_log_info "verify" "Running checksum verification with rsync --checksum --dry-run"
    
    local rsync_output
    rsync_output=$(rsync --archive --checksum --dry-run --itemize-changes "$source_path" "$dest_path" 2>&1)
    local rsync_exit=$?
    
    if [[ $rsync_exit -ne 0 ]]; then
        _local_log_error "verify" "Checksum verification failed (rsync exit: $rsync_exit)"
        return 1
    fi
    
    # Check if any files would be transferred (indicating mismatch)
    local changes
    changes=$(echo "$rsync_output" | grep -c '^[<>ch]' || echo 0)
    
    if [[ "$changes" -eq 0 ]]; then
        _local_log_info "verify" "Checksum verification passed"
        return 0
    else
        _local_log_error "verify" "Checksum verification failed: $changes files differ"
        echo "$rsync_output" | grep '^[<>ch]' | head -10 | while IFS= read -r line; do
            _local_log_error "verify" "  $line"
        done
        return 1
    fi
}

#################################################################################
# transport_cleanup - Cleanup after sync (no-op for local)
#################################################################################
transport_cleanup() {
    _local_log_debug "cleanup" "No cleanup required for local transport"
    return 0
}

#################################################################################
# transport_get_free_space - Get free space at destination
#################################################################################
transport_get_free_space() {
    local dest_path="$1"
    
    local free_bytes
    free_bytes=$(df -B1 --output=avail "$dest_path" 2>/dev/null | tail -1 | tr -d ' ')
    
    if [[ -n "$free_bytes" ]] && [[ "$free_bytes" =~ ^[0-9]+$ ]]; then
        echo "$free_bytes"
    else
        echo 0
    fi
}
