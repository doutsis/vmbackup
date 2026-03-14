#!/bin/bash
#################################################################################
# Transfer Utilities Library
#
# Shared utility functions for rsync and rclone transfer operations.
# Used by transport drivers for consistent progress monitoring, formatting,
# and error handling.
#
# Exported Functions:
#   Formatting:
#     tu_format_bytes()        - Convert bytes to human-readable (e.g., 1.5GiB)
#     tu_parse_bytes()         - Parse human-readable to bytes
#     tu_format_rate()         - Format transfer rate (e.g., 52MiB/s)
#     tu_format_elapsed()      - Format elapsed time (e.g., 2m 30s)
#     tu_calculate_eta()       - Calculate ETA from rate and remaining
#
#   Progress Monitoring:
#     tu_get_dir_size()        - Get directory/file size in bytes
#     tu_log_progress()        - Log standardized progress line
#
#   Exit Code Handling:
#     tu_rsync_exit_message()  - Map rsync exit code to message
#     tu_rclone_exit_message() - Map rclone exit code to message
#
#   Log Path Management:
#     tu_get_replication_log_path() - Get standardized log file path
#
# Version: 1.0
# Created: 2026-02-02
#################################################################################

# Library identification
TU_LIB_NAME="transfer_utils"
TU_LIB_VERSION="1.0"

# Ensure library is only loaded once
if [[ -n "$_TRANSFER_UTILS_LOADED" ]]; then
    return 0
fi
_TRANSFER_UTILS_LOADED=1

#=============================================================================
# BYTE FORMATTING
#=============================================================================

# Convert bytes to human-readable format
# Arguments:
#   $1 - Bytes (integer)
# Output:
#   Human-readable string (e.g., "1.5GiB")
tu_format_bytes() {
    local bytes="${1:-0}"
    numfmt --to=iec-i --suffix=B "$bytes" 2>/dev/null || echo "${bytes}B"
}

# Parse human-readable bytes to integer
# Handles formats: "489.967 MiB", "1.234GiB", "1234567"
# Arguments:
#   $1 - Human-readable string
# Output:
#   Bytes as integer
tu_parse_bytes() {
    local str="$1"
    
    # If already a number, return as-is
    if [[ "$str" =~ ^[0-9]+$ ]]; then
        echo "$str"
        return
    fi
    
    # Remove 'iB' suffix and spaces: "489.967 MiB" -> "489.967M"
    local clean
    clean=$(echo "$str" | sed 's/iB//g; s/ //g')
    
    numfmt --from=iec "$clean" 2>/dev/null || echo 0
}

# Format transfer rate
# Arguments:
#   $1 - Bytes per second
# Output:
#   Formatted rate (e.g., "52MiB/s")
tu_format_rate() {
    local bytes_per_sec="${1:-0}"
    
    if [[ "$bytes_per_sec" -le 0 ]]; then
        echo "--"
        return
    fi
    
    local formatted
    formatted=$(numfmt --to=iec-i --suffix=B "$bytes_per_sec" 2>/dev/null || echo "${bytes_per_sec}B")
    echo "${formatted}/s"
}

#=============================================================================
# TIME FORMATTING
#=============================================================================

# Format elapsed time
# Arguments:
#   $1 - Seconds
# Output:
#   Formatted time (e.g., "2m 30s" or "1h 15m")
tu_format_elapsed() {
    local seconds="${1:-0}"
    local hours=$((seconds / 3600))
    local mins=$(((seconds % 3600) / 60))
    local secs=$((seconds % 60))
    
    if [[ $hours -gt 0 ]]; then
        echo "${hours}h ${mins}m"
    else
        echo "${mins}m ${secs}s"
    fi
}

# Calculate ETA from remaining bytes and rate
# Arguments:
#   $1 - Remaining bytes
#   $2 - Rate in bytes/second
# Output:
#   Formatted ETA (e.g., "42m 15s" or "1h 30m")
tu_calculate_eta() {
    local remaining="${1:-0}"
    local rate="${2:-0}"
    
    if [[ "$rate" -le 0 ]] || [[ "$remaining" -le 0 ]]; then
        echo "--"
        return
    fi
    
    local eta_sec=$((remaining / rate))
    tu_format_elapsed "$eta_sec"
}

#=============================================================================
# SIZE CALCULATION
#=============================================================================

# Get directory or file size in bytes
# Arguments:
#   $1 - Path to directory or file
# Output:
#   Size in bytes (integer)
tu_get_dir_size() {
    local path="$1"
    
    if [[ -d "$path" ]]; then
        du -sb "$path" 2>/dev/null | cut -f1
    elif [[ -f "$path" ]]; then
        stat -c %s "$path" 2>/dev/null || echo 0
    else
        echo 0
    fi
}

#=============================================================================
# PROGRESS LOGGING
#=============================================================================

# Generate standardized progress log line
# Arguments:
#   $1 - Current bytes transferred
#   $2 - Total source bytes
#   $3 - Elapsed seconds
#   $4 - Previous bytes (for interval rate calculation, optional)
#   $5 - Previous time (for interval rate calculation, optional)
# Output:
#   Formatted progress line
tu_format_progress() {
    local current_bytes="${1:-0}"
    local total_bytes="${2:-0}"
    local elapsed="${3:-0}"
    local prev_bytes="${4:-0}"
    local prev_time="${5:-0}"
    
    # Format current and total
    local current_human total_human
    current_human=$(tu_format_bytes "$current_bytes")
    total_human=$(tu_format_bytes "$total_bytes")
    
    # Calculate percentage
    local percent=0
    if [[ "$total_bytes" -gt 0 ]] && [[ "$current_bytes" -gt 0 ]]; then
        percent=$((current_bytes * 100 / total_bytes))
        [[ $percent -gt 100 ]] && percent=100
    fi
    
    # Calculate rate (prefer interval-based if available, fallback to average)
    local rate=0
    if [[ "$prev_time" -gt 0 ]] && [[ "$prev_bytes" -gt 0 ]]; then
        local interval=$((elapsed - prev_time))
        if [[ "$interval" -gt 0 ]]; then
            local bytes_delta=$((current_bytes - prev_bytes))
            if [[ "$bytes_delta" -gt 0 ]]; then
                rate=$((bytes_delta / interval))
            fi
        fi
    elif [[ "$elapsed" -gt 0 ]] && [[ "$current_bytes" -gt 0 ]]; then
        rate=$((current_bytes / elapsed))
    fi
    
    local rate_human
    rate_human=$(tu_format_rate "$rate")
    
    # Calculate ETA
    local eta_text="--"
    if [[ "$rate" -gt 0 ]] && [[ "$total_bytes" -gt "$current_bytes" ]]; then
        local remaining=$((total_bytes - current_bytes))
        eta_text=$(tu_calculate_eta "$remaining" "$rate")
    fi
    
    # Format elapsed
    local elapsed_text
    elapsed_text=$(tu_format_elapsed "$elapsed")
    
    echo "Progress: ${current_human} / ${total_human} (${percent}%) @ ${rate_human} - ETA ${eta_text} (${elapsed_text} elapsed)"
}

#=============================================================================
# EXIT CODE HANDLING
#=============================================================================

# Map rsync exit code to human-readable message
# Arguments:
#   $1 - Exit code
# Output:
#   Message string
tu_rsync_exit_message() {
    local code="$1"
    case $code in
        0)  echo "success" ;;
        1)  echo "syntax/usage error" ;;
        2)  echo "protocol incompatibility" ;;
        3)  echo "errors selecting I/O files/dirs" ;;
        4)  echo "requested action not supported" ;;
        5)  echo "error starting client-server protocol" ;;
        6)  echo "daemon unable to append to log" ;;
        10) echo "error in socket I/O" ;;
        11) echo "error in file I/O" ;;
        12) echo "error in rsync protocol data stream" ;;
        13) echo "errors with program diagnostics" ;;
        14) echo "error in IPC code" ;;
        20) echo "received SIGUSR1 or SIGINT" ;;
        21) echo "some error returned by waitpid()" ;;
        22) echo "error allocating core memory buffers" ;;
        23) echo "partial transfer due to error" ;;
        24) echo "partial transfer due to vanished source files" ;;
        25) echo "max delete limit reached" ;;
        30) echo "timeout in data send/receive" ;;
        35) echo "timeout waiting for daemon connection" ;;
        *)  echo "unknown error (code: $code)" ;;
    esac
}

# Map rclone exit code to human-readable message
# Arguments:
#   $1 - Exit code
# Output:
#   Message string
tu_rclone_exit_message() {
    local code="$1"
    case $code in
        0) echo "success" ;;
        1) echo "syntax/usage error" ;;
        2) echo "error not otherwise categorised" ;;
        3) echo "directory not found" ;;
        4) echo "file not found" ;;
        5) echo "temporary error (retry may succeed)" ;;
        6) echo "less serious errors (some transfers failed)" ;;
        7) echo "fatal error (retries didn't help)" ;;
        8) echo "transfer limit exceeded" ;;
        9) echo "check failed (files differ)" ;;
        *) echo "unknown error (code: $code)" ;;
    esac
}

#=============================================================================
# LOG PATH MANAGEMENT
#=============================================================================

# Get standardized replication log path
# Arguments:
#   $1 - Type: "local" or "cloud"
#   $2 - Endpoint name
#   $3 - Optional timestamp (defaults to current)
# Output:
#   Full path to log file
tu_get_replication_log_path() {
    local type="$1"
    local endpoint="$2"
    local timestamp="${3:-$(date '+%Y-%m-%d_%H%M%S')}"
    
    local state_dir="${STATE_DIR:-${BACKUP_PATH}_state}"
    local log_dir="${state_dir}/replication_logs/${type}"
    
    mkdir -p "$log_dir" 2>/dev/null
    
    echo "${log_dir}/${endpoint}_${timestamp}.log"
}

#=============================================================================
# BACKGROUND PROCESS MONITORING
#=============================================================================

# Monitor a background process with progress updates
# Arguments:
#   $1 - PID to monitor
#   $2 - Source size in bytes
#   $3 - Progress callback function (receives: elapsed, returns: current_bytes)
#   $4 - Log function (receives: message)
#   $5 - Interval in seconds (default: 30)
# Returns:
#   Exit code of the monitored process
# Sets:
#   TU_MONITOR_LAST_BYTES - Last recorded bytes transferred
tu_monitor_transfer() {
    local pid="$1"
    local source_bytes="$2"
    local get_progress_func="$3"
    local log_func="$4"
    local interval="${5:-30}"
    
    local start_time
    start_time=$(date '+%s')
    
    local last_bytes=0
    local last_time=$start_time
    
    while kill -0 "$pid" 2>/dev/null; do
        sleep "$interval"
        
        # Check if still running after sleep
        if ! kill -0 "$pid" 2>/dev/null; then
            break
        fi
        
        local now
        now=$(date '+%s')
        local elapsed=$((now - start_time))
        
        # Get current progress from callback
        local current_bytes
        current_bytes=$("$get_progress_func")
        
        if [[ "$current_bytes" -gt 0 ]]; then
            # Calculate progress using interval-based rate
            local progress_line
            progress_line=$(tu_format_progress "$current_bytes" "$source_bytes" "$elapsed" "$last_bytes" "$last_time")
            
            # Log progress
            "$log_func" "$progress_line"
            
            # Update tracking
            last_bytes=$current_bytes
            last_time=$now
        fi
    done
    
    # Export last bytes for caller
    TU_MONITOR_LAST_BYTES=$last_bytes
    
    # Wait and return exit code
    wait "$pid"
    return $?
}
