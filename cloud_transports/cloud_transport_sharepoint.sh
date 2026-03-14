#!/bin/bash
#=============================================================================
# Cloud Transport Driver: SharePoint (Microsoft 365)
# Uses rclone with delegated authentication
# Refactored to use shared transfer_utils.sh library
#=============================================================================

CLOUD_TRANSPORT_SHAREPOINT_VERSION="2.2.0"

#=============================================================================
# CLOUD TRANSPORT METRICS CONTRACT (v1.0)
#
# All cloud transports (cloud_transport_*.sh) MUST set these globals after
# each upload call returns, so the cloud replication module can pass them
# to sqlite_log_replication_run():
#
#   CLOUD_TRANSPORT_THROTTLE_COUNT   - integer: throttle events (0 = none)
#   CLOUD_TRANSPORT_BWLIMIT_FINAL    - string: final bwlimit after adjustments
#   CLOUD_TRANSPORT_DEST_AVAIL_BYTES - integer: free bytes at dest (0 if unknown)
#   CLOUD_TRANSPORT_DEST_TOTAL_BYTES - integer: total bytes at dest (0 if unknown)
#   CLOUD_TRANSPORT_DEST_SPACE_KNOWN - 0|1: whether space metrics are reliable
#
# Sentinel values:
#   -1 = metric structurally not applicable to this transport type
#    0 = applicable but none occurred / not available
#   "" = no value (string fields)
#
# For SharePoint transport:
#   CLOUD_TRANSPORT_THROTTLE_COUNT = actual 429 count (applicable)
#   CLOUD_TRANSPORT_DEST_SPACE_KNOWN = 0 (quota API unreliable)
#=============================================================================

# Cloud transport metrics globals
declare -g CLOUD_TRANSPORT_THROTTLE_COUNT=0
declare -g CLOUD_TRANSPORT_BWLIMIT_FINAL=""
declare -g CLOUD_TRANSPORT_DEST_AVAIL_BYTES=0
declare -g CLOUD_TRANSPORT_DEST_TOTAL_BYTES=0
declare -g CLOUD_TRANSPORT_DEST_SPACE_KNOWN=0

#=============================================================================
# LOAD SHARED LIBRARY
#=============================================================================
_CLOUD_TRANSPORT_DIR="$(dirname "$(realpath "${BASH_SOURCE[0]}")")"
source "${_CLOUD_TRANSPORT_DIR}/../lib/transfer_utils.sh" || {
    echo "FATAL: Cannot load lib/transfer_utils.sh" >&2
    return 1
}

#=============================================================================
# SHAREPOINT-SPECIFIC CONSTANTS
#=============================================================================
declare -g SHAREPOINT_THROTTLE_COUNT=0
declare -g SHAREPOINT_CURRENT_BWLIMIT=0
declare -g SHAREPOINT_429_RETRY_COUNT=0

# Default throttle settings
declare -g SHAREPOINT_429_MAX_RETRIES=${SHAREPOINT_429_MAX_RETRIES:-5}
declare -g SHAREPOINT_429_BACKOFF_BASE=${SHAREPOINT_429_BACKOFF_BASE:-60}
declare -g SHAREPOINT_429_BACKOFF_MULTIPLIER=${SHAREPOINT_429_BACKOFF_MULTIPLIER:-2}
declare -g SHAREPOINT_429_BW_REDUCE_PERCENT=${SHAREPOINT_429_BW_REDUCE_PERCENT:-50}

#=============================================================================
# LOGGING WRAPPERS
# Format: [timestamp] [LEVEL] [cloud_transport_sharepoint.sh] [function] message
# Uses FUNCNAME[1] to get the calling function name
#=============================================================================
cloud_log_debug() { log_debug "cloud_transport_sharepoint.sh" "${FUNCNAME[1]}" "$*"; }
cloud_log_info()  { log_info  "cloud_transport_sharepoint.sh" "${FUNCNAME[1]}" "$*"; }
cloud_log_warn()  { log_warn  "cloud_transport_sharepoint.sh" "${FUNCNAME[1]}" "$*"; }
cloud_log_error() { log_error "cloud_transport_sharepoint.sh" "${FUNCNAME[1]}" "$*"; }

#=============================================================================
# 429 THROTTLING HANDLERS
#=============================================================================

# Check if output contains 429 error
cloud_transport_sharepoint_check_429() {
    local output="$1"
    [[ "$output" =~ 429|"Too Many Requests"|"rate limit"|"throttl" ]]
}

# Handle 429 with exponential backoff
cloud_transport_sharepoint_handle_429() {
    local current_bwlimit="$1"
    
    SHAREPOINT_429_RETRY_COUNT=$((SHAREPOINT_429_RETRY_COUNT + 1))
    
    # Calculate backoff time
    local backoff_time=$((SHAREPOINT_429_BACKOFF_BASE * (SHAREPOINT_429_BACKOFF_MULTIPLIER ** (SHAREPOINT_429_RETRY_COUNT - 1))))
    [[ $backoff_time -gt 600 ]] && backoff_time=600  # Cap at 10 minutes
    
    cloud_log_warn "429 Throttling: backing off for ${backoff_time}s (attempt $SHAREPOINT_429_RETRY_COUNT)"
    sleep "$backoff_time"
    
    # Reduce bandwidth by configured percentage
    local new_bwlimit
    if [[ "$current_bwlimit" =~ ^[0-9]+$ ]] && [[ "$current_bwlimit" -gt 0 ]]; then
        new_bwlimit=$((current_bwlimit * (100 - SHAREPOINT_429_BW_REDUCE_PERCENT) / 100))
        [[ $new_bwlimit -lt 1000000 ]] && new_bwlimit=1000000  # Min 1MB/s
        cloud_log_warn "429 Throttling: reduced bandwidth from $(tu_format_rate "$current_bwlimit") to $(tu_format_rate "$new_bwlimit")"
    else
        new_bwlimit="$current_bwlimit"
    fi
    
    SHAREPOINT_CURRENT_BWLIMIT="$new_bwlimit"
    echo "$new_bwlimit"
}

#=============================================================================
# RCLONE ARGUMENT BUILDER
#=============================================================================

cloud_transport_sharepoint_build_rclone_args() {
    local dest_num="$1"
    local bwlimit="${2:-}"
    
    local args="--stats 30s --stats-one-line"
    
    # Add transfers and checkers
    local transfers checkers
    eval "transfers=\${CLOUD_DEST_${dest_num}_TRANSFERS:-$CLOUD_REPLICATION_DEFAULT_TRANSFERS}"
    eval "checkers=\${CLOUD_DEST_${dest_num}_CHECKERS:-$CLOUD_REPLICATION_DEFAULT_CHECKERS}"
    
    [[ -n "$transfers" ]] && args+=" --transfers=$transfers"
    [[ -n "$checkers" ]] && args+=" --checkers=$checkers"
    
    # Add bandwidth limit
    if [[ -n "$bwlimit" ]] && [[ "$bwlimit" != "0" ]]; then
        args+=" --bwlimit=$bwlimit"
    fi
    
    # Add size limits
    local min_size max_size
    eval "min_size=\${CLOUD_DEST_${dest_num}_MIN_SIZE:-$CLOUD_REPLICATION_MIN_FILE_SIZE}"
    eval "max_size=\${CLOUD_DEST_${dest_num}_MAX_SIZE:-$CLOUD_REPLICATION_MAX_FILE_SIZE}"
    
    [[ -n "$min_size" ]] && args+=" --min-size=$min_size"
    [[ -n "$max_size" ]] && args+=" --max-size=$max_size"
    
    # Add retry settings
    local retries low_level_retries
    eval "retries=\${CLOUD_DEST_${dest_num}_RETRIES:-3}"
    eval "low_level_retries=\${CLOUD_DEST_${dest_num}_LOW_LEVEL_RETRIES:-10}"
    
    args+=" --retries=$retries --low-level-retries=$low_level_retries --retries-sleep=10s"
    
    # SharePoint-specific flags
    args+=" --tpslimit 4 --tpslimit-burst 8"
    
    echo "$args"
}

#=============================================================================
# BACKUP FILE VALIDATION (for accumulate-valid mode)
#=============================================================================

cloud_transport_sharepoint_get_valid_files() {
    local source_path="$1"
    local valid_files=()
    
    # Find archive directories and validate each archive file
    while IFS= read -r -d '' archive_dir; do
        local vm_name
        vm_name=$(basename "$(dirname "$archive_dir")")
        
        while IFS= read -r -d '' archive_file; do
            local archive_name
            archive_name=$(basename "$archive_file")
            
            # Check for size marker file
            local base_name="${archive_name%.zst}"
            base_name="${base_name%.gz}"
            base_name="${base_name%.lz4}"
            local size_file="$archive_dir/.${base_name}.size"
            
            if [[ -f "$size_file" ]]; then
                local expected_size actual_size
                expected_size=$(cat "$size_file" 2>/dev/null)
                actual_size=$(stat -c %s "$archive_file" 2>/dev/null)
                
                if [[ "$expected_size" == "$actual_size" ]]; then
                    # Make path relative to source_path
                    local rel_path="${archive_file#$source_path/}"
                    valid_files+=("$rel_path")
                    cloud_log_debug "Valid: $archive_name ($actual_size bytes)"
                else
                    cloud_log_warn "Size mismatch: $archive_name (expected=$expected_size, actual=$actual_size)"
                fi
            else
                cloud_log_debug "No size marker: $archive_name (assuming valid)"
                local rel_path="${archive_file#$source_path/}"
                valid_files+=("$rel_path")
            fi
        done < <(find "$archive_dir" -maxdepth 1 -type f \( -name "*.zst" -o -name "*.gz" -o -name "*.lz4" \) -print0)
    done < <(find "$source_path" -type d -name ".archives" -print0)
    
    printf '%s\n' "${valid_files[@]}"
}

#=============================================================================
# PROGRESS MONITOR FOR RCLONE
#=============================================================================

_cloud_monitor_rclone_progress() {
    local rclone_pid="$1"
    local log_file="$2"
    local _unused="$3"  # Was source_bytes - now parsed from rclone stats
    local interval="${4:-30}"
    
    local start_time
    start_time=$(date '+%s')
    
    while kill -0 "$rclone_pid" 2>/dev/null; do
        sleep "$interval"
        kill -0 "$rclone_pid" 2>/dev/null || break
        
        # Check for replication cancellation — kill rclone gracefully
        if type is_replication_cancelled &>/dev/null && is_replication_cancelled; then
            cloud_log_warn "Replication cancellation detected - terminating rclone (PID: $rclone_pid)"
            kill "$rclone_pid" 2>/dev/null
            sleep 2
            kill -0 "$rclone_pid" 2>/dev/null && kill -9 "$rclone_pid" 2>/dev/null
            cloud_log_warn "Rclone terminated by cancellation request"
            return 0  # Monitor's job is done
        fi
        
        # Parse BOTH current and total bytes from rclone's own stats
        # --stats-one-line format: "INFO  :   121.704 MiB / 437.786 MiB, 28%, 5.388 MiB/s, ETA 58s"
        # Regular format:          "Transferred:   121.704 MiB / 437.786 MiB, 28%, 5.388 MiB/s, ETA 58s"
        local current_bytes=0 total_bytes=0
        if [[ -f "$log_file" ]]; then
            local last_stats
            # Match both formats: either "INFO  :" or "Transferred:" prefix followed by size/size
            last_stats=$(grep -E "(INFO\s+:|Transferred:)\s+[0-9.]+ [KMGT]i?B / " "$log_file" 2>/dev/null | tail -1)
            if [[ -n "$last_stats" ]]; then
                # Extract "X MiB / Y MiB" pattern - works for both formats
                # Current: first size value (number + unit)
                local current_str total_str
                current_str=$(echo "$last_stats" | grep -oP ':\s+\K[0-9.]+\s*[KMGT]i?B(?=\s*/)' | head -1)
                # Total: value after / and before ,
                total_str=$(echo "$last_stats" | grep -oP '/\s*\K[0-9.]+\s*[KMGT]i?B(?=,)' | head -1)
                
                if [[ -n "$current_str" ]]; then
                    local clean_cur
                    clean_cur=$(echo "$current_str" | sed 's/iB//; s/ //g')
                    current_bytes=$(tu_parse_bytes "$clean_cur")
                fi
                if [[ -n "$total_str" ]]; then
                    local clean_tot
                    clean_tot=$(echo "$total_str" | sed 's/iB//; s/ //g')
                    total_bytes=$(tu_parse_bytes "$clean_tot")
                fi
            fi
        fi
        
        # Calculate and log progress (using rclone's actual transfer size)
        local elapsed=$(($(date '+%s') - start_time))
        if [[ $total_bytes -gt 0 ]]; then
            cloud_log_info "$(tu_format_progress "$current_bytes" "$total_bytes" "$elapsed")"
        else
            # Fallback: still determining transfer size
            cloud_log_info "Progress: Calculating transfer size... ($(tu_format_elapsed "$elapsed") elapsed)"
        fi
    done
}

#=============================================================================
# MAIN UPLOAD FUNCTION
#=============================================================================

cloud_transport_sharepoint_upload() {
    local dest_num="$1"
    local source_path="$2"
    
    # Load destination config
    local remote path name scope sync_mode verify bwlimit
    eval "remote=\${CLOUD_DEST_${dest_num}_REMOTE}"
    eval "path=\${CLOUD_DEST_${dest_num}_PATH}"
    eval "name=\${CLOUD_DEST_${dest_num}_NAME}"
    eval "scope=\${CLOUD_DEST_${dest_num}_SCOPE:-$CLOUD_REPLICATION_SCOPE}"
    eval "sync_mode=\${CLOUD_DEST_${dest_num}_SYNC_MODE:-$CLOUD_REPLICATION_SYNC_MODE}"
    eval "verify=\${CLOUD_DEST_${dest_num}_VERIFY:-$CLOUD_REPLICATION_POST_VERIFY}"
    eval "bwlimit=\${CLOUD_DEST_${dest_num}_BWLIMIT:-$CLOUD_REPLICATION_DEFAULT_BWLIMIT}"
    
    local original_bwlimit="$bwlimit"
    
    cloud_log_info "SharePoint upload starting"
    cloud_log_info "  Remote: ${remote}${path}"
    cloud_log_info "  Source: $source_path"
    cloud_log_info "  Scope: $scope | Mode: $sync_mode | Verify: $verify"
    
    # Reset throttle tracking
    SHAREPOINT_THROTTLE_COUNT=0
    SHAREPOINT_CURRENT_BWLIMIT=0
    SHAREPOINT_429_RETRY_COUNT=0
    
    # Build filters based on scope
    local filter_args=""
    local -a exclude_arr=()  # Use array for proper escaping
    
    case "$scope" in
        archives-only)
            filter_args="--include '**/.archives/**'"
            cloud_log_info "Scope: archives-only"
            ;;
        *)
            cloud_log_info "Scope: everything"
            ;;
    esac
    
    # Exclude files that are actively being written during backup
    # These cause "source file is being updated" errors
    exclude_arr+=(--exclude "_state/logs/vmbackup.log")
    exclude_arr+=(--exclude "_state/replication_logs/**")  # ALL replication logs (includes rclone's own log!)
    exclude_arr+=(--exclude "**/*.lock")
    
    # Build VM exclusions
    local vm_exclude="${CLOUD_REPLICATION_VM_EXCLUDE:-}"
    if [[ -n "$vm_exclude" ]]; then
        IFS=',' read -ra excluded_vms <<< "$vm_exclude"
        for vm in "${excluded_vms[@]}"; do
            vm=$(echo "$vm" | xargs)
            exclude_arr+=(--exclude "${vm}/**")
            cloud_log_info "Excluding VM: $vm"
        done
    fi
    
    # Handle accumulate-valid mode
    local include_filter_file=""
    if [[ "$sync_mode" == "accumulate-valid" ]]; then
        local files_to_upload
        files_to_upload=$(cloud_transport_sharepoint_get_valid_files "$source_path")
        
        if [[ -z "$files_to_upload" ]]; then
            cloud_log_warn "No valid backup files found for upload"
            CLOUD_REPLICATION_DEST_STATUS+=("$name|skipped|0|0|0|No valid files")
            return 0
        fi
        
        local file_count
        file_count=$(echo "$files_to_upload" | wc -l)
        cloud_log_info "Found $file_count valid backup files"
        
        include_filter_file="/tmp/cloud_valid_files_$$.txt"
        echo "$files_to_upload" > "$include_filter_file"
        filter_args="--files-from=$include_filter_file"
    fi
    
    # Determine rclone command
    local rclone_command="copy"
    if [[ "$sync_mode" == "mirror" ]]; then
        rclone_command="sync"
        cloud_log_info "Mirror mode: using rclone sync"
    fi
    
    # Setup log file
    local rclone_log_file
    rclone_log_file=$(tu_get_replication_log_path "cloud" "$name")
    cloud_log_info "Rclone log: $rclone_log_file"
    
    # Note: rclone log is already excluded by _state/replication_logs/** pattern
    
    # Calculate source size
    local source_bytes
    source_bytes=$(tu_get_dir_size "$source_path")
    
    local start_time output_file="/tmp/cloud_replication_$$.log"
    start_time=$(date '+%s')
    
    #=========================================================================
    # UPLOAD WITH 429 RETRY LOOP
    #=========================================================================
    local retry_attempt=0 result=1 output=""
    
    while [[ $retry_attempt -lt $SHAREPOINT_429_MAX_RETRIES ]]; do
        [[ $retry_attempt -gt 0 ]] && cloud_log_info "429 Retry attempt $((retry_attempt + 1))/$SHAREPOINT_429_MAX_RETRIES"
        
        # Build rclone command array
        local rclone_args
        rclone_args=$(cloud_transport_sharepoint_build_rclone_args "$dest_num" "$bwlimit")
        
        local -a rclone_cmd_arr=(rclone "$rclone_command" "--log-file=$rclone_log_file" "--log-level=INFO")
        read -ra args_arr <<< "$rclone_args"
        rclone_cmd_arr+=("${args_arr[@]}")
        [[ -n "$filter_args" ]] && { read -ra filter_arr <<< "$filter_args"; rclone_cmd_arr+=("${filter_arr[@]}"); }
        # Add exclude array (properly handles spaces/special chars)
        [[ ${#exclude_arr[@]} -gt 0 ]] && rclone_cmd_arr+=("${exclude_arr[@]}")
        rclone_cmd_arr+=("$source_path" "${remote}${path}/")
        
        cloud_log_info "[RCLONE-CMD] ${rclone_cmd_arr[*]}"
        
        # Run rclone in background with progress monitoring
        rm -f "$output_file"
        "${rclone_cmd_arr[@]}" > "$output_file" 2>&1 &
        local rclone_pid=$!
        cloud_log_info "Rclone started (PID: $rclone_pid)"
        
        # Monitor progress
        _cloud_monitor_rclone_progress "$rclone_pid" "$rclone_log_file" "$source_bytes" 30 &
        local monitor_pid=$!
        
        # Wait for completion
        wait "$rclone_pid"
        result=$?
        kill "$monitor_pid" 2>/dev/null
        wait "$monitor_pid" 2>/dev/null
        
        output=$(cat "$output_file" 2>/dev/null)
        
        # Log exit status
        if [[ $result -eq 0 ]]; then
            cloud_log_debug "[RCLONE-EXIT] code=0 (success)"
        else
            cloud_log_warn "[RCLONE-EXIT] code=$result ($(tu_rclone_exit_message "$result"))"
        fi
        
        # Check for cancellation (rclone killed by monitor or externally)
        if [[ $result -ne 0 ]] && type is_replication_cancelled &>/dev/null && is_replication_cancelled; then
            cloud_log_warn "Rclone terminated by replication cancellation"
            local cancel_end_time cancel_duration
            cancel_end_time=$(date '+%s')
            cancel_duration=$((cancel_end_time - start_time))
            
            rm -f "$output_file"
            [[ -n "$include_filter_file" ]] && rm -f "$include_filter_file"
            
            CLOUD_REPLICATION_DEST_STATUS+=("$name|cancelled|0|0|$cancel_duration|Replication cancelled by operator")
            return 1
        fi
        
        # Check for 429 (only on failure)
        if [[ $result -ne 0 ]] && cloud_transport_sharepoint_check_429 "$output"; then
            cloud_log_warn "Received 429 (Too Many Requests)"
            bwlimit=$(cloud_transport_sharepoint_handle_429 "$bwlimit")
            retry_attempt=$((retry_attempt + 1))
            SHAREPOINT_THROTTLE_COUNT=$((SHAREPOINT_THROTTLE_COUNT + 1))
            continue
        fi
        
        break
    done
    
    local end_time duration
    end_time=$(date '+%s')
    duration=$((end_time - start_time))
    
    # Parse transfer statistics from rclone log
    local transferred_bytes=0 transferred_files=0
    if [[ -f "$rclone_log_file" ]]; then
        # Count completed transfers
        transferred_files=$(grep -c "Copied (new\|Copied (replaced" "$rclone_log_file" 2>/dev/null || echo 0)
        
        # Get transferred bytes from the LAST 100% progress line
        # rclone progress lines are cumulative, so each line shows the running total.
        # We must NOT sum multiple 100% lines — just take the final one.
        local last_100_line
        last_100_line=$(grep -E "[0-9.]+ [GMKT]iB / [0-9.]+ [GMKT]iB, 100%" "$rclone_log_file" 2>/dev/null | tail -1)
        if [[ -n "$last_100_line" ]]; then
            local line_bytes
            line_bytes=$(echo "$last_100_line" | grep -oP '[0-9.]+\s*[GMKT]iB\s*/' | head -1 | sed 's|/||; s|iB||; s| ||g')
            [[ -n "$line_bytes" ]] && transferred_bytes=$(tu_parse_bytes "$line_bytes")
        fi
    fi
    
    # Cleanup temp files
    rm -f "$output_file"
    [[ -n "$include_filter_file" ]] && rm -f "$include_filter_file"
    
    # Security: ensure rclone log file is accessible to backup group
    if [[ -f "$rclone_log_file" ]]; then
        command -v set_backup_permissions &>/dev/null && set_backup_permissions "$rclone_log_file"
    fi
    
    if [[ $result -eq 0 ]]; then
        cloud_log_info "SharePoint upload completed successfully"
        cloud_log_info "  Duration: $(tu_format_elapsed "$duration")"
        cloud_log_info "  Transferred: $(tu_format_bytes "$transferred_bytes") ($transferred_files files)"
        cloud_log_info "  Throttle events: $SHAREPOINT_THROTTLE_COUNT"
        [[ "$bwlimit" != "$original_bwlimit" ]] && \
            cloud_log_info "  Bandwidth adjusted: $original_bwlimit -> $bwlimit (throttling)"
        
        # Set cloud transport metrics contract globals
        CLOUD_TRANSPORT_THROTTLE_COUNT="$SHAREPOINT_THROTTLE_COUNT"
        CLOUD_TRANSPORT_BWLIMIT_FINAL="$bwlimit"
        CLOUD_TRANSPORT_DEST_AVAIL_BYTES=0
        CLOUD_TRANSPORT_DEST_TOTAL_BYTES=0
        CLOUD_TRANSPORT_DEST_SPACE_KNOWN=0
        
        # Post-upload verification
        _cloud_verify_upload "$dest_num" "$source_path" "$verify" "$sync_mode" "$exclude_args"
        
        # Update globals
        CLOUD_REPLICATION_TOTAL_BYTES=$((CLOUD_REPLICATION_TOTAL_BYTES + transferred_bytes))
        CLOUD_REPLICATION_TOTAL_FILES=$((CLOUD_REPLICATION_TOTAL_FILES + transferred_files))
        CLOUD_REPLICATION_DEST_STATUS+=("$name|success|$transferred_bytes|$transferred_files|$duration|")
        
        return 0
    else
        cloud_log_error "SharePoint upload failed after $(tu_format_elapsed "$duration")"
        _cloud_handle_auth_error "$output"
        
        # Set cloud transport metrics contract globals (even on failure)
        CLOUD_TRANSPORT_THROTTLE_COUNT="$SHAREPOINT_THROTTLE_COUNT"
        CLOUD_TRANSPORT_BWLIMIT_FINAL="$bwlimit"
        CLOUD_TRANSPORT_DEST_AVAIL_BYTES=0
        CLOUD_TRANSPORT_DEST_TOTAL_BYTES=0
        CLOUD_TRANSPORT_DEST_SPACE_KNOWN=0
        
        CLOUD_REPLICATION_DEST_STATUS+=("$name|failed|0|0|$duration|Upload failed")
        return 1
    fi
}

#=============================================================================
# POST-UPLOAD VERIFICATION
#=============================================================================

_cloud_verify_upload() {
    local dest_num="$1"
    local source_path="$2"
    local verify="$3"
    local sync_mode="$4"
    local exclude_args="$5"
    
    [[ "$verify" != "checksum" && "$verify" != "size" ]] && return 0
    
    local remote path
    eval "remote=\${CLOUD_DEST_${dest_num}_REMOTE}"
    eval "path=\${CLOUD_DEST_${dest_num}_PATH}"
    
    local verify_flag="--size-only"
    [[ "$verify" == "checksum" ]] && verify_flag="--checksum"
    
    local one_way_flag=""
    [[ "$sync_mode" == "mirror" ]] && one_way_flag="--one-way"
    
    cloud_log_info "[VERIFY] Starting $verify verification"
    
    # Build verify command with exclusions (use array passed from upload function scope)
    local -a verify_cmd=(rclone check $verify_flag $one_way_flag --stats 5m --stats-one-line)
    [[ ${#exclude_arr[@]} -gt 0 ]] && verify_cmd+=("${exclude_arr[@]}")
    verify_cmd+=("$source_path" "${remote}${path}/")
    
    local verify_start
    verify_start=$(date '+%s')
    
    local verify_output
    verify_output=$("${verify_cmd[@]}" 2>&1)
    local verify_result=$?
    
    local verify_duration=$(($(date '+%s') - verify_start))
    
    if echo "$verify_output" | grep -q "0 differences"; then
        cloud_log_info "[VERIFY] ✓ Passed in $(tu_format_elapsed "$verify_duration")"
    else
        cloud_log_warn "[VERIFY] Found differences after $(tu_format_elapsed "$verify_duration")"
    fi
}

#=============================================================================
# ERROR HANDLING
#=============================================================================

_cloud_handle_auth_error() {
    local output="$1"
    
    if echo "$output" | grep -qiE "401|403|unauthorized|forbidden|token"; then
        cloud_log_error ""
        cloud_log_error "═══════════════════════════════════════════════════════════════"
        cloud_log_error "AUTHENTICATION ERROR - Re-authentication required"
        cloud_log_error "═══════════════════════════════════════════════════════════════"
        cloud_log_error "Run: sudo $(dirname "${BASH_SOURCE[0]}")/sharepoint_auth.sh"
        cloud_log_error "═══════════════════════════════════════════════════════════════"
    fi
}

#=============================================================================
# QUOTA CHECK
#=============================================================================

cloud_transport_sharepoint_check_quota() {
    local dest_num="$1"
    
    local remote name
    eval "remote=\${CLOUD_DEST_${dest_num}_REMOTE}"
    eval "name=\${CLOUD_DEST_${dest_num}_NAME}"
    
    cloud_log_debug "Checking SharePoint quota for $name"
    
    local quota_info
    quota_info=$(rclone about "$remote" 2>/dev/null)
    
    if [[ -z "$quota_info" ]]; then
        cloud_log_warn "Could not retrieve quota for $name"
        return 0
    fi
    
    local total used free
    total=$(echo "$quota_info" | awk '/Total:/{print $2}')
    used=$(echo "$quota_info" | awk '/Used:/{print $2}')
    free=$(echo "$quota_info" | awk '/Free:/{print $2}')
    
    cloud_log_info "Quota for $name: Total=$total Used=$used Free=$free"
    return 0
}

#=============================================================================
# CONNECTION TEST
#=============================================================================

cloud_transport_sharepoint_test_connection() {
    local dest_num="$1"
    
    local remote name
    eval "remote=\${CLOUD_DEST_${dest_num}_REMOTE}"
    eval "name=\${CLOUD_DEST_${dest_num}_NAME}"
    
    cloud_log_info "Testing SharePoint connection for $name..."
    
    if rclone lsd "$remote" &>/dev/null; then
        cloud_log_info "Connection test: SUCCESS"
        return 0
    else
        cloud_log_error "Connection test: FAILED"
        cloud_log_error "Run: sudo $(dirname "${BASH_SOURCE[0]}")/sharepoint_auth.sh"
        return 1
    fi
}

#=============================================================================
# MODULE LOAD
#=============================================================================

cloud_log_debug "SharePoint transport v${CLOUD_TRANSPORT_SHAREPOINT_VERSION} loaded (delegated auth)"
