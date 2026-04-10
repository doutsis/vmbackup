#!/bin/bash
#################################################################################
# Email Report Module for vmbackup.sh
#
# Sends formatted email reports after VM backup operations complete.
# Reads data from SQLite database (session-scoped queries) and formats
# human-readable plaintext reports.
#
# Dependencies:
#   - msmtp configured at /etc/msmtprc
#   - lib/sqlite_module.sh (DB data source — session-scoped queries)
#   - config/<instance>/email.conf (per-instance configuration)
#
# Data sources:
#   - VM backup details:   SQLite vm_backups table (session-scoped)
#   - Replication details:  SQLite replication_runs table (session-scoped)
#   - Chain health:        SQLite chain_health table (via sqlite_chain_health_summary)
#   - Storage info:        Filesystem (df/du)
#   - Log attachment:      vmbackup.log (session-filtered)
#
# Usage:
#   source email_report_module.sh
#   load_email_config              # Load per-instance config
#   send_backup_report "$start" "$end"
#
# Created: 2026-01-20
# Updated: 2026-02-12 (v2.0 - migrated from CSV to SQLite DB)
#################################################################################

#################################################################################
# MODULE STATE
#################################################################################

# Module identification
EMAIL_MODULE_VERSION="2.0"
EMAIL_MODULE_LOADED=0
EMAIL_MODULE_AVAILABLE=0

#################################################################################
# CONFIGURATION LOADING
#################################################################################

#-------------------------------------------------------------------------------
# load_email_config - Load email configuration from instance config directory
#
# Loads config/<instance>/email.conf and validates settings.
# Sets EMAIL_MODULE_AVAILABLE=1 if successful.
#
# Returns:
#   0 - Config loaded successfully
#   1 - Config missing or invalid (email disabled)
#-------------------------------------------------------------------------------
load_email_config() {
    local script_dir="${SCRIPT_DIR:-$(dirname "$(readlink -f "$0")")}"
    local instance="${CONFIG_INSTANCE:-default}"
    local config_file="$script_dir/config/${instance}/email.conf"
    
    # Check config file exists - instance config is REQUIRED (no fallback)
    if [[ ! -f "$config_file" ]]; then
        echo "WARNING: NO CONFIG FILES FOR $instance - missing $config_file" >&2
        echo "WARNING: Email reports disabled" >&2
        EMAIL_MODULE_AVAILABLE=0
        EMAIL_ENABLED="no"
        return 1
    fi
    
    # Source configuration
    if ! source "$config_file" 2>/dev/null; then
        echo "ERROR: Failed to load email config: $config_file" >&2
        EMAIL_MODULE_AVAILABLE=0
        EMAIL_ENABLED="no"
        return 1
    fi
    
    # Check if email is enabled
    if [[ "${EMAIL_ENABLED:-no}" != "yes" ]]; then
        EMAIL_MODULE_AVAILABLE=0
        return 1
    fi
    
    # Validate required settings
    if [[ -z "$EMAIL_RECIPIENT" ]]; then
        echo "ERROR: EMAIL_RECIPIENT not set in $config_file" >&2
        EMAIL_MODULE_AVAILABLE=0
        return 1
    fi
    
    if [[ -z "$EMAIL_SENDER" ]]; then
        echo "ERROR: EMAIL_SENDER not set in $config_file" >&2
        EMAIL_MODULE_AVAILABLE=0
        return 1
    fi
    
    # Apply defaults for optional settings
    EMAIL_HOSTNAME="${EMAIL_HOSTNAME:-$(hostname)}"
    EMAIL_SUBJECT_PREFIX="${EMAIL_SUBJECT_PREFIX:-[VM Backup]}"
    EMAIL_ON_SUCCESS="${EMAIL_ON_SUCCESS:-yes}"
    EMAIL_ON_FAILURE="${EMAIL_ON_FAILURE:-yes}"
    EMAIL_INCLUDE_REPLICATION="${EMAIL_INCLUDE_REPLICATION:-yes}"
    EMAIL_INCLUDE_DISK_SPACE="${EMAIL_INCLUDE_DISK_SPACE:-yes}"
    
    # Check msmtp is available
    if ! command -v msmtp >/dev/null 2>&1; then
        echo "WARNING: msmtp not found - email sending will fail" >&2
        echo "WARNING: Install with: apt install msmtp msmtp-mta" >&2
        # Don't disable - config is valid, just msmtp missing
    fi
    
    EMAIL_MODULE_AVAILABLE=1
    EMAIL_MODULE_LOADED=1
    return 0
}

# Paths (inherit from vmbackup.sh or use defaults)
# These are set after config load based on BACKUP_PATH
_init_email_paths() {
    EMAIL_BACKUP_PATH="${BACKUP_PATH:-/mnt/backup/vms/}"
    EMAIL_STATE_DIR="${EMAIL_BACKUP_PATH}_state"
    EMAIL_LOG_DIR="${EMAIL_STATE_DIR}/logs"
}

#################################################################################
# HELPER FUNCTIONS
#################################################################################

# Format bytes to human readable (GiB/MiB/KiB)
format_bytes() {
    local bytes="$1"
    
    # Handle empty or non-numeric
    if [[ -z "$bytes" ]] || ! [[ "$bytes" =~ ^[0-9]+$ ]]; then
        echo "0 B"
        return
    fi
    
    if [[ "$bytes" -ge 1099511627776 ]]; then
        # TiB
        echo "$(awk "BEGIN {printf \"%.1f TiB\", $bytes/1099511627776}")"
    elif [[ "$bytes" -ge 1073741824 ]]; then
        # GiB
        echo "$(awk "BEGIN {printf \"%.1f GiB\", $bytes/1073741824}")"
    elif [[ "$bytes" -ge 1048576 ]]; then
        # MiB
        echo "$(awk "BEGIN {printf \"%.1f MiB\", $bytes/1048576}")"
    elif [[ "$bytes" -ge 1024 ]]; then
        # KiB
        echo "$(awk "BEGIN {printf \"%.1f KiB\", $bytes/1024}")"
    else
        echo "$bytes B"
    fi
}

# Format seconds to Xm Ys
format_duration() {
    local seconds="$1"
    
    # Handle empty or non-numeric
    if [[ -z "$seconds" ]] || ! [[ "$seconds" =~ ^[0-9]+$ ]]; then
        seconds=0
    fi
    
    local minutes=$((seconds / 60))
    local secs=$((seconds % 60))
    printf "%dm %02ds" "$minutes" "$secs"
}

# Get consistency description from backup_method and qemu_agent
get_consistency_text() {
    local backup_method="$1"
    local qemu_agent="$2"
    
    case "$backup_method" in
        agent)
            echo "Agent"
            ;;
        paused)
            echo "No Agent (paused)"
            ;;
        offline)
            echo "Offline"
            ;;
        *)
            if [[ "$qemu_agent" == "true" ]]; then
                echo "Agent"
            else
                echo "Unknown"
            fi
            ;;
    esac
}

# Get backup type display text
get_backup_type_display() {
    local type="$1"
    
    case "$type" in
        full)
            echo "Full"
            ;;
        auto|inc|incremental)
            echo "Incremental"
            ;;
        *)
            echo "$type"
            ;;
    esac
}

# Get available space on backup filesystem
get_available_space() {
    local path="$1"
    local available_bytes
    
    # Get available space in 1K blocks, convert to bytes
    available_bytes=$(df --output=avail -B1 "$path" 2>/dev/null | tail -1 | tr -d ' ')
    
    if [[ -n "$available_bytes" ]] && [[ "$available_bytes" =~ ^[0-9]+$ ]]; then
        echo "$available_bytes"
    else
        echo "0"
    fi
}

# Get total size of all VM backups
get_total_backup_size() {
    local path="$1"
    local total_bytes=0
    
    # Sum all total_dir_bytes from DB, or calculate from filesystem
    # Using du for accuracy (includes all months)
    total_bytes=$(du -sb "$path" 2>/dev/null | cut -f1)
    
    if [[ -n "$total_bytes" ]] && [[ "$total_bytes" =~ ^[0-9]+$ ]]; then
        echo "$total_bytes"
    else
        echo "0"
    fi
}

# Get chain health summary for email report
# Uses SQLite chain_health table directly with pipe-separated output
get_chain_health_summary() {
    local backup_path="${1:-$EMAIL_BACKUP_PATH}"
    local output=""
    local vm_count=0
    local broken_count=0
    
    # Query chain_health directly with pipe separator (not column format)
    if [[ "${SQLITE_MODULE_AVAILABLE:-0}" -eq 1 ]] && [[ -n "${SQLITE_DB_PATH:-}" ]]; then
        local db_output
        db_output=$(sqlite3 -separator '|' "$SQLITE_DB_PATH" "
            SELECT vm_name,
                   COUNT(*) as total_chains,
                   SUM(CASE WHEN chain_status='active' THEN 1 ELSE 0 END) as active,
                   SUM(CASE WHEN chain_status='archived' THEN 1 ELSE 0 END) as archived,
                   SUM(CASE WHEN chain_status='broken' THEN 1 ELSE 0 END) as broken,
                   SUM(restorable_count) as restore_points
            FROM chain_health
            WHERE chain_status NOT IN ('deleted','purged')
            GROUP BY vm_name
            ORDER BY vm_name;" 2>/dev/null)
        
        if [[ -n "$db_output" ]]; then
            while IFS='|' read -r vm total_chains active archived broken restore_points; do
                [[ -z "$vm" ]] && continue
                ((vm_count++))
                
                local status_icon="✓"
                if [[ "$broken" -gt 0 ]]; then
                    status_icon="✗"
                    ((broken_count++))
                fi
                
                output+="$status_icon $vm: $active active, $archived archived, $restore_points restore points
"
            done <<< "$db_output"
        fi
    fi
    
    # Fallback: scan filesystem if no SQLite data
    if [[ $vm_count -eq 0 ]] && declare -f scan_all_chains >/dev/null 2>&1 && [[ -d "$backup_path" ]]; then
        while IFS= read -r vm_dir; do
            local vm_name
            vm_name=$(basename "$vm_dir")
            [[ "$vm_name" == "_state" || "$vm_name" == "_archive" || "$vm_name" == "__HOST_CONFIG__" ]] && continue
            
            local scan_result
            scan_result=$(scan_all_chains "$vm_name" "$backup_path" 2>/dev/null | head -1)
            if [[ -n "$scan_result" ]]; then
                IFS=$'\t' read -r period valid total restorable broken reason <<< "$scan_result"
                ((vm_count++))
                
                local status_icon="✓"
                local status_text="healthy"
                if [[ "$valid" == "false" ]]; then
                    status_icon="✗"
                    status_text="BROKEN at $broken"
                    ((broken_count++))
                fi
                
                output+="$status_icon $vm_name ($period): $restorable/$total checkpoints, $status_text
"
            fi
        done < <(find "$backup_path" -mindepth 1 -maxdepth 1 -type d 2>/dev/null | sort | head -20)
    fi
    
    if [[ $vm_count -eq 0 ]]; then
        echo "No chain health data available"
    elif [[ $broken_count -gt 0 ]]; then
        echo "⚠ $broken_count/$vm_count chains have issues
$output"
    else
        echo "✓ All $vm_count chains healthy
$output"
    fi
}

# Extract current session's log portion (not entire day)
# Parameters:
#   $1 = log file path
#   $2 = date string (YYYY-MM-DD)
#   $3 = session start time (HH:MM:SS) - optional, filters to current session only
get_todays_log() {
    local log_file="$1"
    local date_str="$2"
    local start_time="${3:-}"
    local temp_log
    
    temp_log=$(mktemp)
    
    if [[ -f "$log_file" ]]; then
        if [[ -n "$start_time" ]]; then
            # PREFERRED: Extract only current session (from start_time onwards)
            # Log format: [2026-01-25 16:16:45] [INFO] ...
            # If start_time contains a date prefix (YYYY-MM-DD HH:MM:SS), extract
            # just the time portion for comparison against log line timestamps.
            local start_time_only="$start_time"
            if [[ "$start_time" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}\ (.+)$ ]]; then
                start_time_only="${BASH_REMATCH[1]}"
            fi
            # Use awk to filter lines >= start_time on matching date
            awk -v date="$date_str" -v start="$start_time_only" '
                /^\[?[0-9]{4}-[0-9]{2}-[0-9]{2}/ {
                    # Extract date and time from first two fields
                    gsub(/[\[\]]/, "", $1)
                    gsub(/[\[\]]/, "", $2)
                    line_date = $1
                    line_time = $2
                    
                    # Only include if date matches AND time >= start_time
                    if (line_date == date && line_time >= start) {
                        print
                    }
                }
            ' "$log_file" > "$temp_log" 2>/dev/null
        else
            # Fallback: Extract all lines from today
            grep "^$date_str\|^\[$date_str" "$log_file" > "$temp_log" 2>/dev/null
        fi
        
        # If no date-prefixed lines, try to get recent portion (last run)
        if [[ ! -s "$temp_log" ]]; then
            # Get everything after last "Starting backup run" or similar marker
            tac "$log_file" | sed '/Starting backup run\|BACKUP RUN STARTED/q' | tac > "$temp_log"
        fi
        
        # If still empty, just use last 500 lines
        if [[ ! -s "$temp_log" ]]; then
            tail -500 "$log_file" > "$temp_log"
        fi
    fi
    
    echo "$temp_log"
}

#################################################################################
# EMAIL FORMATTING
#################################################################################

# Build the email body from SQLite database
# All data is scoped to the current session via SQLITE_CURRENT_SESSION_ID
build_email_body() {
    local start_time="$1"
    local end_time="$2"
    local date_str="$3"
    
    local body=""
    local success_count=0
    local failed_count=0
    local skipped_count=0
    local excluded_count=0
    local total_written=0
    
    # Separate lists for INCLUDED (success/failed/skipped) and EXCLUDED VMs
    local included_sections=""
    local excluded_list=""
    
    # Calculate duration
    local start_epoch end_epoch duration_seconds duration_text
    start_epoch=$(date -d "$start_time" +%s 2>/dev/null || echo "0")
    end_epoch=$(date -d "$end_time" +%s 2>/dev/null || date +%s)
    duration_seconds=$((end_epoch - start_epoch))
    duration_text=$(format_duration "$duration_seconds")
    
    # Replicate-only session — build simplified body without VM details
    local _session_status=""
    local _session_type=""
    if declare -f sqlite_query_session_summary >/dev/null 2>&1; then
        local _summary_row
        _summary_row=$(sqlite_query_session_summary 2>/dev/null)
        if [[ -n "$_summary_row" ]]; then
            # Row format: total|success|failed|skipped|excluded|bytes|status|session_type
            _session_type="${_summary_row##*|}"
            local _sr_rest="${_summary_row%|*}"
            _session_status="${_sr_rest##*|}"
        fi
    fi
    if [[ "$_session_type" == "replicate_only" ]]; then
        # Build replication sections (reuse standard logic)
        local local_replication_section="" cloud_replication_section=""
        if [[ "$db_available" -eq 0 ]] && declare -f sqlite_query_session_vm_backups >/dev/null 2>&1; then
            db_available=1
        fi
        if declare -f sqlite_query_session_replication >/dev/null 2>&1; then
            local local_details="" cloud_details=""
            while IFS='|' read -r ep_name ep_type transport ep_status ep_bytes \
                               ep_files ep_dur ep_dest ep_error; do
                [[ -z "$ep_name" ]] && continue
                local si="✓"; case "$ep_status" in failed) si="✗" ;; disabled) si="○" ;; esac
                local bf=$(format_bytes "$ep_bytes") df=$(format_duration "$ep_dur")
                local line="$si $ep_name ($transport): "
                if [[ "$ep_status" == "success" ]]; then line+="$bf in $df"
                elif [[ "$ep_status" == "disabled" ]]; then line+="disabled"
                else line+="$ep_status"; [[ -n "$ep_error" ]] && line+=" - $ep_error"; fi
                if [[ "$ep_type" == "local" ]]; then local_details+="$line"$'\n'
                else cloud_details+="$line"$'\n'; fi
            done < <(sqlite_query_session_replication 2>/dev/null)
            [[ -n "$local_details" ]] && local_replication_section="
--- LOCAL REPLICATION ---
${local_details}"
            [[ -n "$cloud_details" ]] && cloud_replication_section="
--- CLOUD REPLICATION ---
${cloud_details}"
        fi
        [[ -z "$local_replication_section" ]] && local_replication_section="
--- LOCAL REPLICATION ---
Not configured"
        [[ -z "$cloud_replication_section" ]] && cloud_replication_section="
--- CLOUD REPLICATION ---
Not configured"

        local total_backup_bytes=$(get_total_backup_size "$EMAIL_BACKUP_PATH")
        local available_bytes=$(get_available_space "$EMAIL_BACKUP_PATH")
        local total_disk_bytes; total_disk_bytes=$(df -B1 --output=size "$EMAIL_BACKUP_PATH" 2>/dev/null | tail -1 | tr -d ' ')
        local total_backup_fmt=$(format_bytes "$total_backup_bytes")
        local available_fmt=$(format_bytes "$available_bytes")
        local available_pct=""
        if [[ -n "$total_disk_bytes" ]] && [[ "$total_disk_bytes" =~ ^[0-9]+$ ]] && [[ "$total_disk_bytes" -gt 0 ]]; then
            available_pct=" ($((available_bytes * 100 / total_disk_bytes))% free)"
        fi

        body="Replication-Only Report
--------------------
Host: $EMAIL_HOSTNAME
Date: $date_str
Started: $start_time
Finished: $end_time
Duration: $duration_text

No backups were performed — this was a replication-only session.

--- STORAGE ($EMAIL_BACKUP_PATH) ---
Used: $total_backup_fmt
Free: $available_fmt$available_pct
$local_replication_section$cloud_replication_section

Log: vmbackup-$date_str.txt"
        echo "$body"
        return
    fi

    # Query VM backup records from database (session-scoped)
    local db_available=0
    if declare -f sqlite_query_session_vm_backups >/dev/null 2>&1; then
        db_available=1
    fi
    
    if [[ "$db_available" -eq 1 ]]; then
        while IFS='|' read -r vm_name vm_status os_type backup_type backup_method \
                           status bytes_written_val chain_size total_dir \
                           restore_points restore_points_before duration_secs \
                           error_code error_message event_type event_detail \
                           qemu_agent vm_paused chain_archived rotation_policy; do
            # Skip empty lines
            [[ -z "$vm_name" ]] && continue
            
            # Count statuses
            case "$status" in
                success) ((success_count++)) ;;
                error|failed) ((failed_count++)) ;;
                skipped) ((skipped_count++)) ;;
                excluded) ((excluded_count++)) ;;
            esac
            
            # EXCLUDED VMs - collect names for inline list
            if [[ "$status" == "excluded" ]]; then
                excluded_list+="$vm_name"$'\n'
                continue
            fi
            
            # Sum bytes written (only for included VMs)
            if [[ "$bytes_written_val" =~ ^[0-9]+$ ]]; then
                total_written=$((total_written + bytes_written_val))
            fi
            
            # Build compact VM section (2-3 lines per VM)
            local status_icon
            case "$status" in
                success) status_icon="✓ SUCCESS" ;;
                error|failed) status_icon="✗ FAILED" ;;
                skipped) status_icon="◇ SKIPPED" ;;
                *) status_icon="? $status" ;;
            esac
            
            local consistency=$(get_consistency_text "$backup_method" "$qemu_agent")
            local duration_fmt=$(format_duration "$duration_secs")
            local this_backup_fmt=$(format_bytes "$bytes_written_val")
            local chain_size_fmt=$(format_bytes "$chain_size")
            local total_dir_fmt=$(format_bytes "$total_dir")
            local backup_type_display=$(get_backup_type_display "$backup_type")
            
            included_sections+="
--- $vm_name ---
  $status_icon, $vm_status, $backup_type_display
  $consistency, $duration_fmt
  Written: $this_backup_fmt
  Chain: $chain_size_fmt, Total: $total_dir_fmt
  Restore Points: $restore_points"
            
            # Add detail/error/reason on third line if present
            if [[ "$status" == "success" ]] && [[ -n "$event_detail" ]]; then
                included_sections+="
  $event_detail"
            elif [[ "$status" == "error" ]] || [[ "$status" == "failed" ]]; then
                included_sections+="
  Error: $error_code - $event_detail"
            elif [[ "$status" == "skipped" ]] && [[ -n "$event_detail" ]]; then
                included_sections+="
  Reason: $event_detail"
            fi
            
        done < <(sqlite_query_session_vm_backups 2>/dev/null)
    fi
    
    # Get storage summary with percentage free
    local total_backup_bytes=$(get_total_backup_size "$EMAIL_BACKUP_PATH")
    local available_bytes=$(get_available_space "$EMAIL_BACKUP_PATH")
    local total_disk_bytes
    total_disk_bytes=$(df -B1 --output=size "$EMAIL_BACKUP_PATH" 2>/dev/null | tail -1 | tr -d ' ')
    local total_backup_fmt=$(format_bytes "$total_backup_bytes")
    local available_fmt=$(format_bytes "$available_bytes")
    local total_written_fmt=$(format_bytes "$total_written")
    
    # Calculate percentage free
    local available_pct=""
    if [[ -n "$total_disk_bytes" ]] && [[ "$total_disk_bytes" =~ ^[0-9]+$ ]] && [[ "$total_disk_bytes" -gt 0 ]]; then
        available_pct=" ($((available_bytes * 100 / total_disk_bytes))% free)"
    fi
    
    # Build replication sections from database (session-scoped)
    local local_replication_section=""
    local cloud_replication_section=""
    local local_summary_line=""
    local cloud_summary_line=""
    
    if [[ "$db_available" -eq 1 ]] && declare -f sqlite_query_session_replication >/dev/null 2>&1; then
        local local_details=""
        local cloud_details=""
        local local_total_bytes=0 local_success=0 local_total=0 local_duration=0
        local cloud_total_bytes=0 cloud_files=0 cloud_success=0 cloud_total=0 cloud_duration=0
        
        while IFS='|' read -r ep_name ep_type transport ep_status ep_bytes \
                           ep_files ep_dur ep_dest ep_error; do
            [[ -z "$ep_name" ]] && continue
            
            local status_icon="✓"
            case "$ep_status" in
                success)  status_icon="✓" ;;
                failed)   status_icon="✗" ;;
                disabled) status_icon="○" ;;
                skipped)  status_icon="◇" ;;
                *)        status_icon="?" ;;
            esac
            
            local bytes_fmt=$(format_bytes "$ep_bytes")
            local dur_fmt=$(format_duration "$ep_dur")
            
            if [[ "$ep_type" == "local" ]]; then
                ((local_total++))
                [[ "$ep_status" == "success" ]] && ((local_success++))
                local_total_bytes=$((local_total_bytes + ep_bytes))
                local_duration=$((local_duration + ep_dur))
                
                local_details+="$status_icon $ep_name ($transport): "
                if [[ "$ep_status" == "success" ]]; then
                    local_details+="$bytes_fmt in $dur_fmt"
                elif [[ "$ep_status" == "disabled" ]]; then
                    local_details+="disabled"
                else
                    local_details+="$ep_status"
                    [[ -n "$ep_error" ]] && local_details+=" - $ep_error"
                fi
                local_details+=$'\n'
            elif [[ "$ep_type" == "cloud" ]]; then
                ((cloud_total++))
                [[ "$ep_status" == "success" ]] && ((cloud_success++))
                cloud_total_bytes=$((cloud_total_bytes + ep_bytes))
                cloud_files=$((cloud_files + ep_files))
                cloud_duration=$((cloud_duration + ep_dur))
                
                cloud_details+="$status_icon $ep_name ($transport): "
                if [[ "$ep_status" == "success" ]]; then
                    cloud_details+="$bytes_fmt ($ep_files files) in $dur_fmt"
                elif [[ "$ep_status" == "disabled" ]]; then
                    cloud_details+="disabled"
                else
                    cloud_details+="$ep_status"
                    [[ -n "$ep_error" ]] && cloud_details+=" - $ep_error"
                fi
                cloud_details+=$'\n'
            fi
        done < <(sqlite_query_session_replication 2>/dev/null)
        
        # Build local replication section
        if [[ $local_total -gt 0 ]]; then
            local local_total_fmt=$(format_bytes "$local_total_bytes")
            local local_dur_fmt=$(format_duration "$local_duration")
            local_summary_line="$local_success/$local_total destinations, $local_total_fmt in $local_dur_fmt"
            local_replication_section="
--- LOCAL REPLICATION ---
${local_details}"
        fi
        
        # Build cloud replication section
        if [[ $cloud_total -gt 0 ]]; then
            local cloud_total_fmt=$(format_bytes "$cloud_total_bytes")
            local cloud_dur_fmt=$(format_duration "$cloud_duration")
            cloud_summary_line="$cloud_success/$cloud_total destinations, $cloud_total_fmt ($cloud_files files) in $cloud_dur_fmt"
            cloud_replication_section="
--- CLOUD REPLICATION ---
${cloud_details}"
        fi
    fi
    
    # Default sections when no data
    [[ -z "$local_replication_section" ]] && local_replication_section="
--- LOCAL REPLICATION ---
Disabled"
    
    [[ -z "$cloud_replication_section" ]] && cloud_replication_section="
--- CLOUD REPLICATION ---
Disabled"
    
    # Build EXCLUDED section (inline list, multiple per line)
    local excluded_section=""
    if [[ -n "$excluded_list" ]]; then
        local excluded_inline=""
        local count=0
        while IFS= read -r name; do
            [[ -z "$name" ]] && continue
            if [[ $count -gt 0 ]]; then
                # Add separator, newline every 4 items
                if (( count % 4 == 0 )); then
                    excluded_inline+=$'\n'
                else
                    excluded_inline+="  "
                fi
            fi
            excluded_inline+="• $name"
            ((count++))
        done <<< "$excluded_list"
        excluded_section="
--- EXCLUDED VMs ($excluded_count) ---
$excluded_inline"
    fi
    
    # Calculate included VM count (backed up + skipped + failed)
    local included_count=$((success_count + failed_count + skipped_count))
    local total_vms=$((included_count + excluded_count))
    
    # Build summary section with replication stats
    local summary_lines="VMs: $total_vms total
  ✓ $success_count success, ◇ $skipped_count skipped
  ✗ $failed_count failed, ○ $excluded_count excluded
Written: $total_written_fmt"
    
    [[ -n "$local_summary_line" ]] && summary_lines+="
Local: $local_summary_line"
    [[ -n "$cloud_summary_line" ]] && summary_lines+="
Cloud: $cloud_summary_line"
    
    # Get chain health summary
    local chain_health_section=""
    if declare -f get_chain_health_summary >/dev/null 2>&1; then
        local chain_health
        chain_health=$(get_chain_health_summary "$EMAIL_BACKUP_PATH" 2>/dev/null)
        if [[ -n "$chain_health" ]] && [[ "$chain_health" != "No chain health data available" ]]; then
            chain_health_section="
--- CHAIN HEALTH ---
$chain_health"
        fi
    fi
    
    # Build the email body with compact format
    body="VM Backup Report
--------------------
Host: $EMAIL_HOSTNAME
Date: $date_str
Started: $start_time
Finished: $end_time
Duration: $duration_text

--- SUMMARY ---
$summary_lines

--- BACKUP DETAILS ($included_count VMs) ---
$included_sections
$chain_health_section

--- STORAGE ($EMAIL_BACKUP_PATH) ---
Used: $total_backup_fmt
Free: $available_fmt$available_pct
$local_replication_section$cloud_replication_section$excluded_section

Log: vmbackup-$date_str.txt"
    
    echo "$body"
}

# Build subject line from database
# Parameters:
#   $1 = session_start (HH:MM:SS) - unused, kept for API compat
#   $2 = session_end (HH:MM:SS) - unused, kept for API compat
#   $3 = date_str (YYYY-MM-DD)
build_subject() {
    local session_start="$1"
    local session_end="$2"
    local date_str="$3"
    
    local success_count=0
    local failed_count=0
    local skipped_count=0
    local excluded_count=0
    
    # Query session summary from database
    if declare -f sqlite_query_session_summary >/dev/null 2>&1; then
        local summary_row
        summary_row=$(sqlite_query_session_summary 2>/dev/null)
        if [[ -n "$summary_row" ]]; then
            IFS='|' read -r _total _success _failed _skipped _excluded _bytes _status _stype <<< "$summary_row"
            # Replicate-only session — distinct subject line
            if [[ "$_stype" == "replicate_only" ]]; then
                if [[ "$_status" == "failed" ]]; then
                    echo "Replication Only — $(hostname -s) — FAILED"
                else
                    echo "Replication Only — $(hostname -s) — OK"
                fi
                return
            fi
            success_count=${_success:-0}
            failed_count=${_failed:-0}
            skipped_count=${_skipped:-0}
            excluded_count=${_excluded:-0}
        fi
    fi
    
    if [[ "$failed_count" -gt 0 ]]; then
        echo "VM Backup - $success_count backed up, $excluded_count excluded, $failed_count FAILED"
    else
        echo "VM Backup - $success_count backed up, $excluded_count excluded - OK"
    fi
}

#################################################################################
# EMAIL SENDING
#################################################################################

# Send email with attachment via msmtp
send_email() {
    local subject="$1"
    local body="$2"
    local attachment="$3"
    
    local boundary="====BOUNDARY_$(date +%s)===="
    local temp_mail
    temp_mail=$(mktemp)
    
    # Build MIME message
    {
        echo "From: $EMAIL_SENDER"
        echo "To: $EMAIL_RECIPIENT"
        echo "Subject: $subject"
        echo "MIME-Version: 1.0"
        echo "Content-Type: multipart/mixed; boundary=\"$boundary\""
        echo ""
        echo "--$boundary"
        echo "Content-Type: text/plain; charset=utf-8"
        echo "Content-Transfer-Encoding: 8bit"
        echo ""
        echo "$body"
        
        # Attach log if exists and not empty
        if [[ -f "$attachment" ]] && [[ -s "$attachment" ]]; then
            echo ""
            echo "--$boundary"
            echo "Content-Type: text/plain; charset=utf-8; name=\"$(basename "$attachment")\""
            echo "Content-Disposition: attachment; filename=\"$(basename "$attachment")\""
            echo "Content-Transfer-Encoding: base64"
            echo ""
            base64 "$attachment"
        fi
        
        echo ""
        echo "--$boundary--"
    } > "$temp_mail"
    
    # Send via msmtp
    if msmtp -t < "$temp_mail" 2>/dev/null; then
        rm -f "$temp_mail"
        return 0
    else
        local exit_code=$?
        rm -f "$temp_mail"
        return $exit_code
    fi
}

#################################################################################
# MAIN FUNCTION
#################################################################################

# Send backup report email
# Parameters:
#   $1 = backup start time (HH:MM:SS)
#   $2 = backup end time (HH:MM:SS) - optional, defaults to now
#   $3 = overall status: "success", "failed", "partial" (optional)
#
# Returns: 0 on success, 1 on failure, 2 if skipped (disabled)
send_backup_report() {
    local start_time="${1:-$(date '+%Y-%m-%d %H:%M:%S')}"
    local end_time="${2:-$(date '+%Y-%m-%d %H:%M:%S')}"
    local overall_status="${3:-success}"
    local date_str=$(date '+%Y-%m-%d')
    
    # Initialize paths if not already done
    _init_email_paths
    
    # Check if email module is available
    if [[ "${EMAIL_MODULE_AVAILABLE:-0}" -ne 1 ]]; then
        echo "Email reports disabled (module not available)" >&2
        return 2
    fi
    
    # Check conditional sending
    if [[ "$overall_status" == "success" ]] && [[ "${EMAIL_ON_SUCCESS:-yes}" != "yes" ]]; then
        echo "Skipping email report (EMAIL_ON_SUCCESS=no)"
        return 2
    fi
    
    if [[ "$overall_status" == "failed" ]] && [[ "${EMAIL_ON_FAILURE:-yes}" != "yes" ]]; then
        echo "Skipping email report (EMAIL_ON_FAILURE=no)"
        return 2
    fi
    
    local log_file="${EMAIL_LOG_DIR}/vmbackup.log"
    
    # Verify database is available for report data
    if ! declare -f sqlite_query_session_vm_backups >/dev/null 2>&1; then
        echo "WARNING: SQLite query functions not available — email report may have incomplete data" >&2
    fi
    
    # Build email content (session-scoped DB queries)
    local subject=$(build_subject "$start_time" "$end_time" "$date_str")
    local body=$(build_email_body "$start_time" "$end_time" "$date_str")
    
    # Get current session's log portion (filtered by start_time, not entire day)
    local today_log=$(get_todays_log "$log_file" "$date_str" "$start_time")
    
    # Use temp log directly for attachment (avoid permission issues)
    local attachment_name="$today_log"
    if [[ -f "$today_log" ]] && [[ -s "$today_log" ]]; then
        # Rename temp file to have meaningful name (for email attachment)
        local renamed_log="/tmp/vmbackup-${date_str}.txt"
        mv "$today_log" "$renamed_log" 2>/dev/null || cp "$today_log" "$renamed_log" 2>/dev/null
        attachment_name="$renamed_log"
    fi
    
    # Save debug copy of email to STATE_DIR/email/
    local email_debug_dir="${STATE_DIR:-/tmp}/email"
    mkdir -p "$email_debug_dir" 2>/dev/null
    local email_debug_file="${email_debug_dir}/email-${date_str}_$(date '+%H%M%S').txt"
    {
        echo "=========================================="
        echo "EMAIL DEBUG COPY"
        echo "Generated: $(date '+%Y-%m-%d %H:%M:%S')"
        echo "Instance:  ${CONFIG_INSTANCE:-default}"
        echo "=========================================="
        echo ""
        echo "TO: $EMAIL_RECIPIENT"
        echo "FROM: $EMAIL_SENDER"
        echo "SUBJECT: $subject"
        echo ""
        echo "=========================================="
        echo "BODY:"
        echo "=========================================="
        echo "$body"
        echo ""
        echo "=========================================="
        echo "ATTACHMENT: $(basename "${attachment_name:-none}")"
        echo "=========================================="
        if [[ -f "$attachment_name" ]]; then
            echo "(First 100 lines of attachment)"
            head -100 "$attachment_name"
            echo "..."
        else
            echo "(No attachment or file not found)"
        fi
    } > "$email_debug_file" 2>/dev/null
    echo "Email debug copy saved: $email_debug_file"
    
    # Send email
    if send_email "$subject" "$body" "$attachment_name"; then
        # Cleanup temp log
        [[ -f "$attachment_name" ]] && [[ "$attachment_name" == /tmp/* ]] && rm -f "$attachment_name"
        echo "Email report sent successfully to $EMAIL_RECIPIENT"
        return 0
    else
        echo "ERROR: Failed to send email report" >&2
        return 1
    fi
}

# Test function - can be called directly for testing
test_email_report() {
    echo "Testing email report module..."
    echo ""
    
    # Load config if not already loaded
    if [[ "${EMAIL_MODULE_LOADED:-0}" -ne 1 ]]; then
        echo "Loading email config..."
        if ! load_email_config; then
            echo "Config load failed or email disabled"
            return 1
        fi
    fi
    
    _init_email_paths
    
    echo "Configuration:"
    echo "  Instance:     ${CONFIG_INSTANCE:-default}"
    echo "  Enabled:      $EMAIL_ENABLED"
    echo "  Recipient:    $EMAIL_RECIPIENT"
    echo "  Sender:       $EMAIL_SENDER"
    echo "  Hostname:     $EMAIL_HOSTNAME"
    echo "  Prefix:       $EMAIL_SUBJECT_PREFIX"
    echo "  On Success:   $EMAIL_ON_SUCCESS"
    echo "  On Failure:   $EMAIL_ON_FAILURE"
    echo ""
    echo "Paths:"
    echo "  Backup Path:  $EMAIL_BACKUP_PATH"
    echo "  Log Dir:      $EMAIL_LOG_DIR"
    echo ""
    
    local date_str=$(date '+%Y-%m-%d')
    
    # Check if SQLite module is available
    if declare -f sqlite_query_session_vm_backups >/dev/null 2>&1; then
        echo "Data source: SQLite DB (session-scoped)"
        echo "Session ID:  ${SQLITE_CURRENT_SESSION_ID:-<not set>}"
        echo ""
        echo "=== EMAIL PREVIEW ==="
        local test_start="00:00:00"
        local test_end="23:59:59"
        echo "Subject: $(build_subject "$test_start" "$test_end" "$date_str")"
        echo ""
        build_email_body "$test_start" "$test_end" "$date_str"
    else
        echo "SQLite module not available."
        echo "Run as part of vmbackup.sh to test with live data."
    fi
}

# If run directly (not sourced), run test
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    test_email_report
fi
