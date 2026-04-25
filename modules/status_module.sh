#!/bin/bash
#################################################################################
# Status Report Module for vmbackup.sh (ENH-10)
#
# Presentation layer for --status reporting command.
# Queries SQLite via lib/sqlite_module.sh, formats output for terminal or CSV.
#
# Three-file architecture:
#   vmbackup.sh          — flag parsing, dispatch
#   lib/sqlite_module.sh — query functions (data layer)
#   modules/status_module.sh — this file (presentation layer)
#
# Extension pattern: new report = one query function + one _status_* function
#                    + one dispatch case in run_status_report()
#
# Dependencies:
#   - lib/sqlite_module.sh (already sourced by vmbackup.sh)
#   - modules/rotation_module.sh (sourced on demand for --policies)
#
#################################################################################

STATUS_MODULE_VERSION="1.0"
STATUS_MODULE_LOADED=0

#################################################################################
# FORMATTING HELPERS
#################################################################################

# Format bytes to human-readable (GiB/MiB/KiB)
# Follows email_report_module.sh pattern (awk for decimal precision)
_format_bytes() {
    local bytes="$1"
    if [[ -z "$bytes" ]] || ! [[ "$bytes" =~ ^[0-9]+\.?[0-9]*$ ]]; then
        echo "0 B"
        return
    fi
    # Truncate to integer for comparisons (AVG() returns floats)
    local int_bytes=${bytes%%.*}
    if [[ "$int_bytes" -ge 1073741824 ]]; then
        awk "BEGIN {printf \"%.1f GiB\", $bytes/1073741824}"
    elif [[ "$int_bytes" -ge 1048576 ]]; then
        awk "BEGIN {printf \"%.1f MiB\", $bytes/1048576}"
    elif [[ "$int_bytes" -ge 1024 ]]; then
        awk "BEGIN {printf \"%.1f KiB\", $bytes/1024}"
    else
        echo "${int_bytes} B"
    fi
}

# Format seconds to Xm Ys
_format_duration() {
    local seconds="$1"
    if [[ -z "$seconds" ]] || ! [[ "$seconds" =~ ^[0-9]+\.?[0-9]*$ ]]; then
        seconds=0
    fi
    # Truncate to integer (duration_sec can be float from AVG)
    seconds=${seconds%%.*}
    local minutes=$((seconds / 60))
    local secs=$((seconds % 60))
    if [[ $minutes -gt 0 ]]; then
        printf "%dm %02ds" "$minutes" "$secs"
    else
        printf "%ds" "$secs"
    fi
}

#################################################################################
# SHARED OUTPUT FORMATTER
#################################################################################

# Format pipe-delimited query output for terminal display
# Replaces byte and duration columns with human-readable values
# Arguments:
#   $1 - byte_cols: comma-separated 1-based column indices for byte fields
#   $2 - dur_cols: comma-separated 1-based column indices for duration fields
# Reads from stdin, writes to stdout
_format_status_output() {
    local byte_cols="${1:-}"
    local dur_cols="${2:-}"

    local header_done=false
    while IFS='|' read -ra cols; do
        if [[ "$header_done" == false ]]; then
            # Print header as-is (replace raw names with display names)
            local hdr=""
            for i in "${!cols[@]}"; do
                local col="${cols[$i]}"
                # Replace byte column headers for terminal display
                [[ "$col" == "archive_size_bytes" ]] && col="archive_size"
                [[ -n "$hdr" ]] && hdr+="|"
                hdr+="$col"
            done
            echo "$hdr"
            header_done=true
            continue
        fi

        local out=""
        for i in "${!cols[@]}"; do
            local val="${cols[$i]}"
            local col_num=$((i + 1))

            # Format byte columns
            if [[ ",$byte_cols," == *",$col_num,"* ]]; then
                if [[ -n "$val" && "$val" != "" ]]; then
                    val=$(_format_bytes "$val")
                else
                    val="-"
                fi
            fi

            # Format duration columns
            if [[ ",$dur_cols," == *",$col_num,"* ]]; then
                val=$(_format_duration "$val")
            fi

            [[ -n "$out" ]] && out+="|"
            out+="$val"
        done
        echo "$out"
    done | column -t -s '|'
}

# Insert _hr columns adjacent to their raw counterparts in CSV output
# Arguments:
#   $1 - byte_cols: comma-separated 1-based column indices for byte fields
#   $2 - dur_cols: comma-separated 1-based column indices for duration fields
# Reads CSV from stdin, writes CSV to stdout
_format_csv_output() {
    local byte_cols="${1:-}"
    local dur_cols="${2:-}"

    # No formatting columns — pass through unchanged
    if [[ -z "$byte_cols" && -z "$dur_cols" ]]; then
        cat
        return
    fi

    local line_num=0
    while IFS= read -r line; do
        line_num=$((line_num + 1))

        # Parse CSV fields (handle quoted fields)
        local -a fields=()
        local field="" in_quote=false
        local i
        for ((i = 0; i < ${#line}; i++)); do
            local ch="${line:$i:1}"
            if [[ "$in_quote" == true ]]; then
                if [[ "$ch" == '"' ]]; then
                    if [[ "${line:$((i+1)):1}" == '"' ]]; then
                        field+='"'
                        ((i++))
                    else
                        in_quote=false
                    fi
                else
                    field+="$ch"
                fi
            elif [[ "$ch" == '"' ]]; then
                in_quote=true
            elif [[ "$ch" == ',' ]]; then
                fields+=("$field")
                field=""
            else
                field+="$ch"
            fi
        done
        fields+=("$field")

        # Build output with _hr columns inserted
        local -a out_fields=()
        for j in "${!fields[@]}"; do
            local col_num=$((j + 1))
            local val="${fields[$j]}"
            out_fields+=("$val")

            if [[ ",$byte_cols," == *",$col_num,"* ]]; then
                if [[ $line_num -eq 1 ]]; then
                    out_fields+=("${val}_hr")
                else
                    if [[ -n "$val" && "$val" != "" ]]; then
                        out_fields+=("$(_format_bytes "$val")")
                    else
                        out_fields+=("—")
                    fi
                fi
            fi

            if [[ ",$dur_cols," == *",$col_num,"* ]]; then
                if [[ $line_num -eq 1 ]]; then
                    out_fields+=("${val}_hr")
                else
                    out_fields+=("$(_format_duration "$val")")
                fi
            fi
        done

        # Output as CSV (quote fields containing commas or quotes)
        local csv_line=""
        for f in "${out_fields[@]}"; do
            [[ -n "$csv_line" ]] && csv_line+=","
            if [[ "$f" == *","* || "$f" == *'"'* || "$f" == *$'\n'* ]]; then
                csv_line+="\"${f//\"/\"\"}\""
            else
                csv_line+="$f"
            fi
        done
        echo "$csv_line"
    done
}

#################################################################################
# REPORT FUNCTIONS
#################################################################################

# Check for empty output and print message
# Returns: 0 if data present, 1 if empty
_status_check_empty() {
    local data="$1"
    # Check if there's anything beyond a header line
    local line_count
    line_count=$(echo "$data" | wc -l)
    if [[ -z "$data" || $line_count -le 1 ]]; then
        echo "No matching records." >&2
        return 1
    fi
    return 0
}

_status_sessions() {
    local days="$1" csv="$2"

    # CSV: flat session rows (10 original cols + 3 new row-count cols, additive)
    if [[ "$csv" == "true" ]]; then
        local data
        data=$(sqlite_query_today_sessions "$days" "csv")
        _status_check_empty "$data" || return 0
        echo "$data"
        return 0
    fi

    # Terminal: classify each session by job type, dispatch to renderer
    local data
    data=$(sqlite_query_today_sessions "$days" "pipe")
    _status_check_empty "$data" || return 0

    local first=true
    local row
    while IFS= read -r row; do
        # Skip header row (starts with "id|")
        [[ "$row" == id\|* ]] && continue
        [[ -z "$row" ]] && continue

        [[ "$first" == true ]] && first=false || echo ""

        # Parse minimum needed for classification (full row passed to renderer)
        local sess_id session_type status vm_rows repl_rows _rest
        IFS='|' read -r sess_id _instance session_type _start _dur status \
                       _vs _vf _vsk _bytes vm_rows repl_rows _retention <<< "$row"

        # Classification ladder (priority order matters)
        local job_type=""
        if [[ "$session_type" == "prune" ]]; then
            job_type="prune"
        elif [[ "${vm_rows:-0}" -gt 0 ]]; then
            job_type="backup"
        elif [[ "$status" == "replication_only" ]] \
             || { [[ "${vm_rows:-0}" -eq 0 ]] && [[ "${repl_rows:-0}" -gt 0 ]]; }; then
            job_type="replicate-only"
        else
            # Empty or unknown: distinguish by session_type
            case "$session_type" in
                ""|standard|targeted|prune|replicate_only)
                    job_type="empty" ;;
                *)
                    job_type="unknown" ;;
            esac
        fi

        case "$job_type" in
            backup)         _render_backup_session         "$row" ;;
            prune)          _render_prune_session          "$row" ;;
            replicate-only) _render_replicate_only_session "$row" ;;
            empty)          _render_empty_session          "$row" ;;
            unknown)        _render_unknown_session        "$row" ;;
        esac
    done <<< "$data"
}

# ─── Per-job-type renderers ───────────────────────────────────────────────────
# Each renderer accepts a single pipe-delimited session row with 13 fields:
#   id|instance|session_type|start_time|duration_sec|status|
#   vms_success|vms_failed|vms_skipped|bytes_total|vm_rows|repl_rows|retention_rows

# Common header builder: SESSION <id> — <instance>  [<label>]  <start>  <status>  <dur>  [<bytes>]
# Arguments:
#   $1 - row (pipe-delimited)
#   $2 - label override (default: session_type or 'standard')
#   $3 - include_bytes ('true'|'false', default 'true')
_render_session_header() {
    local row="$1" label_override="$2" include_bytes="${3:-true}"
    local sess_id instance session_type start_time duration_sec status \
          _vs _vf _vsk bytes_total _vmr _repr _retr
    IFS='|' read -r sess_id instance session_type start_time duration_sec status \
                   _vs _vf _vsk bytes_total _vmr _repr _retr <<< "$row"

    local label="${label_override:-${session_type:-standard}}"
    local dur_fmt size_fmt
    dur_fmt=$(_format_duration "$duration_sec")

    if [[ "$include_bytes" == "true" ]]; then
        size_fmt=$(_format_bytes "$bytes_total")
        printf "SESSION %s — %s  [%s]  %s  %s  %s  %s\n" \
            "$sess_id" "$instance" "$label" "$start_time" "$status" "$dur_fmt" "$size_fmt"
    else
        printf "SESSION %s — %s  [%s]  %s  %s  %s\n" \
            "$sess_id" "$instance" "$label" "$start_time" "$status" "$dur_fmt"
    fi
}

_render_backup_session() {
    local row="$1"
    local sess_id _instance session_type _start _dur _status \
          vms_success vms_failed vms_skipped _bytes vm_rows repl_rows _retention
    IFS='|' read -r sess_id _instance session_type _start _dur _status \
                   vms_success vms_failed vms_skipped _bytes \
                   vm_rows repl_rows _retention <<< "$row"

    local mixed=false
    [[ "${repl_rows:-0}" -gt 0 ]] && mixed=true

    local label="${session_type:-standard}"
    [[ "$mixed" == "true" ]] && label="${label} +repl"
    _render_session_header "$row" "$label" "true"

    # VM table
    if [[ "${vm_rows:-0}" -eq 0 ]]; then
        printf "  (no VM detail rows recorded)\n"
    else
        local vm_data
        vm_data=$(sqlite_query_session_vm_backups_display "$sess_id")
        if [[ -z "$vm_data" ]]; then
            printf "  (no VM detail rows recorded)\n"
        else
            local vm_table="vm|state|type|status|size|duration"
            local _vm _state _type _vstatus _vbytes _vdur
            while IFS='|' read -r _vm _state _type _vstatus _vbytes _vdur; do
                [[ -z "$_vm" ]] && continue
                local sz dur_s
                sz=$(_format_bytes "$_vbytes")
                dur_s=$(_format_duration "$_vdur")
                vm_table+=$'\n'"  $_vm|$_state|$_type|$_vstatus|$sz|$dur_s"
            done <<< "$vm_data"
            echo "$vm_table" | column -t -s '|' | sed 's/^/  /'
        fi
    fi

    # Mixed: one footer line per replication endpoint
    if [[ "$mixed" == "true" ]]; then
        local repl_data
        repl_data=$(sqlite_query_session_replication_summary "$sess_id")
        local _ep _ept _trans _rstatus _rbytes _rfiles _rdur _rerr
        while IFS='|' read -r _ep _ept _trans _rstatus _rbytes _rfiles _rdur _rerr; do
            [[ -z "$_ep" ]] && continue
            local bytes_hr
            bytes_hr=$(_format_bytes "$_rbytes")
            case "$_rstatus" in
                success)
                    printf "  → Replication: %s (%s/%s) success %s\n" \
                        "$_ep" "$_ept" "$_trans" "$bytes_hr"
                    ;;
                failed)
                    local err_short="${_rerr:0:60}"
                    printf "  → Replication: %s (%s/%s) failed — %s\n" \
                        "$_ep" "$_ept" "$_trans" "$err_short"
                    ;;
                *)
                    printf "  → Replication: %s (%s/%s) %s\n" \
                        "$_ep" "$_ept" "$_trans" "$_rstatus"
                    ;;
            esac
        done <<< "$repl_data"
    fi

    # Backup summary line when there are skips or failures
    if [[ "${vms_failed:-0}" -gt 0 || "${vms_skipped:-0}" -gt 0 ]]; then
        printf "  → success=%s  failed=%s  skipped=%s\n" \
            "$vms_success" "$vms_failed" "$vms_skipped"
    fi
}

_render_prune_session() {
    local row="$1"
    local sess_id _rest_fields
    IFS='|' read -r sess_id _rest_fields <<< "$row"

    _render_session_header "$row" "prune" "false"

    # No retention events → nothing to summarize
    local retention_rows
    retention_rows=$(echo "$row" | awk -F'|' '{print $13}')
    if [[ "${retention_rows:-0}" -eq 0 ]]; then
        printf "  (no retention events — nothing pruned this run)\n"
        return 0
    fi

    # Aggregate detail
    local summary
    summary=$(sqlite_query_session_retention_summary "$sess_id")
    if [[ -z "$summary" ]]; then
        printf "  (no retention events — nothing pruned this run)\n"
        return 0
    fi

    local actions_total actions_ok actions_failed freed_total vm_count \
          action_types target_types delete_count evaluate_count skip_count
    IFS='|' read -r actions_total actions_ok actions_failed freed_total vm_count \
                   action_types target_types delete_count evaluate_count skip_count \
                   <<< "$summary"

    printf "  Prune actions: %s ok / %s failed\n" "$actions_ok" "$actions_failed"

    if [[ "${delete_count:-0}" -gt 0 ]]; then
        local freed_hr
        freed_hr=$(_format_bytes "$freed_total")
        printf "    delete:   %s  Freed: %s across %s VMs\n" \
            "$delete_count" "$freed_hr" "$vm_count"
    fi
    if [[ "${evaluate_count:-0}" -gt 0 ]]; then
        printf "    evaluate: %s (no deletes — retention check only)\n" "$evaluate_count"
    fi
    if [[ "${skip_count:-0}" -gt 0 ]]; then
        printf "    skip:     %s (preserved by policy)\n" "$skip_count"
    fi
    if [[ -n "$target_types" ]]; then
        printf "    targets:  %s\n" "$target_types"
    fi
}

_render_replicate_only_session() {
    local row="$1"
    local sess_id _rest
    IFS='|' read -r sess_id _rest <<< "$row"

    _render_session_header "$row" "replicate-only" "false"

    local repl_data
    repl_data=$(sqlite_query_session_replication_summary "$sess_id")
    if [[ -z "$repl_data" ]]; then
        printf "  (no replication runs recorded)\n"
        return 0
    fi

    # Build pipe-delimited table; pipe through _format_status_output
    # Columns: endpoint|type|transport|status|bytes|files|duration  (drop error_message for display)
    local table="endpoint|type|transport|status|bytes|files|duration"
    local repl_count=0
    local ok_count=0 fail_count=0 cancel_count=0 skip_count=0
    local _ep _ept _trans _rstatus _rbytes _rfiles _rdur _rerr
    while IFS='|' read -r _ep _ept _trans _rstatus _rbytes _rfiles _rdur _rerr; do
        [[ -z "$_ep" ]] && continue
        table+=$'\n'"  $_ep|$_ept|$_trans|$_rstatus|$_rbytes|$_rfiles|$_rdur"
        ((repl_count++)) || true
        case "$_rstatus" in
            success)   ((ok_count++))     || true ;;
            failed)    ((fail_count++))   || true ;;
            cancelled) ((cancel_count++)) || true ;;
            skipped)   ((skip_count++))   || true ;;
        esac
    done <<< "$repl_data"

    # Bytes col=5, duration col=7 (1-indexed)
    echo "$table" | _format_status_output "5" "7" | sed 's/^/  /'

    # Summary footer only when >= 4 runs
    if [[ "$repl_count" -ge 4 ]]; then
        printf "  → %s runs: %s success, %s failed, %s cancelled, %s skipped\n" \
            "$repl_count" "$ok_count" "$fail_count" "$cancel_count" "$skip_count"
    fi
}

_render_empty_session() {
    local row="$1"
    local _id _instance _stype _start _dur status _rest
    IFS='|' read -r _id _instance _stype _start _dur status _rest <<< "$row"

    _render_session_header "$row" "" "true"
    if [[ "$status" == "running" ]]; then
        printf "  (session in progress — detail pending)\n"
    else
        printf "  (session incomplete — no detail rows recorded)\n"
    fi
}

_render_unknown_session() {
    local row="$1"
    local _id _instance session_type _rest
    IFS='|' read -r _id _instance session_type _rest <<< "$row"

    _render_session_header "$row" "$session_type" "true"
    printf "  (unknown session type: %s — no renderer available)\n" "$session_type"
}

_status_vm_history() {
    local vm_name="$1" days="$2" csv="$3"
    local output_mode="pipe"
    [[ "$csv" == "true" ]] && output_mode="csv"

    # Use days * 3 as a rough limit (multiple backups per day)
    local limit=$((days * 3))
    [[ $limit -lt 10 ]] && limit=10

    local data
    data=$(sqlite_query_vm_history "$vm_name" "$limit" "$output_mode")
    _status_check_empty "$data" || return 0

    if [[ "$csv" == "true" ]]; then
        # bytes_written=col4, duration_sec=col5
        echo "$data" | _format_csv_output "4" "5"
    else
        echo "$data" | _format_status_output "4" "5"
    fi
}

_status_failures() {
    local days="$1" csv="$2"
    local output_mode="pipe"
    [[ "$csv" == "true" ]] && output_mode="csv"

    local data
    data=$(sqlite_query_recent_failures "$days" "$output_mode")
    _status_check_empty "$data" || return 0

    if [[ "$csv" == "true" ]]; then
        echo "$data"
    else
        echo "$data" | _format_status_output "" ""
    fi
}

_status_replication() {
    local days="$1" csv="$2"
    local output_mode="pipe"
    [[ "$csv" == "true" ]] && output_mode="csv"

    local data
    data=$(sqlite_query_today_replications "$days" "$output_mode")
    _status_check_empty "$data" || return 0

    if [[ "$csv" == "true" ]]; then
        # bytes_transferred=col6, duration_sec=col7 (start_time added at col1)
        echo "$data" | _format_csv_output "6" "7"
    else
        echo "$data" | _format_status_output "6" "7"
    fi
}

_status_chains() {
    local vm_name="$1" csv="$2"
    local output_mode="pipe"
    [[ "$csv" == "true" ]] && output_mode="csv"

    local data
    data=$(sqlite_query_chain_health "$vm_name" "$output_mode")
    _status_check_empty "$data" || return 0

    if [[ "$csv" == "true" ]]; then
        if [[ -z "$vm_name" ]]; then
            # Summary: archive_size_bytes=col8
            echo "$data" | _format_csv_output "8" ""
        else
            # Detail: archive_size_bytes=col7
            echo "$data" | _format_csv_output "7" ""
        fi
    else
        if [[ -z "$vm_name" ]]; then
            echo "$data" | _format_status_output "8" ""
        else
            echo "$data" | _format_status_output "7" ""
        fi
    fi
}

_status_storage() {
    local csv="$1"
    local instance="${CONFIG_INSTANCE:-default}"

    # Effective abort threshold (matches check_disk_space defaults).
    # Spec §5.1: projection line MUST name and use the configured value, not
    # a hardcoded 20%, otherwise it lies on tuned instances.
    local abort_pct="${DISK_ABORT_PCT:-20}"

    # ---------- Per-VM trend table (104 §5.2 / §5.4) ----------
    local data
    data=$(sqlite_query_storage_trends "$instance" "pipe")
    local have_vm_data=1
    _status_check_empty "$data" 2>/dev/null || have_vm_data=0

    # ---------- Destination summary (single row) ----------
    local dest
    dest=$(sqlite_query_storage_destination_summary "$instance" 30 "pipe")
    # Strip header and grab the first data row (may be all NULLs if no v2.1+ sessions yet)
    local dest_row
    dest_row=$(echo "$dest" | sed -n '2p')
    local sample_count oldest_time newest_time oldest_used newest_used \
          newest_free newest_total written_7d written_30d
    IFS='|' read -r sample_count oldest_time newest_time oldest_used newest_used \
                   newest_free newest_total written_7d written_30d <<< "$dest_row"
    sample_count="${sample_count:-0}"
    [[ "$sample_count" =~ ^[0-9]+$ ]] || sample_count=0

    # Live df for the "Free space" line (spec §5.2: current state, not history)
    local live_free_bytes=0 live_total_bytes=0 live_pct_free=0
    if [[ -n "${BACKUP_PATH:-}" ]] && [[ -d "${BACKUP_PATH}" ]]; then
        local _df_out
        _df_out=$(df -PB1 "$BACKUP_PATH" 2>/dev/null | awk 'NR==2 {print $2 " " $4}')
        if [[ -n "$_df_out" ]]; then
            read -r live_total_bytes live_free_bytes <<< "$_df_out"
            [[ "$live_total_bytes" =~ ^[0-9]+$ ]] || live_total_bytes=0
            [[ "$live_free_bytes"  =~ ^[0-9]+$ ]] || live_free_bytes=0
            if (( live_total_bytes > 0 )); then
                live_pct_free=$(( live_free_bytes * 100 / live_total_bytes ))
            fi
        fi
    fi

    # Projection: bytes/day from slope across the v2.1+ window, then days
    # until disk_used reaches (1 - abort_pct/100) * disk_total.
    # Render "(insufficient history)" if <7 v2.1+ sessions per spec §5.2.
    local projection_text="(insufficient history)"
    if (( sample_count >= 7 )) && [[ -n "$oldest_time" ]] && [[ -n "$newest_time" ]] \
       && [[ "$oldest_used" =~ ^[0-9]+$ ]] && [[ "$newest_used" =~ ^[0-9]+$ ]]; then
        local days_span growth_bytes bytes_per_day
        days_span=$(sqlite3 "$SQLITE_DB_PATH" \
            "SELECT CAST(julianday('$newest_time') - julianday('$oldest_time') AS REAL);" 2>/dev/null)
        # Need a meaningful time span (>= 1 day) to compute a rate
        if [[ -n "$days_span" ]] && awk "BEGIN{exit !($days_span >= 1.0)}" 2>/dev/null; then
            growth_bytes=$(( newest_used - oldest_used ))
            if (( growth_bytes > 0 )); then
                bytes_per_day=$(awk "BEGIN{printf \"%d\", $growth_bytes / $days_span}")
                # Headroom = current free - abort threshold (in bytes)
                local abort_threshold_bytes headroom_bytes days_to_abort
                abort_threshold_bytes=$(( live_total_bytes * abort_pct / 100 ))
                headroom_bytes=$(( live_free_bytes - abort_threshold_bytes ))
                if (( headroom_bytes > 0 )) && (( bytes_per_day > 0 )); then
                    days_to_abort=$(( headroom_bytes / bytes_per_day ))
                    projection_text="~${days_to_abort} days until DISK_ABORT_PCT (${abort_pct}%) free"
                elif (( headroom_bytes <= 0 )); then
                    projection_text="ALREADY below DISK_ABORT_PCT (${abort_pct}%) free"
                else
                    projection_text="(no growth — destination stable)"
                fi
            else
                # Negative or zero growth (retention freed more than backups added)
                projection_text="(no growth — destination shrinking or stable)"
            fi
        fi
    fi

    # ---------- CSV output (spec §5.1 / §8 acceptance) ----------
    if [[ "$csv" == "true" ]]; then
        echo "vm,avg_full,avg_incr,last_written,last_size,trend"
        if (( have_vm_data )); then
            local _hdr_skipped=0
            while IFS='|' read -r vm tcount af ai lw ls l5 p5; do
                if (( _hdr_skipped == 0 )); then _hdr_skipped=1; continue; fi
                local trend; trend=$(_storage_trend_symbol "$tcount" "$l5" "$p5")
                # Strip whitespace
                vm=${vm# } vm=${vm% }
                lw=${lw# } lw=${lw% }
                ls=${ls# } ls=${ls% }
                af=${af# } af=${af% } af=${af%.*}
                ai=${ai# } ai=${ai% } ai=${ai%.*}
                echo "$vm,${af:-0},${ai:-0},${lw},${ls:-0},${trend}"
            done <<< "$data"
        fi
        # Destination row (separate, second logical block)
        # projected_days_to_abort: extract integer from projection_text or empty
        local proj_days=""
        if [[ "$projection_text" =~ ^~([0-9]+) ]]; then
            proj_days="${BASH_REMATCH[1]}"
        fi
        echo ""
        echo "instance,free_bytes,total_bytes,written_7d,written_30d,projected_days_to_abort"
        echo "${instance},${live_free_bytes},${live_total_bytes},${written_7d:-0},${written_30d:-0},${proj_days}"
        return 0
    fi

    # ---------- Terminal output ----------
    echo "STORAGE GROWTH — ${instance}  (last 30 sessions, v2.1+ data only)"
    echo ""

    if (( have_vm_data )); then
        # Render to a buffer with human-formatted bytes, pipe through `column -t`.
        local table="vm|avg_full|avg_incr|last_written|last_size|trend"
        local _hdr_skipped=0
        while IFS='|' read -r vm tcount af ai lw ls l5 p5; do
            if (( _hdr_skipped == 0 )); then _hdr_skipped=1; continue; fi
            vm=${vm# } vm=${vm% }
            tcount=${tcount# } tcount=${tcount% }
            af=${af# } af=${af% } af=${af%.*}
            ai=${ai# } ai=${ai% } ai=${ai%.*}
            ls=${ls# } ls=${ls% }
            lw=${lw# } lw=${lw% }
            local af_h ai_h ls_h trend
            af_h=$(_format_bytes "${af:-0}")
            ai_h=$(_format_bytes "${ai:-0}")
            ls_h=$(_format_bytes "${ls:-0}")
            trend=$(_storage_trend_symbol "$tcount" "$l5" "$p5")
            # last_written: take date portion only (YYYY-MM-DD)
            local lw_short="${lw%% *}"
            table+=$'\n'"${vm}|${af_h}|${ai_h}|${lw_short}|${ls_h}|${trend}"
        done <<< "$data"
        echo "$table" | column -t -s '|'
    else
        echo "  (no per-VM backup data for this instance yet)"
    fi

    echo ""
    echo "Backup destination:  ${BACKUP_PATH:-(unset)}"
    if (( live_total_bytes > 0 )); then
        echo "Free space:          $(_format_bytes "$live_free_bytes") / $(_format_bytes "$live_total_bytes")  (${live_pct_free}%)"
    else
        echo "Free space:          (df failed or BACKUP_PATH unreadable)"
    fi
    echo "Written last 7d:     $(_format_bytes "${written_7d:-0}")"
    echo "Written last 30d:    $(_format_bytes "${written_30d:-0}")"
    echo "At current rate:     ${projection_text}"
}

# Compute the trend symbol per spec §5.4.
# Args: total_count, last_5_avg, prev_5_avg
# Output: ↑↑ / ↑ / → / ↓ / —
_storage_trend_symbol() {
    local tcount="${1:-0}" last5="${2:-0}" prev5="${3:-0}"
    # Strip whitespace and sqlite REAL trailing decimals (we only need integer compare)
    tcount=${tcount// /}
    last5=${last5// /}
    prev5=${prev5// /}
    last5=${last5%.*}
    prev5=${prev5%.*}
    [[ -z "$tcount" || ! "$tcount" =~ ^[0-9]+$ ]] && tcount=0
    [[ -z "$last5"  || ! "$last5"  =~ ^[0-9]+$ ]] && last5=0
    [[ -z "$prev5"  || ! "$prev5"  =~ ^[0-9]+$ ]] && prev5=0

    # Minimum-data guard: need 5 + 5 = 10 rows
    if (( tcount < 10 )); then
        echo "—"
        return
    fi
    if (( prev5 == 0 )); then
        echo "—"
        return
    fi

    # Percent change (integer math): (last5 - prev5) * 100 / prev5
    local delta_pct=$(( (last5 - prev5) * 100 / prev5 ))
    if   (( delta_pct >  10 )); then echo "↑↑"
    elif (( delta_pct >=  1 )); then echo "↑"
    elif (( delta_pct >  -1 )); then echo "→"
    elif (( delta_pct >= -1 )); then echo "→"
    else                              echo "↓"
    fi
}

_status_policies() {
    local csv="$1"

    # Source rotation module for policy/retention lookups
    local script_dir="${SCRIPT_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
    # Provide log stubs if logging module not loaded (status runs without full init)
    declare -f log_debug &>/dev/null || log_debug() { :; }
    declare -f log_info &>/dev/null  || log_info()  { :; }
    source "$script_dir/modules/rotation_module.sh" 2>/dev/null || {
        echo "Error: rotation module not found" >&2
        return 1
    }
    load_rotation_config "${CONFIG_INSTANCE:-default}"

    # Get raw chain/orphan data from DB
    local data
    data=$(sqlite_query_policy_summary "pipe")
    _status_check_empty "$data" || return 0

    # Instance-level values for preamble/context columns
    local default_policy="${BACKUP_ROTATION_POLICY:-monthly}"
    local default_limit
    default_limit=$(get_retention_limit "$default_policy")
    local orphan_enabled="${RETENTION_ORPHAN_ENABLED:-true}"
    local orphan_max="${RETENTION_ORPHAN_MAX_AGE_DAYS:-90}"
    local orphan_min="${RETENTION_ORPHAN_MIN_AGE_DAYS:-7}"

    # Terminal preamble
    if [[ "$csv" != "true" ]]; then
        local retention_var
        case "$default_policy" in
            daily)   retention_var="RETENTION_DAYS" ;;
            weekly)  retention_var="RETENTION_WEEKS" ;;
            monthly) retention_var="RETENTION_MONTHS" ;;
            *)       retention_var="RETENTION_LIMIT" ;;
        esac
        echo "Instance default policy: $default_policy ($retention_var=$default_limit)"
        if [[ "$orphan_enabled" == "true" ]]; then
            echo "Orphan retention: enabled, max_age=$orphan_max days, min_age=$orphan_min days"
        else
            echo "Orphan retention: disabled"
        fi
        echo ""
    fi

    # Build enriched output into a buffer
    local output=""
    local header_printed=false

    while IFS='|' read -r vm_name chains orphans oldest_backup; do
        if [[ "$header_printed" == false ]]; then
            header_printed=true
            if [[ "$csv" == "true" ]]; then
                output="vm_name,policy,override,vm_retention_limit,chains,orphans,oldest_backup,retention_status,orphan_ages_out,instance_default_policy,instance_retention_limit,orphan_max_age_days,orphan_min_age_days"
            else
                output="vm_name|policy|override|vm_retention_limit|chains|orphans|oldest_backup|retention_status|orphan_ages_out"
            fi
            continue
        fi

        # Trim whitespace
        vm_name=$(echo "$vm_name" | xargs)
        chains=$(echo "$chains" | xargs)
        orphans=$(echo "$orphans" | xargs)
        oldest_backup=$(echo "$oldest_backup" | xargs)

        # Look up effective policy and retention limit for this VM
        local vm_policy
        vm_policy=$(get_vm_rotation_policy "$vm_name")
        local override="no"
        if [[ -v "VM_POLICY[$vm_name]" && -n "${VM_POLICY[$vm_name]}" ]]; then
            override="yes"
        fi
        local vm_ret_limit
        vm_ret_limit=$(get_retention_limit "$vm_policy")

        # Retention status
        local retention_status="—"
        if [[ "$vm_policy" != "accumulate" && "$vm_policy" != "never" && "$vm_ret_limit" -gt 0 ]]; then
            if [[ "$chains" -ge "$vm_ret_limit" ]]; then
                retention_status="at limit"
            else
                retention_status="not at limit"
            fi
        fi

        # Orphan ages out date
        local orphan_ages_out="—"
        if [[ "$orphans" -gt 0 ]]; then
            if [[ "$orphan_enabled" == "true" && -n "$oldest_backup" ]]; then
                orphan_ages_out=$(date -d "$oldest_backup + $orphan_max days" +%Y-%m-%d 2>/dev/null || echo "—")
            elif [[ "$orphan_enabled" != "true" ]]; then
                orphan_ages_out="disabled"
            fi
        fi

        if [[ "$csv" == "true" ]]; then
            local o_max_val="$orphan_max"
            local o_min_val="$orphan_min"
            [[ "$orphan_enabled" != "true" ]] && o_max_val="—" && o_min_val="—"
            output+=$'\n'"$vm_name,$vm_policy,$override,$vm_ret_limit,$chains,$orphans,$oldest_backup,$retention_status,$orphan_ages_out,$default_policy,$default_limit,$o_max_val,$o_min_val"
        else
            output+=$'\n'"$vm_name|$vm_policy|$override|$vm_ret_limit|$chains|$orphans|$oldest_backup|$retention_status|$orphan_ages_out"
        fi
    done <<< "$data"

    if [[ "$csv" == "true" ]]; then
        echo "$output"
    else
        echo "$output" | column -t -s '|'
    fi
}

#################################################################################
# ENTRY POINT
#################################################################################

# Main entry point for --status reporting
# Arguments:
#   $1 - sub_mode: default|vm|failures|replication|chains|storage|policies
#   $2 - vm_name (for vm and chains modes)
#   $3 - days (default 1)
#   $4 - csv flag: true or false
run_status_report() {
    local sub_mode="${1:-default}" vm_name="$2" days="${3:-1}" csv="${4:-false}"

    sqlite_init_readonly || return 1

    case "$sub_mode" in
        default)      _status_sessions "$days" "$csv" ;;
        vm)           _status_vm_history "$vm_name" "$days" "$csv" ;;
        failures)     _status_failures "$days" "$csv" ;;
        replication)  _status_replication "$days" "$csv" ;;
        chains)       _status_chains "$vm_name" "$csv" ;;
        storage)      _status_storage "$csv" ;;
        policies)     _status_policies "$csv" ;;
        *)
            echo "Error: Unknown status report mode: $sub_mode" >&2
            return 1
            ;;
    esac
}

STATUS_MODULE_LOADED=1
