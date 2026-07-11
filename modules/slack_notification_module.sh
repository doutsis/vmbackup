#!/bin/bash
#################################################################################
# Slack Notification Module for vmbackup.sh
#
# Posts a session summary to a Slack incoming webhook after a backup,
# replicate-only, or pre-flight-aborted run. Designed to mirror the call
# sites of email_report_module.sh so both can be enabled independently.
#
# Dependencies:
#   - curl (transport)
#   - lib/sqlite_module.sh (session totals; falls back to empty stats)
#   - config/<instance>/slack.conf (per-instance configuration)
#
# Usage:
#   source slack_notification_module.sh
#   load_slack_config
#   send_slack_notification "$start" "$end" "$status"
#
# Status values handled: success, partial, failed, unknown
#
# Contributed by @hostarts (PR #3: https://github.com/doutsis/vmbackup/pull/3).
#################################################################################

SLACK_MODULE_VERSION="1.0"
SLACK_MODULE_LOADED=0
SLACK_MODULE_AVAILABLE=0

#-------------------------------------------------------------------------------
# load_slack_config - Load Slack configuration from instance config directory
# Returns: 0 on success, 1 if disabled or invalid
#-------------------------------------------------------------------------------
load_slack_config() {
    local script_dir="${SCRIPT_DIR:-$(dirname "$(readlink -f "$0")")}"
    local instance="${CONFIG_INSTANCE:-default}"
    local config_file="$script_dir/config/${instance}/slack.conf"

    if [[ ! -f "$config_file" ]]; then
        SLACK_MODULE_AVAILABLE=0
        SLACK_ENABLED="no"
        return 1
    fi

    # shellcheck source=/dev/null
    if ! source "$config_file" 2>/dev/null; then
        echo "ERROR: Failed to load Slack config: $config_file" >&2
        SLACK_MODULE_AVAILABLE=0
        SLACK_ENABLED="no"
        return 1
    fi

    if [[ "${SLACK_ENABLED:-no}" != "yes" ]]; then
        SLACK_MODULE_AVAILABLE=0
        return 1
    fi

    if [[ -z "${SLACK_WEBHOOK_URL:-}" ]]; then
        echo "ERROR: SLACK_WEBHOOK_URL not set in $config_file" >&2
        SLACK_MODULE_AVAILABLE=0
        return 1
    fi

    SLACK_HOSTNAME="${SLACK_HOSTNAME:-$(hostname -s)}"
    SLACK_TITLE_PREFIX="${SLACK_TITLE_PREFIX:-[vmbackup]}"
    SLACK_ON_SUCCESS="${SLACK_ON_SUCCESS:-yes}"
    SLACK_ON_FAILURE="${SLACK_ON_FAILURE:-yes}"
    SLACK_TIMEOUT="${SLACK_TIMEOUT:-10}"

    if ! command -v curl >/dev/null 2>&1; then
        echo "WARNING: curl not found - Slack delivery will fail" >&2
    fi

    SLACK_MODULE_AVAILABLE=1
    SLACK_MODULE_LOADED=1
    return 0
}

#-------------------------------------------------------------------------------
# Helpers
#-------------------------------------------------------------------------------

# Format bytes as TiB/GiB/MiB/KiB/B (no awk; integer math is fine for ranges).
_slack_format_bytes() {
    local bytes="${1:-0}"
    [[ "$bytes" =~ ^[0-9]+$ ]] || { echo "0 B"; return; }
    if   (( bytes >= 1099511627776 )); then printf '%d.%d TiB' $((bytes/1099511627776)) $(((bytes%1099511627776)*10/1099511627776))
    elif (( bytes >= 1073741824 ));    then printf '%d.%d GiB' $((bytes/1073741824))    $(((bytes%1073741824)*10/1073741824))
    elif (( bytes >= 1048576 ));       then printf '%d.%d MiB' $((bytes/1048576))       $(((bytes%1048576)*10/1048576))
    elif (( bytes >= 1024 ));          then printf '%d.%d KiB' $((bytes/1024))          $(((bytes%1024)*10/1024))
    else                                    printf '%d B'     "$bytes"
    fi
}

# Compute duration in Xh Ym Zs given two "YYYY-MM-DD HH:MM:SS [TZ]" strings.
_slack_format_duration() {
    local start_epoch end_epoch diff
    start_epoch=$(date -d "$1" +%s 2>/dev/null) || return 1
    end_epoch=$(date -d "$2" +%s 2>/dev/null) || return 1
    diff=$(( end_epoch - start_epoch ))
    (( diff < 0 )) && diff=0
    printf '%dh %dm %02ds' $((diff/3600)) $((diff%3600/60)) $((diff%60))
}

# JSON-escape a string for inline embedding in a payload.
_slack_json_escape() {
    local s="$1"
    s=${s//\\/\\\\}
    s=${s//\"/\\\"}
    s=${s//$'\n'/\\n}
    s=${s//$'\r'/}
    s=${s//$'\t'/\\t}
    printf '%s' "$s"
}

#-------------------------------------------------------------------------------
# send_slack_notification - Build and POST the session summary
# Args:
#   $1 - start_time string
#   $2 - end_time string
#   $3 - overall_status (success|partial|failed|unknown)
# Returns: 0 on success, 1 on transport failure, 2 if intentionally skipped
#-------------------------------------------------------------------------------
send_slack_notification() {
    local start_time="${1:-unknown}"
    local end_time="${2:-$(date '+%Y-%m-%d %H:%M:%S %Z')}"
    local overall_status="${3:-unknown}"

    if [[ "${SLACK_MODULE_AVAILABLE:-0}" -ne 1 ]]; then
        return 2
    fi

    case "$overall_status" in
        success)
            [[ "${SLACK_ON_SUCCESS:-yes}" == "yes" ]] || return 2
            ;;
        partial|failed|unknown)
            [[ "${SLACK_ON_FAILURE:-yes}" == "yes" ]] || return 2
            ;;
    esac

    local color emoji status_label
    case "$overall_status" in
        success) color="#36a64f"; emoji=":white_check_mark:"; status_label="SUCCESS" ;;
        partial) color="#daa038"; emoji=":warning:";          status_label="PARTIAL" ;;
        failed)  color="#cc0000"; emoji=":rotating_light:";   status_label="FAILED"  ;;
        *)       color="#888888"; emoji=":grey_question:";    status_label="UNKNOWN" ;;
    esac

    # Pull session totals from SQLite if available; otherwise leave blank.
    local total=0 ok=0 fail=0 skip=0 excl=0 bytes=0
    if declare -f sqlite_query_session_summary >/dev/null 2>&1; then
        local row
        row=$(sqlite_query_session_summary 2>/dev/null | head -1)
        if [[ -n "$row" ]]; then
            IFS='|' read -r total ok fail skip excl bytes _status _stype <<<"$row"
        fi
    fi

    local size_h
    size_h=$(_slack_format_bytes "${bytes:-0}")
    local dur_h
    dur_h=$(_slack_format_duration "$start_time" "$end_time" 2>/dev/null) || dur_h="n/a"

    local instance="${CONFIG_INSTANCE:-default}"
    local title="${SLACK_TITLE_PREFIX} ${SLACK_HOSTNAME} — ${status_label}"
    local summary="VMs: ${ok:-0} ok / ${fail:-0} failed / ${skip:-0} skipped / ${excl:-0} excluded (total ${total:-0})"
    local meta="Size: ${size_h} | Duration: ${dur_h} | Instance: ${instance}"

    local payload
    payload=$(cat <<JSON
{
  "attachments": [
    {
      "color": "$color",
      "fallback": "$(_slack_json_escape "$title — $summary")",
      "title": "$(_slack_json_escape "$emoji $title")",
      "text": "$(_slack_json_escape "$summary")",
      "footer": "$(_slack_json_escape "$meta")",
      "ts": $(date +%s)
    }
  ]
}
JSON
)

    local http_code
    http_code=$(curl --silent --show-error --max-time "${SLACK_TIMEOUT:-10}" \
        --output /dev/null --write-out '%{http_code}' \
        -X POST -H 'Content-Type: application/json' \
        --data "$payload" \
        "$SLACK_WEBHOOK_URL" 2>/dev/null)

    if [[ "$http_code" =~ ^2 ]]; then
        return 0
    fi
    echo "ERROR: Slack webhook returned HTTP ${http_code:-no-response}" >&2
    return 1
}
