#!/usr/bin/env bash
#
# lib/logging.sh — Unified logging for vmbackup/vmrestore
#
# Requires: $LOG_FILE set before sourcing
# Optional: $LOG_LEVEL (default: INFO)
#
# Levels (highest to lowest): ERROR > WARN > INFO > DEBUG
# All messages always written to log file. Only messages at or above
# the configured level are shown on stderr.

# Map log levels to numeric values for comparison
declare -A _LOG_LEVELS=([ERROR]=3 [WARN]=2 [INFO]=1 [DEBUG]=0)

# Get numeric value for configured log level (default: INFO)
_get_log_level_value() {
  local level="${LOG_LEVEL:-INFO}"
  level="${level^^}"
  echo "${_LOG_LEVELS[$level]:-1}"
}

# Log function with timestamp, process, function context
log_msg() {
  local level=$1
  local process=$2
  local function=$3
  local message=$4

  local timestamp=$(date '+%Y-%m-%d %H:%M:%S %Z')
  local log_line="[$timestamp] [$level] [$process] [$function] $message"

  # Always write to log file
  echo "$log_line" >> "$LOG_FILE"

  # Only display on screen if message level >= configured level
  local msg_level="${_LOG_LEVELS[$level]:-1}"
  local config_level
  config_level=$(_get_log_level_value)

  if [[ $msg_level -ge $config_level ]]; then
    echo "$log_line" >&2
  fi
}

log_info() {
  log_msg "INFO" "$1" "$2" "$3"
}

log_warn() {
  log_msg "WARN" "$1" "$2" "$3"
}

log_error() {
  log_msg "ERROR" "$1" "$2" "$3"
}

log_debug() {
  log_msg "DEBUG" "$1" "$2" "$3"
}
