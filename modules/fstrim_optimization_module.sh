#!/bin/bash

###############################################################################
# FSTRIM OPTIMIZATION MODULE
# Purpose: Optional module for pre-backup filesystem trimming in guest VMs
# Design: Dynamically loaded by vmbackup.sh if ENABLE_FSTRIM=true
#
# Caller contract:
#   - Agent availability is already confirmed (guest-ping) before calling
#   - OS type is detected once by the caller and passed as $2
#   - This function issues a single virsh agent call: guest-fstrim
#
# CONFIGURATION VARIABLES (defined in vmbackup.sh / vmbackup.conf):
#   FSTRIM_TIMEOUT=300                - Timeout for fstrim on Linux guests (seconds)
#   FSTRIM_WINDOWS_TIMEOUT=600        - Timeout for fstrim on Windows guests (seconds)
#   FSTRIM_MINIMUM=1048576            - Minimum free range to trim in bytes (Linux only)
#   FSTRIM_EXCLUDE_FILE               - Path to VM exclusion patterns for FSTRIM
#
# Module Status: OPTIONAL
# If not loaded, FSTRIM operations become no-ops (safe fallback)
###############################################################################

# Check if a VM is excluded from FSTRIM by pattern matching.
# Args: $1=vm_name
# Returns: 0 if excluded, 1 if not excluded
_fstrim_is_vm_excluded() {
  local vm_name=$1
  local config_dir="${CONFIG_DIR:-}"
  local exclude_file="${FSTRIM_EXCLUDE_FILE:-fstrim_exclude.conf}"
  local exclude_path=""

  # Resolve full path to exclude file
  if [[ -n "$config_dir" && -f "$config_dir/$exclude_file" ]]; then
    exclude_path="$config_dir/$exclude_file"
  elif [[ -f "$exclude_file" ]]; then
    exclude_path="$exclude_file"
  fi

  [[ -z "$exclude_path" ]] && return 1

  local pattern
  while IFS= read -r pattern || [[ -n "$pattern" ]]; do
    # Skip comments and empty lines
    pattern=$(echo "$pattern" | sed 's/#.*//' | xargs)
    [[ -z "$pattern" ]] && continue

    # shellcheck disable=SC2254
    if [[ "$vm_name" == $pattern ]]; then
      log_info "fstrim_optimization_module.sh" "_fstrim_is_vm_excluded" \
        "VM $vm_name matches FSTRIM exclude pattern: $pattern"
      return 0
    fi
  done < "$exclude_path"

  return 1
}

# Parse per-path TRIM results from guest-fstrim JSON response.
# Logs each filesystem path with its trim result.
# Args: $1=vm_name  $2=os_type  $3=fstrim_json_output
# Outputs: total_bytes_trimmed to stdout (for capture), logs per-path detail
_fstrim_parse_results() {
  local vm_name=$1
  local os_type=$2
  local output=$3
  local total_trimmed=0
  local path_count=0
  local error_count=0

  # Extract the paths array content — works for both Linux and Windows responses
  # Linux example:  {"return":{"paths":[{"minimum":0,"path":"/","trimmed":123456}]}}
  # Windows example: {"return":{"paths":[{"path":"C:\\"},{"path":"E:\\"}]}}

  # Use grep to extract individual path objects
  local paths
  paths=$(echo "$output" | grep -oP '\{[^{}]*"path"\s*:\s*"[^"]*"[^{}]*\}')

  if [[ -z "$paths" ]]; then
    log_debug "fstrim_optimization_module.sh" "_fstrim_parse_results" \
      "No per-path results found in response for $vm_name"
    echo "0"
    return
  fi

  while IFS= read -r path_obj; do
    local fs_path trimmed minimum error_msg

    fs_path=$(echo "$path_obj" | grep -oP '"path"\s*:\s*"\K[^"]+')
    trimmed=$(echo "$path_obj" | grep -oP '"trimmed"\s*:\s*\K[0-9]+')
    minimum=$(echo "$path_obj" | grep -oP '"minimum"\s*:\s*\K[0-9]+')
    error_msg=$(echo "$path_obj" | grep -oP '"error"\s*:\s*"\K[^"]+')

    ((path_count++))

    if [[ -n "$error_msg" ]]; then
      ((error_count++))
      log_warn "fstrim_optimization_module.sh" "_fstrim_parse_results" \
        "FSTRIM $vm_name [$fs_path]: ERROR — $error_msg"
    elif [[ -n "$trimmed" ]]; then
      # Linux — has trimmed byte count
      local human_trimmed
      if (( trimmed >= 1073741824 )); then
        human_trimmed="$(awk "BEGIN{printf \"%.2f GB\", $trimmed/1073741824}")"
      elif (( trimmed >= 1048576 )); then
        human_trimmed="$(awk "BEGIN{printf \"%.1f MB\", $trimmed/1048576}")"
      elif (( trimmed >= 1024 )); then
        human_trimmed="$(( trimmed / 1024 )) KB"
      else
        human_trimmed="$trimmed bytes"
      fi
      log_info "fstrim_optimization_module.sh" "_fstrim_parse_results" \
        "FSTRIM $vm_name [$fs_path]: trimmed $human_trimmed (minimum: ${minimum:-0})"
      total_trimmed=$(( total_trimmed + trimmed ))
    else
      # Windows — no trimmed byte count, just path reported
      log_info "fstrim_optimization_module.sh" "_fstrim_parse_results" \
        "FSTRIM $vm_name [$fs_path]: completed (Windows — byte count not available)"
    fi
  done <<< "$paths"

  # Summary line
  if [[ "$os_type" == "linux" ]] || [[ "$os_type" == "unknown" ]]; then
    local human_total
    if (( total_trimmed >= 1073741824 )); then
      human_total="$(awk "BEGIN{printf \"%.2f GB\", $total_trimmed/1073741824}")"
    elif (( total_trimmed >= 1048576 )); then
      human_total="$(awk "BEGIN{printf \"%.1f MB\", $total_trimmed/1048576}")"
    else
      human_total="$total_trimmed bytes"
    fi
    log_info "fstrim_optimization_module.sh" "_fstrim_parse_results" \
      "FSTRIM $vm_name: $path_count filesystem(s), total trimmed: $human_total, errors: $error_count"
  else
    log_info "fstrim_optimization_module.sh" "_fstrim_parse_results" \
      "FSTRIM $vm_name: $path_count filesystem(s) trimmed, errors: $error_count"
  fi

  echo "$total_trimmed"
}

# Execute guest-fstrim via QEMU agent.
# Args: $1=vm_name  $2=os_type ("linux"|"windows"|"unknown", default "linux")
# Caller must have already verified the agent is responsive.
# Always returns 0 — trim failure is non-fatal.
execute_fstrim_in_guest() {
  local vm_name=$1
  local os_type=${2:-linux}

  # Check FSTRIM exclusion
  if _fstrim_is_vm_excluded "$vm_name"; then
    log_info "fstrim_optimization_module.sh" "execute_fstrim_in_guest" \
      "Skipping FSTRIM for excluded VM: $vm_name"
    return 0
  fi

  # Ensure timeout variables have defaults
  : "${FSTRIM_TIMEOUT:=300}"
  : "${FSTRIM_WINDOWS_TIMEOUT:=600}"
  : "${FSTRIM_MINIMUM:=1048576}"

  # Select timeout based on caller-provided OS type
  local effective_timeout="$FSTRIM_TIMEOUT"
  if [[ "$os_type" == "windows" ]]; then
    effective_timeout="$FSTRIM_WINDOWS_TIMEOUT"
  fi

  # Build the guest-fstrim command JSON
  # Linux: pass minimum parameter to skip small free ranges (significant speedup)
  # Windows: minimum is ignored by the QEMU agent (defrag.exe doesn't accept it),
  #          but we omit it for clarity since it has no effect.
  local fstrim_cmd='{"execute":"guest-fstrim"}'
  if [[ "$os_type" != "windows" && "$FSTRIM_MINIMUM" -gt 0 ]] 2>/dev/null; then
    fstrim_cmd="{\"execute\":\"guest-fstrim\",\"arguments\":{\"minimum\":$FSTRIM_MINIMUM}}"
  fi

  log_info "fstrim_optimization_module.sh" "execute_fstrim_in_guest" \
    "Executing guest-fstrim for VM: $vm_name (os: $os_type, timeout: ${effective_timeout}s, minimum: ${FSTRIM_MINIMUM})"

  # NOTE: virsh has a 5-second default agent timeout which is too short for
  # guest-fstrim (especially on nested KVM / virtiofs). Pass --timeout explicitly
  # to allow the operation to complete. The external timeout wrapper remains as
  # a safety net in case virsh itself hangs.
  local fstrim_output
  local trim_start_epoch
  trim_start_epoch=$(date +%s)

  fstrim_output=$(timeout "$effective_timeout" virsh qemu-agent-command \
      --timeout "$effective_timeout" "$vm_name" "$fstrim_cmd" 2>&1)
  local trim_status=$?

  local trim_end_epoch
  trim_end_epoch=$(date +%s)
  local trim_duration=$(( trim_end_epoch - trim_start_epoch ))

  if [[ $trim_status -eq 0 ]]; then
    log_debug "fstrim_optimization_module.sh" "execute_fstrim_in_guest" \
      "guest-fstrim raw response: $fstrim_output"

    # Parse and log per-path results
    local total_trimmed
    total_trimmed=$(_fstrim_parse_results "$vm_name" "$os_type" "$fstrim_output")

    log_info "fstrim_optimization_module.sh" "execute_fstrim_in_guest" \
      "guest-fstrim completed for $vm_name in ${trim_duration}s"

    # Store results for SQLite logging (picked up by caller)
    FSTRIM_LAST_DURATION=$trim_duration
    FSTRIM_LAST_BYTES_TRIMMED=${total_trimmed:-0}
    FSTRIM_LAST_STATUS="success"
    FSTRIM_LAST_OUTPUT="$fstrim_output"

  elif [[ $trim_status -eq 124 ]]; then
    log_warn "fstrim_optimization_module.sh" "execute_fstrim_in_guest" \
      "guest-fstrim timed out for VM: $vm_name after ${effective_timeout}s (continuing with backup)"
    FSTRIM_LAST_DURATION=$trim_duration
    FSTRIM_LAST_BYTES_TRIMMED=0
    FSTRIM_LAST_STATUS="timeout"
    FSTRIM_LAST_OUTPUT=""
  else
    log_warn "fstrim_optimization_module.sh" "execute_fstrim_in_guest" \
      "guest-fstrim failed for VM: $vm_name (exit code: $trim_status, continuing with backup)"
    log_debug "fstrim_optimization_module.sh" "execute_fstrim_in_guest" \
      "guest-fstrim error output: $fstrim_output"
    FSTRIM_LAST_DURATION=$trim_duration
    FSTRIM_LAST_BYTES_TRIMMED=0
    FSTRIM_LAST_STATUS="failed"
    FSTRIM_LAST_OUTPUT="$fstrim_output"
  fi

  return 0
}

# Module initialization marker
FSTRIM_OPTIMIZATION_MODULE_LOADED=1

return 0
