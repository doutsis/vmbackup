#!/bin/bash

################################################################################
# FSTRIM OPTIMIZATION MODULE
# Purpose: Optional module for pre-backup filesystem trimming in guest VMs
# Design: Dynamically loaded by vmbackup.sh if FSTRIM_ENABLED=true
# 
# This module handles:
# - Linux filesystem trim via QEMU agent guest-fstrim
# - Windows VSS trim via QEMU agent guest-fstrim with safety pause
# - Timeout handling and error recovery
# - Per-OS response parsing (Linux has "trimmed" field, Windows doesn't)
#
# CONFIGURATION VARIABLES (defined in vmbackup.sh):
#   FSTRIM_ENABLED=true/false         - Enable/disable module
#   FSTRIM_TIMEOUT=300                - Timeout for fstrim on Linux guests (seconds)
#   FSTRIM_WINDOWS_TIMEOUT=600        - Timeout for fstrim on Windows guests (seconds)
#
# Module Status: OPTIONAL
# If not loaded, FSTRIM operations become no-ops (safe fallback)
################################################################################

# Execute fstrim in guest OS (requires QEMU agent) - OPTIMIZED for single virsh call
execute_fstrim_in_guest() {
  local vm_name=$1
  local fstrim_output
  local is_windows=false
  
  # Ensure timeout variables have defaults
  : "${FSTRIM_TIMEOUT:=300}"
  : "${FSTRIM_WINDOWS_TIMEOUT:=600}"
  
  # Check if QEMU agent is available first (use explicit timeout to avoid 5s default)
  virsh qemu-agent-command --timeout 10 "$vm_name" '{"execute":"guest-info"}' &>/dev/null
  if [[ $? -ne 0 ]]; then
    log_info "fstrim_optimization_module.sh" "execute_fstrim_in_guest" "QEMU agent not available for VM: $vm_name, skipping trim"
    return 0
  fi
  
  # Detect guest OS BEFORE running fstrim so we can apply the correct timeout
  # Windows guest-fstrim runs synchronously on the agent thread and can take 8+ minutes
  # on nested KVM / virtiofs, so it needs a longer timeout than Linux (~1-2s)
  local osinfo
  osinfo=$(virsh qemu-agent-command --timeout 10 "$vm_name" '{"execute":"guest-get-osinfo"}' 2>/dev/null)
  if echo "$osinfo" | grep -qi '"id".*"mswindows"'; then
    is_windows=true
  fi
  
  # Select timeout based on detected OS
  local effective_timeout="$FSTRIM_TIMEOUT"
  if [[ "$is_windows" == true ]]; then
    effective_timeout="$FSTRIM_WINDOWS_TIMEOUT"
    log_info "fstrim_optimization_module.sh" "execute_fstrim_in_guest" "Windows guest detected — using FSTRIM_WINDOWS_TIMEOUT=${effective_timeout}s for VM: $vm_name"
  fi
  
  log_info "fstrim_optimization_module.sh" "execute_fstrim_in_guest" "Executing guest-fstrim for VM: $vm_name (timeout: ${effective_timeout}s)"
  
  # Use native guest-fstrim command - works on both Linux and Windows
  # Capture output to log trim results and detect OS type
  # NOTE: virsh has a 5-second default agent timeout which is too short for
  # guest-fstrim (especially on nested KVM / virtiofs). Pass --timeout explicitly
  # to allow the operation to complete. The external timeout wrapper remains as
  # a safety net in case virsh itself hangs.
  fstrim_output=$(timeout "$effective_timeout" virsh qemu-agent-command \
      --timeout "$effective_timeout" "$vm_name" '{"execute":"guest-fstrim"}' 2>&1)
  local trim_status=$?
  
  if [[ $trim_status -eq 0 ]]; then
    # Parse trim results — OS was already detected pre-fstrim via guest-get-osinfo
    if [[ "$is_windows" == true ]]; then
      # Windows retrim completes synchronously — verified via Windows Event Log
      # (Microsoft-Windows-Defrag event 258 fires before guest-fstrim returns)
      log_info "fstrim_optimization_module.sh" "execute_fstrim_in_guest" "guest-fstrim completed for Windows VM: $vm_name"
    else
      # Linux system - has trimmed metrics
      local bytes_trimmed=$(echo "$fstrim_output" | grep -oP '"trimmed"\s*:\s*\K[0-9]+' | head -1)
      if [[ -n "$bytes_trimmed" ]]; then
        log_info "fstrim_optimization_module.sh" "execute_fstrim_in_guest" "guest-fstrim completed for Linux VM: $vm_name (bytes trimmed: $bytes_trimmed)"
      else
        log_info "fstrim_optimization_module.sh" "execute_fstrim_in_guest" "guest-fstrim completed for Linux VM: $vm_name"
      fi
    fi
  else
    if [[ $trim_status -eq 124 ]]; then
      log_warn "fstrim_optimization_module.sh" "execute_fstrim_in_guest" "guest-fstrim timed out for VM: $vm_name after ${effective_timeout}s (continuing with backup)"
    else
      log_warn "fstrim_optimization_module.sh" "execute_fstrim_in_guest" "guest-fstrim failed for VM: $vm_name (exit code: $trim_status, continuing with backup)"
    fi
  fi
  
  return 0
}

# Public API: Apply fstrim optimization with safety checks
apply_fstrim_optimization() {
  local vm_name=$1
  
  # Guard: Check if module is enabled
  if [[ -z "$FSTRIM_ENABLED" ]] || [[ "$FSTRIM_ENABLED" != "true" ]]; then
    log_debug "fstrim_optimization_module.sh" "apply_fstrim_optimization" "Module disabled (FSTRIM_ENABLED=$FSTRIM_ENABLED)"
    return 0  # Module disabled, return success
  fi
  
  # Guard: Check dependencies
  if ! command -v virsh &>/dev/null; then
    log_warn "fstrim_optimization_module.sh" "apply_fstrim_optimization" "virsh not found - fstrim optimization skipped"
    return 0  # Missing dependency, return success
  fi
  
  if ! command -v timeout &>/dev/null; then
    log_warn "fstrim_optimization_module.sh" "apply_fstrim_optimization" "timeout command not found - fstrim optimization skipped"
    return 0  # Missing dependency, return success
  fi
  
  log_info "vmbackup.sh" "apply_fstrim_optimization" "Checking if fstrim optimization can be applied for VM: $vm_name"
  
  # Check if guest is running
  local status=$(virsh domstate "$vm_name" 2>/dev/null)
  if [[ "$status" != "running" ]]; then
    log_info "fstrim_optimization_module.sh" "apply_fstrim_optimization" "VM not running, skipping fstrim hint"
    return 0
  fi
  
  # Check if agent is available and try trim (use explicit timeout to avoid 5s default)
  if virsh qemu-agent-command --timeout 10 "$vm_name" '{"execute":"guest-info"}' &>/dev/null; then
    log_info "fstrim_optimization_module.sh" "apply_fstrim_optimization" "QEMU agent available - attempting filesystem trim before incremental backup"
    execute_fstrim_in_guest "$vm_name"
  fi
  
  return 0
}

# Module initialization marker
FSTRIM_OPTIMIZATION_MODULE_LOADED=1

return 0
