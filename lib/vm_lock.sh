#!/usr/bin/env bash
#
# lib/vm_lock.sh — Per-VM locking for vmbackup/vmrestore
#
# Requires: $LOCK_DIR set before sourcing
# Uses: log_warn, log_debug (from lib/logging.sh)

# Create lock file - ATOMIC creation to prevent race condition
create_lock() {
  local vm_name=${1:?Error: vm_name required}
  local lock_file="$LOCK_DIR/vmbackup-${vm_name}.lock"

  mkdir -p "$LOCK_DIR"

  # Check for stale locks
  if [[ -f "$lock_file" ]]; then
    local locked_pid=$(cat "$lock_file" 2>/dev/null)

    if [[ -n "$locked_pid" ]] && kill -0 "$locked_pid" 2>/dev/null; then
      # Verify process is actually our script (prevent PID wrapping vulnerability)
      local proc_cmdline=$(cat "/proc/$locked_pid/cmdline" 2>/dev/null | tr '\0' ' ')
      if [[ "$proc_cmdline" == *"vmbackup"* ]] || [[ "$proc_cmdline" == *"vmrestore"* ]] || [[ "$proc_cmdline" == *"virtnbdbackup"* ]]; then
        return 1
      else
        log_warn "vm_lock.sh" "create_lock" \
          "Stale lock detected for VM: $vm_name (PID: $locked_pid was reused by different process) - removing and proceeding"
        rm -f "$lock_file"
      fi
    else
      log_warn "vm_lock.sh" "create_lock" \
        "Stale lock detected for VM: $vm_name (PID: $locked_pid no longer running) - removing and proceeding"
      rm -f "$lock_file"
    fi
  fi

  # Atomic lock creation with noclobber — only one process can succeed
  if ( set -o noclobber; echo "$$" > "$lock_file" ) 2>/dev/null; then
    log_debug "vm_lock.sh" "create_lock" "Lock acquired for VM '$vm_name' (PID $$, file=$lock_file)"
    return 0
  else
    log_debug "vm_lock.sh" "create_lock" "Lock acquisition FAILED for VM '$vm_name' (another process holds lock)"
    return 1
  fi
}

# Remove lock file
remove_lock() {
  local vm_name=${1:?Error: vm_name required}
  local lock_file="$LOCK_DIR/vmbackup-${vm_name}.lock"
  log_debug "vm_lock.sh" "remove_lock" "Releasing lock for VM '$vm_name' (file=$lock_file)"
  rm -f "$lock_file"
}
