#!/usr/bin/env bash
#
# lib/vm_lock.sh — Per-VM locking for vmbackup/vmrestore
#
# Requires: $LOCK_DIR set before sourcing
# Uses: log_warn, log_debug (from lib/logging.sh)

# UNI-321: idempotency guard — re-source is a no-op once create_lock is defined.
declare -F create_lock >/dev/null 2>&1 && return 0

# LOCK-01 (118-spaces): derive the per-VM lock-file path from the vm_fs_name
# TOKEN, not the raw name. The lock provides mutual exclusion BETWEEN vmbackup
# and vmrestore; if one keyed the file by the raw spaced name and the other by
# the token they would use different files and stop excluding. Routing every
# lock-path construction through this one helper keeps both binaries (and the
# has_lock / interrupt / emergency-cleanup sites) in agreement. Fails closed if
# the token can't be derived (vm_fs_name returns non-zero) — better to abort the
# lock op than to lock the wrong file.
vm_lock_file() {
  local vm_name=${1:?Error: vm_name required}
  local _tok
  _tok=$(vm_fs_name "$vm_name") || return 1
  printf '%s' "${LOCK_DIR}/vmbackup-${_tok}.lock"
}

# vm_lock_holder_live — CANONICAL test of per-VM lock-holder legitimacy.
#
# Returns 0 iff PID $1 is (a) alive (kill -0) AND (b) running one of the three
# programs that may legitimately hold a vmbackup-<token>.lock: vmbackup,
# vmrestore, or virtnbdbackup. An empty/missing PID, a dead PID, or a PID reused
# by an unrelated program → return 1 (the caller then reaps the stale lock).
#
# This is the ONE place the holder allowlist is defined. FF-6 (has_lock) and
# FF-24 (the session stale-lock reaper) were live-lock-destroying bugs that arose
# precisely because those sites hand-rolled their own cmdline test and DRIFTED
# from this list: has_lock accepted only "vmbackup", the reaper only "vmbackup"/
# "virtnbdbackup" — so a live vmrestore (or virtnbdbackup) holder was misclassed
# as PID-reuse and its lock deleted mid-restore. All three consumers now route
# through this predicate so the acceptor set can never drift again.
#
# nounset-safe (this file is shared by vmrestore's `set -u`). No process spawns
# beyond the single cat|tr /proc read the previous sites already did — has_lock
# is on every backup's hot path.
vm_lock_holder_live() {
  local pid="${1:-}"
  [[ -n "$pid" ]] || return 1
  kill -0 "$pid" 2>/dev/null || return 1
  local proc_cmdline
  proc_cmdline=$(cat "/proc/$pid/cmdline" 2>/dev/null | tr '\0' ' ')
  [[ "$proc_cmdline" == *"vmbackup"* || "$proc_cmdline" == *"vmrestore"* || "$proc_cmdline" == *"virtnbdbackup"* ]]
}

# Create lock file - ATOMIC creation to prevent race condition
create_lock() {
  local vm_name=${1:?Error: vm_name required}
  local lock_file
  lock_file=$(vm_lock_file "$vm_name") || return 1

  mkdir -p "$LOCK_DIR"

  # Check for stale locks
  if [[ -f "$lock_file" ]]; then
    local locked_pid=$(cat "$lock_file" 2>/dev/null)

    if vm_lock_holder_live "$locked_pid"; then
      # Live legitimate holder (vmbackup/vmrestore/virtnbdbackup) — leave it alone.
      return 1
    else
      log_warn "vm_lock.sh" "create_lock" \
        "Stale lock detected for VM: $vm_name (PID $locked_pid dead or reused) - removing and proceeding"
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
  local lock_file
  lock_file=$(vm_lock_file "$vm_name") || return 1
  log_debug "vm_lock.sh" "remove_lock" "Releasing lock for VM '$vm_name' (file=$lock_file)"
  rm -f "$lock_file"
}
