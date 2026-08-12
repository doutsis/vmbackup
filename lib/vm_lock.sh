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

  # Bounded acquire loop. The reap of a stale (dead/reused-PID) lock is the RACE
  # SEAM (FF-196). The old check-then-rm-then-create was non-atomic: a reaper
  # descheduled (SIGSTOP / NFS stall / starvation) BETWEEN judging the lock stale
  # and its `rm -f "$lock_file"` would, on resume, delete BY PATH whatever now sat
  # at "$lock_file" - including a sibling's freshly-acquired lock - yielding TWO
  # winners for one VM. We instead reap by atomically CLAIMING the CURRENT inode
  # via rename: mv "$lock_file" into a name only THIS process owns. rename is
  # atomic, so of N concurrent reapers exactly one moves a given inode - but the
  # SAFETY INVARIANT is the content re-check below, not the mv: the claim may be a
  # lock a sibling acquired since the judgment, so we act ONLY on the file we
  # uniquely own, delete it ONLY after re-verifying it is the judged stale holder,
  # and NEVER rm "$lock_file" by path after a stale judgment - so a sibling that
  # has since acquired a NEW "$lock_file" inode (via the noclobber create) can
  # never be deleted by us. A claimer resumed after a stall holds a uniquely-named
  # file no one can recreate: deleting THAT harms nobody, and its retry is a
  # normal noclobber race it can only win if free.
  local _attempt locked_pid _claim _claimed_pid
  for (( _attempt=0; _attempt<10; _attempt++ )); do
    # Fast path / acquire: atomic O_EXCL create. A free path -> we win outright.
    if ( set -o noclobber; echo "$$" > "$lock_file" ) 2>/dev/null; then
      log_debug "vm_lock.sh" "create_lock" "Lock acquired for VM '$vm_name' (PID $$, file=$lock_file)"
      return 0
    fi

    # Path occupied. Identify the holder from the current file content.
    locked_pid=$(cat "$lock_file" 2>/dev/null)
    if vm_lock_holder_live "$locked_pid"; then
      # Live legitimate holder (vmbackup/vmrestore/virtnbdbackup) — leave it alone.
      return 1
    fi

    # Stale (dead/reused PID). Atomically claim the CURRENT inode. If the rename
    # fails, another reaper already moved/removed it - just retry the acquire. The
    # transient name is ".reap-*.lock": it does NOT match the vmbackup-*.lock
    # namespace globbed by the session reaper / cleanup / emergency sites (so it can
    # never be mis-reaped or wedge them) yet ends in .lock, so the cloud **/*.lock
    # exclude and the staging locks/ exclude both drop it from replication.
    _claim="${LOCK_DIR}/.reap-$$-${_attempt}-${RANDOM}.lock"
    mv "$lock_file" "$_claim" 2>/dev/null || continue

    # We now uniquely own "$_claim"; "$lock_file" is free. The claimed file's
    # content is frozen (no other process references this name), so this check
    # stays valid across ANY stall. Remove it ONLY if it is the SAME dead holder we
    # judged: a different pid means our rename grabbed a lock a sibling acquired
    # between the judgment and the mv - never delete THAT; hand its content back to
    # the free path via noclobber (never clobbering a still-newer winner), stand down.
    _claimed_pid=$(cat "$_claim" 2>/dev/null)
    if [[ "$_claimed_pid" == "$locked_pid" ]] && ! vm_lock_holder_live "$_claimed_pid"; then
      log_warn "vm_lock.sh" "create_lock" \
        "Stale lock detected for VM: $vm_name (PID $locked_pid dead or reused) - removing and proceeding"
      rm -f "$_claim"
      # Loop: retry the noclobber acquire against the now-free path.
    else
      ( set -o noclobber; echo "$_claimed_pid" > "$lock_file" ) 2>/dev/null
      rm -f "$_claim"
      return 1
    fi
  done

  log_debug "vm_lock.sh" "create_lock" "Lock acquisition FAILED for VM '$vm_name' (another process holds lock)"
  return 1
}

# Remove lock file
remove_lock() {
  local vm_name=${1:?Error: vm_name required}
  local lock_file
  lock_file=$(vm_lock_file "$vm_name") || return 1
  log_debug "vm_lock.sh" "remove_lock" "Releasing lock for VM '$vm_name' (file=$lock_file)"
  rm -f "$lock_file"
}
