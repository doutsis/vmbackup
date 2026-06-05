#!/usr/bin/env bash
#
# lib/vm_name_utils.sh — Strict VM-name sanitisation for vmbackup/vmrestore.
#
# UNI-011: replaces the previously-undefined `sanitize_vm_name` symbol that
# vmbackup.sh called from 10 sites in --prune mode (silent retargeting bug:
# the call returned empty, collapsing "${BACKUP_PATH}${safe_name}/<period>"
# to "${BACKUP_PATH}/<period>" and operating on a sibling of the intended
# VM, or on all VMs when the call site used a glob).
#
# Contract: REJECT-not-substitute. Names containing anything outside
# [A-Za-z0-9._-] are an error and exit EXIT_USAGE. This matches libvirt's
# own naming rules and is safer than substituting underscores (which would
# silently merge backups for VM names that differ only in special chars).
#
# Requires: $EXIT_USAGE set before sourcing (defined in both binaries
# today; will move to lib/exit_codes.sh in a later commit). Does not
# require lib/logging.sh — uses bare echo to stderr so this lib is safe
# to source very early in startup.
#

# UNI-321: idempotency guard — re-source is a no-op once sanitize_vm_name is defined.
declare -F sanitize_vm_name >/dev/null 2>&1 && return 0
# UNI-011 followup (out of scope): vmbackup.sh L3980 has a separate inline
# substitute-with-underscore sanitiser used by a different caller. That
# caller's contract is substitute-not-reject, so it is intentionally NOT
# replaced by sanitize_vm_name() here. Audit and reconcile in a later phase.

# Reject names containing anything outside [A-Za-z0-9._-].
# Print the validated name on stdout; exit EXIT_USAGE on rejection.
#
# CALLING CONVENTION: bash command substitution $(...) runs in a subshell,
# so exit-on-rejection only kills the subshell unless the caller propagates.
# Always call as:
#
#     local safe
#     safe=$(sanitize_vm_name "$name") || exit $?
#
# NOT:
#     local safe=$(sanitize_vm_name "$name")   # WRONG: `local` swallows $?
#
# This split-then-test pattern is mandatory; the linting in P2-T1
# (silent-retargeting regression) depends on it.
sanitize_vm_name() {
  local name="${1:-}"
  if [[ -z "$name" ]]; then
    echo "Error: VM name is empty" >&2
    exit "${EXIT_USAGE:-7}"
  fi
  if [[ "$name" =~ [^A-Za-z0-9._-] ]]; then
    echo "Error: VM name '$name' contains invalid characters." >&2
    echo "       Allowed: A-Z a-z 0-9 . _ -" >&2
    echo "       (Matches libvirt VM-name rules.)" >&2
    exit "${EXIT_USAGE:-7}"
  fi
  printf '%s' "$name"
}
