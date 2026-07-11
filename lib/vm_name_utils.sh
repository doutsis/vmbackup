#!/usr/bin/env bash
#
# lib/vm_name_utils.sh — VM-name handling for vmbackup/vmrestore.
#
# 118-spaces: libvirt domain names legally contain spaces (and other characters
# outside [A-Za-z0-9._-]). The B3/R3 strict reject `sanitize_vm_name` assumed
# they did not and silently stopped backing up such VMs (NAME-01). This file
# replaces that single overloaded function with TWO separated concerns:
#
#   sanitize_vm_name <name>   GUARD — reject only genuinely dangerous names
#                             (empty, exactly "." or "..", embedded newline/NUL).
#                             Echoes the validated REAL name. Applied at the
#                             CLI/argv boundary (--vm, --prune, restore target).
#
#   vm_fs_name <name>         ENCODER — map a real name to a filesystem-safe,
#                             injective token used ONLY for on-disk paths:
#                               - a name already == its own slug -> returned
#                                 unchanged (no migration for the 99% of VMs).
#                               - otherwise  <slug>-<hash>, where slug replaces
#                                 every char outside [A-Za-z0-9._-] with '_' and
#                                 hash is the first $VMFS_HASH_HEX hex of the
#                                 SHA-256 of the REAL name. Distinct names ->
#                                 distinct tokens. Output is entirely
#                                 [A-Za-z0-9._-] (local-FS, bash, libvirt AND
#                                 SharePoint/rclone safe).
#
#   vm_fs_name_legacy <name>  the pre-118 lossy slug (space->_, NO hash). Used
#                             ONLY to LOCATE a not-yet-migrated pre-rc3 folder
#                             (restore fallback + MIG-01 migration).
#
# The REAL name (with spaces) keeps flowing to virsh, SQLite, logs and reports;
# a vm_fs_name token is ONLY ever a folder name.
#
# DETERMINISM is load-bearing: the token computed at BACKUP must equal the one
# computed at RESTORE and at MIGRATION, across locales and BOTH binaries, or
# written data becomes unreachable (silent). Hence:
#   * LC_ALL=C while computing the slug — the `${//[^...]/_}` bracket match is
#     locale-sensitive for multibyte (UTF-8) names (e.g. cafe-with-accent slugs
#     to a different number of '_' under C vs C.UTF-8).
#   * `printf '%s'` (no trailing newline) into sha256sum.
#   * ONE shared width constant ($VMFS_HASH_HEX) — a mismatch = restore looks in
#     the wrong folder.
# The hash branch fails CLOSED: a missing/failed sha256sum must abort that VM,
# never emit a hashless "${slug}-" token (which would merge distinct VMs into one
# folder — the exact UNI-011 data-loss this design prevents).
#
# Requires: $EXIT_USAGE set before sourcing (both binaries define it). -u-clean
# (this lib is reached under vmrestore's `set -uo pipefail`). Does not require
# lib/logging.sh — uses bare echo to stderr so it is safe to source very early.
#

# Idempotency guard — re-source is a no-op once the API is defined.
declare -F vm_fs_name >/dev/null 2>&1 && return 0

# Width (hex characters) of the disambiguating hash suffix. ONE definition,
# shared by both binaries — backup and restore MUST agree on the length. 12 hex
# = 48 bits: accidental same-slug collisions stay below 1e-9 even at absurd fleet
# sizes, while staying short and cloud-safe.
: "${VMFS_HASH_HEX:=12}"

# ----------------------------------------------------------------------------
# GUARD: validate an untrusted (CLI) VM name.
#
# Reject ONLY names the encoder cannot neutralise into a safe path component:
# empty, exactly "." or "..", or containing a newline/NUL. Everything else
# (spaces and other printables) is a legal libvirt name and is handled by
# vm_fs_name. Prints the validated REAL name on stdout; exits EXIT_USAGE on
# rejection.
#
# CALLING CONVENTION: command substitution $(...) runs in a subshell, so the
# exit only kills the subshell unless the caller propagates. Always:
#     local safe; safe=$(sanitize_vm_name "$name") || exit $?     # binaries
#     local safe; safe=$(sanitize_vm_name "$name") || return $?   # lib/modules
# NOT: local safe=$(sanitize_vm_name "$name")   # WRONG: `local` swallows $?
# ----------------------------------------------------------------------------
sanitize_vm_name() {
  local name="${1:-}"
  # Note: a bash string cannot contain a NUL byte, so only newline needs an
  # explicit check here (a NUL `*$'\0'*` pattern degenerates to `**` and would
  # reject everything).
  if [[ -z "$name" || "$name" == "." || "$name" == ".." || "$name" == *$'\n'* ]]; then
    echo "Error: invalid VM name '$name'" >&2
    echo "       (must not be empty, '.', '..', or contain a newline)" >&2
    exit "${EXIT_USAGE:-7}"
  fi
  printf '%s' "$name"
}

# ----------------------------------------------------------------------------
# ENCODER: real name -> injective filesystem token (see file header).
#
# Infallible on the SAFE path. On the unsafe path returns 1 if the hash cannot
# be computed (fail CLOSED — never emit a hashless token). Callers building a
# path use `safe=$(vm_fs_name "$name") || return/exit $?` so a hash failure
# aborts that VM rather than mis-filing it.
# ----------------------------------------------------------------------------
vm_fs_name() {
  local name="${1:-}"
  # Force byte-based (C) locale so the bracket match is deterministic for
  # multibyte names. `local LC_ALL=C` triggers bash's setlocale for the function
  # scope and is restored on return.
  local LC_ALL=C
  local slug="${name//[^A-Za-z0-9._-]/_}"
  if [[ "$slug" == "$name" ]]; then
    printf '%s' "$name"                       # already FS-safe -> unchanged
    return 0
  fi
  local hash
  hash=$(printf '%s' "$name" | sha256sum 2>/dev/null | cut -c1-"${VMFS_HASH_HEX}") \
    || return 1                               # split-assign: pipefail rc NOT swallowed
  if [[ -z "$hash" ]]; then                   # missing/failed sha256sum -> FAIL CLOSED
    echo "Error: vm_fs_name: sha256sum unavailable; cannot derive a unique" >&2
    echo "       backup folder for '$name' (refusing to mis-file)." >&2
    return 1
  fi
  printf '%s-%s' "$slug" "$hash"
}

# ----------------------------------------------------------------------------
# LEGACY slug: the pre-118 lossy mapping (space/special -> '_', NO hash). Used
# ONLY to LOCATE a not-yet-migrated pre-rc3 folder (restore new->legacy fallback
# and the MIG-01 migrator). Infallible. Same LC_ALL=C pin as the encoder.
# ----------------------------------------------------------------------------
vm_fs_name_legacy() {
  local name="${1:-}"
  local LC_ALL=C
  printf '%s' "${name//[^A-Za-z0-9._-]/_}"
}
