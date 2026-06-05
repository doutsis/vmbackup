#!/usr/bin/env bash
##############################################################################
# lib/bootstrap.sh — centralised lib loader for vmbackup / vmrestore
#
# UNI-321 (Phase 6 commit 1): single helper that replaces the 16 inline
# `if -r dev-tree; elif -r /opt; else die` source blocks scattered across
# vmbackup.sh and vmrestore.sh. Each binary keeps exactly ONE inline source
# block (loading bootstrap.sh + version.sh + exit_codes.sh — chicken/egg);
# every subsequent lib is loaded via `source_lib_or_die <name.sh>`.
#
# Discovery order matches the original blocks:
#   1) Dev tree:  <binary-dir>/lib/<name>
#   2) Install:   /opt/vmbackup/lib/<name>
# A miss is fatal (exit EXIT_DEPENDENCY=8). Modules MUST NOT call this
# helper directly — they rely on the binary having already loaded their
# dependencies (Phase 6 spec §2.1.1).
##############################################################################

# Idempotency: re-sourcing this file is a no-op once the helper is defined.
declare -F source_lib_or_die >/dev/null 2>&1 && return 0

# Discover lib root from this file's own location. readlink -f resolves the
# symlink at /usr/local/bin/vmbackup → /opt/vmbackup/vmbackup.sh case AND
# direct dev-tree invocation. Falls back to the canonical install path if
# discovery somehow fails (e.g. file sourced via `source` from stdin).
_BOOTSTRAP_LIB_DIR="$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")" && pwd 2>/dev/null)" || _BOOTSTRAP_LIB_DIR=""
[[ -d "$_BOOTSTRAP_LIB_DIR" ]] || _BOOTSTRAP_LIB_DIR="/opt/vmbackup/lib"

# source_lib_or_die <libname.sh>
#   Sources the named lib from the canonical location with /opt fallback.
#   Hard-fails (exit EXIT_DEPENDENCY=8 if defined, else 8) on miss.
source_lib_or_die() {
    local lib="$1"
    if [[ -r "$_BOOTSTRAP_LIB_DIR/$lib" ]]; then
        # shellcheck source=/dev/null
        source "$_BOOTSTRAP_LIB_DIR/$lib"
    elif [[ -r "/opt/vmbackup/lib/$lib" ]]; then
        # shellcheck source=/dev/null
        source "/opt/vmbackup/lib/$lib"
    else
        echo "Error: lib/$lib not found (checked $_BOOTSTRAP_LIB_DIR and /opt/vmbackup/lib)" >&2
        exit "${EXIT_DEPENDENCY:-8}"
    fi
}

export -f source_lib_or_die
