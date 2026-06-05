#!/usr/bin/env bash
##############################################################################
# lib/dry_run.sh — canonical dry-run predicate API
#
# UNI-322 (Phase 7 commit 1): single source of truth for dry-run gating
# across vmbackup.sh, vmrestore.sh, and the modules that read their globals.
# Replaces ~38 binary-global guard sites of the form
# `[[ "$DRY_RUN" == "true" ]]` / `[[ "$OPT_DRY_RUN" == "true" ]]`
# with three named predicates.
#
# THREE TRUTH CONVENTIONS EXIST in the codebase (see 109-phase7-spec.md
# §1.3.2). This lib covers convention A only:
#
#   A. true/false strings  — DRY_RUN (vmbackup), OPT_DRY_RUN (vmrestore),
#                            local `dry_run` params (preserved literal at
#                            ~19 KEEPER sites — see spec §1.3.3 / §1.3.5).
#   B. yes/no strings      — CLOUD_REPLICATION_DRY_RUN (out of scope).
#   C. integer 0/1         — DRY_RUN inside replication_cloud_module.sh
#                            standalone CLI sub-handler (out of scope).
#
# Out of scope (KEEPERs — see spec §4 commit 1 enumeration):
#   * REPLICATION_DRY_RUN sub-flag (replication_local_module.sh L716)
#   * Local `dry_run` string params in retention_module.sh and the
#     vmrestore restore-chain (preserves helper unit-testability)
#   * vmbackup.sh L5995 `local dry_run="$DRY_RUN"` snapshot
#
# LOAD ORDER:
#   * Sourced by vmbackup.sh and vmrestore.sh via `source_lib_or_die` at
#     strict-alphabetical position (between config_instance.sh and
#     exit_codes.sh).
#   * MUST be sourced BEFORE any `load_*_module` call — modules that read
#     DRY_RUN at function-definition time would otherwise see the literal
#     guards before the predicates are defined. Verified by §4 commit 1
#     acceptance grep gate (G17).
#   * No module sources this lib directly — modules rely on the binary
#     having loaded it (Phase 6 §2.1.1 convention).
#   * Idempotency-guarded — safe to re-source.
#
# DEPENDENCIES:
#   * lib/exit_codes.sh — for EXIT_DEPENDENCY (8) used by the conflict-abort
#     in dry_run_normalise_state. Sourced before this lib by both binaries
#     (alphabetical: dry_run < exit_codes is FALSE; corrected: this lib
#     defers EXIT_DEPENDENCY lookup to call time, not source time, so
#     either source order works).
##############################################################################

# Idempotency: re-source no-op once the API is defined.
declare -F is_dry_run >/dev/null 2>&1 && return 0

# Lib-private canonical state — set by dry_run_normalise_state, read by
# is_dry_run. Underscore prefix marks it as not-for-direct-read by callers.
_DRY_RUN_STATE=false

# ----------------------------------------------------------------------------
# Predicates
# ----------------------------------------------------------------------------

# is_dry_run — read-only predicate.
# Returns 0 if dry-run mode is active, 1 otherwise.
# Lazy-normalises on first call to defend against being read before the
# binary's --dry-run parser has fired.
is_dry_run() {
  dry_run_normalise_state
  [[ "$_DRY_RUN_STATE" == "true" ]]
}

# if_dry_run <cmd...> — execute cmd only if dry-run is active.
# Returns cmd's exit code if executed; returns 1 (skipped) otherwise.
# Intended for dry-run-only side effects (preview log lines,
# "[DRY-RUN] WOULD: …" annotations).
if_dry_run() {
  is_dry_run && "$@"
}

# if_not_dry_run <cmd...> — execute cmd only if dry-run is NOT active.
# Returns cmd's exit code if executed; returns 1 (skipped) otherwise.
# Intended for destructive operations (real fs writes, virsh state
# changes, SQLite writes). This helper is the migration target for the
# 11 inverted-polarity sites (see spec §7.1 R1).
if_not_dry_run() {
  ! is_dry_run && "$@"
}

# ----------------------------------------------------------------------------
# Internal: dry_run_normalise_state
# ----------------------------------------------------------------------------
# Resolves DRY_RUN (vmbackup) and OPT_DRY_RUN (vmrestore) into the canonical
# _DRY_RUN_STATE.
#
# Discipline: detect-which-is-set, NOT precedence.
#   * If OPT_DRY_RUN is set, read it.
#   * Else if DRY_RUN is set, read it.
#   * Else default false.
#   * If BOTH are set, abort with EXIT_DEPENDENCY (8) — this can only
#     arise from a structural load-order bug (each binary defines exactly
#     one of the two globals); a loud crash beats fake precedence.
#
# Idempotent — safe to call any number of times. Called once at lib-source
# time (file end) and re-callable from each binary's CLI parser tail.
#
# set -u safe: uses [[ -v X ]] for presence detection and ${X:-false} for
# value reads, so the lib loads cleanly with `set -u` and neither global
# pre-defined.
dry_run_normalise_state() {
  local _have_opt=0 _have_dry=0
  [[ -v OPT_DRY_RUN ]] && _have_opt=1
  [[ -v DRY_RUN ]] && _have_dry=1

  if (( _have_opt == 1 && _have_dry == 1 )); then
    # Use printf to avoid logging-lib dependency at this point.
    printf 'FATAL: lib/dry_run.sh: both DRY_RUN (=%s) and OPT_DRY_RUN (=%s) are set — structural load-order bug, refusing to apply fake precedence\n' \
      "${DRY_RUN:-?}" "${OPT_DRY_RUN:-?}" >&2
    # EXIT_DEPENDENCY may not be defined yet if exit_codes.sh source-order
    # ever drifts; fall back to literal 8.
    exit "${EXIT_DEPENDENCY:-8}"
  fi

  if (( _have_opt == 1 )); then
    [[ "${OPT_DRY_RUN:-false}" == "true" ]] && _DRY_RUN_STATE=true || _DRY_RUN_STATE=false
  elif (( _have_dry == 1 )); then
    [[ "${DRY_RUN:-false}" == "true" ]] && _DRY_RUN_STATE=true || _DRY_RUN_STATE=false
  else
    _DRY_RUN_STATE=false
  fi
}

# ----------------------------------------------------------------------------
# Constants (exported)
# ----------------------------------------------------------------------------

# Suggested prefix for log messages emitted inside if_dry_run blocks.
# Not enforced; per-binary prefixes ([DRY RUN], [DRY-RUN], …) preserved
# at existing call sites to keep visual surface byte-equivalence (R8).
# Idempotency-guarded: readonly assignment errors on re-source.
[[ -n "${DRY_RUN_GUARD_LOG_PREFIX+x}" ]] || readonly DRY_RUN_GUARD_LOG_PREFIX="[DRY-RUN] WOULD: "

# Export predicates so subshells (e.g. xargs/find -exec wrappers) see them.
export -f is_dry_run if_dry_run if_not_dry_run dry_run_normalise_state

# Lib-load-time normalisation. Safe even if --dry-run hasn't been parsed
# yet — defaults to false and will be re-normalised from the binary's CLI
# parser tail.
dry_run_normalise_state
