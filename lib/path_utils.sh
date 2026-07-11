#!/usr/bin/env bash
##############################################################################
# lib/path_utils.sh — canonical path helpers
#
# UNI-323 (Phase 7 commit 2): single source of truth for path
# canonicalisation, trailing-slash conventions, and safe joins. Replaces
# 4 explicit `realpath` sites in vmrestore.sh and 2 cosmetic
# trailing-slash tweaks in vmbackup.sh / vmrestore.sh.
#
# Convention asymmetry (deliberate, not unified by UNI-323):
#   * vmbackup writes BACKUP_PATH with a trailing slash ("path/")
#   * vmrestore consumes BACKUP_PATH and strips the trailing slash
#   * Both binaries continue to do this — UNI-323 unifies the helpers,
#     not the on-disk convention. Changing the convention would break
#     config-file compatibility across versions. See 109-phase7-spec.md
#     §2.2 INT-04 disposition.
#
# All helpers are pure functions (no side effects, no global reads or
# writes). They emit canonical strings to stdout and use stable exit
# codes:
#   * strict variants (pu_normalise_path without --loose) return 1 +
#     empty stdout if the path does not exist;
#   * everything else returns 0.
#
# Idempotency-guarded — safe to re-source.
##############################################################################

declare -F pu_normalise_path >/dev/null 2>&1 && return 0

# ── Trailing-slash helpers (pure parameter expansion) ──────────────────────

# pu_strip_trailing_slash <path> — idempotent strip.
#   "/foo/" → "/foo"   "/foo" → "/foo"   ""     → ""
pu_strip_trailing_slash() {
  local p="${1:-}"
  # FF-78: strip ALL trailing slashes, not just one — the single ${p%/} left
  # 'base//' as 'base/', so pu_join_paths still emitted a double slash.
  while [[ "$p" == */ ]]; do p="${p%/}"; done
  printf '%s\n' "$p"
}

# pu_ensure_trailing_slash <path> — idempotent add.
#   "/foo"  → "/foo/"  "/foo/" → "/foo/"  ""    → "/"
pu_ensure_trailing_slash() {
  local p="${1:-}"
  [[ "$p" == */ ]] && { printf '%s\n' "$p"; return 0; }
  printf '%s/\n' "$p"
}

# ── realpath wrappers ──────────────────────────────────────────────────────

# pu_safe_realpath <path> — never-fail realpath -m wrapper.
# Falls through to the literal input if `realpath` itself errors (extreme
# edge case: binary missing). Always returns 0; output to stdout.
# Intended for predicted/destination paths that may not exist yet.
pu_safe_realpath() {
  local p="${1:-}" out
  if out=$(realpath -m "$p" 2>/dev/null); then
    printf '%s\n' "$out"
  else
    printf '%s\n' "$p"
  fi
}

# pu_normalise_path <path> [--loose] — strict canonicalisation.
# Default (strict): path MUST exist; returns 1 + empty stdout if not.
# --loose: falls through to `realpath -m` (allows non-existent paths).
# Always strips trailing slash from the result.
pu_normalise_path() {
  local p="${1:-}" mode="strict" out
  [[ "${2:-}" == "--loose" ]] && mode="loose"

  if [[ "$mode" == "loose" ]]; then
    out=$(realpath -m "$p" 2>/dev/null) || return 1
  else
    out=$(realpath "$p" 2>/dev/null) || return 1
  fi
  pu_strip_trailing_slash "$out"
}

# ── Safe join ──────────────────────────────────────────────────────────────

# pu_join_paths <base> <segment> [<segment>...] — safe path join.
# Strips trailing slash from base and leading+trailing slash from each
# segment, then joins with single `/`. Prevents double-slashes.
pu_join_paths() {
  local base="${1:-}"
  # FF-78: fail closed on an empty base. An unset/empty base would otherwise make
  # the first segment absolute ('/seg'), retargeting a delete/retention join at the
  # filesystem root; reject rather than emit a root-anchored path.
  [[ -z "$base" ]] && return 1
  shift || true
  local out
  out=$(pu_strip_trailing_slash "$base")
  local seg
  for seg in "$@"; do
    seg="${seg#/}"
    seg=$(pu_strip_trailing_slash "$seg")
    [[ -n "$seg" ]] && out="${out}/${seg}"
  done
  printf '%s\n' "$out"
}

# Export for subshell visibility.
export -f pu_strip_trailing_slash pu_ensure_trailing_slash \
          pu_safe_realpath pu_normalise_path pu_join_paths
