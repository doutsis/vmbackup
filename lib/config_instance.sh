#!/usr/bin/env bash
#
# lib/config_instance.sh — Config-instance path resolution.
#
# UNI-012: Both binaries previously hard-coded the
#   /opt/vmbackup/config/<instance>/vmbackup.conf
# layout in different ways (vmbackup parses --config-instance pre-config-load
# at startup, vmrestore cascades CLI > env > "default" inside resolve_backup_path).
# The differing parser shapes stay; only the path mapping is consolidated here.
#
# Idempotency-safe (function definition is replayable).
# UNI-321: idempotency guard — re-source is a no-op once resolve_config_instance is defined.
declare -F resolve_config_instance >/dev/null 2>&1 && return 0
# resolve_config_instance — return the effective instance name from the
# (already-parsed) inputs. Cascade: CLI override > env var > "default".
#
# Usage:
#   instance=$(resolve_config_instance "$cli_override" "$env_value")
#
# Both args may be empty. Pure function: no logging, no exit, no globals.
resolve_config_instance() {
  local cli="${1:-}"
  local env="${2:-}"
  if [[ -n "$cli" ]]; then
    printf '%s' "$cli"
  elif [[ -n "$env" ]]; then
    printf '%s' "$env"
  else
    printf '%s' "default"
  fi
}

# validate_config_instance — assert a config-instance name is safe to embed in
# a filesystem path that will subsequently be *sourced as shell* (as root, in
# vmbackup). R1: the instance value comes straight off the CLI
# (--config-instance) or $VMBACKUP_INSTANCE with no other gate, so without this
# a value like '../../../../tmp/evil' walks out of the config tree and whatever
# vmbackup.conf lands there is executed. Enforce a strict alphabet and reject
# path separators and traversal.
#
# Pure predicate: no output, no logging, no exit. Returns 0 (valid) / 1 (invalid).
#
# Usage:
#   validate_config_instance "$instance" || die "bad instance"
validate_config_instance() {
  local instance="${1:-}"
  [[ -n "$instance" ]] || return 1
  # Allow only filename-safe characters. Note '/' is not in the class, so no
  # path separators can appear.
  [[ "$instance" =~ ^[A-Za-z0-9._-]+$ ]] || return 1
  # The alphabet permits '.', so reject any traversal component explicitly.
  [[ "$instance" == *..* ]] && return 1
  return 0
}

# get_config_file — return the full path to vmbackup.conf for the given
# instance, honouring an installed-vs-source layout cascade:
#   1. $SCRIPT_DIR/config/<instance>/vmbackup.conf  (source tree)
#   2. /opt/vmbackup/config/<instance>/vmbackup.conf (installed)
#
# Returns the first existing path. If neither exists, returns the
# installed path (the caller is expected to error on its non-existence).
#
# R1: refuses to build a path from an unvalidated instance name. On an invalid
# name it writes an error to stderr, emits nothing on stdout, and returns 1 —
# callers MUST treat a non-zero return as a hard config error and not proceed
# to source the (empty / attacker-influenced) path.
#
# Usage:
#   conf=$(get_config_file "$instance") || die "invalid instance"
get_config_file() {
  local instance="${1:?get_config_file: instance name required}"
  if ! validate_config_instance "$instance"; then
    printf 'Error: invalid config instance name: %s (allowed: letters, digits, . _ - ; no "/" or "..")\n' "$instance" >&2
    return 1
  fi
  local src_path="${SCRIPT_DIR:-}/config/${instance}/vmbackup.conf"
  local inst_path="/opt/vmbackup/config/${instance}/vmbackup.conf"
  if [[ -n "${SCRIPT_DIR:-}" && -f "$src_path" ]]; then
    printf '%s' "$src_path"
  else
    printf '%s' "$inst_path"
  fi
}

# resolve_backup_path_shell — resolve BACKUP_PATH from a vmbackup.conf using the
# SAME shell semantics vmbackup itself uses (vmbackup *sources* the conf),
# instead of a regex scrape that diverges from the writer for variable refs or
# spaces. VMR3: vmrestore previously grep-scraped 'BACKUP_PATH=' and got a
# literal/truncated token, so a conf using e.g. BACKUP_PATH="${STORAGE_ROOT}/vm"
# or a path with spaces resolved differently in backup vs restore.
#
# Prints the resolved BACKUP_PATH on stdout (no trailing-slash normalisation —
# the caller decides). Returns 1 if the conf is unreadable or sets no BACKUP_PATH.
#
# SECURITY: this *sources* the conf as shell. The caller MUST have obtained
# $conf via get_config_file()/validate_config_instance() so it is the trusted,
# in-tree vmbackup.conf and not an attacker-chosen path. The conf is sourced in
# a constrained subshell so only the resolved value is emitted and nothing from
# the conf can clobber the caller's environment. vmrestore may run non-root
# (DOC-01); sourcing the same trusted conf vmbackup already sources as root is
# acceptable, but only after the path confinement above.
#
# Usage:
#   bp=$(resolve_backup_path_shell "$conf") || die "no BACKUP_PATH in $conf"
resolve_backup_path_shell() {
  local conf="${1:?resolve_backup_path_shell: conf path required}"
  [[ -f "$conf" && -r "$conf" ]] || return 1
  local resolved
  resolved=$(
    # Ignore any BACKUP_PATH inherited from the environment so the conf's own
    # value is what we read, then source and emit only that.
    unset BACKUP_PATH
    # FF-73: match vmbackup's writer semantics — vmbackup sources the conf under
    # pipefail-only (no nounset), so a line like BACKUP_PATH="${STORAGE_ROOT}/vm"
    # with STORAGE_ROOT unset must expand to '' here, not abort the subshell.
    # Under vmrestore (set -uo pipefail) this subshell inherits -u, so drop it.
    set +u
    # shellcheck source=/dev/null
    source "$conf" >/dev/null 2>&1 || exit 1
    [[ -n "${BACKUP_PATH:-}" ]] || exit 1
    printf '%s' "$BACKUP_PATH"
  ) || return 1
  [[ -n "$resolved" ]] || return 1
  printf '%s' "$resolved"
}
