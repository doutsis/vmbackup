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

# get_config_file — return the full path to vmbackup.conf for the given
# instance, honouring an installed-vs-source layout cascade:
#   1. $SCRIPT_DIR/config/<instance>/vmbackup.conf  (source tree)
#   2. /opt/vmbackup/config/<instance>/vmbackup.conf (installed)
#
# Returns the first existing path. If neither exists, returns the
# installed path (the caller is expected to error on its non-existence).
#
# Usage:
#   conf=$(get_config_file "$instance")
get_config_file() {
  local instance="${1:?get_config_file: instance name required}"
  local src_path="${SCRIPT_DIR:-}/config/${instance}/vmbackup.conf"
  local inst_path="/opt/vmbackup/config/${instance}/vmbackup.conf"
  if [[ -n "${SCRIPT_DIR:-}" && -f "$src_path" ]]; then
    printf '%s' "$src_path"
  else
    printf '%s' "$inst_path"
  fi
}
