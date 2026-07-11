#!/usr/bin/env bash
#
# modules/migrate_layout.sh — MIG-01 (118-spaces): on-disk layout migration.
#
# When a VM whose libvirt name contains a space (or other char outside
# [A-Za-z0-9._-]) was backed up under <=0.6.x, its folder is the pre-118 lossy
# slug (e.g. "Win11_-_CDX"). Under 118 the canonical folder is the slug+hash
# token (e.g. "Win11_-_CDX-a917cd7415ea"). This migrator renames the folder and
# rewrites the catalogue path columns so the existing chain stays addressable
# and restorable — once, lazily, at cycle start, with zero operator action.
#
# Design (hardened by build-spec pass #2 — see copilot/118-spaces.md § MIG-01):
#   * Run over EVERY libvirt-enumerated VM at cycle start, BEFORE any backup
#     derives a path (so a fresh chain never lands in the legacy folder).
#   * Only UNSAFE-named VMs move; a safe name (token == name) is a no-op, so the
#     99% of VMs are never touched.
#   * mv-THEN-DB, crash-convergent: the catalogue rewrite is idempotent (it only
#     matches rows still on the legacy prefix), so a crash between the mv and the
#     UPDATE is fixed on the next run.
#   * Done-ness is derived from folder state (legacy gone, new present) — NO
#     global "all-done" marker that could skip a later still-legacy VM.
#   * No-clobber: if BOTH folders exist it refuses to merge (fail closed).
#   * Honours is_dry_run for both the mv and the UPDATE.
#   * The catalogue UPDATE is scoped by vm_name = REAL name AND a slash-delimited
#     prefix match, so it can never cross-rewrite a different VM (e.g. "Win_11"
#     vs "Win_11_1") or a path outside the VM's tree.
#
# Requires (vmbackup context only — modules/ is not sourced by vmrestore):
#   BACKUP_PATH, SQLITE_DB_PATH (or STATE_DIR), vm_fs_name, vm_fs_name_legacy,
#   lv_list_all_domains, is_dry_run, log_info/log_warn/log_error.
#

declare -F migrate_all_layouts >/dev/null 2>&1 && return 0

# Escape a value for a single-quoted SQLite string literal (doubling).
_mig01_sqlesc() { local s=${1//\'/\'\'}; printf '%s' "$s"; }

# The (table, column) pairs that store an absolute path which may embed the VM
# folder token. The prefix-guarded UPDATE no-ops on any row whose value is not
# actually under the VM's legacy folder (e.g. a _state/logs/* log_file), so it
# is safe to list every candidate column.
_MIG01_PATH_COLS=(
  "chain_health:chain_location"
  "vm_backups:backup_path"
  "vm_backups:log_file"
  "chain_events:backup_dir"
  "chain_events:chain_location"
  "chain_events:full_backup_file"
  "file_operations:source_path"
  "file_operations:dest_path"
  "period_events:period_dir"
  "period_events:archive_location"
  "retention_events:target_path"
  "restore_sessions:source_backup_path"
)

# Rewrite every backup-folder path column for ONE VM from the legacy token to the
# new token, in a single transaction. Args: $1 real vm_name  $2 legacy  $3 new
_mig01_rewrite_catalogue() {
  local real=$1 legacy=$2 new=$3
  local db="${SQLITE_DB_PATH:-${STATE_DIR:-}/vmbackup.db}"
  if [[ -z "$db" || ! -f "$db" ]]; then
    log_warn "migrate_layout.sh" "_mig01_rewrite_catalogue" "no catalogue DB at '${db:-<unset>}' — folder moved but rows not rewritten"
    return 1
  fi
  local bp="${BACKUP_PATH}"
  # legacy/new prefixes (with trailing slash) + the bare-dir forms.
  local e_real e_lp e_np e_ld e_nd
  e_real=$(_mig01_sqlesc "$real")
  e_lp=$(_mig01_sqlesc "${bp}${legacy}/")
  e_np=$(_mig01_sqlesc "${bp}${new}/")
  e_ld=$(_mig01_sqlesc "${bp}${legacy}")
  e_nd=$(_mig01_sqlesc "${bp}${new}")

  local pair t c sql=""
  for pair in "${_MIG01_PATH_COLS[@]}"; do
    t=${pair%%:*}; c=${pair##*:}
    # 1) paths under the folder (<legacy>/<period>/...): replace the slash-
    #    delimited prefix, lengths computed by SQLite for self-consistency.
    sql+="UPDATE ${t} SET ${c}='${e_np}'||substr(${c},length('${e_lp}')+1) "
    sql+="WHERE vm_name='${e_real}' AND substr(${c},1,length('${e_lp}'))='${e_lp}';"$'\n'
    # 2) the bare VM-dir itself (no trailing component).
    sql+="UPDATE ${t} SET ${c}='${e_nd}' WHERE vm_name='${e_real}' AND ${c}='${e_ld}';"$'\n'
  done

  # FF-103: this is a FRESH sqlite3 connection; the init-time PRAGMA busy_timeout
  # is per-connection and does not persist. Set it here so a concurrent writer
  # (e.g. vmrestore updating restore_sessions on the shared catalogue) makes the
  # rewrite WAIT rather than fail SQLITE_BUSY immediately, leaving the folder
  # moved but rows still on the legacy prefix.
  printf 'PRAGMA busy_timeout=5000;\nBEGIN;\n%sCOMMIT;\n' "$sql" | sqlite3 "$db"
}

# Migrate ONE VM (by real libvirt name) if needed. Idempotent; safe to call every
# cycle. Returns 0 on success/no-op, 1 on a hard error (a VM's failure must not
# abort the whole run — callers ignore the rc).
migrate_vm_layout() {
  local real=$1
  local new legacy
  new=$(vm_fs_name "$real") || {
    log_error "migrate_layout.sh" "migrate_vm_layout" "cannot derive token for VM '$real' — skipping migration"
    return 1
  }
  legacy=$(vm_fs_name_legacy "$real")
  [[ "$new" == "$legacy" ]] && return 0          # safe name -> nothing to migrate

  local nd="${BACKUP_PATH}${new}" ld="${BACKUP_PATH}${legacy}"

  if [[ -e "$nd" && -e "$ld" ]]; then            # no-clobber: never merge
    log_error "migrate_layout.sh" "migrate_vm_layout" \
      "BOTH legacy '${legacy}' and new '${new}' folders exist for VM '$real' — refusing to merge; resolve manually"
    return 1
  fi

  if [[ -e "$ld" && ! -e "$nd" ]]; then          # the migration
    # Cross-VM collision (H6 / F-mig104): vm_fs_name_legacy is lossy, so the legacy
    # slug of an UNSAFE-named VM can equal the CANONICAL token of a DIFFERENT,
    # safe-named VM (e.g. "Win11 CDX".legacy == "Win11_CDX".canonical). That folder
    # is the OTHER VM's live store — never move it (fail closed, manual resolution).
    # Decided purely from the libvirt name set ($2.. = real-name list) => DB-absent-safe.
    # Scoped INSIDE the mv block (ld present, nd absent) so it can NEVER gate the
    # idempotent catalogue-rewrite re-run below (mv-THEN-DB crash-convergence).
    local other
    for other in "$@"; do
      [[ "$other" == "$real" ]] && continue
      if [[ "$(vm_fs_name "$other")" == "$legacy" || "$(vm_fs_name_legacy "$other")" == "$legacy" ]]; then
        log_error "migrate_layout.sh" "migrate_vm_layout" \
          "legacy folder '${legacy}' for VM '$real' is the live folder of VM '$other' — refusing to migrate (manual resolution required)"
        return 1
      fi
    done
    if is_dry_run; then
      log_info "migrate_layout.sh" "migrate_vm_layout" \
        "[DRY-RUN] would migrate '$ld' -> '$nd' and rewrite the catalogue for VM '$real'"
      return 0
    fi
    if mv -- "$ld" "$nd"; then
      log_info "migrate_layout.sh" "migrate_vm_layout" "MIG-01: moved '${legacy}' -> '${new}' for VM '$real'"
    else
      log_error "migrate_layout.sh" "migrate_vm_layout" "MIG-01: mv failed '$ld' -> '$nd'"
      return 1
    fi
  fi

  # Rewrite the catalogue whenever the new folder is present (idempotent — matches
  # only rows still on the legacy prefix). This converges a crash between the mv
  # and the UPDATE on the next run.
  if [[ -e "$nd" ]]; then
    is_dry_run && return 0
    if _mig01_rewrite_catalogue "$real" "$legacy" "$new"; then
      log_info "migrate_layout.sh" "migrate_vm_layout" "MIG-01: catalogue paths rewritten for VM '$real' (${legacy} -> ${new})"
    else
      log_warn "migrate_layout.sh" "migrate_vm_layout" "MIG-01: catalogue rewrite reported an error for VM '$real'"
    fi
  fi
  return 0
}

# Run the lazy migration over every libvirt-enumerated VM at cycle start. Drives
# from the libvirt REAL-name list (so reconcile/policy stay real-name-keyed). A
# VM deleted from libvirt with a lingering legacy folder is not enumerated here;
# it is handled by the --prune legacy fallback instead.
migrate_all_layouts() {
  if ! declare -f vm_fs_name >/dev/null 2>&1; then return 0; fi
  # Snapshot the full libvirt name set ONCE (before the loop) and thread it to
  # every per-VM call so the cross-VM collision guard (F-mig104) decides ownership
  # against a CONSISTENT set — a mid-loop re-enumeration could miss a transiently-
  # absent twin and let a foreign folder through.
  local -a _all=()
  mapfile -t _all < <(lv_list_all_domains 2>/dev/null)
  local vm
  for vm in "${_all[@]}"; do
    [[ -z "$vm" ]] && continue
    migrate_vm_layout "$vm" "${_all[@]}" || true
  done
}
