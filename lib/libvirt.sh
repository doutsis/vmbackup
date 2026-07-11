#!/usr/bin/env bash
##############################################################################
# lib/libvirt.sh — libvirt domain / XML / pool helpers
#
# UNI-013 (Phase 6 commit 4): single source of truth for every generic
# libvirt operation used by vmbackup.sh and vmrestore.sh. 23 functions
# across 5 groups, replacing 65 distinct virsh callsites in the two
# binaries (vmbackup 39 + vmrestore 26).
#
# Out of scope (KEEPERs — stay inline in callers, annotated
# `# [LIBVIRT-KEEPER: <reason>]`):
#   * qemu-agent-command   (guest-agent I/O — orthogonal)
#   * qemu-monitor-command (bitmap surgery — not generic libvirt)
#   * module-internal virsh in tpm_backup_module / vmbackup_integration /
#     fstrim_optimization_module (per spec §2.4)
#
# Conventions:
#   * Read-only helpers print to stdout, return 0/non-zero on success/fail.
#   * Mutation helpers (define/undefine/suspend/resume/checkpoint-delete/
#     pool-refresh/domjobabort) return exit code only.
#   * No sudo (binaries already run as root via systemd).
#   * Default `qemu:///system` connection (no --connect needed).
#   * No xmllint dependency — POSIX grep/sed/awk only.
#
# Bootstrap: sourced by vmbackup.sh, vmrestore.sh via `source_lib_or_die`.
##############################################################################

# Idempotency: re-source no-op once the API is defined.
declare -F lv_domain_state >/dev/null 2>&1 && return 0

# ============================================================================
# G1 — Domain lookup
# ============================================================================

# Probe libvirtd liveness via cheapest virsh call.
lv_daemon_alive() {
  virsh list --all >/dev/null 2>&1
}

# Print libvirt library version (e.g., "8.0.0"), or empty on failure.
lv_libvirt_version() {
  virsh version 2>/dev/null | grep "Using library:" | awk '{print $4}'
}

# Print one VM name per line (all defined domains, running or not).
lv_list_all_domains() {
  virsh list --all --name 2>/dev/null
}

# Return 0 iff a domain with the given name is defined.
lv_domain_exists() {
  local vm=$1
  virsh dominfo "$vm" &>/dev/null
}

# Print the domain state ("running" | "shut off" | "paused" | ...) with
# leading/trailing whitespace stripped but INTERNAL whitespace preserved.
# Critical: libvirt's "shut off" state contains an embedded space — it must
# survive normalisation, otherwise every `[[ "$state" == "shut off" ]]`
# comparison in the binaries silently becomes dead code.
# Regression history: commit 5cc255f (Phase-6 carve, 2026-05-09) used
# `tr -d '[:space:]'` here, which collapsed "shut off" → "shutoff" and
# disabled the offline-VM skip path for 12 nights, producing ~500 GiB
# of redundant archive churn before being caught (2026-05-21).
lv_domain_state() {
  local vm=$1
  virsh domstate "$vm" 2>/dev/null | awk 'NF{$1=$1; print; exit}'
}

# Print the domain UUID, or empty on failure.
lv_domain_uuid() {
  local vm=$1
  virsh domuuid "$vm" 2>/dev/null
}

# ============================================================================
# G2 — XML extraction
# ============================================================================

# Print the domain XML to stdout. Pass `--inactive` as second arg for the
# inactive (defined-but-not-current) variant.
lv_dump_xml() {
  local vm=$1
  local mode=${2:-}
  if [[ $mode == "--inactive" ]]; then
    virsh dumpxml --inactive "$vm" 2>/dev/null
  else
    virsh dumpxml "$vm" 2>/dev/null
  fi
}

# Write the domain XML to a file. Returns virsh's exit code.
# Optional third arg `--inactive` for the inactive variant.
lv_dump_xml_to_file() {
  local vm=$1
  local file=$2
  local mode=${3:-}
  # FF-76: write to a temp sibling and publish with mv only on success, so a
  # transient virsh failure never leaves a zero-byte / torn XML at $file
  # (fail-closed: $file appears only when virsh returned 0). Preserves the
  # "returns virsh's exit code" contract on the virsh-failure path.
  local _tmp="${file}.tmp.$$" _rc
  if [[ $mode == "--inactive" ]]; then
    virsh dumpxml --inactive "$vm" > "$_tmp" 2>/dev/null
  else
    virsh dumpxml "$vm" > "$_tmp" 2>/dev/null
  fi
  _rc=$?
  if [[ $_rc -eq 0 ]]; then
    mv -f "$_tmp" "$file" && return 0
    rm -f "$_tmp"
    return 1
  fi
  rm -f "$_tmp"
  return "$_rc"
}

# Return 0 iff the domain XML contains a <tpm> element.
lv_xml_has_tpm() {
  local vm=$1
  virsh dumpxml "$vm" 2>/dev/null | grep -q "<tpm"
}

# ============================================================================
# G3 — Disk / interface enumeration
# ============================================================================

# Print one disk source path per line (skips '-' entries used for
# CDROM/floppy with no media). Output preserves original ordering.
lv_list_disk_paths() {
  # DISK-01 (118-spaces): read the Source column from `domblklist --details`,
  # NOT `domblklist | awk '{print $2}'` — the latter truncates any source path
  # at its first space (so a disk like "/mnt/vm/dev manjaro space.qcow2" came
  # back as "/mnt/vm/dev") and leaked the "Source" header row. Selecting rows by
  # device type ($2 ∈ disk/cdrom/floppy) skips the header/separator, and the
  # Source path is recovered from the 4th field onward. (DISK-01's original
  # `for i=4..NF` rejoin restored spaces but collapsed runs of whitespace — that
  # regression was closed by FF-173 below.) Same
  # internal-whitespace-is-load-bearing lesson as lv_domain_state.
  # FF-173: the Source is the LAST column, so strip the first three
  # whitespace-delimited fields (Type/Device/Target) and their padding and keep
  # the line REMAINDER — internal whitespace verbatim. The previous `for i=4..NF`
  # rejoin used a single space and collapsed any run of consecutive spaces/tabs
  # inside the path, failing vmrestore's live-disk overwrite gate OPEN; keeping
  # the remainder preserves the exact internal whitespace. Only TRAILING
  # whitespace is stripped — tabular virsh output carries no meaningful trailing
  # bytes, and a trailing-space path would spuriously mismatch downstream
  # exact-path comparisons. A media-less entry ("-") is skipped.
  # AMENDED (E16): NF>=4 — a malformed row with no Source column emits nothing.
  local vm=$1
  virsh domblklist "$vm" --details 2>/dev/null \
    | awk '$2 ~ /^(disk|cdrom|floppy)$/ && NF >= 4 {
             src = $0
             sub(/^[[:space:]]*[^[:space:]]+[[:space:]]+[^[:space:]]+[[:space:]]+[^[:space:]]+[[:space:]]+/, "", src)
             sub(/[[:space:]]+$/, "", src)
             if (src != "-" && src != "") print src
           }'
}

# Print one disk target device per line (vda, vdb, sda, ...).
# Filters --details output to disk-typed entries only (skips cdrom/floppy).
lv_list_disk_targets() {
  local vm=$1
  virsh domblklist "$vm" --details 2>/dev/null | awk '$2=="disk"{print $3}'
}

# Print one MAC address per line for every defined interface.
lv_list_iface_macs() {
  local vm=$1
  virsh domiflist "$vm" 2>/dev/null | awk 'NR>2 && $5 {print $5}'
}

# ============================================================================
# G4 — Identity & lifecycle mutation
# ============================================================================

# Define (or redefine) a domain from an XML file. Prints virsh's combined
# stdout+stderr; returns its exit code. Callers can capture the output:
#   if out=$(lv_define_xml "$xml"); then ...
lv_define_xml() {
  local xml=$1
  virsh define "$xml" 2>&1
}

# Undefine a domain, trying option cascades from most-aggressive
# (--nvram --checkpoints-metadata) down to bare. Returns 0 if any cascade
# succeeded, non-zero only if every variant failed.
lv_undefine_domain() {
  local vm=$1
  virsh undefine "$vm" --nvram --checkpoints-metadata 2>/dev/null && return 0
  virsh undefine "$vm" --checkpoints-metadata 2>/dev/null && return 0
  virsh undefine "$vm" --nvram 2>/dev/null && return 0
  virsh undefine "$vm" 2>/dev/null
}

# Suspend a running domain.
lv_suspend() {
  local vm=$1
  virsh suspend "$vm" 2>/dev/null
}

# Resume a suspended domain.
lv_resume() {
  local vm=$1
  virsh resume "$vm" 2>/dev/null
}

# Abort the active block job on a domain, if any. Always returns 0 — used
# for cleanup paths where the absence of a job is a normal condition.
lv_domjobabort() {
  local vm=$1
  virsh domjobabort "$vm" 2>/dev/null || true
}

# ============================================================================
# G5 — Checkpoint / pool / state
# ============================================================================

# Print one virtnbdbackup checkpoint name per line, sorted as virsh emits.
# Filters to the `virtnbdbackup.*` namespace only (excludes any other
# checkpoint families that may exist).
lv_checkpoint_list_virtnbd() {
  local vm=$1
  virsh checkpoint-list "$vm" --name 2>/dev/null | grep "^virtnbdbackup\." || true
}

# Print the integer count of virtnbdbackup checkpoints. Always emits a
# decimal integer (0 on no-such-domain or empty list).
lv_checkpoint_count_virtnbd() {
  local vm=$1
  local n
  n=$(virsh checkpoint-list "$vm" --name 2>/dev/null | grep -c "^virtnbdbackup\." || true)
  printf '%s\n' "${n:-0}"
}

# Print the integer count of ALL checkpoints (any namespace) for a VM.
# Used by orphan detection where any extant metadata is significant.
lv_checkpoint_count_all() {
  local vm=$1
  local n
  n=$(virsh checkpoint-list "$vm" --name 2>/dev/null | grep -c . || true)
  printf '%s\n' "${n:-0}"
}

# Delete a single checkpoint's metadata only (qcow2 bitmap untouched).
# Used in the cleanup loops scattered across vmbackup.sh.
lv_checkpoint_delete_metadata() {
  local vm=$1
  local cp=$2
  virsh checkpoint-delete "$vm" "$cp" --metadata 2>/dev/null
}

# Print one storage-pool source path per line, by enumerating defined pools
# and extracting `<path>...</path>` from each pool's XML. Skips pools with
# no <path> element (e.g., transient or non-dir pools).
lv_pool_paths() {
  local pool path
  while IFS= read -r pool; do
    [[ -z $pool ]] && continue
    path=$(virsh pool-dumpxml "$pool" 2>/dev/null | grep -oP '<path>\K[^<]+' || true)
    [[ -n $path ]] && printf '%s\n' "$path"
  done < <(virsh pool-list --name 2>/dev/null)
}

# Refresh a single storage pool (re-scan for newly-arrived files).
lv_pool_refresh() {
  local pool=$1
  virsh pool-refresh "$pool" &>/dev/null
}

# ============================================================================
# Export public API.
# ============================================================================

export -f lv_daemon_alive lv_libvirt_version lv_list_all_domains
export -f lv_domain_exists lv_domain_state lv_domain_uuid
export -f lv_dump_xml lv_dump_xml_to_file lv_xml_has_tpm
export -f lv_list_disk_paths lv_list_disk_targets lv_list_iface_macs
export -f lv_define_xml lv_undefine_domain
export -f lv_suspend lv_resume lv_domjobabort
export -f lv_checkpoint_list_virtnbd lv_checkpoint_count_virtnbd
export -f lv_checkpoint_count_all lv_checkpoint_delete_metadata
export -f lv_pool_paths lv_pool_refresh
