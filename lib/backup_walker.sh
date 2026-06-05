#!/usr/bin/env bash
#
# lib/backup_walker.sh — Backup-tree iteration for vmbackup/vmrestore.
#
# UNI-309: a single source of truth for "iterate every VM × period in a
# backup tree". Replaces the divergent inline iteration in vmbackup's
# `_prune_list` (vmbackup.sh L5602) and vmrestore's `list_vms`
# (vmrestore.sh L534).
#
# Skeletal walker (Phase 3 D2): walker only takes (vm_dir, period_dir)
# and applies the skip-list. It does **not** consult `lib/period.sh` —
# callers re-invoke `get_vm_periods` if they need the periodic-only
# filter. This keeps the walker policy-agnostic and presenters fully in
# control of the per-period logic (R1).
#
# UNI-321: idempotency guard — re-source is a no-op once walk_backup_tree is defined.
declare -F walk_backup_tree >/dev/null 2>&1 && return 0

# Skip-list (Phase 3 spec §4 commit 3 / F clarification):
#   VM level     reject: _state, _*, .*
#   Period level reject: _*, .*, .archives
# `config`, `checkpoints`, `tpm-state` are period-internal subdirs
# (under $vm_dir/$period_id/), never appear at period-dir depth, and
# are deliberately omitted from the skip-list (wrong-layered in earlier
# drafts).
#
# B (skip-list convergence): vmbackup previously skipped only `_*` + `.*`
# at period level; vmrestore skipped `.archives|config|checkpoints|
# tpm-state`. Both now use the rule above. This is a deliberate
# behavioural change for vmbackup (`.archives` now skipped at period
# level, which is correct: `.archives` is the archive store, not a
# period). The vmbackup acceptance baseline is regenerated alongside the
# walker rollout in commit 3.
#
# Callback contract (M2):
#   vm_cb     <vm_name> <vm_dir>
#       Invoked once per non-skipped VM dir. Return non-zero to skip
#       period iteration for that VM (used by vmrestore's accumulate
#       branch — H, U1).
#   period_cb <vm_name> <vm_dir> <period_id> <period_dir>
#       Invoked once per non-skipped period dir within a VM that did not
#       suppress iteration.
#
# All callbacks are passed as function-name strings and invoked with
# quoted positional expansion. The walker performs no I/O beyond
# directory listing and the skip-list test (R1) — no du, no find,
# no stat. Presenters own all reduction.
#
# Conventions: M1 (Phase 2 lib header style).
#

# VM-level skip predicate. Returns 0 (skip) for matches.
_walk_skip_vm() {
    local name="$1"
    case "$name" in
        _state|_*|.*) return 0 ;;
    esac
    return 1
}

# Period-level skip predicate. Returns 0 (skip) for matches.
# Note: `.archives` is covered by the `.*` pattern; listing it explicitly
# would be redundant (shellcheck SC2222). Documenting here for clarity:
# the spec calls out `.archives` as the canonical period-level skip; it
# is matched by virtue of the leading-dot rule.
_walk_skip_period() {
    local name="$1"
    case "$name" in
        _*|.*) return 0 ;;
    esac
    return 1
}

# walk_backup_tree <backup_path> <vm_callback> <period_callback>
walk_backup_tree() {
    local backup_path="$1" vm_cb="$2" period_cb="$3"
    local vm_dir vm_name period_dir period_id

    [[ -d "$backup_path" ]] || return 0

    for vm_dir in "${backup_path%/}"/*/; do
        [[ -d "$vm_dir" ]] || continue
        vm_name=$(basename "$vm_dir")
        _walk_skip_vm "$vm_name" && continue
        # Non-zero from vm_cb → skip period iteration for this VM (H, U1).
        "$vm_cb" "$vm_name" "$vm_dir" || continue
        for period_dir in "${vm_dir%/}"/*/; do
            [[ -d "$period_dir" ]] || continue
            period_id=$(basename "$period_dir")
            _walk_skip_period "$period_id" && continue
            "$period_cb" "$vm_name" "$vm_dir" "$period_id" "$period_dir"
        done
    done
}
