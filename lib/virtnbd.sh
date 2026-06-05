#!/usr/bin/env bash
##############################################################################
# lib/virtnbd.sh — virtnbdbackup / virtnbdrestore argument builders
#
# UNI-014 Milestone 1 (Phase 6 commit 3): unifies the construction of CLI
# argument arrays for the two `virtnbd*` tools. Output parsing, retry
# orchestration, error-translation, and progress reporting STAY per-binary
# (Phase 5 audit found no genuine sharing surface for those concerns).
#
# Public API:
#   build_virtnbdbackup_args  <mode> <vm_name> <output_dir> <scratch_dir> [extra_opts...]
#   build_virtnbdrestore_args <input_dir> <output_disk> <action> [extra_opts...]
#
# Both functions populate the GLOBAL array `_VIRTNBD_ARGS` (bash cannot
# return arrays). Callers then exec via:
#   "${_VIRTNBD_ARGS[@]}"               # vmbackup direct exec
#   priority_wrapper "${_VIRTNBD_ARGS[@]}"   # with ionice/nice prefix
#   run_logged virtnbdrestore "${_VIRTNBD_ARGS[@]}"   # vmrestore wrapped exec
#
# build_virtnbdbackup_args reads the following globals (as documented in
# the existing vmbackup conf surface — keeping the same call surface, only
# the construction mechanism changes):
#   VIRTNBD_COMPRESS_LEVEL, VIRTNBD_OUTPUT_FORMAT, VIRTNBD_WORKERS,
#   VIRTNBD_EXCLUDE_DISKS, VIRTNBD_INCLUDE_DISKS, VIRTNBD_FSFREEZE,
#   VIRTNBD_FSFREEZE_PATHS, VIRTNBD_THRESHOLD, VIRTNBD_SPARSE_DETECTION
#
# Out of M1 scope (per spec §2.3):
#   * `--compress=0` workaround (lives at vmbackup config-default block;
#     forces VIRTNBD_COMPRESS_LEVEL=1; arg-builder reads the corrected var)
#   * Output parsing, retry policy, executor wrapping (per-binary)
#   * vmrestore retry path L2109 (stays inline; tracked as INT-06)
##############################################################################

# Idempotency: re-source no-op once the API is defined.
declare -F build_virtnbdbackup_args >/dev/null 2>&1 && return 0

# Global return array (callers read this after each builder call).
# -g so the array stays global when this lib is sourced from inside the
# bootstrap helper's call frame (UNI-321 lesson).
declare -ga _VIRTNBD_ARGS=()

##############################################################################
# build_virtnbdbackup_args <mode> <vm_name> <output_dir> <scratch_dir> [extra_opts...]
#
# Builds an argv array equivalent to the historical string assembly at
# vmbackup.sh perform_backup (pre-UNI-014 L3521–L3563). Mode is forwarded
# to `-l` verbatim — virtnbdbackup accepts {copy, full, inc, diff, auto}
# and vmbackup's determine_backup_level() returns one of {full, auto, copy}
# (NOT "incremental" — see Phase 5 audit note in 109-phase6-spec.md §2.3).
##############################################################################
build_virtnbdbackup_args() {
    local mode="$1"
    local vm_name="$2"
    local output_dir="$3"
    local scratch_dir="$4"
    shift 4

    _VIRTNBD_ARGS=(virtnbdbackup -d "$vm_name" -l "$mode" "--compress=${VIRTNBD_COMPRESS_LEVEL}")

    # Output format: only emitted when not the default "stream".
    if [[ "${VIRTNBD_OUTPUT_FORMAT:-stream}" != "stream" ]]; then
        _VIRTNBD_ARGS+=(-t "${VIRTNBD_OUTPUT_FORMAT}")
    fi

    _VIRTNBD_ARGS+=(-o "$output_dir")

    # Parallel workers: skip when unset or "auto".
    if [[ -n "${VIRTNBD_WORKERS:-}" && "${VIRTNBD_WORKERS}" != "auto" ]]; then
        _VIRTNBD_ARGS+=(--worker "${VIRTNBD_WORKERS}")
    fi

    # Selective disks (mutually-independent flags; both may apply).
    if [[ -n "${VIRTNBD_EXCLUDE_DISKS:-}" ]]; then
        _VIRTNBD_ARGS+=(-x "${VIRTNBD_EXCLUDE_DISKS}")
    fi
    if [[ -n "${VIRTNBD_INCLUDE_DISKS:-}" ]]; then
        _VIRTNBD_ARGS+=(-i "${VIRTNBD_INCLUDE_DISKS}")
    fi

    # Filesystem freeze. Only emit -F when both fsfreeze=true AND a
    # non-empty path list is configured (matches historical behaviour:
    # the old string-build skipped the entire branch when paths were unset).
    if [[ "${VIRTNBD_FSFREEZE:-}" == "true" && -n "${VIRTNBD_FSFREEZE_PATHS:-}" ]]; then
        _VIRTNBD_ARGS+=(-F "${VIRTNBD_FSFREEZE_PATHS}")
    fi

    # Skip threshold: emitted as-is.
    if [[ -n "${VIRTNBD_THRESHOLD:-}" ]]; then
        _VIRTNBD_ARGS+=(--threshold "${VIRTNBD_THRESHOLD}")
    fi

    # Sparse detection: opt-out flag (only emit when explicitly disabled).
    if [[ "${VIRTNBD_SPARSE_DETECTION:-}" == "false" ]]; then
        _VIRTNBD_ARGS+=(--no-sparse-detection)
    fi

    # Scratch dir for NBD socket / fleece operations.
    _VIRTNBD_ARGS+=(--scratchdir "$scratch_dir")

    # Extra opts: appended verbatim (allows callers to pass per-invocation
    # overrides without touching the shared builder).
    if (( $# > 0 )); then
        _VIRTNBD_ARGS+=("$@")
    fi
}

##############################################################################
# build_virtnbdrestore_args <input_dir> <output_disk> <action> [extra_opts...]
#
# Builds an argv array equivalent to the historical inline arrays at
# vmrestore.sh per-disk loop (L1726), full restore (L1913), and
# run_virtnbd_action (L2315).
#
# Action semantics:
#   "restore"        — `-o <output_disk>` (the actual output path)
#   "dump"|"verify"  — `-o <action>` (output_disk argument is ignored)
#
# extra_opts are appended verbatim (e.g. `-c`, `-c -D`, `-U <uri>`,
# `-N <name>`, `-d <disk>`, `--until <cp>`).
##############################################################################
build_virtnbdrestore_args() {
    local input_dir="$1"
    local output_disk="$2"
    local action="$3"
    shift 3

    _VIRTNBD_ARGS=(virtnbdrestore -i "$input_dir")

    case "$action" in
        restore)
            # Real output path (may be a final or staging dir).
            _VIRTNBD_ARGS+=(-o "$output_disk")
            ;;
        dump|verify)
            # virtnbdrestore -o accepts the action keyword in these modes;
            # output_disk is intentionally ignored.
            _VIRTNBD_ARGS+=(-o "$action")
            ;;
        *)
            # Unknown action — pass-through. Matches old `run_logged
            # virtnbdrestore -i "$data_dir" -o "$action"` permissiveness.
            _VIRTNBD_ARGS+=(-o "$action")
            ;;
    esac

    if (( $# > 0 )); then
        _VIRTNBD_ARGS+=("$@")
    fi
}

# Export so the helpers and global array are available in subshells if
# any caller spawns one (current callers do not; defensive only).
export -f build_virtnbdbackup_args build_virtnbdrestore_args
