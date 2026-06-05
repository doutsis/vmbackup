#!/usr/bin/env bash
#
# lib/exit_codes.sh — Shared exit code constants for vmbackup/vmrestore.
#
# UNI-003: Both binaries previously defined identical readonly EXIT_* sets.
# Consolidated here. Both binaries source this file; inline definitions
# removed.
#
# Categorised for monitoring integration. Backward-compatible:
#   0 = success, 1 = generic failure (catch-all)
# Specific codes only appear at sites where the failure category is
# unambiguous.
#
# NOTE: EXIT_TOOL (6) is currently raised by dependency checks and the
# replication_cloud_module standalone CLI. In-pipeline tool failures
# during backup operations (e.g. virtnbdbackup non-zero exit) are reported
# as EXIT_ERROR (1) — see the SQLite session row and email report for the
# tool-level detail (tool name, exit code, log tail).
#
# Idempotency guard: 'readonly' would error on a second source. Skip if
# already set (covers test harnesses and modules that source this twice).

# UNI-321: idempotency guard — readonly assignment would error on re-source.
# Constant-only lib (no functions); use variable-presence guard.
[[ -n "${EXIT_OK+x}" ]] && return 0
readonly EXIT_OK=0
readonly EXIT_ERROR=1
readonly EXIT_CONFIG=2
readonly EXIT_LOCK=3
readonly EXIT_STORAGE=4
readonly EXIT_VM=5
readonly EXIT_TOOL=6
readonly EXIT_USAGE=7
readonly EXIT_DEPENDENCY=8
