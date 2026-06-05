#!/usr/bin/env bash
#
# lib/version.sh — Single source of truth for the vmbackup package version.
#
# UNI-008: Both binaries previously defined their own version constant
# (vmbackup.sh: VMBACKUP_VERSION; vmrestore.sh: VERSION). Consolidated
# here. Debian packaging continues to read VMBACKUP_VERSION from
# vmbackup.sh; the build process keeps the two in sync (the constant
# below is the runtime value, the build-time value lives in debian/changelog).
#
# Idempotency guard for double-source.

# UNI-321: idempotency guard — readonly assignment would error on re-source.
# Constant-only lib (no functions); use variable-presence guard.
[[ -n "${VMBACKUP_VERSION+x}" ]] && return 0
readonly VMBACKUP_VERSION="0.6.0"
