#!/bin/bash
#################################################################################
# SSH Transport Driver for Replication Module (STUB)
#
# Handles rsync-based replication over SSH to remote servers.
#
# STATUS: NOT YET IMPLEMENTED
# This is a stub file with function signatures. The actual implementation
# will be added when SSH-based replication is needed.
#
# Exported Functions:
#   transport_init()    - Test SSH connectivity and verify remote path
#   transport_sync()    - Perform rsync over SSH
#   transport_verify()  - Verify sync completed correctly
#   transport_cleanup() - Cleanup (no-op)
#
# Required Configuration (in replication.conf):
#   DEST_N_HOST     - Remote hostname or IP
#   DEST_N_USER     - SSH username
#   DEST_N_PORT     - SSH port (default: 22)
#   DEST_N_PATH     - Remote destination path
#   DEST_N_SSH_KEY  - Path to SSH private key
#
# Version: 1.0 (stub)
# Created: 2026-01-23
#################################################################################

# Transport identification
TRANSPORT_NAME="ssh"
TRANSPORT_VERSION="1.1-stub"

#=============================================================================
# TRANSPORT METRICS CONTRACT (v1.0)
#
# All local transports (transport_*.sh) MUST set these globals after sync:
#
#   TRANSPORT_BYTES_TRANSFERRED  - integer: bytes transferred (0 if none)
#   TRANSPORT_SYNC_DURATION      - integer: seconds elapsed
#   TRANSPORT_DEST_AVAIL_BYTES   - integer: free bytes at dest (0 if unknown)
#   TRANSPORT_DEST_TOTAL_BYTES   - integer: total bytes at dest (0 if unknown)
#   TRANSPORT_DEST_SPACE_KNOWN   - 0|1: whether space metrics are reliable
#   TRANSPORT_THROTTLE_COUNT     - integer: throttle events (-1 = not applicable)
#   TRANSPORT_BWLIMIT_FINAL      - string: final bwlimit after adjustments ("" = none)
#
# Sentinel values:
#   -1 = metric structurally not applicable to this transport type
#    0 = applicable but none occurred / not available
#   "" = no value (string fields)
#
# For SSH transport:
#   TRANSPORT_THROTTLE_COUNT = -1 (rsync over ssh does not throttle)
#   TRANSPORT_DEST_SPACE_KNOWN = 1 if remote df works, 0 otherwise
#=============================================================================

# Global variables set by transport functions
TRANSPORT_BYTES_TRANSFERRED=0
TRANSPORT_SYNC_DURATION=0
TRANSPORT_DEST_AVAIL_BYTES=0
TRANSPORT_DEST_TOTAL_BYTES=0
TRANSPORT_DEST_SPACE_KNOWN=0
TRANSPORT_THROTTLE_COUNT=-1
TRANSPORT_BWLIMIT_FINAL=""

#################################################################################
# transport_init - Test SSH connectivity (NOT IMPLEMENTED)
#################################################################################
transport_init() {
    local dest_path="$1"
    local dest_name="${2:-ssh}"
    
    log_error "transport_ssh.sh" "transport_init" "SSH transport is not yet implemented"
    log_error "transport_ssh.sh" "transport_init" "Destination '$dest_name' cannot be used"
    log_info "transport_ssh.sh" "transport_init" "To implement: Add SSH key-based authentication and rsync -e ssh support"
    
    return 1
}

#################################################################################
# transport_sync - Rsync over SSH (NOT IMPLEMENTED)
#################################################################################
transport_sync() {
    local source_path="$1"
    local dest_path="$2"
    local sync_mode="${3:-mirror}"
    local bwlimit="${4:-0}"
    local dry_run="${5:-false}"
    local dest_name="${6:-ssh}"
    
    log_error "transport_ssh.sh" "transport_sync" "SSH transport is not yet implemented"
    
    TRANSPORT_BYTES_TRANSFERRED=0
    TRANSPORT_SYNC_DURATION=0
    TRANSPORT_BWLIMIT_FINAL="$bwlimit"
    
    return 1
}

#################################################################################
# transport_verify - Verify SSH sync (NOT IMPLEMENTED)
#################################################################################
transport_verify() {
    local source_path="$1"
    local dest_path="$2"
    local verify_mode="${3:-size}"
    
    log_error "transport_ssh.sh" "transport_verify" "SSH transport is not yet implemented"
    
    return 1
}

#################################################################################
# transport_cleanup - Cleanup (no-op stub)
#################################################################################
transport_cleanup() {
    local dest_path="$1"
    log_debug "transport_ssh.sh" "transport_cleanup" "No cleanup required (stub)"
    return 0
}

#################################################################################
# transport_get_free_space - Get remote free space (NOT IMPLEMENTED)
#################################################################################
transport_get_free_space() {
    local dest_path="$1"
    
    log_warn "transport_ssh.sh" "transport_get_free_space" "SSH transport not implemented - returning 0"
    echo "0"
    return 1
}
