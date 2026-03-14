#!/bin/bash
#################################################################################
# SMB Transport Driver for Replication Module (STUB)
#
# Handles rsync-based replication to pre-mounted SMB/CIFS shares.
# The SMB share must be mounted before replication runs (via fstab or manual mount).
#
# STATUS: NOT YET IMPLEMENTED
# This is a stub file with function signatures. The actual implementation
# will be added when SMB-based replication is needed.
#
# Exported Functions:
#   transport_init()    - Verify SMB mount is accessible
#   transport_sync()    - Perform rsync to SMB mount
#   transport_verify()  - Verify sync completed correctly
#   transport_cleanup() - Cleanup (no-op)
#
# Required Configuration (in replication.conf):
#   DEST_N_PATH     - Local mount point for SMB share
#
# Note: SMB share must be pre-mounted. The transport does not handle
# mounting/unmounting. Configure in /etc/fstab with appropriate credentials.
#
# Version: 1.0 (stub)
# Created: 2026-01-23
#################################################################################

# Transport identification
TRANSPORT_NAME="smb"
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
# For SMB transport:
#   TRANSPORT_THROTTLE_COUNT = -1 (rsync does not throttle)
#   TRANSPORT_DEST_SPACE_KNOWN = 1 when mounted, 0 otherwise
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
# transport_init - Verify SMB mount (NOT IMPLEMENTED)
#################################################################################
transport_init() {
    local dest_path="$1"
    local dest_name="${2:-smb}"
    
    log_error "transport_smb.sh" "transport_init" "SMB transport is not yet implemented"
    log_error "transport_smb.sh" "transport_init" "Destination '$dest_name' cannot be used"
    log_info "transport_smb.sh" "transport_init" "To implement: Add CIFS mount detection and SMB-specific handling"
    
    return 1
}

#################################################################################
# transport_sync - Rsync to SMB mount (NOT IMPLEMENTED)
#################################################################################
transport_sync() {
    local source_path="$1"
    local dest_path="$2"
    local sync_mode="${3:-mirror}"
    local bwlimit="${4:-0}"
    local dry_run="${5:-false}"
    local dest_name="${6:-smb}"
    
    log_error "transport_smb.sh" "transport_sync" "SMB transport is not yet implemented"
    
    TRANSPORT_BYTES_TRANSFERRED=0
    TRANSPORT_SYNC_DURATION=0
    TRANSPORT_BWLIMIT_FINAL="$bwlimit"
    
    return 1
}

#################################################################################
# transport_verify - Verify SMB sync (NOT IMPLEMENTED)
#################################################################################
transport_verify() {
    local source_path="$1"
    local dest_path="$2"
    local verify_mode="${3:-size}"
    
    log_error "transport_smb.sh" "transport_verify" "SMB transport is not yet implemented"
    
    return 1
}

#################################################################################
# transport_cleanup - Cleanup (no-op stub)
#################################################################################
transport_cleanup() {
    local dest_path="$1"
    log_debug "transport_smb.sh" "transport_cleanup" "No cleanup required (stub)"
    return 0
}

#################################################################################
# transport_get_free_space - Get SMB mount free space (NOT IMPLEMENTED)
#################################################################################
transport_get_free_space() {
    local dest_path="$1"
    
    log_warn "transport_smb.sh" "transport_get_free_space" "SMB transport not implemented - returning 0"
    echo "0"
    return 1
}
