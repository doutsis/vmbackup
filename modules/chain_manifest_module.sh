#!/bin/bash

#################################################################################
# Chain Manifest Module - JSON Manifest Management for Backup Chains
#
# Manages the chain-manifest.json files that track restore points within each
# VM's backup directory. Provides discovery, rebuilding, and validation.
#
# MANIFEST LOCATION: ${BACKUP_PATH}/${vm_name}/chain-manifest.json
#
# MANIFEST SCHEMA VERSION: 1.0 (2026-02-01)
#
# Dependencies:
#   - jq: JSON processing
#   - rotation_module.sh: get_vm_backup_dir(), sanitize_vm_name()
#   - logging_module.sh: log_config_event(), log_file_operation()
#
# Usage:
#   source chain_manifest_module.sh
#   init_chain_manifest "vm-name"
#   add_restore_point "vm-name" "$restore_point_id" "$chain_id" "$checkpoint" ...
#   get_restore_points "vm-name"
#################################################################################

# Guard against multiple inclusion
[[ -n "${_CHAIN_MANIFEST_MODULE_LOADED:-}" ]] && return 0
readonly _CHAIN_MANIFEST_MODULE_LOADED=1

# Module version
readonly CHAIN_MANIFEST_MODULE_VERSION="1.0"

#################################################################################
# MANIFEST STRUCTURE
#################################################################################

# JSON Schema:
# {
#   "version": "1.0",
#   "vm_name": "example-vm",
#   "created": "2026-02-01T12:00:00+00:00",
#   "updated": "2026-02-01T14:30:00+00:00",
#   "rotation_policy": "monthly",
#   "restore_points": [
#     {
#       "id": "example-vm:202602:chain-20260201-120000:0",
#       "period_id": "202602",
#       "chain_id": "chain-20260201-120000",
#       "checkpoint": 0,
#       "backup_type": "full",
#       "backup_file": "backup-example-vm.qcow2",
#       "timestamp": "2026-02-01T12:00:00+00:00",
#       "size_bytes": 10737418240,
#       "chain_status": "active",
#       "parent_id": null
#     }
#   ],
#   "chains": {
#     "chain-20260201-120000": {
#       "period_id": "202602",
#       "status": "active",
#       "started": "2026-02-01T12:00:00+00:00",
#       "checkpoint_count": 1,
#       "total_bytes": 10737418240,
#       "base_backup": "backup-example-vm.qcow2"
#     }
#   }
# }

#################################################################################
# PATH FUNCTIONS
#################################################################################

# Get manifest path for a VM
# Args: $1 - vm_name
# Returns: Full path to chain-manifest.json
get_manifest_path() {
    local vm_name="$1"
    echo "${BACKUP_PATH}$(sanitize_vm_name "$vm_name")/chain-manifest.json"
}

#################################################################################
# MANIFEST INITIALIZATION
#################################################################################

# Initialize or validate chain manifest for VM
# Args: $1 - vm_name
# Returns: 0 on success, 1 on error
init_chain_manifest() {
    local vm_name="$1"
    local manifest_path=$(get_manifest_path "$vm_name")
    local manifest_dir=$(dirname "$manifest_path")
    
    # Ensure directory exists
    [[ -d "$manifest_dir" ]] || mkdir -p "$manifest_dir" || {
        log_error "chain_manifest_module.sh" "init_chain_manifest" \
            "Failed to create directory: $manifest_dir"
        return 1
    }
    
    # Create new manifest if doesn't exist
    if [[ ! -f "$manifest_path" ]]; then
        local policy=$(get_vm_rotation_policy "$vm_name")
        local timestamp=$(date -Iseconds)
        
        cat > "$manifest_path" << EOF
{
  "version": "1.0",
  "vm_name": "$vm_name",
  "created": "$timestamp",
  "updated": "$timestamp",
  "rotation_policy": "$policy",
  "restore_points": [],
  "chains": {}
}
EOF
        
        log_file_operation "create" "$vm_name" "$manifest_path" "" \
            "manifest" "Chain manifest initialized" "init_chain_manifest" "true"
        
        log_debug "chain_manifest_module.sh" "init_chain_manifest" \
            "Created manifest for $vm_name"
        return 0
    fi
    
    # Validate existing manifest
    _validate_manifest "$manifest_path" || {
        log_warn "chain_manifest_module.sh" "init_chain_manifest" \
            "Invalid manifest, attempting rebuild: $manifest_path"
        rebuild_chain_manifest "$vm_name"
    }
    
    return 0
}

# Validate manifest JSON structure
# Args: $1 - manifest_path
# Returns: 0 if valid, 1 if invalid
_validate_manifest() {
    local manifest_path="$1"
    
    # Check JSON validity and required fields in one jq call
    jq -e '
        .version and .vm_name and 
        (.restore_points | type == "array") and 
        (.chains | type == "object")
    ' "$manifest_path" >/dev/null 2>&1
}

#################################################################################
# RESTORE POINT MANAGEMENT
#################################################################################

# Add restore point to manifest
# Args: $1  - vm_name
#       $2  - restore_point_id (vm:period:chain:checkpoint)
#       $3  - period_id
#       $4  - chain_id
#       $5  - checkpoint (0 for full backup)
#       $6  - backup_type (full|incremental|copy)
#       $7  - backup_file (filename relative to backup_dir)
#       $8  - size_bytes
#       $9  - parent_id (null for full backups, restore_point_id of parent)
# Returns: 0 on success, 1 on error
add_restore_point() {
    local vm_name="$1"
    local restore_point_id="$2"
    local period_id="$3"
    local chain_id="$4"
    local checkpoint="$5"
    local backup_type="$6"
    local backup_file="$7"
    local size_bytes="$8"
    local parent_id="${9:-null}"
    
    local manifest_path
    manifest_path=$(get_manifest_path "$vm_name")
    local timestamp
    timestamp=$(date -Iseconds)
    
    # Ensure manifest exists
    init_chain_manifest "$vm_name" || return 1
    
    # Prepare parent_id for JSON (null or quoted string)
    local parent_json
    if [[ "$parent_id" == "null" ]] || [[ -z "$parent_id" ]]; then
        parent_json="null"
    else
        parent_json="\"$parent_id\""
    fi
    
    # Add restore point using jq
    local tmp_manifest="${manifest_path}.tmp.$$"
    jq --arg rpid "$restore_point_id" \
       --arg period "$period_id" \
       --arg chain "$chain_id" \
       --argjson cp "$checkpoint" \
       --arg btype "$backup_type" \
       --arg bfile "$backup_file" \
       --arg ts "$timestamp" \
       --argjson size "$size_bytes" \
       --argjson parent "$parent_json" \
       '.restore_points += [{
           "id": $rpid,
           "period_id": $period,
           "chain_id": $chain,
           "checkpoint": $cp,
           "backup_type": $btype,
           "backup_file": $bfile,
           "timestamp": $ts,
           "size_bytes": $size,
           "chain_status": "active",
           "parent_id": $parent
       }] | .updated = $ts' \
       "$manifest_path" > "$tmp_manifest" || {
           rm -f "$tmp_manifest"
           log_error "chain_manifest_module.sh" "add_restore_point" \
               "Failed to add restore point: $restore_point_id"
           return 1
       }
    
    mv "$tmp_manifest" "$manifest_path"
    
    # Update chain info
    _update_chain_info "$vm_name" "$chain_id" "$period_id" "$checkpoint" "$size_bytes" "$backup_file"
    
    log_debug "chain_manifest_module.sh" "add_restore_point" \
        "Added restore point: $restore_point_id"
    
    return 0
}

# Update chain metadata
# Args: $1 - vm_name
#       $2 - chain_id
#       $3 - period_id
#       $4 - checkpoint
#       $5 - size_bytes
#       $6 - base_backup (only used for checkpoint 0)
_update_chain_info() {
    local vm_name="$1"
    local chain_id="$2"
    local period_id="$3"
    local checkpoint="$4"
    local size_bytes="$5"
    local base_backup="$6"
    
    local manifest_path
    manifest_path=$(get_manifest_path "$vm_name")
    local timestamp
    timestamp=$(date -Iseconds)
    
    local tmp_manifest="${manifest_path}.tmp.$$"
    
    # Check if chain exists
    local chain_exists
    chain_exists=$(jq -r --arg c "$chain_id" '.chains[$c] // empty' "$manifest_path")
    
    if [[ -z "$chain_exists" ]]; then
        # Create new chain entry
        jq --arg chain "$chain_id" \
           --arg period "$period_id" \
           --arg ts "$timestamp" \
           --argjson size "$size_bytes" \
           --arg base "$base_backup" \
           '.chains[$chain] = {
               "period_id": $period,
               "status": "active",
               "started": $ts,
               "checkpoint_count": 1,
               "total_bytes": $size,
               "base_backup": $base
           } | .updated = $ts' \
           "$manifest_path" > "$tmp_manifest"
    else
        # Update existing chain
        jq --arg chain "$chain_id" \
           --arg ts "$timestamp" \
           --argjson size "$size_bytes" \
           '.chains[$chain].checkpoint_count += 1 |
            .chains[$chain].total_bytes += $size |
            .updated = $ts' \
           "$manifest_path" > "$tmp_manifest"
    fi
    
    mv "$tmp_manifest" "$manifest_path"
}

# Mark chain as archived
# Args: $1 - vm_name
#       $2 - chain_id
#       $3 - archive_location (e.g., .chain-20260201-120000/)
# Returns: 0 on success, 1 on error
archive_chain_in_manifest() {
    local vm_name="$1"
    local chain_id="$2"
    local archive_location="$3"
    
    local manifest_path
    manifest_path=$(get_manifest_path "$vm_name")
    local timestamp
    timestamp=$(date -Iseconds)
    
    local tmp_manifest="${manifest_path}.tmp.$$"
    
    jq --arg chain "$chain_id" \
       --arg ts "$timestamp" \
       --arg loc "$archive_location" \
       '(.restore_points[] | select(.chain_id == $chain)).chain_status = "archived" |
        .chains[$chain].status = "archived" |
        .chains[$chain].archive_location = $loc |
        .chains[$chain].archived = $ts |
        .updated = $ts' \
       "$manifest_path" > "$tmp_manifest" || {
           rm -f "$tmp_manifest"
           return 1
       }
    
    mv "$tmp_manifest" "$manifest_path"
    
    log_debug "chain_manifest_module.sh" "archive_chain_in_manifest" \
        "Archived chain in manifest: $chain_id -> $archive_location"
    
    return 0
}

#################################################################################
# QUERY FUNCTIONS
#################################################################################

# Get all restore points for a VM
# Args: $1 - vm_name
#       $2 - filter (optional: active|archived|all, default: all)
# Returns: JSON array of restore points
get_restore_points() {
    local vm_name="$1"
    local filter="${2:-all}"
    local manifest_path=$(get_manifest_path "$vm_name")
    
    [[ ! -f "$manifest_path" ]] && { echo "[]"; return 0; }
    
    case "$filter" in
        active)   jq '[.restore_points[] | select(.chain_status == "active")]' "$manifest_path" ;;
        archived) jq '[.restore_points[] | select(.chain_status == "archived")]' "$manifest_path" ;;
        *)        jq '.restore_points' "$manifest_path" ;;
    esac
}

# Get specific restore point
# Args: $1 - vm_name
#       $2 - restore_point_id
# Returns: JSON object of restore point (empty if not found)
get_restore_point() {
    local vm_name="$1"
    local restore_point_id="$2"
    local manifest_path=$(get_manifest_path "$vm_name")
    
    [[ ! -f "$manifest_path" ]] && { echo "{}"; return 1; }
    
    jq --arg id "$restore_point_id" '.restore_points[] | select(.id == $id)' "$manifest_path"
}

# Get chain info
# Args: $1 - vm_name
#       $2 - chain_id
# Returns: JSON object of chain (empty if not found)
get_chain_info() {
    local vm_name="$1"
    local chain_id="$2"
    local manifest_path=$(get_manifest_path "$vm_name")
    
    [[ ! -f "$manifest_path" ]] && { echo "{}"; return 1; }
    
    jq --arg c "$chain_id" '.chains[$c] // {}' "$manifest_path"
}

# Get active chain for a VM
# Args: $1 - vm_name
# Returns: chain_id of active chain (empty if none)
get_active_chain() {
    local vm_name="$1"
    local manifest_path=$(get_manifest_path "$vm_name")
    
    [[ ! -f "$manifest_path" ]] && { echo ""; return 0; }
    
    jq -r '.chains | to_entries[] | select(.value.status == "active") | .key' "$manifest_path" | head -1
}

# Count restore points in period
# Args: $1 - vm_name
#       $2 - period_id
# Returns: count
count_period_restore_points() {
    local vm_name="$1"
    local period_id="$2"
    local manifest_path=$(get_manifest_path "$vm_name")
    
    [[ ! -f "$manifest_path" ]] && { echo "0"; return 0; }
    
    jq --arg p "$period_id" '[.restore_points[] | select(.period_id == $p)] | length' "$manifest_path"
}

#################################################################################
# DISCOVERY AND REBUILD
#################################################################################

# Rebuild manifest from filesystem
# Scans VM backup directory and reconstructs manifest from actual files
# Args: $1 - vm_name
# Returns: 0 on success, 1 on error
rebuild_chain_manifest() {
    local vm_name="$1"
    local safe_name
    safe_name=$(sanitize_vm_name "$vm_name")
    local vm_dir="${BACKUP_PATH}${safe_name}"
    local manifest_path
    manifest_path=$(get_manifest_path "$vm_name")
    
    if [[ ! -d "$vm_dir" ]]; then
        log_warn "chain_manifest_module.sh" "rebuild_chain_manifest" \
            "VM directory not found: $vm_dir"
        return 1
    fi
    
    log_info "chain_manifest_module.sh" "rebuild_chain_manifest" \
        "Rebuilding manifest for: $vm_name"
    
    local policy
    policy=$(get_vm_rotation_policy "$vm_name")
    local timestamp
    timestamp=$(date -Iseconds)
    
    # Initialize fresh manifest
    local tmp_manifest="${manifest_path}.rebuild.$$"
    cat > "$tmp_manifest" << EOF
{
  "version": "1.0",
  "vm_name": "$vm_name",
  "created": "$timestamp",
  "updated": "$timestamp",
  "rotation_policy": "$policy",
  "restore_points": [],
  "chains": {},
  "rebuilt": true,
  "rebuild_timestamp": "$timestamp"
}
EOF
    
    # Discover period directories
    local period_dirs
    case "$policy" in
        daily)
            period_dirs=$(find "$vm_dir" -maxdepth 1 -type d -regextype posix-extended \
                         -regex '.*/[0-9]{8}$' 2>/dev/null | sort)
            ;;
        weekly)
            period_dirs=$(find "$vm_dir" -maxdepth 1 -type d -regextype posix-extended \
                         -regex '.*/[0-9]{4}-W[0-9]{2}$' 2>/dev/null | sort)
            ;;
        monthly)
            period_dirs=$(find "$vm_dir" -maxdepth 1 -type d -regextype posix-extended \
                         -regex '.*/[0-9]{6}$' 2>/dev/null | sort)
            ;;
        accumulate)
            # Flat structure - no period dirs
            period_dirs=""
            _discover_flat_backups "$vm_name" "$vm_dir" "$tmp_manifest"
            ;;
    esac
    
    # Process each period directory
    for period_dir in $period_dirs; do
        local period_id
        period_id=$(basename "$period_dir")
        _discover_period_backups "$vm_name" "$period_id" "$period_dir" "$tmp_manifest"
    done
    
    # Check for archived chains
    local archive_dirs
    archive_dirs=$(find "$vm_dir" -maxdepth 2 -type d -name '.chain-*' 2>/dev/null)
    for archive_dir in $archive_dirs; do
        _discover_archived_chain "$vm_name" "$archive_dir" "$tmp_manifest"
    done
    
    # Move rebuilt manifest into place
    mv "$tmp_manifest" "$manifest_path"
    
    log_config_event "manifest_rebuilt" "$manifest_path" "$vm_name" \
        "chain-manifest.json" "rebuilt" "" "$vm_name" "rebuild_chain_manifest"
    
    log_info "chain_manifest_module.sh" "rebuild_chain_manifest" \
        "Manifest rebuilt for: $vm_name"
    
    return 0
}

# Discover backups in a period directory
# Args: $1 - vm_name
#       $2 - period_id
#       $3 - period_dir
#       $4 - manifest_path
_discover_period_backups() {
    local vm_name="$1"
    local period_id="$2"
    local period_dir="$3"
    local manifest_path="$4"
    
    # Find full backups (backup-*.qcow2)
    local full_backups
    full_backups=$(find "$period_dir" -maxdepth 1 -type f -name "backup-*.qcow2" 2>/dev/null | sort)
    
    # Find incremental backups (backup-*.inc-*.qcow2)
    local inc_backups
    inc_backups=$(find "$period_dir" -maxdepth 1 -type f -name "backup-*.inc-*.qcow2" 2>/dev/null | sort)
    
    # Find copy backups (backup-*.copy-*.qcow2)
    local copy_backups
    copy_backups=$(find "$period_dir" -maxdepth 1 -type f -name "backup-*.copy-*.qcow2" 2>/dev/null | sort)
    
    # Determine chain from backup files
    local chain_id=""
    if [[ -n "$full_backups" ]]; then
        local first_backup
        first_backup=$(echo "$full_backups" | head -1)
        local mtime
        mtime=$(stat -c %Y "$first_backup" 2>/dev/null || date +%s)
        chain_id="chain-$(date -d @"$mtime" +%Y%m%d-%H%M%S)"
    fi
    
    # Process full backups
    local checkpoint=0
    for backup_file in $full_backups; do
        _add_discovered_restore_point "$vm_name" "$period_id" "$chain_id" \
            "$checkpoint" "full" "$backup_file" "$manifest_path"
        ((checkpoint++)) || true
    done
    
    # Process incremental backups
    for backup_file in $inc_backups; do
        _add_discovered_restore_point "$vm_name" "$period_id" "$chain_id" \
            "$checkpoint" "incremental" "$backup_file" "$manifest_path"
        ((checkpoint++)) || true
    done
    
    # Process copy backups
    for backup_file in $copy_backups; do
        _add_discovered_restore_point "$vm_name" "$period_id" "$chain_id" \
            "$checkpoint" "copy" "$backup_file" "$manifest_path"
        ((checkpoint++)) || true
    done
}

# Discover backups in flat (accumulate) structure
# Args: $1 - vm_name
#       $2 - vm_dir
#       $3 - manifest_path
_discover_flat_backups() {
    local vm_name="$1"
    local vm_dir="$2"
    local manifest_path="$3"
    
    # All qcow2 files directly under vm_dir
    local backups
    backups=$(find "$vm_dir" -maxdepth 1 -type f -name "*.qcow2" 2>/dev/null | sort)
    
    local checkpoint=0
    for backup_file in $backups; do
        local mtime
        mtime=$(stat -c %Y "$backup_file" 2>/dev/null || date +%s)
        local chain_id="chain-$(date -d @"$mtime" +%Y%m%d-%H%M%S)"
        
        _add_discovered_restore_point "$vm_name" "flat" "$chain_id" \
            "$checkpoint" "full" "$backup_file" "$manifest_path"
        ((checkpoint++)) || true
    done
}

# Discover archived chain
# Args: $1 - vm_name
#       $2 - archive_dir
#       $3 - manifest_path
_discover_archived_chain() {
    local vm_name="$1"
    local archive_dir="$2"
    local manifest_path="$3"
    
    local chain_id
    chain_id=$(basename "$archive_dir")
    local period_id
    period_id=$(basename "$(dirname "$archive_dir")")
    
    # Find backups in archive
    local backups
    backups=$(find "$archive_dir" -maxdepth 1 -type f -name "*.qcow2" 2>/dev/null | sort)
    
    local checkpoint=0
    for backup_file in $backups; do
        _add_discovered_restore_point "$vm_name" "$period_id" "$chain_id" \
            "$checkpoint" "archived" "$backup_file" "$manifest_path" "archived"
        ((checkpoint++)) || true
    done
}

# Add discovered restore point to manifest
# Args: $1 - vm_name
#       $2 - period_id
#       $3 - chain_id
#       $4 - checkpoint
#       $5 - backup_type
#       $6 - backup_file_path
#       $7 - manifest_path
#       $8 - chain_status (optional, default: active)
_add_discovered_restore_point() {
    local vm_name="$1"
    local period_id="$2"
    local chain_id="$3"
    local checkpoint="$4"
    local backup_type="$5"
    local backup_file_path="$6"
    local manifest_path="$7"
    local chain_status="${8:-active}"
    
    local backup_file
    backup_file=$(basename "$backup_file_path")
    local size_bytes
    size_bytes=$(stat -c %s "$backup_file_path" 2>/dev/null || echo 0)
    local mtime
    mtime=$(stat -c %Y "$backup_file_path" 2>/dev/null || date +%s)
    local timestamp
    timestamp=$(date -d @"$mtime" -Iseconds)
    
    local restore_point_id="${vm_name}:${period_id}:${chain_id}:${checkpoint}"
    
    local parent_id="null"
    if [[ "$checkpoint" -gt 0 ]]; then
        parent_id="${vm_name}:${period_id}:${chain_id}:$((checkpoint - 1))"
    fi
    
    # Prepare parent_id for JSON
    local parent_json
    if [[ "$parent_id" == "null" ]]; then
        parent_json="null"
    else
        parent_json="\"$parent_id\""
    fi
    
    local tmp_manifest="${manifest_path}.tmp.$$"
    jq --arg rpid "$restore_point_id" \
       --arg period "$period_id" \
       --arg chain "$chain_id" \
       --argjson cp "$checkpoint" \
       --arg btype "$backup_type" \
       --arg bfile "$backup_file" \
       --arg ts "$timestamp" \
       --argjson size "$size_bytes" \
       --arg status "$chain_status" \
       --argjson parent "$parent_json" \
       '.restore_points += [{
           "id": $rpid,
           "period_id": $period,
           "chain_id": $chain,
           "checkpoint": $cp,
           "backup_type": $btype,
           "backup_file": $bfile,
           "timestamp": $ts,
           "size_bytes": $size,
           "chain_status": $status,
           "parent_id": $parent,
           "discovered": true
       }]' \
       "$manifest_path" > "$tmp_manifest" && mv "$tmp_manifest" "$manifest_path"
}

#################################################################################
# MODULE INITIALIZATION
#################################################################################

log_debug "chain_manifest_module.sh" "init" \
    "Chain manifest module v${CHAIN_MANIFEST_MODULE_VERSION} loaded"
