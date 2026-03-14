h#!/usr/bin/env bash
#===============================================================================
# chain_validation.sh - Chain Integrity Validation Module
#===============================================================================
# Provides functions to validate backup chain integrity for restoration support.
# Scans chain directories and determines which checkpoints are restorable.
#
# Key Functions:
#   validate_chain_integrity() - Main validation function
#   quick_chain_validation()   - Fast file-existence check
#   deep_chain_validation()    - Full checksum verification (expensive)
#
# Schema:
#   .cpt file: JSON array of checkpoint names ["virtnbdbackup.0", "virtnbdbackup.1", ...]
#   Checkpoint N (N=0): vda.full.data + checkpoints/virtnbdbackup.0.xml
#   Checkpoint N (N>0): vda.inc.virtnbdbackup.N.data + checkpoints/virtnbdbackup.N.xml
#
# Version: 1.0.0 (2026-02-06)
#===============================================================================

# Module guard
[[ -n "${CHAIN_VALIDATION_LOADED:-}" ]] && return 0
declare -g CHAIN_VALIDATION_LOADED=true

#===============================================================================
# GLOBAL RESULTS (set by validate_chain_integrity)
#===============================================================================
declare -g CHAIN_VALID=""              # true/false
declare -g CHAIN_TOTAL_CHECKPOINTS=0   # Total checkpoints in .cpt
declare -g CHAIN_RESTORABLE_COUNT=0    # Count of restorable checkpoints
declare -g CHAIN_BROKEN_AT=""          # First broken checkpoint index (empty if healthy)
declare -g CHAIN_BREAK_REASON=""       # Reason for break (if any)
declare -g CHAIN_DISK_NAMES=""         # Comma-separated list of disk names (vda,vdb,...)

#===============================================================================
# LOGGING HELPERS
#===============================================================================
_cv_log() {
    local level="$1"
    local message="$2"
    local func="${FUNCNAME[2]:-validate_chain_integrity}"
    
    if declare -f log_msg >/dev/null 2>&1; then
        log_msg "$level" "chain_validation.sh" "$func" "$message"
    else
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] [$level] [chain_validation.sh] [$func] $message"
    fi
}

_cv_debug() { _cv_log "DEBUG" "$1"; }
_cv_info()  { _cv_log "INFO" "$1"; }
_cv_warn()  { _cv_log "WARN" "$1"; }
_cv_error() { _cv_log "ERROR" "$1"; }

#===============================================================================
# validate_chain_integrity
#===============================================================================
# Main chain validation function. Scans a backup directory and validates
# that all checkpoints have their required files present.
#
# Arguments:
#   $1 - vm_name        : Virtual machine name
#   $2 - chain_dir      : Absolute path to chain directory
#   $3 - mode           : "quick" (file existence) or "deep" (checksum verify)
#
# Sets globals:
#   CHAIN_VALID, CHAIN_TOTAL_CHECKPOINTS, CHAIN_RESTORABLE_COUNT,
#   CHAIN_BROKEN_AT, CHAIN_BREAK_REASON, CHAIN_DISK_NAMES
#
# Returns:
#   0 - Validation completed (check CHAIN_VALID for result)
#   1 - Validation error (directory not found, .cpt missing, etc.)
#===============================================================================
validate_chain_integrity() {
    local vm_name="$1"
    local chain_dir="$2"
    local mode="${3:-quick}"
    
    # Reset globals
    CHAIN_VALID=""
    CHAIN_TOTAL_CHECKPOINTS=0
    CHAIN_RESTORABLE_COUNT=0
    CHAIN_BROKEN_AT=""
    CHAIN_BREAK_REASON=""
    CHAIN_DISK_NAMES=""
    
    _cv_debug "Validating chain: vm=$vm_name, dir=$chain_dir, mode=$mode"
    
    # Validate inputs
    if [[ -z "$vm_name" || -z "$chain_dir" ]]; then
        _cv_error "Missing required arguments: vm_name='$vm_name', chain_dir='$chain_dir'"
        return 1
    fi
    
    # Check directory exists
    if [[ ! -d "$chain_dir" ]]; then
        _cv_error "Chain directory does not exist: $chain_dir"
        CHAIN_VALID="false"
        CHAIN_BREAK_REASON="directory_not_found"
        return 1
    fi
    
    # Find .cpt file
    local cpt_file
    cpt_file=$(find "$chain_dir" -maxdepth 1 -name "*.cpt" 2>/dev/null | head -1)
    
    if [[ -z "$cpt_file" || ! -f "$cpt_file" ]]; then
        _cv_warn "No .cpt file found in $chain_dir"
        
        # Check for copy-mode backup (no checkpoints)
        if find "$chain_dir" -maxdepth 1 -name "*.copy.data" 2>/dev/null | grep -q .; then
            _cv_info "Copy-mode backup detected (no checkpoint chain)"
            CHAIN_VALID="true"
            CHAIN_TOTAL_CHECKPOINTS=1
            CHAIN_RESTORABLE_COUNT=1
            return 0
        fi
        
        CHAIN_VALID="false"
        CHAIN_BREAK_REASON="no_cpt_file"
        return 1
    fi
    
    # Check .cpt file is non-empty
    if [[ ! -s "$cpt_file" ]]; then
        _cv_error ".cpt file is empty: $cpt_file"
        CHAIN_VALID="false"
        CHAIN_BREAK_REASON="empty_cpt_file"
        return 1
    fi
    
    # Parse .cpt file (JSON array)
    local cpt_content
    cpt_content=$(cat "$cpt_file" 2>/dev/null)
    
    # Validate JSON format
    if ! echo "$cpt_content" | grep -qE '^\[.*\]$'; then
        _cv_error ".cpt file is not valid JSON array: $cpt_content"
        CHAIN_VALID="false"
        CHAIN_BREAK_REASON="invalid_cpt_format"
        return 1
    fi
    
    # Extract checkpoint names from JSON array
    # Format: ["virtnbdbackup.0", "virtnbdbackup.1", ...]
    local checkpoints=()
    while IFS= read -r cp; do
        [[ -n "$cp" ]] && checkpoints+=("$cp")
    done < <(echo "$cpt_content" | tr '[],"' '\n' | grep -E '^virtnbdbackup\.[0-9]+$')
    
    CHAIN_TOTAL_CHECKPOINTS=${#checkpoints[@]}
    _cv_debug "Found $CHAIN_TOTAL_CHECKPOINTS checkpoints in .cpt file"
    
    if [[ $CHAIN_TOTAL_CHECKPOINTS -eq 0 ]]; then
        _cv_error "No checkpoints found in .cpt file"
        CHAIN_VALID="false"
        CHAIN_BREAK_REASON="empty_checkpoint_list"
        return 1
    fi
    
    # Detect disk names from .full.data files
    local disk_names=()
    while IFS= read -r data_file; do
        local disk_name
        disk_name=$(basename "$data_file" | sed 's/\.full\.data$//')
        disk_names+=("$disk_name")
    done < <(find "$chain_dir" -maxdepth 1 -name "*.full.data" 2>/dev/null)
    
    if [[ ${#disk_names[@]} -eq 0 ]]; then
        _cv_error "No .full.data files found - chain has no base backup"
        CHAIN_VALID="false"
        CHAIN_BROKEN_AT="0"
        CHAIN_BREAK_REASON="no_full_backup"
        return 0
    fi
    
    CHAIN_DISK_NAMES=$(IFS=,; echo "${disk_names[*]}")
    _cv_debug "Disk names: $CHAIN_DISK_NAMES"
    
    # Validate each checkpoint
    local restorable=0
    local first_broken=""
    local break_reason=""
    
    for ((i=0; i<CHAIN_TOTAL_CHECKPOINTS; i++)); do
        local cp_name="${checkpoints[$i]}"
        local cp_num
        cp_num=$(echo "$cp_name" | grep -oE '[0-9]+$')
        
        _cv_debug "Checking checkpoint $i: $cp_name (num=$cp_num)"
        
        local cp_valid=true
        local cp_reason=""
        
        # Check for each disk
        for disk in "${disk_names[@]}"; do
            local data_file
            local xml_file="$chain_dir/checkpoints/$cp_name.xml"
            
            if [[ $cp_num -eq 0 ]]; then
                # Full backup
                data_file="$chain_dir/${disk}.full.data"
            else
                # Incremental
                data_file="$chain_dir/${disk}.inc.${cp_name}.data"
            fi
            
            # Check data file exists
            if [[ ! -f "$data_file" ]]; then
                cp_valid=false
                cp_reason="missing_data_file:$data_file"
                _cv_warn "Checkpoint $i ($cp_name): Missing data file: $data_file"
                break
            fi
            
            # Check for .partial file (interrupted backup)
            if [[ -f "${data_file}.partial" ]]; then
                cp_valid=false
                cp_reason="partial_file:${data_file}.partial"
                _cv_warn "Checkpoint $i ($cp_name): Found .partial file (interrupted backup)"
                break
            fi
            
            # Check checkpoint XML exists
            if [[ ! -f "$xml_file" ]]; then
                cp_valid=false
                cp_reason="missing_xml:$xml_file"
                _cv_warn "Checkpoint $i ($cp_name): Missing XML: $xml_file"
                break
            fi
            
            # Deep mode: verify checksums
            if [[ "$mode" == "deep" ]]; then
                local chksum_file="${data_file}.chksum"
                if [[ -f "$chksum_file" ]]; then
                    local expected_sum
                    expected_sum=$(cat "$chksum_file" 2>/dev/null | awk '{print $1}')
                    if [[ -n "$expected_sum" ]]; then
                        local actual_sum
                        actual_sum=$(sha256sum "$data_file" 2>/dev/null | awk '{print $1}')
                        if [[ "$expected_sum" != "$actual_sum" ]]; then
                            cp_valid=false
                            cp_reason="checksum_mismatch:$data_file"
                            _cv_error "Checkpoint $i ($cp_name): Checksum mismatch for $data_file"
                            break
                        fi
                        _cv_debug "Checkpoint $i ($cp_name): Checksum verified for $disk"
                    fi
                fi
            fi
        done
        
        if $cp_valid; then
            ((restorable++))
            _cv_debug "Checkpoint $i ($cp_name): VALID"
        else
            if [[ -z "$first_broken" ]]; then
                first_broken="$i"
                break_reason="$cp_reason"
            fi
            _cv_debug "Checkpoint $i ($cp_name): BROKEN - $cp_reason"
            # Once we find a broken checkpoint, remaining are also not restorable
            # (chain is sequential)
            break
        fi
    done
    
    CHAIN_RESTORABLE_COUNT=$restorable
    CHAIN_BROKEN_AT="$first_broken"
    CHAIN_BREAK_REASON="$break_reason"
    
    if [[ -z "$first_broken" ]]; then
        CHAIN_VALID="true"
        _cv_info "Chain validation: HEALTHY - $restorable/$CHAIN_TOTAL_CHECKPOINTS restorable"
    else
        CHAIN_VALID="false"
        _cv_warn "Chain validation: BROKEN at checkpoint $first_broken - $restorable/$CHAIN_TOTAL_CHECKPOINTS restorable"
    fi
    
    return 0
}

#===============================================================================
# quick_chain_validation
#===============================================================================
# Convenience wrapper for quick (file-existence) validation.
#===============================================================================
quick_chain_validation() {
    validate_chain_integrity "$1" "$2" "quick"
}

#===============================================================================
# deep_chain_validation
#===============================================================================
# Convenience wrapper for deep (checksum) validation.
# WARNING: This can be slow for large backups!
#===============================================================================
deep_chain_validation() {
    validate_chain_integrity "$1" "$2" "deep"
}

#===============================================================================
# get_chain_validation_result
#===============================================================================
# Returns a JSON object with validation results.
#===============================================================================
get_chain_validation_result() {
    cat <<EOF
{
  "valid": $CHAIN_VALID,
  "total_checkpoints": $CHAIN_TOTAL_CHECKPOINTS,
  "restorable_count": $CHAIN_RESTORABLE_COUNT,
  "broken_at": ${CHAIN_BROKEN_AT:-null},
  "break_reason": "${CHAIN_BREAK_REASON:-}",
  "disk_names": "$CHAIN_DISK_NAMES"
}
EOF
}

#===============================================================================
# validate_chain_for_restore
#===============================================================================
# Validates that a specific checkpoint is restorable.
#
# Arguments:
#   $1 - vm_name
#   $2 - chain_dir
#   $3 - checkpoint_index (0-based)
#
# Returns:
#   0 - Checkpoint is restorable
#   1 - Checkpoint is NOT restorable (chain broken before this point)
#===============================================================================
validate_chain_for_restore() {
    local vm_name="$1"
    local chain_dir="$2"
    local target_checkpoint="$3"
    
    # Run validation
    validate_chain_integrity "$vm_name" "$chain_dir" "quick"
    
    if [[ "$CHAIN_VALID" != "true" ]]; then
        # Chain has issues - check if target is restorable
        if [[ -n "$CHAIN_BROKEN_AT" ]]; then
            if [[ $target_checkpoint -lt $CHAIN_BROKEN_AT ]]; then
                _cv_info "Checkpoint $target_checkpoint is restorable (chain broken at $CHAIN_BROKEN_AT)"
                return 0
            else
                _cv_error "Checkpoint $target_checkpoint is NOT restorable (chain broken at $CHAIN_BROKEN_AT)"
                return 1
            fi
        fi
        _cv_error "Chain validation failed: $CHAIN_BREAK_REASON"
        return 1
    fi
    
    # Chain is fully valid
    if [[ $target_checkpoint -lt $CHAIN_TOTAL_CHECKPOINTS ]]; then
        _cv_info "Checkpoint $target_checkpoint is restorable (chain healthy)"
        return 0
    else
        _cv_error "Checkpoint $target_checkpoint does not exist (chain has $CHAIN_TOTAL_CHECKPOINTS)"
        return 1
    fi
}

#===============================================================================
# scan_all_chains
#===============================================================================
# Scans all backup chains for a VM and returns validation summary.
#
# Arguments:
#   $1 - vm_name
#   $2 - backup_root (e.g., /mnt/backup/vms)
#
# Output: Tab-separated lines:
#   period_id<TAB>valid<TAB>total<TAB>restorable<TAB>broken_at<TAB>reason
#===============================================================================
scan_all_chains() {
    local vm_name="$1"
    local backup_root="$2"
    local vm_dir="$backup_root/$vm_name"
    
    if [[ ! -d "$vm_dir" ]]; then
        _cv_error "VM directory not found: $vm_dir"
        return 1
    fi
    
    # Find all period directories
    while IFS= read -r period_dir; do
        local period_id
        period_id=$(basename "$period_dir")
        
        # Skip special directories
        [[ "$period_id" == "_state" || "$period_id" == "_archive" ]] && continue
        
        # Validate chain
        validate_chain_integrity "$vm_name" "$period_dir" "quick"
        
        echo -e "$period_id\t$CHAIN_VALID\t$CHAIN_TOTAL_CHECKPOINTS\t$CHAIN_RESTORABLE_COUNT\t${CHAIN_BROKEN_AT:-}\t${CHAIN_BREAK_REASON:-}"
    done < <(find "$vm_dir" -mindepth 1 -maxdepth 1 -type d 2>/dev/null | sort)
}

#===============================================================================
# EXPORTS
#===============================================================================
export -f validate_chain_integrity
export -f quick_chain_validation
export -f deep_chain_validation
export -f get_chain_validation_result
export -f validate_chain_for_restore
export -f scan_all_chains

_cv_debug "Chain validation module loaded (v1.0.0)"
