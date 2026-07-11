#!/usr/bin/env bash
##############################################################################
# lib/tpm_io.sh — TPM read-side helpers shared by vmbackup, vmrestore, and
#                 modules/tpm_backup_module.sh
#
# UNI-006 (Phase 6 commit 2): unifies the small set of read-only TPM/UUID
# helpers that historically lived inside modules/tpm_backup_module.sh and
# were duplicated (or near-duplicated) by ad-hoc inline code in vmrestore.sh.
#
# Carved functions (semantics preserved byte-for-byte):
#   get_vm_uuid          — virsh dominfo → UUID field
#   has_tpm_device       — true/false based on `<tpm>` element in dumpxml
#   get_tpm_info         — first 5 lines after `<tpm>` element
#   log_tpm              — bridge to log_{debug,info,warn,error} or stderr
#   validate_tpm_backup  — non-empty tpm2* files in <backup_dir>
#   get_tpm_backup_size  — du -sh of backup dir
#
# Modules MUST NOT call source_lib_or_die directly — but per spec §2.1.1
# tpm_backup_module.sh is the ONE intentional exception, since it depends
# on these helpers and predates the binaries' bootstrap. Both binaries also
# load this lib so their inline code can use it without going through the
# (lazy) tpm_backup_module load path.
##############################################################################

# Idempotency guard — re-source is a no-op once the carved API is defined.
declare -F get_vm_uuid >/dev/null 2>&1 && return 0

##############################################################################
# Identity / topology
##############################################################################

# get_vm_uuid <vm_name>
#   Echo the VM's UUID per `virsh dominfo`; empty string on miss.
get_vm_uuid() {
    local vm_name="$1"
    virsh dominfo "$vm_name" 2>/dev/null | grep "^UUID" | awk '{print $2}'
}

# has_tpm_device <vm_name>
#   Return 0 if the domain XML contains a <tpm> element, else 1.
has_tpm_device() {
    local vm_name="$1"
    virsh dumpxml "$vm_name" 2>/dev/null | grep -q '<tpm' && return 0
    return 1
}

# get_tpm_info <vm_name>
#   Print the first 5 lines following the `<tpm>` element.
get_tpm_info() {
    local vm_name="$1"
    virsh dumpxml "$vm_name" 2>/dev/null | grep -A 5 '<tpm'
}

##############################################################################
# Logging bridge
##############################################################################

# log_tpm <level> <message>
#   Prefer log_{debug,info,warn,error} when defined (binary context);
#   fall back to stderr (standalone module use).
log_tpm() {
    local level="$1"
    local message="$2"
    local timestamp
    timestamp=$(date '+%Y-%m-%d %H:%M:%S %Z')

    case "$level" in
        DEBUG) declare -f log_debug >/dev/null 2>&1 && log_debug "tpm_backup_module.sh" "${FUNCNAME[1]:-tpm}" "$message" && return ;;
        INFO)  declare -f log_info  >/dev/null 2>&1 && log_info  "tpm_backup_module.sh" "${FUNCNAME[1]:-tpm}" "$message" && return ;;
        WARN)  declare -f log_warn  >/dev/null 2>&1 && log_warn  "tpm_backup_module.sh" "${FUNCNAME[1]:-tpm}" "$message" && return ;;
        ERROR) declare -f log_error >/dev/null 2>&1 && log_error "tpm_backup_module.sh" "${FUNCNAME[1]:-tpm}" "$message" && return ;;
    esac

    echo "[$timestamp] [$level] TPM: $message" >&2
}

##############################################################################
# Backup-side validation
##############################################################################

# validate_tpm_backup <tpm_backup_dir>
#   Return 0 iff the directory contains a non-empty TPM2 state file.
#   INT-10 (2026-05-23): swtpm writes its persistent state into a `tpm2/`
#   subdirectory (so the on-disk layout is
#   `<tpm_backup_dir>/tpm2/tpm2-00.permall`). The previous glob
#   `"$tpm_backup_dir"/tpm2*` only matched the `tpm2/` directory entry, not
#   the file inside it — so `[[ ! -s "$file" ]]` (file-test on a directory)
#   silently treated the directory as non-empty and validation always
#   passed, hiding partial/corrupt TPM backups. This rewrite uses `find` on
#   the `tpm2/` subdir, requires at least one regular file, and rejects if
#   the canonical `tpm2-00.permall` is missing or smaller than the
#   structural floor (256 bytes).
validate_tpm_backup() {
    local tpm_backup_dir="$1"

    [[ ! -d "$tpm_backup_dir" ]] && return 1

    # Resolve the directory that holds the actual swtpm state files.
    # Modern layout: <dir>/tpm2/tpm2-00.permall. Legacy layout (pre-Phase-5
    # fixtures): files directly under <dir>. Accept both.
    local state_dir="$tpm_backup_dir"
    [[ -d "$tpm_backup_dir/tpm2" ]] && state_dir="$tpm_backup_dir/tpm2"

    # Must contain at least one regular file matching tpm2*.
    local files=()
    while IFS= read -r -d '' f; do
        files+=("$f")
    done < <(find "$state_dir" -maxdepth 1 -type f -name 'tpm2*' -print0 2>/dev/null)

    (( ${#files[@]} == 0 )) && return 1

    # Every present tpm2* file must be non-empty (catches truncation /
    # zero-byte writes from a crashed backup).
    local f
    for f in "${files[@]}"; do
        [[ ! -s "$f" ]] && return 1
    done

    # Structural floor: the canonical permall blob MUST be present and meet a
    # minimum size. It is swtpm's persistent NV/state blob — a TPM backup
    # without it cannot be restored, so its absence is fail-closed (satisfying
    # the documented contract above: "rejects if the canonical tpm2-00.permall
    # is missing"). Previously the size floor was only checked when the blob was
    # present, so a partial backup with a sibling tpm2* file but no permall
    # passed (F-tpm119). swtpm's smallest valid permall is well above 256 bytes;
    # anything below indicates truncation. [[ -s ]] subsumes the empty check.
    local permall="$state_dir/tpm2-00.permall"
    [[ -s "$permall" ]] || return 1
    local sz
    sz=$(stat -c %s "$permall" 2>/dev/null || echo 0)
    (( sz < 256 )) && return 1

    return 0
}

# get_tpm_backup_size <tpm_backup_dir>
#   Echo human-readable size (du -sh first column); echo 0 + return 1 on miss.
get_tpm_backup_size() {
    local tpm_backup_dir="$1"

    [[ ! -d "$tpm_backup_dir" ]] && echo "0" && return 1

    du -sh "$tpm_backup_dir" 2>/dev/null | awk '{print $1}'
}

# Export so subshells (and the module) inherit the carved helpers.
export -f get_vm_uuid has_tpm_device get_tpm_info log_tpm \
          validate_tpm_backup get_tpm_backup_size
