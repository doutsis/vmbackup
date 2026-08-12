# vmrestore — VM Restore Guide

> **100% vibe coded. Could be 100% wrong.**
>
> Appropriate testing in any and all environments is required. Build your own confidence that the restores work.
>
> Backups are only as good as your restores. All backups are worthless if you cannot recover from them.

[vmbackup](https://github.com/doutsis/vmbackup) and vmrestore are two halves of one system. vmbackup backs up — vmrestore restores. As of 0.6.0 they ship together as a single package containing two binaries: they always carry the same version, draw on one shared `lib/` of cross-tool helpers, and read from a single SQLite catalogue, so the two halves can no longer drift apart by accident. vmrestore still operates standalone for disaster recovery — it restores directly from the backup files even when the catalogue is unavailable.

**vmrestore** is a single-command restore tool for libvirt/KVM virtual machines. It wraps `virtnbdrestore` to provide:

- Disaster recovery and clone restore modes with full identity management
- Point-in-time recovery from any restore point in a backup chain
- Automatic detection of backup type, period, restore points, and chain layout
- TPM state and BitLocker key restoration for Windows VMs
- UEFI/NVRAM restore with clone-mode isolation
- Pre-flight safety checks (disk collision detection, free space verification)
- Archived chain recovery for any rotation policy (daily, weekly, monthly, accumulate)
- Broken/incomplete-chain detection that refuses unsafe restore targets unless explicitly overridden
- Catalogue-aware listing and restore-session history via the shared SQLite catalogue
- Dry-run mode to preview every restore before executing

> **Version:** 0.6.1 (ships in the unified vmbackup package)
> **Underlying tools:** virtnbdrestore v2.28

---

## Table of Contents

1. [Quick Start](#1-quick-start)
2. [Installation](#2-installation)
3. [How vmrestore Fits the vmbackup Ecosystem](#3-how-vmrestore-fits-the-vmbackup-ecosystem)
4. [Understanding Your Backups](#4-understanding-your-backups)
    - [4.1 Rotation Policies](#41-rotation-policies)
    - [4.2 Backup Types and File Naming](#42-backup-types-and-file-naming)
    - [4.3 Chains and Checkpoints](#43-chains-and-checkpoints)
5. [What vmrestore Detects Automatically](#5-what-vmrestore-detects-automatically)
6. [Restore Types — DR, Clone and Disk](#6-restore-types--dr-clone-and-disk)
7. [Choosing a Restore Path](#7-choosing-a-restore-path)
8. [Pre-Restore Checklist](#8-pre-restore-checklist)
9. [Restore Scenarios](#9-restore-scenarios)
    - [9.1 Listing Available Backups](#91-listing-available-backups)
    - [9.2 Listing Restore Points](#92-listing-restore-points)
    - [9.3 Disaster Recovery Restore (DR)](#93-disaster-recovery-restore-dr)
    - [9.4 Point-in-Time Restore (Specific Date or Restore Point)](#94-point-in-time-restore-specific-date-or-restore-point)
    - [9.5 Path-Aware `--vm` (Direct Backup Path)](#95-path-aware---vm-direct-backup-path)
    - [9.6 Restore from an Archived Chain](#96-restore-from-an-archived-chain)
    - [9.7 Clone Restore (`--name`)](#97-clone-restore---name)
    - [9.8 Accumulate Policy Restore](#98-accumulate-policy-restore)
    - [9.9 Overwriting an Existing VM (`--force`)](#99-overwriting-an-existing-vm---force)
    - [9.10 Disk Restore (`--disk`)](#910-disk-restore---disk)
    - [9.11 Disk-Only Restore (No VM Definition)](#911-disk-only-restore-no-vm-definition)
    - [9.12 Dry Run](#912-dry-run)
    - [9.13 Verify and Dump](#913-verify-and-dump)
10. [Restore Walkthroughs by Policy](#10-restore-walkthroughs-by-policy)
    - [10.1 Weekly Rotation Policy](#101-weekly-rotation-policy)
    - [10.2 Daily Rotation Policy](#102-daily-rotation-policy)
    - [10.3 Monthly Rotation Policy](#103-monthly-rotation-policy)
    - [10.4 Accumulate Policy](#104-accumulate-policy)
11. [TPM and BitLocker Restore](#11-tpm-and-bitlocker-restore)
12. [UEFI/OVMF Firmware and NVRAM Restore](#12-uefiovmf-firmware-and-nvram-restore)
13. [Verifying Backups Before Restore](#13-verifying-backups-before-restore)
14. [Post-Restore Steps](#14-post-restore-steps)
15. [Troubleshooting](#15-troubleshooting)
16. [Quick Reference Commands](#16-quick-reference-commands)
17. [Exit Codes](#17-exit-codes)

---

## 1. Quick Start

```bash
# What VMs have backups?
sudo vmrestore --list

# What restore points are available for a VM?
sudo vmrestore --list-restore-points my-vm

# Preview a restore (no changes made)
sudo vmrestore --vm my-vm --restore-path /var/lib/libvirt/images --dry-run

# Disaster recovery — rebuild the VM with original identity
sudo vmrestore --vm my-vm --restore-path /var/lib/libvirt/images

# Clone — create an independent copy with new identity
sudo vmrestore --vm my-vm --name test-clone --restore-path /var/lib/libvirt/images

# Restore a specific point in time
sudo vmrestore --vm my-vm --period 2026-W10 --restore-point 3 \
  --restore-path /var/lib/libvirt/images

# Replace a single disk in-place (VM must be shut off)
sudo vmrestore --vm my-vm --disk vdb

# Replace multiple disks at once
sudo vmrestore --vm my-vm --disk vda,vdb,sda

# Replace all disks
sudo vmrestore --vm my-vm --disk all

# Extract a single disk to a staging directory
sudo vmrestore --vm my-vm --disk vdb --restore-path /tmp/extract
```

`--restore-path` is required for DR and clone restores. For disk restore with `--disk`, it is optional — omitting it performs an in-place replacement of the original disk file.

---
## 2. Installation

vmrestore is packaged as a Debian `.deb` package and can also be deployed manually as a standalone script.

### Prerequisites

| Package | Version | Purpose |
|---------|---------|---------|
| `virtnbdbackup` | ≥ 2.28 | Provides `virtnbdrestore` (disk restore engine) |
| `libvirt-daemon-system` | — | `virsh` domain management |
| `qemu-utils` | — | `qemu-img` for post-restore disk integrity checks |

**Installing virtnbdbackup:**

```bash
# Debian / Ubuntu
sudo apt install virtnbdbackup

# Or from source — see https://github.com/abbbi/virtnbdbackup
```

### Standalone / Disaster-Recovery Use

Although vmrestore now ships in the vmbackup package, it does **not** require a *running* vmbackup, a systemd timer, or even the SQLite catalogue to perform a restore. On a bare recovery host you can copy `vmrestore.sh` (plus `lib/`) alongside a backup tree and restore from it directly. It needs two things:

1. **Access to the backup storage** — the directory tree containing `.data` files, config XMLs, TPM state, and NVRAM files that vmbackup created
2. **The backup path** — either passed explicitly via `--backup-path`, or resolved automatically from vmbackup's config file. By default vmrestore reads `/opt/vmbackup/config/default/vmbackup.conf`; use `--config-instance <name>` or set the `VMBACKUP_INSTANCE` environment variable to select a different instance

Even though it now shares vmbackup's `lib/` and SQLite catalogue, vmrestore remains operationally self-contained for recovery: it reads vmbackup's on-disk output format directly and needs neither a running vmbackup nor the catalogue to restore. When the catalogue is present it records the restore there; when it is absent it logs a single `WARN` and continues.

### From GitHub Release (.deb) — Recommended

vmrestore is installed by the **vmbackup** package — there is no separate vmrestore `.deb`.

```bash
wget https://github.com/doutsis/vmbackup/releases/download/v0.6.1/vmbackup_0.6.1_all.deb
sudo apt install ./vmbackup_0.6.1_all.deb
```

The package declares `Provides: vmrestore` / `Replaces: vmrestore (<< 0.6.0)`, so `apt` removes any old standalone `vmrestore` package automatically on upgrade.

### From Source

```bash
git clone https://github.com/doutsis/vmbackup.git
cd vmbackup
sudo make install                       # or, to build a package:
make package && sudo apt install ./build/vmbackup_0.6.1_all.deb
```

### Uninstall

**Debian / Ubuntu (.deb install):**

```bash
sudo apt remove vmbackup    # remove but keep logs (removes both binaries)
sudo apt purge vmbackup     # remove everything
```

**From source (make install):**

```bash
sudo make uninstall
```

### What the Package Installs (vmrestore side)

- Installs `vmrestore.sh` into `/opt/vmbackup/` — the shared tree alongside `vmbackup.sh` and `lib/`
- Creates a symlink at `/usr/local/bin/vmrestore` → `/opt/vmbackup/vmrestore.sh`
- Sets ownership to `root:backup` with `750` permissions (the `backup` group is the read-access group for the install tree and backup data; `libvirt` membership is for libvirt/virsh access, not install-tree ownership)
- Shares the vmbackup state/log locations (the SQLite catalogue lives at `${BACKUP_PATH}/_state/vmbackup.db`)

### Manual / Recovery-Host Installation

On a bare recovery host without the package, copy the script and helper libraries from the source tree (vmrestore sources `lib/` at runtime):

```bash
# Place the unified tree under /opt/vmbackup
sudo mkdir -p /opt/vmbackup
sudo cp -a vmrestore.sh lib/ /opt/vmbackup/

# Set permissions
sudo chown -R root:backup /opt/vmbackup
sudo chmod 750 /opt/vmbackup /opt/vmbackup/vmrestore.sh

# Create symlink
sudo ln -sf /opt/vmbackup/vmrestore.sh /usr/local/bin/vmrestore
```

### Privilege Model

vmrestore is deliberately split across two privilege levels. **Unlike vmbackup**, it does **not** enforce a root-EUID check at startup — this is intentional, to support disaster-recovery workflows on hosts that may not be configured for `sudo` (e.g. a laptop or recovery workstation with a copied backup tree).

**Operations that work as a regular (non-root) user**, provided the user can read the backup tree:

| Operation | Why it works unprivileged |
|-----------|---------------------------|
| `vmrestore --list` | Read-only filesystem walk of `BACKUP_PATH` + optional read of the vmbackup SQLite catalogue |
| `vmrestore --list-restore-points <vm>` | Read-only chain enumeration |
| `vmrestore --dump <vm>` | Read-only metadata dump (libvirt XML, chain JSON) |
| `vmrestore --vm <vm> --disk <d> --restore-path <user-writable-dir>` | Extracts a single disk to a scratch location the user can write |
| `vmrestore --dry-run …` | Non-destructive — never writes disks or calls `virsh define`. Note: DR/clone dry-run previews still **query** libvirt read-only for collision and existing-domain checks (`virsh domblklist`, domain state/existence), so a dry run is not fully libvirt-free. The truly libvirt-light paths are the read-only `--list` / `--dump` operations. |

**Operations that require root** (libvirt + write access to `/var/lib/libvirt/images/`):

| Operation | Why root is needed |
|-----------|--------------------|
| Full DR restore (`vmrestore --vm <vm> --restore-path /var/lib/libvirt/images`) | `virsh define`, image-path writes |
| Clone restore (`vmrestore --vm <vm> --name <clone>`) | Same as DR |
| Disk restore that overwrites a libvirt-managed disk | Write access to the libvirt image path |

When invoked unprivileged for an operation that needs root, vmrestore (and the underlying `virsh`) emit a clear permission error rather than silently doing the wrong thing.

**For the privileged-operation case**, the same group memberships that make vmbackup convenient also help here:

| Group | Purpose | Command |
|-------|---------|---------|
| `libvirt` | Read access to libvirt (VM listing, status) | `sudo usermod -aG libvirt <username>` |
| `backup` | Read access to backup data under `BACKUP_PATH` | `sudo usermod -aG backup <username>` |

The `sudo` prefix on every example in this document reflects the common installed-on-the-backup-host case where the operator wants both the read-only listings **and** the privileged restore step in one workflow. It is not a hard requirement for read-only and forensic operations.

### Concurrency and Signal Handling

A restore now takes the **same per-VM lock** that a backup of that VM takes — `vmbackup-<vm>.lock` under `${BACKUP_PATH}/_state/locks/` — so a restore and a backup of the same VM can no longer run simultaneously and corrupt each other. A stale lock left by a dead or unrelated process is detected and cleared automatically. `SIGINT` and `SIGTERM` are trapped: on interruption vmrestore removes its staging directories and releases the lock, leaving intact the `.pre-restore` rollback copy of any disk it had begun to overwrite.

### Backup Path Resolution

vmrestore resolves the backup root using a two-step cascade:

| Priority | Source | Example |
|----------|--------|---------|
| 1 | `--backup-path` CLI argument | `--backup-path /mnt/backups/vm` |
| 2 | `--config-instance` CLI flag | `--config-instance prod` reads `/opt/vmbackup/config/prod/vmbackup.conf` |
| 3 | `VMBACKUP_INSTANCE` environment variable | `VMBACKUP_INSTANCE=prod vmrestore --list` |
| 4 | Default config | `BACKUP_PATH=` in `/opt/vmbackup/config/default/vmbackup.conf` |

If none of these provides a value, vmrestore exits with an error directing you to use `--backup-path` or configure vmbackup.

If vmbackup is already installed and configured, vmrestore will automatically pick up its configuration — no additional setup is needed.

### Verify Installation

```bash
# Check dependencies
which virtnbdrestore virsh qemu-img

# List available backups (confirms vmrestore + backup path are working)
sudo vmrestore --list
```

---
## 3. How vmrestore Fits the vmbackup Ecosystem

[vmbackup](https://github.com/doutsis/vmbackup) and vmrestore are two halves of one system. vmbackup backs up — vmrestore restores. As of 0.6.0 they ship as one package with two binaries, sharing a common `lib/` and a single SQLite catalogue, yet each runs independently: vmrestore exclusively restores backups created by vmbackup and remains fully functional for disaster recovery even when the catalogue is gone.

| Component | Role | How It Connects |
|-----------|------|-----------------------|
| **vmbackup.sh** | Backup engine — scheduling, rotation, retention, replication | Orchestrates `virtnbdbackup` to create on-disk backup sets |
| **virtnbdbackup** | Disk backup engine used by vmbackup | Orchestrated by vmbackup |
| **vmrestore** | Restore engine — identity management, TPM/NVRAM, pre-flight checks | Orchestrates `virtnbdrestore` to reconstruct VMs from vmbackup's output |
| **virtnbdrestore** | Disk restore engine used by vmrestore | Orchestrated by vmrestore |

### What vmbackup Does

vmbackup is the **backup engine**. It handles everything before a restore is ever needed:

- Scheduling and running backups (via systemd timer or cron)
- Choosing full vs incremental based on rotation policy
- Managing chain lifecycle — archiving old chains, starting new ones
- Capturing TPM state, NVRAM, VM configuration, and checksums
- Writing to the SQLite tracking database and sending email reports
- Retention and cleanup of expired backup sets
- Cloud replication (SharePoint, etc.)

### What vmrestore Does

vmrestore is the **restore engine**. It reads what vmbackup created and reconstructs the VM:

- Locating and validating backup data on disk
- Auto-detecting backup type, period, restore points, and chain layout
- Orchestrating virtnbdrestore with the correct flags for DR or clone mode
- Restoring TPM state and NVRAM with proper ownership and isolation
- Pre-flight safety checks (disk collision detection, free space verification)
- Defining the restored VM in libvirt and refreshing storage pools

### What vmrestore Does NOT Do

vmrestore has a clear boundary. It does not:

- **Create backups** — that's vmbackup
- **Schedule anything** — no timers, no cron, no recurring jobs
- **Manage retention or cleanup** — it never deletes backup data
- **Modify your backup data** — it reads the backup tree; it never alters or deletes `.data` files, configs, or checkpoints

vmrestore is read-only with respect to your **backup data** — it reads vmbackup's on-disk structure and configuration but never modifies them. It does append one row per restore to the shared SQLite catalogue (see [Restore Tracking](#restore-tracking)); that is an addition to history, not a change to any backup.

### Restore Tracking

vmrestore records every restore in the same SQLite catalogue vmbackup writes (`${BACKUP_PATH}/_state/vmbackup.db`, schema v2.2, `restore_sessions` table). Each run logs the VM, restore mode, source, target, outcome, and exit code, so backup and restore history live in one place:

```bash
sudo vmbackup --status --restores            # recent restores
sudo vmbackup --status --restores --days 30  # last 30 days
```

Tracking never blocks a recovery: if the catalogue is unavailable the restore proceeds after a single `WARN`, and `--dry-run` writes no row.

---
## 4. Understanding Your Backups

Before restoring, it helps to understand what [vmbackup](https://github.com/doutsis/vmbackup) created. This section covers the backup structure, file naming, and chain mechanics that vmrestore works with.

### 4.1 Rotation Policies

vmbackup uses `virtnbdbackup` in **hybrid** mode to create online, thin-provisioned backups via libvirt's changed block tracking (dirty bitmaps). Each VM gets its own directory tree under the backup root.

#### Policies

| Policy     | Period ID Format | Full Backup Trigger       | Incrementals           | Example Path                |
|------------|-----------------|--------------------------|------------------------|-----------------------------|
| Daily      | `YYYYMMDD`      | Every day (new period)    | None (full each day)   | `web-server/20260222/`  |
| Weekly     | `YYYY-Www`      | Start of ISO week         | Rest of the week       | `my-vm/2026-W09/`      |
| Monthly    | `YYYYMM`        | 1st of month or new chain | Rest of the month      | `file-server/202602/`   |
| Accumulate | (none)          | First run only            | All subsequent backups  | `appliance/` (flat, no subdirs) |

#### How vmbackup Decides Backup Type

1. **Day 1 of period** (or empty target dir): `virtnbdbackup -l full` — creates a full baseline and checkpoint `virtnbdbackup.0`
2. **Subsequent days in period**: `virtnbdbackup -l auto` — virtnbdbackup detects the existing full backup and automatically creates an incremental, adding checkpoint `virtnbdbackup.N`
3. **Chain break** (backup chain corruption, policy change): Archives old chain to `.archives/`, starts fresh full backup

#### Key Concept: Everything Needed for Restore Lives in One Directory

Each period directory (e.g., `202602/` or `20260222/`) — or the VM root for accumulate policy — is a self-contained backup set containing:
- Full backup data file(s): `*.full.data` or `*.copy.data`
- Incremental data files: `*.inc.virtnbdbackup.N.data`
- Checkpoint XMLs: `checkpoints/virtnbdbackup.N.xml`
- VM configuration: `vmconfig.virtnbdbackup.N.xml`
- UEFI firmware (if applicable): `OVMF_CODE_4M.ms.fd.virtnbdbackup.N`, `*_VARS.fd.virtnbdbackup.N`
- TPM state (if applicable): `tpm-state/tpm2/`
- BitLocker recovery keys (if applicable): `tpm-state/bitlocker-recovery-keys.txt`
- Checksum files: `*.data.chksum`

---

### 4.2 Backup Types and File Naming

| Type | File Pattern | Created When |
|------|-------------|-------------|
| **Full** | `{device}.full.data` | First backup of a period, or chain start |
| **Incremental** | `{device}.inc.virtnbdbackup.{N}.data` | Subsequent backups within a period |
| **Copy** | `{device}.copy.data` | Offline/cold backup (VM was shut off) |

Each `.data` file has a corresponding `.data.chksum` file containing an adler32 checksum (integer).

---

### 4.3 Chains and Checkpoints

#### What is a Chain?

A chain is the complete set of backup files needed to reconstruct a VM's disk to a given point in time. It starts with one full backup and may include zero or more incrementals:

```
Chain start:  vda.full.data                   ← Checkpoint 0 (cp0) — FULL baseline
              vda.inc.virtnbdbackup.1.data    ← Checkpoint 1 (cp1) — changes since cp0
              vda.inc.virtnbdbackup.2.data    ← Checkpoint 2 (cp2) — changes since cp1
```

**Restoring to cp2** requires: full + inc.1 + inc.2 (all applied in sequence)
**Restoring to cp0** requires: full only

#### When Does a Chain Break?

vmbackup archives the current chain and starts fresh when:
- Backup chain corruption is detected
- Backup policy changes (e.g., monthly → daily)
- Manual chain break

Archived chains are moved to `.archives/chain-YYYY-MM-DD[.N]/` within the period directory. They remain fully restorable via vmrestore.

---

## 5. What vmrestore Detects Automatically

vmrestore auto-detects nearly everything it needs from the backup structure. The only required arguments for a basic restore are `--vm` and `--restore-path`. Everything else is resolved automatically.

| What | How | Override |
|------|-----|----------|
| **Backup path** | Reads `BACKUP_PATH` from vmbackup's config (default: `/opt/vmbackup/config/default/vmbackup.conf`) | `--backup-path`, `--config-instance` |
| **VM location** | `--vm my-vm` looks for `{BACKUP_PATH}/my-vm/`. Alternatively, `--vm /full/path/to/backups/my-vm` uses the path directly. | — |
| **Backup type** | Scans for `*.inc.*.data` (incremental), `*.full.data` (full), or `*.copy.data` (copy) | — |
| **Accumulate vs periodic** | If data files exist at the VM root (no period subdirectories), accumulate layout is assumed | — |
| **Period** | Picks the newest period subdirectory automatically | `--period` |
| **Restore point** | Defaults to `latest` (all incrementals applied). `full` restores the base only. A number `N` restores up to restore point N. | `--restore-point` |
| **Chain completeness** | Verifies the chain is complete before restoring; incomplete or broken chains are refused as the default `latest` target | `--include-incomplete` (forensic) |
| **TPM state** | Detects `.tpm-backup-marker` and `tpm-state/tpm2/` directory | `--skip-tpm` |
| **NVRAM** | Resolves the chain-endpoint per-checkpoint NVRAM (`*_VARS*.fd.virtnbdbackup.<N>`) and pairs it with the restored disks. Clone mode copies it to a new filename; in-place restores back up the live NVRAM first. | — |
| **VM config XML** | Searches `vmconfig.virtnbdbackup.*.xml`, then `config/*.xml`, then parent directory | — |
| **Storage pool** | After restore, detects the containing libvirt storage pool and runs `virsh pool-refresh` | — |

---
## 6. Restore Types — DR, Clone and Disk

vmrestore has three restore types. Each one is designed for a different situation — use whichever fits what went wrong and what you need back.

| Type | What it does | Flag | Use when… |
|------|-------------|------|-----------|
| **DR** | Rebuilds the VM exactly as it was — same name, UUID, MACs | *(default)* | The VM is gone or broken. You want a direct replacement. |
| **Clone** | Creates a new independent copy with fresh identity | `--name` | You need a test/dev copy, or want to run a restored VM alongside the original. |
| **Disk** | Replaces one or more disk files. Nothing else is touched. | `--disk` | One disk went bad but the VM definition, other disks, and TPM are fine. |

**How they differ at a glance:**

| Behaviour | DR | Clone | Disk |
|-----------|-----|-------|------|
| VM definition | Re-defined with original identity | Defined with new name, UUID, MACs | Untouched |
| Disk filenames | Original | `{name}.qcow2` or `{name}-{dev}.qcow2` | Original (in-place replacement) |
| NVRAM | Restored to original path | Copied to `{name}_VARS.fd` | Untouched |
| TPM state | Restored at original UUID | Restored at new UUID | Untouched |
| BitLocker | Unlocks automatically | Unlocks automatically | Unlocks automatically |
| VM must be shut off? | Yes | No (uses staging) | Yes (unless `--restore-path` extracts to staging) |
| `--force` needed if VM defined? | Yes | No | No |
| Can coexist with original? | No | Yes | N/A (same VM) |

### Disaster Recovery (DR) Restore

A DR restore rebuilds a VM with its **original identity** — same name, same UUID, same MAC addresses. The restored VM is a direct replacement for the original. To the rest of your infrastructure, it's as if nothing happened.

```bash
sudo vmrestore --vm my-vm --restore-path /var/lib/libvirt/images
```

**What vmrestore does:**
- Restores disk images to `--restore-path` with their original filenames
- Defines the VM in libvirt with its original name and MAC addresses. Because `virtnbdrestore -D` always assigns a *fresh* UUID, vmrestore then re-injects the **original UUID** from the backup config (a quick undefine/redefine), so the VM's identity — and the TPM/BitLocker binding tied to that UUID — is preserved
- Restores the chain-endpoint NVRAM to its original path, saving any existing NVRAM as `.before-restore.<timestamp>` first
- Restores TPM state to the original UUID path (BitLocker unlocks automatically); any pre-existing TPM state is moved aside to `.pre-restore-<timestamp>`
- Before reporting success, verifies the result: every expected disk image must exist and pass an integrity check — a missing or failing disk stops the restore instead of counting as a success. (The expected disk set is derived from the backup's configuration; in the rare DR case where that configuration is unusable, vmrestore warns in the log that its disk safety checks are being skipped and proceeds unverified)

#### When `--force` Is Required

If the VM name is **still defined in libvirt**, vmrestore will refuse to proceed — it won't silently overwrite a defined VM. You must add `--force`:

```bash
sudo vmrestore --vm my-vm --restore-path /var/lib/libvirt/images --force
```

`--force` does two things:
1. **Undefines the existing VM** from libvirt (equivalent to `virsh undefine` with appropriate flags)
2. **Removes existing disk files** at the restore path before writing the restored disks

If the VM is **not defined** in libvirt (e.g. you're restoring onto a fresh host, or the VM was already undefined), `--force` is not needed. vmrestore will define the VM and write the disk files without complaint.

**The decision is simple:**

| VM still defined in libvirt? | Command |
|------------------------------|---------|
| No (fresh host, already undefined) | `sudo vmrestore --vm my-vm --restore-path /path` |
| Yes (replacing in-place) | `sudo vmrestore --vm my-vm --restore-path /path --force` |

> **Important:** The VM must be **shut off** before a DR restore. vmrestore checks whether the predicted output files are in use by a running VM and blocks the restore if they are. Shut down the VM first, then restore with `--force`.

### Clone Restore

A clone restore creates a **new, independent copy** of a VM with a fresh identity. The original VM (if it exists) is completely untouched. Both can run side by side without conflicting.

```bash
sudo vmrestore --vm my-vm --name test-clone --restore-path /var/lib/libvirt/images
```

**What vmrestore does:**
- Restores disk images into a staging directory, then renames them with the clone name:
  - Single disk: `test-clone.qcow2`
  - Multi-disk: `test-clone-vda.qcow2`, `test-clone-vdb.qcow2`, etc.
- Defines the VM in libvirt with a **new name, new UUID, and new MAC addresses** — libvirt assigns the UUID and MACs automatically when the modified XML is defined
- Copies NVRAM to a new file (`test-clone_VARS.fd`) so the clone and original don't share firmware state
- Restores TPM state under the new UUID path (BitLocker unlocks automatically because the TPM is re-mapped)
- Before reporting success, verifies the result: every expected disk image must exist and pass an integrity check — a missing or failing disk stops the restore instead of counting as a success. (Clones get this unconditionally: a clone whose expected disk set cannot be derived from the backup is refused before anything is written)

**No `--force` needed.** Clone mode uses a staging directory during the restore, so it never touches existing files. The final renamed files are checked against live VM disks before being placed.

The clone is fully independent. You can start it, modify it, or delete it without affecting the original VM.

### Disk Restore

A disk restore replaces one or more disk files **without touching anything else** — no VM redefinition, no UUID changes, no TPM or NVRAM restoration. The VM keeps its identity, its configuration, and any disks you don't specify.

```bash
# Replace one disk (VM must be shut off)
sudo vmrestore --vm my-vm --disk vdb

# Replace multiple disks
sudo vmrestore --vm my-vm --disk vda,vdb,sda

# Replace all disks
sudo vmrestore --vm my-vm --disk all

# Extract to staging instead of replacing in-place (VM can be running)
sudo vmrestore --vm my-vm --disk vdb --restore-path /tmp/restore
```

**What vmrestore does:**
- Resolves each disk's current file path from the live VM definition in libvirt
- Creates a `.pre-restore` backup of each existing disk file (safety net)
- Runs `virtnbdrestore -d {disk}` for each specified disk
- Verifies integrity with `qemu-img check` and sets ownership/permissions

**`.pre-restore` safety:** Before overwriting a disk, vmrestore renames the existing file to `{disk}.pre-restore`. If a `.pre-restore` file already exists from a previous restore that wasn't cleaned up, vmrestore refuses to proceed — preventing accidental loss of your rollback copy. Use `--no-pre-restore` to skip the safety backup when disk space is tight.

See [section 9.10](#910-disk-restore---disk) for full details including point-in-time disk restore and checkpoint chain handling.

### Multi-Disk VMs

vmrestore handles multi-disk VMs automatically in all three restore types. If a backup contains data files for more than one disk (e.g. `vda.full.data` and `vdb.full.data`), DR and clone restores process all of them in a single run. Disk restore (`--disk`) works for both single-disk and multi-disk VMs — specify individual disks or use `--disk all`.

#### How Disk Naming Works

[vmbackup](https://github.com/doutsis/vmbackup) names backup data files using the **libvirt device target** — the bus address assigned by the hypervisor. A VM with a VirtIO system disk and a SATA data disk produces files named `vda.full.data` and `sda.full.data`. vmrestore reads these names and uses them to construct the final output filenames.

The naming convention for restored disk images depends on the restore type:

| Type | Single-disk VM | Multi-disk VM |
|------|---------------|---------------|
| **DR** | Original filename (e.g. `my-vm.qcow2`) | Original filenames preserved |
| **Clone** | `{name}.qcow2` | `{name}-{device}.qcow2` per disk |
| **Disk** | In-place replacement at original path | Per-disk replacement at original path |

Where `{name}` is the value passed to `--name`, and `{device}` is the libvirt device target (`vda`, `vdb`, `sda`, etc.).

**Example — clone a two-disk VM:**

The backup for `file-server` contains a VirtIO system disk (`vda`) and a VirtIO data disk (`vdb`):

```bash
sudo vmrestore --vm file-server --name test-clone \
  --restore-path /var/lib/libvirt/images
```

vmrestore produces:

```
/var/lib/libvirt/images/test-clone-vda.qcow2    # System disk
/var/lib/libvirt/images/test-clone-vdb.qcow2    # Data disk
```

The VM definition is updated so each `<source file="...">` in the XML points to the renamed disk.

**Example — clone a VM with mixed bus types (VirtIO + SATA):**

A Windows VM with a VirtIO system disk (`vda`), a VirtIO secondary disk (`vdb`), and a SATA disk (`sda`):

```bash
sudo vmrestore --vm web-server --name test-clone \
  --restore-path /var/lib/libvirt/images
```

vmrestore produces:

```
/var/lib/libvirt/images/test-clone-vda.qcow2    # VirtIO system disk
/var/lib/libvirt/images/test-clone-vdb.qcow2    # VirtIO secondary disk
/var/lib/libvirt/images/test-clone-sda.qcow2    # SATA disk
```

**Example — DR restore of a multi-disk VM:**

DR mode preserves the original filenames — vmrestore writes the same filenames that virtnbdrestore outputs:

```bash
sudo vmrestore --vm file-server \
  --restore-path /var/lib/libvirt/images --force
```

The original filenames are restored. No renaming occurs.

---
## 7. Choosing a Restore Path

`--restore-path` tells vmrestore **which directory to write the restored disk files into**. It is the parent directory — not the disk file itself. For DR and clone restores it is required. For disk restore with `--disk` it is optional (omit it for in-place replacement).

> **⚠ Restore targets inside a backup root are blocked.** vmrestore refuses any `--restore-path` that resolves inside a configured `BACKUP_PATH` — this is enforced at argument-parse time, before the restore begins. Pick a destination outside your backup storage (e.g. a libvirt pool directory), not a path within the backup tree itself.

Getting this right matters: vmrestore updates the VM definition so disk `<source file="...">` entries point at the files it just wrote. If you restore to the wrong directory the VM will either fail to start or be looking at stale disks.

### Where Do Your VMs Live?

Every KVM/libvirt host stores VM disk images somewhere on the filesystem. If you already know the path, use it. If not:

```bash
# Show where an existing VM's disks are stored
sudo virsh dumpxml my-vm | grep 'source file'
#   <source file='/var/lib/libvirt/images/my-vm.qcow2'/>
#   <source file='/var/lib/libvirt/images/my-vm-data.qcow2'/>
```

The directory portion of that path is your restore path. In this case: `/var/lib/libvirt/images`.

If the VM no longer exists (you're restoring onto a fresh host), check what storage you have available and pick the directory where you want the disks to live.

### Storage Pools

libvirt organises disk storage into **storage pools**. A pool is just a directory (or LVM volume group, NFS share, etc.) that libvirt knows about. The default pool on most installations is:

```
/var/lib/libvirt/images    (pool name: "default")
```

Homelabbers commonly have custom pools on separate mounts — a dedicated SSD, a ZFS dataset, an NFS export:

```bash
# List your storage pools
sudo virsh pool-list
#  Name      State    Autostart
# -----------------------------------
#  default   active   yes
#  fast-ssd  active   yes
#  nas       active   yes

# Show the filesystem path for a pool
sudo virsh pool-dumpxml fast-ssd | grep -oP '<path>\K[^<]+'
#  /mnt/ssd/vms
```

**Why this matters for vmrestore:** After a restore, vmrestore auto-detects which storage pool contains your `--restore-path` (longest-prefix match) and runs `virsh pool-refresh` so the new disk volumes appear immediately in virt-manager, Cockpit, or any other management UI. If you restore to a directory that is not inside any pool, the restore still works — but the volume won't show up in your management UI until you add a pool or refresh manually.

### Which Path for Which Restore Type?

| Restore type | Recommended `--restore-path` | Why |
|-------------|------------------------------|-----|
| **DR** | Same directory the VM's disks originally lived in | The restored XML references the original filenames — putting them back where they came from means everything lines up. |
| **Clone** | Same pool (most common), or a different pool for isolation | A different pool is useful if you want the clone on faster/cheaper storage, or don't want it competing for I/O. |
| **Disk (`--disk`)** | Omit for in-place replacement. Pass a path only for staging extraction. | Without `--restore-path`, vmrestore resolves each disk's current path from the live VM definition and writes directly there. |

### Common Layouts

**Single pool (default)** — all VMs in one directory:
```bash
sudo vmrestore --vm my-vm --restore-path /var/lib/libvirt/images
```

**Dedicated mount** — VMs on a separate filesystem:
```bash
sudo vmrestore --vm my-vm --restore-path /mnt/ssd/vms
```

**Per-VM directories** — some setups put each VM in its own subdirectory:
```bash
sudo vmrestore --vm my-vm --restore-path /mnt/VMs/my-vm
```

> **Tip:** If you're unsure, check where the VM's disks lived before the failure. Use `virsh dumpxml` on a working VM, or look at the backup's XML config (`vmconfig.virtnbdbackup.*.xml`) inside the backup directory — the `<source file="...">` paths show you exactly where the disks were.

---
## 8. Pre-Restore Checklist

Before running a restore, verify:

| Check | Command |
|-------|---------|
| Backup path is accessible | `ls {BACKUP_PATH}/{vm-name}/` |
| Backup has data files | `sudo vmrestore --list-restore-points {vm-name}` |
| Backup checksums are valid | `sudo vmrestore --verify {vm-name}` |
| Target disk has enough space | `df -h {restore-path}` (vmrestore checks this automatically) |
| VM is shut off (DR mode) | `sudo virsh domstate {vm-name}` |
| virtnbdrestore is available | `which virtnbdrestore` |

---
## 9. Restore Scenarios

### 9.1 Listing Available Backups

```bash
sudo vmrestore --list
```

Shows all VMs in the backup root with their backup type, total size, restore point count (summed across all periods), archived chain count, and tags for TPM and multi-disk VMs. When the shared SQLite catalogue is available, each VM also gets a catalogue-derived health line — `Chains: <N> active, <N> broken, last backup <ISO>` — so you can spot broken chains before attempting a restore. If the catalogue is missing or unreadable, `--list` falls back to the filesystem walk alone and simply omits that line.

### 9.2 Listing Restore Points

```bash
# All periods (auto-detected)
sudo vmrestore --list-restore-points my-vm

# Specific period only
sudo vmrestore --list-restore-points my-vm --period 2026-W09
```

Shows every retention period with its numbered restore points, dates, types (FULL, Incremental, COPY), per-checkpoint disk set, and any archived chains (with their restore points expanded inline). No log files are created for read-only commands.

> **Incomplete chains:** a chain with missing per-disk data files or `qcow.json` sidecars (for example, one truncated by an interrupted backup) is flagged `⚠ INCOMPLETE` and is **refused** as the default `latest` restore target. Restore an earlier good restore point instead, or — for forensic recovery only — override with `--include-incomplete`. CD-ROM/floppy devices are correctly ignored when judging completeness, so VMs with optical drives are not false-flagged.

### 9.3 Disaster Recovery Restore (DR)

Restore a VM to its latest state, preserving its original identity:

```bash
sudo vmrestore --vm my-vm --restore-path /var/lib/libvirt/images
```

If the VM is still defined in libvirt (e.g., disk is corrupted but definition exists):

```bash
sudo vmrestore --vm my-vm --restore-path /var/lib/libvirt/images --force
```

The `--force` flag undefines the existing VM before restoring. The VM must be **shut off** first.

### 9.4 Point-in-Time Restore (Specific Date or Restore Point)

Restore to a **specific period** (date):

```bash
# Daily policy — restore from March 2 backup
sudo vmrestore --vm my-vm --period 20260302 \
  --restore-path /var/lib/libvirt/images

# Weekly policy — restore from week 9
sudo vmrestore --vm my-vm --period 2026-W09 \
  --restore-path /var/lib/libvirt/images
```

Restore to a **specific restore point** within a period:

```bash
# Restore point 3 (full + incrementals 1, 2, 3)
sudo vmrestore --vm my-vm --restore-point 3 \
  --restore-path /var/lib/libvirt/images

# Full baseline only (restore point 0)
sudo vmrestore --vm my-vm --restore-point full \
  --restore-path /var/lib/libvirt/images
```

#### Disk Configuration Changes Between Checkpoints

When a disk is removed from a VM mid-chain, the backup chain remains valid — earlier checkpoints still contain all the data for that disk. vmrestore detects when the target restore point has a different disk set from the latest checkpoint and automatically handles the restore:

- A staging directory is created with symlinks to the backup data files and the correct `vmconfig` for the target checkpoint
- `virtnbdrestore` sees only the target checkpoint's disk configuration, so all disks present at that point are restored
- The user sees a warning explaining the disk configuration change:

```
WARNING: Disk configuration changed between checkpoint 2 and the latest checkpoint (3).
  Checkpoint 2: sda, vda, vdb (3 disks)
  Latest (CP 3): vda, vdb (2 disks)
  Restoring with checkpoint 2 disk configuration.
```

This applies to DR, clone, and disk restore modes equally, and works with both active and archived chains. In dry-run mode, the staging intent is logged but no staging directory is created.

**Note:** Disk additions cannot occur mid-chain — they trigger a chain break and a new full backup. Mid-chain disk configuration changes are always removals.

### 9.5 Path-Aware `--vm` (Direct Backup Path)

When `--vm` contains a `/`, vmrestore treats it as a path: `basename` becomes the VM name and `dirname` overrides the backup path. This is useful when the backup path differs from the configured default.

```bash
# Backup path derived from the --vm argument
sudo vmrestore --vm /mnt/backups/vm/my-vm \
  --period 20260302 --restore-path /tmp/restore
```

### 9.6 Restore from an Archived Chain

Archived chains can be accessed by passing the full path to the `.archives/chain-*` directory:

```bash
sudo vmrestore \
  --vm /mnt/backups/vm/my-vm/2026-W09/.archives/chain-2026-02-28.1 \
  --restore-path /tmp/restore/archived
```

vmrestore detects data files directly in the provided path and uses it as the data directory without period resolution.

### 9.7 Clone Restore (`--name`)

Create a new, independent copy with a fresh identity:

```bash
sudo vmrestore --vm my-vm \
  --name test-clone --restore-path /var/lib/libvirt/images
```

The clone gets a new UUID, new MAC addresses, independent NVRAM, and independent TPM state. See [section 6 Restore Types](#6-restore-types--dr-clone-and-disk) for full details.

### 9.8 Accumulate Policy Restore

Accumulate VMs store data directly at the VM root (no period subdirectory). vmrestore auto-detects this — no `--period` needed:

```bash
sudo vmrestore --vm my-vm --restore-path /var/lib/libvirt/images
```

Point-in-time restore still works with `--restore-point`:

```bash
sudo vmrestore --vm my-vm --restore-point 3 \
  --restore-path /var/lib/libvirt/images
```

### 9.9 Overwriting an Existing VM (`--force`)

If a VM with the target name already exists in libvirt:

```bash
sudo vmrestore --vm my-vm --restore-path /var/lib/libvirt/images --force
```

`--force` will:
1. `virsh undefine` the existing VM (tries `--nvram --checkpoints-metadata`, falls back gracefully)
2. Remove existing disk files at the restore path (DR mode only)
3. Proceed with the restore

Without `--force`, vmrestore aborts if the target VM is already defined.

### 9.10 Disk Restore (`--disk`)

Disk restore is the third restore mode alongside DR and Clone. It replaces or extracts one or more disks from a backup without touching VM definitions, UUIDs, MAC addresses, TPM state, or NVRAM. Works for both single-disk and multi-disk VMs. Use it when disk(s) are corrupted but the VM definition is fine.

Supports single (`--disk vdb`), multiple (`--disk vda,vdb`), and all (`--disk all`) disks.

**Key differences from DR/Clone:**

| | DR | Clone | Disk Restore |
|---|---|---|---|
| VM definition | Restored | New (cloned) | **Untouched** |
| UUID/MAC | Original | Regenerated | **Untouched** |
| TPM/NVRAM | Restored | Cloned | **Untouched** |
| `--name` | Not used | Required | **Rejected** |
| `--restore-path` | Required | Required | Optional |
| VM must be shut off | No | No | In-place only |

#### Discovering Available Disks

Use `--list-restore-points` to see which disks are available at each checkpoint:

```bash
sudo vmrestore --list-restore-points my-vm
```

Each restore point row includes a `Disk(s)` column showing which disks were backed up at that checkpoint. When disks are added or removed mid-chain, the column reflects the actual disk set at each point in time — so you can see exactly which disks are available before choosing a `--restore-point` or `--disk` target.

#### Mode 1: In-Place Replacement

Omit `--restore-path` to replace disk(s) at their original location. The VM **must be shut off**.

```bash
# Single disk
sudo vmrestore --vm my-vm --disk vdb

# Multiple disks
sudo vmrestore --vm my-vm --disk vda,vdb

# All disks
sudo vmrestore --vm my-vm --disk all
```

What happens (for each disk):
1. Resolves the original disk path from the live VM XML (e.g. `/mnt/VMs/my-vm-data.qcow2`)
2. Checks that no `.pre-restore` file already exists (refuses if it does — see below)
3. Pre-flight space check (aggregate: restore data + `.pre-restore` copies for all disks)
4. Renames the existing disk to `{filename}.pre-restore`
5. Runs `virtnbdrestore -d {disk}` to restore just that disk
6. Sets ownership (`libvirt-qemu:libvirt-qemu`) and permissions
7. Runs `qemu-img check` for integrity verification

After all disks are restored:
8. Refreshes the libvirt storage pool
9. Warns about checkpoint chain invalidation (once)
10. Lists all `.pre-restore` files with sizes for cleanup (once)

If a restore fails, that disk's `.pre-restore` file is automatically restored. Already-restored disks are kept (they succeeded). Remaining disks are skipped.

#### Mode 2: Staging Extract

Pass `--restore-path` to extract the disk to a separate directory. The VM can be running.

```bash
sudo vmrestore --vm my-vm --disk vdb --restore-path /tmp/extract
```

The extracted qcow2 file is integrity-checked. You can then manually swap it in when ready.

#### `.pre-restore` Management

In-place mode creates a `.pre-restore` backup of each existing disk before replacing it. After confirming the VM works correctly, delete them to reclaim space:

```bash
# vmrestore prints the exact commands, e.g.:
rm /mnt/VMs/my-vm-data.qcow2.pre-restore
rm /mnt/VMs/my-vm-os.qcow2.pre-restore
```

vmrestore reports all `.pre-restore` file paths and sizes in a summary at the end.

**Overwrite protection:** If a `.pre-restore` file already exists (from a previous restore that wasn't cleaned up), vmrestore **refuses to proceed** rather than silently overwriting it. This prevents data loss. To resolve:

```bash
# If the previous restore was successful, delete the old .pre-restore:
rm /mnt/VMs/my-vm-data.qcow2.pre-restore
# Then run vmrestore again.

# Or skip .pre-restore entirely (no rollback safety net):
sudo vmrestore --vm my-vm --disk vdb --no-pre-restore
```

To skip `.pre-restore` backups for all disks (e.g. disks are corrupted and not worth keeping):

```bash
sudo vmrestore --vm my-vm --disk all --no-pre-restore
```

#### Checkpoint Chain Invalidation

Replacing a disk invalidates the QEMU checkpoint bitmaps that [vmbackup](https://github.com/doutsis/vmbackup) uses for incremental backups. The next backup will detect the mismatch.

By default, vmbackup's `ENABLE_AUTO_RECOVERY_ON_CHECKPOINT_CORRUPTION` is set to `yes` — it automatically archives the old chain and starts a fresh FULL backup. No manual intervention is needed.

If auto-recovery has been disabled (`warn`), vmbackup will fail until you manually clean the stale checkpoints:

```bash
for cp in $(virsh checkpoint-list my-vm --name 2>/dev/null); do
  virsh checkpoint-delete my-vm $cp --metadata
done
```

#### Point-in-Time Disk Restore

`--restore-point` works with `--disk` for incremental backups:

```bash
sudo vmrestore --vm my-vm --disk vdb --restore-point 2
```

Restores the disk to the state at restore point 2, not the latest.

When the requested disk exists at the target restore point but not at the latest checkpoint (i.e. the disk was removed mid-chain), vmrestore validates the disk against the target checkpoint's disk set and creates a staging directory with the correct `vmconfig` so `virtnbdrestore` can find the disk. If `--restore-point` is omitted and the requested disk is not at the latest checkpoint, vmrestore errors with a message showing which checkpoint the disk was last seen at:

```
ERROR: Disk 'sda' is not available at the latest restore point (checkpoint 3).
       It was last backed up at checkpoint 2.
       Use --restore-point 2 to restore sda from that checkpoint.
```

#### Multi-Disk Failure Handling

When restoring multiple disks and one fails:

1. The **failing disk** is auto-rolled back from its `.pre-restore` file (if it exists)
2. **Already-restored disks** are kept in place — they completed successfully
3. **Remaining disks** are skipped
4. A clear summary shows which disks succeeded, which failed, and which were skipped

This means a partial restore is possible. The VM may or may not boot depending on which disk failed. The admin can fix the issue and re-run `--disk` for just the failed/skipped disk(s).

#### Restrictions

- `--disk` cannot be combined with `--name` (disk restore replaces files, it does not create a VM)
- In-place mode requires the VM to be shut off
- In-place mode requires the VM to be defined in libvirt (to resolve disk paths). Use `--restore-path` if the VM is not defined.
- All disk names must match devices in the backup (use `--list-restore-points` to check)
- If a `.pre-restore` file already exists, vmrestore refuses to proceed (delete it first or use `--no-pre-restore`)

### 9.11 Disk-Only Restore (No VM Definition)

Restore disk images without defining a VM in libvirt:

```bash
sudo vmrestore --vm my-vm --restore-path /tmp/restore --skip-config
```

Also skips TPM restoration (`--skip-config` implies data-only — TPM state should not be modified for a VM that isn't being redefined).

### 9.12 Dry Run

Preview what vmrestore would do without executing:

```bash
sudo vmrestore --vm my-vm --restore-path /tmp/restore --dry-run
```

Shows the virtnbdrestore command that would be run, safety check results, predicted output files, and (for clone mode) the staging and rename plan.

### 9.13 Verify and Dump

Validate backup integrity (checksum verification):

```bash
sudo vmrestore --verify my-vm --period 2026-W09
```

View backup metadata as JSON:

```bash
sudo vmrestore --dump my-vm --period 2026-W09
```

Both commands pass through to `virtnbdrestore -o verify` and `virtnbdrestore -o dump` respectively.

---
## 10. Restore Walkthroughs by Policy

These walkthroughs show what [vmbackup](https://github.com/doutsis/vmbackup) produces on disk for each rotation policy and how to list, understand, and restore from those backups. Section 9 gives quick command recipes; this section provides the full context — directory layouts, restore point numbering, and day-by-day examples.

### 10.1 Weekly Rotation Policy

**Setup**: VM `my-vm`, weekly rotation policy, vmbackup runs once daily. Three weeks of backups: `2026-W10/`, `2026-W11/`, `2026-W12/`.

#### What Happens Each Day

The same pattern repeats every week. vmbackup makes one decision per day based on the VM's state when it runs:

| Day | VM State | vmbackup Action | Files Created |
|-----------|----------|---------------------------------------------|------------------------------|
| Monday | Online | New week → new period directory → **FULL** | `vda.full.data`, restore point 0 |
| Tuesday | Online | Chain continues → **incremental** | `vda.inc.virtnbdbackup.1.data`, restore point 1 |
| Wednesday | Offline | Disk changed (clean shutdown). Archive Mon/Tue chain → **COPY** | `vda.copy.data` |
| Thursday | Offline | VM was started, modified, shut down. Archive Wed copy → **COPY** | `vda.copy.data` |
| Friday | Online | Copy from Thu exists. Archive Thu copy → **FULL** (new chain) | `vda.full.data`, restore point 0 |
| Saturday | Online | Chain continues → **incremental** | `vda.inc.virtnbdbackup.1.data`, restore point 1 |
| Sunday | Online | Chain continues → **incremental** | `vda.inc.virtnbdbackup.2.data`, restore point 2 |

#### Resulting Directory Structure (End of Week)

```
my-vm/2026-W10/
├── .archives/
│   ├── chain-2026-03-04/               ← Mon/Tue chain (archived Wednesday)
│   │   ├── vda.full.data               ← Monday's full backup
│   │   ├── vda.inc.virtnbdbackup.1.data ← Tuesday's incremental
│   │   ├── checkpoints/
│   │   │   ├── virtnbdbackup.0.xml     ← Monday
│   │   │   └── virtnbdbackup.1.xml     ← Tuesday
│   │   └── my-vm.cpt
│   ├── chain-2026-03-05/               ← Wed copy (archived Thursday)
│   │   └── vda.copy.data               ← Wednesday's copy backup
│   └── chain-2026-03-06/               ← Thu copy (archived Friday)
│       └── vda.copy.data               ← Thursday's copy backup
│
├── vda.full.data                       ← Friday's full (current chain base)
├── vda.inc.virtnbdbackup.1.data        ← Saturday's incremental
├── vda.inc.virtnbdbackup.2.data        ← Sunday's incremental
├── checkpoints/
│   ├── virtnbdbackup.0.xml             ← Friday
│   ├── virtnbdbackup.1.xml             ← Saturday
│   └── virtnbdbackup.2.xml             ← Sunday
└── my-vm.cpt
```

Each of the three weeks (`2026-W10`, `2026-W11`, `2026-W12`) has the same structure.

#### Listing Restore Points

```bash
# Show current chain restore points for the latest week
sudo vmrestore --list-restore-points my-vm

# Show restore points for a specific week
sudo vmrestore --list-restore-points my-vm --period 2026-W10
```

Example output for `--period 2026-W10`:

```
Restore Points: my-vm

  ── 2026-W10 ──
  Directory: /mnt/vm-backups/my-vm/2026-W10
  Type: incremental

  Restore Point   Date                 Type            Disk(s)
  ──────────────────────────────────────────────────────────────────────
  0               2026-03-06 22:00:01  FULL (base)     vda
  1               2026-03-07 22:00:01  Incremental     vda
  2               2026-03-08 22:00:01  Incremental     vda
  ──────────────────────────────────────────────────────────────────────
  Total: 3

  Archived Chains:
    chain-2026-03-04                 1.2G  incremental  [vda]
  Restore Point   Date                 Type            Disk(s)
  ──────────────────────────────────────────────────────────────────────
  0               2026-03-02 22:00:01  FULL (base)     vda          ← Monday
  1               2026-03-03 22:00:01  Incremental     vda          ← Tuesday
  ──────────────────────────────────────────────────────────────────────
  Total: 2

    chain-2026-03-05                  800M  copy  [vda]
  Restore Point   Date                 Type            Disk(s)
  ──────────────────────────────────────────────────────────────────────
  0               2026-03-04 22:00:01  COPY (offline)  vda          ← Wednesday
  ──────────────────────────────────────────────────────────────────────
  Total: 1

    chain-2026-03-06                  810M  copy  [vda]
  Restore Point   Date                 Type            Disk(s)
  ──────────────────────────────────────────────────────────────────────
  0               2026-03-05 22:00:01  COPY (offline)  vda          ← Thursday
  ──────────────────────────────────────────────────────────────────────
  Total: 1
```

**Reading this output:**
- The 3 current restore points are from the Fri/Sat/Sun chain.
- Restore point 0 = Friday (FULL base), 1 = Saturday, 2 = Sunday.
- Three archived chains are also available for earlier days of the week.

#### Restore Examples

##### Restore to Sunday (latest — the default)

```bash
sudo vmrestore --vm my-vm --period 2026-W10 \
  --restore-path /var/lib/libvirt/images
```

No `--restore-point` needed — `latest` is the default and applies all restore points (0 + 1 + 2 = Sunday's state).

##### Restore to Saturday (Week 1, Day 6)

Saturday is restore point 1 in the current Fri/Sat/Sun chain:

```bash
sudo vmrestore --vm my-vm --period 2026-W10 \
  --restore-point 1 --restore-path /var/lib/libvirt/images
```

##### Restore to Tuesday (Week 1, Day 2)

Tuesday's data is in the Mon/Tue archived chain (`chain-2026-03-04`). With the new output, you'll already see this chain's restore points expanded under `--list-restore-points`. Alternatively, list just the archive directly:

```bash
sudo vmrestore --list-restore-points \
  /mnt/vm-backups/my-vm/2026-W10/.archives/chain-2026-03-04
```

```
Restore Points: chain-2026-03-04

  ── (archive) ──
  Directory: /mnt/vm-backups/my-vm/2026-W10/.archives/chain-2026-03-04
  Type: incremental

  Restore Point   Date                 Type            Disk(s)
  ──────────────────────────────────────────────────────────────────────
  0               2026-03-02 22:00:01  FULL (base)     vda          ← Monday
  1               2026-03-03 22:00:01  Incremental     vda          ← Tuesday
  ──────────────────────────────────────────────────────────────────────
  Total: 2
```

Tuesday is restore point 1. Restore it:

```bash
sudo vmrestore \
  --vm /mnt/vm-backups/my-vm/2026-W10/.archives/chain-2026-03-04 \
  --restore-point 1 --restore-path /var/lib/libvirt/images
```

- `--restore-point 0` or `--restore-point full` would give Monday's state.

##### Restore to Wednesday (Week 1, Day 3 — offline copy)

Wednesday's copy backup is in `chain-2026-03-05`. List it first:

```bash
sudo vmrestore --list-restore-points \
  /mnt/vm-backups/my-vm/2026-W10/.archives/chain-2026-03-05
```

```
Restore Points: chain-2026-03-05

  ── (archive) ──
  Directory: /mnt/vm-backups/my-vm/2026-W10/.archives/chain-2026-03-05
  Type: copy

  Restore Point   Date                 Type            Disk(s)
  ──────────────────────────────────────────────────────────────────────
  0               2026-03-04 22:00:01  COPY (offline)  vda          ← Wednesday
  ──────────────────────────────────────────────────────────────────────
  Total: 1
```

Copy backups have exactly one restore point — no `--restore-point` needed:

```bash
sudo vmrestore \
  --vm /mnt/vm-backups/my-vm/2026-W10/.archives/chain-2026-03-05 \
  --restore-path /var/lib/libvirt/images
```

##### Restore to Thursday (Week 2, Day 4)

Thursday of week 2 is a copy backup archived on Friday of W11. List the archives for W11 first:

```bash
sudo vmrestore --list-restore-points my-vm --period 2026-W11
```

The output shows the current chain's restore points and the archived chains. Find the archive for Thursday (`chain-2026-03-13`), then list its contents:

```bash
sudo vmrestore --list-restore-points \
  /mnt/vm-backups/my-vm/2026-W11/.archives/chain-2026-03-13
```

```
Restore Points: chain-2026-03-13

  ── (archive) ──
  Directory: /mnt/vm-backups/my-vm/2026-W11/.archives/chain-2026-03-13
  Type: copy

  Restore Point   Date                 Type            Disk(s)
  ──────────────────────────────────────────────────────────────────────
  0               2026-03-12 22:00:01  COPY (offline)  vda          ← Thursday
  ──────────────────────────────────────────────────────────────────────
  Total: 1
```

Restore it:

```bash
sudo vmrestore \
  --vm /mnt/vm-backups/my-vm/2026-W11/.archives/chain-2026-03-13 \
  --restore-path /var/lib/libvirt/images
```

##### Restore Week 2 Fully (Latest State of W11)

This restores the current chain in `2026-W11/` at its latest restore point (Sunday of W11):

```bash
sudo vmrestore --vm my-vm --period 2026-W11 \
  --restore-path /var/lib/libvirt/images
```

##### Restore to Saturday (Week 3, Day 6)

Saturday of W12 is restore point 1 in the current Fri/Sat/Sun chain:

```bash
sudo vmrestore --vm my-vm --period 2026-W12 \
  --restore-point 1 --restore-path /var/lib/libvirt/images
```

#### Additional Weekly Scenarios

##### Clone Instead of DR

Any of the above can be a clone by adding `--name`:

```bash
sudo vmrestore --vm my-vm --period 2026-W10 \
  --name my-vm-clone --restore-path /var/lib/libvirt/images-clone
```

##### Dry Run Before Restoring

Preview what vmrestore would do without executing:

```bash
sudo vmrestore --vm my-vm --period 2026-W10 \
  --restore-path /var/lib/libvirt/images --dry-run
```

##### Verify Backup Integrity Before Restoring

```bash
sudo vmrestore --verify my-vm --period 2026-W10
```

#### Multiple Backups in a Single Day

If vmbackup runs more than once on the same day (manual run or unplanned re-execution), the second run lands in the **same period directory** because the period ID (`2026-W10`) hasn't changed. Since a full backup already exists in the chain, the second run adds another incremental. The result is one extra restore point for that day. This applies identically to all on/off patterns described above — the only difference is an additional restore point in the chain.

---

### 10.2 Daily Rotation Policy

**Setup**: VM `my-vm`, daily rotation policy, vmbackup runs once daily. Seven days of backups: `20260302/` through `20260308/`.

#### Key Difference from Weekly

With daily rotation, **every day is a new period**. Each day gets its own directory. Since vmbackup runs once per day and the period changes daily, there are **no incremental chains** — each day contains exactly one independent backup (either a FULL or a COPY). There are no `.archives/` directories.

#### What Happens Each Day

| Day | VM State | vmbackup Action | Directory | Files Created |
|-----------|----------|------------------------------|--------------|--------------------------|
| Monday | Online | New period → **FULL** | `20260302/` | `vda.full.data` |
| Tuesday | Online | New period → **FULL** | `20260303/` | `vda.full.data` |
| Wednesday | Offline | New period, disk changed → **COPY** | `20260304/` | `vda.copy.data` |
| Thursday | Offline | New period, disk changed → **COPY** | `20260305/` | `vda.copy.data` |
| Friday | Online | New period → **FULL** | `20260306/` | `vda.full.data` |
| Saturday | Online | New period → **FULL** | `20260307/` | `vda.full.data` |
| Sunday | Online | New period → **FULL** | `20260308/` | `vda.full.data` |

#### Resulting Directory Structure

```
my-vm/
├── 20260302/                  ← Monday
│   └── vda.full.data
├── 20260303/                  ← Tuesday
│   └── vda.full.data
├── 20260304/                  ← Wednesday (offline)
│   └── vda.copy.data
├── 20260305/                  ← Thursday (offline)
│   └── vda.copy.data
├── 20260306/                  ← Friday
│   └── vda.full.data
├── 20260307/                  ← Saturday
│   └── vda.full.data
└── 20260308/                  ← Sunday
    └── vda.full.data
```

Each directory is independent. No chains, no archives, no restore point numbering.

#### Listing Restore Points

```bash
# List all VMs and their backup info
sudo vmrestore --list

# Show restore points for a specific day
sudo vmrestore --list-restore-points my-vm --period 20260303
```

Output for `--period 20260303`:

```
Restore Points: my-vm

  ── 20260303 ──
  Directory: /mnt/vm-backups/my-vm/20260303
  Type: full

  Restore Point   Date                 Type            Disk(s)
  ──────────────────────────────────────────────────────────────────────
  0               2026-03-03 22:00:01  FULL (only)     vda
  ──────────────────────────────────────────────────────────────────────
  Total: 1
```

One restore point. No archives.

#### Restore Examples

##### Restore Tuesday

```bash
sudo vmrestore --vm my-vm --period 20260303 \
  --restore-path /var/lib/libvirt/images
```

No `--restore-point` needed — there's only one backup per day.

##### Restore Wednesday (Offline Copy)

```bash
sudo vmrestore --vm my-vm --period 20260304 \
  --restore-path /var/lib/libvirt/images
```

Works identically — vmrestore auto-detects the copy backup type.

##### Restore the Latest Day

```bash
sudo vmrestore --vm my-vm \
  --restore-path /var/lib/libvirt/images
```

No `--period` needed — vmrestore picks the newest period automatically.

#### When Daily Makes Sense

- VMs where every day's state must be independently recoverable.
- Higher storage cost (every backup is a full/copy — no incremental savings).
- Simplest restore workflow: pick a date, restore.

#### Multiple Backups in a Single Day

If vmbackup runs a second time on the same day, it goes into the **same period directory** (e.g. `20260303/`). A full backup already exists, so the second run adds an incremental to it, creating a two-point chain. That day's directory then has restore point 0 (the original full) and restore point 1 (the second run). Use `--restore-point 0` for the first backup or omit it for the latest.

---

### 10.3 Monthly Rotation Policy

**Setup**: VM `my-vm`, monthly rotation policy, vmbackup runs once daily.
One month shown: `202603/`.

#### Key Difference from Weekly

Monthly rotation keeps everything in one period directory for the entire month. Chains can grow long (up to 30 restore points). On/off cycles create archived chains within the month, just like the weekly scenario but spanning more days.

#### What Happens (March 2026, Summarised)

| Days | VM State | vmbackup Action | Result |
|-------------|----------|------------------------------------------------|------------------------------|
| Mar 1 | Online | New month → new period → **FULL** | Starts new chain, restore point 0 |
| Mar 2–5 | Online | Chain continues → **incremental** each day | Restore points 1, 2, 3, 4 |
| Mar 6 | Offline | Disk changed. Archive chain (0–4) → **COPY** | Copy backup in period dir |
| Mar 7 | Offline | VM started, modified, shut down. Archive copy → **COPY** | New copy backup |
| Mar 8–14 | Online | Archive copy → **FULL**, then incrementals | New chain, restore points 0–6 |
| Mar 15 | Offline | Disk changed. Archive chain (0–6) → **COPY** | Copy backup |
| Mar 16–31 | Online | Archive copy → **FULL**, then incrementals | New chain, restore points 0–15 |

#### Resulting Directory Structure (End of March)

```
my-vm/202603/
├── .archives/
│   ├── chain-2026-03-06/               ← Mar 1–5 chain (5 points: full + 4 inc)
│   │   ├── vda.full.data
│   │   ├── vda.inc.virtnbdbackup.1.data
│   │   ├── vda.inc.virtnbdbackup.2.data
│   │   ├── vda.inc.virtnbdbackup.3.data
│   │   ├── vda.inc.virtnbdbackup.4.data
│   │   ├── checkpoints/
│   │   │   ├── virtnbdbackup.0.xml     ← Mar 1
│   │   │   ├── virtnbdbackup.1.xml     ← Mar 2
│   │   │   ├── virtnbdbackup.2.xml     ← Mar 3
│   │   │   ├── virtnbdbackup.3.xml     ← Mar 4
│   │   │   └── virtnbdbackup.4.xml     ← Mar 5
│   │   └── my-vm.cpt
│   ├── chain-2026-03-07/               ← Mar 6 copy (archived Mar 7)
│   │   └── vda.copy.data
│   ├── chain-2026-03-08/               ← Mar 7 copy (archived Mar 8)
│   │   └── vda.copy.data
│   ├── chain-2026-03-15/               ← Mar 8–14 chain (7 points)
│   │   ├── vda.full.data
│   │   ├── vda.inc.virtnbdbackup.{1..6}.data
│   │   ├── checkpoints/
│   │   └── my-vm.cpt
│   ├── chain-2026-03-16/               ← Mar 15 copy (archived Mar 16)
│   │   └── vda.copy.data
│
├── vda.full.data                       ← Mar 16 full (current chain base)
├── vda.inc.virtnbdbackup.1.data        ← Mar 17
├── vda.inc.virtnbdbackup.2.data        ← Mar 18
│   ... (continues through Mar 31)
├── vda.inc.virtnbdbackup.15.data       ← Mar 31
├── checkpoints/
│   ├── virtnbdbackup.0.xml             ← Mar 16
│   ├── virtnbdbackup.1.xml             ← Mar 17
│   │   ...
│   └── virtnbdbackup.15.xml            ← Mar 31
└── my-vm.cpt
```

#### Listing Restore Points

```bash
# Current month (auto-detected as latest period)
sudo vmrestore --list-restore-points my-vm

# Specific month
sudo vmrestore --list-restore-points my-vm --period 202603
```

Output shows 16 restore points in the current chain (Mar 16–31) plus 5 archived chains from earlier in the month.

#### Restore Examples

##### Restore to March 31 (Latest)

```bash
sudo vmrestore --vm my-vm --period 202603 \
  --restore-path /var/lib/libvirt/images
```

##### Restore to March 20

March 20 is restore point 4 in the current chain (Mar 16 = 0, Mar 17 = 1, ..., Mar 20 = 4):

```bash
sudo vmrestore --vm my-vm --period 202603 \
  --restore-point 4 --restore-path /var/lib/libvirt/images
```

##### Restore to March 3

March 3 is in the first archived chain. List the archive's restore points:

```bash
sudo vmrestore --list-restore-points \
  /mnt/vm-backups/my-vm/202603/.archives/chain-2026-03-06
```

```
Restore Points: chain-2026-03-06

  ── (archive) ──
  Directory: /mnt/vm-backups/my-vm/202603/.archives/chain-2026-03-06
  Type: incremental

  Restore Point   Date                 Type            Disk(s)
  ──────────────────────────────────────────────────────────────────────
  0               2026-03-01 22:00:01  FULL (base)     vda          ← Mar 1
  1               2026-03-02 22:00:01  Incremental     vda          ← Mar 2
  2               2026-03-03 22:00:01  Incremental     vda          ← Mar 3
  3               2026-03-04 22:00:01  Incremental     vda          ← Mar 4
  4               2026-03-05 22:00:01  Incremental     vda          ← Mar 5
  ──────────────────────────────────────────────────────────────────────
  Total: 5
```

March 3 is restore point 2. Restore it:

```bash
sudo vmrestore \
  --vm /mnt/vm-backups/my-vm/202603/.archives/chain-2026-03-06 \
  --restore-point 2 --restore-path /var/lib/libvirt/images
```

##### Restore to March 6 (Offline Copy Day)

List the archive's contents:

```bash
sudo vmrestore --list-restore-points \
  /mnt/vm-backups/my-vm/202603/.archives/chain-2026-03-07
```

```
Restore Points: chain-2026-03-07

  ── (archive) ──
  Directory: /mnt/vm-backups/my-vm/202603/.archives/chain-2026-03-07
  Type: copy

  Restore Point   Date                 Type            Disk(s)
  ──────────────────────────────────────────────────────────────────────
  0               2026-03-06 22:00:01  COPY (offline)  vda          ← Mar 6
  ──────────────────────────────────────────────────────────────────────
  Total: 1
```

One restore point. Restore it:

```bash
sudo vmrestore \
  --vm /mnt/vm-backups/my-vm/202603/.archives/chain-2026-03-07 \
  --restore-path /var/lib/libvirt/images
```

#### When Monthly Makes Sense

- VMs where storage efficiency matters — long incremental chains mean only changed blocks are stored each day.
- Fewer period directories to manage.
- Trade-off: longer chains mean restoring older points takes longer (more incrementals to apply).

#### Multiple Backups in a Single Day

If vmbackup runs more than once on the same day, the second run adds another incremental to the current chain within the same `YYYYMM/` directory. The effect is one extra restore point. All restore point numbering and selection works identically — the additional backup simply extends the chain.

---

### 10.4 Accumulate Policy

**Setup**: VM `my-vm`, accumulate rotation policy, vmbackup runs once daily.

#### Key Difference from All Other Policies

There are **no period directories at all**. All backup data lives directly in the VM's top-level folder. The chain grows continuously with no automatic rotation. vmrestore auto-detects this layout — no `--period` flag is needed or accepted.

Archives still happen when on/off cycles break the chain, stored in `.archives/` at the VM root.

#### What Happens Over 2 Weeks (Summarised)

| Days | VM State | vmbackup Action | Result |
|-------------|----------|------------------------------------------------|----------------------------|
| Day 1 | Online | First backup → **FULL** | Chain starts, restore point 0 |
| Day 2–5 | Online | Chain continues → **incremental** each day | Restore points 1–4 |
| Day 6 | Offline | Disk changed. Archive chain (0–4) → **COPY** | Copy backup at VM root |
| Day 7 | Online | Archive copy → **FULL** (new chain) | New chain, restore point 0 |
| Day 8–14 | Online | Chain continues → **incremental** each day | Restore points 1–7 |

#### Resulting Directory Structure (End of Day 14)

```
my-vm/
├── .archives/
│   ├── chain-2026-03-06/               ← Day 1–5 chain (archived Day 6)
│   │   ├── vda.full.data
│   │   ├── vda.inc.virtnbdbackup.{1..4}.data
│   │   ├── checkpoints/
│   │   └── my-vm.cpt
│   └── chain-2026-03-07/               ← Day 6 copy (archived Day 7)
│       └── vda.copy.data
│
├── vda.full.data                       ← Day 7 full (current chain base)
├── vda.inc.virtnbdbackup.1.data        ← Day 8
├── vda.inc.virtnbdbackup.2.data        ← Day 9
│   ...
├── vda.inc.virtnbdbackup.7.data        ← Day 14
├── checkpoints/
│   ├── virtnbdbackup.0.xml             ← Day 7
│   ├── virtnbdbackup.1.xml             ← Day 8
│   │   ...
│   └── virtnbdbackup.7.xml             ← Day 14
└── my-vm.cpt
```

Note: no period subdirectories. Everything is at the VM root.

#### Listing Restore Points

```bash
# No --period needed or expected
sudo vmrestore --list-restore-points my-vm
```

Output shows 8 restore points in the current chain (Day 7–14) plus 2 archived chains.

#### Restore Examples

##### Restore to Latest

```bash
sudo vmrestore --vm my-vm \
  --restore-path /var/lib/libvirt/images
```

No `--period`, no `--restore-point`.

##### Restore to Day 10

Day 10 is restore point 3 in the current chain (Day 7 = 0, Day 8 = 1, Day 9 = 2, Day 10 = 3):

```bash
sudo vmrestore --vm my-vm \
  --restore-point 3 --restore-path /var/lib/libvirt/images
```

##### Restore to Day 3 (Archived Chain)

List the archive's restore points:

```bash
sudo vmrestore --list-restore-points \
  /mnt/vm-backups/my-vm/.archives/chain-2026-03-06
```

```
Restore Points: chain-2026-03-06

  ── (archive) ──
  Directory: /mnt/vm-backups/my-vm/.archives/chain-2026-03-06
  Type: incremental

  Restore Point   Date                 Type            Disk(s)
  ──────────────────────────────────────────────────────────────────────
  0               2026-03-01 22:00:01  FULL (base)     vda          ← Day 1
  1               2026-03-02 22:00:01  Incremental     vda          ← Day 2
  2               2026-03-03 22:00:01  Incremental     vda          ← Day 3
  3               2026-03-04 22:00:01  Incremental     vda          ← Day 4
  4               2026-03-05 22:00:01  Incremental     vda          ← Day 5
  ──────────────────────────────────────────────────────────────────────
  Total: 5
```

Day 3 is restore point 2. Restore it:

```bash
sudo vmrestore \
  --vm /mnt/vm-backups/my-vm/.archives/chain-2026-03-06 \
  --restore-point 2 --restore-path /var/lib/libvirt/images
```

##### Restore to Day 6 (Offline Copy)

List the archive:

```bash
sudo vmrestore --list-restore-points \
  /mnt/vm-backups/my-vm/.archives/chain-2026-03-07
```

```
Restore Points: chain-2026-03-07

  ── (archive) ──
  Directory: /mnt/vm-backups/my-vm/.archives/chain-2026-03-07
  Type: copy

  Restore Point   Date                 Type            Disk(s)
  ──────────────────────────────────────────────────────────────────────
  0               2026-03-06 22:00:01  COPY (offline)  vda          ← Day 6
  ──────────────────────────────────────────────────────────────────────
  Total: 1
```

Restore it:

```bash
sudo vmrestore \
  --vm /mnt/vm-backups/my-vm/.archives/chain-2026-03-07 \
  --restore-path /var/lib/libvirt/images
```

#### Accumulate Safety Limits

If the chain grows past 365 restore points (configurable), vmbackup automatically archives the chain and starts fresh. This prevents unbounded chain growth.

#### When Accumulate Makes Sense

- Long-running appliance VMs that rarely go offline.
- VMs where period-based organisation isn't meaningful.
- Maximum storage efficiency — one chain covers the entire history.
- Trade-off: very long chains take longer to restore (hundreds of incrementals to replay).

#### Multiple Backups in a Single Day

If vmbackup runs more than once on the same day, the second run adds another incremental to the chain at the VM root. The effect is one extra restore point. No different from any other incremental — the chain simply grows by one.

---

### Summary: Choosing a Restore Approach

| What you want | Command |
|---------------------------------------------------|---------|
| Restore to the latest state of the latest period | `sudo vmrestore --vm my-vm --restore-path /path` |
| Restore a specific period's latest state | Add `--period 2026-W10` |
| Restore a specific restore point within a chain | Add `--restore-point 3` |
| Restore from an archived chain | Use `--vm /full/path/to/.archives/chain-YYYY-MM-DD` |
| Clone instead of DR | Add `--name clone-name` |
| Preview without executing | Add `--dry-run` |
| Verify backup integrity first | `sudo vmrestore --verify my-vm --period 2026-W10` |

---
## 11. TPM and BitLocker Restore

vmrestore automatically restores TPM state for VMs that have TPM backups (indicated by a `.tpm-backup-marker` file created by [vmbackup](https://github.com/doutsis/vmbackup)). No manual steps are needed.

### How It Works

1. **Detection:** vmrestore checks for `.tpm-backup-marker` in the data directory
2. **UUID resolution (cascade):**
   - Clone mode: use the new UUID from `virsh define` (after `define_new_identity()`)
   - `BACKUP_METADATA.txt`: extract `VM UUID:` field from `tpm-state/BACKUP_METADATA.txt`
   - Fallback: `virsh dominfo {vm-name}` (only works if VM is already defined)
3. **Pre-backup:** If TPM state already exists at the target path, it is moved to `{path}.pre-restore-{epoch}` as a safety backup
4. **Copy:** `tpm-state/tpm2/` is copied to `/var/lib/libvirt/swtpm/{UUID}/tpm2/`
5. **Permissions:** UUID directory set to `root:root 711`, `tpm2/` subdirectory set to `tss:tss 700` (Debian/Ubuntu; dynamically detected)

### Skipping TPM Restore

```bash
sudo vmrestore --vm my-vm --restore-path /tmp/restore --skip-tpm
```

### BitLocker and TPM

- **DR restore:** Same UUID → TPM sealed data is accessible → BitLocker unlocks automatically
- **Clone restore:** New UUID, but TPM state is copied to the new UUID path → BitLocker unlocks automatically (PCR measurements match because firmware and NVRAM are identical). If the clone's own UUID cannot be resolved, the TPM restore is **skipped** rather than guessed — see the 0.6.2 note below.
- **If TPM state is lost:** BitLocker will request a recovery key (find it in `tpm-state/bitlocker-recovery-keys.txt` in the backup)

> **A clone restore never writes the source VM's TPM (0.6.2):** a clone's TPM state is written to a path derived from the clone's own UUID. If that UUID could not be read back from the newly defined VM, earlier releases fell back to the UUID recorded in the backup's metadata — which is the **original** VM's. On a clone of a VM that is still running, that path is the live original's TPM state, so a restore the operator never aimed at that VM could overwrite it. vmrestore now detects the unresolved-UUID case and **skips the TPM restore entirely**: the source VM is left untouched, a warning names the clone and points at `tpm-state/bitlocker-recovery-keys.txt`, and the summary reports `TPM ✗` rather than success. The clone's disks still restore normally — only its TPM state is absent, so a BitLocker guest will ask for its recovery key on first boot.

> **Restore result reflects TPM outcome (0.6.0):** when the disks restore but TPM/BitLocker state fails to apply, vmrestore no longer reports an unqualified success. The summary line carries a `TPM ✓` token on success or `TPM ✗ (manual unlock required)` on failure (the token is omitted entirely for VMs without TPM), so a half-restored guest cannot look fully healthy.
>
> **Every ✓ is verified (0.6.1):** since 0.6.1 each ✓ in the restore summary is confirmed against the actual state of the restored VM, so a partial or failed restore can no longer print an all-green summary. One narrow exception: if the backup's configuration is unusable, vmrestore cannot derive the expected disk set — a DR restore then warns in the log that its disk safety checks are being skipped and proceeds unverified (a clone restore refuses to run at all in that state).

---
## 12. UEFI/OVMF Firmware and NVRAM Restore

### Checkpoint-Paired NVRAM (0.6.0)

A VM's UEFI variables (Secure Boot keys, `BootOrder`, MOK) are captured **per checkpoint** as `<vm>_VARS*.fd.virtnbdbackup.<N>`. vmrestore resolves the chain-endpoint checkpoint and pairs **that** NVRAM with the restored disks, rather than reusing the live host NVRAM. This fixes a class of `BdsDxe: No mapping` boot failures that occurred when an older period was restored over a VM whose firmware state had since drifted.

- **In-place / DR restore:** the matching backup NVRAM is restored to the VM's NVRAM path. The existing live NVRAM is first backed up to `<path>.before-restore.<timestamp>` so the prior state is recoverable.
- **Clone restore (`--name`):** the backup NVRAM is copied to `{clone-name}_VARS.fd`, the `<nvram>` path in the VM XML is updated, and ownership is set to `libvirt-qemu:libvirt-qemu` mode 600 — so the clone and original never share firmware state.
- **Older chains:** backups predating per-checkpoint NVRAM capture fall back to the live host NVRAM, with a warning that the guest may fail to boot if its identity/Secure Boot state has drifted.
- **OVMF firmware code** (`OVMF_CODE_4M.ms.fd`) is read-only and is never modified — only the per-VM `_VARS` NVRAM is restored.

### NVRAM Recovery if Missing

If the NVRAM file is missing after restore:

```bash
# Check what path the VM expects
sudo virsh dumpxml my-vm | grep nvram

# Copy from backup (bare-named copy is the latest state)
sudo cp /mnt/backups/vm/my-vm/20260303/my-vm_VARS.fd \
  /var/lib/libvirt/qemu/nvram/my-vm_VARS.fd
sudo chown libvirt-qemu:libvirt-qemu /var/lib/libvirt/qemu/nvram/my-vm_VARS.fd
```

### Cross-Host Considerations

When restoring to a different host, verify that OVMF firmware paths match:

```bash
# Check firmware path in VM XML
sudo virsh dumpxml my-vm | grep -E 'loader|nvram'

# Verify firmware exists on target host
ls /usr/share/OVMF/OVMF_CODE_4M.ms.fd
```

---
## 13. Verifying Backups Before Restore

### Checksum Verification

```bash
# Verify checksums for a VM's latest period
sudo vmrestore --verify my-vm

# Verify a specific period
sudo vmrestore --verify my-vm --period 2026-W09
```

Runs `virtnbdrestore -o verify`, which recomputes adler32 checksums and compares them against the `.data.chksum` files. Any mismatch is reported.

### Metadata Dump

```bash
sudo vmrestore --dump my-vm --period 2026-W09
```

Outputs JSON metadata including virtual disk size, data size, checkpoint names, dates, and compression details. Useful for confirming what's in a backup before restoring.

---
## 14. Post-Restore Steps

### Automated by vmrestore

vmrestore handles these automatically — no manual action needed:

| Step | Details |
|------|---------|
| Disk restoration | virtnbdrestore reconstructs qcow2 from `.data` files |
| VM definition | DR re-injects the original UUID (undefine/redefine); clone defines with a new name, UUID, and MACs |
| TPM state | Copied to `/var/lib/libvirt/swtpm/{UUID}/tpm2/` (`tss:tss`, mode 700); pre-existing state saved as `.pre-restore-<timestamp>` |
| NVRAM | DR restores the chain-endpoint NVRAM over the original path (old copy saved as `.before-restore.<timestamp>`); clone copies it to `{clone}_VARS.fd` |
| Disk integrity check | Every expected disk image must exist and pass `qemu-img check` before success — a missing disk stops the restore as incomplete, and "corrupt" and "could not be verified" are reported distinctly; all three outcomes abort the restore (exit 4). The check tolerates the engine's brief lock-release delay (a bounded wait, so momentary timing never fails a healthy restore). In the rare DR case where the backup's configuration is unusable, vmrestore warns that its disk safety checks are being skipped and proceeds unverified; clone restores refuse to run instead — see [Restore Scenarios](#9-restore-scenarios) |
| Storage pool refresh | `virsh pool-refresh` on the containing pool |
| Log file | Written to `/var/log/vmrestore/` with full command and timing details |

### After Every Restore — Manual Steps

These apply to **every** restore, DR or clone. They are not edge cases.

#### 1. Start and Verify the VM

```bash
sudo virsh start {vm-name}
sudo virsh domdisplay {vm-name}   # get VNC/SPICE display URL
```

Or open virt-manager and check the console.

#### 2. Clean Stale Checkpoint Metadata

The restored VM inherits checkpoint metadata from the backup. These checkpoints don't match the restored disk state.

By default, vmbackup handles this automatically — its `ENABLE_AUTO_RECOVERY_ON_CHECKPOINT_CORRUPTION` setting (default: `yes`) archives the old chain and starts a fresh FULL on the next backup run. No manual cleanup is needed.

If auto-recovery has been disabled (`warn`), you must clean the stale checkpoints manually or the next backup will fail with "bitmap already exists" errors:

```bash
for cp in $(sudo virsh checkpoint-list {vm-name} --name 2>/dev/null); do
  sudo virsh checkpoint-delete {vm-name} --checkpointname "$cp" --metadata
done
```

#### 3. Re-Add CD-ROM Drives

virtnbdrestore strips all non-restorable device types from the VM definition, including CD-ROM drives and raw/passthrough devices. This is by design — these devices contain paths that may not exist on the restore host. The VM will boot without them.

To re-add a CD-ROM drive:

```bash
# Add an empty CD-ROM (SATA bus)
sudo virt-xml {vm-name} --add-device --disk device=cdrom,bus=sata

# Or with an ISO attached
sudo virt-xml {vm-name} --add-device --disk device=cdrom,bus=sata,source.file=/path/to/image.iso

# Attach/change ISO on an existing CD-ROM
sudo virsh change-media {vm-name} sdb /path/to/image.iso --insert
```

#### 4. Verify Network Attachment

The restored VM's network interfaces reference the virtual network names from the original host (e.g., `default`, `br0`). If the restore host has different network names, the VM may fail to start or start with no network. Check with:

```bash
sudo virsh domiflist {vm-name}
```

Edit the VM XML if the network name needs changing:

```bash
sudo virsh edit {vm-name}
# Find <source network='...'/> and update to match the restore host
```

---
## 15. Troubleshooting

### Restore Path Inside a Backup Tree — "BLOCKED" Errors

Before anything else, vmrestore checks that `--restore-path` does not point **inside any configured `BACKUP_PATH`**. Writing restored disks into the backup tree could overwrite the very `.data` files being read, so this is refused outright — choose a target outside the backup storage.

### Disk Collision Protection — "BLOCKED" Errors

vmrestore's `preflight_disk_safety()` checks predicted output files against all live VM disks before restoring.

| Scenario | Behaviour |
|----------|-----------|
| Output file is the **live disk of another VM** | **Always blocked** — choose a different `--restore-path` |
| Output file is the **live disk of the same VM** (running) | **Blocked** — shut off the VM first |
| Output file is the **live disk of the same VM** (shut off) | Requires `--force` (disaster recovery) |
| Output file **already exists** (not a live disk) | Requires `--force` to overwrite |
| Clone mode | Uses staging directory — no collision with restore path during restore |

#### Examples

```bash
# SAFE: clone to same directory as original VM (staging dir prevents collision)
sudo vmrestore --vm my-vm --name my-clone --restore-path /var/lib/libvirt/images/

# BLOCKED: final filename would collide with another VM's live disk
sudo vmrestore --vm my-vm --name other-vm --restore-path /var/lib/libvirt/images/
# ERROR: BLOCKED: /var/lib/libvirt/images/other-vm.qcow2 is the live disk of VM 'other-vm'

# DR: must shut off VM and use --force
sudo virsh shutdown my-vm
sudo vmrestore --vm my-vm --restore-path /var/lib/libvirt/images --force
```

### "VM already defined" Error

The VM name already exists in libvirt. Use `--force` to undefine it first:

```bash
sudo vmrestore --vm my-vm --restore-path /tmp/restore --force
```

Or undefine manually:

```bash
sudo virsh destroy my-vm 2>/dev/null
sudo virsh undefine my-vm --managed-save --nvram --checkpoints-metadata
```

### "Cannot resolve data directory"

vmrestore cannot find a data directory containing `.data` files. Common causes:
- Wrong `--period` value — check available periods with `--list-restore-points`
- Backup path not mounted — verify `--backup-path` or check that vmbackup.conf has a valid `BACKUP_PATH`
- Accumulate VMs store data directly in the VM directory (no period subdirectory)

```bash
sudo vmrestore --list-restore-points my-vm
```

### Pre-Flight Free Space Check

vmrestore automatically checks destination free space before restoring:

| Condition | Action |
|-----------|--------|
| Sufficient space with >10% headroom | Logs data size and free space, proceeds |
| Sufficient space but <10% headroom | **Warns** — restored qcow2 can be larger than raw backup data |
| Insufficient space | **Aborts** with `Insufficient space: restore needs X but only Y available on Z` |
| Cannot determine free space | **Warns** and skips the check (non-fatal) |

### "NBD server not available" or Socket Errors

```bash
# Ensure NBD module is loaded
sudo modprobe nbd
# Check for running NBD processes
ps aux | grep nbd
# Clear stale socket files
sudo rm -f /var/tmp/virtnbdbackup.*
```

### "virtnbdrestore failed, retrying disk-only" in the Log

This is expected resilience, not an error. On a full restore vmrestore first asks `virtnbdrestore` to both restore the disks **and** define the VM (`-D`). If that combined step fails (commonly a libvirt define quirk), vmrestore automatically retries the **disk-only** restore without `-D`, then defines the VM itself. If the retry also fails, the run aborts with exit code 6. A single retry line in the log followed by a successful summary means the fallback worked.

### "Bitmap already exists" on First Backup After Restore

This means vmbackup's `ENABLE_AUTO_RECOVERY_ON_CHECKPOINT_CORRUPTION` has been set to `warn`. Clean stale checkpoint metadata (see [section 14 step 2](#2-clean-stale-checkpoint-metadata)) or re-enable auto-recovery (the default).

### Restored VM Won't Boot (UEFI/TPM)

1. **Check OVMF firmware:** `ls /usr/share/OVMF/OVMF_CODE_4M.ms.fd` — path must exist on the restore host
2. **Check NVRAM:** `sudo virsh dumpxml {vm} | grep nvram` — verify file exists at that path
3. **Check TPM state:** `ls -la /var/lib/libvirt/swtpm/{UUID}/tpm2/` — must exist with correct ownership
4. **Ownership:** `tss:tss` on Debian/Ubuntu (vmrestore detects this dynamically)
5. **If TPM state was not restored:** VM with BitLocker may request recovery key (see [section 11 TPM and BitLocker Restore](#11-tpm-and-bitlocker-restore))

### Restored VM is Missing CD-ROM Drives

Expected behaviour. See [section 14 Re-Add CD-ROM Drives](#3-re-add-cd-rom-drives).

### Clone Boots but BitLocker Asks for Recovery Key

This is **expected** when TPM state was not restored or when NVRAM differs. Enter the recovery key from `tpm-state/bitlocker-recovery-keys.txt` in the backup. BitLocker will re-seal on next reboot.

If this happens on a **DR restore** (same UUID), check that TPM state was restored correctly (see [section 11](#11-tpm-and-bitlocker-restore)).

### Incremental File Missing from Chain

If an incremental data file is corrupted or missing, you can only restore up to the last good restore point:

```bash
sudo vmrestore --vm my-vm --restore-point 1 --restore-path /tmp/restore
```

### "Permission denied" During Restore

```bash
# vmrestore must run as root
sudo vmrestore ...

# Check AppArmor if virsh commands fail
sudo aa-teardown  # Disable AppArmor for this session (Debian/Ubuntu)
```

### Dry Run Shows Unexpected Paths

```bash
sudo vmrestore --vm my-vm --restore-path /tmp/test --dry-run
```

---
## 16. Quick Reference Commands

### Inventory & Inspection

```bash
# List all VMs with backup info
sudo vmrestore --list

# List restore points for a VM (auto-resolves latest period)
sudo vmrestore --list-restore-points my-vm

# List restore points for a specific period
sudo vmrestore --list-restore-points my-vm --period 2026-W09

# Verify backup checksums
sudo vmrestore --verify my-vm --period 20260303

# Dump backup metadata (JSON)
sudo vmrestore --dump my-vm --period 2026-W10

# Review restore history (via the shared catalogue)
sudo vmbackup --status --restores
```

### Disaster Recovery

```bash
# Latest state — rebuild the VM with original identity
sudo vmrestore --vm my-vm --restore-path /var/lib/libvirt/images

# Specific period — recover from a particular date
sudo vmrestore --vm my-vm --period 20260302 \
  --restore-path /var/lib/libvirt/images

# Replace existing VM definition
sudo vmrestore --vm my-vm --restore-path /var/lib/libvirt/images --force
```

### Clone Restore

```bash
# New-identity clone (new UUID, new MACs, isolated NVRAM)
sudo vmrestore --vm my-vm \
  --name test-clone --restore-path /var/lib/libvirt/images
```

### Point-in-Time

```bash
# Specific restore point
sudo vmrestore --vm my-vm --restore-point 3 \
  --restore-path /var/lib/libvirt/images

# Full baseline only
sudo vmrestore --vm my-vm --restore-point full \
  --restore-path /var/lib/libvirt/images
```

### Advanced

```bash
# Path-aware --vm (backup path embedded in argument)
sudo vmrestore --vm /mnt/backups/vm/my-vm \
  --period 20260302 --restore-path /tmp/restore

# Restore from archived chain
sudo vmrestore \
  --vm /mnt/backups/vm/my-vm/2026-W09/.archives/chain-2026-02-28.1 \
  --restore-path /tmp/restore/archived

# Replace a single disk in-place (VM must be shut off)
sudo vmrestore --vm my-vm --disk vdb

# Replace multiple disks at once
sudo vmrestore --vm my-vm --disk vda,vdb,sda

# Replace all disks
sudo vmrestore --vm my-vm --disk all

# Extract a single disk to staging
sudo vmrestore --vm my-vm --disk vdb --restore-path /tmp/extract

# Replace disk without .pre-restore backup
sudo vmrestore --vm my-vm --disk vdb --no-pre-restore

# Disk-only (no VM definition)
sudo vmrestore --vm my-vm --restore-path /tmp/restore --skip-config

# Restore from an incomplete/broken chain (forensic use only)
sudo vmrestore --vm my-vm --restore-path /tmp/restore --include-incomplete

# Dry run (preview without executing)
sudo vmrestore --vm my-vm --restore-path /tmp/restore --dry-run
```

### Post-Restore

```bash
# Start restored VM
sudo virsh start {vm-name}

# Verify
sudo virsh domblklist {vm-name}
sudo virsh domdisplay {vm-name}

# Clean stale checkpoints (only needed if vmbackup auto-recovery is disabled)
# By default, vmbackup handles this automatically on the next backup run
for cp in $(sudo virsh checkpoint-list {vm-name} --name 2>/dev/null); do
  sudo virsh checkpoint-delete {vm-name} --checkpointname "$cp" --metadata
done
```

---
## 17. Exit Codes

`vmrestore` returns categorised exit codes so monitoring systems can distinguish *why* a run failed without scraping logs. Backward-compatible — `if vmrestore; then` and `(( $? != 0 ))` patterns continue to work.

| Code | Meaning | Example trigger |
|------|---------|-----------------|
| 0    | Success | Normal completion, `--help`, `--version` |
| 1    | General error | Catch-all for uncategorised failure |
| 2    | Configuration error | Named config instance does not exist, missing required path |
| 3    | Lock conflict | Another backup or restore holds the per-VM lock (`${BACKUP_PATH}/_state/locks/vmbackup-<vm>.lock`) |
| 4    | Storage error | Backup path missing, insufficient space, disk collision with a live VM, `.pre-restore` file already exists |
| 5    | VM problem | VM not found, VM in wrong state, incomplete chain without `--include-incomplete`, disk not in chain at requested checkpoint |
| 6    | External tool failure | `virtnbdrestore` returned non-zero |
| 7    | CLI usage error | Unknown flag, missing required argument, `--restore-path` inside a backup tree, invalid `--restore-point` value |
| 8    | Missing dependency | A required `lib/*.sh` module could not be sourced at startup |
| 130  | SIGINT (Ctrl-C) | Shell convention 128 + 2 |
| 143  | SIGTERM | Shell convention 128 + 15 |

Symmetric with `vmbackup` — the same number means the same category in both tools, so monitoring rules can be written once and applied to either binary.

---

*vmrestore 0.6.1 — ships in the unified [vmbackup](https://github.com/doutsis/vmbackup) package*

<p align="center">
  <img src="docs/vibe-coded.png" alt="Vibe Coded" />
</p>