# vmbackup

[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Version](https://img.shields.io/github/v/release/doutsis/vmbackup)](https://github.com/doutsis/vmbackup/releases)

The backup half of the [vmbackup](https://github.com/doutsis/vmbackup) / [vmrestore](https://github.com/doutsis/vmrestore) ecosystem. Automated backup manager for KVM/libvirt virtual machines, built on [virtnbdbackup](https://github.com/abbbi/virtnbdbackup).

vmbackup automates virtnbdbackup — scheduling, rotation, retention, backup validation, replication and reporting. It works on personal machines, homelabs and production KVM hosts alike. For restores, see [vmrestore](https://github.com/doutsis/vmrestore).

## Why vmbackup

virtnbdbackup handles the hard part but it operates on one VM at a time with no scheduling, no retention management and no replication. If you run more than a couple of VMs you end up writing your own wrapper scripts for rotation, cleanup, backup validation and email alerts.

vmbackup is that wrapper. It orchestrates virtnbdbackup across your entire fleet and handles everything around it — backup validation, failure recovery, multi-destination replication and reporting.

## Quick Start

**Prerequisite:** vmbackup requires [virtnbdbackup](https://github.com/abbbi/virtnbdbackup) (≥ 2.28) — install it first: **[installation instructions](https://github.com/abbbi/virtnbdbackup?tab=readme-ov-file#installation)**

**Debian / Ubuntu:**

```bash
wget https://github.com/doutsis/vmbackup/releases/download/v0.5.2/vmbackup_0.5.2_all.deb
sudo dpkg -i vmbackup_0.5.2_all.deb
```

**Any distro (Arch, Fedora, openSUSE, etc.):**

```bash
git clone https://github.com/doutsis/vmbackup.git
cd vmbackup
sudo make install
```

Then edit `/opt/vmbackup/config/default/vmbackup.conf` to set your backup path and preferences:

```bash
sudo vmbackup                            # run a backup now
sudo systemctl start vmbackup.timer      # enable the daily schedule
```

For the full step-by-step walkthrough — backup path setup, per-VM overrides, email, replication and more — see the [Quick Setup Guide](vmbackup.md#quick-setup-guide) in the detailed documentation.

## Features

- **Every VM, automatically** — discovers and backs up all VMs on the host. No manifest to maintain — new VMs are picked up on the next run
- **Full + incremental, zero decisions** — first backup is a full; every backup after that is an incremental. Period boundaries (daily, weekly, monthly) trigger a fresh full automatically
- **Self-healing** — failed incrementals convert to fulls, broken chains are archived and restarted, interrupted runs clean up after themselves. Scheduled backups should never need manual intervention
- **Multi-destination replication** — rsync to any mounted filesystem, rclone to cloud. Failed replication can be re-run independently without repeating a backup
- **TPM and BitLocker handled** — TPM state and BitLocker recovery keys are extracted and stored alongside each VM backup
- **Host environment captured** — libvirt configuration, network definitions and dependent service config are backed up so you can rebuild the host, not just the VMs
- **FSTRIM optimisation** — trims guest filesystems via the QEMU agent before backup so qcow2 images compress better and incrementals are smaller. Per-path logging, configurable minimum extent, per-VM exclusions, and automatic detection of missing Windows VirtIO `discard_granularity` overrides
- **Paired with vmrestore** — single-command disaster recovery, clones and point-in-time restores via [vmrestore](https://github.com/doutsis/vmrestore)
- **Minimal dependencies** — pure Bash + SQLite with no additional runtimes, frameworks or services to install. If your host runs libvirt, vmbackup runs too

## How It Works

vmbackup wraps `virtnbdbackup` and manages the full backup lifecycle:

1. **Discovery** — queries libvirt for every VM on the host and applies your include/exclude filters. New VMs are picked up automatically.
2. **Backup** — runs full or incremental backups per VM based on what already exists on disk. Per-VM overrides let you set different policies or exclude individual VMs entirely.
3. **Rotation** — organises backups into period-based directories. Daily, weekly and monthly policies archive the previous period and start a fresh full automatically. The accumulate policy runs incrementals indefinitely until a configurable limit is reached. Per-VM overrides apply here too.
4. **Retention** — removes expired archives based on configurable age and count limits per policy. Runs after every backup so storage stays predictable without manual cleanup.
5. **Replication** — copies the backup tree to local and cloud destinations so backups exist in more than one place. Local targets use rsync; cloud targets use rclone. Both can run in parallel. If replication fails or is interrupted, it can be re-run independently without repeating the backup.
6. **Reporting** — sends an email summary with per-VM status, duration, errors and replication results.

## Installation

### Prerequisites

vmbackup is a wrapper around [virtnbdbackup](https://github.com/abbbi/virtnbdbackup) — it **will not function without it**. Install virtnbdbackup (≥2.28) first:

> **[virtnbdbackup installation instructions](https://github.com/abbbi/virtnbdbackup?tab=readme-ov-file#installation)**

Also requires `bash >= 5.0`, `libvirt-daemon-system`, `qemu-utils`, `sqlite3` and `jq`. Optionally `msmtp` for email reports and `rclone` for cloud replication.

### From .deb Package (Debian / Ubuntu)

Download the latest `.deb` from [Releases](https://github.com/doutsis/vmbackup/releases):

```bash
wget https://github.com/doutsis/vmbackup/releases/download/v0.5.2/vmbackup_0.5.2_all.deb
sudo dpkg -i vmbackup_0.5.2_all.deb
```

### From Source (any distro)

```bash
git clone https://github.com/doutsis/vmbackup.git
cd vmbackup
sudo make install
```

Both methods install to `/opt/vmbackup/` and set up:
- `vmbackup` command in PATH
- `root:backup` ownership with restricted permissions
- systemd service and timer units
- AppArmor profile for libvirt/QEMU integration

### Uninstall

**Debian / Ubuntu (.deb install):**

```bash
sudo apt remove vmbackup    # remove but keep config
sudo apt purge vmbackup     # remove everything including config and logs
```

**From source (make install):**

```bash
sudo make uninstall
```

Remove keeps your configuration under `/opt/vmbackup/config/` so you can reinstall later without reconfiguring. Purge (or `make uninstall`) deletes config files, logs and the AppArmor profile. Backup data is never touched — it lives wherever you configured `BACKUP_PATH`.

## Configuration

All configuration lives in `/opt/vmbackup/config/`. Each config directory is a named instance containing:

| File | Purpose |
|------|---------|
| `vmbackup.conf` | Backup path, schedule policy, compression, VM filters |
| `email.conf` | Email reporting (SMTP via msmtp) |
| `replication_local.conf` | Local replication destinations (rsync) |
| `replication_cloud.conf` | Cloud replication destinations (rclone) |
| `vm_overrides.conf` | Per-VM rotation policy and exclusion overrides |
| `exclude_patterns.conf` | Wildcard rules to exclude VMs by name (e.g. `test-*`) |
| `fstrim_exclude.conf` | VM name patterns to exclude from pre-backup FSTRIM |

The `default/` instance is used when vmbackup runs without `--config-instance`. The `template/` directory contains fully documented reference configs — copy it to create a new instance:

```bash
cp -r /opt/vmbackup/config/template /opt/vmbackup/config/prod
vmbackup --config-instance prod
```

This lets you run separate configurations (e.g. dev, staging, prod) from the same installation.

### VM discovery and exclusion

vmbackup discovers and backs up every VM on the host automatically. You don't maintain a list of VMs to back up — if libvirt knows about it, vmbackup backs it up.

To give a specific VM a different rotation policy or exclude it entirely, add an entry to `vm_overrides.conf`. This is the right place for permanent, per-VM decisions — a production database that needs daily rotation while everything else runs monthly, or a template VM that should never be backed up.

To exclude VMs by naming convention, add wildcard rules to `exclude_patterns.conf`. Patterns like `test-*` or `*-clone-*` let you skip entire classes of VMs without listing each one individually. Useful when test or scratch VMs are created and destroyed frequently.

### Self-healing

vmbackup validates backup state, data integrity and lock health at the start of every run. If an incremental backup fails, it converts to a full and retries. If the backup sequence is broken, it archives what's there and starts fresh. If a previous run was interrupted, stale locks and partial files are cleaned up automatically. Scheduled backups should never require manual intervention to get back on track.

### Usage

Once configured, vmbackup runs unattended via the systemd timer. For manual runs and operational tasks:

```bash
# Run a backup using the default config (config/default/)
sudo vmbackup

# Run using a named config instance (config/prod/)
sudo vmbackup --config-instance prod

# Preview what a backup would do without writing anything
sudo vmbackup --dry-run

# Cancel replication on a running session (backups continue)
sudo vmbackup --cancel-replication

# Re-run replication without repeating the backup
sudo vmbackup --replicate-only

# Clean up archived chains and old periods
sudo vmbackup --prune list
```

All commands accept `--config-instance` and `--dry-run`. See [vmbackup.md](vmbackup.md) for the full CLI reference.

## VM State Handling

vmbackup handles VMs in any power state:

| State | Backup Method | Consistency |
|-------|---------------|-------------|
| **Running** (with QEMU agent) | FSFREEZE + incremental | Application-consistent |
| **Running** (no agent) | Pause + incremental | Crash-consistent |
| **Shut off** | Copy backup (if disk changed) | Clean |
| **Paused** | Treated as running | Crash-consistent |

Shut off VMs are only backed up when their disk has changed since the last backup. Unchanged VMs are skipped to avoid wasting storage.

## Rotation & Retention

Rotation policies control how backups are organised and when old data is removed:

| Policy | Behaviour |
|--------|-----------|
| `daily` | Archives existing backups when the date changes and starts a fresh full. Keeps 7 daily folders by default. |
| `weekly` | Archives existing backups at the start of a new ISO week. Keeps 4 weekly folders by default. |
| `monthly` | Archives existing backups at the start of a new month. Keeps 3 monthly folders by default. This is the default policy. |
| `accumulate` | Backups accumulate indefinitely with no scheduled archival. When the number of incremental backups hits the hard limit (default 365) they are automatically archived and a fresh full backup starts. |
| `never` | VM is excluded from backup entirely. Use for templates, scratch VMs or anything you don't want backed up. |

The default rotation policy is set in `vmbackup.conf` and applies to all VMs. Individual VMs can be assigned a different policy in `vm_overrides.conf`. Retention is enforced per policy.

### Manual cleanup

Automated retention runs after each backup, but sometimes you need to reclaim space on demand — remove archived chains, clean up old periods or wipe a decommissioned VM entirely. `--prune` handles this without running a backup session. All operations support `--dry-run` to preview, `--yes` to skip confirmation, and a keep-last guard that prevents removing the last period. See [vmbackup.md](vmbackup.md#on-demand-cleanup---prune) for the full target reference.

## Host Configuration Backup

Each backup session captures the libvirt configuration, network definitions and dependent service config needed to rebuild the virtualisation environment — not just the VMs. Host config is deduplicated and only stored when it has changed.

## TPM & BitLocker Support

For VMs with emulated TPM (Windows BitLocker, Linux Secure Boot), vmbackup backs up TPM state from `/var/lib/libvirt/swtpm/` alongside each VM backup. TPM state is deduplicated — unchanged state is symlinked to the previous copy rather than stored again.

For Windows VMs with BitLocker, vmbackup uses the QEMU guest agent to extract recovery keys from the running guest automatically. The keys are stored alongside the TPM state so they're available if the TPM becomes unusable after restore — new UUID, hardware change or TPM corruption. If the guest agent isn't installed or the VM isn't running, extraction is skipped silently without blocking the backup.

## Security

vmbackup enforces `root:backup` ownership across everything it touches — the install tree, backup data, logs and lock files. This is not configurable.

### The backup group

The `backup` group (GID 34) is a standard system group. Both the `.deb` package and `make install` create it if it doesn't already exist. All vmbackup files are owned `root:backup` so that root can write backups and members of the `backup` group can read them.

To browse backups, check logs or query the SQLite database, add your user to the group:

```bash
sudo usermod -aG backup myuser
# Log out and back in for group membership to take effect
```

If you also want non-root access to `virsh list` and other libvirt commands, add the `libvirt` group too:

```bash
sudo usermod -aG backup,libvirt myuser
```

### SGID and permissions

Backup directories use the SGID bit (mode `2750`, shown as `drwxr-s---`). When SGID is set on a directory, every new file and subdirectory automatically inherits the `backup` group — no post-hoc `chown` is needed. Combined with `umask 027`, the result is files at `640` and directories at `2750` with `root:backup` ownership throughout.

On first run, vmbackup detects that `BACKUP_PATH` lacks SGID and applies it automatically. From that point forward, SGID propagates to all subdirectories created by vmbackup, virtnbdbackup or any other child process.

| Layer | Mechanism |
|-------|-----------|
| Script | `umask 027` — files `640`, dirs `750` |
| Directories | SGID bit (`2750`) — group inheritance propagates to all new files and subdirectories |
| systemd | `UMask=0027` — belt-and-suspenders with the in-script umask |
| Package | `install -m 750/640` — nothing is world-accessible |
| AppArmor | Profile for libvirt/QEMU NBD socket access |

### Sensitive material

TPM private keys and BitLocker recovery keys are isolated from the backup group. The `tpm-state/` directory has SGID stripped and contents are owned `root:root` with mode `600`. A user in the `backup` group can browse the backup tree and read VM configs and logs but cannot read TPM keys or BitLocker recovery keys.

## SQLite Logging

All backup activity is logged to a SQLite database at `$BACKUP_PATH/_state/vmbackup.db`. The database tracks sessions, per-VM results, replication runs, retention actions and backup health events. This enables queries like "last successful backup per VM" or "total bytes replicated this month" without parsing log files.

## Replication

Replication runs after backup completes. Local and cloud replication operate independently and can run in parallel or sequentially.

**Local replication** uses rsync to any locally accessible path — local disks, NFS mounts, virtiofs shares, pre-mounted CIFS, or anything else that appears as a local directory. Configurable bandwidth limits and post-sync verification (size or checksum).

**Cloud replication** uses rclone to sync to SharePoint, Backblaze B2, S3, or any rclone-supported backend. Currently ships with a SharePoint transport driver.

Both systems use a pluggable transport architecture. New local transports can be added by implementing five functions (`init`, `sync`, `verify`, `cleanup`, `get_free_space`) and a metrics contract. New cloud transports are added by implementing the cloud transport function and metrics contracts. See the full transport interface in [vmbackup.md](vmbackup.md#transport-function-contract).

### Run replication on demand

Replication normally runs at the end of each backup session, but `--replicate-only` lets you trigger it independently. Useful when pre-seeding a new destination before the first scheduled run, adding a destination to an existing setup, or re-running replication that was interrupted or cancelled during a backup. Scope can be narrowed to `local` or `cloud` only. No VMs are touched and no retention runs. See [vmbackup.md](vmbackup.md#standalone-replication---replicate-only) for the full reference.

## Restoring

vmbackup and [vmrestore](https://github.com/doutsis/vmrestore) are two halves of one system. vmbackup backs up — vmrestore restores. They share no code and have no runtime coupling, but vmrestore exclusively restores backups created by vmbackup.

vmrestore provides single-command disaster recovery, clone restores and point-in-time recovery — with full identity management, TPM/BitLocker support and pre-flight safety checks.

```bash
sudo vmrestore --vm my-vm --restore-path /var/lib/libvirt/images
```

## Tested

vmbackup and vmrestore are validated together using a destructive end-to-end test that exercises the full backup-to-restore lifecycle. The test is config-driven — VM definitions, paths and timeouts live in an external config file, making it straightforward to add new scenarios such as Linux with TPM.

The current test fleet covers the configurations that matter:

| VM | Disks | TPM | UEFI/NVRAM | Notes |
|----|-------|-----|------------|-------|
| Linux base | 1× VirtIO | No | No | Baseline Linux guest |
| Linux multi-disk | 2× VirtIO + 1× SATA | No | No | Cloned from base, mixed bus disks added |
| Linux multi-disk clone | 2× VirtIO + 1× SATA | No | No | Cloned from multi-disk |
| Windows base | 1× VirtIO | Yes | Yes (OVMF) | BitLocker enabled, UEFI + Secure Boot |
| Windows multi-disk | 2× VirtIO + 1× SATA | Yes | Yes (OVMF) | Cloned from base, mixed bus disks added |
| Windows multi-disk clone | 2× VirtIO + 1× SATA | Yes | Yes (OVMF) | Cloned from multi-disk |

The test runs through these phases:

1. **Record identities** — UUID, MAC addresses, TPM presence, disk layout for every VM
2. **Plant checkfiles** — write a marker file inside each guest via the QEMU agent (Linux and Windows)
3. **Backup** — full backup cycle with FSTRIM, checkpoint validation and incremental chains
4. **Verify** — confirm backup integrity with `vmrestore --verify`
5. **Prune** — auto-detect and prune stale archives and periods from live backup data
6. **Clone** — restore representative VMs as clones, verify new UUID + preserved data + disk paths, then destroy clones
7. **Point-in-time restore** — restore to an earlier restore point (not latest), verify the VM boots and data matches the expected state
8. **Destroy everything** — delete all original VMs including definitions, disks and NVRAM
9. **DR restore** — restore all VMs from backup to a clean path
10. **Post-restore verification** — for every restored VM, confirm:
    - UUID and MAC addresses match originals
    - All disks present and in the correct restore path
    - TPM device and swtpm state directory preserved
    - Checkfile inside the guest survived the full backup → destroy → restore cycle
    - BitLocker not triggered on Windows VMs (disk unlocked, no recovery prompt)

## Documentation

Full technical documentation is included in [vmbackup.md](vmbackup.md) (installed to `/opt/vmbackup/vmbackup.md`). It covers architecture, configuration reference, rotation policies, backup lifecycle, archive management, replication transport interface, SQLite schema, failure detection and security model in detail.

## Known Issues

### Windows VMs: slow FSTRIM with VirtIO disks

QEMU's default `discard_granularity` for VirtIO block devices causes Windows to issue millions of tiny 512-byte TRIM operations instead of coalescing them. A 20 GB disk can take 10+ minutes to trim — versus 1–2 seconds with the fix applied.

Linux guests are unaffected (the kernel coalesces TRIMs regardless). SATA guests also work fine.

**Fix:** Add a `discard_granularity` override (32 MiB recommended) to each VirtIO disk in the VM's libvirt XML. vmbackup detects missing overrides automatically at backup time and logs the exact XML to add.

Full details, performance benchmarks and step-by-step XML instructions: [VirtIO discard_granularity & Windows TRIM Performance](vmbackup.md#virtio-discard_granularity--windows-trim-performance)

## Issues

Found a bug or have a feature request? [Open an issue](https://github.com/doutsis/vmbackup/issues).

## License

MIT

---

<p align="center">
  <img src="docs/vibe-coded.png" alt="100% Vibe Coded" width="300">
</p>
