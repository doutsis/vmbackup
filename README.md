# vmbackup

[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Version](https://img.shields.io/github/v/release/doutsis/vmbackup)](https://github.com/doutsis/vmbackup/releases)

Automated backup manager for KVM/libvirt virtual machines, built on [virtnbdbackup](https://github.com/abbbi/virtnbdbackup).

vmbackup automates virtnbdbackup — scheduling, rotation, retention, chain integrity, replication and reporting. It works on personal machines, homelabs and production KVM hosts alike.

## Why vmbackup

virtnbdbackup handles the hard part — QEMU dirty bitmaps, NBD exports, incremental disk snapshots. But it operates on one VM at a time with no scheduling, no retention management and no replication. If you run more than a couple of VMs you end up writing your own wrapper scripts for rotation, cleanup, chain validation and email alerts.

vmbackup is that wrapper. It orchestrates virtnbdbackup across your entire fleet and handles everything around it — chain integrity, failure recovery, multi-destination replication and reporting. As far as we can tell, it's the only virtnbdbackup orchestration tool on GitHub.

## Features

- **Policy-based backup strategy** — daily, weekly, monthly or accumulate with VM state awareness. Chain validation detects corruption before it matters and checkpoint consolidation happens automatically
- **Works via systemd** — install the .deb, enable the timer, backups run on schedule without intervention
- **Multi-destination replication** — NFS, SMB and SSH for local targets; rclone for cloud. Each destination carries its own retention policy and custom endpoints can be written with adherence to the transport contract
- **Email reporting** — per-run summary with VM status, duration and errors so you know what happened without checking logs
- **Per-VM overrides** — retention, compression and exclusion rules per VM when one size doesn't fit
- **Bash + SQLite** — no Python runtime, no database server, no web UI. Just libvirt, qemu-utils, sqlite3 and jq

## How It Works

vmbackup wraps `virtnbdbackup` and manages the full backup lifecycle:

1. **Discovery** — queries libvirt for running VMs, applies include/exclude filters
2. **Backup** — runs `virtnbdbackup` for each VM (full or incremental based on chain state)
3. **Rotation** — organises backups into period-based directories (e.g. monthly)
4. **Retention** — removes expired backups based on configurable age/count policies
5. **Replication** — copies backups to local and/or cloud destinations via pluggable transports
6. **Reporting** — sends email summaries with per-VM status, duration and errors

```
vmbackup.sh              ← main script
├── modules/             ← rotation, retention, replication, email, etc.
├── lib/                 ← chain validation, logging, SQLite, locking
├── transports/          ← NFS, SMB, SSH transport drivers
├── cloud_transports/    ← SharePoint/rclone cloud drivers
└── config/
    ├── default/         ← active configuration
    └── template/        ← reference configs with documentation
```

## Installation

### From .deb Package (Recommended)

Download the latest `.deb` from [Releases](https://github.com/doutsis/vmbackup/releases):

```bash
wget https://github.com/doutsis/vmbackup/releases/download/v0.5.0/vmbackup_0.5.0_all.deb
sudo dpkg -i vmbackup_0.5.0_all.deb
```

The package installs to `/opt/vmbackup/` and sets up:
- `vmbackup` command in PATH
- `root:backup` ownership with restricted permissions
- systemd service and timer units
- AppArmor profile for libvirt/QEMU integration

### From Source

```bash
git clone https://github.com/doutsis/vmbackup.git
cd vmbackup
make package
sudo dpkg -i build/vmbackup_0.5.0_all.deb
```

## Configuration

All configuration lives in `/opt/vmbackup/config/`. Each config directory is a named instance containing:

| File | Purpose |
|------|---------|
| `vmbackup.conf` | Backup path, schedule policy, compression, VM filters |
| `email.conf` | Email reporting (SMTP via msmtp) |
| `replication_local.conf` | Local replication destinations (NFS/SMB/SSH) |
| `replication_cloud.conf` | Cloud replication destinations (rclone) |
| `vm_overrides.conf` | Per-VM retention, compression or exclusion overrides |
| `exclude_patterns.conf` | Disk/path patterns to exclude from backups |

The `default/` instance is used when vmbackup runs without `--config-instance`. The `template/` directory contains fully documented reference configs — copy it to create a new instance:

```bash
cp -r /opt/vmbackup/config/template /opt/vmbackup/config/prod
vmbackup --config-instance prod
```

This lets you run separate configurations (e.g. dev, staging, prod) from the same installation.

## Requirements

### Required

| Package | Purpose |
|---------|--------|
| bash >= 5.0 | Script runtime |
| libvirt-daemon-system | VM management (virsh, libvirtd) |
| qemu-utils | Disk image utilities |
| virtnbdbackup | The backup engine |
| sqlite3 | Backup state tracking |
| jq | JSON processing |

### Optional

| Package | Purpose |
|---------|--------|
| msmtp | Email report delivery |
| rclone | Cloud replication (SharePoint, Backblaze, S3, etc.) |

## VM State Handling

vmbackup handles VMs in any power state:

| State | Backup Method | Consistency |
|-------|---------------|-------------|
| **Running** (with QEMU agent) | FSFREEZE + incremental | Application-consistent |
| **Running** (no agent) | Pause + incremental | Crash-consistent |
| **Shut off** | Copy backup (if disk changed) | Clean |
| **Paused** | Treated as running | Crash-consistent |

Shut off VMs are only backed up when their disk has changed since the last backup. Unchanged VMs are skipped to avoid wasting storage.

## Backup Strategy

The backup type is determined automatically based on chain state and calendar boundaries:

| Condition | Backup Type |
|-----------|-------------|
| First backup ever | Full |
| Month boundary (new month) | Full |
| Recovery flag present | Full |
| Broken checkpoint chain | Full (after auto-archive) |
| Offline VM with disk changes | Copy |
| Normal daily run | Auto (incremental) |

When an incremental backup fails, vmbackup converts it to a full backup and retries. If the full also fails, it archives the chain, revalidates and retries with backoff.

## Rotation Policies

Each VM can be assigned a rotation policy that controls how backup chains are organised and retained:

| Policy | Behaviour |
|--------|-----------|
| `daily` | Archives the current chain when the date changes. Keeps 7 daily folders by default. |
| `weekly` | Archives the current chain at the start of a new ISO week. Keeps 4 weekly folders by default. |
| `monthly` | Archives the current chain at the start of a new month. Keeps 3 monthly folders by default. This is the default policy. |
| `accumulate` | Chain grows indefinitely with no scheduled archival. When checkpoint depth hits the hard limit (default 365) the chain is automatically archived and a fresh full backup starts. |
| `never` | VM is excluded from backup entirely. Use for templates, scratch VMs or anything you don't want backed up. |

Retention is enforced per policy. Per-VM overrides can assign different policies to individual VMs in `vm_overrides.conf`.

## Failure Detection & Self-Remediation

vmbackup is designed to recover without intervention. Every backup run validates chain state, checkpoint integrity and lock health before doing anything — and if something is wrong, it fixes it rather than failing.

If an incremental backup fails, vmbackup converts it to a full and retries. If the chain is broken, it archives what's there and starts fresh. If a previous run was interrupted, the next run cleans up stale locks and partial files automatically. The goal is that a scheduled backup should never require manual intervention to get back on track.

| Problem | Auto-Recovery |
|---------|---------------|
| Stale lock file | Delete if owning process is dead |
| Orphaned QEMU checkpoint | Delete checkpoint metadata |
| Broken checkpoint chain | Archive chain, start fresh full |
| Incomplete backup | Clean partial files, force full |
| Incremental fails | Convert to full, retry |
| Script interrupted | Next run recovers automatically |

## Host Configuration Backup

Each backup session captures host-level libvirt configuration needed to rebuild the virtualisation environment:

- `/etc/libvirt/` — daemon config, VM domain XMLs, hooks
- `/var/lib/libvirt/qemu/` — runtime state, autostart
- `/var/lib/libvirt/network/` — virtual network definitions, DHCP leases
- `/etc/network/` and `/etc/NetworkManager/` — bridge and network config

Host config is deduplicated — only stored when it has changed since the first-of-month archive.

## TPM & BitLocker Support

For VMs with emulated TPM (Windows BitLocker, Linux Secure Boot), vmbackup backs up TPM state from `/var/lib/libvirt/swtpm/` alongside each VM backup. TPM state is deduplicated — unchanged state is symlinked to the previous copy rather than stored again.

## Security

vmbackup uses SGID on backup directories so all files automatically inherit `root:backup` ownership. No post-hoc `chown` is needed.

| Layer | Mechanism |
|-------|-----------|
| Script | `umask 027` — files `640`, dirs `750` |
| Directories | SGID bit (`2750`) — group inheritance propagates |
| systemd | `UMask=0027` — belt-and-suspenders |
| Package | `install -m 750/640` — not world-accessible |
| AppArmor | Profile for libvirt/QEMU integration |

## SQLite Logging

All backup activity is logged to a SQLite database at `$BACKUP_PATH/_state/vmbackup.db`. The database tracks sessions, per-VM results, replication runs, retention actions and chain health events. This enables queries like "last successful backup per VM" or "total bytes replicated this month" without parsing log files.

## Replication

Replication runs after backup completes.  Local and cloud replication operate independently and can run in parallel or sequentially.

**Local replication** uses rsync over NFS, SMB or SSH with configurable bandwidth limits, verification modes (size or checksum) and per-destination retention.

**Cloud replication** uses rclone to sync to SharePoint, Backblaze, S3 or any rclone-supported backend.

Custom transport endpoints can be added by implementing the transport contract — five functions (`init`, `sync`, `verify`, `cleanup`, `get_free_space`) and a set of metrics globals. See the full transport contract in [vmbackup.md](vmbackup.md#transport-function-contract).

## Documentation

Full technical documentation is included in [vmbackup.md](vmbackup.md) (installed to `/opt/vmbackup/vmbackup.md`). It covers architecture, configuration reference, rotation policy mechanics, checkpoint lifecycle, archive chain management, replication transport contracts, SQLite schema, failure detection and security model in detail.

## License

MIT

---

<p align="center">
  <img src="docs/vibe-coded.png" alt="100% Vibe Coded" width="300">
</p>
