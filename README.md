# vmbackup

[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Version](https://img.shields.io/github/v/release/doutsis/vmbackup)](https://github.com/doutsis/vmbackup/releases)

Automated backup manager for KVM/libvirt virtual machines, built on [virtnbdbackup](https://github.com/abbbi/virtnbdbackup).

vmbackup automates virtnbdbackup — scheduling, rotation, retention, backup validation, replication and reporting. It works on personal machines, homelabs and production KVM hosts alike.

## Why vmbackup

virtnbdbackup handles the hard part but it operates on one VM at a time with no scheduling, no retention management and no replication. If you run more than a couple of VMs you end up writing your own wrapper scripts for rotation, cleanup, backup validation and email alerts.

vmbackup is that wrapper. It orchestrates virtnbdbackup across your entire fleet and handles everything around it — backup validation, failure recovery, multi-destination replication and reporting.

## Quick Start

```bash
wget https://github.com/doutsis/vmbackup/releases/download/v0.5.0/vmbackup_0.5.0_all.deb
sudo dpkg -i vmbackup_0.5.0_all.deb
```

Edit `/opt/vmbackup/config/default/vmbackup.conf` to set your backup path and preferences, then:

```bash
sudo vmbackup                            # run a backup now
sudo systemctl start vmbackup.timer      # enable the daily schedule
```

## Features

- **Policy-based backup strategy** — daily, weekly, monthly or accumulate with VM state awareness. Backups are validated before they're needed and cleanup happens automatically
- **Works via systemd** — install the .deb, enable the timer, backups run on schedule without intervention
- **Multi-destination replication** — NFS for local targets; rclone for cloud. Custom endpoints can be added by implementing the transport interface
- **Email reporting** — per-run summary with VM status, duration and errors so you know what happened without checking logs
- **Per-VM overrides** — rotation policy and exclusion rules per VM when one size doesn't fit
- **Bash + SQLite** — no Python runtime, no database server, no web UI. Just libvirt, qemu-utils, sqlite3 and jq

## How It Works

vmbackup wraps `virtnbdbackup` and manages the full backup lifecycle:

1. **Discovery** — queries libvirt for all VMs, applies include/exclude filters
2. **Backup** — runs `virtnbdbackup` for each VM (full or incremental based on what already exists)
3. **Rotation** — organises backups into period-based directories (e.g. monthly)
4. **Retention** — removes expired backups based on configurable age/count policies
5. **Replication** — copies backups to local and/or cloud destinations via pluggable transports
6. **Reporting** — sends email summaries with per-VM status, duration and errors

```
vmbackup.sh              ← main script
├── modules/             ← rotation, retention, replication, email, etc.
├── lib/                 ← backup validation, logging, SQLite, locking
├── transports/          ← NFS transport driver (SMB, SSH planned)
├── cloud_transports/    ← SharePoint/rclone cloud drivers
└── config/
    ├── default/         ← active configuration
    └── template/        ← reference configs with documentation
```

## Installation

### Prerequisites

Requires `bash >= 5.0`, `libvirt-daemon-system`, `qemu-utils`, [virtnbdbackup](https://github.com/abbbi/virtnbdbackup), `sqlite3` and `jq`. Optionally `msmtp` for email reports and `rclone` for cloud replication.

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
sudo git clone https://github.com/doutsis/vmbackup.git /opt/vmbackup
cd /opt/vmbackup
sudo make install
```

### Uninstall

```bash
sudo apt remove vmbackup    # remove but keep config
sudo apt purge vmbackup     # remove everything including config and logs
```

Remove keeps your configuration under `/opt/vmbackup/config/` so you can reinstall later without reconfiguring. Purge deletes config files, logs and the AppArmor profile. Backup data is never touched by either — it lives wherever you configured `BACKUP_PATH`.

## Configuration

All configuration lives in `/opt/vmbackup/config/`. Each config directory is a named instance containing:

| File | Purpose |
|------|---------|
| `vmbackup.conf` | Backup path, schedule policy, compression, VM filters |
| `email.conf` | Email reporting (SMTP via msmtp) |
| `replication_local.conf` | Local replication destinations (NFS/SMB/SSH) |
| `replication_cloud.conf` | Cloud replication destinations (rclone) |
| `vm_overrides.conf` | Per-VM rotation policy and exclusion overrides |
| `exclude_patterns.conf` | Disk/path patterns to exclude from backups |

The `default/` instance is used when vmbackup runs without `--config-instance`. The `template/` directory contains fully documented reference configs — copy it to create a new instance:

```bash
cp -r /opt/vmbackup/config/template /opt/vmbackup/config/prod
vmbackup --config-instance prod
```

This lets you run separate configurations (e.g. dev, staging, prod) from the same installation.

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

vmbackup automatically chooses full or incremental based on your rotation policy and what already exists on disk. A new period (day, week or month depending on policy) triggers a fresh full backup. Within the same period, backups are incremental. Offline VMs are only backed up when their disk has changed.

The `accumulate` policy has no period boundary — it runs incrementals until the hard limit is reached (default 365) then archives and starts fresh.

## Rotation Policies

Each VM can be assigned a rotation policy that controls how backups are organised and retained:

| Policy | Behaviour |
|--------|-----------|
| `daily` | Archives existing backups when the date changes and starts a fresh full. Keeps 7 daily folders by default. |
| `weekly` | Archives existing backups at the start of a new ISO week. Keeps 4 weekly folders by default. |
| `monthly` | Archives existing backups at the start of a new month. Keeps 3 monthly folders by default. This is the default policy. |
| `accumulate` | Backups accumulate indefinitely with no scheduled archival. When the number of incremental backups hits the hard limit (default 365) they are automatically archived and a fresh full backup starts. |
| `never` | VM is excluded from backup entirely. Use for templates, scratch VMs or anything you don't want backed up. |

The default rotation policy is set in `vmbackup.conf` and applies to all VMs. Individual VMs can be assigned a different policy in `vm_overrides.conf`. Retention is enforced per policy.

## Failure Detection & Self-Remediation

vmbackup is designed to recover without intervention. Every backup run validates backup state, data integrity and lock health before doing anything — and if something is wrong, it fixes it rather than failing.

If an incremental backup fails, vmbackup converts it to a full and retries. If the backup sequence is broken, it archives what's there and starts fresh. If a previous run was interrupted, the next run cleans up stale locks and partial files automatically. The goal is that a scheduled backup should never require manual intervention to get back on track.

## Host Configuration Backup

Each backup session captures host-level libvirt configuration needed to rebuild the virtualisation environment:

- `/etc/libvirt/` — daemon config, VM domain XMLs, hooks
- `/var/lib/libvirt/qemu/` — runtime state, autostart
- `/var/lib/libvirt/network/` — virtual network definitions, DHCP leases
- `/etc/network/` and `/etc/NetworkManager/` — bridge and network config

Host config is deduplicated — only stored when it has changed since the first-of-month archive.

## TPM & BitLocker Support

For VMs with emulated TPM (Windows BitLocker, Linux Secure Boot), vmbackup backs up TPM state from `/var/lib/libvirt/swtpm/` alongside each VM backup. TPM state is deduplicated — unchanged state is symlinked to the previous copy rather than stored again.

For Windows VMs with BitLocker, vmbackup uses the QEMU guest agent to extract recovery keys from the running guest automatically. The keys are stored alongside the TPM state so they're available if the TPM becomes unusable after restore — new UUID, hardware change or TPM corruption. If the guest agent isn't installed or the VM isn't running, extraction is skipped silently without blocking the backup.

## Security

vmbackup enforces `root:backup` ownership across everything it touches — the install tree, backup data, logs and lock files. This is not configurable.

### The backup group

The `backup` group (GID 34) is a standard Debian system group. The `.deb` package creates it if it doesn't already exist. All vmbackup files are owned `root:backup` so that root can write backups and members of the `backup` group can read them.

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

Replication runs after backup completes.  Local and cloud replication operate independently and can run in parallel or sequentially.

**Local replication** uses rsync over NFS with configurable bandwidth limits and verification modes (size or checksum).

**Cloud replication** uses rclone to sync to SharePoint, Backblaze, S3 or any rclone-supported backend.

Custom transport endpoints can be added by implementing the transport interface — five functions (`init`, `sync`, `verify`, `cleanup`, `get_free_space`) and a set of metrics globals. See the full transport interface in [vmbackup.md](vmbackup.md#transport-function-contract).

## Documentation

Full technical documentation is included in [vmbackup.md](vmbackup.md) (installed to `/opt/vmbackup/vmbackup.md`). It covers architecture, configuration reference, rotation policies, backup lifecycle, archive management, replication transport interface, SQLite schema, failure detection and security model in detail.

## Issues

Found a bug or have a feature request? [Open an issue](https://github.com/doutsis/vmbackup/issues).

## License

MIT

---

<p align="center">
  <img src="docs/vibe-coded.png" alt="100% Vibe Coded" width="300">
</p>
