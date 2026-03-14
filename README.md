# vmbackup

Automated backup manager for KVM/libvirt virtual machines, built on [virtnbdbackup](https://github.com/abbbi/virtnbdbackup).

vmbackup automates virtnbdbackup — scheduling, rotation, retention, chain integrity, replication and reporting. It works on personal machines, homelabs and production KVM hosts alike.

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

## Configuration

All configuration lives in `/opt/vmbackup/config/default/`:

| File | Purpose |
|------|---------|
| `vmbackup.conf` | Backup path, schedule policy, compression, VM filters |
| `email.conf` | Email reporting (SMTP via msmtp) |
| `replication_local.conf` | Local replication destinations (NFS/SMB/SSH) |
| `replication_cloud.conf` | Cloud replication destinations (rclone) |
| `vm_overrides.conf` | Per-VM retention, compression or exclusion overrides |
| `exclude_patterns.conf` | Disk/path patterns to exclude from backups |

Template configs with full documentation are in `config/template/`.

### Multiple Instances

Run separate configurations (e.g. dev, staging, prod) by creating named config directories:

```bash
cp -r /opt/vmbackup/config/template /opt/vmbackup/config/prod
vmbackup --config-instance prod
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

## Requirements

### Required

| Package | Purpose |
|---------|---------|
| bash >= 5.0 | Script runtime |
| libvirt-daemon-system | VM management (virsh, libvirtd) |
| qemu-utils | Disk image utilities |
| virtnbdbackup | The backup engine |
| sqlite3 | Backup state tracking |
| jq | JSON processing |

### Optional

| Package | Purpose |
|---------|---------|
| msmtp | Email report delivery |
| rclone | Cloud replication (SharePoint, Backblaze, S3, etc.) |

## License

MIT
