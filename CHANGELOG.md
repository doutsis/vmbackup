# Changelog

All notable changes to [vmbackup](https://github.com/doutsis/vmbackup) will be documented in this file.

Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/). Versions follow [Semantic Versioning](https://semver.org/).

## [0.5.1] - 2026-03-17

### Fixed

- **chain_health off-by-one** — `post_backup_hook()` passed a zero-based index to `sqlite_update_chain_health()` which expects a count. After a successful backup, `total_checkpoints` and `restorable_count` were 0 instead of 1. Fixed by passing `checkpoint + 1` in `vmbackup_integration.sh`.
- **restore_points counted per-disk instead of per-backup** — `get_restore_point_count()` counted raw `.data` files. A 3-disk VM reported `restore_points=3` after one backup instead of 1. Rewritten to count logical restore points: 1 for any full/copy presence + distinct incremental checkpoint levels.
- **csv_ variable name remnants** — 25 `csv_`-prefixed variables and stale CSV comments remained from the pre-SQLite migration. All variables renamed (`csv_backup_method` → `backup_method`, etc.), dead CSV cleanup code removed from `vmbackup.sh` and `logging_module.sh`, `LOG_CSV_KEEP_DAYS` config removed.
- **Archived chains missing vmconfig XML and TPM marker** — `archive_existing_checkpoint_chain()` used a glob that matched checkpoint metadata but not `vmconfig.virtnbdbackup.*.xml` files. The `.tpm-backup-marker` gate file was also never included. Archives were incomplete — vmrestore worked around it via fallbacks, but the archives were not self-contained. Fixed: both file types now archived correctly.

### Changed

- Section header "CSV Logging Helper Functions" renamed to "Backup Metric Helper Functions" in `vmbackup.sh`.

## [0.5.0] - 2026-03-14

Initial public release.

### Features

- Full and incremental backups via virtnbdbackup with automatic checkpoint management.
- Rotation policies: daily, weekly, monthly, accumulate, never.
- Chain lifecycle management with automatic archival and cleanup.
- SQLite logging — sessions, per-VM backup records, chain health, file operations, retention actions, chain lifecycle events.
- Email reports with per-VM summary, replication status and error details.
- Local replication via rsync with configurable transport (SSH, SMB, NFS/local).
- Cloud replication via rclone with SharePoint support.
- FSTRIM optimisation module for thin-provisioned storage.
- TPM state and BitLocker recovery key extraction via QEMU guest agent.
- AppArmor self-healing for libvirt-qemu profile.
- Debian packaging (`make package`) with systemd timer.
- `make install` from source for any distro.
- Configuration instances for multi-environment deployments.
- Dry-run mode (`--dry-run`) for safe previewing.
- Security model: root:backup ownership, 750/640 permissions, SGID inheritance.
