# vmbackup and vmrestore — KVM/libvirt Backup and Restore Manager

[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Version](https://img.shields.io/github/v/release/doutsis/vmbackup)](https://github.com/doutsis/vmbackup/releases)

Automated backup and restore for KVM/libvirt virtual machines, built on [virtnbdbackup](https://github.com/abbbi/virtnbdbackup) / [virtnbdrestore](https://github.com/abbbi/virtnbdbackup?tab=readme-ov-file#restore-examples). `vmbackup` automates virtnbdbackup — scheduling, rotation, retention, backup validation, replication and reporting. `vmrestore` automates virtnbdrestore — disaster recovery, clone restores and point-in-time recovery. Both are in this one package.

## One package, two commands

vmbackup and vmrestore are now one package, two commands. They install together, upgrade together, and never drift out of step. If you previously installed vmrestore separately, the new package replaces it cleanly with no configuration changes and no migration steps. Every existing flag, configuration file, systemd unit and runbook keeps working as before.

You get a few real wins from this:

- **One install gives vmbackup *and* vmrestore.** No second download to remember, no version mismatch to chase.
- **vmrestore now knows about your backups.** It reads the same catalogue vmbackup writes, so `vmrestore --list` shows chain health and last-backup time alongside what's available to restore — and it falls back to walking the disk if the catalogue isn't there, so disaster recovery on a fresh host still works exactly as it always has.
- **Restore history is queryable.** Every restore is recorded; `vmbackup --status --restores` shows you what was restored, when, in which mode, and whether it succeeded.
- **Backup and restore can't trip over each other.** They share a per-VM lock, end to end: a restore of a VM that's mid-backup is refused with a clear lock error, and a scheduled backup that finds a restore in progress skips that VM — reported once as skipped, not as a failure — instead of corrupting either operation.

See [CHANGELOG.md](CHANGELOG.md) for the full list of changes.

## Quick Start

**Prerequisite:** vmbackup requires [virtnbdbackup](https://github.com/abbbi/virtnbdbackup) (>= 2.28) — install it first: **[installation instructions](https://github.com/abbbi/virtnbdbackup?tab=readme-ov-file#installation)**

**Debian / Ubuntu:**

Download the latest `.deb` from [Releases](https://github.com/doutsis/vmbackup/releases) and install:

```bash
sudo apt install ./vmbackup_*_all.deb
```

This installs both `vmbackup` (backup) and `vmrestore` (restore). If you have the old standalone `vmrestore` package installed, `apt` removes it automatically.

**Any distro (Arch, Fedora, openSUSE, etc.):**

```bash
git clone https://github.com/doutsis/vmbackup.git
cd vmbackup
sudo make install
```

Then edit `/opt/vmbackup/config/default/vmbackup.conf` to set your backup path and preferences:

```bash
sudo vmbackup --run                        # run a backup now
sudo systemctl start vmbackup.timer        # enable the daily schedule

sudo vmrestore --list                      # see what's available to restore
sudo vmrestore --vm web --restore-path /var/lib/libvirt/images --force   # restore in place (--force overwrites the existing VM)
```

For the full step-by-step walkthrough — backup path setup, per-VM overrides, email, replication and more — see the [Quick Setup Guide](vmbackup.md#quick-setup-guide).

## Why vmbackup + vmrestore

`virtnbdbackup` and `virtnbdrestore` handle the hard part: getting consistent block-level data in and out of a running VM. They operate on one VM at a time and leave everything around them to you — scheduling, rotation, retention, replication, alerting, identity handling on restore, NVRAM and TPM, collision detection, integrity checks. If you run more than a couple of VMs you end up writing wrapper scripts.

This is that wrapper, for both directions.

- **vmbackup** runs `virtnbdbackup` across your entire fleet automatically — discovers every VM, manages full and incremental chains, rotates by period, enforces retention, replicates to local and cloud destinations, emails you a summary and keeps a queryable history of everything that's happened.
- **vmrestore** runs `virtnbdrestore` — single-command disaster recovery, clone restores with new identity, point-in-time recovery to any checkpoint, single-disk surgical restores, and pre-flight safety checks at every step.

Backups are only as good as your ability to restore them. Both halves are designed, built, tested and released together.

## Features

### vmbackup

- **Every VM, automatically** — discovers and backs up every VM on the host. No manifest to maintain — new VMs are picked up on the next run.
- **Targeted backup** — back up one or more specific VMs on demand with `--vm`, without waiting for the scheduled run.
- **Full + incremental, zero decisions** — first backup is a full, every backup after that is an incremental, period boundaries trigger a fresh full automatically.
- **Self-healing** — failed incrementals convert to fulls, broken chains are archived and restarted, interrupted runs clean up after themselves. Scheduled backups should never need manual intervention.
- **Multi-destination replication** — rsync to any mounted filesystem, rclone to cloud. Failed replication can be re-run independently without repeating a backup.
- **TPM and BitLocker handled** — TPM state and BitLocker recovery keys are extracted and stored alongside each VM backup.
- **FSTRIM optimisation** — trims guest filesystems via the QEMU agent before backup so qcow2 images compress better and incrementals are smaller.
- **Slack notifications** — optional run notifications to a Slack webhook. Disabled by default, needs nothing beyond `curl`, and `SLACK_ON_SUCCESS` / `SLACK_ON_FAILURE` filters control which runs get posted.
- **Status at a glance** — query backup sessions, VM history, failures, replication, chain health, storage, retention policies and restore history from the command line with `--status`. Human-readable tables by default, `--csv` for export.

### vmrestore

- **Disaster recovery** — rebuild a destroyed VM from any backup chain.
- **Clone restores** — stand up an isolated copy with a new name, no identity conflicts.
- **Point-in-time recovery** — roll back to any incremental checkpoint in the chain.
- **Single-disk restore** — surgical recovery of one disk without touching the rest.
- **Identity preserved correctly** — original UUID, MAC addresses, TPM state, NVRAM and BitLocker keys are restored to the right places. Clones get fresh identity, isolated NVRAM and no UEFI key collisions.
- **NVRAM and disk stay in sync** — restores pair the per-checkpoint NVRAM with the matching disk state so the VM boots into the firmware view it had at that point in time.
- **Catalogue-aware listing** — `vmrestore --list` reads chain health from the same database vmbackup populates; broken or incomplete chains are flagged before you commit to a restore.
- **Pre-flight safety** — dry-run mode, collision detection, backup-path overlap guards, and detailed logging at every step.
- **Post-restore verification** — every expected disk image must exist and pass an integrity check before a restore is reported as successful. (If a backup's configuration is too damaged to know what to expect — a rare disaster-recovery corner — the restore warns in its log that the safety checks were skipped.)

### Both

- **One install, one version.** Same package, same configuration directory, same catalogue. Per-VM locks are shared so backup and restore never race.
- **Lightweight** — pure Bash, SQLite and minimal dependencies. No extra runtimes, frameworks or services. If your host runs libvirt, vmbackup runs.

## How It Works

### Backup lifecycle

vmbackup wraps `virtnbdbackup` and manages the full backup lifecycle:

1. **Discovery** — queries libvirt for every VM on the host and applies your include/exclude filters. New VMs are picked up automatically.
2. **Backup** — runs full or incremental backups per VM based on what already exists on disk. Per-VM overrides let you set different policies or exclude individual VMs.
3. **Rotation** — organises backups into period-based directories. Daily, weekly and monthly policies archive the previous period and start a fresh full automatically. The accumulate policy runs incrementals indefinitely until a configurable limit is reached.
4. **Retention** — removes expired archives based on configurable age and count limits per policy. Runs after every backup so storage stays predictable without manual cleanup.
5. **Replication** — copies the backup tree to local and cloud destinations so backups exist in more than one place. Local targets use rsync, cloud targets use rclone, both can run in parallel.
6. **Reporting** — sends an email summary with per-VM status, duration, errors and replication results — and, if enabled, a Slack notification. Between runs, `--status` gives you instant read-only access to the same data.

### Restore lifecycle

vmrestore wraps `virtnbdrestore`:

1. **Discovery** — `--list` walks the backup tree and, if available, consults the catalogue to surface chain health, last-backup timestamp and broken-chain warnings.
2. **Pre-flight** — checks the restore path against every configured `BACKUP_PATH` (refuses overlap), validates the target chain is complete, and previews the action under `--dry-run`.
3. **Restore** — reconstructs disk state across full and incremental chains via `virtnbdrestore`, pairs the disks with the matching per-checkpoint NVRAM, restores TPM state with the right ownership, and refreshes the libvirt storage pool.
4. **Identity** — preserves original UUID and MAC for in-place restores; assigns fresh UUID and MAC for `--name` clones, with isolated NVRAM so UEFI keys don't collide.
5. **Verify** — before reporting success, confirms every expected disk image exists and passes an integrity check.
6. **Audit** — records the restore in the catalogue so `vmbackup --status --restores` can show you it happened.

## Installation

### Prerequisites

vmbackup is a wrapper around [virtnbdbackup](https://github.com/abbbi/virtnbdbackup) — it **will not function without it**. Install virtnbdbackup first:

> **[virtnbdbackup installation instructions](https://github.com/abbbi/virtnbdbackup?tab=readme-ov-file#installation)**

vmbackup requires virtnbdbackup **>= 2.28**. The `.deb` package enforces this as a dependency; if you install from source, check `virtnbdbackup --version` yourself.

Also requires `bash >= 5.0`, `libvirt-daemon-system`, `qemu-utils`, `sqlite3` and `jq`. Optionally `msmtp` for email reports, `rsync` for local replication, `rclone` for cloud replication and `curl` for Slack notifications.

### From .deb Package (Debian / Ubuntu)

Download the latest `.deb` from [Releases](https://github.com/doutsis/vmbackup/releases):

```bash
sudo apt install ./vmbackup_*_all.deb
```

If you have the old standalone `vmrestore` package installed, `apt` removes it automatically — no manual intervention required.

### From Source (any distro)

```bash
git clone https://github.com/doutsis/vmbackup.git
cd vmbackup
sudo make install
```

Both methods install to `/opt/vmbackup/` and set up:
- `vmbackup` and `vmrestore` commands in PATH
- `root:backup` ownership with restricted permissions
- systemd service and timer units (`vmbackup.service`, `vmbackup.timer`)
- AppArmor profile for libvirt/QEMU integration

### Uninstall

**Debian / Ubuntu (.deb install):**

```bash
sudo apt remove vmbackup    # remove but keep config
sudo apt purge vmbackup     # remove shipped config and logs too
```

**From source (make install):**

```bash
sudo make uninstall
```

Remove keeps your configuration under `/opt/vmbackup/config/` so you can reinstall later without reconfiguring. Purge deletes the shipped config files, logs and the AppArmor profile — config instances you created yourself (e.g. `config/prod/`) are left in place. `make uninstall` goes further: it removes the whole `/opt/vmbackup/` tree, including any config instances you created. Backup data is never touched — it lives wherever you configured `BACKUP_PATH`.

## Configuration

All configuration lives in `/opt/vmbackup/config/`. Each config directory is a named instance containing:

| File | Purpose |
|------|---------|
| `vmbackup.conf` | Backup path, schedule policy, compression, VM filters |
| `email.conf` | Email reporting (SMTP via msmtp) |
| `slack.conf` | Slack notifications (webhook; disabled by default) |
| `replication_local.conf` | Local replication destinations (rsync) |
| `replication_cloud.conf` | Cloud replication destinations (rclone) |
| `vm_overrides.conf` | Per-VM rotation policy and exclusion overrides |
| `exclude_patterns.conf` | Wildcard rules to exclude VMs by name (e.g. `test-*`) |
| `fstrim_exclude.conf` | VM name patterns to exclude from pre-backup FSTRIM |

`vmrestore` reads the same instance configuration — `--config-instance prod` works the same way it does for `vmbackup`, and pulls `BACKUP_PATH` from the same `vmbackup.conf`.

The `default/` instance is used when no `--config-instance` is given. The `template/` directory contains fully documented reference configs — copy it to create a new instance:

```bash
sudo cp -r /opt/vmbackup/config/template /opt/vmbackup/config/prod
sudo vmbackup --run --config-instance prod
sudo vmrestore --list --config-instance prod
```

### VM discovery and exclusion

vmbackup discovers and backs up every VM on the host automatically. To give a specific VM a different rotation policy or exclude it entirely, add an entry to `vm_overrides.conf`. To exclude VMs by naming convention, add wildcard rules to `exclude_patterns.conf`.

### Self-healing

vmbackup validates backup state, data integrity and lock health at the start of every run. If an incremental backup fails it converts to a full and retries. If the backup sequence is broken, it archives what's there and starts fresh. If a previous run was interrupted, stale locks and partial files are cleaned up automatically.

### Usage

Once configured, vmbackup runs unattended via the systemd timer. For manual runs and operational tasks:

```bash
# Backup
sudo vmbackup --run                                  # default instance
sudo vmbackup --run --config-instance prod           # named instance
sudo vmbackup --run --dry-run                        # preview only
sudo vmbackup --run --vm web,db                      # specific VMs
sudo vmbackup --replicate-only                       # re-run replication
sudo vmbackup --prune list                           # on-demand cleanup

# Status (read-only, no locks, no session)
sudo vmbackup --status                               # today's sessions
sudo vmbackup --status --failures --days 7           # recent failures
sudo vmbackup --status --chains                      # chain health
sudo vmbackup --status --restores                    # restore history
sudo vmbackup --status --storage --csv               # storage as CSV

# Restore
sudo vmrestore --list                                # what's available
sudo vmrestore --vm web --restore-path /var/lib/libvirt/images --force # in-place
sudo vmrestore --vm web --name web-clone --restore-path /scratch/     # clone
sudo vmrestore --vm web --restore-point 7 --restore-path /scratch/    # point-in-time
sudo vmrestore --vm web --disk vda --restore-path /scratch/           # single disk
sudo vmrestore --vm web --restore-path /scratch/ --dry-run            # preview
```

In-place restores over a VM that is still defined, or whose disk files still exist, require `--force` — vmrestore refuses to overwrite silently. On a bare host `--force` is not needed. See [vmrestore.md](vmrestore.md#99-overwriting-an-existing-vm---force) for details.

All commands accept `--config-instance`; `--dry-run` applies to `--run`, `--prune`, `--replicate-only` and every vmrestore restore mode. See [vmbackup.md](vmbackup.md) and [vmrestore.md](vmrestore.md) for the full CLI reference.

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

| Policy | Behaviour |
|--------|-----------|
| `daily` | Archives existing backups when the date changes and starts a fresh full. Keeps 7 daily folders by default. |
| `weekly` | Archives existing backups at the start of a new ISO week. Keeps 4 weekly folders by default. |
| `monthly` | Archives existing backups at the start of a new month. Keeps 3 monthly folders by default. This is the default policy. |
| `accumulate` | Backups accumulate indefinitely. When the number of incremental backups hits the hard limit (default 365) they are automatically archived and a fresh full starts. |
| `never` | VM is excluded from backup entirely. |

The default rotation policy is set in `vmbackup.conf` and applies to all VMs. Individual VMs can be assigned a different policy in `vm_overrides.conf`.

### Manual cleanup

`vmbackup --prune <target>` removes archived chains, cleans up old periods or wipes a decommissioned VM. All operations support `--dry-run` to preview and `--yes` to skip confirmation. The `period:` target refuses to delete a VM's last remaining period — use `all` to explicitly remove everything. Prune also takes the same per-VM lock as backup and restore: pruning a named VM that's busy is refused (reporting the process that holds the lock), and the all-VMs sweep simply skips busy VMs. See [vmbackup.md](vmbackup.md#on-demand-cleanup---prune) for the full target reference.

## TPM & BitLocker Support

For VMs with emulated TPM (Windows BitLocker, Linux Secure Boot), vmbackup backs up TPM state from `/var/lib/libvirt/swtpm/` alongside each VM backup.

For Windows VMs with BitLocker, vmbackup uses the QEMU guest agent to extract recovery keys from the running guest automatically. The keys are stored alongside the TPM state so they're available if the TPM becomes unusable after restore.

On the restore side, vmrestore restores TPM state with correct ownership and mode isolation, pairs the per-checkpoint NVRAM with the matching disk state, and reports TPM unlock outcome honestly in the summary.

## Security

vmbackup enforces `root:backup` ownership across everything it touches — the install tree, backup data, logs and lock files. This is not configurable.

### The backup group

The `backup` group is a standard system group (GID 34 on Debian/Ubuntu, where it exists out of the box). Both the `.deb` package and `make install` create it as a system group if it doesn't already exist; on other distros it gets a distro-assigned system GID. All vmbackup files are owned `root:backup` so that root can write backups and members of the `backup` group can read them.

To browse backups, check logs or query the SQLite database, add your user to the group:

```bash
sudo usermod -aG backup myuser
# Log out and back in for group membership to take effect
```

If you also want non-root access to `virsh list` and other libvirt commands, add the `libvirt` group too:

```bash
sudo usermod -aG backup,libvirt myuser
```

### Privilege model

`vmbackup` requires root for the whole run — it needs `virsh`, qemu sockets, write access to backup data, catalogue writes and per-VM locks.

`vmrestore` is intentionally asymmetric. `--list`, `--dump` and disk extraction to a user-writable scratch path are supported as a regular user against a readable backup tree. Only the final `virsh define` and start step needs root, and that step fails with a clear libvirt error if invoked unprivileged. This supports disaster-recovery-on-a-recovery-host scenarios where you want to inspect or extract from a backup tree without privilege escalation.

### SGID and permissions

Backup directories use the SGID bit (mode `2750`). When SGID is set on a directory, every new file and subdirectory automatically inherits the `backup` group. Combined with `umask 027`, the result is files at `640` and directories at `2750` with `root:backup` ownership throughout.

| Layer | Mechanism |
|-------|-----------|
| Script | `umask 027` — files `640`, dirs `750` |
| Directories | SGID bit (`2750`) — group inheritance propagates to all new files and subdirectories |
| systemd | `UMask=0027` — belt-and-braces with the in-script umask |
| Package | `install -m 750/640` — nothing is world-accessible |
| AppArmor | Profile for libvirt/QEMU NBD socket access |

### Sensitive material

TPM private keys and BitLocker recovery keys are isolated from the backup group. The `tpm-state/` directory has SGID stripped and contents are owned `root:root` with mode `600`. A user in the `backup` group can browse the backup tree and read VM configs and logs but cannot read TPM keys or BitLocker recovery keys.

## SQLite Catalogue

All backup and restore activity is logged to a SQLite database at `$BACKUP_PATH/_state/vmbackup.db`. The database tracks sessions, per-VM results, replication runs, retention actions, backup health events and restore sessions. Both `vmbackup` and `vmrestore` read and write through the same catalogue, so the entire backup-and-restore history is queryable from one place — no parsing of log files required.

`vmbackup --status` covers eight report modes: sessions, VM history, failures, replication, chains, storage, policies and restores.

The catalogue is an **observability layer, not a restore dependency**. `vmrestore` reconstructs from the backup tree itself and falls back to walking the disk when no catalogue is present, so disaster recovery on a fresh host works with the backup data alone. Replication reflects this: it copies completed catalogue snapshots rather than the live database — see [Replication Architecture](vmbackup.md#what-is-replicated).

## Replication

Replication runs after backup completes. Local and cloud replication operate independently and can run in parallel or sequentially.

**Local replication** uses rsync to any locally accessible path — local disks, NFS mounts, virtiofs shares, pre-mounted CIFS, or anything else that appears as a local directory. Configurable bandwidth limits and post-sync verification (size or checksum).

**Cloud replication** uses rclone to sync to SharePoint, Backblaze B2, S3, or any rclone-supported backend. Currently ships with a SharePoint transport driver.

Both systems use a pluggable transport architecture — new transports are added by implementing a small function contract. See [vmbackup.md](vmbackup.md#transport-function-contract).

Replication normally runs at the end of each backup session, but `--replicate-only` lets you trigger it independently. Useful when pre-seeding a new destination, adding a destination to an existing setup, or re-running replication that was interrupted.

## Tested

vmbackup and vmrestore are tested end-to-end against a fleet of real Linux and Windows guests. The harness builds and installs the package, then drives the public CLI of both commands through a matrix of backup, restore and failure scenarios — the same paths a real operator would take.

### Proof, not just exit codes

The headline test isn't "did the command return zero" — it's "did the right bytes come back". Before each backup, the harness plants a unique witness file inside every guest. After restoring, it boots the restored clone and reads that file back through the QEMU guest agent. A point-in-time restore only passes if the guest contains exactly the content that existed at that point in the backup history — proving the restore is byte-correct, bootable and identity-correct, not merely present on disk.

### Test Fleet

| VM | Disks | TPM | Boot |
|----|-------|-----|------|
| Linux base | 1× VirtIO | No | BIOS |
| Linux multi-disk | 2× VirtIO + 1× SATA | No | BIOS |
| Windows base | 1× VirtIO | Yes | UEFI |
| Windows multi-disk | 2× VirtIO + 1× SATA | Yes | UEFI |
| Linux spaced-name | 2 disks (spaced paths) | No | BIOS |
| Windows spaced-name | 2 disks (spaced paths) | Yes | UEFI |

The spaced-name VMs — whose libvirt names *and* disk paths both contain spaces — are first-class matrix members: every standard backup and restore scenario covers them with the same rigour as the rest of the fleet, spaced clone names included.

### Scenarios

The harness covers seventeen scenarios across six categories:

- **Smoke** — online-state checks and the guest-agent quiesce path.
- **Read-only** — verify, status, list and prune against live data.
- **Clone-restore** — full restore, restore-latest from a chain, point-in-time restore to a chain midpoint, offline-mode restore, Windows TPM round-trip, multi-disk restore, and NVRAM/disk coherency under source-VM drift — each proven with the witness file.
- **Negative** — corrupt TPM surfaced honestly (disk recovered, manual-unlock notice shown) and a missing-dependency gate.
- **Invariant** — VMs with spaces in their names and disk paths, driven end-to-end through backup and restore.
- **Disaster recovery** — destructive scenarios, run opt-in: in-place restore with `--force`, bare-metal rebuild, and surgical single-disk restore.

Every restore verifies disk integrity (`qemu-img check`), identity against pre-test baselines, and successful boot via automated guest-agent polling.

## Documentation

- [vmbackup.md](vmbackup.md) — backup architecture, configuration reference, rotation, retention, replication, SQLite schema, security model.
- [vmrestore.md](vmrestore.md) — restore architecture, CLI reference, identity handling, TPM/BitLocker, NVRAM/disk coherency model.
- [CHANGELOG.md](CHANGELOG.md) — release-by-release history.

Both are installed to `/opt/vmbackup/` alongside the binaries.

## Known Issues

### Windows VMs: slow FSTRIM with VirtIO disks

QEMU's default `discard_granularity` for VirtIO block devices causes Windows to issue millions of tiny 512-byte TRIM operations instead of coalescing them. A 20 GB disk can take 10+ minutes to trim — versus 1–2 seconds with the fix applied.

Linux guests are unaffected. SATA guests also work fine.

**Fix:** Add a `discard_granularity` override (32 MiB recommended) to each VirtIO disk in the VM's libvirt XML. vmbackup detects missing overrides automatically at backup time and logs the exact XML to add.

Full details, performance benchmarks and step-by-step XML instructions: [VirtIO discard_granularity & Windows TRIM Performance](vmbackup.md#virtio-discard_granularity--windows-trim-performance).

### Large backup sets: nightly stopped at 12 hours

The shipped systemd unit allows a backup run 12 hours. That budget covers the replication upload as well as the backups, so on a large backup set — most often the night a new backup period starts, when every VM takes a full backup and the previous period's chain is re-uploaded — the run can legitimately exceed it. systemd then stops it, and the session is reported as `killed`.

Nothing is lost: the backups already written are on disk and restorable, and the next run reconciles the offsite copy. What is lost is the remainder of that night's upload, and the run reports as killed rather than as having run out of time.

**Fix:** raise the limit with a drop-in, keeping it below your backup interval so a long run always releases its lock before the next run starts:

```bash
sudo systemctl edit vmbackup.service
```
```ini
[Service]
TimeoutStartSec=23h
```

## Issues

Found a bug or have an idea? Please [open an issue](https://github.com/doutsis/vmbackup/issues). Full disclosure: I have no idea how to use GitHub, so if someone opens *anything* — good luck to us all.

## License

MIT

---

<p align="center">
  <img src="docs/vibe-coded.png" alt="100% Vibe Coded" width="300">
</p>
