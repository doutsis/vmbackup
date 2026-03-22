# Changelog

All notable changes to [vmbackup](https://github.com/doutsis/vmbackup) will be documented in this file.

Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/). Versions follow [Semantic Versioning](https://semver.org/).

## [0.5.3] - Unreleased

### Added

- **Root privilege check** — Running vmbackup without root now prints a clear error (`vmbackup must be run as root`) and exits 1 instead of failing with cryptic permission errors. `--help` and `--version` still work without root.

### Changed

- **`SKIP_OFFLINE_UNCHANGED_BACKUPS` default changed to `true`** — Offline VMs whose disks haven't changed since the last backup are now skipped by default. Previously defaulted to `false` (always backup). Existing installations with the setting explicitly configured are unaffected.

## [0.5.2] - 2026-03-22

### Added

- **`--replicate-only` mode** — Run replication without performing backups, retention, or FSTRIM. Accepts an optional scope argument: `local`, `cloud`, or `both` (default). Respects `REPLICATION_ORDER` setting (simultaneous/local_first/cloud_first), `--cancel-replication` flag, `--dry-run`, and `--config-instance`. Mutual exclusivity guards prevent combining with `--prune` or `--vm`. Dedicated summary box (`REPLICATION-ONLY SESSION SUMMARY`), distinct email subject (`Replication Only — hostname — OK/FAILED`), simplified email body without VM table, and `status=replication_only` in SQLite sessions. Skips dependency check (virsh/virtnbdbackup not needed) and qemu-nbd cleanup.
- **`--prune` mode** — Standalone on-demand cleanup of backup data without running a backup session. Supports composable targets: `archives` (all archived chains), `archives:<period>` (archives in one period), `chain:<name>` (single archived chain), `period:<period>` (entire period directory), `all` (entire VM). Combines with `--vm`, `--dry-run`, `--yes`, and `--config-instance`. Includes `--prune list` discovery view showing per-VM/period/chain sizes with copy-paste prune commands. All operations log to `vmprune.log` and record audit rows in `chain_events`, `period_events`, `retention_events`, and `file_operations`. Safety guards: keep-last period protection, confirmation prompt (bypass with `--yes`), `_is_safe_to_remove()` validation.
- **`FSTRIM_MINIMUM` config variable** — Minimum contiguous free range (bytes) to pass to `guest-fstrim` on Linux guests. Default `1048576` (1 MB). Skips small free ranges for a significant speedup on fragmented filesystems. Windows guests ignore this parameter (the QEMU agent calls `defrag.exe /L` which has no minimum concept).
- **`fstrim_exclude.conf`** — Pattern-based VM exclusion from FSTRIM. One glob pattern per line (e.g. `*-clone`, `test-*`). Loaded from the active config directory. Template and default files provided.
- **`check_discard_granularity()` advisory** — Before FSTRIM on Windows VMs, parses `virsh dumpxml` to detect VirtIO disks missing the `discard_granularity` override in `<qemu:override>`. Logs a warning with the exact XML fix needed for each affected disk. Advisory only — does not block backup. Runs once per VM per session (cached). Uses POSIX-compatible awk (mawk safe).
- **Per-path FSTRIM logging** — `_fstrim_parse_results()` parses the per-filesystem JSON response from `guest-fstrim`. Logs each mount point with human-readable trimmed size (GB/MB/KB). Detects per-path errors. Logs a summary line with total filesystems, total trimmed, and error count. Windows paths report completion without byte counts (agent limitation).

### Changed

- **FSTRIM module rewritten** — `fstrim_optimization_module.sh` rewritten from 73 to 236 lines. `execute_fstrim_in_guest()` no longer re-checks agent availability or re-detects OS internally — both handled once by the caller via `check_qemu_agent()` / `detect_guest_os()`. Reduces per-VM agent round-trips from 4 to 2 (ping + fstrim). New internal functions: `_fstrim_is_vm_excluded()` (pattern-based exclusion), `_fstrim_parse_results()` (per-path JSON parsing with human-readable sizes). Builds the `guest-fstrim` JSON command with `minimum` parameter for Linux (omitted for Windows). Times each operation with epoch seconds. Sets `FSTRIM_LAST_DURATION`, `FSTRIM_LAST_BYTES_TRIMMED`, `FSTRIM_LAST_STATUS`, and `FSTRIM_LAST_OUTPUT` globals for future SQLite integration. Dead-code wrapper `apply_fstrim_optimization()` removed. Full raw response logged at debug level.
- **`ENABLE_FSTRIM` default changed to `true`** — FSTRIM is now enabled by default for new installations. Existing installations with `ENABLE_FSTRIM` explicitly set in config are unaffected.
- **`MAX_RETRIES` default changed to `3`** — Increased from 2 to give transient errors (e.g. agent timeout, NFS hiccup) one more chance before marking the VM as failed.
- **`ENABLE_AUTO_RECOVERY_ON_CHECKPOINT_CORRUPTION` default changed to `"yes"`** — Previously defaulted to `"warn"` (log but don't act). Now defaults to `"yes"` (automatically remediate broken checkpoint chains). Existing installations with the setting explicitly configured are unaffected.

### Fixed

- **`--prune` without target silently no-ops** — Running `--prune` with no target argument exited 0 without doing anything. Now prints an error with valid targets and exits 1.
- **`--prune` with unknown target silently no-ops** — Running `--prune banana` was silently ignored. Now prints an error with the valid target list and exits 1.
- **`--prune period/chain/all` without `--vm` gives confusing error** — These targets require a VM name but the error was a generic path-not-found deep in the execution. Now validates early with a clear message: `--prune <target> requires --vm NAME`.
- **`--vm` without `--prune` silently ignored** — Passing `--vm dev-win11` to a normal backup run had no effect and no feedback. Now prints a warning: `--vm has no effect without --prune (ignored)`.
- **Accumulate-policy `period_id` mismatch** — `get_period_id("accumulate")` returned an empty string, causing `chain_health` rows to be keyed on `period_id=""`. The archive path derivation in `archive_existing_checkpoint_chain()` used `basename(dirname(archive))` which produced the VM name instead of `""`, so `sqlite_archive_chain()` matched zero rows and the archive went unrecorded. Fixed: `get_period_id("accumulate")` now returns `"accumulate"`, and the archive path derivation uses `get_period_id()` directly with a `basename` fallback.
- **virtnbdbackup false-success detection** — `perform_backup()` trusted the exit code alone. virtnbdbackup sometimes exits 0 despite logging ERROR lines (e.g., target directory conflicts, bitmap issues, extent read failures). Added post-completion log scan: any ERROR in the captured output now triggers `VIRTNBD_FALSE_SUCCESS` failure and aborts the backup.
- **Email duration wrong during DST fall-back** — `session_start_time` and `session_end_time` were formatted without a timezone suffix (`%Z`), making `date -d` epoch conversion ambiguous during DST transitions. Added `%Z` to all three capture sites (main, SIGTERM handler, session end) so the email module's duration calculation is unambiguous.
- **TPM incremental/consistent backup methods missing `log_file_operation`** — `backup_vm_tpm_incremental()` and `backup_vm_tpm_consistent()` completed without recording file operations to SQLite. Added `log_file_operation` calls to both success paths.
- **Standalone log fallbacks missing timezone** — `log_tpm()` in `tpm_backup_module.sh` and `cloud_log()` in `replication_cloud_module.sh` used `%Y-%m-%d %H:%M:%S` without `%Z` in their standalone fallback paths. Added `%Z` for consistency with the main logging system.
- **Mirror-mode replication blocked by incorrect space check** — `_check_destination_space()` compared `free_bytes` against the full `source_size` (total backup tree). For mirror/rsync `--delete` syncs the destination already holds a previous copy, so only the delta needs free space. The check now subtracts the existing destination size from `source_size` for mirror mode, passing the effective delta as the required space. Accumulate mode is unchanged (full source size is correctly required).
- **Replication skip/fail reason missing from session summary** — `get_replication_summary()` (local) and `get_cloud_replication_summary()` (cloud) showed status icons for skipped/disabled destinations but dropped the error message. The console/log summary now includes the reason (e.g., "Insufficient space") matching what the email and database already report.
- **`chain_health.total_checkpoints` wrong after manifest rebuild** — `post_backup_hook()` passed manifest-based `count_period_restore_points()` to `sqlite_update_chain_health()`. After `rebuild_chain_manifest()` (e.g. during prune), the manifest only tracked post-rebuild checkpoints, reporting 2–3 instead of 7–15. Fixed: uses `get_restore_point_count()` (disk-based) for the chain health update. Manifest-based index retained for restore point ID generation.
- **Prune mode creates orphan DB sessions** — `--prune` dispatch called `sqlite_session_start()` but exited without `sqlite_session_end()`. Each prune invocation left an unclosed session row. Fixed: prune dispatch now closes the session before exit.
- **Duplicate lock release for last VM** — The bash RETURN trap set inside `backup_vm()` leaked to `main()`, causing `remove_lock` to fire a second time for the last VM when `main()` returned. Fixed: `trap - RETURN` in `main()` after each `backup_vm` call.
- **Unused `bytes_transferred` column in `replication_vms`** — Column was defined in schema but never populated by `sqlite_log_replication_vm()` or `sqlite_log_replication_vms()`, always returning 0. Dropped from schema. Migration 1.8→1.9 removes it from existing databases.
- **Cloud replication timestamps in UTC** — `run_cloud_replication_batch()` in `replication_cloud_module.sh` captured start/end times with `date -u` (UTC, no timezone suffix), producing log lines like `Started: 2026-03-19 14:09:20` while the log envelope showed AEDT. Changed to `date '+%Y-%m-%d %H:%M:%S %Z'` (local time with timezone suffix), matching the rest of the logging system.
- **FSTRIM failure diagnostic logging** — `execute_fstrim_in_guest()` discarded the agent error output on failure, logging only the exit code. Now logs the full error response at debug level for easier diagnosis of guest agent issues.

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
