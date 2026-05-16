# Changelog

All notable changes to [vmbackup](https://github.com/doutsis/vmbackup) will be documented in this file.

Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/). Versions follow [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Fixed

- **`VIRTNBD_FALSE_SUCCESS` false-positive on Windows VMs with BitLocker** — The post-backup ERROR-line scan in `perform_backup()` used a case-insensitive substring match (`grep -qi "ERROR"`), which fired on the word "ERROR" appearing inside INFO/WARN payloads. On Windows VMs the QEMU guest agent surfaces BitLocker status text containing the word during normal operation, marking otherwise-successful backups as failed and triggering retries / spurious alert noise. The check is now anchored to the timestamped severity prefix (`^[YYYY-MM-DD HH:MM:SS] ERROR `) so only real `ERROR`-level records trigger the guard, with ANSI colour escapes stripped first so coloured output still matches. Same fix applied to the diagnostic snippet extraction.

## [0.5.6] - 2026-04-26

### Changed

- **Structured exit codes** — categorised non-zero exits (config / lock / storage / VM / tool / CLI / dependency) let monitoring distinguish *why* a run failed without parsing logs. Symmetric with vmrestore.

### Fixed

- **Retention not enforced for skipped or excluded VMs** — Retention was wired only to the post-backup success path, so any VM that was skipped (`SKIP_OFFLINE_UNCHANGED_BACKUPS=true`) or excluded (`policy=never`) accumulated period directories indefinitely with no rotation. The same code path also created the period directory via `mkdir -p` *before* deciding whether the backup would run, leaving an empty stub on disk every time a VM was skipped, excluded, or failed before first write. Combined effect on production: VMs at `RETENTION_WEEKS=4` carrying 8+ weekly directories, including pure stubs that no later session would ever clean up. Retention is now invoked from the skip and exclude paths via the new `run_retention_for_unbacked_vm` wrapper, which orders stub cleanup before retention so the period count is correct before the limit check runs. Excluded VMs (`policy=never`) have stubs removed but their populated periods are preserved by the policy short-circuit. Failed-path retention remains intentionally suppressed; failed-path stubs are reaped on the next non-failed session.

### Added

- **Stub-aware retention pipeline for unbacked VMs** — A new `run_retention_for_unbacked_vm` wrapper in `modules/retention_module.sh` runs stub cleanup → retention → orphan retention in that order whenever a VM is skipped or excluded, so the on-disk period count is correct before the limit check fires. Stub cleanup is performed by the new `_remove_empty_period_dirs` helper, which removes pure stub directories (zero `*.data`, no `.archives/`) and is anchored to `BACKUP_PATH` with a path-shape regex guard, deliberately bypassing `_remove_period`'s keep-last, replication, and protected-period guards (all inappropriate for empty directories). Stub deletions in SQLite go through a new UPDATE-only library function `sqlite_mark_chain_deleted_if_exists` (in `lib/sqlite_module.sh`) to avoid injecting phantom `active`-then-`deleted` `chain_health` rows when a pure stub never had a row to begin with.

### Changed

- **`retention_events` audit attribution** — Field 12 (`triggered_by`) no longer carries hardcoded function-name literals; it now records the high-level event that drove the prune, with new enum values `skipped`, `excluded`, and `orphan_dir` joining the existing `post_backup`, `prune`, and `orphan_retention`. The `action` column also gains `remove_stub` for the new stub-cleanup path. Internally, `_remove_period`, `_remove_orphan_period`, `_remove_archive_chain`, `_remove_archives_in_period`, `_remove_vm_all`, `run_retention_for_vm`, and `run_orphan_retention_for_vm` all gain a new `trigger` parameter so the originating event propagates through the call chain into the audit row — making it possible to attribute retention activity to skipped-VM and excluded-VM sessions for the first time.

## [0.5.5] - 2026-04-25

### Added

- **Configurable backup-destination space thresholds** — Four new optional `vmbackup.conf` settings (`DISK_ABORT_PCT`, `DISK_WARN_PCT`, `DISK_ABORT_GB`, `DISK_WARN_GB`) let `check_disk_space()` be tuned per instance. Percent and absolute thresholds are evaluated together so either can fire independently; setting any threshold to `0` disables it. Defaults (20%/30% and 10 GB/50 GB) preserve previous behaviour.
- **Disk-space snapshot per session (schema v2.1)** — `sessions` table gains `disk_free_bytes` and `disk_total_bytes` columns, populated by `sqlite_session_end()` from a `df` capture against `BACKUP_PATH`. Migration from v2.0 is automatic, idempotent and additive.
- **`--status` reporting command** — Seven report modes: sessions, VM history, failures, replication, chains, storage, policies. Terminal tables by default, `--csv` for export, `--days N` for time window, `--all-instances` to span every config instance. Sessions output is job-type-aware (backup / prune / replicate-only / mixed) and scoped to the active `CONFIG_INSTANCE` by default. The storage report includes per-VM size trends and a destination-growth projection that names the configured `DISK_ABORT_PCT` threshold.
- **Post-upgrade config advisory in `postinst`** — On dpkg upgrade, lists `.dpkg-dist` files awaiting merge with per-file `diff -u` commands and points custom config instances at `config/template/vmbackup.conf` for new variables. Visible only on upgrade.
- **`vmbackup --config-prune-removed`** — Cleanup helper that comments out configuration variables removed in the running release. Idempotent; supports `--dry-run`. Operates on `default/` and all custom instances; skips `template/`. Per-name allowlist keyed to release version, designed to be extended by future config-pruning ENHs.

### Fixed

- **Pre-flight aborts failed silently with no email** — `check_backup_destination()`, `check_scratch_path()` and `check_disk_space()` exit before `main()` reaches its normal email send, so destination/scratch/space failures left no notification (only a journal entry). `cleanup_on_exit()` now sends a failure report on any non-zero exit once a SQLite session has been registered, gated by `_EMAIL_SENT` so the existing success/failure path remains the single source of truth on normal runs.
- **`SKIP_OFFLINE_UNCHANGED_BACKUPS` is now honoured** — Previously the variable was defined and validated but never read; offline-unchanged VMs were always skipped regardless of the setting. The change-detection call in `backup_vm()` is now gated by this flag.

### Removed

- **`OFFLINE_CHANGE_DETECTION_THRESHOLD`** — Was never read by the change-detection code (which uses strict `mtime > last_backup`). The variable inverted the safe default and would have introduced false negatives if implemented. Existing values in operator configs are inert; run `vmbackup --config-prune-removed` to clean them up.
- **`EMAIL_INCLUDE_REPLICATION`** — Was never read. Hiding replication results from the email is operator-hostile (silent on success, dangerous on failure). The empty-section logic already handles the no-replication case.
- **`EMAIL_INCLUDE_DISK_SPACE`** — Was never read; gated a section that was never built. A real disk-usage email section is tracked as ENH-16.

## [0.5.4] - 2026-04-12

### Fixed

- **Session PID lock race condition** — Replaced non-atomic check-then-write PID lock with the `noclobber` pattern (already proven in `lib/vm_lock.sh`). Prevents duplicate concurrent sessions when two vmbackup invocations start simultaneously.
- **Reorder config-instance validation before session lock** — `--config-instance nonexistent` now fails immediately at startup instead of blocking on the global session lock while another backup is running. Config existence is validated during early config load, before `main()` and lock acquisition.
- **Double email on SIGTERM** — Added `_EMAIL_SENT` guard flag to prevent duplicate email reports when SIGTERM arrives after normal email send but before process exits.
- **virtnbdbackup not confirmed dead before retry** — Added `pgrep`/`pkill` cleanup and `virsh domjobabort` before retry to ensure orphaned virtnbdbackup children and lingering libvirt backup jobs are terminated. Prevents data corruption from concurrent backup processes on the same VM.
- **SQLite session not finalised on normal exit** — `sqlite_session_end()` was only called for SIGINT/SIGTERM exits, not for early errors or unexpected exit paths. Sessions could be left permanently "in progress" in the database. Now finalised unconditionally in `cleanup_on_exit()` (idempotency guard prevents double-close).
- **Silent permission failures on backup path** — `chown`/`chmod` failures in `ensure_backup_path_sgid()` and `set_backup_permissions()` were suppressed with `|| true`. Now logs warnings so filesystem permission issues are visible instead of silently degrading backup security.
- **Stale lock cleanup could delete active locks** — `cleanup_on_exit()` deleted any `*.lock` file older than 1 hour without checking whether the owning process was still running. A backup taking >1 hour could have its lock deleted by a concurrent session exiting. Now validates PID liveness before deletion and uses the correct `vmbackup-*.lock` glob.

## [0.5.3] - 2026-04-10

### Added

- **`--run` flag required to start backups** — Running `vmbackup.sh` with no arguments now prints a short usage summary instead of starting a backup. Use `--run` to start a backup session: `sudo vmbackup --run`. All mode operations are now explicit: `--run`, `--prune`, `--replicate-only`, `--cancel-replication`.
- **Unknown flag detection** — Unrecognized flags now produce an error and exit instead of being silently ignored.
- **`--cancel-replication` conflict guards** — `--cancel-replication` now rejects combinations with `--prune`, `--replicate-only`, `--vm`, and `--dry-run`.
- **Root privilege check** — Running vmbackup without root now prints a clear error (`vmbackup must be run as root`) and exits 1 instead of failing with cryptic permission errors. `--help` and `--version` still work without root.
- **`--vm` targeted backup mode** — Back up specific VMs without running a full session. Accepts a single VM (`--vm web`) or comma-separated list (`--vm web,db,mail`). Composable with `--dry-run` and `--config-instance`.
- **Global session lock** — vmbackup now creates `$STATE_DIR/vmbackup.pid` at startup and checks for an existing running session. Prevents concurrent vmbackup invocations. Cleaned up on normal exit, SIGINT, SIGTERM, and SIGTSTP. Stale PID files from crashed processes are detected via PID liveness check and removed automatically.
- **`session_type` column in SQLite sessions table** — Tracks the type of each backup session (`standard`, `targeted`, `replicate_only`, `prune`). Schema migrated from v1.9 to v2.0.

### Changed

- **`--vm` requires explicit mode** — `--vm` is now a modifier, not a standalone command. Use `--run --vm web` for targeted backups or `--prune list --vm web` for pruning. Standalone `--vm` exits with an error.
- **`--help` output restructured** — Sections reordered: GENERAL first, then BACKUP, PRUNE, REPLICATE-ONLY, SIGNAL, CONFIG INSTANCES, EXAMPLES. Added destructive warning to `--prune all` example. Added documentation link.
- **Systemd service updated** — `ExecStart` now includes `--run` flag.
- **`SKIP_OFFLINE_UNCHANGED_BACKUPS` default changed to `true`** — Offline VMs whose disks haven't changed since the last backup are now skipped by default. Previously defaulted to `false` (always backup). Existing installations with the setting explicitly configured are unaffected.
- **`--vm` no longer prune-only** — `--vm` previously only worked with `--prune`. Now also works in backup mode. Internal variable renamed from `_PRUNE_VM` to `_TARGET_VM`.
- **`--replicate-only` sessions now write `status='success'`** — Previously wrote `status='replication_only'`. Session type is now tracked via the new `session_type` column instead of overloading the status field.
- **Documentation rewritten** — `vmbackup.md` condensed from ~4,500 to ~2,900 lines. Added CLI reference section. Removed function reference and schema reference (use `grep` and `.schema` instead). Replaced diagrams with prose. Condensed configuration, replication, BitLocker, and cleanup sections. Fixed broken cross-references.
- **`fstrim_exclude.conf` comments improved** — Rewritten with categorised exclusion reasons and practical examples.

### Removed

- **Host Configuration Backup** — Removed `backup_host_config()` function and `Host Configuration Backup` documentation section. This feature backed up libvirt/QEMU/network configuration to `$BACKUP_PATH/__HOST_CONFIG__/`.

### Fixed

- **`RETENTION_ORPHAN_DRY_RUN` config setting ignored** — `post_backup_hook()` in `vmbackup_integration.sh` passed a hardcoded `"false"` as the dry_run argument to `run_orphan_retention_for_vm()` and `run_retention_for_vm()`. This always overrode the `RETENTION_ORPHAN_DRY_RUN` config variable, so setting it to `true` had no effect — orphans were still deleted. Fixed: removed the hardcoded argument so both functions fall through to their config variable defaults.

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
