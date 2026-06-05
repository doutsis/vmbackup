# Changelog

All notable changes to [vmbackup](https://github.com/doutsis/vmbackup) will be documented in this file.

Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/). Versions follow [Semantic Versioning](https://semver.org/).

## [0.6.0] - 2026-06-05

**Unification release.** `vmbackup` and `vmrestore` now ship from one source tree as one Debian package containing two binaries. Both binaries always carry the same version, share a single `lib/` of cross-tool helpers, and read from a single SQLite catalogue. Existing flags, invocations, systemd units, and operator scripts continue to work unchanged. The old standalone `vmrestore` package is replaced cleanly on upgrade.

### Added

- **Unified package** — `vmbackup` and `vmrestore` ship from a single source tree, build via one `Makefile`, and install from one `.deb`. The package declares `Provides: vmrestore`, `Replaces: vmrestore (<< 0.6.0)`, and `Conflicts: vmrestore (<< 0.6.0)`, so `apt` removes the old standalone `vmrestore` package automatically on upgrade. The `vmrestore` binary continues to live at `/usr/local/bin/vmrestore` (symlink to `/opt/vmbackup/vmrestore.sh`).
- **Shared `lib/` consumed by both binaries** — 16 libraries now provide one canonical implementation of behaviour that was previously duplicated or divergent across the two tools: logging, exit codes, versioning, per-VM locking, signal traps, config-instance resolution, period handling, the backup-tree walker, the read-only SQLite reader, path and VM-name helpers, TPM artefact reading, and the `virtnbdbackup` / `virtnbdrestore` and `virsh` wrappers. Where a behaviour exists in both binaries it now comes from exactly one place, so they can no longer disagree by accident.
- **`vmrestore` is catalogue-aware** — `vmrestore --list` reads the same SQLite catalogue that drives `vmbackup --status --chains` and appends a per-VM `Chains: <N> active, <N> broken, last backup <ISO>` line. Falls back to walker-only output when the catalogue is unavailable, preserving the standalone DR contract.
- **`vmrestore` writes restore-session rows to the catalogue** — schema bumped to v2.2 with a new `restore_sessions` table. Every invocation records start, end, VM, restore type, and final outcome. A new `vmbackup --status --restores` subcommand reads the table, so backup and restore history live in one place. Catalogue failures degrade to a single `WARN` and never block the restore; `--dry-run` writes no row.
- **`vmrestore` gains per-VM locking and signal handlers** — restoring a VM now takes the same lock a backup of that VM takes, so backup and restore can no longer race each other. SIGINT and SIGTERM clean up staging directories and partial disk files.
- **`vmrestore --restore-path` overlap guard** — refuses any path that equals or sits inside a configured `vmbackup` `BACKUP_PATH` (checked across all config instances), preventing accidental restores into the live backup tree.
- **Broken-chain detector for `vmrestore`** — incomplete chains (truncated by an interrupted backup, or partially archived) are no longer offered as the default `latest` restore target. The reason for skipping is logged so the operator can override with `--include-incomplete` (forensic use only).
- **In-session re-entry guard for chain archival** — `vmbackup` refuses to archive the same VM twice within one invocation, eliminating collision-suffixed `.archives/chain-<date>.1` directories.
- **Misplaced-database guard** — `vmbackup` refuses to create the SQLite catalogue inside `.archives/` or under a period directory, closing a class of bugs where a misconfigured backup path could spawn a second catalogue that silently diverged from the canonical one.
- **`vmbackup --cleanup-stale-manifests`** — one-shot subcommand that removes leftover per-VM `chain-manifest.json` files from the backup tree. Invoked automatically by `debian/postinst` on package upgrade and safe to re-run manually.

### Changed

- **`chain_health.archive_size_bytes` populated at archive transition** — the retention-path archive caller now writes the archive size immediately, matching the active-path caller. Previously the column stayed at 0 until manual reconciliation.
- **TPM-restore reporting is now truthful** — when disks restore successfully but TPM/BitLocker unlock fails, `vmrestore` no longer reports overall success. The summary line carries a `TPM ✓` / `TPM ✗ (manual unlock required)` token (omitted on VMs without TPM).
- **SharePoint replication verify logs actionable diagnostics on mismatch** — when the post-upload `rclone check` reports a difference, the cloud transport now logs the `rclone check` exit code, elapsed time, and the specific differing/missing files, replacing the previous opaque `Found differences` message. Transient SharePoint verify warnings are now diagnosable instead of mysterious.

### Fixed

- **False-positive backup failures from substring `ERROR` matches in the `virtnbdbackup` log** — the post-run guard used a case-insensitive substring match for `ERROR`, which mis-flagged successful runs whenever the log mentioned `internal error`, `ERROR — trim not supported`, or carried ANSI colour codes. False positives recorded the chain as failed and promoted the next monthly backup from incremental to full, inflating destination write volume. Now anchored to `virtnbdbackup`'s own log-line format and ANSI-stripped. Originally reported and proposed by **@hostarts** with co-author **@houssamchergui**.
- **Email notifier "intentionally skipped" return value logged as a delivery failure** — all four `send_backup_report` call sites collapsed the notifier's three return values (delivered / transport failure / intentionally skipped) into pass-or-fail, so operators who set `EMAIL_ON_SUCCESS=no` saw a misleading "Failed to send email report" WARN on every successful run. A new `_handle_notifier_rc` dispatcher distinguishes the three cases and is wired into all four call sites. Originally proposed by **@hostarts** (email-only scope adopted).
- **`get_last_backup_timestamp()` blind to archived chains** — the probe's `find -maxdepth` was too shallow to see archived data after the chain-archive layout change, so offline-unchanged VMs were treated as having no prior backup and re-ran a full backup nightly. Probe depth corrected; the offline-skip path now fires as intended.
- **False "incomplete backup" WARN on clean shutdown** — `cleanup_on_exit` emitted a misleading WARN on every clean exit because its duplicate-call gate was an in-memory flag the success path could not clear before the trap fired. The gate now uses the `sqlite_session_end()` return code itself. Independently reported by **@hostarts** in PR #4.
- **TPM artefact validation accepted empty bundles** — `validate_tpm_backup()` was `-s`-testing the `tpm2/` directory instead of its files, so an empty TPM bundle passed validation. Replaced with a per-file size check and a minimum-size floor on `tpm2-00.permall`.
- **`xmllint` listed as required but never invoked** — phantom dependency removed.
- **Dead `restore_vm_tpm()` body removed** — had different semantics from `vmrestore`'s `restore_tpm()` and would have corrupted a recovery if ever called. Already marked `# DEAD CODE`; now physically gone.
- **Undefined VM-name sanitisation helper in prune paths** — `vmbackup`'s prune code paths called a helper that had never been defined, so the call was a silent no-op. Replaced by the canonical helper in `lib/vm_name_utils.sh`.
- **NVRAM/disk coherency on restore (`BdsDxe: No mapping` boot failure)** — `vmrestore` paired restored disks with the *live* host NVRAM instead of the NVRAM captured at the backed-up checkpoint, so restoring an older period over a VM that had since rebooted left UEFI variables (SecureBoot keys, `BootOrder`, MOK) out of step with the disks and the guest failed to boot. It now pairs each restore with the matching checkpoint's NVRAM — clones and in-place alike — backing up the live NVRAM to `<path>.before-restore.<timestamp>` first.
- **`chain_check_complete` false-positive on chains containing CD-ROM devices** — the completeness check treated every `<disk>` in the libvirt checkpoint XML as a data disk, but that XML carries no `device=` attribute, so CD-ROMs (which `virtnbdbackup` correctly skips) were indistinguishable from genuinely missing disks — flagging healthy chains `⚠ INCOMPLETE` in `--list-restore-points` and refusing them without `--include-incomplete`. The check now consults the per-checkpoint domain XML snapshot, which preserves `device='cdrom'`, and skips those phantom targets. Chains without that snapshot keep prior behaviour.
- **Stale `chain_id` recorded on SIGTERM / SIGINT** — interrupted backups wrote a `chain_health` row whose `chain_id` was derived from an in-memory index that had never been committed to disk, so the interrupted-chain entry could not be correlated with anything retention or restore could see. The id is now derived from the on-disk chain layout, so the row matches the chain that actually exists.
- **`vmrestore` skipped valid restore points on large backup trees (SIGPIPE under `pipefail`)** — the chain-presence probe used `find … | grep -q .`; with `set -o pipefail` now globally enabled, `grep -q` closed the pipe on the first match and the resulting SIGPIPE made `find` exit non-zero, so `has_backup_data()` wrongly returned false. Rewritten to `find … -print -quit`. The same pipefail-vulnerable idiom was audited and fixed everywhere it occurred (across `vmbackup.sh`, `lib/chain_validation.sh`, and an integration test).
- **`_state/logs/` rotation never ran; central logs grew unbounded** — the rotation routine was gated behind a directory that no code path ever created, so it had been dead since the early-2026 modular refactor: per-VM, replication, and email logs accumulated indefinitely, and `vmbackup.log` / `vmprune.log` grew append-only forever. Rotation now runs at most once per calendar day from the pre-backup hook, and the central logs are size-capped by a new `LOG_MAX_BYTES` knob (default 50 MiB) — an oversized file is rolled to `<name>.<epoch>` and aged out under the existing `LOG_KEEP_DAYS` rule. Deployed installs inherit the default with no config change; the first post-upgrade session clears the accumulated backlog.

### Removed

- **Standalone `vmrestore` Debian package** — `vmrestore` now ships from the `vmbackup` package via `Provides: vmrestore`. Upgrade is automatic via `apt`; previously published `vmrestore_0.5.x_all.deb` artefacts remain reachable on prior GitHub releases.
- **Per-VM chain-manifest subsystem** — the `chain-manifest.json` index and the 678-line module that maintained it have been removed. Its rebuild logic predated the current on-disk layout and matched no files, so every retention or prune pass wiped the manifest to empty and the post-backup write repopulated it from that empty state. The remaining callers now derive chain identity, restore-point counts, and archive transitions from the on-disk layout and the SQLite catalogue, which is now the single source of truth for chain state. Leftover manifest files are reaped automatically on upgrade by the new `--cleanup-stale-manifests` subcommand.
  - **Behavioural change:** `restore_point_id` shape for new rows changes from `<vm>:<period>:chain-YYYYMMDD-HHMMSS:N` to `<vm>:<period>:chain-YYYY-MM-DD:N`, aligning with the archive directory naming. Operator-visible in `vmbackup --status` only; no code parses the id.

### Notes on the unification

- The CLI surface of both binaries is unchanged. Every flag, invocation, systemd unit, alias, monitoring rule, and operator runbook from `0.5.6` (vmbackup) and `0.5.4` (vmrestore) continues to work.
- Configuration files are unchanged. Existing `/opt/vmbackup/config/` instances continue to load.
- The database schema is migrated automatically from any v1.x or v2.x version to v2.2; downgrade is not supported (loud failure, not silent corruption).
- `vmrestore` still works in true disaster-recovery situations where the catalogue is missing or unreadable. It checks for the catalogue first; if it's not available, it logs a single `WARN` and falls back to reading the backup files directly from disk.

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
- **`--vm` without `--prune` silently ignored** — Passing `--vm my-vm` to a normal backup run had no effect and no feedback. Now prints a warning: `--vm has no effect without --prune (ignored)`.
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

---

## Pre-unification vmrestore history

# Changelog

All notable changes to [vmrestore](https://github.com/doutsis/vmrestore) will be documented in this file.

Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/). Versions follow [Semantic Versioning](https://semver.org/).

## [0.5.4] - 2026-04-26

### Changed

- **Structured exit codes** — categorised non-zero exits (config / storage / VM / tool / CLI / dependency) let monitoring distinguish *why* a run failed without parsing logs. Symmetric with vmbackup.

## [0.5.3] - 2026-04-12

### Fixed

- **`--help` showed wrong version** — `usage()` hardcoded `v0.5.1` while `--version` showed `0.5.2`. Version string now uses `$VERSION` variable.

### Removed

- **`--host-config` removed** — Host config backup was removed in vmbackup v0.5.3. The `--host-config` flag, `restore_host_config()` function, and `__HOST_CONFIG__` display in `--list` output have been stripped entirely. The `--host-target` flag is also removed.

### Added

- **`--config-instance` flag** — Select a named vmbackup config instance (e.g., `--config-instance prod`). Falls back to `VMBACKUP_INSTANCE` environment variable, then `default`. Exits with an error if the specified instance does not exist.

## [0.5.2] - 2026-03-29

### Changed

- **Per-checkpoint disk column in `--list-restore-points`** — Each restore point row now shows a `Disk(s)` column listing the disks backed up at that specific checkpoint. Replaces the previous top-level `Disks:` summary that showed the union across all checkpoints — which was misleading when disks were added or removed mid-chain.

- **`--list` disk tag reflects latest checkpoint** — The `[vda, vdb, ...]` disk tag in `--list` output now shows the disk set from the latest checkpoint only, matching the VM's current configuration. Previously showed the union across all checkpoints, which could include disks no longer attached to the VM.

### Fixed

- **Point-in-time restore lost disks when VM configuration changed mid-chain** — `virtnbdrestore` always read the latest `vmconfig`, silently dropping disks that existed at the target checkpoint but not at the latest (e.g. a 3-disk VM at checkpoint 0 becomes 2 disks at checkpoint 3 — restoring to checkpoint 0 would only restore 2 disks). vmrestore now detects disk configuration changes and creates a lightweight staging directory (symlinks to `.data` files + the correct checkpoint's `vmconfig`) so `virtnbdrestore` sees only the right configuration. `predict_output_files()` also used the latest config, so clone staging would silently discard restored disks that weren't predicted — a data loss scenario. It now accepts a config override when PIT staging detects a change. Works across all restore modes: DR, clone, disk restore, and dry-run. Applies to both active and archived chains.

- **`--disk` fell through to full-VM restore on single-disk VMs** — When `--disk` was used on a VM with only one disk, vmrestore counted the available disks and, finding only one, silently switched to full-VM restore mode — undefining the VM from libvirt and attempting a DR reconstruct. Single-disk VMs now follow the same disk-restore code path as multi-disk VMs: in-place disk replacement with `.pre-restore` backup, no VM definition changes, no UUID/MAC changes. `--disk` without `--restore-point` now also fails with a clear error when the requested disk doesn't exist at the latest checkpoint, showing which checkpoint it was last seen at (previously fell through to `virtnbdrestore` with an opaque error).

## [0.5.1] - 2026-03-22

### Added

- **Multi-disk `--disk` support** — `--disk vda,vdb,sda` restores multiple disks in one pass. `--disk all` restores every disk. Each disk gets its own `.pre-restore` backup. Refuses if any `.pre-restore` file already exists.
- **`.pre-restore` overwrite protection** — if a `.pre-restore` backup file already exists for a disk, vmrestore refuses to proceed rather than silently overwriting. Prevents accidental loss of the safety net.
- **`--list-restore-points` shows all periods** — previously only showed the current (latest) retention policy period. Now iterates all period directories (newest first), each with its own section header, restore points, and archived chains.
- **`--list-restore-points` shows archived chain restore points** — archived chains are expanded inline showing their restore points, so users can see exactly what's available for `--restore-point` without manually inspecting `.archives/` directories.
- **`--list-restore-points` accepts full paths** — `--list-restore-points /path/to/.archives/chain-2026-03-04` now works like `--vm` does (splits into basename + dirname). Previously only accepted a VM name.
- **`--version` / `-V` flag** — prints version and exits.

### Changed

- **`--list` redesign** — VM name and size on own line (no column overflow for long names). Restore points summed across all periods (not just one). Type detected from the most recently modified period. Proper pluralisation ("1 point", "8 archives"). Multi-disk VMs show `[sda, vda, vdb]` disk tags. Archive count hidden when zero.
- **`Restore Point` column replaces `Checkpoint`** — the `--list-restore-points` output now shows a `Restore Point` column header with just the number (matching `--restore-point N`), date, and type. Internal checkpoint names (`virtnbdbackup.N`) removed — users don't interact with them.

### Fixed

- **Empty period skip** — `--list` and restore operations now skip empty period directories (created by rotation before the first backup runs). Previously, an empty newest period caused `--list` to show "unknown" type and restores to fail with "No backup data files".
- **Restore point numeric sort fix** — chains with 10+ restore points now display in correct numeric order. Previously used lexicographic glob ordering (0, 1, 10, 11, ..., 2) instead of (0, 1, 2, ..., 10, 11).

## [0.5] - 2026-03-14

### Added

- **`--disk` single-disk restore mode** — restore or replace a single disk from a multi-disk backup without touching the VM definition or other disks. Supports in-place replacement (`--disk vda`) and staging extract (`--disk vda --restore-path /tmp/extract`).
- **`.pre-restore` safety backup** — before in-place disk replacement, the original disk image is renamed to `.pre-restore` so the previous state is recoverable.
- **`--no-pre-restore` flag** — skip the `.pre-restore` safety backup when disk space is tight.

## [0.4] - 2026-03-10

### Added

- **Multi-disk VM support** — `enumerate_disks()` discovers all disks in a backup. Restore handles VMs with multiple virtual disks (vda, vdb, sda, etc.).
- **Pre-disk-restore baseline** — foundation for the `--disk` mode added in v0.5.
- **Test suite** — tests 1–12 covering DR, clone, point-in-time, verify, host-config, dry-run.
- **Packaging** — Makefile and debian/ packaging for `.deb` builds.

### Fixed

- **Bug fixes** — storage pool refresh fix, logging improvements, pre-flight safety check refinements.

## [0.3] - 2026-03-06

### Added

- **Disk collision protection** — predicts output file paths before restore and checks for conflicts. If target disk images already exist, restore is blocked with a clear error.
- **Staging directory** — clone restores write to a temporary staging directory, then move disks to the final location, preventing partial overwrites on failure.
- **Skip-config** — `--skip-config` skips VM definition and TPM restore (disk-only extract).
- **Post-restore integrity** — `qemu-img check` runs automatically after restore completes.
- **NVRAM isolation** — clone gets its own UEFI firmware state file.

## [0.2] - 2026-03-02

### Added

- **Clone mode (`--name`)** — creates an independent copy with new UUID, new MACs, and a new name. One flag transforms a DR restore into a clone.
- **TPM/BitLocker restore** — swtpm state directory is recreated at the correct UUID path for both DR and clone restores.
- **New-identity define** — strips UUID and MACs from domain XML, renames, defines as a new VM.

## [0.1] - 2026-02-20

### Added

- **Initial release** — wraps virtnbdrestore for single-command disaster recovery. Automatic backup type detection, period resolution, point-in-time restore via `--restore-point` and `--period`.
