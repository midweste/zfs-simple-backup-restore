# zfs-simple-backup-restore

Simple, atomic ZFS backup & restore tooling for file level zfs backups with chain-based full/differential backups, gzip/pigz compression, chain retention, and safe restore helpers.

This repository contains:

- `zfs_simple_backup_restore.py` — main script and library code.
- `zfs_simple_backup_restore_tests.py` — internal test suite (non-destructive unit-style tests).

## Quick overview

The tool organizes backups into "chains" (folders named `chain-YYYYMMDD`). Each chain contains a full snapshot and zero-or-more differential snapshots. The script supports:

- Backup to files - suitable for network shares and non zfs backup destinations
- Creating full and differential backups
- Restoring from a chain (latest by default)
- Pruning old chains
- Gzip compression using `pigz` if available
- Dry-run mode for safe testing

## Requirements

- Python 3.8+
- `pv` if you want rate limiting in real backups
- ZFS userland tools (`zfs`, `zpool`) for real backups & restores

Note: The repository includes tests that mock `subprocess.run` for many ZFS behaviors so unit tests are safe to run on a regular machine. Destructive (integration) tests that exercise ZFS must run inside an isolated environment (VM or privileged container).

## Examples and usage
```
  # 1. Run daily backup, full every Sunday, keep 2 weeks of backup chains
  sudo zfs-simple-backup-restore.py --action backup --dataset rpool/data --mount /mnt/backups/zfs --interval 7 --retention 2

  # 2. Limit backup bandwidth to 10 MB/s
  sudo zfs-simple-backup-restore.py --action backup --dataset rpool --mount /mnt/backups/zfs --interval 7 --retention 2 --rate 10M

  # 3. Set a custom prefix for snapshot and file names
  sudo zfs-simple-backup-restore.py --action backup --dataset rpool --mount /mnt/backups/zfs --prefix MYBACKUP

  # 4. Restore the most recent backup chain into a pool named "restored"
  sudo zfs-simple-backup-restore.py --action restore --dataset rpool --mount /mnt/backups/zfs --restore-pool restored

  # 5. Cleanup expired chain folders and orphaned snapshots only (no backup/restore)
  sudo zfs-simple-backup-restore.py --action cleanup --dataset rpool --mount /mnt/backups/zfs --retention 2

  # 6. Dry-run backup (shows what would happen, does not run)
  sudo zfs-simple-backup-restore.py --action backup --dataset rpool --mount /mnt/backups/zfs --dry-run

  # 7. Dry-run restore (shows what would happen, does not run)
  sudo zfs-simple-backup-restore.py --action restore --dataset rpool --mount /mnt/backups/zfs --restore-pool restored --dry-run

  # 8. Internal test mode (non-destructive)
  sudo zfs-simple-backup-restore.py --test

NOTES:
 • Each backup "chain" (full + differentials) is stored in its own folder: chain-YYYYMMDD
 • Only the newest retention chains are kept.
 • Differential backups are always relative to the last full backup in the chain.
 • On restore, the default is to use the latest chain folder unless --restore-chain is specified.
 • You can use -s/--restore-snapshot to restore up to a specific point in a chain (filename or timestamp).
 • Requires root for zfs commands and permissions to write/read mount points.
 • Rate limiting requires pv(1) to be installed on the system.
 • All backups are gzip compressed (.gz), using pigz if available.
 • Always test restores periodically!

CRON JOB EXAMPLES:
--------------------------------------------------
# Run a daily backup at 1am, full every 7 days, keep 2 chains.
0 1 * * * root /usr/local/bin/zfs-simple-backup-restore.py --action backup --dataset rpool --mount /mnt/backups/zfs --interval 7 --retention 2

# Run cleanup daily at 1:30am to prune old chains and orphaned snapshots.
30 1 * * * root /usr/local/bin/zfs-simple-backup-restore.py --action cleanup --dataset rpool --mount /mnt/backups/zfs --retention 2
--------------------------------------------------
```

## Running the internal test suite (non-destructive)

Run the built-in tests (non-destructive/mocked):

```bash
python3 zfs_simple_backup_restore.py --test
```

This will run the `ScriptTests` suite in `zfs_simple_backup_restore_tests.py` and print a simple pass/fail summary.

## Destructive integration tests

Destructive integration tests that exercise real ZFS operations must be run in an isolated environment (a disposable VM or a dedicated CI runner with ZFS support). Do not run destructive tests on machines with data you care about. Prefer a VM or CI runner that can be destroyed and recreated.

## Adding tests

- Unit tests in `zfs_simple_backup_restore_tests.py` currently mock `subprocess.run` for ZFS checks and cover the majority of logic. Consider adding:
  - Integration tests that run in an isolated VM or CI environment (separate tests that actually call `zfs send` / `zfs receive`).
  - Tests for edge cases: corrupted `last_chain` file, missing mount point, partially written `.tmp` files.

## Contributing

- Fork the repo and open pull requests for fixes/features.
- Run the test suite locally before submitting.
- Keep formatting consistent with Black (the repo intentionally allows long lines; see Black config above).

## Safety notes

- This project touches real filesystems and ZFS datasets; do not run destructive tests on machines with data you care about.
- Prefer VMs or isolated CI runners for destructive integration tests.

## License

MIT — see `LICENSE` (if present) or treat as permissive unless otherwise specified.


