# zfs-simple-backup-restore

Simple, atomic ZFS backup & restore tooling for file level zfs backups with chain-based full/differential backups, gzip/pigz compression, chain retention, and safe restore helpers.

**Important:** This tool is designed to work with its own backup files and chain structure. While it can technically restore any gzipped ZFS stream, it should not be used as a general-purpose ZFS restore utility for backups created by other tools.

This repository contains:

- `zfs_simple_backup_restore.py` — main script and library code.
- `tests/` — test directory containing unit tests, a Vagrant environment, and test runners.

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

  # 5. Non-interactive restore (skip confirmation prompt)
  sudo zfs-simple-backup-restore.py --action restore --dataset rpool --mount /mnt/backups/zfs --restore-pool restored --force

  # 6. Cleanup expired chain folders and orphaned snapshots only (no backup/restore)
  sudo zfs-simple-backup-restore.py --action cleanup --dataset rpool --mount /mnt/backups/zfs --retention 2

  # 7. Dry-run backup (shows what would happen, does not run)
  sudo zfs-simple-backup-restore.py --action backup --dataset rpool --mount /mnt/backups/zfs --dry-run

  # 8. Dry-run restore (shows what would happen, does not run)
  sudo zfs-simple-backup-restore.py --action restore --dataset rpool --mount /mnt/backups/zfs --restore-pool restored --dry-run

```

NOTES:
 • Each backup "chain" (full + differentials) is stored in its own folder: chain-YYYYMMDD
 • Only the newest retention chains are kept.
 • Differential backups are always relative to the last full backup in the chain.
 • On restore, the default is to use the latest chain folder unless --restore-chain is specified.
 • You can use -s/--restore-snapshot to restore up to a specific point in a chain (filename or timestamp).
 • Use -f/--force during restore to skip the interactive confirmation prompt (useful for automation/tests).
 • Requires root for zfs commands and permissions to write/read mount points.
 • Rate limiting requires pv(1) to be installed on the system.
 • All backups are gzip compressed (.gz), using pigz if available.
 • This tool is optimized for its own backup chain structure - while it can restore gzipped ZFS streams, it's not recommended for general-purpose use with external backups.
 • Always test restores periodically!

CRON JOB EXAMPLES:
--------------------------------------------------
# Run a daily backup at 1am, full every 7 days, keep 2 chains.
0 1 * * * root /usr/local/bin/zfs-simple-backup-restore.py --action backup --dataset rpool --mount /mnt/backups/zfs --interval 7 --retention 2

# Run cleanup daily at 1:30am to prune old chains and orphaned snapshots.
30 1 * * * root /usr/local/bin/zfs-simple-backup-restore.py --action cleanup --dataset rpool --mount /mnt/backups/zfs --retention 2
--------------------------------------------------
```

## Testing

### Non-destructive tests (Vagrant)

These unit tests mock ZFS commands and are safe to run without ZFS installed. We provide a Vagrant workflow that provisions Python and runs the tests for you.

Prerequisites:

- Vagrant
- VirtualBox (or adjust the provider in `tests/Vagrantfile`)

Run the tests:

```bash
# From the repository root
tests/run-vagrant-tests.sh --provision

# Subsequent runs (no reprovision):
tests/run-vagrant-tests.sh

# Destroy the VM after a run:
tests/run-vagrant-tests.sh --destroy
```

What it does:

- Boots an Ubuntu 22.04 VM
- Installs Python 3
- Runs `tests/suites/test_non_destructive.py`

### Destructive integration tests (optional)

These tests operate on real ZFS pools/datasets and must be run in an isolated environment with ZFS installed. See `tests/suites/test_destructive.py`. A dedicated Vagrant flow for destructive tests can be added to install ZFS inside the VM before running the suite.

> WARNING: Do not run destructive tests on a machine with data you care about.

## Adding tests

- Unit tests in `test_non_destructive.py` currently mock `subprocess.run` for ZFS checks and cover the majority of logic. Consider adding:
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


