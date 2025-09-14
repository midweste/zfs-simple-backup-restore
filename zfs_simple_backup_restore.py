#!/usr/bin/env python3

import sys
import argparse
from dataclasses import dataclass
from pathlib import Path
import shutil
import os
import subprocess
import fcntl
import time
from datetime import datetime


# ========== Exceptions ==========
class FatalError(Exception):
    pass


class ValidationError(FatalError):
    pass


# ========== CONFIG ==========
class CONFIG:
    SCRIPT_ID = "zfs-simple-backup-restore"
    EXIT_SUCCESS = 0
    EXIT_INVALID_ARGS = 2
    EXIT_MOUNT_FAIL = 3
    EXIT_ZFS_FAIL = 4
    EXIT_LOCK_FAIL = 5
    EXIT_NO_BACKUPS = 6
    DEFAULT_INTERVAL_DAYS = 7
    DEFAULT_RETENTION_CHAINS = 2
    DEFAULT_PREFIX = SCRIPT_ID

    # Configurable system paths with fallbacks
    @staticmethod
    def get_log_dir():
        import os

        return os.environ.get("ZFS_BACKUP_LOG_DIR", "/var/log")

    @staticmethod
    def get_lock_dir():
        import os

        return os.environ.get("ZFS_BACKUP_LOCK_DIR", "/var/lock")

    @staticmethod
    def get_default_lockfile():
        return f"{CONFIG.get_lock_dir()}/{CONFIG.SCRIPT_ID}.lock"


# ========== Logger ==========
class Logger:
    def __init__(self, verbose: bool = False):
        self.verbose = verbose
        self.log_file_path = f"{CONFIG.get_log_dir()}/{CONFIG.SCRIPT_ID}.log"
        try:
            os.makedirs(os.path.dirname(self.log_file_path), exist_ok=True)
        except Exception:
            pass
        try:
            self.log_file = open(self.log_file_path, "a")
        except Exception:
            self.log_file = None
        try:
            from systemd import journal

            self.journal = journal
            self.journal_available = True
        except ImportError:
            self.journal = None
            self.journal_available = False

    def _write_logfile(self, level: str, msg: str) -> None:
        if self.log_file:
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            self.log_file.write(f"{now} [{level}] {msg}\n")
            self.log_file.flush()

    def info(self, msg: str) -> None:
        if self.verbose:
            print(f"[INFO]  {msg}", file=sys.stderr)
        self._write_logfile("INFO", msg)
        if self.journal_available:
            self.journal.send(msg, SYSLOG_IDENTIFIER=CONFIG.SCRIPT_ID, PRIORITY=self.journal.LOG_INFO)

    def always(self, msg: str) -> None:
        print(f"[INFO]  {msg}", file=sys.stderr)
        self._write_logfile("INFO", msg)
        if self.journal_available:
            self.journal.send(msg, SYSLOG_IDENTIFIER=CONFIG.SCRIPT_ID, PRIORITY=self.journal.LOG_INFO)

    def error(self, msg: str) -> None:
        print(f"[ERROR] {msg}", file=sys.stderr)
        self._write_logfile("ERROR", msg)
        if self.journal_available:
            self.journal.send(msg, SYSLOG_IDENTIFIER=CONFIG.SCRIPT_ID, PRIORITY=self.journal.LOG_ERR)


# ========== Cmd Class ==========
class Cmd:
    @staticmethod
    def _which(name: str) -> str | None:
        """Find executable in PATH or common sbin locations.

        This helps when cron provides a limited PATH that doesn't include /sbin or /usr/sbin.
        """
        p = shutil.which(name)
        if p:
            return p
        # check common sbin locations
        for prefix in ("/sbin", "/usr/sbin", "/usr/local/sbin"):
            candidate = os.path.join(prefix, name)
            if os.path.isfile(candidate) and os.access(candidate, os.X_OK):
                return candidate
        return None

    @staticmethod
    def zfs(*args):
        return [Cmd._which("zfs") or "zfs"] + list(args)

    @staticmethod
    def zpool(*args):
        return [Cmd._which("zpool") or "zpool"] + list(args)

    @staticmethod
    def pv(rate):
        return [Cmd._which("pv") or "pv", "-q", "-L", rate] if rate else []

    @staticmethod
    def gzip(*args):
        pigz_path = Cmd._which("pigz")
        # Check if pigz actually exists and works
        if pigz_path:
            try:
                subprocess.run([pigz_path, "--version"], capture_output=True, timeout=5, check=True)
                return [pigz_path] + list(args)
            except Exception:
                pass
        return [Cmd._which("gzip") or "gzip"] + list(args)

    @staticmethod
    def gunzip(*args):
        return Cmd.gzip("-dc", *args)

    @staticmethod
    def zstreamdump(*args):
        return [Cmd._which("zstreamdump") or "zstreamdump"] + list(args)

    @staticmethod
    def head(*args):
        return [Cmd._which("head") or "head"] + list(args)

    @staticmethod
    def required_binaries(rate=None):
        bins = {"zfs", "zpool", "gzip", "zstreamdump", "head"}
        if rate:
            bins.add("pv")
        return bins

    @staticmethod
    def has_required_binaries(logger, rate=None):
        missing = []
        for binary in Cmd.required_binaries(rate):
            if not Cmd._which(binary):
                missing.append(binary)

        if missing:
            logger.error(f"Missing required binaries: {' '.join(missing)}")
            print("\nTo install them on Debian/Ubuntu, run:")
            print(f"  sudo apt install {' '.join(missing)}\n")
            return False
        return True


# ========== Dataclass for Args ==========
@dataclass
class Args:
    action: str
    dataset: str
    mount_point: str
    interval: int = CONFIG.DEFAULT_INTERVAL_DAYS
    retention: int = CONFIG.DEFAULT_RETENTION_CHAINS
    prefix: str = CONFIG.DEFAULT_PREFIX
    rate: str = None
    restore_pool: str = None
    restore_chain: str = None
    restore_snapshot: str = None
    lockfile: str = None
    force: bool = False
    dry_run: bool = False
    verbose: bool = False


# ========== LockFile Context Manager ==========
class LockFile:
    def __init__(self, path: Path, logger: Logger):
        self.path = path
        self.logger = logger
        self.fd = None

    def __enter__(self):
        os.makedirs(self.path.parent, exist_ok=True)
        self.fd = os.open(str(self.path), os.O_CREAT | os.O_RDWR)
        try:
            fcntl.flock(self.fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except Exception:
            self.logger.error(f"Lock held: {self.path}")
            raise FatalError(f"Lock held: {self.path}")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            fcntl.flock(self.fd, fcntl.LOCK_UN)
            os.close(self.fd)
        except Exception:
            pass
        try:
            self.path.unlink()
        except Exception:
            pass


# ========== ChainManager ==========
class ChainManager:
    def __init__(self, target_dir: Path, prefix: str, logger: Logger):
        self.target_dir = target_dir
        self.prefix = prefix
        self.logger = logger

    def today(self) -> str:
        return f"chain-{datetime.now().strftime('%Y%m%d')}"

    def prune_old(self, retention_chains: int, dry_run: bool = False) -> None:
        chains = sorted(self.target_dir.glob("chain-*"))
        if len(chains) > retention_chains:
            to_delete = chains[: len(chains) - retention_chains]
            for d in to_delete:
                if self.is_within_backup_dir(d):
                    self.logger.always(f"Deleting old chain folder: {d}")
                    if not dry_run:
                        shutil.rmtree(d, ignore_errors=True)
        now_ts = time.time()
        dirs_to_clean = [self.target_dir] + list(self.target_dir.glob("chain-*"))
        for d in dirs_to_clean:
            for tmp in Path(d).glob("*.tmp"):
                try:
                    if not self.is_within_backup_dir(tmp):
                        continue
                    age = now_ts - tmp.stat().st_mtime
                    if age > 3600:
                        tmp.unlink()
                        self.logger.always(f"Removed orphaned temp file: {tmp}")
                except Exception as e:
                    self.logger.error(f"Failed to remove temp file {tmp}: {e}")

    def latest_chain_dir(self) -> Path:
        chain_dirs = sorted(self.target_dir.glob("chain-*"))
        if not chain_dirs:
            self.logger.error("No chain folders found")
            raise FatalError("No chain folders found")
        return chain_dirs[-1]

    def chain_dir(self, restore_chain: str = None) -> Path:
        if restore_chain:
            d = self.target_dir / restore_chain
            if not d.is_dir():
                self.logger.error(f"Chain folder not found: {d}")
                raise FatalError(f"Chain folder not found: {d}")
            return d
        return self.latest_chain_dir()

    def files(self, chain_dir: Path) -> list:
        def parse_key(p: Path):
            name = p.name
            # Determine type and timestamp: prefer full before diff
            kind = 1
            ts = ""
            if "-full-" in name:
                kind = 0
                ts = name.split("-full-")[-1].split(".zfs")[0].replace(".gz", "")
            elif "-diff-" in name:
                kind = 1
                ts = name.split("-diff-")[-1].split(".zfs")[0].replace(".gz", "")
            return (kind, ts, name)

        files = [f for f in chain_dir.glob("*.zfs.gz") if not str(f).endswith(".tmp") and f.stat().st_size > 0]
        return sorted(files, key=parse_key)

    def is_within_backup_dir(self, path: Path) -> bool:
        abspath = path.resolve()
        abspath_target = self.target_dir.resolve()
        return abspath == abspath_target or str(abspath).startswith(str(abspath_target) + os.sep)


# ========== ZFS ==========
class ZFS:
    @staticmethod
    def is_dataset_exists(dataset: str) -> bool:
        try:
            subprocess.run(
                Cmd.zfs("list", dataset),
                check=True,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            return True
        except subprocess.CalledProcessError:
            return False

    @staticmethod
    def is_pool_exists(pool: str) -> bool:
        try:
            subprocess.run(
                Cmd.zpool("list", pool),
                check=True,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            return True
        except subprocess.CalledProcessError:
            return False

    @staticmethod
    def is_snapshot_exists(dataset: str, snapshot_name: str) -> bool:
        try:
            full_name = f"{dataset}@{snapshot_name}"
            subprocess.run(
                Cmd.zfs("list", "-t", "snapshot", full_name),
                check=True,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            return True
        except subprocess.CalledProcessError:
            return False

    @staticmethod
    def run(cmd: list, logger: Logger, dry_run: bool = False, **kwargs) -> None:
        logger.info(f"Running: {' '.join(cmd)}" + (" [dry-run]" if dry_run else ""))
        if not dry_run:
            subprocess.run(cmd, check=True, **kwargs)

    @staticmethod
    def verify_backup_file(file_path: Path, logger: Logger) -> bool:
        """Verify a gzipped ZFS backup file using basic checks."""
        logger.info(f"Verifying backup file: {file_path}")

        # First check if file exists and has content
        if not file_path.exists():
            logger.error(f"Backup file does not exist: {file_path}")
            return False

        try:
            # Attempt a quick header verification using zstreamdump over the first 1KB
            # We avoid bash-specific process substitution for portability in minimal environments.
            # Resolve binaries via Cmd helpers so cron runs use absolute paths
            gunzip_cmd = Cmd.gunzip(str(file_path))
            gunzip_proc = subprocess.Popen(
                gunzip_cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )

            head_cmd = Cmd.head("-c", "1024")
            head_proc = subprocess.Popen(
                head_cmd,
                stdin=gunzip_proc.stdout,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            # Detach stdout pipes to allow proper SIGPIPE handling
            if gunzip_proc.stdout is not None:
                gunzip_proc.stdout.close()

            zstreamdump_proc = subprocess.Popen(
                Cmd.zstreamdump("-v"),
                stdin=head_proc.stdout,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            if head_proc.stdout is not None:
                head_proc.stdout.close()

            stdout, stderr = zstreamdump_proc.communicate(timeout=10)
            # Ensure upstream processes complete
            try:
                head_proc.communicate(timeout=5)
            except Exception:
                pass
            try:
                gunzip_proc.communicate(timeout=5)
            except Exception:
                pass

            if zstreamdump_proc.returncode == 0:
                logger.info(f"Backup file {file_path.name} verified: valid ZFS stream header")
                return True
            else:
                logger.error(f"Backup file {file_path.name} verification failed: invalid ZFS stream")
                return False

        except subprocess.TimeoutExpired:
            logger.error(f"Backup file {file_path.name} verification timed out")
            return False
        except FileNotFoundError:
            # If zstreamdump is missing, we can't verify here in minimal environments; consider it unverifiable
            logger.error("zstreamdump command not found - ZFS utilities may not be installed")
            return False
        except Exception as e:
            logger.error(f"Failed to verify backup file {file_path.name}: {e}")
            return False


# ========== Base Manager ==========
class BaseManager:
    def __init__(self, args: Args, logger: Logger):
        self.args = args
        self.logger = logger
        self.dry_run = args.dry_run

        # Validate and sanitize paths
        self._validate_dataset_name(args.dataset)
        self._validate_mount_point(args.mount_point)

        self.target_dir = Path(args.mount_point) / self._sanitize_dataset_name(args.dataset)
        self.last_chain_file = self.target_dir / "last_chain"
        self.prefix = args.prefix or CONFIG.DEFAULT_PREFIX
        self.chain = ChainManager(self.target_dir, self.prefix, logger)

    def _validate_dataset_name(self, dataset: str) -> None:
        """Validate ZFS dataset name for security and correctness."""
        if not dataset:
            raise ValidationError("Dataset name cannot be empty")

        # Check for path traversal attempts
        if ".." in dataset or dataset.startswith("/"):
            raise ValidationError("Dataset name contains invalid path components")

        # Basic ZFS dataset name validation
        if not all(c.isalnum() or c in "/_-" for c in dataset):
            raise ValidationError("Dataset name contains invalid characters")

        # Check length (ZFS has limits)
        if len(dataset) > 256:
            raise ValidationError("Dataset name too long")

    def _validate_mount_point(self, mount_point: str) -> None:
        """Validate mount point path for security."""
        if not mount_point:
            raise ValidationError("Mount point cannot be empty")

        mount_path = Path(mount_point).resolve()

        # Check for path traversal attempts
        if ".." in str(mount_path):
            raise ValidationError("Mount point contains path traversal")

        # Ensure it's an absolute path
        if not mount_path.is_absolute():
            raise ValidationError("Mount point must be an absolute path")

    def _sanitize_dataset_name(self, dataset: str) -> str:
        """Convert dataset name to safe directory name."""
        # Replace slashes with underscores and remove any remaining problematic chars
        sanitized = dataset.replace("/", "_")
        sanitized = "".join(c for c in sanitized if c.isalnum() or c in "_-")
        return sanitized

    def cleanup(self) -> None:
        self.chain.prune_old(self.args.retention, dry_run=self.args.dry_run)
        self.logger.always("Cleanup done")


# ========== BackupManager ==========
class BackupManager(BaseManager):

    def backup(self) -> None:
        os.makedirs(self.target_dir, exist_ok=True)
        mode = "full"
        if self.last_chain_file.exists():
            last_chain = self.last_chain_file.read_text().strip()
            chain_dir = self.target_dir / last_chain
            fulls = sorted(chain_dir.glob(f"{self.prefix}-full-*.zfs.gz"))
            if fulls:
                ts = fulls[-1].name.split("-full-")[-1].split(".zfs")[0].replace(".gz", "")
                dt = datetime.strptime(ts, "%Y%m%d%H%M%S")
                age = (datetime.now() - dt).days
                if age < self.args.interval:
                    mode = "diff"
        if mode == "full":
            self.backup_full()
        else:
            self.backup_differential()
        self.chain.prune_old(self.args.retention, dry_run=self.args.dry_run)
        self.logger.always("Backup done")

    def backup_full(self) -> None:
        ts = datetime.now().strftime("%Y%m%d%H%M%S")
        snap = f"{self.prefix}-full-{ts}"
        chain_name = self.chain.today()
        chain_dir = self.target_dir / chain_name
        os.makedirs(chain_dir, exist_ok=True)
        filename = chain_dir / f"{snap}.zfs.gz"
        tmpfile = str(filename) + ".tmp"
        self.logger.always(f"Full snapshot: {snap} into {chain_dir}")

        snapshot_created = False
        try:
            if not self.dry_run:
                # Create snapshot
                ZFS.run(
                    Cmd.zfs("snapshot", "-r", f"{self.args.dataset}@{snap}"),
                    self.logger,
                    dry_run=self.dry_run,
                )
                snapshot_created = True

                # Create backup with improved error handling
                with open(tmpfile, "wb") as f:
                    p1 = subprocess.Popen(
                        Cmd.zfs("send", "-R", f"{self.args.dataset}@{snap}"),
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                    )

                    if self.args.rate:
                        p2 = subprocess.Popen(Cmd.pv(self.args.rate), stdin=p1.stdout, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                        p3 = subprocess.Popen(Cmd.gzip(), stdin=p2.stdout, stdout=f, stderr=subprocess.PIPE)
                        p1.stdout.close()
                        p2.stdout.close()

                        # Wait for all processes and check return codes
                        _, p3_err = p3.communicate()
                        _, p2_err = p2.communicate()
                        _, p1_err = p1.communicate()

                        if p1.returncode != 0:
                            raise subprocess.CalledProcessError(p1.returncode, "zfs send", p1_err)
                        if p2.returncode != 0:
                            raise subprocess.CalledProcessError(p2.returncode, "pv", p2_err)
                        if p3.returncode != 0:
                            raise subprocess.CalledProcessError(p3.returncode, "gzip", p3_err)
                    else:
                        p2 = subprocess.Popen(Cmd.gzip(), stdin=p1.stdout, stdout=f, stderr=subprocess.PIPE)
                        p1.stdout.close()

                        # Wait for processes and check return codes
                        _, p2_err = p2.communicate()
                        _, p1_err = p1.communicate()

                        if p1.returncode != 0:
                            raise subprocess.CalledProcessError(p1.returncode, "zfs send", p1_err)
                        if p2.returncode != 0:
                            raise subprocess.CalledProcessError(p2.returncode, "gzip", p2_err)

                # Verify backup file
                if Path(tmpfile).stat().st_size == 0:
                    raise FatalError("Backup file is empty (pipeline failure)")

                if not ZFS.verify_backup_file(Path(tmpfile), self.logger):
                    raise FatalError("Backup file verification failed")

                # Success - rename temp file to final name
                Path(tmpfile).rename(filename)
                self.logger.always(f"Full backup completed: {filename.name}")

        except Exception as e:
            # Cleanup on failure
            if Path(tmpfile).exists():
                Path(tmpfile).unlink()
                self.logger.always(f"Removed failed backup file: {tmpfile}")

            if snapshot_created and not self.dry_run:
                try:
                    ZFS.run(
                        Cmd.zfs("destroy", "-r", f"{self.args.dataset}@{snap}"),
                        self.logger,
                        dry_run=False,
                    )
                    self.logger.always(f"Cleaned up failed snapshot: {snap}")
                except Exception as cleanup_err:
                    self.logger.error(f"Failed to cleanup snapshot {snap}: {cleanup_err}")

            raise FatalError(f"Full backup failed: {e}")

        self.last_chain_file.write_text(chain_name)

    def backup_differential(self) -> None:
        ts = datetime.now().strftime("%Y%m%d%H%M%S")
        snap = f"{self.prefix}-diff-{ts}"
        last_chain = self.last_chain_file.read_text().strip()
        chain_dir = self.target_dir / last_chain
        fulls = sorted(chain_dir.glob(f"{self.prefix}-full-*.zfs.gz"))
        if not fulls:
            self.logger.error(f"No base full snapshot in {chain_dir}; cannot perform differential.")
            raise FatalError("No base full snapshot for differential backup.")
        base_full_file = fulls[-1]
        base_snap = base_full_file.name.rsplit(".", 2)[0]
        if not ZFS.is_snapshot_exists(self.args.dataset, base_snap):
            self.logger.error(f"Base full snapshot {self.args.dataset}@{base_snap} does not exist. Starting new full backup and new chain.")
            self.backup_full()
            return
        filename = chain_dir / f"{snap}.zfs.gz"
        tmpfile = str(filename) + ".tmp"
        self.logger.always(f"Differential {base_snap} -> {snap} in {chain_dir}")

        snapshot_created = False
        try:
            if not self.dry_run:
                # Create snapshot
                ZFS.run(
                    Cmd.zfs("snapshot", "-r", f"{self.args.dataset}@{snap}"),
                    self.logger,
                    dry_run=self.dry_run,
                )
                snapshot_created = True

                # Create differential backup with improved error handling
                with open(tmpfile, "wb") as f:
                    p1 = subprocess.Popen(
                        Cmd.zfs("send", "-R", "-i", base_snap, f"{self.args.dataset}@{snap}"),
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                    )

                    if self.args.rate:
                        p2 = subprocess.Popen(Cmd.pv(self.args.rate), stdin=p1.stdout, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                        p3 = subprocess.Popen(Cmd.gzip(), stdin=p2.stdout, stdout=f, stderr=subprocess.PIPE)
                        p1.stdout.close()
                        p2.stdout.close()

                        # Wait for all processes and check return codes
                        _, p3_err = p3.communicate()
                        _, p2_err = p2.communicate()
                        _, p1_err = p1.communicate()

                        if p1.returncode != 0:
                            raise subprocess.CalledProcessError(p1.returncode, "zfs send", p1_err)
                        if p2.returncode != 0:
                            raise subprocess.CalledProcessError(p2.returncode, "pv", p2_err)
                        if p3.returncode != 0:
                            raise subprocess.CalledProcessError(p3.returncode, "gzip", p3_err)
                    else:
                        p2 = subprocess.Popen(Cmd.gzip(), stdin=p1.stdout, stdout=f, stderr=subprocess.PIPE)
                        p1.stdout.close()

                        # Wait for processes and check return codes
                        _, p2_err = p2.communicate()
                        _, p1_err = p1.communicate()

                        if p1.returncode != 0:
                            raise subprocess.CalledProcessError(p1.returncode, "zfs send", p1_err)
                        if p2.returncode != 0:
                            raise subprocess.CalledProcessError(p2.returncode, "gzip", p2_err)

                # Verify backup file
                if Path(tmpfile).stat().st_size == 0:
                    raise FatalError("Differential backup file is empty (pipeline failure)")

                if not ZFS.verify_backup_file(Path(tmpfile), self.logger):
                    raise FatalError("Differential backup file verification failed")

                # Success - rename temp file to final name
                Path(tmpfile).rename(filename)
                self.logger.always(f"Differential backup completed: {filename.name}")

        except Exception as e:
            # Cleanup on failure
            if Path(tmpfile).exists():
                Path(tmpfile).unlink()
                self.logger.always(f"Removed failed backup file: {tmpfile}")

            if snapshot_created and not self.dry_run:
                try:
                    ZFS.run(
                        Cmd.zfs("destroy", "-r", f"{self.args.dataset}@{snap}"),
                        self.logger,
                        dry_run=False,
                    )
                    self.logger.always(f"Cleaned up failed snapshot: {snap}")
                except Exception as cleanup_err:
                    self.logger.error(f"Failed to cleanup snapshot {snap}: {cleanup_err}")

            raise FatalError(f"Differential backup failed: {e}")


# ========== RestoreManager ==========
class RestoreManager(BaseManager):

    def restore(self) -> None:
        chain_dir = self.chain.chain_dir(self.args.restore_chain)
        dest = f"{self.args.restore_pool}/{self.args.dataset.split('/')[-1]}"
        files = self.chain.files(chain_dir)
        if not files:
            self.logger.error(f"No backups in {chain_dir}")
            raise FatalError("No backups found in restore chain.")
        # Verify all backup files before proceeding
        self.logger.always("Verifying backup files before restore...")
        for f in files:
            if not ZFS.verify_backup_file(f, self.logger):
                raise FatalError(f"Backup file verification failed: {f.name}")

        # Support restoring up to a specific file/snapshot if requested
        if self.args.restore_snapshot:
            found = False
            filtered = []
            for f in files:
                filtered.append(f)
                base = f.name
                if (self.args.restore_snapshot in str(f)) or (self.args.restore_snapshot == base) or base.endswith(self.args.restore_snapshot):
                    found = True
                    break
                elif self.args.restore_snapshot.isdigit() and self.args.restore_snapshot in base:
                    found = True
                    break
            if not found:
                self.logger.error(f"Could not find file or timestamp {self.args.restore_snapshot} in backup chain!")
                raise FatalError("Restore snapshot not found in chain.")
            files = filtered
        # Print summary
        print(
            f"""
======================================================================
   ZFS RESTORE OPERATION SUMMARY
======================================================================
   Will restore the following:

     Source Chain Folder: {chain_dir}
     Target Dataset:      {dest}
     Number of Snapshots: {len(files)}

   Files to be restored, in order:
"""
        )
        for f in files:
            print(f"     - {f.name}")
        print()

        if self.dry_run:
            print("!!! This is a dry-run. No changes will be made.\n")
        else:
            if not self.args.force:
                print(
                    f"""WARNING: This will OVERWRITE the dataset {dest} with the above snapshots.
If this is not what you want, press Ctrl-C now.
"""
                )
                answer = input("Type 'yes' to proceed: ")
                if answer.strip() != "yes":
                    print("Aborted by user.")
                    sys.exit(CONFIG.EXIT_SUCCESS)
                print()
        self.logger.always(f"Restoring from {chain_dir} to {dest}")
        # Ensure dataset exists
        if not self.dry_run:
            if not ZFS.is_dataset_exists(dest):
                ZFS.run(Cmd.zfs("create", dest), self.logger, dry_run=self.dry_run)
        else:
            self.logger.always(f"Dry-run: Would create dataset {dest} if needed")
        # Restore all files in order
        for f in files:
            self.logger.always(f"Restore {f}")
            if not self.dry_run:
                gunzip = subprocess.Popen(Cmd.gunzip(str(f)), stdout=subprocess.PIPE)
                zfs_recv = subprocess.Popen(
                    Cmd.zfs("receive", "-F", dest),
                    stdin=gunzip.stdout,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                )
                # Close parent's reference to gunzip stdout so zfs_recv sees EOF properly
                if gunzip.stdout is not None:
                    gunzip.stdout.close()

                recv_stdout, recv_stderr = zfs_recv.communicate()
                gunzip_rc = gunzip.wait()

                if zfs_recv.returncode != 0:
                    self.logger.error(recv_stderr.decode() if isinstance(recv_stderr, (bytes, bytearray)) else str(recv_stderr))
                    raise FatalError(f"zfs receive failed for {f.name}")
                if gunzip_rc != 0:
                    raise FatalError(f"gunzip failed for {f.name} (rc={gunzip_rc})")
            else:
                self.logger.always(f"Dry-run: Would restore {f} to {dest}")
        self.logger.always("Restore done")


# ========== Main ==========
class Main:
    def __init__(self):
        self.logger: Logger = None
        self.args: Args = None

    def parse_args(self) -> None:
        description = f"""
    {CONFIG.SCRIPT_ID} — Simple, atomic ZFS backup/restore with retention.

    Back up and restore ZFS datasets to local or remote mounts, with full/diff chains,
    atomic writes, chain retention, gzip/pigz compression, and safety checks.
    """
        epilog = f"""
EXAMPLES:

  # 1. Run daily backup, full every Sunday, keep 2 weeks of backup chains
  sudo {CONFIG.SCRIPT_ID}.py --action backup --dataset rpool/data --mount /mnt/backups/zfs --interval 7 --retention 2

  # 2. Limit backup bandwidth to 10 MB/s
  sudo {CONFIG.SCRIPT_ID}.py --action backup --dataset rpool --mount /mnt/backups/zfs --interval 7 --retention 2 --rate 10M

  # 3. Set a custom prefix for snapshot and file names
  sudo {CONFIG.SCRIPT_ID}.py --action backup --dataset rpool --mount /mnt/backups/zfs --prefix MYBACKUP

  # 4. Restore the most recent backup chain into a pool named "restored"
  sudo {CONFIG.SCRIPT_ID}.py --action restore --dataset rpool --mount /mnt/backups/zfs --restore-pool restored

  # 5. Non-interactive restore (skip confirmation prompt)
  sudo {CONFIG.SCRIPT_ID}.py --action restore --dataset rpool --mount /mnt/backups/zfs --restore-pool restored --force

  # 6. Cleanup expired chain folders and orphaned snapshots only (no backup/restore)
  sudo {CONFIG.SCRIPT_ID}.py --action cleanup --dataset rpool --mount /mnt/backups/zfs --retention 2

  # 7. Dry-run backup (shows what would happen, does not run)
  sudo {CONFIG.SCRIPT_ID}.py --action backup --dataset rpool --mount /mnt/backups/zfs --dry-run

  # 8. Dry-run restore (shows what would happen, does not run)
  sudo {CONFIG.SCRIPT_ID}.py --action restore --dataset rpool --mount /mnt/backups/zfs --restore-pool restored --dry-run


NOTES:
 • Each backup "chain" (full + differentials) is stored in its own folder: chain-YYYYMMDD
 • Only the newest retention chains are kept.
 • Differential backups are always relative to the last full backup in the chain.
 • On restore, the default is to use the latest chain folder unless --restore-chain is specified.
 • You can use -s/--restore-snapshot to restore up to a specific point in a chain (timestamp or filename).
 • Use -f/--force during restore to skip the interactive confirmation prompt (useful for automation/tests).
 • Requires root for zfs commands and permissions to write/read mount points.
 • Rate limiting requires pv(1) to be installed on the system.
 • All backups are gzip compressed (.gz), using pigz if available.
 • Always test restores periodically!

CRON JOB EXAMPLES:
--------------------------------------------------
# Run a daily backup at 1am, full every 7 days, keep 2 chains.
0 1 * * * root /usr/local/bin/{CONFIG.SCRIPT_ID}.py --action backup --dataset rpool --mount /mnt/backups/zfs --interval 7 --retention 2

# Run cleanup daily at 1:30am to prune old chains and orphaned snapshots.
30 1 * * * root /usr/local/bin/{CONFIG.SCRIPT_ID}.py --action cleanup --dataset rpool --mount /mnt/backups/zfs --retention 2
--------------------------------------------------
"""
        parser = argparse.ArgumentParser(
            description=description,
            epilog=epilog,
            formatter_class=argparse.RawDescriptionHelpFormatter,
        )
        parser.add_argument(
            "-a",
            "--action",
            choices=["backup", "restore", "cleanup"],
            help="Main operation: backup (create), restore, or cleanup (prune old chains/snapshots)",
        )
        parser.add_argument("-d", "--dataset", help="ZFS dataset (pool/name), e.g. rpool/data")
        parser.add_argument(
            "-m",
            "--mount",
            dest="mount_point",
            help="Absolute path to local/remote mount point for backups",
        )
        parser.add_argument(
            "-i",
            "--interval",
            type=int,
            default=CONFIG.DEFAULT_INTERVAL_DAYS,
            help="Days between full backups (default: 7)",
        )
        parser.add_argument(
            "-k",
            "--retention",
            type=int,
            default=CONFIG.DEFAULT_RETENTION_CHAINS,
            help="Number of backup chains to keep (default: 2)",
        )
        parser.add_argument(
            "-x",
            "--prefix",
            default=CONFIG.DEFAULT_PREFIX,
            help="Snapshot/file prefix (default: zfs-simple-backup-restore)",
        )
        parser.add_argument(
            "-R",
            "--rate",
            help="Limit send/receive speed, e.g. 10M, 50M, 1G (requires pv)",
        )
        parser.add_argument(
            "-p",
            "--restore-pool",
            help="[RESTORE ONLY] ZFS pool name for restore (required for restore)",
        )
        parser.add_argument(
            "-c",
            "--restore-chain",
            help="[RESTORE ONLY] Chain folder to restore (e.g., chain-20250714; default: latest)",
        )
        parser.add_argument(
            "-s",
            "--restore-snapshot",
            help="[RESTORE ONLY] Only restore up to and including this backup in the chain (timestamp or filename).",
        )
        parser.add_argument(
            "-l",
            "--lockfile",
            default=CONFIG.get_default_lockfile(),
            help=f"Lock file path (default: {CONFIG.get_default_lockfile()})",
        )
        parser.add_argument(
            "-n",
            "--dry-run",
            action="store_true",
            help="Show actions but do not run them",
        )
        parser.add_argument("-f", "--force", action="store_true", help="Do not prompt for confirmation during restore")
        parser.add_argument("-v", "--verbose", action="store_true", help="Verbose logging")
        ns = parser.parse_args()
        # Require action/dataset/mount_point to be provided
        missing = [a for a in ("action", "dataset", "mount_point") if getattr(ns, a, None) is None]
        if missing:
            parser.print_usage()
            sys.exit(CONFIG.EXIT_INVALID_ARGS)
        self.args = Args(**vars(ns))
        self.logger = Logger(verbose=self.args.verbose)

    def validate(self) -> None:
        if not Cmd.has_required_binaries(self.logger, self.args.rate):
            raise ValidationError("Missing required binaries.")
        if os.geteuid() != 0:
            raise ValidationError("This script must be run as root.")
        if not ZFS.is_dataset_exists(self.args.dataset):
            raise ValidationError(f"Dataset not found: {self.args.dataset}")
        if self.args.action == "restore" and self.args.restore_pool and not ZFS.is_pool_exists(self.args.restore_pool):
            raise ValidationError(f"Pool not found: {self.args.restore_pool}")
        mount_point = Path(self.args.mount_point)
        if not mount_point.is_dir():
            raise ValidationError(f"Not a directory: {mount_point}")

    def run(self) -> None:
        self.parse_args()
        try:
            self.validate()
            lockfile = Path(self.args.lockfile or CONFIG.get_default_lockfile())
            with LockFile(lockfile, self.logger):
                if self.args.action == "backup":
                    manager = BackupManager(self.args, self.logger)
                    manager.backup()
                elif self.args.action == "restore":
                    manager = RestoreManager(self.args, self.logger)
                    manager.restore()
                elif self.args.action == "cleanup":
                    manager = BackupManager(self.args, self.logger)
                    manager.cleanup()
        except (ValidationError, FatalError) as e:
            self.logger.error(str(e))
            sys.exit(CONFIG.EXIT_INVALID_ARGS)
        except Exception as e:
            self.logger.error(f"Unexpected error: {e}")
            sys.exit(CONFIG.EXIT_INVALID_ARGS)


def main():
    Main().run()


if __name__ == "__main__":
    main()
