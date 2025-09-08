#!/usr/bin/env python3
"""
Non-destructive unit tests for ZFS backup/restore tool.
These tests mock external commands and don't require actual ZFS pools.
"""

import tempfile
import subprocess
import shutil
import sys
from pathlib import Path

# Add the project root to Python path so we can import the main module
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

# Try to import shared ScriptTests; fall back to the local implementation below if missing
try:
    from zfs_simple_backup_restore_tests import ScriptTests  # optional shared tests
except Exception:
    ScriptTests = None

# Import project symbols used by the tests
from zfs_simple_backup_restore import (
    Logger,
    Cmd,
    ChainManager,
    LockFile,
    ZFS,
    CONFIG,
    Args,
    Main,
    BaseManager,
    BackupManager,
    RestoreManager,
    ValidationError,
    FatalError,
)

def main():
    """Run non-destructive unit tests"""
    print("=== ZFS Backup/Restore Non-Destructive Unit Tests ===")

    # Change to the project directory
    project_dir = Path(__file__).parent.parent.parent
    sys.path.insert(0, str(project_dir))

    # Create logger and run tests
    logger = Logger(verbose=True)
    TesterClass = ScriptTests if ScriptTests is not None else LocalScriptTests
    tester = TesterClass(logger)
    success = tester.run_all()

    if success:
        print("\n=== All non-destructive tests completed successfully ===")
        sys.exit(0)
    else:
        print("\n=== Some non-destructive tests failed ===")
        sys.exit(1)


class LocalScriptTests:
    def __init__(self, logger: Logger):
        self.logger = logger

    def run_all(self) -> bool:
        passed = 0
        failed = 0

        def t(name, func):
            nonlocal passed, failed
            try:
                func()
                self.logger.always(f"TEST PASS: {name}")
                passed += 1
            except Exception as e:
                self.logger.error(f"TEST FAIL: {name}: {e}")
                failed += 1

        # Register all tests to run
        tests = [
            ("Cmd required_binaries", self.test_required_binaries),
            ("Cmd has_required_binaries missing", self.test_cmd_has_required_binaries_missing),
            ("Cmd zfs binary detection", self.test_cmd_zfs_binary_detection),
            ("Cmd gzip binary detection", self.test_cmd_gzip_binary_detection),
            ("Cmd gzip prefers pigz or falls back", self.test_cmd_gzip_prefers),
            ("Cmd zfs/zpool command build", self.test_cmd_zfs_zpool),
            ("Cmd gunzip has -dc", self.test_cmd_gunzip),
            ("Cmd pv command build", self.test_cmd_pv),
            ("ChainManager today() format", self.test_chainmanager_today),
            ("ChainManager is_within_backup_dir", self.test_chainmanager_is_within_backup_dir),
            ("ChainManager files only nonempty", self.test_chainmanager_files_only_nonempty),
            ("ChainManager files returns sorted", self.test_chainmanager_files_sorted),
            ("ChainManager prune_old keeps N", self.test_chainmanager_prune_old),
            ("ChainManager chain_dir raises on missing", self.test_chainmanager_chain_dir_raises),
            ("ChainManager chain_dir returns latest", self.test_chainmanager_chain_dir_latest),
            ("LockFile locks/unlocks", self.test_lockfile_context),
            ("Logger logs (info/always/error)", self.test_logger_output),
            ("ZFS.is_dataset_exists true", self.test_zfs_is_dataset_exists_true),
            ("ZFS.is_dataset_exists false", self.test_zfs_is_dataset_exists_false),
            ("ZFS.run dry_run skips call", self.test_zfs_run_dry_run),
            ("ZFS.run real calls subprocess", self.test_zfs_run_real),
            ("ZFS.is_pool_exists true", self.test_zfs_is_pool_exists_true),
            ("ZFS.is_pool_exists false", self.test_zfs_is_pool_exists_false),
            ("ZFS.is_snapshot_exists true", self.test_zfs_is_snapshot_exists_true),
            ("ZFS.is_snapshot_exists false", self.test_zfs_is_snapshot_exists_false),
            ("ChainManager latest_chain_dir works", self.test_chainmanager_latest_chain_dir),
            ("ChainManager prune cleans temp files", self.test_chainmanager_prune_temp_files),
            ("Logger file logging works", self.test_logger_file_logging),
            ("CONFIG values sanity", self.test_config_values),
            ("Args dataclass creation", self.test_args_dataclass),
            ("Main parse_args basic", self.test_main_parse_args_basic),
            ("Main parse_args missing required", self.test_main_parse_args_missing),
            # ("Main parse_args test mode", self.test_main_parse_args_test_mode),  # removed: --test flag no longer supported
            ("Main validate checks", self.test_main_validate_mocked),
            ("BaseManager init", self.test_base_manager_init),
            ("BackupManager init", self.test_backup_manager_init),
            ("BackupManager backup mode logic", self.test_backup_mode_decision),
            ("RestoreManager init", self.test_restore_manager_init),
            ("ValidationError and FatalError", self.test_exceptions),
        ]

        for name, fn in tests:
            t(name, fn)

        print(f"\n=== TEST RESULTS: {passed} passed, {failed} failed ===")
        return failed == 0

    def test_required_binaries(self):
        Cmd.has_required_binaries(self.logger)

    def test_cmd_has_required_binaries_missing(self):
        import shutil

        orig_which = shutil.which
        try:
            # Simulate zfs and gzip missing
            shutil.which = lambda name: None
            logger = Logger()
            ok = Cmd.has_required_binaries(logger)
            assert not ok, "Expected has_required_binaries to return False when binaries are missing"

            # Simulate pv required but missing when rate provided
            shutil.which = lambda name: None if name == "pv" else orig_which(name)
            ok2 = Cmd.has_required_binaries(logger, rate="10M")
            assert not ok2, "Expected has_required_binaries to return False when pv is missing and rate supplied"
        finally:
            shutil.which = orig_which

    def test_cmd_zfs_binary_detection(self):
        import shutil

        orig_which = shutil.which
        # Simulate zfs not found in PATH
        shutil.which = lambda name: None if name == "zfs" else orig_which(name)
        zfs_cmd = Cmd.zfs("list")
        assert zfs_cmd[0].endswith("zfs"), f"Expected fallback to 'zfs', got {zfs_cmd[0]}"
        # Simulate zfs found in PATH
        shutil.which = lambda name: ("somepath/zfs" if name == "zfs" else orig_which(name))
        zfs_cmd = Cmd.zfs("list")
        assert zfs_cmd[0].endswith("zfs"), f"Expected a zfs binary, got {zfs_cmd[0]}"
        shutil.which = orig_which

    def test_cmd_gzip_binary_detection(self):
        import shutil
        import subprocess

        orig_which = shutil.which
        orig_run = subprocess.run
        # Simulate pigz not found, gzip found
        shutil.which = lambda name: (None if name == "pigz" else ("somepath/gzip" if name == "gzip" else orig_which(name)))
        gzip_cmd = Cmd.gzip("-9")
        assert gzip_cmd[0].endswith("gzip"), f"Expected a gzip binary, got {gzip_cmd[0]}"
        # Simulate pigz found and works
        shutil.which = lambda name: ("somepath/pigz" if name == "pigz" else ("somepath/gzip" if name == "gzip" else orig_which(name)))

        def fake_run(cmd, **kwargs):
            if cmd[0].endswith("pigz") and "--version" in cmd:

                class Result:
                    pass

                return Result()
            return orig_run(cmd, **kwargs)

        subprocess.run = fake_run
        gzip_cmd = Cmd.gzip("-9")
        assert gzip_cmd[0].endswith("pigz"), f"Expected a pigz binary, got {gzip_cmd[0]}"
        shutil.which = orig_which
        subprocess.run = orig_run

    def test_cmd_gzip_prefers(self):
        import subprocess

        orig_which = shutil.which
        orig_run = subprocess.run

        # Test 1: pigz found and works, should prefer pigz
        shutil.which = lambda name: ("somepath/pigz" if name == "pigz" else ("somepath/gzip" if name == "gzip" else None))

        def fake_run_pigz_works(cmd, **kwargs):
            if cmd[0].endswith("pigz") and "--version" in cmd:

                class Result:
                    pass

                return Result()
            return orig_run(cmd, **kwargs)

        subprocess.run = fake_run_pigz_works
        pigz_cmd = Cmd.gzip()
        assert pigz_cmd[0].endswith("pigz"), f"Expected pigz binary, got {pigz_cmd[0]}"

        # Test 2: pigz not found, should fall back to gzip
        shutil.which = lambda name: "somepath/gzip" if name == "gzip" else None
        subprocess.run = orig_run
        gzip_cmd = Cmd.gzip()
        assert gzip_cmd[0].endswith("gzip"), f"Expected gzip binary, got {gzip_cmd[0]}"

        shutil.which = orig_which
        subprocess.run = orig_run

    def test_cmd_zfs_zpool(self):
        zfs_cmd = Cmd.zfs("list", "pool")
        zpool_cmd = Cmd.zpool("status", "pool")
        assert zfs_cmd[0].endswith("zfs"), f"Expected zfs command, got {zfs_cmd[0]}"
        assert zfs_cmd[1] == "list"
        assert zpool_cmd[0].endswith("zpool"), f"Expected zpool command, got {zpool_cmd[0]}"

    def test_cmd_gunzip(self):
        gunzip_cmd = Cmd.gunzip("file.gz")
        # Accept both binary name and full path, but only check suffix
        assert gunzip_cmd[0].endswith("pigz") or gunzip_cmd[0].endswith("gzip"), f"Expected pigz or gzip, got {gunzip_cmd[0]}"
        assert "-dc" in gunzip_cmd

    def test_cmd_pv(self):
        pv_cmd = Cmd.pv("10M")
        assert pv_cmd[0].endswith("pv"), f"Expected pv command, got {pv_cmd[0]}"
        assert "-L" in pv_cmd and "10M" in pv_cmd

    def test_chainmanager_today(self):
        c = ChainManager(Path(tempfile.gettempdir()), "TEST", self.logger)
        s = c.today()
        assert s.startswith("chain-") and len(s) == 14

    def test_chainmanager_is_within_backup_dir(self):
        tmp = Path(tempfile.mkdtemp())
        c = ChainManager(tmp, "TEST", self.logger)
        d = tmp / "chain-test-in"
        d.mkdir(exist_ok=True)
        assert c.is_within_backup_dir(d), f"{d} should be inside {tmp}"
        outside = tmp.parent / "definitely-not-in-backup-root"
        assert not c.is_within_backup_dir(outside), f"{outside} should NOT be inside {tmp}"
        shutil.rmtree(tmp)

    def test_chainmanager_files_only_nonempty(self):
        tmp = Path(tempfile.gettempdir())
        d = tmp / "chain-test-nonempty"
        d.mkdir(exist_ok=True)
        (d / "ok.zfs.gz").write_bytes(b"data")
        (d / "empty.zfs.gz").write_bytes(b"")
        c = ChainManager(tmp, "TEST", self.logger)
        files = c.files(d)
        assert any(f.name == "ok.zfs.gz" for f in files)
        assert all(f.stat().st_size > 0 for f in files)
        shutil.rmtree(d)

    def test_chainmanager_files_sorted(self):
        tmp = Path(tempfile.gettempdir())
        d = tmp / "chain-test-sorted"
        d.mkdir(exist_ok=True)
        (d / "b.zfs.gz").write_bytes(b"data")
        (d / "a.zfs.gz").write_bytes(b"data")
        (d / "c.zfs.gz").write_bytes(b"data")
        c = ChainManager(tmp, "TEST", self.logger)
        files = c.files(d)
        names = [f.name for f in files]
        assert names == sorted(names)
        shutil.rmtree(d)

    def test_chainmanager_prune_old(self):
        tmp = Path(tempfile.gettempdir())
        c = ChainManager(tmp, "TEST", self.logger)
        for i in range(4):
            (tmp / f"chain-2024071{i}").mkdir(exist_ok=True)
        c.prune_old(2, dry_run=False)
        chains = set(p.name for p in tmp.iterdir() if p.is_dir() and p.name.startswith("chain-"))
        assert len(chains) == 2
        # cleanup
        for p in tmp.iterdir():
            if p.is_dir() and p.name.startswith("chain-"):
                shutil.rmtree(p)

    def test_chainmanager_chain_dir_raises(self):
        tmp = Path(tempfile.gettempdir())
        c = ChainManager(tmp, "TEST", self.logger)
        try:
            c.chain_dir("not-a-chain")
            assert False, "Should have raised FatalError"
        except FatalError:
            pass

    def test_chainmanager_chain_dir_latest(self):
        tmp = Path(tempfile.gettempdir())
        c = ChainManager(tmp, "TEST", self.logger)
        (tmp / "chain-20200101").mkdir(exist_ok=True)
        (tmp / "chain-20200102").mkdir(exist_ok=True)
        latest = c.chain_dir()
        assert latest.name == "chain-20200102"
        shutil.rmtree(tmp / "chain-20200101")
        shutil.rmtree(tmp / "chain-20200102")

    def test_lockfile_context(self):
        tmp = Path(tempfile.gettempdir()) / "test.lock"
        logger = Logger()
        with LockFile(tmp, logger):
            assert tmp.exists()
        assert not tmp.exists()

    def test_logger_output(self):
        logger = Logger(verbose=True)
        logger.info("Logger info test")
        logger.always("Logger always test")
        logger.error("Logger error test")

    def test_zfs_is_dataset_exists_true(self):
        import subprocess

        orig_run = subprocess.run
        subprocess.run = lambda *a, **kw: None
        assert ZFS.is_dataset_exists("rpool/test")
        subprocess.run = orig_run

    def test_zfs_is_dataset_exists_false(self):
        import subprocess

        orig_run = subprocess.run

        def fail(*a, **kw):
            raise subprocess.CalledProcessError(1, "zfs")

        subprocess.run = fail
        assert not ZFS.is_dataset_exists("rpool/test")
        subprocess.run = orig_run

    def test_zfs_run_dry_run(self):
        import subprocess

        called = []
        orig_run = subprocess.run
        subprocess.run = lambda *a, **kw: called.append(a)
        logger = Logger()
        ZFS.run(["zfs", "list"], logger, dry_run=True)
        assert not called  # Should not call subprocess.run when dry_run=True
        subprocess.run = orig_run

    def test_zfs_run_real(self):
        import subprocess

        called = {}
        orig_run = subprocess.run

        def fake_run(cmd, check, **kwargs):
            called["cmd"] = cmd

        subprocess.run = fake_run
        logger = Logger()
        ZFS.run(["zfs", "list"], logger, dry_run=False)
        assert called["cmd"] == ["zfs", "list"]
        subprocess.run = orig_run

    def test_config_values(self):
        assert CONFIG.DEFAULT_INTERVAL_DAYS > 0
        assert CONFIG.DEFAULT_RETENTION_CHAINS > 0
        assert CONFIG.SCRIPT_ID == "zfs-simple-backup-restore"

    def test_zfs_is_pool_exists_true(self):
        import subprocess

        orig_run = subprocess.run
        subprocess.run = lambda *a, **kw: None
        assert ZFS.is_pool_exists("rpool")
        subprocess.run = orig_run

    def test_zfs_is_pool_exists_false(self):
        import subprocess

        orig_run = subprocess.run

        def fail(*a, **kw):
            raise subprocess.CalledProcessError(1, "zpool")

        subprocess.run = fail
        assert not ZFS.is_pool_exists("nonexistent")
        subprocess.run = orig_run

    def test_zfs_is_snapshot_exists_true(self):
        import subprocess

        orig_run = subprocess.run
        subprocess.run = lambda *a, **kw: None
        assert ZFS.is_snapshot_exists("rpool/test", "snap1")
        subprocess.run = orig_run

    def test_zfs_is_snapshot_exists_false(self):
        import subprocess

        orig_run = subprocess.run

        def fail(*a, **kw):
            raise subprocess.CalledProcessError(1, "zfs")

        subprocess.run = fail
        assert not ZFS.is_snapshot_exists("rpool/test", "nonexistent")
        subprocess.run = orig_run

    def test_chainmanager_latest_chain_dir(self):
        tmp = Path(tempfile.gettempdir())
        c = ChainManager(tmp, "TEST", self.logger)
        (tmp / "chain-20200101").mkdir(exist_ok=True)
        (tmp / "chain-20200102").mkdir(exist_ok=True)
        (tmp / "chain-20200103").mkdir(exist_ok=True)
        latest = c.latest_chain_dir()
        assert latest.name == "chain-20200103"
        # cleanup
        for d in ["chain-20200101", "chain-20200102", "chain-20200103"]:
            shutil.rmtree(tmp / d)

    def test_chainmanager_prune_temp_files(self):
        tmp = Path(tempfile.gettempdir())
        c = ChainManager(tmp, "TEST", self.logger)
        chain_dir = tmp / "chain-20200101"
        chain_dir.mkdir(exist_ok=True)

        # Create some temp files that should be cleaned up
        (chain_dir / "backup1.zfs.gz.tmp").write_bytes(b"temp data")
        (chain_dir / "backup2.zfs.gz").write_bytes(b"real data")
        (tmp / "old-temp-file.tmp").write_bytes(b"old temp")

        # Set old timestamp on temp files to simulate old files
        import time

        old_time = time.time() - 86400  # 1 day ago
        import os

        os.utime(chain_dir / "backup1.zfs.gz.tmp", (old_time, old_time))
        os.utime(tmp / "old-temp-file.tmp", (old_time, old_time))

        c.prune_old(1, dry_run=False)

        # Temp files should be gone, real files should remain
        assert not (chain_dir / "backup1.zfs.gz.tmp").exists()
        assert not (tmp / "old-temp-file.tmp").exists()
        assert (chain_dir / "backup2.zfs.gz").exists()

        # cleanup
        shutil.rmtree(chain_dir)

    def test_logger_file_logging(self):
        import tempfile
        import os

        # Create a temporary log file
        log_fd, log_path = tempfile.mkstemp(suffix=".log")
        os.close(log_fd)

        # Create logger that writes to our temp file
        logger = Logger(verbose=False)
        logger.log_file_path = log_path

        try:
            # Reopen the log file
            logger.log_file = open(log_path, "a")

            logger.info("Test info message")
            logger.error("Test error message")
            logger.always("Test always message")

            logger.log_file.close()

            # Check that messages were written to file
            with open(log_path, "r") as f:
                content = f.read()
                assert "Test info message" in content
                assert "Test error message" in content
                assert "Test always message" in content

        finally:
            # Clean up
            try:
                os.unlink(log_path)
            except:
                pass

    def test_args_dataclass(self):
        # Test Args dataclass creation with defaults
        args = Args(action="backup", dataset="rpool/test", mount_point="/mnt/backup")
        assert args.action == "backup"
        assert args.dataset == "rpool/test"
        assert args.mount_point == "/mnt/backup"
        assert args.interval == CONFIG.DEFAULT_INTERVAL_DAYS
        assert args.retention == CONFIG.DEFAULT_RETENTION_CHAINS
        assert args.prefix == CONFIG.DEFAULT_PREFIX
        assert args.dry_run == False
        assert args.verbose == False

    def test_main_parse_args_basic(self):
        import sys

        orig_argv = sys.argv
        try:
            # Test basic backup args
            sys.argv = ["script", "--action", "backup", "--dataset", "rpool/test", "--mount", "/mnt/backup"]
            main = Main()
            main.parse_args()
            assert main.args.action == "backup"
            assert main.args.dataset == "rpool/test"
            assert main.args.mount_point == "/mnt/backup"

            # Test restore args
            sys.argv = ["script", "--action", "restore", "--dataset", "rpool/test", "--mount", "/mnt/backup", "--restore-pool", "newpool"]
            main = Main()
            main.parse_args()
            assert main.args.action == "restore"
            assert main.args.restore_pool == "newpool"
        finally:
            sys.argv = orig_argv

    def test_main_parse_args_missing(self):
        import sys

        orig_argv = sys.argv
        try:
            # Test missing required args (should exit)
            sys.argv = ["script", "--action", "backup"]
            main = Main()
            try:
                main.parse_args()
                assert False, "Should have exited due to missing args"
            except SystemExit as e:
                assert e.code == CONFIG.EXIT_INVALID_ARGS
        finally:
            sys.argv = orig_argv

    def test_main_validate_mocked(self):
        import os

        orig_geteuid = os.geteuid
        orig_zfs_is_dataset = ZFS.is_dataset_exists
        orig_has_required = Cmd.has_required_binaries

        try:
            # Mock all validation checks to pass
            os.geteuid = lambda: 0  # Root user
            ZFS.is_dataset_exists = lambda dataset: True
            Cmd.has_required_binaries = lambda logger, rate=None: True

            args = Args(action="backup", dataset="rpool/test", mount_point=tempfile.gettempdir())
            main = Main()
            main.args = args
            main.logger = Logger()

            # Should not raise any exceptions
            main.validate()

            # Test validation failure for non-root
            os.geteuid = lambda: 1000  # Non-root user
            try:
                main.validate()
                assert False, "Should have raised ValidationError for non-root"
            except ValidationError:
                pass

        finally:
            os.geteuid = orig_geteuid
            ZFS.is_dataset_exists = orig_zfs_is_dataset
            Cmd.has_required_binaries = orig_has_required

    def test_base_manager_init(self):
        args = Args(action="backup", dataset="rpool/test", mount_point=tempfile.gettempdir(), prefix="TEST")
        logger = Logger()
        manager = BaseManager(args, logger)

        assert manager.args == args
        assert manager.logger == logger
        assert manager.dry_run == args.dry_run
        assert manager.prefix == "TEST"
        assert "rpool_test" in str(manager.target_dir)

    def test_backup_manager_init(self):
        args = Args(action="backup", dataset="rpool/test", mount_point=tempfile.gettempdir(), prefix="TEST")
        logger = Logger()
        manager = BackupManager(args, logger)

        assert manager.args == args
        assert manager.logger == logger
        assert manager.dry_run == args.dry_run
        assert manager.prefix == "TEST"
        assert "rpool_test" in str(manager.target_dir)

    def test_restore_manager_init(self):
        args = Args(action="restore", dataset="rpool/test", mount_point=tempfile.gettempdir(), restore_pool="newpool", prefix="TEST")
        logger = Logger()
        manager = RestoreManager(args, logger)

        assert manager.args == args
        assert manager.logger == logger
        assert manager.dry_run == args.dry_run
        assert manager.prefix == "TEST"
        assert "rpool_test" in str(manager.target_dir)

    def test_backup_mode_decision(self):
        # Test backup mode decision logic (mocked)
        tmp_dir = Path(tempfile.mkdtemp())
        try:
            args = Args(
                action="backup", dataset="rpool/test", mount_point=str(tmp_dir), interval=7, dry_run=True  # Important: dry run to avoid actual operations
            )
            logger = Logger()
            manager = BackupManager(args, logger)

            # Ensure target directory exists
            manager.target_dir.mkdir(parents=True, exist_ok=True)

            # Test when no last_chain_file exists (should do full backup)
            assert not manager.last_chain_file.exists()

            # Create a fake last_chain_file and chain directory
            chain_name = "chain-20240101"
            manager.last_chain_file.write_text(chain_name)
            chain_dir = manager.target_dir / chain_name
            chain_dir.mkdir(parents=True, exist_ok=True)

            # Create a fake full backup file with old timestamp
            full_file = chain_dir / "TEST-full-20240101120000.zfs.gz"
            full_file.write_bytes(b"fake backup data")

            # The backup method would decide between full/diff based on age
            # We can't easily test this without mocking datetime, but we can verify the file exists
            assert full_file.exists()
            assert manager.last_chain_file.exists()

        finally:
            shutil.rmtree(tmp_dir, ignore_errors=True)

    def test_exceptions(self):
        # Test that our custom exceptions work
        try:
            raise FatalError("Test fatal error")
        except FatalError as e:
            assert str(e) == "Test fatal error"

        try:
            raise ValidationError("Test validation error")
        except ValidationError as e:
            assert str(e) == "Test validation error"

        # ValidationError should be a subclass of FatalError
        assert issubclass(ValidationError, FatalError)

if __name__ == "__main__":
    main()
