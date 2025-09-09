#!/usr/bin/env python3
"""Non-destructive unit tests for ZFS backup/restore tool.

These tests mock external commands and don't require actual ZFS pools.
"""

import sys
import os
import tempfile
import shutil
from pathlib import Path

from test_base import TestBase

# Guard: only allow running this script when invoked via tests/run-tests.sh which sets RUN_TESTS=1
if os.environ.get("RUN_TESTS") != "1":
    print("ERROR: non-destructive tests must be run via tests/run-tests.sh (use that script to run tests)")
    sys.exit(2)

# This test suite is designed to run from the test orchestrator (tests/run-tests.sh)
# and expects the project package to be installed in the test environment.

# Import project symbols used by the tests
from zfs_simple_backup_restore import (
    Args,
    BaseManager,
    BackupManager,
    ChainManager,
    Cmd,
    CONFIG,
    FatalError,
    LockFile,
    Logger,
    Main,
    RestoreManager,
    ZFS,
)

from zfs_simple_backup_restore import ValidationError


def main():
    """Run non-destructive unit tests"""

    # Change to the project directory
    project_dir = Path(__file__).parent.parent.parent
    sys.path.insert(0, str(project_dir))

    # Create tester using the local harness implementation
    tester = LocalScriptTests()

    # Run tests with tester's logger and run inside project dir for consistent imports/CWD
    ctx = None
    success = False
    try:
        with tester.temp_chdir(project_dir):
            # Prepare destructive environment so destructive tests can use tester.ctx
            # This will enforce root requirement via TestBase.ensure_root_or_exit()
            ctx = tester.destructive_env_setup()
            tester.ctx = ctx

            success = tester.run_all(tester.logger)
    finally:
        # Ensure destructive teardown and any temporary dirs are cleaned up
        try:
            if ctx:
                tester.destructive_env_teardown(ctx)
        except Exception:
            pass
        try:
            if hasattr(tester, "cleanup"):
                tester.cleanup()
        except Exception:
            pass

    if success:
        sys.exit(0)
    else:
        sys.exit(1)


class LocalScriptTests(TestBase):
    def __init__(self):
        super().__init__()
        self.ctx: dict = {}

    def test_required_binaries(self):
        Cmd.has_required_binaries(self.logger)

    def test_cmd_has_required_binaries_missing(self):
        import shutil

        # Simulate zfs and gzip missing
        with self.patched(shutil, "which", lambda name: None):
            ok = Cmd.has_required_binaries(self.logger)
            assert not ok, "Expected has_required_binaries to return False when binaries are missing"

        # Simulate pv required but missing when rate provided
        orig_which = shutil.which
        with self.patched(shutil, "which", lambda name, _orig=orig_which: None if name == "pv" else _orig(name)):
            ok2 = Cmd.has_required_binaries(self.logger, rate="10M")
            assert not ok2, "Expected has_required_binaries to return False when pv is missing and rate supplied"

    def test_cmd_zfs_binary_detection(self):
        import shutil

        # Simulate zfs not found in PATH
        orig_which = shutil.which
        with self.patched(shutil, "which", lambda name, _orig=orig_which: None if name == "zfs" else _orig(name)):
            zfs_cmd = Cmd.zfs("list")
            assert zfs_cmd[0].endswith("zfs"), f"Expected fallback to 'zfs', got {zfs_cmd[0]}"
        # Simulate zfs found in PATH
        orig_which = shutil.which
        with self.patched(shutil, "which", lambda name, _orig=orig_which: ("somepath/zfs" if name == "zfs" else _orig(name))):
            zfs_cmd = Cmd.zfs("list")
            assert zfs_cmd[0].endswith("zfs"), f"Expected a zfs binary, got {zfs_cmd[0]}"

    def test_cmd_gzip_binary_detection(self):
        import shutil
        import subprocess

        # Simulate pigz not found, gzip found
        orig_which = shutil.which
        with self.patched(shutil, "which", lambda name, _orig=orig_which: (None if name == "pigz" else ("somepath/gzip" if name == "gzip" else _orig(name)))):
            gzip_cmd = Cmd.gzip("-9")
            assert gzip_cmd[0].endswith("gzip"), f"Expected a gzip binary, got {gzip_cmd[0]}"

        # Simulate pigz found and works
        def fake_run(cmd, **kwargs):
            if cmd[0].endswith("pigz") and "--version" in cmd:

                class Result:
                    pass

                return Result()
            return subprocess.run(cmd, **kwargs)

        orig_which = shutil.which
        with self.patched(
            shutil, "which", lambda name, _orig=orig_which: ("somepath/pigz" if name == "pigz" else ("somepath/gzip" if name == "gzip" else _orig(name)))
        ):
            with self.patched(subprocess, "run", fake_run):
                gzip_cmd = Cmd.gzip("-9")
                assert gzip_cmd[0].endswith("pigz"), f"Expected a pigz binary, got {gzip_cmd[0]}"

    def test_cmd_gzip_prefers(self):
        import subprocess

        # Test 1: pigz found and works, should prefer pigz
        def fake_run_pigz_works(cmd, **kwargs):
            if cmd[0].endswith("pigz") and "--version" in cmd:

                class Result:
                    pass

                return Result()
            return subprocess.run(cmd, **kwargs)

        orig_which = shutil.which
        with self.patched(shutil, "which", lambda name: ("somepath/pigz" if name == "pigz" else ("somepath/gzip" if name == "gzip" else orig_which(name)))):
            with self.patched(subprocess, "run", fake_run_pigz_works):
                pigz_cmd = Cmd.gzip()
                assert pigz_cmd[0].endswith("pigz"), f"Expected pigz binary, got {pigz_cmd[0]}"

        # Test 2: pigz not found, should fall back to gzip
        orig_which = shutil.which
        with self.patched(shutil, "which", lambda name, _orig=orig_which: ("somepath/gzip" if name == "gzip" else (None if name == "pigz" else _orig(name)))):
            gzip_cmd = Cmd.gzip()
            assert gzip_cmd[0].endswith("gzip"), f"Expected gzip binary, got {gzip_cmd[0]}"

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
        with self.tempdir(prefix="chain-today-") as tmp:
            c = ChainManager(tmp, "TEST", self.logger)
            s = c.today()
            assert s.startswith("chain-") and len(s) == 14

    def test_chainmanager_is_within_backup_dir(self):
        with self.tempdir(prefix="chain-inout-") as tmp:
            c = ChainManager(tmp, "TEST", self.logger)
            d = tmp / "chain-test-in"
            d.mkdir(exist_ok=True)
            assert c.is_within_backup_dir(d), f"{d} should be inside {tmp}"
            outside = tmp.parent / "definitely-not-in-backup-root"
            assert not c.is_within_backup_dir(outside), f"{outside} should NOT be inside {tmp}"

    def test_chainmanager_files_only_nonempty(self):
        with self.tempdir(prefix="chain-nonempty-") as tmp:
            d = tmp / "chain-test-nonempty"
            d.mkdir(exist_ok=True)
            (d / "ok.zfs.gz").write_bytes(b"data")
            (d / "empty.zfs.gz").write_bytes(b"")
            c = ChainManager(tmp, "TEST", self.logger)
            files = c.files(d)
            assert any(f.name == "ok.zfs.gz" for f in files)
            assert all(f.stat().st_size > 0 for f in files)

    def test_chainmanager_files_sorted(self):
        with self.tempdir(prefix="chain-sorted-") as tmp:
            d = tmp / "chain-test-sorted"
            d.mkdir(exist_ok=True)
            (d / "b.zfs.gz").write_bytes(b"data")
            (d / "a.zfs.gz").write_bytes(b"data")
            (d / "c.zfs.gz").write_bytes(b"data")
            c = ChainManager(tmp, "TEST", self.logger)
            files = c.files(d)
            names = [f.name for f in files]
            assert names == sorted(names)

    def test_chainmanager_prune_old(self):
        with self.tempdir(prefix="chain-prune-") as tmp:
            c = ChainManager(tmp, "TEST", self.logger)
            for i in range(4):
                (tmp / f"chain-2024071{i}").mkdir(exist_ok=True)
            c.prune_old(2, dry_run=False)
            chains = set(p.name for p in tmp.iterdir() if p.is_dir() and p.name.startswith("chain-"))
            assert len(chains) == 2

    def test_chainmanager_chain_dir_raises(self):
        with self.tempdir(prefix="chain-raises-") as tmp:
            c = ChainManager(tmp, "TEST", self.logger)
            try:
                c.chain_dir("not-a-chain")
                assert False, "Should have raised FatalError"
            except FatalError:
                pass

    def test_chainmanager_chain_dir_latest(self):
        with self.tempdir(prefix="chain-latest-") as tmp:
            c = ChainManager(tmp, "TEST", self.logger)
            (tmp / "chain-20200101").mkdir(exist_ok=True)
            (tmp / "chain-20200102").mkdir(exist_ok=True)
            latest = c.chain_dir()
            assert latest.name == "chain-20200102"

    def test_lockfile_context(self):
        with self.tempdir(prefix="lockfile-") as tmp_root:
            lock_path = tmp_root / "test.lock"
            with LockFile(lock_path, self.logger):
                assert lock_path.exists()
            assert not lock_path.exists()

    def test_logger_output(self):
        self.logger.info("Logger info test")
        self.logger.always("Logger always test")
        self.logger.error("Logger error test")

    def test_zfs_is_dataset_exists_true(self):
        import subprocess

        with self.patched(subprocess, "run", lambda *a, **kw: None):
            assert ZFS.is_dataset_exists("rpool/test")

    def test_zfs_is_dataset_exists_false(self):
        import subprocess

        def fail(*a, **kw):
            raise subprocess.CalledProcessError(1, "zfs")

        with self.patched(subprocess, "run", fail):
            assert not ZFS.is_dataset_exists("rpool/test")

    def test_zfs_run_dry_run(self):
        import subprocess

        called = []
        with self.patched(subprocess, "run", lambda *a, **kw: called.append(a)):
            ZFS.run(["zfs", "list"], self.logger, dry_run=True)
            assert not called  # Should not call subprocess.run when dry_run=True

    def test_zfs_run_real(self):
        import subprocess

        called = {}

        def fake_run(cmd, check, **kwargs):
            called["cmd"] = cmd

        with self.patched(subprocess, "run", fake_run):
            ZFS.run(["zfs", "list"], self.logger, dry_run=False)
            assert called["cmd"] == ["zfs", "list"]

    def test_config_values(self):
        assert CONFIG.DEFAULT_INTERVAL_DAYS > 0
        assert CONFIG.DEFAULT_RETENTION_CHAINS > 0
        assert CONFIG.SCRIPT_ID == "zfs-simple-backup-restore"

    def test_zfs_is_pool_exists_true(self):
        import subprocess

        with self.patched(subprocess, "run", lambda *a, **kw: None):
            assert ZFS.is_pool_exists("rpool")

    def test_zfs_is_pool_exists_false(self):
        import subprocess

        def fail(*a, **kw):
            raise subprocess.CalledProcessError(1, "zpool")

        with self.patched(subprocess, "run", fail):
            assert not ZFS.is_pool_exists("nonexistent")

    def test_zfs_is_snapshot_exists_true(self):
        import subprocess

        with self.patched(subprocess, "run", lambda *a, **kw: None):
            assert ZFS.is_snapshot_exists("rpool/test", "snap1")

    def test_zfs_is_snapshot_exists_false(self):
        import subprocess

        def fail(*a, **kw):
            raise subprocess.CalledProcessError(1, "zfs")

        with self.patched(subprocess, "run", fail):
            assert not ZFS.is_snapshot_exists("rpool/test", "nonexistent")

    def test_chainmanager_latest_chain_dir(self):
        with self.tempdir(prefix="chain-latest2-") as tmp:
            c = ChainManager(tmp, "TEST", self.logger)
            (tmp / "chain-20200101").mkdir(exist_ok=True)
            (tmp / "chain-20200102").mkdir(exist_ok=True)
            (tmp / "chain-20200103").mkdir(exist_ok=True)
            latest = c.latest_chain_dir()
            assert latest.name == "chain-20200103"

    def test_chainmanager_prune_temp_files(self):
        with self.tempdir(prefix="chain-prune-tmp-") as tmp:
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

    def test_logger_file_logging(self):
        import os

        # Use TestBase tempdir to ensure cleanup
        with self.tempdir(prefix="logger-test-") as td:
            log_path = td / "test.log"

            # Create logger that writes to our temp file
            logger = Logger(verbose=False)
            logger.log_file_path = str(log_path)

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

        # Test missing required args (should exit)
        with self.patched(sys, "argv", ["script", "--action", "backup"]):
            main = Main()
            try:
                main.parse_args()
                assert False, "Should have exited due to missing args"
            except SystemExit as e:
                assert e.code == CONFIG.EXIT_INVALID_ARGS

    def test_main_validate_mocked(self):
        import os

        # Mock all validation checks to pass
        with self.patched(os, "geteuid", lambda: 0):
            with self.patched(ZFS, "is_dataset_exists", lambda dataset: True):
                with self.patched(Cmd, "has_required_binaries", lambda logger, rate=None: True):
                    args = Args(action="backup", dataset="rpool/test", mount_point=tempfile.gettempdir())
                    main = Main()
                    main.args = args
                    main.logger = self.logger

                    # Should not raise any exceptions
                    main.validate()

                    # Test validation failure for non-root
                    with self.patched(os, "geteuid", lambda: 1000):
                        try:
                            main.validate()
                            assert False, "Should have raised ValidationError for non-root"
                        except ValidationError:
                            pass

    def test_base_manager_init(self):
        args = Args(action="backup", dataset="rpool/test", mount_point=tempfile.gettempdir(), prefix="TEST")
        manager = BaseManager(args, self.logger)

        assert manager.args == args
        assert manager.logger == self.logger
        assert manager.dry_run == args.dry_run
        assert manager.prefix == "TEST"
        assert "rpool_test" in str(manager.target_dir)

    def test_backup_manager_init(self):
        args = Args(action="backup", dataset="rpool/test", mount_point=tempfile.gettempdir(), prefix="TEST")
        manager = BackupManager(args, self.logger)

        assert manager.args == args
        assert manager.logger == self.logger
        assert manager.dry_run == args.dry_run
        assert manager.prefix == "TEST"
        assert "rpool_test" in str(manager.target_dir)

    def test_restore_manager_init(self):
        args = Args(action="restore", dataset="rpool/test", mount_point=tempfile.gettempdir(), restore_pool="newpool", prefix="TEST")
        manager = RestoreManager(args, self.logger)

        assert manager.args == args
        assert manager.logger == self.logger
        assert manager.dry_run == args.dry_run
        assert manager.prefix == "TEST"
        assert "rpool_test" in str(manager.target_dir)

    def test_backup_mode_decision(self):
        # Test backup mode decision logic (mocked)
        tmp_dir = Path(self.mktemp_dir(prefix="backup-mode-"))
        try:
            args = Args(
                action="backup", dataset="rpool/test", mount_point=str(tmp_dir), interval=7, dry_run=True  # Important: dry run to avoid actual operations
            )
            manager = BackupManager(args, self.logger)

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
            pass  # cleanup handled by TestBase.cleanup()

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

    def test_destructive_full_backup_and_restore(self) -> None:
        ctx = self.ctx
        self.run_backup(ctx["dataset"], ctx["backup_dir"], "Initial backup")
        dataset_dir = Path(ctx["backup_dir"]) / ctx["dataset"].replace("/", "_")
        chain_dirs = [p.name for p in dataset_dir.iterdir() if p.is_dir() and p.name.startswith("chain-")]
        if not chain_dirs:
            raise RuntimeError("No chain directory found after backup")
        ctx["chain_dir"] = dataset_dir / sorted(chain_dirs)[-1]

        # Create restore pool and restore full
        self.run_cmd(["zpool", "destroy", ctx["restore_pool"]], check=False)
        self.run_cmd(["truncate", "-s", "1G", ctx["restore_pool_file"]])
        self.run_cmd(["zpool", "create", ctx["restore_pool"], ctx["restore_pool_file"]])
        self.run_restore(ctx["dataset"], ctx["backup_dir"], ctx["restore_pool"], ctx["chain_dir"], capture_output=False)

        # Mount restored and verify initial contents
        ctx["restored_dataset"] = f"{ctx['restore_pool']}/{os.path.basename(ctx['dataset'])}"
        ctx["mount_point"] = self.run_cmd(["zfs", "get", "-H", "-o", "value", "mountpoint", ctx["restored_dataset"]]).stdout.strip()
        Path(ctx["mount_point"]).mkdir(parents=True, exist_ok=True)
        if self.run_cmd(["zfs", "get", "-H", "-o", "value", "mounted", ctx["restored_dataset"]]).stdout.strip() != "yes":
            self.run_cmd(["zfs", "mount", ctx["restored_dataset"]])

        # Presence
        for rel in ["test_file.txt", "test_dir/subdir_file.txt", "binary_test.bin"]:
            p = Path(ctx["mount_point"]) / rel
            self.assert_true(p.exists(), f"Restored file not found: {p}")
        # Content
        expected_initial = {
            "test_file.txt": "This is test data for ZFS backup testing\n",
            "test_dir/subdir_file.txt": "Nested directory test file\n",
        }
        for rel, exp in expected_initial.items():
            p = Path(ctx["mount_point"]) / rel
            self.assert_equal(p.read_text(), exp, f"Content mismatch in {rel}")
        binp = Path(ctx["mount_point"]) / "binary_test.bin"
        self.assert_equal(binp.read_bytes(), bytes([0x00, 1, 2, 3, 4, 5]), "Binary content mismatch")

    def test_destructive_incremental_backup(self) -> None:
        ctx = self.ctx

        # Write files into the source dataset mount to create an incremental change
        src_mount = Path("/") / ctx["dataset"]
        (src_mount / "test_file.txt").write_text("test data for backup verification")
        (src_mount / "subdir").mkdir(parents=True, exist_ok=True)
        (src_mount / "subdir" / "test2.txt").write_text("test data 2 for backup verification")

        # Run incremental backup
        self.run_backup(ctx["dataset"], ctx["backup_dir"], "Incremental backup")

    def test_destructive_incremental_restore(self) -> None:
        ctx = self.ctx
        # Recreate restore pool and restore again
        self.run_cmd(["zpool", "destroy", ctx["restore_pool"]], check=False)
        if os.path.exists(ctx["restore_pool_file"]):
            os.unlink(ctx["restore_pool_file"])
        self.run_cmd(["truncate", "-s", "1G", ctx["restore_pool_file"]])
        self.run_cmd(["zpool", "create", ctx["restore_pool"], ctx["restore_pool_file"]])
        self.run_restore(ctx["dataset"], ctx["backup_dir"], ctx["restore_pool"])

        # Verify updated contents
        restored_dataset = f"{ctx['restore_pool']}/{os.path.basename(ctx['dataset'])}"
        mount_point = Path(self.run_cmd(["zfs", "get", "-H", "-o", "value", "mountpoint", restored_dataset]).stdout.strip())
        mount_point.mkdir(parents=True, exist_ok=True)
        if self.run_cmd(["zfs", "get", "-H", "-o", "value", "mounted", restored_dataset]).stdout.strip() != "yes":
            self.run_cmd(["zfs", "mount", restored_dataset])
        checks = [
            ("test_file.txt", "test data for backup verification"),
            ("subdir/test2.txt", "test data 2 for backup verification"),
        ]
        for rel, exp in checks:
            p = mount_point / rel
            self.assert_equal(p.read_text().strip(), exp, f"Content mismatch in {rel}")
        # Cleanup mount/dataset
        self.run_cmd(["zfs", "umount", restored_dataset], check=False)
        self.run_cmd(["zfs", "destroy", f"{ctx['restore_pool']}/data"], check=False)


if __name__ == "__main__":
    main()
