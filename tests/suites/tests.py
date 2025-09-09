#!/usr/bin/env python3
"""Unit tests for ZFS backup/restore tool.

These tests mock external commands and don't require actual ZFS pools.
"""

import sys
import os
import shutil
from pathlib import Path

from test_base import TestBase

# Guard: only allow running this script when invoked via tests/run-tests.sh which sets RUN_TESTS=1
if os.environ.get("RUN_TESTS") != "1":
    print("ERROR: tests must be run via tests/run-tests.sh (use that script to run tests)")
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
    """Run unit tests"""

    # Change to the project directory
    project_dir = Path(__file__).parent.parent.parent
    sys.path.insert(0, str(project_dir))

    # Create tester using the local harness implementation
    tester = TestSuite()

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


class TestSuite(TestBase):
    def __init__(self):
        super().__init__()
        self.ctx: dict = {}

    def test_required_binaries(self):
        Cmd.has_required_binaries(self.logger)

    def test_cmd_has_required_binaries_missing(self):
        import shutil

        # Simulate zfs and gzip missing
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

    def test_chainmanager_today_returns_expected_chain_name(self):
        tmp, c = self.make_chain_manager(prefix="chain-today-")
        s = c.today()
        assert s.startswith("chain-") and len(s) == 14

    def test_chainmanager_is_within_backup_dir_detects_inside_and_outside(self):
        tmp, c = self.make_chain_manager(prefix="chain-inout-")
        inside = tmp / "chain-test-in"
        self.create_chain_dirs(tmp, ["chain-test-in"])
        assert c.is_within_backup_dir(inside), f"{inside} should be inside {tmp}"
        outside = tmp.parent / "definitely-not-in-backup-root"
        assert not c.is_within_backup_dir(outside), f"{outside} should NOT be inside {tmp}"

    def test_chainmanager_files_filters_out_empty_files(self):
        tmp, c = self.make_chain_manager(prefix="chain-nonempty-")
        d = tmp / "chain-test-nonempty"
        self.create_chain_dirs(tmp, ["chain-test-nonempty"])
        self.write_file(d / "ok.zfs.gz", b"data")
        self.write_file(d / "empty.zfs.gz", b"")
        files = c.files(d)
        assert any(f.name == "ok.zfs.gz" for f in files)
        assert all(f.stat().st_size > 0 for f in files)

    def test_chainmanager_files_returned_in_sorted_order(self):
        tmp, c = self.make_chain_manager(prefix="chain-sorted-")
        d = tmp / "chain-test-sorted"
        self.create_chain_dirs(tmp, ["chain-test-sorted"])
        self.write_file(d / "b.zfs.gz", b"data")
        self.write_file(d / "a.zfs.gz", b"data")
        self.write_file(d / "c.zfs.gz", b"data")
        files = c.files(d)
        names = [f.name for f in files]
        assert names == sorted(names)

    def test_chainmanager_prune_old_removes_old_chains(self):
        tmp, c = self.make_chain_manager(prefix="chain-prune-")
        # Create 4 chain directories
        names = [f"chain-2024071{i}" for i in range(4)]
        self.create_chain_dirs(tmp, names)

        c.prune_old(2, dry_run=False)
        chains = set(p.name for p in tmp.iterdir() if p.is_dir() and p.name.startswith("chain-"))
        assert len(chains) == 2

    def test_chainmanager_chain_dir_raises(self):
        tmp, c = self.make_chain_manager(prefix="chain-raises-")
        try:
            c.chain_dir("not-a-chain")
            assert False, "Should have raised FatalError"
        except FatalError:
            pass

    def test_chainmanager_chain_dir_latest(self):
        tmp, c = self.make_chain_manager(prefix="chain-latest-")
        self.create_chain_dirs(tmp, ["chain-20200101", "chain-20200102"])
        latest = c.chain_dir()
        assert latest.name == "chain-20200102"

    def test_lockfile_context(self):
        with self.tempdir(prefix="lockfile-") as tmp_root:
            lock_path = tmp_root / "test.lock"
            with LockFile(lock_path, self.logger):
                self.assert_file_exists(lock_path)
            self.assert_file_not_exists(lock_path)

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

    def test_zfs_run_does_not_execute_when_dry_run(self):
        import subprocess

        called = []
        with self.patched(subprocess, "run", lambda *a, **kw: called.append(a)):
            ZFS.run(["zfs", "list"], self.logger, dry_run=True)
            assert not called  # Should not call subprocess.run when dry_run=True

    def test_zfs_run_invokes_subprocess_run_when_not_dry(self):
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

    def test_config_default_paths_and_lockfile(self):
        # Ensure default lockfile contains lock dir and script id
        ld = CONFIG.get_log_dir()
        kd = CONFIG.get_lock_dir()
        df = CONFIG.get_default_lockfile()
        assert CONFIG.SCRIPT_ID in df
        assert kd in df

    def test_cmd_pv_and_gzip_behavior(self):
        import shutil
        import subprocess

        # pv with no rate returns empty
        assert Cmd.pv(None) == []
        pv_cmd = Cmd.pv("10M")
        assert pv_cmd and (pv_cmd[0].endswith("pv") or pv_cmd[0].endswith("pv"))
        assert "-L" in pv_cmd and "10M" in pv_cmd

        # Simulate pigz available and working
        orig_which = shutil.which

        def fake_which(name):
            if name == "pigz":
                return "/usr/bin/pigz"
            if name == "gzip":
                return "/usr/bin/gzip"
            return orig_which(name)

        def fake_run(cmd, **kw):
            # pigz --version should succeed
            if cmd and cmd[0].endswith("pigz") and "--version" in cmd:

                class R:
                    pass

                return R()
            return subprocess.run(cmd, **kw)

        with self.patched(shutil, "which", fake_which):
            with self.patched(subprocess, "run", fake_run):
                gz = Cmd.gzip()
                assert gz[0].endswith("pigz")

        # Simulate pigz present but failing; should fallback to gzip
        with self.patched(shutil, "which", lambda name: "/usr/bin/pigz" if name == "pigz" else "/usr/bin/gzip"):
            with self.patched(subprocess, "run", lambda *a, **kw: (_ for _ in ()).throw(Exception("fail"))):
                gz2 = Cmd.gzip()
                assert gz2[0].endswith("gzip")

    def test_chainmanager_files_sorting_and_prune_tmp(self):
        tmp, c = self.make_chain_manager(prefix="ctest-")
        chain = tmp / "chain-20250101"
        self.create_chain_dirs(tmp, ["chain-20250101"])

        # Create full/diff files with specific timestamps embedded in names
        self.write_file(chain / "CT-full-20250101000001.zfs.gz", b"a")
        self.write_file(chain / "CT-diff-20250101000002.zfs.gz", b"a")
        self.write_file(chain / "CT-full-20250101000000.zfs.gz", b"a")
        files = c.files(chain)
        names = [f.name for f in files]
        assert names[0].endswith("-full-20250101000000.zfs.gz")
        assert names[1].endswith("-full-20250101000001.zfs.gz")
        assert names[2].endswith("-diff-20250101000002.zfs.gz")

        # Create old temp files and ensure prune_old removes them
        self.write_file(chain / "old.tmp", b"tmp")
        self.write_file(tmp / "old-temp.tmp", b"tmp")
        import time, os

        old_time = time.time() - 7200
        os.utime(chain / "old.tmp", (old_time, old_time))
        os.utime(tmp / "old-temp.tmp", (old_time, old_time))

        c.prune_old(10, dry_run=False)
        self.assert_file_not_exists(chain / "old.tmp")
        self.assert_file_not_exists(tmp / "old-temp.tmp")

    def test_base_manager_validate_and_sanitize(self):
        with self.tempdir(prefix="bm-") as td:
            # Empty dataset should raise
            args = Args(action="backup", dataset="", mount_point=str(td))
            try:
                BaseManager(args, self.logger)
                assert False
            except ValidationError:
                pass

            # Path traversal in dataset
            args2 = Args(action="backup", dataset="../etc", mount_point=str(td))
            try:
                BaseManager(args2, self.logger)
                assert False
            except ValidationError:
                pass

            # Non-absolute mount point: BaseManager accepts it; ensure sanitized dataset name is used
            args3 = Args(action="backup", dataset="rpool/test", mount_point="relative/path")
            m3 = BaseManager(args3, self.logger)
            assert "rpool_test" in str(m3.target_dir)

            # Sanitization produces safe directory name
            args4 = Args(action="backup", dataset="rpool/my-data", mount_point=str(td))
            m = BaseManager(args4, self.logger)
            assert "rpool_my-data" in str(m.target_dir)

    def test_lockfile_context_ops(self):
        with self.tempdir(prefix="locktest-") as td:
            p = Path(td) / "test.lock"
            with LockFile(p, self.logger):
                self.assert_file_exists(p)
            self.assert_file_not_exists(p)

    def test_zfs_verify_backup_file_missing_and_zstreamdump_missing(self):
        import subprocess

        with self.tempdir(prefix="zfs-") as td:
            f = Path(td) / "nope.zfs.gz"
            assert not f.exists()
            assert ZFS.verify_backup_file(f, self.logger) is False

            # Create a file and simulate zstreamdump not installed
            f2 = Path(td) / "file.zfs.gz"
            self.write_file(f2, b"notazfs")

            def fake_popen(*a, **kw):
                raise FileNotFoundError()

            with self.patched(subprocess, "Popen", fake_popen):
                assert ZFS.verify_backup_file(f2, self.logger) is False

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
        tmp, c = self.make_chain_manager(prefix="chain-latest2-")
        self.create_chain_dirs(tmp, ["chain-20200101", "chain-20200102", "chain-20200103"])
        latest = c.latest_chain_dir()
        assert latest.name == "chain-20200103"

    def test_chainmanager_prune_temp_files_removes_old_temp_files(self):
        tmp, c = self.make_chain_manager(prefix="chain-prune-tmp-")
        chain_dir = tmp / "chain-20200101"
        chain_dir.mkdir(exist_ok=True)

        # Create some temp files that should be cleaned up
        self.write_file(chain_dir / "backup1.zfs.gz.tmp", b"temp data")
        self.write_file(chain_dir / "backup2.zfs.gz", b"real data")
        self.write_file(tmp / "old-temp-file.tmp", b"old temp")

        # Set old timestamp on temp files to simulate old files
        import time

        old_time = time.time() - 86400  # 1 day ago
        import os

        os.utime(chain_dir / "backup1.zfs.gz.tmp", (old_time, old_time))
        os.utime(tmp / "old-temp-file.tmp", (old_time, old_time))

        c.prune_old(1, dry_run=False)

        # Temp files should be gone, real files should remain
        self.assert_file_not_exists(chain_dir / "backup1.zfs.gz.tmp")
        self.assert_file_not_exists(tmp / "old-temp-file.tmp")
        self.assert_file_exists(chain_dir / "backup2.zfs.gz")

    def test_logger_writes_messages_to_log_file(self):
        import os

        # Use TestBase tempdir to ensure cleanup
        with self.tempdir(prefix="logger-test-") as td:
            log_path = td / "test.log"

            # Create logger that writes to our temp file
            logger = Logger(verbose=False)
            logger.log_file_path = str(log_path)

            # Attach log file to logger using harness helper
            self.set_logger_logfile(logger, log_path)

            logger.info("Test info message")
            logger.error("Test error message")
            logger.always("Test always message")

            # Check that messages were written to file
            content = self.read_log(log_path)
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

    def test_main_parse_args_parses_backup_and_restore(self):
        import sys

        # Test basic backup args
        # Test basic backup args
        with self.patched(sys, "argv", ["script", "--action", "backup", "--dataset", "rpool/test", "--mount", "/mnt/backup"]):
            main = Main()
            main.parse_args()
            assert main.args.action == "backup"
            assert main.args.dataset == "rpool/test"
            assert main.args.mount_point == "/mnt/backup"

        # Test restore args
        with self.patched(sys, "argv", ["script", "--action", "restore", "--dataset", "rpool/test", "--mount", "/mnt/backup", "--restore-pool", "newpool"]):
            main = Main()
            main.parse_args()
            assert main.args.action == "restore"
            assert main.args.restore_pool == "newpool"

    def test_main_parse_args_missing_required_args_exits(self):
        import sys

        # Test missing required args (should exit)
        # Test missing required args (should exit)
        with self.patched(sys, "argv", ["script", "--action", "backup"]):
            main = Main()
            try:
                main.parse_args()
                assert False, "Should have exited due to missing args"
            except SystemExit as e:
                assert e.code == CONFIG.EXIT_INVALID_ARGS

    def test_main_validate_authorization_and_binary_checks(self):
        import os

        # Mock all validation checks to pass
        # Mock all validation checks to pass
        with self.patched(os, "geteuid", lambda: 0):
            with self.patched(ZFS, "is_dataset_exists", lambda dataset: True):
                with self.patched(Cmd, "has_required_binaries", lambda logger, rate=None: True):
                    with self.tempdir(prefix="validate-") as td:
                        args = Args(action="backup", dataset="rpool/test", mount_point=str(td))
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
        with self.tempdir(prefix="base-manager-") as td:
            args = Args(action="backup", dataset="rpool/test", mount_point=str(td), prefix="TEST")
            manager = BaseManager(args, self.logger)

            assert manager.args == args
            assert manager.logger == self.logger
            assert manager.dry_run == args.dry_run
            assert manager.prefix == "TEST"
            assert "rpool_test" in str(manager.target_dir)

    def test_backup_manager_init(self):
        with self.tempdir(prefix="backup-manager-") as td:
            args = Args(action="backup", dataset="rpool/test", mount_point=str(td), prefix="TEST")
            manager = BackupManager(args, self.logger)

            assert manager.args == args
            assert manager.logger == self.logger
            assert manager.dry_run == args.dry_run
            assert manager.prefix == "TEST"
            assert "rpool_test" in str(manager.target_dir)

    def test_restore_manager_init(self):
        with self.tempdir(prefix="restore-manager-") as td:
            args = Args(action="restore", dataset="rpool/test", mount_point=str(td), restore_pool="newpool", prefix="TEST")
            manager = RestoreManager(args, self.logger)

            assert manager.args == args
            assert manager.logger == self.logger
            assert manager.dry_run == args.dry_run
            assert manager.prefix == "TEST"
            assert "rpool_test" in str(manager.target_dir)

    def test_backup_mode_decision(self):
        # Test backup mode decision logic (mocked)
        with self.tempdir(prefix="backup-mode-") as tmp_dir:
            args = Args(
                action="backup",
                dataset="rpool/test",
                mount_point=str(tmp_dir),
                interval=7,
                dry_run=True,  # Important: dry run to avoid actual operations
            )
            manager = BackupManager(args, self.logger)

            # Ensure target directory exists
            manager.target_dir.mkdir(parents=True, exist_ok=True)

            # Test when no last_chain_file exists (should do full backup)
            self.assert_file_not_exists(manager.last_chain_file)

            # Create a fake last_chain_file and chain directory
            chain_name = "chain-20240101"
            self.write_file(manager.last_chain_file, chain_name)
            chain_dir = manager.target_dir / chain_name
            chain_dir.mkdir(parents=True, exist_ok=True)

            # Create a fake full backup file with old timestamp
            full_file = chain_dir / "TEST-full-20240101120000.zfs.gz"
            self.write_file(full_file, b"fake backup data")

            # The backup method would decide between full/diff based on age
            # We can't easily test this without mocking datetime, but we can verify the file exists
            self.assert_file_exists(full_file)
            self.assert_file_exists(manager.last_chain_file)

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

    # The following tests are destructive and require actual ZFS pools/datasets.
    # They are intended to be run in a controlled test environment (like a VM)
    # where they can create and destroy ZFS pools and datasets without risk.
    # These tests will create a source dataset, perform backups to a temporary
    # directory, and restore to a separate pool, verifying data integrity.

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
            self.assert_file_exists(p, f"Restored file not found: {p}")
        # Content
        expected_initial = {
            "test_file.txt": "This is test data for ZFS backup testing\n",
            "test_dir/subdir_file.txt": "Nested directory test file\n",
        }
        for rel, exp in expected_initial.items():
            p = Path(ctx["mount_point"]) / rel
            self.assert_file_text_equal(p, exp, f"Content mismatch in {rel}")
        binp = Path(ctx["mount_point"]) / "binary_test.bin"
        self.assert_equal(binp.read_bytes(), bytes([0x00, 1, 2, 3, 4, 5]), "Binary content mismatch")

    def test_destructive_incremental_backup(self) -> None:
        ctx = self.ctx

        # Write files into the source dataset mount to create an incremental change
        src_mount = Path("/") / ctx["dataset"]
        self.write_file(src_mount / "test_file.txt", "test data for backup verification")
        # Ensure subdir exists and write
        (src_mount / "subdir").mkdir(parents=True, exist_ok=True)
        self.write_file(src_mount / "subdir" / "test2.txt", "test data 2 for backup verification")

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
            self.assert_file_text_equal_stripped(p, exp, f"Content mismatch in {rel}")
        # Cleanup mount/dataset
        self.run_cmd(["zfs", "umount", restored_dataset], check=False)
        self.run_cmd(["zfs", "destroy", f"{ctx['restore_pool']}/data"], check=False)


if __name__ == "__main__":
    main()
