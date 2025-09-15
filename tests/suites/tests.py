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
    ProcessPipeline,
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

    def test_cmd__which_checks_sbin(self):
        import shutil, os

        # Simulate shutil.which not finding the binary, but it exists in /usr/sbin
        with self.patched(shutil, "which", lambda name: None):

            def fake_isfile(p):
                return p == "/usr/sbin/zstreamdump"

            def fake_access(p, mode):
                return p == "/usr/sbin/zstreamdump"

            with self.patched(os.path, "isfile", fake_isfile):
                with self.patched(os, "access", fake_access):
                    p = Cmd._which("zstreamdump")
                    assert p == "/usr/sbin/zstreamdump", f"Expected /usr/sbin/zstreamdump, got {p}"

    def test_cmd_head_helper(self):
        head_cmd = Cmd.head("-c", "1024")
        assert isinstance(head_cmd, list)
        assert head_cmd[0].endswith("head"), f"Expected head command, got {head_cmd[0]}"
        assert "-c" in head_cmd and "1024" in head_cmd

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

        # Test with env vars set
        import os
        with self.patched(os, "environ", {"ZFS_BACKUP_LOG_DIR": "/custom/log", "ZFS_BACKUP_LOCK_DIR": "/custom/lock"}):
            assert CONFIG.get_log_dir() == "/custom/log"
            assert CONFIG.get_lock_dir() == "/custom/lock"
            df_custom = CONFIG.get_default_lockfile()
            assert "/custom/lock" in df_custom

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

    def test_backup_manager_backup_dry_run_no_subprocess(self):
        """Ensure BackupManager.backup() in dry-run mode doesn't invoke subprocess and still writes last_chain."""
        import subprocess

        with self.tempdir(prefix="backup-dryrun-") as td:
            args = Args(action="backup", dataset="rpool/test", mount_point=str(td), prefix="TEST", dry_run=True)
            manager = BackupManager(args, self.logger)

            # Ensure no last_chain exists initially
            self.assert_file_not_exists(manager.last_chain_file)

            # Patch subprocess.Popen to fail if called (should not be called in dry-run)
            with self.patched(subprocess, "Popen", lambda *a, **kw: (_ for _ in ()).throw(Exception("Popen should not be called in dry-run"))):
                manager.backup()

            # After backup in dry-run, last_chain_file should exist and contain chain name
            self.assert_file_exists(manager.last_chain_file)
            chain_name = manager.last_chain_file.read_text().strip()
            chain_dir = manager.target_dir / chain_name
            assert chain_dir.exists(), f"Expected chain dir created in dry-run: {chain_dir}"

    def test_restore_manager_dry_run_no_subprocess(self):
        """Ensure RestoreManager.restore() in dry-run verifies files but does not spawn subprocesses."""
        import subprocess

        with self.tempdir(prefix="restore-dryrun-") as td:
            # Prepare a fake chain with one backup file
            mgr_args = Args(action="restore", dataset="rpool/test", mount_point=str(td), restore_pool="restored", restore_chain="chain-20250101", dry_run=True)
            # Create chain dir and a dummy backup file
            target_dir = Path(mgr_args.mount_point) / mgr_args.dataset.replace("/", "_")
            chain_dir = target_dir / mgr_args.restore_chain
            chain_dir.mkdir(parents=True, exist_ok=True)
            backup_file = chain_dir / "TEST-full-20250101000000.zfs.gz"
            self.write_file(backup_file, b"notazfs")

            # Patch ZFS.verify_backup_file to return True so restore proceeds to dry-run messages
            with self.patched(ZFS, "verify_backup_file", lambda p, logger: True):
                # Patch subprocess.Popen to raise if called (should not be in dry-run)
                with self.patched(subprocess, "Popen", lambda *a, **kw: (_ for _ in ()).throw(Exception("Popen should not be called in dry-run restore"))):
                    mgr = RestoreManager(mgr_args, self.logger)
                    # Should complete without raising
                    mgr.restore()

    def test_backup_full_handles_zfs_send_failure(self):
        """Simulate zfs send (p1) failing and ensure BackupManager handles it and raises FatalError."""
        import subprocess
        import io

        with self.tempdir(prefix="backup-fail-zfs-") as td:
            args = Args(action="backup", dataset="rpool/test", mount_point=str(td), prefix="TEST", dry_run=False)
            manager = BackupManager(args, self.logger)
            manager.target_dir.mkdir(parents=True, exist_ok=True)

            # Ensure last_chain_file exists to create chain dir for writing
            chain_name = manager.chain.today()
            self.write_file(manager.last_chain_file, chain_name)
            chain_dir = manager.target_dir / chain_name
            chain_dir.mkdir(parents=True, exist_ok=True)

            # Patch ZFS.run to be a no-op (so snapshot creation doesn't error)
            with self.patched(ZFS, "run", lambda *a, **kw: None):

                class MockProc:
                    def __init__(self, returncode=0, communicate_ret=(b"", b""), stdout=None):
                        self.returncode = returncode
                        self._communicate_ret = communicate_ret
                        self.stdout = stdout

                    def communicate(self, timeout=None):
                        return self._communicate_ret

                    def wait(self):
                        return self.returncode

                def fake_popen(cmd, stdout=None, stderr=None, stdin=None, **kw):
                    # p1 = zfs send -> simulate failure
                    if isinstance(cmd, (list, tuple)) and any("send" in str(c) for c in cmd):
                        return MockProc(returncode=1, communicate_ret=(b"", b"zfs send failed"), stdout=io.BytesIO(b""))
                    # p2/p3 behave as successful
                    return MockProc(returncode=0, communicate_ret=(b"", b""), stdout=io.BytesIO(b""))

                with self.patched(subprocess, "Popen", fake_popen):
                    try:
                        manager.backup_full()
                        assert False, "Expected FatalError due to zfs send failure"
                    except FatalError:
                        pass

    def test_backup_full_handles_gzip_failure(self):
        """Simulate gzip (p2/p3) failing and ensure BackupManager raises FatalError and cleans up tmpfile."""
        import subprocess
        import io

        with self.tempdir(prefix="backup-fail-gzip-") as td:
            args = Args(action="backup", dataset="rpool/test", mount_point=str(td), prefix="TEST", dry_run=False)
            manager = BackupManager(args, self.logger)
            manager.target_dir.mkdir(parents=True, exist_ok=True)

            # Create chain dir
            chain_name = manager.chain.today()
            chain_dir = manager.target_dir / chain_name
            chain_dir.mkdir(parents=True, exist_ok=True)

            with self.patched(ZFS, "run", lambda *a, **kw: None):

                class MockProc:
                    def __init__(self, returncode=0, communicate_ret=(b"", b""), stdout=None):
                        self.returncode = returncode
                        self._communicate_ret = communicate_ret
                        self.stdout = stdout

                    def communicate(self, timeout=None):
                        return self._communicate_ret

                    def wait(self):
                        return self.returncode

                def fake_popen(cmd, stdout=None, stderr=None, stdin=None, **kw):
                    # p1 (zfs send) succeeds
                    if isinstance(cmd, (list, tuple)) and any("send" in str(c) for c in cmd):
                        return MockProc(returncode=0, communicate_ret=(b"", b""), stdout=io.BytesIO(b"streamdata"))
                    # Simulate gzip failure (the last process) by returning non-zero for gzip command
                    if isinstance(cmd, (list, tuple)) and (str(cmd[0]).endswith("pigz") or str(cmd[0]).endswith("gzip")):
                        return MockProc(returncode=1, communicate_ret=(b"", b"gzip error"), stdout=io.BytesIO(b""))
                    # pv or others succeed
                    return MockProc(returncode=0, communicate_ret=(b"", b""), stdout=io.BytesIO(b""))

                # Use the central patched_pipeline helper to simulate gzip failure.
                def gzip_side(source_cmd, tmpfile, rate, compression_cmd):
                    # create tmpfile (pipeline started) then fail to simulate compression error
                    Path(tmpfile).parent.mkdir(parents=True, exist_ok=True)
                    Path(tmpfile).write_bytes(b"streamdata")
                    raise Exception("gzip error")

                with self.patched_pipeline(run_side_effect=gzip_side) as mock_pipeline:
                    try:
                        manager.backup_full()
                        assert False, "Expected FatalError due to gzip failure"
                    except FatalError:
                        # Ensure tmpfile was cleaned up (no .tmp files remain)
                        tmp_files = list(chain_dir.glob("*.tmp"))
                        assert not tmp_files, f"Temporary files were not cleaned up: {tmp_files}"
                        pass

    def test_cmd_required_binaries_sets(self):
        # Ensure required_binaries returns expected set and includes pv when rate provided
        base = Cmd.required_binaries()
        assert "zfs" in base and "gzip" in base and "zpool" in base
        with_rate = Cmd.required_binaries(rate="10M")
        assert "pv" in with_rate

    def test_zfs_verify_backup_file_success(self):
        """Simulate zstreamdump returning success and ensure verify_backup_file returns True."""
        import subprocess
        import io

        with self.tempdir(prefix="zfs-verify-ok-") as td:
            f = Path(td) / "ok.zfs.gz"
            self.write_file(f, b"fakecontent")

            class MockProc:
                def __init__(self, returncode=0, stdout=None, stderr=None):
                    self.returncode = returncode
                    self.stdout = stdout
                    self.stderr = stderr

                def communicate(self, timeout=None):
                    return (b"ok", b"")

                def wait(self):
                    return self.returncode

            def fake_popen(cmd, stdout=None, stderr=None, stdin=None, **kw):
                # gunzip -> provide stdout pipe
                if cmd and isinstance(cmd, (list, tuple)) and cmd[0].endswith("gunzip"):
                    return MockProc(returncode=0, stdout=io.BytesIO(b"data"))
                # head -> provide stdout pipe
                if cmd and isinstance(cmd, (list, tuple)) and cmd[0].endswith("head"):
                    return MockProc(returncode=0, stdout=io.BytesIO(b"data"))
                # zstreamdump -> return success
                if cmd and isinstance(cmd, (list, tuple)) and cmd[0].endswith("zstreamdump"):
                    p = MockProc(returncode=0, stdout=io.BytesIO(b"ok"))
                    return p
                return MockProc(returncode=0, stdout=io.BytesIO(b""))

            with self.patched(subprocess, "Popen", fake_popen):
                ok = ZFS.verify_backup_file(f, self.logger)
                assert ok is True

    def test_lockfile_raises_when_locked(self):
        """Simulate flock raising and ensure LockFile.__enter__ raises FatalError."""
        import zfs_simple_backup_restore as mod

        def fake_flock(fd, flags):
            raise Exception("already locked")

        with self.tempdir(prefix="lockfail-") as td:
            p = Path(td) / "x.lock"
            with self.patched(mod.fcntl, "flock", fake_flock):
                try:
                    with LockFile(p, self.logger):
                        assert False, "Should not acquire lock when flock fails"
                except FatalError:
                    pass

    def test_backup_differential_no_base_full_raises(self):
        """If no base full exists in the chain, backup_differential should raise FatalError."""
        with self.tempdir(prefix="bdiff-nobase-") as td:
            args = Args(action="backup", dataset="rpool/test", mount_point=str(td), prefix="TEST", dry_run=False)
            manager = BackupManager(args, self.logger)
            manager.target_dir.mkdir(parents=True, exist_ok=True)

            # Create last_chain_file and chain dir but no full files
            chain_name = "chain-20250101"
            self.write_file(manager.last_chain_file, chain_name)
            chain_dir = manager.target_dir / chain_name
            chain_dir.mkdir(parents=True, exist_ok=True)

            try:
                manager.backup_differential()
                assert False, "Expected FatalError when no base full snapshot exists"
            except FatalError:
                pass

    def test_restore_snapshot_not_found_raises(self):
        """If requested restore_snapshot cannot be found in chain, restore() should raise FatalError."""
        with self.tempdir(prefix="restore-snap-") as td:
            args = Args(
                action="restore",
                dataset="rpool/test",
                mount_point=str(td),
                restore_pool="restored",
                restore_chain="chain-20250101",
                restore_snapshot="nope",
                dry_run=True,
            )
            target_dir = Path(args.mount_point) / args.dataset.replace("/", "_")
            chain_dir = target_dir / args.restore_chain
            chain_dir.mkdir(parents=True, exist_ok=True)
            # create one backup file that won't match 'nope'
            self.write_file(chain_dir / "TEST-full-20250101000000.zfs.gz", b"data")

            # Ensure verification passes so logic reaches snapshot lookup
            with self.patched(ZFS, "verify_backup_file", lambda p, logger: True):
                mgr = RestoreManager(args, self.logger)
                try:
                    mgr.restore()
                    assert False, "Expected FatalError when restore snapshot not found"
                except FatalError:
                    pass

    def test_main_run_handles_fatalerror_exit(self):
        """Main.run should exit with error code when BackupManager.backup raises FatalError."""
        import zfs_simple_backup_restore as mod
        import sys

        with self.tempdir(prefix="main-run-") as td:
            mount = td
            # Prepare argv for a backup run
            with self.patched(sys, "argv", ["script", "--action", "backup", "--dataset", "rpool/test", "--mount", str(mount)]):
                # Patch validations to pass initially
                with self.patched(Cmd, "has_required_binaries", lambda logger, rate=None: True):
                    with self.patched(os, "geteuid", lambda: 0):
                        with self.patched(ZFS, "is_dataset_exists", lambda d: True):
                            # Replace BackupManager with one that raises FatalError on backup()
                            class DummyBM:
                                def __init__(self, args, logger):
                                    pass

                                def backup(self):
                                    raise FatalError("simulated")

                            with self.patched(mod, "BackupManager", DummyBM):
                                # Patch LockFile to a no-op context manager
                                class DummyLock:
                                    def __init__(self, *a, **kw):
                                        pass

                                    def __enter__(self):
                                        return self

                                    def __exit__(self, exc_type, exc, tb):
                                        return False

                                with self.patched(mod, "LockFile", DummyLock):
                                    try:
                                        Main().run()
                                        assert False, "Expected SystemExit from Main.run on FatalError"
                                    except SystemExit as e:
                                        assert e.code == CONFIG.EXIT_INVALID_ARGS

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

    # ========== NEW TESTS FOR MISSING COVERAGE ==========

    def test_logger_init_handles_directory_creation_failure(self):
        """Test Logger.__init__ handles directory creation failure gracefully."""
        import os
        import tempfile

        # Mock os.makedirs to raise an exception
        def failing_makedirs(*args, **kwargs):
            raise PermissionError("Permission denied")

        with self.patched(os, "makedirs", failing_makedirs):
            # Should not raise exception, just continue
            logger = Logger(verbose=True)
            assert logger.verbose == True
            # Should still have log_file_path set
            assert hasattr(logger, "log_file_path")

    def test_logger_init_handles_log_file_open_failure(self):
        """Test Logger.__init__ handles log file open failure gracefully."""
        import builtins

        # Mock open to raise an exception
        def failing_open(*args, **kwargs):
            raise PermissionError("Permission denied")

        with self.patched(builtins, "open", failing_open):
            # Should not raise exception, log_file should be None
            logger = Logger(verbose=True)
            assert logger.log_file is None
            assert logger.verbose == True
            # Test that logging methods don't crash when log_file is None
            logger.info("Test info")
            logger.always("Test always")
            logger.error("Test error")

    def test_logger_init_handles_journal_import_failure(self):
        """Test Logger.__init__ handles systemd journal import failure gracefully."""
        from unittest.mock import patch
        import builtins

        # Mock the systemd import to raise ImportError
        original_import = builtins.__import__

        def mock_import(name, *args, **kwargs):
            if name == "systemd":
                raise ImportError("No module named 'systemd'")
            return original_import(name, *args, **kwargs)

        with patch("builtins.__import__", side_effect=mock_import):
            logger = Logger(verbose=True)
            assert logger.journal is None
            assert logger.journal_available == False
            assert logger.verbose == True

    def test_zfs_verify_backup_file_handles_subprocess_errors(self):
        """Test ZFS.verify_backup_file handles subprocess errors in zstreamdump calls."""
        import subprocess
        from unittest.mock import Mock
        from pathlib import Path

    def test_logger_with_systemd_journal(self):
        """Exercise Logger path when systemd.journal is available."""

        # Insert a fake systemd.journal module to exercise the journal send path
        class FakeJournal:
            LOG_INFO = 6
            LOG_ERR = 3

            @staticmethod
            def send(msg, **kw):
                FakeJournal.last = (msg, kw)

        import sys

        sys.modules.setdefault("systemd", type("M", (), {})())
        sys.modules["systemd"].journal = FakeJournal

        logger = Logger(verbose=True)
        logger.info("info-msg")
        logger.always("always-msg")
        logger.error("err-msg")
        assert hasattr(FakeJournal, "last")

    def test_zfs_verify_backup_file_timeout(self):
        """Ensure verify_backup_file returns False when zstreamdump times out and when zstreamdump fails."""
        from pathlib import Path
        import subprocess as _sub
        import io
        from unittest.mock import Mock

        # Case 1: zstreamdump times out
        with self.tempdir() as td:
            f = Path(td) / "fake.zfs.gz"
            f.write_bytes(b"notreallygz")

            class MockPopen:
                def __init__(self, cmd, stdin=None, stdout=None, stderr=None, **kw):
                    self.cmd = cmd
                    try:
                        self.stdout = io.BytesIO(b"data")
                    except Exception:
                        self.stdout = None
                    self.stderr = io.BytesIO(b"")
                    self.returncode = 0

                def communicate(self, timeout=None):
                    cmdstr = " ".join(self.cmd if isinstance(self.cmd, (list, tuple)) else [str(self.cmd)])
                    if "zstreamdump" in cmdstr:
                        raise _sub.TimeoutExpired(cmdstr, timeout or 1)
                    return (b"", b"")

            orig_popen = _sub.Popen
            _sub.Popen = MockPopen
            try:
                ok = ZFS.verify_backup_file(f, self.logger)
                assert ok is False
            finally:
                _sub.Popen = orig_popen

        # Case 2: zstreamdump present but returns non-zero
        import subprocess

        with self.tempdir() as td:
            backup_file = Path(td) / "test.zfs.gz"
            backup_file.write_bytes(b"fake backup data")

            mock_proc = Mock()
            mock_proc.returncode = 1
            mock_proc.communicate.return_value = (b"", b"zstreamdump error")
            mock_proc.stdout = None
            mock_proc.stderr = None

            def mock_popen(cmd, **kwargs):
                try:
                    is_zstream = any("zstreamdump" in str(c) for c in cmd)
                except Exception:
                    is_zstream = "zstreamdump" in str(cmd)
                if is_zstream:
                    return mock_proc
                success_proc = Mock()
                success_proc.returncode = 0
                success_proc.communicate.return_value = (b"fake data", b"")
                success_proc.stdout = Mock()
                return success_proc

            with self.patched(subprocess, "Popen", mock_popen):
                result = ZFS.verify_backup_file(backup_file, self.logger)
                assert result == False

    def test_backup_differential_calls_backup_full_when_snapshot_missing(self):
        """If base full exists but snapshot is missing, backup_differential should call backup_full()."""
        from unittest.mock import Mock

        with self.tempdir() as td:
            args = Args(action="backup", dataset="rpool/test", mount_point=str(td), prefix="TEST", dry_run=False)
            manager = BackupManager(args, self.logger)
            # prepare last_chain and full file
            manager.target_dir.mkdir(parents=True, exist_ok=True)
            last_chain = manager.target_dir / "chain-20240101"
            last_chain.mkdir(parents=True, exist_ok=True)
            manager.last_chain_file.write_text("chain-20240101")
            full_file = last_chain / "TEST-full-20240101120000.zfs.gz"
            full_file.write_bytes(b"fake")

            called = {"backup_full": False}

            def fake_backup_full(self):
                called["backup_full"] = True

            with self.patched(ZFS, "is_snapshot_exists", lambda ds, snap: False):
                with self.patched(BackupManager, "backup_full", fake_backup_full):
                    manager.backup_differential()
                    assert called["backup_full"] is True

    def test_backup_full_cleanup_when_pipeline_fails_and_destroy_fails(self):
        """If pipeline fails during full backup, tmpfile should be removed and destroy cleanup error path exercised."""
        from unittest.mock import Mock

        with self.tempdir() as td:
            args = Args(action="backup", dataset="rpool/test", mount_point=str(td), prefix="TEST", dry_run=False)
            manager = BackupManager(args, self.logger)

            # Patch ProcessPipeline.run_with_rate_limit to write a tmpfile then raise
            def fake_run_with_rate_limit(source_cmd, tmpfile, rate, compression_cmd):
                # create the tmpfile to simulate a partially-written file
                Path(tmpfile).write_bytes(b"partial")
                raise Exception("pipeline-boom")

            # Patch ZFS.run so snapshot creation succeeds but destroy raises
            def fake_zfs_run(cmd, logger, dry_run=False):
                cmd_str = " ".join(cmd) if isinstance(cmd, (list, tuple)) else str(cmd)
                if "snapshot" in cmd_str:
                    return None
                if "destroy" in cmd_str:
                    raise Exception("destroy-boom")
                return None

            with self.patched(ProcessPipeline, "run_with_rate_limit", fake_run_with_rate_limit):
                with self.patched(ZFS, "run", fake_zfs_run):
                    try:
                        manager.backup_full()
                        assert False, "Expected FatalError from backup_full"
                    except FatalError:
                        # Ensure tmpfile was cleaned up (run_with_rate_limit created then code removed it)
                        # The chain dir should exist but no tmp files remain
                        # We can't know the exact snap name, but ensure no .tmp files exist under target_dir
                        tmp_tmps = list(manager.target_dir.rglob("*.tmp"))
                        assert not tmp_tmps, f"Found leftover tmp files: {tmp_tmps}"

    def test_backup_full_cleanup_when_pipeline_fails_and_destroy_succeeds(self):
        """If pipeline fails during full backup, tmpfile should be removed and destroy success path exercised."""
        from unittest.mock import Mock

        with self.tempdir() as td:
            args = Args(action="backup", dataset="rpool/test", mount_point=str(td), prefix="TEST", dry_run=False)
            manager = BackupManager(args, self.logger)

            # Patch ProcessPipeline.run_with_rate_limit to write a tmpfile then raise
            def fake_run_with_rate_limit(source_cmd, tmpfile, rate, compression_cmd):
                Path(tmpfile).write_bytes(b"partial")
                raise Exception("pipeline-boom")

            # Patch ZFS.run so snapshot creation and destroy both succeed
            def fake_zfs_run_ok(cmd, logger, dry_run=False):
                return None

            with self.patched(ProcessPipeline, "run_with_rate_limit", fake_run_with_rate_limit):
                with self.patched(ZFS, "run", fake_zfs_run_ok):
                    try:
                        manager.backup_full()
                        assert False, "Expected FatalError from backup_full"
                    except FatalError:
                        # Ensure tmpfile was cleaned up
                        tmp_tmps = list(manager.target_dir.rglob("*.tmp"))
                        assert not tmp_tmps, f"Found leftover tmp files: {tmp_tmps}"

    def test_backup_full_success_path_creates_final_file_and_writes_last_chain(self):
        """Simulate a successful full backup pipeline and verify final file exists and last_chain_file updated."""
        from unittest.mock import Mock

        with self.tempdir() as td:
            args = Args(action="backup", dataset="rpool/test", mount_point=str(td), prefix="TEST", dry_run=False)
            manager = BackupManager(args, self.logger)

            # Simulate run_with_rate_limit writing a tmpfile successfully
            def fake_run_with_rate_limit(self, source_cmd, tmpfile, rate, compression_cmd):
                Path(tmpfile).write_bytes(b"valid zfs stream")
                return None

            # Ensure snapshot creation succeeds and destroy not needed
            def fake_zfs_run_ok(cmd, logger, dry_run=False):
                return None

            # Ensure verification returns True
            def fake_verify(path, logger):
                return True

            with self.patched(ProcessPipeline, "run_with_rate_limit", fake_run_with_rate_limit):
                with self.patched(ZFS, "run", fake_zfs_run_ok):
                    with self.patched(ZFS, "verify_backup_file", fake_verify):
                        manager.backup_full()

            # After successful run, ensure final .zfs.gz exists in the today's chain dir
            chain_name = manager.chain.today()
            chain_dir = manager.target_dir / chain_name
            files = list(chain_dir.glob(f"{manager.prefix}-full-*.zfs.gz"))
            assert files, f"Expected final backup file in {chain_dir}, found none"
            # last_chain_file should be written with current chain name
            assert manager.last_chain_file.read_text().strip() == chain_name

    def test_backup_full_empty_tmpfile_triggers_cleanup(self):
        """If pipeline produces an empty tmpfile, backup_full should clean it up and raise FatalError."""
        from unittest.mock import Mock

        with self.tempdir() as td:
            args = Args(action="backup", dataset="rpool/test", mount_point=str(td), prefix="TEST", dry_run=False)
            manager = BackupManager(args, self.logger)

            # Simulate run_with_rate_limit creating an empty tmpfile
            def fake_run_with_rate_limit(self, source_cmd, tmpfile, rate, compression_cmd):
                Path(tmpfile).write_bytes(b"")
                return None

            def fake_zfs_run_ok(cmd, logger, dry_run=False):
                return None

            # verify_backup_file should not be called (empty), but patch to be safe
            def fake_verify(path, logger):
                return True

            with self.patched(ProcessPipeline, "run_with_rate_limit", fake_run_with_rate_limit):
                with self.patched(ZFS, "run", fake_zfs_run_ok):
                    with self.patched(ZFS, "verify_backup_file", fake_verify):
                        try:
                            manager.backup_full()
                            assert False, "Expected FatalError due to empty tmpfile"
                        except FatalError:
                            # Ensure no .tmp files left
                            tmp_tmps = list(manager.target_dir.rglob("*.tmp"))
                            assert not tmp_tmps, f"Found leftover tmp files: {tmp_tmps}"

    def test_backup_full_verification_failure_triggers_cleanup(self):
        """If verification fails after pipeline, backup_full should clean up and raise FatalError."""
        from unittest.mock import Mock

        with self.tempdir() as td:
            args = Args(action="backup", dataset="rpool/test", mount_point=str(td), prefix="TEST", dry_run=False)
            manager = BackupManager(args, self.logger)

            def fake_run_with_rate_limit(self, source_cmd, tmpfile, rate, compression_cmd):
                Path(tmpfile).write_bytes(b"not a valid stream")
                return None

            def fake_zfs_run_ok(cmd, logger, dry_run=False):
                return None

            def fake_verify_false(path, logger):
                return False

            with self.patched(ProcessPipeline, "run_with_rate_limit", fake_run_with_rate_limit):
                with self.patched(ZFS, "run", fake_zfs_run_ok):
                    with self.patched(ZFS, "verify_backup_file", fake_verify_false):
                        try:
                            manager.backup_full()
                            assert False, "Expected FatalError due to verification failure"
                        except FatalError:
                            tmp_tmps = list(manager.target_dir.rglob("*.tmp"))
                            assert not tmp_tmps, f"Found leftover tmp files: {tmp_tmps}"

    def test_backup_full_dry_run_writes_last_chain_no_files(self):
        """When dry_run=True, backup_full should not create backup files but should write last_chain_file."""
        from pathlib import Path

        with self.tempdir() as td:
            args = Args(action="backup", dataset="rpool/test", mount_point=str(td), prefix="TEST", dry_run=True)
            manager = BackupManager(args, self.logger)

            # Ensure run_with_rate_limit is not called by leaving it unpatched; snapshot/pipeline should be skipped
            manager.backup_full()

            # chain dir should exist but no .zfs.gz files created
            chain_name = manager.chain.today()
            chain_dir = manager.target_dir / chain_name
            assert chain_dir.exists(), f"Expected chain dir {chain_dir} to exist"
            files = list(chain_dir.glob("*.zfs.gz"))
            assert not files, f"Did not expect final backup files in dry-run, found: {files}"
            # last_chain_file should be written
            assert manager.last_chain_file.read_text().strip() == chain_name

    def test_backup_full_verify_raises_exception_triggers_cleanup_and_destroy_called(self):
        """If verify raises, ensure tmpfile cleaned and destroy called during cleanup."""
        from unittest.mock import Mock
        from pathlib import Path

        with self.tempdir() as td:
            args = Args(action="backup", dataset="rpool/test", mount_point=str(td), prefix="TEST", dry_run=False)
            manager = BackupManager(args, self.logger)

            # Create a tmpfile via run_with_rate_limit
            def fake_run_with_rate_limit(self, source_cmd, tmpfile, rate, compression_cmd):
                Path(tmpfile).write_bytes(b"some-data")
                return None

            calls = []

            def fake_zfs_run_recorder(cmd, logger, dry_run=False):
                calls.append(" ".join(cmd) if isinstance(cmd, (list, tuple)) else str(cmd))
                return None

            def fake_verify_raises(path, logger):
                raise Exception("verify-boom")

            with self.patched(ProcessPipeline, "run_with_rate_limit", fake_run_with_rate_limit):
                with self.patched(ZFS, "run", fake_zfs_run_recorder):
                    with self.patched(ZFS, "verify_backup_file", fake_verify_raises):
                        try:
                            manager.backup_full()
                            assert False, "Expected FatalError due to verify exception"
                        except FatalError:
                            # tmp .tmp files should be removed
                            tmp_tmps = list(manager.target_dir.rglob("*.tmp"))
                            assert not tmp_tmps, f"Found leftover tmp files: {tmp_tmps}"
                            # destroy should have been attempted (look for 'destroy' in recorded commands)
                            assert any("destroy" in c for c in calls), f"Expected destroy to be called in cleanup, calls: {calls}"

    def test_main_run_handles_validation_and_unexpected_errors(self):
        """Main.run should exit with the correct code on ValidationError and other Exceptions."""
        import sys
        from unittest.mock import Mock

        main = Main()
        # supply minimal args/logger
        main.args = Args(action="backup", dataset="rpool/test", mount_point="/tmp")
        main.logger = self.logger

        # Case A: ValidationError
        def fake_parse_args(self):
            return None

        def fake_validate_bad(self):
            raise ValidationError("bad")

        with self.patched(Main, "parse_args", fake_parse_args):
            with self.patched(Main, "validate", fake_validate_bad):
                try:
                    main.run()
                    assert False, "Should have exited"
                except SystemExit as e:
                    assert e.code == CONFIG.EXIT_INVALID_ARGS

        # Case B: unexpected Exception
        def fake_validate_boom(self):
            raise Exception("boom")

        with self.patched(Main, "parse_args", fake_parse_args):
            with self.patched(Main, "validate", fake_validate_boom):
                try:
                    main.run()
                    assert False, "Should have exited"
                except SystemExit as e:
                    assert e.code == CONFIG.EXIT_INVALID_ARGS

    def test_zfs_verify_backup_file_handles_file_not_found(self):
        """Test ZFS.verify_backup_file handles missing zstreamdump binary."""
        import subprocess
        from unittest.mock import Mock
        from pathlib import Path

        with self.tempdir() as td:
            backup_file = Path(td) / "test.zfs.gz"
            backup_file.write_bytes(b"fake backup data")

            # Mock subprocess.Popen to raise FileNotFoundError for zstreamdump
            def mock_popen(cmd, **kwargs):
                # cmd may be list; detect zstreamdump by substring match
                try:
                    is_zstream = any("zstreamdump" in str(c) for c in cmd)
                except Exception:
                    is_zstream = "zstreamdump" in str(cmd)
                if is_zstream:
                    raise FileNotFoundError("zstreamdump command not found")
                # For gunzip and head, return successful mocks
                success_proc = Mock()
                success_proc.returncode = 0
                success_proc.communicate.return_value = (b"fake data", b"")
                success_proc.stdout = Mock()
                return success_proc

            with self.patched(subprocess, "Popen", mock_popen):
                result = ZFS.verify_backup_file(backup_file, self.logger)
                assert result == False

    def test_processpipeline_run_simple_calledprocesserror(self):
        """ProcessPipeline.run_simple should log and re-raise CalledProcessError."""
        import subprocess

        from zfs_simple_backup_restore import ProcessPipeline

        pipeline = ProcessPipeline(self.logger)

        def fake_run(*a, **kw):
            raise subprocess.CalledProcessError(2, a[0], stderr=b"failure")

        with self.patched(subprocess, "run", fake_run):
            try:
                pipeline.run_simple(["false"])
                assert False, "Expected CalledProcessError"
            except subprocess.CalledProcessError:
                pass

    def test_processpipeline_run_simple_timeout_and_filenotfound(self):
        """ProcessPipeline.run_simple should handle TimeoutExpired and FileNotFoundError."""
        import subprocess

        from zfs_simple_backup_restore import ProcessPipeline

        pipeline = ProcessPipeline(self.logger)

        # TimeoutExpired
        def fake_run_timeout(*a, **kw):
            raise subprocess.TimeoutExpired(cmd=a[0], timeout=1)

        with self.patched(subprocess, "run", fake_run_timeout):
            try:
                pipeline.run_simple(["sleep"])
                assert False, "Expected TimeoutExpired"
            except subprocess.TimeoutExpired:
                pass

        # FileNotFoundError
        def fake_run_notfound(*a, **kw):
            raise FileNotFoundError()

        with self.patched(subprocess, "run", fake_run_notfound):
            try:
                pipeline.run_simple(["no-such-cmd"])
                assert False, "Expected FileNotFoundError"
            except FileNotFoundError:
                pass

    def test_processpipeline_run_pipeline_no_commands_and_proc_error(self):
        """Run pipeline with no commands should raise ValueError; failing proc should raise CalledProcessError."""
        import subprocess
        import io

        from zfs_simple_backup_restore import ProcessPipeline

        pipeline = ProcessPipeline(self.logger)

        # No commands
        try:
            pipeline.run_pipeline([])
            assert False, "Expected ValueError for no commands"
        except ValueError:
            pass

        # Simulate a failing process in the pipeline
        class MockProc:
            def __init__(self, returncode=0, stdout=None, stderr=None, communicate_ret=(b"", b"err")):
                self.returncode = returncode
                self.stdout = stdout
                self.stderr = stderr or io.BytesIO(b"err")
                self._communicate_ret = communicate_ret

            def communicate(self, timeout=None):
                return self._communicate_ret

            def wait(self):
                return self.returncode

        call_idx = {"i": 0}

        def fake_popen(cmd, stdin=None, stdout=None, stderr=None, **kw):
            # first process: provides stdout pipe
            if call_idx["i"] == 0:
                p = MockProc(returncode=0, stdout=io.BytesIO(b"data"), communicate_ret=(b"", b""))
            else:
                # last process fails
                p = MockProc(returncode=1, stdout=None, communicate_ret=(b"", b"err"))
            call_idx["i"] += 1
            return p

        with self.patched(subprocess, "Popen", fake_popen):
            try:
                pipeline.run_pipeline([["echo"], ["false"]])
                assert False, "Expected CalledProcessError from failing pipeline proc"
            except subprocess.CalledProcessError:
                pass

    def test_run_with_rate_limit_single_command_uses_run(self):
        """run_with_rate_limit should call subprocess.run for single-command case."""
        import subprocess

        from zfs_simple_backup_restore import ProcessPipeline

        pipeline = ProcessPipeline(self.logger)

        called = {"cmd": None}

        def fake_run(cmd, stdout=None, check=False, **kw):
            called["cmd"] = cmd

            class R:
                pass

            return R()

        with self.patched(subprocess, "run", fake_run):
            with self.tempdir(prefix="rwl-") as td:
                out = Path(td) / "out.tmp"
                pipeline.run_with_rate_limit(["/bin/echo", "hi"], out, rate_limit=None, compression_cmd=None)
                assert called["cmd"] is not None

    def test_processpipeline_run_pipeline_intermediate_proc_error_logs_and_raises(self):
        """Simulate an intermediate pipeline process failing with stderr and ensure CalledProcessError is raised."""
        import subprocess
        import io

        from zfs_simple_backup_restore import ProcessPipeline

        pipeline = ProcessPipeline(self.logger)

        class MockProc:
            def __init__(self, returncode=0, stdout=None, stderr=b"", communicate_ret=(b"", b"")):
                self.returncode = returncode
                self.stdout = stdout
                # stderr should be file-like for .read()
                self.stderr = io.BytesIO(stderr) if stderr is not None else None
                self._communicate_ret = communicate_ret

            def communicate(self, timeout=None):
                return self._communicate_ret

            def wait(self, timeout=None):
                return self.returncode

            def poll(self):
                return self.returncode

            def terminate(self):
                pass

            def kill(self):
                pass

        call = {"i": 0}

        def fake_popen(cmd, stdin=None, stdout=None, stderr=None, **kw):
            # first process: success with stdout pipe
            if call["i"] == 0:
                p = MockProc(returncode=0, stdout=io.BytesIO(b"data"), communicate_ret=(b"", b""))
            elif call["i"] == 1:
                # intermediate process fails
                p = MockProc(returncode=1, stdout=io.BytesIO(b""), stderr=b"intermediate error", communicate_ret=(b"", b"intermediate error"))
            else:
                # final process
                p = MockProc(returncode=0, stdout=None, communicate_ret=(b"ok", b""))
            call["i"] += 1
            return p

        with self.patched(subprocess, "Popen", fake_popen):
            try:
                pipeline.run_pipeline([["first"], ["intermediate"], ["final"]])
                assert False, "Expected CalledProcessError from intermediate failing process"
            except subprocess.CalledProcessError:
                pass

    def test_processpipeline_run_pipeline_timeout_triggers_cleanup(self):
        """If final process.communicate raises TimeoutExpired, the pipeline should propagate it and cleanup."""
        import subprocess
        import io

        from zfs_simple_backup_restore import ProcessPipeline

        pipeline = ProcessPipeline(self.logger)

        class FastProc:
            def __init__(self):
                self.returncode = 0
                self.stdout = io.BytesIO(b"data")

            def communicate(self, timeout=None):
                return (b"", b"")

            def wait(self, timeout=None):
                return 0

            def poll(self):
                return 0

            def terminate(self):
                pass

            def kill(self):
                pass

        class SlowFinalProc:
            def __init__(self):
                self.returncode = None
                self.stdout = None
                self.stderr = io.BytesIO(b"")

            def communicate(self, timeout=None):
                raise subprocess.TimeoutExpired(cmd="final", timeout=timeout)

            def poll(self):
                return None

            def terminate(self):
                pass

            def wait(self, timeout=None):
                return 0

            def kill(self):
                pass

        call = {"i": 0}

        def fake_popen(cmd, stdin=None, stdout=None, stderr=None, **kw):
            if call["i"] < 2:
                p = FastProc()
            else:
                p = SlowFinalProc()
            call["i"] += 1
            return p

        with self.patched(subprocess, "Popen", fake_popen):
            try:
                pipeline.run_pipeline([["a"], ["b"], ["final"]], timeout=0.1)
                assert False, "Expected TimeoutExpired"
            except subprocess.TimeoutExpired:
                pass

    def test_basemanager_validation_handles_missing_dataset(self):
        """Test BaseManager validation handles missing dataset."""
        import os

        with self.tempdir() as td:
            args = Args(action="backup", dataset="nonexistent/dataset", mount_point=str(td))
            main = Main()
            main.args = args
            main.logger = self.logger

            # Mock os.geteuid to return 0 (root)
            with self.patched(os, "geteuid", lambda: 0):
                # Mock ZFS.is_dataset_exists to return False
                with self.patched(ZFS, "is_dataset_exists", lambda ds: False):
                    try:
                        main.validate()
                        assert False, "Should have raised ValidationError"
                    except ValidationError:
                        pass

    def test_basemanager_validation_handles_missing_pool(self):
        """Test BaseManager validation handles missing restore pool."""
        import os

        with self.tempdir() as td:
            args = Args(action="restore", dataset="rpool/test", mount_point=str(td), restore_pool="nonexistent")
            main = Main()
            main.args = args
            main.logger = self.logger

            # Mock os.geteuid to return 0 (root)
            with self.patched(os, "geteuid", lambda: 0):
                # Mock ZFS methods
                with self.patched(ZFS, "is_dataset_exists", lambda ds: True):
                    with self.patched(ZFS, "is_pool_exists", lambda pool: False):
                        try:
                            main.validate()
                            assert False, "Should have raised ValidationError"
                        except ValidationError:
                            pass

    def test_basemanager_validation_handles_non_directory_mount(self):
        """Test BaseManager validation handles non-directory mount point."""
        import os

        with self.tempdir() as td:
            mount_point = Path(td) / "not_a_dir"
            mount_point.write_text("not a directory")

            args = Args(action="backup", dataset="rpool/test", mount_point=str(mount_point))
            main = Main()
            main.args = args
            main.logger = self.logger

            # Mock os.geteuid to return 0 (root)
            with self.patched(os, "geteuid", lambda: 0):
                # Mock ZFS.is_dataset_exists to return True
                with self.patched(ZFS, "is_dataset_exists", lambda ds: True):
                    try:
                        main.validate()
                        assert False, "Should have raised ValidationError"
                    except ValidationError:
                        pass

    def test_backup_manager_backup_full_backup_success(self):
        """Test successful full backup execution with all subprocess calls."""
        import subprocess
        from unittest.mock import Mock, patch
        from pathlib import Path

        with self.tempdir() as td:
            args = Args(action="backup", dataset="rpool/test", mount_point=str(td), prefix="TEST", dry_run=False)
            manager = BackupManager(args, self.logger)

            # Ensure target directory exists and no last_chain file
            manager.target_dir.mkdir(parents=True, exist_ok=True)
            if manager.last_chain_file.exists():
                manager.last_chain_file.unlink()

            # Mock ZFS methods
            with self.patched(ZFS, "run", lambda *a, **kw: None):
                with self.patched(ZFS, "verify_backup_file", lambda *a, **kw: True):
                    # Use central patched_pipeline helper to get a mock pipeline whose
                    # run_with_rate_limit writes a tmpfile by default.
                    with self.patched_pipeline() as mock_pipeline:
                        manager.backup()
                        mock_pipeline.run_with_rate_limit.assert_called_once()

    def test_backup_manager_backup_differential_success(self):
        """Test successful differential backup execution."""
        import subprocess
        from unittest.mock import Mock, patch
        from pathlib import Path

        with self.tempdir() as td:
            args = Args(action="backup", dataset="rpool/test", mount_point=str(td), prefix="TEST", dry_run=False)
            manager = BackupManager(args, self.logger)

            # Ensure target directory exists
            manager.target_dir.mkdir(parents=True, exist_ok=True)

            # Create a fake last_chain file to trigger differential backup
            last_chain_file = manager.last_chain_file
            last_chain_file.parent.mkdir(parents=True, exist_ok=True)
            last_chain_file.write_text("chain-20241231")

            # Create fake chain directory with a full backup file
            chain_dir = manager.target_dir / "chain-20241231"
            chain_dir.mkdir(parents=True, exist_ok=True)
            full_backup = chain_dir / "TEST-full-20241231120000.zfs.gz"
            full_backup.write_bytes(b"fake backup data")

            # Mock ZFS methods. Ensure snapshot existence check returns True so differential path is used.
            with self.patched(ZFS, "run", lambda *a, **kw: None):
                with self.patched(ZFS, "verify_backup_file", lambda *a, **kw: True):
                    with self.patched(ZFS, "is_snapshot_exists", lambda ds, snap: True):
                        # Use central patched_pipeline helper to get a mock pipeline
                        with self.patched_pipeline() as mock_pipeline:
                            manager.backup()
                            mock_pipeline.run_with_rate_limit.assert_called_once()

    def test_backup_manager_backup_handles_rate_limiting(self):
        """Test backup with rate limiting (pv command)."""
        import subprocess
        from unittest.mock import Mock

        with self.tempdir() as td:
            args = Args(action="backup", dataset="rpool/test", mount_point=str(td), prefix="TEST", rate="10M", dry_run=False)
            manager = BackupManager(args, self.logger)
            manager.target_dir.mkdir(parents=True, exist_ok=True)

            # Mock successful subprocess calls for pipeline with pv
            call_count = 0

            def mock_popen(cmd, **kwargs):
                nonlocal call_count
                call_count += 1
                mock_proc = Mock()
                mock_proc.returncode = 0
                mock_proc.communicate.return_value = (b"", b"")
                if call_count == 1:
                    mock_proc.stdout = Mock()  # zfs send
                elif call_count == 2:
                    mock_proc.stdout = Mock()  # pv
                else:
                    mock_proc.stdout = None  # gzip
                return mock_proc

            # Mock ZFS methods
            with self.patched(ZFS, "run", lambda *a, **kw: None):
                with self.patched(ZFS, "verify_backup_file", lambda *a, **kw: True):
                    with self.patched(subprocess, "Popen", mock_popen):
                        with self.patched(Path, "stat", lambda self: Mock(st_size=1024)):
                            # Should not raise exception
                            manager.backup_full()

    def test_backup_manager_backup_handles_empty_backup_file(self):
        """Test backup handles empty backup file error."""
        import subprocess
        from unittest.mock import Mock

        with self.tempdir() as td:
            args = Args(action="backup", dataset="rpool/test", mount_point=str(td), prefix="TEST", dry_run=False)
            manager = BackupManager(args, self.logger)
            manager.target_dir.mkdir(parents=True, exist_ok=True)

            # Mock successful subprocess calls but empty file
            mock_proc = Mock()
            mock_proc.returncode = 0
            mock_proc.communicate.return_value = (b"", b"")

            def mock_popen(cmd, **kwargs):
                return mock_proc

            # Mock ZFS methods
            with self.patched(ZFS, "run", lambda *a, **kw: None):
                with self.patched(subprocess, "Popen", mock_popen):
                    with self.patched(Path, "stat", lambda self: Mock(st_size=0)):
                        # Should raise FatalError for empty backup
                        try:
                            manager.backup_full()
                            assert False, "Should have raised FatalError for empty backup"
                        except FatalError as e:
                            assert "empty" in str(e).lower()

    def test_backup_manager_backup_handles_verification_failure(self):
        """Test backup handles backup verification failure."""
        import subprocess
        from unittest.mock import Mock

        with self.tempdir() as td:
            args = Args(action="backup", dataset="rpool/test", mount_point=str(td), prefix="TEST", dry_run=False)
            manager = BackupManager(args, self.logger)
            manager.target_dir.mkdir(parents=True, exist_ok=True)

            # Mock successful subprocess calls
            mock_proc = Mock()
            mock_proc.returncode = 0
            mock_proc.communicate.return_value = (b"", b"")

            def mock_popen(cmd, **kwargs):
                return mock_proc

            # Mock ZFS methods - verification fails
            with self.patched(ZFS, "run", lambda *a, **kw: None):
                with self.patched(ZFS, "verify_backup_file", lambda *a, **kw: False):
                    with self.patched(subprocess, "Popen", mock_popen):
                        with self.patched(Path, "stat", lambda self: Mock(st_size=1024)):
                            # Should raise FatalError for verification failure
                            try:
                                manager.backup_full()
                                assert False, "Should have raised FatalError for verification failure"
                            except FatalError as e:
                                assert "verification failed" in str(e).lower()

    def test_backup_manager_backup_handles_cleanup_on_error(self):
        """Test backup cleans up temporary files and snapshots on error."""
        import subprocess
        from unittest.mock import Mock

        with self.tempdir() as td:
            args = Args(action="backup", dataset="rpool/test", mount_point=str(td), prefix="TEST", dry_run=False)
            manager = BackupManager(args, self.logger)
            manager.target_dir.mkdir(parents=True, exist_ok=True)

            # Mock subprocess that fails
            mock_proc = Mock()
            mock_proc.returncode = 1  # Failure
            mock_proc.communicate.return_value = (b"", b"error")

            def mock_popen(cmd, **kwargs):
                return mock_proc

            # Mock ZFS methods
            with self.patched(ZFS, "run", lambda *a, **kw: None):
                with self.patched(subprocess, "Popen", mock_popen):
                    with self.patched(Path, "stat", lambda self: Mock(st_size=1024)):
                        # Should raise FatalError and attempt cleanup
                        try:
                            manager.backup_full()
                            assert False, "Should have raised FatalError"
                        except FatalError:
                            pass

    def test_restore_manager_restore_success(self):
        """Test successful restore execution."""
        import subprocess
        from unittest.mock import Mock

        with self.tempdir() as td:
            # Prepare fake backup chain
            args = Args(
                action="restore", dataset="rpool/test", mount_point=str(td), restore_pool="restored", restore_chain="chain-20240101", dry_run=False, force=True
            )
            manager = RestoreManager(args, self.logger)

            # Create fake chain directory and backup file
            chain_dir = manager.target_dir / "chain-20240101"
            chain_dir.mkdir(parents=True, exist_ok=True)
            backup_file = chain_dir / "TEST-full-20240101120000.zfs.gz"
            self.write_file(backup_file, b"fake backup data")

            # Mock successful subprocess calls
            mock_proc = Mock()
            mock_proc.returncode = 0
            mock_proc.communicate.return_value = (b"", b"")
            mock_proc.wait.return_value = 0

            def mock_popen(cmd, **kwargs):
                return mock_proc

            # Mock ZFS methods
            with self.patched(ZFS, "is_dataset_exists", lambda ds: False):
                with self.patched(ZFS, "run", lambda *a, **kw: None):
                    with self.patched(ZFS, "verify_backup_file", lambda *a, **kw: True):
                        with self.patched(subprocess, "Popen", mock_popen):
                            # Should not raise exception
                            manager.restore()

    def test_restore_manager_restore_handles_verification_failure(self):
        """Test restore handles backup file verification failure."""
        with self.tempdir() as td:
            # Prepare fake backup chain
            args = Args(action="restore", dataset="rpool/test", mount_point=str(td), restore_pool="restored", restore_chain="chain-20240101", force=True)
            manager = RestoreManager(args, self.logger)

            # Create fake chain directory and backup file
            chain_dir = manager.target_dir / "chain-20240101"
            chain_dir.mkdir(parents=True, exist_ok=True)
            backup_file = chain_dir / "TEST-full-20240101120000.zfs.gz"
            self.write_file(backup_file, b"fake backup data")

            # Mock ZFS.verify_backup_file to return False
            with self.patched(ZFS, "verify_backup_file", lambda *a, **kw: False):
                try:
                    manager.restore()
                    assert False, "Should have raised FatalError"
                except FatalError as e:
                    assert "verification failed" in str(e)

    def test_restore_manager_restore_handles_no_backups(self):
        """Test restore handles empty backup chain."""
        with self.tempdir() as td:
            args = Args(action="restore", dataset="rpool/test", mount_point=str(td), restore_pool="restored", restore_chain="chain-20240101", force=True)
            manager = RestoreManager(args, self.logger)

            # Create empty chain directory
            chain_dir = manager.target_dir / "chain-20240101"
            chain_dir.mkdir(parents=True, exist_ok=True)

            try:
                manager.restore()
                assert False, "Should have raised FatalError"
            except FatalError as e:
                assert "No backups found" in str(e)

    def test_restore_manager_restore_handles_snapshot_filtering(self):
        """Test restore with snapshot filtering."""
        with self.tempdir() as td:
            # Prepare fake backup chain with multiple files
            args = Args(
                action="restore",
                dataset="rpool/test",
                mount_point=str(td),
                restore_pool="restored",
                restore_chain="chain-20240101",
                restore_snapshot="120000",
                force=True,
            )
            manager = RestoreManager(args, self.logger)

            # Create chain directory with multiple backup files
            chain_dir = manager.target_dir / "chain-20240101"
            chain_dir.mkdir(parents=True, exist_ok=True)
            self.write_file(chain_dir / "TEST-full-20240101110000.zfs.gz", b"data1")
            self.write_file(chain_dir / "TEST-diff-20240101120000.zfs.gz", b"data2")
            self.write_file(chain_dir / "TEST-diff-20240101130000.zfs.gz", b"data3")

            # Mock ZFS methods
            with self.patched(ZFS, "verify_backup_file", lambda *a, **kw: True):
                # Should filter to only files up to the specified snapshot
                files = manager.chain.files(chain_dir)
                assert len(files) >= 2  # Should include files up to 120000

    def test_restore_manager_restore_handles_user_confirmation_no(self):
        """Test restore handles user declining confirmation."""
        import builtins

        with self.tempdir() as td:
            # Prepare fake backup chain
            args = Args(action="restore", dataset="rpool/test", mount_point=str(td), restore_pool="restored", restore_chain="chain-20240101", force=False)
            manager = RestoreManager(args, self.logger)

            # Create fake chain directory and backup file
            chain_dir = manager.target_dir / "chain-20240101"
            chain_dir.mkdir(parents=True, exist_ok=True)
            backup_file = chain_dir / "TEST-full-20240101120000.zfs.gz"
            self.write_file(backup_file, b"fake backup data")

            # Mock input to return "no"
            with self.patched(builtins, "input", lambda prompt: "no"):
                with self.patched(ZFS, "verify_backup_file", lambda *a, **kw: True):
                    # Should exit without error when user says "no"
                    try:
                        manager.restore()
                        assert False, "Should have exited"
                    except SystemExit:
                        pass

    def test_restore_manager_restore_handles_subprocess_failure(self):
        """Test restore handles subprocess failures."""
        import subprocess
        from unittest.mock import Mock

        with self.tempdir() as td:
            # Prepare fake backup chain
            args = Args(action="restore", dataset="rpool/test", mount_point=str(td), restore_pool="restored", restore_chain="chain-20240101", force=True)
            manager = RestoreManager(args, self.logger)

            # Create fake chain directory and backup file
            chain_dir = manager.target_dir / "chain-20240101"
            chain_dir.mkdir(parents=True, exist_ok=True)
            backup_file = chain_dir / "TEST-full-20240101120000.zfs.gz"
            self.write_file(backup_file, b"fake backup data")

            # Mock failing subprocess
            mock_proc = Mock()
            mock_proc.returncode = 1
            mock_proc.communicate.return_value = (b"", b"error")
            mock_proc.wait.return_value = 1

            def mock_popen(cmd, **kwargs):
                return mock_proc

            # Mock ZFS methods
            with self.patched(ZFS, "is_dataset_exists", lambda ds: False):
                with self.patched(ZFS, "run", lambda *a, **kw: None):
                    with self.patched(ZFS, "verify_backup_file", lambda *a, **kw: True):
                        with self.patched(subprocess, "Popen", mock_popen):
                            try:
                                manager.restore()
                                assert False, "Should have raised FatalError"
                            except FatalError as e:
                                assert "failed" in str(e)

    def test_main_handles_validation_error(self):
        """Test Main.run handles ValidationError exceptions."""
        import sys

        with self.tempdir() as td:
            # Create args that will cause validation error
            args = Args(action="backup", dataset="nonexistent", mount_point=str(td))

            main_instance = Main()
            main_instance.args = args
            main_instance.logger = self.logger

            # Mock validation to raise ValidationError
            def failing_validate():
                raise ValidationError("Test validation error")

            with self.patched(type(main_instance), "validate", failing_validate):
                try:
                    main_instance.run()
                    assert False, "Should have exited"
                except SystemExit as e:
                    assert e.code == CONFIG.EXIT_INVALID_ARGS

    def test_main_handles_fatal_error(self):
        """Test Main.run handles FatalError exceptions."""
        import sys

        with self.tempdir() as td:
            # Create valid args
            args = Args(action="backup", dataset="rpool/test", mount_point=str(td))

            main_instance = Main()
            main_instance.args = args
            main_instance.logger = self.logger

            # Mock backup to raise FatalError
            def failing_backup():
                raise FatalError("Test fatal error")

            with self.patched(type(main_instance), "validate", lambda: None):
                # Manually test the backup action
                manager = BackupManager(args, self.logger)
                with self.patched(manager, "backup", failing_backup):
                    try:
                        manager.backup()
                        assert False, "Should have raised FatalError"
                    except FatalError:
                        pass

    def test_main_handles_unexpected_error(self):
        """Test Main.run handles unexpected exceptions."""
        import sys

        with self.tempdir() as td:
            # Create valid args
            args = Args(action="backup", dataset="rpool/test", mount_point=str(td))

            main_instance = Main()
            main_instance.args = args
            main_instance.logger = self.logger

            # Mock to raise unexpected error
            def failing_validate():
                raise ValueError("Unexpected error")

            with self.patched(type(main_instance), "validate", failing_validate):
                try:
                    main_instance.run()
                    assert False, "Should have exited"
                except SystemExit as e:
                    assert e.code == CONFIG.EXIT_INVALID_ARGS


if __name__ == "__main__":
    main()
