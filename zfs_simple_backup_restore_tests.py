import tempfile
from pathlib import Path
import shutil
from zfs_simple_backup_restore import (
    Cmd,
    ChainManager,
    LockFile,
    Logger,
    ZFS,
    FatalError,
    CONFIG,
)


class ScriptTests:
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

        t("Cmd required_binaries", self.test_required_binaries)
        t("Cmd zfs binary detection", self.test_cmd_zfs_binary_detection)
        t("Cmd gzip binary detection", self.test_cmd_gzip_binary_detection)
        t("Cmd gzip prefers pigz or falls back", self.test_cmd_gzip_prefers)
        t("Cmd zfs/zpool command build", self.test_cmd_zfs_zpool)
        t("Cmd gunzip has -dc", self.test_cmd_gunzip)
        t("Cmd pv command build", self.test_cmd_pv)
        t("ChainManager today() format", self.test_chainmanager_today)
        t("ChainManager is_within_backup_dir", self.test_chainmanager_is_within_backup_dir)
        t("ChainManager files only nonempty", self.test_chainmanager_files_only_nonempty)
        t("ChainManager files returns sorted", self.test_chainmanager_files_sorted)
        t("ChainManager prune_old keeps N", self.test_chainmanager_prune_old)
        t("ChainManager chain_dir raises on missing", self.test_chainmanager_chain_dir_raises)
        t("ChainManager chain_dir returns latest", self.test_chainmanager_chain_dir_latest)
        t("LockFile locks/unlocks", self.test_lockfile_context)
        t("Logger logs (info/always/error)", self.test_logger_output)
        t("ZFS.is_dataset_exists true", self.test_zfs_is_dataset_exists_true)
        t("ZFS.is_dataset_exists false", self.test_zfs_is_dataset_exists_false)
        t("ZFS.run dry_run skips call", self.test_zfs_run_dry_run)
        t("ZFS.run real calls subprocess", self.test_zfs_run_real)
        t("ZFS.is_pool_exists true", self.test_zfs_is_pool_exists_true)
        t("ZFS.is_pool_exists false", self.test_zfs_is_pool_exists_false)
        t("ZFS.is_snapshot_exists true", self.test_zfs_is_snapshot_exists_true)
        t("ZFS.is_snapshot_exists false", self.test_zfs_is_snapshot_exists_false)
        t("ChainManager latest_chain_dir works", self.test_chainmanager_latest_chain_dir)
        t("ChainManager prune cleans temp files", self.test_chainmanager_prune_temp_files)
        t("Logger file logging works", self.test_logger_file_logging)
        t("CONFIG values sanity", self.test_config_values)
        print(f"\n=== TEST RESULTS: {passed} passed, {failed} failed ===")
        return failed == 0

    def test_required_binaries(self):
        Cmd.has_required_binaries(self.logger)

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
