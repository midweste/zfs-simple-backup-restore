#!/usr/bin/env python3
import os
import sys
import shutil
import subprocess
import tempfile
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterable, Iterator, Sequence


class TestBase:
    def __init__(self) -> None:
        self._temp_dirs: list[str] = []
        # Provide a shared logger for all suites. Prefer the project's Logger; fall back to StdLogger.
        self.logger = self._make_logger()

    def _make_logger(self):
        # Try to import the project's Logger by adding project root to sys.path if necessary.
        # Project root is three directories up from this file.
        project_root = Path(__file__).parent.parent.parent
        if str(project_root) not in sys.path:
            sys.path.insert(0, str(project_root))

        try:
            from zfs_simple_backup_restore import Logger  # type: ignore

            return Logger(verbose=True)
        except Exception:

            class _StdLogger:
                def always(self, msg: str) -> None:
                    print(f"[INFO]  {msg}")

                def error(self, msg: str) -> None:
                    print(f"[ERROR] {msg}")

                # Compat for tests that might call info()
                def info(self, msg: str) -> None:
                    print(f"[INFO]  {msg}")

            return _StdLogger()

    # Minimal standardized output
    def test_result(self, description: str, ok: bool) -> None:
        """Print a single-line test result with an icon for clarity."""
        # Use a checkmark for pass and an X for fail for clearer output
        icon = "✓" if ok else "✖"
        status = "Pass" if ok else "Failed"
        print(f"{description} ... {icon} {status}")

    # Quiet subprocess runner (raises on error when check=True)
    def run_cmd(self, cmd: Sequence[str | int], check: bool = True) -> subprocess.CompletedProcess:
        """Run a command and return CompletedProcess.

        Args:
            cmd: sequence of command parts.
            check: if True, raise RuntimeError on non-zero exit.

        Returns:
            subprocess.CompletedProcess
        """

        cmd_str = [str(x) for x in cmd]
        result = subprocess.run(cmd_str, capture_output=True, text=True)
        if check and result.returncode != 0:
            raise RuntimeError(result.stderr.strip() or "Command failed")

        return result

    # Simple assertions (raise on failure)
    def assert_true(self, condition: bool, msg: str = "Assertion failed") -> None:
        if not condition:
            raise AssertionError(msg)

    def assert_equal(self, got, expected, msg: str | None = None) -> None:
        if got != expected:
            raise AssertionError(msg or f"Expected {expected!r}, got {got!r}")

    # Temp directory management
    def mktemp_dir(self, prefix: str = "testbase-") -> str:
        path = tempfile.mkdtemp(prefix=prefix)
        self._temp_dirs.append(path)
        return path

    def rm_dir(self, path: str) -> None:
        shutil.rmtree(path, ignore_errors=True)
        if path in self._temp_dirs:
            self._temp_dirs.remove(path)

    @contextmanager
    def tempdir(self, prefix: str = "testbase-") -> Iterator[Path]:
        """Context-managed temporary directory that is removed immediately on exit.

        Usage:
            with self.tempdir(prefix="case-") as tmp:
                # use tmp (pathlib.Path)
                ...
        """

        p = Path(self.mktemp_dir(prefix=prefix))
        try:
            yield p
        finally:
            self.rm_dir(str(p))

    @contextmanager
    def temp_chdir(self, path: str | Path):
        """Temporarily change CWD to `path` and restore it afterwards."""
        old = os.getcwd()
        os.chdir(str(path))
        try:
            yield
        finally:
            os.chdir(old)

    # ----- Convenience helpers for tests -----
    def make_chain_manager(self, prefix: str = "chain-temp-") -> tuple[Path, object]:
        """Create a temporary directory and return (Path, ChainManager instance).

        The temporary directory is registered for cleanup via TestBase.mktemp_dir.
        """
        tmp = Path(self.mktemp_dir(prefix=prefix))
        # Import here to avoid import-time requirements when TestBase is imported standalone
        from zfs_simple_backup_restore import ChainManager  # type: ignore

        return tmp, ChainManager(tmp, "TEST", self.logger)

    def create_chain_dirs(self, root: Path, names: list[str]) -> None:
        """Create chain directories under root with given names."""
        for n in names:
            (root / n).mkdir(parents=True, exist_ok=True)

    def write_file(self, path: Path, content: bytes | str) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        if isinstance(content, str):
            path.write_text(content)
        else:
            path.write_bytes(content)

    def assert_file_exists(self, path: Path, msg: str | None = None) -> None:
        if not path.exists():
            raise AssertionError(msg or f"Expected file to exist: {path}")

    def assert_file_not_exists(self, path: Path, msg: str | None = None) -> None:
        if path.exists():
            raise AssertionError(msg or f"Expected file to be absent: {path}")

    def assert_file_text_equal(self, path: Path, expected: str, msg: str | None = None) -> None:
        actual = path.read_text()
        if actual != expected:
            raise AssertionError(msg or f"File {path} content mismatch: expected {expected!r}, got {actual!r}")

    def set_logger_logfile(self, logger: Any, path: Path) -> None:
        """Prepare and attach a log file to a Logger instance for tests.

        This ensures parent directories exist, closes any existing logger.log_file,
        sets logger.log_file_path, and opens the file for append assigning
        logger.log_file to the file object.
        """
        path.parent.mkdir(parents=True, exist_ok=True)
        # Close any existing file handle if present
        try:
            lf = getattr(logger, "log_file", None)
            if lf:
                try:
                    lf.close()
                except Exception:
                    pass
        except Exception:
            pass

        logger.log_file_path = str(path)
        try:
            logger.log_file = open(path, "a")
        except Exception:
            logger.log_file = None

    def read_log(self, path: Path) -> str:
        """Return the contents of a log file as text."""
        return path.read_text()

    def assert_file_text_equal_stripped(self, path: Path, expected: str, msg: str | None = None) -> None:
        """Assert that file content equals expected after stripping whitespace/newlines."""
        actual = path.read_text().strip()
        exp = expected.strip()
        if actual != exp:
            raise AssertionError(msg or f"File {path} content mismatch (stripped): expected {exp!r}, got {actual!r}")

    # ZFS helpers
    def zfs_get(self, args: Iterable[str]) -> str:
        return self.run_cmd(["zfs", *args]).stdout.strip()

    def zpool(self, args: Iterable[str], check: bool = True) -> subprocess.CompletedProcess:
        return self.run_cmd(["zpool", *args], check=check)

    # Common CLI helpers
    def run_backup(self, dataset: str, backup_dir: str, description: str = "") -> None:
        """Invoke the project CLI to run a backup with typical flags."""
        cmd = [
            "python3",
            "zfs_simple_backup_restore.py",
            "--action",
            "backup",
            "--dataset",
            dataset,
            "--mount",
            backup_dir,
            "--interval",
            "7",
            "--retention",
            "3",
        ]
        self.run_cmd(cmd)

    def run_restore(
        self,
        dataset: str,
        backup_dir: str,
        restore_pool: str,
        chain_dir: str | os.PathLike | None = None,
        capture_output: bool = False,
    ) -> subprocess.CompletedProcess | None:
        """Invoke the project CLI to run a restore. If chain_dir is None, pick latest."""

        # Determine latest chain dir if not provided
        if chain_dir is None:
            dataset_dir = os.path.join(backup_dir, dataset.replace("/", "_"))
            chains = [d for d in os.listdir(dataset_dir) if d.startswith("chain-")]
            if not chains:
                raise RuntimeError("No chain directories found for restore")
            chain_dir = os.path.join(dataset_dir, sorted(chains)[-1])

        cmd = [
            "python3",
            "zfs_simple_backup_restore.py",
            "--action",
            "restore",
            "--dataset",
            str(dataset),
            "--mount",
            str(backup_dir),
            "--restore-pool",
            str(restore_pool),
            "--restore-chain",
            str(chain_dir),
            "--verbose",
            "--force",
        ]

        if capture_output:
            return subprocess.run([str(a) for a in cmd], capture_output=True, text=True)

        self.run_cmd(cmd)
        return None

    # Cleanup hook (temp dirs, etc.)
    def cleanup(self) -> None:
        for d in list(self._temp_dirs):
            self.rm_dir(d)

    # Simple monkeypatch helper
    @contextmanager
    def patched(self, obj: Any, attr: str, value: Any) -> Iterator[None]:
        """Temporarily set obj.attr to value and restore afterwards."""
        had_attr = hasattr(obj, attr)
        original = getattr(obj, attr, None)
        setattr(obj, attr, value)
        try:
            yield
        finally:
            if had_attr:
                setattr(obj, attr, original)
            else:
                # If the attribute didn't exist before, try to delete it; if deletion fails, restore to original
                try:
                    delattr(obj, attr)
                except Exception:
                    setattr(obj, attr, original)

    # Generic test runner: takes a list of (name, callable) and a logger with .always/.error
    def run_tests(self, tests: Sequence[tuple[str, Any]], logger: Any) -> bool:
        passed = 0
        failed = 0

        for name, fn in tests:
            try:
                fn()
                # Prefer structured logger; fall back to stdout
                msg = f"{name} ... ✓ Pass"
                try:
                    logger.always(msg)
                except Exception:
                    print(msg)
                passed += 1
            except Exception as e:
                msg = f"{name} ... ✖ Failed: {e}"
                try:
                    logger.error(msg)
                except Exception:
                    print(msg)
                failed += 1

        print(f"\n=== TEST RESULTS: {passed} passed, {failed} failed ===")
        return failed == 0

    # Auto-discovery of tests: run all instance methods named test_*
    def discover_tests(self) -> list[tuple[str, Any]]:
        tests: list[tuple[str, Any]] = []
        for name in dir(self):
            if not name.startswith("test_"):
                continue
            fn = getattr(self, name)
            if callable(fn):
                # Skip utility methods defined on TestBase itself
                if name in TestBase.__dict__:
                    continue
                tests.append((name, fn))
        # Sort by name for stable order
        tests.sort(key=lambda x: x[0])
        return tests

    def run_all(self, logger: Any | None = None) -> bool:
        tests = self.discover_tests()
        # Prefer provided logger, then instance attribute, then minimal stdout logger
        if logger is None:
            logger = getattr(self, "logger", None)
        if logger is None:

            class _StdLogger:
                def always(self, msg: str) -> None:
                    print(msg)

                def error(self, msg: str) -> None:
                    print(msg)

            logger = _StdLogger()
        return self.run_tests(tests, logger)

    # ===== Destructive-suite helpers =====
    def ensure_root_or_exit(self) -> None:
        if os.geteuid() != 0:
            print("ERROR: Destructive tests must be run as root")
            sys.exit(1)

    def load_zfs_module(self) -> None:
        self.run_cmd(["modprobe", "zfs"], check=False)

    def setup_test_pool(self, pool_name: str = "destructive_testpool", pool_file: str = "/tmp/destructive_test_pool.img") -> str:
        """Create a test pool with a data and data/subdir dataset, seed files, and return dataset name."""
        # Clean any existing
        self.run_cmd(["zpool", "destroy", pool_name], check=False)
        if os.path.exists(pool_file):
            os.unlink(pool_file)

        # Create pool
        self.run_cmd(["truncate", "-s", "1G", pool_file])
        self.run_cmd(["zpool", "create", pool_name, pool_file])

        # Create datasets
        self.run_cmd(["zfs", "create", f"{pool_name}/data"])
        self.run_cmd(["zfs", "create", f"{pool_name}/data/subdir"])

        # Seed files
        test_dir = f"/{pool_name}/data"
        os.makedirs(test_dir, exist_ok=True)
        test_files = {
            "test_file.txt": "This is test data for ZFS backup testing\n",
            "test_dir/subdir_file.txt": "Nested directory test file\n",
            "binary_test.bin": bytes([0x00, 0x01, 0x02, 0x03, 0x04, 0x05]),
        }
        for rel_path, content in test_files.items():
            file_path = os.path.join(test_dir, rel_path)
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            if isinstance(content, str):
                with open(file_path, "w") as f:
                    f.write(content)
            else:
                with open(file_path, "wb") as f:
                    f.write(content)

        # Verify creation
        for rel_path in test_files:
            path = os.path.join(test_dir, rel_path)
            if not os.path.exists(path):
                raise RuntimeError(f"Failed to create test file: {path}")

        return f"{pool_name}/data"

    def cleanup_test_pool(self, pool_name: str = "destructive_testpool", pool_file: str = "/tmp/destructive_test_pool.img") -> None:
        self.run_cmd(["zpool", "destroy", pool_name], check=False)
        if os.path.exists(pool_file):
            try:
                os.unlink(pool_file)
            except Exception:
                pass

    def destructive_env_setup(self) -> dict:
        """Common setup for destructive suites; returns a context dict."""
        self.ensure_root_or_exit()
        self.load_zfs_module()
        dataset = self.setup_test_pool()
        ctx = {
            "dataset": dataset,
            "backup_dir": self.mktemp_dir(prefix="destructive-backup-"),
            "restore_pool": "restored",
            "restore_pool_file": "/tmp/restored_pool.img",
        }
        return ctx

    def destructive_env_teardown(self, ctx: dict) -> None:
        self.run_cmd(["zpool", "destroy", ctx.get("restore_pool", "restored")], check=False)
        rpf = ctx.get("restore_pool_file")
        if rpf and os.path.exists(rpf):
            os.unlink(rpf)
        self.cleanup_test_pool()
        if "backup_dir" in ctx:
            shutil.rmtree(ctx["backup_dir"], ignore_errors=True)
        self.cleanup()
