#!/usr/bin/env python3
"""Destructive integration tests for ZFS backup/restore tool.

These tests create real ZFS pools and datasets for testing.
"""

import os
import sys
from pathlib import Path

from test_base import TestBase

# Guard: only allow running this script when invoked via tests/run-tests.sh which sets RUN_TESTS=1
if os.environ.get("RUN_TESTS") != "1":
    print("ERROR: destructive tests must be run via tests/run-tests.sh (use that script to run tests)")
    sys.exit(2)


class DestructiveTests(TestBase):
    def __init__(self) -> None:
        super().__init__()
        self.ctx: dict = {}

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


def main():
    """Run destructive integration tests via auto-discovery"""
    tests = DestructiveTests()
    try:
        ctx = tests.destructive_env_setup()
        tests.ctx = ctx
        ok = tests.run_all(tests.logger)
        if not ok:
            tests.test_result("Destructive tests", False)
            sys.exit(1)
    finally:
        tests.destructive_env_teardown(tests.ctx)


if __name__ == "__main__":
    main()
