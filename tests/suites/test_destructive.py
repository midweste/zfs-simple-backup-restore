#!/usr/bin/env python3
"""Destructive integration tests for ZFS backup/restore tool.

These tests create real ZFS pools and datasets for testing.
"""

import os
import sys
from pathlib import Path

from test_base import TestBase


class DestructiveTests(TestBase):
    def __init__(self) -> None:
        super().__init__()
        self.ctx: dict = {}

    def test_destructive_full_backup_and_restore(self) -> None:
        ctx = self.ctx
        self.run_backup(ctx["dataset"], ctx["backup_dir"], "Initial backup")
        dataset_dir = os.path.join(ctx["backup_dir"], ctx["dataset"].replace("/", "_"))
        chain_dirs = [d for d in os.listdir(dataset_dir) if d.startswith("chain-")]
        if not chain_dirs:
            raise RuntimeError("No chain directory found after backup")
        ctx["chain_dir"] = Path(os.path.join(dataset_dir, sorted(chain_dirs)[-1]))

        # Create restore pool and restore full
        self.run_cmd(["zpool", "destroy", ctx["restore_pool"]], check=False)
        self.run_cmd(["truncate", "-s", "1G", ctx["restore_pool_file"]])
        self.run_cmd(["zpool", "create", ctx["restore_pool"], ctx["restore_pool_file"]])
        self.run_restore(ctx["dataset"], ctx["backup_dir"], ctx["restore_pool"], ctx["chain_dir"], capture_output=False)

        # Mount restored and verify initial contents
        ctx["restored_dataset"] = f"{ctx['restore_pool']}/{os.path.basename(ctx['dataset'])}"
        ctx["mount_point"] = self.run_cmd(["zfs", "get", "-H", "-o", "value", "mountpoint", ctx["restored_dataset"]]).stdout.strip()
        os.makedirs(ctx["mount_point"], exist_ok=True)
        if self.run_cmd(["zfs", "get", "-H", "-o", "value", "mounted", ctx["restored_dataset"]]).stdout.strip() != "yes":
            self.run_cmd(["zfs", "mount", ctx["restored_dataset"]])

        # Presence
        for rel in ["test_file.txt", "test_dir/subdir_file.txt", "binary_test.bin"]:
            if not os.path.exists(os.path.join(ctx["mount_point"], rel)):
                raise RuntimeError(f"Restored file not found: {os.path.join(ctx['mount_point'], rel)}")
        # Content
        expected_initial = {
            "test_file.txt": "This is test data for ZFS backup testing\n",
            "test_dir/subdir_file.txt": "Nested directory test file\n",
        }
        for rel, exp in expected_initial.items():
            with open(os.path.join(ctx["mount_point"], rel), "r") as f:
                if f.read() != exp:
                    raise RuntimeError(f"Content mismatch in {rel}")
        with open(os.path.join(ctx["mount_point"], "binary_test.bin"), "rb") as f:
            if f.read() != bytes([0x00, 1, 2, 3, 4, 5]):
                raise RuntimeError("Binary content mismatch")

    def test_destructive_incremental_backup(self) -> None:
        ctx = self.ctx
        src_mount = f"/{ctx['dataset']}"
        with open(os.path.join(src_mount, "test_file.txt"), "w") as f:
            f.write("test data for backup verification")
        os.makedirs(os.path.join(src_mount, "subdir"), exist_ok=True)
        with open(os.path.join(src_mount, "subdir", "test2.txt"), "w") as f:
            f.write("test data 2 for backup verification")
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
        mount_point = self.run_cmd(["zfs", "get", "-H", "-o", "value", "mountpoint", restored_dataset]).stdout.strip()
        os.makedirs(mount_point, exist_ok=True)
        if self.run_cmd(["zfs", "get", "-H", "-o", "value", "mounted", restored_dataset]).stdout.strip() != "yes":
            self.run_cmd(["zfs", "mount", restored_dataset])
        checks = [
            ("test_file.txt", "test data for backup verification"),
            ("subdir/test2.txt", "test data 2 for backup verification"),
        ]
        for rel, exp in checks:
            with open(os.path.join(mount_point, rel), "r") as f:
                if f.read().strip() != exp:
                    raise RuntimeError(f"Content mismatch in {rel}")
        # Cleanup mount/dataset
        self.run_cmd(["zfs", "umount", restored_dataset], check=False)
        self.run_cmd(["zfs", "destroy", f"{ctx['restore_pool']}/data"], check=False)


def main():
    """Run destructive integration tests via auto-discovery"""
    tests = DestructiveTests()
    try:
        ctx = tests.destructive_env_setup()
        tests.ctx = ctx
        ok = tests.run_all()
        if not ok:
            tests.test_result("Destructive tests", False)
            sys.exit(1)
    finally:
        tests.destructive_env_teardown(tests.ctx)


if __name__ == "__main__":
    main()
