#!/usr/bin/env python3
"""
Destructive integration tests for ZFS backup/restore tool.
These tests create real ZFS pools and datasets for testing.
"""

import subprocess
import tempfile
import os
import sys
import shutil
import gzip
from pathlib import Path


def test_result(description: str, ok: bool) -> None:
    status = "Pass" if ok else "Failed"
    print(f"{description} ... {status}")


def run_cmd(cmd, check=True):
    """Run a command and return result"""
    # Convert all command arguments to strings
    cmd_str = [str(arg) for arg in cmd]
    # no-op for quiet output
    result = subprocess.run(cmd_str, capture_output=True, text=True)
    if check and result.returncode != 0:
        # Raise to be handled by the caller/main for standardized failure output
        raise RuntimeError(result.stderr.strip() or "Command failed")
    return result


def setup_test_pool():
    """Create a test ZFS pool using a file-based vdev"""
    # Create a 1GB file for the pool
    pool_file = "/tmp/destructive_test_pool.img"
    pool_name = "destructive_testpool"

    # Clean up any existing pool first
    run_cmd(["zpool", "destroy", pool_name], check=False)
    if os.path.exists(pool_file):
        os.unlink(pool_file)

    # Create the pool file and pool
    run_cmd(["truncate", "-s", "1G", pool_file])
    run_cmd(["zpool", "create", pool_name, pool_file])

    # Create test datasets
    run_cmd(["zfs", "create", f"{pool_name}/data"])
    run_cmd(["zfs", "create", f"{pool_name}/data/subdir"])

    # Add some test data
    test_dir = f"/{pool_name}/data"
    os.makedirs(test_dir, exist_ok=True)

    # Create test files with content
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
        else:  # binary data
            with open(file_path, "wb") as f:
                f.write(content)

    # Verify files were created
    for rel_path in test_files:
        path = os.path.join(test_dir, rel_path)
        if not os.path.exists(path):
            raise RuntimeError(f"Failed to create test file: {path}")

    # quiet
    return f"{pool_name}/data"


def cleanup_test_pool():
    """Clean up the test pool"""
    pool_name = "destructive_testpool"
    pool_file = "/tmp/destructive_test_pool.img"

    # Destroy the test pool if it exists
    run_cmd(["zpool", "destroy", pool_name], check=False)

    # Remove the pool file
    if os.path.exists(pool_file):
        try:
            os.unlink(pool_file)
        except Exception:
            pass  # quiet


def test_full_backup_and_restore(ctx: dict) -> bool:
    try:
        run_backup(ctx["dataset"], ctx["backup_dir"], "Initial backup")
        dataset_dir = os.path.join(ctx["backup_dir"], ctx["dataset"].replace("/", "_"))
        chain_dirs = [d for d in os.listdir(dataset_dir) if d.startswith("chain-")]
        if not chain_dirs:
            raise RuntimeError("No chain directory found after backup")
        ctx["chain_dir"] = Path(os.path.join(dataset_dir, sorted(chain_dirs)[-1]))

        # Create restore pool and restore full
        run_cmd(["zpool", "destroy", ctx["restore_pool"]], check=False)
        run_cmd(["truncate", "-s", "1G", ctx["restore_pool_file"]])
        run_cmd(["zpool", "create", ctx["restore_pool"], ctx["restore_pool_file"]])
        run_restore(ctx["dataset"], ctx["backup_dir"], ctx["restore_pool"], ctx["chain_dir"], capture_output=False)

        # Mount restored and verify initial contents
        ctx["restored_dataset"] = f"{ctx['restore_pool']}/{os.path.basename(ctx['dataset'])}"
        ctx["mount_point"] = run_cmd(["zfs", "get", "-H", "-o", "value", "mountpoint", ctx["restored_dataset"]]).stdout.strip()
        os.makedirs(ctx["mount_point"], exist_ok=True)
        if run_cmd(["zfs", "get", "-H", "-o", "value", "mounted", ctx["restored_dataset"]]).stdout.strip() != "yes":
            run_cmd(["zfs", "mount", ctx["restored_dataset"]])

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
        test_result("Full backup verification", True)
        return True
    except Exception:
        test_result("Full backup verification", False)
        return False


def test_incremental_backup(ctx: dict) -> bool:
    try:
        src_mount = f"/{ctx['dataset']}"
        with open(os.path.join(src_mount, "test_file.txt"), "w") as f:
            f.write("test data for backup verification")
        os.makedirs(os.path.join(src_mount, "subdir"), exist_ok=True)
        with open(os.path.join(src_mount, "subdir", "test2.txt"), "w") as f:
            f.write("test data 2 for backup verification")
        run_backup(ctx["dataset"], ctx["backup_dir"], "Incremental backup")
        test_result("Incremental backup", True)
        return True
    except Exception:
        test_result("Incremental backup", False)
        return False

def test_incremental_restore(ctx: dict) -> bool:
    try:
        # Recreate restore pool and restore again
        run_cmd(["zpool", "destroy", ctx["restore_pool"]], check=False)
        if os.path.exists(ctx["restore_pool_file"]):
            os.unlink(ctx["restore_pool_file"])
        run_cmd(["truncate", "-s", "1G", ctx["restore_pool_file"]])
        run_cmd(["zpool", "create", ctx["restore_pool"], ctx["restore_pool_file"]])
        run_restore(ctx["dataset"], ctx["backup_dir"], ctx["restore_pool"])

        # Verify updated contents
        restored_dataset = f"{ctx['restore_pool']}/{os.path.basename(ctx['dataset'])}"
        mount_point = run_cmd(["zfs", "get", "-H", "-o", "value", "mountpoint", restored_dataset]).stdout.strip()
        os.makedirs(mount_point, exist_ok=True)
        if run_cmd(["zfs", "get", "-H", "-o", "value", "mounted", restored_dataset]).stdout.strip() != "yes":
            run_cmd(["zfs", "mount", restored_dataset])
        checks = [
            ("test_file.txt", "test data for backup verification"),
            ("subdir/test2.txt", "test data 2 for backup verification"),
        ]
        for rel, exp in checks:
            with open(os.path.join(mount_point, rel), "r") as f:
                if f.read().strip() != exp:
                    raise RuntimeError(f"Content mismatch in {rel}")
        test_result("Incremental restore verification", True)
        # Cleanup mount/dataset
        run_cmd(["zfs", "umount", restored_dataset], check=False)
        run_cmd(["zfs", "destroy", f"{ctx['restore_pool']}/data"], check=False)
        return True
    except Exception:
        test_result("Incremental restore verification", False)
        return False


def run_backup(dataset, backup_dir, description):
    """Helper function to run a backup with common parameters"""
    # quiet
    cmd = ["python3", "zfs_simple_backup_restore.py", "--action", "backup", "--dataset", dataset, "--mount", backup_dir, "--interval", "7", "--retention", "3"]
    run_cmd(cmd)


def run_restore(dataset, backup_dir, restore_pool, chain_dir=None, capture_output=False):
    """Helper function to run a restore operation"""
    # quiet

    # If chain_dir is not provided, find the latest chain directory
    if chain_dir is None:
        dataset_dir = os.path.join(backup_dir, dataset.replace("/", "_"))
        chain_dirs = sorted([d for d in os.listdir(dataset_dir) if d.startswith("chain-")])
        if not chain_dirs:
            raise Exception("No chain directories found for restore")
        chain_dir = os.path.join(dataset_dir, chain_dirs[-1])

    # quiet

    # Convert all arguments to strings to avoid PosixPath issues
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
        "--verbose",  # Add verbose flag for more detailed output
        "--force",  # Non-interactive restore
    ]

    cmd_str = [str(arg) for arg in cmd]

    if capture_output:
        # No interactive input needed due to --force
        result = subprocess.run(cmd_str, capture_output=True, text=True)
        return result
    else:
        run_cmd(cmd)
        return None


def main():
    """Run destructive integration tests"""

    # Check if we're running as root
    if os.geteuid() != 0:
        print("ERROR: Destructive tests must be run as root")
        sys.exit(1)

    # Load ZFS kernel module if needed
    run_cmd(["modprobe", "zfs"], check=False)

    # Orchestrate tests
    ctx = {
        "dataset": setup_test_pool(),
        "backup_dir": tempfile.mkdtemp(),
        "restore_pool": "restored",
        "restore_pool_file": "/tmp/restored_pool.img",
    }
    try:
        ok1 = test_full_backup_and_restore(ctx)
        ok2 = test_incremental_backup(ctx)
        ok3 = test_incremental_restore(ctx)
        if not (ok1 and ok2 and ok3):
            test_result("Destructive tests", False)
            sys.exit(1)
    finally:
        # Cleanup environment
        run_cmd(["zpool", "destroy", ctx["restore_pool"]], check=False)
        if os.path.exists(ctx["restore_pool_file"]):
            os.unlink(ctx["restore_pool_file"])
        cleanup_test_pool()
        shutil.rmtree(ctx["backup_dir"], ignore_errors=True)


if __name__ == "__main__":
    main()
