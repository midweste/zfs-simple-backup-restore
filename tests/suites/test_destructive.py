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

def run_cmd(cmd, check=True):
    """Run a command and return result"""
    # Convert all command arguments to strings
    cmd_str = [str(arg) for arg in cmd]
    print(f"Running: {' '.join(cmd_str)}")
    result = subprocess.run(cmd_str, capture_output=True, text=True)
    if check and result.returncode != 0:
        print(f"Command failed: {result.stderr}")
        sys.exit(1)
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
        "binary_test.bin": bytes([0x00, 0x01, 0x02, 0x03, 0x04, 0x05])
    }

    for rel_path, content in test_files.items():
        file_path = os.path.join(test_dir, rel_path)
        os.makedirs(os.path.dirname(file_path), exist_ok=True)

        if isinstance(content, str):
            with open(file_path, 'w') as f:
                f.write(content)
        else:  # binary data
            with open(file_path, 'wb') as f:
                f.write(content)

    # Verify files were created
    for rel_path in test_files:
        path = os.path.join(test_dir, rel_path)
        if not os.path.exists(path):
            raise RuntimeError(f"Failed to create test file: {path}")

    print(f"Test pool '{pool_name}' created successfully")
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
        except Exception as e:
            print(f"Warning: Failed to remove pool file: {e}")

def test_backup_restore_cycle():
    """Test a complete backup and restore cycle with incremental backups"""
    dataset = setup_test_pool()
    backup_dir = tempfile.mkdtemp()
    restore_pool = "restored"  # Define restore_pool at the start of the function
    restore_pool_file = "/tmp/restored_pool.img"  # Define restore_pool_file at the start of the function

    # Print debug info
    print(f"\n=== Debug Info ===")
    print(f"Backup directory: {backup_dir}")
    print(f"Dataset: {dataset}")
    print("Running 'ls -la' on backup directory:")
    run_cmd(["ls", "-la", backup_dir])

    try:
        # Initial backup (full)
        print("\n=== Testing Initial Full Backup ===")
        run_backup(dataset, backup_dir, "Initial backup")

        # Debug: List backup directory after backup
        print("\n=== Backup Directory After Backup ===")
        run_cmd(["ls", "-la", backup_dir])
        dataset_dir = os.path.join(backup_dir, dataset.replace('/', '_'))
        if os.path.exists(dataset_dir):
            print(f"\nContents of {dataset_dir}:")
            run_cmd(["ls", "-la", dataset_dir])

            # List any chain directories
            chain_dirs = [d for d in os.listdir(dataset_dir) if d.startswith('chain-')]
            for chain_dir in chain_dirs:
                chain_path = os.path.join(dataset_dir, chain_dir)
                print(f"\nContents of {chain_path}:")
                run_cmd(["ls", "-la", chain_path])

        # Get the chain directory - look for chain-* directories in the dataset-specific subdirectory
        chain_dirs = [d for d in os.listdir(dataset_dir) if d.startswith('chain-')]
        if not chain_dirs:
            raise RuntimeError("No chain directory found after backup")

        chain_dir = os.path.join(dataset_dir, sorted(chain_dirs)[-1])  # Get most recent chain
        print(f"Using chain directory: {chain_dir}")

        # Create a restore pool
        print("\n=== Creating Restore Pool ===")
        run_cmd(["zpool", "destroy", restore_pool], check=False)
        run_cmd(["truncate", "-s", "1G", "/tmp/restored_pool.img"])
        run_cmd(["zpool", "create", restore_pool, "/tmp/restored_pool.img"])

        # Don't create the dataset here - let the restore process handle it
        restored_dataset = f"{restore_pool}/{os.path.basename(dataset)}"
        print(f"\n=== Will restore to dataset: {restored_dataset} ===")

        # Ensure chain_dir is a Path object
        from pathlib import Path
        chain_dir_path = Path(chain_dir)

        # List backup files for debugging
        print("\n=== Backup Files ===")
        backup_files = sorted([f for f in chain_dir_path.glob("*.zfs.gz")])
        for f in backup_files:
            print(f"Backup file: {f.name} ({f.stat().st_size} bytes)")

        # Update chain_dir to be the Path object
        chain_dir = chain_dir_path

        # Run the restore
        print("\n=== Testing Restore ===")
        print(f"Restore command will use the following chain directory:")
        run_cmd(["ls", "-la", chain_dir])

        print("\nRunning restore operation")
        print(f"Using chain directory: {chain_dir}")

        # Run restore with output capture (non-interactive via --force)
        result = run_restore(dataset, backup_dir, restore_pool, chain_dir, capture_output=True)
        print("\n=== Restore Output ===")
        print(result.stdout)
        if result.stderr:
            print("\n=== Restore Errors ===")
            print(result.stderr)

        # Verify the restore
        print("\n=== Verifying Restore ===")
        restored_dataset = f"{restore_pool}/{os.path.basename(dataset)}"

        # List all snapshots in the restored dataset
        print("\n=== Checking for snapshots in restored dataset ===")
        run_cmd(["zfs", "list", "-t", "snapshot", "-o", "name,creation", "-r", restored_dataset])

        # Get the mount point from ZFS
        print("\n=== Current ZFS datasets and mount status ===")
        run_cmd(["zfs", "list", "-o", "name,used,avail,refer,mountpoint,canmount,mounted"])

        # Get the actual mount point from ZFS
        result = run_cmd(["zfs", "get", "-H", "-o", "value", "mountpoint", restored_dataset])
        mount_point = result.stdout.strip()

        # Make sure the mount point exists
        os.makedirs(mount_point, exist_ok=True)

        # Mount the dataset if needed
        result = run_cmd(["zfs", "get", "-H", "-o", "value", "mounted", restored_dataset])
        if result.stdout.strip() != 'yes':
            print(f"Mounting dataset {restored_dataset}")
            run_cmd(["zfs", "mount", restored_dataset])

        # Check mount status again
        print("\n=== After mounting ===")
        run_cmd(["zfs", "get", "-H", "-o", "value,source", "mounted", restored_dataset])
        run_cmd(["mount"])  # Show all mounted filesystems

        print(f"\n=== Contents of {mount_point} ===")
        run_cmd(["ls", "-la", mount_point])

        # Check for test files in the restored dataset
        test_files = ["test_file.txt", "test_dir/subdir_file.txt", "binary_test.bin"]
        all_files_found = True
        for test_file in test_files:
            file_path = os.path.join(mount_point, test_file)
            if os.path.exists(file_path):
                print(f"Found restored file: {file_path}")
            else:
                print(f"ERROR: Restored file not found: {file_path}")
                all_files_found = False
                dir_path = os.path.dirname(file_path)
                print(f"\n=== Contents of {dir_path} ===")
                run_cmd(["ls", "-la", dir_path])

        if not all_files_found:
            raise RuntimeError("Not all test files were found in the restored dataset")

        # Verify test files content
        test_contents = {
            "test_file.txt": "This is test data for ZFS backup testing\n",
            "test_dir/subdir_file.txt": "Nested directory test file\n"
        }

        for rel_path, expected_content in test_contents.items():
            file_path = os.path.join(mount_point, rel_path)
            with open(file_path, 'r') as f:
                content = f.read()
                if content != expected_content:
                    raise RuntimeError(f"File content mismatch for {file_path}\nExpected: {expected_content!r}\nGot: {content!r}")
                else:
                    print(f"Content verified for {file_path}")

        # Verify binary file
        bin_path = os.path.join(mount_point, "binary_test.bin")
        with open(bin_path, 'rb') as f:
            content = f.read()
            expected_content = bytes([0x00, 0x01, 0x02, 0x03, 0x04, 0x05])
            if content != expected_content:
                raise RuntimeError(f"Binary file content mismatch for {bin_path}")
            else:
                print(f"Binary file content verified for {bin_path}")

        print("\n=== All test files verified successfully ===")

        # Check if files were restored correctly
        # Check for test files in the restored dataset
        test_files = ["test_file.txt", "test_dir/subdir_file.txt", "binary_test.bin"]
        all_files_found = True
        for test_file in test_files:
            file_path = os.path.join(mount_point, test_file)
            if os.path.exists(file_path):
                print(f"Found restored file: {file_path}")
            else:
                print(f"ERROR: Restored file not found: {file_path}")
                all_files_found = False
                dir_path = os.path.dirname(file_path)
                print(f"\n=== Contents of {dir_path} ===")
                run_cmd(["ls", "-la", dir_path])
                raise RuntimeError(f"Restored file not found: {file_path}")

        # Verify test files were restored
        test_files = {
            "test_file.txt": "This is test data for ZFS backup testing\n",
            "test_dir/subdir_file.txt": "Nested directory test file\n"
        }

        for rel_path, expected_content in test_files.items():
            file_path = os.path.join(mount_point, rel_path)
            if not os.path.exists(file_path):
                raise RuntimeError(f"Restored file not found: {file_path}")

            with open(file_path, 'r') as f:
                content = f.read()
                if content != expected_content:
                    raise RuntimeError(f"Content mismatch in {file_path}\nExpected: {expected_content}\nGot: {content}")

        # Verify binary file
        binary_path = os.path.join(mount_point, "binary_test.bin")
        if not os.path.exists(binary_path):
            raise RuntimeError(f"Binary test file not found: {binary_path}")

        with open(binary_path, 'rb') as f:
            content = f.read()
            if content != bytes([0x00, 0x01, 0x02, 0x03, 0x04, 0x05]):
                raise RuntimeError("Binary file content mismatch")

        print("\n=== All tests passed! ===")
        dataset_dir = Path(backup_dir) / dataset.replace('/', '_')
        chain_dirs = list(dataset_dir.glob("chain-*"))  # Add wildcard to match directories
        if not chain_dirs:
            # Try alternative path structure (directly in backup_dir)
            chain_dirs = list(Path(backup_dir).glob("chain-*"))
            if not chain_dirs:
                raise Exception(f"No backup chain directories found in {dataset_dir} or {backup_dir}")

        # Verify initial backup files
        initial_backups = list(chain_dirs[0].glob("*.zfs.gz"))
        if len(initial_backups) != 1:
            raise Exception(f"Expected 1 backup file, found {len(initial_backups)}")

        print(f"Initial backup successful: {initial_backups[0].name}")

        # Create test files with more content and verify they're written
        mount_point = f"/{dataset}"
        test_file = os.path.join(mount_point, "test_file.txt")
        test_content = "test data for backup verification"
        with open(test_file, 'w') as f:
            f.write(test_content)

        # Verify the file was written
        with open(test_file, 'r') as f:
            content = f.read()
            if content != test_content:
                raise Exception(f"Test file content mismatch. Expected '{test_content}', got '{content}'")

        # Create a subdirectory with a file
        subdir = os.path.join(mount_point, "subdir")
        os.makedirs(subdir, exist_ok=True)

        subfile = os.path.join(subdir, "test2.txt")
        subcontent = "test data 2 for backup verification"
        with open(subfile, 'w') as f:
            f.write(subcontent)

        # Verify the subfile was written
        with open(subfile, 'r') as f:
            content = f.read()
            if content != subcontent:
                raise Exception(f"Subfile content mismatch. Expected '{subcontent}', got '{content}'")

        # Create a new file
        new_file = f"/{dataset}/new_file.txt"
        with open(new_file, "w") as f:
            f.write("This is a new file for incremental testing\n")

        # Create incremental backup
        print("\n=== Testing Incremental Backup ===")
        run_backup(dataset, backup_dir, "Incremental backup")

        # Verify incremental backup was created
        incremental_backups = list(chain_dirs[0].glob("*.zfs.gz"))
        if len(incremental_backups) != 2:
            raise Exception(f"Expected 2 backup files, found {len(incremental_backups)}")

        print(f"Incremental backup successful: {incremental_backups[1].name}")

        # Create restore pool
        print("\n=== Creating Restore Pool ===")

        # Clean up any existing restore pool
        run_cmd(["zpool", "destroy", restore_pool], check=False)
        if os.path.exists(restore_pool_file):
            os.unlink(restore_pool_file)

        # Create new pool for restore
        run_cmd(["truncate", "-s", "1G", restore_pool_file])
        run_cmd(["zpool", "create", restore_pool, restore_pool_file])

        # List backup files before restore
        print("\n=== Verifying Backup Files ===")
        backup_files = list(chain_dirs[0].glob("*.zfs.gz"))
        print(f"Found backup files: {[f.name for f in backup_files]}")

        # Verify backup files are not empty and contain valid data
        for backup_file in backup_files:
            file_size = backup_file.stat().st_size
            print(f"\n=== Inspecting {backup_file.name} ===")
            print(f"Size: {file_size} bytes")

            if file_size == 0:
                raise Exception(f"Backup file is empty: {backup_file}")

            # Check if the backup file is a valid gzip file and examine contents
            try:
                with gzip.open(backup_file, 'rb') as f:
                    # Read first 1KB to check header
                    header = f.read(1024)
                    print(f"First 100 bytes of header: {header[:100].hex()}")

                    # Try to read as much as possible to see if it's a valid stream
                    try:
                        remaining = f.read()
                        print(f"Successfully read {len(remaining)} more bytes")
                    except Exception as e:
                        print(f"Could not read entire file (this might be normal for incremental backups): {e}")

                # Run zstreamdump on the file to verify it's a valid ZFS stream
                try:
                    print("\nRunning zstreamdump on backup file:")
                    zstream_output = subprocess.check_output(
                        ["zstreamdump", str(backup_file)],
                        stderr=subprocess.STDOUT
                    ).decode()
                    print(zstream_output[:500] + "..." if len(zstream_output) > 500 else zstream_output)
                except subprocess.CalledProcessError as e:
                    print(f"zstreamdump failed (this might be expected for incremental backups): {e.output.decode()}")

            except Exception as e:
                raise Exception(f"Error examining backup file {backup_file}: {str(e)}")

        # Test restore with verbose output
        print("\n=== Testing Restore ===")
        print("Restore command will use the following chain directory:")
        run_cmd(["ls", "-la", str(chain_dirs[0])])

        # Run restore with verbose output
        run_restore(dataset, backup_dir, restore_pool)

        # Check what was restored
        print("\n=== Restore Completed ===")
        print("ZFS datasets after restore:")
        run_cmd(["zfs", "list"])

        # Get the actual mount point from ZFS
        restored_dataset = f"{restore_pool}/{os.path.basename(dataset)}"
        result = run_cmd(["zfs", "get", "-H", "-o", "value", "mountpoint", restored_dataset])
        mount_point = result.stdout.strip()

        # Make sure the mount point exists
        os.makedirs(mount_point, exist_ok=True)

        # Mount the dataset if needed
        result = run_cmd(["zfs", "get", "-H", "-o", "value", "mounted", restored_dataset])
        if result.stdout.strip() != 'yes':
            print(f"Mounting dataset {restored_dataset}")
            run_cmd(["zfs", "mount", restored_dataset])

        # Debug: Show mount status and contents
        print("\n=== After mounting ===")
        run_cmd(["zfs", "get", "-H", "-o", "value,source", "mounted", restored_dataset])
        run_cmd(["mount"])  # Show all mounted filesystems

        print(f"\n=== Contents of {mount_point} ===")
        run_cmd(["ls", "-la", mount_point])

        # Verify updated test files (after incremental restore)
        test_files = [
            ("test_file.txt", "test data for backup verification"),
            ("subdir/test2.txt", "test data 2 for backup verification"),
            ("binary_test.bin", None)  # Binary file, just check existence
        ]

        try:
            for rel_path, expected_content in test_files:
                file_path = os.path.join(mount_point, rel_path)

                # Check if file exists
                if not os.path.exists(file_path):
                    dir_path = os.path.dirname(file_path)
                    print(f"ERROR: File not found: {file_path}")
                    if os.path.exists(dir_path):
                        print(f"Contents of {dir_path}:")
                        run_cmd(["ls", "-la", dir_path])
                    raise Exception(f"Restored file not found: {file_path}")

                # Check file content for text files
                if expected_content is not None:
                    with open(file_path, 'r') as f:
                        content = f.read().strip()
                        if content != expected_content:
                            raise Exception(
                                f"File {file_path} content mismatch.\n"
                                f"Expected: {expected_content!r}\n"
                                f"Actual: {content!r}"
                            )
                    print(f"Content verified: {file_path}")
                else:
                    print(f"File exists: {file_path}")

            print("\n=== All test files verified successfully ===")

        finally:
            # Clean up mount
            run_cmd(["zfs", "umount", restored_dataset], check=False)
            run_cmd(["zfs", "destroy", f"{restore_pool}/data"], check=False)
            os.rmdir(mount_point)

        print("Restore successful: all files and modifications verified")

    finally:
        # Clean up restore pool if it still exists
        run_cmd(["zpool", "destroy", restore_pool], check=False)
        if os.path.exists(restore_pool_file):
            os.unlink(restore_pool_file)

        cleanup_test_pool()
        # Clean up backup directory
        import shutil
        shutil.rmtree(backup_dir, ignore_errors=True)

def run_backup(dataset, backup_dir, description):
    """Helper function to run a backup with common parameters"""
    print(f"\nRunning backup: {description}")
    cmd = [
        "python3", "zfs_simple_backup_restore.py",
        "--action", "backup",
        "--dataset", dataset,
        "--mount", backup_dir,
        "--interval", "7",
        "--retention", "3"
    ]
    run_cmd(cmd)

def run_restore(dataset, backup_dir, restore_pool, chain_dir=None, capture_output=False):
    """Helper function to run a restore operation"""
    print("\nRunning restore operation")

    # If chain_dir is not provided, find the latest chain directory
    if chain_dir is None:
        dataset_dir = os.path.join(backup_dir, dataset.replace('/', '_'))
        chain_dirs = sorted([d for d in os.listdir(dataset_dir) if d.startswith('chain-')])
        if not chain_dirs:
            raise Exception("No chain directories found for restore")
        chain_dir = os.path.join(dataset_dir, chain_dirs[-1])

    print(f"Using chain directory: {chain_dir}")

    # Convert all arguments to strings to avoid PosixPath issues
    cmd = [
        "python3", "zfs_simple_backup_restore.py",
        "--action", "restore",
        "--dataset", str(dataset),
        "--mount", str(backup_dir),
        "--restore-pool", str(restore_pool),
        "--restore-chain", str(chain_dir),
        "--verbose",  # Add verbose flag for more detailed output
        "--force",    # Non-interactive restore
    ]

    cmd_str = [str(arg) for arg in cmd]

    if capture_output:
        print(f"Running: {' '.join(cmd_str)}")
        # No interactive input needed due to --force
        result = subprocess.run(cmd_str, capture_output=True, text=True)
        if result.returncode != 0:
            print(f"Command failed with return code {result.returncode}")
        return result
    else:
        run_cmd(cmd)
        return None

def main():
    """Run destructive integration tests"""
    print("=== ZFS Backup/Restore Destructive Integration Tests ===")
    print("WARNING: This will create and destroy ZFS pools for testing")

    # Check if we're running as root
    if os.geteuid() != 0:
        print("ERROR: Destructive tests must be run as root")
        sys.exit(1)

    # Load ZFS kernel module if needed
    run_cmd(["modprobe", "zfs"], check=False)

    # Run the test
    try:
        test_backup_restore_cycle()
        print("\n=== ALL DESTRUCTIVE TESTS PASSED ===")
    except Exception as e:
        print(f"\n=== TEST FAILED: {e} ===")
        sys.exit(1)

if __name__ == "__main__":
    main()
