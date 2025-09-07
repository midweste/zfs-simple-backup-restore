#!/usr/bin/env python3
"""
Destructive integration tests for ZFS backup/restore tool.
These tests create real ZFS pools and datasets for testing.
"""

import subprocess
import tempfile
import os
import sys
from pathlib import Path

def run_cmd(cmd, check=True):
    """Run a command and return result"""
    print(f"Running: {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=True, text=True)
    if check and result.returncode != 0:
        print(f"Command failed: {result.stderr}")
        sys.exit(1)
    return result

def setup_test_pool():
    """Create a test ZFS pool using a file-based vdev"""
    # Create a 1GB file for the pool
    pool_file = "/tmp/test_pool.img"
    run_cmd(["truncate", "-s", "1G", pool_file])
    
    # Create the test pool
    run_cmd(["zpool", "create", "testpool", pool_file])
    
    # Create test datasets
    run_cmd(["zfs", "create", "testpool/data"])
    run_cmd(["zfs", "create", "testpool/data/subdir"])
    
    # Add some test data
    test_dir = "/testpool/data"
    os.makedirs(test_dir, exist_ok=True)
    with open(f"{test_dir}/test_file.txt", "w") as f:
        f.write("This is test data for ZFS backup testing\n")
    
    print("Test pool 'testpool' created successfully")
    return "testpool/data"

def cleanup_test_pool():
    """Clean up the test pool"""
    try:
        run_cmd(["zpool", "destroy", "testpool"], check=False)
        os.unlink("/tmp/test_pool.img")
        print("Test pool cleaned up")
    except:
        pass

def test_backup_restore_cycle():
    """Test a complete backup and restore cycle"""
    dataset = setup_test_pool()
    backup_dir = tempfile.mkdtemp()
    
    try:
        # Test backup
        print("\n=== Testing Backup ===")
        backup_cmd = [
            "python3", "zfs_simple_backup_restore.py",
            "--action", "backup",
            "--dataset", dataset,
            "--mount", backup_dir,
            "--interval", "7",
            "--retention", "1"
        ]
        run_cmd(backup_cmd)
        
        # Verify backup files were created
        chain_dirs = list(Path(backup_dir).glob("chain-*"))
        if not chain_dirs:
            raise Exception("No backup chain directories found")
        
        backup_files = list(chain_dirs[0].glob("*.zfs.gz"))
        if not backup_files:
            raise Exception("No backup files found in chain directory")
        
        print(f"Backup successful: {len(backup_files)} files created")
        
        # Test restore to a new pool
        print("\n=== Testing Restore ===")
        restore_cmd = [
            "python3", "zfs_simple_backup_restore.py", 
            "--action", "restore",
            "--dataset", dataset,
            "--mount", backup_dir,
            "--restore-pool", "restored"
        ]
        run_cmd(restore_cmd)
        
        # Verify restored data
        restored_file = "/restored/data/test_file.txt"
        if not os.path.exists(restored_file):
            raise Exception("Restored file not found")
        
        with open(restored_file, "r") as f:
            content = f.read()
            if "test data" not in content:
                raise Exception("Restored file content is incorrect")
        
        print("Restore successful: data verified")
        
        # Clean up restored pool
        run_cmd(["zpool", "destroy", "restored"], check=False)
        
    finally:
        cleanup_test_pool()
        # Clean up backup directory
        import shutil
        shutil.rmtree(backup_dir, ignore_errors=True)

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
