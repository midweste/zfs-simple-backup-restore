#!/bin/bash
set -e

# Track test results
declare -A test_results

run_tests() {
    local test_type=$1
    local test_file=$2
    
    echo "\n=== Running $test_type tests ==="
    
    # Run tests and capture output
    local output
    if ! output=$(python3 -m unittest $test_file 2>&1); then
        echo "$output"  # Show detailed test output
        echo "\n❌ $test_type tests failed"
        test_results["$test_type"]=1
        return 1
    else
        echo "$output"  # Show test results
        echo "\n✅ $test_type tests passed"
        test_results["$test_type"]=0
        return 0
    fi
}

# Default to running all tests
RUN_NON_DESTRUCTIVE=1
RUN_DESTRUCTIVE=1

# Parse command line arguments
case "$1" in
    "non-destructive")
        RUN_DESTRUCTIVE=0
        ;;
    "destructive")
        RUN_NON_DESTRUCTIVE=0
        ;;
    "bash")
        exec /bin/bash
        exit 0
        ;;
esac

# Track overall test status
all_passed=0

# Run non-destructive tests if requested
if [ $RUN_NON_DESTRUCTIVE -eq 1 ]; then
    echo "\n=== Running Non-destructive Tests ==="
    if ! run_tests "Non-destructive" "tests/suites/test_non_destructive.py"; then
        all_passed=1
    fi
fi

# Run destructive tests if requested
if [ $RUN_DESTRUCTIVE -eq 1 ]; then
    echo "\n=== Running Destructive Tests ==="
    # Check if ZFS is available
    if ! command -v zfs >/dev/null 2>&1; then
        echo "ERROR: ZFS is not available in the container"
        all_passed=1
    else
        # Set up test ZFS pool using a file-based vdev
        echo "Setting up test ZFS pool..."
        POOL_DIR=/testpool_root
        mkdir -p $POOL_DIR
        
        # Create a sparse file for the pool
        dd if=/dev/zero of=$POOL_DIR/testpool.img bs=1M count=0 seek=1024
        
        # Create a loop device for the file
        LOOP_DEV=$(losetup -f)
        losetup $LOOP_DEV $POOL_DIR/testpool.img
        
        # Create the pool
        if ! zpool create -f -O mountpoint=/testpool testpool $LOOP_DEV; then
            echo "❌ Failed to create test ZFS pool"
            losetup -d $LOOP_DEV 2>/dev/null || true
            all_passed=1
        else
            # Run destructive tests
            if ! run_tests "Destructive" "tests/suites/test_destructive.py"; then
                all_passed=1
            fi
            
            # Clean up test pool
            echo "Cleaning up test ZFS pool..."
            zpool destroy testpool
        fi
        
        # Clean up loop device and pool directory
        if [ -n "$LOOP_DEV" ]; then
            losetup -d $LOOP_DEV 2>/dev/null || true
        fi
        rm -rf $POOL_DIR
    fi
fi

# Exit with appropriate status
if [ $all_passed -eq 0 ]; then
    echo "\n🎉 All tests completed successfully!"
    exit 0
else
    echo "\n❌ Some tests failed"
    exit 1
fi
