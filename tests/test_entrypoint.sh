#!/bin/bash
set -e

# Track test results
declare -A test_results

run_tests() {
    local test_type=$1
    local test_file=$2
    
    echo -n "Running $test_type tests... "
    
    # Run tests and capture output
    if python3 -m unittest $test_file &>/dev/null; then
        echo "✅ PASSED"
        test_results["$test_type"]=0
        return 0
    else
        echo "❌ FAILED"
        # Show detailed output only if tests fail
        echo "=== Test Output ==="
        python3 -m unittest $test_file
        test_results["$test_type"]=1
        return 1
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
    if ! run_tests "Non-destructive" "tests/suites/test_non_destructive.py"; then
        all_passed=1
    fi
fi

# Run destructive tests if requested
if [ $RUN_DESTRUCTIVE -eq 1 ]; then
    # Check if ZFS is available
    if ! command -v zfs >/dev/null 2>&1; then
        echo "ERROR: ZFS is not available in the container"
        all_passed=1
    else
        echo -n "Setting up ZFS test environment... "
        # Set up test ZFS pool using a file-based vdev
        POOL_DIR=/testpool_root
        mkdir -p $POOL_DIR
        
        # Create a sparse file for the pool
        dd if=/dev/zero of=$POOL_DIR/testpool.img bs=1M count=0 seek=1024 &>/dev/null
        
        # Create a loop device for the file
        LOOP_DEV=$(losetup -f)
        losetup $LOOP_DEV $POOL_DIR/testpool.img 2>/dev/null || true
        
        # Create the pool
        if ! zpool create -f -O mountpoint=/testpool testpool $LOOP_DEV &>/dev/null; then
            echo "❌ FAILED"
            echo "Failed to create test ZFS pool"
            all_passed=1
            if [ -n "$LOOP_DEV" ]; then
                losetup -d $LOOP_DEV 2>/dev/null || true
            fi
            rm -rf $POOL_DIR
        else
            echo "✅ READY"
            
            # Run the destructive tests
            if ! run_tests "Destructive" "tests/suites/test_destructive.py"; then
                all_passed=1
            fi
            
            # Clean up test pool
            echo -n "Cleaning up test environment... "
            zpool destroy testpool &>/dev/null || true
            if [ -n "$LOOP_DEV" ]; then
                losetup -d $LOOP_DEV 2>/dev/null || true
            fi
            rm -rf $POOL_DIR
            echo "✅ DONE"
        fi
    fi
fi

# Exit with appropriate status
if [ $all_passed -eq 0 ]; then
    echo "🎉 All tests completed successfully!"
    exit 0
else
    echo "❌ Some tests failed"
    exit 1
fi
