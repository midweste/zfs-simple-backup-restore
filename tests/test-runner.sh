#!/bin/bash
set -e

echo "=== ZFS Backup/Restore Test Runner ==="

# Build the test image
echo "Building test image..."
cd "$(dirname "$0")/.."
if ! docker build -t zfs-test-runner -f tests/Dockerfile .; then
    echo "❌ Failed to build test image"
    exit 1
fi

# Function to run tests in the container
run_tests() {
    local test_type=$1
    local cmd=$2
    
    echo -n "Running $test_type tests... "
    
    if docker run --rm -it \
        --privileged \
        -v /dev/zfs:/dev/zfs \
        -v /etc/machine-id:/etc/machine-id:ro \
        -v /etc/localtime:/etc/localtime:ro \
        -v /sys/fs/cgroup:/sys/fs/cgroup:ro \
        zfs-test-runner $cmd &>/dev/null; then
        echo "✅ PASSED"
        return 0
    else
        echo "❌ FAILED"
        # Show detailed output for failed tests
        echo "=== Test Output ==="
        docker run --rm -it \
            --privileged \
            -v /dev/zfs:/dev/zfs \
            -v /etc/machine-id:/etc/machine-id:ro \
            -v /etc/localtime:/etc/localtime:ro \
            -v /sys/fs/cgroup:/sys/fs/cgroup:ro \
            zfs-test-runner $cmd
        return 1
    fi
}

# Run non-destructive tests
if ! run_tests "Non-destructive" "non-destructive"; then
    exit 1
fi

# Run destructive tests
if ! run_tests "Destructive" "destructive"; then
    exit 1
fi

echo "✅ All tests completed successfully!"

