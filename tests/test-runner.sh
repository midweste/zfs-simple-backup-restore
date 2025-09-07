#!/bin/bash
set -e

echo "=== ZFS Backup/Restore Test Runner ==="
echo ""

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
    
    echo "\n=== Running $test_type Tests ==="
    if ! docker run --rm \
        --name zfs-test-runner \
        --privileged \
        --device=/dev/zfs \
        --device=/dev/loop-control \
        --device=/dev/loop0 \
        --device=/dev/loop1 \
        --device=/dev/loop2 \
        --tmpfs /run \
        --tmpfs /run/lock \
        -v /sys/fs/cgroup:/sys/fs/cgroup:ro \
        zfs-test-runner $cmd; then
        
        echo "❌ $test_type tests failed"
        return 1
    fi
    return 0
}

# Run non-destructive tests
if ! run_tests "Non-destructive" "non-destructive"; then
    exit 1
fi

# Run destructive tests
if ! run_tests "Destructive" "destructive"; then
    exit 1
fi

echo "\n✅ All tests completed successfully!"

