#!/usr/bin/env bash
set -euo pipefail

# Run tests in Vagrant VM (non-destructive first, then destructive)
# Usage: tests/run-vagrant-tests.sh [--destroy]
#  --destroy    Destroy the VM after tests complete

cd "$(dirname "$0")"

DESTROY=false
for arg in "$@"; do
  case "$arg" in
    --destroy) DESTROY=true ;;
  esac
done

if $DESTROY; then
  vagrant destroy -f
fi
vagrant up

set -o pipefail

echo "=== Running non-destructive tests ==="
vagrant ssh -c "python3 /vagrant/tests/suites/test_non_destructive.py"

echo "=== Running destructive tests ==="
# Ensure ZFS module is available in the guest and run destructive tests with sudo
vagrant ssh -c "sudo modprobe zfs || true; cd /vagrant && sudo python3 tests/suites/test_destructive.py"


