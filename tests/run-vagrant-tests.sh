#!/usr/bin/env bash
set -euo pipefail

# Run tests in Vagrant VM (non-destructive first, then destructive)
# Usage: tests/run-vagrant-tests.sh [--provision] [--destroy]
#  --provision  Force reprovision of the VM
#  --destroy    Destroy the VM after tests complete

cd "$(dirname "$0")"

REPROVISION=false
DESTROY_AFTER=false
for arg in "$@"; do
  case "$arg" in
    --provision) REPROVISION=true ;;
    --destroy) DESTROY_AFTER=true ;;
  esac
done

if $REPROVISION; then
  vagrant up --provision
else
  vagrant up
fi

set -o pipefail

echo "=== Running non-destructive tests ==="
vagrant ssh -c "python3 /vagrant/tests/suites/test_non_destructive.py"

echo "=== Running destructive tests ==="
# Ensure ZFS module is available in the guest and run destructive tests with sudo
vagrant ssh -c "sudo modprobe zfs || true; cd /vagrant && sudo python3 tests/suites/test_destructive.py"

if $DESTROY_AFTER; then
  vagrant destroy -f
fi
