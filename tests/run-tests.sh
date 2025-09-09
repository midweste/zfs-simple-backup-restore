#!/usr/bin/env bash
set -euo pipefail

# Run tests in Vagrant VM (non-destructive first, then destructive)
# Usage: tests/run-tests.sh [--destroy]
#  --destroy  Destroy the VM before starting (fresh run)

cd "$(dirname "$0")"

DESTROY=false
for arg in "$@"; do
  case "$arg" in
    --destroy) DESTROY=true ;;
    *) echo "Unknown option: $arg" >&2; exit 2 ;;
  esac
done

if $DESTROY; then
  vagrant destroy -f || true
fi

vagrant up

set -o pipefail
echo "=== Running test suite ==="
# Run non-destructive tests with PYTHONPATH pointing at the repo root inside the VM
vagrant ssh -c "cd /vagrant && env RUN_TESTS=1 PYTHONPATH=/vagrant python3 tests/suites/test_non_destructive.py"
# Run destructive tests as root (keep PYTHONPATH in the sudo environment)
vagrant ssh -c "sudo modprobe zfs || true; cd /vagrant && sudo env RUN_TESTS=1 PYTHONPATH=/vagrant python3 tests/suites/test_destructive.py"
