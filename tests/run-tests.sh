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
vagrant ssh -c "RUN_TESTS=1 python3 /vagrant/tests/suites/test_non_destructive.py"
vagrant ssh -c "sudo modprobe zfs || true; cd /vagrant && sudo env RUN_TESTS=1 python3 tests/suites/test_destructive.py"
