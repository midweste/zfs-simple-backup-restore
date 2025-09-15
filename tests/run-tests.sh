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
# Run the test suite under coverage as root inside the VM so destructive tests can access ZFS
# Use sudo -E to preserve the environment variables (RUN_TESTS, PYTHONPATH)
vagrant ssh -c "cd /vagrant && sudo -E env RUN_TESTS=1 PYTHONPATH=/vagrant:/vagrant/tests/suites bash -lc 'python3 -m coverage run --source=zfs_simple_backup_restore tests/suites/tests.py'"

echo
echo "=== Coverage: missing lines ==="
vagrant ssh -c "cd /vagrant && sudo -E env PYTHONPATH=/vagrant:/vagrant/tests/suites bash -lc 'python3 -m coverage report --skip-covered --show-missing'"
