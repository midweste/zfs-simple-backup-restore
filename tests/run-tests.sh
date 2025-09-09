#!/usr/bin/env bash
set -euo pipefail

# Run tests in Vagrant VM (non-destructive first, then destructive)
# Usage: tests/run-tests.sh [--destroy] [--verbose]
#  --destroy  Destroy the VM before starting (fresh run)
#  --verbose  Enable verbose output in destructive tests (--verbose-tests)

cd "$(dirname "$0")"

DESTROY=false
VERBOSE=false
for arg in "$@"; do
  case "$arg" in
    --destroy) DESTROY=true ;;
    --verbose) VERBOSE=true ;;
    *) echo "Unknown option: $arg" >&2; exit 2 ;;
  esac
done

if $DESTROY; then
  vagrant destroy -f || true
fi

VERBOSE_FLAG=""
if $VERBOSE; then
  VERBOSE_FLAG="--verbose"
fi

vagrant up

set -o pipefail
echo "=== Running non-destructive tests ==="
vagrant ssh -c "python3 /vagrant/tests/suites/test_non_destructive.py"
echo "=== Running destructive tests ==="
vagrant ssh -c "sudo modprobe zfs || true; cd /vagrant && sudo python3 tests/suites/test_destructive.py ${VERBOSE_FLAG}"
