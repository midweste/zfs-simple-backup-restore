#!/usr/bin/env bash
set -euo pipefail

# Run non-destructive tests in Vagrant VM
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

# Execute the non-destructive tests
vagrant ssh -c "python3 /vagrant/tests/suites/test_non_destructive.py"

if $DESTROY_AFTER; then
  vagrant destroy -f
fi
