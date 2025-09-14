#!/usr/bin/env bash
set -euo pipefail

# Wrapper script to run tests with coverage analysis in Vagrant VM
# Usage: ./run-coverage.sh [--html] [--xml]

cd "$(dirname "$0")"

HTML=false
XML=false

for arg in "$@"; do
  case "$arg" in
    --html) HTML=true ;;
    --xml) XML=true ;;
    *) echo "Unknown option: $arg" >&2; exit 2 ;;
  esac
done

echo "=== Running tests with coverage in Vagrant VM ==="

# Run tests with coverage
vagrant ssh -c "cd /vagrant && sudo -E env RUN_TESTS=1 PYTHONPATH=/vagrant:/vagrant/tests/suites python3 -m coverage run --source=zfs_simple_backup_restore tests/suites/tests.py"

# Generate coverage report
if $HTML; then
  echo "=== Generating HTML coverage report ==="
  vagrant ssh -c "cd /vagrant && python3 -m coverage html"
  echo "HTML report generated in /vagrant/htmlcov/"
fi

if $XML; then
  echo "=== Generating XML coverage report ==="
  vagrant ssh -c "cd /vagrant && python3 -m coverage xml"
  echo "XML report generated as /vagrant/coverage.xml"
fi

# Always generate text report
echo "=== Coverage Report ==="
vagrant ssh -c "cd /vagrant && python3 -m coverage report"

echo "=== Coverage data saved to /vagrant/.coverage ==="
