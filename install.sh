#!/bin/bash
# housekeeper installation script

set -e

echo "=== housekeeper installer ==="
echo

# Check Python version
if ! command -v python3 &> /dev/null; then
    echo "Error: python3 not found"
    exit 1
fi

PYTHON_VERSION=$(python3 -c 'import sys; print(".".join(map(str, sys.version_info[:2])))')
echo "Found Python $PYTHON_VERSION"

# Check if we're on an HPC system
SCHEDULER="none"
if command -v sbatch &> /dev/null; then
    SCHEDULER="SLURM"
    echo "Detected SLURM scheduler"
elif command -v qsub &> /dev/null; then
    SCHEDULER="PBS"
    echo "Detected PBS scheduler"
else
    echo "Warning: No scheduler detected (sbatch or qsub not found)"
    echo "housekeeper will not work without a scheduler"
fi

echo

# Install
echo "Installing housekeeper..."
pip install -e . --quiet

# Test import
echo "Testing installation..."
python3 -c "from housekeeper import housekeeper; print('✓ Import successful')"

echo
echo "=== Installation complete ==="
echo
echo "Quick start:"
echo "  >>> from housekeeper import housekeeper"
echo "  >>> hk = housekeeper(workdir='./my_jobs')"
echo "  >>> job = hk.submit('echo hello', name='test')"
echo
echo "Documentation: README.md, USAGE.md"
echo "Scheduler detected: $SCHEDULER"
