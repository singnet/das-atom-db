#!/bin/bash

which metta 2>&1>/dev/null || {
cat <<EOF
metta not found in PATH, please install hyperon package:

  pip install hyperon

EOF
exit 1
}

metta --version | grep -E '^(0\.1\.1[2-9]|0\.[2-9])' 2>&1>/dev/null || {
cat <<EOF
metta version 0.1.12 or higher is required
current installed version is $(metta --version), please upgrade:

  - using pip:
    pip install --upgrade hyperon
    check version: metta --version
  OR
  - building from source:
    clone the repository: https://github.com/trueagi-io/hyperon-experimental
    follow the instructions here: https://github.com/trueagi-io/hyperon-experimental/blob/main/README.md
    check version: metta --version

EOF
exit 1
}

echo -n "Running performance test using MeTTa's built-in Atom Space..."
time (metta ${PWD}/tests/performance/test_builtin.metta 2>&1>/dev/null)

echo "-----"

echo -n "Running performance test using DAS..."
time (metta ${PWD}/tests/performance/test_das_ram_only.metta 2>&1>/dev/null)

echo "-----"
