#!/bin/bash

# This script is used to build a wheel for the current package.

TMP_DEST_DIR="tmp_dist"
DEST_DIR="dist"

# Remove existing/old wheel files
rm -rf ${TMP_DEST_DIR}
rm -f ${DEST_DIR}/*manylinux*.whl

# Create dirs
mkdir -p ${TMP_DEST_DIR} ${DEST_DIR}
[ $? -ne 0 ] && exit 1

# Build wheel
${PYTHON_EXECUTABLE} -m pip wheel . --wheel-dir ${TMP_DEST_DIR}
[ $? -ne 0 ] && exit 1

# Repair wheel - tag as manylinux
find ${TMP_DEST_DIR} -type f -name "*.whl" \
  -exec ${PYTHON_EXECUTABLE} -m auditwheel repair {} -w ${DEST_DIR} \;
[ $? -ne 0 ] && exit 1

# Change ownership
chown -R ${_USER}:${_GROUP} ${DEST_DIR}
[ $? -ne 0 ] && exit 1

# Clean up
[ -d ${TMP_DEST_DIR} ] && rm -rf ${TMP_DEST_DIR}

exit 0
