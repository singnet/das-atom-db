#!/bin/bash

# This script is used to build a wheel for the current package.

TMP_DEST_DIR="tmp_dist"
DEST_DIR="dist"
rm -rf ${TMP_DEST_DIR} ${DEST_DIR}
mkdir -p ${TMP_DEST_DIR} ${DEST_DIR}
${PYTHON_EXECUTABLE} -m pip wheel . --wheel-dir ${TMP_DEST_DIR}

find ${TMP_DEST_DIR} -type f -name "*.whl" \
  -exec ${PYTHON_EXECUTABLE} -m auditwheel repair {} -w ${DEST_DIR} \;

chown -R ${_USER}:${_GROUP} ${DEST_DIR}

# Clean up
rm -rf ${TMP_DEST_DIR}


