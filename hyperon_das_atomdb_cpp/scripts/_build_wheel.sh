#!/bin/bash

# This script is used to build a wheel for the current package.

TMP_DEST_DIR="tmp_dist"
DEST_DIR="dist"

rm -rf ${TMP_DEST_DIR} ${DEST_DIR}

mkdir -p ${TMP_DEST_DIR} ${DEST_DIR}
[ $? -ne 0 ] && exit 1

${PYTHON_EXECUTABLE} -m pip wheel . --wheel-dir ${TMP_DEST_DIR}
[ $? -ne 0 ] && exit 1

find ${TMP_DEST_DIR} -type f -name "*.whl" \
  -exec ${PYTHON_EXECUTABLE} -m auditwheel repair {} -w ${DEST_DIR} \;
[ $? -ne 0 ] && exit 1

chown -R ${_USER}:${_GROUP} ${DEST_DIR}
[ $? -ne 0 ] && exit 1

# Clean up
[ -d ${TMP_DEST_DIR} ] && rm -rf ${TMP_DEST_DIR}

exit 0
