#!/bin/bash -x

source $(dirname $0)/common.sh

# This script is used to build a wheel for the current package.

export CMAKE_PROJECT_VERSION=${PROJECT_VERSION}
bash -x $(dirname $0)/_build_atomdb_lib_with_glibc.sh
[ $? -ne 0 ] && exit 1

# Check if the RPM files are present
if [ ! -e ${LIB_DIST_DIR}/${LIB_NAME_PATTERN}*.rpm 2>&1 >/dev/null ]; then
    echo "Error: No RPM files found in ${LIB_DIST_DIR}"
    exit 1
fi

# Install the RPM file
rpm -U --force ${LIB_DIST_DIR}/${LIB_NAME_PATTERN}*.rpm
[ $? -ne 0 ] && exit 1

NANOBIND_ROOT=$(realpath "${PWD}/nanobind")
TMP_DEST_DIR="${NANOBIND_ROOT}/tmp_dist"
DEST_DIR="${NANOBIND_ROOT}/dist"
BUILD_DIR="${NANOBIND_ROOT}/build"

# Remove existing/old wheel files
rm -rf ${TMP_DEST_DIR} ${BUILD_DIR}
rm -f ${DEST_DIR}/${GLIBC_WHEEL_NAME_PATTERN}

# Create dirs
mkdir -p ${TMP_DEST_DIR} ${DEST_DIR}
[ $? -ne 0 ] && exit 1

# Build wheel
cd ${NANOBIND_ROOT}
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
