#!/bin/bash -x

# This script is used to build installable packages (DEB, TGZ, etc.) for the current package.

LIB_DIR="${PWD}/lib"
DIST_DIR="${LIB_DIR}/dist"
BUILD_DIR="${LIB_DIR}/build"

# Remove existing/old packages
rm -rf ${BUILD_DIR}
rm -f ${DIST_DIR}/*tar.gz
[ $? -ne 0 ] && exit 1

# Create dirs
mkdir -p ${DIST_DIR} ${BUILD_DIR}
[ $? -ne 0 ] && exit 1

# Build package
cd ${BUILD_DIR} \
  && cmake -DCMAKE_PROJECT_VERSION=${CMAKE_PROJECT_VERSION} -DTARGET_TYPE=musl .. \
  && make -j$(nproc) \
  && cpack -G TGZ
[ $? -ne 0 ] && exit 1

# Change ownership
chown -R ${_USER}:${_GROUP} ${DIST_DIR}
[ $? -ne 0 ] && exit 1

# Clean up
[ -d ${BUILD_DIR} ] && rm -rf ${BUILD_DIR}

exit 0
