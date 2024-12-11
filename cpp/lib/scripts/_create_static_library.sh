#!/bin/bash -x

# This script was created to embed the `mbedtls` library into the `hyperon_das_atomdb_cpp` static
# library. This is done to avoid the need to install the `mbedtls` library on the target system.
# Teoretically, it would be possible to do the same with `CMAKE_CXX_CREATE_STATIC_LIBRARY` or
# `CMAKE_CXX_ARCHIVE_CREATE` in the `CMakeLists.txt` file, but those options did not work as expected.
# It seems that both have some limitations related to the number of files that can be passed as objects.
ar qc ${@}
