set(CPACK_PACKAGE_NAME ${PROJECT_NAME} CACHE STRING "hyperon_das_atomdb_cpp")
set(
  CPACK_PACKAGE_DESCRIPTION_SUMMARY "Atom Space DB for Hyperon DAS"
  CACHE STRING "Atom Space DB for Hyperon DAS is a C++ library for managing Atom databases."
)
set(CPACK_PACKAGE_VENDOR "SingularityNET")

set(CPACK_VERBATIM_VARIABLES YES)

set(CPACK_PACKAGE_INSTALL_DIRECTORY ${CPACK_PACKAGE_NAME})
set(CPACK_OUTPUT_FILE_PREFIX "${CMAKE_SOURCE_DIR}/dist")
set(CPACK_STRIP_FILES YES)

set(
  CPACK_INSTALL_DEFAULT_DIRECTORY_PERMISSIONS
  OWNER_READ OWNER_WRITE OWNER_EXECUTE
  GROUP_READ GROUP_EXECUTE
  WORLD_READ WORLD_EXECUTE
)

set(CPACK_PACKAGING_INSTALL_PREFIX "/usr/local")

set(CPACK_PACKAGE_VERSION_MAJOR ${PROJECT_VERSION_MAJOR})
set(CPACK_PACKAGE_VERSION_MINOR ${PROJECT_VERSION_MINOR})
set(CPACK_PACKAGE_VERSION_PATCH ${PROJECT_VERSION_PATCH})

set(CPACK_PACKAGE_CONTACT "andre@singularitynet.io")
set(CPACK_DEBIAN_PACKAGE_MAINTAINER "Andre Senna <${CPACK_PACKAGE_CONTACT}>")

set(CPACK_RESOURCE_FILE_LICENSE "${CMAKE_CURRENT_SOURCE_DIR}/LICENSE")
set(CPACK_RESOURCE_FILE_README "${CMAKE_CURRENT_SOURCE_DIR}/README.md")

set(CPACK_PACKAGE_ARCHITECTURE "amd64")
set(CPACK_DEBIAN_PACKAGE_ARCHITECTURE ${CPACK_PACKAGE_ARCHITECTURE})
set(CPACK_PACKAGE_VERSION
  "${CPACK_PACKAGE_VERSION_MAJOR}.${CPACK_PACKAGE_VERSION_MINOR}.${CPACK_PACKAGE_VERSION_PATCH}")
set(CPACK_PACKAGE_FILE_NAME
  "${CPACK_PACKAGE_NAME}-${CPACK_PACKAGE_VERSION}-${CPACK_PACKAGE_ARCHITECTURE}-${TARGET_TYPE}")
set(CPACK_COMPONENTS_GROUPING ALL_COMPONENTS_IN_ONE)
set(CPACK_DEB_COMPONENT_INSTALL YES)

include(CPack)

