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

# package name for deb. If set, then instead of some-application-0.9.2-Linux.deb
# you'll get some-application_0.9.2_amd64.deb (note the underscores too)
set(CPACK_DEBIAN_FILE_NAME DEB-DEFAULT)
# that is if you want every group to have its own package,
# although the same will happen if this is not set (so it defaults to ONE_PER_GROUP)
# and CPACK_DEB_COMPONENT_INSTALL is set to YES
set(CPACK_COMPONENTS_GROUPING ALL_COMPONENTS_IN_ONE) #ONE_PER_GROUP)
set(CPACK_DEB_COMPONENT_INSTALL YES)
# list dependencies
set(CPACK_DEBIAN_PACKAGE_SHLIBDEPS YES)

include(CPack)

message(STATUS "Components to pack: ${CPACK_COMPONENTS_ALL}")