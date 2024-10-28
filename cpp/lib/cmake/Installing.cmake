# for CMAKE_INSTALL_LIBDIR, CMAKE_INSTALL_BINDIR, CMAKE_INSTALL_INCLUDEDIR and others
include(GNUInstallDirs)

# note that ${public_headers} should be in quotes
set_target_properties(${PROJECT_NAME} PROPERTIES PUBLIC_HEADER "${public_headers}")

set_target_properties(${PROJECT_NAME} PROPERTIES DEBUG_POSTFIX "d")

# install the target and create export-set
install(
  TARGETS ${PROJECT_NAME}
  EXPORT "${PROJECT_NAME}-targets"
  COMPONENT ${PROJECT_NAME} # must be here, not any line lower
  PUBLIC_HEADER DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/${PROJECT_NAME} # include/hyperon_das_atomdb_cpp
  INCLUDES DESTINATION ${CMAKE_INSTALL_INCLUDEDIR} # include
)

set(CMAKE_INSTALL_DESTINATION "cmake/${PROJECT_NAME}")

# generate and install export file
install(
  EXPORT "${PROJECT_NAME}-targets"
  FILE "${PROJECT_NAME}-targets.cmake"
  NAMESPACE ${namespace}::
  DESTINATION ${CMAKE_INSTALL_DESTINATION}
  COMPONENT ${PROJECT_NAME}
)

include(CMakePackageConfigHelpers)

# generate the version file for the config file
write_basic_package_version_file(
  "${CMAKE_CURRENT_BINARY_DIR}/${PROJECT_NAME}-config-version.cmake"
  COMPATIBILITY AnyNewerVersion
)
# create config file
configure_package_config_file(
  ${CMAKE_CURRENT_SOURCE_DIR}/Config.cmake.in
  "${CMAKE_CURRENT_BINARY_DIR}/${PROJECT_NAME}-config.cmake"
  INSTALL_DESTINATION ${CMAKE_INSTALL_DESTINATION}
)
# install config files
install(
  FILES
  "${CMAKE_CURRENT_BINARY_DIR}/${PROJECT_NAME}-config.cmake"
  "${CMAKE_CURRENT_BINARY_DIR}/${PROJECT_NAME}-config-version.cmake"
  DESTINATION ${CMAKE_INSTALL_DESTINATION}
  COMPONENT ${PROJECT_NAME}
)

