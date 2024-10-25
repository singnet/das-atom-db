cmake_minimum_required(VERSION 3.15...3.30)

set(LIB_VERSION 0.0.1)
if(DEFINED CMAKE_PROJECT_VERSION AND NOT "${CMAKE_PROJECT_VERSION}" STREQUAL "")
  set(LIB_VERSION ${CMAKE_PROJECT_VERSION})
endif()

set(namespace "atomdb")
project(
  "hyperon_das_atomdb_cpp"
  VERSION ${LIB_VERSION}
  DESCRIPTION "Atom Space DB for Hyperon DAS"
)

set(CPACK_DEBIAN_PACKAGE_NAME "${PROJECT_NAME}-dev")

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Ofast -flto")
set(CMAKE_VERBOSE_MAKEFILE ON)

add_library(${PROJECT_NAME} STATIC)

file(GLOB_RECURSE sources
  ${CMAKE_CURRENT_SOURCE_DIR}/src/*.c
  ${CMAKE_CURRENT_SOURCE_DIR}/src/*.cc
  ${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp)

target_sources(${PROJECT_NAME} PRIVATE ${sources})

target_include_directories(${PROJECT_NAME}
  PRIVATE
  # where the library itself will look for its internal headers
  ${CMAKE_CURRENT_SOURCE_DIR}/src
  PUBLIC
  # where top-level project will look for the library's public headers
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
  # where external projects will look for the library's public headers
  $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
)

file(GLOB_RECURSE headers
  ${CMAKE_CURRENT_SOURCE_DIR}/include/*.h
  ${CMAKE_CURRENT_SOURCE_DIR}/include/*.hh
  ${CMAKE_CURRENT_SOURCE_DIR}/include/*.hpp)

# without it public headers won't get installed
set(public_headers ${headers})

target_compile_options(${PROJECT_NAME} PRIVATE -Werror)

# where to find our CMake modules
set(CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}/cmake")
include(Installing)
include(Packing)
