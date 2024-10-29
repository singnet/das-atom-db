cmake_minimum_required(VERSION 3.15...3.30)

if(DEFINED CMAKE_PROJECT_VERSION AND NOT "${CMAKE_PROJECT_VERSION}" STREQUAL "")
  set(LIB_VERSION ${CMAKE_PROJECT_VERSION})
else()
  message(FATAL_ERROR "CMAKE_PROJECT_VERSION is not defined or is empty")
endif()

if(NOT DEFINED TARGET_TYPE OR "${TARGET_TYPE}" STREQUAL "")
  message(FATAL_ERROR "TARGET_TYPE is not defined or is empty")
endif()

set(namespace "atomdb")
project(
  "hyperon_das_atomdb_cpp"
  VERSION ${LIB_VERSION}
  DESCRIPTION "Atom Space DB for Hyperon DAS"
)

set(CPACK_PACKAGE_NAME "${PROJECT_NAME}-dev")

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fPIC -Ofast")
set(CMAKE_VERBOSE_MAKEFILE ON)

add_library(${PROJECT_NAME} STATIC)

if(EXISTS /usr/local/lib/libmbedcrypto.a)
  set(LIBMBCRYPTO_PATH /usr/local/lib/libmbedcrypto.a)
elseif(EXISTS /usr/local/lib64/libmbedcrypto.a)
  set(LIBMBCRYPTO_PATH /usr/local/lib64/libmbedcrypto.a)
else()
  message(FATAL_ERROR "libmbedcrypto.a not found in /usr/local/lib or /usr/local/lib64")
endif()

set(MBCRYPTO_SYMBOLS_PATH ${CMAKE_BINARY_DIR}/mbedcrypto_symbols)
make_directory(${MBCRYPTO_SYMBOLS_PATH})
execute_process(COMMAND ar x ${LIBMBCRYPTO_PATH} --output=${MBCRYPTO_SYMBOLS_PATH})
file(GLOB mbedcrypto_symbols ${MBCRYPTO_SYMBOLS_PATH}/*.o)
set(CREATE_STATIC_LIB_SCRIPT ${PROJECT_SOURCE_DIR}/scripts/_create_static_library.sh)
set(CMAKE_CXX_CREATE_STATIC_LIBRARY
  "${CREATE_STATIC_LIB_SCRIPT} <TARGET> <OBJECTS> ${MBCRYPTO_SYMBOLS_PATH}/*o")

file(GLOB_RECURSE sources
  ${CMAKE_CURRENT_SOURCE_DIR}/src/*.c
  ${CMAKE_CURRENT_SOURCE_DIR}/src/*.cc
  ${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp)
file(GLOB_RECURSE headers
  ${CMAKE_CURRENT_SOURCE_DIR}/include/*.h
  ${CMAKE_CURRENT_SOURCE_DIR}/include/*.hh
  ${CMAKE_CURRENT_SOURCE_DIR}/include/*.hpp)

target_sources(${PROJECT_NAME} PRIVATE ${sources} ${headers})

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

# without it public headers won't get installed
set(public_headers ${headers})

target_compile_options(${PROJECT_NAME} PRIVATE -Werror)

# where to find our CMake modules
set(CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}/cmake")
include(Installing)
include(Packing)


