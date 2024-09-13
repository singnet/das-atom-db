cmake_minimum_required(VERSION 3.15...3.27)
project(hyperon_das_atomdb)

# Set C++ standard and optimization flags
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O2 -flto -w")
set(CMAKE_VERBOSE_MAKEFILE ON)

if (CMAKE_VERSION VERSION_LESS 3.18)
  set(DEV_MODULE Development)
else()
  set(DEV_MODULE Development.Module)
endif()

find_package(Python 3.10 COMPONENTS Interpreter ${DEV_MODULE} REQUIRED)

# Find OpenSSL package
find_package(OpenSSL REQUIRED)

# Set the module path for custom CMake modules
set(CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR}/cmake)

# Include directories
include_directories(
  ${CMAKE_CURRENT_SOURCE_DIR}/src
  ${CMAKE_CURRENT_SOURCE_DIR}/ext/include)

# Collect header and source files
file(GLOB_RECURSE headers
  ${CMAKE_CURRENT_SOURCE_DIR}/*.h
  ${CMAKE_CURRENT_SOURCE_DIR}/*.hpp)
file(GLOB_RECURSE sources 
  ${CMAKE_CURRENT_SOURCE_DIR}/src/*.c
  ${CMAKE_CURRENT_SOURCE_DIR}/src/*.cc
  ${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp)

set(PYTHON_EXECUTABLE ${CMAKE_CURRENT_SOURCE_DIR}/venv/bin/python)
execute_process(
  COMMAND ${PYTHON_EXECUTABLE} -m nanobind --cmake_dir
  OUTPUT_STRIP_TRAILING_WHITESPACE OUTPUT_VARIABLE nanobind_DIR)

find_package(nanobind CONFIG REQUIRED)

# Print the source files for debugging purposes
message(STATUS "Source files: ${sources}")

# Add the executable target
# add_executable(${PROJECT_NAME} ${sources} ${headers})

nanobind_add_module(${PROJECT_NAME} MODULE ${cpp} ${sources} ${includes})

# Include OpenSSL directories
target_include_directories(${PROJECT_NAME} PRIVATE ${OpenSSL_INCLUDE_DIR})

# Link OpenSSL libraries
target_link_libraries(${PROJECT_NAME} PRIVATE OpenSSL::Crypto)