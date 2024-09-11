cmake_minimum_required(VERSION 3.5.1)
project(AtomDB)

# Set C++ standard and optimization flags
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O0 -w")
set(CMAKE_VERBOSE_MAKEFILE ON)

# Find OpenSSL package
find_package(OpenSSL REQUIRED)

# Set the module path for custom CMake modules
set(CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR}/cmake)

# Include directories
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/src)

# Collect header and source files
file(GLOB_RECURSE headers ${CMAKE_CURRENT_SOURCE_DIR}/*.h ${CMAKE_CURRENT_SOURCE_DIR}/*.hpp)
file(GLOB_RECURSE sources ${CMAKE_CURRENT_SOURCE_DIR}/src/*.c ${CMAKE_CURRENT_SOURCE_DIR}/src/*.cc ${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp)

# Print the source files for debugging purposes
message(STATUS "Source files: ${sources}")

# Add the executable target
add_executable(AtomDB ${sources} ${headers})

# Include OpenSSL directories
target_include_directories(AtomDB PRIVATE ${OpenSSL_INCLUDE_DIR})

# Link OpenSSL libraries
target_link_libraries(AtomDB PRIVATE OpenSSL::Crypto)