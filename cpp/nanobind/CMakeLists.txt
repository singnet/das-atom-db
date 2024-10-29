cmake_minimum_required(VERSION 3.15...3.30)

project(hyperon_das_atomdb_cpp LANGUAGES CXX)
set(EXTENSION_NAME "ext")

# Set C++ standard and optimization flags
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Ofast -flto")
set(CMAKE_VERBOSE_MAKEFILE ON)

# Warn if the user invokes CMake directly
if(NOT SKBUILD)
  message(WARNING "\
  This CMake file is meant to be executed using 'scikit-build-core'.
  Running it directly will almost certainly not produce the desired
  result. If you are a user trying to install this package, use the
  command below, which will install all necessary build dependencies,
  compile the package in an isolated environment, and then install it.
  =====================================================================
   $ pip install .
  =====================================================================
  If you are a software developer, and this is your own package, then
  it is usually much more efficient to install the build dependencies
  in your environment once and use the following command that avoids
  a costly creation of a new virtual environment at every compilation:
  =====================================================================
   $ pip install nanobind scikit-build-core[pyproject]
   $ pip install --no-build-isolation -ve .
  =====================================================================
  You may optionally add -Ceditable.rebuild=true to auto-rebuild when
  the package is imported. Otherwise, you need to rerun the above
  after editing C++ files."  )
endif()

if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  set(CMAKE_BUILD_TYPE Release CACHE STRING "Choose the type of build." FORCE)
  set_property(
    CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS "Debug" "Release" "MinSizeRel" "RelWithDebInfo")
endif()

# Try to import all Python components potentially needed by nanobind
if(CMAKE_VERSION VERSION_LESS 3.18)
  set(DEV_MODULE Development)
else()
  set(DEV_MODULE Development.Module)
endif()
find_package(Python 3.10
  REQUIRED COMPONENTS Interpreter ${DEV_MODULE}
  OPTIONAL_COMPONENTS Development.SABIModule)

# Detect the installed nanobind package and import it into CMake
execute_process(
  COMMAND "${PYTHON_EXECUTABLE}" -m nanobind --cmake_dir
  OUTPUT_STRIP_TRAILING_WHITESPACE OUTPUT_VARIABLE nanobind_ROOT)
find_package(nanobind CONFIG REQUIRED)

# Include directories
include_directories(
  ${CMAKE_CURRENT_SOURCE_DIR}/include
  /usr/local/include
)

if(EXISTS /usr/local/lib/libhyperon_das_atomdb_cpp.a)
  set(LIB_HYPERON_DAS_ATOMDB_CPP_PATH /usr/local/lib/libhyperon_das_atomdb_cpp.a)
elseif(EXISTS /usr/local/lib64/libhyperon_das_atomdb_cpp.a)
  set(LIB_HYPERON_DAS_ATOMDB_CPP_PATH /usr/local/lib64/libhyperon_das_atomdb_cpp.a)
else()
  message(FATAL_ERROR "libhyperon_das_atomdb_cpp.a not found in /usr/local/lib or /usr/local/lib64")
endif()

add_library(libhyperon_das_atomdb_cpp STATIC IMPORTED)
set_target_properties(
  libhyperon_das_atomdb_cpp PROPERTIES IMPORTED_LOCATION ${LIB_HYPERON_DAS_ATOMDB_CPP_PATH})

file(GLOB_RECURSE headers ${CMAKE_CURRENT_SOURCE_DIR}/include/*.h)
file(GLOB_RECURSE sources ${CMAKE_CURRENT_SOURCE_DIR}/src/*.cc)

nanobind_add_module(
  ${EXTENSION_NAME}

  # Target the stable ABI for Python 3.12+, which reduces
  # the number of binary wheels that must be built. This
  # does nothing on older Python versions
  STABLE_ABI

  # Build libnanobind statically and merge it into the
  # extension (which itself remains a shared library)
  #
  # If your project builds multiple extensions, you can
  # replace this flag by NB_SHARED to conserve space by
  # reusing a shared libnanobind across libraries
  NB_STATIC
  ${sources}
  ${headers}
)

target_link_libraries(${EXTENSION_NAME} PRIVATE libhyperon_das_atomdb_cpp)

install(TARGETS ${EXTENSION_NAME} LIBRARY DESTINATION ${PROJECT_NAME})
