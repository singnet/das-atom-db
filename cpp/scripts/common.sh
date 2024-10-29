# AlmaLinux variables
WHEELER_GLIBC_CONTAINER_NAME="das-atomdb-cpp-glibc-wheeler"
WHEELER_GLIBC_DOCKER_FILE="docker/Dockerfile.glibc.wheeler"
GLIBC_WHEEL_NAME_PATTERN="*manylinux_2_28_x86_64*.whl"

# Alpine variables
WHEELER_MUSL_CONTAINER_NAME="das-atomdb-cpp-musl-wheeler"
WHEELER_MUSL_DOCKER_FILE="docker/Dockerfile.musl.wheeler"
MUSL_WHEEL_NAME_PATTERN="*musllinux_1_2_x86_64*.whl"

# Packer variables
PACKER_CONTAINER_NAME="das-atomdb-cpp-packer"
PACKER_DOCKER_FILE="docker/Dockerfile.packer"

# common variables
NANOBIND_ROOT=$(realpath "$(dirname $0)/../nanobind")
PYPROJECT_TOML_FILE="${NANOBIND_ROOT}/pyproject.toml"
PROJECT_VERSION=$(
    grep -A 2 '\[project\]' "${PYPROJECT_TOML_FILE}" | \
    awk '/version/ {print $3}' | \
    tr -d '"'
)
LIB_ROOT=$(realpath "$(dirname $0)/../lib")
LIB_DIST_DIR="${LIB_ROOT}/dist"
LIB_NAME_PATTERN="hyperon_das_atomdb_cpp-dev-${PROJECT_VERSION}"
