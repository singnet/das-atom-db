# AlmaLinux variables
PACKER_GLIBC_CONTAINER_NAME="das-atomdb-cpp-glibc-packer"
PACKER_GLIBC_DOCKER_FILE="docker/Dockerfile.glibc.packer"
GLIBC_WHEEL_NAME_PATTERN="*manylinux_2_28_x86_64*.whl"

# Alpine variables
PACKER_MUSL_CONTAINER_NAME="das-atomdb-cpp-musl-packer"
PACKER_MUSL_DOCKER_FILE="docker/Dockerfile.musl.packer"
MUSL_WHEEL_NAME_PATTERN="*musllinux_1_2_x86_64*.whl"

# common variables
PACKER_COMMON_DOCKER_FILE="docker/Dockerfile.common.packer"
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
