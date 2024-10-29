format:
	@find ./lib/include ./lib/src ./nanobind/include ./nanobind/src \
		-iname "*.hpp" -o \
		-iname "*.cpp" -o \
		-iname "*.hh" -o \
		-iname "*.cc" -o \
		-iname "*.h" -o \
		-iname "*.c" \
		| xargs clang-format -verbose -style=file -i

packer-images:
	@./scripts/build_glibc_packer_docker_image.sh
	@./scripts/build_musl_packer_docker_image.sh

packs: packer-images
	@./scripts/run_glibc_packer.sh
	@./scripts/run_musl_packer.sh

install:
	@./scripts/build_glibc_packer_docker_image.sh
	@./scripts/run_glibc_packer.sh
	@pip install ./nanobind/dist/hyperon_das_atomdb_cpp*manylinux*.whl --force-reinstall --no-cache-dir

