format:
	@find ./lib/include ./lib/src ./nanobind/include ./nanobind/src \
		-iname "*.hpp" -o \
		-iname "*.cpp" -o \
		-iname "*.hh" -o \
		-iname "*.cc" -o \
		-iname "*.h" -o \
		-iname "*.c" \
		| xargs clang-format -verbose -style=file -i

wheeler-images:
	@./scripts/build_glibc_wheeler_docker_image.sh
	@./scripts/build_musl_wheeler_docker_image.sh

wheels: wheeler-images
	@./scripts/run_glibc_wheeler.sh
	@./scripts/run_musl_wheeler.sh

install:
	@./scripts/build_glibc_wheeler_docker_image.sh
	@./scripts/run_glibc_wheeler.sh
	@pip install ./dist/hyperon_das_atomdb_cpp*manylinux*.whl --force-reinstall --no-cache-dir

