format:
	@find ./src \
		-iname "*.hpp" -o \
		-iname "*.cpp" -o \
		-iname "*.hh" -o \
		-iname "*.cc" -o \
		-iname "*.h" -o \
		-iname "*.c" \
		| xargs clang-format -verbose -style=file -i

wheeler-image:
	@./scripts/build_wheeler_docker_image.sh

wheel: wheeler-image
	@./scripts/run_wheeler.sh

install: wheel
	@pip install ./dist/hyperon_das_atomdb_cpp*.whl --force-reinstall --no-cache-dir

