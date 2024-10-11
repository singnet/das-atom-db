format:
	@find ./src \
		-iname "*.hpp" -o \
		-iname "*.cpp" -o \
		-iname "*.hh" -o \
		-iname "*.cc" -o \
		-iname "*.h" -o \
		-iname "*.c" \
		| xargs clang-format -verbose -style=file -i

wheel:
	@./scripts/build_wheel.sh

make install: wheel
	@pip install hyperon_das_atomdb_cpp*.whl --force-reinstall --no-cache-dir

