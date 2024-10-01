NPROC := $(shell nproc)

format:
	@find ./src \
		-iname "*.hpp" -o \
		-iname "*.cpp" -o \
		-iname "*.hh" -o \
		-iname "*.cc" -o \
		-iname "*.h" -o \
		-iname "*.c" \
		| xargs clang-format -verbose -style=file -i

build-make: clean
	@mkdir -p build && cd build && cmake .. && make -j ${NPROC}

build-ninja: clean
	@mkdir -p build && cd build && cmake -G Ninja .. && ninja -j ${NPROC}

clean:
	@rm -rf build/*
	@mkdir -p build

