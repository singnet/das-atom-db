format:
	@find ./src \
	    -iname "*.hpp" -o \
		-iname "*.cpp" -o \
		-iname "*.h" -o \
		-iname "*.c" -o \
		-iname "*.cc" \
		| xargs clang-format -verbose -style=file -i

build: clean
	@mkdir -p build && cd build && cmake .. && make

clean:
	@rm -rf build/*
	@mkdir -p build

