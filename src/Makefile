CXX = g++
CXXFLAGS = -std=c++11 -Wall -O3

INCLUDES = -I/usr/local/cuda/include
LDFLAGS = -L/usr/local/cuda/lib64
LIBS = -lcudart -luv -libverbs
PYTHON=python3
PYBIND11_INCLUDES = $(shell $(PYTHON) -m pybind11 --includes)
PYTHON_EXTENSION_SUFFIX = $(shell $(PYTHON)-config --extension-suffix)

PYBIND_TARGET= _infinistore$(PYTHON_EXTENSION_SUFFIX)

SOURCES := $(wildcard *.cpp)
OBJECTS = $(SOURCES:.cpp=.o)

all:$(PYBIND_TARGET)

manylinux: PYTHON ?= python3.11
manylinux: CXXFLAGS = -std=c++11 -g -O3 -Wall
manylinux: INCLUDES += -I/usr/local/include -I$(shell $(PYTHON) -c "import site; print(site.getsitepackages()[0])")/triton/backends/nvidia/include/
manylinux: LDFLAGS += -L$(shell $(PYTHON) -c "import site; print(site.getsitepackages()[0])")/nvidia/cuda_runtime/lib/
manylinux: PYBIND11_INCLUDES = $(shell $(PYTHON) -m pybind11 --includes)
manylinux: PYTHON_EXTENSION_SUFFIX = $(shell $(PYTHON)-config --extension-suffix)
manylinux: PYBIND_TARGET = _infinistore$(PYTHON_EXTENSION_SUFFIX)
manylinux: $(PYBIND_TARGET)

-include $(OBJECTS:.o=.d)
%.o: %.cpp
	$(CXX) $(CXXFLAGS) $(INCLUDES) -MMD -MP -fPIC -c $< -o $@

$(PYBIND_TARGET): pybind.cpp libinfinistore.o utils.o protocol.o infinistore.o log.o ibv_helper.o mempool.o
	$(CXX) $(CXXFLAGS) $(INCLUDES) --shared -fPIC $(PYBIND11_INCLUDES) $^ \
	-o $(PYBIND_TARGET) $(LDFLAGS) $(LIBS)
	rm -rf ../infinistore/$(PYBIND_TARGET)
	cp $(PYBIND_TARGET) ../infinistore/


.PHONY: clean test
clean:
	rm *.so *.o *.d
	make -C test clean
