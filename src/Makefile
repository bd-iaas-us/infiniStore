CXX = g++

CXXFLAGS = -std=c++11 -Wall -g
CUDA_FLAGS = -arch=sm_75

INCLUDES = -I/usr/local/cuda/include
LDFLAGS = -L/usr/local/cuda/lib64
LIBS = -lcudart# -lgdrapi
PYBIND11_INCLUDES = $(shell python3 -m pybind11 --includes)
PYTHON_EXTENSION_SUFFIX = $(shell python3-config --extension-suffix)

PYBIND_TARGET= _infinity$(PYTHON_EXTENSION_SUFFIX)

all: test_client infinity_server $(PYBIND_TARGET)

utils.o : utils.c utils.h
	$(CXX) $(CXXFLAGS) $(INCLUDES) -fPIC -c $< -o $@ $(LDFLAGS) $(LIBS)

libinfinity.o: libinfinity.c protocol.h utils.h libinfinity.h
	$(CXX) $(CXXFLAGS) $(INCLUDES) -fPIC -c $< -o $@ $(LDFLAGS) $(LIBS)

infinity_server: infinity.c utils.o
	$(CXX) $(CXXFLAGS) $(INCLUDES) $^ -o $@ $(LDFLAGS) $(LIBS)

test_client: test_client.c utils.o libinfinity.o
	$(CXX) $(CXXFLAGS) $(INCLUDES) $^ -o $@ $(LDFLAGS) $(LIBS)

$(PYBIND_TARGET): pybind.cc libinfinity.o utils.o
	${CXX} $(CXXFLAGS) $(INCLUDES) --shared -fPIC $(PYBIND11_INCLUDES) $^\
        -o $(PYBIND_TARGET) $(LDFLAGS) $(LIBS)
	cp $(PYBIND_TARGET) ../infinity/

.PHONY: clean
clean:
	rm -f client server infinity_server *.so *.o test_client
