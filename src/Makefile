CXX = g++

CXXFLAGS = -std=c++11 -Wall -g
CUDA_FLAGS = -arch=sm_75

INCLUDES = -I/usr/local/cuda/include
LDFLAGS = -L/usr/local/cuda/lib64
LIBS = -lcudart -luv
PYBIND11_INCLUDES = $(shell python3 -m pybind11 --includes)
PYTHON_EXTENSION_SUFFIX = $(shell python3-config --extension-suffix)

PYBIND_TARGET= _infinity$(PYTHON_EXTENSION_SUFFIX)

all: test_client infinity_server.o $(PYBIND_TARGET) test_run

utils.o : utils.cpp utils.h
	$(CXX) $(CXXFLAGS) $(INCLUDES) -fPIC -c $< -o $@ $(LDFLAGS) $(LIBS)

libinfinity.o: libinfinity.cpp protocol.h utils.h libinfinity.h
	$(CXX) $(CXXFLAGS) $(INCLUDES) -fPIC -c $< -o $@ $(LDFLAGS) $(LIBS)

infinity_server.o: infinity.cpp
	$(CXX) $(CXXFLAGS) $(INCLUDES) -fPIC -c $^ -o $@ $(LDFLAGS) $(LIBS)

protocol.o: protocol.cpp protocol.h
	$(CXX) $(CXXFLAGS) $(INCLUDES) -fPIC -c $< -o $@ $(LDFLAGS) $(LIBS)

test_client: test_client.c utils.o libinfinity.o protocol.o
	$(CXX) $(CXXFLAGS) $(INCLUDES) $^ -o $@ $(LDFLAGS) $(LIBS)

$(PYBIND_TARGET): pybind.cpp libinfinity.o utils.o protocol.o infinity_server.o
	$(CXX) $(CXXFLAGS) $(INCLUDES) --shared -fPIC $(PYBIND11_INCLUDES) $^\
        -o $(PYBIND_TARGET) $(LDFLAGS) $(LIBS)
	cp $(PYBIND_TARGET) ../infinity/
	
#for debug
infinity_server: infinity.cpp utils.o protocol.o
	$(CXX) $(CXXFLAGS) $(INCLUDES) $^ -o $@ $(LDFLAGS) $(LIBS)
test_run: test_protocol.cpp protocol.o
	$(CXX) $(INCLUDES) -I/usr/local/include/gtest -std=c++11 -pthread $^ -o test_run -L/usr/local/lib -lgtest -lgtest_main


.PHONY: clean, test
clean:
	rm -f client server infinity_server.o *.so *.o test_client test_run
