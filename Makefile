CXX = g++

CXXFLAGS = -std=c++11 -Wall -g
CUDA_FLAGS = -arch=sm_75

INCLUDES = -I/usr/local/cuda/include
LDFLAGS = -L/usr/local/cuda/lib64
LIBS = -lcudart# -lgdrapi

all:infinity test_client

utils.o : utils.c utils.h
	$(CXX) $(CXXFLAGS) $(INCLUDES) -c $< -o $@ $(LDFLAGS) $(LIBS)

libinfinity.o: libinfinity.c protocol.h utils.h libinfinity.h
	$(CXX) $(CXXFLAGS) $(INCLUDES) -c $< -o $@ $(LDFLAGS) $(LIBS)

infinity: infinity.c utils.o
	$(CXX) $(CXXFLAGS) $(INCLUDES) $^ -o $@ $(LDFLAGS) $(LIBS)

test_client: test_client.c utils.o libinfinity.o
	$(CXX) $(CXXFLAGS) $(INCLUDES) $^ -o $@ $(LDFLAGS) $(LIBS)
	
.PHONY: clean
clean:
	rm -f client server infinity