CXX = g++

CXXFLAGS = -std=c++11 -Wall -g
CUDA_FLAGS = -arch=sm_75

INCLUDES = -I/usr/local/cuda/include
LDFLAGS = -L/usr/local/cuda/lib64
LIBS = -lcudart -lgdrapi

all:infinity test_client
# 目标文件
client: client.c
	$(CXX) $(CXXFLAGS) $(INCLUDES) $^ -o $@ $(LDFLAGS) $(LIBS)

server: server.c
	$(CXX) $(CXXFLAGS) $(INCLUDES) $^ -o $@ $(LDFLAGS) $(LIBS)

utils.o : utils.c utils.h
	$(CXX) $(CXXFLAGS) $(INCLUDES) -c $< -o $@ $(LDFLAGS) $(LIBS)

infinity: infinity.c protocol.h utils.o
	$(CXX) $(CXXFLAGS) $(INCLUDES) $< utils.o -o $@ $(LDFLAGS) $(LIBS)

test_client: libinfinity.c protocol.h utils.o
	$(CXX) $(CXXFLAGS) $(INCLUDES) $< utils.o -o $@ $(LDFLAGS) $(LIBS)
	
.PHONY: clean
clean:
	rm -f client server infinity