CXX = g++

CXXFLAGS = -std=c++11 -Wall -g
CUDA_FLAGS = -arch=sm_75

INCLUDES = -I/usr/local/cuda/include
LDFLAGS = -L/usr/local/cuda/lib64
LIBS = -lcudart -lgdrapi

all:client server
# 目标文件
client: client.c
	$(CXX) $(CXXFLAGS) $(INCLUDES) $^ -o $@ $(LDFLAGS) $(LIBS)

server: server.c
	$(CXX) $(CXXFLAGS) $(INCLUDES) $^ -o $@ $(LDFLAGS) $(LIBS)

.PHONY: clean
clean:
	rm -f client server
