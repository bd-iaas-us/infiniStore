
CXX = g++

CXXFLAGS = -std=c++11 -Wall -g
CUDA_FLAGS = -arch=sm_75

INCLUDES = -I/usr/local/cuda/include
LDFLAGS = -L/usr/local/cuda/lib64
LIBS = -lcudart -luv -libverbs

all: test_run test_client
protocol.o:
	make -C ..
libinfinity.o:
	make -C ..
utils.o:
	make -C ..
test_run: test_protocol.cpp ../protocol.o
	$(CXX) $(INCLUDES) -I/usr/local/include/gtest -std=c++11 -pthread $^ -o test_run -L/usr/local/lib -lgtest -lgtest_main
test_client: test_client.c ../utils.o ../libinfinity.o ../protocol.o
	$(CXX) $(CXXFLAGS) $(INCLUDES) $^ -o $@ $(LDFLAGS) $(LIBS)
clean:
	rm -rf test_run test_client