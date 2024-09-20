CC = gcc
CFLAGS = -g -Wall -O2 -Iinclude
LDFLAGS = -lpthread -lrdmacm -libverbs

ifeq ($(HAVE_CUDA),1)
	CFLAGS += -I/usr/local/cuda/include -DHAVE_CUDA
	LDFLAGS += -L/usr/local/cuda/lib64 -lcuda
endif

EXEC = example
SRC_DIR = src
BUILD_DIR = build
SRC_FILES = $(wildcard $(SRC_DIR)/*.c)
OBJ_FILES = $(patsubst $(SRC_DIR)/%.c, $(BUILD_DIR)/%.o, $(SRC_FILES))

all: $(EXEC)

$(EXEC): $(OBJ_FILES)
	$(CC) $(CFLAGS) $(OBJ_FILES) -o $(EXEC) $(LDFLAGS)

$(BUILD_DIR)/%.o: $(SRC_DIR)/%.c
	@mkdir -p $(BUILD_DIR)
	$(CC) $(CFLAGS) -c $< -o $@

clean:
	rm -rf $(BUILD_DIR)/*.o $(EXEC)

.PHONY: all clean
