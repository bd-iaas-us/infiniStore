#include <gtest/gtest.h>
#include "protocol.h" // Include the header where the serialization functions are defined

class SerializationTest : public ::testing::Test {
protected:
    void SetUp() override {
        meta = (local_meta_t*)malloc(sizeof(local_meta_t));
        memset(meta, 0, sizeof(local_meta_t));

        memset(&meta->ipc_handle, 0, sizeof(cudaIpcMemHandle_t));

        meta->block_size = 2;
        meta->blocks = (block_t*)malloc(sizeof(block_t) * meta->block_size);

        meta->blocks[0].offset = 12345;
        meta->blocks[0].key = strdup("block1_key");
        meta->blocks[1].offset = 67890;
        meta->blocks[1].key = strdup("block2_key");
    }

    void TearDown() override {
        free_local_meta(meta);
    }

    local_meta_t *meta;
};

TEST_F(SerializationTest, SerializeAndDeserialize) {
    size_t serialized_size;
    char *serialized_data = serialize_local_meta(meta, &serialized_size);
    ASSERT_NE(serialized_data, nullptr) << "failed to serialize";

    local_meta_t *deserialized_meta = deserialize_local_meta(serialized_data, serialized_size);
    ASSERT_NE(deserialized_meta, nullptr) << "failed to deserialize";

    EXPECT_EQ(deserialized_meta->block_size, meta->block_size);
    for (int i = 0; i < meta->block_size; ++i) {
        EXPECT_EQ(deserialized_meta->blocks[i].offset, meta->blocks[i].offset);
        EXPECT_STREQ(deserialized_meta->blocks[i].key, meta->blocks[i].key);
    }

    //std::cout << deserialized_meta->blocks[0].key << std::endl;
    ASSERT_STREQ(deserialized_meta->blocks[0].key, "block1_key");

    free(serialized_data);
    free_local_meta(deserialized_meta);
}

// test invalid data
TEST_F(SerializationTest, DeserializeInvalidData) {
    // 传入无效数据
    char invalid_data[] = "invalid data";
    local_meta_t *deserialized_meta = deserialize_local_meta(invalid_data, sizeof(invalid_data));

    // 应该返回 NULL
    EXPECT_EQ(deserialized_meta, nullptr);
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}