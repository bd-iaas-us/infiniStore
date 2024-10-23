#include <gtest/gtest.h>

#include <cstring>

#include "../protocol.h"

class SerializationTest : public ::testing::Test {
   protected:
    void SetUp() override {
        memset(&meta.ipc_handle, 0, sizeof(cudaIpcMemHandle_t));
        meta.block_size = 2;
        meta.blocks = {{"block1_key", 12345}, {"block2_key", 67890}};
        ibv_gid gid;
        gid.global.subnet_prefix = 0x123456789abcdef0;
        gid.global.interface_id = 0x0fedcba987654321;
    }

    local_meta_t meta;
};

TEST_F(SerializationTest, SerializeAndDeserialize) {
    std::string serialized_data;
    ASSERT_TRUE(serialize(meta, serialized_data)) << "Failed to serialize";

    local_meta_t deserialized_meta;
    ASSERT_TRUE(deserialize(serialized_data.data(), serialized_data.size(), deserialized_meta))
        << "Failed to deserialize";

    EXPECT_EQ(deserialized_meta.block_size, meta.block_size);
    EXPECT_EQ(deserialized_meta.blocks.size(), meta.blocks.size());
    for (size_t i = 0; i < meta.blocks.size(); ++i) {
        EXPECT_EQ(deserialized_meta.blocks[i].offset, meta.blocks[i].offset);
        EXPECT_EQ(deserialized_meta.blocks[i].key, meta.blocks[i].key);
    }

    ASSERT_EQ(deserialized_meta.blocks[0].key, "block1_key");
}

TEST_F(SerializationTest, DeserializeInvalidData) {
    std::string invalid_data = "invalid data";
    local_meta_t deserialized_meta;

    EXPECT_FALSE(deserialize(invalid_data.data(), invalid_data.size(), deserialized_meta));
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
