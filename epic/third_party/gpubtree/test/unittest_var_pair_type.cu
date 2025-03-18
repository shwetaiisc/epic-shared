//
// Created by Shujian Qian on 2024-03-20.
//

#include <gtest/gtest.h>

#include <var_pair_type.hpp>

TEST(VarPairTypeTest, DefaultConstructorTest) {
  using key_type   = uint32_t;
  using value_type = uint32_t;
  var_pair_type<key_type, value_type> pair;
  ASSERT_EQ(pair.first, 0xFFFFFFFFu);
  ASSERT_EQ(pair.second, 0xFFFFFFFFu);
}

TEST(VarPairTypeTest, InitializationConstructorTest) {
  using key_type   = uint32_t;
  using value_type = uint32_t;
  var_pair_type<key_type, value_type> pair(1, 2);
  ASSERT_EQ(pair.first, 1);
  ASSERT_EQ(pair.second, 2);
}

TEST(VarPairTypeTest, CopyConstructorTest) {
  using key_type     = uint64_t;
  using value_type   = uint64_t;
  using varpair_type = var_pair_type<key_type, value_type, 48, 16>;
  varpair_type pair(1, 2);
  varpair_type pair_copy(pair);
  ASSERT_EQ(pair_copy.first, 1);
  ASSERT_EQ(pair_copy.second, 2);
}

TEST(VarPairTypeTest, VolatileCopyConstructorTest) {
  using key_type     = uint64_t;
  using value_type   = uint64_t;
  using varpair_type = var_pair_type<key_type, value_type, 48, 16>;
  volatile varpair_type pair(1, 2);
  varpair_type pair_copy(pair);
  ASSERT_EQ(pair_copy.first, 1);
  ASSERT_EQ(pair_copy.second, 2);
}

TEST(VarPairTypeTest, AssignmentTest) {
  using key_type     = uint64_t;
  using value_type   = uint64_t;
  using varpair_type = var_pair_type<key_type, value_type, 48, 16>;
  varpair_type pair(1, 2);
  varpair_type pair_copy;
  pair_copy = pair;
  ASSERT_EQ(pair_copy.first, 1);
  ASSERT_EQ(pair_copy.second, 2);
}

TEST(VarPairTypeTest, VolatileAssignmentTest) {
  using key_type     = uint64_t;
  using value_type   = uint64_t;
  using varpair_type = var_pair_type<key_type, value_type, 48, 16>;
  volatile varpair_type pair(1, 2);
  varpair_type pair_copy;
  pair_copy = pair;
  ASSERT_EQ(pair_copy.first, 1);
  ASSERT_EQ(pair_copy.second, 2);
}

TEST(VarPairTypeTest, EqualityTest) {
  using key_type     = uint64_t;
  using value_type   = uint64_t;
  using varpair_type = var_pair_type<key_type, value_type, 48, 16>;
  varpair_type pair(1, 2);
  varpair_type pair_copy(1, 2);
  varpair_type pair_diff;
  ASSERT_TRUE(pair == pair_copy);
  ASSERT_FALSE(pair != pair_copy);
  ASSERT_FALSE(pair == pair_diff);
  ASSERT_TRUE(pair != pair_diff);
}

TEST(VarPairTypeTest, InvalidKeyValueTest) {
  using key_type   = uint64_t;
  using value_type = uint64_t;
  var_pair_type<key_type, value_type, 48, 16> pair;
  ASSERT_EQ(pair.first, 0xFFFFFFFFFFFFu);
  ASSERT_EQ(pair.second, 0xFFFFu);
  var_pair_type<key_type, value_type, 63, 1> pair2;
  ASSERT_EQ(pair2.first, 0x7FFFFFFFFFFFFFFFu);
  ASSERT_EQ(pair2.second, 0x1u);
}

TEST(VarPairTypeTest, LayoutTest) {
  using key_type     = uint64_t;
  using value_type   = uint64_t;
  using varpair_type = var_pair_type<key_type, value_type, 48, 16>;
  ASSERT_EQ(sizeof(varpair_type), 8);
  varpair_type pair(0, 0xffffffffffffffffull);
  ASSERT_EQ(*reinterpret_cast<uint64_t *>(&pair), 0xffffull);
  pair.first = 0xffffffffffffffffull;
  ASSERT_EQ(*reinterpret_cast<uint64_t *>(&pair), 0xffffffffffffffffull);
  pair.second = 0;
  ASSERT_EQ(*reinterpret_cast<uint64_t *>(&pair), 0xffffffffffff0000ull);
}

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}