
/*
 *   Copyright 2022 The Regents of the University of California, Davis
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

#include <gpu_btree.h>
#include <gtest/gtest.h>
#include <cmd.hpp>
#include <cstdint>

std::size_t num_keys = 1024;

namespace {
using key_type   = uint32_t;
using value_type = uint32_t;

// const auto sentinel_value = std::numeric_limits<key_type>::max();
// const auto sentinel_key = std::numeric_limits<value_type>::max();
template <typename BTreeMap>
struct BTreeMapData {
  using btree_map = BTreeMap;
};

template <typename T>
struct mapped_vector {
  mapped_vector(std::size_t capacity) : capacity_(capacity) { allocate(capacity); }
  T& operator[](std::size_t index) { return dh_buffer_[index]; }
  ~mapped_vector() {}
  void free() {
    cuda_try(cudaDeviceSynchronize());
    cuda_try(cudaFreeHost(dh_buffer_));
  }
  T* data() const { return dh_buffer_; }

  std::vector<T> to_std_vector() {
    std::vector<T> copy(capacity_);
    for (std::size_t i = 0; i < capacity_; i++) { copy[i] = dh_buffer_[i]; }
    return copy;
  }

 private:
  void allocate(std::size_t count) { cuda_try(cudaMallocHost(&dh_buffer_, sizeof(T) * count)); }
  std::size_t capacity_;
  T* dh_buffer_;
};

template <typename key_type = uint32_t, typename value_type = uint32_t>
struct testing_input {
  testing_input(std::size_t input_num_keys)
      : num_keys(input_num_keys)
      , keys(input_num_keys)
      , values(input_num_keys)
      , keys_exist(input_num_keys)
      , keys_not_exist(input_num_keys) {
    make_input();
  }
  void make_input() {
    for (std::size_t i = 0; i < num_keys; i++) {
      // Make sure that the input doesn't contain 0
      // and, queries that do not exist in the table are uniformly distributed to avoid
      // contention... an optimzation is to avoid locking if key doesn't exist in node
      keys[i]           = static_cast<key_type>(i + 1) * 2;
      values[i]         = static_cast<value_type>(keys[i]);
      keys_exist[i]     = keys[i];
      keys_not_exist[i] = keys[i] + 1;
    }
  }
  void free() {
    keys.free();
    values.free();
    keys_exist.free();
    keys_not_exist.free();
  }

  std::size_t num_keys;
  mapped_vector<key_type> keys;
  mapped_vector<value_type> values;
  mapped_vector<key_type> keys_exist;
  mapped_vector<key_type> keys_not_exist;
};

template <class MapData>
class BTreeMapTest : public testing::Test {
 protected:
  BTreeMapTest() { btree_map_ = new typename map_data::btree_map(); }
  ~BTreeMapTest() override { delete btree_map_; }
  using map_data         = MapData;
  using input_key_type   = typename map_data::btree_map::key_type;
  using input_value_type = typename map_data::btree_map::value_type;
  typename map_data::btree_map* btree_map_;
  testing_input<input_key_type, input_value_type> input{num_keys};
  mapped_vector<input_value_type> find_results{num_keys};
  constexpr static input_value_type sentinel_value = map_data::btree_map::pair_type::invalid_value;
};

struct TreeParam {
  static constexpr int BranchingFactor = 16;
};
struct SlabAllocParam {
  static constexpr uint32_t NumSuperBlocks  = 1;
  static constexpr uint32_t NumMemoryBlocks = 1024 * 1;
  static constexpr uint32_t TileSize        = TreeParam::BranchingFactor;
  static constexpr uint32_t SlabSize        = 128;
};
using node_type           = GpuBTree::node_type<key_type, value_type, TreeParam::BranchingFactor>;
using bump_allocator_type = device_bump_allocator<node_type>;
using slab_allocator_type = device_allocator::SlabAllocLight<node_type,
                                                             SlabAllocParam::NumSuperBlocks,
                                                             SlabAllocParam::NumMemoryBlocks,
                                                             SlabAllocParam::TileSize,
                                                             SlabAllocParam::SlabSize>;

typedef testing::Types<
    BTreeMapData<
        GpuBTree::
            gpu_blink_tree<key_type, value_type, TreeParam::BranchingFactor, bump_allocator_type>>,
    BTreeMapData<
        GpuBTree::
            gpu_blink_tree<key_type, value_type, TreeParam::BranchingFactor, slab_allocator_type>>,
    BTreeMapData<GpuBTree::gpu_versioned_btree<key_type,
                                               value_type,
                                               TreeParam::BranchingFactor,
                                               bump_allocator_type>>,
    BTreeMapData<GpuBTree::gpu_versioned_btree<key_type,
                                               value_type,
                                               TreeParam::BranchingFactor,
                                               slab_allocator_type>>,
    BTreeMapData<GpuBTree::gpu_blink_tree<key_type,
                                          value_type,
                                          TreeParam::BranchingFactor,
                                          slab_allocator_type,
                                          var_pair_type<key_type, value_type>>>,
    BTreeMapData<GpuBTree::gpu_blink_tree<uint64_t,
                                          uint64_t,
                                          TreeParam::BranchingFactor,
                                          slab_allocator_type,
                                          var_pair_type<uint64_t, uint64_t, 32, 32>>>,
    BTreeMapData<GpuBTree::gpu_blink_tree<uint64_t,
        uint64_t,
        TreeParam::BranchingFactor,
        slab_allocator_type,
        var_pair_type<uint64_t, uint64_t, 33, 31>>>,
    BTreeMapData<GpuBTree::gpu_blink_tree<uint64_t,
                                          uint64_t,
                                          TreeParam::BranchingFactor,
                                          slab_allocator_type,
                                          var_pair_type<uint64_t, uint64_t, 42, 22>>>
                                          >
    Implementations;

TYPED_TEST_SUITE(BTreeMapTest, Implementations);

TYPED_TEST(BTreeMapTest, Validation) {
  //  testing_input<input_key_type, input_value_type> input(num_keys);
  EXPECT_EQ(cudaDeviceSynchronize(), cudaSuccess);
  this->btree_map_->insert(this->input.keys.data(), this->input.values.data(), num_keys);
  EXPECT_EQ(cudaDeviceSynchronize(), cudaSuccess);
  auto keys = this->input.keys.to_std_vector();
  EXPECT_NO_THROW(this->btree_map_->validate_tree_structure(
      keys, [](auto key) { return static_cast<value_type>(key); }));
}

TYPED_TEST(BTreeMapTest, FindExist) {
  EXPECT_EQ(cudaDeviceSynchronize(), cudaSuccess);
  this->btree_map_->insert(this->input.keys.data(), this->input.values.data(), num_keys);
  cuda_try(cudaDeviceSynchronize());
  this->btree_map_->find(this->input.keys_exist.data(), this->find_results.data(), num_keys);
  cuda_try(cudaDeviceSynchronize());
  EXPECT_EQ(cudaDeviceSynchronize(), cudaSuccess);
  for (std::size_t i = 0; i < num_keys; i++) {
    auto expected_value = this->input.values[i];
    auto found_value    = this->find_results[i];
    ASSERT_EQ(found_value, expected_value);
  }
}

TYPED_TEST(BTreeMapTest, FindNotExist) {
  EXPECT_EQ(cudaDeviceSynchronize(), cudaSuccess);
  this->btree_map_->insert(this->input.keys.data(), this->input.values.data(), num_keys);
  cuda_try(cudaDeviceSynchronize());
  this->btree_map_->find(this->input.keys_not_exist.data(), this->find_results.data(), num_keys);
  cuda_try(cudaDeviceSynchronize());
  EXPECT_EQ(cudaDeviceSynchronize(), cudaSuccess);
  for (std::size_t i = 0; i < num_keys; i++) {
    auto expected_value = this->sentinel_value;
    auto found_value    = this->find_results[i];
    ASSERT_EQ(found_value, expected_value);
  }
}

TYPED_TEST(BTreeMapTest, EraseAllTest) {
  this->btree_map_->insert(this->input.keys.data(), this->input.values.data(), num_keys);
  cuda_try(cudaDeviceSynchronize());
  this->btree_map_->erase(this->input.keys.data(), num_keys);
  cuda_try(cudaDeviceSynchronize());
  this->btree_map_->find(this->input.keys_exist.data(), this->find_results.data(), num_keys);
  cuda_try(cudaDeviceSynchronize());
  EXPECT_EQ(cudaDeviceSynchronize(), cudaSuccess);
  for (std::size_t i = 0; i < num_keys; i++) {
    auto expected_value = this->sentinel_value;
    auto found_value    = this->find_results[i];
    ASSERT_EQ(found_value, expected_value);
  }
}

TYPED_TEST(BTreeMapTest, EraseNoneTest) {
  this->btree_map_->insert(this->input.keys.data(), this->input.values.data(), num_keys);
  cuda_try(cudaDeviceSynchronize());
  this->btree_map_->erase(this->input.keys_not_exist.data(), num_keys);
  cuda_try(cudaDeviceSynchronize());
  this->btree_map_->find(this->input.keys_exist.data(), this->find_results.data(), num_keys);
  cuda_try(cudaDeviceSynchronize());
  // EXPECT_EQ(cudaDeviceSynchronize(), cudaSuccess);
  for (std::size_t i = 0; i < num_keys; i++) {
    auto expected_value = this->input.values[i];
    auto found_value    = this->find_results[i];
    ASSERT_EQ(found_value, expected_value);
  }
}

TYPED_TEST(BTreeMapTest, EraseAllInsertAllTest) {
  this->btree_map_->insert(this->input.keys.data(), this->input.values.data(), num_keys);
  cuda_try(cudaDeviceSynchronize());
  this->btree_map_->erase(this->input.keys.data(), num_keys);
  cuda_try(cudaDeviceSynchronize());
  this->btree_map_->find(this->input.keys_exist.data(), this->find_results.data(), num_keys);
  cuda_try(cudaDeviceSynchronize());
  EXPECT_EQ(cudaDeviceSynchronize(), cudaSuccess);
  for (std::size_t i = 0; i < num_keys; i++) {
    auto expected_value = this->sentinel_value;
    auto found_value    = this->find_results[i];
    ASSERT_EQ(found_value, expected_value);
  }
  EXPECT_EQ(cudaDeviceSynchronize(), cudaSuccess);
  this->btree_map_->insert(this->input.keys.data(), this->input.values.data(), num_keys);
  cuda_try(cudaDeviceSynchronize());
  this->btree_map_->find(this->input.keys_exist.data(), this->find_results.data(), num_keys);
  EXPECT_EQ(cudaDeviceSynchronize(), cudaSuccess);
  cuda_try(cudaDeviceSynchronize());
  for (std::size_t i = 0; i < num_keys; i++) {
    auto expected_value = this->input.values[i];
    auto found_value    = this->find_results[i];
    ASSERT_EQ(found_value, expected_value);
  }
}

}  // namespace

int main(int argc, char** argv) {
  auto arguments = std::vector<std::string>(argv, argv + argc);
  num_keys       = get_arg_value<uint32_t>(arguments, "num-keys").value_or(1024);
  std::cout << "Testing using " << num_keys << " keys\n";
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}