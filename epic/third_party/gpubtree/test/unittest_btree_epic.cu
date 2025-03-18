
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
#include <random>
#include <unordered_set>
#include <map>
#include <chrono>

std::size_t num_keys = 30000;

namespace {
using key_type   = uint32_t;
using value_type = uint32_t;

union PackedCustomerOrderKey {
  using baseType = uint64_t;
  constexpr static baseType max_o_id = (1ull << 20) - 1;
  constexpr static baseType invalid_key = (1ull << 42) - 1;
  struct {
    baseType o_id : 20;
    baseType c_id : 12;
    baseType d_id : 4;
    baseType w_id : 6;
  };
  baseType base_key        = 0;
  PackedCustomerOrderKey() = default;
  PackedCustomerOrderKey(baseType o_id, baseType c_id, baseType d_id, baseType w_id) {
    base_key        = 0;
    this->o_id   = o_id;
    this->c_id   = c_id;
    this->d_id   = d_id;
    this->w_id = w_id;
  }
};

class RandomKeyGen {
  std::mt19937 gen;
  std::uniform_int_distribution<uint64_t> w_id_dist, d_id_dist, c_id_dist, o_id_dist, val_dist;

 public:
  RandomKeyGen()
      : gen(std::random_device{}())
      , w_id_dist(1, 64)
      , d_id_dist(1, 10)
      , c_id_dist(1, 3000)
      , o_id_dist(0, PackedCustomerOrderKey::max_o_id - 1)
      , val_dist(0, (1ull << 22) - 2) {}
  PackedCustomerOrderKey next() {
    return PackedCustomerOrderKey{o_id_dist(gen), c_id_dist(gen), d_id_dist(gen), w_id_dist(gen)};
  }
  uint64_t rand_val() { return val_dist(gen); }
};

TEST(PackedCustomerOrderKeyTest, TestLayout) {
  PackedCustomerOrderKey key;
  key.o_id = (1ull << 20) - 1;
  EXPECT_EQ(key.base_key, (1ull << 20) - 1);
  key.c_id = (1ull << 12) - 1;
  EXPECT_EQ(key.base_key, (1ull << 32) - 1);
  key.d_id = (1ull << 4) - 1;
  EXPECT_EQ(key.base_key, (1ull << 36) - 1);
  key.w_id = (1ull << 6) - 1;
  EXPECT_EQ(key.base_key, (1ull << 42) - 1);
}

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
      , keys_not_exist(input_num_keys)
      , max_keys(input_num_keys)
    , non_exist_keys_upper_bound(input_num_keys)
  {
    make_input();
  }
  void make_input() {
    RandomKeyGen key_gen;
    for (std::size_t i = 0; i < num_keys; i++) {
      // Make sure that the input doesn't contain 0
      // and, queries that do not exist in the table are uniformly distributed to avoid
      // contention... an optimzation is to avoid locking if key doesn't exist in node
      PackedCustomerOrderKey key;
      do {
        key = key_gen.next();
      } while (existing_keys.count(key.base_key) > 0);
      value_type val = key_gen.rand_val();
      existing_keys[key.base_key] = val;
      keys[i] = key.base_key;
      keys_exist[i] = key.base_key;
      values[i] = val;

      // upper bound with the same customer
      key.o_id = PackedCustomerOrderKey::max_o_id;
      max_keys[i] = key.base_key;
    }

    for (std::size_t i = 0; i < num_keys; i++) {
      // Make sure that the input doesn't contain 0
      // and, queries that do not exist in the table are uniformly distributed to avoid
      // contention... an optimzation is to avoid locking if key doesn't exist in node
      PackedCustomerOrderKey key;
      do {
        key = key_gen.next();
      } while (existing_keys.count(key.base_key) > 0 || non_existing_keys.count(key.base_key) > 0);
      keys_not_exist[i] = key.base_key;

      // upper bound with the same customer
      key.o_id = PackedCustomerOrderKey::max_o_id;
      non_exist_keys_upper_bound[i] = key.base_key;
    }
  }
  void free() {
    keys.free();
    values.free();
    keys_exist.free();
    keys_not_exist.free();
    max_keys.free();
  }

  std::size_t num_keys;
  mapped_vector<key_type> keys;
  mapped_vector<value_type> values;
  mapped_vector<key_type> keys_exist;
  mapped_vector<key_type> keys_not_exist;
  mapped_vector<key_type> max_keys;;
  mapped_vector<key_type> non_exist_keys_upper_bound;;
  std::map<key_type, value_type> existing_keys;
  std::unordered_set<key_type> non_existing_keys;
};

template <class MapData>
class BTreeMapTest : public testing::Test {
 protected:
  BTreeMapTest() { btree_map_ = new typename map_data::btree_map(); }
  ~BTreeMapTest() override { delete btree_map_; }
  using map_data         = MapData;
  using input_key_type   = typename map_data::btree_map::key_type;
  using input_value_type = typename map_data::btree_map::value_type;
  using input_pair_type = typename map_data::btree_map::pair_type;
  typename map_data::btree_map* btree_map_;
  testing_input<input_key_type, input_value_type> input{num_keys};
  mapped_vector<input_value_type> find_results{num_keys};
  mapped_vector<input_pair_type> find_pair_results{num_keys};
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

using node_type = GpuBTree::node_type<key_type,
                                      value_type,
                                      TreeParam::BranchingFactor,
                                      var_pair_type<uint64_t, uint64_t, 42, 22>>;
static_assert(sizeof(node_type) == 128);

using bump_allocator_type = device_bump_allocator<node_type, 100000>;
using slab_allocator_type = device_allocator::SlabAllocLight<node_type,
                                                             SlabAllocParam::NumSuperBlocks,
                                                             SlabAllocParam::NumMemoryBlocks,
                                                             SlabAllocParam::TileSize,
                                                             SlabAllocParam::SlabSize>;

typedef testing::Types<
    BTreeMapData<GpuBTree::gpu_blink_tree<uint64_t,
                                          uint64_t,
                                          TreeParam::BranchingFactor,
                                          bump_allocator_type,
                                          var_pair_type<uint64_t, uint64_t, 42, 22>>>,
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
      keys, [&](auto key) { return this->input.existing_keys[key]; }));
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

TYPED_TEST(BTreeMapTest, FindNextExistTest) {
  EXPECT_EQ(cudaDeviceSynchronize(), cudaSuccess);
  auto start = std::chrono::high_resolution_clock::now();
  this->btree_map_->insert(this->input.keys.data(), this->input.values.data(), num_keys);
  cuda_try(cudaDeviceSynchronize());
  auto end = std::chrono::high_resolution_clock::now();
  std::cout << "Inserting " << num_keys << " took "
            << std::chrono::duration_cast<std::chrono::microseconds>(end - start).count() << "us" << std::endl;
  this->btree_map_->find_next(this->input.keys_exist.data(),
                              this->input.max_keys.data(),
                              this->find_pair_results.data(),
                              num_keys);
  cuda_try(cudaDeviceSynchronize());
  EXPECT_EQ(cudaDeviceSynchronize(), cudaSuccess);
  for (std::size_t i = 0; i < num_keys; i++) {
    auto expected_key = this->input.keys[i];
    auto found_key    = this->find_pair_results[i].first;
    EXPECT_EQ(found_key, expected_key);

    auto expected_value = this->input.values[i];
    auto found_value    = this->find_pair_results[i].second;
    EXPECT_EQ(found_value, expected_value);
  }
}

TYPED_TEST(BTreeMapTest, FindNextNonExistTest) {
  EXPECT_EQ(cudaDeviceSynchronize(), cudaSuccess);
  auto start = std::chrono::high_resolution_clock::now();
  this->btree_map_->insert(this->input.keys.data(), this->input.values.data(), num_keys);
  cuda_try(cudaDeviceSynchronize());
  auto end = std::chrono::high_resolution_clock::now();
  std::cout << "Inserting " << num_keys << " took "
            << std::chrono::duration_cast<std::chrono::microseconds>(end - start).count() << "us" << std::endl;

  start = std::chrono::high_resolution_clock::now();
  this->btree_map_->find_next(this->input.keys_not_exist.data(),
                              this->input.non_exist_keys_upper_bound.data(),
                              this->find_pair_results.data(),
                              num_keys);
  cuda_try(cudaDeviceSynchronize());
  end = std::chrono::high_resolution_clock::now();
  std::cout << "Finding Next for " << num_keys << " took "
            << std::chrono::duration_cast<std::chrono::microseconds>(end - start).count() << "us" << std::endl;

  EXPECT_EQ(cudaDeviceSynchronize(), cudaSuccess);

  uint64_t num_found = 0;
  for (std::size_t i = 0; i < num_keys; i++) {
    PackedCustomerOrderKey expect_key;
    expect_key.base_key = PackedCustomerOrderKey::invalid_key;
    constexpr value_type invalid_value = (1ull << 22) - 1;
    value_type expected_value = invalid_value;

    PackedCustomerOrderKey lowerbound_key;
    lowerbound_key.base_key = this->input.keys_not_exist[i];

    auto it = this->input.existing_keys.lower_bound(lowerbound_key.base_key);
    if (it != this->input.existing_keys.end()) {
      expect_key.base_key = it->first;

      // looking for orders of the same customer
      // if not the same customer, should return invalid kv pair
      if (expect_key.w_id != lowerbound_key.w_id
        || expect_key.d_id != lowerbound_key.d_id
        || expect_key.c_id != lowerbound_key.c_id) {
        expect_key.base_key = PackedCustomerOrderKey::invalid_key;
      } else {
        expected_value = this->input.existing_keys[expect_key.base_key];
        ++num_found;
      }
    }

    auto expected_key = expect_key.base_key;
    auto found_key    = this->find_pair_results[i].first;
    EXPECT_EQ(found_key, expected_key);

    auto found_value    = this->find_pair_results[i].second;
    EXPECT_EQ(found_value, expected_value);
  }
  std::cout << "Number of keys found for range_find_next: " << num_found << std::endl;
}

}  // namespace

int main(int argc, char** argv) {
  auto arguments = std::vector<std::string>(argv, argv + argc);
  num_keys       = get_arg_value<uint32_t>(arguments, "num-keys").value_or(500000);
  std::cout << "Testing using " << num_keys << " keys\n";
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}