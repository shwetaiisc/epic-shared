/*
 * Copyright (c) 2022-2023, NVIDIA CORPORATION.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <utils.hpp>

#include <cuco/static_map.cuh>

#include <thrust/device_vector.h>
#include <thrust/execution_policy.h>
#include <thrust/functional.h>
#include <thrust/iterator/zip_iterator.h>
#include <thrust/sequence.h>
#include <thrust/tuple.h>

#include <catch2/catch_template_test_macros.hpp>

using size_type = int32_t;

template <typename Map>
void test_erase(Map& map, size_type num_keys)
{
  using Key   = typename Map::key_type;
  using Value = typename Map::mapped_type;

  thrust::device_vector<Key> d_keys(num_keys);
  thrust::device_vector<Value> d_values(num_keys);
  thrust::device_vector<bool> d_keys_exist(num_keys);

  thrust::sequence(thrust::device, d_keys.begin(), d_keys.end(), 1);
  thrust::sequence(thrust::device, d_values.begin(), d_values.end(), 1);

  auto pairs_begin =
    thrust::make_zip_iterator(thrust::make_tuple(d_keys.begin(), d_values.begin()));

  SECTION("Check basic insert/erase")
  {
    map.insert(pairs_begin, pairs_begin + num_keys);

    REQUIRE(map.size() == num_keys);

    map.erase(d_keys.begin(), d_keys.end());

    REQUIRE(map.size() == 0);

    map.contains(d_keys.begin(), d_keys.end(), d_keys_exist.begin());

    REQUIRE(cuco::test::none_of(d_keys_exist.begin(), d_keys_exist.end(), thrust::identity{}));

    map.insert(pairs_begin, pairs_begin + num_keys);

    REQUIRE(map.size() == num_keys);

    map.contains(d_keys.begin(), d_keys.end(), d_keys_exist.begin());

    REQUIRE(cuco::test::all_of(d_keys_exist.begin(), d_keys_exist.end(), thrust::identity{}));

    map.erase(d_keys.begin(), d_keys.begin() + num_keys / 2);
    map.contains(d_keys.begin(), d_keys.end(), d_keys_exist.begin());

    REQUIRE(cuco::test::none_of(
      d_keys_exist.begin(), d_keys_exist.begin() + num_keys / 2, thrust::identity{}));

    REQUIRE(cuco::test::all_of(
      d_keys_exist.begin() + num_keys / 2, d_keys_exist.end(), thrust::identity{}));

    map.erase(d_keys.begin() + num_keys / 2, d_keys.end());
    REQUIRE(map.size() == 0);
  }
}

TEMPLATE_TEST_CASE_SIG(
  "static_map erase tests",
  "",
  ((typename Key, typename Value, cuco::test::probe_sequence Probe, int CGSize),
   Key,
   Value,
   Probe,
   CGSize),
  (int32_t, int32_t, cuco::test::probe_sequence::double_hashing, 1),
  (int32_t, int64_t, cuco::test::probe_sequence::double_hashing, 1),
  (int32_t, int32_t, cuco::test::probe_sequence::double_hashing, 2),
  (int32_t, int64_t, cuco::test::probe_sequence::double_hashing, 2),
  (int64_t, int32_t, cuco::test::probe_sequence::double_hashing, 1),
  (int64_t, int64_t, cuco::test::probe_sequence::double_hashing, 1),
  (int64_t, int32_t, cuco::test::probe_sequence::double_hashing, 2),
  (int64_t, int64_t, cuco::test::probe_sequence::double_hashing, 2),
  (int32_t, int32_t, cuco::test::probe_sequence::linear_probing, 1),
  (int32_t, int64_t, cuco::test::probe_sequence::linear_probing, 1),
  (int32_t, int32_t, cuco::test::probe_sequence::linear_probing, 2),
  (int32_t, int64_t, cuco::test::probe_sequence::linear_probing, 2),
  (int64_t, int32_t, cuco::test::probe_sequence::linear_probing, 1),
  (int64_t, int64_t, cuco::test::probe_sequence::linear_probing, 1),
  (int64_t, int32_t, cuco::test::probe_sequence::linear_probing, 2),
  (int64_t, int64_t, cuco::test::probe_sequence::linear_probing, 2))
{
  constexpr size_type num_keys{1'000'000};

  using probe =
    std::conditional_t<Probe == cuco::test::probe_sequence::linear_probing,
                       cuco::experimental::linear_probing<CGSize, cuco::murmurhash3_32<Key>>,
                       cuco::experimental::double_hashing<CGSize,
                                                          cuco::murmurhash3_32<Key>,
                                                          cuco::murmurhash3_32<Key>>>;

  auto map = cuco::experimental::static_map<Key,
                                            Value,
                                            cuco::experimental::extent<size_type>,
                                            cuda::thread_scope_device,
                                            thrust::equal_to<Key>,
                                            probe,
                                            cuco::cuda_allocator<std::byte>,
                                            cuco::experimental::storage<2>>{
    num_keys, cuco::empty_key<Key>{-1}, cuco::empty_value<Value>{-1}, cuco::erased_key<Key>{-2}};

  test_erase(map, num_keys);
}
