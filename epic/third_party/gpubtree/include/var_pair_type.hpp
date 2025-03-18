//
// Created by Shujian Qian on 2024-03-20.
//

#pragma once
#include <bit>
#include <cstdint>
#include <limits>
#include <type_traits>

#include <macros.hpp>

template <typename Key,
          typename Value,
          size_t KeyBits   = sizeof(Key) * 8,
          size_t ValueBits = sizeof(Value) * 8>
struct __align__(8) var_pair_type {
 public:
  using key_type       = Key;
  using value_type     = Value;
  using size_type      = uint32_t;
  using this_pair_type = var_pair_type<Key, Value, KeyBits, ValueBits>;

  constexpr static size_t key_bits   = KeyBits;
  constexpr static size_t value_bits = ValueBits;

  template <typename UintType>
  HOST_DEVICE_QUALIFIER constexpr static UintType lowest_kbit_set(uint32_t k) {
    static_assert(std::is_unsigned<UintType>::value, "UintType must be an unsigned type");
    return (k == sizeof(UintType) * 8) ? std::numeric_limits<UintType>::max() : (1ULL << k) - 1;
  }

  constexpr static key_type invalid_key     = lowest_kbit_set<key_type>(key_bits);
  constexpr static value_type invalid_value = lowest_kbit_set<value_type>(value_bits);

  static_assert(key_bits > 0, "KeyBits must be greater than 0");
  static_assert(key_bits <= sizeof(Key) * 8,
                "KeyBits must be less than or equal to number of bits in Key, otherwise data loss "
                "will occur");
  static_assert(value_bits > 0, "ValueBits must be greater than 0");
  static_assert(value_bits <= sizeof(Value) * 8,
                "ValueBits must be less than or equal to number of bits in Value, otherwise data "
                "loss will occur");
  static_assert(key_bits + value_bits == 64, "KeyBits + ValueBits must be 64");

  constexpr HOST_DEVICE_QUALIFIER var_pair_type(const key_type& key, const value_type& value)
      : first(key), second(value) {
    static_assert(sizeof(*this) == 8, "Size of var_pair_type must be 8 bytes");
    //    static_assert(offsetof(typeof(*this), second) ==0, "second must be the last member of
    //    var_pair_type");
    //    union pair_to_int{
    //      uint64_t int_val;
    //      this_pair_type pair;
    //      constexpr pair_to_int(uint64_t int_val) : int_val(int_val) {}
    //    };
    //    static_assert(pair_to_int(lowest_kbit_set<uint64_t>(value_bits)).pair.first == 0ull,
    //    "first must be 0");
    //    static_assert(pair_to_int(lowest_kbit_set<uint64_t>(value_bits)).pair.second == 0ull,
    //    "first must be 0"); static_assert(reinterpret_cast<uint64_t>(this_pair_type(1, 2)) == 0);
  }

  constexpr HOST_DEVICE_QUALIFIER var_pair_type(const uint64_t& int_val) {
    static_assert(sizeof(*this) == 8, "Size of var_pair_type must be 8 bytes");
  }

  HOST_DEVICE_QUALIFIER var_pair_type() : first(invalid_key), second(invalid_value){};

  HOST_DEVICE_QUALIFIER var_pair_type(
      const var_pair_type<key_type, value_type, key_bits, value_bits>& other) {
    first  = other.first;
    second = other.second;
  }

  HOST_DEVICE_QUALIFIER var_pair_type(
      const volatile var_pair_type<key_type, value_type, key_bits, value_bits>& other) {
    first  = other.first;
    second = other.second;
  }

  HOST_DEVICE_QUALIFIER var_pair_type<key_type, value_type, key_bits, value_bits>& operator=(
      const var_pair_type<key_type, value_type, key_bits, value_bits>& other) {
    first  = other.first;
    second = other.second;
    return *this;
  }

  HOST_DEVICE_QUALIFIER volatile var_pair_type<key_type, value_type, key_bits, value_bits>&
  operator=(
      const volatile var_pair_type<key_type, value_type, key_bits, value_bits>& other) volatile {
    first  = other.first;
    second = other.second;
    return *this;
  }

  HOST_DEVICE_QUALIFIER bool operator==(
      const var_pair_type<key_type, value_type, key_bits, value_bits>& other) {
    return (first == other.first) && (second == other.second);
  }

  HOST_DEVICE_QUALIFIER bool operator!=(
      const var_pair_type<key_type, value_type, key_bits, value_bits>& other) {
    return (first != other.first) || (second != other.second);
  }

  value_type second : ValueBits;
  key_type first : KeyBits;
};