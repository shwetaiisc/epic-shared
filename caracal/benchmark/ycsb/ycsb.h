#ifndef YCSB_H
#define YCSB_H

#include <random>

#include "table_decl.h"
#include "epoch.h"
#include "slice.h"
#include "index.h"

#include "zipfian_random.h"

namespace ycsb {

enum class TableType : int {
  YCSBBase = 200,
  Ycsb,
};

struct Ycsb {
  static uint32_t HashKey(const felis::VarStrView &k) {
    auto x = (uint8_t *) k.data();
    return *(uint32_t *) x;
  }

  static constexpr auto kTable = TableType::Ycsb;
  static constexpr auto kIndexArgs = std::make_tuple(HashKey, 10000000, false);

  using IndexBackend = felis::HashtableIndex;
  using Key = sql::YcsbKey;
  using Value = sql::YcsbValue;
  using Field = sql::YcsbField;
};

using RandRng = foedus::assorted::ZipfianRandom;

class Client : public felis::EpochClient {
  // Zipfian random generator
  RandRng rand;
  std::mt19937 rng;
  std::uniform_int_distribution<int> field_dist;
  std::uniform_int_distribution<int> write_dist;
  int write_threshold;

  friend class RMWTxn;
  static char zero_data[100];
 public:
  static double g_theta;
  static size_t g_table_size;
  static int g_extra_read;
  static int g_contention_key;
  static bool g_dependency;

  Client() noexcept;
  unsigned int LoadPercentage() final override { return 100; }
  felis::BaseTxn *CreateTxn(uint64_t serial_id) final override;

  template <typename T> T GenerateTransactionInput();
};

class YcsbLoader : public go::Routine {
  std::atomic_bool done = false;
 public:
  YcsbLoader() {}
  void Run() override final;
  void Wait() { while (!done) sleep(1); }
};

}

namespace felis {

using namespace ycsb;

SHARD_TABLE(Ycsb) { return 0; }

}

#endif
