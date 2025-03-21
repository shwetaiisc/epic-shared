#include <random>

#include "ycsb.h"
#include "index.h"
#include "txn_cc.h"
#include "pwv_graph.h"
#include "util/os.h"

namespace ycsb {

using namespace felis;

static constexpr int kTotal = 10;
static constexpr int kNrMSBContentionKey = 6;

class DummySliceRouter {
 public:
  static int SliceToNodeId(int16_t slice_id) { return 1; } // Always on node 1
};


// static uint64_t *g_permutation_map;

enum YcsbOpType: uint8_t {
    READ, FULLREAD, UPDATE, RMW
};

struct RMWStruct {
  uint64_t keys[kTotal];
  YcsbOpType ops[kTotal];
  uint8_t fields[kTotal];
};

struct RMWState {
  VHandle *rows[kTotal];
  YcsbOpType ops[kTotal];
  uint8_t fields[kTotal];
  InvokeHandle<RMWState> futures[kTotal];

  std::atomic_ulong signal; // Used only if g_dependency
  FutureValue<void> deps; // Used only if g_dependency

  struct LookupCompletion : public TxnStateCompletion<RMWState> {
    void operator()(int id, BaseTxn::LookupRowResult rows) {
      state->rows[id] = rows[0];
//      if (id < kTotal - Client::g_extra_read) {
//        bool last = (id == kTotal - Client::g_extra_read - 1);
//        handle(rows[0]).AppendNewVersion(last ? 0 : 1);
//      }
      if (state->ops[id] == YcsbOpType::RMW || state->ops[id] == YcsbOpType::UPDATE) {
        handle(rows[0]).AppendNewVersion(1);
      }
    }
  };
};

template <>
RMWStruct Client::GenerateTransactionInput<RMWStruct>()
{
  RMWStruct s;

  int nr_lsb = 63 - __builtin_clzll(g_table_size) - kNrMSBContentionKey;
  size_t mask = 0;
  if (nr_lsb > 0) mask = (1 << nr_lsb) - 1;

  for (int i = 0; i < kTotal; i++) {
 again:
    // s.keys[i] = g_permutation_map[rand.next() % g_table_size];
    s.keys[i] = rand.next() % g_table_size;
    if (i < g_contention_key) {
      s.keys[i] &= ~mask;
    } else {
      if ((s.keys[i] & mask) == 0)
        goto again;
    }
    for (int j = 0; j < i; j++)
      if (s.keys[i] == s.keys[j])
        goto again;
    s.fields[i] = field_dist(rng);
    if (write_dist(rng) < write_threshold)
      s.ops[i] = YcsbOpType::RMW;
    else
      s.ops[i] = full_read ? YcsbOpType::FULLREAD : YcsbOpType::READ;
  }

  return s;
}

char Client::zero_data[100];

class RMWTxn : public Txn<RMWState>, public RMWStruct {
  Client *client;
 public:
  RMWTxn(Client *client, uint64_t serial_id);
  void Run() override final;
  void Prepare() override final;
  void PrepareInsert() override final {}
  static void WriteRow(TxnRow vhandle);
  static void ReadRow(TxnRow vhandle);
  static void ReadField(TxnRow vhandle, uint8_t field_id);
  static void UpdateRow(TxnRow vhandle);

  template <typename Func>
  void RunOnPartition(Func f) {
    auto handle = index_handle();
    for (int i = 0; i < kTotal; i++) {
      auto part = (keys[i] * NodeConfiguration::g_nr_threads) / Client::g_table_size;
      f(part, root, Tuple<unsigned long, int, decltype(state), decltype(handle), int>(keys[i], i, state, handle, part));
    }
  }
};

RMWTxn::RMWTxn(Client *client, uint64_t serial_id)
    : Txn<RMWState>(serial_id),
      RMWStruct(client->GenerateTransactionInput<RMWStruct>()),
      client(client)
{}

void RMWTxn::Prepare()
{
  if (!VHandleSyncService::g_lock_elision) {
    Ycsb::Key dbk[kTotal];
    for (int i = 0; i < kTotal; i++) dbk[i].k = keys[i];
    INIT_ROUTINE_BRK(8192);

    memcpy(state->ops, ops, sizeof(ops));
    memcpy(state->fields, fields, sizeof(fields));
    // Omit the return value because this workload is totally single node
    TxnIndexLookup<DummySliceRouter, RMWState::LookupCompletion, void>(
        nullptr,
        KeyParam<Ycsb>(dbk, kTotal));
  } else {
    static constexpr auto LookupIndex = [](auto k, int i, auto state, auto handle) {
      auto &rel = util::Instance<TableManager>().Get<ycsb::Ycsb>();
      Ycsb::Key dbk;
      dbk.k = k;
      void *buf = alloca(512);
      state->rows[i] = rel.Search(dbk.EncodeView(buf));
      if (i < kTotal - Client::g_extra_read)
        handle(state->rows[i]).AppendNewVersion();
    };
    if (Client::g_enable_pwv) {
      RunOnPartition(
          [this](auto part, auto root, const auto &t) {
            auto [_1, i, _2, _3, _part] = t;
            util::Instance<PWVGraphManager>()[part]->ReserveEdge(serial_id());
          });
    }
    RunOnPartition(
        [this](auto part, auto root, const auto &t) {
          root->AttachRoutine(
              t, 1, // Always on the local node.
              [](auto &ctx) {
                auto [k, i, state, handle, part] = ctx;
                LookupIndex(k, i, state, handle);

                if (Client::g_enable_pwv)
                  util::Instance<PWVGraphManager>()[part]->AddResource(
                      handle.serial_id(), PWVGraph::VHandleToResource(state->rows[i]));
              },
              part); // Partitioning affinity.

        });

  }
}

void RMWTxn::WriteRow(TxnRow vhandle)
{
  auto dbv = vhandle.Read<Ycsb::Value>();
  dbv.v.assign(Client::zero_data, sql::kYcsbFieldSize);
  dbv.v.resize_junk(sql::kYcsbRecordSize - 1);
  vhandle.Write(dbv);
}

void RMWTxn::ReadRow(TxnRow vhandle)
{
  vhandle.Read<Ycsb::Value>();
//  vhandle.Read<Ycsb::Field>();
}

void RMWTxn::ReadField(TxnRow vhandle, uint8_t field_id)
{
  vhandle.ReadField<Ycsb::Value>(field_id, sql::kYcsbFieldSize);
}

void RMWTxn::UpdateRow(TxnRow vhandle)
{
  Ycsb::Value dbv;
  dbv.v.resize_junk(sql::kYcsbRecordSize - 1);
  vhandle.Write(dbv);
}

void RMWTxn::Run()
{
  if (Client::g_dependency)
    state->signal = 0;

  if (!Options::kEnablePartition) {
//    auto bitmap = 1ULL << (kTotal - Client::g_extra_read - 1);
//    for (int i = 0; i < kTotal - Client::g_extra_read - 1; i++) {
//      state->futures[i] = UpdateForKey(
//          1, state->rows[i],
//          [](const auto &ctx, VHandle *row) {
//            auto &[state, index_handle] = ctx;
//            WriteRow(index_handle(row));
//            if (Client::g_dependency
//                && state->signal.fetch_add(1) + 1 == kTotal - Client::g_extra_read - 1)
//              state->deps.Signal();
//          });
//
//      if (state->futures[i].has_callback())
//        bitmap |= 1ULL << i;
//    }

    auto aff = std::numeric_limits<uint64_t>::max();
    // auto aff = AffinityFromRows(bitmap, state->rows);
    root->AttachRoutine(
        MakeContext(), 1,
        [](const auto &ctx) {
          auto &[state, index_handle] = ctx;
//          for (int i = 0; i < kTotal - Client::g_extra_read - 1; i++) {
//            state->futures[i].Invoke(state, index_handle);
//          }
//          if (Client::g_dependency) {
//            state->deps.Wait();
//          }
//          WriteRow(index_handle(state->rows[kTotal - Client::g_extra_read - 1]));
//          for (auto i = kTotal - Client::g_extra_read; i < kTotal; i++) {
//            ReadRow(index_handle(state->rows[i]));
//          }
          for (int i = 0; i < kTotal; i++) {
            if (state->ops[i] == YcsbOpType::FULLREAD)
              ReadRow(index_handle(state->rows[i]));
            else if (state->ops[i] == YcsbOpType::READ)
              ReadField(index_handle(state->rows[i]), state->fields[i]);
            else if (state->ops[i] == YcsbOpType::UPDATE)
              UpdateRow(index_handle(state->rows[i]));
            else if (state->ops[i] == YcsbOpType::RMW)
              WriteRow(index_handle(state->rows[i]));
          }
        },
        aff);

  } else if (Client::g_enable_granola || Client::g_enable_pwv) {
    RunOnPartition(
        [this](auto part, auto root, const auto &t) {
          root->AttachRoutine(
              t, 1,
              [](auto &ctx) {
                auto &[k, i, state, handle, _part] = ctx;

                if (Client::g_dependency && i == kTotal - Client::g_extra_read - 1) {
                  while (state->signal != i) _mm_pause();
                }

                TxnRow vhandle = handle(state->rows[i]);
                auto dbv = vhandle.Read<Ycsb::Value>();

                static thread_local volatile char buffer[100];
                std::copy(dbv.v.data(), dbv.v.data() + 100, buffer);

                if (i < kTotal - Client::g_extra_read) {
                  dbv.v.resize_junk(90);
                  vhandle.Write(dbv);
                  if (Client::g_dependency && i < kTotal - Client::g_extra_read - 1) {
                    state->signal.fetch_add(1);
                  }
                }

                if (Client::g_enable_pwv) {
                  util::Instance<PWVGraphManager>().local_graph()->ActivateResource(
                      handle.serial_id(), PWVGraph::VHandleToResource(state->rows[i]));
                }
              },
              part);
        });
  } else {
    // Bohm
    RunOnPartition(
        [this](auto part, auto root, const auto &t) {
          const auto &[k, i, _1, _2, _part] = t;
          if (i > kTotal - Client::g_extra_read) return;

          static thread_local volatile char buffer[100];

          if (i == kTotal - Client::g_extra_read) {
            // All reads here
            root->AttachRoutine(
                t, 1,
                [](auto &ctx) {
                  auto [k, i, state, handle, _part] = ctx;

                  TxnRow vhandle = handle(state->rows[i]);
                  auto v = vhandle.Read<Ycsb::Value>();
                  std::copy(v.v.data(), v.v.data() + 100, buffer);
                });
          } else {
            root->AttachRoutine(
                t, 1,
                [](auto &ctx) {
                  auto [k, i, state, handle, _part] = ctx;
                  // Last write
                  if (Client::g_dependency && i == kTotal - Client::g_extra_read - 1) {
                    while (state->signal != i) _mm_pause();
                  }

                  TxnRow vhandle = handle(state->rows[i]);
                  auto v = vhandle.Read<Ycsb::Value>();

                  std::copy(v.v.data(), v.v.data() + 100, buffer);

                  v.v.resize_junk(90);
                  vhandle.Write(v);
                  state->signal.fetch_add(1);
                }, part);
          }
        });
  }
}

void YcsbLoader::Run()
{
  auto &mgr = util::Instance<felis::TableManager>();
  mgr.Create<Ycsb>();

  void *buf = alloca(512);

  auto nr_threads = NodeConfiguration::g_nr_threads;
  for (auto t = 0; t < nr_threads; t++) {
    printf("t = %d\n", t);
    MasstreeIndex::ResetThreadInfo();

    mem::ParallelPool::SetCurrentAffinity(t);
    util::Cpu info;
    info.set_affinity(t);
    info.Pin();

    unsigned long start = t * Client::g_table_size / nr_threads;
    unsigned long end = (t + 1) * Client::g_table_size / nr_threads;

    for (unsigned long i = start; i < end; i++) {
      Ycsb::Key dbk;
      Ycsb::Value dbv;
      dbk.k = i;
      dbv.v.resize_junk(sql::kYcsbRecordSize - 1);
      auto handle = mgr.Get<ycsb::Ycsb>().SearchOrCreate(dbk.EncodeView(buf));
      // TODO: slice mapping table stuff?
      felis::InitVersion(handle, dbv.Encode());
    }
  }
  util::Cpu info;
  info.set_affinity(go::Scheduler::CurrentThreadPoolId() - 1);
  info.Pin();

  mem::ParallelPool::SetCurrentAffinity(-1);
  MasstreeIndex::ResetThreadInfo();

  done = true;

  // Generate a random permutation
#if 0
  g_permutation_map = new uint64_t[Client::g_table_size];
  for (size_t i = 0; i < Client::g_table_size; i++) {
    g_permutation_map[i] = i;
  }
  util::FastRandom perm_rand(1001);
  for (size_t i = Client::g_table_size - 1; i >= 1; i--) {
    auto j = perm_rand.next() % (i + 1);
    std::swap(g_permutation_map[j], g_permutation_map[i]);
  }
#endif
}

size_t Client::g_table_size = 1000000;
double Client::g_theta = 0.00;
int Client::g_extra_read = 0;
int Client::g_contention_key = 0;
bool Client::g_dependency = false;

Client::Client() noexcept : rng{std::random_device{}()}, field_dist{0, sql::kYcsbNumFields - 1}, write_dist{0, 99}
{
  std::random_device rd;
  rand.init(g_table_size, g_theta, rd());
  if (ycsb_type == YcsbType::YCSB_A) {
    write_threshold = 50;
  } else if (ycsb_type == YcsbType::YCSB_B) {
    write_threshold = 5;
  } else if (ycsb_type == YcsbType::YCSB_C) {
    write_threshold = 0;
  } else {
    write_threshold = 50;
  }
}

BaseTxn *Client::CreateTxn(uint64_t serial_id)
{
  return new RMWTxn(this, serial_id);
}

}
