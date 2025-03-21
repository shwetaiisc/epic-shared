#ifndef TXN_H
#define TXN_H

#include <cstdlib>
#include <cstdint>
#include <array>
#include <initializer_list>

#include "epoch.h"
#include "varstr.h"
#include "piece.h"
#include "slice.h"
#include "contention_manager.h"
#include "opts.h"

namespace felis {

class VHandle;
class Relation;
class EpochClient;

using UpdateForKeyCallback = void (*)(VHandle *row, void *ctx);

class BaseTxn {
 protected:
  friend class EpochClient;

  Epoch *epoch;
  uint64_t sid;

  using BrkType = std::array<mem::Brk *, NodeConfiguration::kMaxNrThreads / mem::kNrCorePerNode>;
  static BrkType g_brk;
  static int g_cur_numa_node;

 public:
  BaseTxn(uint64_t serial_id)
      : epoch(nullptr), sid(serial_id) {}

  static void *operator new(size_t nr_bytes) { return g_brk[g_cur_numa_node]->Alloc(nr_bytes); }
  static void operator delete(void *ptr) {}
  static void InitBrk(long nr_epochs);

  virtual void PrepareState() {}

  virtual ~BaseTxn() {}
  virtual void Prepare() = 0;
  virtual void PrepareInsert() = 0;
  virtual void Run() = 0;

 private:
  // Entry functions for Epoch. Granola, PWV and Felis all have different
  // requirements on each phase is executed (in-order vs out-of-order).
  void Run0() {
    PieceRoutine *last = nullptr;
    if (EpochClient::g_enable_granola || EpochClient::g_enable_pwv) {
      PrepareInsert();
      Prepare();
      if (EpochClient::g_enable_pwv)
        last = root_promise()->last();
    }
    Run();

    if (!last) { // !PWV
      root_promise()->AssignSchedulingKey(serial_id());
    } else { // PWV
      uint64_t k = 0;
      for (int i = 0; i < root_promise()->nr_routines(); i++) {
        auto r = root_promise()->routine(i);
        r->sched_key = k;
        if (r == last) k = serial_id();
      }
    }
  }
  void Prepare0() {
    if (EpochClient::g_enable_granola || EpochClient::g_enable_pwv)
      return;

    Prepare();
  }
  void PrepareInsert0() {
    if (EpochClient::g_enable_granola || EpochClient::g_enable_pwv)
      return;
    PrepareInsert();
  }
 public:

  virtual BasePieceCollection *root_promise() = 0;
  virtual void ResetRoot() = 0;

  uint64_t serial_id() const { return sid; }
  uint64_t epoch_nr() const { return sid >> 32; }

  class BaseTxnRow {
   protected:
    uint64_t sid;
    uint64_t epoch_nr;
    VHandle *vhandle;
   public:
    BaseTxnRow(uint64_t sid, uint64_t epoch_nr, VHandle *vhandle)
        : sid(sid), epoch_nr(epoch_nr), vhandle(vhandle) {}

    uint64_t serial_id() const { return sid; }

    void AppendNewVersion(int ondemand_split_weight = 0);
    VarStr *ReadVarStr();
    bool WriteVarStr(VarStr *obj);
    bool Delete() { return WriteVarStr(nullptr); }
  };

  class BaseTxnHandle {
   protected:
    uint64_t sid;
    uint64_t epoch_nr;
   public:
    BaseTxnHandle(uint64_t sid, uint64_t epoch_nr) : sid(sid), epoch_nr(epoch_nr) {}
    BaseTxnHandle() {}

    uint64_t serial_id() const { return sid; }

    // C++ wrapper will name hide this.
    BaseTxnRow operator()(VHandle *vhandle) const { return BaseTxnRow(sid, epoch_nr, vhandle); }
  };

  // C++ wrapper will name hide this.
  BaseTxnHandle index_handle() { return BaseTxnHandle(sid, epoch->id()); }

  // Low level API for UpdateForKey (on demand splitting)
  int64_t UpdateForKeyAffinity(int node, VHandle *row);

  struct BaseTxnIndexOpContext {
    static constexpr size_t kMaxPackedKeys = 15;
    BaseTxnHandle handle;
    EpochObject state;

    // We can batch a lot of keys in the same context. We also should mark if
    // some keys are not used at all. Therefore, we need a bitmap.
    uint16_t keys_bitmap;
    uint16_t slices_bitmap;
    uint16_t rels_bitmap;

    uint16_t key_len[kMaxPackedKeys];
    const uint8_t *key_data[kMaxPackedKeys];
    int16_t slice_ids[kMaxPackedKeys];
    int16_t relation_ids[kMaxPackedKeys];

    template <typename Func>
    static void ForEachWithBitmap(uint16_t bitmap, Func f) {
      for (int i = 0, j = 0; i < kMaxPackedKeys; i++) {
        const uint16_t mask = (1 << i);
        if (bitmap & mask) {
          f(j, i);
          j++;
        }
      }
    }

    // We don't need to worry about padding because TxnHandle is perfectly padded.
    // We also need to send three bitmaps.
    static constexpr size_t kHeaderSize =
        sizeof(BaseTxnHandle) + sizeof(EpochObject)
        + sizeof(uint16_t) + sizeof(uint16_t) + sizeof(uint16_t);

    BaseTxnIndexOpContext(BaseTxnHandle handle, EpochObject state,
                      uint16_t keys_bitmap, VarStr **keys,
                      uint16_t slices_bitmap, int16_t *slice_ids,
                      uint16_t rels_bitmap, int16_t *rels);


    BaseTxnIndexOpContext() {}

    size_t EncodeSize() const;
    uint8_t *EncodeTo(uint8_t *buf) const;
    const uint8_t *DecodeFrom(const uint8_t *buf);
  };

  static constexpr size_t kMaxRangeScanKeys = 32;
  using LookupRowResult = std::array<VHandle *, kMaxRangeScanKeys>;

  static LookupRowResult BaseTxnIndexOpLookup(const BaseTxnIndexOpContext &ctx, int idx);
  static VHandle *BaseTxnIndexOpInsert(const BaseTxnIndexOpContext &ctx, int idx);
};

class BaseFutureValue {
 public:
  static constexpr uint8_t kMaxSubscription = 6;
 protected:
   protected:
  std::atomic_bool ready = false;
  uint8_t nr_nodes = 0;
  uint8_t nodes[kMaxSubscription];
 public:
  BaseFutureValue() {}
  BaseFutureValue(const BaseFutureValue &rhs) : ready(rhs.ready.load()) {}
  const BaseFutureValue &operator=(const BaseFutureValue &rhs) {
    ready = rhs.ready.load();
    return *this;
  }
  void Signal();
  void Wait();
  void Subscribe(uint8_t node) { nodes[nr_nodes++] = node; }
  virtual size_t EncodeSize() { return 0; }
  virtual void EncodeTo(uint8_t *buf) {}
  virtual void DecodeFrom(const uint8_t *buf) {}
 protected:
  GenericEpochObject<BaseFutureValue> ConvertToEpochObject() { return EpochObject::Convert(this); }
};

}

#endif /* TXN_H */
