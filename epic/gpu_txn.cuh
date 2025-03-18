//
// Created by Shujian Qian on 2023-08-23.
//

#ifndef GPU_TXN_CUH
#define GPU_TXN_CUH

#include <type_traits>

#include "txn.h"
#include "util_log.h"
#include "util_math.h"
#include "util_gpu_error_check.cuh"

#ifdef EPIC_CUDA_AVAILABLE

namespace epic {

/* this is to be used by the GPU kernels */
class GpuTxnArray
{
public:
    const size_t kBaseTxnSize;
    void *txns = nullptr;
    size_t num_txns;
    template<typename TxnType>
    explicit GpuTxnArray(TxnArray<TxnType> &txn_array)
    : kBaseTxnSize(BaseTxnSize<TxnType>::value)
    {
        txns = txn_array.txns;
        num_txns = txn_array.num_txns;
    }
    explicit GpuTxnArray(size_t num_txns, size_t baseTxnSize)
        : num_txns(num_txns)
        , kBaseTxnSize(baseTxnSize)
    {
        auto &logger = Logger::GetInstance();
        logger.Trace("Allocating {} bytes for {} txns", formatSizeBytes(kBaseTxnSize * num_txns), num_txns);
        gpu_err_check(cudaMalloc(&txns, kBaseTxnSize * num_txns));
    };

    __device__ __host__ inline BaseTxn *getTxn(size_t index) const
    {
        assert(index < num_txns);
        size_t offset = index * kBaseTxnSize;
        return reinterpret_cast<BaseTxn *>(reinterpret_cast<uint8_t *>(txns) + offset);
    }
};

class GpuPackedTxnArray
{
public:
    uint8_t *txns = nullptr;
    uint32_t *index = nullptr;
    uint32_t size;
    uint32_t num_txns;

    template <typename TxnType>
    __device__ __host__ GpuPackedTxnArray(PackedTxnArray<TxnType> &txn_array)
        : num_txns{txn_array.num_txns}
        , txns{txn_array.txns}
        , index{txn_array.index}
        , size{txn_array.size}
    {}

    BaseTxn *__device__ __host__ getTxn(size_t txn_id) const
    {
        assert(txn_id < num_txns);
        assert(index[txn_id] < size);
        return reinterpret_cast<BaseTxn *>(&txns[index[txn_id]]);
    }
};

} // namespace epic

#endif // EPIC_CUDA_AVAILABLE

#endif // GPU_TXN_CUH
