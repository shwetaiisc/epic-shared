//
// Created by Shujian Qian on 2023-11-20.
//

#ifndef EPIC_BENCHMARKS_TPCC_GPU_INDEX_H
#define EPIC_BENCHMARKS_TPCC_GPU_INDEX_H

#include <any>

#include <benchmarks/tpcc_table.h>
#include <benchmarks/tpcc_index.h>

namespace epic::tpcc {

template <typename TxnArrayType, typename TxnParamArrayType>
class TpccGpuIndex : public TpccIndex<TxnArrayType, TxnParamArrayType>
{
public:
    TpccConfig tpcc_config;
    std::any gpu_index_impl;
    explicit TpccGpuIndex(TpccConfig tpcc_config);

    void loadInitialData() override;
    void indexTxns(TxnArrayType &txn_array, TxnParamArrayType &index_array, uint32_t epoch_id) override;

};

} // namespace epic::tpcc

#endif // EPIC_BENCHMARKS_TPCC_GPU_INDEX_H
