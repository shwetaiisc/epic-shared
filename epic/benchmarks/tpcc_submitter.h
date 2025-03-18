//
// Created by Shujian Qian on 2023-10-11.
//

#ifndef TPCC_SUBMITTER_H
#define TPCC_SUBMITTER_H

#include <cstdint>
#include <tuple>

#include "util_log.h"
#include "tpcc_txn.h"

namespace epic::tpcc {

template <typename TxnParamArrayType>
class TpccSubmitter
{
public:
    struct TableSubmitDest
    {
        uint32_t *d_num_ops = nullptr;
        uint32_t *d_op_offsets = nullptr;
        void *d_submitted_ops = nullptr;
        void *temp_storage = nullptr;
        size_t temp_storage_bytes = 0;
        uint32_t &curr_num_ops;
    };

    TableSubmitDest warehouse_submit_dest;
    TableSubmitDest district_submit_dest;
    TableSubmitDest customer_submit_dest;
    TableSubmitDest history_submit_dest;
    TableSubmitDest new_order_submit_dest;
    TableSubmitDest order_submit_dest;
    TableSubmitDest order_line_submit_dest;
    TableSubmitDest item_submit_dest;
    TableSubmitDest stock_submit_dest;

    virtual ~TpccSubmitter() = default;
    TpccSubmitter(TableSubmitDest warehouse_submit_dest, TableSubmitDest district_submit_dest,
        TableSubmitDest customer_submit_dest, TableSubmitDest history_submit_dest,
        TableSubmitDest new_order_submit_dest, TableSubmitDest order_submit_dest,
        TableSubmitDest order_line_submit_dest, TableSubmitDest item_submit_dest, TableSubmitDest stock_submit_dest)
        : warehouse_submit_dest(warehouse_submit_dest)
        , district_submit_dest(district_submit_dest)
        , customer_submit_dest(customer_submit_dest)
        , history_submit_dest(history_submit_dest)
        , new_order_submit_dest(new_order_submit_dest)
        , order_submit_dest(order_submit_dest)
        , order_line_submit_dest(order_line_submit_dest)
        , item_submit_dest(item_submit_dest)
        , stock_submit_dest(stock_submit_dest)
    {}

    virtual void submit(TxnParamArrayType &txn_array)
    {
        auto &logger = Logger::GetInstance();
        logger.Error("TpccSubmittor::submit not implemented");
    };
};
} // namespace epic::tpcc

#endif // TPCC_SUBMITTER_H
