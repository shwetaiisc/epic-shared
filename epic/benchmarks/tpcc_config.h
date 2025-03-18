//
// Created by Shujian Qian on 2023-09-20.
//

#ifndef TPCC_CONFIG_H
#define TPCC_CONFIG_H

#include <cstdint>
#include <cstdlib>

#include "util_device_type.h"

namespace epic::tpcc {

struct TpccTxnMix
{
    uint32_t new_order = 50;
    uint32_t payment = 50;
    uint32_t order_status = 0;
    uint32_t delivery = 0;
    uint32_t stock_level = 0;

    TpccTxnMix() = default;
    TpccTxnMix(uint32_t new_order, uint32_t payment, uint32_t order_status, uint32_t delivery, uint32_t stock_level);
};

struct TpccConfig
{
    TpccTxnMix txn_mix;
    size_t num_txns = 100'000;
    size_t epochs = 20;
    size_t num_warehouses = 8;
    size_t order_table_size = 1'000'000;
    size_t orderline_table_size = 15'000'000;
    DeviceType index_device = DeviceType::GPU;
    DeviceType initialize_device = DeviceType::GPU;
    DeviceType execution_device = DeviceType::GPU;
    bool gacco_separate_txn_queue = true;
    bool gacco_use_atomic = false;
    bool gacco_tpcc_stock_use_atomic = true;
    uint32_t cpu_exec_num_threads = 1;

    size_t warehouseTableSize() const
    {
        return num_warehouses * 2;
    }
    size_t districtTableSize() const
    {
        return num_warehouses * 2 * 10;
    }
    size_t customerTableSize() const
    {
        return num_warehouses * 2 * 10 * 3'000;
    }
    size_t historyTableSize() const
    {
        return num_warehouses * 2 * 20;
        /* TODO: fix history table size */
        return num_warehouses * 2 * 20 * 96'000 * num_warehouses * 2 * 20;
    }
    size_t newOrderTableSize() const
    {
        return num_warehouses * 2 * 10 * 900 + num_txns * (epochs + 1);
    }
    size_t orderTableSize() const
    {
        return num_warehouses * 2 * 10 * 3000 + num_txns * (epochs + 1);
    }
    size_t orderLineTableSize() const
    {
        return num_warehouses * 10 * 15 * 3000 + num_txns * (epochs + 1) * 15;
    }
    size_t itemTableSize() const
    {
        return 200'000;
    }
    size_t stockTableSize() const
    {
        return 200'000 * num_warehouses;
    }
};

} // namespace epic::tpcc

#endif // TPCC_CONFIG_H
